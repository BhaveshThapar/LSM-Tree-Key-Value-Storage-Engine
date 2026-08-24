//! A build that changes a checksum has to keep reading what the last one wrote.
//!
//! This is the hazard the version gate exists for, and it is not a failed open:
//! the manifest's frames would all fail their CRC, it would replay to an empty
//! state, and the open would decide every SSTable in the directory was an orphan
//! and delete it. So the assertion at the end is on the data still being there.

use std::path::Path;

use lsm_kv::{Db, Maintenance, Options, SyncMode};

const HEADER_LEN: usize = 8;

fn opts() -> Options {
    Options {
        memtable_threshold: 4096,
        sync_wal: SyncMode::None,
        maintenance: Maintenance::Manual,
        ..Options::default()
    }
}

/// Rewrite a frame file in an older format: a different version in the header,
/// and every frame re-checksummed with CRC-32/ISO-HDLC.
///
/// The frames themselves are copied byte for byte. Only the header's version
/// and the four-byte CRC in front of each frame change, which is exactly the
/// difference between what an older build wrote and what this one does.
fn downgrade_to_iso_hdlc(path: &Path, version: u16) {
    let bytes = std::fs::read(path).unwrap();
    assert!(bytes.len() >= HEADER_LEN, "{path:?} has no header");

    let mut out = bytes[..HEADER_LEN].to_vec();
    out[4..6].copy_from_slice(&version.to_le_bytes());

    let mut pos = HEADER_LEN;
    while pos + 8 <= bytes.len() {
        let len = u32::from_le_bytes(bytes[pos + 4..pos + 8].try_into().unwrap()) as usize;
        let start = pos + 8;
        let end = start + len;
        assert!(end <= bytes.len(), "{path:?} has a torn frame");
        let payload = &bytes[start..end];
        out.extend(crc32fast::hash(payload).to_le_bytes());
        out.extend((len as u32).to_le_bytes());
        out.extend(payload);
        pos = end;
    }
    std::fs::write(path, &out).unwrap();
}

fn sstables(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with("sst_") && n.ends_with(".db"))
        .collect();
    names.sort();
    names
}

/// A directory whose manifest and WAL were written before the checksum changed
/// opens, keeps its SSTables, and returns every key.
#[test]
fn a_database_from_before_the_checksum_changed_still_opens_with_its_data() {
    let dir = tempfile::tempdir().unwrap();

    // Flushed keys, so there are SSTables the manifest names and the
    // reclamation loop could delete; and unflushed keys, so the WAL has frames
    // that have to replay.
    {
        let db = Db::open_with(dir.path(), opts()).unwrap();
        for i in 0..300u32 {
            db.put(format!("flushed{i:04}").as_bytes(), b"value")
                .unwrap();
        }
        db.flush().unwrap();
        for i in 0..50u32 {
            db.put(format!("buffered{i:04}").as_bytes(), b"value")
                .unwrap();
        }
    }

    let tables_before = sstables(dir.path());
    assert!(
        !tables_before.is_empty(),
        "nothing was flushed, so the reclamation hazard is not being tested"
    );

    // Turn the clock back on both log files.
    let current = std::fs::read_to_string(dir.path().join("CURRENT")).unwrap();
    downgrade_to_iso_hdlc(&dir.path().join(current.trim()), 1);
    downgrade_to_iso_hdlc(&dir.path().join("wal.log"), 2);

    let db = Db::open_with(dir.path(), opts()).unwrap();

    assert_eq!(
        sstables(dir.path()),
        tables_before,
        "opening an older database deleted its SSTables"
    );
    for i in 0..300u32 {
        assert_eq!(
            db.get(format!("flushed{i:04}").as_bytes())
                .unwrap()
                .as_deref(),
            Some(&b"value"[..]),
            "flushed key {i} was lost"
        );
    }
    for i in 0..50u32 {
        assert_eq!(
            db.get(format!("buffered{i:04}").as_bytes())
                .unwrap()
                .as_deref(),
            Some(&b"value"[..]),
            "buffered key {i} was lost, so the old WAL did not replay"
        );
    }
}

/// And it comes forward: after the open has rolled the manifest over and a
/// flush has replaced the WAL, both are at this build's version and checksum,
/// with nothing lost on the way.
#[test]
fn an_older_database_is_rewritten_at_the_current_version() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Db::open_with(dir.path(), opts()).unwrap();
        for i in 0..100u32 {
            db.put(format!("k{i:04}").as_bytes(), b"value").unwrap();
        }
        db.flush().unwrap();
        db.put(b"unflushed", b"value").unwrap();
    }

    let current = std::fs::read_to_string(dir.path().join("CURRENT")).unwrap();
    downgrade_to_iso_hdlc(&dir.path().join(current.trim()), 1);
    downgrade_to_iso_hdlc(&dir.path().join("wal.log"), 2);

    let db = Db::open_with(dir.path(), opts()).unwrap();
    db.put(b"after", b"value").unwrap();
    db.flush().unwrap();
    drop(db);

    let version_of = |name: &str| -> u16 {
        let bytes = std::fs::read(dir.path().join(name)).unwrap();
        u16::from_le_bytes([bytes[4], bytes[5]])
    };
    let current = std::fs::read_to_string(dir.path().join("CURRENT")).unwrap();
    assert_eq!(
        version_of(current.trim()),
        2,
        "the rollover left the manifest at the old version"
    );
    assert_eq!(
        version_of("wal.log"),
        3,
        "the flush left the WAL at the old version"
    );

    // Reopen once more: what was written under the old checksum, what was
    // written under the new one, and what crossed between them are all there.
    let db = Db::open_with(dir.path(), opts()).unwrap();
    for i in 0..100u32 {
        assert_eq!(
            db.get(format!("k{i:04}").as_bytes()).unwrap().as_deref(),
            Some(&b"value"[..])
        );
    }
    assert!(db.get(b"unflushed").unwrap().is_some());
    assert!(db.get(b"after").unwrap().is_some());
}
