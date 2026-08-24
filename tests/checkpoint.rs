//! A checkpoint is a second set of names for the same bytes, and the property
//! that matters is that it survives the source deleting the first set.

use std::path::Path;

use lsm_kv::{Db, Maintenance, Options, SyncMode};

fn opts() -> Options {
    Options {
        memtable_threshold: 4 * 1024,
        compaction_threshold: 2,
        sync_wal: SyncMode::None,
        maintenance: Maintenance::Manual,
        ..Options::default()
    }
}

fn sstables(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with("sst_") && n.ends_with(".db"))
        .collect();
    names.sort();
    names
}

/// The bytes are shared, not copied.
///
/// Two names for one inode is the whole mechanism: it is why a checkpoint of a
/// gigabyte costs milliseconds, and why the source can go on to delete its own
/// name without taking the data with it.
#[test]
fn a_checkpoint_shares_its_bytes_with_the_source() {
    use std::os::unix::fs::MetadataExt;

    let source = tempfile::tempdir().unwrap();
    let holder = tempfile::tempdir().unwrap();
    let target = holder.path().join("cp");

    let db = Db::open_with(source.path(), opts()).unwrap();
    for i in 0..2_000u32 {
        db.put(
            format!("k{i:05}").as_bytes(),
            b"a value long enough to matter",
        )
        .unwrap();
        if db.pending_work() {
            db.maintain().unwrap();
        }
    }
    db.flush_only().unwrap();
    db.checkpoint(&target).unwrap();

    let linked = sstables(&target);
    assert!(!linked.is_empty(), "the checkpoint linked no tables");
    for name in &linked {
        let a = std::fs::metadata(source.path().join(name)).unwrap();
        let b = std::fs::metadata(target.join(name)).unwrap();
        assert_eq!(
            a.ino(),
            b.ino(),
            "{name} was copied rather than linked, so a checkpoint costs as much \
             as the data it describes"
        );
        assert!(a.nlink() >= 2, "{name} does not have a second name");
    }
}

/// The exit criterion: a checkpoint opens with the same contents after the
/// source has removed the names it linked.
///
/// The removal is done directly rather than by hoping a size-tiered compaction
/// picks the right run. What a compaction does to a table it has merged is
/// exactly this — unlink the name — and driving the engine until it happens to
/// choose those tables would make the test's coverage a function of a heuristic
/// rather than of the property being tested. The churn below still runs, so the
/// engine really is compacting; the assertion simply does not depend on which
/// run it picked.
#[test]
fn a_checkpoint_survives_the_source_losing_the_names_it_linked() {
    let source = tempfile::tempdir().unwrap();
    let holder = tempfile::tempdir().unwrap();
    let target = holder.path().join("cp");

    let db = Db::open_with(source.path(), opts()).unwrap();
    for i in 0..2_000u32 {
        db.put(
            format!("k{i:05}").as_bytes(),
            b"a value long enough to matter",
        )
        .unwrap();
        if db.pending_work() {
            db.maintain().unwrap();
        }
    }
    db.flush_only().unwrap();
    db.checkpoint(&target).unwrap();
    let linked = sstables(&target);

    // Real churn: rewrite everything and compact hard.
    for round in 0..3u32 {
        for i in 0..2_000u32 {
            db.put(
                format!("k{i:05}").as_bytes(),
                format!("REWRITTEN-{round}").as_bytes(),
            )
            .unwrap();
        }
        db.flush().unwrap();
        while db.maintain().unwrap() {}
    }
    drop(db);

    // And then the thing a compaction does to a table it has merged: unlink it.
    for name in &linked {
        let path = source.path().join(name);
        if path.exists() {
            std::fs::remove_file(&path).unwrap();
        }
    }
    for name in &linked {
        assert!(
            !source.path().join(name).exists(),
            "{name} still has a name in the source"
        );
    }

    // The checkpoint opens, and holds what it held when it was taken.
    let restored = Db::open_with(&target, opts()).unwrap();
    for i in 0..2_000u32 {
        assert_eq!(
            restored
                .get(format!("k{i:05}").as_bytes())
                .unwrap()
                .as_deref(),
            Some(&b"a value long enough to matter"[..]),
            "key {i} in the checkpoint was lost when the source removed the \
             names it had linked"
        );
    }
}

/// Everything buffered goes into the checkpoint, because it is flushed on the
/// way in.
#[test]
fn a_checkpoint_includes_what_was_still_in_memory() {
    let source = tempfile::tempdir().unwrap();
    let holder = tempfile::tempdir().unwrap();
    let target = holder.path().join("cp");

    let db = Db::open_with(
        source.path(),
        Options {
            memtable_threshold: 1 << 20,
            ..opts()
        },
    )
    .unwrap();
    for i in 0..50u32 {
        db.put(format!("buffered{i}").as_bytes(), b"v").unwrap();
    }
    assert_eq!(db.sstable_count(), 0, "something flushed early");

    db.checkpoint(&target).unwrap();
    drop(db);

    let restored = Db::open_with(&target, opts()).unwrap();
    for i in 0..50u32 {
        assert_eq!(
            restored.get(format!("buffered{i}").as_bytes()).unwrap(),
            Some(b"v".to_vec()),
            "a buffered key was not in the checkpoint"
        );
    }
}

/// A checkpoint is a point in time. Writes after it do not appear in it.
#[test]
fn writes_after_a_checkpoint_are_not_in_it() {
    let source = tempfile::tempdir().unwrap();
    let holder = tempfile::tempdir().unwrap();
    let target = holder.path().join("cp");

    let db = Db::open_with(source.path(), opts()).unwrap();
    db.put(b"before", b"v").unwrap();
    db.checkpoint(&target).unwrap();
    db.put(b"after", b"v").unwrap();
    db.flush().unwrap();
    drop(db);

    let restored = Db::open_with(&target, opts()).unwrap();
    assert_eq!(restored.get(b"before").unwrap(), Some(b"v".to_vec()));
    assert_eq!(
        restored.get(b"after").unwrap(),
        None,
        "a write made after the checkpoint appeared in it"
    );
}

#[test]
fn a_checkpoint_over_an_existing_one_is_refused() {
    let source = tempfile::tempdir().unwrap();
    let holder = tempfile::tempdir().unwrap();
    let target = holder.path().join("cp");

    let db = Db::open_with(source.path(), opts()).unwrap();
    db.put(b"k", b"v").unwrap();
    db.checkpoint(&target).unwrap();

    assert!(
        db.checkpoint(&target).is_err(),
        "a second checkpoint over the first would describe two states"
    );
}

/// The source keeps working afterwards, which is what would break if the
/// checkpoint had moved anything rather than linking it.
#[test]
fn the_source_is_unaffected_by_being_checkpointed() {
    let source = tempfile::tempdir().unwrap();
    let holder = tempfile::tempdir().unwrap();
    let target = holder.path().join("cp");

    let db = Db::open_with(source.path(), opts()).unwrap();
    for i in 0..500u32 {
        db.put(format!("k{i:04}").as_bytes(), b"v").unwrap();
        if db.pending_work() {
            db.maintain().unwrap();
        }
    }
    db.checkpoint(&target).unwrap();

    for i in 0..500u32 {
        assert_eq!(
            db.get(format!("k{i:04}").as_bytes()).unwrap(),
            Some(b"v".to_vec()),
            "the source lost key {i} to being checkpointed"
        );
    }
    db.put(b"after", b"v").unwrap();
    while db.maintain().unwrap() {}
    assert_eq!(db.get(b"after").unwrap(), Some(b"v".to_vec()));
}
