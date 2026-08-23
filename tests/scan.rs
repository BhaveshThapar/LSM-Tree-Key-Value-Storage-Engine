//! Scans, against a model. The engine's answer has to match a `BTreeMap`'s
//! whatever state the data is in — buffered, frozen, flushed, compacted, or
//! spread across all four.

use std::collections::BTreeMap;

use lsm_kv::{Db, Maintenance, Options, SyncMode, WriteBatch};

fn opts(threshold: usize) -> Options {
    Options {
        memtable_threshold: threshold,
        compaction_threshold: 2,
        sync_wal: SyncMode::None,
        maintenance: Maintenance::Manual,
        ..Options::default()
    }
}

fn key(i: u32) -> Vec<u8> {
    format!("k{i:04}").into_bytes()
}

/// The model: whatever a BTreeMap would say.
fn expected(
    model: &BTreeMap<Vec<u8>, Vec<u8>>,
    start: Option<&[u8]>,
    end: Option<&[u8]>,
    limit: usize,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    model
        .iter()
        .filter(|(k, _)| start.is_none_or(|s| k.as_slice() >= s))
        .filter(|(k, _)| end.is_none_or(|e| k.as_slice() < e))
        .take(limit)
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

/// One database, driven through every state the data can be in, checked against
/// the model after each. A scan must not depend on whether a write happens to
/// be in a MemTable or an SSTable.
#[test]
fn a_scan_matches_the_model_in_every_state() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open_with(dir.path(), opts(2048)).unwrap();
    let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();

    let check = |db: &Db, model: &BTreeMap<Vec<u8>, Vec<u8>>, where_: &str| {
        type Case = (Option<Vec<u8>>, Option<Vec<u8>>, usize);
        let cases: [Case; 7] = [
            (None, None, usize::MAX),
            (None, None, 5),
            (Some(key(10)), None, usize::MAX),
            (None, Some(key(10)), usize::MAX),
            (Some(key(5)), Some(key(15)), usize::MAX),
            (Some(key(5)), Some(key(15)), 3),
            (Some(b"zzz".to_vec()), None, usize::MAX),
        ];
        for (start, end, limit) in cases {
            let (s, e) = (start.as_deref(), end.as_deref());
            assert_eq!(
                db.scan(s, e, limit).unwrap(),
                expected(model, s, e, limit),
                "{where_}: scan({s:?}, {e:?}, {limit}) disagreed with the model"
            );
        }
    };

    // 1. Everything buffered.
    for i in 0..40u32 {
        db.put(&key(i), format!("v{i}").as_bytes()).unwrap();
        model.insert(key(i), format!("v{i}").into_bytes());
    }
    check(&db, &model, "buffered");

    // 2. Some of it flushed, the rest buffered.
    db.flush_only().unwrap();
    for i in 40..60u32 {
        db.put(&key(i), format!("v{i}").as_bytes()).unwrap();
        model.insert(key(i), format!("v{i}").into_bytes());
    }
    check(&db, &model, "half flushed");

    // 3. Rewrites and deletes spread across both.
    let mut batch = WriteBatch::new();
    for i in (0..60).step_by(3) {
        batch.put(&key(i), b"rewritten");
        model.insert(key(i), b"rewritten".to_vec());
    }
    for i in (1..60).step_by(7) {
        batch.delete(&key(i));
        model.remove(&key(i));
    }
    db.write_batch(&batch).unwrap();
    check(&db, &model, "rewritten and deleted");

    // 4. Several SSTables.
    for round in 0..3u32 {
        for i in 0..20u32 {
            let k = key(1000 + round * 100 + i);
            db.put(&k, b"extra").unwrap();
            model.insert(k, b"extra".to_vec());
        }
        db.flush_only().unwrap();
    }
    check(&db, &model, "several sstables");

    // 5. Compacted.
    db.flush().unwrap();
    check(&db, &model, "compacted");

    // 6. Reopened.
    drop(db);
    let db = Db::open_with(dir.path(), opts(2048)).unwrap();
    check(&db, &model, "reopened");
}

/// A scan through a snapshot sees the database as it was, including keys that
/// have since been deleted and values that have since been rewritten.
#[test]
fn a_scan_through_a_snapshot_sees_the_database_as_it_was() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open_with(dir.path(), opts(1 << 20)).unwrap();

    for i in 0..10u32 {
        db.put(&key(i), b"before").unwrap();
    }
    let snapshot = db.snapshot();

    let mut batch = WriteBatch::new();
    for i in 0..10u32 {
        batch.put(&key(i), b"after");
    }
    batch.delete(&key(3));
    db.write_batch(&batch).unwrap();

    let now = db.scan(None, None, usize::MAX).unwrap();
    assert_eq!(now.len(), 9, "the delete did not take effect");
    assert!(now.iter().all(|(_, v)| v == b"after"));

    let then = db.scan_at(&snapshot, None, None, usize::MAX).unwrap();
    assert_eq!(then.len(), 10, "the snapshot lost the deleted key");
    assert!(
        then.iter().all(|(_, v)| v == b"before"),
        "the snapshot saw writes made after it was taken"
    );
}

/// The claim that makes `scan` different from "read everything and filter": a
/// bounded scan does bounded work. Measured as blocks decompressed, which the
/// engine does not report — so measured instead as the thing a caller can see,
/// that a ten-key scan of a large table returns promptly and correctly.
#[test]
fn a_bounded_scan_of_a_large_table_returns_only_what_was_asked_for() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open_with(dir.path(), opts(1 << 16)).unwrap();
    for i in 0..20_000u32 {
        db.put(&key(i), b"value").unwrap();
        if db.pending_work() {
            db.maintain().unwrap();
        }
    }
    db.flush().unwrap();

    let got = db.scan(Some(&key(10_000)), None, 10).unwrap();
    assert_eq!(got.len(), 10);
    assert_eq!(got[0].0, key(10_000));
    assert_eq!(got[9].0, key(10_009));
}

#[test]
fn a_limit_of_zero_returns_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open_with(dir.path(), opts(1 << 20)).unwrap();
    db.put(b"k", b"v").unwrap();
    assert!(db.scan(None, None, 0).unwrap().is_empty());
}

#[test]
fn an_empty_database_scans_to_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open_with(dir.path(), opts(1 << 20)).unwrap();
    assert!(db.scan(None, None, usize::MAX).unwrap().is_empty());
}
