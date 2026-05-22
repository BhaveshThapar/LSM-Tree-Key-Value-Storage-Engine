//! Property-based model test: drive the engine with random operation
//! sequences and assert it behaves identically to an in-memory `HashMap`
//! oracle, including point-in-time snapshots.

use std::collections::HashMap;

use lsm_kv::{Db, Options, Snapshot};
use proptest::prelude::*;

/// One operation in a generated sequence. The key space is deliberately tiny
/// (`0..16`) so keys collide often, exercising overwrites and compaction.
#[derive(Debug, Clone)]
enum Op {
    Put(u8, u8),
    Delete(u8),
    Flush,
    Snapshot,
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => (0u8..16, 0u8..255).prop_map(|(k, v)| Op::Put(k, v)),
        2 => (0u8..16).prop_map(Op::Delete),
        1 => Just(Op::Flush),
        1 => Just(Op::Snapshot),
    ]
}

fn run(ops: &[Op]) {
    let dir = tempfile::tempdir().unwrap();
    let opts = Options {
        // A small threshold so flushes and compaction fire frequently.
        memtable_threshold: 256,
        sync_wal: false,
        compaction_threshold: 3,
        ..Options::default()
    };
    let db = Db::open_with(dir.path(), opts).unwrap();

    let mut model: HashMap<u8, u8> = HashMap::new();
    // Each snapshot is pinned alongside the oracle state at capture time.
    let mut snapshots: Vec<(Snapshot, HashMap<u8, u8>)> = Vec::new();

    for op in ops {
        match op {
            Op::Put(k, v) => {
                db.put(&[*k], &[*v]).unwrap();
                model.insert(*k, *v);
            }
            Op::Delete(k) => {
                db.delete(&[*k]).unwrap();
                model.remove(k);
            }
            Op::Flush => {
                db.flush().unwrap();
            }
            Op::Snapshot => {
                // Flush first so every pre-snapshot write is on disk: a
                // Design B snapshot is fully correct once the active MemTable
                // holds nothing older than the horizon.
                db.flush().unwrap();
                snapshots.push((db.snapshot(), model.clone()));
            }
        }

        // The live view must match the oracle after every operation.
        for k in 0u8..16 {
            let got = db.get(&[k]).unwrap();
            assert_eq!(got.as_deref(), model.get(&k).map(std::slice::from_ref));
        }
        // Each snapshot must still match the oracle it was captured with.
        for (snap, snap_model) in &snapshots {
            for k in 0u8..16 {
                let got = db.get_at(snap, &[k]).unwrap();
                assert_eq!(
                    got.as_deref(),
                    snap_model.get(&k).map(std::slice::from_ref),
                    "snapshot view diverged for key {k}"
                );
            }
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(200))]

    #[test]
    fn engine_matches_hashmap_oracle(ops in prop::collection::vec(op_strategy(), 0..120)) {
        run(&ops);
    }
}
