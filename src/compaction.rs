//! Size-tiered compaction: merges a contiguous, id-ordered run of SSTables
//! into one, bounding read and space amplification.
//!
//! Correctness rule: inputs MUST be a contiguous run in id order (`inputs[0]`
//! is the oldest). The merged output takes the highest input id, so every
//! other table on disk keeps a correct older/newer relationship to it. On a
//! duplicate key the record from the highest input id (the newest) wins.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::path::Path;

use crate::error::Result;
use crate::record::Record;
use crate::sstable::{SsTableReader, SsTableWriter};

/// One record in flight through the k-way merge, tagged with its source index.
struct MergeItem {
    record: Record,
    input_idx: usize,
}

impl PartialEq for MergeItem {
    fn eq(&self, other: &Self) -> bool {
        self.record.key == other.record.key && self.input_idx == other.input_idx
    }
}
impl Eq for MergeItem {}

impl Ord for MergeItem {
    /// Order by key ascending, then by input index ascending. Used inside a
    /// `Reverse` min-heap, so the merge yields a key-ascending stream with all
    /// of a key's versions grouped together for per-key retention.
    fn cmp(&self, other: &Self) -> Ordering {
        self.record
            .key
            .cmp(&other.record.key)
            .then(self.input_idx.cmp(&other.input_idx))
    }
}
impl PartialOrd for MergeItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Merge `inputs` into a single SSTable written to `out_path`.
///
/// `drop_tombstones` must be true only when the run includes the
/// globally-oldest SSTable, since no older table can then still need a delete
/// marker to shadow it.
///
/// `min_snapshot_seq` is the lowest sequence number any live snapshot can
/// observe (or `u64::MAX` if there are none). Every version at or above it is
/// preserved for snapshot reads; below it only the newest version per key is
/// kept. Output is sorted key-ascending, and seq-descending within a key.
/// Returns the number of records written.
pub fn compact(
    out_path: &Path,
    inputs: &[&SsTableReader],
    drop_tombstones: bool,
    min_snapshot_seq: u64,
) -> Result<u64> {
    let mut iters: Vec<_> = inputs
        .iter()
        .map(|r| r.iter_all())
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .map(|v| v.into_iter())
        .collect();

    let mut heap: BinaryHeap<std::cmp::Reverse<MergeItem>> = BinaryHeap::new();
    for (idx, it) in iters.iter_mut().enumerate() {
        if let Some(record) = it.next() {
            heap.push(std::cmp::Reverse(MergeItem {
                record,
                input_idx: idx,
            }));
        }
    }

    // Drain the k-way merge into a flat, key-ascending stream.
    let mut stream: Vec<Record> = Vec::new();
    while let Some(std::cmp::Reverse(item)) = heap.pop() {
        if let Some(next) = iters[item.input_idx].next() {
            heap.push(std::cmp::Reverse(MergeItem {
                record: next,
                input_idx: item.input_idx,
            }));
        }
        stream.push(item.record);
    }

    // Process each key's versions: keep everything visible to a snapshot, plus
    // the single newest version below the snapshot horizon.
    let mut out: Vec<Record> = Vec::new();
    let mut i = 0;
    while i < stream.len() {
        let mut j = i + 1;
        while j < stream.len() && stream[j].key == stream[i].key {
            j += 1;
        }
        let mut group: Vec<Record> = stream[i..j].to_vec();
        group.sort_by(|a, b| b.seq.cmp(&a.seq)); // seq-descending

        let mut kept: Vec<Record> = Vec::new();
        let mut newest_below_taken = false;
        for rec in group {
            if rec.seq >= min_snapshot_seq {
                kept.push(rec);
            } else if !newest_below_taken {
                kept.push(rec);
                newest_below_taken = true;
            }
        }
        // In the oldest run a tombstone shadows nothing once no kept version
        // is older than it; peel such trailing tombstones off the group.
        if drop_tombstones {
            while kept.last().is_some_and(|r| r.value.is_none()) {
                kept.pop();
            }
        }
        out.extend(kept);
        i = j;
    }

    SsTableWriter::write(out_path, &out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table(dir: &Path, name: &str, records: &[Record]) -> SsTableReader {
        let path = dir.join(name);
        SsTableWriter::write(&path, records).unwrap();
        SsTableReader::open(&path, true).unwrap()
    }

    #[test]
    fn newest_input_wins_on_duplicate_key() {
        let dir = tempfile::tempdir().unwrap();
        let old = table(
            dir.path(),
            "a.db",
            &[Record::put(b"k".to_vec(), b"old".to_vec(), 1)],
        );
        let new = table(
            dir.path(),
            "b.db",
            &[Record::put(b"k".to_vec(), b"new".to_vec(), 2)],
        );
        let out = dir.path().join("out.db");
        compact(&out, &[&old, &new], false, u64::MAX).unwrap();
        let r = SsTableReader::open(&out, true).unwrap();
        assert_eq!(r.get(b"k").unwrap().unwrap().value, Some(b"new".to_vec()));
    }

    #[test]
    fn tombstone_dropped_when_oldest_run() {
        let dir = tempfile::tempdir().unwrap();
        let old = table(
            dir.path(),
            "a.db",
            &[Record::put(b"k".to_vec(), b"v".to_vec(), 1)],
        );
        let new = table(dir.path(), "b.db", &[Record::tombstone(b"k".to_vec(), 2)]);
        let out = dir.path().join("out.db");
        let n = compact(&out, &[&old, &new], true, u64::MAX).unwrap();
        assert_eq!(n, 0);
        let r = SsTableReader::open(&out, true).unwrap();
        assert!(r.get(b"k").unwrap().is_none());
    }

    #[test]
    fn tombstone_retained_when_not_oldest_run() {
        let dir = tempfile::tempdir().unwrap();
        let old = table(
            dir.path(),
            "a.db",
            &[Record::put(b"k".to_vec(), b"v".to_vec(), 1)],
        );
        let new = table(dir.path(), "b.db", &[Record::tombstone(b"k".to_vec(), 2)]);
        let out = dir.path().join("out.db");
        let n = compact(&out, &[&old, &new], false, u64::MAX).unwrap();
        assert_eq!(n, 1);
        let r = SsTableReader::open(&out, true).unwrap();
        assert!(r.get(b"k").unwrap().unwrap().value.is_none());
    }

    #[test]
    fn snapshot_horizon_preserves_old_versions() {
        let dir = tempfile::tempdir().unwrap();
        let old = table(
            dir.path(),
            "a.db",
            &[Record::put(b"k".to_vec(), b"v1".to_vec(), 1)],
        );
        let new = table(
            dir.path(),
            "b.db",
            &[Record::put(b"k".to_vec(), b"v2".to_vec(), 5)],
        );
        let out = dir.path().join("out.db");
        // A snapshot horizon of 3 keeps v2 (seq 5 >= 3) and the newest version
        // below the horizon (v1, seq 1).
        compact(&out, &[&old, &new], true, 3).unwrap();
        let r = SsTableReader::open(&out, true).unwrap();
        assert_eq!(r.record_count(), 2);
        assert_eq!(r.get(b"k").unwrap().unwrap().value, Some(b"v2".to_vec()));
        // The snapshot at horizon 3 still sees v1.
        assert_eq!(
            r.get_at(b"k", 3).unwrap().unwrap().value,
            Some(b"v1".to_vec())
        );
    }

    #[test]
    fn no_snapshot_collapses_to_newest_version() {
        let dir = tempfile::tempdir().unwrap();
        let old = table(
            dir.path(),
            "a.db",
            &[Record::put(b"k".to_vec(), b"v1".to_vec(), 1)],
        );
        let new = table(
            dir.path(),
            "b.db",
            &[Record::put(b"k".to_vec(), b"v2".to_vec(), 5)],
        );
        let out = dir.path().join("out.db");
        compact(&out, &[&old, &new], true, u64::MAX).unwrap();
        let r = SsTableReader::open(&out, true).unwrap();
        assert_eq!(r.record_count(), 1);
        assert_eq!(r.get(b"k").unwrap().unwrap().value, Some(b"v2".to_vec()));
    }

    #[test]
    fn disjoint_keys_all_survive() {
        let dir = tempfile::tempdir().unwrap();
        let a = table(
            dir.path(),
            "a.db",
            &[
                Record::put(b"a".to_vec(), b"1".to_vec(), 1),
                Record::put(b"c".to_vec(), b"3".to_vec(), 2),
            ],
        );
        let b = table(
            dir.path(),
            "b.db",
            &[Record::put(b"b".to_vec(), b"2".to_vec(), 3)],
        );
        let out = dir.path().join("out.db");
        compact(&out, &[&a, &b], false, u64::MAX).unwrap();
        let r = SsTableReader::open(&out, true).unwrap();
        assert_eq!(r.record_count(), 3);
        for (k, v) in [(b"a", b"1"), (b"b", b"2"), (b"c", b"3")] {
            assert_eq!(r.get(k).unwrap().unwrap().value, Some(v.to_vec()));
        }
    }
}
