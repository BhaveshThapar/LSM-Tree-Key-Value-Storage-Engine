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
    /// `Reverse` min-heap, so equal keys surface oldest-input-first — letting
    /// later (newer) duplicates overwrite earlier ones.
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

/// Merge `inputs` into a single SSTable written to `out_path`. `drop_tombstones`
/// must be true only when the run includes the globally-oldest SSTable, since
/// no older table can then still need a delete marker to shadow it.
/// Returns the number of records written.
pub fn compact(
    out_path: &Path,
    inputs: &[&SsTableReader],
    drop_tombstones: bool,
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

    let mut merged: Vec<Record> = Vec::new();
    while let Some(std::cmp::Reverse(item)) = heap.pop() {
        if let Some(next) = iters[item.input_idx].next() {
            heap.push(std::cmp::Reverse(MergeItem {
                record: next,
                input_idx: item.input_idx,
            }));
        }
        // Equal keys pop consecutively, oldest input first; keep the newest.
        match merged.last() {
            Some(last) if last.key == item.record.key => {
                *merged.last_mut().unwrap() = item.record;
            }
            _ => merged.push(item.record),
        }
    }

    if drop_tombstones {
        merged.retain(|r| r.value.is_some());
    }
    SsTableWriter::write(out_path, &merged)
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
        compact(&out, &[&old, &new], false).unwrap();
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
        let n = compact(&out, &[&old, &new], true).unwrap();
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
        let n = compact(&out, &[&old, &new], false).unwrap();
        assert_eq!(n, 1);
        let r = SsTableReader::open(&out, true).unwrap();
        assert!(r.get(b"k").unwrap().unwrap().value.is_none());
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
        compact(&out, &[&a, &b], false).unwrap();
        let r = SsTableReader::open(&out, true).unwrap();
        assert_eq!(r.record_count(), 3);
        for (k, v) in [(b"a", b"1"), (b"b", b"2"), (b"c", b"3")] {
            assert_eq!(r.get(k).unwrap().unwrap().value, Some(v.to_vec()));
        }
    }
}
