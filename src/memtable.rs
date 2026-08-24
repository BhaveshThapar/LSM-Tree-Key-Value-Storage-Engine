//! In-memory write buffer. Mutations land here (after the WAL) and are kept
//! sorted by key so a flush can stream them straight into an SSTable.
//!
//! **Every version is kept, not just the newest.** Keeping one version per key
//! is smaller and is wrong in a way that only shows up through a snapshot: a
//! snapshot taken between two writes of the same key could not see the older
//! one while both were still buffered, so a read through it returned `None` for
//! a key that plainly had a value. The SSTables have always kept versions; the
//! MemTable now agrees with them, which also means a checkpoint built by reading
//! through a snapshot is correct without a full flush in front of it.
//!
//! Versions cost memory, and they are bounded by the same threshold as anything
//! else here: rewriting one key a thousand times fills the MemTable and triggers
//! a flush, exactly as writing a thousand keys would.

use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::ops::Bound;

use crate::record::Record;

/// Default flush threshold: 4 MiB of approximate live data.
pub const DEFAULT_THRESHOLD: usize = 4 * 1024 * 1024;

/// `(key, Reverse(seq))`, so the natural order of the map is key ascending and,
/// within a key, sequence *descending* — the same order an SSTable stores
/// versions in, and the order a bounded lookup wants: the first entry at or
/// below a horizon is the newest one visible.
type VersionKey = (Vec<u8>, Reverse<u64>);

/// A sorted, size-bounded map of every buffered version of every key.
pub struct MemTable {
    map: BTreeMap<VersionKey, Record>,
    size: usize,
    threshold: usize,
}

impl MemTable {
    pub fn new(threshold: usize) -> MemTable {
        MemTable {
            map: BTreeMap::new(),
            size: 0,
            threshold,
        }
    }

    /// Buffer a record. A second write of the same key at a *new* sequence
    /// number is a new version; writing the same key at the same sequence
    /// replaces it, which only happens on replay of the same WAL frame.
    pub fn insert(&mut self, record: Record) {
        let new_len = record.encoded_len();
        let key = (record.key.clone(), Reverse(record.seq));
        match self.map.insert(key, record) {
            Some(old) => self.size = self.size - old.encoded_len() + new_len,
            None => self.size += new_len,
        }
    }

    /// The most recent record for `key`, if any (may be a tombstone).
    #[allow(dead_code)] // convenience wrapper over `get_at`; used in tests
    pub fn get(&self, key: &[u8]) -> Option<&Record> {
        self.get_at(key, u64::MAX)
    }

    /// The newest record for `key` whose sequence number is below `seq_bound`.
    pub fn get_at(&self, key: &[u8], seq_bound: u64) -> Option<&Record> {
        // Descending by sequence within the key, so the first entry that is
        // below the bound is the newest one the bound can see.
        self.versions_of(key).find(|r| r.seq < seq_bound)
    }

    /// Every buffered version of `key`, newest first.
    fn versions_of(&self, key: &[u8]) -> impl Iterator<Item = &Record> {
        self.map
            .range((
                Bound::Included((key.to_vec(), Reverse(u64::MAX))),
                Bound::Included((key.to_vec(), Reverse(0))),
            ))
            .map(|(_, record)| record)
    }

    /// Records in ascending key order and descending sequence within a key —
    /// the flush iteration order, and the order an SSTable expects.
    pub fn iter(&self) -> impl Iterator<Item = &Record> {
        self.map.values()
    }

    /// Records within `start..end`, in the same order as [`MemTable::iter`].
    pub fn range<'a>(
        &'a self,
        start: Option<&[u8]>,
        end: Option<&'a [u8]>,
    ) -> impl Iterator<Item = &'a Record> {
        let low = match start {
            // `Reverse(u64::MAX)` is the *smallest* value for a given key, so
            // this includes every version of the start key.
            Some(k) => Bound::Included((k.to_vec(), Reverse(u64::MAX))),
            None => Bound::Unbounded,
        };
        self.map
            .range((low, Bound::Unbounded))
            .map(|(_, record)| record)
            .take_while(move |record| match end {
                Some(e) => record.key.as_slice() < e,
                None => true,
            })
    }

    /// Approximate serialized size of the buffered data, in bytes.
    #[allow(dead_code)] // used by tests and Phase 2 stats reporting
    pub fn size(&self) -> usize {
        self.size
    }

    /// How many records are buffered, counting every version separately.
    #[allow(dead_code)] // used by tests and Phase 2 stats reporting
    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn is_full(&self) -> bool {
        self.size >= self.threshold
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_get() {
        let mut m = MemTable::new(DEFAULT_THRESHOLD);
        m.insert(Record::put(b"k".to_vec(), b"v".to_vec(), 1));
        assert_eq!(m.get(b"k").unwrap().value, Some(b"v".to_vec()));
        assert_eq!(m.get(b"missing"), None);
    }

    #[test]
    fn a_rewrite_is_a_new_version_and_the_newest_one_wins() {
        let mut m = MemTable::new(DEFAULT_THRESHOLD);
        m.insert(Record::put(b"k".to_vec(), b"short".to_vec(), 1));
        let size_after_first = m.size();
        m.insert(Record::put(
            b"k".to_vec(),
            b"a much longer value".to_vec(),
            2,
        ));
        assert_eq!(m.len(), 2, "the older version was discarded");
        assert!(m.size() > size_after_first);
        assert_eq!(m.get(b"k").unwrap().seq, 2);
    }

    /// The bug this module's versions exist for. A horizon between two writes
    /// of one key used to see nothing at all, because only the newer version
    /// was kept and it was above the horizon.
    #[test]
    fn a_horizon_between_two_writes_sees_the_older_one() {
        let mut m = MemTable::new(DEFAULT_THRESHOLD);
        m.insert(Record::put(b"k".to_vec(), b"first".to_vec(), 1));
        m.insert(Record::put(b"k".to_vec(), b"second".to_vec(), 2));

        assert_eq!(
            m.get_at(b"k", 2).map(|r| r.value.clone()),
            Some(Some(b"first".to_vec())),
            "a horizon at 2 must see the write at 1"
        );
        assert_eq!(
            m.get_at(b"k", 3).map(|r| r.value.clone()),
            Some(Some(b"second".to_vec()))
        );
        assert!(
            m.get_at(b"k", 1).is_none(),
            "a horizon below every version must see nothing"
        );
    }

    #[test]
    fn tombstone_shadows_the_value_beneath_it() {
        let mut m = MemTable::new(DEFAULT_THRESHOLD);
        m.insert(Record::put(b"k".to_vec(), b"v".to_vec(), 1));
        m.insert(Record::tombstone(b"k".to_vec(), 2));
        assert!(m.get(b"k").unwrap().value.is_none());
        assert_eq!(
            m.get_at(b"k", 2).unwrap().value,
            Some(b"v".to_vec()),
            "a horizon below the tombstone must still see the value"
        );
    }

    #[test]
    fn iter_is_key_ascending_and_sequence_descending() {
        let mut m = MemTable::new(DEFAULT_THRESHOLD);
        for k in [b"c", b"a", b"b"] {
            m.insert(Record::put(k.to_vec(), b"v".to_vec(), 1));
        }
        m.insert(Record::put(b"b".to_vec(), b"v2".to_vec(), 9));

        let order: Vec<(Vec<u8>, u64)> = m.iter().map(|r| (r.key.clone(), r.seq)).collect();
        assert_eq!(
            order,
            vec![
                (b"a".to_vec(), 1),
                (b"b".to_vec(), 9),
                (b"b".to_vec(), 1),
                (b"c".to_vec(), 1),
            ]
        );
    }

    #[test]
    fn range_respects_both_bounds() {
        let mut m = MemTable::new(DEFAULT_THRESHOLD);
        for k in [b"a", b"b", b"c", b"d"] {
            m.insert(Record::put(k.to_vec(), b"v".to_vec(), 1));
        }
        let keys = |start: Option<&[u8]>, end: Option<&[u8]>| -> Vec<Vec<u8>> {
            m.range(start, end).map(|r| r.key.clone()).collect()
        };
        assert_eq!(
            keys(Some(b"b"), Some(b"d")),
            vec![b"b".to_vec(), b"c".to_vec()],
            "the end bound must be exclusive and the start inclusive"
        );
        assert_eq!(keys(None, Some(b"b")), vec![b"a".to_vec()]);
        assert_eq!(keys(Some(b"c"), None), vec![b"c".to_vec(), b"d".to_vec()]);
        assert_eq!(keys(None, None).len(), 4);
        assert!(keys(Some(b"x"), None).is_empty());
    }

    /// Every version of the start key is in range, not just the newest.
    #[test]
    fn range_includes_every_version_of_the_start_key() {
        let mut m = MemTable::new(DEFAULT_THRESHOLD);
        m.insert(Record::put(b"k".to_vec(), b"one".to_vec(), 1));
        m.insert(Record::put(b"k".to_vec(), b"two".to_vec(), 2));
        let seqs: Vec<u64> = m.range(Some(b"k"), None).map(|r| r.seq).collect();
        assert_eq!(seqs, vec![2, 1]);
    }

    #[test]
    fn is_full_tracks_threshold() {
        let mut m = MemTable::new(64);
        assert!(!m.is_full());
        let mut n = 0u8;
        while !m.is_full() {
            m.insert(Record::put(vec![n], b"value".to_vec(), u64::from(n)));
            n += 1;
        }
        assert!(m.is_full());
    }

    /// Rewriting one key a thousand times fills the MemTable, exactly as
    /// writing a thousand keys would. Versions are bounded by the threshold
    /// like everything else here.
    #[test]
    fn rewriting_one_key_still_fills_the_memtable() {
        let mut m = MemTable::new(1024);
        for seq in 0..1000u64 {
            m.insert(Record::put(b"k".to_vec(), b"value".to_vec(), seq));
            if m.is_full() {
                return;
            }
        }
        panic!("a thousand versions of one key did not fill a 1 KiB MemTable");
    }
}
