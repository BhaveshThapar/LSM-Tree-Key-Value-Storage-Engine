//! In-memory write buffer. Mutations land here (after the WAL) and are kept
//! sorted by key so a flush can stream them straight into an SSTable.

use std::collections::BTreeMap;

use crate::record::Record;

/// Default flush threshold: 4 MiB of approximate live data.
pub const DEFAULT_THRESHOLD: usize = 4 * 1024 * 1024;

/// A sorted, size-bounded map of the most recent mutation per key.
pub struct MemTable {
    map: BTreeMap<Vec<u8>, Record>,
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

    /// Insert or overwrite the record for its key, updating the size estimate.
    pub fn insert(&mut self, record: Record) {
        let new_len = record.encoded_len();
        match self.map.insert(record.key.clone(), record) {
            Some(old) => {
                self.size = self.size - old.encoded_len() + new_len;
            }
            None => self.size += new_len,
        }
    }

    /// The most recent record for `key`, if any (may be a tombstone).
    #[allow(dead_code)] // convenience wrapper over `get_at`; used in tests
    pub fn get(&self, key: &[u8]) -> Option<&Record> {
        self.map.get(key)
    }

    /// The record for `key` if it is visible at `seq_bound` (its seq is below
    /// the bound). The MemTable holds only the latest version per key, so a
    /// snapshot taken between two writes of the same key cannot see the older
    /// one while both are still buffered here — see the Design B note.
    pub fn get_at(&self, key: &[u8], seq_bound: u64) -> Option<&Record> {
        self.map.get(key).filter(|r| r.seq < seq_bound)
    }

    /// Records in ascending key order — the flush iteration order.
    pub fn iter(&self) -> impl Iterator<Item = &Record> {
        self.map.values()
    }

    /// Approximate serialized size of the buffered data, in bytes.
    #[allow(dead_code)] // used by tests and Phase 2 stats reporting
    pub fn size(&self) -> usize {
        self.size
    }

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
    fn overwrite_keeps_latest_and_adjusts_size() {
        let mut m = MemTable::new(DEFAULT_THRESHOLD);
        m.insert(Record::put(b"k".to_vec(), b"short".to_vec(), 1));
        let size_after_first = m.size();
        m.insert(Record::put(
            b"k".to_vec(),
            b"a much longer value".to_vec(),
            2,
        ));
        assert_eq!(m.len(), 1);
        assert!(m.size() > size_after_first);
        assert_eq!(m.get(b"k").unwrap().seq, 2);
    }

    #[test]
    fn tombstone_overwrites_value() {
        let mut m = MemTable::new(DEFAULT_THRESHOLD);
        m.insert(Record::put(b"k".to_vec(), b"v".to_vec(), 1));
        m.insert(Record::tombstone(b"k".to_vec(), 2));
        assert!(m.get(b"k").unwrap().value.is_none());
    }

    #[test]
    fn iter_is_sorted() {
        let mut m = MemTable::new(DEFAULT_THRESHOLD);
        for k in [b"c", b"a", b"b"] {
            m.insert(Record::put(k.to_vec(), b"v".to_vec(), 1));
        }
        let keys: Vec<_> = m.iter().map(|r| r.key.clone()).collect();
        assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
    }

    #[test]
    fn is_full_tracks_threshold() {
        let mut m = MemTable::new(64);
        assert!(!m.is_full());
        while !m.is_full() {
            m.insert(Record::put(b"key".to_vec(), b"value".to_vec(), 1));
            // single key, so force growth with distinct keys instead
            m.insert(Record::put(vec![m.len() as u8], b"value".to_vec(), 1));
        }
        assert!(m.is_full());
    }
}
