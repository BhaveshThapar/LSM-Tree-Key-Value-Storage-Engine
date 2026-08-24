//! Merging a key range across the MemTables and every SSTable.
//!
//! Each source is already sorted the same way — key ascending, sequence
//! descending within a key — so the merge is a k-way walk that keeps, for each
//! key, the first record it sees at or below the caller's horizon. That record
//! is the newest visible one, because sequence numbers are global: a record
//! from the MemTable and one from an SSTable are ordered against each other by
//! the same number, and "which source is newer" never has to be asked.
//!
//! Tombstones are resolved here rather than by the caller. A deleted key has a
//! visible record — that is the whole point of a tombstone — and it is not a
//! result.

use std::cmp::Ordering;

use crate::error::Result;
use crate::record::Record;

/// One record from one source, or the source's failure.
pub(crate) type Item = Result<Record>;

/// Merges pre-sorted sources into one key-ascending stream of live values.
pub(crate) struct Merge<I: Iterator<Item = Item>> {
    sources: Vec<Peeking<I>>,
    seq_bound: u64,
    end: Option<Vec<u8>>,
    /// The key just emitted or skipped. Every remaining version of it is
    /// shadowed and must be dropped without being looked at again.
    done: Option<Vec<u8>>,
}

/// An iterator with one item held back, because a merge has to compare heads
/// before deciding which source to advance.
struct Peeking<I: Iterator<Item = Item>> {
    iter: I,
    head: Option<Item>,
}

impl<I: Iterator<Item = Item>> Peeking<I> {
    fn new(mut iter: I) -> Self {
        let head = iter.next();
        Self { iter, head }
    }

    fn advance(&mut self) -> Option<Item> {
        std::mem::replace(&mut self.head, self.iter.next())
    }
}

impl<I: Iterator<Item = Item>> Merge<I> {
    pub(crate) fn new(sources: Vec<I>, seq_bound: u64, end: Option<Vec<u8>>) -> Self {
        Self {
            sources: sources.into_iter().map(Peeking::new).collect(),
            seq_bound,
            end,
            done: None,
        }
    }

    /// The index of the source whose head sorts first: smallest key, and among
    /// equal keys the largest sequence number.
    ///
    /// An error at the head of any source sorts first of all, so a failure is
    /// reported rather than being overtaken by a source that still works.
    fn next_source(&self) -> Option<usize> {
        let mut best: Option<usize> = None;
        for (i, source) in self.sources.iter().enumerate() {
            let head = match &source.head {
                Some(Err(_)) => return Some(i),
                Some(Ok(record)) => record,
                None => continue,
            };
            let better = match best {
                None => true,
                Some(b) => match &self.sources[b].head {
                    Some(Ok(current)) => match head.key.cmp(&current.key) {
                        Ordering::Less => true,
                        Ordering::Greater => false,
                        Ordering::Equal => head.seq > current.seq,
                    },
                    _ => true,
                },
            };
            if better {
                best = Some(i);
            }
        }
        best
    }
}

impl<I: Iterator<Item = Item>> Iterator for Merge<I> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let index = self.next_source()?;
            let record = match self.sources[index].advance()? {
                Ok(record) => record,
                Err(e) => return Some(Err(e)),
            };

            if let Some(end) = &self.end
                && record.key.as_slice() >= end.as_slice()
            {
                // Sources are key-ascending, so this one has nothing more in
                // range. Drop it and keep merging the others.
                self.sources[index].head = None;
                self.sources[index].iter.by_ref().for_each(drop);
                continue;
            }

            // Everything below the newest visible version of a key is shadowed,
            // including versions in other sources.
            if self.done.as_deref() == Some(record.key.as_slice()) {
                continue;
            }
            // A version above the horizon is invisible, but it does not shadow
            // anything: an older version of the same key may still be visible.
            if record.seq >= self.seq_bound {
                continue;
            }

            self.done = Some(record.key.clone());
            match record.value {
                Some(value) => return Some(Ok((record.key, value))),
                // A tombstone is the newest visible version, so the key is
                // absent. It shadows what is beneath it and is not a result.
                None => continue,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn put(key: &str, value: &str, seq: u64) -> Item {
        Ok(Record::put(
            key.as_bytes().to_vec(),
            value.as_bytes().to_vec(),
            seq,
        ))
    }

    fn dead(key: &str, seq: u64) -> Item {
        Ok(Record::tombstone(key.as_bytes().to_vec(), seq))
    }

    fn merged(sources: Vec<Vec<Item>>, bound: u64, end: Option<&str>) -> Vec<(String, String)> {
        Merge::new(
            sources.into_iter().map(|v| v.into_iter()).collect(),
            bound,
            end.map(|e| e.as_bytes().to_vec()),
        )
        .map(|r| {
            let (k, v) = r.unwrap();
            (String::from_utf8(k).unwrap(), String::from_utf8(v).unwrap())
        })
        .collect()
    }

    #[test]
    fn sources_interleave_by_key() {
        let got = merged(
            vec![
                vec![put("a", "1", 1), put("c", "3", 3)],
                vec![put("b", "2", 2), put("d", "4", 4)],
            ],
            u64::MAX,
            None,
        );
        assert_eq!(
            got,
            vec![
                ("a".into(), "1".into()),
                ("b".into(), "2".into()),
                ("c".into(), "3".into()),
                ("d".into(), "4".into()),
            ]
        );
    }

    /// Which source a record came from never matters. The sequence number does.
    #[test]
    fn the_highest_sequence_wins_regardless_of_source_order() {
        let newer_second = merged(
            vec![vec![put("k", "old", 1)], vec![put("k", "new", 9)]],
            u64::MAX,
            None,
        );
        let newer_first = merged(
            vec![vec![put("k", "new", 9)], vec![put("k", "old", 1)]],
            u64::MAX,
            None,
        );
        assert_eq!(newer_second, vec![("k".into(), "new".into())]);
        assert_eq!(newer_first, newer_second);
    }

    #[test]
    fn a_tombstone_removes_the_key_rather_than_appearing_as_one() {
        let got = merged(
            vec![
                vec![dead("k", 9), put("k", "old", 1)],
                vec![put("z", "z", 2)],
            ],
            u64::MAX,
            None,
        );
        assert_eq!(got, vec![("z".into(), "z".into())]);
    }

    /// A horizon below a tombstone sees the value the tombstone hid.
    #[test]
    fn a_horizon_below_a_tombstone_sees_what_it_covered() {
        let got = merged(vec![vec![dead("k", 9), put("k", "old", 1)]], 5, None);
        assert_eq!(got, vec![("k".into(), "old".into())]);
    }

    #[test]
    fn the_end_bound_is_exclusive() {
        let got = merged(
            vec![vec![put("a", "1", 1), put("b", "2", 2), put("c", "3", 3)]],
            u64::MAX,
            Some("c"),
        );
        assert_eq!(
            got,
            vec![("a".into(), "1".into()), ("b".into(), "2".into())]
        );
    }

    #[test]
    fn an_error_in_a_source_is_reported_rather_than_skipped() {
        let sources = vec![
            vec![Err(crate::error::Error::Corrupt("block".into()))],
            vec![put("a", "1", 1)],
        ];
        let mut merge = Merge::new(
            sources.into_iter().map(|v| v.into_iter()).collect(),
            u64::MAX,
            None,
        );
        assert!(
            merge.next().is_some_and(|r| r.is_err()),
            "a failing source was overtaken by a working one"
        );
    }

    #[test]
    fn no_sources_is_an_empty_scan() {
        let empty: Vec<Vec<Item>> = vec![];
        assert!(merged(empty, u64::MAX, None).is_empty());
    }
}
