//! SSTable: an immutable, sorted on-disk run of records.
//!
//! File layout (v1):
//! ```text
//! [data section]   concatenated Record encodings, ascending by key
//! [sparse index]   every Nth key -> data offset
//! [footer]         [index_offset:8][index_entries:4][record_count:8][magic:4]
//! ```
//! A point lookup binary-searches the sparse index for the bracketing block,
//! reads just that block, and scans it.

use std::cmp::Ordering;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

use crate::error::{Error, Result};
use crate::record::Record;

const MAGIC: &[u8; 4] = b"LSM1";
const FOOTER_LEN: u64 = 24;
/// One sparse-index entry per this many records.
const SPARSE_INTERVAL: usize = 16;

/// Streams a sorted record sequence into a new SSTable file.
pub struct SsTableWriter;

impl SsTableWriter {
    /// Write `records` (which MUST be sorted ascending by key) to `path`.
    /// Returns the number of records written.
    pub fn write<'a, I>(path: &Path, records: I) -> Result<u64>
    where
        I: IntoIterator<Item = &'a Record>,
    {
        let mut w = BufWriter::new(File::create(path)?);
        let mut offset: u64 = 0;
        let mut count: u64 = 0;
        let mut index: Vec<(Vec<u8>, u64)> = Vec::new();
        let mut buf = Vec::new();

        for record in records {
            if (count as usize) % SPARSE_INTERVAL == 0 {
                index.push((record.key.clone(), offset));
            }
            buf.clear();
            record.encode_into(&mut buf);
            w.write_all(&buf)?;
            offset += buf.len() as u64;
            count += 1;
        }

        let index_offset = offset;
        for (key, off) in &index {
            w.write_all(&(key.len() as u32).to_le_bytes())?;
            w.write_all(key)?;
            w.write_all(&off.to_le_bytes())?;
        }
        w.write_all(&index_offset.to_le_bytes())?;
        w.write_all(&(index.len() as u32).to_le_bytes())?;
        w.write_all(&count.to_le_bytes())?;
        w.write_all(MAGIC)?;
        w.flush()?;
        w.get_ref().sync_all()?;
        Ok(count)
    }
}

/// Read handle over an immutable SSTable. The sparse index is held in memory;
/// data blocks are read on demand.
pub struct SsTableReader {
    file: File,
    #[allow(dead_code)] // surfaced via path(); used by Phase 2 compaction
    path: PathBuf,
    /// Sparse index: (first key of block, data offset), ascending by key.
    index: Vec<(Vec<u8>, u64)>,
    /// Length of the data section (== offset where the index begins).
    data_len: u64,
    #[allow(dead_code)] // surfaced via record_count(); used by Phase 2 compaction
    record_count: u64,
}

impl SsTableReader {
    pub fn open(path: impl Into<PathBuf>) -> Result<SsTableReader> {
        let path = path.into();
        let file = File::open(&path)?;
        let file_len = file.metadata()?.len();
        if file_len < FOOTER_LEN {
            return Err(Error::BadFormat("file smaller than footer".into()));
        }

        let mut footer = [0u8; FOOTER_LEN as usize];
        file.read_exact_at(&mut footer, file_len - FOOTER_LEN)?;
        if &footer[20..24] != MAGIC {
            return Err(Error::BadFormat("bad magic".into()));
        }
        let index_offset = u64::from_le_bytes(footer[0..8].try_into().unwrap());
        let index_entries = u32::from_le_bytes(footer[8..12].try_into().unwrap()) as usize;
        let record_count = u64::from_le_bytes(footer[12..20].try_into().unwrap());

        let index_end = file_len - FOOTER_LEN;
        if index_offset > index_end {
            return Err(Error::BadFormat("index offset past footer".into()));
        }
        let mut ibuf = vec![0u8; (index_end - index_offset) as usize];
        file.read_exact_at(&mut ibuf, index_offset)?;

        let mut index = Vec::with_capacity(index_entries);
        let mut pos = 0usize;
        for _ in 0..index_entries {
            let key_len = read_u32(&ibuf, &mut pos)? as usize;
            let key = read_bytes(&ibuf, &mut pos, key_len)?.to_vec();
            let off = read_u64(&ibuf, &mut pos)?;
            index.push((key, off));
        }

        Ok(SsTableReader {
            file,
            path,
            index,
            data_len: index_offset,
            record_count,
        })
    }

    /// Filesystem path of this SSTable (used by Phase 2 compaction).
    #[allow(dead_code)]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Total number of records in the table.
    #[allow(dead_code)] // used by tests and Phase 2 compaction planning
    pub fn record_count(&self) -> u64 {
        self.record_count
    }

    /// Look up `key`, returning its most recent record in this table (which
    /// may be a tombstone) or `None` if the table does not contain it.
    pub fn get(&self, key: &[u8]) -> Result<Option<Record>> {
        if self.index.is_empty() {
            return Ok(None);
        }
        let block_idx = match self.index.binary_search_by(|(k, _)| k.as_slice().cmp(key)) {
            Ok(i) => i,
            Err(0) => return Ok(None), // key precedes the first key in the table
            Err(i) => i - 1,
        };
        let start = self.index[block_idx].1;
        let end = self
            .index
            .get(block_idx + 1)
            .map(|(_, o)| *o)
            .unwrap_or(self.data_len);

        let mut block = vec![0u8; (end - start) as usize];
        self.file.read_exact_at(&mut block, start)?;

        let mut pos = 0;
        while pos < block.len() {
            let rec = Record::decode_at(&block, &mut pos)?;
            match rec.key.as_slice().cmp(key) {
                Ordering::Equal => return Ok(Some(rec)),
                Ordering::Greater => return Ok(None),
                Ordering::Less => {}
            }
        }
        Ok(None)
    }

    /// Every record in the table, ascending by key. Reads the whole data
    /// section into memory; used by flush verification and (later) compaction.
    #[allow(dead_code)] // used by tests and Phase 2 compaction
    pub fn iter_all(&self) -> Result<Vec<Record>> {
        let mut data = vec![0u8; self.data_len as usize];
        self.file.read_exact_at(&mut data, 0)?;
        let mut records = Vec::with_capacity(self.record_count as usize);
        let mut pos = 0;
        while pos < data.len() {
            records.push(Record::decode_at(&data, &mut pos)?);
        }
        Ok(records)
    }
}

fn read_bytes<'a>(buf: &'a [u8], pos: &mut usize, n: usize) -> Result<&'a [u8]> {
    let end = pos
        .checked_add(n)
        .filter(|&e| e <= buf.len())
        .ok_or_else(|| Error::Corrupt("sstable index truncated".into()))?;
    let out = &buf[*pos..end];
    *pos = end;
    Ok(out)
}

fn read_u32(buf: &[u8], pos: &mut usize) -> Result<u32> {
    Ok(u32::from_le_bytes(
        read_bytes(buf, pos, 4)?.try_into().unwrap(),
    ))
}

fn read_u64(buf: &[u8], pos: &mut usize) -> Result<u64> {
    Ok(u64::from_le_bytes(
        read_bytes(buf, pos, 8)?.try_into().unwrap(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_table(records: &[Record]) -> (tempfile::TempDir, SsTableReader) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.db");
        let n = SsTableWriter::write(&path, records.iter()).unwrap();
        assert_eq!(n, records.len() as u64);
        let reader = SsTableReader::open(&path).unwrap();
        (dir, reader)
    }

    #[test]
    fn write_then_point_lookup() {
        let recs: Vec<_> = (0u32..200)
            .map(|i| Record::put(format!("key{i:04}").into_bytes(), vec![i as u8], i as u64))
            .collect();
        let (_d, r) = write_table(&recs);
        assert_eq!(r.record_count(), 200);
        for i in [0u32, 1, 15, 16, 17, 99, 199] {
            let got = r.get(format!("key{i:04}").as_bytes()).unwrap().unwrap();
            assert_eq!(got.value, Some(vec![i as u8]));
        }
        assert!(r.get(b"key9999").unwrap().is_none());
        assert!(r.get(b"aaa").unwrap().is_none());
    }

    #[test]
    fn tombstone_survives_round_trip() {
        let recs = vec![
            Record::put(b"a".to_vec(), b"1".to_vec(), 1),
            Record::tombstone(b"b".to_vec(), 2),
        ];
        let (_d, r) = write_table(&recs);
        assert!(r.get(b"b").unwrap().unwrap().value.is_none());
    }

    #[test]
    fn iter_all_returns_sorted_records() {
        let recs: Vec<_> = (0u32..50)
            .map(|i| Record::put(format!("k{i:03}").into_bytes(), vec![1], i as u64))
            .collect();
        let (_d, r) = write_table(&recs);
        assert_eq!(r.iter_all().unwrap(), recs);
    }

    #[test]
    fn empty_table_lookup() {
        let (_d, r) = write_table(&[]);
        assert_eq!(r.record_count(), 0);
        assert!(r.get(b"anything").unwrap().is_none());
    }

    #[test]
    fn bad_magic_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("junk.db");
        std::fs::write(&path, vec![0u8; 64]).unwrap();
        assert!(SsTableReader::open(&path).is_err());
    }
}
