//! SSTable: an immutable, sorted on-disk run of records.
//!
//! File layout (v2, magic `LSM2`):
//! ```text
//! [data block 0]   LZ4-compressed run of Record encodings (~4 KiB uncompressed)
//! [data block 1]   …
//! [index]          per block: [first_key_len:4][first_key][block_offset:8][comp_len:4]
//! [bloom]          encoded BloomFilter bytes
//! [footer]         [index_offset:8][index_entries:4][bloom_offset:8]
//!                  [bloom_len:8][record_count:8][format_version:4][magic:4]
//! ```
//! A point lookup first consults the per-table Bloom filter, then binary-searches
//! the block index for the bracketing block, decompresses just that block (or
//! reads it from the in-memory block cache), and scans it.

use std::cmp::Ordering;
use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::Mutex;

use crate::bloom::BloomFilter;
use crate::error::{Error, Result};
use crate::fs::{BufAppend, File as _, Fs, OpenMode};
use crate::record::Record;

const MAGIC: &[u8; 4] = b"LSM2";
const FORMAT_VERSION: u32 = 2;
const FOOTER_LEN: u64 = 44;
/// Target uncompressed size of one data block.
const BLOCK_SIZE: usize = 4 * 1024;
/// Target Bloom-filter false-positive rate.
const BLOOM_FP_RATE: f64 = 0.01;
/// Number of decompressed blocks held in each reader's block cache.
const BLOCK_CACHE_CAP: usize = 64;

/// Streams a sorted record sequence into a new SSTable file.
pub struct SsTableWriter;

impl SsTableWriter {
    /// Write `records` (which MUST be sorted ascending by key) to `path`.
    /// Returns the number of records written.
    pub fn write<F: Fs>(fs: &F, path: &Path, records: &[Record]) -> Result<u64> {
        let mut w = BufAppend::new(fs.open(path, OpenMode::Truncate)?);
        let mut bloom = BloomFilter::new(records.len(), BLOOM_FP_RATE);

        // (first_key, block_offset, compressed_len) per data block.
        let mut index: Vec<(Vec<u8>, u64, u32)> = Vec::new();
        let mut offset: u64 = 0;
        let mut block = Vec::new();
        let mut block_first_key: Vec<u8> = Vec::new();

        let flush_block = |block: &mut Vec<u8>,
                           first_key: &mut Vec<u8>,
                           w: &mut BufAppend<F::File>,
                           offset: &mut u64,
                           index: &mut Vec<(Vec<u8>, u64, u32)>|
         -> Result<()> {
            if block.is_empty() {
                return Ok(());
            }
            let comp = lz4_flex::compress_prepend_size(block);
            w.write(&comp)?;
            index.push((std::mem::take(first_key), *offset, comp.len() as u32));
            *offset += comp.len() as u64;
            block.clear();
            Ok(())
        };

        for (i, record) in records.iter().enumerate() {
            bloom.insert(&record.key);
            if block.is_empty() {
                block_first_key = record.key.clone();
            }
            record.encode_into(&mut block);
            // Never split a key's versions across blocks: a seq-aware lookup
            // scans only the one block the binary search lands on.
            let next_differs = records.get(i + 1).is_none_or(|next| next.key != record.key);
            if block.len() >= BLOCK_SIZE && next_differs {
                flush_block(
                    &mut block,
                    &mut block_first_key,
                    &mut w,
                    &mut offset,
                    &mut index,
                )?;
            }
        }
        flush_block(
            &mut block,
            &mut block_first_key,
            &mut w,
            &mut offset,
            &mut index,
        )?;

        let index_offset = offset;
        for (key, block_offset, comp_len) in &index {
            w.write(&(key.len() as u32).to_le_bytes())?;
            w.write(key)?;
            w.write(&block_offset.to_le_bytes())?;
            w.write(&comp_len.to_le_bytes())?;
        }

        let bloom_bytes = bloom.encode();
        let bloom_offset = {
            let mut o = index_offset;
            for (key, _, _) in &index {
                o += 4 + key.len() as u64 + 8 + 4;
            }
            o
        };
        w.write(&bloom_bytes)?;

        let count = records.len() as u64;
        w.write(&index_offset.to_le_bytes())?;
        w.write(&(index.len() as u32).to_le_bytes())?;
        w.write(&bloom_offset.to_le_bytes())?;
        w.write(&(bloom_bytes.len() as u64).to_le_bytes())?;
        w.write(&count.to_le_bytes())?;
        w.write(&FORMAT_VERSION.to_le_bytes())?;
        w.write(MAGIC)?;
        w.sync()?;
        Ok(count)
    }
}

/// A bounded FIFO cache of decompressed data blocks, keyed by block offset.
struct BlockCache {
    map: HashMap<u64, Arc<Vec<Record>>>,
    order: VecDeque<u64>,
}

impl BlockCache {
    fn new() -> BlockCache {
        BlockCache {
            map: HashMap::new(),
            order: VecDeque::new(),
        }
    }

    fn get(&self, offset: u64) -> Option<Arc<Vec<Record>>> {
        self.map.get(&offset).cloned()
    }

    fn put(&mut self, offset: u64, block: Arc<Vec<Record>>) {
        if self.map.contains_key(&offset) {
            return;
        }
        if self.order.len() >= BLOCK_CACHE_CAP
            && let Some(evicted) = self.order.pop_front()
        {
            self.map.remove(&evicted);
        }
        self.map.insert(offset, block);
        self.order.push_back(offset);
    }
}

/// Read handle over an immutable SSTable. The block index and Bloom filter are
/// held in memory; data blocks are decompressed on demand and cached.
pub struct SsTableReader<F: Fs> {
    file: F::File,
    path: PathBuf,
    /// Block index: (first key of block, block offset, compressed length).
    index: Vec<(Vec<u8>, u64, u32)>,
    bloom: BloomFilter,
    bloom_enabled: bool,
    record_count: u64,
    cache: Mutex<BlockCache>,
}

impl<F: Fs> SsTableReader<F> {
    /// Open the SSTable at `path`. `bloom_enabled` gates the Bloom-filter
    /// pre-check on lookups (always `true` outside benchmarks).
    pub fn open(fs: &F, path: impl Into<PathBuf>, bloom_enabled: bool) -> Result<SsTableReader<F>> {
        let path = path.into();
        let file = fs.open(&path, OpenMode::Read)?;
        let file_len = file.size()?;
        if file_len < FOOTER_LEN {
            return Err(Error::BadFormat("file smaller than footer".into()));
        }

        let mut footer = [0u8; FOOTER_LEN as usize];
        file.read_exact_at(file_len - FOOTER_LEN, &mut footer)?;
        if &footer[40..44] != MAGIC {
            return Err(Error::BadFormat("bad magic (expected LSM2)".into()));
        }
        let index_offset = u64::from_le_bytes(footer[0..8].try_into().unwrap());
        let index_entries = u32::from_le_bytes(footer[8..12].try_into().unwrap()) as usize;
        let bloom_offset = u64::from_le_bytes(footer[12..20].try_into().unwrap());
        let bloom_len = u64::from_le_bytes(footer[20..28].try_into().unwrap());
        let record_count = u64::from_le_bytes(footer[28..36].try_into().unwrap());

        let footer_start = file_len - FOOTER_LEN;
        if index_offset > bloom_offset || bloom_offset + bloom_len > footer_start {
            return Err(Error::BadFormat("footer offsets out of range".into()));
        }

        let mut ibuf = vec![0u8; (bloom_offset - index_offset) as usize];
        file.read_exact_at(index_offset, &mut ibuf)?;
        let mut index = Vec::with_capacity(index_entries);
        let mut pos = 0usize;
        for _ in 0..index_entries {
            let key_len = read_u32(&ibuf, &mut pos)? as usize;
            let key = read_bytes(&ibuf, &mut pos, key_len)?.to_vec();
            let block_offset = read_u64(&ibuf, &mut pos)?;
            let comp_len = read_u32(&ibuf, &mut pos)?;
            index.push((key, block_offset, comp_len));
        }

        let mut bbuf = vec![0u8; bloom_len as usize];
        file.read_exact_at(bloom_offset, &mut bbuf)?;
        let bloom = BloomFilter::decode(&bbuf)?;

        Ok(SsTableReader {
            file,
            path,
            index,
            bloom,
            bloom_enabled,
            record_count,
            cache: Mutex::new(BlockCache::new()),
        })
    }

    /// Filesystem path of this SSTable.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Total number of records in the table.
    #[allow(dead_code)] // used by tests; reserved for compaction planning stats
    pub fn record_count(&self) -> u64 {
        self.record_count
    }

    /// Decompress and decode the data block at `block_offset`/`comp_len`,
    /// returning it through the block cache.
    fn load_block(&self, block_offset: u64, comp_len: u32) -> Result<Arc<Vec<Record>>> {
        if let Some(block) = self.cache.lock().get(block_offset) {
            return Ok(block);
        }
        let mut comp = vec![0u8; comp_len as usize];
        self.file.read_exact_at(block_offset, &mut comp)?;
        let raw = lz4_flex::decompress_size_prepended(&comp)
            .map_err(|e| Error::Corrupt(format!("block decompression failed: {e}")))?;
        let mut records = Vec::new();
        let mut pos = 0;
        while pos < raw.len() {
            records.push(Record::decode_at(&raw, &mut pos)?);
        }
        let block = Arc::new(records);
        self.cache.lock().put(block_offset, block.clone());
        Ok(block)
    }

    /// Look up `key`, returning its most recent record in this table (which
    /// may be a tombstone) or `None` if the table does not contain it.
    #[allow(dead_code)] // convenience wrapper over `get_at`; used in tests
    pub fn get(&self, key: &[u8]) -> Result<Option<Record>> {
        self.get_at(key, u64::MAX)
    }

    /// Look up `key` as visible at `seq_bound`: the newest record for `key`
    /// whose sequence number is below the bound. A compacted table may hold
    /// several versions of a key, stored newest-first, so the scan returns the
    /// first match it finds at or below the bound.
    pub fn get_at(&self, key: &[u8], seq_bound: u64) -> Result<Option<Record>> {
        if self.index.is_empty() {
            return Ok(None);
        }
        if self.bloom_enabled && !self.bloom.contains(key) {
            return Ok(None);
        }
        let block_idx = match self
            .index
            .binary_search_by(|(k, _, _)| k.as_slice().cmp(key))
        {
            Ok(i) => i,
            Err(0) => return Ok(None), // key precedes the first key in the table
            Err(i) => i - 1,
        };
        let (_, block_offset, comp_len) = self.index[block_idx];
        let block = self.load_block(block_offset, comp_len)?;
        for rec in block.iter() {
            match rec.key.as_slice().cmp(key) {
                // Versions of a key are stored seq-descending, so the first
                // one below the bound is the newest visible at the bound.
                Ordering::Equal if rec.seq < seq_bound => return Ok(Some(rec.clone())),
                Ordering::Equal => {}
                Ordering::Greater => return Ok(None),
                Ordering::Less => {}
            }
        }
        Ok(None)
    }

    /// Every record in the table, ascending by key. Decompresses block by
    /// block; used by compaction and flush verification.
    pub fn iter_all(&self) -> Result<Vec<Record>> {
        let mut records = Vec::with_capacity(self.record_count as usize);
        for &(_, block_offset, comp_len) in &self.index {
            let block = self.load_block(block_offset, comp_len)?;
            records.extend(block.iter().cloned());
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
    use crate::fs::StdFs;

    fn write_table(records: &[Record]) -> (tempfile::TempDir, SsTableReader<StdFs>) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.db");
        let n = SsTableWriter::write(&StdFs, &path, records).unwrap();
        assert_eq!(n, records.len() as u64);
        let reader = SsTableReader::open(&StdFs, &path, true).unwrap();
        (dir, reader)
    }

    #[test]
    fn write_then_point_lookup() {
        // Large values force many blocks so lookups cross block boundaries.
        let recs: Vec<_> = (0u32..2000)
            .map(|i| {
                Record::put(
                    format!("key{i:05}").into_bytes(),
                    vec![i as u8; 64],
                    i as u64,
                )
            })
            .collect();
        let (_d, r) = write_table(&recs);
        assert_eq!(r.record_count(), 2000);
        for i in [0u32, 1, 15, 16, 17, 99, 500, 1999] {
            let got = r.get(format!("key{i:05}").as_bytes()).unwrap().unwrap();
            assert_eq!(got.value, Some(vec![i as u8; 64]));
        }
        assert!(r.get(b"key99999").unwrap().is_none());
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
        let recs: Vec<_> = (0u32..500)
            .map(|i| Record::put(format!("k{i:04}").into_bytes(), vec![7; 32], i as u64))
            .collect();
        let (_d, r) = write_table(&recs);
        assert_eq!(r.iter_all().unwrap(), recs);
    }

    #[test]
    fn empty_table_lookup() {
        let (_d, r) = write_table(&[]);
        assert_eq!(r.record_count(), 0);
        assert!(r.get(b"anything").unwrap().is_none());
        assert!(r.iter_all().unwrap().is_empty());
    }

    #[test]
    fn bloom_disabled_still_correct() {
        let recs: Vec<_> = (0u32..300)
            .map(|i| Record::put(format!("k{i:04}").into_bytes(), vec![1; 32], i as u64))
            .collect();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.db");
        SsTableWriter::write(&StdFs, &path, &recs).unwrap();
        let r = SsTableReader::open(&StdFs, &path, false).unwrap();
        assert!(r.get(b"k0100").unwrap().is_some());
        assert!(r.get(b"missing").unwrap().is_none());
    }

    #[test]
    fn bad_magic_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("junk.db");
        std::fs::write(&path, vec![0u8; 64]).unwrap();
        assert!(SsTableReader::open(&StdFs, &path, true).is_err());
    }
}
