//! The storage engine: orchestrates the WAL, the MemTable, and the stack of
//! immutable SSTables behind a simple `put` / `get` / `delete` API.

use std::fs;
use std::path::{Path, PathBuf};

use crate::compaction;
use crate::error::Result;
use crate::memtable::{MemTable, DEFAULT_THRESHOLD};
use crate::record::Record;
use crate::sstable::{SsTableReader, SsTableWriter};
use crate::wal::Wal;

const WAL_FILENAME: &str = "wal.log";

/// Tuning knobs for opening a database.
#[derive(Debug, Clone)]
pub struct Options {
    /// MemTable size (bytes) at which an automatic flush is triggered.
    pub memtable_threshold: usize,
    /// If true, `fsync` the WAL after every append (durable but slower).
    pub sync_wal: bool,
    /// Number of similarly-sized, adjacent SSTables that triggers a
    /// size-tiered compaction of that run.
    pub compaction_threshold: usize,
    /// If false, SSTable reads skip the Bloom-filter pre-check (benchmarks only).
    pub bloom_enabled: bool,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            memtable_threshold: DEFAULT_THRESHOLD,
            sync_wal: true,
            compaction_threshold: 4,
            bloom_enabled: true,
        }
    }
}

/// An immutable SSTable on disk, tagged with its id (higher id == newer).
struct SsTable {
    id: u64,
    reader: SsTableReader,
}

/// An embedded LSM-tree key/value store.
pub struct Db {
    dir: PathBuf,
    opts: Options,
    memtable: MemTable,
    wal: Wal,
    /// SSTables ordered oldest -> newest by id; reads walk this in reverse.
    sstables: Vec<SsTable>,
    next_sst_id: u64,
    next_seq: u64,
}

impl Db {
    /// Open (creating if needed) the database rooted at `dir` with defaults.
    pub fn open(dir: impl AsRef<Path>) -> Result<Db> {
        Db::open_with(dir, Options::default())
    }

    /// Open the database rooted at `dir` with explicit [`Options`].
    pub fn open_with(dir: impl AsRef<Path>, opts: Options) -> Result<Db> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        // Load existing SSTables, ordered oldest -> newest by id.
        let mut sst_ids = Vec::new();
        for entry in fs::read_dir(&dir)? {
            let name = entry?.file_name();
            if let Some(id) = parse_sst_id(&name.to_string_lossy()) {
                sst_ids.push(id);
            }
        }
        sst_ids.sort_unstable();
        let next_sst_id = sst_ids.last().map_or(0, |&id| id + 1);
        let sstables = sst_ids
            .iter()
            .map(|&id| {
                SsTableReader::open(sst_path(&dir, id), opts.bloom_enabled)
                    .map(|reader| SsTable { id, reader })
            })
            .collect::<Result<Vec<_>>>()?;

        // Replay the WAL into a fresh MemTable.
        let wal_path = dir.join(WAL_FILENAME);
        let replayed = Wal::replay(&wal_path)?;
        let mut memtable = MemTable::new(opts.memtable_threshold);
        let mut next_seq = 0;
        for record in replayed {
            next_seq = next_seq.max(record.seq + 1);
            memtable.insert(record);
        }
        // NOTE: seq numbering restarts after a clean flush (empty WAL). Reads
        // never depend on seq; compaction resolves duplicates by SSTable id.
        // A manifest persists next_seq in Phase 3.
        //
        // Open the WAL for *appending*: it still backs the records just
        // replayed into the MemTable until the next flush moves them to an
        // SSTable. Truncating here would lose them if we exit before flushing.
        let wal = Wal::open_append(&wal_path, opts.sync_wal)?;

        Ok(Db {
            dir,
            opts,
            memtable,
            wal,
            sstables,
            next_sst_id,
            next_seq,
        })
    }

    /// Insert or overwrite `key` with `value`.
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let record = Record::put(key.to_vec(), value.to_vec(), self.take_seq());
        self.apply(record)
    }

    /// Delete `key` (writes a tombstone). A no-op if the key is absent.
    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        let record = Record::tombstone(key.to_vec(), self.take_seq());
        self.apply(record)
    }

    /// Return the current value for `key`, or `None` if absent or deleted.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if let Some(record) = self.memtable.get(key) {
            return Ok(record.value.clone());
        }
        for table in self.sstables.iter().rev() {
            if let Some(record) = table.reader.get(key)? {
                return Ok(record.value);
            }
        }
        Ok(None)
    }

    /// Flush the MemTable to a new SSTable and start a fresh WAL. A no-op if
    /// the MemTable is empty. May trigger a size-tiered compaction afterwards.
    pub fn flush(&mut self) -> Result<()> {
        if self.memtable.is_empty() {
            return Ok(());
        }
        let id = self.next_sst_id;
        let final_path = sst_path(&self.dir, id);
        let tmp_path = final_path.with_extension("db.tmp");

        let records: Vec<Record> = self.memtable.iter().cloned().collect();
        SsTableWriter::write(&tmp_path, &records)?;
        // Atomic publish: a crash before the rename leaves only a stale .tmp.
        fs::rename(&tmp_path, &final_path)?;
        let reader = SsTableReader::open(&final_path, self.opts.bloom_enabled)?;
        self.sstables.push(SsTable { id, reader });
        self.next_sst_id += 1;

        // The flushed data is now durable in the SSTable; reset WAL + MemTable.
        self.wal = Wal::create(self.dir.join(WAL_FILENAME), self.opts.sync_wal)?;
        self.memtable = MemTable::new(self.opts.memtable_threshold);

        self.maybe_compact()?;
        Ok(())
    }

    /// Number of immutable SSTables currently on disk.
    pub fn sstable_count(&self) -> usize {
        self.sstables.len()
    }

    fn take_seq(&mut self) -> u64 {
        let seq = self.next_seq;
        self.next_seq += 1;
        seq
    }

    fn apply(&mut self, record: Record) -> Result<()> {
        self.wal.append(&record)?;
        self.memtable.insert(record);
        if self.memtable.is_full() {
            self.flush()?;
        }
        Ok(())
    }

    /// Run size-tiered compactions until no qualifying run remains.
    fn maybe_compact(&mut self) -> Result<()> {
        while let Some((start, end)) = self.pick_compaction()? {
            let max_id = self.sstables[end - 1].id;
            let oldest_id = self.sstables[0].id;
            let drop_tombstones = self.sstables[start].id == oldest_id;

            let final_path = sst_path(&self.dir, max_id);
            let tmp_path = final_path.with_extension("db.tmp");
            {
                let inputs: Vec<&SsTableReader> =
                    self.sstables[start..end].iter().map(|t| &t.reader).collect();
                compaction::compact(&tmp_path, &inputs, drop_tombstones)?;
            }
            // Atomic swap: rename over the max-id input, then drop the rest.
            // A crash after the rename leaves stale older inputs — still
            // correct, since the merged table's higher id shadows them.
            fs::rename(&tmp_path, &final_path)?;
            let stale_ids: Vec<u64> =
                self.sstables[start..end - 1].iter().map(|t| t.id).collect();
            for id in stale_ids {
                fs::remove_file(sst_path(&self.dir, id))?;
            }

            let reader = SsTableReader::open(&final_path, self.opts.bloom_enabled)?;
            self.sstables
                .splice(start..end, [SsTable { id: max_id, reader }]);
        }
        Ok(())
    }

    /// Pick a contiguous, id-ordered run of `compaction_threshold`+ SSTables
    /// that fall in the same size tier, returning its `start..end` range.
    fn pick_compaction(&self) -> Result<Option<(usize, usize)>> {
        let threshold = self.opts.compaction_threshold;
        if self.sstables.len() < threshold {
            return Ok(None);
        }
        let mut tiers = Vec::with_capacity(self.sstables.len());
        for t in &self.sstables {
            let len = fs::metadata(t.reader.path())?.len();
            // Tier = bit-width of the file size, i.e. floor(log2)+1.
            tiers.push(64 - len.leading_zeros());
        }
        let mut i = 0;
        while i < tiers.len() {
            let mut j = i;
            while j < tiers.len() && tiers[j] == tiers[i] {
                j += 1;
            }
            if j - i >= threshold {
                return Ok(Some((i, j)));
            }
            i = j;
        }
        Ok(None)
    }
}

fn sst_path(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("sst_{id:010}.db"))
}

/// Parse the id out of an `sst_<id>.db` filename, if it matches.
fn parse_sst_id(name: &str) -> Option<u64> {
    name.strip_prefix("sst_")?.strip_suffix(".db")?.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fast_opts() -> Options {
        Options {
            sync_wal: false,
            ..Options::default()
        }
    }

    #[test]
    fn put_get_delete() {
        let dir = tempfile::tempdir().unwrap();
        let mut db = Db::open_with(dir.path(), fast_opts()).unwrap();
        db.put(b"k", b"v").unwrap();
        assert_eq!(db.get(b"k").unwrap(), Some(b"v".to_vec()));
        db.delete(b"k").unwrap();
        assert_eq!(db.get(b"k").unwrap(), None);
        assert_eq!(db.get(b"missing").unwrap(), None);
    }

    #[test]
    fn reopen_recovers_from_wal() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut db = Db::open_with(dir.path(), fast_opts()).unwrap();
            db.put(b"a", b"1").unwrap();
            db.put(b"b", b"2").unwrap();
        }
        let db = Db::open_with(dir.path(), fast_opts()).unwrap();
        assert_eq!(db.get(b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(db.get(b"b").unwrap(), Some(b"2".to_vec()));
    }

    #[test]
    fn data_survives_repeated_reopen_without_flush() {
        // Each open replays the WAL but must not truncate it; otherwise data
        // recovered into the MemTable is lost on the *next* open.
        let dir = tempfile::tempdir().unwrap();
        {
            let mut db = Db::open_with(dir.path(), fast_opts()).unwrap();
            db.put(b"persist", b"me").unwrap();
        }
        for _ in 0..5 {
            let db = Db::open_with(dir.path(), fast_opts()).unwrap();
            assert_eq!(db.get(b"persist").unwrap(), Some(b"me".to_vec()));
        }
    }

    #[test]
    fn writes_across_reopens_accumulate() {
        let dir = tempfile::tempdir().unwrap();
        for i in 0..10u32 {
            let mut db = Db::open_with(dir.path(), fast_opts()).unwrap();
            db.put(format!("k{i}").as_bytes(), &i.to_le_bytes())
                .unwrap();
        }
        let db = Db::open_with(dir.path(), fast_opts()).unwrap();
        for i in 0..10u32 {
            assert_eq!(
                db.get(format!("k{i}").as_bytes()).unwrap(),
                Some(i.to_le_bytes().to_vec())
            );
        }
    }

    #[test]
    fn deleted_key_stays_deleted_after_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut db = Db::open_with(dir.path(), fast_opts()).unwrap();
            db.put(b"k", b"v").unwrap();
            db.delete(b"k").unwrap();
        }
        let db = Db::open_with(dir.path(), fast_opts()).unwrap();
        assert_eq!(db.get(b"k").unwrap(), None);
    }

    #[test]
    fn flush_persists_and_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut db = Db::open_with(dir.path(), fast_opts()).unwrap();
            for i in 0..1000u32 {
                db.put(format!("k{i:04}").as_bytes(), &i.to_le_bytes())
                    .unwrap();
            }
            db.flush().unwrap();
            assert_eq!(db.sstable_count(), 1);
            assert!(db.memtable.is_empty());
        }
        let db = Db::open_with(dir.path(), fast_opts()).unwrap();
        assert_eq!(db.sstable_count(), 1);
        for i in 0..1000u32 {
            assert_eq!(
                db.get(format!("k{i:04}").as_bytes()).unwrap(),
                Some(i.to_le_bytes().to_vec())
            );
        }
    }

    #[test]
    fn newer_sstable_shadows_older() {
        let dir = tempfile::tempdir().unwrap();
        let mut db = Db::open_with(dir.path(), fast_opts()).unwrap();
        db.put(b"k", b"old").unwrap();
        db.flush().unwrap();
        db.put(b"k", b"new").unwrap();
        db.flush().unwrap();
        assert_eq!(db.sstable_count(), 2);
        assert_eq!(db.get(b"k").unwrap(), Some(b"new".to_vec()));

        db.delete(b"k").unwrap();
        db.flush().unwrap();
        assert_eq!(db.get(b"k").unwrap(), None);
    }

    #[test]
    fn auto_flush_on_threshold() {
        let dir = tempfile::tempdir().unwrap();
        let opts = Options {
            memtable_threshold: 4 * 1024,
            sync_wal: false,
            ..Options::default()
        };
        let mut db = Db::open_with(dir.path(), opts).unwrap();
        for i in 0..5000u32 {
            db.put(format!("key{i:06}").as_bytes(), b"some-value-payload")
                .unwrap();
        }
        for i in 0..5000u32 {
            assert_eq!(
                db.get(format!("key{i:06}").as_bytes()).unwrap(),
                Some(b"some-value-payload".to_vec())
            );
        }
    }

    #[test]
    fn compaction_reduces_table_count_and_preserves_data() {
        let dir = tempfile::tempdir().unwrap();
        let opts = Options {
            sync_wal: false,
            compaction_threshold: 4,
            ..Options::default()
        };
        let mut db = Db::open_with(dir.path(), opts).unwrap();
        // Four flushes of similarly-sized tables trigger one compaction.
        for batch in 0..4u32 {
            for i in 0..200u32 {
                let k = format!("k{batch}_{i:04}");
                db.put(k.as_bytes(), b"payload-value-data").unwrap();
            }
            db.flush().unwrap();
        }
        assert!(
            db.sstable_count() < 4,
            "expected compaction to merge tables, got {}",
            db.sstable_count()
        );
        for batch in 0..4u32 {
            for i in 0..200u32 {
                let k = format!("k{batch}_{i:04}");
                assert_eq!(
                    db.get(k.as_bytes()).unwrap(),
                    Some(b"payload-value-data".to_vec())
                );
            }
        }
        // Survives reopen.
        let db = Db::open_with(dir.path(), fast_opts()).unwrap();
        assert_eq!(db.get(b"k2_0100").unwrap(), Some(b"payload-value-data".to_vec()));
    }

    #[test]
    fn compaction_keeps_newest_value_and_drops_deleted_key() {
        let dir = tempfile::tempdir().unwrap();
        let opts = Options {
            sync_wal: false,
            compaction_threshold: 3,
            ..Options::default()
        };
        let mut db = Db::open_with(dir.path(), opts).unwrap();
        db.put(b"k", b"v1").unwrap();
        db.flush().unwrap();
        db.put(b"k", b"v2").unwrap();
        db.flush().unwrap();
        db.put(b"gone", b"x").unwrap();
        db.delete(b"gone").unwrap();
        db.flush().unwrap();
        assert_eq!(db.get(b"k").unwrap(), Some(b"v2".to_vec()));
        assert_eq!(db.get(b"gone").unwrap(), None);
    }

    #[test]
    fn parse_sst_id_matching() {
        assert_eq!(parse_sst_id("sst_0000000007.db"), Some(7));
        assert_eq!(parse_sst_id("wal.log"), None);
        assert_eq!(parse_sst_id("sst_.db"), None);
    }
}
