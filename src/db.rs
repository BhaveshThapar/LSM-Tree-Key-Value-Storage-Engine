//! The storage engine: orchestrates the WAL, the MemTable, and the stack of
//! immutable SSTables behind a simple `put` / `get` / `delete` API.
//!
//! [`Db`] is `Send + Sync` and exposes a `&self` API: all mutable state lives
//! in [`DbInner`] behind `parking_lot` locks, so the handle can be shared
//! across threads (typically via `Arc<Db>`). Flushing the MemTable runs on a
//! background worker thread, so writers never stall waiting on flush I/O.

use std::fs;
use std::mem;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Weak};
use std::thread::{self, JoinHandle};

use parking_lot::{Mutex, RwLock};

use crate::compaction;
use crate::error::Result;
use crate::manifest::{Manifest, ManifestState, VersionEdit};
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

/// A unit of background work for the worker thread.
enum Task {
    Flush,
    Shutdown,
}

/// Owns the background worker thread and shuts it down on drop.
struct WorkerHandle {
    tx: Sender<Task>,
    handle: Option<JoinHandle<()>>,
}

impl Drop for WorkerHandle {
    fn drop(&mut self) {
        let _ = self.tx.send(Task::Shutdown);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

/// The shared, lock-protected engine state. Reachable from both the public
/// [`Db`] handle and (via a `Weak`) the background worker thread.
struct DbInner {
    dir: PathBuf,
    opts: Options,
    /// Serializes WAL appends; also held across the matching MemTable insert
    /// so a record is never visible in one without the other.
    wal: Mutex<Wal>,
    /// The MemTable accepting writes.
    active: RwLock<MemTable>,
    /// A MemTable frozen for flushing; reads still consult it until the flush
    /// publishes its SSTable.
    frozen: RwLock<Option<Arc<MemTable>>>,
    /// Live SSTables, oldest -> newest by id; swapped wholesale on change.
    sstables: RwLock<Arc<Vec<Arc<SsTable>>>>,
    next_sst_id: AtomicU64,
    next_seq: AtomicU64,
    manifest: Mutex<Manifest>,
    /// Serializes flush/compaction so only one mutates `sstables` at a time.
    flush_lock: Mutex<()>,
    /// Triggers background flushes.
    worker_tx: Sender<Task>,
}

/// An embedded LSM-tree key/value store.
pub struct Db {
    // Field order matters: `worker` is dropped first, joining the background
    // thread before `inner` (and its `Arc` refcount) goes away.
    worker: WorkerHandle,
    inner: Arc<DbInner>,
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

        // The manifest is the authoritative record of the live SSTable set and
        // the global counters. If absent, migrate a Phase 2 directory by
        // scanning its `sst_*.db` files into a fresh manifest.
        let (manifest, state) = if Manifest::exists(&dir) {
            Manifest::open(&dir)?
        } else {
            Db::migrate_dir(&dir)?
        };

        // Reclaim orphan SSTables: any `sst_*.db` not named by the manifest is
        // a leftover from a crash before its `AddTable` edit was durable.
        for entry in fs::read_dir(&dir)? {
            let name = entry?.file_name();
            let name = name.to_string_lossy();
            if let Some(id) = parse_sst_id(&name) {
                if !state.live_tables.contains(&id) {
                    fs::remove_file(dir.join(name.as_ref()))?;
                }
            } else if name.ends_with(".db.tmp") {
                fs::remove_file(dir.join(name.as_ref()))?;
            }
        }

        // Open the live SSTables, ordered oldest -> newest by id.
        let sstables: Vec<Arc<SsTable>> = state
            .live_tables
            .iter()
            .map(|&id| {
                SsTableReader::open(sst_path(&dir, id), opts.bloom_enabled)
                    .map(|reader| Arc::new(SsTable { id, reader }))
            })
            .collect::<Result<Vec<_>>>()?;

        // Replay the WAL into a fresh MemTable. The manifest persists `next_seq`
        // across clean flushes; the live WAL may carry seqs beyond it.
        let wal_path = dir.join(WAL_FILENAME);
        let replayed = Wal::replay(&wal_path)?;
        let mut memtable = MemTable::new(opts.memtable_threshold);
        let mut next_seq = state.next_seq;
        for record in replayed {
            next_seq = next_seq.max(record.seq + 1);
            memtable.insert(record);
        }
        // Open the WAL for *appending*: it still backs the records just
        // replayed into the MemTable until the next flush moves them to an
        // SSTable. Truncating here would lose them if we exit before flushing.
        let wal = Wal::open_append(&wal_path, opts.sync_wal)?;

        let (tx, rx) = mpsc::channel();
        let inner = Arc::new(DbInner {
            dir,
            opts,
            wal: Mutex::new(wal),
            active: RwLock::new(memtable),
            frozen: RwLock::new(None),
            sstables: RwLock::new(Arc::new(sstables)),
            next_sst_id: AtomicU64::new(state.next_sst_id),
            next_seq: AtomicU64::new(next_seq),
            manifest: Mutex::new(manifest),
            flush_lock: Mutex::new(()),
            worker_tx: tx.clone(),
        });

        let weak = Arc::downgrade(&inner);
        let handle = thread::spawn(move || worker_loop(rx, weak));

        Ok(Db {
            worker: WorkerHandle {
                tx,
                handle: Some(handle),
            },
            inner,
        })
    }

    /// First open of a directory: synthesize a manifest from whatever SSTables
    /// already exist (a Phase 2 layout, or an empty directory).
    fn migrate_dir(dir: &Path) -> Result<(Manifest, ManifestState)> {
        let mut sst_ids = Vec::new();
        for entry in fs::read_dir(dir)? {
            let name = entry?.file_name();
            if let Some(id) = parse_sst_id(&name.to_string_lossy()) {
                sst_ids.push(id);
            }
        }
        sst_ids.sort_unstable();
        let next_sst_id = sst_ids.last().map_or(0, |&id| id + 1);

        let mut manifest = Manifest::create(dir)?;
        let mut edits = vec![VersionEdit::SetNextSstId(next_sst_id)];
        edits.extend(sst_ids.iter().map(|&id| VersionEdit::AddTable { id }));
        manifest.append_batch(&edits)?;

        let state = ManifestState {
            live_tables: sst_ids.into_iter().collect(),
            next_seq: 0,
            next_sst_id,
        };
        Ok((manifest, state))
    }

    /// Insert or overwrite `key` with `value`.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.write(key, Some(value.to_vec()))
    }

    /// Delete `key` (writes a tombstone). A no-op if the key is absent.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.write(key, None)
    }

    /// Return the current value for `key`, or `None` if absent or deleted.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.inner.get(key)
    }

    /// Flush the MemTable to a new SSTable and start a fresh WAL, synchronously.
    /// A no-op if the MemTable is empty. May trigger a size-tiered compaction.
    pub fn flush(&self) -> Result<()> {
        self.inner.flush_inner()
    }

    /// Number of immutable SSTables currently on disk.
    pub fn sstable_count(&self) -> usize {
        self.inner.sstables.read().len()
    }
}

impl DbInner {
    /// Append a mutation to the WAL and the active MemTable.
    fn write(&self, key: &[u8], value: Option<Vec<u8>>) -> Result<()> {
        let trigger_flush;
        {
            // Hold the WAL lock across the MemTable insert so the record's WAL
            // frame and its in-memory presence are published as one unit —
            // a concurrent flush rewriting the WAL can never miss it.
            let mut wal = self.wal.lock();
            let seq = self.next_seq.fetch_add(1, Ordering::SeqCst);
            let record = match value {
                Some(v) => Record::put(key.to_vec(), v, seq),
                None => Record::tombstone(key.to_vec(), seq),
            };
            wal.append(&record)?;
            let mut active = self.active.write();
            active.insert(record);
            trigger_flush = active.is_full();
        }
        if trigger_flush {
            let _ = self.worker_tx.send(Task::Flush);
        }
        Ok(())
    }

    /// Resolve `key` across the active MemTable, the frozen MemTable, then the
    /// SSTables newest-to-oldest.
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if let Some(record) = self.active.read().get(key) {
            return Ok(record.value.clone());
        }
        let frozen = self.frozen.read().clone();
        if let Some(frozen) = frozen {
            if let Some(record) = frozen.get(key) {
                return Ok(record.value.clone());
            }
        }
        let sstables = self.sstables.read().clone();
        for table in sstables.iter().rev() {
            if let Some(record) = table.reader.get(key)? {
                return Ok(record.value);
            }
        }
        Ok(None)
    }

    /// Freeze the active MemTable, write it to a new SSTable, publish it, and
    /// rewrite the WAL to back only the records written since the freeze.
    fn flush_inner(&self) -> Result<()> {
        let _flush_guard = self.flush_lock.lock();

        // Freeze: swap the active MemTable for a fresh one. The frozen slot is
        // set while still holding the `active` write lock, so any reader that
        // observes the emptied `active` also observes the frozen MemTable.
        let frozen = {
            let mut active = self.active.write();
            if active.is_empty() {
                return Ok(());
            }
            let old = mem::replace(&mut *active, MemTable::new(self.opts.memtable_threshold));
            let frozen = Arc::new(old);
            *self.frozen.write() = Some(Arc::clone(&frozen));
            frozen
        };

        let id = self.next_sst_id.fetch_add(1, Ordering::SeqCst);
        let final_path = sst_path(&self.dir, id);
        let tmp_path = final_path.with_extension("db.tmp");

        let records: Vec<Record> = frozen.iter().cloned().collect();
        SsTableWriter::write(&tmp_path, &records)?;
        // Atomic publish: a crash before the rename leaves only a stale .tmp.
        fs::rename(&tmp_path, &final_path)?;
        let reader = SsTableReader::open(&final_path, self.opts.bloom_enabled)?;

        // Record the new table in the manifest *before* truncating the WAL:
        // the WAL still backs these records until the manifest makes the
        // SSTable authoritative.
        self.manifest.lock().append_batch(&[
            VersionEdit::AddTable { id },
            VersionEdit::SetNextSstId(self.next_sst_id.load(Ordering::SeqCst)),
            VersionEdit::SetNextSeq(self.next_seq.load(Ordering::SeqCst)),
        ])?;

        // Publish the new SSTable into the live set, then clear the frozen
        // slot — done in this order so reads see continuous coverage.
        {
            let mut sstables = self.sstables.write();
            let mut next = Vec::with_capacity(sstables.len() + 1);
            next.extend(sstables.iter().cloned());
            next.push(Arc::new(SsTable { id, reader }));
            *sstables = Arc::new(next);
        }

        // Rewrite the WAL to back only the records written since the freeze.
        // Holding the WAL lock blocks appends — but they already serialize on
        // it — and `active` now holds exactly those post-freeze records.
        {
            let mut wal = self.wal.lock();
            let active = self.active.read();
            let mut fresh = Wal::create(self.dir.join(WAL_FILENAME), self.opts.sync_wal)?;
            for record in active.iter() {
                fresh.append(record)?;
            }
            *wal = fresh;
        }
        *self.frozen.write() = None;

        self.compact_inner()
    }

    /// Run size-tiered compactions until no qualifying run remains. Caller must
    /// hold `flush_lock`.
    fn compact_inner(&self) -> Result<()> {
        loop {
            let tables = self.sstables.read().clone();
            let Some((start, end)) = pick_compaction(&tables, self.opts.compaction_threshold)?
            else {
                break;
            };
            let max_id = tables[end - 1].id;
            let oldest_id = tables[0].id;
            let drop_tombstones = tables[start].id == oldest_id;

            let final_path = sst_path(&self.dir, max_id);
            let tmp_path = final_path.with_extension("db.tmp");
            {
                let inputs: Vec<&SsTableReader> =
                    tables[start..end].iter().map(|t| &t.reader).collect();
                compaction::compact(&tmp_path, &inputs, drop_tombstones)?;
            }
            // Atomic swap: rename over the max-id input, then drop the rest.
            fs::rename(&tmp_path, &final_path)?;
            let stale_ids: Vec<u64> = tables[start..end - 1].iter().map(|t| t.id).collect();
            // Drop the stale inputs from the manifest before unlinking them, so
            // a crash mid-cleanup leaves them as reclaimable orphans.
            self.manifest.lock().append_batch(
                &stale_ids
                    .iter()
                    .map(|&id| VersionEdit::DeleteTable { id })
                    .collect::<Vec<_>>(),
            )?;
            for id in &stale_ids {
                fs::remove_file(sst_path(&self.dir, *id))?;
            }

            let reader = SsTableReader::open(&final_path, self.opts.bloom_enabled)?;
            let mut next = Vec::with_capacity(tables.len() - (end - start) + 1);
            next.extend(tables[..start].iter().cloned());
            next.push(Arc::new(SsTable { id: max_id, reader }));
            next.extend(tables[end..].iter().cloned());
            *self.sstables.write() = Arc::new(next);
        }
        Ok(())
    }
}

/// Background worker: drains flush requests until the [`Db`] is dropped.
fn worker_loop(rx: Receiver<Task>, inner: Weak<DbInner>) {
    while let Ok(task) = rx.recv() {
        match task {
            Task::Shutdown => break,
            Task::Flush => {
                let Some(inner) = inner.upgrade() else { break };
                if let Err(e) = inner.flush_inner() {
                    eprintln!("lsm: background flush failed: {e}");
                }
            }
        }
    }
}

/// Pick a contiguous, id-ordered run of `threshold`+ SSTables that fall in the
/// same size tier, returning its `start..end` range.
fn pick_compaction(
    tables: &[Arc<SsTable>],
    threshold: usize,
) -> Result<Option<(usize, usize)>> {
    if tables.len() < threshold {
        return Ok(None);
    }
    let mut tiers = Vec::with_capacity(tables.len());
    for t in tables {
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
        let db = Db::open_with(dir.path(), fast_opts()).unwrap();
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
            let db = Db::open_with(dir.path(), fast_opts()).unwrap();
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
            let db = Db::open_with(dir.path(), fast_opts()).unwrap();
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
            let db = Db::open_with(dir.path(), fast_opts()).unwrap();
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
            let db = Db::open_with(dir.path(), fast_opts()).unwrap();
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
            let db = Db::open_with(dir.path(), fast_opts()).unwrap();
            for i in 0..1000u32 {
                db.put(format!("k{i:04}").as_bytes(), &i.to_le_bytes())
                    .unwrap();
            }
            db.flush().unwrap();
            assert_eq!(db.sstable_count(), 1);
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
        let db = Db::open_with(dir.path(), fast_opts()).unwrap();
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
        let db = Db::open_with(dir.path(), opts).unwrap();
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
        let db = Db::open_with(dir.path(), opts).unwrap();
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
        assert_eq!(
            db.get(b"k2_0100").unwrap(),
            Some(b"payload-value-data".to_vec())
        );
    }

    #[test]
    fn compaction_keeps_newest_value_and_drops_deleted_key() {
        let dir = tempfile::tempdir().unwrap();
        let opts = Options {
            sync_wal: false,
            compaction_threshold: 3,
            ..Options::default()
        };
        let db = Db::open_with(dir.path(), opts).unwrap();
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
    fn open_creates_manifest_and_reclaims_orphans() {
        let dir = tempfile::tempdir().unwrap();
        {
            let db = Db::open_with(dir.path(), fast_opts()).unwrap();
            db.put(b"k", b"v").unwrap();
            db.flush().unwrap();
        }
        assert!(dir.path().join("CURRENT").exists());

        // A stray SSTable not named by the manifest must be reclaimed on open.
        let orphan = dir.path().join("sst_0000009999.db");
        fs::copy(sst_path(dir.path(), 0), &orphan).unwrap();
        let db = Db::open_with(dir.path(), fast_opts()).unwrap();
        assert!(!orphan.exists());
        assert_eq!(db.get(b"k").unwrap(), Some(b"v".to_vec()));
    }

    #[test]
    fn migrates_phase2_directory_without_manifest() {
        let dir = tempfile::tempdir().unwrap();
        {
            let db = Db::open_with(dir.path(), fast_opts()).unwrap();
            db.put(b"old", b"data").unwrap();
            db.flush().unwrap();
        }
        // Simulate a Phase 2 layout: drop the manifest, keep the SSTable.
        fs::remove_file(dir.path().join("CURRENT")).unwrap();
        for entry in fs::read_dir(dir.path()).unwrap() {
            let name = entry.unwrap().file_name();
            if name.to_string_lossy().starts_with("MANIFEST-") {
                fs::remove_file(dir.path().join(name)).unwrap();
            }
        }
        let db = Db::open_with(dir.path(), fast_opts()).unwrap();
        assert!(dir.path().join("CURRENT").exists());
        assert_eq!(db.get(b"old").unwrap(), Some(b"data".to_vec()));
        assert_eq!(db.sstable_count(), 1);
    }

    #[test]
    fn concurrent_writers_and_readers() {
        let dir = tempfile::tempdir().unwrap();
        let opts = Options {
            memtable_threshold: 16 * 1024,
            sync_wal: false,
            ..Options::default()
        };
        let db = Arc::new(Db::open_with(dir.path(), opts).unwrap());
        let mut handles = Vec::new();
        for t in 0..4u32 {
            let db = Arc::clone(&db);
            handles.push(thread::spawn(move || {
                for i in 0..2000u32 {
                    let k = format!("t{t}_k{i:05}");
                    db.put(k.as_bytes(), &i.to_le_bytes()).unwrap();
                    db.get(k.as_bytes()).unwrap();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        for t in 0..4u32 {
            for i in 0..2000u32 {
                let k = format!("t{t}_k{i:05}");
                assert_eq!(
                    db.get(k.as_bytes()).unwrap(),
                    Some(i.to_le_bytes().to_vec())
                );
            }
        }
    }

    #[test]
    fn parse_sst_id_matching() {
        assert_eq!(parse_sst_id("sst_0000000007.db"), Some(7));
        assert_eq!(parse_sst_id("wal.log"), None);
        assert_eq!(parse_sst_id("sst_.db"), None);
    }
}
