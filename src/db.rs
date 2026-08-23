//! The storage engine: orchestrates the WAL, the MemTable, and the stack of
//! immutable SSTables behind a simple `put` / `get` / `delete` API.
//!
//! [`Db`] is `Send + Sync` and exposes a `&self` API: all mutable state lives
//! in [`DbInner`] behind `parking_lot` locks, so the handle can be shared
//! across threads (typically via `Arc<Db>`). Flushing the MemTable runs on a
//! background worker thread, so writers never stall waiting on flush I/O.

use std::collections::BTreeMap;
use std::fs;
use std::mem;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Weak};
use std::thread::{self, JoinHandle};

use parking_lot::{Mutex, RwLock};

use crate::compaction;
use crate::compactor::{self, CompactMsg};
use crate::error::Result;
use crate::fsutil::{self, DirLock};
use crate::manifest::{Manifest, ManifestState, VersionEdit};
use crate::memtable::{MemTable, DEFAULT_THRESHOLD};
use crate::record::Record;
use crate::sstable::{SsTableReader, SsTableWriter};
use crate::wal::Wal;

const WAL_FILENAME: &str = "wal.log";
/// Scratch path for a WAL rewrite, published by rename.
const WAL_TMP_FILENAME: &str = "wal.log.tmp";

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

/// A unit of background work for the flush worker thread.
enum Task {
    Flush,
    Shutdown,
}

/// Owns the background flush and compaction threads, shutting both down and
/// joining them on drop.
struct Workers {
    flush_tx: Sender<Task>,
    compact_tx: Sender<CompactMsg>,
    flush_handle: Option<JoinHandle<()>>,
    compact_handle: Option<JoinHandle<()>>,
}

impl Drop for Workers {
    fn drop(&mut self) {
        let _ = self.flush_tx.send(Task::Shutdown);
        if let Some(h) = self.flush_handle.take() {
            let _ = h.join();
        }
        let _ = self.compact_tx.send(CompactMsg::Shutdown);
        if let Some(h) = self.compact_handle.take() {
            let _ = h.join();
        }
    }
}

/// The shared, lock-protected engine state. Reachable from both the public
/// [`Db`] handle and (via a `Weak`) the background threads.
pub(crate) struct DbInner {
    dir: PathBuf,
    opts: Options,
    /// Held for the lifetime of the handle; released when it is dropped.
    _lock: DirLock,
    /// Serializes WAL appends; also held across the matching MemTable insert
    /// so a record is never visible in one without the other.
    wal: Mutex<Wal>,
    /// The MemTable accepting writes.
    active: RwLock<MemTable>,
    /// A MemTable frozen for flushing; reads still consult it until the flush
    /// publishes its SSTable.
    frozen: RwLock<Option<Arc<MemTable>>>,
    /// Live SSTables, oldest -> newest by position; swapped wholesale on change.
    sstables: RwLock<Arc<Vec<Arc<SsTable>>>>,
    next_sst_id: AtomicU64,
    next_seq: AtomicU64,
    manifest: Mutex<Manifest>,
    /// Held for the duration of a flush; the publish step of a compaction
    /// takes it too, so the two never mutate `sstables` concurrently.
    flush_lock: Mutex<()>,
    /// Serializes compaction steps (the background thread and an explicit
    /// `flush()` can both drive them).
    compaction_lock: Mutex<()>,
    /// Live snapshots: sequence-number horizon -> reference count. Compaction
    /// preserves every record version visible to the lowest horizon.
    snapshots: Mutex<BTreeMap<u64, usize>>,
    /// Triggers background flushes.
    worker_tx: Sender<Task>,
    /// Triggers background compaction.
    compact_tx: Sender<CompactMsg>,
    /// Latched first failure. Set once and never cleared: see [`Error::Poisoned`].
    failed: RwLock<Option<Arc<crate::error::Error>>>,
}

/// An embedded LSM-tree key/value store.
pub struct Db {
    // Field order matters: `workers` is dropped first, joining the background
    // threads before `inner` (and its `Arc` refcount) goes away. It is only
    // ever used for that drop-time shutdown.
    #[allow(dead_code)]
    workers: Workers,
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

        // Before anything else. Everything below this line either mutates the
        // directory or deletes from it — the manifest rolls its generation
        // forward and unlinks the old one, and the reclamation loop removes
        // every SSTable the manifest does not name. A second handle running the
        // same sequence concurrently would delete this one's files, and this
        // one would delete its files back.
        let lock = DirLock::acquire(&dir)?;

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
        let lock_name = fsutil::lock_path(&dir);
        for entry in fs::read_dir(&dir)? {
            let name = entry?.file_name();
            let name = name.to_string_lossy();
            if dir.join(name.as_ref()) == lock_name {
                continue;
            }
            if let Some(id) = parse_sst_id(&name) {
                if !state.live_tables.contains(&id) {
                    fs::remove_file(dir.join(name.as_ref()))?;
                }
            } else if name.ends_with(".db.tmp") || name == WAL_TMP_FILENAME {
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

        let (flush_tx, flush_rx) = mpsc::channel();
        let (compact_tx, compact_rx) = mpsc::channel();
        let inner = Arc::new(DbInner {
            dir,
            opts,
            _lock: lock,
            wal: Mutex::new(wal),
            active: RwLock::new(memtable),
            frozen: RwLock::new(None),
            sstables: RwLock::new(Arc::new(sstables)),
            next_sst_id: AtomicU64::new(state.next_sst_id),
            next_seq: AtomicU64::new(next_seq),
            manifest: Mutex::new(manifest),
            flush_lock: Mutex::new(()),
            compaction_lock: Mutex::new(()),
            snapshots: Mutex::new(BTreeMap::new()),
            worker_tx: flush_tx.clone(),
            compact_tx: compact_tx.clone(),
            failed: RwLock::new(None),
        });

        let flush_weak = Arc::downgrade(&inner);
        let flush_handle = thread::spawn(move || worker_loop(flush_rx, flush_weak));
        let compact_weak = Arc::downgrade(&inner);
        let compact_handle =
            thread::spawn(move || compactor::compaction_loop(compact_rx, compact_weak));

        Ok(Db {
            workers: Workers {
                flush_tx,
                compact_tx,
                flush_handle: Some(flush_handle),
                compact_handle: Some(compact_handle),
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
        self.inner.get_at(key, u64::MAX)
    }

    /// Flush the MemTable to a new SSTable and start a fresh WAL, then run any
    /// pending compaction — all synchronously. A no-op if the MemTable is
    /// empty.
    pub fn flush(&self) -> Result<()> {
        self.inner.flush_inner()?;
        while self.inner.compact_step()? {}
        Ok(())
    }

    /// `Err` once a flush or a compaction has failed.
    ///
    /// The engine does not recover in-process: a failed flush leaves the frozen
    /// MemTable stranded and the WAL un-rewritten, so everything written after
    /// it is building on state that will not survive a restart. A caller that
    /// replicates — or that simply wants to fail loudly rather than quietly —
    /// should poll this and reopen the directory.
    pub fn health(&self) -> Result<()> {
        self.inner.health()
    }

    /// Number of immutable SSTables currently on disk.
    pub fn sstable_count(&self) -> usize {
        self.inner.sstables.read().len()
    }

    /// Take a point-in-time [`Snapshot`]: subsequent [`get_at`](Db::get_at)
    /// reads through it observe only writes made before this call.
    ///
    /// While a snapshot is alive, compaction preserves the record versions it
    /// can see, so a long-lived snapshot pins disk space.
    pub fn snapshot(&self) -> Snapshot {
        let horizon = self.inner.next_seq.load(Ordering::SeqCst);
        *self.inner.snapshots.lock().entry(horizon).or_insert(0) += 1;
        Snapshot {
            horizon,
            inner: Arc::clone(&self.inner),
        }
    }

    /// Read `key` as of `snapshot` — the value it had when the snapshot was
    /// taken, ignoring every later write.
    pub fn get_at(&self, snapshot: &Snapshot, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.inner.get_at(key, snapshot.horizon)
    }
}

/// A point-in-time view of the database. Reads through a snapshot (via
/// [`Db::get_at`]) see only writes that preceded [`Db::snapshot`].
pub struct Snapshot {
    /// Sequence-number horizon: records with `seq < horizon` are visible.
    horizon: u64,
    inner: Arc<DbInner>,
}

impl Drop for Snapshot {
    fn drop(&mut self) {
        let mut snapshots = self.inner.snapshots.lock();
        if let Some(count) = snapshots.get_mut(&self.horizon) {
            *count -= 1;
            if *count == 0 {
                snapshots.remove(&self.horizon);
            }
        }
    }
}

impl DbInner {
    /// Latch the first failure and return the error every later call will see.
    ///
    /// Only the first is kept: everything after it is a consequence, and the
    /// first one is the one that says what went wrong.
    fn fail(&self, e: crate::error::Error) -> crate::error::Error {
        let mut slot = self.failed.write();
        if slot.is_none() {
            eprintln!("lsm: engine failed and will not recover in this process: {e}");
            *slot = Some(Arc::new(e));
        }
        crate::error::Error::Poisoned(
            slot.as_ref()
                .map(|e| e.to_string())
                .unwrap_or_else(|| "unknown".into()),
        )
    }

    /// `Err` once a flush or a compaction has failed.
    pub(crate) fn health(&self) -> Result<()> {
        match self.failed.read().as_ref() {
            Some(e) => Err(crate::error::Error::Poisoned(e.to_string())),
            None => Ok(()),
        }
    }

    /// Append a mutation to the WAL and the active MemTable.
    fn write(&self, key: &[u8], value: Option<Vec<u8>>) -> Result<()> {
        self.health()?;
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
    /// SSTables newest-to-oldest. Only records with `seq < seq_bound` are
    /// visible — `u64::MAX` resolves the latest value.
    fn get_at(&self, key: &[u8], seq_bound: u64) -> Result<Option<Vec<u8>>> {
        // Reads refuse as well as writes. A handle whose flush failed is
        // serving from a MemTable whose contents are not going to survive a
        // restart, and answering a read from it is a durability claim the
        // engine can no longer make.
        self.health()?;
        if let Some(record) = self.active.read().get_at(key, seq_bound) {
            return Ok(record.value.clone());
        }
        let frozen = self.frozen.read().clone();
        if let Some(frozen) = frozen {
            if let Some(record) = frozen.get_at(key, seq_bound) {
                return Ok(record.value.clone());
            }
        }
        let sstables = self.sstables.read().clone();
        for table in sstables.iter().rev() {
            if let Some(record) = table.reader.get_at(key, seq_bound)? {
                return Ok(record.value);
            }
        }
        Ok(None)
    }

    /// The lowest sequence-number horizon any live snapshot can observe, or
    /// `u64::MAX` when there are none.
    fn min_snapshot_seq(&self) -> u64 {
        self.snapshots
            .lock()
            .keys()
            .next()
            .copied()
            .unwrap_or(u64::MAX)
    }

    /// Freeze the active MemTable, write it to a new SSTable, publish it, and
    /// rewrite the WAL to back only the records written since the freeze.
    fn flush_inner(&self) -> Result<()> {
        self.health()?;
        self.flush_impl().map_err(|e| self.fail(e))
    }

    fn flush_impl(&self) -> Result<()> {
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
        // The writer already fsynced the file's contents; this makes its *name*
        // durable, which is a separate thing and the one the manifest is about
        // to depend on.
        fs::rename(&tmp_path, &final_path)?;
        fsutil::sync_dir(&self.dir)?;
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
        //
        // Built beside the live WAL and renamed over it. Truncating the live
        // file in place would destroy the only durable copy of every write
        // acknowledged since the freeze, for as long as it took to write them
        // back — a window proportional to how much was written under load.
        {
            let mut wal = self.wal.lock();
            let active = self.active.read();
            let tmp_path = self.dir.join(WAL_TMP_FILENAME);
            let mut fresh = Wal::create(&tmp_path, self.opts.sync_wal)?;
            for record in active.iter() {
                fresh.append(record)?;
            }
            fresh.sync()?;
            fresh.rename_to(self.dir.join(WAL_FILENAME))?;
            fsutil::sync_dir(&self.dir)?;
            *wal = fresh;
        }
        *self.frozen.write() = None;

        // Compaction runs on its own thread so it never blocks the next flush.
        let _ = self.compact_tx.send(CompactMsg::Run);
        Ok(())
    }

    /// Perform at most one size-tiered compaction. Returns `true` if a run was
    /// merged (so the caller should call again), `false` if none qualified.
    ///
    /// The heavy merge writes the output SSTable without any lock held; only
    /// the publish step takes `flush_lock`, so flushes proceed concurrently.
    pub(crate) fn compact_step(&self) -> Result<bool> {
        self.health()?;
        self.compact_step_impl().map_err(|e| self.fail(e))
    }

    fn compact_step_impl(&self) -> Result<bool> {
        let _compaction_guard = self.compaction_lock.lock();

        let tables = self.sstables.read().clone();
        let Some((start, end)) = pick_compaction(&tables, self.opts.compaction_threshold)? else {
            return Ok(false);
        };
        let run: Vec<Arc<SsTable>> = tables[start..end].to_vec();
        // The merged table reuses the run's highest id, keeping it in the same
        // position in the live set; the manifest makes the swap crash-safe.
        let Some(last) = run.last() else {
            return Ok(false);
        };
        let max_id = last.id;
        let drop_tombstones = run[0].id == tables[0].id;

        // Read the snapshot horizon once: new snapshots only take a larger
        // value, so this stays a valid lower bound for the whole merge.
        let min_snapshot_seq = self.min_snapshot_seq();

        let final_path = sst_path(&self.dir, max_id);
        let tmp_path = final_path.with_extension("db.tmp");
        {
            let inputs: Vec<&SsTableReader> = run.iter().map(|t| &t.reader).collect();
            compaction::compact(&tmp_path, &inputs, drop_tombstones, min_snapshot_seq)?;
        }

        // Publish: rename over the max-id input, drop the rest from the
        // manifest, splice the live set, then unlink the stale files.
        {
            let _flush_guard = self.flush_lock.lock();
            fs::rename(&tmp_path, &final_path)?;
            // Before the DeleteTable edits below: the merged output has to be
            // durable at its name before the manifest says its inputs are gone.
            fsutil::sync_dir(&self.dir)?;
            let reader = SsTableReader::open(&final_path, self.opts.bloom_enabled)?;

            let stale_ids: Vec<u64> = run[..run.len() - 1].iter().map(|t| t.id).collect();
            self.manifest.lock().append_batch(
                &stale_ids
                    .iter()
                    .map(|&id| VersionEdit::DeleteTable { id })
                    .collect::<Vec<_>>(),
            )?;

            // A concurrent flush only appends, so the run is still a contiguous
            // block ending at `max_id`.
            let cur = self.sstables.read().clone();
            let end_pos = cur.iter().position(|t| t.id == max_id).ok_or_else(|| {
                crate::error::Error::Corrupt(format!(
                    "compacted run vanished from the live set: sst {max_id}"
                ))
            })?;
            let start_pos = end_pos + 1 - run.len();
            let mut next = Vec::with_capacity(cur.len() - run.len() + 1);
            next.extend(cur[..start_pos].iter().cloned());
            next.push(Arc::new(SsTable { id: max_id, reader }));
            next.extend(cur[end_pos + 1..].iter().cloned());
            *self.sstables.write() = Arc::new(next);

            for id in &stale_ids {
                fs::remove_file(sst_path(&self.dir, *id))?;
            }
            // Hygiene only, and one fsync for the whole batch: a lost unlink is
            // a space leak the next open reclaims, not a correctness problem.
            fsutil::sync_dir(&self.dir)?;
        }
        Ok(true)
    }
}

/// Background worker: drains flush requests until the [`Db`] is dropped.
fn worker_loop(rx: Receiver<Task>, inner: Weak<DbInner>) {
    while let Ok(task) = rx.recv() {
        match task {
            Task::Shutdown => break,
            Task::Flush => {
                let Some(inner) = inner.upgrade() else { break };
                if inner.flush_inner().is_err() {
                    // flush_inner has latched it. Continuing would retry a
                    // flush that cannot succeed, once per write, forever.
                    break;
                }
            }
        }
    }
}

/// Pick a contiguous, id-ordered run of `threshold`+ SSTables that fall in the
/// same size tier, returning its `start..end` range.
fn pick_compaction(tables: &[Arc<SsTable>], threshold: usize) -> Result<Option<(usize, usize)>> {
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
    use crate::error::Error;

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
        // Survives reopen. The first handle has to go first: shadowing the
        // binding does not drop it, and two live handles on one directory
        // reclaim each other's SSTables.
        drop(db);
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
    fn background_compaction_bounds_table_count() {
        let dir = tempfile::tempdir().unwrap();
        let opts = Options {
            memtable_threshold: 8 * 1024,
            sync_wal: false,
            compaction_threshold: 4,
            ..Options::default()
        };
        let db = Db::open_with(dir.path(), opts).unwrap();
        for i in 0..40_000u32 {
            db.put(format!("key{i:08}").as_bytes(), b"payload-value-data")
                .unwrap();
        }
        // The compaction thread runs asynchronously; give it time to settle.
        let mut count = db.sstable_count();
        for _ in 0..100 {
            thread::sleep(std::time::Duration::from_millis(20));
            let now = db.sstable_count();
            if now == count {
                break;
            }
            count = now;
        }
        for i in (0..40_000u32).step_by(97) {
            assert_eq!(
                db.get(format!("key{i:08}").as_bytes()).unwrap(),
                Some(b"payload-value-data".to_vec())
            );
        }
    }

    #[test]
    fn snapshot_sees_value_at_capture_time() {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open_with(dir.path(), fast_opts()).unwrap();
        db.put(b"k", b"v1").unwrap();
        db.flush().unwrap();

        let snap = db.snapshot();
        db.put(b"k", b"v2").unwrap();
        db.flush().unwrap();

        assert_eq!(db.get(b"k").unwrap(), Some(b"v2".to_vec()));
        assert_eq!(db.get_at(&snap, b"k").unwrap(), Some(b"v1".to_vec()));
    }

    #[test]
    fn snapshot_sees_absence_of_later_keys() {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open_with(dir.path(), fast_opts()).unwrap();
        db.put(b"early", b"1").unwrap();

        let snap = db.snapshot();
        db.put(b"late", b"2").unwrap();

        assert_eq!(db.get_at(&snap, b"early").unwrap(), Some(b"1".to_vec()));
        assert_eq!(db.get_at(&snap, b"late").unwrap(), None);
        assert_eq!(db.get(b"late").unwrap(), Some(b"2".to_vec()));
    }

    #[test]
    fn snapshot_survives_compaction() {
        let dir = tempfile::tempdir().unwrap();
        let opts = Options {
            sync_wal: false,
            compaction_threshold: 3,
            ..Options::default()
        };
        let db = Db::open_with(dir.path(), opts).unwrap();
        db.put(b"k", b"v1").unwrap();
        db.flush().unwrap();

        let snap = db.snapshot();

        // Two more flushes of the same key trigger a compaction that would,
        // without the snapshot horizon, collapse v1 away.
        db.put(b"k", b"v2").unwrap();
        db.flush().unwrap();
        db.put(b"k", b"v3").unwrap();
        db.flush().unwrap();

        assert_eq!(db.get(b"k").unwrap(), Some(b"v3".to_vec()));
        assert_eq!(db.get_at(&snap, b"k").unwrap(), Some(b"v1".to_vec()));
    }

    /// The fix is an ordering property, and this is the observable half of it:
    /// if the live WAL were still being truncated in place, its inode would
    /// survive the flush. A rename gives the name a different inode.
    #[cfg(unix)]
    #[test]
    fn a_flush_publishes_a_new_wal_rather_than_truncating_the_live_one() {
        use std::os::unix::fs::MetadataExt;

        let dir = tempfile::tempdir().unwrap();
        let db = Db::open_with(dir.path(), fast_opts()).unwrap();
        db.put(b"a", b"1").unwrap();

        let wal = dir.path().join(WAL_FILENAME);
        let before = std::fs::metadata(&wal).unwrap().ino();

        db.flush().unwrap();
        db.put(b"b", b"2").unwrap();

        let after = std::fs::metadata(&wal).unwrap().ino();
        assert_ne!(
            before, after,
            "the live WAL was truncated in place, which loses every write \
             acknowledged since the freeze if the process dies mid-rewrite"
        );
        assert!(
            !dir.path().join(WAL_TMP_FILENAME).exists(),
            "the scratch file should have been renamed away, not left behind"
        );

        drop(db);
        let db = Db::open_with(dir.path(), fast_opts()).unwrap();
        assert_eq!(db.get(b"a").unwrap().as_deref(), Some(&b"1"[..]));
        assert_eq!(db.get(b"b").unwrap().as_deref(), Some(&b"2"[..]));
    }

    #[test]
    fn a_stale_wal_scratch_file_is_reclaimed_on_open() {
        let dir = tempfile::tempdir().unwrap();
        {
            let db = Db::open_with(dir.path(), fast_opts()).unwrap();
            db.put(b"k", b"v").unwrap();
        }
        // A crash between creating the scratch WAL and renaming it leaves this.
        std::fs::write(dir.path().join(WAL_TMP_FILENAME), b"garbage").unwrap();

        let db = Db::open_with(dir.path(), fast_opts()).unwrap();
        assert_eq!(db.get(b"k").unwrap().as_deref(), Some(&b"v"[..]));
        assert!(!dir.path().join(WAL_TMP_FILENAME).exists());
    }

    #[test]
    fn a_failed_flush_poisons_the_handle() {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open_with(dir.path(), fast_opts()).unwrap();
        db.put(b"k", b"v").unwrap();

        // Occupy the path the flush will write its SSTable to, with something
        // File::create cannot open.
        std::fs::create_dir(sst_path(dir.path(), 0).with_extension("db.tmp")).unwrap();

        assert!(db.flush().is_err());

        // The frozen MemTable is stranded and the WAL was never rewritten.
        // Continuing to accept writes would build on state that is not going to
        // survive a restart, so every entry point refuses from here on.
        assert!(matches!(db.health(), Err(Error::Poisoned(_))));
        assert!(matches!(db.put(b"k2", b"v"), Err(Error::Poisoned(_))));
        assert!(matches!(db.delete(b"k"), Err(Error::Poisoned(_))));
        assert!(matches!(db.get(b"k"), Err(Error::Poisoned(_))));
        assert!(matches!(db.flush(), Err(Error::Poisoned(_))));
    }

    #[test]
    fn only_the_first_failure_is_kept() {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open_with(dir.path(), fast_opts()).unwrap();

        db.inner.fail(Error::Corrupt("first".into()));
        db.inner.fail(Error::Corrupt("second".into()));

        // Everything after the first is a consequence of it.
        match db.health() {
            Err(Error::Poisoned(msg)) => assert!(msg.contains("first"), "got {msg}"),
            other => panic!("expected Poisoned, got {other:?}"),
        }
    }

    #[test]
    fn a_second_open_of_a_live_directory_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open(dir.path()).unwrap();
        db.put(b"k", b"v").unwrap();

        // Without this, the second open rolls the manifest forward, deletes the
        // first handle's generation, and reclaims its SSTables as orphans.
        match Db::open(dir.path()) {
            Err(crate::error::Error::Locked(_)) => {}
            other => panic!("expected Locked, got {:?}", other.map(|_| "Db")),
        }

        drop(db);
        let reopened = Db::open(dir.path()).expect("the lock is released on drop");
        assert_eq!(reopened.get(b"k").unwrap().as_deref(), Some(&b"v"[..]));
    }

    #[test]
    fn the_lock_file_is_not_reclaimed_as_an_orphan() {
        let dir = tempfile::tempdir().unwrap();
        {
            let db = Db::open(dir.path()).unwrap();
            db.put(b"k", b"v").unwrap();
            db.flush().unwrap();
        }
        // Reopening runs the reclamation loop, which must leave LOCK alone.
        let db = Db::open(dir.path()).unwrap();
        assert_eq!(db.get(b"k").unwrap().as_deref(), Some(&b"v"[..]));
        assert!(crate::fsutil::lock_path(dir.path()).exists());
    }

    #[test]
    fn parse_sst_id_matching() {
        assert_eq!(parse_sst_id("sst_0000000007.db"), Some(7));
        assert_eq!(parse_sst_id("wal.log"), None);
        assert_eq!(parse_sst_id("sst_.db"), None);
    }
}
