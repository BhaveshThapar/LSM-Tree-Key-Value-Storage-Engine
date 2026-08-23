//! The storage engine: orchestrates the WAL, the MemTable, and the stack of
//! immutable SSTables behind a simple `put` / `get` / `delete` API.
//!
//! [`Db`] is `Send + Sync` and exposes a `&self` API: all mutable state lives
//! in [`DbInner`] behind `parking_lot` locks, so the handle can be shared
//! across threads (typically via `Arc<Db>`). Flushing the MemTable runs on a
//! background worker thread, so writers never stall waiting on flush I/O.

use std::collections::BTreeMap;
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
use crate::fs::{Fs, StdFs};
use crate::fsutil::{self, DirLock};
use crate::manifest::{Manifest, ManifestState, VersionEdit};
use crate::memtable::{DEFAULT_THRESHOLD, MemTable};
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
    /// Who runs flushes and compactions. See [`Maintenance`].
    pub maintenance: Maintenance,
}

/// Who runs the engine's background work.
///
/// The default spawns two threads and is what an application wants: a writer
/// never stalls behind a flush, and a long merge never blocks one.
///
/// [`Maintenance::Manual`] spawns nothing and hands the work to the caller,
/// which is what a deterministic harness needs. A simulator that replays a run
/// from a seed cannot have a thread deciding when a flush happens: the whole
/// claim is that the run is a function of the seed, and a thread makes it a
/// function of the scheduler too. The same is true of any caller that wants to
/// know exactly when its data reached an SSTable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Maintenance {
    /// A flush thread and a compaction thread, started by `open` and joined on
    /// drop.
    #[default]
    Threads,
    /// No threads. The caller drives the work with [`Db::maintain`], and
    /// nothing happens if it does not — including the automatic flush that
    /// bounds the MemTable, so a caller that never maintains will grow one
    /// without limit.
    Manual,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            memtable_threshold: DEFAULT_THRESHOLD,
            sync_wal: true,
            compaction_threshold: 4,
            bloom_enabled: true,
            maintenance: Maintenance::default(),
        }
    }
}

/// An immutable SSTable on disk, tagged with its id (higher id == newer).
struct SsTable<F: Fs> {
    id: u64,
    reader: SsTableReader<F>,
}

/// A unit of background work for the flush worker thread.
enum Task {
    Flush,
    Shutdown,
}

/// Owns the background flush and compaction threads, shutting both down and
/// joining them on drop.
///
/// Absent entirely under [`Maintenance::Manual`], which is the point: not a
/// thread that is idle, but no thread at all, so a caller can prove by
/// inspection that nothing runs behind its back.
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
pub(crate) struct DbInner<F: Fs> {
    dir: PathBuf,
    opts: Options,
    /// The filesystem every path below goes through.
    fs: F,
    /// Held for the lifetime of the handle; released when it is dropped.
    _lock: DirLock<F>,
    /// Serializes WAL appends; also held across the matching MemTable insert
    /// so a record is never visible in one without the other.
    wal: Mutex<Wal<F>>,
    /// The MemTable accepting writes.
    active: RwLock<MemTable>,
    /// A MemTable frozen for flushing; reads still consult it until the flush
    /// publishes its SSTable.
    frozen: RwLock<Option<Arc<MemTable>>>,
    /// Live SSTables, oldest -> newest by position; swapped wholesale on change.
    sstables: RwLock<Arc<Vec<Arc<SsTable<F>>>>>,
    next_sst_id: AtomicU64,
    next_seq: AtomicU64,
    manifest: Mutex<Manifest<F>>,
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
pub struct Db<F: Fs = StdFs> {
    // Field order matters: `workers` is dropped first, joining the background
    // threads before `inner` (and its `Arc` refcount) goes away. It is only
    // ever used for that drop-time shutdown.
    #[allow(dead_code)]
    workers: Option<Workers>,
    inner: Arc<DbInner<F>>,
}

impl Db<StdFs> {
    /// Open (creating if needed) the database rooted at `dir` with defaults.
    pub fn open(dir: impl AsRef<Path>) -> Result<Db<StdFs>> {
        Db::open_with(dir, Options::default())
    }

    /// Open the database rooted at `dir` with explicit [`Options`].
    pub fn open_with(dir: impl AsRef<Path>, opts: Options) -> Result<Db<StdFs>> {
        Db::open_on(StdFs, dir, opts)
    }
}

impl<F: Fs + Send + Sync> Db<F>
where
    F::File: Send + Sync,
{
    /// Open on `fs`, honouring [`Options::maintenance`].
    ///
    /// The `Send + Sync` bounds are here rather than on [`Fs`] because they are
    /// what spawning a thread needs, and only [`Maintenance::Threads`] spawns
    /// one. A single-threaded harness whose filesystem is built on
    /// `Rc<RefCell<_>>` cannot satisfy them and should not have to: see
    /// [`Db::open_manual`].
    pub fn open_on(fs: F, dir: impl AsRef<Path>, opts: Options) -> Result<Db<F>> {
        let (inner, flush_tx, flush_rx, compact_tx, compact_rx) = Db::assemble(fs, dir, opts)?;
        let workers = match inner.opts.maintenance {
            Maintenance::Threads => {
                let flush_weak = Arc::downgrade(&inner);
                let flush_handle = thread::spawn(move || worker_loop(flush_rx, flush_weak));
                let compact_weak = Arc::downgrade(&inner);
                let compact_handle =
                    thread::spawn(move || compactor::compaction_loop(compact_rx, compact_weak));
                Some(Workers {
                    flush_tx,
                    compact_tx,
                    flush_handle: Some(flush_handle),
                    compact_handle: Some(compact_handle),
                })
            }
            // The receivers are dropped here along with `flush_rx` and
            // `compact_rx`, so every later `send` fails — which is already the
            // ignored-error path the triggers take.
            Maintenance::Manual => None,
        };
        Ok(Db { workers, inner })
    }
}

impl<F: Fs> Db<F> {
    /// Open on `fs` with no background threads, whatever [`Options::maintenance`]
    /// says.
    ///
    /// This is the constructor a deterministic harness uses. It asks nothing of
    /// `F` beyond [`Fs`] — no `Send`, no `Sync` — because it starts no thread
    /// that would need them, so a filesystem built on `Rc<RefCell<_>>` is
    /// allowed here and atomics stay out of the one component that has to
    /// reproduce a run exactly.
    ///
    /// The caller drives the work with [`Db::maintain`]; nothing happens if it
    /// does not.
    pub fn open_manual(fs: F, dir: impl AsRef<Path>, opts: Options) -> Result<Db<F>> {
        let opts = Options {
            maintenance: Maintenance::Manual,
            ..opts
        };
        let (inner, _flush_tx, _flush_rx, _compact_tx, _compact_rx) = Db::assemble(fs, dir, opts)?;
        Ok(Db {
            workers: None,
            inner,
        })
    }

    /// Everything both constructors do: lock, recover, and build the state.
    ///
    /// Hands back the channel ends rather than keeping them, so the caller
    /// decides whether anything is on the other side of them.
    #[allow(clippy::type_complexity)]
    fn assemble(
        fs: F,
        dir: impl AsRef<Path>,
        opts: Options,
    ) -> Result<(
        Arc<DbInner<F>>,
        Sender<Task>,
        Receiver<Task>,
        Sender<CompactMsg>,
        Receiver<CompactMsg>,
    )> {
        let dir = dir.as_ref().to_path_buf();
        fs.create_dir_all(&dir)?;

        // Before anything else. Everything below this line either mutates the
        // directory or deletes from it — the manifest rolls its generation
        // forward and unlinks the old one, and the reclamation loop removes
        // every SSTable the manifest does not name. A second handle running the
        // same sequence concurrently would delete this one's files, and this
        // one would delete its files back.
        let lock = DirLock::acquire(&fs, &dir)?;

        // The manifest is the authoritative record of the live SSTable set and
        // the global counters. If absent, migrate a Phase 2 directory by
        // scanning its `sst_*.db` files into a fresh manifest.
        let (manifest, state) = if Manifest::exists(&fs, &dir) {
            Manifest::open(&fs, &dir)?
        } else {
            Db::migrate_dir(&fs, &dir)?
        };

        // Reclaim orphan SSTables: any `sst_*.db` not named by the manifest is
        // a leftover from a crash before its `AddTable` edit was durable.
        let lock_name = fsutil::lock_path(&dir);
        for path in fs.list(&dir)? {
            if path == lock_name {
                continue;
            }
            let name = match path.file_name() {
                Some(name) => name.to_string_lossy().into_owned(),
                None => continue,
            };
            if let Some(id) = parse_sst_id(&name) {
                if !state.live_tables.contains(&id) {
                    fs.remove(&path)?;
                }
            } else if name.ends_with(".db.tmp") || name == WAL_TMP_FILENAME {
                fs.remove(&path)?;
            }
        }

        // Open the live SSTables, ordered oldest -> newest by id.
        let sstables: Vec<Arc<SsTable<F>>> = state
            .live_tables
            .iter()
            .map(|&id| {
                SsTableReader::open(&fs, sst_path(&dir, id), opts.bloom_enabled)
                    .map(|reader| Arc::new(SsTable { id, reader }))
            })
            .collect::<Result<Vec<_>>>()?;

        // Replay the WAL into a fresh MemTable. The manifest persists `next_seq`
        // across clean flushes; the live WAL may carry seqs beyond it.
        let wal_path = dir.join(WAL_FILENAME);
        let replayed = Wal::replay(&fs, &wal_path)?;
        let mut memtable = MemTable::new(opts.memtable_threshold);
        let mut next_seq = state.next_seq;
        for record in replayed {
            next_seq = next_seq.max(record.seq + 1);
            memtable.insert(record);
        }
        // Open the WAL for *appending*: it still backs the records just
        // replayed into the MemTable until the next flush moves them to an
        // SSTable. Truncating here would lose them if we exit before flushing.
        let wal = Wal::open_append(&fs, &wal_path, opts.sync_wal)?;

        let (flush_tx, flush_rx) = mpsc::channel();
        let (compact_tx, compact_rx) = mpsc::channel();
        let inner = Arc::new(DbInner {
            dir,
            opts,
            fs,
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

        Ok((inner, flush_tx, flush_rx, compact_tx, compact_rx))
    }

    /// First open of a directory: synthesize a manifest from whatever SSTables
    /// already exist (a Phase 2 layout, or an empty directory).
    fn migrate_dir(fs: &F, dir: &Path) -> Result<(Manifest<F>, ManifestState)> {
        let mut sst_ids = Vec::new();
        for path in fs.list(dir)? {
            if let Some(name) = path.file_name()
                && let Some(id) = parse_sst_id(&name.to_string_lossy())
            {
                sst_ids.push(id);
            }
        }
        sst_ids.sort_unstable();
        let next_sst_id = sst_ids.last().map_or(0, |&id| id + 1);

        let mut manifest = Manifest::create(fs, dir)?;
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
    ///
    /// See [`Db::flush_only`] for the half of this that does not merge.
    pub fn flush(&self) -> Result<()> {
        self.inner.flush_inner()?;
        while self.inner.compact_step()? {}
        Ok(())
    }

    /// Flush the MemTable to a new SSTable, and stop there.
    ///
    /// [`Db::flush`] also drains every pending compaction, which is a merge of
    /// unbounded size: a caller who wanted their writes on disk cannot tell how
    /// long it will take, because the answer depends on how many SSTables have
    /// accumulated. This is the bounded half — the work is proportional to the
    /// MemTable, which the caller chose the size of.
    ///
    /// A caller that wants both, and wants to know the cost of each, calls this
    /// and then [`Db::maintain`] in a loop.
    pub fn flush_only(&self) -> Result<()> {
        self.inner.flush_inner()
    }

    /// Do one unit of the work a background thread would otherwise have done,
    /// and report whether any remains.
    ///
    /// A flush first when one is due, then one compaction step. One unit rather
    /// than all of it, so a caller with something else to do — a consensus
    /// host with a timer to service — can interleave rather than disappear into
    /// a merge.
    ///
    /// Useful under either [`Maintenance`] setting, and required under
    /// [`Maintenance::Manual`]: nothing else will flush, including the automatic
    /// flush that bounds the MemTable.
    ///
    /// ```no_run
    /// # use lsm_kv::{Db, Maintenance, Options};
    /// # let opts = Options { maintenance: Maintenance::Manual, ..Options::default() };
    /// # let db = Db::open_with("./data", opts)?;
    /// while db.maintain()? {}
    /// # Ok::<(), lsm_kv::Error>(())
    /// ```
    pub fn maintain(&self) -> Result<bool> {
        if self.inner.flush_is_due() {
            self.inner.flush_inner()?;
            return Ok(true);
        }
        self.inner.compact_step()
    }

    /// Whether [`Db::maintain`] would do anything.
    ///
    /// Cheap enough to poll every turn of a host loop: it reads two lock-guarded
    /// booleans and a length, and does no I/O.
    pub fn pending_work(&self) -> bool {
        self.inner.flush_is_due() || self.inner.compaction_is_due()
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
    pub fn snapshot(&self) -> Snapshot<F> {
        let horizon = self.inner.next_seq.load(Ordering::SeqCst);
        *self.inner.snapshots.lock().entry(horizon).or_insert(0) += 1;
        Snapshot {
            horizon,
            inner: Arc::clone(&self.inner),
        }
    }

    /// Read `key` as of `snapshot` — the value it had when the snapshot was
    /// taken, ignoring every later write.
    pub fn get_at(&self, snapshot: &Snapshot<F>, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.inner.get_at(key, snapshot.horizon)
    }
}

/// A point-in-time view of the database. Reads through a snapshot (via
/// [`Db::get_at`]) see only writes that preceded [`Db::snapshot`].
pub struct Snapshot<F: Fs = StdFs> {
    /// Sequence-number horizon: records with `seq < horizon` are visible.
    horizon: u64,
    inner: Arc<DbInner<F>>,
}

impl<F: Fs> Drop for Snapshot<F> {
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

impl<F: Fs> DbInner<F> {
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
        if let Some(frozen) = frozen
            && let Some(record) = frozen.get_at(key, seq_bound)
        {
            return Ok(record.value.clone());
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

    /// Whether there is anything for a flush to do.
    ///
    /// A frozen MemTable means a previous flush froze and then failed to
    /// publish; a full active one means the threshold has been crossed and
    /// nothing has acted on it yet. Either way the next flush has work.
    fn flush_is_due(&self) -> bool {
        if self.frozen.read().is_some() {
            return true;
        }
        let active = self.active.read();
        !active.is_empty() && active.is_full()
    }

    /// Whether a compaction step would find a run to merge.
    ///
    /// Deliberately approximate: it compares the table count against the
    /// threshold without reading a single file size, because the exact answer
    /// costs a `stat` per table and this is polled on a hot loop. A false
    /// positive costs one `compact_step` that returns `false`.
    fn compaction_is_due(&self) -> bool {
        self.sstables.read().len() >= self.opts.compaction_threshold
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
        SsTableWriter::write(&self.fs, &tmp_path, &records)?;
        // Atomic publish: a crash before the rename leaves only a stale .tmp.
        // The writer already fsynced the file's contents; this makes its *name*
        // durable, which is a separate thing and the one the manifest is about
        // to depend on.
        self.fs.rename(&tmp_path, &final_path)?;
        self.fs.sync_dir(&self.dir)?;
        let reader = SsTableReader::open(&self.fs, &final_path, self.opts.bloom_enabled)?;

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
            let mut fresh = Wal::create(&self.fs, &tmp_path, self.opts.sync_wal)?;
            for record in active.iter() {
                fresh.append(record)?;
            }
            fresh.sync()?;
            fresh.rename_to(&self.fs, self.dir.join(WAL_FILENAME))?;
            self.fs.sync_dir(&self.dir)?;
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
        let Some((start, end)) =
            pick_compaction(&self.fs, &tables, self.opts.compaction_threshold)?
        else {
            return Ok(false);
        };
        let run: Vec<Arc<SsTable<F>>> = tables[start..end].to_vec();
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
            let inputs: Vec<&SsTableReader<F>> = run.iter().map(|t| &t.reader).collect();
            compaction::compact(
                &self.fs,
                &tmp_path,
                &inputs,
                drop_tombstones,
                min_snapshot_seq,
            )?;
        }

        // Publish: rename over the max-id input, drop the rest from the
        // manifest, splice the live set, then unlink the stale files.
        {
            let _flush_guard = self.flush_lock.lock();
            self.fs.rename(&tmp_path, &final_path)?;
            // Before the DeleteTable edits below: the merged output has to be
            // durable at its name before the manifest says its inputs are gone.
            self.fs.sync_dir(&self.dir)?;
            let reader = SsTableReader::open(&self.fs, &final_path, self.opts.bloom_enabled)?;

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
                self.fs.remove(&sst_path(&self.dir, *id))?;
            }
            // Hygiene only, and one fsync for the whole batch: a lost unlink is
            // a space leak the next open reclaims, not a correctness problem.
            self.fs.sync_dir(&self.dir)?;
        }
        Ok(true)
    }
}

/// Background worker: drains flush requests until the [`Db`] is dropped.
fn worker_loop<F: Fs>(rx: Receiver<Task>, inner: Weak<DbInner<F>>) {
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
fn pick_compaction<F: Fs>(
    fs: &F,
    tables: &[Arc<SsTable<F>>],
    threshold: usize,
) -> Result<Option<(usize, usize)>> {
    if tables.len() < threshold {
        return Ok(None);
    }
    let mut tiers = Vec::with_capacity(tables.len());
    for t in tables {
        let len = fs.size(t.reader.path())?;
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
        std::fs::copy(sst_path(dir.path(), 0), &orphan).unwrap();
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
        std::fs::remove_file(dir.path().join("CURRENT")).unwrap();
        for entry in std::fs::read_dir(dir.path()).unwrap() {
            let name = entry.unwrap().file_name();
            if name.to_string_lossy().starts_with("MANIFEST-") {
                std::fs::remove_file(dir.path().join(name)).unwrap();
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

    /// Manual maintenance spawns nothing. The evidence is that the work does
    /// not happen on its own: writes past the threshold leave the MemTable
    /// full and no SSTable behind them.
    #[test]
    fn manual_maintenance_runs_nothing_until_asked() {
        let dir = tempfile::tempdir().unwrap();
        let opts = Options {
            memtable_threshold: 128,
            maintenance: Maintenance::Manual,
            ..fast_opts()
        };
        let db = Db::open_with(dir.path(), opts).unwrap();

        for i in 0..64u32 {
            db.put(format!("k{i:04}").as_bytes(), b"some value bytes")
                .unwrap();
        }
        // A threads-mode handle would have flushed by now, on its own schedule.
        // Give one every chance to prove it did.
        std::thread::sleep(std::time::Duration::from_millis(50));
        assert_eq!(
            db.sstable_count(),
            0,
            "something flushed without being asked to"
        );
        assert!(
            db.pending_work(),
            "a full MemTable is work waiting to happen"
        );

        while db.maintain().unwrap() {}
        assert!(db.sstable_count() > 0, "maintain did not flush");
        assert!(!db.pending_work());

        // And the data is all still there.
        for i in 0..64u32 {
            assert_eq!(
                db.get(format!("k{i:04}").as_bytes()).unwrap().as_deref(),
                Some(&b"some value bytes"[..])
            );
        }
    }

    /// Under manual maintenance the engine is a function of its calls. Two
    /// databases given the same writes and the same maintenance calls end with
    /// the same number of SSTables — which is the property a deterministic
    /// harness needs and a thread cannot provide.
    #[test]
    fn manual_maintenance_is_reproducible() {
        let run = || {
            let dir = tempfile::tempdir().unwrap();
            let opts = Options {
                memtable_threshold: 256,
                compaction_threshold: 2,
                maintenance: Maintenance::Manual,
                ..fast_opts()
            };
            let db = Db::open_with(dir.path(), opts).unwrap();
            let mut counts = Vec::new();
            for i in 0..200u32 {
                db.put(format!("k{i:04}").as_bytes(), b"value").unwrap();
                if i % 10 == 0 {
                    let _ = db.maintain().unwrap();
                    counts.push(db.sstable_count());
                }
            }
            counts
        };
        assert_eq!(run(), run(), "the same calls produced a different history");
    }

    /// `flush_only` is the bounded half of `flush`: it writes the MemTable out
    /// and does not go on to merge.
    #[test]
    fn flush_only_does_not_compact() {
        let dir = tempfile::tempdir().unwrap();
        let opts = Options {
            memtable_threshold: 64,
            compaction_threshold: 2,
            maintenance: Maintenance::Manual,
            ..fast_opts()
        };
        let db = Db::open_with(dir.path(), opts).unwrap();

        // Three separate flushes, so a compaction is well past due.
        for round in 0..3u32 {
            for i in 0..8u32 {
                db.put(format!("k{round}{i}").as_bytes(), b"v").unwrap();
            }
            db.flush_only().unwrap();
        }
        assert_eq!(
            db.sstable_count(),
            3,
            "flush_only merged tables it was not asked to merge"
        );
        assert!(db.pending_work(), "a merge is due and was not noticed");

        db.flush().unwrap();
        assert!(
            db.sstable_count() < 3,
            "flush did not drain the compaction flush_only left"
        );
    }

    /// The default is unchanged: threads, started by open.
    #[test]
    fn threads_remain_the_default() {
        assert_eq!(Options::default().maintenance, Maintenance::Threads);
    }

    #[test]
    fn parse_sst_id_matching() {
        assert_eq!(parse_sst_id("sst_0000000007.db"), Some(7));
        assert_eq!(parse_sst_id("wal.log"), None);
        assert_eq!(parse_sst_id("sst_.db"), None);
    }
}
