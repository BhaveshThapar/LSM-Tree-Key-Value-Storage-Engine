//! The seam is only worth its diff if something other than `StdFs` can go
//! through it. These are that something.
//!
//! Three filesystems, each proving a different thing:
//!
//! * `CountingFs` — that every path really goes through the seam. If some
//!   operation still called `std::fs` directly, the engine would work and the
//!   counter would not move, so the assertion is on the counter.
//! * `MemFs` — that the seam does not require `Send` or `Sync`. It is built on
//!   `Rc<RefCell<_>>` and cannot be shared across threads at all, which is the
//!   shape a single-threaded deterministic harness needs and the reason the
//!   thread bounds live on the constructor rather than on the trait.
//! * `RefusingFs` — that an injected error reaches the engine's latch rather
//!   than being swallowed, and that a failure which made nothing visible does
//!   *not* latch.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use lsm_kv::{Db, File, Fs, Maintenance, OpenMode, Options, StdFs, SyncMode};

fn manual_opts() -> Options {
    Options {
        memtable_threshold: 512,
        compaction_threshold: 2,
        sync_wal: SyncMode::None,
        maintenance: Maintenance::Manual,
        ..Options::default()
    }
}

// ------------------------------------------------------------- CountingFs

#[derive(Default)]
struct Counts {
    opens: AtomicUsize,
    renames: AtomicUsize,
    removes: AtomicUsize,
    dir_syncs: AtomicUsize,
}

struct CountingFs {
    counts: Arc<Counts>,
}

impl Fs for CountingFs {
    type File = <StdFs as Fs>::File;

    fn create_dir_all(&self, dir: &Path) -> io::Result<()> {
        StdFs.create_dir_all(dir)
    }
    fn list(&self, dir: &Path) -> io::Result<Vec<PathBuf>> {
        StdFs.list(dir)
    }
    fn open(&self, path: &Path, mode: OpenMode) -> io::Result<Self::File> {
        self.counts.opens.fetch_add(1, Ordering::SeqCst);
        StdFs.open(path, mode)
    }
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        self.counts.renames.fetch_add(1, Ordering::SeqCst);
        StdFs.rename(from, to)
    }
    fn remove(&self, path: &Path) -> io::Result<()> {
        self.counts.removes.fetch_add(1, Ordering::SeqCst);
        StdFs.remove(path)
    }
    fn size(&self, path: &Path) -> io::Result<u64> {
        StdFs.size(path)
    }
    fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        self.counts.dir_syncs.fetch_add(1, Ordering::SeqCst);
        StdFs.sync_dir(dir)
    }
}

/// Every file the engine touches goes through the seam, including the ones a
/// flush publishes by rename and the directory fsync that makes the rename
/// durable.
#[test]
fn every_operation_goes_through_the_seam() {
    let dir = tempfile::tempdir().unwrap();
    let counts = Arc::new(Counts::default());
    let db = Db::open_on(
        CountingFs {
            counts: Arc::clone(&counts),
        },
        dir.path(),
        manual_opts(),
    )
    .unwrap();

    for i in 0..200u32 {
        db.put(
            format!("k{i:04}").as_bytes(),
            b"a value long enough to matter",
        )
        .unwrap();
    }
    while db.maintain().unwrap() {}

    assert!(
        counts.opens.load(Ordering::SeqCst) > 0,
        "nothing was opened"
    );
    assert!(
        counts.renames.load(Ordering::SeqCst) > 0,
        "a flush published without a rename, so it bypassed the seam"
    );
    assert!(
        counts.dir_syncs.load(Ordering::SeqCst) > 0,
        "a rename was published without a directory fsync going through the seam"
    );
    drop(db);

    // And the data is intact when read back through the ordinary filesystem,
    // which says the seam did not change what was written.
    let reopened = Db::open_with(dir.path(), manual_opts()).unwrap();
    for i in 0..200u32 {
        assert_eq!(
            reopened
                .get(format!("k{i:04}").as_bytes())
                .unwrap()
                .as_deref(),
            Some(&b"a value long enough to matter"[..]),
            "key {i} did not survive"
        );
    }
}

// ------------------------------------------------------------------ MemFs

/// A whole filesystem in memory, behind `Rc<RefCell<_>>`.
///
/// Deliberately neither `Send` nor `Sync`. That is the point: it can only be
/// opened with [`Db::open_manual`], and the fact that it compiles at all is the
/// evidence that the thread bounds are on the constructor and not on the trait.
/// One file's bytes, shared between the directory and every open handle on it.
type Blob = Rc<RefCell<Vec<u8>>>;

#[derive(Clone, Default)]
struct MemFs {
    files: Rc<RefCell<BTreeMap<PathBuf, Blob>>>,
    locks: Rc<RefCell<BTreeMap<PathBuf, bool>>>,
}

struct MemFile {
    bytes: Blob,
}

impl File for MemFile {
    fn read_at(&self, off: u64, buf: &mut [u8]) -> io::Result<usize> {
        let bytes = self.bytes.borrow();
        let off = off as usize;
        if off >= bytes.len() {
            return Ok(0);
        }
        let n = buf.len().min(bytes.len() - off);
        buf[..n].copy_from_slice(&bytes[off..off + n]);
        Ok(n)
    }
    fn append(&mut self, buf: &[u8]) -> io::Result<()> {
        self.bytes.borrow_mut().extend_from_slice(buf);
        Ok(())
    }
    fn sync_as(&mut self, _mode: SyncMode) -> io::Result<()> {
        // Nothing to make durable: there is no device under this.
        Ok(())
    }
    fn size(&self) -> io::Result<u64> {
        Ok(self.bytes.borrow().len() as u64)
    }
    fn set_len(&mut self, len: u64) -> io::Result<()> {
        self.bytes.borrow_mut().resize(len as usize, 0);
        Ok(())
    }
}

impl Fs for MemFs {
    type File = MemFile;

    fn create_dir_all(&self, _dir: &Path) -> io::Result<()> {
        Ok(())
    }

    fn list(&self, dir: &Path) -> io::Result<Vec<PathBuf>> {
        Ok(self
            .files
            .borrow()
            .keys()
            .filter(|p| p.parent() == Some(dir))
            .cloned()
            .collect())
    }

    fn open(&self, path: &Path, mode: OpenMode) -> io::Result<MemFile> {
        let mut files = self.files.borrow_mut();
        let bytes = match mode {
            OpenMode::Read => files
                .get(path)
                .cloned()
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no such file"))?,
            OpenMode::Truncate => {
                let slot = files
                    .entry(path.to_path_buf())
                    .or_insert_with(|| Rc::new(RefCell::new(Vec::new())));
                slot.borrow_mut().clear();
                Rc::clone(slot)
            }
            OpenMode::Append | OpenMode::Lock => Rc::clone(
                files
                    .entry(path.to_path_buf())
                    .or_insert_with(|| Rc::new(RefCell::new(Vec::new()))),
            ),
        };
        if mode == OpenMode::Lock {
            let mut locks = self.locks.borrow_mut();
            if locks.get(path).copied().unwrap_or(false) {
                return Err(io::Error::new(io::ErrorKind::WouldBlock, "locked"));
            }
            locks.insert(path.to_path_buf(), true);
        }
        Ok(MemFile { bytes })
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        let mut files = self.files.borrow_mut();
        let bytes = files
            .remove(from)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no such file"))?;
        files.insert(to.to_path_buf(), bytes);
        Ok(())
    }

    fn remove(&self, path: &Path) -> io::Result<()> {
        self.files
            .borrow_mut()
            .remove(path)
            .map(|_| ())
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no such file"))
    }

    fn size(&self, path: &Path) -> io::Result<u64> {
        self.files
            .borrow()
            .get(path)
            .map(|b| b.borrow().len() as u64)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no such file"))
    }

    fn sync_dir(&self, _dir: &Path) -> io::Result<()> {
        Ok(())
    }
}

/// The engine runs with no real filesystem under it at all, and with a
/// filesystem that could not be shared between threads if it wanted to be.
#[test]
fn the_engine_runs_on_a_filesystem_that_is_neither_send_nor_sync() {
    let fs = MemFs::default();
    let dir = PathBuf::from("/db");
    let db = Db::open_manual(fs.clone(), &dir, manual_opts()).unwrap();

    for i in 0..300u32 {
        db.put(format!("k{i:04}").as_bytes(), format!("v{i}").as_bytes())
            .unwrap();
    }
    while db.maintain().unwrap() {}
    assert!(db.sstable_count() > 0, "nothing was ever flushed");

    for i in 0..300u32 {
        assert_eq!(
            db.get(format!("k{i:04}").as_bytes()).unwrap(),
            Some(format!("v{i}").into_bytes())
        );
    }

    // Reopen against the same in-memory filesystem: the durability story holds
    // even when the bytes never left the process.
    drop(db);
    fs.locks.borrow_mut().clear();
    let reopened = Db::open_manual(fs.clone(), &dir, manual_opts()).unwrap();
    for i in 0..300u32 {
        assert_eq!(
            reopened.get(format!("k{i:04}").as_bytes()).unwrap(),
            Some(format!("v{i}").into_bytes()),
            "key {i} did not survive a reopen"
        );
    }
}

// ------------------------------------------------------------ RefusingFs

/// Refuses to write anything to a path matching `refuse`, with `ENOSPC` —
/// the failure a storage engine most has to survive and least often does.
///
/// Targeted rather than budgeted, because "the twentieth write" is whichever
/// write the engine happens to make twentieth, and a test that moves when an
/// unrelated buffer size changes is a test nobody trusts.
struct RefusingFs {
    refuse: &'static str,
    /// Appends to let through before refusing. Opening a database writes a file
    /// header before it writes anything else, and a filesystem that refuses
    /// that refuses the open — which is correct, and not what these tests are
    /// aimed at.
    allow_first: usize,
}

struct MaybeFailingFile {
    inner: <StdFs as Fs>::File,
    refuse_writes: bool,
    allowance: usize,
}

impl File for MaybeFailingFile {
    fn read_at(&self, off: u64, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.read_at(off, buf)
    }
    fn append(&mut self, buf: &[u8]) -> io::Result<()> {
        if self.refuse_writes {
            if self.allowance == 0 {
                return Err(io::Error::new(io::ErrorKind::StorageFull, "no space left"));
            }
            self.allowance -= 1;
        }
        self.inner.append(buf)
    }
    fn sync_as(&mut self, mode: SyncMode) -> io::Result<()> {
        self.inner.sync_as(mode)
    }
    fn size(&self) -> io::Result<u64> {
        self.inner.size()
    }
    fn set_len(&mut self, len: u64) -> io::Result<()> {
        self.inner.set_len(len)
    }
}

impl Fs for RefusingFs {
    type File = MaybeFailingFile;

    fn create_dir_all(&self, dir: &Path) -> io::Result<()> {
        StdFs.create_dir_all(dir)
    }
    fn list(&self, dir: &Path) -> io::Result<Vec<PathBuf>> {
        StdFs.list(dir)
    }
    fn open(&self, path: &Path, mode: OpenMode) -> io::Result<MaybeFailingFile> {
        let refuse_writes = path.to_string_lossy().contains(self.refuse);
        Ok(MaybeFailingFile {
            inner: StdFs.open(path, mode)?,
            refuse_writes,
            allowance: self.allow_first,
        })
    }
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        StdFs.rename(from, to)
    }
    fn remove(&self, path: &Path) -> io::Result<()> {
        StdFs.remove(path)
    }
    fn size(&self, path: &Path) -> io::Result<u64> {
        StdFs.size(path)
    }
    fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        StdFs.sync_dir(dir)
    }
}

/// An injected `ENOSPC` on the SSTable a flush is building reaches the engine's
/// latch. What matters is not that the write fails — anything can fail a write —
/// but that the handle then refuses everything, rather than carrying on against
/// a MemTable whose contents are not going to survive a restart.
#[test]
fn an_injected_flush_failure_poisons_the_handle() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open_manual(
        RefusingFs {
            refuse: ".db.tmp",
            allow_first: 0,
        },
        dir.path(),
        manual_opts(),
    )
    .unwrap();

    for i in 0..200u32 {
        db.put(
            format!("k{i:04}").as_bytes(),
            b"a value long enough to matter",
        )
        .unwrap();
    }
    assert!(
        db.pending_work(),
        "no flush became due, so nothing was tested"
    );
    db.maintain()
        .expect_err("the SSTable could not be written and the flush reported success");

    assert!(
        db.health().is_err(),
        "a flush failed and the handle went on claiming to be healthy"
    );
    // And it stays refused, for reads as well as writes: answering a read from
    // a MemTable that will not survive a restart is a durability claim the
    // engine can no longer make.
    assert!(db.put(b"anything", b"at all").is_err());
    assert!(db.get(b"k0000").is_err());
    assert!(db.maintain().is_err());
}

/// A WAL append that fails is *not* a poisoning. The record never reached the
/// MemTable either — the append and the insert are one unit under the WAL lock
/// — so nothing was made visible that is not durable, and the caller can retry
/// or give up. Latching here would turn a full disk into a dead handle for a
/// write that never happened.
#[test]
fn a_refused_wal_append_fails_the_write_without_poisoning_the_handle() {
    let dir = tempfile::tempdir().unwrap();
    // One append through: the WAL's file header, which the open writes before
    // any record and without which there is no database to test.
    let db = Db::open_manual(
        RefusingFs {
            refuse: "wal.log",
            allow_first: 1,
        },
        dir.path(),
        manual_opts(),
    )
    .unwrap();

    db.put(b"k", b"v")
        .expect_err("the WAL refused the append and the write reported success");
    assert!(
        db.health().is_ok(),
        "a write that never became visible poisoned the handle"
    );
    assert_eq!(
        db.get(b"k").unwrap(),
        None,
        "a record whose WAL append failed is visible in the MemTable"
    );
}

// Compile-time evidence that the bounds are where the module docs say they are.
// `MemFs` is not `Send`, so this would not compile if `Fs` required it.
#[allow(dead_code)]
fn mem_fs_is_not_send() {
    fn assert_not_required<F: Fs>(_: F) {}
    assert_not_required(MemFs::default());
}

// ------------------------------------------------------ the reclamation hazard

/// What the file header exists to prevent, end to end.
///
/// The reclamation loop on open deletes every SSTable the manifest does not
/// name. A manifest that cannot be read is therefore not a failed open — it is
/// a *successful* one that takes the database with it. The engine now refuses
/// instead, and the assertion that matters is not the error: it is that the
/// SSTables are still on disk afterwards.
#[test]
fn an_unreadable_manifest_refuses_the_open_instead_of_deleting_the_database() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Db::open_with(dir.path(), manual_opts()).unwrap();
        for i in 0..400u32 {
            db.put(
                format!("k{i:04}").as_bytes(),
                b"a value long enough to matter",
            )
            .unwrap();
        }
        db.flush().unwrap();
        assert!(db.sstable_count() > 0, "nothing was flushed to test with");
    }

    let tables_before: Vec<PathBuf> = StdFs
        .list(dir.path())
        .unwrap()
        .into_iter()
        .filter(|p| p.to_string_lossy().ends_with(".db"))
        .collect();
    assert!(!tables_before.is_empty());

    // Make the live manifest unreadable, the way a foreign file or a format
    // this build does not understand would be.
    let current = std::fs::read_to_string(dir.path().join("CURRENT")).unwrap();
    let manifest = dir.path().join(current.trim());
    std::fs::write(&manifest, b"not a manifest, not even close").unwrap();

    let message = match Db::open_with(dir.path(), manual_opts()) {
        Ok(_) => panic!("an unreadable manifest opened successfully"),
        Err(e) => format!("{e}"),
    };
    assert!(
        message.contains("readable frame"),
        "the refusal did not say why: {message}"
    );

    let tables_after: Vec<PathBuf> = StdFs
        .list(dir.path())
        .unwrap()
        .into_iter()
        .filter(|p| p.to_string_lossy().ends_with(".db"))
        .collect();
    assert_eq!(
        tables_after, tables_before,
        "the refused open deleted SSTables on its way out"
    );
}
