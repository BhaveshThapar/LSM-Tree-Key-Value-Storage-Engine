//! A batch is atomic because of the log format, and cheap because of the fsync
//! count. Both are asserted here rather than described.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use lsm_kv::{Db, File, Fs, Maintenance, OpenMode, Options, StdFs, SyncMode, WriteBatch};

fn opts(sync: SyncMode) -> Options {
    Options {
        memtable_threshold: 1 << 20,
        sync_wal: sync,
        maintenance: Maintenance::Manual,
        ..Options::default()
    }
}

// -------------------------------------------------------------- counting fsyncs

#[derive(Default)]
struct SyncCounts {
    durable: AtomicUsize,
}

struct CountingFs {
    counts: Arc<SyncCounts>,
}

struct CountingFile {
    inner: <StdFs as Fs>::File,
    counts: Arc<SyncCounts>,
    counted: bool,
}

impl File for CountingFile {
    fn read_at(&self, off: u64, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.read_at(off, buf)
    }
    fn append(&mut self, buf: &[u8]) -> io::Result<()> {
        self.inner.append(buf)
    }
    fn sync_as(&mut self, mode: SyncMode) -> io::Result<()> {
        if self.counted && mode.is_durable() {
            self.counts.durable.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.sync_as(mode)
    }
    fn size(&self) -> io::Result<u64> {
        self.inner.size()
    }
    fn set_len(&mut self, len: u64) -> io::Result<()> {
        self.inner.set_len(len)
    }
}

impl Fs for CountingFs {
    type File = CountingFile;

    fn create_dir_all(&self, dir: &Path) -> io::Result<()> {
        StdFs.create_dir_all(dir)
    }
    fn list(&self, dir: &Path) -> io::Result<Vec<PathBuf>> {
        StdFs.list(dir)
    }
    fn open(&self, path: &Path, mode: OpenMode) -> io::Result<CountingFile> {
        Ok(CountingFile {
            inner: StdFs.open(path, mode)?,
            counts: Arc::clone(&self.counts),
            // Only the live write-ahead log. A flush syncs other files for
            // reasons that have nothing to do with how writes were batched.
            counted: path.to_string_lossy().ends_with("wal.log"),
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

/// The claim the batch API exists to make: a hundred keys cost one fsync, and
/// the same hundred written separately cost a hundred.
#[test]
fn a_batch_costs_one_fsync_and_the_same_keys_written_singly_cost_one_each() {
    let batched = {
        let dir = tempfile::tempdir().unwrap();
        let counts = Arc::new(SyncCounts::default());
        let db = Db::open_on(
            CountingFs {
                counts: Arc::clone(&counts),
            },
            dir.path(),
            opts(SyncMode::Durable),
        )
        .unwrap();
        let before = counts.durable.load(Ordering::SeqCst);
        let mut batch = WriteBatch::new();
        for i in 0..100u32 {
            batch.put(format!("k{i:04}").as_bytes(), b"v");
        }
        db.write_batch(&batch).unwrap();
        counts.durable.load(Ordering::SeqCst) - before
    };

    let singly = {
        let dir = tempfile::tempdir().unwrap();
        let counts = Arc::new(SyncCounts::default());
        let db = Db::open_on(
            CountingFs {
                counts: Arc::clone(&counts),
            },
            dir.path(),
            opts(SyncMode::Durable),
        )
        .unwrap();
        let before = counts.durable.load(Ordering::SeqCst);
        for i in 0..100u32 {
            db.put(format!("k{i:04}").as_bytes(), b"v").unwrap();
        }
        counts.durable.load(Ordering::SeqCst) - before
    };

    assert_eq!(
        batched, 1,
        "a batch of a hundred keys cost {batched} fsyncs"
    );
    assert_eq!(
        singly, 100,
        "a hundred separate writes cost {singly} fsyncs, so the comparison means nothing"
    );
}

// ----------------------------------------------------------------- atomicity

/// A batch cut short by a crash is discarded whole. Every prefix of the batch's
/// frame is tried, so this is not one interleaving that happened to work.
#[test]
fn every_truncation_of_a_batch_leaves_all_of_it_or_none_of_it() {
    let dir = tempfile::tempdir().unwrap();
    let wal = dir.path().join("wal.log");

    // One batch, into a fresh database, so the file holds a header and one
    // frame and nothing else.
    {
        let db = Db::open_with(dir.path(), opts(SyncMode::None)).unwrap();
        let mut batch = WriteBatch::new();
        for i in 0..8u32 {
            batch.put(format!("k{i}").as_bytes(), format!("v{i}").as_bytes());
        }
        db.write_batch(&batch).unwrap();
    }
    let whole = std::fs::read(&wal).unwrap();
    assert!(whole.len() > 16, "the batch did not reach the file");

    for cut in 0..whole.len() {
        std::fs::write(&wal, &whole[..cut]).unwrap();
        let db = Db::open_with(dir.path(), opts(SyncMode::None)).unwrap();
        let present = (0..8u32)
            .filter(|i| db.get(format!("k{i}").as_bytes()).unwrap().is_some())
            .count();
        drop(db);
        assert!(
            present == 0 || present == 8,
            "a WAL truncated to {cut} bytes replayed {present} of 8 records, \
             so the batch was not atomic"
        );
    }

    // And the untruncated file really does replay all of it, or the loop above
    // was only ever observing zero.
    std::fs::write(&wal, &whole).unwrap();
    let db = Db::open_with(dir.path(), opts(SyncMode::None)).unwrap();
    for i in 0..8u32 {
        assert_eq!(
            db.get(format!("k{i}").as_bytes()).unwrap(),
            Some(format!("v{i}").into_bytes())
        );
    }
}

/// Within one batch, the later write to a key wins — the same as two separate
/// calls in the same order.
#[test]
fn a_later_write_in_a_batch_shadows_an_earlier_one() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open_with(dir.path(), opts(SyncMode::None)).unwrap();

    let mut batch = WriteBatch::new();
    batch.put(b"k", b"first");
    batch.put(b"k", b"second");
    batch.put(b"gone", b"here");
    batch.delete(b"gone");
    db.write_batch(&batch).unwrap();

    assert_eq!(db.get(b"k").unwrap(), Some(b"second".to_vec()));
    assert_eq!(db.get(b"gone").unwrap(), None);
}

#[test]
fn an_empty_batch_is_a_no_op() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open_with(dir.path(), opts(SyncMode::None)).unwrap();
    db.write_batch(&WriteBatch::new()).unwrap();
    assert_eq!(db.get(b"anything").unwrap(), None);
}

/// A batch survives a flush and a reopen like any other write.
#[test]
fn a_batch_survives_a_flush_and_a_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Db::open_with(dir.path(), opts(SyncMode::Durable)).unwrap();
        let mut batch = WriteBatch::new();
        for i in 0..500u32 {
            batch.put(format!("k{i:04}").as_bytes(), format!("v{i}").as_bytes());
        }
        db.write_batch(&batch).unwrap();
        db.flush().unwrap();
    }
    let db = Db::open_with(dir.path(), opts(SyncMode::Durable)).unwrap();
    for i in 0..500u32 {
        assert_eq!(
            db.get(format!("k{i:04}").as_bytes()).unwrap(),
            Some(format!("v{i}").into_bytes()),
            "key {i} did not survive"
        );
    }
}
