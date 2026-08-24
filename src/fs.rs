//! The seam between the engine and the filesystem.
//!
//! Two traits. [`StdFs`] is the production implementation and does exactly what
//! the engine did before this module existed. The reason for the seam is
//! everything else: a fault model that decides, per crash, which bytes reached
//! the device; a test that wants `ENOSPC` on the third write of a compaction; a
//! deterministic harness that has to replay a run from a seed and therefore
//! cannot have a real disk in it.
//!
//! **Neither trait requires `Send` or `Sync`.** `StdFs` is both, so
//! `Db<StdFs>` — which is what `Db` means when nothing says otherwise — is
//! still `Send + Sync` and still spawns its background threads. A single
//! threaded harness supplying an `Rc<RefCell<_>>`-backed filesystem gets a `Db`
//! that is neither, which is correct: it is running on one thread and putting
//! atomics in the one component that has to reproduce exactly is the last thing
//! anybody wants. The bound lives on the constructor that spawns threads
//! rather than on the trait, so each caller pays only for what it uses.
//!
//! There is no seek cursor on [`File`]. Every read is addressed, because a
//! shared cursor between a reader and a compaction is a hazard the seam does not
//! need to expose; the one sequential writer keeps its own position by
//! appending.

use std::io;
use std::path::{Path, PathBuf};

/// How durable a sync has to be.
///
/// This is an enum rather than a call to [`std::fs::File::sync_all`] because
/// that function is not the same operation on every platform. On Linux it is
/// `fsync`; on macOS it is also `fsync`, and macOS `fsync` does **not** flush
/// the drive's write cache. It is not a durability primitive there, and a
/// benchmark run against it is measuring something other than what it claims.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SyncMode {
    /// `fsync` on Linux, `F_FULLFSYNC` on macOS. Survives power loss, and the
    /// only mode a durability claim may be made under.
    #[default]
    Durable,
    /// `fsync` everywhere: ordering, not power-loss durability. Exists so that
    /// development on a Mac is not unusably slow, and so an operator who has
    /// decided their battery-backed controller makes the distinction moot can
    /// say so.
    Barrier,
    /// No sync at all. Tests, and benchmarks that are labelled as unsafe.
    None,
}

impl SyncMode {
    /// Whether a durability claim may be made under this mode.
    pub fn is_durable(self) -> bool {
        matches!(self, SyncMode::Durable)
    }
}

/// What a file is being opened for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenMode {
    /// Read only. Fails if absent.
    Read,
    /// Append, creating if absent. Existing contents are preserved.
    Append,
    /// Write, creating if absent, truncating if present.
    Truncate,
    /// Take an exclusive, non-blocking advisory lock, creating the file if
    /// absent. Fails with [`io::ErrorKind::WouldBlock`] if held elsewhere.
    Lock,
}

/// The filesystem operations the engine needs.
pub trait Fs: 'static {
    type File: File;

    fn create_dir_all(&self, dir: &Path) -> io::Result<()>;
    /// Every entry in `dir`, as full paths. Order is unspecified — the engine
    /// sorts what it needs sorted, so an implementation is free to be as
    /// arbitrary as a real readdir is.
    fn list(&self, dir: &Path) -> io::Result<Vec<PathBuf>>;
    fn open(&self, path: &Path, mode: OpenMode) -> io::Result<Self::File>;
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()>;
    /// Make `link` another name for the file at `original`.
    ///
    /// A hard link rather than a copy, and that is the whole reason a checkpoint
    /// of a gigabyte costs milliseconds: the bytes are not moved, a second name
    /// is added to them. It also means the source may go on to delete its own
    /// name — a compaction does exactly that — and the checkpoint's name keeps
    /// the data alive, because a file's contents outlive its last link and not
    /// its first.
    ///
    /// Fails if `link` exists. A checkpoint that silently replaced a file it did
    /// not expect to be there would be a checkpoint of two different states.
    fn hard_link(&self, original: &Path, link: &Path) -> io::Result<()>;
    fn remove(&self, path: &Path) -> io::Result<()>;
    fn size(&self, path: &Path) -> io::Result<u64>;
    /// fsync a directory, so a create or a rename within it is durable.
    ///
    /// Renaming a file makes the *directory entry* the thing that has to
    /// survive a crash, and fsyncing the file itself says nothing about it.
    fn sync_dir(&self, dir: &Path) -> io::Result<()>;

    fn exists(&self, path: &Path) -> bool {
        self.size(path).is_ok()
    }
}

/// One open file, addressed by offset.
pub trait File: 'static {
    /// Read into `buf` starting at `off`, returning how much was read.
    fn read_at(&self, off: u64, buf: &mut [u8]) -> io::Result<usize>;
    /// Append to the end of the file.
    fn append(&mut self, buf: &[u8]) -> io::Result<()>;
    /// Make everything written so far durable, in the strongest sense the
    /// platform offers.
    fn sync(&mut self) -> io::Result<()> {
        self.sync_as(SyncMode::Durable)
    }

    /// Make everything written so far durable to the degree `mode` asks for.
    fn sync_as(&mut self, mode: SyncMode) -> io::Result<()>;
    fn size(&self) -> io::Result<u64>;
    fn set_len(&mut self, len: u64) -> io::Result<()>;

    /// Read exactly `buf.len()` bytes at `off`, or fail.
    ///
    /// Provided, because every implementation would write the same loop and one
    /// of them would get the short-read case wrong. A short read that is not at
    /// end of file is not an error and must be retried; a read that returns
    /// zero before the buffer is full is.
    fn read_exact_at(&self, off: u64, buf: &mut [u8]) -> io::Result<()> {
        let mut done = 0;
        while done < buf.len() {
            match self.read_at(off + done as u64, &mut buf[done..]) {
                Ok(0) => {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "file ended before the requested bytes",
                    ));
                }
                Ok(n) => done += n,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    /// The whole file.
    fn read_all(&self) -> io::Result<Vec<u8>> {
        let len = self.size()?;
        let mut buf = vec![0u8; len as usize];
        self.read_exact_at(0, &mut buf)?;
        Ok(buf)
    }
}

/// Appends through a buffer, so a caller writing a record at a time does not
/// make a syscall per record.
///
/// The engine used `BufWriter` for this. `BufWriter` needs `io::Write`, which a
/// seam addressed by offset deliberately does not provide, so the buffering is
/// here instead — and it is explicit about when it flushes, which `BufWriter`
/// is not.
pub struct BufAppend<F: File> {
    file: F,
    buf: Vec<u8>,
    cap: usize,
    /// Set when a flush failed. Bytes left buffered after that are bytes the
    /// filesystem refused, not bytes somebody forgot about, and the check in
    /// `Drop` has to be able to tell those apart.
    write_failed: bool,
}

/// Sixty-four kilobytes: large enough that a record-at-a-time writer makes one
/// syscall per hundreds of records, small enough that a compaction merging
/// gigabytes never holds more than this beyond what it is merging.
pub const APPEND_BUFFER_BYTES: usize = 64 * 1024;

impl<F: File> BufAppend<F> {
    pub fn new(file: F) -> Self {
        Self::with_capacity(file, APPEND_BUFFER_BYTES)
    }

    pub fn with_capacity(file: F, cap: usize) -> Self {
        Self {
            file,
            buf: Vec::with_capacity(cap),
            cap: cap.max(1),
            write_failed: false,
        }
    }

    pub fn write(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.buf.extend_from_slice(bytes);
        if self.buf.len() >= self.cap {
            self.flush()?;
        }
        Ok(())
    }

    /// Hand everything buffered to the filesystem. Not a durability barrier;
    /// see [`BufAppend::sync`].
    pub fn flush(&mut self) -> io::Result<()> {
        if !self.buf.is_empty() {
            if let Err(e) = self.file.append(&self.buf) {
                self.write_failed = true;
                return Err(e);
            }
            self.buf.clear();
        }
        Ok(())
    }

    /// Flush, then make it durable.
    pub fn sync(&mut self) -> io::Result<()> {
        self.flush()?;
        match self.file.sync() {
            Ok(()) => Ok(()),
            Err(e) => {
                self.write_failed = true;
                Err(e)
            }
        }
    }

    pub fn get_mut(&mut self) -> &mut F {
        &mut self.file
    }
}

/// Dropping with bytes still buffered loses them silently, which is the one
/// failure mode a buffer in front of a durability path must not have. There is
/// nowhere to report an error from a destructor, so this does not try to write
/// them — it says so in a debug build and lets a release build lose them, which
/// is what `BufWriter` does and is at least not worse.
///
/// Bytes left behind by a *failed* flush are a different thing: the filesystem
/// refused them, nobody forgot them, and there was never anywhere for them to
/// go. Those are not reported.
impl<F: File> Drop for BufAppend<F> {
    fn drop(&mut self) {
        debug_assert!(
            self.buf.is_empty() || self.write_failed,
            "{} buffered bytes were dropped without a flush",
            self.buf.len()
        );
    }
}

// --------------------------------------------------------------- production

/// The real filesystem.
#[derive(Debug, Clone, Copy, Default)]
pub struct StdFs;

impl Fs for StdFs {
    type File = StdFile;

    fn create_dir_all(&self, dir: &Path) -> io::Result<()> {
        std::fs::create_dir_all(dir)
    }

    fn list(&self, dir: &Path) -> io::Result<Vec<PathBuf>> {
        let mut out = Vec::new();
        for entry in std::fs::read_dir(dir)? {
            out.push(entry?.path());
        }
        Ok(out)
    }

    fn open(&self, path: &Path, mode: OpenMode) -> io::Result<StdFile> {
        let mut opts = std::fs::OpenOptions::new();
        match mode {
            OpenMode::Read => {
                opts.read(true);
            }
            OpenMode::Append => {
                opts.read(true).append(true).create(true);
            }
            OpenMode::Truncate => {
                opts.read(true).write(true).create(true).truncate(true);
            }
            OpenMode::Lock => {
                // Never truncate: the lock file's existence is the whole of its
                // content, and truncating one another process is about to flock
                // would be a write to a file this handle does not own yet.
                opts.read(true).write(true).create(true).truncate(false);
            }
        }
        let file = opts.open(path)?;
        if mode == OpenMode::Lock {
            lock_exclusive(&file)?;
        }
        Ok(StdFile { file })
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        std::fs::rename(from, to)
    }

    fn hard_link(&self, original: &Path, link: &Path) -> io::Result<()> {
        std::fs::hard_link(original, link)
    }

    fn remove(&self, path: &Path) -> io::Result<()> {
        std::fs::remove_file(path)
    }

    fn size(&self, path: &Path) -> io::Result<u64> {
        Ok(std::fs::metadata(path)?.len())
    }

    fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        std::fs::File::open(dir)?.sync_all()
    }
}

/// A file on the real filesystem.
#[derive(Debug)]
pub struct StdFile {
    file: std::fs::File,
}

impl File for StdFile {
    fn read_at(&self, off: u64, buf: &mut [u8]) -> io::Result<usize> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            self.file.read_at(buf, off)
        }
        #[cfg(not(unix))]
        {
            let _ = (off, buf);
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "positional reads are implemented for Unix only",
            ))
        }
    }

    fn append(&mut self, buf: &[u8]) -> io::Result<()> {
        use std::io::Write;
        // Opened with `append(true)` where it matters, so the kernel places
        // this at the end atomically. `Truncate` mode starts empty and only
        // ever grows, so writing at the cursor is the same thing.
        self.file.write_all(buf)
    }

    fn sync_as(&mut self, mode: SyncMode) -> io::Result<()> {
        match mode {
            SyncMode::None => Ok(()),
            SyncMode::Barrier => self.file.sync_all(),
            SyncMode::Durable => full_sync(&self.file),
        }
    }

    fn size(&self) -> io::Result<u64> {
        Ok(self.file.metadata()?.len())
    }

    fn set_len(&mut self, len: u64) -> io::Result<()> {
        self.file.set_len(len)
    }
}

/// The strongest durability the platform offers.
///
/// On macOS that is `F_FULLFSYNC`, which asks the drive to flush its write
/// cache; plain `fsync` there returns once the data has reached the drive's
/// cache and says nothing about power loss. On Linux `fsync` already is that,
/// so there is nothing stronger to ask for.
#[cfg(target_os = "macos")]
fn full_sync(file: &std::fs::File) -> io::Result<()> {
    use std::os::unix::io::AsRawFd;
    // SAFETY: `fcntl` with F_FULLFSYNC takes an open descriptor and no further
    // arguments. The descriptor is open for as long as `file` is borrowed.
    let rc = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_FULLFSYNC) };
    if rc == -1 {
        // Some filesystems — network mounts especially — do not implement it.
        // Falling back is honest as long as it is only on the platforms where
        // there is nothing else to try.
        let err = io::Error::last_os_error();
        if err.raw_os_error() == Some(libc::ENOTSUP) || err.raw_os_error() == Some(libc::EINVAL) {
            return file.sync_all();
        }
        return Err(err);
    }
    Ok(())
}

#[cfg(not(target_os = "macos"))]
fn full_sync(file: &std::fs::File) -> io::Result<()> {
    file.sync_all()
}

#[cfg(unix)]
fn lock_exclusive(file: &std::fs::File) -> io::Result<()> {
    use std::os::unix::io::AsRawFd;
    // SAFETY: `flock` takes an open file descriptor and a flag word. The
    // descriptor is open for as long as `file` is borrowed.
    let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if rc == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(not(unix))]
fn lock_exclusive(_file: &std::fs::File) -> io::Result<()> {
    // Better to refuse than to hand back a lock that does not lock.
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "directory locking is implemented for Unix only",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dir() -> tempfile::TempDir {
        tempfile::tempdir().unwrap()
    }

    #[test]
    fn a_file_round_trips_through_the_seam() {
        let d = dir();
        let path = d.path().join("f");
        let fs = StdFs;
        {
            let mut f = fs.open(&path, OpenMode::Truncate).unwrap();
            f.append(b"hello ").unwrap();
            f.append(b"world").unwrap();
            f.sync().unwrap();
        }
        let f = fs.open(&path, OpenMode::Read).unwrap();
        assert_eq!(f.size().unwrap(), 11);
        assert_eq!(f.read_all().unwrap(), b"hello world");
        let mut buf = [0u8; 5];
        f.read_exact_at(6, &mut buf).unwrap();
        assert_eq!(&buf, b"world");
    }

    #[test]
    fn append_preserves_what_was_there() {
        let d = dir();
        let path = d.path().join("f");
        let fs = StdFs;
        fs.open(&path, OpenMode::Truncate)
            .unwrap()
            .append(b"first")
            .unwrap();
        fs.open(&path, OpenMode::Append)
            .unwrap()
            .append(b"second")
            .unwrap();
        assert_eq!(
            fs.open(&path, OpenMode::Read).unwrap().read_all().unwrap(),
            b"firstsecond"
        );
    }

    #[test]
    fn truncate_does_not_preserve_what_was_there() {
        let d = dir();
        let path = d.path().join("f");
        let fs = StdFs;
        fs.open(&path, OpenMode::Truncate)
            .unwrap()
            .append(b"first")
            .unwrap();
        fs.open(&path, OpenMode::Truncate)
            .unwrap()
            .append(b"x")
            .unwrap();
        assert_eq!(
            fs.open(&path, OpenMode::Read).unwrap().read_all().unwrap(),
            b"x"
        );
    }

    #[test]
    fn reading_past_the_end_is_an_error_rather_than_a_short_answer() {
        let d = dir();
        let path = d.path().join("f");
        let fs = StdFs;
        fs.open(&path, OpenMode::Truncate)
            .unwrap()
            .append(b"abc")
            .unwrap();
        let f = fs.open(&path, OpenMode::Read).unwrap();
        let mut buf = [0u8; 8];
        assert_eq!(
            f.read_exact_at(0, &mut buf).unwrap_err().kind(),
            io::ErrorKind::UnexpectedEof
        );
    }

    #[test]
    fn a_second_lock_is_refused_without_waiting() {
        let d = dir();
        let path = d.path().join("LOCK");
        let fs = StdFs;
        let first = fs.open(&path, OpenMode::Lock).unwrap();
        assert_eq!(
            fs.open(&path, OpenMode::Lock).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        drop(first);
        fs.open(&path, OpenMode::Lock)
            .expect("the lock should be free again");
    }

    #[test]
    fn listing_finds_what_was_created() {
        let d = dir();
        let fs = StdFs;
        for name in ["a", "b", "c"] {
            fs.open(&d.path().join(name), OpenMode::Truncate).unwrap();
        }
        let mut names: Vec<String> = fs
            .list(d.path())
            .unwrap()
            .iter()
            .map(|p| p.file_name().unwrap().to_string_lossy().into_owned())
            .collect();
        names.sort();
        assert_eq!(names, ["a", "b", "c"]);
    }

    /// The property a checkpoint is built on: a second name keeps the bytes
    /// alive after the first one is removed.
    #[test]
    fn a_hard_link_outlives_the_name_it_was_made_from() {
        let d = dir();
        let fs = StdFs;
        let (original, link) = (d.path().join("original"), d.path().join("link"));
        fs.open(&original, OpenMode::Truncate)
            .unwrap()
            .append(b"the bytes")
            .unwrap();

        fs.hard_link(&original, &link).unwrap();
        fs.remove(&original).unwrap();

        assert!(!fs.exists(&original));
        assert_eq!(
            fs.open(&link, OpenMode::Read).unwrap().read_all().unwrap(),
            b"the bytes",
            "removing the original took the data with it"
        );
    }

    #[test]
    fn a_hard_link_onto_an_existing_name_is_refused() {
        let d = dir();
        let fs = StdFs;
        let (a, b) = (d.path().join("a"), d.path().join("b"));
        fs.open(&a, OpenMode::Truncate).unwrap();
        fs.open(&b, OpenMode::Truncate).unwrap();
        assert!(
            fs.hard_link(&a, &b).is_err(),
            "linking onto an existing name would make a checkpoint of two states"
        );
    }

    #[test]
    fn rename_moves_the_contents_and_removes_the_source() {
        let d = dir();
        let fs = StdFs;
        let (from, to) = (d.path().join("from"), d.path().join("to"));
        fs.open(&from, OpenMode::Truncate)
            .unwrap()
            .append(b"payload")
            .unwrap();
        fs.rename(&from, &to).unwrap();
        assert!(!fs.exists(&from));
        assert_eq!(
            fs.open(&to, OpenMode::Read).unwrap().read_all().unwrap(),
            b"payload"
        );
    }

    /// The buffer must not change what ends up in the file, whatever size it is
    /// relative to the writes going through it.
    #[test]
    fn buffered_appends_produce_the_same_file_as_direct_ones() {
        let d = dir();
        let fs = StdFs;
        let pieces: Vec<Vec<u8>> = (0..64u8).map(|i| vec![i; (i as usize % 17) + 1]).collect();
        let expected: Vec<u8> = pieces.concat();

        for cap in [1usize, 7, 64, 4096] {
            let path = d.path().join(format!("buffered-{cap}"));
            let mut w = BufAppend::with_capacity(fs.open(&path, OpenMode::Truncate).unwrap(), cap);
            for piece in &pieces {
                w.write(piece).unwrap();
            }
            w.sync().unwrap();
            drop(w);
            assert_eq!(
                fs.open(&path, OpenMode::Read).unwrap().read_all().unwrap(),
                expected,
                "a buffer of {cap} bytes changed the file"
            );
        }
    }

    #[test]
    fn an_unflushed_buffer_has_not_reached_the_file() {
        let d = dir();
        let fs = StdFs;
        let path = d.path().join("held");
        let mut w = BufAppend::with_capacity(fs.open(&path, OpenMode::Truncate).unwrap(), 1024);
        w.write(b"not yet").unwrap();
        assert_eq!(fs.size(&path).unwrap(), 0, "the buffer wrote through");
        w.flush().unwrap();
        assert_eq!(fs.size(&path).unwrap(), 7);
    }
}
