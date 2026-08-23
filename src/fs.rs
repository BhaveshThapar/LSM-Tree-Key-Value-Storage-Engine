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
    /// Make everything written so far durable.
    fn sync(&mut self) -> io::Result<()>;
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
            self.file.append(&self.buf)?;
            self.buf.clear();
        }
        Ok(())
    }

    /// Flush, then make it durable.
    pub fn sync(&mut self) -> io::Result<()> {
        self.flush()?;
        self.file.sync()
    }

    pub fn get_ref(&self) -> &F {
        &self.file
    }

    pub fn get_mut(&mut self) -> &mut F {
        &mut self.file
    }

    /// Flush and hand the file back.
    pub fn into_inner(mut self) -> io::Result<F> {
        self.flush()?;
        Ok(self.file)
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

    fn sync(&mut self) -> io::Result<()> {
        self.file.sync_all()
    }

    fn size(&self) -> io::Result<u64> {
        Ok(self.file.metadata()?.len())
    }

    fn set_len(&mut self, len: u64) -> io::Result<()> {
        self.file.set_len(len)
    }
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
