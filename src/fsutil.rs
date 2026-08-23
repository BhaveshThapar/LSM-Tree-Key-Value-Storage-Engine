//! Filesystem operations the engine depends on for crash safety and that `std`
//! does not offer: fsyncing a directory, and an exclusive lock on one.
//!
//! Both are Unix-only, as the rest of the engine's durability story already is
//! — `File::open` on a directory does not work on Windows, so `sync_dir` could
//! never have been portable.

use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};

use crate::error::{Error, Result};

const LOCK_FILENAME: &str = "LOCK";

/// Path of the lock file within `dir`.
pub(crate) fn lock_path(dir: &Path) -> PathBuf {
    dir.join(LOCK_FILENAME)
}

/// fsync a directory, so a rename or a create within it is durable.
///
/// Renaming a file makes the *directory entry* the thing that has to survive a
/// crash, and fsyncing the file itself says nothing about it. Every rename in
/// this engine publishes something the manifest is about to name, so losing the
/// entry while keeping the manifest edit leaves a database that will not open.
pub(crate) fn sync_dir(dir: &Path) -> Result<()> {
    File::open(dir)?.sync_all()?;
    Ok(())
}

/// An exclusive advisory lock on a database directory, held for as long as the
/// [`Db`](crate::Db) handle that took it.
///
/// Opening a directory is not a read-only act: the manifest rolls its
/// generation forward and deletes the previous one, and open reclaims every
/// SSTable the manifest does not name. Two handles on one directory therefore
/// delete each other's SSTables and each other's manifest generation. There is
/// no benign version of that race, so it is refused rather than serialized.
#[derive(Debug)]
pub(crate) struct DirLock {
    /// The lock lives as long as this descriptor — `flock` releases on last
    /// close. The field is never read; dropping it is the entire point.
    _file: File,
}

impl DirLock {
    /// Take the lock on `dir`, or fail immediately if another handle holds it.
    ///
    /// Deliberately non-blocking. Waiting would turn a misconfiguration — two
    /// processes pointed at one directory — into a hang, which is harder to
    /// diagnose than an error that names the directory.
    pub(crate) fn acquire(dir: &Path) -> Result<DirLock> {
        let path = lock_path(dir);
        let existed = path.exists();
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;

        lock_exclusive(&file).map_err(|e| {
            if e.kind() == std::io::ErrorKind::WouldBlock {
                Error::Locked(dir.to_path_buf())
            } else {
                Error::Io(e)
            }
        })?;

        // Only the first creation needs the directory entry made durable.
        if !existed {
            sync_dir(dir)?;
        }
        Ok(DirLock { _file: file })
    }
}

// The lock file is never unlinked, not even on a clean drop. Unlinking races a
// process that has already opened it and is about to call `flock` on a
// descriptor whose file no longer has a name: it would take the lock on an
// orphan inode and let a second handle straight through. A stray empty file is
// a far smaller problem than a lock that silently stops working.

#[cfg(unix)]
fn lock_exclusive(file: &File) -> std::io::Result<()> {
    use std::os::unix::io::AsRawFd;
    // SAFETY: `flock` takes an open file descriptor and a flag word. The
    // descriptor is open for as long as `file` is borrowed.
    let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if rc == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(not(unix))]
fn lock_exclusive(_file: &File) -> std::io::Result<()> {
    // Better to refuse than to hand back a lock that does not lock.
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "directory locking is implemented for Unix only",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn a_second_lock_on_a_held_directory_is_refused() {
        let dir = tempdir().unwrap();
        let first = DirLock::acquire(dir.path()).unwrap();

        match DirLock::acquire(dir.path()) {
            Err(Error::Locked(p)) => assert_eq!(p, dir.path()),
            other => panic!("expected Locked, got {other:?}"),
        }

        drop(first);
    }

    #[test]
    fn dropping_the_handle_releases_the_lock() {
        let dir = tempdir().unwrap();
        drop(DirLock::acquire(dir.path()).unwrap());

        DirLock::acquire(dir.path()).expect("the lock should be free again");
    }

    #[test]
    fn the_lock_file_survives_a_release() {
        let dir = tempdir().unwrap();
        drop(DirLock::acquire(dir.path()).unwrap());
        assert!(lock_path(dir.path()).exists());
    }

    #[test]
    fn syncing_a_directory_succeeds() {
        let dir = tempdir().unwrap();
        sync_dir(dir.path()).unwrap();
    }
}
