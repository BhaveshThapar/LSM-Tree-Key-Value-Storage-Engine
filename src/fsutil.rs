//! The database directory lock.
//!
//! Directory fsync used to live here too; it is a [`Fs`] method now, because a
//! filesystem that is being injected has to be able to decide what a directory
//! fsync means.

use std::path::{Path, PathBuf};

use crate::error::{Error, Result};
use crate::fs::{Fs, OpenMode};

const LOCK_FILENAME: &str = "LOCK";

/// Path of the lock file within `dir`.
pub(crate) fn lock_path(dir: &Path) -> PathBuf {
    dir.join(LOCK_FILENAME)
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
pub(crate) struct DirLock<F: Fs> {
    /// The lock lives as long as this file — `flock` releases on last close.
    /// The field is never read; dropping it is the entire point.
    _file: F::File,
}

impl<F: Fs> DirLock<F> {
    /// Take the lock on `dir`, or fail immediately if another handle holds it.
    ///
    /// Deliberately non-blocking. Waiting would turn a misconfiguration — two
    /// processes pointed at one directory — into a hang, which is harder to
    /// diagnose than an error that names the directory.
    pub(crate) fn acquire(fs: &F, dir: &Path) -> Result<DirLock<F>> {
        let path = lock_path(dir);
        let existed = fs.exists(&path);

        let file = fs.open(&path, OpenMode::Lock).map_err(|e| {
            if e.kind() == std::io::ErrorKind::WouldBlock {
                Error::Locked(dir.to_path_buf())
            } else {
                Error::Io(e)
            }
        })?;

        // Only the first creation needs the directory entry made durable.
        if !existed {
            fs.sync_dir(dir)?;
        }
        Ok(DirLock { _file: file })
    }
}

// The lock file is never unlinked, not even on a clean drop. Unlinking races a
// process that has already opened it and is about to call `flock` on a
// descriptor whose file no longer has a name: it would take the lock on an
// orphan inode and let a second handle straight through. A stray empty file is
// a far smaller problem than a lock that silently stops working.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::StdFs;
    use tempfile::tempdir;

    #[test]
    fn a_second_lock_on_a_held_directory_is_refused() {
        let dir = tempdir().unwrap();
        let fs = StdFs;
        let first = DirLock::acquire(&fs, dir.path()).unwrap();

        match DirLock::<StdFs>::acquire(&fs, dir.path()) {
            Err(Error::Locked(p)) => assert_eq!(p, dir.path()),
            other => panic!("expected Locked, got {other:?}"),
        }

        drop(first);
    }

    #[test]
    fn dropping_the_handle_releases_the_lock() {
        let dir = tempdir().unwrap();
        let fs = StdFs;
        drop(DirLock::acquire(&fs, dir.path()).unwrap());

        DirLock::<StdFs>::acquire(&fs, dir.path()).expect("the lock should be free again");
    }

    #[test]
    fn the_lock_file_survives_a_release() {
        let dir = tempdir().unwrap();
        let fs = StdFs;
        drop(DirLock::acquire(&fs, dir.path()).unwrap());
        assert!(lock_path(dir.path()).exists());
    }

    #[test]
    fn syncing_a_directory_succeeds() {
        let dir = tempdir().unwrap();
        StdFs.sync_dir(dir.path()).unwrap();
    }
}
