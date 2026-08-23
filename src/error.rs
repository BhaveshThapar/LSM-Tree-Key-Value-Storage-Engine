use std::path::PathBuf;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("corrupt data: {0}")]
    Corrupt(String),

    #[error("unsupported sstable format: {0}")]
    BadFormat(String),

    #[error("database directory is already open by another handle: {0}")]
    Locked(PathBuf),

    /// A flush or a compaction failed. The engine does not recover in-process:
    /// a failed flush leaves the frozen MemTable stranded and the WAL
    /// un-rewritten, so every later write is building on a state that will not
    /// survive a restart. Reopening the directory is the recovery path.
    #[error("engine poisoned by an earlier failure: {0}")]
    Poisoned(String),
}

pub type Result<T> = std::result::Result<T, Error>;
