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
}

pub type Result<T> = std::result::Result<T, Error>;
