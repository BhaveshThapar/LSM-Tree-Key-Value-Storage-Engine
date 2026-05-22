use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("corrupt data: {0}")]
    Corrupt(String),

    #[error("unsupported sstable format: {0}")]
    BadFormat(String),
}

pub type Result<T> = std::result::Result<T, Error>;
