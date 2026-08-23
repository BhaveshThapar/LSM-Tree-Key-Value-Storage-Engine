//! `lsm_kv` — a log-structured merge-tree key/value storage engine.
//!
//! Writes are appended to a [write-ahead log](wal) for durability, buffered in
//! an in-memory [`MemTable`](memtable::MemTable), and periodically flushed to
//! immutable, sorted [SSTables](sstable). Reads consult the MemTable first,
//! then SSTables newest-to-oldest.
//!
//! ```no_run
//! use lsm_kv::Db;
//! let mut db = Db::open("./data")?;
//! db.put(b"hello", b"world")?;
//! assert_eq!(db.get(b"hello")?, Some(b"world".to_vec()));
//! db.delete(b"hello")?;
//! # Ok::<(), lsm_kv::Error>(())
//! ```

mod bloom;
mod compaction;
mod compactor;
mod db;
mod error;
mod fsutil;
mod manifest;
mod memtable;
mod record;
mod sstable;
mod wal;

pub use db::{Db, Options, Snapshot};

/// Internal decoders exposed for fuzzing only (`--features fuzzing`).
///
/// Every entry point here must reject arbitrary bytes with an `Err` rather
/// than panicking; the `fuzz/` crate asserts exactly that.
#[cfg(feature = "fuzzing")]
pub mod fuzzing {
    use crate::Result;
    use std::path::Path;

    /// Decode a single [`Record`](crate::record::Record) from raw bytes.
    pub fn decode_record(bytes: &[u8]) -> Result<()> {
        let mut pos = 0;
        crate::record::Record::decode_at(bytes, &mut pos).map(|_| ())
    }

    /// Replay a WAL file holding arbitrary bytes.
    pub fn replay_wal(path: &Path) -> Result<()> {
        crate::wal::Wal::replay(path).map(|_| ())
    }

    /// Open an SSTable file holding arbitrary bytes.
    pub fn open_sstable(path: &Path) -> Result<()> {
        crate::sstable::SsTableReader::open(path, true).map(|_| ())
    }
}
pub use error::{Error, Result};
