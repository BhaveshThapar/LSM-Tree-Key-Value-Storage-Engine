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
mod db;
mod error;
mod memtable;
mod record;
mod sstable;
mod wal;

pub use db::{Db, Options};
pub use error::{Error, Result};
