//! Write-ahead log: every mutation is appended here and durably flushed
//! before it reaches the MemTable, so a crash never loses an acked write.
//!
//! Frame layout: `[crc32:4][payload_len:4][payload]`, where `payload` is a
//! [`Record`] encoding and the CRC is computed over `payload` only.

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use crate::error::{Error, Result};
use crate::record::Record;

const FRAME_HEADER: usize = 8; // crc32 + payload_len

/// An append-only write-ahead log bound to a single active MemTable.
pub struct Wal {
    file: BufWriter<File>,
    path: PathBuf,
    sync: bool,
}

impl Wal {
    /// Create a *fresh* WAL at `path`, truncating any existing file.
    ///
    /// Only ever called on a scratch path. Pointing it at the live `wal.log`
    /// would truncate a file that is still the only durable home of every write
    /// acknowledged since the last freeze; the flush path builds the
    /// replacement beside it and renames it into place instead.
    pub fn create(path: impl Into<PathBuf>, sync: bool) -> Result<Wal> {
        let path = path.into();
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;
        Ok(Wal {
            file: BufWriter::new(file),
            path,
            sync,
        })
    }

    /// Open the WAL at `path` for appending, preserving any existing records.
    ///
    /// This is the correct choice on database open: the file still backs the
    /// records just replayed into the MemTable until the next flush.
    pub fn open_append(path: impl Into<PathBuf>, sync: bool) -> Result<Wal> {
        let path = path.into();
        let file = OpenOptions::new().append(true).create(true).open(&path)?;
        Ok(Wal {
            file: BufWriter::new(file),
            path,
            sync,
        })
    }

    /// Filesystem path of this WAL (used by compaction/recovery tooling).
    #[allow(dead_code)]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Append one record and flush it to the OS (and to disk if `sync`).
    pub fn append(&mut self, record: &Record) -> Result<()> {
        let payload = record.encode();
        let crc = crc32fast::hash(&payload);
        self.file.write_all(&crc.to_le_bytes())?;
        self.file.write_all(&(payload.len() as u32).to_le_bytes())?;
        self.file.write_all(&payload)?;
        self.file.flush()?;
        if self.sync {
            self.file.get_ref().sync_all()?;
        }
        Ok(())
    }

    /// Make everything appended so far durable, regardless of `sync`.
    ///
    /// `append` only fsyncs when the WAL was opened with `sync`, so a caller
    /// about to publish this file by rename has to ask explicitly: the rename
    /// would otherwise make a file visible whose contents may not be on disk.
    pub fn sync(&mut self) -> Result<()> {
        self.file.flush()?;
        self.file.get_ref().sync_all()?;
        Ok(())
    }

    /// Rename this WAL to `dest`, taking the new path with it.
    ///
    /// The open descriptor follows the inode, so appends after this land in the
    /// renamed file.
    pub fn rename_to(&mut self, dest: impl AsRef<Path>) -> Result<()> {
        let dest = dest.as_ref();
        std::fs::rename(&self.path, dest)?;
        self.path = dest.to_path_buf();
        Ok(())
    }

    /// Read every intact record from the WAL at `path`.
    ///
    /// A torn trailing frame (a crash mid-append) is expected: replay stops
    /// cleanly at the first truncated or CRC-mismatched frame.
    pub fn replay(path: impl AsRef<Path>) -> Result<Vec<Record>> {
        let mut bytes = Vec::new();
        match File::open(path.as_ref()) {
            Ok(mut f) => {
                f.read_to_end(&mut bytes)?;
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e.into()),
        }

        let mut records = Vec::new();
        let mut pos = 0;
        while pos + FRAME_HEADER <= bytes.len() {
            let crc = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap());
            let len = u32::from_le_bytes(bytes[pos + 4..pos + 8].try_into().unwrap()) as usize;
            let start = pos + FRAME_HEADER;
            let end = match start.checked_add(len) {
                Some(end) if end <= bytes.len() => end,
                _ => break, // torn trailing frame
            };
            let payload = &bytes[start..end];
            if crc32fast::hash(payload) != crc {
                break; // corrupt trailing frame
            }
            let mut rpos = 0;
            let record = Record::decode_at(payload, &mut rpos)?;
            if rpos != payload.len() {
                return Err(Error::Corrupt("trailing bytes in WAL frame".into()));
            }
            records.push(record);
            pos = end;
        }
        Ok(records)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Seek;

    fn tmp() -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");
        (dir, path)
    }

    #[test]
    fn append_and_replay() {
        let (_d, path) = tmp();
        let mut wal = Wal::create(&path, false).unwrap();
        wal.append(&Record::put(b"a".to_vec(), b"1".to_vec(), 1))
            .unwrap();
        wal.append(&Record::tombstone(b"b".to_vec(), 2)).unwrap();
        drop(wal);

        let records = Wal::replay(&path).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].value, Some(b"1".to_vec()));
        assert!(records[1].value.is_none());
    }

    #[test]
    fn replay_missing_file_is_empty() {
        let (_d, path) = tmp();
        assert!(Wal::replay(&path).unwrap().is_empty());
    }

    #[test]
    fn torn_trailing_frame_is_ignored() {
        let (_d, path) = tmp();
        let mut wal = Wal::create(&path, false).unwrap();
        wal.append(&Record::put(b"good".to_vec(), b"v".to_vec(), 1))
            .unwrap();
        wal.append(&Record::put(b"torn".to_vec(), b"vvvv".to_vec(), 2))
            .unwrap();
        drop(wal);

        // Simulate a crash mid-append: chop off the last 3 bytes.
        let f = OpenOptions::new().write(true).open(&path).unwrap();
        let len = f.metadata().unwrap().len();
        f.set_len(len - 3).unwrap();
        f.sync_all().unwrap();

        let records = Wal::replay(&path).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].key, b"good");
    }

    #[test]
    fn crc_mismatch_stops_replay() {
        let (_d, path) = tmp();
        let mut wal = Wal::create(&path, false).unwrap();
        wal.append(&Record::put(b"k".to_vec(), b"v".to_vec(), 1))
            .unwrap();
        drop(wal);

        // Flip a byte inside the payload.
        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        f.seek(std::io::SeekFrom::End(-1)).unwrap();
        let mut last = [0u8; 1];
        f.read_exact(&mut last).unwrap();
        f.seek(std::io::SeekFrom::End(-1)).unwrap();
        f.write_all(&[last[0] ^ 0xFF]).unwrap();
        drop(f);

        assert!(Wal::replay(&path).unwrap().is_empty());
    }
}
