//! Write-ahead log: every mutation is appended here and durably flushed
//! before it reaches the MemTable, so a crash never loses an acked write.
//!
//! File layout: an eight-byte [header](crate::header), then frames.
//!
//! Frame layout: `[crc32:4][payload_len:4][payload]`, where the CRC covers
//! `payload` only.
//!
//! In version 2 the payload is `[count:u32]` followed by that many [`Record`]
//! encodings. That is what makes a batch atomic: one frame, one CRC, so a crash
//! that catches it mid-write leaves a frame that does not verify and replay
//! discards *all* of it. A caller that needs two keys to become durable together
//! — an index and the data it describes — gets that from the format rather than
//! from luck.
//!
//! Version 1 held exactly one record per frame and is still read.
//!
//! A file written before the header existed is read without one; see
//! [`crate::header`] for why that is accepted rather than refused.

use std::path::{Path, PathBuf};

use crate::error::{Error, Result};
use crate::fs::{BufAppend, File, Fs, OpenMode, SyncMode};
use crate::header::{self, Checksum, Kind, WAL_MAGIC, WAL_VERSION};
use crate::record::Record;

const FRAME_HEADER: usize = 8; // crc32 + payload_len

/// An append-only write-ahead log bound to a single active MemTable.
pub struct Wal<F: Fs> {
    file: BufAppend<F::File>,
    path: PathBuf,
    sync: SyncMode,
    /// Which checksum this file's frames are protected by. Decided by the
    /// file, like `counted` and for the same reason.
    checksum: Checksum,
    /// Whether frames appended to this file carry a batch count.
    ///
    /// Decided by what is already in the file, not by what this build prefers.
    /// A version 1 file's frames hold exactly one record and no count, and
    /// appending a counted frame to it would produce a file whose two halves
    /// disagree about their own format — which is the failure the header exists
    /// to prevent and would be an embarrassing way to cause.
    counted: bool,
}

impl<F: Fs> Wal<F> {
    /// Create a *fresh* WAL at `path`, truncating any existing file.
    ///
    /// Only ever called on a scratch path. Pointing it at the live `wal.log`
    /// would truncate a file that is still the only durable home of every write
    /// acknowledged since the last freeze; the flush path builds the
    /// replacement beside it and renames it into place instead.
    pub fn create(fs: &F, path: impl Into<PathBuf>, sync: SyncMode) -> Result<Wal<F>> {
        let path = path.into();
        let file = fs.open(&path, OpenMode::Truncate)?;
        let mut wal = Wal {
            file: BufAppend::new(file),
            path,
            sync,
            checksum: header::wal_checksum(Kind::Versioned(WAL_VERSION)),
            counted: true,
        };
        wal.file.write(&header::encode(WAL_MAGIC, WAL_VERSION))?;
        // Out of the buffer immediately: a file that exists and holds nothing
        // is indistinguishable from one that was never written, and the flush
        // path renames this file into place over a live one.
        wal.file.flush()?;
        Ok(wal)
    }

    /// Open the WAL at `path` for appending, preserving any existing records.
    ///
    /// This is the correct choice on database open: the file still backs the
    /// records just replayed into the MemTable until the next flush.
    pub fn open_append(fs: &F, path: impl Into<PathBuf>, sync: SyncMode) -> Result<Wal<F>> {
        let path = path.into();
        let file = fs.open(&path, OpenMode::Append)?;
        let existing = file.size()?;

        // What is already in the file decides what may be appended to it.
        let mut front = [0u8; header::HEADER_LEN];
        let front = if existing == 0 {
            &front[..0]
        } else {
            let n = file.read_at(0, &mut front)?;
            &front[..n]
        };
        let kind = header::classify(front, WAL_MAGIC, WAL_VERSION, "the write-ahead log")?;
        let effective = match kind {
            Kind::Empty => Kind::Versioned(WAL_VERSION),
            other => other,
        };
        let counted = !matches!(effective, Kind::Legacy | Kind::Versioned(1));

        let mut wal = Wal {
            file: BufAppend::new(file),
            path,
            sync,
            checksum: header::wal_checksum(effective),
            counted,
        };
        // A file that does not exist yet is created empty by `Append`, so it
        // needs the header this build writes. One that already has content —
        // header or legacy — is appended to as it is: rewriting its front is
        // not something an append may do, and the flush that replaces this file
        // writes a fresh one with a header.
        if existing == 0 {
            wal.file.write(&header::encode(WAL_MAGIC, WAL_VERSION))?;
            wal.file.flush()?;
        }
        Ok(wal)
    }

    /// Filesystem path of this WAL (used by compaction/recovery tooling).
    #[allow(dead_code)]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Append one record and flush it to the OS (and to disk if `sync`).
    /// Append one record as a batch of one.
    pub fn append(&mut self, record: &Record) -> Result<()> {
        self.append_batch(std::slice::from_ref(record))
    }

    /// Append `records` as a single frame, and sync once for all of them.
    ///
    /// The atomicity is the format's, not the filesystem's: the whole batch is
    /// one length-prefixed payload under one CRC, so a crash mid-write leaves a
    /// frame that does not verify and replay drops every record in it. There is
    /// no interleaving in which half a batch survives.
    ///
    /// The single fsync is the other half. One `put` per fsync is the difference
    /// between a usable write path and an unusable one, and a caller batching a
    /// hundred keys should pay for one.
    pub fn append_batch(&mut self, records: &[Record]) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        if !self.counted && records.len() > 1 {
            // The alternative would be to write one frame per record and say
            // nothing, which is a silent loss of the atomicity the caller asked
            // for. A caller that needs a batch on a database opened before
            // version 2 gets it after the next flush, which writes a fresh file.
            return Err(Error::BadFormat(format!(
                "{} is a version 1 write-ahead log and cannot hold a batch of {} records; \
                 it is replaced by a version 2 file at the next flush",
                self.path.display(),
                records.len()
            )));
        }
        let mut payload = Vec::new();
        if self.counted {
            payload.extend((records.len() as u32).to_le_bytes());
        }
        for record in records {
            record.encode_into(&mut payload);
        }
        let crc = self.checksum.hash(&payload);
        self.file.write(&crc.to_le_bytes())?;
        self.file.write(&(payload.len() as u32).to_le_bytes())?;
        self.file.write(&payload)?;
        // A record still in this process's buffer is a record a crash loses
        // without the file ever being short, so the buffer is emptied on every
        // append whether or not a sync follows.
        self.file.flush()?;
        self.file.get_mut().sync_as(self.sync)?;
        Ok(())
    }

    /// Make everything appended so far durable, regardless of `sync`.
    ///
    /// `append` only fsyncs when the WAL was opened with `sync`, so a caller
    /// about to publish this file by rename has to ask explicitly: the rename
    /// would otherwise make a file visible whose contents may not be on disk.
    pub fn sync(&mut self) -> Result<()> {
        self.file.flush()?;
        self.file.get_mut().sync_as(SyncMode::Durable)?;
        Ok(())
    }

    /// Rename this WAL to `dest`, taking the new path with it.
    ///
    /// The open descriptor follows the inode, so appends after this land in the
    /// renamed file.
    pub fn rename_to(&mut self, fs: &F, dest: impl AsRef<Path>) -> Result<()> {
        let dest = dest.as_ref();
        fs.rename(&self.path, dest)?;
        self.path = dest.to_path_buf();
        Ok(())
    }

    /// Read every intact record from the WAL at `path`.
    ///
    /// A torn trailing frame (a crash mid-append) is expected: replay stops
    /// cleanly at the first truncated or CRC-mismatched frame.
    pub fn replay(fs: &F, path: impl AsRef<Path>) -> Result<Vec<Record>> {
        let bytes = match fs.open(path.as_ref(), OpenMode::Read) {
            Ok(f) => f.read_all()?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e.into()),
        };

        let kind = header::classify(&bytes, WAL_MAGIC, WAL_VERSION, "the write-ahead log")?;
        if kind == Kind::Empty {
            return Ok(Vec::new());
        }

        // Version 1 wrote one record per frame with no count in front of it.
        let counted = !matches!(kind, Kind::Legacy | Kind::Versioned(1));
        let checksum = header::wal_checksum(kind);

        let mut records = Vec::new();
        let mut pos = header::frames_start(kind);
        while pos + FRAME_HEADER <= bytes.len() {
            let crc = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap());
            let len = u32::from_le_bytes(bytes[pos + 4..pos + 8].try_into().unwrap()) as usize;
            let start = pos + FRAME_HEADER;
            let end = match start.checked_add(len) {
                Some(end) if end <= bytes.len() => end,
                _ => break, // torn trailing frame
            };
            let payload = &bytes[start..end];
            if checksum.hash(payload) != crc {
                break; // corrupt trailing frame
            }
            let mut rpos = 0;
            let count = if counted {
                if payload.len() < 4 {
                    return Err(Error::Corrupt(
                        "WAL frame too short for a batch count".into(),
                    ));
                }
                rpos = 4;
                u32::from_le_bytes(payload[0..4].try_into().unwrap_or([0; 4])) as usize
            } else {
                1
            };
            // Decode into a scratch vector and commit it only once the whole
            // frame parses. A batch is atomic on the way in and must be atomic
            // on the way out; pushing as we go would publish half of one if the
            // second record were malformed.
            let mut batch = Vec::with_capacity(count.min(1024));
            for _ in 0..count {
                batch.push(Record::decode_at(payload, &mut rpos)?);
            }
            if rpos != payload.len() {
                return Err(Error::Corrupt("trailing bytes in WAL frame".into()));
            }
            records.extend(batch);
            pos = end;
        }
        Ok(records)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::StdFs;
    use std::io::{Read, Seek, Write};

    fn tmp() -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");
        (dir, path)
    }

    #[test]
    fn append_and_replay() {
        let (_d, path) = tmp();
        let mut wal = Wal::create(&StdFs, &path, SyncMode::None).unwrap();
        wal.append(&Record::put(b"a".to_vec(), b"1".to_vec(), 1))
            .unwrap();
        wal.append(&Record::tombstone(b"b".to_vec(), 2)).unwrap();
        drop(wal);

        let records = Wal::replay(&StdFs, &path).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].value, Some(b"1".to_vec()));
        assert!(records[1].value.is_none());
    }

    #[test]
    fn replay_missing_file_is_empty() {
        let (_d, path) = tmp();
        assert!(Wal::replay(&StdFs, &path).unwrap().is_empty());
    }

    #[test]
    fn torn_trailing_frame_is_ignored() {
        let (_d, path) = tmp();
        let mut wal = Wal::create(&StdFs, &path, SyncMode::None).unwrap();
        wal.append(&Record::put(b"good".to_vec(), b"v".to_vec(), 1))
            .unwrap();
        wal.append(&Record::put(b"torn".to_vec(), b"vvvv".to_vec(), 2))
            .unwrap();
        drop(wal);

        // Simulate a crash mid-append: chop off the last 3 bytes.
        let f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        let len = f.metadata().unwrap().len();
        f.set_len(len - 3).unwrap();
        f.sync_all().unwrap();

        let records = Wal::replay(&StdFs, &path).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].key, b"good");
    }

    #[test]
    fn crc_mismatch_stops_replay() {
        let (_d, path) = tmp();
        let mut wal = Wal::create(&StdFs, &path, SyncMode::None).unwrap();
        wal.append(&Record::put(b"k".to_vec(), b"v".to_vec(), 1))
            .unwrap();
        drop(wal);

        // Flip a byte inside the payload.
        let mut f = std::fs::OpenOptions::new()
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

        assert!(Wal::replay(&StdFs, &path).unwrap().is_empty());
    }

    /// A WAL written before headers existed still replays.
    #[test]
    fn a_headerless_wal_still_replays() {
        let (_d, path) = tmp();
        let mut bytes = Vec::new();
        for record in [
            Record::put(b"a".to_vec(), b"1".to_vec(), 1),
            Record::tombstone(b"b".to_vec(), 2),
        ] {
            let payload = record.encode();
            bytes.extend(crc32fast::hash(&payload).to_le_bytes());
            bytes.extend((payload.len() as u32).to_le_bytes());
            bytes.extend(payload);
        }
        std::fs::write(&path, &bytes).unwrap();

        let records = Wal::<StdFs>::replay(&StdFs, &path).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].key, b"a");
        assert!(records[1].value.is_none());
    }

    /// Appending to a legacy file does not rewrite its front. The header
    /// arrives when the flush path builds a replacement, not before — an append
    /// that moved where the frames start would invalidate every frame already
    /// in the file.
    #[test]
    fn appending_to_a_legacy_wal_leaves_it_headerless_and_readable() {
        let (_d, path) = tmp();
        let first = Record::put(b"old".to_vec(), b"v".to_vec(), 1);
        let payload = first.encode();
        let mut bytes = Vec::new();
        bytes.extend(crc32fast::hash(&payload).to_le_bytes());
        bytes.extend((payload.len() as u32).to_le_bytes());
        bytes.extend(payload);
        std::fs::write(&path, &bytes).unwrap();

        let mut wal = Wal::open_append(&StdFs, &path, SyncMode::None).unwrap();
        wal.append(&Record::put(b"new".to_vec(), b"v".to_vec(), 2))
            .unwrap();
        wal.sync().unwrap();
        drop(wal);

        let records = Wal::<StdFs>::replay(&StdFs, &path).unwrap();
        assert_eq!(records.len(), 2, "the append or the legacy frame was lost");
        assert_eq!(records[0].key, b"old");
        assert_eq!(records[1].key, b"new");
    }

    /// A fresh WAL whose very first append was cut short really does hold no
    /// records. This is the case the manifest refuses and the WAL must not.
    #[test]
    fn a_wal_whose_first_frame_is_torn_holds_no_records() {
        let (_d, path) = tmp();
        let mut wal = Wal::create(&StdFs, &path, SyncMode::None).unwrap();
        wal.append(&Record::put(b"k".to_vec(), b"v".to_vec(), 1))
            .unwrap();
        wal.sync().unwrap();
        drop(wal);

        let f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        f.set_len(crate::header::HEADER_LEN as u64 + 3).unwrap();
        f.sync_all().unwrap();

        assert!(Wal::<StdFs>::replay(&StdFs, &path).unwrap().is_empty());
    }

    #[test]
    fn a_fresh_wal_starts_with_a_header() {
        let (_d, path) = tmp();
        drop(Wal::create(&StdFs, &path, SyncMode::None).unwrap());
        let bytes = std::fs::read(&path).unwrap();
        assert_eq!(bytes.len(), crate::header::HEADER_LEN);
        assert_eq!(&bytes[0..4], crate::header::WAL_MAGIC);
    }
}
