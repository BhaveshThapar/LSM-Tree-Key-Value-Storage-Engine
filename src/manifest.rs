//! Manifest: the crash-safe spine that records which SSTables are live and
//! persists the global `next_seq` / `next_sst_id` counters.
//!
//! It is an append-only log of [`VersionEdit`]s — the LevelDB VersionEdit
//! model. On open the log is replayed into a [`ManifestState`], then rolled
//! over into a fresh, compacted generation so the file never grows unbounded.
//!
//! Frame layout mirrors the WAL: `[crc32:4][payload_len:4][payload]`, where
//! `payload` is `[edit_tag:1][body:8]` (all little-endian). A torn trailing
//! frame is tolerated exactly like [`Wal::replay`](crate::wal).
//!
//! A `CURRENT` file holds the ASCII name of the live `MANIFEST-<gen>` file and
//! is swapped atomically via a rename, so the manifest set is always
//! recoverable even across a crash mid-rollover.

use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use crate::error::{Error, Result};

const CURRENT_FILENAME: &str = "CURRENT";
const FRAME_HEADER: usize = 8; // crc32 + payload_len
const PAYLOAD_LEN: usize = 9; // tag + 8-byte body

/// One atomic change to the set of live SSTables or the global counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VersionEdit {
    AddTable { id: u64 },
    DeleteTable { id: u64 },
    SetNextSeq(u64),
    SetNextSstId(u64),
}

impl VersionEdit {
    fn encode(&self) -> [u8; PAYLOAD_LEN] {
        let (tag, body) = match *self {
            VersionEdit::AddTable { id } => (0u8, id),
            VersionEdit::DeleteTable { id } => (1u8, id),
            VersionEdit::SetNextSeq(s) => (2u8, s),
            VersionEdit::SetNextSstId(id) => (3u8, id),
        };
        let mut buf = [0u8; PAYLOAD_LEN];
        buf[0] = tag;
        buf[1..9].copy_from_slice(&body.to_le_bytes());
        buf
    }

    fn decode(payload: &[u8]) -> Result<VersionEdit> {
        if payload.len() != PAYLOAD_LEN {
            return Err(Error::Corrupt("bad manifest payload length".into()));
        }
        let body = u64::from_le_bytes(payload[1..9].try_into().unwrap());
        match payload[0] {
            0 => Ok(VersionEdit::AddTable { id: body }),
            1 => Ok(VersionEdit::DeleteTable { id: body }),
            2 => Ok(VersionEdit::SetNextSeq(body)),
            3 => Ok(VersionEdit::SetNextSstId(body)),
            other => Err(Error::Corrupt(format!("bad manifest edit tag {other}"))),
        }
    }
}

/// The replayed state of the manifest: the authoritative live SSTable set and
/// the global counters.
#[derive(Debug, Default, Clone)]
pub struct ManifestState {
    pub live_tables: BTreeSet<u64>,
    pub next_seq: u64,
    pub next_sst_id: u64,
}

impl ManifestState {
    fn apply(&mut self, edit: VersionEdit) {
        match edit {
            VersionEdit::AddTable { id } => {
                self.live_tables.insert(id);
            }
            VersionEdit::DeleteTable { id } => {
                self.live_tables.remove(&id);
            }
            VersionEdit::SetNextSeq(s) => self.next_seq = s,
            VersionEdit::SetNextSstId(id) => self.next_sst_id = id,
        }
    }

    /// The edits that, replayed into an empty state, reproduce `self`. Used to
    /// seed a freshly rolled-over manifest generation.
    fn snapshot_edits(&self) -> Vec<VersionEdit> {
        let mut edits = Vec::with_capacity(self.live_tables.len() + 2);
        edits.push(VersionEdit::SetNextSeq(self.next_seq));
        edits.push(VersionEdit::SetNextSstId(self.next_sst_id));
        for &id in &self.live_tables {
            edits.push(VersionEdit::AddTable { id });
        }
        edits
    }
}

/// An append-only manifest log open for writing.
pub struct Manifest {
    dir: PathBuf,
    gen: u64,
    file: BufWriter<File>,
}

impl Manifest {
    /// Whether a database at `dir` already has a manifest.
    pub fn exists(dir: &Path) -> bool {
        dir.join(CURRENT_FILENAME).exists()
    }

    /// Open the existing manifest at `dir`, replay it, and roll it over into a
    /// fresh compacted generation. Returns the writer and the replayed state.
    pub fn open(dir: &Path) -> Result<(Manifest, ManifestState)> {
        let gen = read_current(dir)?;
        let state = replay(&manifest_path(dir, gen))?;
        let mut manifest = Manifest {
            dir: dir.to_path_buf(),
            gen,
            file: BufWriter::new(open_append(&manifest_path(dir, gen))?),
        };
        // Compact the log so it never grows across the lifetime of the dir.
        manifest.rollover(&state)?;
        Ok((manifest, state))
    }

    /// Create a fresh, empty manifest at `dir` (generation 0) and point
    /// `CURRENT` at it. Used on first open and during Phase 2 migration.
    pub fn create(dir: &Path) -> Result<Manifest> {
        let gen = 0;
        let path = manifest_path(dir, gen);
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;
        file.sync_all()?;
        write_current(dir, gen)?;
        Ok(Manifest {
            dir: dir.to_path_buf(),
            gen,
            file: BufWriter::new(file),
        })
    }

    /// Append a batch of edits and fsync them as a unit.
    pub fn append_batch(&mut self, edits: &[VersionEdit]) -> Result<()> {
        for edit in edits {
            let payload = edit.encode();
            let crc = crc32fast::hash(&payload);
            self.file.write_all(&crc.to_le_bytes())?;
            self.file.write_all(&(payload.len() as u32).to_le_bytes())?;
            self.file.write_all(&payload)?;
        }
        self.file.flush()?;
        self.file.get_ref().sync_all()?;
        Ok(())
    }

    /// Write `state` into a fresh manifest generation and atomically swap
    /// `CURRENT` to point at it, then delete the previous generation.
    pub fn rollover(&mut self, state: &ManifestState) -> Result<()> {
        let old_gen = self.gen;
        let new_gen = old_gen + 1;
        let new_path = manifest_path(&self.dir, new_gen);

        let mut writer = Manifest {
            dir: self.dir.clone(),
            gen: new_gen,
            file: BufWriter::new(
                OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(&new_path)?,
            ),
        };
        writer.append_batch(&state.snapshot_edits())?;

        // Atomic publish: CURRENT now names the new, durable generation.
        write_current(&self.dir, new_gen)?;

        self.gen = new_gen;
        self.file = writer.file;
        let _ = fs::remove_file(manifest_path(&self.dir, old_gen));
        Ok(())
    }
}

fn manifest_path(dir: &Path, gen: u64) -> PathBuf {
    dir.join(format!("MANIFEST-{gen:06}"))
}

fn open_append(path: &Path) -> Result<File> {
    Ok(OpenOptions::new().append(true).create(true).open(path)?)
}

/// Read the generation number named by `CURRENT`.
fn read_current(dir: &Path) -> Result<u64> {
    let raw = fs::read_to_string(dir.join(CURRENT_FILENAME))?;
    let name = raw.trim();
    name.strip_prefix("MANIFEST-")
        .and_then(|g| g.parse().ok())
        .ok_or_else(|| Error::Corrupt(format!("bad CURRENT contents: {name:?}")))
}

/// Atomically point `CURRENT` at `MANIFEST-<gen>` via a rename.
fn write_current(dir: &Path, gen: u64) -> Result<()> {
    let tmp = dir.join("CURRENT.tmp");
    {
        let mut f = File::create(&tmp)?;
        writeln!(f, "MANIFEST-{gen:06}")?;
        f.sync_all()?;
    }
    fs::rename(&tmp, dir.join(CURRENT_FILENAME))?;
    sync_dir(dir)?;
    Ok(())
}

/// fsync a directory so a rename within it is durable.
fn sync_dir(dir: &Path) -> Result<()> {
    File::open(dir)?.sync_all()?;
    Ok(())
}

/// Replay a manifest file into a [`ManifestState`], tolerating a torn trailing
/// frame from a crash mid-append.
fn replay(path: &Path) -> Result<ManifestState> {
    let mut bytes = Vec::new();
    match File::open(path) {
        Ok(mut f) => {
            f.read_to_end(&mut bytes)?;
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(ManifestState::default()),
        Err(e) => return Err(e.into()),
    }

    let mut state = ManifestState::default();
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
        state.apply(VersionEdit::decode(payload)?);
        pos = end;
    }
    Ok(state)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_open_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut m = Manifest::create(dir.path()).unwrap();
            m.append_batch(&[
                VersionEdit::AddTable { id: 1 },
                VersionEdit::AddTable { id: 2 },
                VersionEdit::SetNextSeq(42),
                VersionEdit::SetNextSstId(3),
            ])
            .unwrap();
        }
        let (_m, state) = Manifest::open(dir.path()).unwrap();
        assert_eq!(state.live_tables, BTreeSet::from([1, 2]));
        assert_eq!(state.next_seq, 42);
        assert_eq!(state.next_sst_id, 3);
    }

    #[test]
    fn delete_table_removes_from_live_set() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut m = Manifest::create(dir.path()).unwrap();
            m.append_batch(&[
                VersionEdit::AddTable { id: 1 },
                VersionEdit::AddTable { id: 2 },
                VersionEdit::DeleteTable { id: 1 },
            ])
            .unwrap();
        }
        let (_m, state) = Manifest::open(dir.path()).unwrap();
        assert_eq!(state.live_tables, BTreeSet::from([2]));
    }

    #[test]
    fn rollover_compacts_and_preserves_state() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut m = Manifest::create(dir.path()).unwrap();
            for id in 0..10 {
                m.append_batch(&[VersionEdit::AddTable { id }]).unwrap();
            }
            for id in 0..8 {
                m.append_batch(&[VersionEdit::DeleteTable { id }]).unwrap();
            }
            m.append_batch(&[VersionEdit::SetNextSeq(100)]).unwrap();
        }
        // open() rolls over to a fresh generation.
        let (_m, state) = Manifest::open(dir.path()).unwrap();
        assert_eq!(state.live_tables, BTreeSet::from([8, 9]));
        assert_eq!(state.next_seq, 100);
        // Generation 0 is gone after the open-time rollover.
        assert!(!manifest_path(dir.path(), 0).exists());
        assert!(manifest_path(dir.path(), 1).exists());
    }

    #[test]
    fn torn_trailing_frame_is_ignored() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut m = Manifest::create(dir.path()).unwrap();
            m.append_batch(&[VersionEdit::AddTable { id: 7 }]).unwrap();
        }
        let path = manifest_path(dir.path(), 0);
        let f = OpenOptions::new().write(true).open(&path).unwrap();
        let len = f.metadata().unwrap().len();
        f.set_len(len - 2).unwrap();
        f.sync_all().unwrap();

        let state = replay(&path).unwrap();
        assert!(state.live_tables.is_empty());
    }
}
