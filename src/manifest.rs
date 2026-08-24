//! Manifest: the crash-safe spine that records which SSTables are live and
//! persists the global `next_seq` / `next_sst_id` counters.
//!
//! It is an append-only log of [`VersionEdit`]s — the LevelDB VersionEdit
//! model. On open the log is replayed into a [`ManifestState`], then rolled
//! over into a fresh, compacted generation so the file never grows unbounded.
//!
//! File layout mirrors the WAL: an eight-byte [header](crate::header), then
//! frames.
//!
//! Frame layout: `[crc32:4][payload_len:4][payload]`, where
//! `payload` is `[edit_tag:1][body:8]` (all little-endian). A torn trailing
//! frame is tolerated exactly like [`Wal::replay`](crate::wal).
//!
//! A `CURRENT` file holds the ASCII name of the live `MANIFEST-<generation>` file and
//! is swapped atomically via a rename, so the manifest set is always
//! recoverable even across a crash mid-rollover.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use crate::error::{Error, Result};
use crate::fs::{BufAppend, File as _, Fs, OpenMode};
use crate::header::{self, Checksum, Kind, MANIFEST_MAGIC, MANIFEST_VERSION};

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
pub struct Manifest<F: Fs> {
    dir: PathBuf,
    generation: u64,
    file: BufAppend<F::File>,
    /// Which checksum this generation's frames use. Every generation this build
    /// writes is the current one; the field exists because `open` appends to a
    /// generation it did not write before rolling it over.
    checksum: Checksum,
}

impl<F: Fs> Manifest<F> {
    /// Whether a database at `dir` already has a manifest.
    pub fn exists(fs: &F, dir: &Path) -> bool {
        fs.exists(&dir.join(CURRENT_FILENAME))
    }

    /// Open the existing manifest at `dir`, replay it, and roll it over into a
    /// fresh compacted generation. Returns the writer and the replayed state.
    pub fn open(fs: &F, dir: &Path) -> Result<(Manifest<F>, ManifestState)> {
        let generation = read_current(fs, dir)?;
        let state = replay(fs, &manifest_path(dir, generation))?;
        let mut manifest = Manifest::<F> {
            dir: dir.to_path_buf(),
            generation,
            file: BufAppend::new(fs.open(&manifest_path(dir, generation), OpenMode::Append)?),
            checksum: header::manifest_checksum(classify_manifest(fs, dir, generation)?),
        };
        // Compact the log so it never grows across the lifetime of the dir.
        manifest.rollover(fs, &state)?;
        Ok((manifest, state))
    }

    /// Create a fresh, empty manifest at `dir` (generation 0) and point
    /// `CURRENT` at it. Used on first open and during Phase 2 migration.
    pub fn create(fs: &F, dir: &Path) -> Result<Manifest<F>> {
        let generation = 0;
        let path = manifest_path(dir, generation);
        let file = fs.open(&path, OpenMode::Truncate)?;
        let mut manifest = Manifest {
            dir: dir.to_path_buf(),
            generation,
            file: BufAppend::new(file),
            checksum: header::manifest_checksum(Kind::Versioned(MANIFEST_VERSION)),
        };
        manifest
            .file
            .write(&header::encode(MANIFEST_MAGIC, MANIFEST_VERSION))?;
        manifest.file.sync()?;
        write_current(fs, dir, generation)?;
        Ok(manifest)
    }

    /// Append a batch of edits and fsync them as a unit.
    pub fn append_batch(&mut self, edits: &[VersionEdit]) -> Result<()> {
        for edit in edits {
            let payload = edit.encode();
            let crc = self.checksum.hash(&payload);
            self.file.write(&crc.to_le_bytes())?;
            self.file.write(&(payload.len() as u32).to_le_bytes())?;
            self.file.write(&payload)?;
        }
        self.file.sync()?;
        Ok(())
    }

    /// Write `state` into a fresh manifest generation and atomically swap
    /// `CURRENT` to point at it, then delete the previous generation.
    pub fn rollover(&mut self, fs: &F, state: &ManifestState) -> Result<()> {
        let old_gen = self.generation;
        let new_gen = old_gen + 1;
        let new_path = manifest_path(&self.dir, new_gen);

        let mut writer = Manifest::<F> {
            dir: self.dir.clone(),
            generation: new_gen,
            file: BufAppend::new(fs.open(&new_path, OpenMode::Truncate)?),
            checksum: header::manifest_checksum(Kind::Versioned(MANIFEST_VERSION)),
        };
        writer
            .file
            .write(&header::encode(MANIFEST_MAGIC, MANIFEST_VERSION))?;
        writer.append_batch(&state.snapshot_edits())?;

        // Atomic publish: CURRENT now names the new, durable generation.
        write_current(fs, &self.dir, new_gen)?;

        self.generation = new_gen;
        self.file = writer.file;
        self.checksum = writer.checksum;
        let _ = fs.remove(&manifest_path(&self.dir, old_gen));
        Ok(())
    }
}

fn manifest_path(dir: &Path, generation: u64) -> PathBuf {
    dir.join(format!("MANIFEST-{generation:06}"))
}

/// Read the generation number named by `CURRENT`.
fn read_current<F: Fs>(fs: &F, dir: &Path) -> Result<u64> {
    let bytes = fs
        .open(&dir.join(CURRENT_FILENAME), OpenMode::Read)?
        .read_all()?;
    let raw = String::from_utf8_lossy(&bytes);
    let name = raw.trim();
    name.strip_prefix("MANIFEST-")
        .and_then(|g| g.parse().ok())
        .ok_or_else(|| Error::Corrupt(format!("bad CURRENT contents: {name:?}")))
}

/// Atomically point `CURRENT` at `MANIFEST-<generation>` via a rename.
fn write_current<F: Fs>(fs: &F, dir: &Path, generation: u64) -> Result<()> {
    let tmp = dir.join("CURRENT.tmp");
    {
        let mut f = fs.open(&tmp, OpenMode::Truncate)?;
        f.append(format!("MANIFEST-{generation:06}\n").as_bytes())?;
        f.sync()?;
    }
    fs.rename(&tmp, &dir.join(CURRENT_FILENAME))?;
    fs.sync_dir(dir)?;
    Ok(())
}

/// What version a manifest generation on disk is, so an append to it uses that
/// generation's checksum rather than this build's.
fn classify_manifest<F: Fs>(fs: &F, dir: &Path, generation: u64) -> Result<Kind> {
    let path = manifest_path(dir, generation);
    let mut front = [0u8; header::HEADER_LEN];
    let front = match fs.open(&path, OpenMode::Read) {
        Ok(f) => {
            let n = f.read_at(0, &mut front)?;
            &front[..n]
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => &front[..0],
        Err(e) => return Err(e.into()),
    };
    header::classify(front, MANIFEST_MAGIC, MANIFEST_VERSION, "the manifest")
}

/// Replay a manifest file into a [`ManifestState`], tolerating a torn trailing
/// frame from a crash mid-append.
fn replay<F: Fs>(fs: &F, path: &Path) -> Result<ManifestState> {
    let bytes = match fs.open(path, OpenMode::Read) {
        Ok(f) => f.read_all()?,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(ManifestState::default()),
        Err(e) => return Err(e.into()),
    };

    let kind = header::classify(&bytes, MANIFEST_MAGIC, MANIFEST_VERSION, "the manifest")?;
    if kind == Kind::Empty {
        return Ok(ManifestState::default());
    }
    let checksum = header::manifest_checksum(kind);

    let mut state = ManifestState::default();
    let first_frame = header::frames_start(kind);
    let mut pos = first_frame;
    let mut frames = 0usize;
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
        state.apply(VersionEdit::decode(payload)?);
        frames += 1;
        pos = end;
    }

    // A manifest with bytes in it and not one readable frame is corrupt, and
    // saying so here is the point of this whole module.
    //
    // The reclamation loop on open deletes every SSTable the manifest does not
    // name, so a manifest read as empty is not a failed open — it is a
    // successful one that takes the database with it. Tolerating a torn
    // *trailing* frame is right, because that is what a crash mid-append looks
    // like. Tolerating a file where even the first frame does not verify is
    // not: the engine rolls the manifest over on every open, so a live one
    // always holds at least the snapshot edits, and zero readable frames is a
    // state this engine never produces.
    //
    // The WAL deliberately has no equivalent check. A fresh WAL followed by a
    // crash during its first append leaves exactly this shape, and there it
    // really does mean "no records".
    if frames == 0 && bytes.len() > first_frame {
        return Err(Error::Corrupt(format!(
            "{} holds {} bytes and not one readable frame; refusing to treat it as empty, \
             because doing so would reclaim every SSTable in the directory",
            path.display(),
            bytes.len() - first_frame
        )));
    }
    Ok(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::StdFs;

    #[test]
    fn create_open_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut m = Manifest::create(&StdFs, dir.path()).unwrap();
            m.append_batch(&[
                VersionEdit::AddTable { id: 1 },
                VersionEdit::AddTable { id: 2 },
                VersionEdit::SetNextSeq(42),
                VersionEdit::SetNextSstId(3),
            ])
            .unwrap();
        }
        let (_m, state) = Manifest::open(&StdFs, dir.path()).unwrap();
        assert_eq!(state.live_tables, BTreeSet::from([1, 2]));
        assert_eq!(state.next_seq, 42);
        assert_eq!(state.next_sst_id, 3);
    }

    #[test]
    fn delete_table_removes_from_live_set() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut m = Manifest::create(&StdFs, dir.path()).unwrap();
            m.append_batch(&[
                VersionEdit::AddTable { id: 1 },
                VersionEdit::AddTable { id: 2 },
                VersionEdit::DeleteTable { id: 1 },
            ])
            .unwrap();
        }
        let (_m, state) = Manifest::open(&StdFs, dir.path()).unwrap();
        assert_eq!(state.live_tables, BTreeSet::from([2]));
    }

    #[test]
    fn rollover_compacts_and_preserves_state() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut m = Manifest::create(&StdFs, dir.path()).unwrap();
            for id in 0..10 {
                m.append_batch(&[VersionEdit::AddTable { id }]).unwrap();
            }
            for id in 0..8 {
                m.append_batch(&[VersionEdit::DeleteTable { id }]).unwrap();
            }
            m.append_batch(&[VersionEdit::SetNextSeq(100)]).unwrap();
        }
        // open() rolls over to a fresh generation.
        let (_m, state) = Manifest::open(&StdFs, dir.path()).unwrap();
        assert_eq!(state.live_tables, BTreeSet::from([8, 9]));
        assert_eq!(state.next_seq, 100);
        // Generation 0 is gone after the open-time rollover.
        assert!(!manifest_path(dir.path(), 0).exists());
        assert!(manifest_path(dir.path(), 1).exists());
    }

    /// A crash mid-append leaves the last frame torn. Everything before it is
    /// still good and is still read.
    #[test]
    fn torn_trailing_frame_is_ignored() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut m = Manifest::create(&StdFs, dir.path()).unwrap();
            m.append_batch(&[VersionEdit::AddTable { id: 7 }]).unwrap();
            m.append_batch(&[VersionEdit::AddTable { id: 8 }]).unwrap();
        }
        let path = manifest_path(dir.path(), 0);
        let f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        let len = f.metadata().unwrap().len();
        f.set_len(len - 2).unwrap();
        f.sync_all().unwrap();

        let state = replay(&StdFs, &path).unwrap();
        assert_eq!(
            state.live_tables.iter().copied().collect::<Vec<_>>(),
            vec![7],
            "the intact frame before the torn one was lost"
        );
    }

    /// The whole reason this file has a header. A manifest with bytes in it and
    /// not one readable frame is refused — because the alternative is a
    /// *successful* open that decides every SSTable in the directory is an
    /// orphan and deletes it.
    #[test]
    fn a_manifest_with_no_readable_frame_is_refused_rather_than_read_as_empty() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut m = Manifest::create(&StdFs, dir.path()).unwrap();
            m.append_batch(&[VersionEdit::AddTable { id: 7 }]).unwrap();
        }
        let path = manifest_path(dir.path(), 0);
        // Cut the one frame short: exactly the shape a foreign or
        // wrong-version file replays to.
        let f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        f.set_len(header::HEADER_LEN as u64 + 3).unwrap();
        f.sync_all().unwrap();

        let err = replay(&StdFs, &path).unwrap_err();
        assert!(
            matches!(err, Error::Corrupt(_)),
            "a manifest with no readable frame gave {err:?} rather than Corrupt"
        );
    }

    /// A file holding nothing but a header is a manifest that was created and
    /// not yet written to. That is empty, not corrupt.
    #[test]
    fn a_header_and_nothing_else_is_an_empty_manifest() {
        let dir = tempfile::tempdir().unwrap();
        let path = manifest_path(dir.path(), 0);
        drop(Manifest::create(&StdFs, dir.path()).unwrap());
        assert_eq!(
            StdFs.size(&path).unwrap(),
            header::HEADER_LEN as u64,
            "create wrote more than a header"
        );
        assert!(replay(&StdFs, &path).unwrap().live_tables.is_empty());
    }

    /// A manifest written before headers existed still opens, and comes back
    /// with a header once the open has rolled it over.
    #[test]
    fn a_headerless_manifest_still_replays_and_is_rewritten_with_a_header() {
        let dir = tempfile::tempdir().unwrap();
        // Build one by hand in the old layout: frames from offset zero.
        let path = manifest_path(dir.path(), 0);
        {
            let mut bytes = Vec::new();
            for edit in [
                VersionEdit::SetNextSstId(3),
                VersionEdit::AddTable { id: 2 },
            ] {
                let payload = edit.encode();
                bytes.extend(crc32fast::hash(&payload).to_le_bytes());
                bytes.extend((payload.len() as u32).to_le_bytes());
                bytes.extend(payload);
            }
            std::fs::write(&path, &bytes).unwrap();
            std::fs::write(dir.path().join(CURRENT_FILENAME), b"MANIFEST-000000\n").unwrap();
        }

        let state = replay(&StdFs, &path).unwrap();
        assert_eq!(state.next_sst_id, 3);
        assert_eq!(
            state.live_tables.iter().copied().collect::<Vec<_>>(),
            vec![2]
        );

        // Opening rolls it over, and the new generation carries a header.
        let (_m, opened) = Manifest::open(&StdFs, dir.path()).unwrap();
        assert_eq!(opened.next_sst_id, 3);
        let rolled = std::fs::read(manifest_path(dir.path(), 1)).unwrap();
        assert_eq!(
            &rolled[0..4],
            MANIFEST_MAGIC,
            "the rollover did not write a header"
        );
    }
}
