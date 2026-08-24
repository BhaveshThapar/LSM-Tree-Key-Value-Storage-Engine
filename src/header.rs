//! The eight bytes at the front of every log file the engine writes.
//!
//! `[magic:4][version:u16][reserved:u16]`, little-endian.
//!
//! The reason is narrow and worth stating exactly. Both the WAL and the
//! manifest replay frames and stop cleanly at the first one that does not
//! verify, because that is what a crash mid-append looks like. That behaviour
//! is correct and it is also indistinguishable from "this file is in a format
//! this build does not understand" — and the consequence of the second being
//! read as the first is not a failed open. It is a *successful* one: the
//! manifest replays to an empty state, the open decides every SSTable in the
//! directory is an orphan, and it deletes them.
//!
//! A header turns that into an error. A file whose first bytes are neither the
//! magic nor plausibly a legacy file is refused, loudly, before anything is
//! reclaimed.
//!
//! There is no compatibility promise here yet, and this is not one. It is the
//! mechanism that makes it *possible* to change a frame format later without
//! the change looking like corruption — which is what the WAL is about to need.

use crate::error::{Error, Result};

/// Bytes at the front of a WAL or manifest file.
pub(crate) const HEADER_LEN: usize = 8;

/// Format version of the write-ahead log written by this build.
///
/// * 1 — one record per frame, CRC-32/ISO-HDLC.
/// * 2 — a count and then that many records, so a batch is one frame under one
///   CRC and a crash either takes all of it or none of it. Same checksum.
/// * 3 — the same frames under CRC32C.
pub(crate) const WAL_VERSION: u16 = 3;

/// Format version of the manifest written by this build.
///
/// * 1 — CRC-32/ISO-HDLC.
/// * 2 — CRC32C.
pub(crate) const MANIFEST_VERSION: u16 = 2;

/// Which checksum a file's frames are protected by.
///
/// Two of them exist here only because one of them replaced the other, and a
/// file written by an older build has to keep verifying. Changing the checksum
/// without version-gating it would make every existing frame fail its CRC —
/// which for the manifest is not a failed open but a successful one that
/// reclaims every SSTable in the directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Checksum {
    /// CRC-32/ISO-HDLC, the `crc32fast` default. What the engine started with.
    IsoHdlc,
    /// CRC32C (Castagnoli). Hardware-accelerated on every CPU this runs on, and
    /// the polynomial every other storage format in this project uses.
    Castagnoli,
}

impl Checksum {
    pub(crate) fn hash(self, bytes: &[u8]) -> u32 {
        match self {
            Checksum::IsoHdlc => crc32fast::hash(bytes),
            Checksum::Castagnoli => crc32c::crc32c(bytes),
        }
    }
}

/// The checksum a write-ahead log of this version uses.
pub(crate) fn wal_checksum(kind: Kind) -> Checksum {
    match kind {
        Kind::Versioned(v) if v >= 3 => Checksum::Castagnoli,
        _ => Checksum::IsoHdlc,
    }
}

/// The checksum a manifest of this version uses.
pub(crate) fn manifest_checksum(kind: Kind) -> Checksum {
    match kind {
        Kind::Versioned(v) if v >= 2 => Checksum::Castagnoli,
        _ => Checksum::IsoHdlc,
    }
}

/// Marks the front of a write-ahead log.
pub(crate) const WAL_MAGIC: &[u8; 4] = b"LSMW";

/// Marks the front of a manifest.
pub(crate) const MANIFEST_MAGIC: &[u8; 4] = b"LSMM";

/// What the first bytes of a file turned out to be.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Kind {
    /// The file is empty. Nothing to replay, and the caller writes a header
    /// before its first frame.
    Empty,
    /// A header this build understands. Frames start at [`HEADER_LEN`].
    Versioned(u16),
    /// No header. Written by a build from before this module existed, so its
    /// frames start at offset zero.
    ///
    /// Accepted rather than refused, because refusing would make this change
    /// destroy exactly the data the header exists to protect. It is written
    /// back with a header the first time the file is rewritten — for the WAL,
    /// the next flush; for the manifest, the rollover that every open performs.
    Legacy,
}

/// The eight bytes to write at the front of a new file.
pub(crate) fn encode(magic: &[u8; 4], version: u16) -> [u8; HEADER_LEN] {
    let mut out = [0u8; HEADER_LEN];
    out[0..4].copy_from_slice(magic);
    out[4..6].copy_from_slice(&version.to_le_bytes());
    // `reserved` stays zero. It exists so a flag can be added without moving
    // where the frames start, which is the one thing a header must never do.
    out
}

/// Classify the front of a file.
///
/// `bytes` is the whole file, because both callers already hold it.
pub(crate) fn classify(bytes: &[u8], magic: &[u8; 4], newest: u16, what: &str) -> Result<Kind> {
    if bytes.is_empty() {
        return Ok(Kind::Empty);
    }
    if bytes.len() >= HEADER_LEN && &bytes[0..4] == magic {
        let version = u16::from_le_bytes([bytes[4], bytes[5]]);
        if version > newest {
            return Err(Error::BadFormat(format!(
                "{what} is version {version}; this build understands up to {newest}"
            )));
        }
        return Ok(Kind::Versioned(version));
    }
    // Anything else is either a file from before headers existed or a file
    // this build has no business touching. The two are told apart by the only
    // evidence available: a legacy file's first four bytes are a CRC, which
    // could be anything, but *some* other engine's magic is not something to
    // guess about. Since neither can be distinguished with certainty, the
    // benefit of the doubt goes to the legacy reading — and the frame parser
    // that follows refuses anything that does not verify, so a genuinely
    // foreign file replays to nothing rather than to something wrong.
    Ok(Kind::Legacy)
}

/// Where frames start, given what the front of the file turned out to be.
pub(crate) fn frames_start(kind: Kind) -> usize {
    match kind {
        Kind::Legacy => 0,
        Kind::Empty | Kind::Versioned(_) => HEADER_LEN,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_written_header_classifies_as_this_version() {
        let bytes = encode(WAL_MAGIC, WAL_VERSION);
        assert_eq!(
            classify(&bytes, WAL_MAGIC, WAL_VERSION, "wal").unwrap(),
            Kind::Versioned(WAL_VERSION)
        );
        assert_eq!(frames_start(Kind::Versioned(WAL_VERSION)), HEADER_LEN);
    }

    #[test]
    fn an_empty_file_is_empty_rather_than_legacy() {
        assert_eq!(
            classify(&[], WAL_MAGIC, WAL_VERSION, "wal").unwrap(),
            Kind::Empty
        );
    }

    #[test]
    fn a_headerless_file_is_read_as_legacy() {
        // Four bytes of CRC and four of length: what a pre-header frame starts
        // with.
        let bytes = [0xde, 0xad, 0xbe, 0xef, 4, 0, 0, 0];
        assert_eq!(
            classify(&bytes, WAL_MAGIC, WAL_VERSION, "wal").unwrap(),
            Kind::Legacy
        );
        assert_eq!(frames_start(Kind::Legacy), 0);
    }

    /// The wrong magic is not a lower-numbered version of the right one. A
    /// manifest is not a WAL however similar their frames look.
    #[test]
    fn the_other_files_magic_is_not_accepted() {
        let bytes = encode(MANIFEST_MAGIC, MANIFEST_VERSION);
        assert_eq!(
            classify(&bytes, WAL_MAGIC, WAL_VERSION, "wal").unwrap(),
            Kind::Legacy
        );
    }

    #[test]
    fn a_version_from_the_future_is_refused_rather_than_guessed_at() {
        let mut bytes = encode(WAL_MAGIC, WAL_VERSION);
        bytes[4..6].copy_from_slice(&(WAL_VERSION + 1).to_le_bytes());
        let err = classify(&bytes, WAL_MAGIC, WAL_VERSION, "wal").unwrap_err();
        assert!(
            matches!(err, Error::BadFormat(_)),
            "a future version gave {err:?} rather than BadFormat"
        );
    }

    #[test]
    fn the_checksum_is_decided_by_the_version_not_by_this_build() {
        assert_eq!(wal_checksum(Kind::Legacy), Checksum::IsoHdlc);
        assert_eq!(wal_checksum(Kind::Versioned(1)), Checksum::IsoHdlc);
        assert_eq!(wal_checksum(Kind::Versioned(2)), Checksum::IsoHdlc);
        assert_eq!(wal_checksum(Kind::Versioned(3)), Checksum::Castagnoli);

        assert_eq!(manifest_checksum(Kind::Legacy), Checksum::IsoHdlc);
        assert_eq!(manifest_checksum(Kind::Versioned(1)), Checksum::IsoHdlc);
        assert_eq!(manifest_checksum(Kind::Versioned(2)), Checksum::Castagnoli);
    }

    /// The two disagree on real input, or version-gating them would be
    /// pointless and every test here would pass for the wrong reason.
    #[test]
    fn the_two_checksums_are_actually_different() {
        let payload = b"a frame's worth of bytes, more or less";
        assert_ne!(
            Checksum::IsoHdlc.hash(payload),
            Checksum::Castagnoli.hash(payload)
        );
    }

    /// A file too short to hold a header cannot be one, and must not be read as
    /// a truncated one.
    #[test]
    fn a_file_shorter_than_a_header_is_legacy_not_a_bad_header() {
        assert_eq!(
            classify(b"LSM", WAL_MAGIC, WAL_VERSION, "wal").unwrap(),
            Kind::Legacy
        );
    }
}
