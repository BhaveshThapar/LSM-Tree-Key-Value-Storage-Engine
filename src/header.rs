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

/// Format version written by this build.
pub(crate) const VERSION: u16 = 1;

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
pub(crate) fn encode(magic: &[u8; 4]) -> [u8; HEADER_LEN] {
    let mut out = [0u8; HEADER_LEN];
    out[0..4].copy_from_slice(magic);
    out[4..6].copy_from_slice(&VERSION.to_le_bytes());
    // `reserved` stays zero. It exists so a flag can be added without moving
    // where the frames start, which is the one thing a header must never do.
    out
}

/// Classify the front of a file.
///
/// `bytes` is the whole file, because both callers already hold it.
pub(crate) fn classify(bytes: &[u8], magic: &[u8; 4], what: &str) -> Result<Kind> {
    if bytes.is_empty() {
        return Ok(Kind::Empty);
    }
    if bytes.len() >= HEADER_LEN && &bytes[0..4] == magic {
        let version = u16::from_le_bytes([bytes[4], bytes[5]]);
        if version > VERSION {
            return Err(Error::BadFormat(format!(
                "{what} is version {version}; this build understands up to {VERSION}"
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
        let bytes = encode(WAL_MAGIC);
        assert_eq!(
            classify(&bytes, WAL_MAGIC, "wal").unwrap(),
            Kind::Versioned(VERSION)
        );
        assert_eq!(frames_start(Kind::Versioned(VERSION)), HEADER_LEN);
    }

    #[test]
    fn an_empty_file_is_empty_rather_than_legacy() {
        assert_eq!(classify(&[], WAL_MAGIC, "wal").unwrap(), Kind::Empty);
    }

    #[test]
    fn a_headerless_file_is_read_as_legacy() {
        // Four bytes of CRC and four of length: what a pre-header frame starts
        // with.
        let bytes = [0xde, 0xad, 0xbe, 0xef, 4, 0, 0, 0];
        assert_eq!(classify(&bytes, WAL_MAGIC, "wal").unwrap(), Kind::Legacy);
        assert_eq!(frames_start(Kind::Legacy), 0);
    }

    /// The wrong magic is not a lower-numbered version of the right one. A
    /// manifest is not a WAL however similar their frames look.
    #[test]
    fn the_other_files_magic_is_not_accepted() {
        let bytes = encode(MANIFEST_MAGIC);
        assert_eq!(classify(&bytes, WAL_MAGIC, "wal").unwrap(), Kind::Legacy);
    }

    #[test]
    fn a_version_from_the_future_is_refused_rather_than_guessed_at() {
        let mut bytes = encode(WAL_MAGIC);
        bytes[4..6].copy_from_slice(&(VERSION + 1).to_le_bytes());
        let err = classify(&bytes, WAL_MAGIC, "wal").unwrap_err();
        assert!(
            matches!(err, Error::BadFormat(_)),
            "a future version gave {err:?} rather than BadFormat"
        );
    }

    /// A file too short to hold a header cannot be one, and must not be read as
    /// a truncated one.
    #[test]
    fn a_file_shorter_than_a_header_is_legacy_not_a_bad_header() {
        assert_eq!(classify(b"LSM", WAL_MAGIC, "wal").unwrap(), Kind::Legacy);
    }
}
