//! Shared record type and binary encoding used by both the WAL and SSTables.
//!
//! On-disk layout of one record payload:
//! `[seq:8][type:1][key_len:4][key][val_len:4][val]` (all little-endian).
//! `type` is 0 for a put and 1 for a tombstone (delete marker).

use crate::error::{Error, Result};

/// A single versioned key/value mutation. `value == None` is a tombstone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    pub seq: u64,
}

impl Record {
    pub fn put(key: Vec<u8>, value: Vec<u8>, seq: u64) -> Self {
        Record {
            key,
            value: Some(value),
            seq,
        }
    }

    pub fn tombstone(key: Vec<u8>, seq: u64) -> Self {
        Record {
            key,
            value: None,
            seq,
        }
    }

    /// Serialized byte length of this record's payload.
    pub fn encoded_len(&self) -> usize {
        8 + 1 + 4 + self.key.len() + 4 + self.value.as_ref().map_or(0, |v| v.len())
    }

    /// Append the record's payload encoding to `buf`.
    pub fn encode_into(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.seq.to_le_bytes());
        buf.push(if self.value.is_some() { 0 } else { 1 });
        buf.extend_from_slice(&(self.key.len() as u32).to_le_bytes());
        buf.extend_from_slice(&self.key);
        match &self.value {
            Some(v) => {
                buf.extend_from_slice(&(v.len() as u32).to_le_bytes());
                buf.extend_from_slice(v);
            }
            None => buf.extend_from_slice(&0u32.to_le_bytes()),
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.encoded_len());
        self.encode_into(&mut buf);
        buf
    }

    /// Decode one record starting at `buf[*pos..]`, advancing `pos` past it.
    pub fn decode_at(buf: &[u8], pos: &mut usize) -> Result<Record> {
        let mut r = Reader { buf, pos: *pos };
        let seq = r.u64()?;
        let kind = r.u8()?;
        let key_len = r.u32()? as usize;
        let key = r.bytes(key_len)?.to_vec();
        let val_len = r.u32()? as usize;
        let val = r.bytes(val_len)?.to_vec();
        let value = match kind {
            0 => Some(val),
            1 => None,
            other => return Err(Error::Corrupt(format!("bad record type {other}"))),
        };
        *pos = r.pos;
        Ok(Record { key, value, seq })
    }
}

/// Bounds-checked little-endian cursor over a byte slice.
struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn bytes(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self
            .pos
            .checked_add(n)
            .filter(|&e| e <= self.buf.len())
            .ok_or_else(|| Error::Corrupt("record truncated".into()))?;
        let out = &self.buf[self.pos..end];
        self.pos = end;
        Ok(out)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.bytes(1)?[0])
    }

    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.bytes(4)?.try_into().unwrap()))
    }

    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.bytes(8)?.try_into().unwrap()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_round_trip() {
        let r = Record::put(b"key".to_vec(), b"value".to_vec(), 7);
        let bytes = r.encode();
        assert_eq!(bytes.len(), r.encoded_len());
        let mut pos = 0;
        let decoded = Record::decode_at(&bytes, &mut pos).unwrap();
        assert_eq!(decoded, r);
        assert_eq!(pos, bytes.len());
    }

    #[test]
    fn tombstone_round_trip() {
        let r = Record::tombstone(b"gone".to_vec(), 99);
        let mut pos = 0;
        let decoded = Record::decode_at(&r.encode(), &mut pos).unwrap();
        assert_eq!(decoded, r);
        assert!(decoded.value.is_none());
    }

    #[test]
    fn truncated_input_errors() {
        let bytes = Record::put(b"k".to_vec(), b"v".to_vec(), 1).encode();
        let mut pos = 0;
        assert!(Record::decode_at(&bytes[..bytes.len() - 1], &mut pos).is_err());
    }

    #[test]
    fn decodes_consecutive_records() {
        let mut buf = Vec::new();
        Record::put(b"a".to_vec(), b"1".to_vec(), 1).encode_into(&mut buf);
        Record::tombstone(b"b".to_vec(), 2).encode_into(&mut buf);
        let mut pos = 0;
        let first = Record::decode_at(&buf, &mut pos).unwrap();
        let second = Record::decode_at(&buf, &mut pos).unwrap();
        assert_eq!(first.key, b"a");
        assert_eq!(second.key, b"b");
        assert_eq!(pos, buf.len());
    }
}
