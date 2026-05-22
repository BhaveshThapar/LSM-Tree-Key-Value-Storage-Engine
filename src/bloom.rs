//! A per-SSTable Bloom filter: a compact probabilistic set that answers
//! "is this key definitely absent?". Checked before any block I/O on a point
//! lookup, it lets a reader skip SSTables that cannot contain the key.
//!
//! `k` bit positions are derived from a single 64-bit hash by *double hashing*
//! (`bit_i = h1 + i*h2`), which performs as well as `k` independent hashes.
//!
//! Encoded layout: `[k:4][bit_count:8][bits…]` (little-endian).

use xxhash_rust::xxh3::xxh3_64_with_seed;

use crate::error::{Error, Result};

const LN2: f64 = std::f64::consts::LN_2;

/// A fixed-size Bloom filter sized for a target false-positive rate.
pub struct BloomFilter {
    bits: Vec<u8>,
    bit_count: u64,
    k: u32,
}

impl BloomFilter {
    /// Build an empty filter sized for `expected_n` keys at `fp_rate`.
    pub fn new(expected_n: usize, fp_rate: f64) -> BloomFilter {
        let n = expected_n.max(1) as f64;
        // m = -n*ln(p) / (ln2)^2 ; k = (m/n)*ln2
        let m = (-n * fp_rate.ln() / (LN2 * LN2)).ceil().max(1.0);
        let bit_count = m as u64;
        let k = ((bit_count as f64 / n) * LN2).round().max(1.0) as u32;
        let byte_len = bit_count.div_ceil(8) as usize;
        BloomFilter {
            bits: vec![0u8; byte_len],
            bit_count,
            k,
        }
    }

    fn hashes(&self, key: &[u8]) -> (u64, u64) {
        (xxh3_64_with_seed(key, 0), xxh3_64_with_seed(key, 1))
    }

    /// Record `key` as a member.
    pub fn insert(&mut self, key: &[u8]) {
        let (h1, h2) = self.hashes(key);
        for i in 0..self.k as u64 {
            let bit = h1.wrapping_add(i.wrapping_mul(h2)) % self.bit_count;
            self.bits[(bit / 8) as usize] |= 1 << (bit % 8);
        }
    }

    /// Return `false` only if `key` is definitely not a member; `true` means
    /// "possibly present" (subject to the configured false-positive rate).
    pub fn contains(&self, key: &[u8]) -> bool {
        let (h1, h2) = self.hashes(key);
        for i in 0..self.k as u64 {
            let bit = h1.wrapping_add(i.wrapping_mul(h2)) % self.bit_count;
            if self.bits[(bit / 8) as usize] & (1 << (bit % 8)) == 0 {
                return false;
            }
        }
        true
    }

    /// Serialize the filter: `[k:4][bit_count:8][bits…]`.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(12 + self.bits.len());
        buf.extend_from_slice(&self.k.to_le_bytes());
        buf.extend_from_slice(&self.bit_count.to_le_bytes());
        buf.extend_from_slice(&self.bits);
        buf
    }

    /// Parse a filter previously produced by [`BloomFilter::encode`].
    pub fn decode(buf: &[u8]) -> Result<BloomFilter> {
        if buf.len() < 12 {
            return Err(Error::Corrupt("bloom filter truncated".into()));
        }
        let k = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        let bit_count = u64::from_le_bytes(buf[4..12].try_into().unwrap());
        let byte_len = bit_count.div_ceil(8) as usize;
        if buf.len() != 12 + byte_len {
            return Err(Error::Corrupt("bloom filter length mismatch".into()));
        }
        if k == 0 || bit_count == 0 {
            return Err(Error::Corrupt("bloom filter has zero parameters".into()));
        }
        Ok(BloomFilter {
            bits: buf[12..].to_vec(),
            bit_count,
            k,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_false_negatives() {
        let mut f = BloomFilter::new(1000, 0.01);
        for i in 0..1000u32 {
            f.insert(&i.to_le_bytes());
        }
        for i in 0..1000u32 {
            assert!(f.contains(&i.to_le_bytes()), "missing key {i}");
        }
    }

    #[test]
    fn false_positive_rate_is_near_target() {
        let n = 10_000u32;
        let mut f = BloomFilter::new(n as usize, 0.01);
        for i in 0..n {
            f.insert(&i.to_le_bytes());
        }
        let mut fp = 0;
        let trials = 10_000u32;
        for i in n..n + trials {
            if f.contains(&i.to_le_bytes()) {
                fp += 1;
            }
        }
        let rate = fp as f64 / trials as f64;
        assert!(rate < 0.03, "false-positive rate too high: {rate}");
    }

    #[test]
    fn encode_decode_round_trip() {
        let mut f = BloomFilter::new(500, 0.01);
        for i in 0..500u32 {
            f.insert(&i.to_le_bytes());
        }
        let decoded = BloomFilter::decode(&f.encode()).unwrap();
        assert_eq!(decoded.k, f.k);
        assert_eq!(decoded.bit_count, f.bit_count);
        for i in 0..500u32 {
            assert!(decoded.contains(&i.to_le_bytes()));
        }
    }

    #[test]
    fn empty_filter_is_valid() {
        let f = BloomFilter::new(0, 0.01);
        assert!(!f.contains(b"anything"));
        let decoded = BloomFilter::decode(&f.encode()).unwrap();
        assert!(!decoded.contains(b"anything"));
    }

    #[test]
    fn decode_rejects_short_buffer() {
        assert!(BloomFilter::decode(&[0u8; 4]).is_err());
    }
}
