//! Packed binary codec for durable co-liker profiles (`ucl:{hash}` keys).
//!
//! # Wire format
//!
//! A flat array of fixed-width entries, no header:
//!
//! ```text
//! [ 8-byte raw DID hash | 4-byte big-endian f32 score ] × K
//! ```
//!
//! # Why a packed string rather than a ZSET
//!
//! Profiles are rebuilt wholesale by a nightly batch job and always read in full, so
//! none of a ZSET's incremental machinery (`ZADD`, `ZINCRBY`, `ZREMRANGEBYRANK`) or
//! range-access capability is ever used. Measured on the production instance at K=128:
//!
//! | representation                | bytes/user |
//! |-------------------------------|-----------:|
//! | ZSET (16-hex member + score)  |      3,632 |
//! | packed string (this format)   |  **1,840** |
//!
//! A single `GET` of 1,840 bytes also beats a 128-member `ZREVRANGE` on both round-trip
//! payload and allocations.
//!
//! Note K=128 is not arbitrary: a ZSET crosses the listpack→skiplist boundary at 128
//! members, where per-member cost jumps from 28.4 to 85.0 bytes. Keeping profiles at or
//! below 128 keeps every representation cheap.
//!
//! # Why f32 for the score
//!
//! Scores are `Σ 1/L_j` over overlapping posts — in practice ~1e-3 to ~1e2. f32 carries
//! ~7 significant decimal digits, far more than needed to order 128 neighbours, and saves
//! 4 bytes per entry (25% of the payload) against f64.

use crate::redis::hash_did;

/// Raw bytes of a DID hash (`hash_did` returns this as 16 hex chars).
pub const HASH_BYTES: usize = 8;

/// Hex-encoded length of a DID hash.
pub const HASH_HEX_LEN: usize = HASH_BYTES * 2;

/// Bytes per profile entry: 8-byte hash + 4-byte f32 score.
pub const PROFILE_ENTRY_BYTES: usize = HASH_BYTES + 4;

/// Encode `(coliker_hash_hex, score)` pairs into the packed wire format.
///
/// Entries whose hash is not exactly [`HASH_HEX_LEN`] valid hex characters are skipped —
/// a malformed row from an upstream query should not poison an entire profile. Input
/// order is preserved, so callers should pass entries already sorted by descending score.
pub fn encode_profile(entries: &[(String, f64)]) -> Vec<u8> {
    let mut out = Vec::with_capacity(entries.len() * PROFILE_ENTRY_BYTES);
    for (hash_hex, score) in entries {
        if hash_hex.len() != HASH_HEX_LEN {
            continue;
        }
        let mut raw = [0u8; HASH_BYTES];
        if hex::decode_to_slice(hash_hex.as_bytes(), &mut raw).is_err() {
            continue;
        }
        out.extend_from_slice(&raw);
        out.extend_from_slice(&(*score as f32).to_be_bytes());
    }
    out
}

/// Decode packed bytes back into `(coliker_hash_hex, score)` pairs.
///
/// A trailing partial entry is ignored rather than treated as an error, so a truncated
/// value degrades to a shorter profile instead of failing the request.
pub fn decode_profile(bytes: &[u8]) -> Vec<(String, f64)> {
    let n = bytes.len() / PROFILE_ENTRY_BYTES;
    let mut out = Vec::with_capacity(n);
    for chunk in bytes.chunks_exact(PROFILE_ENTRY_BYTES) {
        let hash_hex = hex::encode(&chunk[..HASH_BYTES]);
        let score = f32::from_be_bytes([
            chunk[HASH_BYTES],
            chunk[HASH_BYTES + 1],
            chunk[HASH_BYTES + 2],
            chunk[HASH_BYTES + 3],
        ]);
        out.push((hash_hex, score as f64));
    }
    out
}

/// Number of entries a packed profile holds, without decoding it.
#[inline]
pub fn profile_len(bytes: &[u8]) -> usize {
    bytes.len() / PROFILE_ENTRY_BYTES
}

/// Encode `(coliker_did, score)` pairs, hashing each DID first.
///
/// Convenience for the batch builder, which reads raw DIDs out of ClickHouse.
pub fn encode_profile_from_dids(entries: &[(String, f64)]) -> Vec<u8> {
    let hashed: Vec<(String, f64)> = entries
        .iter()
        .map(|(did, score)| (hash_did(did), *score))
        .collect();
    encode_profile(&hashed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_preserves_order_and_hashes() {
        let entries = vec![
            ("0123456789abcdef".to_string(), 12.5_f64),
            ("fedcba9876543210".to_string(), 3.25_f64),
            ("00000000000000ff".to_string(), 0.125_f64),
        ];
        let packed = encode_profile(&entries);
        assert_eq!(packed.len(), 3 * PROFILE_ENTRY_BYTES);
        assert_eq!(profile_len(&packed), 3);

        let decoded = decode_profile(&packed);
        assert_eq!(decoded.len(), 3);
        for (i, (hash, score)) in decoded.iter().enumerate() {
            assert_eq!(*hash, entries[i].0);
            // 12.5 / 3.25 / 0.125 are all exactly representable in f32.
            assert_eq!(*score, entries[i].1);
        }
    }

    #[test]
    fn packed_size_matches_design_budget() {
        // 128 entries must be 1,536 bytes on the wire; the production measurement of
        // 1,840 bytes of Valkey memory is this plus per-key overhead.
        let entries: Vec<(String, f64)> = (0..128)
            .map(|i| (format!("{:016x}", i), 1.0 / (i + 1) as f64))
            .collect();
        assert_eq!(encode_profile(&entries).len(), 1_536);
    }

    #[test]
    fn skips_malformed_hashes_without_dropping_the_profile() {
        let entries = vec![
            ("0123456789abcdef".to_string(), 1.0),
            ("tooshort".to_string(), 2.0),         // wrong length
            ("zzzzzzzzzzzzzzzz".to_string(), 3.0), // not hex
            ("fedcba9876543210".to_string(), 4.0),
        ];
        let decoded = decode_profile(&encode_profile(&entries));
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].0, "0123456789abcdef");
        assert_eq!(decoded[1].0, "fedcba9876543210");
    }

    #[test]
    fn truncated_value_degrades_to_shorter_profile() {
        let entries = vec![
            ("0123456789abcdef".to_string(), 1.0),
            ("fedcba9876543210".to_string(), 2.0),
        ];
        let mut packed = encode_profile(&entries);
        packed.truncate(PROFILE_ENTRY_BYTES + 5); // one whole entry + a partial
        let decoded = decode_profile(&packed);
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].0, "0123456789abcdef");
    }

    #[test]
    fn empty_input_encodes_to_empty() {
        assert!(encode_profile(&[]).is_empty());
        assert!(decode_profile(&[]).is_empty());
        assert_eq!(profile_len(&[]), 0);
    }

    #[test]
    fn dids_are_hashed_to_16_hex_chars() {
        let entries = vec![("did:plc:65lxax66ewvlshze4ytsdohk".to_string(), 1.5)];
        let decoded = decode_profile(&encode_profile_from_dids(&entries));
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].0.len(), HASH_HEX_LEN);
        assert_eq!(decoded[0].0, hash_did("did:plc:65lxax66ewvlshze4ytsdohk"));
    }

    #[test]
    fn score_ordering_survives_f32_narrowing() {
        // Ranking is what matters; confirm a realistic Sum(1/L_j) spread stays ordered.
        let entries: Vec<(String, f64)> = (0..128)
            .map(|i| (format!("{:016x}", i), 10.0 / (i + 1) as f64))
            .collect();
        let decoded = decode_profile(&encode_profile(&entries));
        for w in decoded.windows(2) {
            assert!(w[0].1 > w[1].1, "order broken at {:?}", w);
        }
    }
}
