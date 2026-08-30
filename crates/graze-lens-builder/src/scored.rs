//! The v2 scored lens blob.
//!
//! A v1 lens was a Redis SET of DIDs: membership, yes or no. That cannot express
//! "this author is in your second degree, reached by 40 of the people you
//! follow" — and without a degree of belief there is nothing to rank by, so a
//! feed can only be filtered, never gracefully degraded. Scores are what let a
//! lens fall back instead of failing.
//!
//! # Layout
//!
//! ```text
//! header, 16 bytes
//!   0..4    magic  b"GLZ2"
//!   4       version = 2
//!   5       facet id
//!   6       id space (0 = the shared didint space, 1 = the lens-owned one)
//!   7       reserved (zero)
//!   8..12   entry count, u32 LE
//!   12..16  built_at, u32 LE unix seconds
//! entries, 6 bytes each, SORTED ASCENDING BY ID
//!   +0..4   didint, u32 LE
//!   +4..6   weight, u16 LE fixed-point (65535 == 1.0)
//! ```
//!
//! Two choices worth defending:
//!
//! **Sorted, so the reader never decodes.** The serve path binary-searches the
//! bytes as they came out of Redis — no allocation, no HashMap construction, no
//! deserialization step on the request path. Decoding a 250k-entry map per cache
//! miss would cost more than the lookup it enables.
//!
//! **u16 fixed-point weights, not f32/f16.** Weights only ever rank; two bytes
//! give ~4½ digits of usable precision, which is far more than a ranking needs,
//! and it avoids both a half-float dependency and the NaN ordering questions
//! that come with floats in a sorted structure.
//!
//! # Cross-repo
//!
//! feeder-rs decodes this in another repository. The layout above and
//! `GOLDEN_VECTOR` in the tests are the contract; both sides assert the same
//! bytes.

pub const MAGIC: &[u8; 4] = b"GLZ2";
pub const VERSION: u8 = 2;
pub const HEADER_LEN: usize = 16;
pub const ENTRY_LEN: usize = 6;

/// Which u32 id space this blob's entries belong to.
///
/// Ids are only meaningful relative to the interner that issued them, and two
/// interners will happily assign the same u32 to different accounts. A blob
/// built against one space and read against another does not error — every
/// lookup simply resolves to the wrong person, which reads as a lens that
/// filters strangely rather than one that is broken. Stamping the space here
/// turns that into a blob the reader refuses.
pub const IDSPACE_SHARED: u8 = 0;
pub const IDSPACE_LENS: u8 = 1;

/// Facet ids. Stored as one byte in the header so a reader can reject a blob
/// built for a different signal rather than silently ranking by the wrong one.
pub const FACET_FOLLOWS: u8 = 1;
pub const FACET_FOLLOWS2: u8 = 2;

/// The full-confidence weight. A first-degree follow is exactly this.
pub const WEIGHT_MAX: u16 = u16::MAX;

/// Turn a 0.0–1.0 confidence into the stored fixed-point weight.
///
/// Clamped rather than wrapping: a scorer that hands us 1.4 has a bug, and
/// wrapping would turn its strongest signal into its weakest.
pub fn weight_from_f32(v: f32) -> u16 {
    let clamped = v.clamp(0.0, 1.0);
    (clamped * WEIGHT_MAX as f32).round() as u16
}

/// Encode entries into a blob. Input need not be sorted; output always is.
pub fn encode(facet: u8, built_at: u32, entries: Vec<(u32, u16)>) -> Vec<u8> {
    encode_in_space(facet, IDSPACE_SHARED, built_at, entries)
}

/// Encode, stamping the id space the entries were interned against.
pub fn encode_in_space(
    facet: u8,
    idspace: u8,
    built_at: u32,
    mut entries: Vec<(u32, u16)>,
) -> Vec<u8> {
    entries.sort_unstable_by_key(|(id, _)| *id);
    entries.dedup_by_key(|(id, _)| *id);

    let mut out = Vec::with_capacity(HEADER_LEN + entries.len() * ENTRY_LEN);
    out.extend_from_slice(MAGIC);
    out.push(VERSION);
    out.push(facet);
    out.push(idspace);
    out.push(0u8);
    out.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    out.extend_from_slice(&built_at.to_le_bytes());
    for (id, weight) in entries {
        out.extend_from_slice(&id.to_le_bytes());
        out.extend_from_slice(&weight.to_le_bytes());
    }
    out
}

/// A blob's header, once validated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
    pub facet: u8,
    pub idspace: u8,
    pub count: u32,
    pub built_at: u32,
}

/// Validate and read the header, or `None` if this is not a v2 blob.
///
/// Returns `None` rather than erroring for a v1 set or truncated value, so a
/// reader can fall back instead of failing.
pub fn header(blob: &[u8]) -> Option<Header> {
    if blob.len() < HEADER_LEN || &blob[0..4] != MAGIC || blob[4] != VERSION {
        return None;
    }
    let count = u32::from_le_bytes(blob[8..12].try_into().ok()?);
    // A blob shorter than its declared entries is truncation or corruption;
    // trusting the count would read past the end. Longer is legal: the space
    // after the entries belongs to optional trailers (the bloom), which the
    // entries section — being exactly sized by `count` — safely delimits.
    if blob.len() < HEADER_LEN + count as usize * ENTRY_LEN {
        return None;
    }
    Some(Header {
        facet: blob[5],
        idspace: blob[6],
        count,
        built_at: u32::from_le_bytes(blob[12..16].try_into().ok()?),
    })
}

/// The weight for one id, by binary search over the raw bytes.
///
/// No decoding, no allocation. `None` means the author is not in this lens.
pub fn weight_of(blob: &[u8], id: u32) -> Option<u16> {
    let h = header(blob)?;
    let (mut lo, mut hi) = (0usize, h.count as usize);
    while lo < hi {
        let mid = (lo + hi) / 2;
        let at = HEADER_LEN + mid * ENTRY_LEN;
        let candidate = u32::from_le_bytes(blob[at..at + 4].try_into().ok()?);
        match candidate.cmp(&id) {
            std::cmp::Ordering::Less => lo = mid + 1,
            std::cmp::Ordering::Greater => hi = mid,
            std::cmp::Ordering::Equal => {
                return Some(u16::from_le_bytes(blob[at + 4..at + 6].try_into().ok()?));
            }
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Bloom trailer: the approximate tail of a lens.
//
// The scored entries carry the top-K, but second degree runs to hundreds of
// thousands of members, and shipping them all defeats the size budget. The
// bloom answers "is this author in the lens AT ALL" for everyone past the
// top-K, at ~1 byte per member.
//
// The error direction is what makes an approximation acceptable here: a bloom
// has false POSITIVES only. A stranger occasionally passes the lens with
// epsilon weight and ranks last — an extra post shown. It can never hide a
// genuinely-followed author, because membership never reads false for a real
// member. Inclusive errors degrade gracefully; exclusive ones would not.
//
// Hashing is FNV-1a/splitmix double-hashing implemented inline, because the
// bits are read by feeder-rs in another repo and std's SipHash keys are
// deliberately unstable across processes.
// ---------------------------------------------------------------------------

pub const BLOOM_MAGIC: &[u8; 4] = b"BLM1";
/// Bits per member. 8 → ~2.1% false positives with k=6, chosen over 10 bits/1%
/// because the marginal 60KB per viewer buys almost nothing at epsilon weight.
pub const BLOOM_BITS_PER_MEMBER: usize = 8;
pub const BLOOM_K: u32 = 6;

fn fnv1a(id: u32) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in id.to_le_bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

fn splitmix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e3779b97f4a7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
    x ^ (x >> 31)
}

/// Append a bloom of `members` to an encoded blob.
pub fn append_bloom(blob: &mut Vec<u8>, members: &[u32]) {
    let m_bits = (members.len().max(1) * BLOOM_BITS_PER_MEMBER).next_power_of_two() as u32;
    let mut bits = vec![0u8; m_bits as usize / 8];
    for &id in members {
        let (h1, h2) = (fnv1a(id), splitmix(id as u64));
        for i in 0..BLOOM_K as u64 {
            let bit = (h1.wrapping_add(i.wrapping_mul(h2)) % m_bits as u64) as usize;
            bits[bit / 8] |= 1 << (bit % 8);
        }
    }
    blob.extend_from_slice(BLOOM_MAGIC);
    blob.extend_from_slice(&m_bits.to_le_bytes());
    blob.extend_from_slice(&BLOOM_K.to_le_bytes());
    blob.extend_from_slice(&bits);
}

/// Where the bloom trailer starts, if the blob carries one.
fn bloom_at(blob: &[u8]) -> Option<usize> {
    let h = header(blob)?;
    let at = HEADER_LEN + h.count as usize * ENTRY_LEN;
    if blob.len() >= at + 12 && &blob[at..at + 4] == BLOOM_MAGIC {
        Some(at)
    } else {
        None
    }
}

/// Approximate membership via the bloom trailer.
///
/// `None` means "no bloom present" — distinct from `Some(false)`, so a caller
/// can fall back to exact-only behaviour on blobs built without one.
pub fn bloom_contains(blob: &[u8], id: u32) -> Option<bool> {
    let at = bloom_at(blob)?;
    let m_bits = u32::from_le_bytes(blob[at + 4..at + 8].try_into().ok()?);
    let k = u32::from_le_bytes(blob[at + 8..at + 12].try_into().ok()?);
    let bits = &blob[at + 12..];
    if m_bits as usize / 8 > bits.len() || m_bits == 0 {
        return None;
    }
    let (h1, h2) = (fnv1a(id), splitmix(id as u64));
    for i in 0..k as u64 {
        let bit = (h1.wrapping_add(i.wrapping_mul(h2)) % m_bits as u64) as usize;
        if bits[bit / 8] & (1 << (bit % 8)) == 0 {
            return Some(false);
        }
    }
    Some(true)
}

// The layout of the header means the entries section is self-delimiting, so a
// v2 reader that predates blooms simply never looks past the entries — the
// trailer is backward compatible by construction.

#[cfg(test)]
mod tests {
    use super::*;

    /// The bytes feeder-rs must agree on. If this changes, the other repo's
    /// decoder changes with it or lenses silently stop matching.
    const GOLDEN_FACET: u8 = FACET_FOLLOWS;
    const GOLDEN_BUILT_AT: u32 = 1_788_000_000;

    fn golden() -> Vec<u8> {
        encode(
            GOLDEN_FACET,
            GOLDEN_BUILT_AT,
            vec![(700_530, WEIGHT_MAX), (42, 1000), (4_875_165, 32_768)],
        )
    }

    #[test]
    fn golden_vector_is_stable() {
        let blob = golden();
        assert_eq!(&blob[0..4], MAGIC);
        assert_eq!(blob[4], VERSION);
        assert_eq!(blob[5], FACET_FOLLOWS);
        assert_eq!(blob.len(), HEADER_LEN + 3 * ENTRY_LEN);
        let h = header(&blob).expect("valid header");
        assert_eq!(h.count, 3);
        assert_eq!(h.built_at, GOLDEN_BUILT_AT);
    }

    /// Entries must come out ascending regardless of input order — the reader's
    /// binary search is only correct on sorted data, and an unsorted blob would
    /// return wrong answers rather than errors.
    #[test]
    fn entries_are_sorted_regardless_of_input_order() {
        let blob = golden();
        let mut prev = 0u32;
        for i in 0..3 {
            let at = HEADER_LEN + i * ENTRY_LEN;
            let id = u32::from_le_bytes(blob[at..at + 4].try_into().unwrap());
            assert!(id > prev, "ids must ascend: {id} after {prev}");
            prev = id;
        }
    }

    #[test]
    fn lookups_find_every_member_and_reject_others() {
        let blob = golden();
        assert_eq!(weight_of(&blob, 700_530), Some(WEIGHT_MAX));
        assert_eq!(weight_of(&blob, 42), Some(1000));
        assert_eq!(weight_of(&blob, 4_875_165), Some(32_768));
        assert_eq!(weight_of(&blob, 999), None);
        assert_eq!(weight_of(&blob, 0), None);
        assert_eq!(weight_of(&blob, u32::MAX), None);
    }

    /// A v1 SET value or any other junk must read as "not a v2 blob" so the
    /// caller can fall back, never as an empty lens — an empty lens would
    /// filter the reader's feed to nothing.
    #[test]
    fn foreign_values_are_rejected_not_misread() {
        assert!(header(b"did:plc:abc").is_none());
        assert!(header(b"").is_none());
        assert!(header(b"GLZ2").is_none());
        assert!(weight_of(b"did:plc:abc", 42).is_none());
    }

    /// A truncated blob must be rejected outright. Trusting the header's count
    /// would read past the end of the buffer.
    #[test]
    fn truncated_blobs_are_rejected() {
        let mut blob = golden();
        blob.truncate(blob.len() - 1);
        assert!(header(&blob).is_none());
        assert!(weight_of(&blob, 42).is_none());
    }

    /// A weight above full confidence is a scorer bug; clamping keeps the
    /// strongest signal strongest, where wrapping would make it the weakest.
    #[test]
    fn out_of_range_weights_clamp_rather_than_wrap() {
        assert_eq!(weight_from_f32(1.0), WEIGHT_MAX);
        assert_eq!(weight_from_f32(1.4), WEIGHT_MAX);
        assert_eq!(weight_from_f32(0.0), 0);
        assert_eq!(weight_from_f32(-3.0), 0);
    }

    /// Duplicate ids would break binary search's assumption of a unique key and
    /// waste space; the encoder collapses them.
    #[test]
    fn duplicate_ids_are_collapsed() {
        let blob = encode(FACET_FOLLOWS, 0, vec![(7, 10), (7, 20), (9, 30)]);
        assert_eq!(header(&blob).unwrap().count, 2);
        assert!(weight_of(&blob, 7).is_some());
        assert_eq!(weight_of(&blob, 9), Some(30));
    }

    /// The bloom must never produce a false negative — that would hide a
    /// genuinely-followed author, the one error direction we cannot accept.
    #[test]
    fn bloom_has_no_false_negatives() {
        let members: Vec<u32> = (0..50_000u32).map(|i| i.wrapping_mul(2654435761)).collect();
        let mut blob = encode(FACET_FOLLOWS2, 0, vec![(1, 1)]);
        append_bloom(&mut blob, &members);
        for &m in &members {
            assert_eq!(
                bloom_contains(&blob, m),
                Some(true),
                "false negative for {m}"
            );
        }
    }

    /// And its false-positive rate must be near the design point (~2.1% at 8
    /// bits/member, k=6). Well above that means the sizing math is wrong and
    /// strangers pour through the lens.
    #[test]
    fn bloom_false_positive_rate_is_near_design() {
        let members: Vec<u32> = (0..100_000u32).map(|i| i * 7 + 1).collect();
        let mut blob = encode(FACET_FOLLOWS2, 0, vec![(1, 1)]);
        append_bloom(&mut blob, &members);
        let mut fp = 0u32;
        let trials = 100_000u32;
        for i in 0..trials {
            let candidate = 1_000_000_000u32 + i; // disjoint from members
            if bloom_contains(&blob, candidate) == Some(true) {
                fp += 1;
            }
        }
        let rate = fp as f64 / trials as f64;
        assert!(
            rate < 0.035,
            "fp rate {rate:.4} far above the ~2.1% design point"
        );
        assert!(
            rate > 0.001,
            "fp rate {rate:.4} suspiciously low; bloom likely broken"
        );
    }

    /// A blob without a trailer answers None — "no bloom", not "not a member" —
    /// so readers can distinguish exact-only blobs from misses.
    #[test]
    fn absent_bloom_is_none_not_false() {
        let blob = golden();
        assert_eq!(bloom_contains(&blob, 42), None);
    }

    /// A blob with a bloom must still serve exact lookups: the trailer cannot
    /// break the entries section it follows.
    #[test]
    fn entries_survive_a_bloom_trailer() {
        let mut blob = golden();
        append_bloom(&mut blob, &[9, 10, 11]);
        assert_eq!(weight_of(&blob, 700_530), Some(WEIGHT_MAX));
        assert_eq!(weight_of(&blob, 999), None);
        assert!(header(&blob).is_some());
        assert_eq!(bloom_contains(&blob, 9), Some(true));
    }

    /// Serve-path cost check: both lookups must be far under a microsecond, so
    /// a 30-post page costs tens of microseconds, invisible next to a 28ms p99.
    #[test]
    fn lookups_are_sub_microsecond() {
        let entries: Vec<(u32, u16)> = (0..20_000u32).map(|i| (i * 13, 1u16)).collect();
        let members: Vec<u32> = (0..250_000u32).map(|i| i * 11).collect();
        let mut blob = encode(FACET_FOLLOWS2, 0, entries);
        append_bloom(&mut blob, &members);

        let start = std::time::Instant::now();
        let mut hits = 0u64;
        for i in 0..1_000_000u32 {
            if weight_of(&blob, i).is_some() {
                hits += 1;
            }
        }
        let per_search = start.elapsed().as_nanos() / 1_000_000;

        let start = std::time::Instant::now();
        for i in 0..1_000_000u32 {
            if bloom_contains(&blob, i) == Some(true) {
                hits += 1;
            }
        }
        let per_bloom = start.elapsed().as_nanos() / 1_000_000;

        eprintln!(
            "binary search: {per_search} ns/lookup, bloom: {per_bloom} ns/lookup (hits={hits})"
        );
        // Debug builds are ~10x slower than release; 5µs here is ~200-500ns released.
        assert!(
            per_search < 5_000,
            "binary search {per_search} ns is too slow"
        );
        assert!(per_bloom < 5_000, "bloom probe {per_bloom} ns is too slow");
    }

    #[test]
    fn empty_map_round_trips() {
        let blob = encode(FACET_FOLLOWS, 5, vec![]);
        let h = header(&blob).expect("empty is still a valid blob");
        assert_eq!(h.count, 0);
        assert_eq!(weight_of(&blob, 1), None);
    }

    /// Scale sanity: the size claim that justified interning at all.
    #[test]
    fn size_is_six_bytes_per_entry() {
        let blob = encode(FACET_FOLLOWS2, 0, (0..250_000).map(|i| (i, 1u16)).collect());
        assert_eq!(blob.len(), HEADER_LEN + 250_000 * 6);
        assert!(blob.len() < 1_600_000, "250k entries should be ~1.5 MB");
    }
}
