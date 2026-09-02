//! The `follows²` facet: who the people you follow follow.
//!
//! This is the facet that makes lenses usable. Measured on real feeds, 94% of
//! (feed, reader) pairs had *zero* first-degree overlap — the lens matched
//! nothing and the page came back unfiltered, which reads to a user as a toggle
//! that does nothing. Second degree turns one viewer's 1,182-author lens into
//! ~286,000 authors, ranked, which is the difference between a filter that
//! usually fires and one that usually doesn't.
//!
//! # The score is the product
//!
//! `reach` is how many of *your own follows* also follow this author. On real
//! data it discriminates sharply: the top author for one viewer was followed by
//! 596 of their 1,182, tailing smoothly from there. That is exactly the
//! "network statistic with the logged-in user as the distance subject" the
//! requirements asked for, and it falls out of the same query that produces
//! membership — aggregated rather than flattened.
//!
//! # Why it queries the projection
//!
//! Against `follow_edges` this same query read the entire table: 636 MB and
//! 424–874 ms on real data. Against `follow_graph_int` it reads 11.87 MB in
//! 34–37 ms. Two rules keep it there, and both are easy to lose:
//!
//! * **Literal seed ids, never `IN (subquery)`.** A subquery defeats
//!   primary-key index analysis and full-scans. We already hold first degree.
//! * **Never against `follow_edges`.** That table is keyed for per-viewer
//!   lookups and partitioned for fold correctness, neither of which helps a
//!   multi-seed traversal.

pub use graze_lens_fold::project::seed_list as seed_list_of;
use graze_lens_fold::project::{seed_list, LIVE_TABLE};
use tracing::{debug, warn};

use crate::scored::{self, FACET_FOLLOWS2};

/// A built second-degree map, before publishing.
pub struct SecondDegree {
    /// Top-K, scored and sorted by id, ready to encode.
    pub entries: Vec<(u32, u16)>,
    /// Every id reached, for the bloom tail.
    pub all_ids: Vec<u32>,
    pub max_reach: u32,
}

impl SecondDegree {
    pub fn is_empty(&self) -> bool {
        self.all_ids.is_empty()
    }

    /// Encode as a v2 blob: scored top-K plus a bloom over the whole tail.
    pub fn encode(&self, idspace: u8, built_at: u32) -> Vec<u8> {
        self.encode_as(FACET_FOLLOWS2, idspace, built_at)
    }

    /// Encode under an explicit facet header id. The header byte is what lets
    /// the reader reject a blob published under the wrong key, so it must be
    /// the facet the caller is actually publishing — six facets share this
    /// struct now, and stamping them all as follows² would have every one of
    /// them refused at read time (the good failure) or, worse, accepted as a
    /// signal they are not.
    pub fn encode_as(&self, facet_id: u8, idspace: u8, built_at: u32) -> Vec<u8> {
        let mut blob = scored::encode_in_space(facet_id, idspace, built_at, self.entries.clone());
        scored::append_bloom(&mut blob, &self.all_ids);
        blob
    }
}

/// Parse the projection's TSV response into a scored map.
///
/// Rows arrive ordered by reach descending, so the first `top_k` are the
/// strongest signals; everything else still contributes to the bloom, which is
/// what lets a lens degrade smoothly past the top-K instead of falling off a
/// cliff.
pub fn parse_reach_tsv(text: &str, top_k: usize) -> SecondDegree {
    let mut entries: Vec<(u32, u16)> = Vec::with_capacity(top_k.min(1024));
    let mut all_ids: Vec<u32> = Vec::new();
    let mut max_reach = 0u32;

    for line in text.lines() {
        let mut cols = line.split('\t');
        let (Some(id), Some(reach)) = (cols.next(), cols.next()) else {
            continue;
        };
        let (Ok(id), Ok(reach)) = (id.trim().parse::<u32>(), reach.trim().parse::<u32>()) else {
            continue;
        };
        if max_reach == 0 {
            // First row is the largest, since the query orders by reach desc.
            max_reach = reach;
        }
        all_ids.push(id);
        if entries.len() < top_k {
            // Normalised against this viewer's own maximum rather than a global
            // constant: reach is only meaningful relative to how many people
            // this particular viewer follows. A viewer with 40 follows and one
            // with 4,000 should both get a full-strength top signal.
            let w = if max_reach == 0 {
                0.0
            } else {
                reach as f32 / max_reach as f32
            };
            entries.push((id, scored::weight_from_f32(w)));
        }
    }

    SecondDegree {
        entries,
        all_ids,
        max_reach,
    }
}

/// The traversal query, with seeds inlined.
///
/// `cap` bounds both the transfer and the bloom: a viewer with a very wide
/// network yields hundreds of thousands of rows, and past some point the tail
/// is noise that costs bytes on the serve path's critical read.
///
/// Reach counts DISTINCT followers, not rows. The projection carries one row
/// per (follower, rkey) edge, and a follow → unfollow → refollow cycle can
/// leave two live rows for the same pair — counting rows would let one of your
/// follows contribute twice. It is checkable: reach must never exceed the seed
/// count, and when it did (2,234 against 1,183 seeds) that was the symptom.
/// Seed-bounded edge source: the nightly projection plus the incremental delta.
///
/// Returns a parenthesised subquery, so a caller drops it in where it used to
/// name a table. `base_extra` is appended to the base half's `WHERE` for callers
/// with their own predicate (velocity's window); the delta needs no equivalent
/// because every row in it is newer than the last rebuild by construction.
///
/// Both halves carry the **literal** seed list, which is what keeps this free:
/// each uses its own sparse index rather than scanning. Measured on production
/// against a real viewer's 1,187 seeds, three runs each — base alone
/// 0.70/0.57/0.56 s, base UNION a 131M-row second table 0.60/0.55/0.47 s.
/// Identical within noise.
///
/// Duplicates across the two halves are harmless and expected: every caller
/// counts `uniqExact(follower_int)`, which is precisely the collapsing
/// `project.rs` deliberately defers to this layer rather than paying for over
/// 2.77 billion rows.
///
/// Deletes are NOT excluded yet. The delta carries creates only — a jetstream
/// follow-delete names no followee — so `op = 'create'` is written explicitly
/// here to keep the predicate honest, and the resolving pass can add a tombstone
/// exclusion without revisiting every call site.
pub fn edge_source(database: &str, base: &str, seeds: &str, base_extra: &str) -> String {
    let delta = graze_lens_fold::delta_projection::DELTA_TABLE;
    format!(
        "(            SELECT follower_int, followee_int FROM {database}.{base}            WHERE follower_int IN ({seeds}) {base_extra}           UNION ALL            SELECT follower_int, followee_int FROM {database}.{delta}            WHERE follower_int IN ({seeds}) AND op = 'create'          )"
    )
}

pub fn reach_query(database: &str, seeds: &[u32], cap: usize) -> String {
    format!(
        "SELECT followee_int, uniqExact(follower_int) AS reach \
         FROM {source} \
         GROUP BY followee_int \
         ORDER BY reach DESC, followee_int ASC \
         LIMIT {cap} \
         FORMAT TabSeparated",
        source = edge_source(database, LIVE_TABLE, &seed_list(seeds), "")
    )
}

/// Drop the viewer's own first degree from a second-degree map.
///
/// Someone you already follow is not a second-degree discovery, and leaving
/// them in would let `follows²` quietly restate `follows` at a lower weight —
/// making a blend of the two look like it was working when it was double
/// counting one signal.
pub fn exclude_first_degree(map: &mut SecondDegree, first_degree: &[u32]) {
    if first_degree.is_empty() {
        return;
    }
    let mut sorted = first_degree.to_vec();
    sorted.sort_unstable();
    let before = map.all_ids.len();
    map.all_ids.retain(|id| sorted.binary_search(id).is_err());
    map.entries
        .retain(|(id, _)| sorted.binary_search(id).is_err());
    debug!(
        removed = before - map.all_ids.len(),
        "excluded first degree from second"
    );
}

/// Warn when a viewer's network is too small for this facet to mean anything.
///
/// Not an error: a genuinely small network is a real answer, and the blend
/// handles it by falling through to weaker signals. But it is worth seeing in
/// logs, because it is also what a broken backfill looks like.
pub fn warn_if_thin(viewer: &str, seeds: usize, reached: usize) {
    if seeds < 10 || reached < 100 {
        warn!(
            viewer,
            seeds, reached, "second degree is thin; lens will lean on fallbacks"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_reach_rows_and_normalises_to_the_viewers_own_max() {
        let tsv = "100\t596\n200\t300\n300\t150\n";
        let m = parse_reach_tsv(tsv, 10);
        assert_eq!(m.max_reach, 596);
        assert_eq!(m.all_ids, vec![100, 200, 300]);
        // Top entry is full confidence; the rest scale against it.
        assert_eq!(m.entries[0].1, scored::WEIGHT_MAX);
        assert!(m.entries[1].1 < m.entries[0].1);
        assert!(m.entries[2].1 < m.entries[1].1);
    }

    /// Everything past top-K must still reach the bloom — that tail is the
    /// whole reason the lens degrades smoothly rather than cutting off.
    #[test]
    fn tail_beyond_top_k_still_feeds_the_bloom() {
        let tsv: String = (0..500).map(|i| format!("{i}\t{}\n", 500 - i)).collect();
        let m = parse_reach_tsv(&tsv, 50);
        assert_eq!(m.entries.len(), 50, "scored entries are capped");
        assert_eq!(m.all_ids.len(), 500, "but the bloom sees everything");

        let blob = m.encode(scored::IDSPACE_LENS, 0);
        // An id past the top-K is absent from entries but present in the bloom.
        assert_eq!(scored::weight_of(&blob, 400), None);
        assert_eq!(scored::bloom_contains(&blob, 400), Some(true));
        // And one inside the top-K is in both.
        assert!(scored::weight_of(&blob, 0).is_some());
        assert_eq!(scored::bloom_contains(&blob, 0), Some(true));
    }

    /// The seed list must be inlined. `IN (subquery)` reads the whole table —
    /// this is the difference between 11 MB and 636 MB.
    #[test]
    fn query_inlines_literal_seeds() {
        let q = reach_query("default", &[7, 8, 9], 1000);
        assert!(q.contains("IN (7,8,9)"));
        assert!(!q.to_uppercase().contains("IN (SELECT"));
        assert!(q.contains("follow_graph_int"), "must use the projection");
        assert!(
            q.contains(graze_lens_fold::delta_projection::DELTA_TABLE),
            "must union the incremental delta, or a follow since the nightly \
             rebuild is invisible for up to 24 hours"
        );
        assert_eq!(
            q.matches("follower_int IN (").count(),
            2,
            "both halves must carry the literal seed list"
        );
        assert!(
            !q.contains("follow_edges"),
            "must never traverse the fold table"
        );
    }

    /// Reach must count distinct followers. The projection can hold two rows
    /// for one (follower, followee) pair, and counting rows lets a single
    /// follow contribute twice — which produced a reach larger than the
    /// viewer's entire follow list.
    #[test]
    fn reach_counts_distinct_followers_not_rows() {
        let q = reach_query("default", &[1, 2], 100);
        assert!(q.contains("uniqExact(follower_int) AS reach"));
        assert!(!q.contains("count() AS reach"));
    }

    /// Ordering must be deterministic. Ties broken only by reach would make the
    /// top-K cut arbitrary between rebuilds, so a viewer's lens would churn
    /// members for no reason.
    #[test]
    fn ordering_is_deterministic() {
        let q = reach_query("default", &[1], 10);
        assert!(q.contains("ORDER BY reach DESC, followee_int ASC"));
    }

    /// People you already follow are not second-degree discoveries; leaving
    /// them in would let follows² restate follows at lower weight.
    #[test]
    fn first_degree_is_excluded() {
        let tsv = "100\t50\n200\t40\n300\t30\n";
        let mut m = parse_reach_tsv(tsv, 10);
        exclude_first_degree(&mut m, &[200]);
        assert_eq!(m.all_ids, vec![100, 300]);
        assert!(m.entries.iter().all(|(id, _)| *id != 200));
    }

    #[test]
    fn malformed_rows_are_skipped_not_fatal() {
        let tsv = "100\t50\ngarbage\n\n200\tnope\n300\t30\n";
        let m = parse_reach_tsv(tsv, 10);
        assert_eq!(m.all_ids, vec![100, 300]);
    }

    /// Blobs must declare the id space their entries came from. Without it a
    /// reader cannot tell a lens-space blob from a shared-space one, and the
    /// two resolve the same u32 to different accounts.
    #[test]
    fn encoded_blob_declares_the_lens_id_space() {
        let m = parse_reach_tsv("100\t5\n", 10);
        let blob = m.encode(scored::IDSPACE_LENS, 0);
        let h = scored::header(&blob).expect("valid header");
        assert_eq!(h.idspace, scored::IDSPACE_LENS);
        assert_ne!(h.idspace, scored::IDSPACE_SHARED);
    }

    #[test]
    fn empty_result_is_empty_not_a_panic() {
        let m = parse_reach_tsv("", 10);
        assert!(m.is_empty());
        assert_eq!(m.max_reach, 0);
        let blob = m.encode(scored::IDSPACE_LENS, 0);
        assert!(scored::header(&blob).is_some());
    }
}
