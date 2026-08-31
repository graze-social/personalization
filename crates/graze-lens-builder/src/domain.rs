//! The `domain` facet: who a feed's own authors collectively follow.
//!
//! Every other facet answers a question about the *viewer's* graph. This one
//! answers a question about the *feed's*: take the accounts whose posts the
//! feed has recently kept — its live voice — and ask who they follow. A
//! basketball feed's authors collectively follow the basketball world; their
//! aggregated follow graph is a map of the domain's authorities that no
//! keyword or classifier could produce.
//!
//! # Time decay is the "recent" in "recent authors"
//!
//! Each author seeds the traversal with a weight: the sum of `exp(-age/τ)`
//! over their kept posts. An author the feed featured daily this week counts
//! for far more than one who appeared twice last month, so the blob tracks the
//! feed's *current* editorial center of gravity rather than its lifetime
//! archive. τ = 10 days: a post's influence halves in about a week.
//!
//! # Feed-scoped, viewer-independent
//!
//! One blob per feed (`lens:v2:domain:{algo_id}`), shared by every reader.
//! Personalization happens at serve time, where the reader SUMS this facet
//! with the viewer's own graph facets — sum, not max, because "this feed's
//! authors follow them" and "my network follows them" are different facts
//! about an account, and holding both is genuinely stronger than either.

use crate::priors::parse_prior_tsv;
use crate::second_degree::{seed_list_of, SecondDegree};

/// Half-life-ish decay constant, days.
const DECAY_TAU_DAYS: f64 = 10.0;
/// How far back a feed's authorship counts at all.
const AUTHOR_WINDOW_DAYS: u32 = 30;
/// Seed ceiling. A firehose feed can have tens of thousands of authors; past
/// the strongest couple of thousand, the tail's weights are noise that costs
/// query size.
pub const MAX_SEED_AUTHORS: usize = 2_000;

/// Time-decayed author weights for one feed.
pub fn author_weights_query(database: &str, algo_id: u32) -> String {
    format!(
        "SELECT author, sum(exp(-dateDiff('day', created_at, now()) / {DECAY_TAU_DAYS})) AS w \
         FROM {database}.algorithm_posts_v2 \
         WHERE algo_id = {algo_id} \
           AND created_at > now() - INTERVAL {AUTHOR_WINDOW_DAYS} DAY \
         GROUP BY author \
         ORDER BY w DESC, author ASC \
         LIMIT {MAX_SEED_AUTHORS} \
         FORMAT TabSeparated"
    )
}

/// Parse `(author_did, weight)` rows.
pub fn parse_author_weights(text: &str) -> Vec<(String, f64)> {
    text.lines()
        .filter_map(|line| {
            let mut cols = line.split('\t');
            let (did, w) = (cols.next()?, cols.next()?);
            let w: f64 = w.trim().parse().ok()?;
            if !did.starts_with("did:") || w <= 0.0 {
                return None;
            }
            Some((did.trim().to_string(), w))
        })
        .collect()
}

/// Weighted reach: how much of the feed's recent voice follows each account.
///
/// Output columns are (id, reach, score) so `parse_prior_tsv` and the
/// `reach ≤ seeds` invariant both carry over unchanged. The weights ride in a
/// `values()` table on the RIGHT of the join (a couple of thousand rows), and
/// the literal IN list keeps the primary-key pruning that makes this an
/// index seek rather than a scan.
pub fn weighted_reach_query(database: &str, seeds: &[(u32, f64)], cap: usize) -> String {
    let ids: Vec<u32> = seeds.iter().map(|(id, _)| *id).collect();
    let tuples: String = seeds
        .iter()
        .map(|(id, w)| format!("({id},{w:.6})"))
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "SELECT g.followee_int, uniqExact(g.follower_int) AS reach, sum(v.w) AS score \
         FROM {database}.follow_graph_int AS g \
         INNER JOIN (SELECT * FROM values('f UInt32, w Float64', {tuples})) AS v \
             ON g.follower_int = v.f \
         WHERE g.follower_int IN ({ids}) \
         GROUP BY g.followee_int \
         ORDER BY score DESC, g.followee_int ASC \
         LIMIT {cap} \
         FORMAT TabSeparated",
        ids = seed_list_of(&ids),
    )
}

/// Parse into a scored map. Same shape as the prior facets.
pub fn parse_domain_tsv(text: &str, top_k: usize) -> SecondDegree {
    parse_prior_tsv(text, top_k)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The seed query is where "expertise" gets its meaning: authors weighted
    /// by decayed presence, not lifetime post count. A feed that pivoted last
    /// month should have already pivoted its domain blob.
    #[test]
    fn author_weights_decay_and_window() {
        let q = author_weights_query("default", 29094);
        assert!(q.contains("exp(-dateDiff('day', created_at, now()) / 10)"));
        assert!(q.contains("INTERVAL 30 DAY"));
        assert!(q.contains("algo_id = 29094"));
        assert!(q.contains("LIMIT 2000"));
        assert!(!q.to_uppercase().contains("OFFSET"));
    }

    /// Weights ride a values() table on the RIGHT of the join (small side in
    /// memory), and the literal IN list keeps primary-key pruning. Losing
    /// either half re-runs the 21.6 GiB lesson.
    #[test]
    fn weighted_reach_keeps_pruning_and_small_right() {
        let q = weighted_reach_query("default", &[(7, 1.5), (9, 0.25)], 100);
        assert!(q.contains("IN (7,9)"), "literal ids prune the scan");
        assert!(q.contains("values('f UInt32, w Float64', (7,1.500000),(9,0.250000))"));
        let join_at = q.find("INNER JOIN (SELECT * FROM values").unwrap();
        let from_at = q.find("FROM default.follow_graph_int").unwrap();
        assert!(
            from_at < join_at,
            "the big table streams, the weights build"
        );
        assert!(q.contains("uniqExact(g.follower_int) AS reach"));
        assert!(q.contains("sum(v.w) AS score"));
    }

    #[test]
    fn author_rows_parse_and_reject_junk() {
        let tsv = "did:plc:a\t3.5\nnot-a-did\t9\ndid:plc:b\t0\ndid:plc:c\t1.25\n";
        let w = parse_author_weights(tsv);
        assert_eq!(w.len(), 2);
        assert_eq!(w[0].0, "did:plc:a");
        assert!((w[1].1 - 1.25).abs() < 1e-9);
    }

    /// Output columns match the prior facets, so the invariant machinery —
    /// including reach ≤ seeds — carries over without a parallel parser.
    #[test]
    fn domain_rows_reuse_the_prior_parser() {
        let m = parse_domain_tsv("10\t5\t99.5\n20\t3\t42.0\n", 10);
        assert_eq!(m.all_ids, vec![10, 20]);
        assert_eq!(m.max_reach, 5);
        assert_eq!(m.entries[0].1, crate::scored::WEIGHT_MAX);
    }
}
