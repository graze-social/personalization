//! Prior-weighted and time-filtered reach: the facet family beyond `follows²`.
//!
//! The reach query is secretly an algebra: `reach(seeds, edge_filter, prior)`.
//! Every knob is a lens, and every lens here is one indexed query at build time
//! against a table the nightly job maintains — nothing new ever touches the
//! serve path, which keeps reading blobs.
//!
//! # The facets
//!
//! **`popular`** — `reach × ln(2 + fame)`. Your second degree, tilted toward
//! accounts the wider network also rates. The "a-priori popular, nearby focus"
//! blend.
//!
//! **`niche`** — `reach / log2(2 + fame)`. The inversion, and the discovery
//! engine: accounts your network is disproportionately close to *relative to
//! their global fame*. TF-IDF on the follow graph; your community's beloved
//! obscurities, structurally invisible to any trending list.
//!
//! **`velocity`** — reach counted only over edges created in the last N days:
//! who your network is *discovering right now*. This is why the archive replay
//! fought to preserve each record's own `createdAt` — the recency slice is
//! built from it.
//!
//! **`community`** — the LPA output: members of the communities your follows
//! concentrate in, ranked by in-community fame and weighted by how much of
//! your following lives there.
//!
//! # The join direction is load-bearing
//!
//! ClickHouse builds the RIGHT side of a hash join in memory. The reach result
//! is bounded (≤ cap rows); `account_stats` is 42M rows. Every query here puts
//! the small set on the right and streams the big table past it — reversed, the
//! 42M-row hash table joins the club of designs this cluster's 21.6 GiB
//! ceiling has already rejected.

use crate::scored;
use crate::second_degree::{seed_list_of, SecondDegree};
use tracing::warn;

/// How a global per-account prior reweights raw reach.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Prior {
    /// `reach × ln(2 + followers)` — nearby and widely known.
    Popular,
    /// `reach / (100 + followers)` — the fraction of an account's audience
    /// that is *your network*, smoothed.
    ///
    /// The first version divided by `log2(fame)`, which reads as the obvious
    /// TF-IDF move and is wrong here: a logarithm compresses a 35,000× fame
    /// difference into a factor of ~2.5, so raw reach still dominated and the
    /// "niche" ranking came out identical to the popular one, crowned by a
    /// 32M-follower account. Verified against real output, not the invariants —
    /// every invariant passed on the broken version. Linear fame in the
    /// denominator makes the score a smoothed audience fraction: an account
    /// with 200 followers, 150 of them your network, scores ~0.5; the
    /// 32M-follower account scores ~0.00003.
    Niche,
}

/// Below this, a "niche" signal is one person's follow, which is just
/// follows² wearing a costume. Two independent confirmations is the floor for
/// claiming *the network* is close to an account.
const NICHE_MIN_REACH: u32 = 2;

/// Reach reweighted by global fame, top `cap` by the blended score.
///
/// Three output columns: id, raw reach (kept so the `reach ≤ seeds` invariant
/// stays checkable — it is the tell that caught a corrupt id map), and score.
pub fn prior_reach_query(database: &str, seeds: &[u32], cap: usize, prior: Prior) -> String {
    let (weight, having) = match prior {
        Prior::Popular => ("r.reach * ln(2 + s.followers)", String::new()),
        Prior::Niche => (
            "r.reach / (100 + s.followers)",
            format!("HAVING uniqExact(follower_int) >= {NICHE_MIN_REACH} "),
        ),
    };
    format!(
        "SELECT r.followee_int, r.reach, {weight} AS score \
         FROM {database}.account_stats AS s \
         INNER JOIN ( \
             SELECT followee_int, uniqExact(follower_int) AS reach \
             FROM {source} \
             GROUP BY followee_int \
             {having}\
         ) AS r ON s.account_int = r.followee_int \
         ORDER BY score DESC, r.followee_int ASC \
         LIMIT {cap} \
         FORMAT TabSeparated",
        source = crate::second_degree::edge_source(
            database,
            "follow_graph_int",
            &seed_list_of(seeds),
            "",
        ),
    )
}

/// Reach over only the freshest edges: what the viewer's network is following
/// *now*. Same distinct-follower counting as follows² — a duplicated edge must
/// not let one follow count twice here either.
pub fn velocity_query(database: &str, seeds: &[u32], cap: usize, days: u32) -> String {
    format!(
        "SELECT followee_int, uniqExact(follower_int) AS reach \
         FROM {source} \
         GROUP BY followee_int \
         ORDER BY reach DESC, followee_int ASC \
         LIMIT {cap} \
         FORMAT TabSeparated",
        source = crate::second_degree::edge_source(
            database,
            "follow_graph_recent",
            &seed_list_of(seeds),
            &format!("AND followed_at > today() - {days} "),
        ),
    )
}

/// Which communities the viewer's follows concentrate in.
pub fn community_affinity_query(database: &str, seeds: &[u32], top: usize) -> String {
    format!(
        "SELECT community, count() AS cnt \
         FROM {database}.account_community \
         WHERE account IN ({seeds}) \
         GROUP BY community \
         ORDER BY cnt DESC, community ASC \
         LIMIT {top} \
         FORMAT TabSeparated",
        seeds = seed_list_of(seeds),
    )
}

/// Members of the viewer's top communities, ranked by in-community fame.
///
/// Small-right again: the member set for a handful of communities is the
/// bounded side, `account_stats` streams past it.
pub fn community_members_query(database: &str, communities: &[u32], cap: usize) -> String {
    format!(
        "SELECT m.account, m.community, s.followers \
         FROM {database}.account_stats AS s \
         INNER JOIN ( \
             SELECT account, community FROM {database}.community_members \
             WHERE community IN ({list}) \
         ) AS m ON s.account_int = m.account \
         ORDER BY s.followers DESC, m.account ASC \
         LIMIT {cap} \
         FORMAT TabSeparated",
        list = seed_list_of(communities),
    )
}

/// Parse `(id, reach, score)` rows into a scored map, normalising weights
/// against the viewer's own best score.
pub fn parse_prior_tsv(text: &str, top_k: usize) -> SecondDegree {
    let mut entries: Vec<(u32, u16)> = Vec::with_capacity(top_k.min(1024));
    let mut all_ids: Vec<u32> = Vec::new();
    let mut max_reach = 0u32;
    let mut max_score = 0f64;

    for line in text.lines() {
        let mut cols = line.split('\t');
        let (Some(id), Some(reach), Some(score)) = (cols.next(), cols.next(), cols.next()) else {
            continue;
        };
        let (Ok(id), Ok(reach), Ok(score)) = (
            id.trim().parse::<u32>(),
            reach.trim().parse::<u32>(),
            score.trim().parse::<f64>(),
        ) else {
            continue;
        };
        max_reach = max_reach.max(reach);
        if max_score == 0.0 {
            // Rows arrive ordered by score descending.
            max_score = score;
        }
        all_ids.push(id);
        if entries.len() < top_k {
            let w = if max_score > 0.0 {
                score / max_score
            } else {
                0.0
            };
            entries.push((id, scored::weight_from_f32(w as f32)));
        }
    }

    SecondDegree {
        entries,
        all_ids,
        max_reach,
    }
}

/// Parse `(community, count)` affinity rows into (community, share-of-seeds).
pub fn parse_affinity_tsv(text: &str, seeds: usize) -> Vec<(u32, f32)> {
    let mut out = Vec::new();
    let mut total = 0u64;
    for line in text.lines() {
        let mut cols = line.split('\t');
        let (Some(c), Some(n)) = (cols.next(), cols.next()) else {
            continue;
        };
        let (Ok(c), Ok(n)) = (c.trim().parse::<u32>(), n.trim().parse::<u64>()) else {
            continue;
        };
        total += n;
        out.push((c, n as f32 / seeds.max(1) as f32));
    }
    // Affinity counts are follows-with-a-community; they can never exceed the
    // seed count. Same class of invariant as reach ≤ seeds.
    if total > seeds as u64 {
        warn!(
            total,
            seeds, "community affinity exceeds seed count; id map suspect"
        );
    }
    out
}

/// Parse `(account, community, followers)` member rows, weighting each member
/// by `affinity_share(community) × ln(2 + fame)`, normalised to the best.
pub fn parse_members_tsv(text: &str, affinity: &[(u32, f32)], top_k: usize) -> SecondDegree {
    let share = |c: u32| affinity.iter().find(|(k, _)| *k == c).map(|(_, s)| *s);
    let mut raw: Vec<(u32, f64)> = Vec::new();
    let mut all_ids: Vec<u32> = Vec::new();

    for line in text.lines() {
        let mut cols = line.split('\t');
        let (Some(a), Some(c), Some(f)) = (cols.next(), cols.next(), cols.next()) else {
            continue;
        };
        let (Ok(a), Ok(c), Ok(f)) = (
            a.trim().parse::<u32>(),
            c.trim().parse::<u32>(),
            f.trim().parse::<u64>(),
        ) else {
            continue;
        };
        let Some(sh) = share(c) else { continue };
        all_ids.push(a);
        raw.push((a, sh as f64 * (2.0 + f as f64).ln()));
    }

    raw.sort_by(|x, y| y.1.partial_cmp(&x.1).unwrap_or(std::cmp::Ordering::Equal));
    let max = raw.first().map(|(_, s)| *s).unwrap_or(0.0);
    let entries: Vec<(u32, u16)> = raw
        .into_iter()
        .take(top_k)
        .map(|(a, s)| {
            let w = if max > 0.0 { s / max } else { 0.0 };
            (a, scored::weight_from_f32(w as f32))
        })
        .collect();

    SecondDegree {
        entries,
        all_ids,
        max_reach: 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The join direction is the whole performance story: the bounded reach
    /// result must be the RIGHT side (built in memory), account_stats the left
    /// (streamed). Reversed, this is a 42M-row hash table and a rejected query.
    #[test]
    fn prior_queries_put_the_small_set_on_the_right() {
        for prior in [Prior::Niche, Prior::Popular] {
            let q = prior_reach_query("default", &[1, 2, 3], 1000, prior);
            let from_at = q.find("FROM default.account_stats").expect("stats is FROM");
            let join_at = q.find("INNER JOIN (").expect("reach is the joined side");
            assert!(from_at < join_at, "stats must stream, reach must build");
            assert!(q.contains("uniqExact(follower_int) AS reach"));
            assert!(q.contains("IN (1,2,3)"));
            assert!(!q.to_uppercase().contains("OFFSET"));
        }
    }

    /// Niche must divide by LINEAR fame, not a logarithm. The log version
    /// passed every structural test and produced a ranking identical to
    /// popular's, topped by a 32M-follower account — a log compresses fame
    /// differences until reach dominates. Linear fame makes the score an
    /// audience fraction, which is the actual definition of "niche to me".
    #[test]
    fn niche_divides_by_linear_fame_and_floors_reach() {
        let n = prior_reach_query("default", &[1], 10, Prior::Niche);
        let p = prior_reach_query("default", &[1], 10, Prior::Popular);
        assert!(n.contains("r.reach / (100 + s.followers)"));
        assert!(!n.contains("log2"), "log fame is the bug, not the feature");
        assert!(
            n.contains("HAVING uniqExact(follower_int) >= 2"),
            "one person's follow is follows2, not a network signal"
        );
        assert!(p.contains("r.reach * ln(2 + s.followers)"));
        assert!(!p.contains("HAVING"), "popular keeps single-reach rows");
    }

    /// Velocity must read the recency slice, bound by day, and count distinct
    /// followers — a duplicated edge must not double a follow here either.
    #[test]
    fn velocity_reads_the_recent_slice_bounded_by_day() {
        let q = velocity_query("default", &[7], 500, 7);
        assert!(q.contains("follow_graph_recent"));
        assert!(q.contains("followed_at > today() - 7"));
        assert!(q.contains("uniqExact(follower_int)"));
        // Trailing space on purpose: `follow_graph_int_delta` contains
        // `follow_graph_int` as a substring, and unioning the delta is wanted
        // here. What must never appear is the full 2.78B-row base table as a
        // source of its own.
        assert!(
            !q.contains("default.follow_graph_int "),
            "must not scan the full graph"
        );
    }

    /// Every reach query must union the incremental delta, or a follow made
    /// since the nightly rebuild is invisible for up to 24 hours.
    #[test]
    fn reach_queries_union_the_incremental_delta() {
        let delta = graze_lens_fold::delta_projection::DELTA_TABLE;
        for (name, q) in [
            (
                "niche",
                prior_reach_query("default", &[7], 500, Prior::Niche),
            ),
            (
                "popular",
                prior_reach_query("default", &[7], 500, Prior::Popular),
            ),
            ("velocity", velocity_query("default", &[7], 500, 7)),
        ] {
            assert!(q.contains(delta), "{name} must union {delta}");
            assert!(q.contains("UNION ALL"), "{name} must union, not join");
            assert!(
                q.contains("op = 'create'"),
                "{name} must take only additions from the delta"
            );
            // Both halves need the LITERAL seed list; an `IN (subquery)` defeats
            // index analysis and turns each half into a scan.
            assert_eq!(
                q.matches("follower_int IN (7)").count(),
                2,
                "{name}: both halves must carry the literal seeds"
            );
        }
    }

    /// Three columns, weights normalised to the best score, everything into
    /// the bloom, and the raw-reach invariant still visible to the caller.
    #[test]
    fn prior_rows_parse_and_normalise() {
        let tsv = "10\t50\t99.5\n20\t40\t50.0\n30\t9\t10.0\n";
        let m = parse_prior_tsv(tsv, 2);
        assert_eq!(m.all_ids, vec![10, 20, 30]);
        assert_eq!(m.entries.len(), 2, "top_k caps scored entries");
        assert_eq!(m.entries[0].1, scored::WEIGHT_MAX);
        assert!(m.entries[1].1 < m.entries[0].1);
        assert_eq!(
            m.max_reach, 50,
            "raw reach survives for the invariant check"
        );
    }

    #[test]
    fn affinity_shares_are_fractions_of_seeds() {
        let a = parse_affinity_tsv("5\t60\n9\t30\n", 100);
        assert_eq!(a, vec![(5, 0.6), (9, 0.3)]);
    }

    /// A member of a stronger community outranks a slightly more famous member
    /// of a weaker one — affinity is the point of the facet, fame only breaks
    /// ties within a community.
    #[test]
    fn member_weights_blend_affinity_and_fame() {
        let affinity = vec![(5u32, 0.9f32), (9u32, 0.1f32)];
        // account 100: community 5, 1k followers. account 200: community 9, 5k.
        let tsv = "200\t9\t5000\n100\t5\t1000\n";
        let m = parse_members_tsv(tsv, &affinity, 10);
        assert_eq!(m.entries[0].0, 100, "high-affinity community wins");
        assert!(
            m.all_ids.contains(&200),
            "but the other member still blooms"
        );
    }

    #[test]
    fn members_of_unknown_communities_are_dropped() {
        let m = parse_members_tsv("100\t77\t1000\n", &[(5, 1.0)], 10);
        assert!(
            m.is_empty(),
            "a member row for a community we did not ask about is a bug upstream"
        );
    }
}
