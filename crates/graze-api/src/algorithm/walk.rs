//! Pixie-style sampled random walk over the like graph.
//!
//! # Why sample at all
//!
//! Both traversals we have measured are *enumerative*, and both hit a wall:
//!
//! - **Post-first** (`Scorer::score`) truncates each candidate's liker list to the 30 most recent.
//!   Measured: 72-100% of overlapping candidates have more than 30 likers, so the truncation is
//!   biased toward whoever liked most recently rather than whoever is most informative.
//! - **Co-liker-first** (`Scorer::score_inverted`) enumerates every co-liker's likes. It was
//!   predicted 1.88x better coverage and 33-145x cheaper; measured **0.94x coverage at 5x latency**,
//!   because real co-liker sets reach 5,157 while the harness that predicted the win capped at 128.
//!
//! Sampling escapes the dilemma rather than picking a side: cost is bounded by the walk budget
//! instead of by graph degree, and visit *proportions* are unbiased estimates of the quantity the
//! enumerative paths compute exactly but can only afford on a truncated subgraph.
//!
//! # The walk, and why it is two round trips rather than N
//!
//! A literal 3-step walk (post -> liker -> post) would need a Redis round trip per step, which at
//! p99 latency is unaffordable. Instead the same walk is executed in two *batched* phases: sample
//! (seed, liker) pairs from liker lists already fetched, then fetch the sampled likers' like lists in
//! one pipeline. The distribution walked is the same; only the scheduling differs.
//!
//! # Scoring
//!
//! Visit counts, not weights. [`multi_hit_score`] is Pixie's booster: a candidate reached from many
//! *different* seeds scores far above one reached repeatedly from a single seed. This is the
//! principled form of the existing `paths_boost = overlap_count ^ num_paths_power`
//! (`scorer.rs`), which is near-inert in production because measured `overlap_mean` is
//! approximately 1.0 — the enumerative paths almost never find two paths to the same candidate,
//! which is itself a symptom of their truncation.

use rustc_hash::FxHashMap;

/// A candidate's visit tally, kept per originating seed so the multi-hit booster can see breadth.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VisitTally {
    /// Visits from each distinct seed that reached this candidate.
    pub per_seed: Vec<u32>,
}

impl VisitTally {
    /// Total visits, across all seeds.
    pub fn total(&self) -> u32 {
        self.per_seed.iter().sum()
    }

    /// Number of distinct seeds that reached this candidate.
    pub fn breadth(&self) -> usize {
        self.per_seed.len()
    }
}

/// Pixie's multi-hit booster: `(Σ_s √V_s)²`.
///
/// Rewards *breadth* over depth. Two seeds contributing one visit each scores `(1+1)² = 4`, while one
/// seed contributing two visits scores `(√2)² = 2` — so a candidate corroborated by independent parts
/// of the user's taste outranks one that a single seed happened to hit twice.
///
/// This is the property the enumerative paths cannot exploit: with `overlap_mean ≈ 1.0` they
/// effectively never observe breadth at all.
pub fn multi_hit_score(tally: &VisitTally) -> f64 {
    let sum_sqrt: f64 = tally.per_seed.iter().map(|v| (*v as f64).sqrt()).sum();
    sum_sqrt * sum_sqrt
}

/// Discount a walk score by the candidate's global popularity.
///
/// A post everyone likes is reachable from everywhere, so raw visit counts favour it for reasons that
/// have nothing to do with this user. `power = 0.0` disables the discount; `1.0` divides by the liker
/// count outright. This plays the same role as LinkLonk's step-3 term `1/|items the source upvoted|`,
/// which exists so prolific accounts cannot dominate — a fairness property we have deliberately kept
/// elsewhere and should not quietly drop here.
pub fn popularity_discounted(score: f64, liker_count: usize, power: f64) -> f64 {
    if power <= 0.0 {
        return score;
    }
    let denom = (liker_count.max(1) as f64).powf(power);
    if denom <= 0.0 || !denom.is_finite() {
        return score;
    }
    score / denom
}

/// Split a walk budget across seeds, favouring niche seeds over popular ones.
///
/// Weight is `1 / (1 + ln(1 + degree))`. A like on a post with six likers says far more about a user's
/// taste than a like on a post with six thousand, which is the same reasoning behind LinkLonk's
/// `1/|likers|` step-2 normalization; using a *logarithmic* rather than reciprocal discount keeps
/// high-degree seeds contributing something instead of starving them entirely.
///
/// Every seed receives at least one walk when the budget allows, because a seed with zero walks is
/// indistinguishable from a seed that was never in the set — and silently dropping seeds is how the
/// enumerative paths acquired their bias.
pub fn allocate_walks(seed_degrees: &[usize], total_budget: usize) -> Vec<usize> {
    let n = seed_degrees.len();
    if n == 0 || total_budget == 0 {
        return vec![0; n];
    }
    if total_budget <= n {
        // Not enough budget for everyone: give one walk each to the most informative seeds rather
        // than spreading fractional budget that rounds to nothing.
        let mut idx: Vec<usize> = (0..n).collect();
        idx.sort_by_key(|&i| seed_degrees[i]);
        let mut out = vec![0; n];
        for &i in idx.iter().take(total_budget) {
            out[i] = 1;
        }
        return out;
    }

    let weights: Vec<f64> = seed_degrees
        .iter()
        .map(|d| 1.0 / (1.0 + (1.0 + *d as f64).ln()))
        .collect();
    let sum: f64 = weights.iter().sum();
    if sum <= 0.0 || !sum.is_finite() {
        return vec![total_budget / n; n];
    }

    // One walk floor per seed, then distribute the remainder by weight.
    let remainder = total_budget - n;
    let mut out: Vec<usize> = weights
        .iter()
        .map(|w| 1 + (remainder as f64 * (w / sum)).floor() as usize)
        .collect();

    // Hand any rounding slack to the highest-weight seeds so the budget is spent exactly.
    let spent: usize = out.iter().sum();
    let mut slack = total_budget.saturating_sub(spent);
    if slack > 0 {
        let mut idx: Vec<usize> = (0..n).collect();
        idx.sort_by(|&a, &b| {
            weights[b]
                .partial_cmp(&weights[a])
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        for &i in idx.iter().cycle().take(slack.min(n * 4)) {
            if slack == 0 {
                break;
            }
            out[i] += 1;
            slack -= 1;
        }
    }
    out
}

/// Pixie's early-stopping rule: stop once `np` candidates have each been visited at least `nv` times.
///
/// The point is that a walk long enough to rank the top candidates confidently is much shorter than a
/// walk long enough to visit everything — so most of the budget buys nothing. This is also the direct
/// fix for the 3.8 s latency outliers observed during the inverted-lookup test, where cost scaled with
/// the graph rather than with the answer.
///
/// `nv = 0` disables the rule (no candidate can fail a zero threshold, so stopping immediately would
/// be wrong).
pub fn early_stop_reached(tallies: &FxHashMap<String, VisitTally>, np: usize, nv: u32) -> bool {
    if np == 0 || nv == 0 {
        return false;
    }
    tallies.values().filter(|t| t.total() >= nv).count() >= np
}

/// Final ranking from walk tallies.
///
/// Returned as `(score, post_id)` to match what the rest of the scorer produces, sorted descending.
pub fn rank_from_tallies(
    tallies: &FxHashMap<String, VisitTally>,
    liker_counts: &FxHashMap<String, usize>,
    popularity_power: f64,
    min_visits: u32,
) -> Vec<(f64, String)> {
    let mut out: Vec<(f64, String)> = tallies
        .iter()
        .filter(|(_, t)| t.total() >= min_visits)
        .map(|(post_id, tally)| {
            let base = multi_hit_score(tally);
            let count = liker_counts.get(post_id).copied().unwrap_or(0);
            (
                popularity_discounted(base, count, popularity_power),
                post_id.clone(),
            )
        })
        .collect();
    // Tie-break on post_id so the ranking is total and reproducible. Without this, equal-scoring
    // candidates would order by hash-map iteration, which differs between two runs in one process and
    // would reintroduce exactly the self-disagreement the interleaving self-check caught.
    out.sort_by(|a, b| {
        b.0.partial_cmp(&a.0)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.1.cmp(&b.1))
    });
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tally(per_seed: &[u32]) -> VisitTally {
        VisitTally {
            per_seed: per_seed.to_vec(),
        }
    }

    #[test]
    fn breadth_beats_depth() {
        // The whole point of the booster: corroboration from independent seeds outranks repetition
        // from one. Two seeds x one visit = 4; one seed x two visits = 2.
        let broad = multi_hit_score(&tally(&[1, 1]));
        let deep = multi_hit_score(&tally(&[2]));
        assert!(broad > deep, "broad={broad} deep={deep}");
        assert!((broad - 4.0).abs() < 1e-9);
        assert!((deep - 2.0).abs() < 1e-9);
    }

    #[test]
    fn breadth_counts_distinct_seeds_not_visits() {
        let t = tally(&[3, 1, 1]);
        assert_eq!(t.breadth(), 3);
        assert_eq!(t.total(), 5);
    }

    #[test]
    fn a_single_visit_scores_one() {
        assert!((multi_hit_score(&tally(&[1])) - 1.0).abs() < 1e-9);
    }

    #[test]
    fn no_visits_scores_zero() {
        assert_eq!(multi_hit_score(&tally(&[])), 0.0);
    }

    #[test]
    fn popularity_discount_demotes_universally_liked_posts() {
        let niche = popularity_discounted(4.0, 10, 0.5);
        let viral = popularity_discounted(4.0, 10_000, 0.5);
        assert!(niche > viral, "niche={niche} viral={viral}");
    }

    #[test]
    fn popularity_discount_is_a_no_op_at_zero_power() {
        assert_eq!(popularity_discounted(4.0, 10_000, 0.0), 4.0);
    }

    #[test]
    fn popularity_discount_survives_a_zero_liker_count() {
        // Pool posts always have at least one liker, but a missing `apc:` entry reads as 0 and must
        // not produce a division by zero that poisons the ranking.
        let scored = popularity_discounted(4.0, 0, 1.0);
        assert!(scored.is_finite() && scored > 0.0);
    }

    #[test]
    fn allocation_favours_niche_seeds() {
        let alloc = allocate_walks(&[5, 5000], 100);
        assert!(
            alloc[0] > alloc[1],
            "a 5-liker seed must get more walks than a 5000-liker seed: {alloc:?}"
        );
    }

    #[test]
    fn allocation_spends_the_whole_budget() {
        for budget in [10usize, 37, 100, 1000] {
            let alloc = allocate_walks(&[3, 30, 300, 3000, 30000], budget);
            assert_eq!(alloc.iter().sum::<usize>(), budget, "budget={budget}");
        }
    }

    #[test]
    fn every_seed_gets_at_least_one_walk_when_budget_allows() {
        let alloc = allocate_walks(&[1, 10, 100, 1000], 40);
        assert!(
            alloc.iter().all(|&w| w >= 1),
            "a seed with zero walks is indistinguishable from one that was never there: {alloc:?}"
        );
    }

    #[test]
    fn tight_budget_prefers_the_most_informative_seeds() {
        // Budget smaller than the seed count: the two lowest-degree seeds should win the walks.
        let alloc = allocate_walks(&[9000, 5, 8000, 7], 2);
        assert_eq!(alloc.iter().sum::<usize>(), 2);
        assert_eq!(alloc[1], 1, "the 5-liker seed must be walked: {alloc:?}");
        assert_eq!(alloc[3], 1, "the 7-liker seed must be walked: {alloc:?}");
    }

    #[test]
    fn allocation_handles_degenerate_input() {
        assert_eq!(allocate_walks(&[], 100), Vec::<usize>::new());
        assert_eq!(allocate_walks(&[1, 2, 3], 0), vec![0, 0, 0]);
    }

    #[test]
    fn early_stop_fires_once_enough_candidates_are_confident() {
        let mut t = FxHashMap::default();
        t.insert("a".to_string(), tally(&[2, 2]));
        t.insert("b".to_string(), tally(&[4]));
        t.insert("c".to_string(), tally(&[1]));
        assert!(early_stop_reached(&t, 2, 4));
        assert!(!early_stop_reached(&t, 3, 4));
    }

    #[test]
    fn early_stop_is_disabled_rather_than_instant_at_zero() {
        // A zero threshold must mean "no early stopping", never "stop before walking", which would
        // silently disable personalization for everyone.
        let mut t = FxHashMap::default();
        t.insert("a".to_string(), tally(&[1]));
        assert!(!early_stop_reached(&t, 0, 5));
        assert!(!early_stop_reached(&t, 5, 0));
    }

    #[test]
    fn ranking_is_total_and_reproducible_under_ties() {
        // Equal scores must not order by hash-map iteration: two runs in one process would disagree,
        // which is the exact defect the interleaving self-check caught in production.
        let mut t = FxHashMap::default();
        for id in ["zzz", "aaa", "mmm"] {
            t.insert(id.to_string(), tally(&[1]));
        }
        let counts = FxHashMap::default();
        let first = rank_from_tallies(&t, &counts, 0.0, 1);
        let again = rank_from_tallies(&t, &counts, 0.0, 1);
        assert_eq!(first, again);
        assert_eq!(
            first.iter().map(|(_, id)| id.as_str()).collect::<Vec<_>>(),
            vec!["aaa", "mmm", "zzz"]
        );
    }

    #[test]
    fn ranking_drops_candidates_below_the_visit_floor() {
        let mut t = FxHashMap::default();
        t.insert("solid".to_string(), tally(&[3]));
        t.insert("noise".to_string(), tally(&[1]));
        let ranked = rank_from_tallies(&t, &FxHashMap::default(), 0.0, 2);
        assert_eq!(ranked.len(), 1);
        assert_eq!(ranked[0].1, "solid");
    }

    #[test]
    fn ranking_applies_the_popularity_discount() {
        let mut t = FxHashMap::default();
        t.insert("niche".to_string(), tally(&[1, 1]));
        t.insert("viral".to_string(), tally(&[1, 1]));
        let mut counts = FxHashMap::default();
        counts.insert("niche".to_string(), 8usize);
        counts.insert("viral".to_string(), 50_000usize);
        let ranked = rank_from_tallies(&t, &counts, 0.5, 1);
        assert_eq!(ranked[0].1, "niche", "{ranked:?}");
    }
}
