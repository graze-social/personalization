//! High-performance scoring for the LinkLonk algorithm.
//!
//! This module provides optimized Rust implementations of CPU-intensive operations:
//!
//! 1. Post scoring hot loop - scores posts against co-liker weights
//! 2. Co-liker aggregation - computes co-liker weights from post likers
//!
//! ## Original LinkLonk Formula
//!
//! The original LinkLonk algorithm performs a 3-step random walk:
//!   1. From user → one of user's liked posts (probability 1/|user_likes|)
//!   2. From post → one of sources who liked it before user (probability 1/|sources_who_liked_post|)
//!   3. From source → one of other posts source liked (probability 1/|items_source_liked|)
//!
//! For a candidate post i, the score is the sum of all path probabilities:
//!   Score(i) = Σ over paths (1/|user_likes|) × (1/|sources_who_liked_j|) × (1/|items_s_liked|)
//!
//! ## Normalized Aggregation Formula
//!
//! With normalization enabled, the co-liker weight for source s is:
//!   weight[s] = Σ over posts j that s liked before user [
//!       (1/|user_likes|) × (1/|sources_who_liked_j|) × (1/|items_s_liked|) × recency_weight
//!   ]
//!
//! Where:
//!   - |user_likes| = total posts the user has liked
//!   - |sources_who_liked_j| = number of sources who liked post j (before user)
//!   - |items_s_liked| = total items source s has liked
//!   - recency_weight = exp(-0.693 * age / half_life)
//!
//! Ported from rust_scorer PyO3 extension to native Rust.

use rayon::prelude::*;
use rustc_hash::FxHashMap;
use std::cmp::Ordering;
use std::collections::BinaryHeap;

/// Decay constant for exponential recency weighting.
/// ln(2) ≈ 0.693, so exp(-0.693 * age / half_life) gives 0.5 at age = half_life.
use std::f64::consts::LN_2;

/// Threshold for switching to parallel processing.
const PARALLEL_THRESHOLD: usize = 1000;

/// A scored post with its metadata.
#[derive(Debug, Clone)]
pub struct ScoredPostResult {
    pub post_id: String,
    pub score: f64,
    pub matching_count: u32,
}

/// Wrapper for min-heap ordering (we want to track top K by keeping smallest at top).
#[derive(Debug)]
struct HeapEntry {
    post_id: String,
    score: f64,
    matching_count: u32,
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.score == other.score
    }
}

impl Eq for HeapEntry {}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse ordering for min-heap (smallest score at top)
        other
            .score
            .partial_cmp(&self.score)
            .unwrap_or(Ordering::Equal)
    }
}

/// Score a single post against co-liker weights.
/// Returns (score, matching_count) or None if score is 0.
#[inline]
fn score_single_post(
    likers: &[(String, f64)],
    source_weights: &FxHashMap<String, f64>,
    decay_factor: f64,
    now: f64,
) -> Option<(f64, u32)> {
    let mut score = 0.0_f64;
    let mut matching_count = 0_u32;

    for (liker_hash, like_time) in likers {
        if let Some(&weight) = source_weights.get(liker_hash) {
            let age = now - like_time;
            let recency_weight = (decay_factor * age).exp();
            score += weight * recency_weight;
            matching_count += 1;
        }
    }

    if score > 0.0 {
        Some((score, matching_count))
    } else {
        None
    }
}

/// Score a batch of posts against a set of co-liker weights.
///
/// # Arguments
/// * `posts_data` - List of (post_id, [(liker_hash, like_time), ...])
/// * `source_weights` - Map of co-liker hash to weight
/// * `now` - Current timestamp (unix seconds)
/// * `half_life` - Recency half-life in seconds
///
/// # Returns
/// List of scored posts with score > 0
pub fn score_posts(
    posts_data: Vec<(String, Vec<(String, f64)>)>,
    source_weights: &FxHashMap<String, f64>,
    now: f64,
    half_life: f64,
) -> Vec<ScoredPostResult> {
    let decay_factor = -LN_2 / half_life;

    posts_data
        .into_iter()
        .filter_map(|(post_id, likers)| {
            score_single_post(&likers, source_weights, decay_factor, now).map(|(score, count)| {
                ScoredPostResult {
                    post_id,
                    score,
                    matching_count: count,
                }
            })
        })
        .collect()
}

/// Score posts with parallel processing for large batches.
/// Uses rayon for parallel iteration when post count exceeds threshold.
///
/// # Arguments
/// * `posts_data` - List of (post_id, [(liker_hash, like_time), ...])
/// * `source_weights` - Map of co-liker hash to weight
/// * `now` - Current timestamp (unix seconds)
/// * `half_life` - Recency half-life in seconds
///
/// # Returns
/// List of scored posts with score > 0
pub fn score_posts_parallel(
    posts_data: Vec<(String, Vec<(String, f64)>)>,
    source_weights: &FxHashMap<String, f64>,
    now: f64,
    half_life: f64,
) -> Vec<ScoredPostResult> {
    // For small batches, sequential is faster (avoids thread overhead)
    if posts_data.len() < PARALLEL_THRESHOLD {
        return score_posts(posts_data, source_weights, now, half_life);
    }

    let decay_factor = -LN_2 / half_life;

    posts_data
        .into_par_iter()
        .filter_map(|(post_id, likers)| {
            score_single_post(&likers, source_weights, decay_factor, now).map(|(score, count)| {
                ScoredPostResult {
                    post_id,
                    score,
                    matching_count: count,
                }
            })
        })
        .collect()
}

/// Score posts with early termination, returning only top K results.
///
/// This is more efficient when you only need a subset of results, as it:
/// 1. Uses a min-heap to track top K scores efficiently
/// 2. For large batches, uses parallel processing
/// 3. Sorts final results by score descending
///
/// # Arguments
/// * `posts_data` - List of (post_id, [(liker_hash, like_time), ...])
/// * `source_weights` - Map of co-liker hash to weight
/// * `now` - Current timestamp (unix seconds)
/// * `half_life` - Recency half-life in seconds
/// * `max_results` - Maximum number of results to return (0 = unlimited)
///
/// # Returns
/// List of scored posts sorted by score descending
pub fn score_posts_topk(
    posts_data: Vec<(String, Vec<(String, f64)>)>,
    source_weights: &FxHashMap<String, f64>,
    now: f64,
    half_life: f64,
    max_results: usize,
) -> Vec<ScoredPostResult> {
    // If no limit or limit >= posts, use regular parallel scoring
    if max_results == 0 || max_results >= posts_data.len() {
        let mut results = score_posts_parallel(posts_data, source_weights, now, half_life);
        // Sort by score descending for consistency
        results.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(Ordering::Equal));
        return results;
    }

    let decay_factor = -LN_2 / half_life;

    // For parallel processing with top-K, we process in parallel chunks,
    // then merge the top results from each chunk
    if posts_data.len() >= PARALLEL_THRESHOLD {
        // Process in parallel, collect all scoring posts
        let mut all_scores: Vec<ScoredPostResult> = posts_data
            .into_par_iter()
            .filter_map(|(post_id, likers)| {
                score_single_post(&likers, source_weights, decay_factor, now).map(
                    |(score, count)| ScoredPostResult {
                        post_id,
                        score,
                        matching_count: count,
                    },
                )
            })
            .collect();

        // Partial sort to get top K (more efficient than full sort)
        if all_scores.len() > max_results {
            all_scores.select_nth_unstable_by(max_results, |a, b| {
                b.score.partial_cmp(&a.score).unwrap_or(Ordering::Equal)
            });
            all_scores.truncate(max_results);
        }

        // Sort the top K by score descending
        all_scores.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(Ordering::Equal));
        return all_scores;
    }

    // Sequential with min-heap for top-K tracking
    let mut heap: BinaryHeap<HeapEntry> = BinaryHeap::with_capacity(max_results + 1);

    for (post_id, likers) in posts_data {
        if let Some((score, matching_count)) =
            score_single_post(&likers, source_weights, decay_factor, now)
        {
            // If heap not full, always add
            if heap.len() < max_results {
                heap.push(HeapEntry {
                    post_id,
                    score,
                    matching_count,
                });
            } else if let Some(min) = heap.peek() {
                // Only add if score beats current minimum
                if score > min.score {
                    heap.pop();
                    heap.push(HeapEntry {
                        post_id,
                        score,
                        matching_count,
                    });
                }
            }
        }
    }

    // Extract results and sort by score descending
    let mut results: Vec<ScoredPostResult> = heap
        .into_iter()
        .map(|e| ScoredPostResult {
            post_id: e.post_id,
            score: e.score,
            matching_count: e.matching_count,
        })
        .collect();

    results.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(Ordering::Equal));
    results
}

/// Aggregate co-liker weights from post likers data.
///
/// This is the CPU-intensive part of co-liker computation. Given the likers
/// of each post a user has liked, it:
/// 1. Calculates recency-weighted contributions for each source user
/// 2. Aggregates weights across all posts
/// 3. Sorts by weight and returns top N
///
/// # Arguments
/// * `all_likers_data` - List of likers for each post: [[(liker_hash, like_time), ...], ...]
/// * `user_hash` - The user's hash (to exclude self from results)
/// * `now` - Current timestamp (unix seconds)
/// * `recency_half_life_seconds` - Half-life for recency decay
/// * `max_total_sources` - Maximum number of sources to return
///
/// # Returns
/// List of (source_hash, weight) sorted by weight descending, up to max_total_sources
pub fn aggregate_coliker_weights(
    all_likers_data: Vec<Vec<(String, f64)>>,
    user_hash: &str,
    now: f64,
    recency_half_life_seconds: f64,
    max_total_sources: usize,
) -> Vec<(String, f64)> {
    let decay_factor = -LN_2 / recency_half_life_seconds;

    // Aggregate weights into a hash map
    let mut source_weights: FxHashMap<String, f64> = FxHashMap::default();

    for likers in all_likers_data {
        for (source_did_hash, source_like_time) in likers {
            // Skip self
            if source_did_hash == user_hash {
                continue;
            }

            // Calculate recency weight using exponential decay
            let age_seconds = now - source_like_time;
            let recency_weight = (decay_factor * age_seconds).exp();

            // Accumulate weight for this source
            *source_weights.entry(source_did_hash).or_insert(0.0) += recency_weight;
        }
    }

    if source_weights.is_empty() {
        return Vec::new();
    }

    // Collect into vec and sort by weight descending
    let mut sorted_sources: Vec<(String, f64)> = source_weights.into_iter().collect();
    sorted_sources.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal));

    // Take top N
    sorted_sources.truncate(max_total_sources);

    sorted_sources
}

/// Aggregate co-liker weights with parallel processing for large datasets.
///
/// Uses rayon to parallelize the aggregation across multiple posts.
pub fn aggregate_coliker_weights_parallel(
    all_likers_data: Vec<Vec<(String, f64)>>,
    user_hash: &str,
    now: f64,
    recency_half_life_seconds: f64,
    max_total_sources: usize,
) -> Vec<(String, f64)> {
    if all_likers_data.len() < PARALLEL_THRESHOLD {
        return aggregate_coliker_weights(
            all_likers_data,
            user_hash,
            now,
            recency_half_life_seconds,
            max_total_sources,
        );
    }

    let decay_factor = -LN_2 / recency_half_life_seconds;

    // Process each post's likers in parallel, producing partial weight maps
    let partial_maps: Vec<FxHashMap<String, f64>> = all_likers_data
        .into_par_iter()
        .map(|likers| {
            let mut partial: FxHashMap<String, f64> = FxHashMap::default();
            for (source_did_hash, source_like_time) in likers {
                if source_did_hash == user_hash {
                    continue;
                }
                let age_seconds = now - source_like_time;
                let recency_weight = (decay_factor * age_seconds).exp();
                *partial.entry(source_did_hash).or_insert(0.0) += recency_weight;
            }
            partial
        })
        .collect();

    // Merge all partial maps
    let mut source_weights: FxHashMap<String, f64> = FxHashMap::default();
    for partial in partial_maps {
        for (source, weight) in partial {
            *source_weights.entry(source).or_insert(0.0) += weight;
        }
    }

    if source_weights.is_empty() {
        return Vec::new();
    }

    // Collect into vec and sort by weight descending
    let mut sorted_sources: Vec<(String, f64)> = source_weights.into_iter().collect();
    sorted_sources.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal));

    // Take top N
    sorted_sources.truncate(max_total_sources);

    sorted_sources
}

/// Aggregate co-liker weights with full LinkLonk normalization.
///
/// This implements the mathematically correct LinkLonk formula with all three
/// branching factor normalizations:
///   - 1/|user_likes| (Step 1 normalization)
///   - 1/|sources_who_liked_j| (Step 2 normalization)
///   - 1/|items_s_liked| (Step 3 normalization)
///
/// # Arguments
/// * `all_likers_data` - List of likers for each post: [[(liker_hash, like_time), ...], ...]
/// * `user_hash` - The user's hash (to exclude self from results)
/// * `user_likes_count` - Total number of posts the user has liked (for Step 1 normalization)
/// * `source_like_counts` - Map of source_hash -> total_likes for Step 3 normalization
/// * `now` - Current timestamp (unix seconds)
/// * `recency_half_life_seconds` - Half-life for recency decay
/// * `max_total_sources` - Maximum number of sources to return
/// * `max_coliker_weight` - Maximum weight cap per co-liker (0.0 = no cap)
///
/// # Returns
/// List of (source_hash, weight) sorted by weight descending, up to max_total_sources
#[allow(clippy::too_many_arguments)]
pub fn aggregate_coliker_weights_normalized(
    all_likers_data: Vec<Vec<(String, f64)>>,
    user_hash: &str,
    user_likes_count: usize,
    source_like_counts: &FxHashMap<String, i64>,
    now: f64,
    recency_half_life_seconds: f64,
    max_total_sources: usize,
    max_coliker_weight: f64,
) -> Vec<(String, f64)> {
    if all_likers_data.is_empty() || user_likes_count == 0 {
        return Vec::new();
    }

    let decay_factor = -LN_2 / recency_half_life_seconds;

    // Step 1 normalization: 1/|user_likes|
    let user_likes_norm = 1.0 / user_likes_count as f64;

    // Aggregate weights into a hash map
    let mut source_weights: FxHashMap<String, f64> = FxHashMap::default();

    for likers in all_likers_data {
        // Step 2 normalization: 1/|sources_who_liked_j|
        // This is the number of sources who liked this particular post
        let sources_who_liked_j = likers.len() as f64;
        if sources_who_liked_j == 0.0 {
            continue;
        }
        let step2_norm = 1.0 / sources_who_liked_j;

        for (source_did_hash, source_like_time) in likers {
            // Skip self
            if source_did_hash == user_hash {
                continue;
            }

            // Step 3 normalization: 1/|items_s_liked|
            // How prolific is this source? More likes = less weight per item
            let source_total_likes = source_like_counts
                .get(&source_did_hash)
                .copied()
                .unwrap_or(1) // Default to 1 to avoid division by zero
                .max(1) as f64;
            let step3_norm = 1.0 / source_total_likes;

            // Calculate recency weight using exponential decay
            let age_seconds = now - source_like_time;
            let recency_weight = (decay_factor * age_seconds).exp();

            // Full LinkLonk formula with all normalizations
            let contribution = user_likes_norm * step2_norm * step3_norm * recency_weight;

            // Accumulate weight for this source
            *source_weights.entry(source_did_hash).or_insert(0.0) += contribution;
        }
    }

    if source_weights.is_empty() {
        return Vec::new();
    }

    // Apply weight cap if configured (cap > 0)
    let sorted_sources: Vec<(String, f64)> = if max_coliker_weight > 0.0 {
        source_weights
            .into_iter()
            .map(|(hash, weight)| (hash, weight.min(max_coliker_weight)))
            .collect()
    } else {
        source_weights.into_iter().collect()
    };

    // Sort by weight descending
    let mut sorted_sources = sorted_sources;
    sorted_sources.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal));

    // Take top N
    sorted_sources.truncate(max_total_sources);

    sorted_sources
}

/// Aggregate co-liker weights with full LinkLonk normalization (parallel version).
///
/// Uses rayon for parallel processing when dataset is large enough.
#[allow(clippy::too_many_arguments)]
pub fn aggregate_coliker_weights_normalized_parallel(
    all_likers_data: Vec<Vec<(String, f64)>>,
    user_hash: &str,
    user_likes_count: usize,
    source_like_counts: &FxHashMap<String, i64>,
    now: f64,
    recency_half_life_seconds: f64,
    max_total_sources: usize,
    max_coliker_weight: f64,
) -> Vec<(String, f64)> {
    if all_likers_data.len() < PARALLEL_THRESHOLD {
        return aggregate_coliker_weights_normalized(
            all_likers_data,
            user_hash,
            user_likes_count,
            source_like_counts,
            now,
            recency_half_life_seconds,
            max_total_sources,
            max_coliker_weight,
        );
    }

    if all_likers_data.is_empty() || user_likes_count == 0 {
        return Vec::new();
    }

    let decay_factor = -LN_2 / recency_half_life_seconds;
    let user_likes_norm = 1.0 / user_likes_count as f64;

    // Process each post's likers in parallel, producing partial weight maps
    let partial_maps: Vec<FxHashMap<String, f64>> = all_likers_data
        .into_par_iter()
        .map(|likers| {
            let mut partial: FxHashMap<String, f64> = FxHashMap::default();

            let sources_who_liked_j = likers.len() as f64;
            if sources_who_liked_j == 0.0 {
                return partial;
            }
            let step2_norm = 1.0 / sources_who_liked_j;

            for (source_did_hash, source_like_time) in likers {
                if source_did_hash == user_hash {
                    continue;
                }

                let source_total_likes = source_like_counts
                    .get(&source_did_hash)
                    .copied()
                    .unwrap_or(1)
                    .max(1) as f64;
                let step3_norm = 1.0 / source_total_likes;

                let age_seconds = now - source_like_time;
                let recency_weight = (decay_factor * age_seconds).exp();

                let contribution = user_likes_norm * step2_norm * step3_norm * recency_weight;
                *partial.entry(source_did_hash).or_insert(0.0) += contribution;
            }
            partial
        })
        .collect();

    // Merge all partial maps
    let mut source_weights: FxHashMap<String, f64> = FxHashMap::default();
    for partial in partial_maps {
        for (source, weight) in partial {
            *source_weights.entry(source).or_insert(0.0) += weight;
        }
    }

    if source_weights.is_empty() {
        return Vec::new();
    }

    // Apply weight cap if configured (cap > 0)
    let sorted_sources: Vec<(String, f64)> = if max_coliker_weight > 0.0 {
        source_weights
            .into_iter()
            .map(|(hash, weight)| (hash, weight.min(max_coliker_weight)))
            .collect()
    } else {
        source_weights.into_iter().collect()
    };

    // Sort by weight descending
    let mut sorted_sources = sorted_sources;
    sorted_sources.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal));

    // Take top N
    sorted_sources.truncate(max_total_sources);

    sorted_sources
}

/// Convert a standard HashMap to FxHashMap for faster lookups.
pub fn to_fx_hashmap<K: std::hash::Hash + Eq, V>(
    map: std::collections::HashMap<K, V>,
) -> FxHashMap<K, V> {
    map.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_score_posts_basic() {
        let posts_data = vec![
            (
                "post1".to_string(),
                vec![
                    ("user_a".to_string(), 1000.0),
                    ("user_b".to_string(), 900.0),
                ],
            ),
            (
                "post2".to_string(),
                vec![
                    ("user_c".to_string(), 950.0), // Not a co-liker
                ],
            ),
        ];

        let mut source_weights = FxHashMap::default();
        source_weights.insert("user_a".to_string(), 1.0);
        source_weights.insert("user_b".to_string(), 0.5);

        let now = 1000.0;
        let half_life = 3600.0 * 72.0; // 72 hours

        let results = score_posts(posts_data, &source_weights, now, half_life);

        // post1 should have a score (has co-likers)
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].post_id, "post1");
        assert!(results[0].score > 0.0);
        assert_eq!(results[0].matching_count, 2); // 2 matching likers
    }

    #[test]
    fn test_score_empty() {
        let posts_data: Vec<(String, Vec<(String, f64)>)> = vec![];
        let source_weights = FxHashMap::default();

        let results = score_posts(posts_data, &source_weights, 1000.0, 3600.0);
        assert!(results.is_empty());
    }

    #[test]
    fn test_recency_decay() {
        let posts_data = vec![
            (
                "recent".to_string(),
                vec![("user_a".to_string(), 990.0)], // 10s ago
            ),
            (
                "old".to_string(),
                vec![("user_a".to_string(), 0.0)], // 1000s ago
            ),
        ];

        let mut source_weights = FxHashMap::default();
        source_weights.insert("user_a".to_string(), 1.0);

        let results = score_posts(posts_data, &source_weights, 1000.0, 100.0);

        // Both should score, but recent should score higher
        assert_eq!(results.len(), 2);

        let recent_score = results
            .iter()
            .find(|r| r.post_id == "recent")
            .unwrap()
            .score;
        let old_score = results.iter().find(|r| r.post_id == "old").unwrap().score;

        assert!(recent_score > old_score);
    }

    #[test]
    fn test_aggregate_coliker_weights_basic() {
        // Simulate: user liked 2 posts, each with 2 likers
        let all_likers_data = vec![
            vec![
                ("source_a".to_string(), 900.0), // liked 100s ago
                ("source_b".to_string(), 950.0), // liked 50s ago
            ],
            vec![
                ("source_a".to_string(), 800.0), // liked 200s ago (same source!)
                ("source_c".to_string(), 990.0), // liked 10s ago
            ],
        ];

        let user_hash = "current_user";
        let now = 1000.0;
        let half_life = 3600.0; // 1 hour
        let max_sources = 10;

        let results =
            aggregate_coliker_weights(all_likers_data, user_hash, now, half_life, max_sources);

        // Should have 3 unique sources (a, b, c)
        assert_eq!(results.len(), 3);

        // source_a should have highest weight (appears twice)
        assert_eq!(results[0].0, "source_a");

        // All weights should be positive
        for (_, weight) in &results {
            assert!(*weight > 0.0);
        }
    }

    #[test]
    fn test_aggregate_coliker_weights_excludes_self() {
        let all_likers_data = vec![vec![
            ("other_user".to_string(), 900.0),
            ("current_user".to_string(), 950.0), // This is self, should be excluded
        ]];

        let user_hash = "current_user";
        let now = 1000.0;
        let half_life = 3600.0;
        let max_sources = 10;

        let results =
            aggregate_coliker_weights(all_likers_data, user_hash, now, half_life, max_sources);

        // Should only have 1 source (self excluded)
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, "other_user");
    }

    #[test]
    fn test_score_posts_topk() {
        // Create posts with varying scores
        let posts_data: Vec<(String, Vec<(String, f64)>)> = (0..100)
            .map(|i| {
                (
                    format!("post_{}", i),
                    vec![
                        // More recent = higher score, so post_99 should score highest
                        ("user_a".to_string(), 900.0 + i as f64),
                    ],
                )
            })
            .collect();

        let mut source_weights = FxHashMap::default();
        source_weights.insert("user_a".to_string(), 1.0);

        // Request top 10
        let results = score_posts_topk(posts_data, &source_weights, 1000.0, 100.0, 10);

        // Should only return 10 results
        assert_eq!(results.len(), 10);

        // Results should be sorted by score descending
        for i in 1..results.len() {
            assert!(
                results[i - 1].score >= results[i].score,
                "Results not sorted"
            );
        }

        // Top result should be post_99 (most recent like)
        assert_eq!(results[0].post_id, "post_99");
    }

    #[test]
    fn test_score_posts_parallel() {
        // Create enough posts to trigger parallel processing
        let posts_data: Vec<(String, Vec<(String, f64)>)> = (0..2000)
            .map(|i| {
                (
                    format!("post_{}", i),
                    vec![("user_a".to_string(), 990.0), ("user_b".to_string(), 980.0)],
                )
            })
            .collect();

        let mut source_weights = FxHashMap::default();
        source_weights.insert("user_a".to_string(), 1.0);
        source_weights.insert("user_b".to_string(), 0.5);

        let results = score_posts_parallel(posts_data, &source_weights, 1000.0, 3600.0);

        // All posts should score
        assert_eq!(results.len(), 2000);

        // All scores should be positive
        for result in &results {
            assert!(result.score > 0.0);
            assert_eq!(result.matching_count, 2);
        }
    }
}
