//! Python-style scorer for LinkLonk algorithm.
//!
//! This module implements the inverted algorithm scoring, using Redis
//! purely as a data store. This reduces Redis CPU load significantly
//! since Lua scripts block Redis's single thread.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use rand::seq::SliceRandom;
use rand::thread_rng;
use rustc_hash::{FxHashMap, FxHashSet};
use tracing::debug;

use crate::algorithm::liker_cache::LikerCache;
use crate::algorithm::params::LinkLonkParams;
use crate::audit::{AuditCollector, PostBreakdownData};
use crate::config::Config;
use crate::error::Result;
use graze_common::{Keys, RedisClient, DEFAULT_RETENTION_DAYS};

/// Maximum results to return from scorer (early termination optimization).
const DEFAULT_MAX_SCORER_RESULTS: usize = 500;

/// Result of scoring operation, including the scored posts themselves.
#[derive(Debug, Clone)]
pub struct ScoringResult {
    /// The scored posts: (score, post_id)
    pub scored_posts: Vec<(f64, String)>,
    pub scored_count: usize,
    pub posts_checked: usize,
    pub posts_skipped_no_likers: usize,
    pub posts_skipped_few_likers: usize,
    pub scoring_time_ms: f64,
    /// Cache statistics for this scoring run
    pub cache_hits: usize,
    pub cache_misses: usize,
}

/// Scorer for the inverted LinkLonk algorithm.
///
/// This replaces Lua script execution with Rust-side scoring,
/// using Redis purely for data storage.
pub struct Scorer {
    redis: Arc<RedisClient>,
    liker_cache: Arc<LikerCache>,
    min_post_likes: usize,
    max_likers_per_post: usize,
    max_posts_to_score: usize,
    max_coliker_weight: f64,
    min_overlapping_colikers: usize,
    read_only: bool,
    liker_cache_enabled: bool,
}

impl Scorer {
    /// Create a new scorer.
    pub fn new(redis: Arc<RedisClient>, liker_cache: Arc<LikerCache>, config: Arc<Config>) -> Self {
        Self {
            redis,
            liker_cache,
            min_post_likes: config.inverted_min_post_likes,
            max_likers_per_post: config.inverted_max_likers_per_post,
            max_posts_to_score: config.inverted_max_posts_to_score,
            max_coliker_weight: config.max_coliker_weight,
            min_overlapping_colikers: config.min_overlapping_colikers,
            read_only: config.read_only_mode,
            liker_cache_enabled: config.liker_cache_enabled,
        }
    }

    /// Score posts for a user using the inverted algorithm.
    ///
    /// If `audit` is provided, detailed scoring information will be collected.
    /// Returns scored posts directly to avoid re-fetching from Redis.
    pub async fn score(
        &self,
        user_hash: &str,
        algo_id: i32,
        source_weights: &HashMap<String, f64>,
        params: &LinkLonkParams,
        mut audit: Option<&mut AuditCollector>,
    ) -> Result<ScoringResult> {
        let start_time = Instant::now();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs_f64();
        let time_window_seconds = params.time_window_hours * 3600.0;
        let min_time = now - time_window_seconds;
        let recency_half_life_seconds = params.recency_half_life_hours * 3600.0;

        // Convert to FxHashMap for faster lookups in the hot scoring loop
        let source_weights: FxHashMap<&String, f64> =
            source_weights.iter().map(|(k, v)| (k, *v)).collect();

        // Keys for data
        let result_key = Keys::cached_result(algo_id, user_hash);
        let algo_posts_key = Keys::algo_posts(algo_id);
        let algo_counts_key = Keys::algo_posts_counts(algo_id);
        let user_likes_keys = Keys::user_likes_retention(user_hash, DEFAULT_RETENTION_DAYS);
        let user_seen_key = Keys::user_seen(user_hash);

        // Step 1: Load algo posts, user's likes, and seen posts IN PARALLEL
        // If seed_sample_pool > 0, fetch a larger pool for random sampling
        let fetch_limit = if params.seed_sample_pool > 0 {
            params.seed_sample_pool.max(params.max_user_likes)
        } else {
            params.max_user_likes
        };
        let seen_limit = if params.max_seen_posts > 0 {
            (params.max_seen_posts as isize) - 1
        } else {
            -1
        };
        let (algo_posts_result, user_likes_result, seen_posts_result) = tokio::join!(
            self.redis.smembers(&algo_posts_key),
            self.redis
                .zrevrangebyscore_merged(&user_likes_keys, now, min_time, fetch_limit),
            self.redis.zrevrange(&user_seen_key, 0, seen_limit)
        );

        let algo_posts: Vec<String> = algo_posts_result?;
        let mut user_likes = user_likes_result?;
        let seen_posts: Vec<String> = seen_posts_result.unwrap_or_default();

        // Random seed sampling: shuffle and truncate to max_user_likes
        if params.seed_sample_pool > 0 && user_likes.len() > params.max_user_likes {
            user_likes.shuffle(&mut thread_rng());
            user_likes.truncate(params.max_user_likes);
        }

        if algo_posts.is_empty() {
            debug!(
                user_hash = %&user_hash[..8.min(user_hash.len())],
                algo_id,
                "early_exit: no candidates in algo posts set"
            );
            return Ok(ScoringResult {
                scored_posts: Vec::new(),
                scored_count: 0,
                posts_checked: 0,
                posts_skipped_no_likers: 0,
                posts_skipped_few_likers: 0,
                scoring_time_ms: start_time.elapsed().as_secs_f64() * 1000.0,
                cache_hits: 0,
                cache_misses: 0,
            });
        }

        let mut excluded_posts: FxHashSet<String> =
            user_likes.into_iter().map(|(id, _)| id).collect();
        excluded_posts.extend(seen_posts);

        // Step 2: Filter posts (remove already liked or seen)
        let mut candidates: Vec<String> = algo_posts
            .into_iter()
            .filter(|p| !excluded_posts.contains(p))
            .collect();

        if self.max_posts_to_score > 0 && candidates.len() > self.max_posts_to_score {
            candidates.truncate(self.max_posts_to_score);
        }

        if candidates.is_empty() {
            return Ok(ScoringResult {
                scored_posts: Vec::new(),
                scored_count: 0,
                posts_checked: 0,
                posts_skipped_no_likers: 0,
                posts_skipped_few_likers: 0,
                scoring_time_ms: start_time.elapsed().as_secs_f64() * 1000.0,
                cache_hits: 0,
                cache_misses: 0,
            });
        }

        // Step 3: Get pre-computed liker counts from hash (faster than ZCARD)
        let post_id_refs: Vec<&str> = candidates.iter().map(|s| s.as_str()).collect();
        let count_strs = self.redis.hmget(&algo_counts_key, &post_id_refs).await?;

        // Parse counts, defaulting to 0 for missing entries
        let liker_counts: Vec<usize> = count_strs
            .into_iter()
            .map(|opt| opt.and_then(|s| s.parse().ok()).unwrap_or(0))
            .collect();

        // Step 4: Filter by min likes and prepare fetch list
        let mut posts_skipped_no_likers = 0;
        let mut posts_skipped_few_likers = 0;
        let mut posts_to_score: Vec<(&str, usize)> = Vec::with_capacity(candidates.len());

        for (post_id, count) in candidates.iter().zip(liker_counts.iter()) {
            if *count == 0 {
                posts_skipped_no_likers += 1;
            } else if *count < self.min_post_likes {
                posts_skipped_few_likers += 1;
            } else {
                posts_to_score.push((post_id.as_str(), *count));
            }
        }

        if posts_to_score.is_empty() {
            return Ok(ScoringResult {
                scored_posts: Vec::new(),
                scored_count: 0,
                posts_checked: candidates.len(),
                posts_skipped_no_likers,
                posts_skipped_few_likers,
                scoring_time_ms: start_time.elapsed().as_secs_f64() * 1000.0,
                cache_hits: 0,
                cache_misses: 0,
            });
        }

        // Step 5: Fetch likers - check local cache first, then batch fetch misses
        let mut cache_hits = 0;
        let mut cache_misses = 0;
        let mut all_likers: Vec<Vec<(String, f64)>> = Vec::with_capacity(posts_to_score.len());
        let mut cache_miss_indices: Vec<usize> = Vec::new();
        let mut cache_miss_post_ids: Vec<&str> = Vec::new();

        // Check cache for each post
        if self.liker_cache_enabled {
            for (i, (post_id, _)) in posts_to_score.iter().enumerate() {
                if let Some(entry) = self.liker_cache.get(post_id) {
                    // Filter by time window - use owned version to avoid extra clone
                    let filtered =
                        LikerCache::filter_likers_by_time_owned(entry.likers, min_time, now);
                    all_likers.push(filtered);
                    cache_hits += 1;
                } else {
                    // Mark for fetching
                    all_likers.push(Vec::new()); // Placeholder
                    cache_miss_indices.push(i);
                    cache_miss_post_ids.push(post_id);
                    cache_misses += 1;
                }
            }
        } else {
            // Cache disabled - fetch all
            for (post_id, _) in &posts_to_score {
                all_likers.push(Vec::new());
                cache_miss_indices.push(all_likers.len() - 1);
                cache_miss_post_ids.push(post_id);
            }
            cache_misses = posts_to_score.len();
        }

        // Batch fetch cache misses from Redis using date-based keys
        if !cache_miss_post_ids.is_empty() {
            // Build date-based key groups for each post
            let post_key_groups: Vec<Vec<String>> = cache_miss_post_ids
                .iter()
                .map(|post_id| Keys::post_likers_retention(post_id, DEFAULT_RETENTION_DAYS))
                .collect();

            // Fetch from all date-based keys and merge results per post
            let fetched_likers = self
                .redis
                .zrevrangebyscore_merged_multi(
                    &post_key_groups,
                    now,
                    min_time,
                    self.max_likers_per_post,
                )
                .await?;

            // Fill in results for scoring
            for (idx, likers) in cache_miss_indices.iter().zip(fetched_likers.into_iter()) {
                all_likers[*idx] = likers;
            }
        }

        // Step 5b: Build corater rank map for decay (zero-cost when corater_decay == 0.0)
        // For each co-liker, ranks their likes across candidate posts by recency.
        // rank 0 = most recent like, rank 1 = second most recent, etc.
        // Structure: post_index -> co_liker_hash -> rank
        let corater_decay = params.corater_decay;
        let corater_ranks: FxHashMap<usize, FxHashMap<&str, usize>> = if corater_decay > 0.0 {
            // Transpose all_likers into per-co-liker view: co_liker_hash -> [(post_idx, like_time)]
            let mut per_coliker: FxHashMap<&str, Vec<(usize, f64)>> = FxHashMap::default();
            for (post_idx, likers) in all_likers.iter().enumerate() {
                for (liker_hash, like_time) in likers {
                    if source_weights.contains_key(liker_hash) {
                        per_coliker
                            .entry(liker_hash.as_str())
                            .or_default()
                            .push((post_idx, *like_time));
                    }
                }
            }
            // Sort each co-liker's likes by time descending, assign ranks, build lookup
            let mut ranks: FxHashMap<usize, FxHashMap<&str, usize>> = FxHashMap::default();
            for (co_liker, mut entries) in per_coliker {
                entries.sort_by(|a, b| {
                    b.1.partial_cmp(&a.1)
                        .unwrap_or(std::cmp::Ordering::Equal)
                });
                for (rank, (post_idx, _)) in entries.iter().enumerate() {
                    ranks.entry(*post_idx).or_default().insert(co_liker, rank);
                }
            }
            ranks
        } else {
            FxHashMap::default()
        };
        let decay_base = 1.0 - corater_decay;

        // Step 6: Score all posts
        let estimated_capacity =
            (posts_to_score.len() / 5).clamp(100, DEFAULT_MAX_SCORER_RESULTS * 2);
        let mut post_scores: Vec<(f64, String)> = Vec::with_capacity(estimated_capacity);

        if let Some(ref mut a) = audit {
            // Audit-enabled path
            for (post_idx, ((post_id, liker_count), likers)) in
                posts_to_score.iter().zip(all_likers.iter()).enumerate()
            {
                let mut score = 0.0;
                let mut overlap_count = 0usize;
                let post_ranks = corater_ranks.get(&post_idx);

                for (liker_hash, like_time) in likers {
                    if let Some(weight) = source_weights.get(liker_hash) {
                        overlap_count += 1;
                        // Cap the co-liker weight to prevent any single user from dominating
                        let capped_weight = weight.min(self.max_coliker_weight);
                        let age_seconds = now - like_time;
                        let recency_weight =
                            (-0.693 * age_seconds / recency_half_life_seconds).exp();
                        // Apply corater decay: (1 - decay)^rank where rank 0 = most recent
                        let decay_mult = if corater_decay > 0.0 {
                            post_ranks
                                .and_then(|pr| pr.get(liker_hash.as_str()))
                                .map(|&rank| decay_base.powi(rank as i32))
                                .unwrap_or(1.0)
                        } else {
                            1.0
                        };
                        let contribution = capped_weight * recency_weight * decay_mult;
                        score += contribution;

                        a.add_contribution(
                            post_id,
                            liker_hash,
                            *weight,
                            recency_weight,
                            contribution,
                            age_seconds,
                        );
                    }
                }

                // Skip posts without enough overlapping co-likers (prevents single-user contamination)
                if overlap_count < self.min_overlapping_colikers {
                    continue;
                }

                if score > 0.0 {
                    // (1) Num paths exponent: boost posts with more distinct co-liker paths
                    let num_paths = overlap_count.max(1) as f64;
                    let paths_boost = num_paths.powf(params.num_paths_power);
                    let score_after_paths = score * paths_boost;
                    // (2) Popularity exponent: demote viral posts
                    let popularity_penalty = if params.popularity_power > 0.0 && *liker_count > 1 {
                        (1.0 / *liker_count as f64).powf(params.popularity_power * 0.5)
                    } else {
                        1.0
                    };
                    let final_score = score_after_paths * popularity_penalty;
                    a.set_post_breakdown(
                        post_id,
                        PostBreakdownData {
                            raw_score: score,
                            final_score,
                            popularity_penalty,
                            liker_count: *liker_count,
                            num_paths: overlap_count,
                            paths_boost,
                        },
                    );
                    post_scores.push((final_score, (*post_id).to_string()));
                }
            }
        } else {
            // Fast path - no audit
            for (post_idx, ((post_id, liker_count), likers)) in
                posts_to_score.iter().zip(all_likers.iter()).enumerate()
            {
                let mut score = 0.0;
                let mut overlap_count = 0usize;
                let post_ranks = corater_ranks.get(&post_idx);

                for (liker_hash, like_time) in likers {
                    if let Some(weight) = source_weights.get(liker_hash) {
                        overlap_count += 1;
                        // Cap the co-liker weight to prevent any single user from dominating
                        let capped_weight = weight.min(self.max_coliker_weight);
                        let age_seconds = now - like_time;
                        let recency_weight =
                            (-0.693 * age_seconds / recency_half_life_seconds).exp();
                        // Apply corater decay: (1 - decay)^rank where rank 0 = most recent
                        let decay_mult = if corater_decay > 0.0 {
                            post_ranks
                                .and_then(|pr| pr.get(liker_hash.as_str()))
                                .map(|&rank| decay_base.powi(rank as i32))
                                .unwrap_or(1.0)
                        } else {
                            1.0
                        };
                        score += capped_weight * recency_weight * decay_mult;
                    }
                }

                // Skip posts without enough overlapping co-likers (prevents single-user contamination)
                if overlap_count < self.min_overlapping_colikers {
                    continue;
                }

                if score > 0.0 {
                    // (1) Num paths exponent: boost posts with more distinct co-liker paths
                    let num_paths = overlap_count.max(1) as f64;
                    let paths_boost = num_paths.powf(params.num_paths_power);
                    let score_after_paths = score * paths_boost;
                    // (2) Popularity exponent: demote viral posts
                    let popularity_penalty = if params.popularity_power > 0.0 && *liker_count > 1 {
                        (1.0 / *liker_count as f64).powf(params.popularity_power * 0.5)
                    } else {
                        1.0
                    };
                    post_scores.push((
                        score_after_paths * popularity_penalty,
                        (*post_id).to_string(),
                    ));
                }
            }
        }

        // Sort and truncate
        post_scores.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap());
        post_scores.truncate(DEFAULT_MAX_SCORER_RESULTS);

        let scored_count = post_scores.len();

        // Populate local liker cache with fetched data (after scoring is done)
        // This clone is necessary since the cache needs owned data
        if self.liker_cache_enabled && !cache_miss_indices.is_empty() {
            for idx in &cache_miss_indices {
                let likers = &all_likers[*idx];
                if !likers.is_empty() {
                    let (post_id, liker_count) = posts_to_score[*idx];
                    self.liker_cache
                        .set(post_id.to_string(), likers.clone(), liker_count);
                }
            }
        }

        // Store in Redis cache for future requests (single pipelined call)
        if !post_scores.is_empty() && !self.read_only {
            let items: Vec<(f64, &str)> = post_scores
                .iter()
                .map(|(score, id)| (*score, id.as_str()))
                .collect();
            self.redis
                .store_sorted_set(&result_key, &items, params.result_ttl_seconds as i64)
                .await?;
        }

        let scoring_time_ms = start_time.elapsed().as_secs_f64() * 1000.0;

        debug!(
            user_hash = %&user_hash[..8.min(user_hash.len())],
            algo_id,
            scored_count,
            posts_checked = candidates.len(),
            cache_hits,
            cache_misses,
            scoring_time_ms = format!("{:.2}", scoring_time_ms),
            "scorer_completed"
        );

        Ok(ScoringResult {
            scored_posts: post_scores,
            scored_count,
            posts_checked: candidates.len(),
            posts_skipped_no_likers,
            posts_skipped_few_likers,
            scoring_time_ms,
            cache_hits,
            cache_misses,
        })
    }
}
