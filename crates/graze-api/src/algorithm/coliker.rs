//! Co-Liker Pre-computation Worker.
//!
//! This module pre-computes "users who liked similar posts" relationships
//! to dramatically reduce Step 2 computation time in the LinkLonk algorithm.
//!
//! The computation:
//! 1. Gets a user's recent likes
//! 2. For each liked post, finds other users who liked it (before the user)
//! 3. Aggregates weights based on recency (using optimized scoring_core)
//! 4. Stores top N co-likers with their accumulated weights
//!
//! When LinkLonk normalization is enabled, the aggregation includes:
//! - 1/|user_likes| (Step 1 normalization)
//! - 1/|sources_who_liked_j| (Step 2 normalization)
//! - 1/|items_s_liked| (Step 3 normalization)
//!
//! This converts O(n) per-request computation into O(1) lookup.

use std::collections::HashMap;

use rand::seq::SliceRandom;
use rand::SeedableRng;
use rustc_hash::FxHashSet;
use std::sync::Arc;
use std::time::Instant;

use rustc_hash::FxHashMap;
use tracing::debug;

use crate::algorithm::scoring_core::{
    aggregate_coliker_weights_normalized_parallel, aggregate_coliker_weights_parallel,
};
use crate::config::Config;
use crate::error::Result;
use graze_common::services::UriInterner;
use graze_common::{author_did_from_at_uri, hash_did, Keys, RedisClient, DEFAULT_RETENTION_DAYS};

/// Rescale stored durable-profile scores into scorer weights.
///
/// Stored scores are `Σ 1/L_j` (order 1e-3..1e2); the scorer expects live-path magnitudes
/// (~2e-7) and clamps at `max_coliker_weight` (1e-6). Dividing by the maximum puts the top
/// entry exactly at `target` and everything else below it, so:
///
/// - ranking is preserved exactly (a positive affine map on positive scores);
/// - no weight can reach the clamp, provided `target < max_coliker_weight`, which keeps
///   `weight.min(cap)` from flattening the profile into ties;
/// - a 12-member profile is not handed systematically larger weights than a 128-member one,
///   which is why this normalises by max rather than by sum.
///
/// Returns `None` when nothing usable survives — no entries, or no positive finite score
/// (which would also make the division ill-defined).
fn profile_weights_from_scores(
    entries: Vec<(String, f64)>,
    target: f64,
) -> Option<HashMap<String, f64>> {
    let max_score = entries
        .iter()
        .map(|(_, s)| *s)
        .filter(|s| s.is_finite())
        .fold(0.0_f64, f64::max);

    if max_score <= 0.0 || !target.is_finite() || target <= 0.0 {
        return None;
    }

    let weights: HashMap<String, f64> = entries
        .into_iter()
        .filter(|(hash, s)| !hash.is_empty() && s.is_finite() && *s > 0.0)
        .map(|(hash, score)| (hash, target * (score / max_score)))
        .collect();

    if weights.is_empty() {
        None
    } else {
        Some(weights)
    }
}

/// Computes and caches co-liker aggregations for users.
pub struct ColikerWorker {
    redis: Arc<RedisClient>,
    config: Arc<Config>,
    /// Needed only by the per-feed seeding path, to resolve seed post IDs to author DIDs. The
    /// interner has its own LRU, so hot posts cost nothing and a full miss costs one pipelined
    /// lookup for the whole seed.
    interner: Arc<UriInterner>,
}

/// A per-(user, day) deterministic RNG for seed sampling.
///
/// Stable within a request — which is what makes any two rankers in one request comparable — and
/// across a day, which matches how long the derived weights are cached anyway.
pub(crate) fn daily_rng(user_hash: &str) -> rand::rngs::StdRng {
    use std::hash::{Hash, Hasher};
    let mut hasher = rustc_hash::FxHasher::default();
    user_hash.hash(&mut hasher);
    graze_common::today_date().hash(&mut hasher);
    rand::rngs::StdRng::seed_from_u64(hasher.finish())
}

/// What the per-feed seed filter did, for telemetry.
///
/// Recorded because the whole hypothesis is falsifiable through these numbers: if `kept` is
/// routinely near `seen`, the filter is inert and the experiment cannot show anything; if `kept`
/// collapses to near zero, the treatment is really "no personalization" wearing a ranker's name,
/// which would look like a ranking result while being a coverage result.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SeedFilterStats {
    /// Seed posts considered.
    pub seen: usize,
    /// Seed posts whose author is in the feed's pool.
    pub kept: usize,
    /// Seed posts whose author could not be resolved, and were therefore kept.
    pub unresolved: usize,
}

impl SeedFilterStats {
    /// Fraction of the seed retained, or 1.0 for an empty seed.
    pub fn keep_rate(&self) -> f64 {
        if self.seen == 0 {
            return 1.0;
        }
        self.kept as f64 / self.seen as f64
    }
}

impl ColikerWorker {
    /// Create a new co-liker worker.
    pub fn new(redis: Arc<RedisClient>, config: Arc<Config>, interner: Arc<UriInterner>) -> Self {
        Self {
            redis,
            config,
            interner,
        }
    }

    /// Load the durable co-liker profile (`ucl:{hash}`) built offline from long-range like
    /// history, converted to scorer weights.
    ///
    /// Returns `None` when no profile exists — the common case, since profiles are only
    /// built for users with ≥20 likes of history who requested a feed recently.
    ///
    /// # Weight conversion
    ///
    /// Stored scores are `Σ 1/L_j` over overlapping posts (order 1e-3..1e2), whereas the
    /// live path produces weights around 2e-7 and the scorer clamps at
    /// `max_coliker_weight` (1e-6). Absolute magnitude is load-bearing *only* at that clamp
    /// and at `score > 0.0`, so a rank-preserving rescale that puts the top entry at
    /// `durable_profile_weight_target` reproduces the live regime without needing the live
    /// normalization terms. Scaling by the max (rather than the sum) keeps a small profile
    /// from being handed systematically larger per-entry weights than a large one.
    pub async fn get_durable_profile(
        &self,
        user_hash: &str,
    ) -> Result<Option<HashMap<String, f64>>> {
        let key = Keys::user_colikers(user_hash);
        let Some(packed) = self.redis.get_bytes(&key).await? else {
            return Ok(None);
        };

        let entries = graze_common::decode_profile(&packed);
        let target = self.config.durable_profile_weight_target;

        let Some(weights) = profile_weights_from_scores(entries, target) else {
            debug!(
                user_hash = %&user_hash[..8.min(user_hash.len())],
                "durable_profile_unusable"
            );
            return Ok(None);
        };

        debug!(
            user_hash = %&user_hash[..8.min(user_hash.len())],
            profile_size = weights.len(),
            weight_target = target,
            "durable_profile_loaded"
        );

        Ok(Some(weights))
    }

    /// Get or compute co-liker weights for a user.
    ///
    /// This method checks if pre-computed co-likers exist and are fresh.
    /// If not, it triggers computation.
    ///
    /// Cache invalidation happens when:
    /// 1. TTL expires (coliker_ttl_seconds, default 6 hours)
    /// 2. User has new likes since cache was computed
    /// 3. force_refresh is true
    #[allow(clippy::too_many_arguments)]
    pub async fn get_or_compute_colikes(
        &self,
        user_hash: &str,
        max_user_likes: usize,
        max_sources_per_post: usize,
        max_total_sources: usize,
        time_window_seconds: u64,
        recency_half_life_seconds: u64,
        force_refresh: bool,
        seed_sample_pool: usize,
    ) -> Result<HashMap<String, f64>> {
        if !self.config.coliker_enabled {
            debug!(user_hash = %&user_hash[..8.min(user_hash.len())], "early_exit: coliker disabled");
            return Ok(HashMap::new());
        }

        let colikes_key = Keys::colikes(user_hash);
        let colikes_ts_key = Keys::colikes_timestamp(user_hash);

        if !force_refresh {
            // Check if cache exists and is fresh enough
            let ttl = self.redis.ttl(&colikes_key).await?;

            if ttl > 0 {
                // Cache exists - check if user has new likes since computation
                let should_invalidate = self
                    .check_user_has_new_likes(user_hash, &colikes_ts_key)
                    .await?;

                if should_invalidate {
                    debug!(
                        user_hash = %&user_hash[..8.min(user_hash.len())],
                        "cache_invalidated: user has new likes since computation"
                    );
                } else if ttl > self.config.coliker_refresh_threshold_seconds as i64 {
                    // Cache is fresh and user has no new likes, return it
                    return self.get_cached_colikes(&colikes_key).await;
                } else {
                    // Cache is getting stale but user has no new likes - still usable
                    let cached = self.get_cached_colikes(&colikes_key).await?;
                    if !cached.is_empty() {
                        return Ok(cached);
                    }
                }
            }
        }

        // Cache miss, invalidated, or force refresh - compute now
        self.compute_colikes(
            user_hash,
            max_user_likes,
            max_sources_per_post,
            max_total_sources,
            time_window_seconds,
            recency_half_life_seconds,
            seed_sample_pool,
        )
        .await
    }

    /// Derive co-liker weights from a seed restricted to authors this feed actually carries.
    ///
    /// **The hypothesis.** LinkLonk scopes step 1 of its walk to a channel; we have only ever scoped
    /// step 3 to the feed pool. Measured cost of scoping late: 34-56% of a user's top-128 co-likers
    /// contribute zero coverage on a given feed, and *none* of those were inactive accounts — they
    /// are active people who like different things. Filtering the seed instead should spend the same
    /// co-liker budget on people whose tastes overlap this feed.
    ///
    /// **The risk, stated plainly.** Users with little in-topic history get a smaller seed, and a
    /// smaller seed could reduce coverage rather than sharpen it. Three independent measurements say
    /// coverage is *not* seed-limited here (23x seed -> 1.1x coverage; 13,623 co-likers -> 44
    /// covered versus 455 -> 95; 117 seed -> 4 versus 9 seed -> 55), and this is the direct test of
    /// that. `SeedFilterStats` is returned so the experiment can distinguish a ranking result from a
    /// coverage collapse.
    ///
    /// Falls back to the unfiltered seed when `apa:{algo}` is missing — a sync that has not run yet
    /// must not silently turn personalization off.
    #[allow(clippy::too_many_arguments)]
    pub async fn get_or_compute_colikes_per_feed(
        &self,
        algo_id: i32,
        user_hash: &str,
        max_user_likes: usize,
        max_sources_per_post: usize,
        max_total_sources: usize,
        time_window_seconds: u64,
        recency_half_life_seconds: u64,
        force_refresh: bool,
        seed_sample_pool: usize,
    ) -> Result<(HashMap<String, f64>, SeedFilterStats)> {
        if !self.config.coliker_enabled {
            return Ok((HashMap::new(), SeedFilterStats::default()));
        }

        let cache_key = Keys::colikes_per_feed(algo_id, user_hash);
        if !force_refresh {
            let ttl = self.redis.ttl(&cache_key).await?;
            if ttl > self.config.coliker_refresh_threshold_seconds as i64 {
                let cached = self.get_cached_colikes(&cache_key).await?;
                if !cached.is_empty() {
                    // Stats are not cached: they describe the derivation, not the result, and a
                    // fabricated value here would be indistinguishable from a measured one.
                    return Ok((cached, SeedFilterStats::default()));
                }
            }
        }

        let start_time = Instant::now();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs_f64();
        let min_time = now - time_window_seconds as f64;

        // Fetch a wider seed than the unfaceted path, because the filter is about to discard some of
        // it. Without this the treatment would be confounded: a smaller *post-filter* seed than the
        // control's would make the comparison partly about seed size rather than seed composition.
        let fetch_limit = seed_sample_pool.max(max_user_likes) * 2;
        let user_likes_keys = Keys::user_likes_retention(user_hash, DEFAULT_RETENTION_DAYS);
        let candidate_seed = self
            .redis
            .zrevrangebyscore_merged(&user_likes_keys, now, min_time, fetch_limit)
            .await?;

        if candidate_seed.is_empty() {
            return Ok((HashMap::new(), SeedFilterStats::default()));
        }

        let (mut seed, stats) = self
            .filter_seed_to_pool_authors(algo_id, candidate_seed)
            .await?;

        if seed.len() > max_user_likes {
            if seed_sample_pool > 0 {
                seed.shuffle(&mut daily_rng(user_hash));
            }
            seed.truncate(max_user_likes);
        }

        debug!(
            user_hash = %&user_hash[..8.min(user_hash.len())],
            algo_id,
            seed_seen = stats.seen,
            seed_kept = stats.kept,
            seed_unresolved = stats.unresolved,
            keep_rate = stats.keep_rate(),
            seed_used = seed.len(),
            "per_feed_seed_filtered"
        );

        let weights = self
            .colikes_from_seed(
                user_hash,
                seed,
                max_sources_per_post,
                max_total_sources,
                now,
                min_time,
                recency_half_life_seconds,
                start_time,
            )
            .await?;
        Ok((weights, stats))
    }

    /// Keep only seed posts whose author appears in `apa:{algo}`.
    ///
    /// Two round trips regardless of seed size: one batched interner lookup, one `SMISMEMBER`.
    async fn filter_seed_to_pool_authors(
        &self,
        algo_id: i32,
        seed: Vec<(String, f64)>,
    ) -> Result<(Vec<(String, f64)>, SeedFilterStats)> {
        let authors_key = Keys::algo_pool_authors(algo_id);
        if !self.redis.exists(&authors_key).await? {
            // No pool author set (sync has not run, or it expired). Filtering against an absent set
            // would drop every seed post, so pass the seed through untouched instead.
            let seen = seed.len();
            return Ok((
                seed,
                SeedFilterStats {
                    seen,
                    kept: seen,
                    unresolved: seen,
                },
            ));
        }

        let post_ids: Vec<String> = seed.iter().map(|(id, _)| id.clone()).collect();
        let uris = self.interner.get_uris_batch(&post_ids).await?;

        // Resolve each seed post to an author hash. A post whose URI is no longer interned (its
        // `id2uri:` shard expired) is unresolvable, and is kept rather than dropped: a lookup
        // failure is not evidence that the feed lacks that author.
        let mut resolved: Vec<(usize, String)> = Vec::with_capacity(post_ids.len());
        let mut unresolved_idx: Vec<usize> = Vec::new();
        for (i, post_id) in post_ids.iter().enumerate() {
            match uris.get(post_id).and_then(|u| author_did_from_at_uri(u)) {
                Some(did) => resolved.push((i, hash_did(did))),
                None => unresolved_idx.push(i),
            }
        }

        let probe: Vec<String> = resolved.iter().map(|(_, h)| h.clone()).collect();
        let flags = self.redis.sismember_multi(&authors_key, &probe).await?;

        let mut keep = vec![false; seed.len()];
        for idx in &unresolved_idx {
            keep[*idx] = true;
        }
        let mut in_pool = 0usize;
        for ((i, _), is_member) in resolved.iter().zip(flags.iter()) {
            if *is_member {
                keep[*i] = true;
                in_pool += 1;
            }
        }

        let stats = SeedFilterStats {
            seen: seed.len(),
            kept: in_pool + unresolved_idx.len(),
            unresolved: unresolved_idx.len(),
        };
        let filtered: Vec<(String, f64)> = seed
            .into_iter()
            .zip(keep.iter())
            .filter_map(|(entry, k)| if *k { Some(entry) } else { None })
            .collect();
        Ok((filtered, stats))
    }

    /// Check if user has new likes since the co-likers cache was computed.
    async fn check_user_has_new_likes(
        &self,
        user_hash: &str,
        colikes_ts_key: &str,
    ) -> Result<bool> {
        // Get the timestamp when co-likers were last computed
        let cached_ts = self.redis.get_string(colikes_ts_key).await?;
        let Some(ts_str) = cached_ts else {
            // No timestamp stored - treat as needing refresh for safety
            return Ok(true);
        };

        let cached_timestamp: f64 = ts_str.parse().unwrap_or(0.0);
        if cached_timestamp == 0.0 {
            return Ok(true);
        }

        // Get user's most recent like timestamp from date-based keys
        // Only need to check today's key since that's where new likes go
        let today = graze_common::today_date();
        let user_likes_key = Keys::user_likes_date(user_hash, &today);
        let recent_likes = self
            .redis
            .zrevrangebyscore_with_scores(&user_likes_key, f64::INFINITY, cached_timestamp, 1)
            .await?;

        // If there are likes newer than cached_timestamp, cache is stale
        Ok(!recent_likes.is_empty())
    }

    /// Compute co-liker aggregation for a user.
    ///
    /// This replicates Step 2 of the Lua script but stores the result
    /// for later O(1) retrieval. Uses optimized parallel aggregation from scoring_core.
    #[allow(clippy::too_many_arguments)]
    pub async fn compute_colikes(
        &self,
        user_hash: &str,
        max_user_likes: usize,
        max_sources_per_post: usize,
        max_total_sources: usize,
        time_window_seconds: u64,
        recency_half_life_seconds: u64,
        seed_sample_pool: usize,
    ) -> Result<HashMap<String, f64>> {
        let start_time = Instant::now();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs_f64();
        let min_time = now - time_window_seconds as f64;

        // Step 1: Get user's recent likes from date-based keys
        // If seed_sample_pool > 0, fetch a larger pool and randomly sample max_user_likes
        let fetch_limit = if seed_sample_pool > 0 {
            seed_sample_pool.max(max_user_likes)
        } else {
            max_user_likes
        };
        let user_likes_keys = Keys::user_likes_retention(user_hash, DEFAULT_RETENTION_DAYS);
        let mut user_likes = self
            .redis
            .zrevrangebyscore_merged(&user_likes_keys, now, min_time, fetch_limit)
            .await?;

        if user_likes.is_empty() {
            debug!(
                user_hash = %&user_hash[..8.min(user_hash.len())],
                time_window_seconds,
                max_user_likes,
                "early_exit: user has no likes in time window"
            );
            return Ok(HashMap::new());
        }

        // Random seed sampling: shuffle and truncate to max_user_likes.
        //
        // Seeded per (user, day) rather than from `thread_rng`. The shuffle exists to vary *which*
        // of a user's likes seed the walk, which is a between-user and between-day property; drawing
        // it fresh per request bought nothing and cost a great deal. It made the same ranker return
        // different rankings when run twice in one request, which the interleaving self-check
        // measured as a 6% spurious disagreement floor — noise indistinguishable from a treatment
        // effect in every experiment that would ever run through the harness.
        //
        // Determinism here is also nearly free behaviourally: the derived weights are cached with a
        // TTL, so a user's seed sample was already fixed for the life of that cache entry. Rotating
        // daily keeps the variety the shuffle was for.
        if seed_sample_pool > 0 && user_likes.len() > max_user_likes {
            user_likes.shuffle(&mut daily_rng(user_hash));
            user_likes.truncate(max_user_likes);
        }

        self.colikes_from_seed(
            user_hash,
            user_likes,
            max_sources_per_post,
            max_total_sources,
            now,
            min_time,
            recency_half_life_seconds,
            start_time,
        )
        .await
    }

    /// Derive co-liker weights from an already-chosen seed.
    ///
    /// Split out of [`Self::compute_colikes`] so the per-feed variant can substitute a *filtered*
    /// seed without duplicating steps 2 and 3. Both paths therefore share byte-identical
    /// aggregation, which is what makes an interleaved comparison of the two a comparison of the
    /// seed alone.
    #[allow(clippy::too_many_arguments)]
    async fn colikes_from_seed(
        &self,
        user_hash: &str,
        user_likes: Vec<(String, f64)>,
        max_sources_per_post: usize,
        max_total_sources: usize,
        now: f64,
        min_time: f64,
        recency_half_life_seconds: u64,
        start_time: Instant,
    ) -> Result<HashMap<String, f64>> {
        let liked_posts: Vec<(String, f64)> = user_likes;

        // Step 2: Fetch co-likers for ALL posts using pipelined requests
        // Build date-based key groups for each post
        let post_key_groups: Vec<Vec<String>> = liked_posts
            .iter()
            .map(|(post_id, _)| Keys::post_likers_retention(post_id, DEFAULT_RETENTION_DAYS))
            .collect();

        // Fetch from all day-tranche keys and merge results per post
        let all_results = self
            .redis
            .zrevrangebyscore_merged_multi(
                &post_key_groups,
                now, // Use now as max since we filter by user_like_time below
                min_time,
                max_sources_per_post,
            )
            .await?;

        // Filter results to only include likers who liked BEFORE the user
        let all_results: Vec<Vec<(String, f64)>> = all_results
            .into_iter()
            .zip(liked_posts.iter())
            .map(|(likers, (_, user_like_time))| {
                likers
                    .into_iter()
                    .filter(|(_, like_time)| *like_time < *user_like_time)
                    .collect()
            })
            .collect();

        // Filter out empty results
        let all_likers_data: Vec<Vec<(String, f64)>> = all_results
            .into_iter()
            .filter(|likers| !likers.is_empty())
            .collect();

        if all_likers_data.is_empty() {
            debug!(
                user_hash = %&user_hash[..8.min(user_hash.len())],
                posts_checked = liked_posts.len(),
                "early_exit: no co-likers found for any liked posts"
            );
            return Ok(HashMap::new());
        }

        // Step 3: Aggregate co-liker weights
        let sorted_sources = if self.config.linklonk_normalization_enabled {
            // Use normalized LinkLonk formula with all branching factors

            // Collect unique source hashes to fetch their like counts
            let unique_sources: FxHashSet<String> = all_likers_data
                .iter()
                .flatten()
                .filter(|(hash, _)| hash != user_hash)
                .map(|(hash, _)| hash.clone())
                .collect();

            // Fetch source like counts from the ulc hash
            let source_hashes: Vec<&str> = unique_sources.iter().map(|s| s.as_str()).collect();
            let source_like_counts: FxHashMap<String, i64> = if !source_hashes.is_empty() {
                let counts = self
                    .redis
                    .hmget(Keys::USER_LIKE_COUNTS, &source_hashes)
                    .await?;

                source_hashes
                    .into_iter()
                    .zip(counts)
                    .map(|(hash, count)| {
                        let c = count.and_then(|s| s.parse::<i64>().ok()).unwrap_or(1);
                        (hash.to_string(), c)
                    })
                    .collect()
            } else {
                FxHashMap::default()
            };

            debug!(
                sources_with_counts = source_like_counts.len(),
                user_likes_count = liked_posts.len(),
                "linklonk_normalization_enabled"
            );

            aggregate_coliker_weights_normalized_parallel(
                all_likers_data,
                user_hash,
                liked_posts.len(),
                &source_like_counts,
                now,
                recency_half_life_seconds as f64,
                max_total_sources,
                self.config.max_coliker_weight,
            )
        } else {
            // Use original simplified aggregation (recency only)
            aggregate_coliker_weights_parallel(
                all_likers_data,
                user_hash,
                now,
                recency_half_life_seconds as f64,
                max_total_sources,
            )
        };

        if sorted_sources.is_empty() {
            debug!(
                user_hash = %&user_hash[..8.min(user_hash.len())],
                posts_checked = liked_posts.len(),
                "early_exit: no sources after aggregation (all self-likes?)"
            );
            return Ok(HashMap::new());
        }

        // Store in Redis sorted set along with the timestamp for cache invalidation
        let colikes_key = Keys::colikes(user_hash);
        let colikes_ts_key = Keys::colikes_timestamp(user_hash);

        // Get the most recent like timestamp to use for cache invalidation checks
        let most_recent_like_ts = liked_posts
            .iter()
            .map(|(_, ts)| *ts)
            .fold(f64::NEG_INFINITY, f64::max);

        if self.config.read_only_mode {
            // Log skipped write in read-only mode
            tracing::info!(
                target: "graze::read_only",
                operation = "ZADD",
                key = %colikes_key,
                items_count = sorted_sources.len(),
                "write_skipped"
            );
        } else {
            // Store sorted set + timestamp in a single pipelined call (4 RTT → 1)
            let items: Vec<(f64, &str)> = sorted_sources
                .iter()
                .map(|(hash, weight)| (*weight, hash.as_str()))
                .collect();
            self.redis
                .store_sorted_set_with_timestamp(
                    &colikes_key,
                    &items,
                    self.config.coliker_ttl_seconds,
                    &colikes_ts_key,
                    &most_recent_like_ts.to_string(),
                )
                .await?;
        }

        let compute_time = start_time.elapsed();
        debug!(
            user_hash = %&user_hash[..8.min(user_hash.len())],
            sources = sorted_sources.len(),
            posts_checked = liked_posts.len(),
            compute_time_ms = compute_time.as_millis(),
            "coliker_computed"
        );

        Ok(sorted_sources.into_iter().collect())
    }

    /// Get cached co-likers from Redis.
    async fn get_cached_colikes(&self, colikes_key: &str) -> Result<HashMap<String, f64>> {
        let results = self
            .redis
            .zrevrangebyscore_with_scores(colikes_key, f64::INFINITY, f64::NEG_INFINITY, 10000)
            .await?;

        if results.is_empty() {
            return Ok(HashMap::new());
        }

        Ok(results.into_iter().collect())
    }

    /// Invalidate cached co-likers for a user.
    pub async fn invalidate_colikes(&self, user_hash: &str) -> Result<bool> {
        let colikes_key = Keys::colikes(user_hash);
        let colikes_ts_key = Keys::colikes_timestamp(user_hash);
        if self.config.read_only_mode {
            tracing::info!(
                target: "graze::read_only",
                operation = "DEL",
                key = %colikes_key,
                "write_skipped"
            );
            return Ok(true);
        }
        // Delete both keys in a single pipeline (2 RTT → 1)
        self.redis
            .del_multi(&[colikes_key.as_str(), colikes_ts_key.as_str()])
            .await?;
        Ok(true)
    }

    /// Check if co-liker cache exists and its TTL.
    pub async fn check_cache_status(&self, user_hash: &str) -> Result<(bool, i64)> {
        let colikes_key = Keys::colikes(user_hash);
        let ttl = self.redis.ttl(&colikes_key).await?;

        if ttl < 0 {
            return Ok((false, 0));
        }

        Ok((true, ttl))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `max_coliker_weight` default — weights must stay strictly under this or the
    /// scorer's `weight.min(cap)` flattens the profile into ties and ranking degenerates
    /// to bare membership.
    const CAP: f64 = 0.000001;
    const TARGET: f64 = 0.0000002;

    fn entries(scores: &[f64]) -> Vec<(String, f64)> {
        scores
            .iter()
            .enumerate()
            .map(|(i, s)| (format!("{:016x}", i), *s))
            .collect()
    }

    #[test]
    fn top_entry_lands_on_target_and_nothing_reaches_the_cap() {
        let w = profile_weights_from_scores(entries(&[12.5, 3.25, 0.125]), TARGET).unwrap();
        let max = w.values().cloned().fold(0.0_f64, f64::max);
        assert!((max - TARGET).abs() < f64::EPSILON * TARGET.max(1.0));
        for v in w.values() {
            assert!(*v <= TARGET, "{} exceeded target", v);
            assert!(*v < CAP, "{} would hit max_coliker_weight", v);
        }
    }

    #[test]
    fn ranking_is_preserved_exactly() {
        let scores = [50.0, 12.5, 12.4, 3.25, 0.001];
        let w = profile_weights_from_scores(entries(&scores), TARGET).unwrap();
        let mut ordered: Vec<(String, f64)> = w.into_iter().collect();
        ordered.sort_unstable_by(|a, b| b.1.total_cmp(&a.1));
        let got: Vec<usize> = ordered
            .iter()
            .map(|(h, _)| usize::from_str_radix(h, 16).unwrap())
            .collect();
        assert_eq!(got, vec![0, 1, 2, 3, 4], "input was already descending");
    }

    #[test]
    fn small_profile_is_not_advantaged_over_a_large_one() {
        // Normalising by max (not sum) means profile size does not inflate per-entry
        // weight: both profiles' top entry lands on exactly the same value.
        let small = profile_weights_from_scores(entries(&[4.0, 2.0]), TARGET).unwrap();
        let large = profile_weights_from_scores(entries(&[4.0; 128]), TARGET).unwrap();
        let smax = small.values().cloned().fold(0.0_f64, f64::max);
        let lmax = large.values().cloned().fold(0.0_f64, f64::max);
        assert_eq!(smax, lmax);
    }

    #[test]
    fn rejects_profiles_with_no_usable_signal() {
        assert!(profile_weights_from_scores(vec![], TARGET).is_none());
        assert!(profile_weights_from_scores(entries(&[0.0, 0.0]), TARGET).is_none());
        assert!(profile_weights_from_scores(entries(&[-1.0]), TARGET).is_none());
        assert!(profile_weights_from_scores(entries(&[f64::NAN]), TARGET).is_none());
        assert!(profile_weights_from_scores(entries(&[f64::INFINITY]), TARGET).is_none());
    }

    #[test]
    fn drops_individual_bad_entries_but_keeps_the_profile() {
        let mixed = vec![
            ("0000000000000001".to_string(), 5.0),
            ("0000000000000002".to_string(), f64::NAN),
            ("0000000000000003".to_string(), 0.0),
            (String::new(), 9.0), // empty hash
            ("0000000000000004".to_string(), 2.5),
        ];
        let w = profile_weights_from_scores(mixed, TARGET).unwrap();
        assert_eq!(w.len(), 2);
        assert!(w.contains_key("0000000000000001"));
        assert!(w.contains_key("0000000000000004"));
    }

    #[test]
    fn non_positive_target_is_rejected() {
        assert!(profile_weights_from_scores(entries(&[1.0]), 0.0).is_none());
        assert!(profile_weights_from_scores(entries(&[1.0]), -1.0).is_none());
        assert!(profile_weights_from_scores(entries(&[1.0]), f64::NAN).is_none());
    }
}

#[cfg(test)]
mod per_feed_seed_tests {
    use super::*;
    use rand::seq::SliceRandom;

    #[test]
    fn keep_rate_reports_the_share_retained() {
        let stats = SeedFilterStats {
            seen: 128,
            kept: 32,
            unresolved: 0,
        };
        assert!((stats.keep_rate() - 0.25).abs() < 1e-9);
    }

    #[test]
    fn empty_seed_reports_a_full_keep_rate_not_a_division_by_zero() {
        // 0/0 must not become NaN: a NaN would propagate into the telemetry and make the
        // coverage-versus-ranking distinction unreadable exactly when the seed is empty.
        let stats = SeedFilterStats::default();
        assert_eq!(stats.keep_rate(), 1.0);
        assert!(stats.keep_rate().is_finite());
    }

    #[test]
    fn unresolved_seed_posts_count_as_kept() {
        // A post whose URI shard expired cannot be tested for pool membership. Dropping it would
        // shrink the seed for a lookup failure, which is not evidence about the feed's authors.
        let stats = SeedFilterStats {
            seen: 10,
            kept: 4,
            unresolved: 3,
        };
        assert!(stats.kept >= stats.unresolved);
        assert!(stats.keep_rate() > 0.0);
    }

    #[test]
    fn seed_shuffle_is_stable_within_a_day_for_one_user() {
        // This is the property the interleaving self-check needs: the same ranker run twice must
        // choose the same seed. A `thread_rng` shuffle here produced a 6% spurious disagreement
        // floor in production.
        let seed: Vec<u32> = (0..64).collect();
        let mut a = seed.clone();
        let mut b = seed.clone();
        a.shuffle(&mut daily_rng("user-hash-abc"));
        b.shuffle(&mut daily_rng("user-hash-abc"));
        assert_eq!(
            a, b,
            "the same user must get the same seed order within a day"
        );
    }

    #[test]
    fn seed_shuffle_still_differs_between_users() {
        // Determinism must not become "everyone samples identically", which would collapse the
        // between-user seed variety the shuffle exists to provide.
        let seed: Vec<u32> = (0..64).collect();
        let mut a = seed.clone();
        let mut b = seed.clone();
        a.shuffle(&mut daily_rng("user-hash-abc"));
        b.shuffle(&mut daily_rng("user-hash-xyz"));
        assert_ne!(a, b, "different users must get different seed orders");
    }

    #[test]
    fn seed_shuffle_actually_permutes() {
        let seed: Vec<u32> = (0..64).collect();
        let mut shuffled = seed.clone();
        shuffled.shuffle(&mut daily_rng("user-hash-abc"));
        assert_ne!(
            shuffled, seed,
            "a deterministic shuffle must still be a shuffle"
        );
        let mut sorted = shuffled.clone();
        sorted.sort_unstable();
        assert_eq!(sorted, seed, "shuffling must not add or drop entries");
    }
}
