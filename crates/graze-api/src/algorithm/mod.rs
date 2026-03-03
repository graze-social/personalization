//! LinkLonk personalization algorithm implementation.
//!
//! This module contains the core algorithm for personalized feed ranking.

mod author_affinity;
mod coliker;
mod diversity;
mod feed_cache;
pub mod features;
mod liker_cache;
mod params;
mod proof;
mod scorer;
mod scoring_core;
mod thompson;

pub use author_affinity::AuthorColikerWorker;
pub use coliker::ColikerWorker;
pub use diversity::{diversify_posts, DiversityConfig, DiversityResult};
pub use feed_cache::{FeedCache, FeedCacheStats};
pub use features::{NetworkStats, PostFeatures, FEATURE_COUNT, FEATURE_NAMES};
pub use graze_common::models::FeedSuccessConfig;
pub use liker_cache::{CacheStats, LikerCache};
pub use params::{apply_thompson_params, get_preset, merge_params, LinkLonkParams};
pub use proof::{compute_proof, LinkLonkProof, ProofCollector};
pub use scorer::{Scorer, ScoringResult};
pub use scoring_core::{
    aggregate_coliker_weights, aggregate_coliker_weights_normalized,
    aggregate_coliker_weights_normalized_parallel, aggregate_coliker_weights_parallel, score_posts,
    score_posts_parallel, score_posts_topk, to_fx_hashmap, ScoredPostResult,
};
pub use thompson::{
    FeedOutcome, FeedOutcomeDetails, SelectedParams, ThompsonConfig, ThompsonLearner,
};

use rustc_hash::FxHashMap;
use std::sync::Arc;
use std::time::Instant;

use tracing::{debug, info};

use crate::audit::AuditCollector;
use crate::config::Config;
use crate::error::Result;
use graze_common::models::{PersonalizationParams, PersonalizeResponse, ResponseMeta, ScoredPost};
use graze_common::services::UriInterner;
use graze_common::{Keys, RedisClient};

/// Main personalization algorithm orchestrator.
pub struct LinkLonkAlgorithm {
    redis: Arc<RedisClient>,
    interner: Arc<UriInterner>,
    config: Arc<Config>,
    coliker: ColikerWorker,
    author_coliker: AuthorColikerWorker,
    scorer: Scorer,
    liker_cache: Arc<LikerCache>,
}

impl LinkLonkAlgorithm {
    /// Create a new LinkLonk algorithm instance.
    pub fn new(redis: Arc<RedisClient>, interner: Arc<UriInterner>, config: Arc<Config>) -> Self {
        let coliker = ColikerWorker::new(redis.clone(), config.clone());
        let author_coliker = AuthorColikerWorker::new(redis.clone(), config.clone());
        let liker_cache = Arc::new(LikerCache::new(
            config.liker_cache_max_size,
            config.liker_cache_ttl_seconds,
        ));
        let scorer = Scorer::new(redis.clone(), liker_cache.clone(), config.clone());

        Self {
            redis,
            interner,
            config,
            coliker,
            author_coliker,
            scorer,
            liker_cache,
        }
    }

    /// Personalize feed for a user.
    pub async fn personalize(
        &self,
        user_did: &str,
        algo_id: i32,
        limit: usize,
        cursor: Option<&str>,
        params_override: Option<&PersonalizationParams>,
        preset: Option<&str>,
    ) -> Result<PersonalizeResponse> {
        self.personalize_with_audit(
            user_did,
            algo_id,
            limit,
            cursor,
            params_override,
            preset,
            None,
        )
        .await
    }

    /// Personalize feed for a user with optional audit collection.
    #[allow(clippy::too_many_arguments, clippy::option_as_ref_deref)]
    pub async fn personalize_with_audit(
        &self,
        user_did: &str,
        algo_id: i32,
        limit: usize,
        cursor: Option<&str>,
        params_override: Option<&PersonalizationParams>,
        preset: Option<&str>,
        mut audit: Option<&mut AuditCollector>,
    ) -> Result<PersonalizeResponse> {
        let start_time = Instant::now();

        // Get user hash
        let user_hash = graze_common::hash_did(user_did);

        // Merge parameters from preset and override
        let base_params = get_preset(preset.unwrap_or("default"));
        let params = merge_params(base_params, params_override);

        // Set audit params if enabled
        if let Some(ref mut a) = audit {
            a.set_params(&params);
        }

        // Check for cached results
        let result_key = Keys::cached_result(algo_id, &user_hash);
        let ttl = self.redis.ttl(&result_key).await?;

        let (posts, cached, cache_age_seconds, scoring_stats) =
            if ttl > self.config.stale_refresh_threshold_seconds as i64 {
                // Fresh cache exists - use it
                let posts = self.get_cached_posts(&result_key, limit, cursor).await?;
                (
                    posts,
                    true,
                    Some((params.result_ttl_seconds as i64 - ttl) as u32),
                    None, // No scoring stats for cached results
                )
            } else {
                // Need to compute fresh results
                let audit_ref = audit.as_mut().map(|a| &mut **a);
                let compute_result = self
                    .compute_personalization(&user_hash, algo_id, &params, audit_ref)
                    .await?;

                // Capture scoring stats before converting
                let stats = Some((
                    compute_result.scored_count,
                    compute_result.posts_checked,
                    compute_result.scoring_time_ms,
                ));

                // Use computed posts directly instead of re-fetching from Redis!
                // Apply cursor and limit to the computed results
                let posts = self
                    .convert_scored_posts_to_response(&compute_result.scored_posts, limit, cursor)
                    .await?;
                (posts, false, Some(0), stats)
            };

        let compute_time_ms = start_time.elapsed().as_secs_f64() * 1000.0;

        // Build cursor for next page
        let next_cursor = if posts.len() >= limit {
            posts.last().map(|p| format!("{}:{}", p.score, p.post_id))
        } else {
            None
        };

        // Extract scoring stats if available
        let (total_scored, posts_checked, scoring_time_ms) = scoring_stats
            .map(|(s, p, t)| (Some(s), Some(p), Some(t)))
            .unwrap_or((None, None, None));

        Ok(PersonalizeResponse {
            posts,
            cursor: next_cursor,
            meta: ResponseMeta {
                cached,
                cache_age_seconds,
                total_scored,
                compute_time_ms: Some(compute_time_ms),
                syncing: false,
                retry_after_ms: None,
                read_only: self.config.read_only_mode,
                posts_checked,
                colikers_used: None, // Tracked at co-liker level
                scoring_time_ms,
            },
        })
    }

    /// Compute personalization for a user.
    ///
    /// This can use either post-level co-likers (standard LinkLonk) or
    /// author-level co-likers (coarse LinkLonk) based on `params.use_author_affinity`.
    #[allow(clippy::needless_option_as_deref)]
    pub async fn compute_personalization(
        &self,
        user_hash: &str,
        algo_id: i32,
        params: &LinkLonkParams,
        audit: Option<&mut AuditCollector>,
    ) -> Result<ScoringResult> {
        let compute_start = Instant::now();
        info!(
            user_hash = %&user_hash[..8.min(user_hash.len())],
            algo_id,
            use_author_affinity = params.use_author_affinity,
            max_user_likes = params.max_user_likes,
            min_co_likes = params.min_co_likes,
            "compute_personalization_start"
        );

        // Step 1: Get or compute co-likers (post-level or author-level)
        let coliker_weights = if params.use_author_affinity {
            debug!("step1_author_coliker_start");
            let weights = self
                .author_coliker
                .get_or_compute_author_colikes(user_hash, false)
                .await?;
            debug!(
                coliker_count = weights.len(),
                "step1_author_coliker_complete"
            );
            weights
        } else {
            debug!("step1_coliker_start");
            let weights = self
                .coliker
                .get_or_compute_colikes(
                    user_hash,
                    params.max_user_likes,
                    params.max_sources_per_post,
                    params.max_total_sources,
                    (params.time_window_hours * 3600.0) as u64,
                    (params.recency_half_life_hours * 3600.0) as u64,
                    false,
                    params.seed_sample_pool,
                )
                .await?;
            debug!(coliker_count = weights.len(), "step1_coliker_complete");
            weights
        };

        if coliker_weights.is_empty() {
            debug!(
                user_hash = %&user_hash[..8.min(user_hash.len())],
                algo_id,
                use_author_affinity = params.use_author_affinity,
                "early_exit: no coliker weights found"
            );
            return Ok(ScoringResult {
                scored_posts: Vec::new(),
                post_features: Vec::new(),
                scored_count: 0,
                posts_checked: 0,
                posts_skipped_no_likers: 0,
                posts_skipped_few_likers: 0,
                scoring_time_ms: 0.0,
                cache_hits: 0,
                cache_misses: 0,
            });
        }

        // Step 2: Optionally compute NetworkStats for ML feature collection
        let network_stats = if self.config.ml_impressions_enabled || self.config.ml_reranker_enabled
        {
            Some(NetworkStats::from_source_weights(&coliker_weights))
        } else {
            None
        };

        // Step 3: Score posts using co-liker weights
        let scoring_result = self
            .scorer
            .score(user_hash, algo_id, &coliker_weights, params, network_stats, audit)
            .await?;

        debug!(
            user_hash = %&user_hash[..8.min(user_hash.len())],
            algo_id,
            scored_count = scoring_result.scored_count,
            posts_checked = scoring_result.posts_checked,
            scoring_time_ms = scoring_result.scoring_time_ms,
            total_time_ms = compute_start.elapsed().as_millis() as u64,
            "compute_personalization_complete"
        );

        Ok(scoring_result)
    }

    /// Get cached posts from Redis with diversity re-ranking.
    async fn get_cached_posts(
        &self,
        result_key: &str,
        limit: usize,
        cursor: Option<&str>,
    ) -> Result<Vec<ScoredPost>> {
        // Parse cursor to get max score
        let (max_score, offset) = Self::parse_cursor(cursor);

        // Get more posts than needed to allow for diversity filtering
        // Fetch extra to account for posts removed by author cap
        let fetch_limit = limit * (self.config.max_posts_per_author + 1);

        let results = self
            .redis
            .zrevrangebyscore_with_scores(result_key, max_score, 0.0, fetch_limit + offset)
            .await?;

        // Skip offset
        let cursor_filtered: Vec<(f64, String)> = results
            .into_iter()
            .skip(offset)
            .map(|(id, score)| (score, id))
            .collect();

        if cursor_filtered.is_empty() {
            return Ok(Vec::new());
        }

        // Get URIs for all candidate posts
        let ids_to_lookup: Vec<i64> = cursor_filtered
            .iter()
            .filter_map(|(_, post_id)| post_id.parse::<i64>().ok())
            .collect();

        let id_to_uri = self
            .interner
            .get_uris_batch(&ids_to_lookup)
            .await
            .unwrap_or_default();

        // Build post_id -> uri map
        let post_uris: FxHashMap<String, String> = cursor_filtered
            .iter()
            .filter_map(|(_, post_id)| {
                let id = post_id.parse::<i64>().ok()?;
                let uri = id_to_uri.get(&id)?.clone();
                Some((post_id.clone(), uri))
            })
            .collect();

        // Apply diversity re-ranking
        let diversity_config = DiversityConfig {
            enabled: self.config.diversity_enabled,
            max_posts_per_author: self.config.max_posts_per_author,
            diminishing_factor: self.config.author_diminishing_factor,
            mmr_lambda: self.config.diversity_mmr_lambda,
        };

        let diversity_result =
            diversify_posts(&cursor_filtered, &post_uris, &diversity_config, limit);

        // Convert to ScoredPost response
        let posts: Vec<ScoredPost> = diversity_result
            .posts
            .into_iter()
            .map(|(score, post_id, uri)| ScoredPost {
                uri,
                post_id,
                score,
                reasons: Vec::new(),
            })
            .collect();

        Ok(posts)
    }

    /// Convert computed scored posts directly to response format (no re-fetch needed).
    /// Applies author diversity re-ranking if enabled.
    async fn convert_scored_posts_to_response(
        &self,
        scored_posts: &[(f64, String)],
        limit: usize,
        cursor: Option<&str>,
    ) -> Result<Vec<ScoredPost>> {
        let (max_score, offset) = Self::parse_cursor(cursor);

        // Filter by cursor first
        let cursor_filtered: Vec<(f64, String)> = scored_posts
            .iter()
            .filter(|(score, _)| *score < max_score || (cursor.is_none() && *score <= max_score))
            .skip(offset)
            .cloned()
            .collect();

        if cursor_filtered.is_empty() {
            return Ok(Vec::new());
        }

        // Get URIs for all candidate posts (needed for diversity author extraction)
        let ids_to_lookup: Vec<i64> = cursor_filtered
            .iter()
            .filter_map(|(_, post_id)| post_id.parse::<i64>().ok())
            .collect();

        let id_to_uri = self
            .interner
            .get_uris_batch(&ids_to_lookup)
            .await
            .unwrap_or_default();

        // Build post_id -> uri map
        let post_uris: FxHashMap<String, String> = cursor_filtered
            .iter()
            .filter_map(|(_, post_id)| {
                let id = post_id.parse::<i64>().ok()?;
                let uri = id_to_uri.get(&id)?.clone();
                Some((post_id.clone(), uri))
            })
            .collect();

        // Apply diversity re-ranking if enabled
        let diversity_config = DiversityConfig {
            enabled: self.config.diversity_enabled,
            max_posts_per_author: self.config.max_posts_per_author,
            diminishing_factor: self.config.author_diminishing_factor,
            mmr_lambda: self.config.diversity_mmr_lambda,
        };

        let diversity_result =
            diversify_posts(&cursor_filtered, &post_uris, &diversity_config, limit);

        debug!(
            unique_authors = diversity_result.unique_authors,
            posts_demoted = diversity_result.posts_demoted,
            posts_removed_by_cap = diversity_result.posts_removed_by_cap,
            "diversity_result"
        );

        // Convert to ScoredPost response
        let posts: Vec<ScoredPost> = diversity_result
            .posts
            .into_iter()
            .map(|(score, post_id, uri)| ScoredPost {
                uri,
                post_id,
                score,
                reasons: Vec::new(),
            })
            .collect();

        Ok(posts)
    }

    /// Parse cursor string into (max_score, offset).
    fn parse_cursor(cursor: Option<&str>) -> (f64, usize) {
        if let Some(c) = cursor {
            if let Some((score_str, _)) = c.split_once(':') {
                if let Ok(score) = score_str.parse::<f64>() {
                    return (score, 1); // Skip the post at cursor position
                }
            }
        }
        (f64::INFINITY, 0)
    }

    /// Invalidate cached results for a user.
    pub async fn invalidate_user(&self, user_did: &str, algo_id: i32) -> Result<()> {
        let user_hash = graze_common::hash_did(user_did);
        let result_key = Keys::cached_result(algo_id, &user_hash);
        self.redis.del(&result_key).await?;

        // Also invalidate co-likers
        self.coliker.invalidate_colikes(&user_hash).await?;

        info!(user_did = %user_did, algo_id, "user_cache_invalidated");
        Ok(())
    }

    /// Get liker cache statistics.
    pub fn get_cache_stats(&self) -> CacheStats {
        self.liker_cache.get_stats()
    }
}
