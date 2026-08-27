//! LinkLonk personalization algorithm implementation.
//!
//! This module contains the core algorithm for personalized feed ranking.

mod author_affinity;
mod coliker;
mod diversity;
mod feed_cache;
mod liker_cache;
mod params;
mod pool_cache;
mod proof;
mod scorer;
mod scoring_core;
mod thompson;
mod walk;

pub use author_affinity::AuthorColikerWorker;
pub use coliker::ColikerWorker;
pub use diversity::{diversify_posts, DiversityConfig, DiversityResult};
pub use feed_cache::{FeedCache, FeedCacheStats};
pub use graze_common::models::FeedSuccessConfig;
pub use liker_cache::{CacheStats, LikerCache};
pub use params::{apply_thompson_params, get_preset, merge_params, LinkLonkParams};
pub use pool_cache::{PoolCache, PoolCacheStats};
pub use proof::{compute_proof, LinkLonkProof, ProofCollector};
pub use scorer::{Scorer, ScoringResult};
pub use scoring_core::{
    aggregate_coliker_weights, aggregate_coliker_weights_normalized,
    aggregate_coliker_weights_normalized_parallel, aggregate_coliker_weights_parallel, score_posts,
    score_posts_parallel, score_posts_topk, to_fx_hashmap, ScoredPostResult,
};
pub use thompson::{
    FeedOutcome, FeedOutcomeDetails, HashExperiment, SelectedParams, ThompsonConfig,
    ThompsonLearner,
};

use rustc_hash::FxHashMap;
use std::sync::Arc;
use std::time::Instant;

use tracing::{debug, info, warn};

use crate::audit::AuditCollector;
use crate::config::Config;
use crate::error::Result;
use crate::experiment::{interleave, Ranker};
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
    /// Scorer for the durable-profile arm. Identical to `scorer` except for
    /// `min_post_likes`, which the durable arm relaxes to reach candidates the live arm
    /// discards. Kept as a separate instance so the live arm's configuration is provably
    /// unchanged — there is no per-call flag that could leak across arms.
    profile_scorer: Scorer,
    liker_cache: Arc<LikerCache>,
}

impl LinkLonkAlgorithm {
    /// Create a new LinkLonk algorithm instance.
    pub fn new(redis: Arc<RedisClient>, interner: Arc<UriInterner>, config: Arc<Config>) -> Self {
        let coliker = ColikerWorker::new(redis.clone(), config.clone(), interner.clone());
        let author_coliker = AuthorColikerWorker::new(redis.clone(), config.clone());
        let liker_cache = Arc::new(LikerCache::new(
            config.liker_cache_max_size,
            config.liker_cache_ttl_seconds,
        ));
        // One pool cache, shared by both scorers. Unlike LikerCache -- which is deliberately
        // per-arm because liker lists depend on max_likers_per_post -- a candidate pool is the
        // same Redis set whatever the params, so sharing is correct rather than a leak.
        let pool_cache = Arc::new(pool_cache::PoolCache::new(
            config.pool_cache_ttl_seconds,
            config.pool_cache_max_members,
        ));
        let scorer = Scorer::new(
            redis.clone(),
            liker_cache.clone(),
            config.clone(),
            pool_cache.clone(),
        );

        // The durable arm gets its own scorer with relaxed candidate filters. Cloning the
        // config and overriding just those fields keeps every other knob in lockstep with
        // the live arm, so the two arms stay comparable.
        //
        // It also gets its OWN liker cache. The scorer writes fetched liker lists back into
        // its cache after scoring, so a shared cache would let the durable arm's longer
        // lists (max_likers_per_post 100 vs 30) leak into the live arm and silently change
        // what the live path scores. A separate cache is what makes "strictly additive"
        // actually true.
        let profile_scorer = {
            let mut profile_config = (*config).clone();
            profile_config.inverted_min_post_likes = config.durable_profile_min_post_likes;
            profile_config.inverted_max_likers_per_post =
                config.durable_profile_max_likers_per_post;
            let profile_liker_cache = Arc::new(LikerCache::new(
                config.liker_cache_max_size,
                config.liker_cache_ttl_seconds,
            ));
            Scorer::new(
                redis.clone(),
                profile_liker_cache,
                Arc::new(profile_config),
                pool_cache.clone(),
            )
        };

        Self {
            redis,
            interner,
            config,
            coliker,
            author_coliker,
            scorer,
            profile_scorer,
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
                let mut compute_result = self
                    .compute_personalization(&user_hash, algo_id, &params, audit_ref)
                    .await?;

                // Interleaving: blend a second ranker's list into this one so the user acts as
                // their own control. Runs only for enrolled users, and only on a fresh compute —
                // cached pages replay the draft that was already stored.
                if let Some((control, treatment, control_first)) =
                    self.interleave_assignment(Some(user_did), algo_id)
                {
                    // Derive co-liker weights ONCE and score both arms from them, so the arms differ
                    // only by traversal. Deriving per arm reintroduces the seed-sampling shuffle as
                    // noise (see `score_with_ranker`).
                    let weights = self
                        .coliker
                        .get_or_compute_colikes(
                            &user_hash,
                            params.max_user_likes,
                            params.max_sources_per_post,
                            params.max_total_sources,
                            (params.time_window_hours * 3600.0) as u64,
                            (params.recency_half_life_hours * 3600.0) as u64,
                            false,
                            params.seed_sample_pool,
                        )
                        .await?;

                    if !weights.is_empty() {
                        let control_result = self
                            .score_with_ranker(&user_hash, algo_id, &params, &weights, control)
                            .await?;
                        let treatment_result = self
                            .score_with_ranker(&user_hash, algo_id, &params, &weights, treatment)
                            .await?;
                        compute_result = self.apply_interleaving(
                            control_result,
                            treatment_result,
                            control,
                            treatment,
                            control_first,
                            limit,
                        );
                    }
                }

                // Capture scoring stats before converting
                let stats = Some((
                    compute_result.scored_count,
                    compute_result.posts_checked,
                    compute_result.scoring_time_ms,
                ));

                // Use computed posts directly instead of re-fetching from Redis!
                // Apply cursor and limit to the computed results
                let posts = self
                    .convert_scored_posts_to_response(
                        &compute_result.scored_posts,
                        limit,
                        cursor,
                        &compute_result.ranker_by_post,
                        compute_result.requires_preserved_order,
                    )
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
    /// Resolve this user's interleaving assignment, if any.
    ///
    /// Returns `(treatment_ranker, control_drafts_first)`. `None` means serve normally: the
    /// experiment is off, the user is not enrolled, a ranker name is invalid, or control and
    /// treatment are the same ranker (which would produce zero competitive pairs anyway).
    fn interleave_assignment(
        &self,
        user_did: Option<&str>,
        algo_id: i32,
    ) -> Option<(Ranker, Ranker, bool)> {
        if !self.config.interleave_enabled {
            return None;
        }
        let did = user_did?;

        let control = Ranker::parse(&self.config.interleave_control);
        let treatment = Ranker::parse(&self.config.interleave_treatment);
        let (Some(control), Some(treatment)) = (control, treatment) else {
            // Fail loudly rather than silently comparing the control against itself.
            warn!(
                algo_id,
                control = %self.config.interleave_control,
                treatment = %self.config.interleave_treatment,
                "interleave_invalid_ranker_name"
            );
            return None;
        };
        // Identical rankers normally mean a misconfiguration, but with the self-check flag it is
        // a deliberate, feed-neutral validation of the harness itself.
        if control == treatment && !self.config.interleave_self_check {
            return None;
        }

        // Enrolment reuses the hash-experiment primitive: stable per user, orthogonal to time
        // and to bandit state.
        let exp = HashExperiment {
            dimension: "interleave".to_string(),
            values: vec![1],
            traffic_pct: self.config.interleave_traffic_pct,
            salt: self.config.interleave_salt.clone(),
        };
        exp.assign(did)?;

        Some((
            control,
            treatment,
            interleave::control_drafts_first(did, &self.config.interleave_salt),
        ))
    }

    /// Score with a specific ranker from **already-derived** co-liker weights.
    ///
    /// Sharing the weights between arms is not merely an optimisation. Deriving them per arm made
    /// "the same ranker twice" produce different rankings — the production self-check measured 123
    /// competitive pairs and 32 co-liker derivations across 15 drafts, because
    /// `seed_sample_pool > 0` shuffles the seed with `thread_rng` on every derivation
    /// (`coliker.rs:283`, `scorer.rs:662`). That run-to-run noise is indistinguishable from a real
    /// treatment effect and would consume the sensitivity interleaving exists to buy.
    ///
    /// Sharing also halves the co-liker work, measured at 27-106 ms per derivation.
    async fn score_with_ranker(
        &self,
        user_hash: &str,
        algo_id: i32,
        params: &LinkLonkParams,
        weights: &std::collections::HashMap<String, f64>,
        ranker: Ranker,
    ) -> Result<ScoringResult> {
        // `PerFeedSeeded` is the one ranker so far that differs in the *weights* rather than the
        // traversal, so it derives its own and ignores the shared ones. That is the treatment, not a
        // regression of the shared-weight fix: rankers that differ only by traversal still share, so
        // the self-check's null stays null. Any two rankers in one request are still reproducible,
        // because seed sampling is now deterministic per (user, day).
        if ranker == Ranker::PerFeedSeeded {
            let (per_feed_weights, stats) = self
                .coliker
                .get_or_compute_colikes_per_feed(
                    algo_id,
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
            if per_feed_weights.is_empty() {
                return Ok(ScoringResult::default());
            }
            let mut result = self
                .scorer
                .score(user_hash, algo_id, &per_feed_weights, params, None)
                .await?;
            result.seed_keep_rate = Some(stats.keep_rate());
            return Ok(result);
        }

        // The sampled walk derives its own candidates from the graph, so it neither needs nor uses
        // the shared co-liker weights. It falls back to exhaustive scoring for very small seeds, where
        // sampling variance is worst and enumeration is cheap anyway.
        if ranker == Ranker::SampledWalk {
            let seed_is_large_enough = self
                .scorer
                .seed_size_at_least(user_hash, params, self.scorer.walk_min_seed_for_sampling())
                .await?;
            if seed_is_large_enough {
                return self
                    .scorer
                    .score_sampled_walk(user_hash, algo_id, params)
                    .await;
            }
            if weights.is_empty() {
                return Ok(ScoringResult::default());
            }
            return self
                .scorer
                .score(user_hash, algo_id, weights, params, None)
                .await;
        }

        if weights.is_empty() {
            return Ok(ScoringResult::default());
        }
        match ranker {
            Ranker::Inverted => {
                self.scorer
                    .score_inverted(user_hash, algo_id, weights, params, None)
                    .await
            }
            // Remaining variants land with their stages; until then they fall back to the
            // production traversal, so a misconfigured experiment degrades to "no difference"
            // rather than to an error.
            _ => {
                self.scorer
                    .score(user_hash, algo_id, weights, params, None)
                    .await
            }
        }
    }

    /// Team-draft the control and treatment lists into a single ranking.
    ///
    /// The draft order *is* the ranking, so `DIVERSITY_PRESERVE_ORDER` must be on for the
    /// assignment to survive — a warning fires if it is not, because silently re-sorting would
    /// scramble attribution and make the experiment unreadable rather than merely noisy.
    fn apply_interleaving(
        &self,
        control: ScoringResult,
        treatment: ScoringResult,
        control_ranker: Ranker,
        treatment_ranker: Ranker,
        control_first: bool,
        limit: usize,
    ) -> ScoringResult {
        // Draft wider than `limit` so pagination and the author cap have material to work with.
        let draft_limit = (limit * 4).max(limit);
        let draft = interleave::team_draft(
            &control.scored_posts,
            &treatment.scored_posts,
            control_ranker,
            treatment_ranker,
            control_first,
            draft_limit,
        );

        let mut scored_posts = Vec::with_capacity(draft.items.len());
        let mut ranker_by_post = std::collections::HashMap::new();
        for item in draft.items {
            if let Some(r) = item.ranker {
                ranker_by_post.insert(item.post_id.clone(), r.as_str().to_string());
            }
            scored_posts.push((item.score, item.post_id));
        }

        info!(
            control_scored = control.scored_count,
            treatment_scored = treatment.scored_count,
            treatment_ranker = treatment_ranker.as_str(),
            competitive_pairs = draft.competitive_pairs,
            shared_items = draft.shared_items,
            control_first = draft.control_first,
            drafted = scored_posts.len(),
            "interleave_draft"
        );

        ScoringResult {
            scored_count: scored_posts.len(),
            scored_posts,
            ranker_by_post,
            // The draft order IS the ranking; downstream must not re-sort it.
            requires_preserved_order: true,
            // Keep the control arm's funnel counters so existing dashboards stay comparable.
            posts_checked: control.posts_checked,
            posts_skipped_no_likers: control.posts_skipped_no_likers,
            posts_skipped_few_likers: control.posts_skipped_few_likers,
            posts_skipped_low_overlap: control.posts_skipped_low_overlap,
            scoring_time_ms: control.scoring_time_ms + treatment.scoring_time_ms,
            cache_hits: control.cache_hits,
            cache_misses: control.cache_misses,
            overlap_sum: control.overlap_sum,
            overlap_max: control.overlap_max,
            overlap_hist: control.overlap_hist,
            // Whichever arm faceted the seed reported its keep rate; carry it so an interleaved
            // readout can still tell a ranking effect from a coverage collapse.
            seed_keep_rate: treatment.seed_keep_rate.or(control.seed_keep_rate),
        }
    }

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

        // Step 0: Pool-size gate. Bail out before doing ANY expensive work if the
        // feed's candidate pool is too small to personalize from. Overlap between a
        // user's co-liker set and the pool is linear in pool size, so below a few
        // hundred candidates the expected number of scoreable posts is < 1 and the
        // whole pipeline reliably returns nothing. SCARD is O(1), whereas the work
        // it skips is the co-liker walk, author-affinity supplementation, a full
        // SMEMBERS of the pool and an HMGET of every liker count.
        //
        // Disabled by default (0). See Config::min_candidate_pool_for_personalization.
        let pool_gate = self.config.min_candidate_pool_for_personalization;
        if pool_gate > 0 {
            let pool_size = self
                .redis
                .scard(&Keys::algo_posts(algo_id))
                .await
                .unwrap_or(0);
            if pool_size < pool_gate {
                info!(
                    user_hash = %&user_hash[..8.min(user_hash.len())],
                    algo_id,
                    pool_size,
                    pool_gate,
                    "early_exit: candidate pool below personalization gate"
                );
                return Ok(ScoringResult {
                    posts_checked: pool_size,
                    ..Default::default()
                });
            }
        }

        // Step 0b: Like-density gate. A feed whose candidates are mostly unliked cannot be
        // personalized by any co-liker method — there is no like signal to rank — so the walk is pure
        // waste there. Distinct from the pool-size gate above because size does not predict density:
        // algo 8352 (596 posts) is 22% scoreable while algo 5395 (1,000 posts) is 2%.
        //
        // Reads the histogram candidate-sync publishes into `am:{algo}`, so this costs one HGET against
        // counts that were already computed during the sync. Disabled by default (0.0).
        //
        // Fails OPEN: a missing or unparseable field means "sync has not published this yet", and
        // treating absence as "not viable" would silently disable personalization for every feed the
        // moment this deploys ahead of the candidate-sync change.
        let density_gate = self.config.min_pool_scoreable_share;
        if density_gate > 0.0 {
            let field = format!("scoreable_{}", self.config.inverted_min_post_likes);
            let meta_key = Keys::algo_meta(algo_id);
            let scoreable = self
                .redis
                .hget(&meta_key, &field)
                .await
                .ok()
                .flatten()
                .and_then(|v| v.parse::<f64>().ok());
            let post_count = self
                .redis
                .hget(&meta_key, "post_count")
                .await
                .ok()
                .flatten()
                .and_then(|v| v.parse::<f64>().ok());
            if let (Some(scoreable), Some(post_count)) = (scoreable, post_count) {
                if post_count > 0.0 {
                    let share = scoreable / post_count;
                    if share < density_gate {
                        info!(
                            user_hash = %&user_hash[..8.min(user_hash.len())],
                            algo_id,
                            scoreable,
                            post_count,
                            share = format!("{:.4}", share),
                            density_gate,
                            "early_exit: pool like-density below personalization gate"
                        );
                        return Ok(ScoringResult::default());
                    }
                }
            }
        }

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
            // This is the branch the durable-profile work targets: the user has no likes in
            // the 6-day window, so the live path derives no co-likers and they get 100%
            // fallback. Measured: 13,196 such users per 3 days against 21,597 who are
            // personalizable at all. See DESIGN-durable-coliker-profiles.md.
            //
            // Attaching here makes the change strictly additive — anyone the live path can
            // already serve never reaches this code, so there is no regression surface.
            if self.config.durable_profile_shadow_mode || self.config.durable_profile_enabled {
                if let Some(result) = self
                    .try_durable_profile(user_hash, algo_id, params, audit)
                    .await?
                {
                    return Ok(result);
                }
            }

            debug!(
                user_hash = %&user_hash[..8.min(user_hash.len())],
                algo_id,
                use_author_affinity = params.use_author_affinity,
                "early_exit: no coliker weights found"
            );
            return Ok(ScoringResult::default());
        }

        // Step 2: Score posts using co-liker weights
        let scoring_result = self
            .score_with_lookup_comparison(user_hash, algo_id, &coliker_weights, params, audit)
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

    /// Run the scorer, optionally computing the co-liker-first arm alongside the post-first one
    /// and logging a per-request A/B.
    ///
    /// Three modes, from the two flags:
    /// - neither set: post-first only, byte-identical to before this method existed.
    /// - `INVERTED_LOOKUP_SHADOW_MODE=1`: both arms computed, comparison logged as
    ///   `lookup_arm_comparison`, **post-first served**. Doubles scoring latency for affected
    ///   requests, which is why it is opt-in and meant to be run briefly.
    /// - `INVERTED_LOOKUP_ENABLED=1`: inverted served. If shadow is also on, post-first is still
    ///   computed for comparison; otherwise only the inverted arm runs and the request gets the
    ///   full cost saving.
    ///
    /// The audit collector goes to whichever arm is actually served, so audit output always
    /// describes the response the user received.
    async fn score_with_lookup_comparison(
        &self,
        user_hash: &str,
        algo_id: i32,
        coliker_weights: &std::collections::HashMap<String, f64>,
        params: &LinkLonkParams,
        audit: Option<&mut AuditCollector>,
    ) -> Result<ScoringResult> {
        let shadow = self.config.inverted_lookup_shadow_mode;
        let serve_inverted = self.config.inverted_lookup_enabled;

        if !shadow && !serve_inverted {
            return self
                .scorer
                .score(user_hash, algo_id, coliker_weights, params, audit)
                .await;
        }

        let t_post = Instant::now();
        let (served, other, other_label) = if serve_inverted {
            let inv = self
                .scorer
                .score_inverted(user_hash, algo_id, coliker_weights, params, audit)
                .await?;
            let post = if shadow {
                Some(
                    self.scorer
                        .score(user_hash, algo_id, coliker_weights, params, None)
                        .await?,
                )
            } else {
                None
            };
            (inv, post, "post_first")
        } else {
            let post = self
                .scorer
                .score(user_hash, algo_id, coliker_weights, params, audit)
                .await?;
            let inv = Some(
                self.scorer
                    .score_inverted(user_hash, algo_id, coliker_weights, params, None)
                    .await?,
            );
            (post, inv, "inverted")
        };

        if let Some(ref o) = other {
            let (post_arm, inv_arm) = if serve_inverted {
                (o, &served)
            } else {
                (&served, o)
            };
            info!(
                user_hash = %&user_hash[..8.min(user_hash.len())],
                algo_id,
                colikers = coliker_weights.len(),
                served_arm = if serve_inverted { "inverted" } else { "post_first" },
                other_arm = other_label,
                // the headline comparison
                post_scored = post_arm.scored_count,
                inv_scored = inv_arm.scored_count,
                post_ms = format!("{:.1}", post_arm.scoring_time_ms),
                inv_ms = format!("{:.1}", inv_arm.scoring_time_ms),
                // where each arm loses candidates
                post_checked = post_arm.posts_checked,
                inv_checked = inv_arm.posts_checked,
                post_no_likers = post_arm.posts_skipped_no_likers,
                inv_unreached = inv_arm.posts_skipped_no_likers,
                post_few_likers = post_arm.posts_skipped_few_likers,
                inv_few_likers = inv_arm.posts_skipped_few_likers,
                post_low_overlap = post_arm.posts_skipped_low_overlap,
                inv_low_overlap = inv_arm.posts_skipped_low_overlap,
                // ranking shape: overlap_count feeds a nonlinear paths_boost
                post_overlap_mean = format!("{:.2}", post_arm.overlap_mean()),
                inv_overlap_mean = format!("{:.2}", inv_arm.overlap_mean()),
                post_overlap_max = post_arm.overlap_max,
                inv_overlap_max = inv_arm.overlap_max,
                post_overlap_hist = %post_arm.overlap_hist_str(),
                inv_overlap_hist = %inv_arm.overlap_hist_str(),
                total_ms = t_post.elapsed().as_millis() as u64,
                "lookup_arm_comparison"
            );
        }

        Ok(served)
    }

    /// Score against the durable co-liker profile (`ucl:`) for a user the live path could
    /// not serve.
    ///
    /// Returns:
    /// - `Ok(None)` when there is no profile, **or** when only shadow mode is on. In shadow
    ///   mode the arm is fully computed and logged, then discarded, so the caller falls
    ///   through to its normal early-exit and the response is byte-identical to today.
    /// - `Ok(Some(result))` only when `durable_profile_enabled` is set (Phase C).
    ///
    /// The emitted `durable_profile_shadow` line is the Phase B deliverable: it carries the
    /// `overlap_count` mean/max/histogram, which is what determines whether a ~128-member
    /// profile distorts `paths_boost = overlap_count^num_paths_power` relative to the live
    /// arm, versus merely rescaling it.
    async fn try_durable_profile(
        &self,
        user_hash: &str,
        algo_id: i32,
        params: &LinkLonkParams,
        audit: Option<&mut AuditCollector>,
    ) -> Result<Option<ScoringResult>> {
        let shadow_start = Instant::now();

        let Some(profile) = self.coliker.get_durable_profile(user_hash).await? else {
            return Ok(None);
        };
        let profile_size = profile.len();

        // Shadow runs must not contribute to an audit trail for a response they do not
        // affect; only the serving path gets the collector.
        let serving = self.config.durable_profile_enabled;
        let result = self
            .profile_scorer
            .score(
                user_hash,
                algo_id,
                &profile,
                params,
                if serving { audit } else { None },
            )
            .await?;

        info!(
            user_hash = %&user_hash[..8.min(user_hash.len())],
            algo_id,
            serving,
            profile_size,
            min_post_likes = self.config.durable_profile_min_post_likes,
            max_likers_per_post = self.config.durable_profile_max_likers_per_post,
            scored_count = result.scored_count,
            posts_checked = result.posts_checked,
            posts_skipped_no_likers = result.posts_skipped_no_likers,
            posts_skipped_few_likers = result.posts_skipped_few_likers,
            posts_skipped_low_overlap = result.posts_skipped_low_overlap,
            overlap_mean = format!("{:.2}", result.overlap_mean()),
            overlap_max = result.overlap_max,
            overlap_observed = result.overlap_observed(),
            overlap_hist = %result.overlap_hist_str(),
            scoring_time_ms = format!("{:.2}", result.scoring_time_ms),
            total_time_ms = shadow_start.elapsed().as_millis() as u64,
            "durable_profile_shadow"
        );

        if serving {
            Ok(Some(result))
        } else {
            Ok(None)
        }
    }

    /// Get cached posts from Redis with diversity re-ranking.
    async fn get_cached_posts(
        &self,
        result_key: &str,
        limit: usize,
        cursor: Option<&str>,
    ) -> Result<Vec<ScoredPost>> {
        // The Redis result cache stores only (score, post_id); interleaving attribution for
        // cached pages comes from the `fsc:` tag instead, not from here.
        let ranker_by_post: std::collections::HashMap<String, String> = Default::default();
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
        let ids_to_lookup: Vec<String> = cursor_filtered
            .iter()
            .map(|(_, post_id)| post_id.clone())
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
                let uri = id_to_uri.get(post_id)?.clone();
                Some((post_id.clone(), uri))
            })
            .collect();

        // Apply diversity re-ranking
        let diversity_config = DiversityConfig {
            enabled: self.config.diversity_enabled,
            max_posts_per_author: self.config.max_posts_per_author,
            diminishing_factor: self.config.author_diminishing_factor,
            mmr_lambda: self.config.diversity_mmr_lambda,
            // Interleaving sets this per request; default ordering behaviour otherwise.
            preserve_order: self.config.diversity_preserve_order,
        };

        let diversity_result =
            diversify_posts(&cursor_filtered, &post_uris, &diversity_config, limit);

        // Convert to ScoredPost response
        let posts: Vec<ScoredPost> = diversity_result
            .posts
            .into_iter()
            .map(|(score, post_id, uri)| ScoredPost {
                // Carry the interleaving attribution alongside the post so the response layer
                // can credit engagement to the right ranker.
                ranker: ranker_by_post.get(&post_id).cloned(),
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
        ranker_by_post: &std::collections::HashMap<String, String>,
        requires_preserved_order: bool,
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
        let ids_to_lookup: Vec<String> = cursor_filtered
            .iter()
            .map(|(_, post_id)| post_id.clone())
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
                let uri = id_to_uri.get(post_id)?.clone();
                Some((post_id.clone(), uri))
            })
            .collect();

        // Apply diversity re-ranking if enabled
        let diversity_config = DiversityConfig {
            enabled: self.config.diversity_enabled,
            max_posts_per_author: self.config.max_posts_per_author,
            diminishing_factor: self.config.author_diminishing_factor,
            mmr_lambda: self.config.diversity_mmr_lambda,
            // Interleaving sets this per request; default ordering behaviour otherwise.
            // Enforced, not just configured: an interleaved draft is meaningless
            // if diversity re-sorts it, so forgetting the env var cannot
            // silently invalidate an experiment.
            preserve_order: self.config.diversity_preserve_order || requires_preserved_order,
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
                // Carry the interleaving attribution alongside the post so the response layer
                // can credit engagement to the right ranker.
                ranker: ranker_by_post.get(&post_id).cloned(),
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
