//! ATProto feed skeleton endpoints.
//!
//! Implements the Bluesky feed generator protocol endpoints.

use std::collections::HashSet;
use std::sync::Arc;

use axum::{
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Extension, Json,
};
use base64::Engine;
use chrono::{Datelike, Timelike, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tracing::{debug, error, info, warn};

use crate::algorithm::{
    get_preset, merge_params, FeedOutcome, FeedSuccessConfig, PostFeatures, ScoringResult,
    SelectedParams,
};
use crate::api::cursor::FeedCursor;
use crate::api::fallback::{
    get_blended_posts_with_stats, get_fallback_blend, load_user_liked_uris, BlendedSource,
};
use crate::api::special_posts::{count_injected, inject_special_posts, ItemProvenance};
use crate::api::RequestId;
use crate::audit::{emit_skip_log, should_audit, AuditCollector};
use crate::AppState;
use graze_common::models::{
    FeedContextProvenance, FeedImpressionRow, FeedSkeletonResponse, FeedThompsonConfig,
    PersonalizationParams, ProvenanceParams, SkeletonFeedPost, ThompsonSearchSpace,
};
use graze_common::services::special_posts::SpecialPostsResponse;
use graze_common::{hash_did, Keys};

/// Placeholder posts for edge cases when feed cannot be served
const PLACEHOLDER_ERROR: &str =
    "at://did:plc:i6y3jdklpvkjvynvsrnqfdoq/app.bsky.feed.post/3ljpll7sa3s27";
const PLACEHOLDER_EMPTY: &str =
    "at://did:plc:i6y3jdklpvkjvynvsrnqfdoq/app.bsky.feed.post/3mdv46xe7ms2i";
const PLACEHOLDER_NO_AUTH: &str =
    "at://did:plc:i6y3jdklpvkjvynvsrnqfdoq/app.bsky.feed.post/3mdv4amtp7c2i";

/// Record feed access for rolling sync scheduling (fire-and-forget).
///
/// Updates the feed:access HSET with the current timestamp.
/// This allows the candidate sync worker to track which feeds are actively used.
async fn record_feed_access(redis: &graze_common::RedisClient, algo_id: i32) {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);

    if let Err(e) = redis
        .hset(Keys::FEED_ACCESS, &algo_id.to_string(), &now.to_string())
        .await
    {
        warn!(algo_id, error = %e, "failed_to_record_feed_access");
    }
}

/// Feed skeleton query parameters.
#[derive(Debug, Deserialize)]
pub struct FeedSkeletonQuery {
    /// AT-URI of the feed generator record.
    pub feed: String,
    /// Number of posts to return.
    #[serde(default = "default_limit")]
    pub limit: usize,
    /// Pagination cursor.
    pub cursor: Option<String>,
}

fn default_limit() -> usize {
    30
}

/// Payload for post-render log queue (log_tasks). Same shape as other feed servers for shared consumers.
#[derive(Debug, serde::Serialize)]
struct PostRenderLogTask {
    feed_uri: String,
    authorization_header: Option<String>,
    cursor: Option<String>,
    limit: usize,
    post_ids: Vec<String>,
    attributions: Vec<Option<String>>,
    contexts: Vec<String>,
    created_at: String,
    updated_at: String,
    uuid: String,
}

/// Thompson metadata for encoding into feedContext (interaction-based learning).
struct ResponseThompsonMeta {
    params: SelectedParams,
    response_time_ms: f64,
    is_holdout: bool,
}

/// Build feedContext provenance for one item and encode to base64 string.
/// feed_uri must always be provided—the client echoes this back with interactions
/// and we rely on it for ClickHouse storage.
#[allow(clippy::too_many_arguments)]
fn encode_feed_context(
    feed_uri: &str,
    algo_id: i32,
    depth: usize,
    total: usize,
    personalized_count: usize,
    prov: &ItemProvenance,
    thompson_meta: Option<&ResponseThompsonMeta>,
    is_personalization_holdout: Option<bool>,
    impression_id: Option<String>,
) -> Option<String> {
    let (source, personalization_type, fallback_tranche, attribution, personalized) = match prov {
        ItemProvenance::Base(BlendedSource::PostLevelPersonalization) => (
            "personalized".to_string(),
            Some("post_level".to_string()),
            None,
            None,
            true,
        ),
        ItemProvenance::Base(BlendedSource::AuthorAffinity) => (
            "author_affinity".to_string(),
            Some("author_level".to_string()),
            None,
            None,
            true,
        ),
        ItemProvenance::Base(BlendedSource::Fallback { tranche }) => (
            "fallback".to_string(),
            None,
            Some(tranche.clone()),
            None,
            false,
        ),
        ItemProvenance::Pinned { attribution: a } => {
            ("pinned".to_string(), None, None, Some(a.clone()), false)
        }
        ItemProvenance::Rotating { attribution: a } => {
            ("rotating".to_string(), None, None, Some(a.clone()), false)
        }
        ItemProvenance::Sponsored { attribution: a } => {
            ("sponsored".to_string(), None, None, Some(a.clone()), false)
        }
    };
    let (params, response_time_ms, is_holdout) = match thompson_meta {
        Some(meta) => (
            Some(ProvenanceParams::from_selected(
                meta.params.min_post_likes,
                meta.params.max_likers_per_post,
                meta.params.max_total_sources,
                meta.params.max_algo_checks,
                meta.params.min_co_likes,
                meta.params.max_user_likes,
                meta.params.max_sources_per_post,
                meta.params.seed_sample_pool,
                meta.params.corater_decay_pct,
            )),
            Some(meta.response_time_ms),
            Some(meta.is_holdout),
        ),
        None => (None, None, None),
    };

    let ctx = FeedContextProvenance {
        feed_uri: feed_uri.to_string(),
        algo_id,
        depth,
        personalized,
        source,
        personalization_type,
        fallback_tranche,
        total,
        personalized_count,
        attribution,
        params,
        response_time_ms,
        is_holdout,
        is_personalization_holdout,
        impression_id,
    };
    ctx.encode()
}

/// Describe feed generator feed.
#[derive(Debug, Serialize)]
pub struct DescribeFeedGeneratorFeed {
    pub uri: String,
}

/// Describe feed generator response.
#[derive(Debug, Serialize)]
pub struct DescribeFeedGeneratorResponse {
    pub did: String,
    pub feeds: Vec<DescribeFeedGeneratorFeed>,
}

/// Extract DID from Authorization header (JWT claims without verification).
fn extract_did_from_auth(headers: &HeaderMap) -> Option<String> {
    let auth = headers.get("authorization")?.to_str().ok()?;
    let token = auth.strip_prefix("Bearer ")?;
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() < 2 {
        return None;
    }

    // JWT payload is base64url encoded, may need padding
    let mut payload_b64 = parts[1].to_string();
    let padding = 4 - payload_b64.len() % 4;
    if padding != 4 {
        payload_b64.push_str(&"=".repeat(padding));
    }

    let payload = base64::engine::general_purpose::URL_SAFE
        .decode(&payload_b64)
        .ok()?;
    let claims: serde_json::Value = serde_json::from_slice(&payload).ok()?;

    // The DID is in either 'sub' or 'iss' claim
    claims["iss"]
        .as_str()
        .or_else(|| claims["sub"].as_str())
        .map(String::from)
}

/// Compact URI format: "{did} {rkey}" for space-efficient feed cache storage.
fn uri_to_compact(uri: &str) -> String {
    // at://did:plc:abc123/app.bsky.feed.post/3ldefgh456 -> did:plc:abc123 3ldefgh456
    let path = uri.strip_prefix("at://").unwrap_or(uri);
    let parts: Vec<&str> = path.split('/').collect();
    if parts.len() >= 3 {
        format!("{} {}", parts[0], parts[2])
    } else {
        uri.to_string()
    }
}

/// Convert compact format back to AT-URI.
fn compact_to_uri(compact: &str) -> String {
    // did:plc:abc123 3ldefgh456 -> at://did:plc:abc123/app.bsky.feed.post/3ldefgh456
    if let Some((did, rkey)) = compact.split_once(' ') {
        format!("at://{}/app.bsky.feed.post/{}", did, rkey)
    } else {
        compact.to_string()
    }
}

/// Queue a sync request for an algorithm (fire-and-forget).
///
/// Uses SADD to pending:syncs for deduplication before LPUSH to queue.
/// This allows the feed handler to trigger syncs when algorithm data
/// is missing, without blocking the response.
async fn queue_algo_sync(redis: &graze_common::RedisClient, algo_id: i32) {
    let pending_key = "pending:syncs";
    let algo_str = algo_id.to_string();

    // Try to add to pending set - if already pending, skip
    match redis
        .sadd(pending_key, std::slice::from_ref(&algo_str))
        .await
    {
        Ok(added) if added > 0 => {
            // Not already pending, add to queue
            if let Err(e) = redis.lpush(Keys::SYNC_QUEUE, &algo_str).await {
                warn!(algo_id, error = %e, "failed_to_queue_sync");
            } else {
                debug!(algo_id, "sync_queued_from_feed");
            }
        }
        Ok(_) => {
            // Already in pending set, skip
            debug!(algo_id, "sync_already_pending");
        }
        Err(e) => {
            warn!(algo_id, error = %e, "failed_to_check_pending_syncs");
        }
    }
}

/// GET /xrpc/app.bsky.feed.getFeedSkeleton
///
/// ATProto-compliant feed skeleton endpoint.
pub async fn get_feed_skeleton(
    State(state): State<Arc<AppState>>,
    Extension(request_id): Extension<RequestId>,
    headers: HeaderMap,
    Query(query): Query<FeedSkeletonQuery>,
) -> Response {
    let request_start = std::time::Instant::now();
    let request_id = request_id.to_string();
    let limit = query.limit.min(100);

    debug!(
        feed = %query.feed,
        limit,
        cursor = query.cursor.as_deref().unwrap_or("none"),
        "feed_skeleton_request"
    );

    // Look up feed URI to get algo_id
    let algo_id_str = match state.redis.hget(Keys::SUPPORTED_FEEDS, &query.feed).await {
        Ok(Some(id)) => id,
        Ok(None) => {
            emit_skip_log(&request_id, None, None, "unknown_feed", Some("UnknownFeed"));
            let error_response = json!({
                "error": "UnknownFeed",
                "message": format!("Feed not found: {}", query.feed)
            });
            debug!(
                feed = %query.feed,
                error = "UnknownFeed",
                response = %error_response,
                "feed_skeleton_error"
            );
            return (StatusCode::BAD_REQUEST, Json(error_response)).into_response();
        }
        Err(e) => {
            emit_skip_log(
                &request_id,
                None,
                None,
                "redis_lookup_error",
                Some("InternalError"),
            );
            error!(error = %e, feed = %query.feed, "redis_error");
            let error_response = json!({
                "error": "InternalError",
                "message": "Database error"
            });
            debug!(
                feed = %query.feed,
                error = "InternalError",
                response = %error_response,
                "feed_skeleton_error"
            );
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(error_response)).into_response();
        }
    };

    let algo_id: i32 = match algo_id_str.parse() {
        Ok(id) => id,
        Err(_) => {
            emit_skip_log(
                &request_id,
                None,
                None,
                "invalid_algo_config",
                Some("InternalError"),
            );
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "error": "InternalError",
                    "message": "Invalid algorithm configuration"
                })),
            )
                .into_response();
        }
    };

    // Parse cursor to get pagination state
    let feed_cursor = FeedCursor::decode(query.cursor.as_deref());

    // Check for end of feed
    if feed_cursor.is_eof() {
        let user_hash_for_log = extract_did_from_auth(&headers).map(|d| hash_did(&d));
        emit_skip_log(
            &request_id,
            user_hash_for_log.as_deref(),
            Some(algo_id),
            "cursor_eof",
            None,
        );
        let response = FeedSkeletonResponse {
            feed: vec![],
            cursor: Some("eof".to_string()),
        };
        debug!(
            feed = %query.feed,
            algo_id,
            reason = "cursor_eof",
            "feed_skeleton_empty"
        );
        return (StatusCode::OK, Json(response)).into_response();
    }

    // Extract user DID from Authorization header
    let user_did = extract_did_from_auth(&headers);

    // Initialize audit collector if audit is enabled for this user
    let user_hash_for_audit = user_did.as_ref().map(|d| hash_did(d));
    let mut audit = if should_audit(&state.config, &state.redis, user_did.as_deref()).await {
        Some(AuditCollector::new(
            request_id.clone(),
            user_hash_for_audit.clone().unwrap_or_default(),
            algo_id,
            state.config.audit_max_contributors,
            state.config.audit_log_full_breakdown,
        ))
    } else {
        None
    };

    // Check if algorithm posts exist
    let algo_posts_key = Keys::algo_posts(algo_id);
    let algo_exists = state.redis.exists(&algo_posts_key).await.unwrap_or(false);

    if !algo_exists {
        // Fetch diagnostic info for better debugging
        let meta_key = Keys::algo_meta(algo_id);
        let meta_exists = state.redis.exists(&meta_key).await.unwrap_or(false);
        let last_sync: Option<String> = if meta_exists {
            state
                .redis
                .hget(&meta_key, "last_sync")
                .await
                .ok()
                .flatten()
        } else {
            None
        };
        let lock_key = Keys::sync_lock(algo_id);
        let sync_locked = state.redis.exists(&lock_key).await.unwrap_or(false);

        // Queue sync request so data is populated for future requests
        if !state.config.read_only_mode {
            queue_algo_sync(&state.redis, algo_id).await;
        }

        emit_skip_log(
            &request_id,
            user_hash_for_audit.as_deref(),
            Some(algo_id),
            "no_algo_posts",
            None,
        );

        let fallback_tagged = get_fallback_blend(
            &state.redis,
            &state.interner,
            &state.config,
            algo_id,
            limit,
            0,
            &HashSet::new(),
        )
        .await
        .unwrap_or_default();

        if !fallback_tagged.is_empty() {
            let take_count = fallback_tagged.len().min(limit);
            let shown_fallback: HashSet<String> = fallback_tagged
                .iter()
                .take(take_count)
                .map(|(u, _)| u.clone())
                .collect();
            let total = take_count;
            let feed: Vec<SkeletonFeedPost> = fallback_tagged
                .into_iter()
                .take(take_count)
                .enumerate()
                .map(|(depth, (uri, tranche))| {
                    let prov = ItemProvenance::Base(BlendedSource::Fallback { tranche });
                    let feed_context = encode_feed_context(
                        &query.feed,
                        algo_id,
                        depth,
                        total,
                        0,
                        &prov,
                        None,
                        None,
                        None,
                    );
                    SkeletonFeedPost {
                        post: uri,
                        reason: None,
                        feed_context,
                    }
                })
                .collect();
            let cursor = FeedCursor {
                fallback_only: true,
                fallback_offset: feed.len(),
                shown_fallback,
                ..Default::default()
            };

            debug!(
                feed = %query.feed,
                algo_id,
                count = feed.len(),
                reason = "no_algo_posts_fallback_served",
                "feed_skeleton_fallback"
            );

            return (
                StatusCode::OK,
                Json(FeedSkeletonResponse {
                    feed,
                    cursor: Some(cursor.encode()),
                }),
            )
                .into_response();
        }

        // No algo posts AND no fallback - return placeholder
        debug!(
            feed = %query.feed,
            algo_id,
            algo_posts_key = %algo_posts_key,
            meta_exists,
            last_sync_timestamp = last_sync.as_deref(),
            sync_rate_limited = sync_locked,
            reason = "no_algo_posts",
            placeholder = PLACEHOLDER_EMPTY,
            "feed_skeleton_placeholder"
        );
        let placeholder_context = FeedContextProvenance {
            feed_uri: query.feed.clone(),
            algo_id,
            depth: 0,
            personalized: false,
            source: "placeholder".to_string(),
            personalization_type: None,
            fallback_tranche: None,
            total: 0,
            personalized_count: 0,
            attribution: None,
            params: None,
            response_time_ms: None,
            is_holdout: None,
            is_personalization_holdout: None,
            impression_id: None,
        }
        .encode();
        let response = FeedSkeletonResponse {
            feed: vec![SkeletonFeedPost {
                post: PLACEHOLDER_EMPTY.to_string(),
                reason: None,
                feed_context: placeholder_context,
            }],
            cursor: Some("eof".to_string()),
        };
        return (StatusCode::OK, Json(response)).into_response();
    }

    let mut base_posts_tagged: Vec<(String, BlendedSource)> = Vec::new();
    let mut was_personalized = false;
    let mut fallback_reason: Option<&str> = None;
    let mut feed_cache_hit = false;
    let mut response_thompson_meta: Option<ResponseThompsonMeta> = None;
    let mut is_personalization_holdout_for_provenance = false;
    let is_fallback_only = feed_cursor.fallback_only;
    // ML state: uri_to_features map and scoring result, populated by the ML scoring path
    let mut ml_uri_to_features: rustc_hash::FxHashMap<String, PostFeatures> =
        rustc_hash::FxHashMap::default();
    let mut ml_scoring_result: Option<ScoringResult> = None;
    // Hoisted so impression logging (outside user_has_data block) can reference them
    let user_hash: String = user_did.as_ref().map(|d| hash_did(d)).unwrap_or_default();
    let is_first_page = feed_cursor.is_first_page();

    // ═══════════════════════════════════════════════════════════════════
    // Handle fallback-only mode (personalization exhausted or holdout)
    // ═══════════════════════════════════════════════════════════════════
    if is_fallback_only {
        fallback_reason = Some(if feed_cursor.is_personalization_holdout {
            "personalization_holdout"
        } else {
            "personalization_exhausted"
        });
        is_personalization_holdout_for_provenance = feed_cursor.is_personalization_holdout;
        let exclude_set: HashSet<String> = feed_cursor.shown_fallback.clone();

        let fallback_raw = get_fallback_blend(
            &state.redis,
            &state.interner,
            &state.config,
            algo_id,
            limit * 2,
            feed_cursor.fallback_offset,
            &exclude_set,
        )
        .await
        .unwrap_or_default();

        let mut seen = exclude_set;
        for (uri, tranche) in fallback_raw {
            if !seen.contains(&uri) {
                seen.insert(uri.clone());
                base_posts_tagged.push((uri, BlendedSource::Fallback { tranche }));
            }
        }
        base_posts_tagged.truncate(limit);
    }
    // ═══════════════════════════════════════════════════════════════════
    // Check feed cache first (per-user, stores multiple pages)
    // ═══════════════════════════════════════════════════════════════════
    else if let Some(ref did) = user_did {
        let feed_cache_key = Keys::feed_cache(algo_id, &user_hash);

        // Personalization holdout: 5% of first-page requests get non-personalized blend for A/B test
        if is_first_page
            && state.config.personalization_holdout_rate > 0.0
            && rand::random::<f64>() < state.config.personalization_holdout_rate
        {
            fallback_reason = Some("personalization_holdout");
            is_personalization_holdout_for_provenance = true;
            let fallback_raw = get_fallback_blend(
                &state.redis,
                &state.interner,
                &state.config,
                algo_id,
                limit * 2,
                0,
                &HashSet::new(),
            )
            .await
            .unwrap_or_default();
            for (uri, tranche) in fallback_raw {
                if base_posts_tagged.len() >= limit {
                    break;
                }
                base_posts_tagged.push((uri, BlendedSource::Fallback { tranche }));
            }
        } else if state.config.feed_cache_enabled {
            let cache_offset = feed_cursor.offset.max(0) as isize;

            // Check if cache exists by getting its length
            let cache_len = state.redis.llen(&feed_cache_key).await.unwrap_or(0);

            if cache_len > 0 {
                // Cache exists - try to get posts at offset
                if let Ok(cached) = state
                    .redis
                    .lrange(
                        &feed_cache_key,
                        cache_offset,
                        cache_offset + (limit as isize) - 1,
                    )
                    .await
                {
                    if !cached.is_empty() {
                        feed_cache_hit = true;
                        base_posts_tagged = cached
                            .into_iter()
                            .map(|c| (compact_to_uri(&c), BlendedSource::PostLevelPersonalization))
                            .collect();
                        was_personalized = true;
                    } else if !is_first_page {
                        // Cache exists but offset is beyond cached posts - return EOF
                        // This means the user has scrolled past all cached content
                        debug!(
                            feed = %query.feed,
                            algo_id,
                            user_did = user_did.as_deref(),
                            cache_len,
                            requested_offset = feed_cursor.offset,
                            "feed_cache_exhausted"
                        );
                        let response = FeedSkeletonResponse {
                            feed: vec![],
                            cursor: Some("eof".to_string()),
                        };
                        return (StatusCode::OK, Json(response)).into_response();
                    }
                }
            }
        }

        // If cache miss and NOT holdout, run personalization (only on first page when cache enabled)
        // When holdout, we already set base_posts_tagged from fallback above - do not overwrite.
        if !feed_cache_hit && !is_personalization_holdout_for_provenance {
            // Check if user has any likes in date-based keys.
            // Check today and yesterday so users who liked before UTC midnight still
            // get the personalization path rather than falling to unfiltered fallback.
            let now_secs = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs_f64();
            let today = graze_common::today_date();
            let yesterday = graze_common::date_from_timestamp(now_secs - 86400.0);
            let user_likes_key_today = Keys::user_likes_date(&user_hash, &today);
            let user_likes_key_yesterday = Keys::user_likes_date(&user_hash, &yesterday);
            let (exists_today, exists_yesterday) = tokio::join!(
                state.redis.exists(&user_likes_key_today),
                state.redis.exists(&user_likes_key_yesterday),
            );
            let user_has_data = exists_today.unwrap_or(false) || exists_yesterday.unwrap_or(false);

            if user_has_data {
                // Load feed-specific Thompson config (holdout override, etc.)
                let feed_config: Option<FeedThompsonConfig> = state
                    .redis
                    .get_string(&Keys::feed_thompson_config(algo_id))
                    .await
                    .ok()
                    .flatten()
                    .and_then(|s| serde_json::from_str(&s).ok());

                // Load global search space and success criteria
                let global_search_space: Option<ThompsonSearchSpace> = state
                    .redis
                    .get_string(Keys::thompson_search_space())
                    .await
                    .ok()
                    .flatten()
                    .and_then(|s| serde_json::from_str(&s).ok());
                let global_success_criteria: Option<FeedSuccessConfig> = state
                    .redis
                    .get_string(Keys::thompson_success_criteria())
                    .await
                    .ok()
                    .flatten()
                    .and_then(|s| serde_json::from_str(&s).ok());

                let holdout_override = feed_config.as_ref().and_then(|c| c.holdout_params.as_ref());
                let treatment_override = feed_config
                    .as_ref()
                    .and_then(|c| c.treatment_params.as_ref());
                let search_space = feed_config
                    .as_ref()
                    .and_then(|c| c.search_space.as_ref())
                    .or(global_search_space.as_ref());

                // Select Thompson Sampling parameters for this request
                let thompson_params: SelectedParams =
                    state.thompson.select_params_with_holdout_and_search_space(
                        algo_id,
                        holdout_override,
                        treatment_override,
                        search_space,
                    );

                // Convert Thompson params to PersonalizationParams override
                let params_override = PersonalizationParams {
                    max_user_likes: Some(thompson_params.max_user_likes),
                    max_sources_per_post: Some(thompson_params.max_sources_per_post),
                    max_total_sources: Some(thompson_params.max_total_sources),
                    min_co_likes: Some(thompson_params.min_co_likes),
                    seed_sample_pool: Some(thompson_params.seed_sample_pool),
                    corater_decay: Some(thompson_params.corater_decay_pct as f64 / 100.0),
                    ..Default::default()
                };

                // On first page with cache enabled, fetch larger batch for caching
                let fetch_limit = if is_first_page && state.config.feed_cache_enabled {
                    state.config.feed_cache_batch_size
                } else {
                    limit
                };

                // Try personalization with Thompson-selected parameters.
                // When ML is enabled, call compute_personalization() directly so we get
                // PostFeatures for impression logging and optional re-ranking.
                // When disabled, fall back to the cache-aware personalize_with_audit().
                let ml_active =
                    state.config.ml_impressions_enabled || state.config.ml_reranker_enabled;

                // Produce (personalized_uris, posts_checked, posts_scored) via either path
                let perso_result: crate::error::Result<(Vec<String>, usize, usize)> = if ml_active {
                    let base_params = get_preset("default");
                    let params_for_ml = merge_params(base_params, Some(&params_override));
                    let audit_ref = audit.as_mut();
                    match state
                        .algorithm
                        .compute_personalization(&user_hash, algo_id, &params_for_ml, audit_ref)
                        .await
                    {
                        Ok(scoring_result) => {
                            let posts_checked = scoring_result.posts_checked;
                            let posts_scored = scoring_result.scored_count;

                            // Optional ML re-ranking before URI conversion
                            let scored_posts_ordered: Vec<(f64, String)> =
                                if state.config.ml_reranker_enabled
                                    && !scoring_result.post_features.is_empty()
                                {
                                    if let Some(ref ranker) = state.ml_ranker {
                                        let now_ch = Utc::now();
                                        let richness = if scoring_result.posts_checked > 0 {
                                            scoring_result.scored_count as f32
                                                / scoring_result.posts_checked as f32
                                        } else {
                                            0.0
                                        };
                                        let with_features: Vec<(f64, String, PostFeatures)> =
                                            scoring_result
                                                .scored_posts
                                                .iter()
                                                .zip(scoring_result.post_features.iter())
                                                .map(|((s, id), f)| (*s, id.clone(), f.clone()))
                                                .collect();
                                        ranker.rerank(
                                            &with_features,
                                            0u32, // user_like_count (not yet available here)
                                            0u8,  // user_segment: default cold
                                            richness,
                                            now_ch.hour() as u8,
                                            now_ch.weekday().num_days_from_monday() as u8,
                                            is_first_page,
                                            thompson_params.is_holdout,
                                        )
                                    } else {
                                        scoring_result.scored_posts.clone()
                                    }
                                } else {
                                    scoring_result.scored_posts.clone()
                                };

                            // Convert post IDs to URIs
                            let ids: Vec<i64> = scored_posts_ordered
                                .iter()
                                .take(fetch_limit)
                                .filter_map(|(_, id)| id.parse().ok())
                                .collect();
                            let id_to_uri = state
                                .interner
                                .get_uris_batch(&ids)
                                .await
                                .unwrap_or_default();

                            // Build uri_to_features map for impression logging
                            for ((_, id_str), feat) in scoring_result
                                .scored_posts
                                .iter()
                                .zip(scoring_result.post_features.iter())
                            {
                                if let Ok(int_id) = id_str.parse::<i64>() {
                                    if let Some(uri) = id_to_uri.get(&int_id) {
                                        ml_uri_to_features.insert(uri.clone(), feat.clone());
                                    }
                                }
                            }

                            let personalized_uris: Vec<String> = scored_posts_ordered
                                .iter()
                                .take(fetch_limit)
                                .filter_map(|(score, id_str)| {
                                    let uri = id_str
                                        .parse::<i64>()
                                        .ok()
                                        .and_then(|i| id_to_uri.get(&i).cloned())?;
                                    if let Some(ref mut a) = audit {
                                        a.add_personalized_post(id_str, &uri, *score);
                                    }
                                    Some(uri)
                                })
                                .collect();

                            ml_scoring_result = Some(scoring_result);
                            Ok((personalized_uris, posts_checked, posts_scored))
                        }
                        Err(e) => Err(e),
                    }
                } else {
                    // Standard path: cache-aware personalize_with_audit()
                    match state
                        .algorithm
                        .personalize_with_audit(
                            did,
                            algo_id,
                            fetch_limit,
                            None,
                            Some(&params_override),
                            None,
                            audit.as_mut(),
                        )
                        .await
                    {
                        Ok(result) => {
                            let posts_checked = result.meta.posts_checked.unwrap_or(0);
                            let posts_scored = result.meta.total_scored.unwrap_or(0);

                            let posts_to_convert: Vec<i64> = result
                                .posts
                                .iter()
                                .filter(|p| p.uri.is_empty())
                                .filter_map(|p| p.post_id.parse::<i64>().ok())
                                .collect();
                            let id_to_uri = state
                                .interner
                                .get_uris_batch(&posts_to_convert)
                                .await
                                .unwrap_or_default();

                            let personalized_uris: Vec<String> = result
                                .posts
                                .into_iter()
                                .filter_map(|p| {
                                    let uri = if !p.uri.is_empty() {
                                        Some(p.uri.clone())
                                    } else if let Ok(id) = p.post_id.parse::<i64>() {
                                        id_to_uri.get(&id).cloned()
                                    } else {
                                        None
                                    };
                                    if let Some(ref uri) = uri {
                                        if let Some(ref mut a) = audit {
                                            a.add_personalized_post(&p.post_id, uri, p.score);
                                        }
                                    }
                                    uri
                                })
                                .collect();

                            Ok((personalized_uris, posts_checked, posts_scored))
                        }
                        Err(e) => Err(e),
                    }
                };

                match perso_result {
                    Ok((personalized_uris, posts_checked, posts_scored)) => {
                        // Apply progressive blending with appropriate limit
                        let blend_limit = if is_first_page && state.config.feed_cache_enabled {
                            fetch_limit
                        } else {
                            limit
                        };

                        let blend_result = get_blended_posts_with_stats(
                            &state.redis,
                            &state.interner,
                            &state.config,
                            &state.algorithm,
                            &user_hash,
                            algo_id,
                            blend_limit,
                            personalized_uris,
                        )
                        .await
                        .unwrap_or_default();

                        // Store full batch in feed cache for subsequent pages
                        if state.config.feed_cache_enabled && !blend_result.posts.is_empty() {
                            let compact_posts: Vec<String> = blend_result
                                .posts
                                .iter()
                                .map(|u| uri_to_compact(u))
                                .collect();
                            let _ = state
                                .redis
                                .store_list(
                                    &feed_cache_key,
                                    &compact_posts,
                                    state.config.feed_cache_ttl_seconds as i64,
                                )
                                .await;
                        }

                        // Record Thompson observation
                        let total_response_time_ms = request_start.elapsed().as_secs_f64() * 1000.0;
                        let outcome = FeedOutcome {
                            total_posts: blend_result.posts.len().min(limit),
                            personalized_posts: blend_result.personalized_count,
                            author_affinity_posts: blend_result.author_affinity_count,
                            fallback_posts: blend_result.fallback_count,
                            posts_checked,
                            posts_scored,
                            colikers_used: 0,
                            response_time_ms: total_response_time_ms,
                        };

                        let success_config = feed_config
                            .as_ref()
                            .and_then(|c| c.success_criteria.as_ref())
                            .or(global_success_criteria.as_ref())
                            .unwrap_or(&FeedSuccessConfig::default())
                            .clone();
                        let (success, details) = outcome.evaluate(&success_config);

                        debug!(
                            algo_id,
                            is_holdout = thompson_params.is_holdout,
                            is_exploration = thompson_params.is_exploration,
                            success,
                            personalization_ratio = details.personalization_ratio,
                            richness_passed = details.richness_passed,
                            speed_passed = details.speed_passed,
                            response_time_ms = total_response_time_ms,
                            posts_checked,
                            posts_scored,
                            "thompson_observation"
                        );

                        if feed_config
                            .as_ref()
                            .is_none_or(|c| c.interaction_weights.is_empty())
                        {
                            state
                                .thompson
                                .record_observation(algo_id, &thompson_params, success);
                        }

                        base_posts_tagged = blend_result
                            .posts_with_source
                            .into_iter()
                            .take(limit)
                            .collect();
                        was_personalized = !base_posts_tagged.is_empty();
                        response_thompson_meta = Some(ResponseThompsonMeta {
                            params: thompson_params.clone(),
                            response_time_ms: total_response_time_ms,
                            is_holdout: thompson_params.is_holdout,
                        });
                    }
                    Err(e) => {
                        error!(
                            user_did = user_did.as_deref(),
                            algo_id,
                            error = %e,
                            "personalization_failed"
                        );
                        fallback_reason = Some("personalization_error");

                        // Record failure for Thompson learning
                        let thompson_params_err: SelectedParams =
                            state.thompson.select_params_with_holdout_and_search_space(
                                algo_id,
                                holdout_override,
                                treatment_override,
                                search_space,
                            );
                        state
                            .thompson
                            .record_observation(algo_id, &thompson_params_err, false);

                        base_posts_tagged = get_fallback_blend(
                            &state.redis,
                            &state.interner,
                            &state.config,
                            algo_id,
                            limit,
                            0,
                            &HashSet::new(),
                        )
                        .await
                        .unwrap_or_default()
                        .into_iter()
                        .map(|(u, t)| (u, BlendedSource::Fallback { tranche: t }))
                        .collect();
                    }
                }
            } else {
                fallback_reason = Some("no_user_data");
                base_posts_tagged = get_fallback_blend(
                    &state.redis,
                    &state.interner,
                    &state.config,
                    algo_id,
                    limit,
                    0,
                    &HashSet::new(),
                )
                .await
                .unwrap_or_default()
                .into_iter()
                .map(|(u, t)| (u, BlendedSource::Fallback { tranche: t }))
                .collect();
            }
        }
    } else {
        fallback_reason = Some("no_auth");
        base_posts_tagged = get_fallback_blend(
            &state.redis,
            &state.interner,
            &state.config,
            algo_id,
            limit,
            0,
            &HashSet::new(),
        )
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|(u, t)| (u, BlendedSource::Fallback { tranche: t }))
        .collect();
    }

    // ═══════════════════════════════════════════════════════════════════
    // Universal liked-post filter: remove posts the user already liked
    // from every feed response, regardless of which path produced them.
    // Catches feed-cache hits, fallback paths, and anything else.
    // ═══════════════════════════════════════════════════════════════════
    if state.config.liked_posts_filter_enabled {
        if let Some(ref did) = user_did {
            let filter_hash = hash_did(did);
            let liked_uris = load_user_liked_uris(
                &state.redis,
                &state.interner,
                &filter_hash,
                state.config.liked_posts_filter_max,
            )
            .await;
            if !liked_uris.is_empty() {
                base_posts_tagged.retain(|(uri, _)| !liked_uris.contains(uri));
            }
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    // Inject placeholder if feed would be empty on first page
    // ═══════════════════════════════════════════════════════════════════
    if base_posts_tagged.is_empty() && feed_cursor.is_first_page() {
        let placeholder = match fallback_reason {
            Some("no_auth") => PLACEHOLDER_NO_AUTH,
            Some("personalization_error") => PLACEHOLDER_ERROR,
            _ => PLACEHOLDER_EMPTY,
        };
        debug!(
            reason = fallback_reason.unwrap_or("unknown"),
            placeholder, "feed_skeleton_placeholder"
        );
        base_posts_tagged.push((
            placeholder.to_string(),
            BlendedSource::Fallback {
                tranche: "placeholder".to_string(),
            },
        ));
    }

    // ═══════════════════════════════════════════════════════════════════
    // Get special posts and inject them
    // ═══════════════════════════════════════════════════════════════════
    let special_posts = state
        .special_posts
        .get_special_posts(algo_id)
        .await
        .unwrap_or_else(|_| SpecialPostsResponse::empty(algo_id));

    let (final_with_provenance, updated_cursor) = inject_special_posts(
        base_posts_tagged.clone(),
        &special_posts,
        &feed_cursor,
        limit,
    );

    // ═══════════════════════════════════════════════════════════════════
    // Build response cursor
    // ═══════════════════════════════════════════════════════════════════
    let mut response_cursor = updated_cursor;
    let is_end_of_feed: bool;

    let base_len = base_posts_tagged.len();
    if is_fallback_only {
        response_cursor.fallback_offset = feed_cursor.fallback_offset + base_len;
        response_cursor.shown_fallback = base_posts_tagged.iter().map(|(u, _)| u.clone()).collect();
        response_cursor.is_personalization_holdout = feed_cursor.is_personalization_holdout;
        is_end_of_feed = base_posts_tagged.is_empty();
    } else if is_personalization_holdout_for_provenance {
        // Holdout: set cursor for fallback-only so next page continues with non-personalized
        response_cursor.fallback_only = true;
        response_cursor.fallback_offset = base_len;
        response_cursor.shown_fallback = base_posts_tagged.iter().map(|(u, _)| u.clone()).collect();
        response_cursor.is_personalization_holdout = true;
        is_end_of_feed = base_posts_tagged.is_empty();
    } else if was_personalized {
        response_cursor.offset = feed_cursor.offset + (base_len as i32);
        if base_len < limit / 2 {
            response_cursor.fallback_only = true;
            response_cursor.fallback_offset = 0;
            response_cursor.shown_fallback =
                base_posts_tagged.iter().map(|(u, _)| u.clone()).collect();
        }
        is_end_of_feed = false;
    } else {
        response_cursor.offset = feed_cursor.offset + (base_len as i32);
        is_end_of_feed = base_posts_tagged.is_empty();
    }

    let next_cursor = if is_end_of_feed {
        Some("eof".to_string())
    } else {
        Some(response_cursor.encode())
    };

    let total = final_with_provenance.len();
    let personalized_count = final_with_provenance
        .iter()
        .filter(|(_, p)| {
            matches!(
                p,
                ItemProvenance::Base(BlendedSource::PostLevelPersonalization)
                    | ItemProvenance::Base(BlendedSource::AuthorAffinity)
            )
        })
        .count();

    // Update response_time_ms with fresh value before encode (includes special posts injection)
    if let Some(ref mut meta) = response_thompson_meta {
        meta.response_time_ms = request_start.elapsed().as_secs_f64() * 1000.0;
    }

    // Generate per-post impression IDs for ML tracking (only for non-special posts)
    let ml_impressions_active = state.config.ml_impressions_enabled
        && state.impression_queue.is_some()
        && !user_hash.is_empty();
    let impression_ids: Vec<Option<String>> = if ml_impressions_active {
        final_with_provenance
            .iter()
            .map(|(_, prov)| match prov {
                ItemProvenance::Pinned { .. }
                | ItemProvenance::Rotating { .. }
                | ItemProvenance::Sponsored { .. } => None,
                _ => Some(format!("{:016x}", rand::random::<u64>())),
            })
            .collect()
    } else {
        vec![None; final_with_provenance.len()]
    };

    let feed: Vec<SkeletonFeedPost> = final_with_provenance
        .iter()
        .enumerate()
        .map(|(depth, (uri, prov))| {
            let impression_id = impression_ids.get(depth).and_then(|id| id.clone());
            let feed_context = encode_feed_context(
                &query.feed,
                algo_id,
                depth,
                total,
                personalized_count,
                prov,
                response_thompson_meta.as_ref(),
                if is_personalization_holdout_for_provenance {
                    Some(true)
                } else {
                    None
                },
                impression_id,
            );
            SkeletonFeedPost {
                post: uri.clone(),
                reason: None,
                feed_context,
            }
        })
        .collect();

    let (injected_pinned, injected_rotating, injected_sponsored) =
        count_injected(&feed_cursor, &response_cursor);

    let response_time_ms = request_start.elapsed().as_millis();

    // ═══════════════════════════════════════════════════════════════════
    // ML Impression logging (fire-and-forget, only when enabled)
    // ═══════════════════════════════════════════════════════════════════
    if ml_impressions_active {
        if let Some(ref imp_queue) = state.impression_queue {
            let richness_ratio = ml_scoring_result
                .as_ref()
                .map(|sr| {
                    if sr.posts_checked > 0 {
                        sr.scored_count as f32 / sr.posts_checked as f32
                    } else {
                        0.0
                    }
                })
                .unwrap_or(0.0);
            let served_at = Utc::now();
            let hour_of_day = served_at.hour() as u8;
            let day_of_week = served_at.weekday().num_days_from_monday() as u8;
            let is_holdout_flag = response_thompson_meta
                .as_ref()
                .map(|m| m.is_holdout)
                .unwrap_or(false);
            let is_exploration_flag = response_thompson_meta
                .as_ref()
                .map(|m| m.params.is_exploration)
                .unwrap_or(false);
            let resp_time_ms = response_time_ms as f32;

            let mut impression_rows: Vec<FeedImpressionRow> = Vec::new();
            for ((depth, (uri, source)), imp_id_opt) in final_with_provenance
                .iter()
                .enumerate()
                .zip(impression_ids.iter())
            {
                let imp_id = match imp_id_opt {
                    Some(id) => id.clone(),
                    None => continue, // skip special posts
                };
                let features = ml_uri_to_features.get(uri).cloned().unwrap_or_default();
                let source_str = match source {
                    ItemProvenance::Base(BlendedSource::PostLevelPersonalization) => "personalized",
                    ItemProvenance::Base(BlendedSource::AuthorAffinity) => "author_affinity",
                    ItemProvenance::Base(BlendedSource::Fallback { .. }) => "fallback",
                    _ => "other",
                };
                impression_rows.push(FeedImpressionRow {
                    impression_id: imp_id,
                    user_hash: user_hash.clone(),
                    post_id: uri.clone(),
                    algo_id,
                    served_at,
                    depth: depth.min(u8::MAX as usize) as u8,
                    source: source_str.to_string(),
                    is_holdout: is_holdout_flag,
                    is_exploration: is_exploration_flag,
                    response_time_ms: resp_time_ms,
                    is_first_page,
                    raw_score: features.raw_score,
                    final_score: features.final_score,
                    num_paths: features.num_paths,
                    liker_count: features.liker_count,
                    popularity_penalty: features.popularity_penalty,
                    paths_boost: features.paths_boost,
                    max_contribution: features.max_contribution,
                    score_concentration: features.score_concentration,
                    newest_like_age_hours: features.newest_like_age_hours,
                    oldest_like_age_hours: features.oldest_like_age_hours,
                    was_liker_cache_hit: features.was_liker_cache_hit,
                    coliker_count: features.coliker_count,
                    top_coliker_weight: features.top_coliker_weight,
                    top5_weight_sum: features.top5_weight_sum,
                    mean_coliker_weight: features.mean_coliker_weight,
                    weight_concentration: features.weight_concentration,
                    user_like_count: 0, // TODO: query from Redis in a follow-up
                    user_segment: "unknown".to_string(),
                    richness_ratio,
                    hour_of_day,
                    day_of_week,
                });
            }

            if !impression_rows.is_empty() {
                let queue = imp_queue.clone();
                tokio::spawn(async move {
                    queue.send_batch(impression_rows).await;
                });
            }
        }
    }

    let response = FeedSkeletonResponse {
        feed,
        cursor: next_cursor,
    };

    info!(
        feed = %query.feed,
        algo_id,
        user_did = user_did.as_deref(),
        posts_count = response.feed.len(),
        base_posts_count = base_posts_tagged.len(),
        injected_pinned,
        injected_rotating,
        injected_sponsored,
        personalized = was_personalized,
        fallback_only = response_cursor.fallback_only,
        feed_cache_hit,
        fallback_reason,
        response_time_ms,
        "feed_skeleton_served"
    );

    // Record feed access for rolling sync (fire-and-forget)
    if !state.config.read_only_mode
        && state.config.feed_access_sync_enabled
        && !response.feed.is_empty()
    {
        let redis = state.redis.clone();
        tokio::spawn(async move {
            record_feed_access(&redis, algo_id).await;
        });
    }

    // Post-render logging: enqueue to log_tasks when REDIS_REQUESTS_LOGGER is set (skip quietly when unset)
    if let Some(ref redis_logger) = state.redis_requests_logger {
        if !response.feed.is_empty() {
            let feed_uri = query.feed.clone();
            let authorization_header = headers
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .map(String::from);
            let cursor = query.cursor.clone();
            let post_ids: Vec<String> = response.feed.iter().map(|e| e.post.clone()).collect();
            let attributions: Vec<Option<String>> = response.feed.iter().map(|_| None).collect();
            let contexts: Vec<String> = response
                .feed
                .iter()
                .map(|e| e.feed_context.clone().unwrap_or_default())
                .collect();
            let now = Utc::now().to_rfc3339();
            let task = PostRenderLogTask {
                feed_uri,
                authorization_header,
                cursor,
                limit,
                post_ids,
                attributions,
                contexts,
                created_at: now.clone(),
                updated_at: now,
                uuid: uuid::Uuid::new_v4().to_string(),
            };
            let redis_logger = redis_logger.clone();
            tokio::spawn(async move {
                if let Ok(json) = serde_json::to_string(&task) {
                    let _ = redis_logger.rpush(Keys::LOG_TASKS, &[json]).await;
                }
            });
        }
    }

    // Debug log the full response JSON
    if let Ok(response_json) = serde_json::to_string(&response) {
        debug!(
            feed = %query.feed,
            algo_id,
            response_json,
            "feed_skeleton_response"
        );
    }

    // Emit audit log if enabled
    if let Some(mut a) = audit {
        a.set_timing(response_time_ms as f64, None, None, None, None);
        a.emit_log();
    }

    (StatusCode::OK, Json(response)).into_response()
}

/// GET /xrpc/app.bsky.feed.describeFeedGenerator
///
/// Returns metadata about the feed generator.
pub async fn describe_feed_generator(State(state): State<Arc<AppState>>) -> Response {
    // Get all supported feeds
    let feeds: Vec<DescribeFeedGeneratorFeed> =
        match state.redis.hgetall(Keys::SUPPORTED_FEEDS).await {
            Ok(pairs) => pairs
                .into_iter()
                .map(|(uri, _)| DescribeFeedGeneratorFeed { uri })
                .collect(),
            Err(e) => {
                warn!(error = %e, "describe_feed_generator_redis_error");
                Vec::new()
            }
        };

    let response = DescribeFeedGeneratorResponse {
        did: state.config.feed_generator_did.clone(),
        feeds,
    };

    debug!(
        did = %response.did,
        feed_count = response.feeds.len(),
        "describe_feed_generator"
    );

    // Log each registered feed URI
    if let Ok(response_json) = serde_json::to_string(&response) {
        debug!(response_json, "describe_feed_generator_response");
    }

    (StatusCode::OK, Json(response)).into_response()
}

/// GET /.well-known/did.json
///
/// Returns the DID document for the feed generator.
pub async fn well_known_did(State(state): State<Arc<AppState>>) -> Response {
    let did = &state.config.feed_generator_did;

    // Extract host from DID (did:web:example.com -> example.com)
    let host = did.strip_prefix("did:web:").unwrap_or("localhost");

    (
        StatusCode::OK,
        Json(json!({
            "@context": ["https://www.w3.org/ns/did/v1"],
            "id": did,
            "alsoKnownAs": [],
            "verificationMethod": [],
            "service": [
                {
                    "id": "#bsky_fg",
                    "type": "BskyFeedGenerator",
                    "serviceEndpoint": format!("https://{}", host)
                }
            ]
        })),
    )
        .into_response()
}

/// POST /xrpc/app.bsky.feed.sendInteractions
///
/// Receive interaction events from the client.
///
/// This endpoint:
/// 1. Logs all interactions to ClickHouse for analytics (if enabled)
/// 2. Processes "seen" events for feed deduplication in Redis
pub async fn send_interactions(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(request): Json<graze_common::models::SendInteractionsRequest>,
) -> Response {
    let user_did = extract_did_from_auth(&headers);

    debug!(
        user_did = user_did.as_deref().unwrap_or("anon"),
        interaction_count = request.interactions.len(),
        "interactions_received"
    );

    // Queue interactions for batched ClickHouse persistence (if enabled and user is authenticated).
    // Returns immediately; background worker batches and flushes every few seconds.
    if state.config.interactions_logging_enabled {
        if let (Some(ref did), Some(ref queue)) = (&user_did, &state.interaction_queue) {
            queue.send(did.clone(), request.interactions.clone()).await;
        }
    }

    // Process seen posts for Redis deduplication (if enabled)
    if state.config.seen_posts_enabled {
        if let Some(ref did) = user_did {
            let user_hash = hash_did(did);
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as f64;

            let seen_key = Keys::user_seen(&user_hash);
            let mut seen_ids: Vec<String> = Vec::new();

            for interaction in &request.interactions {
                if interaction.event_type == "app.bsky.feed.defs#interactionSeen" {
                    // Use the item URI directly as the post identifier
                    let item = &interaction.item;
                    if !item.is_empty() {
                        // Get post ID from interner (don't create new ones for seen posts)
                        if let Ok(Some(post_id)) = state.interner.get_id(item).await {
                            seen_ids.push(post_id.to_string());
                        }
                    }
                }
            }

            if !seen_ids.is_empty() {
                // Add to seen posts sorted set
                let items: Vec<(f64, &str)> =
                    seen_ids.iter().map(|id| (now, id.as_str())).collect();
                let _ = state.redis.zadd(&seen_key, &items).await;
                let _ = state
                    .redis
                    .expire(&seen_key, (state.config.seen_posts_ttl_hours * 3600) as i64)
                    .await;
            }
        }
    }

    // Second-pass Thompson learning: interaction-based success signal
    for interaction in &request.interactions {
        let feed_context_str = match &interaction.feed_context {
            Some(s) if !s.is_empty() => s,
            _ => continue,
        };

        let provenance = match graze_common::models::FeedContextProvenance::decode(feed_context_str)
        {
            Some(p) => p,
            None => continue,
        };

        // Skip if no params or holdout
        let params = match &provenance.params {
            Some(p) => p,
            None => continue,
        };
        if provenance.is_holdout == Some(true) {
            continue;
        }

        // Need response_time_ms for speed gate
        let response_time_ms = match provenance.response_time_ms {
            Some(rt) => rt,
            None => continue,
        };

        // Load feed config
        let feed_config: Option<FeedThompsonConfig> = state
            .redis
            .get_string(&Keys::feed_thompson_config(provenance.algo_id))
            .await
            .ok()
            .flatten()
            .and_then(|s| serde_json::from_str(&s).ok());

        let config = match &feed_config {
            Some(c) if c.enabled => c,
            _ => continue,
        };

        let selected_params = state.thompson.selected_params_from_provenance(params);

        // Speed gate: slow response = negative signal (learn to avoid these params)
        if response_time_ms > config.speed_gate_ms {
            state
                .thompson
                .record_observation(provenance.algo_id, &selected_params, false);
            continue;
        }

        // Look up interaction weight
        let weight = match config.interaction_weights.get(&interaction.event_type) {
            Some(w) if w.sign != 0 => w,
            _ => continue,
        };

        let success_score = (weight.sign as f64) * weight.multiplier;
        let success = success_score > 0.0;

        state
            .thompson
            .record_observation(provenance.algo_id, &selected_params, success);
    }

    (StatusCode::OK, Json(json!({}))).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::special_posts::ItemProvenance;
    use graze_common::models::FeedContextProvenance;

    // ─── URI helpers ─────────────────────────────────────────────────────────

    #[test]
    fn test_uri_to_compact() {
        let uri = "at://did:plc:abc123/app.bsky.feed.post/3ldefgh456";
        assert_eq!(uri_to_compact(uri), "did:plc:abc123 3ldefgh456");
    }

    #[test]
    fn test_compact_to_uri() {
        let compact = "did:plc:abc123 3ldefgh456";
        assert_eq!(
            compact_to_uri(compact),
            "at://did:plc:abc123/app.bsky.feed.post/3ldefgh456"
        );
    }

    #[test]
    fn test_uri_roundtrip() {
        let uri = "at://did:plc:xyz789/app.bsky.feed.post/abc123";
        assert_eq!(compact_to_uri(&uri_to_compact(uri)), uri);
    }

    // ─── encode_feed_context: impression_id threading ────────────────────────

    fn call_encode(prov: &ItemProvenance, impression_id: Option<String>) -> Option<String> {
        encode_feed_context(
            "at://did:plc:svc/app.bsky.feed.generator/test",
            1,
            0,
            10,
            5,
            prov,
            None,
            None,
            impression_id,
        )
    }

    #[test]
    fn test_encode_feed_context_impression_id_present() {
        let prov = ItemProvenance::Base(BlendedSource::PostLevelPersonalization);
        let encoded =
            call_encode(&prov, Some("cafebabe00112233".to_string())).expect("should encode");
        let decoded = FeedContextProvenance::decode(&encoded).expect("should decode");
        assert_eq!(decoded.impression_id, Some("cafebabe00112233".to_string()));
    }

    #[test]
    fn test_encode_feed_context_impression_id_none() {
        let prov = ItemProvenance::Base(BlendedSource::PostLevelPersonalization);
        let encoded = call_encode(&prov, None).expect("should encode");
        let decoded = FeedContextProvenance::decode(&encoded).expect("should decode");
        assert_eq!(decoded.impression_id, None);
    }

    #[test]
    fn test_encode_feed_context_special_post_no_impression_id() {
        // Special posts (pinned/rotating/sponsored) should never get impression IDs
        let provs = [
            ItemProvenance::Pinned {
                attribution: "pin-test".to_string(),
            },
            ItemProvenance::Rotating {
                attribution: "rot-test".to_string(),
            },
            ItemProvenance::Sponsored {
                attribution: "spon-test".to_string(),
            },
        ];
        for prov in &provs {
            // Passing None always works; assert we don't accidentally embed one
            let encoded = call_encode(prov, None).expect("should encode");
            let decoded = FeedContextProvenance::decode(&encoded).expect("should decode");
            assert_eq!(decoded.impression_id, None);
        }
    }

    // ─── Impression ID format ────────────────────────────────────────────────

    #[test]
    fn test_impression_id_is_16_hex_chars() {
        // IDs are generated as format!("{:016x}", rand::random::<u64>())
        let id = format!("{:016x}", 0x_a3f9bc7d_21e84c05_u64);
        assert_eq!(id.len(), 16);
        assert!(id.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_impression_ids_are_unique() {
        let ids: Vec<String> = (0..100)
            .map(|_| format!("{:016x}", rand::random::<u64>()))
            .collect();
        let unique: std::collections::HashSet<_> = ids.iter().collect();
        // Probability of collision in 100 draws from 2^64 is negligible
        assert_eq!(unique.len(), ids.len());
    }

    // ─── encode_feed_context: provenance fields ───────────────────────────────

    #[test]
    fn test_encode_feed_context_personalized_source() {
        let prov = ItemProvenance::Base(BlendedSource::PostLevelPersonalization);
        let encoded = call_encode(&prov, None).unwrap();
        let decoded = FeedContextProvenance::decode(&encoded).unwrap();
        assert_eq!(decoded.source, "personalized");
        assert!(decoded.personalized);
    }

    #[test]
    fn test_encode_feed_context_author_affinity_source() {
        let prov = ItemProvenance::Base(BlendedSource::AuthorAffinity);
        let encoded = call_encode(&prov, None).unwrap();
        let decoded = FeedContextProvenance::decode(&encoded).unwrap();
        assert_eq!(decoded.source, "author_affinity");
        assert!(decoded.personalized);
    }

    #[test]
    fn test_encode_feed_context_fallback_source() {
        let prov = ItemProvenance::Base(BlendedSource::Fallback {
            tranche: "trending".to_string(),
        });
        let encoded = call_encode(&prov, None).unwrap();
        let decoded = FeedContextProvenance::decode(&encoded).unwrap();
        assert_eq!(decoded.source, "fallback");
        assert!(!decoded.personalized);
        assert_eq!(decoded.fallback_tranche, Some("trending".to_string()));
    }
}
