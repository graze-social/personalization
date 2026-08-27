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
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tracing::{debug, error, info, warn};

use crate::algorithm::{FeedOutcome, FeedSuccessConfig, SelectedParams};
use crate::api::cursor::FeedCursor;
use crate::api::fallback::{
    get_blended_posts_with_stats, get_fallback_blend, load_user_liked_uris, BlendedSource,
};
use crate::api::special_posts::{count_injected, inject_special_posts, ItemProvenance};
use crate::api::RequestId;
use crate::audit::{emit_skip_log, should_audit, AuditCollector};
use crate::AppState;
use graze_common::models::{
    FeedContextProvenance, FeedSkeletonResponse, FeedThompsonConfig, PersonalizationParams,
    ProvenanceParams, SkeletonFeedPost, ThompsonSearchSpace,
};
use graze_common::services::special_posts::SpecialPostsResponse;
use graze_common::{hash_did, is_excluded_post_uri, Keys};

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

/// Build the randomized-experiment descriptor from config.
fn graze_api_hash_experiment(config: &crate::config::Config) -> crate::algorithm::HashExperiment {
    crate::algorithm::HashExperiment {
        dimension: config.ab_experiment_dimension.clone(),
        values: config.ab_experiment_values.clone(),
        traffic_pct: config.ab_experiment_traffic_pct,
        salt: config.ab_experiment_salt.clone(),
    }
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
    // Which ranker produced this item, when interleaving is active.
    ranker: Option<String>,
    // Why this response was not personalized, when it was not. Carried into provenance so coverage
    // failures are decomposable in ClickHouse instead of only in log lines.
    fallback_reason: Option<String>,
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
            Some(
                ProvenanceParams::from_selected(
                    meta.params.min_post_likes,
                    meta.params.max_likers_per_post,
                    meta.params.max_total_sources,
                    meta.params.max_algo_checks,
                    meta.params.min_co_likes,
                    meta.params.max_user_likes,
                    meta.params.max_sources_per_post,
                    meta.params.seed_sample_pool,
                    meta.params.corater_decay_pct,
                )
                .with_follow_seed(meta.params.follow_seed_arm),
            ),
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
        ranker,
        fallback_reason,
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

/// Encode a `BlendedSource` as a short tag for the `fsc:` cache.
///
/// The cache previously stored bare URIs, so every item read back from it was relabelled
/// `PostLevelPersonalization` — meaning pages 2+ have always mis-attributed fallback and
/// author-affinity items. Storing the source alongside the URI fixes that, and is a
/// prerequisite for any per-item experiment attribution (which would otherwise be silently
/// wrong on every page after the first).
///
/// Deliberately extensible: the tag is an opaque string, so an experiment arm can later be
/// appended (e.g. `p/walk`) without another cache format change.
fn source_to_tag(source: &BlendedSource) -> String {
    match source {
        BlendedSource::PostLevelPersonalization => "p".to_string(),
        BlendedSource::AuthorAffinity => "a".to_string(),
        BlendedSource::Fallback { tranche } => format!("f:{}", tranche),
    }
}

/// Inverse of [`source_to_tag`].
fn tag_to_source(tag: &str) -> BlendedSource {
    match tag {
        "a" => BlendedSource::AuthorAffinity,
        t if t.starts_with("f:") => BlendedSource::Fallback {
            tranche: t[2..].to_string(),
        },
        // "p", plus anything unrecognised (e.g. a tag written by a newer build).
        _ => BlendedSource::PostLevelPersonalization,
    }
}

/// Separator between the source tag and the compact URI. Cannot occur in a DID or an rkey,
/// which is what makes the legacy (untagged) format unambiguously detectable.
const CACHE_TAG_SEP: char = '|';

/// Encode `(uri, source)` for the `fsc:` cache.
/// Whether this user is in the personalization holdout.
///
/// Stable per user: the same DID always lands in the same arm for a given rate and salt, which is what
/// makes a user-level readout meaningful. Changing the *rate* does move the boundary, so a rate change
/// mid-experiment starts a new experiment — record a fresh `start` in the spec when you change it.
fn is_personalization_holdout_user(user_did: &str, salt: &str, rate: f64) -> bool {
    if rate <= 0.0 {
        return false;
    }
    if rate >= 1.0 {
        return true;
    }
    let h = graze_common::hash_did(&format!("{}|{}", salt, user_did));
    // 16 hex chars of SHA-256. Take a wide slice and map to [0,1) so the comparison is against the
    // configured rate directly rather than a coarse bucket count.
    let v = u64::from_str_radix(&h[..15], 16).unwrap_or(0);
    (v as f64 / 0xFFF_FFFF_FFFF_FFFF_u64 as f64) < rate
}

fn tagged_to_compact(uri: &str, source: &BlendedSource, ranker: Option<&str>) -> String {
    let tag = match ranker {
        Some(r) => format!("{}/{}", source_to_tag(source), r),
        None => source_to_tag(source),
    };
    format!("{}{}{}", tag, CACHE_TAG_SEP, uri_to_compact(uri))
}

/// Decode a `fsc:` cache entry into `(uri, source)`.
///
/// Entries written before this format existed have no separator; they are treated as
/// `PostLevelPersonalization`, which preserves the previous behaviour exactly rather than
/// dropping in-flight caches on deploy.
fn compact_to_tagged(entry: &str) -> (String, BlendedSource, Option<String>) {
    // Split from the RIGHT: the compact URI half (DID + rkey, both base32) can never contain
    // the separator, whereas a tranche name theoretically could. Splitting from the left would
    // let a stray separator in the tag corrupt the URI.
    match entry.rsplit_once(CACHE_TAG_SEP) {
        Some((tag, compact)) => {
            // A `/` in the tag separates the source from the interleaving ranker.
            let (src_tag, ranker) = match tag.split_once('/') {
                Some((s, r)) if !r.is_empty() => (s, Some(r.to_string())),
                _ => (tag, None),
            };
            (compact_to_uri(compact), tag_to_source(src_tag), ranker)
        }
        None => (
            compact_to_uri(entry),
            BlendedSource::PostLevelPersonalization,
            None,
        ),
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

    // Log that this feed was requested (fire-and-forget, unconditional)
    if let Some(ref redis_logger) = state.redis_requests_logger {
        let feed_uri = query.feed.clone();
        let redis_logger = redis_logger.clone();
        tokio::spawn(async move {
            let event = serde_json::json!({
                "feed_uri": feed_uri,
                "requested_at": Utc::now().to_rfc3339(),
            });
            if let Ok(json) = serde_json::to_string(&event) {
                let _ = redis_logger.rpush(Keys::FEED_REQUESTS, &[json]).await;
            }
        });
    }

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
                        // This whole branch is the empty-candidate-pool path; `emit_skip_log`
                        // above records the same reason.
                        Some("no_algo_posts".to_string()),
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
            ranker: None,
            // Same reason the debug! above records: no candidate pool and no fallback either.
            fallback_reason: Some("no_algo_posts".to_string()),
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
    // URI -> ranker for interleaving attribution. Populated on a fresh compute, and recovered
    // from the `fsc:` cache tag on paginated requests.
    let mut ranker_by_uri: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();
    let mut was_personalized = false;
    let mut fallback_reason: Option<&str> = None;
    let mut feed_cache_hit = false;
    let mut response_thompson_meta: Option<ResponseThompsonMeta> = None;
    // The arm is a pure function of the DID, so compute it once for EVERY request — first page or
    // not, cursor or no cursor.
    //
    // Measured leak this replaces: 5 users appeared under both arms across three days. Their
    // mislabelled rows were all at depth 3-6 (never page 1) and some carried `source: personalized`.
    // Cause: the holdout branch was gated on `is_first_page`, so a paginated request whose cursor did
    // not happen to carry `fallback_only` skipped the holdout entirely, read `fsc:`, and served
    // personalized content *labelled as treated*. Two-way contamination — a holdout user both
    // receiving treatment and being counted as treated.
    //
    // Deriving the arm from the DID on every request makes the invariant hold by construction rather
    // than by every caller remembering to thread a flag: a holdout user can neither be served
    // personalized content nor be labelled treated, on any page, with or without a cursor.
    let holdout_user = user_did
        .as_ref()
        .map(|did| {
            is_personalization_holdout_user(
                did,
                &state.config.personalization_holdout_salt,
                state.config.personalization_holdout_rate,
            )
        })
        .unwrap_or(false);
    let mut is_personalization_holdout_for_provenance = holdout_user;
    // Holdout users take the fallback-only path on every page. That path already handles pagination
    // offsets and exclusions correctly, so routing through it removes the duplicate first-page-only
    // branch rather than adding a second one to keep in sync.
    let is_fallback_only = feed_cursor.fallback_only || holdout_user;

    // ═══════════════════════════════════════════════════════════════════
    // Handle fallback-only mode (personalization exhausted or holdout)
    // ═══════════════════════════════════════════════════════════════════
    if is_fallback_only {
        fallback_reason = Some(if holdout_user || feed_cursor.is_personalization_holdout {
            "personalization_holdout"
        } else {
            "personalization_exhausted"
        });
        // `holdout_user` is authoritative; the cursor's copy is only a hint that can be absent on a
        // fresh or truncated cursor, which is exactly how the old leak arose.
        is_personalization_holdout_for_provenance =
            holdout_user || feed_cursor.is_personalization_holdout;
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
        let user_hash = hash_did(did);
        let feed_cache_key = Keys::feed_cache(algo_id, &user_hash);
        let is_first_page = feed_cursor.is_first_page();

        // No holdout branch here: holdout users were routed to the fallback-only path above, before
        // this block, so reaching here already implies the user is in the treated arm. Keeping a
        // second first-page-only check would recreate the leak this removed.
        if state.config.feed_cache_enabled {
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
                            .map(|c| {
                                let (uri, src, ranker) = compact_to_tagged(&c);
                                if let Some(r) = ranker {
                                    ranker_by_uri.insert(uri.clone(), r);
                                }
                                (uri, src)
                            })
                            .collect();
                        // Only claim personalization if the cached page actually contains a
                        // personalized item. Previously this was unconditionally true, which
                        // inflated `was_personalized` on fallback-only cached pages.
                        was_personalized = base_posts_tagged.iter().any(|(_, s)| {
                            matches!(
                                s,
                                BlendedSource::PostLevelPersonalization
                                    | BlendedSource::AuthorAffinity
                            )
                        });
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
            // Check if the user has any like-seed the scorer could actually use.
            //
            // This previously checked only today and yesterday, to cover users who liked just
            // before UTC midnight. But the scorer reads the FULL retention window
            // (`user_likes_retention`, 6 days), so a 2-day gate turned away users whose seed
            // the scorer would have found — reporting `no_user_data` and serving unfiltered
            // fallback instead.
            //
            // Measured on 2,500 sampled daily-active users: the 2-day gate passed 49.2%, while
            // 63.2% had seed somewhere in the 6-day window. **14.0% of all DAU — 22.1% of every
            // seeded user — were being turned away despite usable seed.** That matched the
            // observed `fallback_reason=no_user_data` rate of ~50% exactly.
            //
            // Their seed is thin (median 2 likes), so quality is modest, but the alternative
            // for them is 100% fallback. Cost is one pipelined EXISTS batch — a single round
            // trip regardless of window width — plus a scoring run for the newly admitted
            // users. Window is configurable via USER_DATA_CHECK_DAYS to allow rollback without
            // a rebuild.
            let check_days = state.config.user_data_check_days;
            let user_likes_keys = Keys::user_likes_retention(&user_hash, check_days);
            let user_has_data = state
                .redis
                .exists_any(&user_likes_keys)
                .await
                .unwrap_or(false);

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
                let mut thompson_params: SelectedParams =
                    state.thompson.select_params_with_holdout_and_search_space(
                        algo_id,
                        holdout_override,
                        treatment_override,
                        search_space,
                    );

                // Optionally override one dimension with a user-hash randomized assignment.
                //
                // Thompson picks adaptively, so its arms cannot be compared retrospectively:
                // arm choice tracks bandit state, which tracks time, and engagement varies by
                // time of day. Hash assignment is orthogonal to both and stable per user, giving
                // a genuine randomized experiment. Enrolled requests are excluded from bandit
                // learning (`is_hash_experiment`), and the forced value still flows into the
                // feedContext provenance, so engagement analysis needs no extra plumbing.
                if state.config.ab_experiment_enabled {
                    let exp = graze_api_hash_experiment(&state.config);
                    if let Some(v) = state.thompson.apply_hash_experiment(
                        &mut thompson_params,
                        user_did.as_deref().unwrap_or(""),
                        &exp,
                    ) {
                        debug!(
                            algo_id,
                            dimension = %exp.dimension,
                            value = v,
                            "ab_experiment_assigned"
                        );
                    }
                }

                // Follow-seed experiment: a per-user on/off assignment, orthogonal to both the
                // bandit experiment above and the holdout (its own salt).
                //
                // The holdout is never enrolled -- holdout users receive no personalization at all,
                // so granting them a seed capability would be meaningless and would blur two arms.
                if state.config.follow_seed_experiment_enabled && !thompson_params.is_holdout {
                    let fs_exp = crate::algorithm::FollowSeedExperiment {
                        enabled: true,
                        traffic_pct: state.config.follow_seed_experiment_traffic_pct,
                        salt: state.config.follow_seed_experiment_salt.clone(),
                    };
                    thompson_params.follow_seed_arm =
                        fs_exp.assign(user_did.as_deref().unwrap_or(""));
                    if let Some(arm) = thompson_params.follow_seed_arm {
                        debug!(algo_id, arm, "follow_seed_experiment_assigned");
                    }
                }

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

                // Try personalization with Thompson-selected parameters
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
                        // Capture scoring stats for Thompson learning
                        let posts_checked = result.meta.posts_checked.unwrap_or(0);
                        let posts_scored = result.meta.total_scored.unwrap_or(0);
                        let _scoring_time_ms = result.meta.scoring_time_ms.unwrap_or(0.0);

                        // Collect post IDs that need conversion to URIs
                        let posts_to_convert: Vec<String> = result
                            .posts
                            .iter()
                            .filter(|p| p.uri.is_empty() && !p.post_id.is_empty())
                            .map(|p| p.post_id.clone())
                            .collect();

                        // Batch lookup URIs from interner
                        let id_to_uri = state
                            .interner
                            .get_uris_batch(&posts_to_convert)
                            .await
                            .unwrap_or_default();

                        // Convert posts to URIs and track for audit
                        let personalized_uris: Vec<String> = result
                            .posts
                            .into_iter()
                            .filter_map(|p| {
                                let uri = if !p.uri.is_empty() {
                                    Some(p.uri.clone())
                                } else {
                                    id_to_uri.get(&p.post_id).cloned()
                                };

                                // Track personalized posts in audit
                                if let Some(ref uri) = uri {
                                    if let Some(ref mut a) = audit {
                                        a.add_personalized_post(&p.post_id, uri, p.score);
                                    }
                                    // Keep the interleaving attribution keyed by URI, which is
                                    // what the rest of this handler works in.
                                    if let Some(ref r) = p.ranker {
                                        ranker_by_uri.insert(uri.clone(), r.clone());
                                    }
                                }

                                uri
                            })
                            .collect();

                        // Apply progressive blending with appropriate limit
                        let blend_limit = if is_first_page && state.config.feed_cache_enabled {
                            // Blend entire batch for caching
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
                            // Store the per-item source so pagination can attribute correctly.
                            let compact_posts: Vec<String> = blend_result
                                .posts_with_source
                                .iter()
                                .map(|(u, src)| {
                                    tagged_to_compact(
                                        u,
                                        src,
                                        ranker_by_uri.get(u).map(|r| r.as_str()),
                                    )
                                })
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

                        // Record Thompson observation (only for non-holdout requests)
                        let total_response_time_ms = request_start.elapsed().as_secs_f64() * 1000.0;
                        let outcome = FeedOutcome {
                            total_posts: blend_result.posts.len().min(limit),
                            personalized_posts: blend_result.personalized_count,
                            author_affinity_posts: blend_result.author_affinity_count,
                            fallback_posts: blend_result.fallback_count,
                            posts_checked,
                            posts_scored,
                            colikers_used: 0, // TODO: track at co-liker level
                            response_time_ms: total_response_time_ms,
                        };

                        let success_config = feed_config
                            .as_ref()
                            .and_then(|c| c.success_criteria.as_ref())
                            .or(global_success_criteria.as_ref())
                            .unwrap_or(&FeedSuccessConfig::default())
                            .clone();
                        let (success, details) = outcome.evaluate(&success_config);

                        // Log Thompson learning outcome
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

                        // Record observation for learning (skipped for holdout group).
                        // Skip request-time recording when interaction_weights is configured -
                        // we only learn from likes (interaction path), otherwise fast-response
                        // signal drowns out the like signal (~100x more common).
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
                        let thompson_params: SelectedParams =
                            state.thompson.select_params_with_holdout_and_search_space(
                                algo_id,
                                holdout_override,
                                treatment_override,
                                search_space,
                            );
                        state
                            .thompson
                            .record_observation(algo_id, &thompson_params, false);

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

    let (mut final_with_provenance, updated_cursor) = inject_special_posts(
        base_posts_tagged.clone(),
        &special_posts,
        &feed_cursor,
        limit,
    );

    if !state.config.exclusion_dids.is_empty() {
        final_with_provenance
            .retain(|(uri, _)| !is_excluded_post_uri(uri, state.config.exclusion_dids.as_ref()));
    }

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

    let feed: Vec<SkeletonFeedPost> = final_with_provenance
        .iter()
        .enumerate()
        .map(|(depth, (uri, prov))| {
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
                ranker_by_uri.get(uri).cloned(),
                fallback_reason.map(str::to_string),
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

    /// The cache previously stored bare URIs, so every item read back was relabelled
    /// `PostLevelPersonalization` — silently mis-attributing fallback and author-affinity
    /// items on every page after the first.
    #[test]
    fn cache_tag_roundtrips_every_source() {
        let uri = "at://did:plc:abc123/app.bsky.feed.post/3ldefgh456";
        for src in [
            BlendedSource::PostLevelPersonalization,
            BlendedSource::AuthorAffinity,
            BlendedSource::Fallback {
                tranche: "trending".to_string(),
            },
            BlendedSource::Fallback {
                tranche: "discovery".to_string(),
            },
        ] {
            let encoded = tagged_to_compact(uri, &src, None);
            let (got_uri, got_src, got_ranker) = compact_to_tagged(&encoded);
            assert_eq!(got_ranker, None);
            assert_eq!(got_uri, uri);
            assert_eq!(source_to_tag(&got_src), source_to_tag(&src));
        }
    }

    /// Entries written before the tagged format existed must still decode, so deploying does
    /// not garble in-flight caches.
    #[test]
    fn legacy_untagged_cache_entries_still_decode() {
        let legacy = uri_to_compact("at://did:plc:abc123/app.bsky.feed.post/3ldefgh456");
        assert!(!legacy.contains(CACHE_TAG_SEP));
        let (uri, src, ranker) = compact_to_tagged(&legacy);
        assert_eq!(ranker, None);
        assert_eq!(uri, "at://did:plc:abc123/app.bsky.feed.post/3ldefgh456");
        assert!(matches!(src, BlendedSource::PostLevelPersonalization));
    }

    /// A tag written by a newer build must not panic or lose the URI.
    #[test]
    fn unknown_tag_degrades_to_personalized_without_losing_the_uri() {
        let entry = format!(
            "p/somefutureranker{}{}",
            CACHE_TAG_SEP,
            uri_to_compact("at://did:plc:abc123/app.bsky.feed.post/3ldefgh456")
        );
        let (uri, src, _) = compact_to_tagged(&entry);
        assert_eq!(uri, "at://did:plc:abc123/app.bsky.feed.post/3ldefgh456");
        assert!(matches!(src, BlendedSource::PostLevelPersonalization));
    }

    /// A fallback tranche containing the separator must not corrupt the URI half.
    #[test]
    fn tranche_with_separator_does_not_corrupt_the_uri() {
        let uri = "at://did:plc:abc123/app.bsky.feed.post/3ldefgh456";
        let src = BlendedSource::Fallback {
            tranche: format!("odd{}name", CACHE_TAG_SEP),
        };
        let (got_uri, _, _) = compact_to_tagged(&tagged_to_compact(uri, &src, None));
        assert_eq!(got_uri, uri, "uri must survive a separator in the tranche");
    }

    /// Interleaving attribution must survive pagination, which is served from the `fsc:` cache.
    /// Without this the ranker credit silently vanishes after page 1 and the experiment reads as
    /// having no effect.
    #[test]
    fn cache_tag_roundtrips_the_interleaving_ranker() {
        let uri = "at://did:plc:abc123/app.bsky.feed.post/3ldefgh456";
        let src = BlendedSource::PostLevelPersonalization;
        let encoded = tagged_to_compact(uri, &src, Some("sampled_walk"));
        let (got_uri, got_src, got_ranker) = compact_to_tagged(&encoded);
        assert_eq!(got_uri, uri);
        assert!(matches!(got_src, BlendedSource::PostLevelPersonalization));
        assert_eq!(got_ranker.as_deref(), Some("sampled_walk"));
    }

    /// A fallback item can also be attributed, and the tranche must survive alongside the ranker.
    #[test]
    fn cache_tag_keeps_tranche_and_ranker_together() {
        let uri = "at://did:plc:abc123/app.bsky.feed.post/3ldefgh456";
        let src = BlendedSource::Fallback {
            tranche: "trending".to_string(),
        };
        let (got_uri, got_src, got_ranker) =
            compact_to_tagged(&tagged_to_compact(uri, &src, Some("item_item")));
        assert_eq!(got_uri, uri);
        match got_src {
            BlendedSource::Fallback { tranche } => assert_eq!(tranche, "trending"),
            other => panic!("expected fallback, got {:?}", other),
        }
        assert_eq!(got_ranker.as_deref(), Some("item_item"));
    }

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
}

#[cfg(test)]
mod holdout_assignment_tests {
    use super::*;

    fn dids(n: usize) -> Vec<String> {
        (0..n)
            .map(|i| format!("did:plc:testuser{:06}", i))
            .collect()
    }

    #[test]
    fn assignment_is_stable_for_a_user_across_calls() {
        // The property the whole readout rests on. A per-request coin flip put active users in BOTH
        // arms simultaneously, which attenuates any real effect toward zero — the first readout's
        // +5.4% (p=0.91) is what that looks like whether or not personalization works.
        for did in dids(50) {
            let first = is_personalization_holdout_user(&did, "pholdout-v1", 0.2);
            for _ in 0..20 {
                assert_eq!(
                    is_personalization_holdout_user(&did, "pholdout-v1", 0.2),
                    first,
                    "assignment must not vary between requests for {did}"
                );
            }
        }
    }

    #[test]
    fn observed_rate_tracks_the_configured_rate() {
        for rate in [0.05_f64, 0.2, 0.5] {
            let n = 4000;
            let held = dids(n)
                .iter()
                .filter(|d| is_personalization_holdout_user(d, "pholdout-v1", rate))
                .count();
            let observed = held as f64 / n as f64;
            assert!(
                (observed - rate).abs() < 0.03,
                "rate {rate} produced {observed:.4} over {n} users"
            );
        }
    }

    #[test]
    fn zero_rate_holds_nobody_and_full_rate_holds_everybody() {
        for did in dids(200) {
            assert!(!is_personalization_holdout_user(&did, "s", 0.0));
            assert!(is_personalization_holdout_user(&did, "s", 1.0));
        }
    }

    #[test]
    fn holdout_arm_does_not_depend_on_page_or_cursor() {
        // The leak this replaces: the arm was decided only on first pages, so a paginated request
        // whose cursor lacked `fallback_only` skipped the holdout, read `fsc:`, and served
        // personalized content *labelled treated*. Measured: 5 users under both arms over three days,
        // all mislabelled rows at depth 3-6, some with `source: personalized`.
        //
        // The arm is now derived from the DID alone. This test states that contract directly: nothing
        // about the request — page number, cursor contents, request ordering — may enter the decision.
        for did in dids(200) {
            let arm = is_personalization_holdout_user(&did, "pholdout-v1", 0.2);
            // Whatever a caller does between requests, the same inputs must give the same arm.
            for _ in 0..5 {
                assert_eq!(
                    is_personalization_holdout_user(&did, "pholdout-v1", 0.2),
                    arm,
                    "arm for {did} must be a function of the DID, salt and rate only"
                );
            }
        }
    }

    #[test]
    fn holdout_membership_is_monotone_in_the_rate() {
        // Raising the rate must only ever ADD users to the holdout, never swap them out. Otherwise a
        // rate change would reassign existing members and silently invalidate accrued data beyond the
        // documented "a rate change starts a new experiment" caveat.
        let users = dids(1000);
        for did in &users {
            if is_personalization_holdout_user(did, "pholdout-v1", 0.05) {
                assert!(
                    is_personalization_holdout_user(did, "pholdout-v1", 0.20),
                    "{did} was held out at 5% but not at 20%; membership is not monotone"
                );
            }
        }
    }

    #[test]
    fn changing_the_salt_reshuffles_membership() {
        // Documented consequence: a salt change starts a new experiment rather than continuing one.
        let users = dids(500);
        let moved = users
            .iter()
            .filter(|d| {
                is_personalization_holdout_user(d, "pholdout-v1", 0.2)
                    != is_personalization_holdout_user(d, "pholdout-v2", 0.2)
            })
            .count();
        assert!(
            moved > 50,
            "only {moved} of 500 users moved; salt is not reshuffling"
        );
    }

    #[test]
    fn holdout_is_orthogonal_to_the_interleaving_and_thompson_salts() {
        // Sharing a salt would correlate holdout membership with bandit arms, so a holdout readout
        // would partly be measuring the bandit. Assert the assignments are near-independent.
        let users = dids(2000);
        let mut both = 0usize;
        let mut held = 0usize;
        let mut other = 0usize;
        for d in &users {
            let h = is_personalization_holdout_user(d, "pholdout-v1", 0.5);
            // Same construction the interleaving/Thompson assignment uses, different salt.
            let x = graze_common::hash_did(&format!("interleave-salt|{}", d));
            let o = u64::from_str_radix(&x[..15], 16)
                .unwrap_or(0)
                .is_multiple_of(2);
            if h {
                held += 1;
            }
            if o {
                other += 1;
            }
            if h && o {
                both += 1;
            }
        }
        let expected = held as f64 * other as f64 / users.len() as f64;
        let ratio = both as f64 / expected.max(1.0);
        assert!(
            (0.85..1.15).contains(&ratio),
            "holdout and interleaving assignments are correlated: joint/expected = {ratio:.3}"
        );
    }
}
