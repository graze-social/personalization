//! Admin and management API endpoints.

use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Extension, Json,
};
use serde::Deserialize;
use serde_json::json;
use tracing::{debug, error, info, warn};

use crate::api::RequestId;
use crate::audit::{AuditCollector, FeedAuditRecord};
use crate::AppState;
use graze_common::models::{
    FeedSuccessConfig, FeedThompsonConfig, PersonalizeRequest, ThompsonSearchSpace,
};
use graze_common::services::special_posts::{
    SpecialPost, SpecialPostsResponse, SponsoredPost,
};
use graze_common::{hash_did, Keys};

/// GET /v1/feeds
///
/// List all registered feeds and their algorithm mappings.
pub async fn list_feeds(State(state): State<Arc<AppState>>) -> Response {
    match state.redis.hgetall(Keys::SUPPORTED_FEEDS).await {
        Ok(feeds_map) => {
            let feeds: std::collections::HashMap<String, i32> = feeds_map
                .into_iter()
                .filter_map(|(uri, algo_id_str)| {
                    algo_id_str.parse::<i32>().ok().map(|id| (uri, id))
                })
                .collect();

            (StatusCode::OK, Json(json!({ "feeds": feeds }))).into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({
                "error": "ListFailed",
                "message": format!("Failed to list feeds: {}", e)
            })),
        )
            .into_response(),
    }
}

/// Request body for POST /v1/feeds (register feed).
#[derive(Debug, Deserialize)]
pub struct RegisterFeedBody {
    /// AT-URI of the feed generator (e.g. at://did:plc:.../app.bsky.feed.generator/my-feed).
    pub feed_uri: String,
    /// Algorithm ID for this feed (one-to-one with feed_uri).
    pub algo_id: i32,
}

/// POST /v1/feeds
///
/// Register a feed as active (one-to-one feed_uri ↔ algo_id). Idempotent if already
/// registered for this algo_id. Persists to supported_feeds, seeds feed:access for
/// rolling sync, and enqueues one candidate sync.
pub async fn register_feed(
    State(state): State<Arc<AppState>>,
    Json(body): Json<RegisterFeedBody>,
) -> Response {
    if state.config.read_only_mode {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "error": "ReadOnlyMode",
                "message": "Write operations disabled in read-only mode"
            })),
        )
            .into_response();
    }

    let feed_uri = body.feed_uri.trim();
    if feed_uri.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "InvalidRequest",
                "message": "feed_uri is required and must be non-empty"
            })),
        )
            .into_response();
    }

    let algo_id = body.algo_id;
    let algo_id_str = algo_id.to_string();

    // One-to-one: if this algo_id was already registered with a different feed_uri, remove the old mapping
    if let Ok(Some(old_uri)) = state
        .redis
        .hget(Keys::ALGO_ID_TO_FEED_URI, &algo_id_str)
        .await
    {
        if old_uri != feed_uri {
            let _ = state.redis.hdel(Keys::SUPPORTED_FEEDS, &old_uri).await;
            let _ = state.redis.hdel(Keys::ALGO_ID_TO_FEED_URI, &algo_id_str).await;
        }
    }

    if let Err(e) = state
        .redis
        .hset(Keys::SUPPORTED_FEEDS, feed_uri, &algo_id_str)
        .await
    {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({
                "error": "RedisError",
                "message": format!("Failed to register feed: {}", e)
            })),
        )
            .into_response();
    }
    if let Err(e) = state
        .redis
        .hset(Keys::ALGO_ID_TO_FEED_URI, &algo_id_str, feed_uri)
        .await
    {
        let _ = state.redis.hdel(Keys::SUPPORTED_FEEDS, feed_uri).await;
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({
                "error": "RedisError",
                "message": format!("Failed to set algo_id mapping: {}", e)
            })),
        )
            .into_response();
    }
    if let Err(e) = state
        .redis
        .hset(Keys::FEED_URI_TO_ALGO_WRITES, feed_uri, &algo_id_str)
        .await
    {
        warn!(algo_id, feed_uri = %feed_uri, error = %e, "failed to set feed_uri_to_algo_writes");
        // non-fatal: supported feed is still registered
    }

    // Seed feed:access so rolling sync picks this feed up until deregister
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    if let Err(e) = state
        .redis
        .hset(Keys::FEED_ACCESS, &algo_id_str, &now.to_string())
        .await
    {
        warn!(algo_id, error = %e, "failed_to_seed_feed_access");
    }

    // Enqueue one sync so candidate pool and fallback tranches are populated quickly
    let pending_key = "pending:syncs";
    match state.redis.sadd(pending_key, std::slice::from_ref(&algo_id_str)).await {
        Ok(added) if added > 0 => {
            if let Err(e) = state.redis.lpush(Keys::SYNC_QUEUE, &algo_id_str).await {
                warn!(algo_id, error = %e, "failed_to_queue_sync");
            } else {
                debug!(algo_id, "sync_queued_on_register");
            }
        }
        Ok(_) => {
            debug!(algo_id, "sync_already_pending");
        }
        Err(e) => {
            warn!(algo_id, error = %e, "failed_to_check_pending_syncs");
        }
    }

    info!(algo_id, feed_uri = %feed_uri, "feed_registered");
    (
        StatusCode::CREATED,
        Json(json!({ "ok": true, "registered": true })),
    )
        .into_response()
}

/// DELETE /v1/feeds/:algo_id
///
/// Deregister a feed. Removes it from supported_feeds and feed:access (stops serving
/// and stops rolling sync). Idempotent if not registered. Does not remove from
/// feed_uri_to_algo_writes so late-arriving interaction events are still accepted.
pub async fn deregister_feed(
    State(state): State<Arc<AppState>>,
    Path(algo_id): Path<i32>,
) -> Response {
    if state.config.read_only_mode {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "error": "ReadOnlyMode",
                "message": "Write operations disabled in read-only mode"
            })),
        )
            .into_response();
    }

    let algo_id_str = algo_id.to_string();

    let feed_uri = match state
        .redis
        .hget(Keys::ALGO_ID_TO_FEED_URI, &algo_id_str)
        .await
    {
        Ok(Some(uri)) => uri,
        Ok(None) => {
            // Fallback: feed may have been registered before algo_id_to_feed_uri existed
            let supported = match state.redis.hgetall(Keys::SUPPORTED_FEEDS).await {
                Ok(m) => m,
                Err(e) => {
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(json!({
                            "error": "RedisError",
                            "message": format!("Failed to list feeds: {}", e)
                        })),
                    )
                        .into_response()
                }
            };
            let found = supported
                .into_iter()
                .find(|(_, v)| v == &algo_id_str)
                .map(|(uri, _)| uri);
            match found {
                Some(uri) => uri,
                None => {
                    // Idempotent: already not registered
                    return (StatusCode::OK, Json(json!({ "ok": true }))).into_response();
                }
            }
        }
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "error": "RedisError",
                    "message": format!("Failed to lookup feed for deregister: {}", e)
                })),
            )
                .into_response()
        }
    };

    let _ = state.redis.hdel(Keys::SUPPORTED_FEEDS, &feed_uri).await;
    let _ = state.redis.hdel(Keys::ALGO_ID_TO_FEED_URI, &algo_id_str).await;
    let _ = state.redis.hdel(Keys::FEED_ACCESS, &algo_id_str).await;
    // Intentionally do not remove from FEED_URI_TO_ALGO_WRITES so interaction writes still work

    info!(algo_id, feed_uri = %feed_uri, "feed_deregistered");
    StatusCode::NO_CONTENT.into_response()
}

/// GET /v1/thompson/stats
///
/// Get Thompson Sampling learner statistics.
pub async fn thompson_stats(State(state): State<Arc<AppState>>) -> Response {
    let stats = state.thompson.get_stats();

    (
        StatusCode::OK,
        Json(json!({
            "enabled": true,
            "stats": stats,
            "success_criteria": {
                "min_personalization_ratio": 0.60,
                "min_posts_checked": 50,
                "min_posts_scored": 10,
                "max_response_time_ms": 500.0
            }
        })),
    )
        .into_response()
}

/// GET /v1/feeds/:algo_id/thompson-config
///
/// Get per-feed Thompson interaction config. Returns default if not set.
pub async fn get_feed_thompson_config(
    State(state): State<Arc<AppState>>,
    Path(algo_id): Path<i32>,
) -> Response {
    let key = Keys::feed_thompson_config(algo_id);
    match state.redis.get_string(&key).await {
        Ok(Some(json_str)) => match serde_json::from_str::<FeedThompsonConfig>(&json_str) {
            Ok(config) => (StatusCode::OK, Json(config)).into_response(),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "error": "InvalidConfig",
                    "message": format!("Failed to parse config: {}", e)
                })),
            )
                .into_response(),
        },
        Ok(None) => (StatusCode::OK, Json(FeedThompsonConfig::default())).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({
                "error": "RedisError",
                "message": format!("Failed to load config: {}", e)
            })),
        )
            .into_response(),
    }
}

/// PUT /v1/feeds/:algo_id/thompson-config
///
/// Set per-feed Thompson interaction config.
pub async fn put_feed_thompson_config(
    State(state): State<Arc<AppState>>,
    Path(algo_id): Path<i32>,
    Json(config): Json<FeedThompsonConfig>,
) -> Response {
    if state.config.read_only_mode {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "error": "ReadOnlyMode",
                "message": "Write operations disabled in read-only mode"
            })),
        )
            .into_response();
    }

    let key = Keys::feed_thompson_config(algo_id);
    let json_str = match serde_json::to_string(&config) {
        Ok(s) => s,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "error": "InvalidConfig",
                    "message": format!("Invalid config: {}", e)
                })),
            )
                .into_response()
        }
    };

    // Store with 10-year TTL (config is effectively persistent)
    match state.redis.set_ex(&key, &json_str, 315_360_000).await {
        Ok(()) => {
            // Clear bandits on config update so learned state matches new search/success config
            state.thompson.clear_bandits_for_algo(algo_id);
            info!(algo_id, "feed_thompson_config_updated");
            (
                StatusCode::OK,
                Json(json!({
                    "success": true,
                    "algo_id": algo_id
                })),
            )
                .into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({
                "error": "RedisError",
                "message": format!("Failed to save config: {}", e)
            })),
        )
            .into_response(),
    }
}

/// GET /v1/thompson/search-space
///
/// Get global Thompson search space. Returns default if not set.
pub async fn get_thompson_search_space(State(state): State<Arc<AppState>>) -> Response {
    match state
        .redis
        .get_string(Keys::thompson_search_space())
        .await
    {
        Ok(Some(json_str)) => match serde_json::from_str::<ThompsonSearchSpace>(&json_str) {
            Ok(config) => (StatusCode::OK, Json(config)).into_response(),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "error": "InvalidConfig",
                    "message": format!("Failed to parse config: {}", e)
                })),
            )
                .into_response(),
        },
        Ok(None) => (StatusCode::OK, Json(ThompsonSearchSpace::default())).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({
                "error": "RedisError",
                "message": format!("Failed to load config: {}", e)
            })),
        )
            .into_response(),
    }
}

/// PUT /v1/thompson/search-space
///
/// Set global Thompson search space. Clears bandits for algos that use global config.
pub async fn put_thompson_search_space(
    State(state): State<Arc<AppState>>,
    Json(config): Json<ThompsonSearchSpace>,
) -> Response {
    if state.config.read_only_mode {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "error": "ReadOnlyMode",
                "message": "Write operations disabled in read-only mode"
            })),
        )
            .into_response();
    }

    // Validate: all option arrays non-empty, default in options
    let validate = |opts: &[usize], default: usize, name: &str| {
        if opts.is_empty() {
            return Err(format!("{}_options cannot be empty", name));
        }
        if !opts.contains(&default) {
            return Err(format!("{}_default must be in {}_options", name, name));
        }
        Ok(())
    };
    if let Err(e) = validate(&config.min_likes_options, config.min_likes_default, "min_likes") {
        return (StatusCode::BAD_REQUEST, Json(json!({ "error": "ValidationError", "message": e })))
            .into_response();
    }
    if let Err(e) = validate(&config.max_likers_options, config.max_likers_default, "max_likers") {
        return (StatusCode::BAD_REQUEST, Json(json!({ "error": "ValidationError", "message": e })))
            .into_response();
    }
    if let Err(e) = validate(&config.max_sources_options, config.max_sources_default, "max_sources") {
        return (StatusCode::BAD_REQUEST, Json(json!({ "error": "ValidationError", "message": e })))
            .into_response();
    }
    if let Err(e) = validate(&config.max_checks_options, config.max_checks_default, "max_checks") {
        return (StatusCode::BAD_REQUEST, Json(json!({ "error": "ValidationError", "message": e })))
            .into_response();
    }
    if let Err(e) = validate(&config.min_colikes_options, config.min_colikes_default, "min_colikes") {
        return (StatusCode::BAD_REQUEST, Json(json!({ "error": "ValidationError", "message": e })))
            .into_response();
    }
    if let Err(e) = validate(&config.max_user_likes_options, config.max_user_likes_default, "max_user_likes") {
        return (StatusCode::BAD_REQUEST, Json(json!({ "error": "ValidationError", "message": e })))
            .into_response();
    }
    if let Err(e) = validate(&config.max_sources_per_post_options, config.max_sources_per_post_default, "max_sources_per_post") {
        return (StatusCode::BAD_REQUEST, Json(json!({ "error": "ValidationError", "message": e })))
            .into_response();
    }
    if let Err(e) = validate(&config.seed_pool_options, config.seed_pool_default, "seed_pool") {
        return (StatusCode::BAD_REQUEST, Json(json!({ "error": "ValidationError", "message": e })))
            .into_response();
    }
    if let Err(e) = validate(&config.corater_decay_options, config.corater_decay_default, "corater_decay") {
        return (StatusCode::BAD_REQUEST, Json(json!({ "error": "ValidationError", "message": e })))
            .into_response();
    }

    let json_str = match serde_json::to_string(&config) {
        Ok(s) => s,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "error": "InvalidConfig",
                    "message": format!("Invalid config: {}", e)
                })),
            )
                .into_response()
        }
    };

    match state
        .redis
        .set_ex(Keys::thompson_search_space(), &json_str, 315_360_000)
        .await
    {
        Ok(()) => {
            // Collect algo_ids that have algo-specific search_space
            let mut algo_ids_with_algo_config = Vec::new();
            let mut cursor = 0u64;
            loop {
                let (new_cursor, keys) = match state
                    .redis
                    .scan(cursor, "feed_thompson_config:*", 100)
                    .await
                {
                    Ok((c, k)) => (c, k),
                    Err(_) => break,
                };
                for key in keys {
                    if let Some(suffix) = key.strip_prefix("feed_thompson_config:") {
                        if let Ok(algo_id) = suffix.parse::<i32>() {
                            if let Ok(Some(json_str)) = state.redis.get_string(&key).await {
                                if let Ok(cfg) = serde_json::from_str::<FeedThompsonConfig>(&json_str) {
                                    if cfg.search_space.is_some() {
                                        algo_ids_with_algo_config.push(algo_id);
                                    }
                                }
                            }
                        }
                    }
                }
                cursor = new_cursor;
                if cursor == 0 {
                    break;
                }
            }
            state
                .thompson
                .clear_bandits_for_global_use(&algo_ids_with_algo_config);
            info!("thompson_search_space_updated");
            (
                StatusCode::OK,
                Json(json!({ "success": true })),
            )
                .into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({
                "error": "RedisError",
                "message": format!("Failed to save config: {}", e)
            })),
        )
            .into_response(),
    }
}

/// GET /v1/thompson/success-criteria
///
/// Get global Thompson success criteria. Returns default if not set.
pub async fn get_thompson_success_criteria(State(state): State<Arc<AppState>>) -> Response {
    match state
        .redis
        .get_string(Keys::thompson_success_criteria())
        .await
    {
        Ok(Some(json_str)) => match serde_json::from_str::<FeedSuccessConfig>(&json_str) {
            Ok(config) => (StatusCode::OK, Json(config)).into_response(),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "error": "InvalidConfig",
                    "message": format!("Failed to parse config: {}", e)
                })),
            )
                .into_response(),
        },
        Ok(None) => (StatusCode::OK, Json(FeedSuccessConfig::default())).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({
                "error": "RedisError",
                "message": format!("Failed to load config: {}", e)
            })),
        )
            .into_response(),
    }
}

/// PUT /v1/thompson/success-criteria
///
/// Set global Thompson success criteria.
pub async fn put_thompson_success_criteria(
    State(state): State<Arc<AppState>>,
    Json(config): Json<FeedSuccessConfig>,
) -> Response {
    if state.config.read_only_mode {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "error": "ReadOnlyMode",
                "message": "Write operations disabled in read-only mode"
            })),
        )
            .into_response();
    }

    let json_str = match serde_json::to_string(&config) {
        Ok(s) => s,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "error": "InvalidConfig",
                    "message": format!("Invalid config: {}", e)
                })),
            )
                .into_response()
        }
    };

    match state
        .redis
        .set_ex(Keys::thompson_success_criteria(), &json_str, 315_360_000)
        .await
    {
        Ok(()) => {
            info!("thompson_success_criteria_updated");
            (
                StatusCode::OK,
                Json(json!({ "success": true })),
            )
                .into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({
                "error": "RedisError",
                "message": format!("Failed to save config: {}", e)
            })),
        )
            .into_response(),
    }
}

/// GET /v1/thompson/algorithm/:algo_id
///
/// Get Thompson learned parameters for a specific algorithm.
pub async fn thompson_algorithm(
    State(state): State<Arc<AppState>>,
    Path(algo_id): Path<i32>,
) -> Response {
    // Get a sample of what Thompson would select for this algo
    let selected = state.thompson.select_params(algo_id);
    let stats = state.thompson.get_stats();
    let bandit_stats = state.thompson.get_bandit_stats(algo_id);

    (
        StatusCode::OK,
        Json(json!({
            "algo_id": algo_id,
            "enabled": true,
            "sample_params": {
                "min_post_likes": selected.min_post_likes,
                "max_likers_per_post": selected.max_likers_per_post,
                "max_total_sources": selected.max_total_sources,
                "max_algo_checks": selected.max_algo_checks,
                "min_co_likes": selected.min_co_likes,
                "max_user_likes": selected.max_user_likes,
                "max_sources_per_post": selected.max_sources_per_post,
                "is_holdout": selected.is_holdout,
                "is_exploration": selected.is_exploration
            },
            "stats": stats,
            "bandits": bandit_stats
        })),
    )
        .into_response()
}

// ═══════════════════════════════════════════════════════════════════════════════
// Audit User Management Endpoints
// ═══════════════════════════════════════════════════════════════════════════════

/// Request body for adding audit users.
#[derive(Debug, Deserialize)]
pub struct AuditUsersRequest {
    /// List of user DIDs to add or remove from audit.
    pub dids: Vec<String>,
}

/// POST /v1/audit/users
///
/// Add user DIDs to the audit set for detailed feed logging.
pub async fn add_audit_users(
    State(state): State<Arc<AppState>>,
    Json(request): Json<AuditUsersRequest>,
) -> Response {
    if state.config.read_only_mode {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "error": "ReadOnlyMode",
                "message": "Write operations disabled in read-only mode"
            })),
        )
            .into_response();
    }

    if request.dids.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "InvalidRequest",
                "message": "No DIDs provided"
            })),
        )
            .into_response();
    }

    // Hash the DIDs and add to the audit set
    let hashed_dids: Vec<String> = request.dids.iter().map(|d| hash_did(d)).collect();

    match state.redis.sadd(Keys::audit_users(), &hashed_dids).await {
        Ok(added) => {
            info!(dids_count = request.dids.len(), added, "audit_users_added");
            (
                StatusCode::OK,
                Json(json!({
                    "success": true,
                    "added": added,
                    "total_requested": request.dids.len()
                })),
            )
                .into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({
                "error": "AddAuditUsersFailed",
                "message": format!("Failed to add audit users: {}", e)
            })),
        )
            .into_response(),
    }
}

/// DELETE /v1/audit/users
///
/// Remove user DIDs from the audit set.
pub async fn remove_audit_users(
    State(state): State<Arc<AppState>>,
    Json(request): Json<AuditUsersRequest>,
) -> Response {
    if state.config.read_only_mode {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "error": "ReadOnlyMode",
                "message": "Write operations disabled in read-only mode"
            })),
        )
            .into_response();
    }

    if request.dids.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "InvalidRequest",
                "message": "No DIDs provided"
            })),
        )
            .into_response();
    }

    // Hash the DIDs and remove from the audit set
    let hashed_dids: Vec<String> = request.dids.iter().map(|d| hash_did(d)).collect();

    match state.redis.srem(Keys::audit_users(), &hashed_dids).await {
        Ok(removed) => {
            info!(
                dids_count = request.dids.len(),
                removed, "audit_users_removed"
            );
            (
                StatusCode::OK,
                Json(json!({
                    "success": true,
                    "removed": removed,
                    "total_requested": request.dids.len()
                })),
            )
                .into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({
                "error": "RemoveAuditUsersFailed",
                "message": format!("Failed to remove audit users: {}", e)
            })),
        )
            .into_response(),
    }
}

/// GET /v1/audit/users
///
/// List all user hashes currently in the audit set.
pub async fn list_audit_users(State(state): State<Arc<AppState>>) -> Response {
    match state.redis.smembers(Keys::audit_users()).await {
        Ok(user_hashes) => (
            StatusCode::OK,
            Json(json!({
                "count": user_hashes.len(),
                "user_hashes": user_hashes,
                "note": "User hashes are shown instead of DIDs for privacy"
            })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({
                "error": "ListAuditUsersFailed",
                "message": format!("Failed to list audit users: {}", e)
            })),
        )
            .into_response(),
    }
}

/// GET /v1/audit/status
///
/// Get current audit configuration status.
pub async fn audit_status(State(state): State<Arc<AppState>>) -> Response {
    let audit_user_count = state.redis.scard(Keys::audit_users()).await.unwrap_or(0);

    (
        StatusCode::OK,
        Json(json!({
            "enabled": state.config.audit_enabled,
            "all_users": state.config.audit_all_users,
            "sample_rate": state.config.audit_sample_rate,
            "log_full_breakdown": state.config.audit_log_full_breakdown,
            "max_contributors": state.config.audit_max_contributors,
            "per_user_audit_count": audit_user_count
        })),
    )
        .into_response()
}

/// POST /v1/audit/personalize
///
/// Run personalization for a given user and algorithm and return the full audit
/// record in the response: params used, each post with score and scoring breakdown,
/// blending stats, and timing. Use this to inspect exactly what this user would
/// get at this moment in time.
pub async fn audit_personalize(
    State(state): State<Arc<AppState>>,
    Extension(request_id): Extension<RequestId>,
    Json(request): Json<PersonalizeRequest>,
) -> Response {
    let request_id = request_id.to_string();
    let user_hash = hash_did(&request.user_did);
    let preset = request.params.as_ref().and_then(|p| p.preset.as_deref());

    info!(
        request_id = %request_id,
        user_did = %request.user_did,
        algo_id = request.algo_id,
        limit = request.limit,
        "audit_personalize_request"
    );

    let request_start = std::time::Instant::now();
    let mut audit = AuditCollector::new(
        request_id,
        user_hash,
        request.algo_id,
        state.config.audit_max_contributors,
        true, // always full breakdown for audit endpoint
    );

    match state
        .algorithm
        .personalize_with_audit(
            &request.user_did,
            request.algo_id,
            request.limit,
            request.cursor.as_deref(),
            request.params.as_ref(),
            preset,
            Some(&mut audit),
        )
        .await
    {
        Ok(response) => {
            let response_time_ms = request_start.elapsed().as_millis() as f64;
            audit.set_timing(response_time_ms, None, None, None, None);
            for post in &response.posts {
                audit.add_personalized_post(&post.post_id, &post.uri, post.score);
            }
            let record: FeedAuditRecord = audit.build_record();
            (StatusCode::OK, Json(record)).into_response()
        }
        Err(e) => {
            error!(error = %e, "audit_personalize_error");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "error": "personalization_failed",
                    "message": e.to_string()
                })),
            )
                .into_response()
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Feed Status Debugging Endpoint
// ═══════════════════════════════════════════════════════════════════════════════

/// GET /v1/feeds/status/:algo_id
///
/// Get comprehensive diagnostic information about a feed's health status.
/// Useful for debugging why feeds may be returning empty.
pub async fn feed_status(State(state): State<Arc<AppState>>, Path(algo_id): Path<i32>) -> Response {
    // Query algorithm posts key
    let algo_posts_key = Keys::algo_posts(algo_id);
    let algo_posts_exists = state.redis.exists(&algo_posts_key).await.unwrap_or(false);
    let algo_posts_count = if algo_posts_exists {
        state.redis.scard(&algo_posts_key).await.unwrap_or(0)
    } else {
        0
    };
    let algo_posts_ttl = state.redis.ttl(&algo_posts_key).await.unwrap_or(-2);

    // Query algorithm metadata
    let meta_key = Keys::algo_meta(algo_id);
    let meta_exists = state.redis.exists(&meta_key).await.unwrap_or(false);
    let meta_fields = if meta_exists {
        state.redis.hgetall(&meta_key).await.unwrap_or_default()
    } else {
        vec![]
    };
    let meta_map: std::collections::HashMap<String, String> = meta_fields.into_iter().collect();

    let last_sync_timestamp = meta_map.get("last_sync").cloned();
    let last_sync_post_count = meta_map
        .get("post_count")
        .and_then(|s| s.parse::<usize>().ok());

    // Calculate last sync age
    let last_sync_age_seconds = last_sync_timestamp.as_ref().and_then(|ts| {
        ts.parse::<i64>().ok().map(|sync_ts| {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0);
            now - sync_ts
        })
    });

    // Query rate limit lock
    let lock_key = Keys::sync_lock(algo_id);
    let rate_limit_locked = state.redis.exists(&lock_key).await.unwrap_or(false);
    let rate_limit_ttl = if rate_limit_locked {
        Some(state.redis.ttl(&lock_key).await.unwrap_or(-2))
    } else {
        None
    };

    // Query sync queue depth
    let queue_depth = state.redis.llen(Keys::SYNC_QUEUE).await.unwrap_or(0);

    // Query fallback tranches
    let trending_key = Keys::trending_posts(algo_id);
    let popular_key = Keys::popular_posts(algo_id);
    let velocity_key = Keys::velocity_posts(algo_id);
    let discovery_key = Keys::discovery_posts(algo_id);

    let trending_exists = state.redis.exists(&trending_key).await.unwrap_or(false);
    let trending_count = if trending_exists {
        state.redis.zcard(&trending_key).await.unwrap_or(0)
    } else {
        0
    };

    let popular_exists = state.redis.exists(&popular_key).await.unwrap_or(false);
    let popular_count = if popular_exists {
        state.redis.zcard(&popular_key).await.unwrap_or(0)
    } else {
        0
    };

    let velocity_exists = state.redis.exists(&velocity_key).await.unwrap_or(false);
    let velocity_count = if velocity_exists {
        state.redis.zcard(&velocity_key).await.unwrap_or(0)
    } else {
        0
    };

    let discovery_exists = state.redis.exists(&discovery_key).await.unwrap_or(false);
    let discovery_count = if discovery_exists {
        state.redis.zcard(&discovery_key).await.unwrap_or(0)
    } else {
        0
    };

    // Generate diagnosis
    let diagnosis = if !algo_posts_exists && !meta_exists {
        "Sync has never been triggered. No metadata or posts exist for this algorithm.".to_string()
    } else if !algo_posts_exists && meta_exists {
        if let Some(age) = last_sync_age_seconds {
            format!(
                "Posts key expired. Last sync was {} seconds ago. May need to trigger sync.",
                age
            )
        } else {
            "Posts key missing but metadata exists. Sync may have failed or key expired."
                .to_string()
        }
    } else if algo_posts_count == 0 {
        "Posts key exists but is empty. ClickHouse may be returning no data.".to_string()
    } else if algo_posts_ttl > 0 && algo_posts_ttl < 300 {
        format!(
            "Posts key will expire in {} seconds. Consider triggering sync soon.",
            algo_posts_ttl
        )
    } else {
        "Feed appears healthy.".to_string()
    };

    let healthy = algo_posts_exists && algo_posts_count > 0;

    (
        StatusCode::OK,
        Json(json!({
            "algo_id": algo_id,
            "healthy": healthy,
            "algo_posts": {
                "exists": algo_posts_exists,
                "count": algo_posts_count,
                "ttl_seconds": algo_posts_ttl
            },
            "sync_metadata": {
                "exists": meta_exists,
                "last_sync_timestamp": last_sync_timestamp,
                "last_sync_age_seconds": last_sync_age_seconds,
                "last_sync_post_count": last_sync_post_count
            },
            "rate_limit": {
                "locked": rate_limit_locked,
                "ttl_seconds": rate_limit_ttl
            },
            "sync_queue": {
                "depth": queue_depth
            },
            "fallback_tranches": {
                "trending": { "exists": trending_exists, "count": trending_count },
                "popular": { "exists": popular_exists, "count": popular_count },
                "velocity": { "exists": velocity_exists, "count": velocity_count },
                "discovery": { "exists": discovery_exists, "count": discovery_count }
            },
            "diagnosis": diagnosis
        })),
    )
        .into_response()
}

// ═══════════════════════════════════════════════════════════════════════════════
// Candidate management (HTTP admin for add/remove posts)
// ═══════════════════════════════════════════════════════════════════════════════

/// Ensure the algo_id exists in SUPPORTED_FEEDS (at least one feed maps to it).
async fn ensure_feed_exists(state: &AppState, algo_id: i32) -> Result<(), Response> {
    let feeds = match state.redis.hgetall(Keys::SUPPORTED_FEEDS).await {
        Ok(f) => f,
        Err(e) => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "error": "RedisError",
                    "message": format!("Failed to list feeds: {}", e)
                })),
            )
                .into_response())
        }
    };
    let has_feed = feeds
        .iter()
        .any(|(_, v)| v.parse::<i32>().ok().as_ref() == Some(&algo_id));
    if !has_feed {
        return Err((
            StatusCode::NOT_FOUND,
            Json(json!({
                "error": "NotFound",
                "message": format!("No feed registered for algo_id {}", algo_id)
            })),
        )
            .into_response());
    }
    Ok(())
}

#[derive(Debug, Deserialize)]
pub struct CandidatesQuery {
    pub limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub struct CandidatesBody {
    pub uris: Vec<String>,
}

/// GET /v1/feeds/:algo_id/candidates
///
/// List candidate post URIs for an algorithm. Used by the candidate-sync worker
/// when CANDIDATE_SOURCE=http and for admin debugging.
pub async fn get_candidates(
    State(state): State<Arc<AppState>>,
    Path(algo_id): Path<i32>,
    Query(query): Query<CandidatesQuery>,
) -> Response {
    if let Err(r) = ensure_feed_exists(&state, algo_id).await {
        return r;
    }

    let posts_key = Keys::algo_posts(algo_id);
    let id_strs: Vec<String> = match state.redis.smembers(&posts_key).await {
        Ok(s) => s,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "error": "RedisError",
                    "message": format!("{}", e)
                })),
            )
                .into_response()
        }
    };

    let limit = query.limit.unwrap_or(10_000).min(50_000);
    let ids: Vec<i64> = id_strs
        .iter()
        .take(limit)
        .filter_map(|s| s.parse().ok())
        .collect();

    let id_to_uri = match state.interner.get_uris_batch(&ids).await {
        Ok(m) => m,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "error": "InternerError",
                    "message": format!("{}", e)
                })),
            )
                .into_response()
        }
    };

    let uris: Vec<String> = ids
        .into_iter()
        .filter_map(|id| id_to_uri.get(&id).cloned())
        .collect();

    (
        StatusCode::OK,
        Json(json!({
            "uris": uris,
            "total": uris.len()
        })),
    )
        .into_response()
}

/// POST /v1/feeds/:algo_id/candidates
///
/// Add post URIs to the candidate set. Incremental; does not replace the set.
pub async fn add_candidates(
    State(state): State<Arc<AppState>>,
    Path(algo_id): Path<i32>,
    Json(body): Json<CandidatesBody>,
) -> Response {
    if state.config.read_only_mode {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "error": "ReadOnlyMode",
                "message": "Write operations disabled in read-only mode"
            })),
        )
            .into_response();
    }
    if let Err(r) = ensure_feed_exists(&state, algo_id).await {
        return r;
    }
    if body.uris.is_empty() {
        return (
            StatusCode::OK,
            Json(json!({ "added": 0, "message": "No URIs provided" })),
        )
            .into_response();
    }

    let uri_to_id = match state.interner.get_or_create_ids_batch(&body.uris).await {
        Ok(m) => m,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "error": "InternerError",
                    "message": format!("{}", e)
                })),
            )
                .into_response()
        }
    };

    let posts_key = Keys::algo_posts(algo_id);
    let meta_key = Keys::algo_meta(algo_id);
    let str_ids: Vec<String> = uri_to_id.values().map(|id| id.to_string()).collect();

    if let Err(e) = state.redis.sadd(&posts_key, &str_ids).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({
                "error": "RedisError",
                "message": format!("{}", e)
            })),
        )
            .into_response();
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let _ = state
        .redis
        .hset_multiple(
            &meta_key,
            &[
                ("last_updated", &now.to_string()),
                ("source", "admin"),
            ],
        )
        .await;

    info!(algo_id, added = uri_to_id.len(), "candidates_added");
    (
        StatusCode::OK,
        Json(json!({ "added": uri_to_id.len() })),
    )
        .into_response()
}

/// DELETE /v1/feeds/:algo_id/candidates
///
/// Remove post URIs from the candidate set.
pub async fn remove_candidates(
    State(state): State<Arc<AppState>>,
    Path(algo_id): Path<i32>,
    Json(body): Json<CandidatesBody>,
) -> Response {
    if state.config.read_only_mode {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "error": "ReadOnlyMode",
                "message": "Write operations disabled in read-only mode"
            })),
        )
            .into_response();
    }
    if let Err(r) = ensure_feed_exists(&state, algo_id).await {
        return r;
    }
    if body.uris.is_empty() {
        return (
            StatusCode::OK,
            Json(json!({ "removed": 0 })),
        )
            .into_response();
    }

    let uri_to_id = match state.interner.get_ids_batch(&body.uris).await {
        Ok(m) => m,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "error": "InternerError",
                    "message": format!("{}", e)
                })),
            )
                .into_response()
        }
    };

    let str_ids: Vec<String> = uri_to_id.values().map(|id| id.to_string()).collect();
    let posts_key = Keys::algo_posts(algo_id);
    let removed = match state.redis.srem(&posts_key, &str_ids).await {
        Ok(n) => n,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "error": "RedisError",
                    "message": format!("{}", e)
                })),
            )
                .into_response()
        }
    };

    info!(algo_id, removed, "candidates_removed");
    (
        StatusCode::OK,
        Json(json!({ "removed": removed })),
    )
        .into_response()
}

// ═══════════════════════════════════════════════════════════════════════════════
// Special posts CRUD (pinned, rotating, sponsored)
// ═══════════════════════════════════════════════════════════════════════════════

fn special_posts_key(algo_id: i32) -> String {
    Keys::special_posts(algo_id)
}

async fn load_special_posts(state: &AppState, algo_id: i32) -> Result<SpecialPostsResponse, Response> {
    let key = special_posts_key(algo_id);
    let raw = state.redis.get_string(&key).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({
                "error": "RedisError",
                "message": format!("{}", e)
            })),
        )
            .into_response()
    })?;
    match raw {
        None => Ok(SpecialPostsResponse::empty(algo_id)),
        Some(s) => serde_json::from_str(&s).map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "error": "InvalidData",
                    "message": format!("Invalid special posts JSON: {}", e)
                })),
            )
                .into_response()
        }),
    }
}

async fn save_special_posts(
    state: &AppState,
    algo_id: i32,
    data: &SpecialPostsResponse,
) -> Result<(), Response> {
    let key = special_posts_key(algo_id);
    let json = serde_json::to_string(data).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({
                "error": "SerializeError",
                "message": format!("{}", e)
            })),
        )
            .into_response()
    })?;
    // Persist with long TTL (10 years) when managed via admin
    state.redis.set_ex(&key, &json, 315_360_000).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({
                "error": "RedisError",
                "message": format!("{}", e)
            })),
        )
            .into_response()
    })?;
    Ok(())
}

/// GET /v1/feeds/:algo_id/special-posts
pub async fn get_special_posts(
    State(state): State<Arc<AppState>>,
    Path(algo_id): Path<i32>,
) -> Response {
    if let Err(r) = ensure_feed_exists(&state, algo_id).await {
        return r;
    }
    match load_special_posts(&state, algo_id).await {
        Ok(data) => (StatusCode::OK, Json(data)).into_response(),
        Err(r) => r,
    }
}

#[derive(Debug, Deserialize)]
pub struct AddPinnedBody {
    pub attribution: String,
    pub post: String,
}

#[derive(Debug, Deserialize)]
pub struct AddSponsoredBody {
    pub attribution: String,
    pub post: String,
    #[serde(default)]
    pub cpm_cents: i32,
}

/// POST /v1/feeds/:algo_id/special-posts/pinned
pub async fn add_pinned(
    State(state): State<Arc<AppState>>,
    Path(algo_id): Path<i32>,
    Json(body): Json<AddPinnedBody>,
) -> Response {
    if state.config.read_only_mode {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "ReadOnlyMode", "message": "Write operations disabled"})),
        )
            .into_response();
    }
    if let Err(r) = ensure_feed_exists(&state, algo_id).await {
        return r;
    }
    let mut data = match load_special_posts(&state, algo_id).await {
        Ok(d) => d,
        Err(r) => return r,
    };
    data.pinned.retain(|p| p.attribution != body.attribution);
    data.pinned.push(SpecialPost {
        attribution: body.attribution.clone(),
        post: body.post.clone(),
    });
    if let Err(r) = save_special_posts(&state, algo_id, &data).await {
        return r;
    }
    info!(algo_id, attribution = %body.attribution, "special_post_pinned_added");
    (StatusCode::OK, Json(json!({ "ok": true }))).into_response()
}

/// DELETE /v1/feeds/:algo_id/special-posts/pinned/:attribution
pub async fn remove_pinned(
    State(state): State<Arc<AppState>>,
    Path((algo_id, attribution)): Path<(i32, String)>,
) -> Response {
    if state.config.read_only_mode {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "ReadOnlyMode", "message": "Write operations disabled"})),
        )
            .into_response();
    }
    if let Err(r) = ensure_feed_exists(&state, algo_id).await {
        return r;
    }
    let mut data = match load_special_posts(&state, algo_id).await {
        Ok(d) => d,
        Err(r) => return r,
    };
    let before = data.pinned.len();
    data.pinned.retain(|p| p.attribution != attribution);
    if data.pinned.len() == before {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "NotFound", "message": "Pinned post not found"})),
        )
            .into_response();
    }
    if let Err(r) = save_special_posts(&state, algo_id, &data).await {
        return r;
    }
    info!(algo_id, attribution = %attribution, "special_post_pinned_removed");
    (StatusCode::OK, Json(json!({ "ok": true }))).into_response()
}

/// POST /v1/feeds/:algo_id/special-posts/rotating
pub async fn add_rotating(
    State(state): State<Arc<AppState>>,
    Path(algo_id): Path<i32>,
    Json(body): Json<AddPinnedBody>,
) -> Response {
    if state.config.read_only_mode {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "ReadOnlyMode", "message": "Write operations disabled"})),
        )
            .into_response();
    }
    if let Err(r) = ensure_feed_exists(&state, algo_id).await {
        return r;
    }
    let mut data = match load_special_posts(&state, algo_id).await {
        Ok(d) => d,
        Err(r) => return r,
    };
    data.sticky.retain(|p| p.attribution != body.attribution);
    data.sticky.push(SpecialPost {
        attribution: body.attribution.clone(),
        post: body.post.clone(),
    });
    if let Err(r) = save_special_posts(&state, algo_id, &data).await {
        return r;
    }
    info!(algo_id, attribution = %body.attribution, "special_post_rotating_added");
    (StatusCode::OK, Json(json!({ "ok": true }))).into_response()
}

/// DELETE /v1/feeds/:algo_id/special-posts/rotating/:attribution
pub async fn remove_rotating(
    State(state): State<Arc<AppState>>,
    Path((algo_id, attribution)): Path<(i32, String)>,
) -> Response {
    if state.config.read_only_mode {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "ReadOnlyMode", "message": "Write operations disabled"})),
        )
            .into_response();
    }
    if let Err(r) = ensure_feed_exists(&state, algo_id).await {
        return r;
    }
    let mut data = match load_special_posts(&state, algo_id).await {
        Ok(d) => d,
        Err(r) => return r,
    };
    let before = data.sticky.len();
    data.sticky.retain(|p| p.attribution != attribution);
    if data.sticky.len() == before {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "NotFound", "message": "Rotating post not found"})),
        )
            .into_response();
    }
    if let Err(r) = save_special_posts(&state, algo_id, &data).await {
        return r;
    }
    info!(algo_id, attribution = %attribution, "special_post_rotating_removed");
    (StatusCode::OK, Json(json!({ "ok": true }))).into_response()
}

/// POST /v1/feeds/:algo_id/special-posts/sponsored
pub async fn add_sponsored(
    State(state): State<Arc<AppState>>,
    Path(algo_id): Path<i32>,
    Json(body): Json<AddSponsoredBody>,
) -> Response {
    if state.config.read_only_mode {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "ReadOnlyMode", "message": "Write operations disabled"})),
        )
            .into_response();
    }
    if let Err(r) = ensure_feed_exists(&state, algo_id).await {
        return r;
    }
    let mut data = match load_special_posts(&state, algo_id).await {
        Ok(d) => d,
        Err(r) => return r,
    };
    data.sponsored.retain(|p| p.attribution != body.attribution);
    data.sponsored.push(SponsoredPost {
        attribution: body.attribution.clone(),
        post: body.post.clone(),
        cpm_cents: body.cpm_cents,
    });
    if let Err(r) = save_special_posts(&state, algo_id, &data).await {
        return r;
    }
    info!(algo_id, attribution = %body.attribution, "special_post_sponsored_added");
    (StatusCode::OK, Json(json!({ "ok": true }))).into_response()
}

/// DELETE /v1/feeds/:algo_id/special-posts/sponsored/:attribution
pub async fn remove_sponsored(
    State(state): State<Arc<AppState>>,
    Path((algo_id, attribution)): Path<(i32, String)>,
) -> Response {
    if state.config.read_only_mode {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "ReadOnlyMode", "message": "Write operations disabled"})),
        )
            .into_response();
    }
    if let Err(r) = ensure_feed_exists(&state, algo_id).await {
        return r;
    }
    let mut data = match load_special_posts(&state, algo_id).await {
        Ok(d) => d,
        Err(r) => return r,
    };
    let before = data.sponsored.len();
    data.sponsored.retain(|p| p.attribution != attribution);
    if data.sponsored.len() == before {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "NotFound", "message": "Sponsored post not found"})),
        )
            .into_response();
    }
    if let Err(r) = save_special_posts(&state, algo_id, &data).await {
        return r;
    }
    info!(algo_id, attribution = %attribution, "special_post_sponsored_removed");
    (StatusCode::OK, Json(json!({ "ok": true }))).into_response()
}
