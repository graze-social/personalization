//! Personalization API endpoint.

use std::sync::Arc;

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    Extension, Json,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tracing::{debug, error, info};

use crate::algorithm::{compute_proof, get_preset, merge_params};
use crate::api::RequestId;
use crate::audit::{emit_skip_log, should_audit, AuditCollector};
use crate::AppState;
use graze_common::hash_did;
use graze_common::models::PersonalizeRequest;

/// POST /v1/personalize
///
/// Personalize feed for a user using the LinkLonk algorithm.
pub async fn personalize(
    State(state): State<Arc<AppState>>,
    Extension(request_id): Extension<RequestId>,
    Json(request): Json<PersonalizeRequest>,
) -> Response {
    tracing::info!(
        user_did = %request.user_did,
        algo_id = request.algo_id,
        limit = request.limit,
        "personalize_request_received"
    );

    let request_start = std::time::Instant::now();
    let request_id = request_id.to_string();
    let user_hash = hash_did(&request.user_did);

    tracing::debug!(
        request_id = %request_id,
        user_hash = %&user_hash[..8.min(user_hash.len())],
        "personalize_starting"
    );

    // Initialize audit collector if audit is enabled for this user
    tracing::debug!(request_id = %request_id, "checking_audit_status");
    let mut audit = if should_audit(&state.config, &state.redis, Some(&request.user_did)).await {
        Some(AuditCollector::new(
            request_id.clone(),
            user_hash.clone(),
            request.algo_id,
            state.config.audit_max_contributors,
            state.config.audit_log_full_breakdown,
        ))
    } else {
        None
    };

    // Extract preset from params if provided
    let preset = request.params.as_ref().and_then(|p| p.preset.as_deref());

    tracing::info!(
        request_id = %request_id,
        audit_enabled = audit.is_some(),
        preset = preset.unwrap_or("default"),
        "starting_personalization"
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
            audit.as_mut(),
        )
        .await
    {
        Ok(response) => {
            // Emit audit log if enabled
            if let Some(mut a) = audit {
                let response_time_ms = request_start.elapsed().as_millis() as f64;
                a.set_timing(response_time_ms, None, None, None, None);
                // Add personalized posts to audit
                for post in &response.posts {
                    a.add_personalized_post(&post.post_id, &post.uri, post.score);
                }
                a.emit_log();
            }
            (StatusCode::OK, Json(response)).into_response()
        }
        Err(e) => {
            error!(error = %e, "personalization_error");
            // Emit skip log for error case
            if state.config.audit_enabled {
                emit_skip_log(
                    &request_id,
                    Some(&user_hash),
                    Some(request.algo_id),
                    "personalization_error",
                    Some(&e.to_string()),
                );
            }
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

/// POST /v1/invalidate
///
/// Invalidate cached personalization results for a user.
pub async fn invalidate(
    State(state): State<Arc<AppState>>,
    Json(request): Json<graze_common::models::InvalidateRequest>,
) -> Response {
    if let (Some(user_did), Some(algo_id)) = (&request.user_did, request.algo_id) {
        debug!(user_did = %user_did, algo_id, "invalidation_request_received");
        match state.algorithm.invalidate_user(user_did, algo_id).await {
            Ok(()) => {
                info!(user_did = %user_did, algo_id, "cache_invalidated_successfully");
                (
                    StatusCode::OK,
                    Json(graze_common::models::InvalidateResponse {
                        invalidated: 1,
                        message: "Cache invalidated".to_string(),
                    }),
                )
                    .into_response()
            }
            Err(e) => {
                error!(user_did = %user_did, algo_id, error = %e, "invalidation_error");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({
                        "error": "invalidation_failed",
                        "message": e.to_string()
                    })),
                )
                    .into_response()
            }
        }
    } else {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "invalid_request",
                "message": "Both user_did and algo_id are required"
            })),
        )
            .into_response()
    }
}

/// Request for the prove endpoint.
#[derive(Debug, Deserialize)]
pub struct ProveRequest {
    pub user_did: String,
    pub algo_id: i32,
    #[serde(default)]
    pub preset: Option<String>,
}

/// POST /v1/prove
///
/// Generate a detailed proof of the LinkLonk algorithm traversal.
/// This shows exactly how personalization results were computed:
/// - Step 1: User's liked posts
/// - Step 2: Co-liker computation and weights
/// - Step 3: Post scoring with contributor breakdown
pub async fn prove(
    State(state): State<Arc<AppState>>,
    Json(request): Json<ProveRequest>,
) -> Response {
    tracing::info!(
        user_did = %request.user_did,
        algo_id = request.algo_id,
        "prove_request_received"
    );

    // Get parameters
    let base_params = get_preset(request.preset.as_deref().unwrap_or("default"));
    let params = merge_params(base_params, None);

    match compute_proof(
        state.redis.clone(),
        state.interner.clone(),
        state.config.clone(),
        &request.user_did,
        request.algo_id,
        &params,
    )
    .await
    {
        Ok(proof) => (StatusCode::OK, Json(proof)).into_response(),
        Err(e) => {
            error!(error = %e, "prove_error");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "error": "proof_generation_failed",
                    "message": e.to_string()
                })),
            )
                .into_response()
        }
    }
}

/// Request body for author affinity diagnostic endpoint.
#[derive(Debug, Deserialize)]
pub struct AuthorAffinityRequest {
    pub user_did: String,
    pub algo_id: i32,
    #[serde(default = "default_limit")]
    pub limit: usize,
}

fn default_limit() -> usize {
    30
}

/// Response for author affinity diagnostic endpoint.
#[derive(Debug, Serialize)]
pub struct AuthorAffinityResponse {
    pub user_did: String,
    pub algo_id: i32,
    pub author_affinity_enabled: bool,
    pub posts: Vec<AuthorAffinityPost>,
    pub stats: AuthorAffinityStats,
}

#[derive(Debug, Serialize)]
pub struct AuthorAffinityPost {
    pub uri: String,
    pub score: f64,
    pub post_id: String,
}

#[derive(Debug, Serialize)]
pub struct AuthorAffinityStats {
    pub colikers_found: usize,
    pub posts_scored: usize,
    pub posts_returned: usize,
    pub scoring_time_ms: f64,
}

/// POST /v1/author-affinity
///
/// Diagnostic endpoint for testing author-level co-likers (coarse LinkLonk).
/// This runs the author affinity algorithm independently and returns the results.
pub async fn author_affinity_diagnostic(
    State(state): State<Arc<AppState>>,
    Json(request): Json<AuthorAffinityRequest>,
) -> Response {
    tracing::info!(
        user_did = %request.user_did,
        algo_id = request.algo_id,
        limit = request.limit,
        "author_affinity_diagnostic_request"
    );

    let user_hash = graze_common::hash_did(&request.user_did);

    // Create params for author affinity
    let params = crate::algorithm::LinkLonkParams {
        use_author_affinity: true,
        ..Default::default()
    };

    // Check if author affinity is enabled
    let author_affinity_enabled = state.config.author_affinity_enabled;

    // Compute author affinity personalization
    match state
        .algorithm
        .compute_personalization(&user_hash, request.algo_id, &params, None)
        .await
    {
        Ok(result) => {
            // Collect post IDs and scores
            let scored: Vec<(f64, String, i64)> = result
                .scored_posts
                .into_iter()
                .take(request.limit)
                .filter_map(|(score, post_id)| {
                    post_id.parse::<i64>().ok().map(|id| (score, post_id, id))
                })
                .collect();

            // Batch fetch URIs
            let post_ids: Vec<i64> = scored.iter().map(|(_, _, id)| *id).collect();
            let uri_map = state
                .interner
                .get_uris_batch(&post_ids)
                .await
                .unwrap_or_default();

            // Convert to response format
            let posts: Vec<AuthorAffinityPost> = scored
                .into_iter()
                .filter_map(|(score, post_id, id)| {
                    uri_map.get(&id).map(|uri| AuthorAffinityPost {
                        uri: uri.clone(),
                        score,
                        post_id,
                    })
                })
                .collect();

            let response = AuthorAffinityResponse {
                user_did: request.user_did,
                algo_id: request.algo_id,
                author_affinity_enabled,
                stats: AuthorAffinityStats {
                    colikers_found: 0, // Not tracked in current implementation
                    posts_scored: result.scored_count,
                    posts_returned: posts.len(),
                    scoring_time_ms: result.scoring_time_ms,
                },
                posts,
            };

            (StatusCode::OK, Json(response)).into_response()
        }
        Err(e) => {
            error!(error = %e, "author_affinity_diagnostic_error");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "error": "author_affinity_failed",
                    "message": e.to_string(),
                    "author_affinity_enabled": author_affinity_enabled
                })),
            )
                .into_response()
        }
    }
}
