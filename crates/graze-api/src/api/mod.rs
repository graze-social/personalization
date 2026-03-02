//! HTTP API handlers for the Graze service.
//!
//! This module contains Axum handlers for all API endpoints.

use std::sync::Arc;

use axum::{
    middleware,
    routing::{delete, get, post},
    Router,
};

use crate::AppState;

mod admin;
mod auth;
pub mod cursor;
mod docs;
mod fallback;
mod feed;
mod health;
mod personalize;
mod request_id;
mod special_posts;

pub use admin::*;
pub use cursor::FeedCursor;
pub use fallback::*;
pub use feed::*;
pub use health::*;
pub use personalize::*;
pub use request_id::RequestId;
pub use special_posts::*;

/// Create the main API router.
pub fn create_router(state: Arc<AppState>) -> Router {
    Router::new()
        // Health and metrics
        .route("/metrics", get(health::metrics))
        .route("/", get(health::root))
        // Docs (embedded from repo docs/)
        .route("/docs", get(docs::docs_index))
        .route("/docs/:name", get(docs::docs_page))
        // Internal probe endpoints
        .route("/internal/started", get(health::started))
        .route("/internal/ready", get(health::ready))
        .route("/internal/alive", get(health::alive))
        // Personalization API
        .route("/v1/personalize", post(personalize::personalize))
        .route("/v1/invalidate", post(personalize::invalidate))
        .route("/v1/prove", post(personalize::prove))
        .route(
            "/v1/author-affinity",
            post(personalize::author_affinity_diagnostic),
        )
        // Feed management
        .route("/v1/feeds", get(admin::list_feeds).post(admin::register_feed))
        .route("/v1/feeds/:algo_id", delete(admin::deregister_feed))
        .route("/v1/feeds/status/:algo_id", get(admin::feed_status))
        .route(
            "/v1/feeds/:algo_id/candidates",
            get(admin::get_candidates)
                .post(admin::add_candidates)
                .delete(admin::remove_candidates),
        )
        .route(
            "/v1/feeds/:algo_id/special-posts",
            get(admin::get_special_posts),
        )
        .route(
            "/v1/feeds/:algo_id/special-posts/pinned",
            post(admin::add_pinned),
        )
        .route(
            "/v1/feeds/:algo_id/special-posts/pinned/:attribution",
            delete(admin::remove_pinned),
        )
        .route(
            "/v1/feeds/:algo_id/special-posts/rotating",
            post(admin::add_rotating),
        )
        .route(
            "/v1/feeds/:algo_id/special-posts/rotating/:attribution",
            delete(admin::remove_rotating),
        )
        .route(
            "/v1/feeds/:algo_id/special-posts/sponsored",
            post(admin::add_sponsored),
        )
        .route(
            "/v1/feeds/:algo_id/special-posts/sponsored/:attribution",
            delete(admin::remove_sponsored),
        )
        .route(
            "/v1/feeds/:algo_id/thompson-config",
            get(admin::get_feed_thompson_config).put(admin::put_feed_thompson_config),
        )
        // Thompson learner stats
        .route("/v1/thompson/stats", get(admin::thompson_stats))
        .route(
            "/v1/thompson/algorithm/:algo_id",
            get(admin::thompson_algorithm),
        )
        .route(
            "/v1/thompson/search-space",
            get(admin::get_thompson_search_space).put(admin::put_thompson_search_space),
        )
        .route(
            "/v1/thompson/success-criteria",
            get(admin::get_thompson_success_criteria)
                .put(admin::put_thompson_success_criteria),
        )
        // Audit user management
        .route("/v1/audit/users", post(admin::add_audit_users))
        .route("/v1/audit/users", delete(admin::remove_audit_users))
        .route("/v1/audit/users", get(admin::list_audit_users))
        .route("/v1/audit/status", get(admin::audit_status))
        .route("/v1/audit/personalize", post(admin::audit_personalize))
        // ATProto feed endpoints
        .route(
            "/xrpc/app.bsky.feed.getFeedSkeleton",
            get(feed::get_feed_skeleton),
        )
        .route(
            "/xrpc/app.bsky.feed.describeFeedGenerator",
            get(feed::describe_feed_generator),
        )
        .route(
            "/xrpc/app.bsky.feed.sendInteractions",
            post(feed::send_interactions),
        )
        .route("/.well-known/did.json", get(feed::well_known_did))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            auth::require_admin_api_key,
        ))
        .layer(middleware::from_fn(request_id::request_id_middleware))
        .with_state(state)
}
