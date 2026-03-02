//! Graze API - Personalized feed ranking API server.
//!
//! This crate provides the HTTP API for the Graze personalization service,
//! implementing the LinkLonk algorithm for real-time feed ranking.

pub mod algorithm;
pub mod api;
pub mod audit;
pub mod config;
pub mod error;
pub mod interaction_queue;
pub mod metrics;

use std::sync::Arc;

use graze_common::{InteractionsClient, RedisClient, SpecialPostsClient, UriInterner};
// InteractionsConfig is used in main.rs when creating InteractionsClient

use crate::algorithm::{LinkLonkAlgorithm, ThompsonLearner};
use crate::config::Config;

/// Shared application state for the API server.
pub struct AppState {
    pub config: Arc<Config>,
    pub redis: Arc<RedisClient>,
    /// URI interner for memory-efficient post ID mapping.
    pub interner: Arc<UriInterner>,
    pub algorithm: Arc<LinkLonkAlgorithm>,
    pub special_posts: Arc<SpecialPostsClient>,
    pub metrics: Arc<metrics::Metrics>,
    /// Thompson Sampling learner for adaptive parameter optimization.
    pub thompson: Arc<ThompsonLearner>,
    /// Client for persisting user interactions to ClickHouse.
    pub interactions: Arc<InteractionsClient>,
    /// Sender for the interaction queue (batched ClickHouse writes). None when queue disabled.
    pub interaction_queue: Option<interaction_queue::InteractionQueueSender>,
    /// Optional Redis client for post-render / request logging (log_tasks queue). None when REDIS_REQUESTS_LOGGER unset.
    pub redis_requests_logger: Option<Arc<RedisClient>>,
}
