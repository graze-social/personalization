//! Per-feed Thompson Sampling configuration for interaction-based learning.
//!
//! Stored in Redis as JSON at feed_thompson_config:{algo_id}.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::{FeedSuccessConfig, ThompsonSearchSpace};

/// Per-feed config for Thompson interaction-based learning.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct FeedThompsonConfig {
    /// Weights per interaction event type (e.g. "app.bsky.feed.defs#clickthroughItem").
    pub interaction_weights: HashMap<String, InteractionWeight>,
    /// Max response time (ms) for interaction-based observation to count.
    pub speed_gate_ms: f64,
    /// Whether interaction-based Thompson learning is enabled for this feed.
    pub enabled: bool,
    /// Override params for holdout group. If None, use ThompsonConfig defaults.
    pub holdout_params: Option<HoldoutParams>,
    /// Override params for treatment group (personalized arm). If None, use Thompson Sampling.
    /// When set, bypasses Thompson bandits and uses these params for all non-holdout requests.
    pub treatment_params: Option<HoldoutParams>,
    /// Search space for Thompson bandits. If None, use global or hardcoded default.
    pub search_space: Option<ThompsonSearchSpace>,
    /// Success criteria for request-time evaluation. If None, use global or hardcoded default.
    pub success_criteria: Option<FeedSuccessConfig>,
}

impl Default for FeedThompsonConfig {
    fn default() -> Self {
        Self {
            interaction_weights: HashMap::new(),
            speed_gate_ms: 200.0,
            enabled: false,
            holdout_params: None,
            treatment_params: None,
            search_space: None,
            success_criteria: None,
        }
    }
}

/// Sign and multiplier for one interaction event type.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InteractionWeight {
    /// 1 = good outcome, -1 = bad outcome, 0 = ignore.
    pub sign: i8,
    /// Weight in success score computation.
    pub multiplier: f64,
}

/// Holdout params for programmable holdout per feed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HoldoutParams {
    pub min_post_likes: usize,
    pub max_likers_per_post: usize,
    pub max_total_sources: usize,
    pub max_algo_checks: usize,
    pub min_co_likes: usize,
    pub max_user_likes: usize,
    pub max_sources_per_post: usize,
}
