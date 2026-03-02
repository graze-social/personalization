//! Feed success criteria for Thompson Sampling (request-time evaluation).
//!
//! Defines what counts as "success" when evaluating a feed outcome for learning.

use serde::{Deserialize, Serialize};

/// Configuration for feed success evaluation (request-time Thompson path).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct FeedSuccessConfig {
    /// Minimum personalization ratio (post-level + author-affinity) / total.
    pub min_personalization_ratio: f64,
    /// Minimum posts that must be checked for "rich" scoring.
    pub min_posts_checked: usize,
    /// Minimum posts that must be scored for "rich" scoring.
    pub min_posts_scored: usize,
    /// Maximum response time in milliseconds.
    pub max_response_time_ms: f64,
}

impl Default for FeedSuccessConfig {
    fn default() -> Self {
        Self {
            min_personalization_ratio: 0.60,
            min_posts_checked: 50,
            min_posts_scored: 10,
            max_response_time_ms: 500.0,
        }
    }
}
