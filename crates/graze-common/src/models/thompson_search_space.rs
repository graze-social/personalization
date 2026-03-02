//! Thompson Sampling search space configuration.
//!
//! Stored in Redis or per-feed config for PUTable parameter exploration.

use serde::{Deserialize, Serialize};

/// Search space for Thompson Sampling bandits.
/// Mirrors ThompsonConfig option fields for JSON (de)serialization.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ThompsonSearchSpace {
    pub min_likes_options: Vec<usize>,
    pub min_likes_default: usize,
    pub max_likers_options: Vec<usize>,
    pub max_likers_default: usize,
    pub max_sources_options: Vec<usize>,
    pub max_sources_default: usize,
    pub max_checks_options: Vec<usize>,
    pub max_checks_default: usize,
    pub min_colikes_options: Vec<usize>,
    pub min_colikes_default: usize,
    pub max_user_likes_options: Vec<usize>,
    pub max_user_likes_default: usize,
    pub max_sources_per_post_options: Vec<usize>,
    pub max_sources_per_post_default: usize,
    pub seed_pool_options: Vec<usize>,
    pub seed_pool_default: usize,
    pub corater_decay_options: Vec<usize>,
    pub corater_decay_default: usize,
}

impl Default for ThompsonSearchSpace {
    fn default() -> Self {
        Self {
            min_likes_options: vec![1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50],
            min_likes_default: 5,
            max_likers_options: vec![10, 20, 30, 50, 75, 100, 150, 200],
            max_likers_default: 30,
            max_sources_options: vec![250, 500, 750, 1000, 1500, 2000, 5000, 10000, 20000],
            max_sources_default: 1000,
            max_checks_options: vec![100, 200, 300, 500, 750, 1000],
            max_checks_default: 500,
            min_colikes_options: vec![1, 2, 3, 5],
            min_colikes_default: 1,
            max_user_likes_options: vec![100, 200, 300, 500, 750],
            max_user_likes_default: 500,
            max_sources_per_post_options: vec![25, 50, 75, 100, 150],
            max_sources_per_post_default: 100,
            seed_pool_options: vec![0, 500, 1000, 1500, 2000],
            seed_pool_default: 0,
            corater_decay_options: vec![0, 5, 10, 20, 30, 50],
            corater_decay_default: 0,
        }
    }
}
