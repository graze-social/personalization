//! LinkLonk algorithm parameters and presets.
//!
//! These parameters control the behavior of the personalization algorithm.

use graze_common::models::PersonalizationParams;

/// Parameters for the LinkLonk personalization algorithm.
///
/// The algorithm performs a 3-step random walk on the like graph:
/// 1. Start from user's recent likes
/// 2. Find other users who liked those posts (sources)
/// 3. Recommend posts that sources have liked
#[derive(Debug, Clone)]
pub struct LinkLonkParams {
    // Step 1: How many of user's likes to consider
    pub max_user_likes: usize,

    // Step 2: Source user limits
    pub max_sources_per_post: usize, // Per liked post
    pub max_total_sources: usize,    // Overall cap
    pub min_co_likes: usize,         // Minimum shared likes to be considered a source

    // Time windowing
    pub time_window_hours: f64, // 7 days default: sample user likes from this window (up to max_user_likes)
    pub recency_half_life_hours: f64, // 1 day default (aggressive decay for fresh content)

    // Scoring adjustments
    pub specificity_power: f64, // Higher = penalize users who like everything
    pub popularity_power: f64,  // Higher = penalize viral posts (popularity exponent)
    /// Exponent for boosting posts with more distinct co-liker paths (num_paths^num_paths_power).
    pub num_paths_power: f64,

    // Cache behavior
    pub result_ttl_seconds: u64, // 5 minutes default

    // Optimization: Max SISMEMBER checks for algo posts
    // Higher = more thorough (checks more candidates), Lower = faster
    pub max_algo_checks: usize,

    // Seen posts limit: max seen posts to load for filtering
    // Uses most recent N posts to bound memory usage
    pub max_seen_posts: usize,

    // Use author-level co-likers instead of post-level (coarse linklonk).
    // Creates denser connections by matching on authors rather than exact posts.
    pub use_author_affinity: bool,

    // Seed sampling: fetch this many likes from Redis, then randomly sample max_user_likes.
    // 0 = disabled (deterministic, current behavior). e.g. 2000 = fetch 2000, sample 500.
    pub seed_sample_pool: usize,

    // Corater decay: per-rank decay factor for co-liker contributions (0.0 to 1.0).
    // 0.0 = disabled (current behavior). 0.2 = each successive co-liker like gets 20% less weight.
    pub corater_decay: f64,
}

impl Default for LinkLonkParams {
    fn default() -> Self {
        Self {
            max_user_likes: 500,
            max_sources_per_post: 500, // Increased from 100 to capture more co-likers
            max_total_sources: 10000,  // Curator search space: at least 10k (main For You–style)
            min_co_likes: 1,
            time_window_hours: 168.0, // 7 days
            recency_half_life_hours: 24.0,
            specificity_power: 1.0,
            popularity_power: 1.0,
            num_paths_power: 0.3, // Boost posts with more distinct paths (blog-tuned default)
            result_ttl_seconds: 300,
            max_algo_checks: 500,
            max_seen_posts: 1000,
            use_author_affinity: false,
            seed_sample_pool: 0,
            corater_decay: 0.0,
        }
    }
}

impl LinkLonkParams {
    /// Create discovery mode preset: favor newer, niche content.
    pub fn discovery() -> Self {
        Self {
            max_user_likes: 200,
            recency_half_life_hours: 24.0,
            specificity_power: 1.5,
            popularity_power: 1.5,
            max_algo_checks: 300,
            ..Default::default()
        }
    }

    /// Create stable mode preset: broader, more consistent results.
    pub fn stable() -> Self {
        Self {
            max_user_likes: 1000,
            recency_half_life_hours: 168.0,
            specificity_power: 0.5,
            max_algo_checks: 750,
            ..Default::default()
        }
    }

    /// Create fast mode preset: optimized for speed.
    pub fn fast() -> Self {
        Self {
            max_user_likes: 200,
            max_sources_per_post: 50,
            max_total_sources: 500,
            result_ttl_seconds: 600,
            max_algo_checks: 200,
            max_seen_posts: 500,
            ..Default::default()
        }
    }

    /// Convert parameters to a vector of string arguments for Lua script.
    ///
    /// Returns arguments in the order expected by graze_compute.lua:
    /// 1: max_user_likes
    /// 2: max_sources_per_post
    /// 3: max_total_sources
    /// 4: min_co_likes (reserved)
    /// 5: time_window_seconds
    /// 6: recency_half_life_seconds
    /// 7: specificity_power
    /// 8: popularity_power
    /// 9: result_ttl_seconds
    /// 10: current_timestamp (provided separately)
    /// 11: max_algo_checks
    /// 12: use_precomputed_colikes (provided separately)
    /// 13: max_seen_posts
    /// 14: use_algo_likers (provided separately)
    /// 15: num_paths_power (paths exponent for boosting posts with more distinct paths)
    /// 16: seed_sample_pool (0 = disabled; >0 = fetch this many likes, randomly sample max_user_likes)
    /// 17: corater_decay (0.0-1.0; per-rank decay factor for co-liker contributions)
    pub fn to_lua_args(
        &self,
        now: f64,
        use_precomputed_colikes: bool,
        use_algo_likers: bool,
    ) -> Vec<String> {
        vec![
            self.max_user_likes.to_string(),
            self.max_sources_per_post.to_string(),
            self.max_total_sources.to_string(),
            self.min_co_likes.to_string(),
            (self.time_window_hours * 3600.0).to_string(),
            (self.recency_half_life_hours * 3600.0).to_string(),
            self.specificity_power.to_string(),
            self.popularity_power.to_string(),
            self.result_ttl_seconds.to_string(),
            now.to_string(),
            self.max_algo_checks.to_string(),
            if use_precomputed_colikes { "1" } else { "0" }.to_string(),
            self.max_seen_posts.to_string(),
            if use_algo_likers { "1" } else { "0" }.to_string(),
            self.num_paths_power.to_string(),
            self.seed_sample_pool.to_string(),
            self.corater_decay.to_string(),
        ]
    }
}

/// Get a preset by name, defaulting to 'default' if not found.
pub fn get_preset(name: &str) -> LinkLonkParams {
    match name.to_lowercase().as_str() {
        "discovery" => LinkLonkParams::discovery(),
        "stable" => LinkLonkParams::stable(),
        "fast" => LinkLonkParams::fast(),
        _ => LinkLonkParams::default(),
    }
}

/// Apply Thompson Sampling selected parameters to a base preset.
///
/// This overlays the dynamically selected values onto the base parameters.
pub fn apply_thompson_params(
    base: LinkLonkParams,
    thompson: &crate::algorithm::SelectedParams,
) -> LinkLonkParams {
    LinkLonkParams {
        max_user_likes: thompson.max_user_likes,
        max_sources_per_post: thompson.max_sources_per_post,
        max_total_sources: thompson.max_total_sources,
        min_co_likes: thompson.min_co_likes,
        max_algo_checks: thompson.max_algo_checks,
        seed_sample_pool: thompson.seed_sample_pool,
        corater_decay: thompson.corater_decay_pct as f64 / 100.0,
        // Keep other params from base preset
        ..base
    }
}

/// Merge parameter overrides into a base preset.
pub fn merge_params(
    base: LinkLonkParams,
    overrides: Option<&PersonalizationParams>,
) -> LinkLonkParams {
    let Some(o) = overrides else {
        return base;
    };

    LinkLonkParams {
        max_user_likes: o.max_user_likes.unwrap_or(base.max_user_likes),
        max_sources_per_post: o.max_sources_per_post.unwrap_or(base.max_sources_per_post),
        max_total_sources: o.max_total_sources.unwrap_or(base.max_total_sources),
        specificity_power: o.specificity_power.unwrap_or(base.specificity_power),
        popularity_power: o.popularity_power.unwrap_or(base.popularity_power),
        num_paths_power: o.num_paths_power.unwrap_or(base.num_paths_power),
        recency_half_life_hours: o
            .recency_half_life_hours
            .unwrap_or(base.recency_half_life_hours),
        use_author_affinity: o.use_author_affinity,
        seed_sample_pool: o.seed_sample_pool.unwrap_or(base.seed_sample_pool),
        corater_decay: o.corater_decay.unwrap_or(base.corater_decay),
        ..base
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_params() {
        let params = LinkLonkParams::default();
        assert_eq!(params.max_user_likes, 500);
        assert_eq!(params.time_window_hours, 168.0);
        assert_eq!(params.result_ttl_seconds, 300);
    }

    #[test]
    fn test_presets() {
        let discovery = get_preset("discovery");
        assert_eq!(discovery.max_user_likes, 200);
        assert_eq!(discovery.specificity_power, 1.5);

        let stable = get_preset("stable");
        assert_eq!(stable.max_user_likes, 1000);
        assert_eq!(stable.recency_half_life_hours, 168.0);

        let fast = get_preset("fast");
        assert_eq!(fast.max_sources_per_post, 50);
        assert_eq!(fast.result_ttl_seconds, 600);
    }

    #[test]
    fn test_get_preset_unknown() {
        let params = get_preset("unknown");
        assert_eq!(params.max_user_likes, 500); // Default
    }

    #[test]
    fn test_lua_args() {
        let params = LinkLonkParams::default();
        let args = params.to_lua_args(1000000.0, true, false);
        assert_eq!(args.len(), 17);
        assert_eq!(args[0], "500"); // max_user_likes
        assert_eq!(args[4], "604800"); // time_window_seconds (7 * 24 * 3600)
        assert_eq!(args[11], "1"); // use_precomputed_colikes
        assert_eq!(args[13], "0"); // use_algo_likers
        assert_eq!(args[14], "0.3"); // num_paths_power
        assert_eq!(args[15], "0"); // seed_sample_pool (disabled)
        assert_eq!(args[16], "0"); // corater_decay (disabled)
    }
}
