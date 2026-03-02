//! Thompson Sampling for Parameter Optimization.
//!
//! This module implements independent Thompson Sampling bandits for optimizing
//! scoring parameters. Each parameter gets its own bandit per feed, allowing
//! the system to find optimal values through exploration/exploitation.
//!
//! Key advantages:
//! - Can find non-monotonic optima (sweet spots in the middle)
//! - Simpler implementation, no matrix math
//! - Independent bandits scale linearly with parameters
//!
//! A/B Testing:
//! - 10% of requests are "holdout" (control group) using default config values
//! - 90% of requests are "treatment" using adaptive Thompson Sampling
//! - Both groups are logged for comparison

use std::collections::{HashMap, HashSet};

use graze_common::models::{FeedSuccessConfig, HoldoutParams, ThompsonSearchSpace};
use parking_lot::RwLock;
use rand::prelude::*;

/// Configuration for Thompson Sampling learner.
#[derive(Debug, Clone)]
pub struct ThompsonConfig {
    /// Holdout rate for A/B testing (control group uses defaults).
    pub holdout_rate: f64,
    /// Min post likes options to explore.
    pub min_likes_options: Vec<usize>,
    pub min_likes_default: usize,
    /// Max likers per post options.
    pub max_likers_options: Vec<usize>,
    pub max_likers_default: usize,
    /// Max total sources options.
    pub max_sources_options: Vec<usize>,
    pub max_sources_default: usize,
    /// Max algo checks options.
    pub max_checks_options: Vec<usize>,
    pub max_checks_default: usize,
    /// Min co-likes options.
    pub min_colikes_options: Vec<usize>,
    pub min_colikes_default: usize,
    /// Max user likes options.
    pub max_user_likes_options: Vec<usize>,
    pub max_user_likes_default: usize,
    /// Max sources per post options.
    pub max_sources_per_post_options: Vec<usize>,
    pub max_sources_per_post_default: usize,
    /// Seed sample pool options (0 = disabled).
    pub seed_pool_options: Vec<usize>,
    pub seed_pool_default: usize,
    /// Corater decay options (percentages: 0 = disabled, 20 = 0.2 decay, etc.).
    pub corater_decay_options: Vec<usize>,
    pub corater_decay_default: usize,
    /// Prior strength for Beta distribution.
    pub prior_alpha: f64,
    pub prior_beta: f64,
    /// Exploration probability (epsilon-greedy on top of Thompson).
    pub exploration_prob: f64,
}

impl Default for ThompsonConfig {
    fn default() -> Self {
        Self {
            holdout_rate: 0.10,
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
            prior_alpha: 1.0,
            prior_beta: 1.0,
            exploration_prob: 0.05,
        }
    }
}

/// Parameters selected by Thompson Sampling (or defaults for holdout).
#[derive(Debug, Clone)]
pub struct SelectedParams {
    pub min_post_likes: usize,
    pub max_likers_per_post: usize,
    pub max_total_sources: usize,
    pub max_algo_checks: usize,
    pub min_co_likes: usize,
    pub max_user_likes: usize,
    pub max_sources_per_post: usize,
    /// Seed sample pool size (0 = disabled).
    pub seed_sample_pool: usize,
    /// Corater decay as percentage (0 = disabled, 20 = 0.2 decay).
    pub corater_decay_pct: usize,
    /// True = control group (defaults), False = treatment (adaptive).
    pub is_holdout: bool,
    /// True = random exploration, False = Thompson selection.
    pub is_exploration: bool,
    /// True = fixed params from feed treatment_params override (don't record for learning).
    pub is_treatment_override: bool,
}

/// Beta distribution parameters for a single arm.
#[derive(Debug, Clone)]
struct BetaArm {
    alpha: f64,
    beta: f64,
}

impl BetaArm {
    fn new(prior_alpha: f64, prior_beta: f64) -> Self {
        Self {
            alpha: prior_alpha,
            beta: prior_beta,
        }
    }

    /// Sample from Beta distribution using inverse transform sampling.
    fn sample(&self, rng: &mut impl Rng) -> f64 {
        // Simple approximation: use gamma distribution ratio
        let gamma_alpha = gamma_sample(rng, self.alpha);
        let gamma_beta = gamma_sample(rng, self.beta);
        gamma_alpha / (gamma_alpha + gamma_beta)
    }

    /// Update with observation.
    fn update(&mut self, success: bool) {
        if success {
            self.alpha += 1.0;
        } else {
            self.beta += 1.0;
        }
    }
}

/// Simple gamma distribution sampler (Marsaglia and Tsang's method).
fn gamma_sample(rng: &mut impl Rng, shape: f64) -> f64 {
    if shape < 1.0 {
        return gamma_sample(rng, shape + 1.0) * rng.gen::<f64>().powf(1.0 / shape);
    }

    let d = shape - 1.0 / 3.0;
    let c = 1.0 / (9.0 * d).sqrt();

    loop {
        let x: f64 = rng.gen::<f64>() * 2.0 - 1.0;
        let u: f64 = rng.gen();
        let v = (1.0 + c * x).powi(3);

        if v > 0.0 && u.ln() < 0.5 * x * x + d - d * v + d * v.ln() {
            return d * v;
        }
    }
}

/// Multi-armed bandit for a single parameter.
struct ParameterBandit {
    arms: Vec<(usize, BetaArm)>, // (value, arm)
    default_value: usize,
}

impl ParameterBandit {
    fn new(options: &[usize], default: usize, prior_alpha: f64, prior_beta: f64) -> Self {
        let arms = options
            .iter()
            .map(|&v| (v, BetaArm::new(prior_alpha, prior_beta)))
            .collect();
        Self {
            arms,
            default_value: default,
        }
    }

    /// Select a value using Thompson Sampling.
    fn select(&self, rng: &mut impl Rng) -> usize {
        let mut best_value = self.default_value;
        let mut best_sample = f64::NEG_INFINITY;

        for (value, arm) in &self.arms {
            let sample = arm.sample(rng);
            if sample > best_sample {
                best_sample = sample;
                best_value = *value;
            }
        }

        best_value
    }

    /// Random exploration selection.
    fn explore(&self, rng: &mut impl Rng) -> usize {
        self.arms[rng.gen_range(0..self.arms.len())].0
    }

    /// Update the arm for a given value.
    fn update(&mut self, value: usize, success: bool) {
        for (v, arm) in &mut self.arms {
            if *v == value {
                arm.update(success);
                break;
            }
        }
    }
}

/// Thompson Sampling learner for parameter optimization.
pub struct ThompsonLearner {
    config: ThompsonConfig,
    /// Bandits for each (algo_id, parameter_name).
    bandits: RwLock<HashMap<(i32, &'static str), ParameterBandit>>,
    /// Statistics tracking.
    stats: RwLock<ThompsonStats>,
}

#[derive(Debug, Default)]
struct ThompsonStats {
    total_requests: u64,
    holdout_requests: u64,
    exploration_requests: u64,
    treatment_requests: u64,
    observations_recorded: u64,
}

impl ThompsonLearner {
    /// Create a new Thompson learner with default config.
    pub fn new() -> Self {
        Self::with_config(ThompsonConfig::default())
    }

    /// Create a new Thompson learner with custom config.
    pub fn with_config(config: ThompsonConfig) -> Self {
        Self {
            config,
            bandits: RwLock::new(HashMap::new()),
            stats: RwLock::new(ThompsonStats::default()),
        }
    }

    /// Select parameters for a request.
    pub fn select_params(&self, algo_id: i32) -> SelectedParams {
        self.select_params_with_holdout_and_search_space(algo_id, None, None, None)
    }

    /// Select parameters for a request, with optional per-feed holdout and treatment overrides.
    /// When treatment_params_override is Some, bypasses Thompson Sampling for non-holdout requests.
    #[allow(clippy::too_many_arguments)]
    pub fn select_params_with_holdout(
        &self,
        algo_id: i32,
        holdout_params_override: Option<&HoldoutParams>,
        treatment_params_override: Option<&HoldoutParams>,
    ) -> SelectedParams {
        self.select_params_with_holdout_and_search_space(
            algo_id,
            holdout_params_override,
            treatment_params_override,
            None,
        )
    }

    /// Select parameters with optional search space override.
    /// search_space_override: algo-level or global; if None, use hardcoded ThompsonConfig.
    pub fn select_params_with_holdout_and_search_space(
        &self,
        algo_id: i32,
        holdout_params_override: Option<&HoldoutParams>,
        treatment_params_override: Option<&HoldoutParams>,
        search_space_override: Option<&ThompsonSearchSpace>,
    ) -> SelectedParams {
        let mut rng = thread_rng();
        let mut stats = self.stats.write();
        stats.total_requests += 1;

        // Resolve options from search_space or config
        let (min_likes_opts, min_likes_def) = search_space_override
            .map(|s| (s.min_likes_options.as_slice(), s.min_likes_default))
            .unwrap_or((
                self.config.min_likes_options.as_slice(),
                self.config.min_likes_default,
            ));
        let (max_likers_opts, max_likers_def) = search_space_override
            .map(|s| (s.max_likers_options.as_slice(), s.max_likers_default))
            .unwrap_or((
                self.config.max_likers_options.as_slice(),
                self.config.max_likers_default,
            ));
        let (max_sources_opts, max_sources_def) = search_space_override
            .map(|s| (s.max_sources_options.as_slice(), s.max_sources_default))
            .unwrap_or((
                self.config.max_sources_options.as_slice(),
                self.config.max_sources_default,
            ));
        let (max_checks_opts, max_checks_def) = search_space_override
            .map(|s| (s.max_checks_options.as_slice(), s.max_checks_default))
            .unwrap_or((
                self.config.max_checks_options.as_slice(),
                self.config.max_checks_default,
            ));
        let (min_colikes_opts, min_colikes_def) = search_space_override
            .map(|s| (s.min_colikes_options.as_slice(), s.min_colikes_default))
            .unwrap_or((
                self.config.min_colikes_options.as_slice(),
                self.config.min_colikes_default,
            ));
        let (max_user_likes_opts, max_user_likes_def) = search_space_override
            .map(|s| (s.max_user_likes_options.as_slice(), s.max_user_likes_default))
            .unwrap_or((
                self.config.max_user_likes_options.as_slice(),
                self.config.max_user_likes_default,
            ));
        let (max_src_per_post_opts, max_src_per_post_def) = search_space_override
            .map(|s| {
                (
                    s.max_sources_per_post_options.as_slice(),
                    s.max_sources_per_post_default,
                )
            })
            .unwrap_or((
                self.config.max_sources_per_post_options.as_slice(),
                self.config.max_sources_per_post_default,
            ));
        let (seed_pool_opts, seed_pool_def) = search_space_override
            .map(|s| (s.seed_pool_options.as_slice(), s.seed_pool_default))
            .unwrap_or((
                self.config.seed_pool_options.as_slice(),
                self.config.seed_pool_default,
            ));
        let (corater_decay_opts, corater_decay_def) = search_space_override
            .map(|s| (s.corater_decay_options.as_slice(), s.corater_decay_default))
            .unwrap_or((
                self.config.corater_decay_options.as_slice(),
                self.config.corater_decay_default,
            ));

        // Holdout defaults use resolved values
        let holdout_defaults = (
            min_likes_def,
            max_likers_def,
            max_sources_def,
            max_checks_def,
            min_colikes_def,
            max_user_likes_def,
            max_src_per_post_def,
            seed_pool_def,
            corater_decay_def,
        );

        // A/B test: holdout group uses defaults or feed-specific override
        if rng.gen::<f64>() < self.config.holdout_rate {
            stats.holdout_requests += 1;
            return if let Some(holdout) = holdout_params_override {
                SelectedParams {
                    min_post_likes: holdout.min_post_likes,
                    max_likers_per_post: holdout.max_likers_per_post,
                    max_total_sources: holdout.max_total_sources,
                    max_algo_checks: holdout.max_algo_checks,
                    min_co_likes: holdout.min_co_likes,
                    max_user_likes: holdout.max_user_likes,
                    max_sources_per_post: holdout.max_sources_per_post,
                    seed_sample_pool: holdout_defaults.7,
                    corater_decay_pct: holdout_defaults.8,
                    is_holdout: true,
                    is_exploration: false,
                    is_treatment_override: false,
                }
            } else {
                SelectedParams {
                    min_post_likes: holdout_defaults.0,
                    max_likers_per_post: holdout_defaults.1,
                    max_total_sources: holdout_defaults.2,
                    max_algo_checks: holdout_defaults.3,
                    min_co_likes: holdout_defaults.4,
                    max_user_likes: holdout_defaults.5,
                    max_sources_per_post: holdout_defaults.6,
                    seed_sample_pool: holdout_defaults.7,
                    corater_decay_pct: holdout_defaults.8,
                    is_holdout: true,
                    is_exploration: false,
                    is_treatment_override: false,
                }
            };
        }

        // Treatment override: use feed-specific params instead of Thompson Sampling
        if let Some(treatment) = treatment_params_override {
            stats.treatment_requests += 1;
            return SelectedParams {
                min_post_likes: treatment.min_post_likes,
                max_likers_per_post: treatment.max_likers_per_post,
                max_total_sources: treatment.max_total_sources,
                max_algo_checks: treatment.max_algo_checks,
                min_co_likes: treatment.min_co_likes,
                max_user_likes: treatment.max_user_likes,
                max_sources_per_post: treatment.max_sources_per_post,
                seed_sample_pool: holdout_defaults.7,
                corater_decay_pct: holdout_defaults.8,
                is_holdout: false,
                is_exploration: false,
                is_treatment_override: true,
            };
        }

        // Exploration mode
        let is_exploration = rng.gen::<f64>() < self.config.exploration_prob;
        if is_exploration {
            stats.exploration_requests += 1;
        } else {
            stats.treatment_requests += 1;
        }

        // Get or create bandits for this algo
        let mut bandits = self.bandits.write();

        let params: [(&str, &[usize], usize); 9] = [
            ("min_likes", min_likes_opts, min_likes_def),
            ("max_likers", max_likers_opts, max_likers_def),
            ("max_sources", max_sources_opts, max_sources_def),
            ("max_checks", max_checks_opts, max_checks_def),
            ("min_colikes", min_colikes_opts, min_colikes_def),
            ("max_user_likes", max_user_likes_opts, max_user_likes_def),
            ("max_src_per_post", max_src_per_post_opts, max_src_per_post_def),
            ("seed_pool", seed_pool_opts, seed_pool_def),
            ("corater_decay", corater_decay_opts, corater_decay_def),
        ];

        let mut selected = vec![];
        for (name, options, default) in params {
            let bandit = bandits.entry((algo_id, name)).or_insert_with(|| {
                ParameterBandit::new(
                    options,
                    default,
                    self.config.prior_alpha,
                    self.config.prior_beta,
                )
            });

            let value = if is_exploration {
                bandit.explore(&mut rng)
            } else {
                bandit.select(&mut rng)
            };
            selected.push(value);
        }

        SelectedParams {
            min_post_likes: selected[0],
            max_likers_per_post: selected[1],
            max_total_sources: selected[2],
            max_algo_checks: selected[3],
            min_co_likes: selected[4],
            max_user_likes: selected[5],
            max_sources_per_post: selected[6],
            seed_sample_pool: selected[7],
            corater_decay_pct: selected[8],
            is_holdout: false,
            is_exploration,
            is_treatment_override: false,
        }
    }

    /// Clear all bandits for a specific algorithm.
    pub fn clear_bandits_for_algo(&self, algo_id: i32) {
        let mut bandits = self.bandits.write();
        bandits.retain(|(aid, _), _| *aid != algo_id);
    }

    /// Clear bandits for algos that use global config (i.e. NOT in algo_ids_with_algo_config).
    pub fn clear_bandits_for_global_use(&self, algo_ids_with_algo_config: &[i32]) {
        let set: HashSet<i32> = algo_ids_with_algo_config.iter().copied().collect();
        let mut bandits = self.bandits.write();
        bandits.retain(|(aid, _), _| set.contains(aid));
    }

    /// Convert ProvenanceParams (from decoded feedContext) to SelectedParams.
    /// Uses ThompsonConfig defaults for any missing fields. is_holdout and is_exploration
    /// are set to false since we only record for non-holdout observation paths.
    pub fn selected_params_from_provenance(
        &self,
        p: &graze_common::models::ProvenanceParams,
    ) -> SelectedParams {
        let c = &self.config;
        SelectedParams {
            min_post_likes: p.min_post_likes.unwrap_or(c.min_likes_default),
            max_likers_per_post: p.max_likers_per_post.unwrap_or(c.max_likers_default),
            max_total_sources: p.max_total_sources.unwrap_or(c.max_sources_default),
            max_algo_checks: p.max_algo_checks.unwrap_or(c.max_checks_default),
            min_co_likes: p.min_co_likes.unwrap_or(c.min_colikes_default),
            max_user_likes: p.max_user_likes.unwrap_or(c.max_user_likes_default),
            max_sources_per_post: p.max_sources_per_post.unwrap_or(c.max_sources_per_post_default),
            seed_sample_pool: p.seed_sample_pool.unwrap_or(c.seed_pool_default),
            corater_decay_pct: p.corater_decay_pct.unwrap_or(c.corater_decay_default),
            is_holdout: false,
            is_exploration: false,
            is_treatment_override: false,
        }
    }

    /// Record an observation (success/failure) for selected parameters.
    pub fn record_observation(&self, algo_id: i32, params: &SelectedParams, success: bool) {
        if params.is_holdout || params.is_treatment_override {
            return; // Don't learn from holdout or fixed treatment override
        }

        let mut bandits = self.bandits.write();
        let mut stats = self.stats.write();
        stats.observations_recorded += 1;

        let param_values = [
            ("min_likes", params.min_post_likes),
            ("max_likers", params.max_likers_per_post),
            ("max_sources", params.max_total_sources),
            ("max_checks", params.max_algo_checks),
            ("min_colikes", params.min_co_likes),
            ("max_user_likes", params.max_user_likes),
            ("max_src_per_post", params.max_sources_per_post),
            ("seed_pool", params.seed_sample_pool),
            ("corater_decay", params.corater_decay_pct),
        ];

        for (name, value) in param_values {
            if let Some(bandit) = bandits.get_mut(&(algo_id, name)) {
                bandit.update(value, success);
            }
        }
    }

    /// Get statistics.
    pub fn get_stats(&self) -> HashMap<String, u64> {
        let stats = self.stats.read();
        let mut result = HashMap::new();
        result.insert("total_requests".to_string(), stats.total_requests);
        result.insert("holdout_requests".to_string(), stats.holdout_requests);
        result.insert(
            "exploration_requests".to_string(),
            stats.exploration_requests,
        );
        result.insert("treatment_requests".to_string(), stats.treatment_requests);
        result.insert(
            "observations_recorded".to_string(),
            stats.observations_recorded,
        );
        result
    }

    /// Get detailed bandit arm statistics for an algorithm.
    /// Returns a map of parameter_name -> [(value, alpha, beta, win_rate)]
    pub fn get_bandit_stats(&self, algo_id: i32) -> HashMap<String, Vec<(usize, f64, f64, f64)>> {
        let bandits = self.bandits.read();
        let mut result: HashMap<String, Vec<(usize, f64, f64, f64)>> = HashMap::new();

        let param_names = [
            "min_likes",
            "max_likers",
            "max_sources",
            "max_checks",
            "min_colikes",
            "max_user_likes",
            "max_src_per_post",
            "seed_pool",
            "corater_decay",
        ];

        for name in param_names {
            if let Some(bandit) = bandits.get(&(algo_id, name)) {
                let arms: Vec<(usize, f64, f64, f64)> = bandit
                    .arms
                    .iter()
                    .map(|(value, arm)| {
                        let win_rate = arm.alpha / (arm.alpha + arm.beta);
                        (*value, arm.alpha, arm.beta, win_rate)
                    })
                    .collect();
                result.insert(name.to_string(), arms);
            }
        }

        result
    }
}

/// Outcome of a feed request, used to evaluate success for Thompson learning.
///
/// Success is determined by three criteria:
/// - (a) At least 60% of posts are personalized (post-level or author-level LinkLonk)
/// - (b) Scoring was "rich" - we exhausted a reasonable candidate set
/// - (c) Response was fast (under 500ms threshold)
#[derive(Debug, Clone)]
pub struct FeedOutcome {
    /// Total posts returned in the feed.
    pub total_posts: usize,
    /// Posts from post-level personalization.
    pub personalized_posts: usize,
    /// Posts from author affinity (coarse LinkLonk).
    pub author_affinity_posts: usize,
    /// Posts from fallback tranches (popular/velocity/discovery).
    pub fallback_posts: usize,
    /// Number of candidate posts checked during scoring.
    pub posts_checked: usize,
    /// Number of posts that received non-zero scores.
    pub posts_scored: usize,
    /// Number of co-likers used in scoring.
    pub colikers_used: usize,
    /// Total response time in milliseconds.
    pub response_time_ms: f64,
}

impl FeedOutcome {
    /// Evaluate whether this feed outcome counts as a "success" for Thompson learning.
    ///
    /// Returns (success, details) where details explains why it passed or failed.
    pub fn evaluate(&self, config: &FeedSuccessConfig) -> (bool, FeedOutcomeDetails) {
        // (a) Personalization ratio: (post-level + author-affinity) / total
        let personalized_total = self.personalized_posts + self.author_affinity_posts;
        let personalization_ratio = if self.total_posts > 0 {
            personalized_total as f64 / self.total_posts as f64
        } else {
            0.0
        };
        let personalization_passed = personalization_ratio >= config.min_personalization_ratio;

        // (b) Richness: did we exhaust a reasonable candidate set?
        let posts_checked_passed = self.posts_checked >= config.min_posts_checked;
        let posts_scored_passed = self.posts_scored >= config.min_posts_scored;
        let richness_passed = posts_checked_passed && posts_scored_passed;

        // (c) Speed: fast response
        let speed_passed = self.response_time_ms <= config.max_response_time_ms;

        // All three criteria must pass
        let success = personalization_passed && richness_passed && speed_passed;

        let details = FeedOutcomeDetails {
            personalization_ratio,
            personalization_passed,
            posts_checked_passed,
            posts_scored_passed,
            richness_passed,
            speed_passed,
            success,
        };

        (success, details)
    }
}

/// Detailed breakdown of feed outcome evaluation.
#[derive(Debug, Clone)]
pub struct FeedOutcomeDetails {
    pub personalization_ratio: f64,
    pub personalization_passed: bool,
    pub posts_checked_passed: bool,
    pub posts_scored_passed: bool,
    pub richness_passed: bool,
    pub speed_passed: bool,
    pub success: bool,
}

impl Default for ThompsonLearner {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_thompson_config_default() {
        let config = ThompsonConfig::default();
        assert_eq!(config.holdout_rate, 0.10);
        assert_eq!(config.min_likes_default, 5);
        assert_eq!(config.max_likers_default, 30);
        assert_eq!(config.prior_alpha, 1.0);
        assert_eq!(config.prior_beta, 1.0);
    }

    #[test]
    fn test_thompson_learner_creation() {
        let learner = ThompsonLearner::new();
        let stats = learner.get_stats();
        assert_eq!(stats["total_requests"], 0);
    }

    #[test]
    fn test_thompson_select_params() {
        let learner = ThompsonLearner::new();
        let params = learner.select_params(123);

        // Should return valid parameters
        assert!(params.min_post_likes > 0);
        assert!(params.max_likers_per_post > 0);
        assert!(params.max_total_sources > 0);
    }

    #[test]
    fn test_thompson_holdout_rate() {
        // With 100% holdout rate, all requests should be holdout
        let config = ThompsonConfig {
            holdout_rate: 1.0,
            ..Default::default()
        };
        let learner = ThompsonLearner::with_config(config);

        let params = learner.select_params(123);
        assert!(params.is_holdout);

        // Should use default values
        assert_eq!(params.min_post_likes, 5);
        assert_eq!(params.max_likers_per_post, 30);
    }

    #[test]
    fn test_thompson_no_holdout() {
        // With 0% holdout rate, no requests should be holdout
        let config = ThompsonConfig {
            holdout_rate: 0.0,
            exploration_prob: 0.0,
            ..Default::default()
        };
        let learner = ThompsonLearner::with_config(config);

        // Run multiple times, none should be holdout
        for _ in 0..10 {
            let params = learner.select_params(123);
            assert!(!params.is_holdout);
        }
    }

    #[test]
    fn test_thompson_record_observation() {
        let learner = ThompsonLearner::new();

        // Select params first
        let params = learner.select_params(123);

        // Record success
        learner.record_observation(123, &params, true);

        let stats = learner.get_stats();
        // If it was treatment (not holdout), observation should be recorded
        if !params.is_holdout {
            assert!(stats["observations_recorded"] >= 1);
        }
    }

    #[test]
    fn test_thompson_holdout_not_learned() {
        let config = ThompsonConfig {
            holdout_rate: 1.0,
            ..Default::default()
        };
        let learner = ThompsonLearner::with_config(config);

        let params = learner.select_params(123);
        assert!(params.is_holdout);

        // Record observation - should NOT be learned from
        learner.record_observation(123, &params, true);

        let stats = learner.get_stats();
        assert_eq!(stats["observations_recorded"], 0);
    }

    #[test]
    fn test_thompson_stats_tracking() {
        let learner = ThompsonLearner::new();

        // Make some requests
        for _ in 0..100 {
            let _ = learner.select_params(1);
        }

        let stats = learner.get_stats();
        assert_eq!(stats["total_requests"], 100);

        // Holdout + exploration + treatment should equal total
        let sum =
            stats["holdout_requests"] + stats["exploration_requests"] + stats["treatment_requests"];
        assert_eq!(sum, 100);
    }

    #[test]
    fn test_beta_arm_sample_range() {
        let arm = BetaArm::new(1.0, 1.0);
        let mut rng = rand::thread_rng();

        // Sample should be between 0 and 1
        for _ in 0..100 {
            let sample = arm.sample(&mut rng);
            assert!((0.0..=1.0).contains(&sample));
        }
    }

    #[test]
    fn test_beta_arm_update() {
        let mut arm = BetaArm::new(1.0, 1.0);

        arm.update(true);
        assert_eq!(arm.alpha, 2.0);
        assert_eq!(arm.beta, 1.0);

        arm.update(false);
        assert_eq!(arm.alpha, 2.0);
        assert_eq!(arm.beta, 2.0);
    }

    #[test]
    fn test_select_params_with_treatment_override() {
        use graze_common::models::HoldoutParams;

        let treatment = HoldoutParams {
            min_post_likes: 1,
            max_likers_per_post: 100,
            max_total_sources: 20000,
            max_algo_checks: 1000,
            min_co_likes: 1,
            max_user_likes: 750,
            max_sources_per_post: 150,
        };

        let config = ThompsonConfig {
            holdout_rate: 0.0, // 0% holdout so we hit treatment path
            exploration_prob: 0.0, // No exploration
            ..Default::default()
        };
        let learner = ThompsonLearner::with_config(config);

        // With treatment override, we bypass Thompson and get fixed params
        let params = learner.select_params_with_holdout(123, None, Some(&treatment));
        assert!(params.is_treatment_override);
        assert!(!params.is_holdout);
        assert_eq!(params.max_algo_checks, 1000);
        assert_eq!(params.max_total_sources, 20000);
        assert_eq!(params.max_user_likes, 750);
    }

    #[test]
    fn test_select_params_with_holdout_override() {
        use graze_common::models::HoldoutParams;

        let holdout = HoldoutParams {
            min_post_likes: 3,
            max_likers_per_post: 50,
            max_total_sources: 500,
            max_algo_checks: 100,
            min_co_likes: 2,
            max_user_likes: 100,
            max_sources_per_post: 20,
        };

        let config = ThompsonConfig {
            holdout_rate: 1.0,
            ..Default::default()
        };
        let learner = ThompsonLearner::with_config(config);

        let params = learner.select_params_with_holdout(123, Some(&holdout), None);
        assert!(params.is_holdout);
        assert_eq!(params.min_post_likes, 3);
        assert_eq!(params.max_likers_per_post, 50);
        assert_eq!(params.max_total_sources, 500);
        assert_eq!(params.max_algo_checks, 100);
        assert_eq!(params.min_co_likes, 2);
        assert_eq!(params.max_user_likes, 100);
        assert_eq!(params.max_sources_per_post, 20);
    }

    #[test]
    fn test_selected_params_from_provenance() {
        use graze_common::models::ProvenanceParams;

        let learner = ThompsonLearner::new();

        // Full params
        let p = ProvenanceParams {
            min_post_likes: Some(10),
            max_likers_per_post: Some(40),
            max_total_sources: Some(2000),
            max_algo_checks: Some(300),
            min_co_likes: Some(2),
            max_user_likes: Some(300),
            max_sources_per_post: Some(75),
            seed_sample_pool: Some(1000),
            corater_decay_pct: Some(20),
        };
        let selected = learner.selected_params_from_provenance(&p);
        assert_eq!(selected.min_post_likes, 10);
        assert_eq!(selected.max_likers_per_post, 40);
        assert_eq!(selected.max_total_sources, 2000);
        assert_eq!(selected.seed_sample_pool, 1000);
        assert_eq!(selected.corater_decay_pct, 20);
        assert!(!selected.is_holdout);
        assert!(!selected.is_exploration);

        // Partial params - should use defaults for missing
        let p_partial = ProvenanceParams {
            min_post_likes: Some(7),
            max_likers_per_post: None,
            max_total_sources: None,
            max_algo_checks: None,
            min_co_likes: None,
            max_user_likes: Some(400),
            max_sources_per_post: None,
            seed_sample_pool: None,
            corater_decay_pct: None,
        };
        let selected2 = learner.selected_params_from_provenance(&p_partial);
        assert_eq!(selected2.min_post_likes, 7);
        assert_eq!(selected2.max_user_likes, 400);
        assert_eq!(selected2.max_likers_per_post, 30); // default
        assert_eq!(selected2.seed_sample_pool, 0); // default
        assert_eq!(selected2.corater_decay_pct, 0); // default
    }

    #[test]
    fn test_selected_params_fields() {
        let params = SelectedParams {
            min_post_likes: 5,
            max_likers_per_post: 30,
            max_total_sources: 1000,
            max_algo_checks: 500,
            min_co_likes: 1,
            max_user_likes: 500,
            max_sources_per_post: 100,
            seed_sample_pool: 0,
            corater_decay_pct: 0,
            is_holdout: false,
            is_exploration: false,
            is_treatment_override: false,
        };

        assert_eq!(params.min_post_likes, 5);
        assert_eq!(params.seed_sample_pool, 0);
        assert_eq!(params.corater_decay_pct, 0);
        assert!(!params.is_holdout);
        assert!(!params.is_exploration);
    }
}
