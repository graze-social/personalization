//! ML feature types for post ranking.
//!
//! `PostFeatures` captures per-post scoring signals collected during the LinkLonk
//! scoring pass. Combined with feed-level context, they form the 24-element
//! feature vector fed into the ONNX re-ranker and logged to ClickHouse for training.
//!
//! Feature layout (matches `feed_impressions` column order):
//!   [0..10]  scoring features (from hot scoring loop)
//!   [11..15] network features (from source_weights before scoring)
//!   [16..23] context features (added by feed handler at serve time)

use std::collections::HashMap;

/// Number of features in the ONNX input tensor. Keep in sync with training script.
pub const FEATURE_COUNT: usize = 24;

/// Feature names in order — used for training schema documentation and ONNX export.
pub const FEATURE_NAMES: [&str; FEATURE_COUNT] = [
    "raw_score",
    "final_score",
    "num_paths",
    "liker_count",
    "popularity_penalty",
    "paths_boost",
    "max_contribution",
    "score_concentration",
    "newest_like_age_hours",
    "oldest_like_age_hours",
    "was_liker_cache_hit",
    "coliker_count",
    "top_coliker_weight",
    "top5_weight_sum",
    "mean_coliker_weight",
    "weight_concentration",
    "depth",
    "user_like_count",
    "user_segment",
    "richness_ratio",
    "hour_of_day",
    "day_of_week",
    "is_first_page",
    "is_holdout",
];

/// Per-post feature vector collected during the LinkLonk scoring pass.
///
/// Fields [0..15] are filled by `Scorer::score()`.
/// Fields [16..23] are filled by the feed handler using `to_array()`.
#[derive(Debug, Clone, Default)]
pub struct PostFeatures {
    // --- Scoring features (from the hot loop over post likers) ---

    /// Raw weighted co-liker sum before boosts/penalties.
    pub raw_score: f32,
    /// Final score after paths_boost × popularity_penalty.
    pub final_score: f32,
    /// Number of distinct co-likers who matched this post.
    pub num_paths: u16,
    /// Total global liker count for this post (from algo counts hash).
    pub liker_count: u32,
    /// Popularity penalty applied: (1/liker_count)^(pop_power*0.5).
    pub popularity_penalty: f32,
    /// Paths exponent boost: num_paths^num_paths_power.
    pub paths_boost: f32,
    /// Largest single co-liker contribution in the inner sum.
    pub max_contribution: f32,
    /// max_contribution / raw_score — how concentrated the signal is.
    pub score_concentration: f32,
    /// Age of the most recent matching co-liker like (hours).
    pub newest_like_age_hours: f32,
    /// Age of the oldest matching co-liker like (hours).
    pub oldest_like_age_hours: f32,
    /// True if liker data came from the in-process LRU cache (vs Redis fetch).
    pub was_liker_cache_hit: bool,

    // --- Network features (computed from source_weights before scoring) ---

    /// Total co-likers in the user's co-liker graph.
    pub coliker_count: u16,
    /// Weight of the highest-weight co-liker.
    pub top_coliker_weight: f32,
    /// Sum of top-5 co-liker weights.
    pub top5_weight_sum: f32,
    /// Mean co-liker weight.
    pub mean_coliker_weight: f32,
    /// top1 / mean — Herfindahl-style concentration of the co-liker graph.
    pub weight_concentration: f32,
}

impl PostFeatures {
    /// Build the full 24-element `[f32; FEATURE_COUNT]` array including context.
    ///
    /// Call this in the feed handler after blending, when `depth` and user context
    /// are known. The returned array is fed directly into `OnnxRanker::rerank()`.
    ///
    /// # Arguments
    /// * `depth`               — 0-based position in the served feed
    /// * `user_like_count`     — total user likes in retention window
    /// * `user_segment_encoded`— 0=cold, 1=warm, 2=active
    /// * `richness_ratio`      — posts_scored / posts_checked
    /// * `hour_of_day`         — UTC hour [0–23]
    /// * `day_of_week`         — 0=Monday … 6=Sunday
    /// * `is_first_page`       — true if cursor is at the first page
    /// * `is_holdout`          — true if this request used holdout Thompson params
    #[allow(clippy::too_many_arguments)]
    pub fn to_array(
        &self,
        depth: u8,
        user_like_count: u32,
        user_segment_encoded: u8,
        richness_ratio: f32,
        hour_of_day: u8,
        day_of_week: u8,
        is_first_page: bool,
        is_holdout: bool,
    ) -> [f32; FEATURE_COUNT] {
        [
            self.raw_score,
            self.final_score,
            self.num_paths as f32,
            self.liker_count as f32,
            self.popularity_penalty,
            self.paths_boost,
            self.max_contribution,
            self.score_concentration,
            self.newest_like_age_hours,
            self.oldest_like_age_hours,
            if self.was_liker_cache_hit { 1.0 } else { 0.0 },
            self.coliker_count as f32,
            self.top_coliker_weight,
            self.top5_weight_sum,
            self.mean_coliker_weight,
            self.weight_concentration,
            depth as f32,
            user_like_count as f32,
            user_segment_encoded as f32,
            richness_ratio,
            hour_of_day as f32,
            day_of_week as f32,
            if is_first_page { 1.0 } else { 0.0 },
            if is_holdout { 1.0 } else { 0.0 },
        ]
    }
}

/// Network-level statistics computed from the user's co-liker weight map.
///
/// Computed once per scoring run (before the per-post hot loop) and copied
/// into every `PostFeatures` produced in that run.
#[derive(Debug, Clone, Default)]
pub struct NetworkStats {
    pub coliker_count: u16,
    pub top_coliker_weight: f32,
    pub top5_weight_sum: f32,
    pub mean_coliker_weight: f32,
    /// top1 / mean; higher = more concentrated signal from a few super-fans.
    pub weight_concentration: f32,
}

impl NetworkStats {
    /// Compute network stats from the co-liker `source_weights` map.
    ///
    /// O(n log n) due to sorting; n is typically < 10 000 co-likers.
    pub fn from_source_weights(source_weights: &HashMap<String, f64>) -> Self {
        let count = source_weights.len();
        if count == 0 {
            return Self::default();
        }

        let mut weights: Vec<f64> = source_weights.values().copied().collect();
        weights.sort_by(|a, b| b.partial_cmp(a).unwrap_or(std::cmp::Ordering::Equal));

        let top1 = weights[0];
        let top5_sum: f64 = weights.iter().take(5).sum();
        let mean = weights.iter().sum::<f64>() / count as f64;
        let concentration = if mean > 0.0 { top1 / mean } else { 0.0 };

        Self {
            coliker_count: count.min(u16::MAX as usize) as u16,
            top_coliker_weight: top1 as f32,
            top5_weight_sum: top5_sum as f32,
            mean_coliker_weight: mean as f32,
            weight_concentration: concentration as f32,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_features() -> PostFeatures {
        PostFeatures {
            raw_score: 2.5,
            final_score: 1.8,
            num_paths: 4,
            liker_count: 200,
            popularity_penalty: 0.9,
            paths_boost: 1.4,
            max_contribution: 1.0,
            score_concentration: 0.4,
            newest_like_age_hours: 3.0,
            oldest_like_age_hours: 48.0,
            was_liker_cache_hit: true,
            coliker_count: 12,
            top_coliker_weight: 3.0,
            top5_weight_sum: 10.0,
            mean_coliker_weight: 1.5,
            weight_concentration: 2.0,
        }
    }

    // ─── PostFeatures::to_array ───────────────────────────────────────────────

    #[test]
    fn test_to_array_length() {
        let arr = make_features().to_array(0, 0, 0, 0.0, 0, 0, false, false);
        assert_eq!(arr.len(), FEATURE_COUNT);
    }

    #[test]
    fn test_to_array_scoring_fields() {
        let f = make_features();
        let arr = f.to_array(0, 0, 0, 0.0, 0, 0, false, false);
        // Indices match FEATURE_NAMES order
        assert_eq!(arr[0], f.raw_score);
        assert_eq!(arr[1], f.final_score);
        assert_eq!(arr[2], f.num_paths as f32);
        assert_eq!(arr[3], f.liker_count as f32);
        assert_eq!(arr[4], f.popularity_penalty);
        assert_eq!(arr[5], f.paths_boost);
        assert_eq!(arr[6], f.max_contribution);
        assert_eq!(arr[7], f.score_concentration);
        assert_eq!(arr[8], f.newest_like_age_hours);
        assert_eq!(arr[9], f.oldest_like_age_hours);
        assert_eq!(arr[10], 1.0); // was_liker_cache_hit = true
    }

    #[test]
    fn test_to_array_network_fields() {
        let f = make_features();
        let arr = f.to_array(0, 0, 0, 0.0, 0, 0, false, false);
        assert_eq!(arr[11], f.coliker_count as f32);
        assert_eq!(arr[12], f.top_coliker_weight);
        assert_eq!(arr[13], f.top5_weight_sum);
        assert_eq!(arr[14], f.mean_coliker_weight);
        assert_eq!(arr[15], f.weight_concentration);
    }

    #[test]
    fn test_to_array_context_fields() {
        let f = make_features();
        let arr = f.to_array(7, 1234, 2, 0.85, 14, 3, true, false);
        assert_eq!(arr[16], 7.0);    // depth
        assert_eq!(arr[17], 1234.0); // user_like_count
        assert_eq!(arr[18], 2.0);    // user_segment_encoded (active)
        assert_eq!(arr[19], 0.85);   // richness_ratio
        assert_eq!(arr[20], 14.0);   // hour_of_day
        assert_eq!(arr[21], 3.0);    // day_of_week
        assert_eq!(arr[22], 1.0);    // is_first_page = true
        assert_eq!(arr[23], 0.0);    // is_holdout = false
    }

    #[test]
    fn test_to_array_bool_encoding() {
        let mut f = make_features();
        f.was_liker_cache_hit = false;
        let arr_false = f.to_array(0, 0, 0, 0.0, 0, 0, false, true);
        assert_eq!(arr_false[10], 0.0); // was_liker_cache_hit = false
        assert_eq!(arr_false[22], 0.0); // is_first_page = false
        assert_eq!(arr_false[23], 1.0); // is_holdout = true
    }

    #[test]
    fn test_to_array_default_is_all_zeros() {
        let arr = PostFeatures::default().to_array(0, 0, 0, 0.0, 0, 0, false, false);
        assert!(arr.iter().all(|&v| v == 0.0));
    }

    #[test]
    fn test_feature_names_length_matches_feature_count() {
        assert_eq!(FEATURE_NAMES.len(), FEATURE_COUNT);
    }

    // ─── NetworkStats::from_source_weights ───────────────────────────────────

    #[test]
    fn test_network_stats_empty() {
        let stats = NetworkStats::from_source_weights(&HashMap::new());
        assert_eq!(stats.coliker_count, 0);
        assert_eq!(stats.top_coliker_weight, 0.0);
        assert_eq!(stats.top5_weight_sum, 0.0);
        assert_eq!(stats.mean_coliker_weight, 0.0);
        assert_eq!(stats.weight_concentration, 0.0);
    }

    #[test]
    fn test_network_stats_single() {
        let mut map = HashMap::new();
        map.insert("a".to_string(), 5.0_f64);
        let stats = NetworkStats::from_source_weights(&map);
        assert_eq!(stats.coliker_count, 1);
        assert_eq!(stats.top_coliker_weight, 5.0);
        assert_eq!(stats.top5_weight_sum, 5.0);
        assert_eq!(stats.mean_coliker_weight, 5.0);
        // top1 / mean = 5 / 5 = 1.0
        assert!((stats.weight_concentration - 1.0).abs() < 1e-5);
    }

    #[test]
    fn test_network_stats_multiple() {
        let weights = [10.0_f64, 8.0, 6.0, 4.0, 2.0, 1.0];
        let map: HashMap<String, f64> = weights
            .iter()
            .enumerate()
            .map(|(i, &w)| (format!("u{}", i), w))
            .collect();
        let stats = NetworkStats::from_source_weights(&map);
        assert_eq!(stats.coliker_count, 6);
        assert_eq!(stats.top_coliker_weight, 10.0);
        // Top-5 = 10+8+6+4+2 = 30
        assert!((stats.top5_weight_sum - 30.0).abs() < 1e-4);
        // Mean = (10+8+6+4+2+1)/6 = 31/6 ≈ 5.167
        let expected_mean = 31.0_f32 / 6.0;
        assert!((stats.mean_coliker_weight - expected_mean).abs() < 1e-3);
        // concentration = top1 / mean = 10 / (31/6) ≈ 1.935
        let expected_conc = 10.0_f32 / expected_mean;
        assert!((stats.weight_concentration - expected_conc).abs() < 1e-3);
    }

    #[test]
    fn test_network_stats_fewer_than_five() {
        // When there are < 5 co-likers, top5_weight_sum == total weight sum
        let map: HashMap<String, f64> = [("a", 3.0_f64), ("b", 2.0)]
            .iter()
            .map(|(k, v)| (k.to_string(), *v))
            .collect();
        let stats = NetworkStats::from_source_weights(&map);
        assert_eq!(stats.coliker_count, 2);
        assert_eq!(stats.top_coliker_weight, 3.0);
        assert!((stats.top5_weight_sum - 5.0).abs() < 1e-5);
    }

    #[test]
    fn test_network_stats_u16_clamp() {
        // Verify coliker_count clamps at u16::MAX
        let map: HashMap<String, f64> = (0..=(u16::MAX as usize + 5))
            .map(|i| (format!("u{}", i), 1.0_f64))
            .collect();
        let stats = NetworkStats::from_source_weights(&map);
        assert_eq!(stats.coliker_count, u16::MAX);
    }
}
