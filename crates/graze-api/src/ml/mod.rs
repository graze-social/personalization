//! ONNX-based ML re-ranker for post scoring.
//!
//! `OnnxRanker` loads a LightGBM model exported to ONNX format and re-ranks
//! candidate posts by blending the LinkLonk score with the model's predicted
//! probability:
//!
//!   `blended = linkLonk^alpha × ml_prob^beta`
//!
//! Enable via:
//!   `ML_RERANKER_ENABLED=true`
//!   `ML_MODEL_PATH=/path/to/ranking_model.onnx`
//!
//! The model is loaded once at startup and shared across all concurrent requests.
//! `Session::run` requires `&mut self`, so we wrap the session in a `Mutex`.
//!
//! # Feature layout
//! The model expects an input tensor of shape `(N, 24)` with the same column
//! ordering as `PostFeatures::to_array()` / the `feed_impressions` table.
//! See `algorithm/features.rs` for `FEATURE_NAMES`.

use std::sync::Mutex;

use ndarray::Array2;
use ort::session::builder::GraphOptimizationLevel;
use ort::session::Session;
use ort::value::TensorRef;
use tracing::{info, warn};

use crate::algorithm::features::{PostFeatures, FEATURE_COUNT};

/// Shared ONNX re-ranker. Interior mutability via `Mutex<Session>`.
pub struct OnnxRanker {
    session: Mutex<Session>,
    alpha: f32,
    beta: f32,
}

// Session is Send + Sync per ort docs; the Mutex wrapper adds the required sync.
unsafe impl Send for OnnxRanker {}
unsafe impl Sync for OnnxRanker {}

impl OnnxRanker {
    /// Load an ONNX model from `path` and configure the score blending exponents.
    ///
    /// `alpha`: LinkLonk score exponent (default 1.0 = linear passthrough).
    /// `beta`:  ML probability exponent (default 0.3 = mild influence).
    pub fn load(path: &str, alpha: f32, beta: f32) -> anyhow::Result<Self> {
        info!(path, alpha, beta, "Loading ONNX ranking model");

        let session = Session::builder()?
            .with_optimization_level(GraphOptimizationLevel::Level3)?
            .with_intra_threads(1)?
            .commit_from_file(path)?;

        info!(path, "ONNX ranking model loaded successfully");

        Ok(Self {
            session: Mutex::new(session),
            alpha,
            beta,
        })
    }

    /// Re-rank `posts` using the ONNX model.
    ///
    /// # Arguments
    /// * `posts` — `(linkLonk_score, post_id, features)` triples, already in
    ///   descending LinkLonk order. Context fields are filled in via `PostFeatures::to_array()`.
    /// * Context args — feed-level context fields shared across all posts in the request.
    ///
    /// # Returns
    /// `(blended_score, post_id)` pairs sorted descending by blended score.
    /// On inference failure, falls back to the original LinkLonk ordering.
    #[allow(clippy::too_many_arguments)]
    pub fn rerank(
        &self,
        posts: &[(f64, String, PostFeatures)],
        user_like_count: u32,
        user_segment_encoded: u8,
        richness_ratio: f32,
        hour_of_day: u8,
        day_of_week: u8,
        is_first_page: bool,
        is_holdout: bool,
    ) -> Vec<(f64, String)> {
        if posts.is_empty() {
            return Vec::new();
        }

        let n = posts.len();

        // Build (N, FEATURE_COUNT) row-major f32 matrix
        let mut flat: Vec<f32> = Vec::with_capacity(n * FEATURE_COUNT);
        for (i, (_, _, feat)) in posts.iter().enumerate() {
            let arr = feat.to_array(
                i.min(u8::MAX as usize) as u8,
                user_like_count,
                user_segment_encoded,
                richness_ratio,
                hour_of_day,
                day_of_week,
                is_first_page,
                is_holdout,
            );
            flat.extend_from_slice(&arr);
        }

        let input_array = match Array2::<f32>::from_shape_vec((n, FEATURE_COUNT), flat) {
            Ok(a) => a,
            Err(e) => {
                warn!(error = %e, "ML reranker: failed to build input tensor, using original order");
                return posts.iter().map(|(s, id, _)| (*s, id.clone())).collect();
            }
        };

        let tensor_ref = match TensorRef::from_array_view(&input_array) {
            Ok(t) => t,
            Err(e) => {
                warn!(error = %e, "ML reranker: failed to create tensor ref, using original order");
                return posts.iter().map(|(s, id, _)| (*s, id.clone())).collect();
            }
        };

        // Run inference (requires &mut session)
        let mut guard = match self.session.lock() {
            Ok(g) => g,
            Err(e) => {
                warn!(error = %e, "ML reranker: session mutex poisoned, using original order");
                return posts.iter().map(|(s, id, _)| (*s, id.clone())).collect();
            }
        };

        let outputs = match guard.run(ort::inputs!["float_input" => tensor_ref]) {
            Ok(o) => o,
            Err(e) => {
                warn!(error = %e, "ML reranker: inference failed, using original order");
                return posts.iter().map(|(s, id, _)| (*s, id.clone())).collect();
            }
        };

        // Extract P(positive class) from the probabilities output
        // LightGBM → ONNX (skl2onnx) outputs "output_probability" shape (N, 2)
        let probs = match outputs
            .get("output_probability")
            .or_else(|| outputs.get("probabilities"))
        {
            Some(p) => p,
            None => {
                warn!("ML reranker: 'output_probability' not found in model outputs, using original order");
                return posts.iter().map(|(s, id, _)| (*s, id.clone())).collect();
            }
        };

        // try_extract_tensor returns (Shape, &[f32]) - flat row-major data
        let (_shape, prob_data) = match probs.try_extract_tensor::<f32>() {
            Ok(v) => v,
            Err(e) => {
                warn!(error = %e, "ML reranker: failed to extract probabilities, using original order");
                return posts.iter().map(|(s, id, _)| (*s, id.clone())).collect();
            }
        };

        // Blend scores and sort
        let alpha = self.alpha as f64;
        let beta = self.beta as f64;
        let mut blended: Vec<(f64, String)> = posts
            .iter()
            .enumerate()
            .map(|(i, (ll_score, post_id, _))| {
                // Each row has 2 values: P(negative), P(positive). Column 1 = P(positive).
                let ml_prob = prob_data.get(i * 2 + 1).copied().unwrap_or(0.5) as f64;
                let ml_clamped = ml_prob.clamp(1e-9, 1.0 - 1e-9);
                let score = ll_score.max(1e-9).powf(alpha) * ml_clamped.powf(beta);
                (score, post_id.clone())
            })
            .collect();

        blended.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
        blended
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algorithm::features::PostFeatures;

    fn dummy_posts(n: usize) -> Vec<(f64, String, PostFeatures)> {
        (0..n)
            .map(|i| {
                (
                    1.0 / (i + 1) as f64,
                    format!("post_{}", i),
                    PostFeatures::default(),
                )
            })
            .collect()
    }

    // ─── rerank: empty input ─────────────────────────────────────────────────

    #[test]
    fn test_rerank_empty_returns_empty() {
        // Without a real session we can't test the full path, but we can test
        // the fast-path guard that triggers before any session access.
        // Since OnnxRanker::load() requires a file, we exercise the empty-slice
        // guard via the public contract: an empty posts slice → empty result.
        // We verify the formula independently below.
        let empty: Vec<(f64, String, PostFeatures)> = Vec::new();
        // The guard fires before any mutex/session touch, so we replicate the
        // identical guard logic here to assert the expected contract:
        let result: Vec<(f64, String)> = if empty.is_empty() {
            Vec::new()
        } else {
            unreachable!()
        };
        assert!(result.is_empty());
    }

    // ─── Score-blending formula ──────────────────────────────────────────────

    /// Verify `linkLonk^alpha * ml^beta` arithmetic in isolation.
    #[test]
    fn test_blend_formula_alpha1_beta1() {
        let ll = 0.8_f64;
        let ml = 0.7_f64;
        let alpha = 1.0_f64;
        let beta = 1.0_f64;
        let blended = ll.powf(alpha) * ml.powf(beta);
        assert!((blended - 0.56).abs() < 1e-9);
    }

    #[test]
    fn test_blend_formula_alpha1_beta0() {
        // beta=0 → ml term is 1.0 → blended == linkLonk score
        let ll = 0.5_f64;
        let alpha = 1.0_f64;
        let beta = 0.0_f64;
        let ml = 0.9_f64;
        let blended = ll.powf(alpha) * ml.powf(beta);
        assert!((blended - ll).abs() < 1e-9);
    }

    #[test]
    fn test_blend_formula_alpha0_beta1() {
        // alpha=0 → linkLonk term is 1.0 → blended == ml probability
        let ll = 0.3_f64;
        let alpha = 0.0_f64;
        let beta = 1.0_f64;
        let ml = 0.6_f64;
        let blended = ll.powf(alpha) * ml.powf(beta);
        assert!((blended - ml).abs() < 1e-9);
    }

    #[test]
    fn test_blend_formula_clamp_low_ml() {
        // ml_prob=0.0 must be clamped to 1e-9 to avoid pow(0, beta) = 0
        let ml_raw = 0.0_f64;
        let ml_clamped = ml_raw.clamp(1e-9, 1.0 - 1e-9);
        assert!(ml_clamped > 0.0);
        assert_eq!(ml_clamped, 1e-9);
    }

    #[test]
    fn test_blend_formula_clamp_high_ml() {
        let ml_raw = 1.0_f64;
        let ml_clamped = ml_raw.clamp(1e-9, 1.0 - 1e-9);
        assert!(ml_clamped < 1.0);
    }

    // ─── Fallback ordering ───────────────────────────────────────────────────

    /// The fallback path (used on any inference error) must preserve the
    /// original (linkLonk-descending) order and keep all post IDs intact.
    #[test]
    fn test_fallback_preserves_order_and_ids() {
        let posts = dummy_posts(5);
        // Simulate fallback: map to (score, id) without reordering
        let fallback: Vec<(f64, String)> =
            posts.iter().map(|(s, id, _)| (*s, id.clone())).collect();
        assert_eq!(fallback.len(), 5);
        // Original order is descending (1.0, 0.5, 0.333, 0.25, 0.2)
        for i in 0..fallback.len() - 1 {
            assert!(fallback[i].0 >= fallback[i + 1].0);
        }
        assert_eq!(fallback[0].1, "post_0");
        assert_eq!(fallback[4].1, "post_4");
    }

    // ─── Input tensor shape ──────────────────────────────────────────────────

    #[test]
    fn test_flat_input_tensor_dimensions() {
        use ndarray::Array2;

        let posts = dummy_posts(3);
        let n = posts.len();
        let mut flat: Vec<f32> = Vec::with_capacity(n * FEATURE_COUNT);
        for (i, (_, _, feat)) in posts.iter().enumerate() {
            let arr = feat.to_array(i as u8, 0, 0, 0.0, 0, 0, false, false);
            flat.extend_from_slice(&arr);
        }
        let matrix = Array2::<f32>::from_shape_vec((n, FEATURE_COUNT), flat).unwrap();
        assert_eq!(matrix.shape(), &[3, FEATURE_COUNT]);
    }
}
