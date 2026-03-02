//! Feed audit and explainability module.
//!
//! Provides detailed observability for understanding why items appear in feeds.
//! Captures scoring factors, post sources, and timing information.

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::Serialize;
use tracing::{debug, info};

use crate::algorithm::LinkLonkParams;
use crate::config::Config;
use graze_common::RedisClient;

/// Complete audit record for a feed request.
#[derive(Debug, Clone, Serialize)]
pub struct FeedAuditRecord {
    /// Unique request identifier for correlation.
    pub request_id: String,
    /// Hashed user DID (anonymized).
    pub user_hash: String,
    /// Algorithm ID used.
    pub algo_id: i32,
    /// Timestamp of the request.
    pub timestamp: DateTime<Utc>,
    /// Algorithm parameters used.
    pub params: AuditParams,
    /// Audit entries for each post in the feed.
    pub posts: Vec<PostAuditEntry>,
    /// Information about how content was blended.
    pub blending: BlendingInfo,
    /// Timing breakdown.
    pub timing: TimingInfo,
}

/// Algorithm parameters captured for audit.
#[derive(Debug, Clone, Serialize)]
pub struct AuditParams {
    /// Preset name (default, discovery, stable, fast).
    pub preset: String,
    /// Time window in hours.
    pub time_window_hours: f64,
    /// Recency half-life in hours.
    pub recency_half_life_hours: f64,
    /// Popularity power factor (popularity exponent).
    pub popularity_power: f64,
    /// Num paths power (paths exponent).
    pub num_paths_power: f64,
    /// Maximum user likes considered.
    pub max_user_likes: usize,
    /// Maximum co-likers considered.
    pub max_total_sources: usize,
}

impl From<&LinkLonkParams> for AuditParams {
    fn from(params: &LinkLonkParams) -> Self {
        Self {
            // Infer preset from parameter values since it's not stored on the struct
            preset: infer_preset_name(params),
            time_window_hours: params.time_window_hours,
            recency_half_life_hours: params.recency_half_life_hours,
            popularity_power: params.popularity_power,
            num_paths_power: params.num_paths_power,
            max_user_likes: params.max_user_likes,
            max_total_sources: params.max_total_sources,
        }
    }
}

/// Infer the preset name from parameter values.
fn infer_preset_name(params: &LinkLonkParams) -> String {
    // Check against known presets to identify which was used
    if params.max_user_likes == 200 && params.specificity_power > 1.4 {
        "discovery".to_string()
    } else if params.max_user_likes == 1000 && params.recency_half_life_hours > 100.0 {
        "stable".to_string()
    } else if params.max_sources_per_post == 50 && params.result_ttl_seconds > 500 {
        "fast".to_string()
    } else {
        "default".to_string()
    }
}

/// Audit entry for a single post.
#[derive(Debug, Clone, Serialize)]
pub struct PostAuditEntry {
    /// Post AT-URI.
    pub post_uri: String,
    /// Internal post ID.
    pub post_id: String,
    /// Final score after all adjustments.
    pub final_score: f64,
    /// Source of this post (personalized, fallback, special).
    pub source: PostSource,
    /// Detailed scoring breakdown (for personalized posts).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub breakdown: Option<ScoringBreakdown>,
}

/// Source of a post in the feed.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PostSource {
    /// From personalization algorithm.
    Personalized,
    /// From popular fallback tranche.
    FallbackPopular,
    /// From velocity fallback tranche.
    FallbackVelocity,
    /// From discovery fallback tranche.
    FallbackDiscovery,
    /// Pinned special post.
    SpecialPinned {
        /// Attribution identifier.
        attribution: String,
    },
    /// Rotating special post.
    SpecialRotating {
        /// Attribution identifier.
        attribution: String,
    },
    /// Sponsored special post.
    SpecialSponsored {
        /// Attribution identifier.
        attribution: String,
        /// CPM in cents.
        cpm_cents: i32,
    },
}

/// Detailed scoring breakdown for a personalized post.
#[derive(Debug, Clone, Serialize)]
pub struct ScoringBreakdown {
    /// Raw score before paths boost and popularity penalty.
    pub raw_score: f64,
    /// Popularity penalty factor applied (popularity exponent).
    pub popularity_penalty: f64,
    /// Number of users who liked this post.
    pub liker_count: usize,
    /// Number of distinct co-liker paths (sources who liked this post).
    pub num_paths: usize,
    /// Paths boost factor applied (num_paths^num_paths_power).
    pub paths_boost: f64,
    /// Top contributing co-likers.
    pub top_contributors: Vec<CoLikerContribution>,
}

/// Data for a post's scoring breakdown (used internally during collection).
#[derive(Debug, Clone)]
pub struct PostBreakdownData {
    /// Raw score before adjustments.
    pub raw_score: f64,
    /// Final score after all adjustments.
    pub final_score: f64,
    /// Popularity penalty factor applied.
    pub popularity_penalty: f64,
    /// Number of users who liked this post.
    pub liker_count: usize,
    /// Number of distinct co-liker paths.
    pub num_paths: usize,
    /// Paths boost factor applied.
    pub paths_boost: f64,
}

/// Contribution from a single co-liker to a post's score.
#[derive(Debug, Clone, Serialize)]
pub struct CoLikerContribution {
    /// Hashed co-liker user ID (anonymized).
    pub liker_hash: String,
    /// Co-liker's weight (from shared likes with target user).
    pub co_liker_weight: f64,
    /// Recency weight for this like.
    pub recency_weight: f64,
    /// Total contribution (co_liker_weight * recency_weight).
    pub contribution: f64,
    /// Age of the like in seconds.
    pub like_age_seconds: f64,
}

/// Information about content blending.
#[derive(Debug, Clone, Default, Serialize)]
pub struct BlendingInfo {
    /// Number of likes by the user (determines user tier).
    pub user_like_count: u64,
    /// Personalization ratio used.
    pub personalization_ratio: f64,
    /// Number of personalized posts.
    pub personalized_count: usize,
    /// Number of popular fallback posts.
    pub fallback_popular_count: usize,
    /// Number of velocity fallback posts.
    pub fallback_velocity_count: usize,
    /// Number of discovery fallback posts.
    pub fallback_discovery_count: usize,
    /// Number of pinned special posts.
    pub special_pinned_count: usize,
    /// Number of rotating special posts.
    pub special_rotating_count: usize,
    /// Number of sponsored special posts.
    pub special_sponsored_count: usize,
}

/// Timing breakdown for the request.
#[derive(Debug, Clone, Default, Serialize)]
pub struct TimingInfo {
    /// Total request time in milliseconds.
    pub total_ms: f64,
    /// Co-liker computation time.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub coliker_ms: Option<f64>,
    /// Scoring time.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scoring_ms: Option<f64>,
    /// Fallback fetch time.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fallback_ms: Option<f64>,
    /// Special posts fetch time.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub special_posts_ms: Option<f64>,
}

/// Builder for collecting audit data during request processing.
#[derive(Debug)]
pub struct AuditCollector {
    request_id: String,
    user_hash: String,
    algo_id: i32,
    timestamp: DateTime<Utc>,
    params: Option<AuditParams>,
    max_contributors: usize,
    log_full_breakdown: bool,

    // Post scoring data (post_id -> contributions)
    post_contributions: HashMap<String, Vec<CoLikerContribution>>,
    post_breakdowns: HashMap<String, PostBreakdownData>,

    // Final posts with sources
    posts: Vec<PostAuditEntry>,

    // Blending info
    blending: BlendingInfo,

    // Timing
    timing: TimingInfo,
}

impl AuditCollector {
    /// Create a new audit collector.
    pub fn new(
        request_id: String,
        user_hash: String,
        algo_id: i32,
        max_contributors: usize,
        log_full_breakdown: bool,
    ) -> Self {
        Self {
            request_id,
            user_hash,
            algo_id,
            timestamp: Utc::now(),
            params: None,
            max_contributors,
            log_full_breakdown,
            post_contributions: HashMap::new(),
            post_breakdowns: HashMap::new(),
            posts: Vec::new(),
            blending: BlendingInfo::default(),
            timing: TimingInfo::default(),
        }
    }

    /// Set algorithm parameters used.
    pub fn set_params(&mut self, params: &LinkLonkParams) {
        self.params = Some(AuditParams::from(params));
    }

    /// Record a co-liker contribution to a post's score.
    pub fn add_contribution(
        &mut self,
        post_id: &str,
        liker_hash: &str,
        co_liker_weight: f64,
        recency_weight: f64,
        contribution: f64,
        like_age_seconds: f64,
    ) {
        if !self.log_full_breakdown {
            return;
        }

        let contributions = self
            .post_contributions
            .entry(post_id.to_string())
            .or_default();

        // Only keep top N contributors
        if contributions.len() < self.max_contributors {
            contributions.push(CoLikerContribution {
                liker_hash: truncate_hash(liker_hash),
                co_liker_weight,
                recency_weight,
                contribution,
                like_age_seconds,
            });
        } else {
            // Replace smallest contribution if this one is larger
            if let Some((idx, min)) = contributions
                .iter()
                .enumerate()
                .min_by(|a, b| a.1.contribution.partial_cmp(&b.1.contribution).unwrap())
            {
                if contribution > min.contribution {
                    contributions[idx] = CoLikerContribution {
                        liker_hash: truncate_hash(liker_hash),
                        co_liker_weight,
                        recency_weight,
                        contribution,
                        like_age_seconds,
                    };
                }
            }
        }
    }

    /// Set the scoring breakdown for a post.
    pub fn set_post_breakdown(&mut self, post_id: &str, breakdown: PostBreakdownData) {
        self.post_breakdowns.insert(post_id.to_string(), breakdown);
    }

    /// Add a personalized post to the audit.
    pub fn add_personalized_post(&mut self, post_id: &str, post_uri: &str, score: f64) {
        let breakdown = if self.log_full_breakdown {
            self.post_breakdowns.get(post_id).map(|data| {
                let mut contributors = self
                    .post_contributions
                    .get(post_id)
                    .cloned()
                    .unwrap_or_default();
                // Sort by contribution descending
                contributors.sort_by(|a, b| b.contribution.partial_cmp(&a.contribution).unwrap());
                ScoringBreakdown {
                    raw_score: data.raw_score,
                    popularity_penalty: data.popularity_penalty,
                    liker_count: data.liker_count,
                    num_paths: data.num_paths,
                    paths_boost: data.paths_boost,
                    top_contributors: contributors,
                }
            })
        } else {
            None
        };

        self.posts.push(PostAuditEntry {
            post_uri: post_uri.to_string(),
            post_id: post_id.to_string(),
            final_score: score,
            source: PostSource::Personalized,
            breakdown,
        });
        self.blending.personalized_count += 1;
    }

    /// Add a fallback post to the audit.
    pub fn add_fallback_post(&mut self, post_id: &str, post_uri: &str, source: PostSource) {
        match &source {
            PostSource::FallbackPopular => self.blending.fallback_popular_count += 1,
            PostSource::FallbackVelocity => self.blending.fallback_velocity_count += 1,
            PostSource::FallbackDiscovery => self.blending.fallback_discovery_count += 1,
            _ => {}
        }

        self.posts.push(PostAuditEntry {
            post_uri: post_uri.to_string(),
            post_id: post_id.to_string(),
            final_score: 0.0,
            source,
            breakdown: None,
        });
    }

    /// Add a special post to the audit.
    pub fn add_special_post(&mut self, post_uri: &str, source: PostSource) {
        match &source {
            PostSource::SpecialPinned { .. } => self.blending.special_pinned_count += 1,
            PostSource::SpecialRotating { .. } => self.blending.special_rotating_count += 1,
            PostSource::SpecialSponsored { .. } => self.blending.special_sponsored_count += 1,
            _ => {}
        }

        self.posts.push(PostAuditEntry {
            post_uri: post_uri.to_string(),
            post_id: String::new(),
            final_score: 0.0,
            source,
            breakdown: None,
        });
    }

    /// Set user like count for blending info.
    pub fn set_user_like_count(&mut self, count: u64) {
        self.blending.user_like_count = count;
    }

    /// Set personalization ratio used.
    pub fn set_personalization_ratio(&mut self, ratio: f64) {
        self.blending.personalization_ratio = ratio;
    }

    /// Set timing information.
    pub fn set_timing(
        &mut self,
        total_ms: f64,
        coliker_ms: Option<f64>,
        scoring_ms: Option<f64>,
        fallback_ms: Option<f64>,
        special_posts_ms: Option<f64>,
    ) {
        self.timing = TimingInfo {
            total_ms,
            coliker_ms,
            scoring_ms,
            fallback_ms,
            special_posts_ms,
        };
    }

    /// Build the final audit record.
    pub fn build_record(self) -> FeedAuditRecord {
        FeedAuditRecord {
            request_id: self.request_id,
            user_hash: truncate_hash(&self.user_hash),
            algo_id: self.algo_id,
            timestamp: self.timestamp,
            params: self.params.unwrap_or_else(|| AuditParams {
                preset: "unknown".to_string(),
                time_window_hours: 0.0,
                recency_half_life_hours: 0.0,
                popularity_power: 0.0,
                num_paths_power: 0.0,
                max_user_likes: 0,
                max_total_sources: 0,
            }),
            posts: self.posts,
            blending: self.blending,
            timing: self.timing,
        }
    }

    /// Emit the audit log.
    pub fn emit_log(self) {
        let record = self.build_record();

        // Serialize the full audit data
        let audit_json = serde_json::to_string(&record)
            .unwrap_or_else(|e| format!("{{\"error\": \"serialization failed: {}\"}}", e));

        info!(
            target: "graze_audit",
            request_id = %record.request_id,
            user_hash = %record.user_hash,
            algo_id = record.algo_id,
            posts_count = record.posts.len(),
            personalized_count = record.blending.personalized_count,
            fallback_count = record.blending.fallback_popular_count
                + record.blending.fallback_velocity_count
                + record.blending.fallback_discovery_count,
            special_count = record.blending.special_pinned_count
                + record.blending.special_rotating_count
                + record.blending.special_sponsored_count,
            total_ms = format!("{:.2}", record.timing.total_ms),
            audit = %audit_json,
            "feed_audit"
        );
    }
}

/// Log a minimal audit record for requests that exited early (errors, EOF, etc.).
pub fn emit_skip_log(
    request_id: &str,
    user_hash: Option<&str>,
    algo_id: Option<i32>,
    reason: &str,
    error_type: Option<&str>,
) {
    debug!(
        target: "graze_audit",
        request_id = %request_id,
        user_hash = user_hash.map(truncate_hash).as_deref().unwrap_or("none"),
        algo_id = algo_id.unwrap_or(-1),
        reason = %reason,
        error_type = error_type.unwrap_or("none"),
        "feed_audit_skip"
    );
}

/// Check if audit should be enabled for this request.
pub async fn should_audit(
    config: &Config,
    redis: &Arc<RedisClient>,
    user_did: Option<&str>,
) -> bool {
    // Master switch
    if !config.audit_enabled {
        debug!(target: "graze_audit", reason = "disabled", "audit_skipped");
        return false;
    }

    // Audit all users
    if config.audit_all_users {
        return true;
    }

    // Sample rate
    if config.audit_sample_rate > 0.0 {
        let roll = rand::random::<f64>();
        if roll < config.audit_sample_rate {
            return true;
        }
        debug!(
            target: "graze_audit",
            reason = "sample_rate_miss",
            sample_rate = config.audit_sample_rate,
            roll = format!("{:.4}", roll),
            "audit_skipped"
        );
    }

    // Per-user audit set
    if let Some(did) = user_did {
        let user_hash = graze_common::hash_did(did);
        match redis
            .sismember(graze_common::Keys::audit_users(), &user_hash)
            .await
        {
            Ok(true) => return true,
            Ok(false) => {
                debug!(
                    target: "graze_audit",
                    reason = "user_not_in_audit_set",
                    user_hash = %truncate_hash(&user_hash),
                    "audit_skipped"
                );
            }
            Err(e) => {
                debug!(
                    target: "graze_audit",
                    reason = "redis_error",
                    error = %e,
                    "audit_skipped"
                );
            }
        }
    } else {
        debug!(
            target: "graze_audit",
            reason = "no_user_did",
            "audit_skipped"
        );
    }

    false
}

/// Truncate a hash for logging (first 8 characters).
fn truncate_hash(hash: &str) -> String {
    hash.chars().take(8).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_audit_collector_basic() {
        let mut collector = AuditCollector::new(
            "test-request-id".to_string(),
            "abc123def456".to_string(),
            42,
            10,
            true,
        );

        collector.add_contribution("post1", "liker1", 1.5, 0.8, 1.2, 3600.0);
        collector.set_post_breakdown(
            "post1",
            PostBreakdownData {
                raw_score: 1.2,
                final_score: 1.0,
                popularity_penalty: 0.83,
                liker_count: 50,
                num_paths: 3,
                paths_boost: 1.39,
            },
        );
        collector.add_personalized_post("post1", "at://did:plc:abc/post/1", 1.0);

        let record = collector.build_record();
        assert_eq!(record.posts.len(), 1);
        assert_eq!(record.blending.personalized_count, 1);
    }

    #[test]
    fn test_truncate_hash() {
        assert_eq!(truncate_hash("abcdefghijklmnop"), "abcdefgh");
        assert_eq!(truncate_hash("abc"), "abc");
    }
}
