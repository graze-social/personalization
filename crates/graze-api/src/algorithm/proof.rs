//! LinkLonk Algorithm Proof/Explainability Module.
//!
//! This module provides detailed traversal proof for the LinkLonk 3-step random walk:
//!
//! Step 1: User → Liked Posts
//!   - User's recent likes within time window
//!
//! Step 2: Liked Posts → Co-Likers
//!   - For each liked post, find other users who liked it
//!   - Aggregate weights based on recency and normalization
//!
//! Step 3: Co-Likers → Candidate Posts
//!   - Score candidate posts based on co-liker weights
//!   - Apply popularity penalty and diversity re-ranking

use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use crate::algorithm::params::LinkLonkParams;
use crate::config::Config;
use crate::error::Result;
use graze_common::services::UriInterner;
use graze_common::{Keys, RedisClient, DEFAULT_RETENTION_DAYS};

/// Complete proof of a LinkLonk personalization computation.
#[derive(Debug, Clone, Serialize)]
pub struct LinkLonkProof {
    /// User information
    pub user: UserProof,
    /// Step 1: User's liked posts
    pub step1_user_likes: Step1Proof,
    /// Step 2: Co-liker aggregation
    pub step2_colikers: Step2Proof,
    /// Step 3: Post scoring
    pub step3_scoring: Step3Proof,
    /// Final results after diversity
    pub final_results: FinalResultsProof,
    /// Timing breakdown
    pub timing: ProofTiming,
}

/// User information for the proof.
#[derive(Debug, Clone, Serialize)]
pub struct UserProof {
    pub user_did: String,
    pub user_hash: String,
    pub algo_id: i32,
}

/// Step 1 proof: User's liked posts.
#[derive(Debug, Clone, Serialize)]
pub struct Step1Proof {
    /// Total likes by this user (all time)
    pub total_user_likes: usize,
    /// Likes within the time window
    pub likes_in_window: usize,
    /// Likes used for computation (after max_user_likes limit)
    pub likes_used: usize,
    /// Time window in hours
    pub time_window_hours: f64,
    /// Sample of liked posts (post_id, uri, like_timestamp)
    pub sample_likes: Vec<LikedPostProof>,
}

/// A single liked post in the proof.
#[derive(Debug, Clone, Serialize)]
pub struct LikedPostProof {
    pub post_id: String,
    pub uri: Option<String>,
    pub like_timestamp: f64,
    pub like_age_hours: f64,
    /// Number of other users who liked this post (before this user)
    pub other_likers_count: usize,
}

/// Step 2 proof: Co-liker aggregation.
#[derive(Debug, Clone, Serialize)]
pub struct Step2Proof {
    /// Total unique co-likers found
    pub total_colikers_found: usize,
    /// Co-likers kept after max_total_sources limit
    pub colikers_kept: usize,
    /// Normalization applied
    pub normalization_enabled: bool,
    /// Top co-likers with their weights and breakdown
    pub top_colikers: Vec<ColikerProof>,
}

/// A single co-liker in the proof.
#[derive(Debug, Clone, Serialize)]
pub struct ColikerProof {
    /// Hashed co-liker ID (first 12 chars for privacy)
    pub coliker_hash: String,
    /// Final aggregated weight
    pub weight: f64,
    /// Number of posts this co-liker shares with the user
    pub shared_posts_count: usize,
    /// Sample of shared posts
    pub shared_posts_sample: Vec<SharedPostProof>,
}

/// A shared post between user and co-liker.
#[derive(Debug, Clone, Serialize)]
pub struct SharedPostProof {
    pub post_id: String,
    pub uri: Option<String>,
    /// Co-liker's like timestamp
    pub coliker_like_time: f64,
    /// User's like timestamp
    pub user_like_time: f64,
    /// Recency weight contribution
    pub recency_weight: f64,
}

/// Step 3 proof: Post scoring.
#[derive(Debug, Clone, Serialize)]
pub struct Step3Proof {
    /// Total posts in candidate pool
    pub candidate_pool_size: usize,
    /// Posts meeting minimum likes threshold
    pub posts_above_min_likes: usize,
    /// Posts actually scored
    pub posts_scored: usize,
    /// Posts skipped (no likers in our co-liker set)
    pub posts_skipped_no_overlap: usize,
    /// Minimum likes threshold used
    pub min_post_likes: usize,
    /// Top scored posts with breakdown
    pub top_posts: Vec<ScoredPostProof>,
}

/// A single scored post in the proof.
#[derive(Debug, Clone, Serialize)]
pub struct ScoredPostProof {
    pub post_id: String,
    pub uri: Option<String>,
    pub author_did: Option<String>,
    /// Raw score before penalties
    pub raw_score: f64,
    /// Popularity penalty applied
    pub popularity_penalty: f64,
    /// Score after popularity penalty
    pub score_after_popularity: f64,
    /// Final score after diversity adjustment
    pub final_score: f64,
    /// Total likers on this post
    pub total_likers: usize,
    /// Likers that overlap with our co-liker set
    pub overlapping_likers: usize,
    /// Top contributing co-likers for this post
    pub top_contributors: Vec<PostContributorProof>,
}

/// A co-liker's contribution to a post's score.
#[derive(Debug, Clone, Serialize)]
pub struct PostContributorProof {
    pub coliker_hash: String,
    /// Co-liker's weight from Step 2
    pub coliker_weight: f64,
    /// Recency of their like on this post
    pub like_recency_weight: f64,
    /// Total contribution: coliker_weight * recency_weight
    pub contribution: f64,
}

/// Final results after diversity re-ranking.
#[derive(Debug, Clone, Serialize)]
pub struct FinalResultsProof {
    /// Posts before diversity filtering
    pub posts_before_diversity: usize,
    /// Posts after diversity filtering
    pub posts_after_diversity: usize,
    /// Unique authors in final results
    pub unique_authors: usize,
    /// Posts demoted by diversity
    pub posts_demoted: usize,
    /// Posts removed by author cap
    pub posts_removed_by_cap: usize,
    /// Diversity settings used
    pub diversity_config: DiversityConfigProof,
}

/// Diversity configuration used.
#[derive(Debug, Clone, Serialize)]
pub struct DiversityConfigProof {
    pub enabled: bool,
    pub max_posts_per_author: usize,
    pub diminishing_factor: f64,
    pub mmr_lambda: f64,
}

/// Timing breakdown for the proof.
#[derive(Debug, Clone, Serialize)]
pub struct ProofTiming {
    pub total_ms: f64,
    pub step1_likes_fetch_ms: f64,
    pub step2_coliker_compute_ms: f64,
    pub step3_scoring_ms: f64,
    pub diversity_ms: f64,
    pub uri_resolution_ms: f64,
}

/// Builder for collecting proof data during computation.
pub struct ProofCollector {
    config: Arc<Config>,
    #[allow(dead_code)]
    redis: Arc<RedisClient>,
    interner: Arc<UriInterner>,

    // User info
    user_did: String,
    user_hash: String,
    algo_id: i32,

    // Step 1 data
    total_user_likes: usize,
    likes_in_window: usize,
    likes_used: usize,
    time_window_hours: f64,
    user_likes: Vec<(String, f64)>,             // (post_id, timestamp)
    post_likers_counts: HashMap<String, usize>, // post_id -> other likers count

    // Step 2 data
    total_colikers_found: usize,
    coliker_weights: HashMap<String, f64>,
    coliker_shared_posts: HashMap<String, Vec<(String, f64, f64)>>, // coliker -> [(post_id, coliker_time, user_time)]

    // Step 3 data
    candidate_pool_size: usize,
    posts_above_min_likes: usize,
    posts_scored: usize,
    posts_skipped_no_overlap: usize,
    min_post_likes: usize,
    scored_posts: Vec<ScoredPostProofData>,

    // Diversity data
    posts_before_diversity: usize,
    posts_after_diversity: usize,
    unique_authors: usize,
    posts_demoted: usize,
    posts_removed_by_cap: usize,

    // Timing
    start_time: Instant,
    step1_ms: f64,
    step2_ms: f64,
    step3_ms: f64,
    diversity_ms: f64,
}

struct ScoredPostProofData {
    post_id: String,
    raw_score: f64,
    popularity_penalty: f64,
    score_after_popularity: f64,
    final_score: f64,
    total_likers: usize,
    overlapping_likers: usize,
    contributors: Vec<(String, f64, f64, f64)>, // (coliker_hash, weight, recency, contribution)
}

impl ProofCollector {
    pub fn new(
        config: Arc<Config>,
        redis: Arc<RedisClient>,
        interner: Arc<UriInterner>,
        user_did: String,
        algo_id: i32,
    ) -> Self {
        let user_hash = graze_common::hash_did(&user_did);
        Self {
            config,
            redis,
            interner,
            user_did,
            user_hash,
            algo_id,
            total_user_likes: 0,
            likes_in_window: 0,
            likes_used: 0,
            time_window_hours: 0.0,
            user_likes: Vec::new(),
            post_likers_counts: HashMap::new(),
            total_colikers_found: 0,
            coliker_weights: HashMap::new(),
            coliker_shared_posts: HashMap::new(),
            candidate_pool_size: 0,
            posts_above_min_likes: 0,
            posts_scored: 0,
            posts_skipped_no_overlap: 0,
            min_post_likes: 0,
            scored_posts: Vec::new(),
            posts_before_diversity: 0,
            posts_after_diversity: 0,
            unique_authors: 0,
            posts_demoted: 0,
            posts_removed_by_cap: 0,
            start_time: Instant::now(),
            step1_ms: 0.0,
            step2_ms: 0.0,
            step3_ms: 0.0,
            diversity_ms: 0.0,
        }
    }

    // Step 1 setters
    pub fn set_user_likes_info(
        &mut self,
        total: usize,
        in_window: usize,
        used: usize,
        time_window_hours: f64,
        likes: Vec<(String, f64)>,
    ) {
        self.total_user_likes = total;
        self.likes_in_window = in_window;
        self.likes_used = used;
        self.time_window_hours = time_window_hours;
        self.user_likes = likes;
    }

    pub fn set_post_likers_counts(&mut self, counts: HashMap<String, usize>) {
        self.post_likers_counts = counts;
    }

    pub fn set_step1_time(&mut self, ms: f64) {
        self.step1_ms = ms;
    }

    // Step 2 setters
    pub fn set_coliker_info(
        &mut self,
        total_found: usize,
        weights: HashMap<String, f64>,
        shared_posts: HashMap<String, Vec<(String, f64, f64)>>,
    ) {
        self.total_colikers_found = total_found;
        self.coliker_weights = weights;
        self.coliker_shared_posts = shared_posts;
    }

    pub fn set_step2_time(&mut self, ms: f64) {
        self.step2_ms = ms;
    }

    // Step 3 setters
    pub fn set_scoring_info(
        &mut self,
        candidate_pool_size: usize,
        posts_above_min: usize,
        posts_scored: usize,
        posts_skipped: usize,
        min_likes: usize,
    ) {
        self.candidate_pool_size = candidate_pool_size;
        self.posts_above_min_likes = posts_above_min;
        self.posts_scored = posts_scored;
        self.posts_skipped_no_overlap = posts_skipped;
        self.min_post_likes = min_likes;
    }

    #[allow(clippy::too_many_arguments)]
    pub fn add_scored_post(
        &mut self,
        post_id: String,
        raw_score: f64,
        popularity_penalty: f64,
        score_after_popularity: f64,
        final_score: f64,
        total_likers: usize,
        overlapping_likers: usize,
        contributors: Vec<(String, f64, f64, f64)>,
    ) {
        self.scored_posts.push(ScoredPostProofData {
            post_id,
            raw_score,
            popularity_penalty,
            score_after_popularity,
            final_score,
            total_likers,
            overlapping_likers,
            contributors,
        });
    }

    pub fn set_step3_time(&mut self, ms: f64) {
        self.step3_ms = ms;
    }

    // Diversity setters
    pub fn set_diversity_info(
        &mut self,
        before: usize,
        after: usize,
        unique_authors: usize,
        demoted: usize,
        removed: usize,
    ) {
        self.posts_before_diversity = before;
        self.posts_after_diversity = after;
        self.unique_authors = unique_authors;
        self.posts_demoted = demoted;
        self.posts_removed_by_cap = removed;
    }

    pub fn set_diversity_time(&mut self, ms: f64) {
        self.diversity_ms = ms;
    }

    /// Build the final proof with URI resolution.
    pub async fn build(self) -> Result<LinkLonkProof> {
        let uri_start = Instant::now();

        // Collect all post IDs that need URI resolution
        let mut post_ids: Vec<i64> = Vec::new();
        for (post_id, _) in &self.user_likes {
            if let Ok(id) = post_id.parse::<i64>() {
                post_ids.push(id);
            }
        }
        for post in &self.scored_posts {
            if let Ok(id) = post.post_id.parse::<i64>() {
                post_ids.push(id);
            }
        }

        // Batch resolve URIs
        let id_to_uri = self
            .interner
            .get_uris_batch(&post_ids)
            .await
            .unwrap_or_default();

        let uri_ms = uri_start.elapsed().as_secs_f64() * 1000.0;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs_f64();

        // Build Step 1 proof
        let sample_likes: Vec<LikedPostProof> = self
            .user_likes
            .iter()
            .take(20) // Sample first 20
            .map(|(post_id, ts)| {
                let id = post_id.parse::<i64>().ok();
                let uri = id.and_then(|i| id_to_uri.get(&i).cloned());
                LikedPostProof {
                    post_id: post_id.clone(),
                    uri,
                    like_timestamp: *ts,
                    like_age_hours: (now - ts) / 3600.0,
                    other_likers_count: self.post_likers_counts.get(post_id).copied().unwrap_or(0),
                }
            })
            .collect();

        // Build Step 2 proof - top 20 co-likers
        let mut coliker_vec: Vec<_> = self.coliker_weights.iter().collect();
        coliker_vec.sort_by(|a, b| b.1.partial_cmp(a.1).unwrap());

        let top_colikers: Vec<ColikerProof> = coliker_vec
            .iter()
            .take(20)
            .map(|(coliker_hash, weight)| {
                let shared = self.coliker_shared_posts.get(*coliker_hash);
                let shared_count = shared.map(|s| s.len()).unwrap_or(0);
                let shared_sample: Vec<SharedPostProof> = shared
                    .map(|posts| {
                        posts
                            .iter()
                            .take(5)
                            .map(|(post_id, coliker_time, user_time)| {
                                let id = post_id.parse::<i64>().ok();
                                let uri = id.and_then(|i| id_to_uri.get(&i).cloned());
                                SharedPostProof {
                                    post_id: post_id.clone(),
                                    uri,
                                    coliker_like_time: *coliker_time,
                                    user_like_time: *user_time,
                                    recency_weight: 0.0, // Computed during aggregation
                                }
                            })
                            .collect()
                    })
                    .unwrap_or_default();

                ColikerProof {
                    coliker_hash: coliker_hash.chars().take(12).collect(),
                    weight: **weight,
                    shared_posts_count: shared_count,
                    shared_posts_sample: shared_sample,
                }
            })
            .collect();

        // Build Step 3 proof - top 20 scored posts
        let top_posts: Vec<ScoredPostProof> = self
            .scored_posts
            .iter()
            .take(20)
            .map(|p| {
                let id = p.post_id.parse::<i64>().ok();
                let uri = id.and_then(|i| id_to_uri.get(&i).cloned());
                let author_did = uri.as_ref().and_then(|u| {
                    u.strip_prefix("at://")
                        .and_then(|s| s.split('/').next())
                        .map(|s| s.to_string())
                });

                let top_contributors: Vec<PostContributorProof> = p
                    .contributors
                    .iter()
                    .take(10)
                    .map(|(hash, weight, recency, contrib)| PostContributorProof {
                        coliker_hash: hash.chars().take(12).collect(),
                        coliker_weight: *weight,
                        like_recency_weight: *recency,
                        contribution: *contrib,
                    })
                    .collect();

                ScoredPostProof {
                    post_id: p.post_id.clone(),
                    uri,
                    author_did,
                    raw_score: p.raw_score,
                    popularity_penalty: p.popularity_penalty,
                    score_after_popularity: p.score_after_popularity,
                    final_score: p.final_score,
                    total_likers: p.total_likers,
                    overlapping_likers: p.overlapping_likers,
                    top_contributors,
                }
            })
            .collect();

        Ok(LinkLonkProof {
            user: UserProof {
                user_did: self.user_did,
                user_hash: self.user_hash.chars().take(12).collect(),
                algo_id: self.algo_id,
            },
            step1_user_likes: Step1Proof {
                total_user_likes: self.total_user_likes,
                likes_in_window: self.likes_in_window,
                likes_used: self.likes_used,
                time_window_hours: self.time_window_hours,
                sample_likes,
            },
            step2_colikers: Step2Proof {
                total_colikers_found: self.total_colikers_found,
                colikers_kept: self.coliker_weights.len(),
                normalization_enabled: self.config.linklonk_normalization_enabled,
                top_colikers,
            },
            step3_scoring: Step3Proof {
                candidate_pool_size: self.candidate_pool_size,
                posts_above_min_likes: self.posts_above_min_likes,
                posts_scored: self.posts_scored,
                posts_skipped_no_overlap: self.posts_skipped_no_overlap,
                min_post_likes: self.min_post_likes,
                top_posts,
            },
            final_results: FinalResultsProof {
                posts_before_diversity: self.posts_before_diversity,
                posts_after_diversity: self.posts_after_diversity,
                unique_authors: self.unique_authors,
                posts_demoted: self.posts_demoted,
                posts_removed_by_cap: self.posts_removed_by_cap,
                diversity_config: DiversityConfigProof {
                    enabled: self.config.diversity_enabled,
                    max_posts_per_author: self.config.max_posts_per_author,
                    diminishing_factor: self.config.author_diminishing_factor,
                    mmr_lambda: self.config.diversity_mmr_lambda,
                },
            },
            timing: ProofTiming {
                total_ms: self.start_time.elapsed().as_secs_f64() * 1000.0,
                step1_likes_fetch_ms: self.step1_ms,
                step2_coliker_compute_ms: self.step2_ms,
                step3_scoring_ms: self.step3_ms,
                diversity_ms: self.diversity_ms,
                uri_resolution_ms: uri_ms,
            },
        })
    }
}

/// Compute a full proof for a user's personalization.
pub async fn compute_proof(
    redis: Arc<RedisClient>,
    interner: Arc<UriInterner>,
    config: Arc<Config>,
    user_did: &str,
    algo_id: i32,
    params: &LinkLonkParams,
) -> Result<LinkLonkProof> {
    let mut collector = ProofCollector::new(
        config.clone(),
        redis.clone(),
        interner.clone(),
        user_did.to_string(),
        algo_id,
    );

    let user_hash = graze_common::hash_did(user_did);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs_f64();
    let time_window_seconds = (params.time_window_hours * 3600.0) as u64;
    let min_time = now - time_window_seconds as f64;
    let recency_half_life = params.recency_half_life_hours * 3600.0;

    // ═══════════════════════════════════════════════════════════════════
    // STEP 1: Get user's liked posts (from date-based keys)
    // ═══════════════════════════════════════════════════════════════════
    let step1_start = Instant::now();

    let user_likes_keys = Keys::user_likes_retention(&user_hash, DEFAULT_RETENTION_DAYS);
    let all_likes: Vec<(String, f64)> = redis
        .zrevrangebyscore_merged(&user_likes_keys, f64::INFINITY, 0.0, 10000)
        .await?;

    let total_likes = all_likes.len();
    let likes_in_window: Vec<(String, f64)> = all_likes
        .iter()
        .filter(|(_, ts)| *ts >= min_time)
        .cloned()
        .collect();
    let window_count = likes_in_window.len();
    let used_likes: Vec<(String, f64)> = likes_in_window
        .into_iter()
        .take(params.max_user_likes)
        .collect();
    let used_count = used_likes.len();

    collector.set_user_likes_info(
        total_likes,
        window_count,
        used_count,
        params.time_window_hours,
        used_likes.clone(),
    );
    collector.set_step1_time(step1_start.elapsed().as_secs_f64() * 1000.0);

    // ═══════════════════════════════════════════════════════════════════
    // STEP 2: Compute co-likers
    // ═══════════════════════════════════════════════════════════════════
    let step2_start = Instant::now();

    // Get likers for each of the user's liked posts
    let mut coliker_weights: HashMap<String, f64> = HashMap::new();
    let mut coliker_shared_posts: HashMap<String, Vec<(String, f64, f64)>> = HashMap::new();
    let mut total_colikers_seen: std::collections::HashSet<String> =
        std::collections::HashSet::new();
    let mut post_likers_counts: HashMap<String, usize> = HashMap::new();

    for (post_id, user_like_time) in &used_likes {
        // Get likers from all date-based keys
        let likers_keys = Keys::post_likers_retention(post_id, DEFAULT_RETENTION_DAYS);
        let all_likers: Vec<(String, f64)> = redis
            .zrevrangebyscore_merged(&likers_keys, *user_like_time, 0.0, 10000)
            .await?;

        // Count other likers (excluding self) for proof
        let other_count = all_likers
            .iter()
            .filter(|(liker_hash, _)| liker_hash != &user_hash)
            .count();
        post_likers_counts.insert(post_id.clone(), other_count);

        let likers: Vec<(String, f64)> = all_likers
            .into_iter()
            .take(params.max_sources_per_post)
            .collect();

        for (liker_hash, liker_time) in likers {
            if liker_hash == user_hash {
                continue; // Skip self
            }

            total_colikers_seen.insert(liker_hash.clone());

            // Compute recency weight
            let age = now - liker_time;
            let recency = (-age.ln() / recency_half_life).exp();

            // Add to weights
            *coliker_weights.entry(liker_hash.clone()).or_insert(0.0) += recency;

            // Track shared posts
            coliker_shared_posts.entry(liker_hash).or_default().push((
                post_id.clone(),
                liker_time,
                *user_like_time,
            ));
        }
    }

    // Keep top N co-likers
    let mut coliker_vec: Vec<_> = coliker_weights.into_iter().collect();
    coliker_vec.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
    coliker_vec.truncate(params.max_total_sources);
    let coliker_weights: HashMap<String, f64> = coliker_vec.into_iter().collect();

    collector.set_coliker_info(
        total_colikers_seen.len(),
        coliker_weights.clone(),
        coliker_shared_posts,
    );
    collector.set_post_likers_counts(post_likers_counts);
    collector.set_step2_time(step2_start.elapsed().as_secs_f64() * 1000.0);

    // ═══════════════════════════════════════════════════════════════════
    // STEP 3: Score candidate posts
    // ═══════════════════════════════════════════════════════════════════
    let step3_start = Instant::now();

    let algo_posts_key = Keys::algo_posts(algo_id);
    let algo_counts_key = Keys::algo_posts_counts(algo_id);

    // Get candidate posts
    let candidate_posts: Vec<String> = redis.smembers(&algo_posts_key).await?;
    let candidate_pool_size = candidate_posts.len();

    // Get liker counts
    let post_refs: Vec<&str> = candidate_posts.iter().map(|s| s.as_str()).collect();
    let liker_counts: Vec<Option<String>> = redis.hmget(&algo_counts_key, &post_refs).await?;

    // Filter by min likes (use config setting)
    let min_likes = config.inverted_min_post_likes;
    let posts_with_counts: Vec<(String, usize)> = candidate_posts
        .into_iter()
        .zip(liker_counts)
        .filter_map(|(post_id, count_opt)| {
            let count = count_opt.and_then(|s| s.parse().ok()).unwrap_or(0);
            if count >= min_likes {
                Some((post_id, count))
            } else {
                None
            }
        })
        .collect();
    let posts_above_min = posts_with_counts.len();

    // Score posts
    let mut scored_posts: Vec<ScoredPostProofData> = Vec::new();
    let mut posts_scored = 0;
    let mut posts_skipped = 0;

    // Sample size: 0 means all posts, otherwise use configured limit
    let sample_limit = if config.prove_max_posts_to_sample == 0 {
        posts_with_counts.len()
    } else {
        config.prove_max_posts_to_sample
    };
    let max_coliker_weight = config.max_coliker_weight;

    for (post_id, liker_count) in posts_with_counts.iter().take(sample_limit) {
        // Get likers from all date-based keys
        let likers_keys = Keys::post_likers_retention(post_id, DEFAULT_RETENTION_DAYS);
        let likers: Vec<(String, f64)> = redis
            .zrevrangebyscore_merged(&likers_keys, f64::INFINITY, min_time, 10000)
            .await?;

        let mut raw_score = 0.0;
        let mut contributors: Vec<(String, f64, f64, f64)> = Vec::new();
        let mut overlapping = 0;

        for (liker_hash, like_time) in &likers {
            if let Some(coliker_weight) = coliker_weights.get(liker_hash) {
                overlapping += 1;
                // Cap the co-liker weight to prevent any single user from dominating
                let capped_weight = coliker_weight.min(max_coliker_weight);
                let age = now - like_time;
                let recency = (-age.ln() / recency_half_life).exp();
                let contribution = capped_weight * recency;
                raw_score += contribution;

                if contributors.len() < 10 {
                    contributors.push((liker_hash.clone(), *coliker_weight, recency, contribution));
                }
            }
        }

        if overlapping == 0 {
            posts_skipped += 1;
            continue;
        }

        posts_scored += 1;

        // Apply popularity penalty
        let popularity_penalty = 1.0 / (*liker_count as f64).powf(params.popularity_power);
        let score_after_popularity = raw_score * popularity_penalty;

        // Sort contributors by contribution
        contributors.sort_by(|a, b| b.3.partial_cmp(&a.3).unwrap());

        scored_posts.push(ScoredPostProofData {
            post_id: post_id.clone(),
            raw_score,
            popularity_penalty,
            score_after_popularity,
            final_score: score_after_popularity, // Will be updated by diversity
            total_likers: *liker_count,
            overlapping_likers: overlapping,
            contributors,
        });
    }

    // Sort by score
    scored_posts.sort_by(|a, b| {
        b.score_after_popularity
            .partial_cmp(&a.score_after_popularity)
            .unwrap()
    });
    scored_posts.truncate(50); // Keep top 50 for proof

    collector.set_scoring_info(
        candidate_pool_size,
        posts_above_min,
        posts_scored,
        posts_skipped,
        min_likes,
    );

    for post in &scored_posts {
        collector.add_scored_post(
            post.post_id.clone(),
            post.raw_score,
            post.popularity_penalty,
            post.score_after_popularity,
            post.final_score,
            post.total_likers,
            post.overlapping_likers,
            post.contributors.clone(),
        );
    }

    collector.set_step3_time(step3_start.elapsed().as_secs_f64() * 1000.0);

    // ═══════════════════════════════════════════════════════════════════
    // Diversity (simplified for proof)
    // ═══════════════════════════════════════════════════════════════════
    let diversity_start = Instant::now();

    collector.set_diversity_info(
        scored_posts.len(),
        scored_posts.len().min(15),
        0, // Would need to compute
        0,
        0,
    );
    collector.set_diversity_time(diversity_start.elapsed().as_secs_f64() * 1000.0);

    // Build final proof
    collector.build().await
}
