//! Author-Level Co-Liker Pre-computation.
//!
//! This module pre-computes "users who liked similar authors" relationships
//! to provide denser connections than post-level co-likers.
//!
//! The computation:
//! 1. Gets authors a user has liked (from ula:{hash})
//! 2. For each liked author, finds other users who liked them (from authl:{hash})
//! 3. Aggregates weights: more shared authors = higher weight
//! 4. Stores top N author-level co-likers with their accumulated weights
//!
//! This creates a thicker network than post-level co-likers because:
//! - Author-level: Match on any posts from an author (dense)
//! - Post-level: Must match on exact posts (sparse)
//!
//! Used to supplement post-level personalization when it doesn't fill the feed.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use tracing::debug;

use crate::config::Config;
use crate::error::Result;
use graze_common::{Keys, RedisClient, DEFAULT_RETENTION_DAYS};

/// How a followed author is weighted when the seed came from follows rather than likes.
///
/// The like path weights each seed author by `sqrt(like_count)` -- more likes to an author is a
/// stronger signal. A follow has no such count, so a weight has to be *chosen*. This is the whole
/// reason follow-seeding is not a drop-in seed swap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FollowWeightMode {
    /// Every followed author counts the same. Loses the affinity signal, but assumes nothing.
    Uniform,
    /// Weight `1/sqrt(|likers|)`: following a niche account says more about a user than following a
    /// huge one. Same intuition as LinkLonk's fairness term, which production already relies on to
    /// stop prolific likers dominating.
    ///
    /// ⚠️ The popularity proxy is the author's liker list as returned by the fan-out, which is
    /// LIMITed to `AUTHOR_AFFINITY_MAX_LIKERS_PER_AUTHOR` (default 100). So every author at or above
    /// that cap gets an identical weight, and the mode only discriminates below it -- precisely
    /// where discrimination matters least. Reading true cardinality would cost another ZCARD per
    /// author per date shard, roughly doubling the fan-out. Anyone evaluating this mode needs to
    /// know the signal is compressed, rather than concluding from a null that the idea is wrong.
    InversePopularity,
}

impl FollowWeightMode {
    pub fn from_config(value: &str) -> Self {
        match value.trim().to_ascii_lowercase().as_str() {
            "inverse_popularity" | "inverse" => Self::InversePopularity,
            _ => Self::Uniform,
        }
    }
}

/// Which store the seed authors came from.
enum Seed {
    /// `(author_hash, like_count)` from `ula:`.
    Likes(Vec<(String, f64)>),
    /// `author_hash` from `uf:` -- no strength signal attached.
    Follows(Vec<String>),
}

impl Seed {
    fn author_hashes(&self) -> Vec<&str> {
        match self {
            Self::Likes(v) => v.iter().map(|(h, _)| h.as_str()).collect(),
            Self::Follows(v) => v.iter().map(|h| h.as_str()).collect(),
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Likes(v) => v.len(),
            Self::Follows(v) => v.len(),
        }
    }

    fn kind(&self) -> &'static str {
        match self {
            Self::Likes(_) => "likes",
            Self::Follows(_) => "follows",
        }
    }

    /// Weight contributed by seed author `idx` to each of its likers.
    fn weight_for(&self, idx: usize, liker_count: usize, mode: FollowWeightMode) -> f64 {
        match self {
            Self::Likes(v) => v[idx].1.sqrt(),
            Self::Follows(_) => match mode {
                FollowWeightMode::Uniform => 1.0,
                FollowWeightMode::InversePopularity => 1.0 / (liker_count.max(1) as f64).sqrt(),
            },
        }
    }
}

/// Computes and caches author-level co-liker aggregations for users.
pub struct AuthorColikerWorker {
    redis: Arc<RedisClient>,
    config: Arc<Config>,
}

impl AuthorColikerWorker {
    /// Create a new author co-liker worker.
    pub fn new(redis: Arc<RedisClient>, config: Arc<Config>) -> Self {
        Self { redis, config }
    }

    /// Get or compute author-level co-liker weights for a user.
    ///
    /// Checks if pre-computed author co-likers exist and are fresh.
    /// If not, it triggers computation.
    pub async fn get_or_compute_author_colikes(
        &self,
        user_hash: &str,
        force_refresh: bool,
    ) -> Result<HashMap<String, f64>> {
        if !self.config.author_affinity_enabled {
            debug!(user_hash = %&user_hash[..8.min(user_hash.len())], "early_exit: author affinity disabled");
            return Ok(HashMap::new());
        }

        let author_colikes_key = Keys::author_colikes(user_hash);

        if !force_refresh {
            // Check if cache exists and is fresh enough
            let ttl = self.redis.ttl(&author_colikes_key).await?;

            if ttl > self.config.author_affinity_refresh_threshold_seconds as i64 {
                // Cache is fresh, return it
                return self.get_cached_author_colikes(&author_colikes_key).await;
            } else if ttl > 0 {
                // Cache exists but stale, return it anyway
                let cached = self.get_cached_author_colikes(&author_colikes_key).await?;
                if !cached.is_empty() {
                    return Ok(cached);
                }
            }
        }

        // Cache miss or force refresh - compute now
        self.compute_author_colikes(user_hash).await
    }

    /// Compute author-level co-liker aggregation for a user.
    pub async fn compute_author_colikes(&self, user_hash: &str) -> Result<HashMap<String, f64>> {
        let start_time = Instant::now();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs_f64();
        let time_window_seconds = self.config.author_affinity_time_window_hours as f64 * 3600.0;
        let min_time = now - time_window_seconds;

        let max_authors = self.config.author_affinity_max_authors;
        let max_colikers = self.config.author_affinity_max_colikers;
        let max_likers_per_author = self.config.author_affinity_max_likers_per_author;
        let min_author_likes = self.config.author_affinity_min_author_likes;

        // Step 1: Get authors this user has liked (from ula:{user_hash})
        // Sorted by like count (weight)
        let user_liked_authors_key = Keys::user_liked_authors(user_hash);
        let liked_authors = self
            .redis
            .zrevrange_with_scores(&user_liked_authors_key, 0, (max_authors - 1) as isize)
            .await?;

        // Filter authors with insufficient likes
        let liked_authors: Vec<(String, f64)> = liked_authors
            .into_iter()
            .filter(|(_, like_count)| *like_count >= min_author_likes as f64)
            .collect();

        // Fall back to the follow graph when the like-based seed yields nothing usable.
        //
        // Note this covers BOTH previous early exits -- no liked authors at all, and none meeting
        // min_author_likes. The filter is also why follows cannot reuse this path directly: a
        // follow has no like count, so it would be rejected here and then weighted sqrt(0)=0.
        //
        // Measured: no_user_data is 98.1% of addressable non-personalization, 73.6% of those users
        // have zero likes in 365 days, and 86% of them follow 10+ accounts (median 50).
        let seed = if !liked_authors.is_empty() {
            Seed::Likes(liked_authors)
        } else if self.config.follow_seed_read_enabled {
            let follows = self
                .redis
                .zrevrange(
                    &Keys::user_follows(user_hash),
                    0,
                    (max_authors - 1) as isize,
                )
                .await?;
            if follows.is_empty() {
                debug!(
                    user_hash = %&user_hash[..8.min(user_hash.len())],
                    min_author_likes,
                    "early_exit: no usable like seed and no follow seed"
                );
                return Ok(HashMap::new());
            }
            Seed::Follows(follows)
        } else {
            debug!(
                user_hash = %&user_hash[..8.min(user_hash.len())],
                min_author_likes,
                "early_exit: no authors meet min_author_likes threshold"
            );
            return Ok(HashMap::new());
        };
        let weight_mode = FollowWeightMode::from_config(&self.config.follow_seed_weight_mode);

        // Step 2: For each liked author, get other users who liked them (pipelined)
        // Build date-based key groups for each author
        let author_key_groups: Vec<Vec<String>> = seed
            .author_hashes()
            .into_iter()
            .map(|author_hash| Keys::author_likers_retention(author_hash, DEFAULT_RETENTION_DAYS))
            .collect();

        // Fetch from all date-based keys and merge results per author
        let all_likers = self
            .redis
            .zrevrangebyscore_merged_multi(&author_key_groups, now, min_time, max_likers_per_author)
            .await?;

        // Aggregate weights from all results
        let mut source_weights: HashMap<String, f64> = HashMap::new();

        for (idx, likers) in all_likers.iter().enumerate() {
            // Computed once per seed author rather than per liker: for the like path it is a sqrt of
            // a constant, and for inverse_popularity it depends on the liker-list length, not the
            // individual liker.
            let weight = seed.weight_for(idx, likers.len(), weight_mode);
            for (source_hash, _like_time) in likers {
                // Skip self
                if source_hash == user_hash {
                    continue;
                }
                *source_weights.entry(source_hash.clone()).or_insert(0.0) += weight;
            }
        }

        if source_weights.is_empty() {
            debug!(
                user_hash = %&user_hash[..8.min(user_hash.len())],
                authors_checked = seed.len(),
            seed_kind = seed.kind(),
                "early_exit: no co-likers after self-exclusion"
            );
            return Ok(HashMap::new());
        }

        // Sort by weight and keep top N
        let mut sorted_sources: Vec<(String, f64)> = source_weights.into_iter().collect();
        sorted_sources.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        sorted_sources.truncate(max_colikers);

        // Store in Redis sorted set
        let author_colikes_key = Keys::author_colikes(user_hash);

        self.redis.del(&author_colikes_key).await?;

        let items: Vec<(f64, &str)> = sorted_sources
            .iter()
            .map(|(hash, weight)| (*weight, hash.as_str()))
            .collect();
        self.redis.zadd(&author_colikes_key, &items).await?;
        self.redis
            .expire(
                &author_colikes_key,
                self.config.author_affinity_ttl_seconds as i64,
            )
            .await?;

        let compute_time = start_time.elapsed();
        debug!(
            user_hash = %&user_hash[..8.min(user_hash.len())],
            sources = sorted_sources.len(),
            authors_checked = seed.len(),
            seed_kind = seed.kind(),
            compute_time_ms = compute_time.as_millis(),
            "author_colikes_computed"
        );

        Ok(sorted_sources.into_iter().collect())
    }

    /// Get cached author co-likers from Redis.
    async fn get_cached_author_colikes(&self, key: &str) -> Result<HashMap<String, f64>> {
        let results = self
            .redis
            .zrevrangebyscore_with_scores(key, f64::INFINITY, f64::NEG_INFINITY, 10000)
            .await?;

        Ok(results.into_iter().collect())
    }

    /// Invalidate cached author co-likers for a user.
    pub async fn invalidate_author_colikes(&self, user_hash: &str) -> Result<bool> {
        let author_colikes_key = Keys::author_colikes(user_hash);
        self.redis.del(&author_colikes_key).await?;
        Ok(true)
    }
}

#[cfg(test)]
mod follow_seed_tests {
    use super::*;

    #[test]
    fn weight_mode_parses_and_defaults_to_uniform() {
        assert_eq!(
            FollowWeightMode::from_config("inverse_popularity"),
            FollowWeightMode::InversePopularity
        );
        assert_eq!(
            FollowWeightMode::from_config("  INVERSE  "),
            FollowWeightMode::InversePopularity
        );
        assert_eq!(
            FollowWeightMode::from_config("uniform"),
            FollowWeightMode::Uniform
        );
        // Anything unrecognised must fall back to the assumption-free mode rather than panicking or
        // silently selecting the unvalidated one.
        assert_eq!(
            FollowWeightMode::from_config("typo"),
            FollowWeightMode::Uniform
        );
        assert_eq!(FollowWeightMode::from_config(""), FollowWeightMode::Uniform);
    }

    #[test]
    fn like_seed_keeps_the_sqrt_like_count_weight() {
        let seed = Seed::Likes(vec![("a".into(), 9.0), ("b".into(), 4.0)]);
        // Liker count and mode are irrelevant on the like path.
        assert_eq!(seed.weight_for(0, 50, FollowWeightMode::Uniform), 3.0);
        assert_eq!(
            seed.weight_for(1, 50, FollowWeightMode::InversePopularity),
            2.0
        );
    }

    /// The bug this whole design exists to avoid: a follow has no like count, so reusing the like
    /// weight would give every source sqrt(0) = 0 and the seed would silently do nothing.
    #[test]
    fn follow_seed_never_produces_a_zero_weight() {
        let seed = Seed::Follows(vec!["a".into(), "b".into()]);
        assert_eq!(seed.weight_for(0, 100, FollowWeightMode::Uniform), 1.0);
        let w = seed.weight_for(0, 100, FollowWeightMode::InversePopularity);
        assert!(
            w > 0.0,
            "inverse popularity weight must be positive, got {w}"
        );
    }

    #[test]
    fn inverse_popularity_prefers_niche_authors() {
        let seed = Seed::Follows(vec!["a".into()]);
        let niche = seed.weight_for(0, 4, FollowWeightMode::InversePopularity);
        let popular = seed.weight_for(0, 100, FollowWeightMode::InversePopularity);
        assert!(
            niche > popular,
            "following a niche author should count for more: {niche} vs {popular}"
        );
        assert_eq!(niche, 0.5); // 1/sqrt(4)
    }

    /// A zero-length liker list must not divide by zero.
    #[test]
    fn inverse_popularity_survives_an_empty_liker_list() {
        let seed = Seed::Follows(vec!["a".into()]);
        let w = seed.weight_for(0, 0, FollowWeightMode::InversePopularity);
        assert!(w.is_finite() && w > 0.0, "got {w}");
    }

    #[test]
    fn seed_reports_its_kind_and_size() {
        let likes = Seed::Likes(vec![("a".into(), 1.0)]);
        let follows = Seed::Follows(vec!["a".into(), "b".into()]);
        assert_eq!((likes.kind(), likes.len()), ("likes", 1));
        assert_eq!((follows.kind(), follows.len()), ("follows", 2));
        assert_eq!(follows.author_hashes(), vec!["a", "b"]);
    }
}
