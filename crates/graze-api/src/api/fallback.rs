//! Fallback tranches system for non-personalized content.
//!
//! Provides fallback content when personalization is insufficient:
//! - Popular: Total engagement with slow decay (7-day half-life)
//! - Velocity: Rate of engagement (not total)
//! - Discovery: Cold posts from successful authors
//! - Trending: Legacy fallback combining the above
//! - Author Affinity: Coarse LinkLonk using author-level co-likers

use std::collections::HashSet;
use std::sync::Arc;

use tracing::debug;

use crate::algorithm::{LinkLonkAlgorithm, LinkLonkParams};
use crate::config::Config;
use crate::error::Result;
use graze_common::services::UriInterner;
use graze_common::{Keys, RedisClient, DEFAULT_RETENTION_DAYS};

/// Source of a post in the blended feed (personalized path).
#[derive(Debug, Clone)]
pub enum BlendedSource {
    PostLevelPersonalization,
    AuthorAffinity,
    Fallback { tranche: String },
}

/// Get posts from a specific fallback tranche.
///
/// # Arguments
/// * `redis` - Redis client
/// * `interner` - URI interner for ID to URI conversion
/// * `algo_id` - Algorithm ID
/// * `tranche` - One of "popular", "velocity", "discovery", "trending" (legacy)
/// * `limit` - Maximum posts to return
/// * `offset` - Starting offset for ZREVRANGE pagination
/// * `exclude` - Set of post URIs to exclude (duplicates across tranches)
///
/// Returns list of post URIs.
pub async fn get_fallback_tranche(
    redis: &RedisClient,
    interner: &UriInterner,
    algo_id: i32,
    tranche: &str,
    limit: usize,
    offset: usize,
    exclude: &HashSet<String>,
) -> Result<Vec<String>> {
    // Get the appropriate key for this tranche
    let key = match tranche {
        "popular" => Keys::popular_posts(algo_id),
        "velocity" => Keys::velocity_posts(algo_id),
        "discovery" => Keys::discovery_posts(algo_id),
        _ => Keys::trending_posts(algo_id), // "trending" - legacy fallback
    };

    // Request extra to account for cross-tranche exclusions (cap at 50 to avoid huge fetches)
    let fetch_limit = limit + exclude.len().min(50) + 10;
    let start = offset as isize;
    let end = start + (fetch_limit as isize) - 1;
    let post_ids: Vec<String> = redis.zrevrange(&key, start, end).await.unwrap_or_default();

    if post_ids.is_empty() {
        // Fallback to random selection if tranche is exhausted (empty or paginated past end)
        // Only do this on the first page (offset=0) to avoid duplicate randoms across pages
        if offset == 0 {
            let algo_posts_key = Keys::algo_posts(algo_id);
            let random_ids = redis.srandmember(&algo_posts_key, fetch_limit).await?;
            if random_ids.is_empty() {
                return Ok(Vec::new());
            }
            return convert_ids_to_uris(interner, &random_ids, exclude, limit).await;
        }
        return Ok(Vec::new());
    }

    convert_ids_to_uris(interner, &post_ids, exclude, limit).await
}

/// Convert post IDs to URIs, filtering out excluded ones.
async fn convert_ids_to_uris(
    interner: &UriInterner,
    post_ids: &[String],
    exclude: &HashSet<String>,
    limit: usize,
) -> Result<Vec<String>> {
    // Convert IDs to integers
    let int_ids: Vec<i64> = post_ids
        .iter()
        .filter_map(|id| id.parse::<i64>().ok())
        .collect();

    if int_ids.is_empty() {
        return Ok(Vec::new());
    }

    // Batch lookup URIs
    let id_to_uri = interner.get_uris_batch(&int_ids).await?;

    // Filter out excluded posts and return up to limit
    let mut result = Vec::with_capacity(limit);
    for id in &int_ids {
        if let Some(uri) = id_to_uri.get(id) {
            if !exclude.contains(uri) {
                result.push(uri.clone());
                if result.len() >= limit {
                    break;
                }
            }
        }
    }

    Ok(result)
}

/// Get a blended mix of fallback tranches (popular, velocity, discovery).
///
/// The blend ratios are configured via Config:
/// - fallback_popular_ratio (default 0.34)
/// - fallback_trending_ratio (default 0.33) - velocity-based
/// - fallback_discovery_ratio (default 0.33)
///
/// # Arguments
/// * `fallback_offset` - Total offset into fallback content (divided by 3 for per-tranche offset)
///
/// Returns list of (post URI, tranche) with interleaved ordering. Tranche is "popular" | "velocity" | "discovery".
pub async fn get_fallback_blend(
    redis: &RedisClient,
    interner: &UriInterner,
    config: &Config,
    algo_id: i32,
    limit: usize,
    fallback_offset: usize,
    exclude: &HashSet<String>,
) -> Result<Vec<(String, String)>> {
    // Calculate counts for each tranche
    let popular_count = (limit as f64 * config.fallback_popular_ratio).max(1.0) as usize;
    let velocity_count = (limit as f64 * config.fallback_trending_ratio).max(1.0) as usize;
    let discovery_count = (limit as f64 * config.fallback_discovery_ratio).max(1.0) as usize;

    // Since we interleave ~evenly from 3 tranches, divide the total offset by 3
    let per_tranche_offset = fallback_offset / 3;

    // Fetch all tranches concurrently
    let (popular, velocity, discovery) = tokio::join!(
        get_fallback_tranche(
            redis,
            interner,
            algo_id,
            "popular",
            popular_count + 5,
            per_tranche_offset,
            exclude
        ),
        get_fallback_tranche(
            redis,
            interner,
            algo_id,
            "velocity",
            velocity_count + 5,
            per_tranche_offset,
            exclude
        ),
        get_fallback_tranche(
            redis,
            interner,
            algo_id,
            "discovery",
            discovery_count + 5,
            per_tranche_offset,
            exclude
        ),
    );

    let popular = popular.unwrap_or_default();
    let velocity = velocity.unwrap_or_default();
    let discovery = discovery.unwrap_or_default();

    // Dedupe across tranches (priority: popular > velocity > discovery)
    let mut seen: HashSet<String> = exclude.clone();

    let mut popular_deduped = Vec::new();
    for uri in popular {
        if !seen.contains(&uri) {
            popular_deduped.push(uri.clone());
            seen.insert(uri);
        }
    }

    let mut velocity_deduped = Vec::new();
    for uri in velocity {
        if !seen.contains(&uri) {
            velocity_deduped.push(uri.clone());
            seen.insert(uri);
        }
    }

    let mut discovery_deduped = Vec::new();
    for uri in discovery {
        if !seen.contains(&uri) {
            discovery_deduped.push(uri.clone());
            seen.insert(uri);
        }
    }

    // Trim to requested counts
    popular_deduped.truncate(popular_count);
    velocity_deduped.truncate(velocity_count);
    discovery_deduped.truncate(discovery_count);

    // Interleave the tranches for variety (round-robin style); tag each with tranche name
    let mut result = Vec::with_capacity(limit);
    let max_len = popular_deduped
        .len()
        .max(velocity_deduped.len())
        .max(discovery_deduped.len());

    for i in 0..max_len {
        if i < popular_deduped.len() {
            result.push((popular_deduped[i].clone(), "popular".to_string()));
        }
        if i < velocity_deduped.len() {
            result.push((velocity_deduped[i].clone(), "velocity".to_string()));
        }
        if i < discovery_deduped.len() {
            result.push((discovery_deduped[i].clone(), "discovery".to_string()));
        }
    }

    result.truncate(limit);
    Ok(result)
}

/// Stagger fallback posts into personalized list.
///
/// # Arguments
/// * `personalized` - Primary personalized posts (higher priority)
/// * `fallback` - Fallback posts to intersperse
/// * `stagger_factor` - 0.0 = fallback at end, 1.0 = fully interleaved
///
/// Returns combined list with fallback posts distributed according to stagger_factor.
pub fn stagger_posts(
    personalized: Vec<String>,
    fallback: Vec<String>,
    stagger_factor: f64,
) -> Vec<String> {
    stagger_posts_tagged(
        personalized
            .into_iter()
            .map(|u| (u, BlendedSource::PostLevelPersonalization))
            .collect(),
        fallback
            .into_iter()
            .map(|u| (u, BlendedSource::Fallback { tranche: String::new() }))
            .collect(),
        stagger_factor,
    )
    .into_iter()
    .map(|(u, _)| u)
    .collect()
}

/// Stagger tagged fallback into tagged personalized list; preserves source for each URI.
pub fn stagger_posts_tagged(
    personalized: Vec<(String, BlendedSource)>,
    fallback: Vec<(String, BlendedSource)>,
    stagger_factor: f64,
) -> Vec<(String, BlendedSource)> {
    if fallback.is_empty() {
        return personalized;
    }

    if stagger_factor <= 0.0 || personalized.is_empty() {
        let mut result = personalized;
        result.extend(fallback);
        return result;
    }

    if stagger_factor >= 1.0 {
        let mut result = Vec::with_capacity(personalized.len() + fallback.len());
        let mut p_idx = 0;
        let mut f_idx = 0;
        while p_idx < personalized.len() || f_idx < fallback.len() {
            if p_idx < personalized.len() {
                result.push(personalized[p_idx].clone());
                p_idx += 1;
            }
            if f_idx < fallback.len() {
                result.push(fallback[f_idx].clone());
                f_idx += 1;
            }
        }
        return result;
    }

    let n_stagger = ((fallback.len() as f64) * stagger_factor) as usize;
    let (staggered, remainder) = fallback.split_at(n_stagger.min(fallback.len()));
    let mut result = personalized.clone();

    if !staggered.is_empty() && !result.is_empty() {
        let interval = (result.len() / (staggered.len() + 1)).max(2);
        for (i, item) in staggered.iter().enumerate() {
            let insert_pos = ((i + 1) * interval + i).min(result.len());
            result.insert(insert_pos, item.clone());
        }
    }
    result.extend(remainder.iter().cloned());
    result
}

/// Result of blending personalized posts with fallback content.
#[derive(Debug, Clone, Default)]
pub struct BlendResult {
    /// Final blended posts (URIs only).
    pub posts: Vec<String>,
    /// Posts with provenance source for feedContext.
    pub posts_with_source: Vec<(String, BlendedSource)>,
    /// Number of posts from post-level personalization.
    pub personalized_count: usize,
    /// Number of posts from author affinity (coarse LinkLonk).
    pub author_affinity_count: usize,
    /// Number of posts from fallback tranches.
    pub fallback_count: usize,
}

/// Blend personalized posts with author-affinity and fallback tranches.
///
/// Uses a tiered approach:
/// 1. Post-level personalization fills up to personalization_ratio (default 80%)
/// 2. If post-level is thin, author-affinity supplements to reach 80%
/// 3. Remaining 20% filled with fallback blend (popular/velocity/discovery)
/// 4. Fallback posts are staggered into personalized based on stagger_factor
///
/// For cold/warm users:
/// - 0-5 likes: Higher fallback ratio
/// - 6-20 likes: Medium fallback ratio
/// - 21+ likes: Use configured personalization_ratio
///
/// Returns a BlendResult with the posts and breakdown of post sources.
#[allow(clippy::too_many_arguments)]
pub async fn get_blended_posts_with_stats(
    redis: &RedisClient,
    interner: &UriInterner,
    config: &Config,
    algorithm: &Arc<LinkLonkAlgorithm>,
    user_hash: &str,
    algo_id: i32,
    limit: usize,
    personalized_posts: Vec<String>,
) -> Result<BlendResult> {
    // Get user's like count for cold/warm handling
    let like_count = get_user_like_count(redis, user_hash).await;

    // Determine effective personalization ratio based on user engagement
    let personalization_ratio = if like_count <= config.cold_user_max_likes as u64 {
        // Cold user: invert old trending_ratio to get personalization
        1.0 - config.cold_user_trending_ratio
    } else if like_count <= config.warm_user_max_likes as u64 {
        // Warm user
        1.0 - config.warm_user_trending_ratio
    } else {
        // Active user: use configured ratio
        config.fallback_personalization_ratio
    };

    // Calculate slot counts
    let personalized_target = ((limit as f64) * personalization_ratio) as usize;
    let fallback_count = limit - personalized_target;

    // Take personalized posts up to the target; tag as post-level personalization
    let personalized_from_linklonk: Vec<(String, BlendedSource)> = personalized_posts
        .into_iter()
        .take(personalized_target)
        .map(|u| (u, BlendedSource::PostLevelPersonalization))
        .collect();
    let personalized_count = personalized_from_linklonk.len();

    let mut personalized_final: Vec<(String, BlendedSource)> = personalized_from_linklonk;
    let mut exclude_set: HashSet<String> = personalized_final.iter().map(|(u, _)| u.clone()).collect();

    // Author-affinity supplementation: if post-level personalization is thin,
    // use author-level co-likers (coarse LinkLonk) to fill the gap
    let shortfall = personalized_target.saturating_sub(personalized_final.len());
    let mut author_affinity_count = 0;

    if shortfall > 0 && config.author_affinity_enabled {
        debug!(
            shortfall,
            personalized_count = personalized_final.len(),
            target = personalized_target,
            "author_affinity_supplementation_start"
        );

        let params = LinkLonkParams {
            use_author_affinity: true,
            ..Default::default()
        };

        match algorithm
            .compute_personalization(user_hash, algo_id, &params, None)
            .await
        {
            Ok(result) => {
                let scored_posts: Vec<(f64, i64)> = result
                    .scored_posts
                    .iter()
                    .take(shortfall * 2)
                    .filter_map(|(score, post_id)| {
                        post_id.parse::<i64>().ok().map(|id| (*score, id))
                    })
                    .collect();
                let post_ids: Vec<i64> = scored_posts.iter().map(|(_, id)| *id).collect();
                let uri_map = interner.get_uris_batch(&post_ids).await.unwrap_or_default();

                for (_, post_id) in scored_posts {
                    if author_affinity_count >= shortfall {
                        break;
                    }
                    if let Some(uri) = uri_map.get(&post_id) {
                        if !exclude_set.contains(uri) {
                            exclude_set.insert(uri.clone());
                            personalized_final.push((
                                uri.clone(),
                                BlendedSource::AuthorAffinity,
                            ));
                            author_affinity_count += 1;
                        }
                    }
                }

                debug!(
                    author_affinity_posts = author_affinity_count,
                    "author_affinity_supplementation_complete"
                );
            }
            Err(e) => {
                debug!(error = %e, "author_affinity_supplementation_failed");
            }
        }
    }

    let fallback_tagged = if fallback_count > 0 {
        get_fallback_blend(
            redis,
            interner,
            config,
            algo_id,
            fallback_count,
            0,
            &exclude_set,
        )
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|(u, tranche)| (u, BlendedSource::Fallback { tranche }))
        .collect()
    } else {
        Vec::new()
    };
    let actual_fallback_count = fallback_tagged.len();

    let blended = stagger_posts_tagged(
        personalized_final,
        fallback_tagged,
        config.fallback_stagger_factor,
    );
    let posts_with_source: Vec<(String, BlendedSource)> = blended.into_iter().take(limit).collect();
    let posts: Vec<String> = posts_with_source.iter().map(|(u, _)| u.clone()).collect();

    Ok(BlendResult {
        posts,
        posts_with_source,
        personalized_count,
        author_affinity_count,
        fallback_count: actual_fallback_count,
    })
}

/// Blend personalized posts with author-affinity and fallback tranches.
/// Returns only the posts (convenience wrapper for backward compatibility).
#[allow(clippy::too_many_arguments)]
pub async fn get_blended_posts(
    redis: &RedisClient,
    interner: &UriInterner,
    config: &Config,
    algorithm: &Arc<LinkLonkAlgorithm>,
    user_hash: &str,
    algo_id: i32,
    limit: usize,
    personalized_posts: Vec<String>,
) -> Result<Vec<String>> {
    let result = get_blended_posts_with_stats(
        redis,
        interner,
        config,
        algorithm,
        user_hash,
        algo_id,
        limit,
        personalized_posts,
    )
    .await?;
    Ok(result.posts)
}

/// Get the total number of likes a user has in Redis (across all date-based keys).
async fn get_user_like_count(redis: &RedisClient, user_hash: &str) -> u64 {
    // Sum cardinalities from all date-based keys within retention window
    let keys: Vec<String> = Keys::user_likes_retention(user_hash, DEFAULT_RETENTION_DAYS);
    let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
    let counts = redis.zcard_multi(&key_refs).await.unwrap_or_default();
    counts.into_iter().sum::<usize>() as u64
}

/// Get popular posts as a simple fallback (legacy function).
pub async fn get_popular_posts(
    redis: &RedisClient,
    interner: &UriInterner,
    config: &Config,
    algo_id: i32,
    limit: usize,
) -> Vec<String> {
    get_fallback_blend(redis, interner, config, algo_id, limit, 0, &HashSet::new())
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|(u, _)| u)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stagger_posts_empty_fallback() {
        let personalized = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let fallback = vec![];
        let result = stagger_posts(personalized.clone(), fallback, 0.5);
        assert_eq!(result, personalized);
    }

    #[test]
    fn test_stagger_posts_empty_personalized() {
        let personalized = vec![];
        let fallback = vec!["x".to_string(), "y".to_string()];
        let result = stagger_posts(personalized, fallback.clone(), 0.5);
        assert_eq!(result, fallback);
    }

    #[test]
    fn test_stagger_posts_zero_factor() {
        let personalized = vec!["a".to_string(), "b".to_string()];
        let fallback = vec!["x".to_string(), "y".to_string()];
        let result = stagger_posts(personalized, fallback, 0.0);
        assert_eq!(result, vec!["a", "b", "x", "y"]);
    }

    #[test]
    fn test_stagger_posts_full_interleave() {
        let personalized = vec!["a".to_string(), "b".to_string()];
        let fallback = vec!["x".to_string(), "y".to_string()];
        let result = stagger_posts(personalized, fallback, 1.0);
        assert_eq!(result, vec!["a", "x", "b", "y"]);
    }

    #[test]
    fn test_stagger_posts_partial() {
        let personalized = vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "d".to_string(),
        ];
        let fallback = vec!["x".to_string(), "y".to_string(), "z".to_string()];
        let result = stagger_posts(personalized, fallback, 0.3);
        // With stagger_factor 0.3, only ~1 of 3 fallback posts should be interspersed
        // The rest go at the end
        assert!(result.contains(&"a".to_string()));
        assert!(result.contains(&"x".to_string()));
        assert_eq!(result.len(), 7);
    }
}
