//! Author diversity re-ranking for feed results.
//!
//! This module implements three diversity mechanisms:
//! 1. Diminishing returns - each subsequent post from same author gets reduced score
//! 2. Hard cap - maximum N posts per author
//! 3. MMR-style selection - penalize posts similar to already-selected posts

use rustc_hash::FxHashMap;
use tracing::debug;

/// Configuration for diversity re-ranking.
#[derive(Debug, Clone)]
pub struct DiversityConfig {
    /// Enable diversity re-ranking.
    pub enabled: bool,
    /// Maximum posts from the same author in results.
    pub max_posts_per_author: usize,
    /// Diminishing returns factor (0.5 = each subsequent post gets 50% of previous).
    pub diminishing_factor: f64,
    /// MMR lambda: balance between relevance (0.0) and diversity (1.0).
    pub mmr_lambda: f64,
}

impl Default for DiversityConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_posts_per_author: 3,
            diminishing_factor: 0.5,
            mmr_lambda: 0.3,
        }
    }
}

/// Extract author DID from an AT-URI.
/// Format: at://did:plc:xxxx/app.bsky.feed.post/yyyy
fn extract_author_did(uri: &str) -> Option<&str> {
    uri.strip_prefix("at://").and_then(|s| s.split('/').next())
}

/// Result of diversity re-ranking with statistics.
#[derive(Debug, Clone)]
pub struct DiversityResult {
    /// Re-ranked posts: (score, post_id, uri)
    pub posts: Vec<(f64, String, String)>,
    /// Number of unique authors in results
    pub unique_authors: usize,
    /// Number of posts demoted due to diversity
    pub posts_demoted: usize,
    /// Number of posts removed due to author cap
    pub posts_removed_by_cap: usize,
}

/// Apply diversity re-ranking to scored posts.
///
/// This implements a greedy MMR-style selection with diminishing returns:
/// 1. Start with empty result set
/// 2. For each candidate post:
///    - Calculate diversity-adjusted score based on how many posts from same author are selected
///    - Apply diminishing factor for same-author posts
/// 3. Select posts greedily by adjusted score
/// 4. Apply hard cap per author
///
/// # Arguments
/// * `scored_posts` - Posts with scores: (score, post_id)
/// * `post_uris` - Map from post_id to URI (for author extraction)
/// * `config` - Diversity configuration
/// * `limit` - Maximum posts to return
///
/// # Returns
/// DiversityResult with re-ranked posts
pub fn diversify_posts(
    scored_posts: &[(f64, String)],
    post_uris: &FxHashMap<String, String>,
    config: &DiversityConfig,
    limit: usize,
) -> DiversityResult {
    if !config.enabled || scored_posts.is_empty() {
        // No diversity - just return posts with URIs
        let posts: Vec<(f64, String, String)> = scored_posts
            .iter()
            .take(limit)
            .filter_map(|(score, post_id)| {
                let uri = post_uris.get(post_id)?;
                Some((*score, post_id.clone(), uri.clone()))
            })
            .collect();

        return DiversityResult {
            unique_authors: count_unique_authors(&posts),
            posts,
            posts_demoted: 0,
            posts_removed_by_cap: 0,
        };
    }

    // Track author counts for diminishing returns
    let mut author_counts: FxHashMap<String, usize> = FxHashMap::default();
    let mut selected: Vec<(f64, String, String)> = Vec::with_capacity(limit);
    let mut posts_demoted = 0;
    let mut posts_removed_by_cap = 0;

    // Create candidates with their URIs and authors
    let mut candidates: Vec<(f64, String, String, String)> = scored_posts
        .iter()
        .filter_map(|(score, post_id)| {
            let uri = post_uris.get(post_id)?;
            let author = extract_author_did(uri)?.to_string();
            Some((*score, post_id.clone(), uri.clone(), author))
        })
        .collect();

    // Sort by original score descending
    candidates.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));

    // Greedy selection with diversity
    for (original_score, post_id, uri, author) in candidates {
        if selected.len() >= limit {
            break;
        }

        let author_count = *author_counts.get(&author).unwrap_or(&0);

        // Check hard cap
        if author_count >= config.max_posts_per_author {
            posts_removed_by_cap += 1;
            continue;
        }

        // Calculate diversity-adjusted score
        // Each subsequent post from same author gets diminishing_factor^n of original score
        let diminishing_penalty = config.diminishing_factor.powi(author_count as i32);

        // MMR-style: score = λ * relevance - (1-λ) * similarity_to_selected
        // Here we simplify: similarity penalty based on same-author posts already selected
        let similarity_penalty = if author_count > 0 {
            config.mmr_lambda * (1.0 - diminishing_penalty)
        } else {
            0.0
        };

        let adjusted_score = original_score * diminishing_penalty * (1.0 - similarity_penalty);

        if author_count > 0 {
            posts_demoted += 1;
        }

        // Update author count
        *author_counts.entry(author).or_insert(0) += 1;

        selected.push((adjusted_score, post_id, uri));
    }

    // Re-sort by adjusted score (since greedy selection may have changed order)
    selected.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));

    let unique_authors = author_counts.len();

    debug!(
        total_candidates = scored_posts.len(),
        selected_count = selected.len(),
        unique_authors,
        posts_demoted,
        posts_removed_by_cap,
        "diversity_applied"
    );

    DiversityResult {
        posts: selected,
        unique_authors,
        posts_demoted,
        posts_removed_by_cap,
    }
}

/// Count unique authors in a list of posts.
fn count_unique_authors(posts: &[(f64, String, String)]) -> usize {
    posts
        .iter()
        .filter_map(|(_, _, uri)| extract_author_did(uri))
        .collect::<std::collections::HashSet<_>>()
        .len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_author_did() {
        let uri = "at://did:plc:abc123/app.bsky.feed.post/xyz789";
        assert_eq!(extract_author_did(uri), Some("did:plc:abc123"));

        let invalid = "invalid-uri";
        assert_eq!(extract_author_did(invalid), None);
    }

    #[test]
    fn test_diversity_disabled() {
        let posts = vec![(1.0, "post1".to_string()), (0.9, "post2".to_string())];
        let mut uris = FxHashMap::default();
        uris.insert(
            "post1".to_string(),
            "at://did:plc:author1/app.bsky.feed.post/1".to_string(),
        );
        uris.insert(
            "post2".to_string(),
            "at://did:plc:author1/app.bsky.feed.post/2".to_string(),
        );

        let config = DiversityConfig {
            enabled: false,
            ..Default::default()
        };

        let result = diversify_posts(&posts, &uris, &config, 10);
        assert_eq!(result.posts.len(), 2);
        assert_eq!(result.posts_demoted, 0);
    }

    #[test]
    fn test_diminishing_returns() {
        let posts = vec![
            (1.0, "post1".to_string()),
            (0.9, "post2".to_string()),
            (0.8, "post3".to_string()),
        ];
        let mut uris = FxHashMap::default();
        // All from same author
        uris.insert(
            "post1".to_string(),
            "at://did:plc:author1/app.bsky.feed.post/1".to_string(),
        );
        uris.insert(
            "post2".to_string(),
            "at://did:plc:author1/app.bsky.feed.post/2".to_string(),
        );
        uris.insert(
            "post3".to_string(),
            "at://did:plc:author1/app.bsky.feed.post/3".to_string(),
        );

        let config = DiversityConfig {
            enabled: true,
            max_posts_per_author: 3,
            diminishing_factor: 0.5,
            mmr_lambda: 0.0, // Disable MMR for this test
        };

        let result = diversify_posts(&posts, &uris, &config, 10);

        // First post should have full score
        assert!((result.posts[0].0 - 1.0).abs() < 0.01);
        // Second post should have 0.9 * 0.5 = 0.45
        assert!((result.posts[1].0 - 0.45).abs() < 0.01);
        // Third post should have 0.8 * 0.25 = 0.2
        assert!((result.posts[2].0 - 0.2).abs() < 0.01);
    }

    #[test]
    fn test_max_posts_per_author() {
        let posts = vec![
            (1.0, "post1".to_string()),
            (0.9, "post2".to_string()),
            (0.8, "post3".to_string()),
            (0.7, "post4".to_string()),
        ];
        let mut uris = FxHashMap::default();
        // All from same author
        for (i, (_, id)) in posts.iter().enumerate() {
            uris.insert(
                id.clone(),
                format!("at://did:plc:author1/app.bsky.feed.post/{}", i),
            );
        }

        let config = DiversityConfig {
            enabled: true,
            max_posts_per_author: 2,
            diminishing_factor: 0.5,
            mmr_lambda: 0.0,
        };

        let result = diversify_posts(&posts, &uris, &config, 10);

        // Should only have 2 posts (max per author)
        assert_eq!(result.posts.len(), 2);
        assert_eq!(result.posts_removed_by_cap, 2);
    }

    #[test]
    fn test_diverse_authors_preferred() {
        let posts = vec![
            (1.0, "post1".to_string()),  // author1
            (0.95, "post2".to_string()), // author1
            (0.9, "post3".to_string()),  // author2
            (0.85, "post4".to_string()), // author2
        ];
        let mut uris = FxHashMap::default();
        uris.insert(
            "post1".to_string(),
            "at://did:plc:author1/app.bsky.feed.post/1".to_string(),
        );
        uris.insert(
            "post2".to_string(),
            "at://did:plc:author1/app.bsky.feed.post/2".to_string(),
        );
        uris.insert(
            "post3".to_string(),
            "at://did:plc:author2/app.bsky.feed.post/1".to_string(),
        );
        uris.insert(
            "post4".to_string(),
            "at://did:plc:author2/app.bsky.feed.post/2".to_string(),
        );

        let config = DiversityConfig {
            enabled: true,
            max_posts_per_author: 3,
            diminishing_factor: 0.5,
            mmr_lambda: 0.3,
        };

        let result = diversify_posts(&posts, &uris, &config, 4);

        // Should have 2 unique authors
        assert_eq!(result.unique_authors, 2);
        // First post should still be post1 (highest score)
        assert_eq!(result.posts[0].1, "post1");
    }
}
