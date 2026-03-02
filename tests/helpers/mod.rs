//! Test helpers and fixtures for Graze integration tests.
//!
//! This module provides utilities for testing including:
//! - LikeGraphBuilder: A fluent builder for populating Redis with test like data
//! - Test user/author/post creation helpers
//! - Time constants
//! - Mock data generators

#![allow(dead_code)]

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use graze::redis::{hash_did, Keys, RedisClient};

/// Current time in seconds since Unix epoch.
pub fn now() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs_f64()
}

/// Time constants for test scenarios.
pub const HOUR: f64 = 3600.0;
pub const DAY: f64 = 86400.0;
pub const WEEK: f64 = 604800.0;

/// A test user with DID and hash.
#[derive(Debug, Clone)]
pub struct TestUser {
    pub did: String,
    pub did_hash: String,
}

impl TestUser {
    /// Create a test user with a given name.
    pub fn new(name: &str) -> Self {
        let did = format!("did:plc:test{}", name);
        let did_hash = hash_did(&did);
        Self { did, did_hash }
    }

    /// Create a test user with a specific DID.
    pub fn with_did(did: &str) -> Self {
        let did_hash = hash_did(did);
        Self {
            did: did.to_string(),
            did_hash,
        }
    }
}

/// A test author with DID and hash.
#[derive(Debug, Clone)]
pub struct TestAuthor {
    pub did: String,
    pub did_hash: String,
}

impl TestAuthor {
    /// Create a test author with a given name.
    pub fn new(name: &str) -> Self {
        let did = format!("did:plc:author{}", name);
        let did_hash = hash_did(&did);
        Self { did, did_hash }
    }
}

/// A test post with URI and ID.
#[derive(Debug, Clone)]
pub struct TestPost {
    pub uri: String,
    pub id: String,
    pub author_did: String,
}

impl TestPost {
    /// Create a test post with a given author and rkey.
    pub fn new(author: &TestAuthor, rkey: &str) -> Self {
        let uri = format!("at://{}/app.bsky.feed.post/{}", author.did, rkey);
        // Post ID is typically a hash or interned ID; for tests we use the rkey
        Self {
            uri,
            id: rkey.to_string(),
            author_did: author.did.clone(),
        }
    }

    /// Create a test post with a simple ID.
    pub fn simple(id: &str) -> Self {
        Self {
            uri: format!("at://did:plc:testauthor/app.bsky.feed.post/{}", id),
            id: id.to_string(),
            author_did: "did:plc:testauthor".to_string(),
        }
    }
}

/// Builder for constructing a like graph in Redis for testing.
///
/// This provides a fluent API for setting up test scenarios:
///
/// ```ignore
/// let builder = LikeGraphBuilder::new(redis.clone());
/// builder
///     .add_like(&alice, &post1, now - DAY)
///     .add_like(&bob, &post1, now - 2.0 * DAY)
///     .add_like(&alice, &post2, now - HOUR)
///     .build()
///     .await?;
/// ```
pub struct LikeGraphBuilder {
    redis: std::sync::Arc<RedisClient>,
    /// Likes to add: (user, post_id, timestamp)
    likes: Vec<(TestUser, String, f64)>,
    /// Author likes to add: (user, author, like_count, timestamp)
    author_likes: Vec<(TestUser, TestAuthor, f64, f64)>,
    /// Algo posts to add: (algo_id, post_id)
    algo_posts: Vec<(i32, String)>,
    /// Trending posts: (algo_id, post_id, score)
    trending: Vec<(i32, String, f64)>,
    /// Coliker weights to pre-populate: (user_hash, co_likers)
    coliker_weights: Vec<(String, Vec<(String, f64)>)>,
}

impl LikeGraphBuilder {
    /// Create a new like graph builder.
    pub fn new(redis: std::sync::Arc<RedisClient>) -> Self {
        Self {
            redis,
            likes: Vec::new(),
            author_likes: Vec::new(),
            algo_posts: Vec::new(),
            trending: Vec::new(),
            coliker_weights: Vec::new(),
        }
    }

    /// Add a like from a user to a post.
    pub fn add_like(mut self, user: &TestUser, post_id: &str, timestamp: f64) -> Self {
        self.likes
            .push((user.clone(), post_id.to_string(), timestamp));
        self
    }

    /// Add a like from a user to an author.
    pub fn add_author_like(
        mut self,
        user: &TestUser,
        author: &TestAuthor,
        like_count: f64,
        timestamp: f64,
    ) -> Self {
        self.author_likes
            .push((user.clone(), author.clone(), like_count, timestamp));
        self
    }

    /// Add a post to an algorithm's eligible posts.
    pub fn add_algo_post(mut self, algo_id: i32, post_id: &str) -> Self {
        self.algo_posts.push((algo_id, post_id.to_string()));
        self
    }

    /// Add a trending post.
    pub fn add_trending(mut self, algo_id: i32, post_id: &str, score: f64) -> Self {
        self.trending.push((algo_id, post_id.to_string(), score));
        self
    }

    /// Pre-populate co-liker weights for a user.
    pub fn set_coliker_weights(mut self, user_hash: &str, weights: Vec<(String, f64)>) -> Self {
        self.coliker_weights.push((user_hash.to_string(), weights));
        self
    }

    /// Build the like graph in Redis.
    pub async fn build(self) -> graze::error::Result<()> {
        // Add likes
        for (user, post_id, timestamp) in self.likes {
            // Add to user's likes
            let user_likes_key = Keys::user_likes(&user.did_hash);
            self.redis
                .zadd(&user_likes_key, &[(timestamp, post_id.as_str())])
                .await?;

            // Add to post's likers
            let post_likers_key = Keys::post_likers(&post_id);
            self.redis
                .zadd(&post_likers_key, &[(timestamp, user.did_hash.as_str())])
                .await?;
        }

        // Add author likes
        for (user, author, like_count, timestamp) in self.author_likes {
            // Add to user's liked authors
            let user_liked_authors_key = Keys::user_liked_authors(&user.did_hash);
            self.redis
                .zadd(
                    &user_liked_authors_key,
                    &[(like_count, author.did_hash.as_str())],
                )
                .await?;

            // Add to author's likers
            let author_likers_key = Keys::author_likers(&author.did_hash);
            self.redis
                .zadd(&author_likers_key, &[(timestamp, user.did_hash.as_str())])
                .await?;
        }

        // Add algo posts
        let mut algo_posts_map: HashMap<i32, Vec<String>> = HashMap::new();
        for (algo_id, post_id) in self.algo_posts {
            algo_posts_map.entry(algo_id).or_default().push(post_id);
        }
        for (algo_id, post_ids) in algo_posts_map {
            let key = Keys::algo_posts(algo_id);
            // Use SADD via eval for set operations
            let members: Vec<&str> = post_ids.iter().map(|s| s.as_str()).collect();
            let script =
                "for i = 1, #ARGV do redis.call('SADD', KEYS[1], ARGV[i]) end return #ARGV";
            let args: Vec<&str> = members;
            let _: i64 = self.redis.eval(script, &[&key], &args).await?;
        }

        // Add trending posts
        let mut trending_map: HashMap<i32, Vec<(f64, String)>> = HashMap::new();
        for (algo_id, post_id, score) in self.trending {
            trending_map
                .entry(algo_id)
                .or_default()
                .push((score, post_id));
        }
        for (algo_id, items) in trending_map {
            let key = Keys::trending(algo_id);
            let refs: Vec<(f64, &str)> = items.iter().map(|(s, p)| (*s, p.as_str())).collect();
            self.redis.zadd(&key, &refs).await?;
        }

        // Pre-populate co-liker weights
        for (user_hash, weights) in self.coliker_weights {
            let colikes_key = Keys::colikes(&user_hash);
            let items: Vec<(f64, &str)> = weights
                .iter()
                .map(|(hash, weight)| (*weight, hash.as_str()))
                .collect();
            self.redis.zadd(&colikes_key, &items).await?;
        }

        Ok(())
    }
}

/// Extract author DID from an AT-URI.
pub fn extract_author_did(uri: &str) -> Option<String> {
    if !uri.starts_with("at://") {
        return None;
    }

    let rest = &uri[5..]; // Skip "at://"
    let parts: Vec<&str> = rest.split('/').collect();

    if parts.is_empty() {
        return None;
    }

    let did = parts[0];
    if !did.starts_with("did:") {
        return None;
    }

    Some(did.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_user_creation() {
        let user = TestUser::new("alice");
        assert!(user.did.starts_with("did:plc:test"));
        assert_eq!(user.did_hash.len(), 16);
    }

    #[test]
    fn test_author_creation() {
        let author = TestAuthor::new("bob");
        assert!(author.did.starts_with("did:plc:author"));
        assert_eq!(author.did_hash.len(), 16);
    }

    #[test]
    fn test_post_creation() {
        let author = TestAuthor::new("writer");
        let post = TestPost::new(&author, "abc123");
        assert!(post.uri.contains("app.bsky.feed.post"));
        assert!(post.uri.contains(&author.did));
    }

    #[test]
    fn test_extract_author_did() {
        assert_eq!(
            extract_author_did("at://did:plc:author123/app.bsky.feed.post/rkey456"),
            Some("did:plc:author123".to_string())
        );
        assert_eq!(
            extract_author_did("at://did:web:example.com/app.bsky.feed.post/xyz"),
            Some("did:web:example.com".to_string())
        );
        assert_eq!(extract_author_did("https://example.com/post"), None);
        assert_eq!(extract_author_did(""), None);
    }
}
