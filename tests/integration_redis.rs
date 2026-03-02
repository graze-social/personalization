//! Integration tests for the Graze service with Redis.
//!
//! These tests require a Redis connection. Run with:
//!   REDIS_URL=redis://localhost:6379 cargo test --features integration-tests integration_redis
//!
//! IMPORTANT: Run integration tests with a single thread to avoid race conditions:
//!   REDIS_URL=redis://localhost:6379 cargo test --features integration-tests -- --test-threads=1
//!
//! Tests cover:
//! - Co-liker computation
//! - Author-level affinity
//! - Like graph operations
//! - Feed scoring

// Only compile these tests when integration-tests feature is enabled
#![cfg(feature = "integration-tests")]

// Include the helper modules
#[path = "helpers/mod.rs"]
mod helpers;

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;

use graze::algorithm::{
    aggregate_coliker_weights, aggregate_coliker_weights_parallel, score_posts, score_posts_topk,
    AuthorColikerWorker, ColikerWorker, LinkLonkParams,
};
use graze::redis::{hash_did, Keys};

use helpers::{LikeGraphBuilder, TestAuthor, TestUser, DAY, HOUR};

/// Macro to skip tests if Redis is not available.
/// Returns a TestContext or skips the test.
macro_rules! skip_without_redis {
    () => {{
        match common::TestContext::new().await {
            Some(ctx) => ctx,
            None => {
                eprintln!("Skipping test: Redis not available");
                return;
            }
        }
    }};
}

/// Get current time as Unix timestamp.
fn now() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs_f64()
}

// ═══════════════════════════════════════════════════════════════════════════════
// Co-Liker Tests (ported from test_coliker_worker.py)
// ═══════════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_compute_colikes_basic() {
    let ctx = skip_without_redis!();
    let now = now();

    // Create test users with unique names to avoid conflicts with parallel tests
    let alice = TestUser::new("colikes_basic_alice");
    let bob = TestUser::new("colikes_basic_bob");
    let carol = TestUser::new("colikes_basic_carol");

    // Build like graph:
    // - Alice and Bob both liked post1 (Bob first)
    // - Alice and Carol both liked post2 (Carol first)
    // Note: All likes must be within the time window for co-liker detection
    let builder = LikeGraphBuilder::new(ctx.redis.clone());
    builder
        .add_like(&bob, "colikes_basic_post1", now - 2.0 * DAY)
        .add_like(&alice, "colikes_basic_post1", now - DAY)
        .add_like(&carol, "colikes_basic_post2", now - 2.5 * DAY) // Changed from 3 days to 2.5 days
        .add_like(&alice, "colikes_basic_post2", now - DAY)
        .build()
        .await
        .unwrap();

    // Create worker and compute co-likers for Alice
    // Time window: 4 days (345600 seconds) to include all test likes
    // Half-life: 1 day (86400 seconds)
    let worker = ColikerWorker::new(ctx.redis.clone(), ctx.config.clone());
    let colikes = worker
        .get_or_compute_colikes(&alice.did_hash, 500, 100, 1000, 345600, 86400, true)
        .await
        .unwrap();

    // Both Bob and Carol should be co-likers
    assert_eq!(
        colikes.len(),
        2,
        "Expected 2 co-likers, got {}",
        colikes.len()
    );
    assert!(
        colikes.contains_key(&bob.did_hash),
        "Bob should be a co-liker"
    );
    assert!(
        colikes.contains_key(&carol.did_hash),
        "Carol should be a co-liker"
    );

    // Bob should have higher weight (more recent co-like)
    assert!(
        colikes[&bob.did_hash] > colikes[&carol.did_hash],
        "Bob's weight ({}) should be higher than Carol's ({})",
        colikes[&bob.did_hash],
        colikes[&carol.did_hash]
    );
}

#[tokio::test]
async fn test_compute_colikes_excludes_self() {
    let ctx = skip_without_redis!();
    let now = now();

    let alice = TestUser::new("alice_self");

    // Alice liked a post
    let builder = LikeGraphBuilder::new(ctx.redis.clone());
    builder
        .add_like(&alice, "selfpost", now - DAY)
        .build()
        .await
        .unwrap();

    let worker = ColikerWorker::new(ctx.redis.clone(), ctx.config.clone());
    let colikes = worker
        .get_or_compute_colikes(&alice.did_hash, 500, 100, 1000, 172800, 86400, true)
        .await
        .unwrap();

    // Alice should NOT be in her own co-likers
    assert!(!colikes.contains_key(&alice.did_hash));
}

#[tokio::test]
async fn test_compute_colikes_no_likes() {
    let ctx = skip_without_redis!();

    let lonely = TestUser::new("lonely");

    let worker = ColikerWorker::new(ctx.redis.clone(), ctx.config.clone());
    let colikes = worker
        .get_or_compute_colikes(&lonely.did_hash, 500, 100, 1000, 172800, 86400, true)
        .await
        .unwrap();

    assert!(colikes.is_empty());
}

#[tokio::test]
async fn test_compute_colikes_respects_time_window() {
    let ctx = skip_without_redis!();
    let now = now();

    let alice = TestUser::new("time_window_alice");
    let recent = TestUser::new("time_window_recent");
    let old = TestUser::new("time_window_old");

    // Build graph: recent user liked 1 day ago, old user liked 10 days ago
    let builder = LikeGraphBuilder::new(ctx.redis.clone());
    builder
        .add_like(&recent, "time_window_post", now - DAY)
        .add_like(&old, "time_window_post", now - 10.0 * DAY)
        .add_like(&alice, "time_window_post", now - HOUR)
        .build()
        .await
        .unwrap();

    // Use 2-day time window (48 hours)
    let worker = ColikerWorker::new(ctx.redis.clone(), ctx.config.clone());
    let colikes = worker
        .get_or_compute_colikes(
            &alice.did_hash,
            500,
            100,
            1000,
            48 * 3600, // 2 days
            24 * 3600, // 1 day half-life
            true,
        )
        .await
        .unwrap();

    // Only recent should be included (within time window)
    assert!(
        colikes.contains_key(&recent.did_hash),
        "Recent user should be a co-liker within the 2-day window"
    );
    // Old user is outside time window, should NOT be included
    assert!(
        !colikes.contains_key(&old.did_hash),
        "Old user (10 days ago) should NOT be a co-liker outside 2-day window"
    );
}

#[tokio::test]
async fn test_coliker_cache_stores_and_retrieves() {
    let ctx = skip_without_redis!();
    let now = now();

    let alice = TestUser::new("cache_test_alice");
    let bob = TestUser::new("cache_test_bob");

    let builder = LikeGraphBuilder::new(ctx.redis.clone());
    builder
        .add_like(&bob, "cache_test_post", now - DAY)
        .add_like(&alice, "cache_test_post", now - HOUR)
        .build()
        .await
        .unwrap();

    let worker = ColikerWorker::new(ctx.redis.clone(), ctx.config.clone());

    // First call - should compute
    let colikes1 = worker
        .get_or_compute_colikes(&alice.did_hash, 500, 100, 1000, 172800, 86400, true)
        .await
        .unwrap();

    assert!(colikes1.contains_key(&bob.did_hash));

    // Second call - should use cache
    let colikes2 = worker
        .get_or_compute_colikes(&alice.did_hash, 500, 100, 1000, 172800, 86400, false)
        .await
        .unwrap();

    assert_eq!(colikes1.len(), colikes2.len());
}

#[tokio::test]
async fn test_coliker_invalidation() {
    let ctx = skip_without_redis!();
    let now = now();

    let alice = TestUser::new("invalidation_alice");
    let bob = TestUser::new("invalidation_bob");

    let builder = LikeGraphBuilder::new(ctx.redis.clone());
    builder
        .add_like(&bob, "invalidation_post", now - DAY)
        .add_like(&alice, "invalidation_post", now - HOUR)
        .build()
        .await
        .unwrap();

    let worker = ColikerWorker::new(ctx.redis.clone(), ctx.config.clone());

    // Compute and cache
    worker
        .get_or_compute_colikes(&alice.did_hash, 500, 100, 1000, 172800, 86400, true)
        .await
        .unwrap();

    // Verify cache exists
    let (exists, ttl) = worker.check_cache_status(&alice.did_hash).await.unwrap();
    assert!(exists);
    assert!(ttl > 0);

    // Invalidate
    let deleted = worker.invalidate_colikes(&alice.did_hash).await.unwrap();
    assert!(deleted);

    // Verify cache is gone
    let (exists, _) = worker.check_cache_status(&alice.did_hash).await.unwrap();
    assert!(!exists);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Author Affinity Tests (ported from test_author_coliker_worker.py)
// ═══════════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_author_colikes_basic() {
    let ctx = skip_without_redis!();
    let now = now();

    let alice = TestUser::new("alice_author");
    let bob = TestUser::new("bob_author");
    let carol = TestUser::new("carol_author");
    let author1 = TestAuthor::new("author1");

    // Build graph: Alice liked author1 (2 times), Bob and Carol also liked author1
    let builder = LikeGraphBuilder::new(ctx.redis.clone());
    builder
        .add_author_like(&alice, &author1, 2.0, now - DAY)
        .add_author_like(&bob, &author1, 1.0, now - 2.0 * DAY)
        .add_author_like(&carol, &author1, 1.0, now - 3.0 * DAY)
        .build()
        .await
        .unwrap();

    let worker = AuthorColikerWorker::new(ctx.redis.clone(), ctx.config.clone());
    let colikes = worker
        .get_or_compute_author_colikes(&alice.did_hash, true)
        .await
        .unwrap();

    // Both Bob and Carol should be co-likers
    assert_eq!(colikes.len(), 2);
    assert!(colikes.contains_key(&bob.did_hash));
    assert!(colikes.contains_key(&carol.did_hash));
}

#[tokio::test]
async fn test_author_colikes_excludes_self() {
    let ctx = skip_without_redis!();
    let now = now();

    let alice = TestUser::new("alice_selfauth");
    let author1 = TestAuthor::new("selfauth1");

    // Alice liked the author
    let builder = LikeGraphBuilder::new(ctx.redis.clone());
    builder
        .add_author_like(&alice, &author1, 3.0, now - DAY)
        .build()
        .await
        .unwrap();

    let worker = AuthorColikerWorker::new(ctx.redis.clone(), ctx.config.clone());
    let colikes = worker
        .get_or_compute_author_colikes(&alice.did_hash, true)
        .await
        .unwrap();

    // Alice should NOT appear in her own co-likers
    assert!(!colikes.contains_key(&alice.did_hash));
}

#[tokio::test]
async fn test_author_colikes_multiple_authors() {
    let ctx = skip_without_redis!();
    let now = now();

    let alice = TestUser::new("alice_multi");
    let bob = TestUser::new("bob_multi");
    let author1 = TestAuthor::new("multi1");
    let author2 = TestAuthor::new("multi2");

    // Both Alice and Bob liked both authors
    let builder = LikeGraphBuilder::new(ctx.redis.clone());
    builder
        .add_author_like(&alice, &author1, 2.0, now - DAY)
        .add_author_like(&alice, &author2, 3.0, now - DAY)
        .add_author_like(&bob, &author1, 1.0, now - 2.0 * DAY)
        .add_author_like(&bob, &author2, 1.0, now - 2.0 * DAY)
        .build()
        .await
        .unwrap();

    let worker = AuthorColikerWorker::new(ctx.redis.clone(), ctx.config.clone());
    let colikes = worker
        .get_or_compute_author_colikes(&alice.did_hash, true)
        .await
        .unwrap();

    // Bob should have accumulated weight from both authors
    assert!(colikes.contains_key(&bob.did_hash));
    assert!(colikes[&bob.did_hash] > 0.0);
}

#[tokio::test]
async fn test_author_affinity_disabled() {
    let ctx = skip_without_redis!();

    let mut config = (*ctx.config).clone();
    config.author_affinity_enabled = false;
    let config = Arc::new(config);

    let alice = TestUser::new("alice_disabled");

    let worker = AuthorColikerWorker::new(ctx.redis.clone(), config);
    let colikes = worker
        .get_or_compute_author_colikes(&alice.did_hash, true)
        .await
        .unwrap();

    assert!(colikes.is_empty());
}

// ═══════════════════════════════════════════════════════════════════════════════
// Scoring Core Tests (ported from rust_scorer tests)
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_scoring_core_basic() {
    use rustc_hash::FxHashMap;

    let posts_data = vec![
        (
            "post1".to_string(),
            vec![
                ("user_a".to_string(), 1000.0),
                ("user_b".to_string(), 900.0),
            ],
        ),
        (
            "post2".to_string(),
            vec![
                ("user_c".to_string(), 950.0), // Not a co-liker
            ],
        ),
    ];

    let mut source_weights: FxHashMap<String, f64> = FxHashMap::default();
    source_weights.insert("user_a".to_string(), 1.0);
    source_weights.insert("user_b".to_string(), 0.5);

    let now = 1000.0;
    let half_life = 3600.0 * 72.0; // 72 hours

    let results = score_posts(posts_data, &source_weights, now, half_life);

    // post1 should have a score (has co-likers)
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].post_id, "post1");
    assert!(results[0].score > 0.0);
    assert_eq!(results[0].matching_count, 2);
}

#[test]
fn test_scoring_recency_decay() {
    use rustc_hash::FxHashMap;

    let posts_data = vec![
        (
            "recent".to_string(),
            vec![("user_a".to_string(), 990.0)], // 10s ago
        ),
        (
            "old".to_string(),
            vec![("user_a".to_string(), 0.0)], // 1000s ago
        ),
    ];

    let mut source_weights: FxHashMap<String, f64> = FxHashMap::default();
    source_weights.insert("user_a".to_string(), 1.0);

    let results = score_posts(posts_data, &source_weights, 1000.0, 100.0);

    // Both should score, but recent should score higher
    assert_eq!(results.len(), 2);

    let recent_score = results
        .iter()
        .find(|r| r.post_id == "recent")
        .unwrap()
        .score;
    let old_score = results.iter().find(|r| r.post_id == "old").unwrap().score;

    assert!(recent_score > old_score);
}

#[test]
fn test_scoring_topk() {
    use rustc_hash::FxHashMap;

    // Create 100 posts with varying scores
    let posts_data: Vec<(String, Vec<(String, f64)>)> = (0..100)
        .map(|i| {
            (
                format!("post_{}", i),
                vec![("user_a".to_string(), 900.0 + i as f64)],
            )
        })
        .collect();

    let mut source_weights: FxHashMap<String, f64> = FxHashMap::default();
    source_weights.insert("user_a".to_string(), 1.0);

    // Request top 10
    let results = score_posts_topk(posts_data, &source_weights, 1000.0, 100.0, 10);

    // Should only return 10 results
    assert_eq!(results.len(), 10);

    // Results should be sorted by score descending
    for i in 1..results.len() {
        assert!(
            results[i - 1].score >= results[i].score,
            "Results not sorted"
        );
    }

    // Top result should be post_99 (most recent like)
    assert_eq!(results[0].post_id, "post_99");
}

#[test]
fn test_aggregate_coliker_weights() {
    // Simulate: user liked 2 posts, each with 2 likers
    let all_likers_data = vec![
        vec![
            ("source_a".to_string(), 900.0),
            ("source_b".to_string(), 950.0),
        ],
        vec![
            ("source_a".to_string(), 800.0), // Same source!
            ("source_c".to_string(), 990.0),
        ],
    ];

    let user_hash = "current_user";
    let now = 1000.0;
    let half_life = 3600.0;
    let max_sources = 10;

    let results =
        aggregate_coliker_weights(all_likers_data, user_hash, now, half_life, max_sources);

    // Should have 3 unique sources
    assert_eq!(results.len(), 3);

    // source_a should have highest weight (appears twice)
    assert_eq!(results[0].0, "source_a");

    // All weights should be positive
    for (_, weight) in &results {
        assert!(*weight > 0.0);
    }
}

#[test]
fn test_aggregate_coliker_weights_excludes_self() {
    let all_likers_data = vec![vec![
        ("other_user".to_string(), 900.0),
        ("current_user".to_string(), 950.0), // Self, should be excluded
    ]];

    let user_hash = "current_user";
    let now = 1000.0;
    let half_life = 3600.0;
    let max_sources = 10;

    let results =
        aggregate_coliker_weights(all_likers_data, user_hash, now, half_life, max_sources);

    // Should only have 1 source (self excluded)
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, "other_user");
}

#[test]
fn test_aggregate_coliker_weights_parallel() {
    // Create a large dataset to trigger parallel processing
    let all_likers_data: Vec<Vec<(String, f64)>> = (0..2000)
        .map(|i| {
            vec![
                (format!("source_{}", i % 100), 900.0 + (i % 100) as f64),
                (format!("source_{}", (i + 1) % 100), 950.0),
            ]
        })
        .collect();

    let user_hash = "test_user";
    let now = 1000.0;
    let half_life = 3600.0;
    let max_sources = 50;

    let results =
        aggregate_coliker_weights_parallel(all_likers_data, user_hash, now, half_life, max_sources);

    // Should return max_sources
    assert_eq!(results.len(), 50);

    // All weights should be positive
    for (_, weight) in &results {
        assert!(*weight > 0.0);
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Parameter Tests (ported from test_algorithm.py)
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_params_default() {
    let params = LinkLonkParams::default();
    assert_eq!(params.max_user_likes, 500);
    assert_eq!(params.time_window_hours, 48.0);
    assert_eq!(params.result_ttl_seconds, 300);
}

#[test]
fn test_params_presets() {
    use graze::algorithm::get_preset;

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
fn test_params_unknown_preset() {
    use graze::algorithm::get_preset;

    let params = get_preset("unknown");
    assert_eq!(params.max_user_likes, 500); // Should return default
}

#[test]
fn test_params_merge() {
    use graze::algorithm::merge_params;
    use graze::models::PersonalizationParams;

    let base = LinkLonkParams::default();
    let overrides = PersonalizationParams {
        max_user_likes: Some(100),
        specificity_power: Some(2.0),
        ..Default::default()
    };

    let merged = merge_params(base, Some(&overrides));
    assert_eq!(merged.max_user_likes, 100);
    assert_eq!(merged.specificity_power, 2.0);
    // Non-overridden values should be from base
    assert_eq!(merged.time_window_hours, 48.0);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Hash Tests (ported from test_algorithm.py)
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_hash_did_length() {
    let did = "did:plc:testuser123";
    let hash = hash_did(did);
    assert_eq!(hash.len(), 16);
}

#[test]
fn test_hash_did_consistency() {
    let did = "did:plc:testuser123";
    let hash1 = hash_did(did);
    let hash2 = hash_did(did);
    assert_eq!(hash1, hash2);
}

#[test]
fn test_hash_did_uniqueness() {
    let hash1 = hash_did("did:plc:user1");
    let hash2 = hash_did("did:plc:user2");
    assert_ne!(hash1, hash2);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Keys Tests (ported from test_seen_posts.py)
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_user_seen_key_format() {
    let user_hash = "abc123def456";
    let key = Keys::user_seen(user_hash);
    assert_eq!(key, "seen:abc123def456");
}

#[test]
fn test_user_likes_key_format() {
    let user_hash = "abc123";
    let key = Keys::user_likes(user_hash);
    assert_eq!(key, "ul:abc123");
}

#[test]
fn test_cached_result_key_format() {
    let key = Keys::cached_result(42, "def456");
    assert_eq!(key, "ll:42:def456");
}

#[test]
fn test_colikes_key_format() {
    let key = Keys::colikes("user123");
    assert_eq!(key, "colikes:user123");
}

#[test]
fn test_author_colikes_key_format() {
    let key = Keys::author_colikes("user123");
    assert_eq!(key, "acolikes:user123");
}

// ═══════════════════════════════════════════════════════════════════════════════
// Liker Cache Tests (ported from internal cache tests)
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_liker_cache_basic() {
    use graze::algorithm::LikerCache;

    let cache = LikerCache::new(100, 300);

    // Test set and get
    cache.set("post1".to_string(), vec![("user1".to_string(), 1000.0)], 1);

    let entry = cache.get("post1").unwrap();
    assert_eq!(entry.likers.len(), 1);
    assert_eq!(entry.liker_count, 1);
}

#[test]
fn test_liker_cache_miss() {
    use graze::algorithm::LikerCache;
    use std::sync::atomic::Ordering;

    let cache = LikerCache::new(100, 300);

    assert!(cache.get("nonexistent").is_none());
    assert_eq!(cache.misses.load(Ordering::Relaxed), 1);
}

#[test]
fn test_liker_cache_stats() {
    use graze::algorithm::LikerCache;

    let cache = LikerCache::new(100, 300);

    cache.set("post1".to_string(), vec![], 0);
    let _ = cache.get("post1"); // hit
    let _ = cache.get("post2"); // miss

    let stats = cache.get_stats();
    assert_eq!(stats.hits, 1);
    assert_eq!(stats.misses, 1);
    assert_eq!(stats.hit_rate_pct, 50.0);
}

#[test]
fn test_liker_cache_filter_by_time() {
    use graze::algorithm::LikerCache;

    let likers = vec![
        ("user1".to_string(), 100.0),
        ("user2".to_string(), 200.0),
        ("user3".to_string(), 300.0),
    ];

    let filtered = LikerCache::filter_likers_by_time(&likers, 150.0, 250.0);
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].0, "user2");
}

// ═══════════════════════════════════════════════════════════════════════════════
// Extract Author DID Tests (ported from test_like_streamer.py)
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_extract_author_did_valid_uri() {
    use helpers::extract_author_did;

    let uri = "at://did:plc:author123/app.bsky.feed.post/rkey456";
    let result = extract_author_did(uri);
    assert_eq!(result, Some("did:plc:author123".to_string()));
}

#[test]
fn test_extract_author_did_web_did() {
    use helpers::extract_author_did;

    let uri = "at://did:web:example.com/app.bsky.feed.post/xyz";
    let result = extract_author_did(uri);
    assert_eq!(result, Some("did:web:example.com".to_string()));
}

#[test]
fn test_extract_author_did_invalid_prefix() {
    use helpers::extract_author_did;

    let uri = "https://did:plc:author123/app.bsky.feed.post/rkey456";
    let result = extract_author_did(uri);
    assert_eq!(result, None);
}

#[test]
fn test_extract_author_did_empty_string() {
    use helpers::extract_author_did;

    let result = extract_author_did("");
    assert_eq!(result, None);
}
