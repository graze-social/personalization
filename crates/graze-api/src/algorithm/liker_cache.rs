//! In-memory LRU cache for post likers.
//!
//! This cache dramatically reduces Redis round-trips when scoring feeds by caching
//! the liker data (`pl:{post_id}`) locally. Since the same posts appear in many
//! users' feeds, cache hit rates are typically 70-90%.
//!
//! Cache entries have a TTL to ensure freshness while providing significant speedup.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use lru::LruCache;
use parking_lot::RwLock;

/// A cached liker list entry.
#[derive(Debug, Clone)]
pub struct CacheEntry {
    /// List of (user_hash, like_time) tuples.
    pub likers: Vec<(String, f64)>,
    /// Total count (may be more than likers.len() due to limit).
    pub liker_count: usize,
    /// When this entry was created.
    pub created_at: Instant,
    /// When this entry expires.
    pub expires_at: Instant,
}

impl CacheEntry {
    /// Check if this entry has expired.
    pub fn is_expired(&self) -> bool {
        Instant::now() > self.expires_at
    }
}

/// LRU cache for post liker data with TTL expiration.
///
/// This cache stores `pl:{post_id}` data (likers of a post) to avoid
/// repeated Redis queries for the same posts across different users' feeds.
pub struct LikerCache {
    cache: RwLock<LruCache<String, CacheEntry>>,
    ttl: Duration,
    /// Cache hit counter.
    pub hits: AtomicUsize,
    /// Cache miss counter.
    pub misses: AtomicUsize,
    /// Eviction counter.
    pub evictions: AtomicUsize,
}

impl LikerCache {
    /// Create a new liker cache.
    pub fn new(max_size: usize, ttl_seconds: u64) -> Self {
        Self {
            cache: RwLock::new(LruCache::new(
                std::num::NonZeroUsize::new(max_size).unwrap(),
            )),
            ttl: Duration::from_secs(ttl_seconds),
            hits: AtomicUsize::new(0),
            misses: AtomicUsize::new(0),
            evictions: AtomicUsize::new(0),
        }
    }

    /// Get a cached entry if it exists and hasn't expired.
    pub fn get(&self, post_id: &str) -> Option<CacheEntry> {
        let mut cache = self.cache.write();

        if let Some(entry) = cache.get(post_id) {
            if entry.is_expired() {
                cache.pop(post_id);
                self.misses.fetch_add(1, Ordering::Relaxed);
                return None;
            }
            self.hits.fetch_add(1, Ordering::Relaxed);
            return Some(entry.clone());
        }

        self.misses.fetch_add(1, Ordering::Relaxed);
        None
    }

    /// Store a liker list in the cache.
    pub fn set(&self, post_id: String, likers: Vec<(String, f64)>, liker_count: usize) {
        let now = Instant::now();
        let entry = CacheEntry {
            likers,
            liker_count,
            created_at: now,
            expires_at: now + self.ttl,
        };

        let mut cache = self.cache.write();
        if cache.len() >= cache.cap().get() {
            self.evictions.fetch_add(1, Ordering::Relaxed);
        }
        cache.put(post_id, entry);
    }

    /// Check if a post is in the cache (without moving it in LRU order).
    pub fn contains(&self, post_id: &str) -> bool {
        let cache = self.cache.read();
        if let Some(entry) = cache.peek(post_id) {
            !entry.is_expired()
        } else {
            false
        }
    }

    /// Get cache statistics.
    pub fn get_stats(&self) -> CacheStats {
        let cache = self.cache.read();
        let hits = self.hits.load(Ordering::Relaxed);
        let misses = self.misses.load(Ordering::Relaxed);
        let total = hits + misses;
        let hit_rate = if total > 0 {
            (hits as f64 / total as f64) * 100.0
        } else {
            0.0
        };

        CacheStats {
            size: cache.len(),
            max_size: cache.cap().get(),
            hits,
            misses,
            hit_rate_pct: hit_rate,
            evictions: self.evictions.load(Ordering::Relaxed),
            ttl_seconds: self.ttl.as_secs(),
        }
    }

    /// Clear the entire cache.
    pub fn clear(&self) {
        let mut cache = self.cache.write();
        cache.clear();
        self.hits.store(0, Ordering::Relaxed);
        self.misses.store(0, Ordering::Relaxed);
        self.evictions.store(0, Ordering::Relaxed);
    }

    /// Get the number of cached entries.
    pub fn len(&self) -> usize {
        self.cache.read().len()
    }

    /// Check if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.cache.read().is_empty()
    }

    /// Filter likers by time window (takes references, clones matches).
    pub fn filter_likers_by_time(
        likers: &[(String, f64)],
        min_time: f64,
        max_time: f64,
    ) -> Vec<(String, f64)> {
        likers
            .iter()
            .filter(|(_, t)| *t >= min_time && *t <= max_time)
            .cloned()
            .collect()
    }

    /// Filter likers by time window (takes ownership, avoids cloning).
    /// Use this when you have an owned Vec and don't need the original.
    pub fn filter_likers_by_time_owned(
        likers: Vec<(String, f64)>,
        min_time: f64,
        max_time: f64,
    ) -> Vec<(String, f64)> {
        likers
            .into_iter()
            .filter(|(_, t)| *t >= min_time && *t <= max_time)
            .collect()
    }
}

/// Cache statistics.
#[derive(Debug, Clone)]
pub struct CacheStats {
    pub size: usize,
    pub max_size: usize,
    pub hits: usize,
    pub misses: usize,
    pub hit_rate_pct: f64,
    pub evictions: usize,
    pub ttl_seconds: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_basic() {
        let cache = LikerCache::new(100, 300);

        // Test set and get
        cache.set("post1".to_string(), vec![("user1".to_string(), 1000.0)], 1);

        let entry = cache.get("post1").unwrap();
        assert_eq!(entry.likers.len(), 1);
        assert_eq!(entry.liker_count, 1);
    }

    #[test]
    fn test_cache_miss() {
        let cache = LikerCache::new(100, 300);

        assert!(cache.get("nonexistent").is_none());
        assert_eq!(cache.misses.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_cache_stats() {
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
    fn test_filter_likers() {
        let likers = vec![
            ("user1".to_string(), 100.0),
            ("user2".to_string(), 200.0),
            ("user3".to_string(), 300.0),
        ];

        let filtered = LikerCache::filter_likers_by_time(&likers, 150.0, 250.0);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].0, "user2");
    }
}
