//! Per-algorithm candidate-pool cache.
//!
//! `Scorer::score` fetched the entire `ap:{algo_id}` set with `SMEMBERS` on **every request**.
//! Measured on prod 2026-08-27: 56 live pools, median 4,554 members, max 39,959, and `SMEMBERS`
//! costs **71–107 ms** on the large ones. The same identical set was refetched for every user of
//! that feed, and `SCARD` on the five largest pools drifted by **exactly zero** over 20 s — the
//! data is effectively static between candidate-sync runs.
//!
//! That work showed up as `posts_checked` being pinned at 23,734 regardless of `max_total_sources`,
//! and as 40.1% of personalized responses breaching the 500 ms Thompson speed gate (which marks
//! them as failures for bandit learning even when they personalized perfectly well).
//!
//! Deliberately **shared between the live and durable-profile scorers**, unlike `LikerCache`. That
//! separation exists because liker lists depend on `max_likers_per_post`, so sharing them would
//! leak one arm's longer lists into the other. A candidate pool has no such dependency — it is the
//! same Redis set whatever the params — so sharing is correct here rather than a hazard.
//!
//! Bounded on total members rather than entry count: pools range from 1 to ~40,000 members, so an
//! entry cap would bound almost nothing. This service has a 2 GiB limit against ~400 MiB in use,
//! and the whole live set is ~485k members (~25–40 MB), so the default fits with wide margin.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;

struct PoolEntry {
    fetched_at: Instant,
    posts: Arc<Vec<String>>,
}

/// Hit/miss accounting, so a cache that silently stops working is visible.
#[derive(Debug, Clone, Copy, Default)]
pub struct PoolCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub entries: usize,
    pub members: usize,
}

/// TTL cache of `ap:{algo_id}` membership, keyed by algo.
pub struct PoolCache {
    entries: DashMap<i32, PoolEntry>,
    ttl: Duration,
    max_members: usize,
    members: AtomicU64,
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
}

impl PoolCache {
    /// `ttl_seconds == 0` disables the cache entirely.
    ///
    /// That is the rollback path: `POOL_CACHE_TTL_SECONDS=0` restores the previous
    /// fetch-every-request behaviour with an env edit and no rebuild.
    pub fn new(ttl_seconds: u64, max_members: usize) -> Self {
        Self {
            entries: DashMap::new(),
            ttl: Duration::from_secs(ttl_seconds),
            max_members,
            members: AtomicU64::new(0),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
        }
    }

    #[inline]
    pub fn enabled(&self) -> bool {
        !self.ttl.is_zero()
    }

    /// Fresh pool for `algo_id`, or `None` when disabled, absent, or expired.
    pub fn get(&self, algo_id: i32) -> Option<Arc<Vec<String>>> {
        if !self.enabled() {
            return None;
        }
        // Read the entry, then drop the guard before any mutation. Holding a DashMap reference
        // across a `remove` on the same shard deadlocks, which is exactly the class of bug the
        // network-cache port hit with its handle dedup.
        let expired_len = {
            match self.entries.get(&algo_id) {
                Some(e) if e.fetched_at.elapsed() < self.ttl => {
                    self.hits.fetch_add(1, Ordering::Relaxed);
                    return Some(e.posts.clone());
                }
                Some(e) => Some(e.posts.len()),
                None => None,
            }
        };
        self.misses.fetch_add(1, Ordering::Relaxed);
        if let Some(len) = expired_len {
            if self.entries.remove(&algo_id).is_some() {
                self.members.fetch_sub(len as u64, Ordering::Relaxed);
            }
        }
        None
    }

    /// Store a freshly fetched pool. No-op when disabled or when the pool alone exceeds the bound.
    pub fn put(&self, algo_id: i32, posts: Arc<Vec<String>>) {
        if !self.enabled() {
            return;
        }
        let len = posts.len();
        if len > self.max_members {
            // A single pool larger than the whole budget is never cached; caching it would evict
            // everything else on every request and turn the cache into pure overhead.
            return;
        }
        // Evict arbitrary entries until the new pool fits. Arbitrary is acceptable: entries are
        // interchangeable, cheap to refetch, and expire on their own within the TTL anyway.
        while self.members.load(Ordering::Relaxed) as usize + len > self.max_members {
            let victim = self.entries.iter().next().map(|e| *e.key());
            match victim {
                Some(k) => {
                    if let Some((_, e)) = self.entries.remove(&k) {
                        self.members
                            .fetch_sub(e.posts.len() as u64, Ordering::Relaxed);
                        self.evictions.fetch_add(1, Ordering::Relaxed);
                    }
                }
                None => break,
            }
        }
        if let Some(old) = self.entries.insert(
            algo_id,
            PoolEntry {
                fetched_at: Instant::now(),
                posts,
            },
        ) {
            self.members
                .fetch_sub(old.posts.len() as u64, Ordering::Relaxed);
        }
        self.members.fetch_add(len as u64, Ordering::Relaxed);
    }

    pub fn stats(&self) -> PoolCacheStats {
        PoolCacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            entries: self.entries.len(),
            members: self.members.load(Ordering::Relaxed) as usize,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pool(n: usize) -> Arc<Vec<String>> {
        Arc::new((0..n).map(|i| format!("post{i}")).collect())
    }

    #[test]
    fn hit_within_ttl_and_miss_after_it() {
        let c = PoolCache::new(1, 10_000);
        c.put(1, pool(5));
        assert_eq!(c.get(1).map(|p| p.len()), Some(5));
        std::thread::sleep(Duration::from_millis(1100));
        assert!(c.get(1).is_none(), "entry must expire after its TTL");
        // The expired entry is dropped rather than left to occupy the member budget.
        assert_eq!(c.stats().members, 0);
    }

    #[test]
    fn ttl_zero_disables_entirely() {
        let c = PoolCache::new(0, 10_000);
        assert!(!c.enabled());
        c.put(1, pool(5));
        assert!(c.get(1).is_none());
        // Nothing is retained, so the rollback path cannot leak memory either.
        assert_eq!(c.stats().entries, 0);
    }

    #[test]
    fn member_bound_is_enforced_by_eviction() {
        let c = PoolCache::new(60, 100);
        c.put(1, pool(60));
        c.put(2, pool(60)); // does not fit alongside 1, so 1 is evicted
        assert!(c.stats().members <= 100, "member bound must hold");
        assert_eq!(c.stats().entries, 1);
        assert!(c.stats().evictions >= 1);
    }

    #[test]
    fn a_pool_bigger_than_the_budget_is_never_cached() {
        let c = PoolCache::new(60, 100);
        c.put(1, pool(500));
        assert_eq!(
            c.stats().entries,
            0,
            "oversized pool must not evict everything each request"
        );
        assert_eq!(c.stats().members, 0);
    }

    #[test]
    fn replacing_an_entry_does_not_double_count_members() {
        let c = PoolCache::new(60, 10_000);
        c.put(7, pool(100));
        c.put(7, pool(30));
        assert_eq!(c.stats().members, 30);
        assert_eq!(c.stats().entries, 1);
    }

    #[test]
    fn stats_track_hits_and_misses() {
        let c = PoolCache::new(60, 10_000);
        assert!(c.get(42).is_none());
        c.put(42, pool(3));
        assert!(c.get(42).is_some());
        let s = c.stats();
        assert_eq!((s.hits, s.misses), (1, 1));
    }
}
