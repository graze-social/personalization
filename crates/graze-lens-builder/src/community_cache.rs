//! Per-community member lists, cached in process.
//!
//! The community facet's member query does not depend on the viewer at all,
//! only on which communities it names, and the tables behind it
//! (`community_members`, `account_stats`) change about once a week, when LPA
//! runs. Re-asking ClickHouse on every build cost the cluster more memory than
//! anything else this worker does: 16,475 runs in 3.7h of the 2026-10-25
//! refresh at 1.57 GiB each, 80% of them for the same three communities.
//!
//! So each community's top members are fetched once per TTL per pod and shared
//! by every build that needs them. Concurrent builds missing on the same
//! community wait on ONE query rather than each issuing their own: that is the
//! moment, right after a giant's entry expires, when a plain cache would send
//! the same 3-second query several times at once.

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::OnceCell;

use crate::priors::MemberRow;

type Members = Arc<Vec<MemberRow>>;

struct Slot {
    cell: Arc<OnceCell<Members>>,
    created: Instant,
}

pub struct CommunityCache {
    ttl: Duration,
    /// Most communities held at once.
    capacity: usize,
    /// Most member rows held at once, across all communities. The pod runs
    /// with 512 Mi, and a full community list is `community_cap` rows (1 MiB
    /// at 65,536), so the entry count alone does not bound memory.
    max_rows: usize,
    slots: Mutex<HashMap<u32, Slot>>,
}

impl CommunityCache {
    /// A `ttl` of zero turns caching off: every lookup fetches.
    pub fn new(ttl: Duration, capacity: usize, max_rows: usize) -> Self {
        Self {
            ttl,
            capacity: capacity.max(1),
            max_rows,
            slots: Mutex::new(HashMap::new()),
        }
    }

    /// The members of `community`, from cache if fresh, otherwise from `fetch`.
    ///
    /// A failed fetch caches nothing; the next caller (or a concurrent waiter)
    /// tries again.
    pub async fn get_or_fetch<F, Fut>(&self, community: u32, fetch: F) -> anyhow::Result<Members>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = anyhow::Result<Vec<MemberRow>>>,
    {
        if self.ttl.is_zero() {
            return Ok(Arc::new(fetch().await?));
        }
        let cell = self.cell_for(community);
        let mut fetched = false;
        let members = cell
            .get_or_try_init(|| async {
                fetched = true;
                fetch().await.map(Arc::new)
            })
            .await?
            .clone();
        if fetched {
            self.enforce_row_budget(community);
        }
        Ok(members)
    }

    /// Drop the oldest other communities until the rows held fit `max_rows`.
    /// The one just fetched always stays: it is at most `community_cap` rows.
    fn enforce_row_budget(&self, keep: u32) {
        let mut slots = self.slots.lock().unwrap_or_else(|p| p.into_inner());
        let rows = |s: &Slot| s.cell.get().map_or(0, |m| m.len());
        let mut held: usize = slots.values().map(rows).sum();
        if held <= self.max_rows {
            return;
        }
        let mut by_age: Vec<(Instant, u32, usize)> = slots
            .iter()
            .filter(|(c, _)| **c != keep)
            .map(|(c, s)| (s.created, *c, rows(s)))
            .collect();
        by_age.sort_unstable();
        for (_, c, n) in by_age {
            if held <= self.max_rows {
                break;
            }
            slots.remove(&c);
            held -= n;
        }
    }

    fn cell_for(&self, community: u32) -> Arc<OnceCell<Members>> {
        let now = Instant::now();
        let mut slots = self.slots.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(slot) = slots.get(&community) {
            if now.duration_since(slot.created) < self.ttl {
                return slot.cell.clone();
            }
        }
        if slots.len() >= self.capacity && !slots.contains_key(&community) {
            let ttl = self.ttl;
            slots.retain(|_, s| now.duration_since(s.created) < ttl);
            if slots.len() >= self.capacity {
                if let Some(oldest) = slots.iter().min_by_key(|(_, s)| s.created).map(|(c, _)| *c) {
                    slots.remove(&oldest);
                }
            }
        }
        let cell = Arc::new(OnceCell::new());
        slots.insert(
            community,
            Slot {
                cell: cell.clone(),
                created: now,
            },
        );
        cell
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.slots.lock().unwrap().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn rows(community: u32) -> Vec<MemberRow> {
        vec![MemberRow {
            account: 1,
            community,
            followers: 10,
        }]
    }

    #[tokio::test]
    async fn a_fresh_entry_is_not_refetched() {
        let cache = CommunityCache::new(Duration::from_secs(60), 16, 1_000);
        let calls = AtomicUsize::new(0);
        for _ in 0..3 {
            let got = cache
                .get_or_fetch(7, || async {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(rows(7))
                })
                .await
                .unwrap();
            assert_eq!(*got, rows(7));
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn concurrent_misses_share_one_fetch() {
        let cache = Arc::new(CommunityCache::new(Duration::from_secs(60), 16, 1_000));
        let calls = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::new();
        for _ in 0..8 {
            let (cache, calls) = (cache.clone(), calls.clone());
            tasks.push(tokio::spawn(async move {
                cache
                    .get_or_fetch(7, || async {
                        calls.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        Ok(rows(7))
                    })
                    .await
                    .unwrap()
            }));
        }
        for t in tasks {
            assert_eq!(*t.await.unwrap(), rows(7));
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_failed_fetch_is_retried_not_cached() {
        let cache = CommunityCache::new(Duration::from_secs(60), 16, 1_000);
        let err = cache
            .get_or_fetch(7, || async { anyhow::bail!("clickhouse said no") })
            .await;
        assert!(err.is_err());
        let got = cache.get_or_fetch(7, || async { Ok(rows(7)) }).await;
        assert_eq!(*got.unwrap(), rows(7));
    }

    #[tokio::test]
    async fn an_expired_entry_is_refetched() {
        let cache = CommunityCache::new(Duration::from_millis(20), 16, 1_000);
        let calls = AtomicUsize::new(0);
        let fetch = || async {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(rows(7))
        };
        cache.get_or_fetch(7, fetch).await.unwrap();
        tokio::time::sleep(Duration::from_millis(40)).await;
        cache.get_or_fetch(7, fetch).await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn zero_ttl_always_fetches() {
        let cache = CommunityCache::new(Duration::ZERO, 16, 1_000);
        let calls = AtomicUsize::new(0);
        let fetch = || async {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(rows(7))
        };
        cache.get_or_fetch(7, fetch).await.unwrap();
        cache.get_or_fetch(7, fetch).await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(cache.len(), 0);
    }

    #[tokio::test]
    async fn capacity_evicts_the_oldest() {
        let cache = CommunityCache::new(Duration::from_secs(60), 2, 1_000);
        for c in [1, 2, 3] {
            cache
                .get_or_fetch(c, || async move { Ok(rows(c)) })
                .await
                .unwrap();
        }
        assert_eq!(cache.len(), 2);
        let calls = AtomicUsize::new(0);
        cache
            .get_or_fetch(3, || async {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(rows(3))
            })
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 0, "the newest survived");
    }

    #[tokio::test]
    async fn the_row_budget_evicts_the_oldest_other_community() {
        let cache = CommunityCache::new(Duration::from_secs(60), 16, 5);
        let n_rows = |community: u32, n: u32| -> Vec<MemberRow> {
            (0..n)
                .map(|account| MemberRow {
                    account,
                    community,
                    followers: 1,
                })
                .collect()
        };
        cache
            .get_or_fetch(1, || async { Ok(n_rows(1, 3)) })
            .await
            .unwrap();
        cache
            .get_or_fetch(2, || async { Ok(n_rows(2, 2)) })
            .await
            .unwrap();
        assert_eq!(cache.len(), 2, "5 rows fit a 5-row budget");
        cache
            .get_or_fetch(3, || async { Ok(n_rows(3, 2)) })
            .await
            .unwrap();
        assert_eq!(cache.len(), 2, "community 1, the oldest, went");
        let calls = AtomicUsize::new(0);
        cache
            .get_or_fetch(2, || async {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(n_rows(2, 2))
            })
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
}
