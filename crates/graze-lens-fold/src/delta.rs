//! Keeping live lens sets fresh, so they never have to expire.
//!
//! Without this, a lens set is only as correct as its TTL: it is built once and
//! then rots, so the TTL has to be short (minutes), which means an active reader
//! keeps falling off the end of it and getting an unfiltered feed while a
//! rebuild runs. Applying the follow stream to sets that already exist lets the
//! TTL move out to days, and an actively-read set then never expires at all.
//!
//! # Creates apply; deletes rebuild
//!
//! A follow *create* carries its subject, so it applies directly: one `SADD`.
//!
//! A follow *delete* does not carry the subject — the wire gives only
//! `(repo, rkey)` — so there is nothing to `SREM`. Rather than keep a second
//! rkey→followee map in Redis purely to resolve unfollows (doubling the memory
//! for every active viewer, to serve the rarer event), an unfollow simply
//! enqueues a rebuild of that viewer's set. A rebuild is one ClickHouse query
//! and is correct by construction, and people unfollow far less often than they
//! follow. Correctness where it is cheap; freshness where it is hot.
//!
//! # Never create a set that did not exist
//!
//! `SADD` on a missing key *creates* it — which here would mean a lens set
//! containing exactly one account. That is worse than no lens at all: the reader
//! would silently see almost nothing rather than fail open. Every write is
//! therefore guarded by an existence check inside a Lua script, so the check and
//! the write cannot race.

use deadpool_redis::redis::AsyncCommands;
use deadpool_redis::Pool;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, warn};

use crate::event::FollowEdge;
use crate::metrics::Metrics;

/// Viewers with a published lens set. The builder adds to this on publish.
pub const ACTIVE_KEY: &str = "lens:active";
/// The build queue feeder-rs and this module both write to.
const BUILD_QUEUE_KEY: &str = "queue:lens";
const BUILD_QUEUE_MAXLEN: usize = 100_000;

/// Add one member to an existing set and extend its life, or do nothing.
///
/// The `EXISTS` guard is the whole point: a plain `SADD` would resurrect an
/// expired viewer's set with a single member. Returns 1 when applied.
const APPLY_FOLLOW: &str = r#"
if redis.call('EXISTS', KEYS[1]) == 1 then
  redis.call('SADD', KEYS[1], ARGV[1])
  redis.call('EXPIRE', KEYS[1], ARGV[2])
  if redis.call('EXISTS', KEYS[2]) == 1 then
    redis.call('EXPIRE', KEYS[2], ARGV[2])
  end
  return 1
end
return 0
"#;

pub struct DeltaApplier {
    redis: Pool,
    metrics: Metrics,
    /// Viewers whose sets are live. Refreshed on a timer; the follow stream is
    /// far too hot to ask Redis about every event.
    active: Arc<RwLock<HashSet<String>>>,
    set_ttl_seconds: u64,
    facet: String,
}

impl DeltaApplier {
    pub fn new(redis: Pool, metrics: Metrics, set_ttl_seconds: u64) -> Self {
        Self {
            redis,
            metrics,
            active: Arc::new(RwLock::new(HashSet::new())),
            set_ttl_seconds,
            facet: crate::event::FACET_FOLLOWS.to_string(),
        }
    }

    fn set_key(&self, viewer: &str) -> String {
        format!("lens:v1:{}:{}", self.facet, viewer)
    }

    fn meta_key(viewer: &str) -> String {
        format!("lensmeta:{viewer}")
    }

    /// Is this viewer worth spending a Redis round trip on?
    pub async fn is_active(&self, viewer: &str) -> bool {
        self.active.read().await.contains(viewer)
    }

    /// Reload the active-viewer set from Redis.
    pub async fn refresh_active(&self) -> anyhow::Result<usize> {
        let mut conn = self.redis.get().await?;
        let members: HashSet<String> = conn.smembers(ACTIVE_KEY).await?;
        let count = members.len();
        *self.active.write().await = members;
        self.metrics.active_viewers.set(count as i64);
        Ok(count)
    }

    /// Apply one follow event to a live set.
    ///
    /// Only called for viewers already known active, so the common case (an
    /// event for one of millions of uninteresting accounts) costs a hash lookup
    /// and nothing else.
    pub async fn apply(&self, edge: &FollowEdge) {
        match edge.op {
            "create" => self.apply_follow(edge).await,
            "delete" => self.request_rebuild(&edge.follower).await,
            _ => {}
        }
    }

    async fn apply_follow(&self, edge: &FollowEdge) {
        let result = async {
            let mut conn = self.redis.get().await?;
            let applied: i64 = deadpool_redis::redis::Script::new(APPLY_FOLLOW)
                .key(self.set_key(&edge.follower))
                .key(Self::meta_key(&edge.follower))
                .arg(&edge.followee)
                .arg(self.set_ttl_seconds)
                .invoke_async(&mut conn)
                .await?;
            Ok::<_, anyhow::Error>(applied)
        }
        .await;

        match result {
            Ok(1) => {
                self.metrics.deltas_applied.inc();
                debug!(viewer = %edge.follower, "applied follow to live lens");
            }
            Ok(_) => {
                // The set expired between the active-list refresh and now.
                // Drop the viewer so we stop trying; their next request rebuilds.
                self.forget(&edge.follower).await;
            }
            Err(e) => {
                warn!(error = %e, viewer = %edge.follower, "delta apply failed");
                self.metrics.delta_failures.inc();
            }
        }
    }

    /// An unfollow cannot be applied directly, so ask for a rebuild.
    async fn request_rebuild(&self, viewer: &str) {
        let payload = serde_json::json!({ "viewer_did": viewer, "facet": self.facet }).to_string();
        let result = async {
            let mut conn = self.redis.get().await?;
            deadpool_redis::redis::cmd("XADD")
                .arg(BUILD_QUEUE_KEY)
                .arg("MAXLEN")
                .arg("~")
                .arg(BUILD_QUEUE_MAXLEN)
                .arg("*")
                .arg("data")
                .arg(&payload)
                .query_async::<()>(&mut conn)
                .await?;
            Ok::<_, anyhow::Error>(())
        }
        .await;

        match result {
            Ok(()) => {
                self.metrics.deltas_rebuild_requested.inc();
                debug!(viewer, "unfollow: requested lens rebuild");
            }
            Err(e) => {
                warn!(error = %e, viewer, "could not request rebuild");
                self.metrics.delta_failures.inc();
            }
        }
    }

    /// Stop tracking a viewer whose set is gone, in memory and in Redis.
    async fn forget(&self, viewer: &str) {
        self.active.write().await.remove(viewer);
        let mut conn = match self.redis.get().await {
            Ok(c) => c,
            Err(e) => {
                warn!(error = %e, "could not prune active viewer");
                return;
            }
        };
        let _: Result<i64, _> = conn.srem(ACTIVE_KEY, viewer).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `SADD` on a missing key creates it. A lens set with one member would
    /// silently hide almost everything from that reader — strictly worse than
    /// no lens, which fails open. The guard must come first.
    #[test]
    fn apply_script_refuses_to_create_a_missing_set() {
        let exists_at = APPLY_FOLLOW.find("EXISTS").expect("must guard on EXISTS");
        let sadd_at = APPLY_FOLLOW.find("SADD").expect("must SADD");
        assert!(
            exists_at < sadd_at,
            "the existence guard must precede the write"
        );
    }

    /// Applying a follow must also push the expiry out, or the set still dies on
    /// the original TTL and the whole exercise is pointless.
    #[test]
    fn apply_script_extends_the_ttl() {
        assert!(APPLY_FOLLOW.contains("EXPIRE"));
    }

    /// The metadata key has to live at least as long as the set: feeder-rs
    /// requires state == "ready", so a set outliving its meta reads as unbuilt
    /// and would be rebuilt on every request.
    #[test]
    fn apply_script_extends_the_metadata_key_too() {
        assert_eq!(
            APPLY_FOLLOW.matches("EXPIRE").count(),
            2,
            "both the set and lensmeta must be extended"
        );
    }
}
