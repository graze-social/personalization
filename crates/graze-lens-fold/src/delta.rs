//! Keeping live lens sets fresh, so they never have to expire.
//!
//! Without this, a lens set is only as correct as its TTL: it is built once and
//! then rots, so the TTL has to be short (minutes), which means an active reader
//! keeps falling off the end of it and getting an unfiltered feed while a
//! rebuild runs. Applying the follow stream to sets that already exist lets the
//! TTL move out to days, and an actively-read set then never expires at all.
//!
//! # The v2 blobs are what readers actually get
//!
//! Everything above is about the v1 *sets*, and the serve path stopped reading
//! those: it loads `lens:v2:{facet}:{did}` blobs and only falls back to a v1 set
//! when a single-facet plan has no blob at all. So applying a follow to the v1
//! set kept the set correct and changed nothing a reader saw — a follow made
//! today did not reach their lens until the nightly rebuild, up to ~24h later.
//!
//! A blob cannot be updated the way a set can. It is a sorted `(didint, score)`
//! array with a bloom trailer, so inserting one author means rewriting the whole
//! value — 640 KB for `community` — per event, racing every other writer.
//!
//! So the blobs are not mutated; they are **rebuilt**, and this module's job is
//! to notice that a rebuild is owed and to ask for exactly one. A graph change
//! marks the viewer dirty (`SADD lens:dirty`, O(1) and idempotent) and a sweeper
//! drains that set on a timer, enqueuing a rebuild per facet the viewer already
//! has.
//!
//! **Trailing edge, deliberately.** A per-viewer cooldown would lose updates: a
//! follow at t=0 enqueues, a follow at t=5s is suppressed as "recently done", and
//! the t=0 rebuild has already run without it. Marking and sweeping coalesces by
//! construction and cannot drop an update — any change inside the window is
//! picked up by the next sweep. It also bounds the cost: at most one rebuild per
//! viewer per sweep however many times they follow, which matters because a
//! `community` rebuild is a 500k-row ClickHouse query.
//!
//! What this does NOT fix: *other people's* follows changing this viewer's second
//! degree. The seeds are live, but the traversal graph `follow_graph_int` is
//! rebuilt nightly, so an edge someone else made today enters the second degree
//! tomorrow.
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
use tracing::{debug, info, warn};

use crate::event::FollowEdge;
use crate::metrics::Metrics;

/// Viewers with a published lens set. The builder adds to this on publish.
pub const ACTIVE_KEY: &str = "lens:active";
/// Viewers whose own graph changed since the last sweep.
pub const DIRTY_KEY: &str = "lens:dirty";

/// Per-viewer facets a rebuild may cover.
///
/// Must stay in step with `FACETS` in `graze-lens-builder/src/bin/refresh.rs` —
/// the sweeper is the event-driven twin of that nightly job, and a facet missing
/// here would simply never be refreshed on change. `domain` is absent because it
/// is keyed by feed, not viewer, so a reader's follows cannot affect it.
pub const VIEWER_FACETS: &[&str] = &[
    "follows",
    "follows2",
    "niche",
    "popular",
    "velocity",
    "community",
];

/// Dirty viewers drained in one sweep. A ceiling rather than a target: it bounds
/// how much ClickHouse work a single tick can schedule if something upstream
/// marks far more viewers than usual.
const SWEEP_BATCH: usize = 256;
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
            _ => return,
        }
        // Both directions change what this reader's lens should say, and neither
        // is expressible as an edit to a blob. Mark, and let the sweeper coalesce.
        self.mark_dirty(&edge.follower).await;
    }

    /// Note that this viewer's own graph moved, so their blobs are stale.
    async fn mark_dirty(&self, viewer: &str) {
        let result = async {
            let mut conn = self.redis.get().await?;
            let _: i64 = conn.sadd(DIRTY_KEY, viewer).await?;
            Ok::<_, anyhow::Error>(())
        }
        .await;
        match result {
            Ok(()) => {
                self.metrics.dirty_marked.inc();
            }
            Err(e) => {
                warn!(error = %e, viewer, "could not mark viewer dirty");
                self.metrics.delta_failures.inc();
            }
        }
    }

    /// Drain the dirty set and enqueue a rebuild per facet each viewer has.
    ///
    /// `SPOP` with a count is atomic: whatever it returns is removed, so a viewer
    /// cannot be swept twice, and a viewer who changes again mid-sweep is simply
    /// re-marked and picked up next tick. On failure the viewers are already
    /// popped, so they are re-marked rather than silently dropped — a lost mark
    /// means a lens stale until the nightly job, which is the outcome this whole
    /// module exists to avoid.
    pub async fn sweep(&self) {
        self.metrics.sweeps.inc();
        let mut conn = match self.redis.get().await {
            Ok(c) => c,
            Err(e) => {
                warn!(error = %e, "sweep could not reach redis");
                self.metrics.sweep_failures.inc();
                return;
            }
        };

        let drained = deadpool_redis::redis::cmd("SPOP")
            .arg(DIRTY_KEY)
            .arg(SWEEP_BATCH)
            .query_async::<Vec<String>>(&mut conn)
            .await;
        let viewers: Vec<String> = match drained {
            Ok(v) => v,
            Err(e) => {
                warn!(error = %e, "sweep could not drain the dirty set");
                self.metrics.sweep_failures.inc();
                return;
            }
        };
        self.metrics.dirty_viewers.set(viewers.len() as i64);
        if viewers.is_empty() {
            return;
        }

        let mut requested = 0usize;
        let mut failed: Vec<String> = Vec::new();
        for viewer in &viewers {
            match self.enqueue_existing_facets(&mut conn, viewer).await {
                Ok(n) => requested += n,
                Err(e) => {
                    warn!(error = %e, viewer, "sweep failed for viewer; re-marking");
                    failed.push(viewer.clone());
                }
            }
        }

        if !failed.is_empty() {
            self.metrics.sweep_failures.inc();
            let _: Result<i64, _> = conn.sadd(DIRTY_KEY, failed).await;
        }
        self.metrics
            .sweep_rebuilds_requested
            .inc_by(requested as u64);
        info!(
            viewers = viewers.len(),
            rebuilds = requested,
            "swept dirty viewers"
        );
    }

    /// Enqueue a rebuild for every facet this viewer ALREADY has.
    ///
    /// Only existing facets, mirroring the nightly refresh: enqueuing a facet
    /// nobody has asked for would build a blob no feed reads and pay a
    /// ClickHouse query for it.
    async fn enqueue_existing_facets(
        &self,
        conn: &mut deadpool_redis::Connection,
        viewer: &str,
    ) -> anyhow::Result<usize> {
        let mut requested = 0;
        for facet in VIEWER_FACETS {
            let exists: bool = conn.exists(format!("lens:v2:{facet}:{viewer}")).await?;
            if !exists {
                continue;
            }
            let payload = serde_json::json!({ "viewer_did": viewer, "facet": facet }).to_string();
            deadpool_redis::redis::cmd("XADD")
                .arg(BUILD_QUEUE_KEY)
                .arg("MAXLEN")
                .arg("~")
                .arg(BUILD_QUEUE_MAXLEN)
                .arg("*")
                .arg("data")
                .arg(&payload)
                .query_async::<()>(conn)
                .await?;
            requested += 1;
        }
        Ok(requested)
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

    /// The sweeper is the event-driven twin of the nightly refresh job, so it has
    /// to cover the same facets. There is no shared constant to import across
    /// crates, so the list is pinned here and this test is the thing that fails
    /// when the two drift — a facet missing from one side is simply never
    /// refreshed on that path, with nothing to say so.
    #[test]
    fn viewer_facets_match_the_nightly_refresh_job() {
        assert_eq!(
            VIEWER_FACETS,
            &[
                "follows",
                "follows2",
                "niche",
                "popular",
                "velocity",
                "community"
            ],
            "keep in step with FACETS in graze-lens-builder/src/bin/refresh.rs"
        );
    }

    /// `domain` is keyed by feed, not by viewer, so no amount of following
    /// changes it. Enqueuing it per reader would pay a ClickHouse query to
    /// rebuild a blob that cannot have moved.
    #[test]
    fn domain_is_not_a_viewer_facet() {
        assert!(!VIEWER_FACETS.contains(&"domain"));
    }

    /// Both directions have to mark. An unfollow already requested a rebuild of
    /// `follows` alone; without marking, every other facet the reader has stayed
    /// stale until the nightly job.
    #[test]
    fn both_ops_are_handled_not_just_creates() {
        let src = include_str!("delta.rs");
        let apply = src
            .split("pub async fn apply(")
            .nth(1)
            .expect("apply must exist");
        let body = &apply[..apply.find("\n    }").expect("apply must close")];
        assert!(body.contains("\"create\""), "creates must be handled");
        assert!(body.contains("\"delete\""), "deletes must be handled");
        assert!(
            body.contains("mark_dirty"),
            "both ops must mark the viewer dirty, or the blobs never get rebuilt"
        );
    }

    #[test]
    fn dirty_key_is_stable() {
        // Named in operational checks and in the sweeper's own logs.
        assert_eq!(DIRTY_KEY, "lens:dirty");
    }

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
