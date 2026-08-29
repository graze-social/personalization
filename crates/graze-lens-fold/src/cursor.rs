//! Crash-safe cursor tracking.
//!
//! Two keys, copied from `graze-like-streamer`: a committed cursor and a pending
//! one. The pending key is written *before* a batch is inserted and cleared
//! after; if the process dies mid-insert, its presence on restart says "a batch
//! was in flight, resume from before it" rather than skipping those events.
//! Re-delivering a few thousand follow events is free — ReplacingMergeTree
//! collapses them on (follower, rkey, seq).
//!
//! The gauge this module feeds is the single most important alert in the
//! service: a silently wedged consumer is exactly how the turbostream bridge
//! outage went unnoticed. Jetstream v2's ~36h websocket rewind plus the HTTP
//! segment archive mean a wedge caught within that window is recoverable
//! losslessly — but only if someone is told.

use deadpool_redis::redis::AsyncCommands;
use deadpool_redis::Pool;

pub const COMMITTED_KEY: &str = "lens:fold:cursor";
pub const PENDING_KEY: &str = "lens:fold:cursor:pending";

pub struct Cursor {
    redis: Pool,
}

impl Cursor {
    pub fn new(redis: Pool) -> Self {
        Self { redis }
    }

    /// Where to resume. Prefers the pending cursor: its presence means a batch
    /// was in flight when we stopped, so those events may never have landed.
    pub async fn resume_from(&self) -> anyhow::Result<Option<u64>> {
        let mut conn = self.redis.get().await?;
        let pending: Option<String> = conn.get(PENDING_KEY).await?;
        if let Some(p) = pending {
            if let Ok(v) = p.parse() {
                return Ok(Some(v));
            }
        }
        let committed: Option<String> = conn.get(COMMITTED_KEY).await?;
        Ok(committed.and_then(|c| c.parse().ok()))
    }

    /// Mark a batch as in flight, starting from `cursor`.
    pub async fn mark_pending(&self, cursor: u64) -> anyhow::Result<()> {
        let mut conn = self.redis.get().await?;
        let _: () = conn.set(PENDING_KEY, cursor.to_string()).await?;
        Ok(())
    }

    /// Commit progress through `cursor` and clear the in-flight marker.
    ///
    /// Order matters: commit first, then clear. Clearing first would leave a
    /// window where a crash loses both markers and resumes from the old
    /// committed cursor — which is safe, but re-does more work than necessary.
    pub async fn commit(&self, cursor: u64) -> anyhow::Result<()> {
        let mut conn = self.redis.get().await?;
        deadpool_redis::redis::pipe()
            .atomic()
            .set(COMMITTED_KEY, cursor.to_string())
            .del(PENDING_KEY)
            .query_async::<()>(&mut conn)
            .await?;
        Ok(())
    }

    /// Age of the committed cursor in seconds, for the staleness gauge.
    ///
    /// `now_us` is passed in rather than read here so this stays testable.
    pub async fn age_seconds(&self, now_us: u64) -> anyhow::Result<Option<f64>> {
        let mut conn = self.redis.get().await?;
        let committed: Option<String> = conn.get(COMMITTED_KEY).await?;
        Ok(committed
            .and_then(|c| c.parse::<u64>().ok())
            .map(|c| age_seconds(now_us, c)))
    }
}

/// Seconds between a cursor and now, clamped at zero.
///
/// Clock skew (or a cursor from a host running slightly ahead) can make a cursor
/// look like it is in the future; a negative age would render as a healthy gauge
/// forever, which is the opposite of what this exists to catch.
pub fn age_seconds(now_us: u64, cursor_us: u64) -> f64 {
    now_us.saturating_sub(cursor_us) as f64 / 1_000_000.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn age_is_seconds_behind_now() {
        assert_eq!(age_seconds(10_000_000, 4_000_000), 6.0);
    }

    #[test]
    fn a_future_cursor_reads_as_zero_not_negative() {
        assert_eq!(age_seconds(4_000_000, 10_000_000), 0.0);
    }

    #[test]
    fn keys_are_distinct() {
        assert_ne!(COMMITTED_KEY, PENDING_KEY);
    }
}
