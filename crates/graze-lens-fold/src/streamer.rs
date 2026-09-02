//! The Jetstream tail: connect, batch, insert, commit.
//!
//! Transport is forked from `graze-like-streamer` — jittered exponential
//! backoff, a per-read timeout so a silently dead socket reconnects instead of
//! hanging forever, and the dual-cursor resume. What differs is the sink
//! (ClickHouse rather than Redis) and the collection.

use std::time::Duration;

use futures_util::StreamExt;
use rand::Rng;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{error, info, warn};

use crate::config::Config;
use crate::cursor::Cursor;
use crate::delta::DeltaApplier;
use crate::delta_projection::DeltaProjector;
use crate::event::{self, FollowEdge, FOLLOW_COLLECTION};
use crate::metrics::Metrics;
use crate::sink::Sink;

pub struct Streamer {
    config: Config,
    cursor: Cursor,
    sink: Sink,
    metrics: Metrics,
    /// Keeps already-published lens sets fresh. `None` disables live
    /// maintenance and falls back to sets expiring on their TTL.
    deltas: Option<DeltaApplier>,
    projector: Option<DeltaProjector>,
}

impl Streamer {
    pub fn new(
        config: Config,
        cursor: Cursor,
        sink: Sink,
        metrics: Metrics,
        deltas: Option<DeltaApplier>,
        projector: Option<DeltaProjector>,
    ) -> Self {
        Self {
            config,
            cursor,
            sink,
            metrics,
            deltas,
            projector,
        }
    }

    /// Reconnect forever. Only a shutdown signal ends this.
    pub async fn run(&self, mut shutdown: tokio::sync::watch::Receiver<bool>) {
        let mut delay = Duration::from_secs(1);
        let max = Duration::from_secs(60);

        loop {
            if *shutdown.borrow() {
                info!("shutdown requested");
                return;
            }

            match self.connect_and_stream(&mut shutdown).await {
                Ok(()) => {
                    info!("jetstream connection closed");
                    delay = Duration::from_secs(1);
                }
                Err(e) => {
                    error!(error = %e, "jetstream connection error");
                    self.metrics.reconnects.inc();
                    delay = (delay * 2).min(max);
                }
            }

            let wait = with_jitter(delay);
            info!(delay_ms = wait.as_millis() as u64, "reconnecting");
            tokio::select! {
                _ = tokio::time::sleep(wait) => {}
                _ = shutdown.changed() => return,
            }
        }
    }

    async fn connect_and_stream(
        &self,
        shutdown: &mut tokio::sync::watch::Receiver<bool>,
    ) -> anyhow::Result<()> {
        let resume = self.cursor.resume_from().await?;
        let mut url = format!(
            "{}?wantedCollections={}",
            self.config.jetstream_url, FOLLOW_COLLECTION
        );
        if let Some(c) = resume {
            // Jetstream replays from the cursor; v2 keeps roughly 36h on the
            // websocket, so a wedge shorter than that costs nothing but time.
            url.push_str(&format!("&cursor={c}"));
            info!(cursor = c, "resuming");
        } else {
            info!("no stored cursor; starting from live tip");
        }

        let (ws, _) = connect_async(&url).await?;
        info!("connected to jetstream");
        let (_write, mut read) = ws.split();

        let mut batch: Vec<FollowEdge> = Vec::with_capacity(self.config.batch_size);
        let mut batch_start: Option<u64> = None;
        let mut last_seq: u64 = resume.unwrap_or(0);
        let read_timeout = Duration::from_secs(self.config.read_timeout_seconds);
        let mut flush_tick = tokio::time::interval(self.config.batch_interval);
        flush_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut active_tick = tokio::time::interval(self.config.active_refresh_interval);
        active_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    // Drain what we have so a rollout does not re-fetch it.
                    self.flush(&mut batch, &mut batch_start, last_seq).await;
                    return Ok(());
                }
                _ = flush_tick.tick() => {
                    self.flush(&mut batch, &mut batch_start, last_seq).await;
                    self.publish_age(last_seq);
                }
                _ = active_tick.tick() => {
                    // Whose sets are live? Refreshed on a timer rather than per
                    // event; a viewer who appears between ticks simply waits one
                    // interval before their set starts tracking the stream.
                    if let Some(deltas) = &self.deltas {
                        if let Err(e) = deltas.refresh_active().await {
                            warn!(error = %e, "could not refresh active viewers");
                        }
                    }
                }
                frame = tokio::time::timeout(read_timeout, read.next()) => {
                    match frame {
                        Ok(Some(Ok(Message::Text(text)))) => {
                            self.metrics.frames_received.inc();
                            if let Some(edge) = event::parse(&text) {
                                last_seq = edge.seq;
                                if batch_start.is_none() {
                                    batch_start = Some(edge.seq);
                                    // Mark in flight before the first row of a
                                    // batch, so a crash resumes from here.
                                    if let Err(e) = self.cursor.mark_pending(edge.seq).await {
                                        warn!(error = %e, "could not mark pending cursor");
                                    }
                                }
                                match edge.op {
                                    "create" => self.metrics.follows.inc(),
                                    _ => self.metrics.unfollows.inc(),
                                };
                                // Keep live sets fresh. Gated on a hash lookup
                                // first, so the overwhelming majority of events
                                // -- follows by accounts nobody has a lens for
                                // -- cost nothing beyond that check.
                                if let Some(deltas) = &self.deltas {
                                    if deltas.is_active(&edge.follower).await {
                                        deltas.apply(&edge).await;
                                    }
                                }
                                batch.push(edge);
                                if batch.len() >= self.config.batch_size {
                                    self.flush(&mut batch, &mut batch_start, last_seq).await;
                                }
                            }
                        }
                        Ok(Some(Ok(Message::Close(_)))) => {
                            self.flush(&mut batch, &mut batch_start, last_seq).await;
                            return Ok(());
                        }
                        Ok(Some(Ok(_))) => {}
                        Ok(Some(Err(e))) => {
                            self.flush(&mut batch, &mut batch_start, last_seq).await;
                            return Err(e.into());
                        }
                        Ok(None) => {
                            self.flush(&mut batch, &mut batch_start, last_seq).await;
                            return Ok(());
                        }
                        Err(_) => {
                            // No frame within the budget. Follow traffic never
                            // goes quiet globally, so silence means a dead
                            // socket, not an idle network.
                            warn!(
                                timeout_secs = read_timeout.as_secs(),
                                "read timeout; reconnecting"
                            );
                            self.flush(&mut batch, &mut batch_start, last_seq).await;
                            return Ok(());
                        }
                    }
                }
            }
        }
    }

    /// Insert the batch and commit the cursor. A failed insert keeps the batch
    /// and leaves the pending cursor in place, so the rows are retried on the
    /// next tick and, failing that, re-fetched after a reconnect.
    async fn flush(&self, batch: &mut Vec<FollowEdge>, start: &mut Option<u64>, last_seq: u64) {
        if batch.is_empty() {
            return;
        }
        match self.sink.insert(batch).await {
            Ok(()) => {
                self.metrics.rows_written.inc_by(batch.len() as u64);
                // Only after the raw insert succeeded: the delta is a derived
                // view, and a row in it whose edge never landed in `follow_edges`
                // would survive the nightly compaction as a phantom the base
                // table cannot account for.
                if let Some(projector) = &self.projector {
                    projector.project(batch).await;
                }
                batch.clear();
                *start = None;
                if let Err(e) = self.cursor.commit(last_seq).await {
                    warn!(error = %e, "could not commit cursor");
                }
            }
            Err(e) => {
                error!(error = %e, rows = batch.len(), "insert failed; will retry");
                self.metrics.insert_failures.inc();
                if batch.len() > self.config.max_pending_rows {
                    // Backstop: an insert failing for hours would otherwise grow
                    // the batch until the pod is OOMKilled. The pending cursor
                    // survives, so dropping in-memory rows loses nothing — the
                    // reconnect replays them.
                    warn!(
                        rows = batch.len(),
                        "pending rows over budget; dropping to the stored cursor"
                    );
                    batch.clear();
                    *start = None;
                }
            }
        }
    }

    fn publish_age(&self, last_seq: u64) {
        if last_seq == 0 {
            return;
        }
        let now_us = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_micros() as u64)
            .unwrap_or(0);
        self.metrics
            .cursor_age_seconds
            .set(crate::cursor::age_seconds(now_us, last_seq) as i64);
    }
}

/// Exponential backoff with up to 25% jitter, so a fleet-wide disconnect does
/// not reconnect in lockstep.
fn with_jitter(base: Duration) -> Duration {
    let jitter_range = base.as_millis() as u64 / 4;
    if jitter_range == 0 {
        return base;
    }
    base + Duration::from_millis(rand::thread_rng().gen_range(0..=jitter_range))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jitter_never_shortens_the_delay() {
        let base = Duration::from_secs(4);
        for _ in 0..50 {
            let d = with_jitter(base);
            assert!(d >= base);
            assert!(d <= base + Duration::from_millis(1000));
        }
    }

    #[test]
    fn sub_millisecond_delays_are_left_alone() {
        assert_eq!(
            with_jitter(Duration::from_micros(500)),
            Duration::from_micros(500)
        );
    }
}
