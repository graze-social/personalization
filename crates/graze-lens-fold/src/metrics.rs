//! Prometheus metrics.
//!
//! `lens_fold_cursor_age_seconds` is the one that matters. A consumer that
//! wedges silently is how the turbostream bridge outage ran for hours unnoticed;
//! alert on this above ~1800s.

use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::registry::Registry;
use std::sync::Arc;

#[derive(Clone)]
pub struct Metrics {
    registry: Arc<Registry>,
    pub frames_received: Counter,
    pub follows: Counter,
    pub unfollows: Counter,
    pub rows_written: Counter,
    pub insert_failures: Counter,
    pub reconnects: Counter,
    pub cursor_age_seconds: Gauge,

    /// Live-set maintenance. `deltas_applied` rising while
    /// `deltas_rebuild_requested` stays low is the healthy shape: follows are
    /// cheap to apply, unfollows cost a rebuild.
    pub deltas_applied: Counter,
    pub deltas_rebuild_requested: Counter,
    pub delta_failures: Counter,
    pub active_viewers: Gauge,
}

impl Metrics {
    pub fn new() -> Self {
        // Counters are registered WITHOUT `_total`; the encoder appends it.
        // Registering "rows_written_total" would emit `..._total_total` and any
        // alert on the sane name would never fire.
        let mut registry = Registry::with_prefix("lens_fold");

        let frames_received = Counter::default();
        registry.register(
            "frames_received",
            "Jetstream frames received",
            frames_received.clone(),
        );

        let follows = Counter::default();
        registry.register("follows", "Follow creates parsed", follows.clone());

        let unfollows = Counter::default();
        registry.register("unfollows", "Follow deletes parsed", unfollows.clone());

        let rows_written = Counter::default();
        registry.register(
            "rows_written",
            "Edge rows accepted by ClickHouse",
            rows_written.clone(),
        );

        let insert_failures = Counter::default();
        registry.register(
            "insert_failures",
            "ClickHouse insert attempts that failed",
            insert_failures.clone(),
        );

        let reconnects = Counter::default();
        registry.register(
            "reconnects",
            "Jetstream reconnections after an error",
            reconnects.clone(),
        );

        let cursor_age_seconds = Gauge::default();
        registry.register(
            "cursor_age_seconds",
            "Seconds between now and the last processed event; alert above 1800",
            cursor_age_seconds.clone(),
        );

        let deltas_applied = Counter::default();
        registry.register(
            "deltas_applied",
            "Follows applied directly to a live lens set",
            deltas_applied.clone(),
        );

        let deltas_rebuild_requested = Counter::default();
        registry.register(
            "deltas_rebuild_requested",
            "Rebuilds requested because an unfollow cannot name its subject",
            deltas_rebuild_requested.clone(),
        );

        let delta_failures = Counter::default();
        registry.register(
            "delta_failures",
            "Delta applications that errored",
            delta_failures.clone(),
        );

        let active_viewers = Gauge::default();
        registry.register(
            "active_viewers",
            "Viewers with a live lens set being kept fresh",
            active_viewers.clone(),
        );

        Self {
            registry: Arc::new(registry),
            frames_received,
            follows,
            unfollows,
            rows_written,
            insert_failures,
            reconnects,
            cursor_age_seconds,
            deltas_applied,
            deltas_rebuild_requested,
            delta_failures,
            active_viewers,
        }
    }

    pub fn encode(&self) -> String {
        let mut buffer = String::new();
        encode(&mut buffer, &self.registry).expect("encoding cannot fail");
        buffer
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The encoder appends `_total`; registering it too would double it and
    /// silently break every alert built on the sane name.
    #[test]
    fn counter_names_are_not_double_suffixed() {
        let m = Metrics::new();
        m.rows_written.inc();
        let out = m.encode();
        assert!(out.contains("lens_fold_rows_written_total"));
        assert!(!out.contains("_total_total"));
    }

    /// The alertable gauge must be present and unsuffixed.
    #[test]
    fn cursor_age_gauge_is_exposed() {
        let m = Metrics::new();
        m.cursor_age_seconds.set(42);
        let out = m.encode();
        assert!(out.contains("lens_fold_cursor_age_seconds 42"));
    }
}
