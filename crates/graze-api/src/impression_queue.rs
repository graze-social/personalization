//! In-process queue for batched ClickHouse impression row persistence.
//!
//! Mirrors `interaction_queue.rs`: handlers send `FeedImpressionRow`s to an
//! mpsc channel; a background worker batches them and flushes to ClickHouse
//! via `ImpressionWriter` every N seconds or when the batch is full.
//!
//! Only active when `ML_IMPRESSIONS_ENABLED=true`.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{broadcast, mpsc};
use tracing::{info, warn};

use graze_common::models::FeedImpressionRow;
use graze_common::ImpressionWriter;

/// Sender handle for the impression queue. Clone and store in `AppState`.
#[derive(Clone)]
pub struct ImpressionQueueSender {
    tx: mpsc::Sender<FeedImpressionRow>,
}

impl ImpressionQueueSender {
    /// Create from a raw mpsc sender.
    pub fn new(tx: mpsc::Sender<FeedImpressionRow>) -> Self {
        Self { tx }
    }

    /// Enqueue a single impression row. Non-blocking; drops on queue full.
    pub async fn send(&self, row: FeedImpressionRow) {
        if self.tx.send(row).await.is_err() {
            warn!("Impression queue closed; dropping impression row");
        }
    }

    /// Enqueue a batch of impression rows. Non-blocking; drops on queue full.
    pub async fn send_batch(&self, rows: Vec<FeedImpressionRow>) {
        for row in rows {
            if self.tx.send(row).await.is_err() {
                warn!("Impression queue closed; dropping impression batch");
                break;
            }
        }
    }
}

/// Run the impression queue worker. Blocks until shutdown is received.
pub async fn run_impression_worker(
    mut rx: mpsc::Receiver<FeedImpressionRow>,
    mut shutdown_rx: broadcast::Receiver<()>,
    writer: Arc<dyn ImpressionWriter>,
    batch_interval_ms: u64,
    batch_size: usize,
) {
    let batch_interval = Duration::from_millis(batch_interval_ms);
    let mut interval = tokio::time::interval(batch_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut batch: Vec<FeedImpressionRow> = Vec::with_capacity(batch_size);
    let mut last_flush = tokio::time::Instant::now();

    info!(
        batch_interval_ms,
        batch_size, "Impression queue worker started"
    );

    loop {
        tokio::select! {
            biased; // Prefer draining messages over ticking

            msg = rx.recv() => {
                match msg {
                    Some(row) => {
                        batch.push(row);
                        if batch.len() >= batch_size {
                            flush(&writer, &mut batch).await;
                            last_flush = tokio::time::Instant::now();
                        }
                    }
                    None => {
                        info!("Impression queue channel closed");
                        break;
                    }
                }
            }
            _ = interval.tick() => {
                if !batch.is_empty() && last_flush.elapsed() >= batch_interval {
                    flush(&writer, &mut batch).await;
                    last_flush = tokio::time::Instant::now();
                }
            }
            _ = shutdown_rx.recv() => {
                info!("Impression queue worker received shutdown signal");
                break;
            }
        }
    }

    // Flush remaining on shutdown
    if !batch.is_empty() {
        info!(remaining = batch.len(), "Flushing remaining impressions on shutdown");
        flush(&writer, &mut batch).await;
    }

    info!("Impression queue worker stopped");
}

async fn flush(writer: &Arc<dyn ImpressionWriter>, batch: &mut Vec<FeedImpressionRow>) {
    let rows = std::mem::take(batch);
    if let Err(e) = writer.persist_impressions(rows).await {
        warn!(error = %e, "Failed to persist impression batch");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    use chrono::Utc;
    use graze_common::error::Result;
    use graze_common::models::FeedImpressionRow;

    // ─── Test writer that records persisted rows ──────────────────────────────

    struct RecordingWriter {
        rows: Arc<Mutex<Vec<FeedImpressionRow>>>,
    }

    impl RecordingWriter {
        fn new() -> (Self, Arc<Mutex<Vec<FeedImpressionRow>>>) {
            let rows = Arc::new(Mutex::new(Vec::new()));
            (Self { rows: Arc::clone(&rows) }, rows)
        }
    }

    #[async_trait::async_trait]
    impl ImpressionWriter for RecordingWriter {
        async fn persist_impressions(&self, rows: Vec<FeedImpressionRow>) -> Result<()> {
            self.rows.lock().unwrap().extend(rows);
            Ok(())
        }
    }

    fn make_row(id: &str) -> FeedImpressionRow {
        FeedImpressionRow {
            impression_id: id.to_string(),
            served_at: Utc::now(),
            ..Default::default()
        }
    }

    // ─── ImpressionQueueSender ────────────────────────────────────────────────

    #[tokio::test]
    async fn test_send_single_row_reaches_receiver() {
        let (tx, mut rx) = mpsc::channel::<FeedImpressionRow>(16);
        let sender = ImpressionQueueSender::new(tx);
        sender.send(make_row("a")).await;
        let received = rx.recv().await.expect("row");
        assert_eq!(received.impression_id, "a");
    }

    #[tokio::test]
    async fn test_send_batch_all_rows_reach_receiver() {
        let (tx, mut rx) = mpsc::channel::<FeedImpressionRow>(16);
        let sender = ImpressionQueueSender::new(tx);
        let batch = vec![make_row("x"), make_row("y"), make_row("z")];
        sender.send_batch(batch).await;
        drop(sender);
        let mut ids = Vec::new();
        while let Ok(row) = rx.try_recv() {
            ids.push(row.impression_id);
        }
        assert_eq!(ids, vec!["x", "y", "z"]);
    }

    #[tokio::test]
    async fn test_send_to_closed_channel_does_not_panic() {
        let (tx, rx) = mpsc::channel::<FeedImpressionRow>(1);
        drop(rx);
        let sender = ImpressionQueueSender::new(tx);
        // Should warn but not panic
        sender.send(make_row("drop_me")).await;
    }

    // ─── Worker: batch-size flush ─────────────────────────────────────────────

    #[tokio::test]
    async fn test_worker_flushes_when_batch_full() {
        let (writer, recorded) = RecordingWriter::new();
        let writer: Arc<dyn ImpressionWriter> = Arc::new(writer);

        let (tx, rx) = mpsc::channel::<FeedImpressionRow>(64);
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);

        // batch_size=3, very long interval so only size trigger fires
        let handle = tokio::spawn(run_impression_worker(
            rx, shutdown_rx, writer, 60_000, 3,
        ));

        // Send exactly 3 rows — should trigger a flush
        for i in 0..3 {
            tx.send(make_row(&format!("batch_{}", i))).await.unwrap();
        }

        // Give the worker a tick to process
        tokio::time::sleep(Duration::from_millis(50)).await;

        let _ = shutdown_tx.send(());
        let _ = handle.await;

        let flushed = recorded.lock().unwrap();
        assert_eq!(flushed.len(), 3, "batch of 3 should have been flushed");
    }

    // ─── Worker: shutdown flushes remainder ───────────────────────────────────

    #[tokio::test]
    async fn test_worker_flushes_on_shutdown() {
        let (writer, recorded) = RecordingWriter::new();
        let writer: Arc<dyn ImpressionWriter> = Arc::new(writer);

        let (tx, rx) = mpsc::channel::<FeedImpressionRow>(64);
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);

        // batch_size=100 so only shutdown triggers flush
        let handle = tokio::spawn(run_impression_worker(
            rx, shutdown_rx, writer, 60_000, 100,
        ));

        for i in 0..5 {
            tx.send(make_row(&format!("shutdown_{}", i))).await.unwrap();
        }

        // Signal shutdown — worker should flush remaining 5
        tokio::time::sleep(Duration::from_millis(20)).await;
        let _ = shutdown_tx.send(());
        let _ = handle.await;

        let flushed = recorded.lock().unwrap();
        assert_eq!(flushed.len(), 5, "remaining rows should flush on shutdown");
    }

    // ─── Worker: empty queue on shutdown ─────────────────────────────────────

    #[tokio::test]
    async fn test_worker_empty_shutdown_is_clean() {
        let (writer, recorded) = RecordingWriter::new();
        let writer: Arc<dyn ImpressionWriter> = Arc::new(writer);
        let (_tx, rx) = mpsc::channel::<FeedImpressionRow>(1);
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);

        let handle = tokio::spawn(run_impression_worker(
            rx, shutdown_rx, writer, 60_000, 10,
        ));

        let _ = shutdown_tx.send(());
        let _ = handle.await;

        assert!(recorded.lock().unwrap().is_empty());
    }
}
