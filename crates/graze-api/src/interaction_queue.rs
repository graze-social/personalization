//! In-process queue for batched ClickHouse interaction persistence.
//!
//! Handlers send interactions to a channel; a background worker batches them
//! and flushes to ClickHouse every N seconds or when the batch is full.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{broadcast, mpsc};
use tracing::{info, warn};

use graze_common::models::Interaction;
use graze_common::InteractionsClient;

/// Message sent to the interaction queue: (user_did, interactions).
pub type InteractionBatchItem = (String, Vec<Interaction>);

/// Sender for the interaction queue. Clone and store in AppState.
#[derive(Clone)]
pub struct InteractionQueueSender {
    tx: mpsc::Sender<InteractionBatchItem>,
}

impl InteractionQueueSender {
    /// Create a new queue sender from an mpsc sender.
    pub fn new(tx: mpsc::Sender<InteractionBatchItem>) -> Self {
        Self { tx }
    }

    /// Send interactions to the queue. Returns immediately; does not block on ClickHouse.
    pub async fn send(&self, user_did: String, interactions: Vec<Interaction>) {
        if interactions.is_empty() {
            return;
        }
        if self.tx.send((user_did, interactions)).await.is_err() {
            warn!("Interaction queue closed; dropping interactions");
        }
    }
}

/// Run the interaction queue worker. Blocks until shutdown is received.
pub async fn run_interaction_worker(
    mut rx: mpsc::Receiver<InteractionBatchItem>,
    mut shutdown_rx: broadcast::Receiver<()>,
    interactions: Arc<InteractionsClient>,
    batch_interval_ms: u64,
    batch_size: usize,
) {
    let batch_interval = Duration::from_millis(batch_interval_ms);
    let mut interval = tokio::time::interval(batch_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut batch: Vec<InteractionBatchItem> = Vec::with_capacity(batch_size);
    let mut last_flush = tokio::time::Instant::now();

    info!(
        batch_interval_ms,
        batch_size, "Interaction queue worker started"
    );

    loop {
        tokio::select! {
            biased; // Prefer processing messages over ticking

            msg = rx.recv() => {
                match msg {
                    Some(item) => {
                        batch.push(item);

                        // Flush if batch is full
                        if batch.len() >= batch_size {
                            if let Err(e) = interactions.persist_interactions_batch(&batch).await {
                                warn!(error = %e, "Failed to persist interaction batch");
                            }
                            batch.clear();
                            last_flush = tokio::time::Instant::now();
                        }
                    }
                    None => {
                        info!("Interaction queue channel closed");
                        break;
                    }
                }
            }
            _ = interval.tick() => {
                if !batch.is_empty() && last_flush.elapsed() >= batch_interval {
                    if let Err(e) = interactions.persist_interactions_batch(&batch).await {
                        warn!(error = %e, "Failed to persist interaction batch");
                    }
                    batch.clear();
                    last_flush = tokio::time::Instant::now();
                }
            }
            _ = shutdown_rx.recv() => {
                info!("Interaction queue worker received shutdown signal");
                break;
            }
        }
    }

    // Flush remaining on shutdown
    if !batch.is_empty() {
        info!(
            remaining = batch.len(),
            "Flushing remaining interactions on shutdown"
        );
        if let Err(e) = interactions.persist_interactions_batch(&batch).await {
            warn!(error = %e, "Failed to persist final interaction batch");
        }
    }

    info!("Interaction queue worker stopped");
}
