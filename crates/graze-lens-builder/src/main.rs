//! graze-lens-builder: drains the lens build queue.

use std::sync::Arc;

use anyhow::Context;
use deadpool_redis::{Config as RedisConfig, Runtime};
use graze_lens_builder::{BuildOutcome, Builder, Config, Queue};
use tokio::signal;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let config = Config::from_env().context("configuration")?;
    info!(
        consumer = %config.consumer_name,
        group = %config.consumer_group,
        "starting lens builder"
    );

    let pool = RedisConfig::from_url(config.redis_url.clone())
        .builder()?
        .max_size(config.redis_pool_size)
        .runtime(Runtime::Tokio1)
        .build()
        .context("redis pool")?;

    let queue = Queue::new(
        pool.clone(),
        config.consumer_group.clone(),
        config.consumer_name.clone(),
    );
    queue.ensure_group().await.context("consumer group")?;

    let builder = Arc::new(Builder::with_backfill(pool, config.clone()).context("builder")?);

    let mut shutdown = Box::pin(signal::ctrl_c());
    let block_ms = config.block.as_millis() as u64;

    loop {
        let deliveries = tokio::select! {
            _ = &mut shutdown => {
                info!("shutdown requested");
                break;
            }
            result = queue.read(config.batch_size, block_ms) => match result {
                Ok(d) => d,
                Err(e) => {
                    // A Redis blip must not kill the worker; the next read
                    // reconnects through the pool.
                    error!(error = %e, "queue read failed");
                    tokio::time::sleep(config.block).await;
                    continue;
                }
            },
        };

        for delivery in deliveries {
            let viewer = delivery.request.viewer_did.clone();
            let facet = delivery.request.facet.clone();

            match builder.build(&viewer, &facet).await {
                Ok(BuildOutcome::Published) => info!(viewer, facet, "lens published"),
                Ok(BuildOutcome::Empty) => info!(viewer, facet, "viewer follows nobody"),
                Ok(BuildOutcome::TooLarge) => warn!(viewer, facet, "lens over size budget"),
                Ok(BuildOutcome::NeedsBackfill) => {
                    warn!(
                        viewer,
                        facet, "no follow history and no way to backfill; not publishing"
                    )
                }
                Err(e) => error!(error = %e, viewer, facet, "lens build failed"),
            }

            // Acknowledged either way. A failed build leaves `lensmeta` unset or
            // `failed`, so the serve path will re-enqueue on the viewer's next
            // request; keeping it pending here would just re-run the same
            // failing query on a timer.
            if let Err(e) = queue.ack(&delivery.id).await {
                warn!(error = %e, id = %delivery.id, "ack failed");
            }
        }
    }

    Ok(())
}
