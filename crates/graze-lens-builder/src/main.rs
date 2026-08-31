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

    // The shared DID interner lives on the *cache* Redis, a different instance
    // from the one lens blobs are published to. Optional: without it the
    // builder still serves v1 sets, it just cannot build v2 or follows².
    let interner_pool = match std::env::var("REDIS_URL").ok().filter(|u| !u.is_empty()) {
        Some(url) => {
            let built = RedisConfig::from_url(url)
                .builder()
                .map_err(anyhow::Error::from)
                .and_then(|b| {
                    b.max_size(4)
                        .runtime(Runtime::Tokio1)
                        .build()
                        .map_err(anyhow::Error::from)
                });
            match built {
                Ok(p) => Some(p),
                Err(e) => {
                    warn!(error = %e, "interner redis unavailable; v2 and follows2 disabled");
                    None
                }
            }
        }
        None => {
            warn!("REDIS_URL unset; v2 and follows2 disabled");
            None
        }
    };

    let builder =
        Arc::new(Builder::with_backfill(pool, interner_pool, config.clone()).context("builder")?);
    let queue = Arc::new(queue);
    let semaphore = Arc::new(tokio::sync::Semaphore::new(config.concurrency.max(1)));

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

        // Builds run concurrently, bounded by a semaphore.
        //
        // Serially, one slow build stalls every other viewer behind it: a whale
        // backfill is ~2 minutes of paging someone else's PDS (measured at
        // 35,092 follows), during which nobody else's lens gets built at all.
        // The work is almost entirely waiting on other people's servers and on
        // ClickHouse, so overlapping it costs us nothing and removes the
        // head-of-line stall.
        //
        // Two builds for the same viewer may overlap. That is safe rather than
        // merely tolerable: publishing stages into a temporary key and RENAMEs,
        // so a reader sees one complete set or the other, never a mixture.
        for delivery in deliveries {
            let permit = match semaphore.clone().acquire_owned().await {
                Ok(p) => p,
                Err(e) => {
                    error!(error = %e, "build semaphore closed");
                    break;
                }
            };
            let builder = builder.clone();
            let queue = queue.clone();

            tokio::spawn(async move {
                let _permit = permit;
                let viewer = delivery.request.viewer_did.clone();
                let facet = delivery.request.facet.clone();

                // Feed-scoped facets carry an algorithm id and no viewer.
                let result = if facet == "domain" {
                    match delivery.request.feed_algo_id {
                        Some(algo_id) => builder.build_domain(algo_id).await,
                        None => {
                            warn!(facet, "domain request without feed_algo_id; dropping");
                            Ok(BuildOutcome::UnknownFacet)
                        }
                    }
                } else {
                    builder.build(&viewer, &facet).await
                };

                match result {
                    Ok(BuildOutcome::Published) => info!(viewer, facet, "lens published"),
                    Ok(BuildOutcome::Empty) => info!(viewer, facet, "viewer follows nobody"),
                    Ok(BuildOutcome::TooLarge) => warn!(viewer, facet, "lens over size budget"),
                    Ok(BuildOutcome::UnknownFacet) => {
                        warn!(viewer, facet, "unknown facet; nothing built")
                    }
                    Ok(BuildOutcome::NeedsBackfill) => {
                        warn!(
                            viewer,
                            facet, "no follow history and no way to backfill; not publishing"
                        )
                    }
                    Err(e) => error!(error = %e, viewer, facet, "lens build failed"),
                }

                // Acknowledged either way. A failed build leaves `lensmeta`
                // unset or `failed`, so the serve path re-enqueues on the
                // viewer's next request; keeping it pending would just re-run
                // the same failing query on a timer.
                if let Err(e) = queue.ack(&delivery.id).await {
                    warn!(error = %e, id = %delivery.id, "ack failed");
                }
            });
        }
    }

    // Let in-flight builds finish rather than abandoning a half-done backfill.
    // Acquiring every permit means every task has released one.
    info!("draining in-flight builds");
    let _ = tokio::time::timeout(
        config.drain_timeout,
        semaphore.acquire_many(config.concurrency as u32),
    )
    .await;

    Ok(())
}
