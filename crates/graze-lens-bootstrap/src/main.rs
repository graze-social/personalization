//! graze-lens-bootstrap: backfill `follow_edges` for a set of accounts.
//!
//! Usage:
//!     graze-lens-bootstrap did:plc:aaa did:plc:bbb
//!     graze-lens-bootstrap --file dids.txt
//!     cat dids.txt | graze-lens-bootstrap
//!
//! Safe to re-run: rows are versioned by the record's TID, so repeats collapse.

use std::io::BufRead;
use std::sync::Arc;

use anyhow::Context;
use graze_lens_bootstrap::{Backfiller, CompletenessStore, Config, Resolver, SOURCE_BOOTSTRAP};
use graze_lens_fold::{FollowEdge, Sink};
use tokio::sync::Mutex;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let dids = read_dids()?;
    if dids.is_empty() {
        anyhow::bail!("no DIDs given; pass them as arguments, via --file, or on stdin");
    }

    let config = Config::from_env().context("configuration")?;
    info!(
        accounts = dids.len(),
        concurrency = config.concurrency,
        dry_run = config.dry_run,
        "starting follow-graph backfill"
    );

    let http = reqwest::Client::builder()
        .connect_timeout(std::time::Duration::from_secs(10))
        .user_agent(concat!("graze-lens-bootstrap/", env!("CARGO_PKG_VERSION")))
        .build()?;

    let resolver = Resolver::new(http.clone(), config.plc_directory.clone());
    let backfiller = Arc::new(Backfiller::new(
        http,
        resolver,
        config.request_timeout,
        config.page_delay,
        config.max_pages,
        config.max_retries,
    ));
    let sink =
        Arc::new(Sink::new(config.clickhouse.clone(), config.insert_timeout).context("sink")?);

    // Without this, every account this job backfills is re-walked from its PDS
    // the first time it asks for a lens: the builder's completeness guard only
    // trusts markers, and only the builder's own inline path was writing them.
    // A 9,423-account run left 6 markers behind, so the bulk warming bought the
    // cohort nothing but a slow first request each.
    let completeness = Arc::new(
        CompletenessStore::new(
            config.clickhouse.clone(),
            config.insert_timeout,
            config.insert_timeout.as_secs().max(1),
        )
        .context("completeness store")?,
    );

    // Edges and the viewers they belong to travel together, and a viewer is only
    // marked once the batch carrying their edges is durable. Marking on task
    // completion instead would race the flush: a crash in between leaves a
    // marker whose edges were never written, and the next build trusts it,
    // skips the backfill, and publishes a lens from a graph that does not exist.
    // That is the exact failure the marker was introduced to prevent.
    let pending: Arc<Mutex<Vec<FollowEdge>>> = Arc::new(Mutex::new(Vec::new()));
    let pending_viewers: Arc<Mutex<Vec<(String, usize)>>> = Arc::new(Mutex::new(Vec::new()));
    let total = dids.len();
    let done = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let failed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let rows = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    // A plain bounded worker pool: the fan-out here is to strangers' PDS hosts,
    // so the ceiling is politeness rather than our own capacity.
    let semaphore = Arc::new(tokio::sync::Semaphore::new(config.concurrency.max(1)));
    let mut tasks = Vec::with_capacity(dids.len());

    for did in dids {
        let permit = semaphore.clone().acquire_owned().await?;
        let (backfiller, sink, pending) = (backfiller.clone(), sink.clone(), pending.clone());
        let (completeness, pending_viewers) = (completeness.clone(), pending_viewers.clone());
        let (done, failed, rows) = (done.clone(), failed.clone(), rows.clone());
        let config = config.clone();

        tasks.push(tokio::spawn(async move {
            let _permit = permit;
            match backfiller.edges_for(&did).await {
                Ok(backfilled) => {
                    let count = backfilled.edges.len();
                    rows.fetch_add(count, std::sync::atomic::Ordering::Relaxed);
                    if !config.dry_run && count > 0 {
                        let mut buf = pending.lock().await;
                        buf.extend(backfilled.edges);
                        // A truncated account is deliberately not recorded: its
                        // edges are a prefix, and a marker would freeze that
                        // prefix as its graph forever. Zero-edge accounts are
                        // skipped too — the guard treats a zero-edge row as
                        // incomplete anyway, so writing one is pure noise.
                        if !backfilled.truncated {
                            pending_viewers.lock().await.push((did.clone(), count));
                        }

                        if buf.len() >= config.insert_batch {
                            let batch = std::mem::take(&mut *buf);
                            drop(buf);
                            let viewers = std::mem::take(&mut *pending_viewers.lock().await);
                            match sink.insert(&batch).await {
                                Ok(()) => {
                                    // Only now are these viewers' edges durable.
                                    if let Err(e) =
                                        completeness.mark_many(&viewers, SOURCE_BOOTSTRAP).await
                                    {
                                        // Not fatal: the edges are written, so
                                        // the worst case is the builder
                                        // re-walking these accounts once.
                                        warn!(error = %e, viewers = viewers.len(),
                                              "recording completeness failed");
                                    }
                                }
                                Err(e) => {
                                    // Keep going: one failed insert should not
                                    // abort a long backfill, and the job is
                                    // re-runnable. The viewers are dropped
                                    // unmarked, which is the safe direction.
                                    error!(error = %e, rows = batch.len(), "insert failed");
                                    failed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    warn!(error = %e, did, "backfill failed for account");
                    failed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }

            let n = done.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
            if n % 100 == 0 || n == total {
                info!(
                    done = n,
                    total,
                    edges = rows.load(std::sync::atomic::Ordering::Relaxed),
                    "backfill progress"
                );
            }
        }));
    }

    for task in tasks {
        let _ = task.await;
    }

    // Flush the tail.
    let remaining = std::mem::take(&mut *pending.lock().await);
    let remaining_viewers = std::mem::take(&mut *pending_viewers.lock().await);
    if !config.dry_run && !remaining.is_empty() {
        match sink.insert(&remaining).await {
            Ok(()) => {
                if let Err(e) = completeness
                    .mark_many(&remaining_viewers, SOURCE_BOOTSTRAP)
                    .await
                {
                    warn!(error = %e, viewers = remaining_viewers.len(),
                          "recording completeness failed for the final batch");
                }
            }
            Err(e) => {
                error!(error = %e, rows = remaining.len(), "final insert failed");
                failed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
    }

    let failures = failed.load(std::sync::atomic::Ordering::Relaxed);
    info!(
        accounts = total,
        edges = rows.load(std::sync::atomic::Ordering::Relaxed),
        failures,
        dry_run = config.dry_run,
        "backfill complete"
    );

    // Non-zero exit on any failure so a Job shows as failed rather than
    // silently having skipped accounts.
    if failures > 0 {
        anyhow::bail!("{failures} account(s) or insert(s) failed; see logs");
    }
    Ok(())
}

/// DIDs from argv, `--file <path>`, or stdin. Blank lines and `#` comments are
/// ignored so a hand-maintained allowlist can carry notes.
fn read_dids() -> anyhow::Result<Vec<String>> {
    let args: Vec<String> = std::env::args().skip(1).collect();

    if let Some(idx) = args.iter().position(|a| a == "--file") {
        let path = args
            .get(idx + 1)
            .ok_or_else(|| anyhow::anyhow!("--file needs a path"))?;
        let file = std::fs::File::open(path).with_context(|| format!("opening {path}"))?;
        return Ok(collect(
            std::io::BufReader::new(file).lines().map_while(Result::ok),
        ));
    }

    let positional: Vec<String> = args.into_iter().filter(|a| !a.starts_with("--")).collect();
    if !positional.is_empty() {
        return Ok(collect(positional.into_iter()));
    }

    Ok(collect(
        std::io::stdin().lock().lines().map_while(Result::ok),
    ))
}

fn collect(lines: impl Iterator<Item = String>) -> Vec<String> {
    let mut seen = std::collections::HashSet::new();
    lines
        .map(|l| l.trim().to_string())
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .filter(|l| seen.insert(l.clone()))
        .collect()
}
