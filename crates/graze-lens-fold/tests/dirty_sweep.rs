//! The propagation chain: a viewer's own graph change reaching a rebuild request.
//!
//! Env-gated on `TEST_REDIS_URL` like the other live tests in this workspace. The
//! whole point of the mark-and-sweep is behaviour Redis mediates — coalescing,
//! atomic draining, only-existing-facets — so a unit test over pure functions
//! would prove none of it.
//!
//! Deliberately ONE test with phases rather than four tests: `lens:dirty` and
//! `queue:lens` are fixed global keys, and `SPOP` drains the dirty set wholesale,
//! so a parallel sweep in another test would swallow this one's marked viewer.
//! Phases keep it honest without requiring `--test-threads=1` to pass.
//!
//!   TEST_REDIS_URL=redis://127.0.0.1:16379 \
//!   cargo test -p graze-lens-fold --test dirty_sweep -- --nocapture

use deadpool_redis::redis::AsyncCommands;
use deadpool_redis::{Config as RedisConfig, Runtime};
use graze_lens_fold::{DeltaApplier, Metrics};

const VIEWER: &str = "did:plc:sweep-test-viewer";

fn url() -> Option<String> {
    std::env::var("TEST_REDIS_URL")
        .ok()
        .filter(|u| !u.is_empty())
}

async fn pool(url: &str) -> deadpool_redis::Pool {
    RedisConfig::from_url(url)
        .builder()
        .expect("pool builder")
        .max_size(4)
        .runtime(Runtime::Tokio1)
        .build()
        .expect("pool")
}

async fn cleanup(pool: &deadpool_redis::Pool) {
    let mut conn = pool.get().await.expect("conn");
    let _: i64 = conn.del("lens:dirty").await.unwrap_or(0);
    let _: i64 = conn.del("queue:lens").await.unwrap_or(0);
    for facet in [
        "follows",
        "follows2",
        "niche",
        "popular",
        "velocity",
        "community",
    ] {
        let _: i64 = conn
            .del(format!("lens:v2:{facet}:{VIEWER}"))
            .await
            .unwrap_or(0);
    }
}

/// Queue length, so a test can count what the sweeper asked for.
async fn queue_len(pool: &deadpool_redis::Pool) -> usize {
    let mut conn = pool.get().await.expect("conn");
    let n: usize = deadpool_redis::redis::cmd("XLEN")
        .arg("queue:lens")
        .query_async(&mut conn)
        .await
        .unwrap_or(0);
    n
}

#[tokio::test]
async fn a_viewers_own_change_becomes_exactly_the_rebuilds_it_should() {
    let Some(url) = url() else {
        eprintln!("skipping: TEST_REDIS_URL unset");
        return;
    };
    let pool = pool(&url).await;
    let applier = DeltaApplier::new(pool.clone(), Metrics::new(), 604_800);

    // --- an idle sweep is silent and free -------------------------------------
    // It runs every 30s forever, so doing nothing has to cost nothing.
    cleanup(&pool).await;
    applier.sweep().await;
    assert_eq!(queue_len(&pool).await, 0, "no work, no queue entries");

    // --- only the facets the viewer HAS --------------------------------------
    // Enqueuing a facet nobody asked for pays a ClickHouse query — a 500k-row one
    // for `community` — to build a blob no feed will read.
    cleanup(&pool).await;
    let mut conn = pool.get().await.expect("conn");
    let _: () = conn
        .set(format!("lens:v2:follows:{VIEWER}"), "x")
        .await
        .expect("seed");
    let _: () = conn
        .set(format!("lens:v2:niche:{VIEWER}"), "x")
        .await
        .expect("seed");
    let _: i64 = conn.sadd("lens:dirty", VIEWER).await.expect("mark");

    applier.sweep().await;
    assert_eq!(
        queue_len(&pool).await,
        2,
        "one rebuild per EXISTING facet, not one per facet that could exist"
    );
    assert_eq!(
        conn.scard::<_, usize>("lens:dirty").await.expect("scard"),
        0,
        "a swept viewer must not stay dirty"
    );

    // --- many marks coalesce into one rebuild --------------------------------
    // The property that makes it safe to mark on every event.
    cleanup(&pool).await;
    let _: () = conn
        .set(format!("lens:v2:follows:{VIEWER}"), "x")
        .await
        .expect("seed");
    for _ in 0..50 {
        let _: i64 = conn.sadd("lens:dirty", VIEWER).await.expect("mark");
    }
    assert_eq!(
        conn.scard::<_, usize>("lens:dirty").await.expect("scard"),
        1,
        "a set dedupes by construction"
    );
    applier.sweep().await;
    assert_eq!(
        queue_len(&pool).await,
        1,
        "50 follows must cost one rebuild, not 50"
    );

    // --- a viewer with no blobs costs nothing, and is still drained ----------
    // Someone outside the cohort can be marked (active once, or the active list
    // is mid-refresh). They must not cause builds, and must not be retried forever.
    cleanup(&pool).await;
    let _: i64 = conn.sadd("lens:dirty", VIEWER).await.expect("mark");
    applier.sweep().await;
    assert_eq!(queue_len(&pool).await, 0, "no blobs, no rebuilds");
    assert_eq!(
        conn.scard::<_, usize>("lens:dirty").await.expect("scard"),
        0,
        "still drained, so we do not retry them forever"
    );

    cleanup(&pool).await;
}
