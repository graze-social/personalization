//! Contract tests against a real Redis.
//!
//! These cover the two seams that unit tests cannot: the queue payload written
//! by feeder-rs in *another repo*, and the exact keys feeder-rs reads back. Both
//! sides are pinned by string equality in their own repos; these tests prove the
//! two halves actually meet in Redis.
//!
//! Skipped unless `TEST_REDIS_URL` is set, matching the house convention
//! (`membership-service/.github/workflows/ci.yml` runs a `redis:7-alpine`
//! service container and sets it).

use deadpool_redis::redis::AsyncCommands;
use deadpool_redis::{Config as RedisConfig, Runtime};
use graze_lens_builder::{Builder, Config, Queue};

fn redis_url() -> Option<String> {
    std::env::var("TEST_REDIS_URL")
        .ok()
        .filter(|u| !u.is_empty())
}

/// Build a config pointed at the test Redis. ClickHouse fields are present but
/// unused: these tests never call `build()`, only the Redis halves.
fn test_config(url: &str) -> Config {
    // Safety: tests in this file run single-threaded via `--test-threads` in CI;
    // each sets the same values, so a race would be benign anyway.
    unsafe {
        std::env::set_var("LENS_REDIS_URL", url);
        std::env::set_var("CLICKHOUSE_HOST", "unused.localhost");
        std::env::set_var("CLICKHOUSE_USER", "unused");
        std::env::set_var("CLICKHOUSE_PASSWORD", "unused");
    }
    Config::from_env().expect("config")
}

fn pool(url: &str, config: &Config) -> deadpool_redis::Pool {
    RedisConfig::from_url(url.to_string())
        .builder()
        .expect("pool builder")
        .max_size(config.redis_pool_size)
        .runtime(Runtime::Tokio1)
        .build()
        .expect("pool")
}

/// The full serve-path read: does a published set look the way feeder-rs
/// expects? feeder-rs pipelines `HGET lensmeta:{did} state` and
/// `SMEMBERS lens:v1:{facet}:{did}`, and requires state == "ready".
#[tokio::test]
async fn published_set_is_readable_the_way_the_feeder_reads_it() {
    let Some(url) = redis_url() else {
        eprintln!("skipping: TEST_REDIS_URL unset");
        return;
    };
    let config = test_config(&url);
    let pool = pool(&url, &config);
    let builder = Builder::new(pool.clone(), config).expect("builder");

    let viewer = "did:plc:integration-reader";
    let members: Vec<String> = ["did:plc:alice", "did:plc:bob", "did:plc:carol"]
        .iter()
        .map(|s| s.to_string())
        .collect();

    builder
        .publish(viewer, "follows", &members)
        .await
        .expect("publish");

    let mut conn = pool.get().await.expect("conn");
    let (state, found): (Option<String>, std::collections::HashSet<String>) =
        deadpool_redis::redis::pipe()
            .hget(format!("lensmeta:{viewer}"), "state")
            .smembers(format!("lens:v1:follows:{viewer}"))
            .query_async(&mut conn)
            .await
            .expect("read back");

    assert_eq!(state.as_deref(), Some("ready"));
    assert_eq!(found.len(), 3);
    assert!(found.contains("did:plc:alice"));

    // Both keys must expire; an immortal lens set would drift from the graph
    // forever once M0's TTL is the only freshness mechanism.
    let set_ttl: i64 = conn
        .ttl(format!("lens:v1:follows:{viewer}"))
        .await
        .expect("ttl");
    let meta_ttl: i64 = conn.ttl(format!("lensmeta:{viewer}")).await.expect("ttl");
    assert!(set_ttl > 0, "lens set must carry a TTL, got {set_ttl}");
    assert!(meta_ttl > 0, "lensmeta must carry a TTL, got {meta_ttl}");

    let _: () = conn.del(format!("lens:v1:follows:{viewer}")).await.unwrap();
    let _: () = conn.del(format!("lensmeta:{viewer}")).await.unwrap();
}

/// Republishing must replace the previous set, not union with it. The staging
/// key exists precisely to make this true; a plain SADD loop would leave
/// unfollowed accounts in the lens forever.
#[tokio::test]
async fn republish_replaces_rather_than_accumulates() {
    let Some(url) = redis_url() else {
        eprintln!("skipping: TEST_REDIS_URL unset");
        return;
    };
    let config = test_config(&url);
    let pool = pool(&url, &config);
    let builder = Builder::new(pool.clone(), config).expect("builder");

    let viewer = "did:plc:integration-republish";
    let first: Vec<String> = ["did:plc:alice", "did:plc:bob"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    let second: Vec<String> = ["did:plc:carol"].iter().map(|s| s.to_string()).collect();

    builder.publish(viewer, "follows", &first).await.unwrap();
    builder.publish(viewer, "follows", &second).await.unwrap();

    let mut conn = pool.get().await.expect("conn");
    let found: std::collections::HashSet<String> = conn
        .smembers(format!("lens:v1:follows:{viewer}"))
        .await
        .expect("smembers");

    assert_eq!(
        found.len(),
        1,
        "stale members survived a republish: {found:?}"
    );
    assert!(found.contains("did:plc:carol"));

    let _: () = conn.del(format!("lens:v1:follows:{viewer}")).await.unwrap();
    let _: () = conn.del(format!("lensmeta:{viewer}")).await.unwrap();
}

/// The cross-repo queue contract: an entry shaped exactly as feeder-rs writes it
/// (`XADD queue:lens MAXLEN ~ N * data <json>`) must be claimable here.
#[tokio::test]
async fn feeder_written_request_is_claimable() {
    let Some(url) = redis_url() else {
        eprintln!("skipping: TEST_REDIS_URL unset");
        return;
    };
    let config = test_config(&url);
    let pool = pool(&url, &config);

    // A unique group per run keeps concurrent test runs from stealing each
    // other's messages, since `>` delivers each entry to one consumer per group.
    let group = format!("test-builders-{}", std::process::id());
    let queue = Queue::new(pool.clone(), group.clone(), "test-consumer".into());
    queue.ensure_group().await.expect("ensure group");

    // Byte-for-byte what feeder-rs/src/lens.rs::enqueue_build sends.
    let payload = r#"{"viewer_did":"did:plc:queued","facet":"follows"}"#;
    let mut conn = pool.get().await.expect("conn");
    let _: String = deadpool_redis::redis::cmd("XADD")
        .arg("queue:lens")
        .arg("MAXLEN")
        .arg("~")
        .arg(100_000)
        .arg("*")
        .arg("data")
        .arg(payload)
        .query_async(&mut conn)
        .await
        .expect("xadd");

    let claimed = queue.read(10, 1_000).await.expect("read");
    let ours = claimed
        .iter()
        .find(|d| d.request.viewer_did == "did:plc:queued")
        .expect("our request was not delivered");
    assert_eq!(ours.request.facet, "follows");

    queue.ack(&ours.id).await.expect("ack");

    let _: () = deadpool_redis::redis::cmd("XGROUP")
        .arg("DESTROY")
        .arg("queue:lens")
        .arg(&group)
        .query_async(&mut conn)
        .await
        .unwrap_or(());
}
