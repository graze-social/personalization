//! The fold semantics, against a real ClickHouse.
//!
//! This is the highest-risk behaviour in graze-lens and the least visible when
//! wrong: if an unfollow fails to retract, nothing errors — lenses just quietly
//! keep serving people the viewer stopped following, forever.
//!
//! Run with a throwaway server:
//!
//! ```text
//! docker run -d --rm --name lens-ch -p 8124:8123 \
//!   -e CLICKHOUSE_USER=lens -e CLICKHOUSE_PASSWORD=lenspw \
//!   -e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 \
//!   clickhouse/clickhouse-server:24.8-alpine
//! TEST_CLICKHOUSE_URL=http://lens:lenspw@localhost:8124 cargo test -p graze-lens-fold --test fold_semantics
//! ```
//!
//! The local engine is `ReplacingMergeTree`; production is Cloud's
//! `SharedReplacingMergeTree`. The fold semantics under test are identical.

use graze_common::ClickHouseConfig;
use graze_lens_fold::{FollowEdge, Sink};
use std::time::Duration;

fn config() -> Option<ClickHouseConfig> {
    let raw = std::env::var("TEST_CLICKHOUSE_URL")
        .ok()
        .filter(|u| !u.is_empty())?;
    let url = reqwest::Url::parse(&raw).expect("TEST_CLICKHOUSE_URL must be a URL");
    Some(ClickHouseConfig {
        host: url.host_str().expect("host").to_string(),
        port: url.port().unwrap_or(8123),
        database: "default".into(),
        user: if url.username().is_empty() {
            "default".into()
        } else {
            url.username().into()
        },
        password: url.password().unwrap_or("").to_string(),
        secure: url.scheme() == "https",
    })
}

async fn exec(cfg: &ClickHouseConfig, sql: &str) -> String {
    let client = reqwest::Client::new();
    let response = client
        .post(cfg.base_url())
        .basic_auth(&cfg.user, Some(&cfg.password))
        .timeout(Duration::from_secs(30))
        .body(sql.to_string())
        .send()
        .await
        .expect("clickhouse request");
    let status = response.status();
    let text = response.text().await.unwrap_or_default();
    assert!(
        status.is_success(),
        "query failed ({status}): {text}\nSQL: {sql}"
    );
    text
}

/// The builder's production query, verbatim in shape.
fn builder_query(table: &str, follower: &str) -> String {
    format!(
        "SELECT followee FROM (SELECT followee, op FROM default.{table} FINAL \
         WHERE follower = '{follower}') WHERE op = 'create' AND followee != ''"
    )
}

fn edge(rkey: &str, followee: &str, op: &'static str, seq: u64) -> FollowEdge {
    FollowEdge {
        follower: "did:plc:testviewer".into(),
        rkey: rkey.into(),
        followee: followee.into(),
        op,
        seq,
        created_at: "2026-08-28 15:41:17.774".into(),
    }
}

/// An unfollow must retract the edge — even though the delete event carries no
/// followee, which is what forces the (follower, rkey) key.
#[tokio::test]
async fn an_unfollow_retracts_the_edge() {
    let Some(cfg) = config() else {
        eprintln!("skipping: TEST_CLICKHOUSE_URL unset");
        return;
    };
    let table = "fold_test_edges";

    exec(&cfg, &format!("DROP TABLE IF EXISTS default.{table}")).await;
    exec(
        &cfg,
        &format!(
            "CREATE TABLE default.{table} (follower String, rkey String, followee String, \
             op Enum8('create'=1,'delete'=2), seq UInt64, created_at DateTime64(3,'UTC')) \
             ENGINE = ReplacingMergeTree(seq) PARTITION BY cityHash64(follower) % 32 \
             ORDER BY (follower, rkey)"
        ),
    )
    .await;

    let sink = Sink::new_with_table(cfg.clone(), Duration::from_secs(30), table).expect("sink");

    // Two follows, then an unfollow of the first — the delete naming only rkey.
    sink.insert(&[
        edge("rkeyA", "did:plc:alice", "create", 1_000),
        edge("rkeyB", "did:plc:bob", "create", 2_000),
    ])
    .await
    .expect("insert creates");

    let before = exec(&cfg, &builder_query(table, "did:plc:testviewer")).await;
    assert!(before.contains("did:plc:alice"));
    assert!(before.contains("did:plc:bob"));

    sink.insert(&[edge("rkeyA", "", "delete", 3_000)])
        .await
        .expect("insert delete");

    let after = exec(&cfg, &builder_query(table, "did:plc:testviewer")).await;
    assert!(
        !after.contains("did:plc:alice"),
        "the unfollowed account is still being served: {after}"
    );
    assert!(
        after.contains("did:plc:bob"),
        "an unrelated follow was lost: {after}"
    );

    exec(&cfg, &format!("DROP TABLE default.{table}")).await;
}

/// The counterfactual, kept as a live demonstration rather than a comment: keyed
/// on (follower, followee) — as the design originally said — the delete and the
/// create have different sort keys, so nothing collapses and the unfollowed
/// account is served forever. If this test ever starts failing, the wire format
/// changed and deletes began naming their subject.
#[tokio::test]
async fn the_followee_keyed_schema_would_not_retract() {
    let Some(cfg) = config() else {
        eprintln!("skipping: TEST_CLICKHOUSE_URL unset");
        return;
    };
    let table = "fold_test_old_design";

    exec(&cfg, &format!("DROP TABLE IF EXISTS default.{table}")).await;
    exec(
        &cfg,
        &format!(
            "CREATE TABLE default.{table} (follower String, followee String, \
             op Enum8('create'=1,'delete'=2), seq UInt64, created_at DateTime64(3,'UTC')) \
             ENGINE = ReplacingMergeTree(seq) PARTITION BY toYYYYMM(created_at) \
             ORDER BY (follower, followee)"
        ),
    )
    .await;

    exec(
        &cfg,
        &format!(
            "INSERT INTO default.{table} VALUES \
             ('did:plc:testviewer','did:plc:alice','create',1000,'2026-08-28 15:41:17.774'), \
             ('did:plc:testviewer','','delete',3000,'2026-08-28 16:30:00.000')"
        ),
    )
    .await;

    let after = exec(&cfg, &builder_query(table, "did:plc:testviewer")).await;
    assert!(
        after.contains("did:plc:alice"),
        "the followee-keyed schema unexpectedly retracted the edge — if deletes now \
         carry a subject, the rkey keying could be revisited: {after}"
    );

    exec(&cfg, &format!("DROP TABLE default.{table}")).await;
}
