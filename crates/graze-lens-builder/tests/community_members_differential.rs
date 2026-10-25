//! The per-community member path must return exactly what the old set query
//! did, byte for byte, for real community sets.
//!
//! The old query is frozen here as the golden: one `account_stats INNER JOIN
//! community_members WHERE community IN (...)` per viewer, retired 2026-10-25
//! because it ran ~16k times a night at 1.57 GiB each. The new path asks per
//! community and merges, so it can be cached by community; this test runs
//! both against a live ClickHouse and compares the TSV the scorer would see.
//!
//! Env-gated like the other live-data tests in this crate:
//!
//!   CLICKHOUSE_HOST=... CLICKHOUSE_USER=... CLICKHOUSE_PASSWORD=... \
//!   COMMUNITY_SETS="27954532,40492306,41952164;3,40492306,41952164" \
//!   cargo test -p graze-lens-builder --test community_members_differential -- --nocapture

use graze_lens_builder::priors;

const CAP: usize = 65_536;

fn golden_query(database: &str, communities: &[u32]) -> String {
    let list = communities
        .iter()
        .map(|c| c.to_string())
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "SELECT m.account, m.community, s.followers \
         FROM {database}.account_stats AS s \
         INNER JOIN ( \
             SELECT account, community FROM {database}.community_members \
             WHERE community IN ({list}) \
         ) AS m ON s.account_int = m.account \
         ORDER BY s.followers DESC, m.account ASC \
         LIMIT {CAP} \
         FORMAT TabSeparated"
    )
}

async fn run(http: &reqwest::Client, url: &str, user: &str, pass: &str, sql: String) -> String {
    let r = http
        .post(url)
        .basic_auth(user, Some(pass))
        .query(&[("log_comment", "community-members-differential")])
        .body(sql)
        .send()
        .await
        .expect("send");
    let status = r.status();
    let text = r.text().await.expect("body");
    assert!(status.is_success(), "query failed ({status}): {text}");
    text
}

#[tokio::test]
async fn per_community_merge_matches_the_set_query() {
    let (Ok(host), Ok(user), Ok(pass), Ok(sets)) = (
        std::env::var("CLICKHOUSE_HOST"),
        std::env::var("CLICKHOUSE_USER"),
        std::env::var("CLICKHOUSE_PASSWORD"),
        std::env::var("COMMUNITY_SETS"),
    ) else {
        eprintln!("skipping: CLICKHOUSE_HOST / USER / PASSWORD / COMMUNITY_SETS unset");
        return;
    };
    let database = std::env::var("CLICKHOUSE_DATABASE").unwrap_or_else(|_| "default".into());
    let url = format!("https://{host}:8443/");
    let http = reqwest::Client::new();

    for set in sets.split(';').filter(|s| !s.trim().is_empty()) {
        let communities: Vec<u32> = set
            .split(',')
            .map(|c| c.trim().parse().expect("community id"))
            .collect();
        let golden = run(
            &http,
            &url,
            &user,
            &pass,
            golden_query(&database, &communities),
        )
        .await;

        let mut lists = Vec::new();
        for &c in &communities {
            let sql = priors::community_top_members_query(&database, c, CAP);
            let text = run(&http, &url, &user, &pass, sql).await;
            lists.push(priors::parse_community_top_tsv(&text, c));
        }
        let lists: Vec<&[priors::MemberRow]> = lists.iter().map(|l| l.as_slice()).collect();
        let merged: String = priors::merge_members(&lists, CAP)
            .iter()
            .map(|r| format!("{}\t{}\t{}\n", r.account, r.community, r.followers))
            .collect();

        println!(
            "{set}: golden {} rows, merged {} rows, identical={}",
            golden.lines().count(),
            merged.lines().count(),
            golden == merged
        );
        assert_eq!(golden, merged, "community set {set}");
    }
}
