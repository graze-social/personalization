//! Label propagation: coarse community detection over the follow graph.
//!
//! Louvain proper does not fit this substrate — it needs random-access mutation
//! of a modularity state no SQL engine will hold for 42M nodes. Label
//! propagation converges to the same coarse communities and is expressible as
//! the one thing ClickHouse is unbeatable at: iterated join + group-by. Each
//! round, every account adopts the most common label among its sampled
//! neighbours; a handful of rounds and the labels stop moving.
//!
//! # Sampling is load-bearing, not a shortcut
//!
//! The full graph is 2.77B directed edges — 5.5B once both directions count as
//! adjacency. A per-round vote over that would fight the 21.6 GiB memory
//! ceiling this cluster has already enforced three times. Sampling edges by
//! hash keeps rounds bounded, and community detection is exactly the workload
//! where that is safe: communities are a *coarse* structure, and a uniform
//! sample preserves coarse structure. (The same argument does NOT hold for
//! reach, where every edge is one unit of the answer.)
//!
//! # The degenerate outcome is a real risk
//!
//! Batch LPA on social graphs can collapse into one giant community. That
//! failure is not an error anywhere — it just makes a "my communities" lens
//! mean "everyone". So the swap refuses a result whose largest community
//! exceeds a share of all accounts, and the previous labels stay live.

use std::time::Duration;

use anyhow::Context;
use graze_common::ClickHouseConfig;
use tracing::{info, warn};

pub const COMMUNITY_TABLE: &str = "account_community";
pub const COMMUNITY_STAGING: &str = "account_community_next";
pub const MEMBERS_TABLE: &str = "community_members";
pub const MEMBERS_STAGING: &str = "community_members_next";

/// Working tables, dropped at the end of a run.
const EDGES: &str = "lpa_edges";
const LABELS: &str = "lpa_labels";
const LABELS_NEXT: &str = "lpa_labels_next";
const VOTES: &str = "lpa_votes";
const TOP: &str = "lpa_top";

pub struct LpaConfig {
    pub clickhouse: ClickHouseConfig,
    pub timeout: Duration,
    pub max_execution_seconds: u64,
    /// Percent of edges sampled into the adjacency, 1..=100.
    pub sample_pct: u8,
    /// Voting rounds. LPA converges fast; past ~5 the labels barely move.
    pub iterations: u32,
    /// Refuse to swap if the largest community holds more than this share of
    /// all accounts — the collapse guard.
    pub max_dominant_share: f64,
}

pub struct Lpa {
    http: reqwest::Client,
    cfg: LpaConfig,
}

pub struct LpaReport {
    pub accounts: u64,
    pub communities: u64,
    pub largest: u64,
}

impl Lpa {
    pub fn new(cfg: LpaConfig) -> anyhow::Result<Self> {
        Ok(Self {
            http: reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(10))
                .build()?,
            cfg,
        })
    }

    pub async fn run(&self) -> anyhow::Result<LpaReport> {
        let db = &self.cfg.clickhouse.database.clone();
        let pct = self.cfg.sample_pct.clamp(1, 100);

        // Adjacency sample, both directions. Follow direction is meaningless
        // for "are these accounts in the same social cluster", so the graph is
        // treated as undirected by inserting each sampled edge both ways.
        // Ordered by src for the per-round join.
        self.exec(&format!("DROP TABLE IF EXISTS {db}.{EDGES}"))
            .await?;
        self.exec(&format!(
            "CREATE TABLE {db}.{EDGES} ENGINE = MergeTree ORDER BY src AS
             SELECT follower_int AS src, followee_int AS dst FROM {db}.follow_graph_int
             WHERE cityHash64(follower_int, followee_int) % 100 < {pct}
             UNION ALL
             SELECT followee_int, follower_int FROM {db}.follow_graph_int
             WHERE cityHash64(follower_int, followee_int) % 100 < {pct}
             SETTINGS max_threads = 4"
        ))
        .await
        .context("sampling adjacency")?;
        let sampled = self.count(EDGES).await?;
        info!(sampled, pct, "adjacency sampled");

        // Every account starts as its own community. Seeded from account_stats
        // (already one row per account) rather than re-deriving the node set.
        self.exec(&format!("DROP TABLE IF EXISTS {db}.{LABELS}"))
            .await?;
        self.exec(&format!(
            "CREATE TABLE {db}.{LABELS} ENGINE = MergeTree ORDER BY account AS
             SELECT account_int AS account, account_int AS label FROM {db}.account_stats"
        ))
        .await
        .context("seeding labels")?;

        for round in 1..=self.cfg.iterations {
            // Votes: for each account, how many sampled neighbours carry each
            // label. Bounded by the sample size, and spilled to disk past the
            // group-by budget — this is the round's heavy step.
            self.exec(&format!("DROP TABLE IF EXISTS {db}.{VOTES}"))
                .await?;
            self.exec(&format!(
                "CREATE TABLE {db}.{VOTES} ENGINE = MergeTree ORDER BY account AS
                 SELECT e.src AS account, l.label AS label, count() AS cnt
                 FROM {db}.{EDGES} AS e
                 INNER JOIN {db}.{LABELS} AS l ON e.dst = l.account
                 GROUP BY e.src, l.label
                 SETTINGS join_algorithm = 'grace_hash', max_threads = 4,
                          max_bytes_before_external_group_by = 8000000000"
            ))
            .await
            .with_context(|| format!("votes, round {round}"))?;

            // Majority label per account, ties broken by the label value so a
            // rerun of the same round gives the same answer.
            self.exec(&format!("DROP TABLE IF EXISTS {db}.{TOP}"))
                .await?;
            self.exec(&format!(
                "CREATE TABLE {db}.{TOP} ENGINE = MergeTree ORDER BY account AS
                 SELECT account, argMax(label, (cnt, label)) AS label
                 FROM {db}.{VOTES} GROUP BY account
                 SETTINGS max_threads = 4,
                          max_bytes_before_external_group_by = 8000000000"
            ))
            .await
            .with_context(|| format!("majority, round {round}"))?;

            // Accounts with no sampled neighbours keep their label. Interner
            // ids start at 1, so label 0 can only mean "no vote row joined".
            self.exec(&format!("DROP TABLE IF EXISTS {db}.{LABELS_NEXT}"))
                .await?;
            self.exec(&format!(
                "CREATE TABLE {db}.{LABELS_NEXT} ENGINE = MergeTree ORDER BY account AS
                 SELECT l.account AS account, if(t.label = 0, l.label, t.label) AS label
                 FROM {db}.{LABELS} AS l
                 ANY LEFT JOIN {db}.{TOP} AS t ON l.account = t.account
                 SETTINGS join_algorithm = 'grace_hash', max_threads = 4"
            ))
            .await
            .with_context(|| format!("carry-forward, round {round}"))?;

            self.exec(&format!("DROP TABLE {db}.{LABELS}")).await?;
            self.exec(&format!("RENAME TABLE {db}.{LABELS_NEXT} TO {db}.{LABELS}"))
                .await?;

            let communities = self
                .scalar(&format!(
                    "SELECT uniqExact(label) FROM {db}.{LABELS} FORMAT TabSeparated"
                ))
                .await?;
            info!(round, communities, "round complete");
        }

        let accounts = self.count(LABELS).await?;
        let communities = self
            .scalar(&format!(
                "SELECT uniqExact(label) FROM {db}.{LABELS} FORMAT TabSeparated"
            ))
            .await?;
        let largest = self
            .scalar(&format!(
                "SELECT max(c) FROM (SELECT count() AS c FROM {db}.{LABELS} GROUP BY label) \
                 FORMAT TabSeparated"
            ))
            .await?;

        let share = largest as f64 / accounts.max(1) as f64;
        if share > self.cfg.max_dominant_share {
            // Leave the previous labels serving; a collapsed run must not
            // replace a good one. Cleanup still happens below.
            self.cleanup(db).await;
            anyhow::bail!(
                "refusing to swap: largest community holds {:.1}% of {accounts} accounts \
                 (ceiling {:.0}%) — batch LPA collapse",
                share * 100.0,
                self.cfg.max_dominant_share * 100.0
            );
        }

        // Publish: account → community, and the reverse ordering the community
        // facet reads members from.
        self.exec(&format!("TRUNCATE TABLE {db}.{COMMUNITY_STAGING}"))
            .await?;
        self.exec(&format!(
            "INSERT INTO {db}.{COMMUNITY_STAGING} (account, community)
             SELECT account, label FROM {db}.{LABELS}"
        ))
        .await?;
        self.exec(&format!(
            "EXCHANGE TABLES {db}.{COMMUNITY_TABLE} AND {db}.{COMMUNITY_STAGING}"
        ))
        .await?;
        self.exec(&format!("TRUNCATE TABLE {db}.{COMMUNITY_STAGING}"))
            .await?;

        self.exec(&format!("TRUNCATE TABLE {db}.{MEMBERS_STAGING}"))
            .await?;
        self.exec(&format!(
            "INSERT INTO {db}.{MEMBERS_STAGING} (community, account)
             SELECT community, account FROM {db}.{COMMUNITY_TABLE}"
        ))
        .await?;
        self.exec(&format!(
            "EXCHANGE TABLES {db}.{MEMBERS_TABLE} AND {db}.{MEMBERS_STAGING}"
        ))
        .await?;
        self.exec(&format!("TRUNCATE TABLE {db}.{MEMBERS_STAGING}"))
            .await?;

        self.cleanup(db).await;
        Ok(LpaReport {
            accounts,
            communities,
            largest,
        })
    }

    async fn cleanup(&self, db: &str) {
        for t in [EDGES, LABELS, LABELS_NEXT, VOTES, TOP] {
            if let Err(e) = self.exec(&format!("DROP TABLE IF EXISTS {db}.{t}")).await {
                warn!(table = t, error = %e, "cleanup failed; drop it by hand");
            }
        }
    }

    async fn count(&self, table: &str) -> anyhow::Result<u64> {
        let db = &self.cfg.clickhouse.database;
        self.scalar(&format!(
            "SELECT count() FROM {db}.{table} FORMAT TabSeparated"
        ))
        .await
    }

    async fn scalar(&self, sql: &str) -> anyhow::Result<u64> {
        Ok(self.exec(sql).await?.trim().parse().unwrap_or(0))
    }

    async fn exec(&self, sql: &str) -> anyhow::Result<String> {
        let response = self
            .http
            .post(self.cfg.clickhouse.base_url())
            .basic_auth(
                &self.cfg.clickhouse.user,
                Some(&self.cfg.clickhouse.password),
            )
            .header("Content-Type", "text/plain")
            .timeout(self.cfg.timeout)
            .query(&[(
                "max_execution_time",
                self.cfg.max_execution_seconds.to_string(),
            )])
            .body(sql.to_string())
            .send()
            .await?;
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        if !status.is_success() {
            anyhow::bail!(
                "lpa query failed ({status}): {}\nSQL: {}",
                &text[..text.len().min(400)],
                &sql[..sql.len().min(300)]
            );
        }
        Ok(text)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The staging/live pairs must be distinct names, and the members table
    /// must be the reverse ordering of the community table — the facet reads
    /// members BY community, which the account-ordered table cannot serve.
    #[test]
    fn table_names_are_coherent() {
        assert_ne!(COMMUNITY_TABLE, COMMUNITY_STAGING);
        assert_ne!(MEMBERS_TABLE, MEMBERS_STAGING);
        assert_ne!(COMMUNITY_TABLE, MEMBERS_TABLE);
    }
}
