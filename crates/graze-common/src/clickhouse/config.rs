//! Shared ClickHouse connection configuration.
//!
//! Used by both the writer (interaction inserts) and the reader (candidate queries).

/// Shared ClickHouse configuration for HTTP interface.
///
/// Used when `INTERACTIONS_WRITER=clickhouse` or `CANDIDATE_SOURCE=clickhouse`.
#[derive(Debug, Clone)]
pub struct ClickHouseConfig {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub user: String,
    pub password: String,
    pub secure: bool,
}

impl ClickHouseConfig {
    /// Base URL for the ClickHouse HTTP interface (e.g. `http://localhost:8123`).
    pub fn base_url(&self) -> String {
        let protocol = if self.secure { "https" } else { "http" };
        format!("{}://{}:{}", protocol, self.host, self.port)
    }
}
