//! Redis configuration.

/// Redis connection configuration.
#[derive(Debug, Clone)]
pub struct RedisConfig {
    /// Redis connection URL (e.g., "redis://localhost:6379").
    pub url: String,
    /// Maximum number of connections in the pool.
    pub pool_size: usize,
    /// Maximum number of connection retries on startup.
    pub connect_max_retries: u32,
    /// Initial delay between retries in milliseconds.
    pub connect_initial_delay_ms: u64,
}

impl Default for RedisConfig {
    fn default() -> Self {
        Self {
            url: "redis://localhost:6379".to_string(),
            pool_size: 100,
            connect_max_retries: 10,
            connect_initial_delay_ms: 500,
        }
    }
}
