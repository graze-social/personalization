//! Redis client and utilities.

mod client;
mod config;
mod hash;
mod keys;
mod scripts;

pub use client::RedisClient;
pub use config::RedisConfig;
pub use hash::{hash_did, hash_uri};
pub use keys::{
    // New date-based functions
    date_from_timestamp,
    // Legacy (deprecated) - keep for migration
    day_offset_from_timestamp,
    retention_dates,
    today_date,
    ttl_for_date,
    ttl_for_day,
    // Key patterns
    Keys,
    DAY_TRANCHES,
    DEFAULT_RETENTION_DAYS,
};
pub use scripts::ScriptManager;
