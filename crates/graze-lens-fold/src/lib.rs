//! Tails the Bluesky follow graph into `follow_edges`.
//!
//! One websocket, one ClickHouse table. Everything downstream (lens builds,
//! the serve path) reads that table; nothing reads this process. It is a
//! singleton by deployment — two consumers would double-write, which the
//! ReplacingMergeTree fold makes harmless but wasteful.

pub mod config;
pub mod cursor;
pub mod event;
pub mod metrics;
pub mod rev;
pub mod sink;
pub mod streamer;

pub use config::Config;
pub use cursor::Cursor;
pub use event::{parse, FollowEdge};
pub use metrics::Metrics;
pub use sink::Sink;
pub use streamer::Streamer;
