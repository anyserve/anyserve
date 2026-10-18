pub mod auth;
pub mod frame;
pub mod kernel;
pub mod model;
pub mod notify;
pub mod scheduler;
pub mod service;
pub mod sql_store;
pub mod store;

/// Milliseconds since the Unix epoch for persisted record timestamps.
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}
