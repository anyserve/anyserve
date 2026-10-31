pub mod auth;
pub mod client_service;
pub mod error;
pub mod journal;
mod postgres;
pub mod runtime;
pub mod worker_service;

/// Milliseconds since the Unix epoch for persisted record timestamps.
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}
