use std::collections::HashMap;

use anyhow::{Context, Result};
use anyserve_client::{AnyserveClient, FrameWrite, StreamDirection, StreamOpen};
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue};

use super::config::WorkerConfig;

pub(crate) const DEFAULT_OUTPUT_STREAM: &str = "output.default";

pub(crate) fn build_http_client(config: &WorkerConfig) -> Result<reqwest::Client> {
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    if let Some(api_key) = config.api_key.as_ref() {
        let header = HeaderValue::from_str(&format!("Bearer {api_key}"))
            .context("build authorization header")?;
        headers.insert(AUTHORIZATION, header);
    }

    reqwest::Client::builder()
        .default_headers(headers)
        .tcp_nodelay(true)
        .pool_max_idle_per_host(config.max_active_leases as usize)
        .timeout(config.upstream_timeout)
        .build()
        .context("build HTTP client")
}

pub(crate) async fn connect_client(
    control_plane_endpoint: &str,
    config: &WorkerConfig,
) -> Result<AnyserveClient> {
    AnyserveClient::connect_with_token(control_plane_endpoint.to_string(), config.token.as_deref())
        .await
}

pub(crate) fn available_capacity(
    total: &HashMap<String, i64>,
    active_leases: u32,
) -> HashMap<String, i64> {
    let mut available = total.clone();
    if let Some(slots) = available.get_mut("slot") {
        *slots = (*slots - i64::from(active_leases)).max(0);
    }
    available
}

pub(crate) fn truncate(value: &str, limit: usize) -> String {
    let chars = value.chars().collect::<Vec<_>>();
    if chars.len() <= limit {
        return value.to_string();
    }
    chars.into_iter().take(limit).collect::<String>()
}

pub(crate) fn open_output_stream(
    job_id: String,
    worker_id: String,
    lease_id: String,
    attempt_id: String,
) -> StreamOpen {
    StreamOpen::job(
        job_id,
        DEFAULT_OUTPUT_STREAM,
        StreamDirection::WorkerToClient,
    )
    .with_worker_id(worker_id)
    .with_lease_id(lease_id)
    .with_attempt_id(attempt_id)
}

pub(crate) fn frame_write_with_content_type(chunk: Vec<u8>, content_type: String) -> FrameWrite {
    FrameWrite::data(chunk)
        .with_metadata(HashMap::from([("content_type".to_string(), content_type)]))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::{available_capacity, truncate};

    #[test]
    fn llm_worker_reduces_slot_capacity_for_active_lease() {
        let available = available_capacity(&HashMap::from([("slot".to_string(), 4)]), 2);
        assert_eq!(available.get("slot").copied(), Some(2));
    }

    #[test]
    fn truncate_keeps_short_values() {
        assert_eq!(truncate("hello", 10), "hello");
    }
}
