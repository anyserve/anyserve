use std::collections::HashMap;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use anyserve_client::{
    AnyserveClient, FrameKind, FrameWrite, ObjectRef, StreamDirection, StreamOpen, StreamState,
    object_ref,
};
use futures::StreamExt;
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue};

use super::config::ResolvedServeOpenAIWorkerConfig;

pub(crate) const DEFAULT_OUTPUT_STREAM: &str = "output.default";
pub(crate) const DEFAULT_INPUT_STREAM: &str = "input.default";

pub(crate) fn build_http_client(
    config: &ResolvedServeOpenAIWorkerConfig,
) -> Result<reqwest::Client> {
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    if let Some(api_key) = config.api_key.as_ref() {
        let header = HeaderValue::from_str(&format!("Bearer {api_key}"))
            .context("build authorization header")?;
        headers.insert(AUTHORIZATION, header);
    }

    reqwest::Client::builder()
        .default_headers(headers)
        .timeout(config.upstream_timeout)
        .build()
        .context("build HTTP client")
}

pub(crate) async fn connect_client(control_plane_endpoint: &str) -> Result<AnyserveClient> {
    AnyserveClient::connect(control_plane_endpoint.to_string()).await
}

pub(crate) async fn read_request_body(
    client: &mut AnyserveClient,
    job_id: &str,
    params: &[u8],
    input_stream_wait: Duration,
) -> Result<Vec<u8>> {
    if !params.is_empty() {
        return Ok(params.to_vec());
    }

    let deadline = tokio::time::Instant::now() + input_stream_wait;
    loop {
        let streams = client.list_streams(job_id.to_string()).await?;
        if let Some(input_stream) = streams
            .into_iter()
            .find(|stream| stream.stream_name == DEFAULT_INPUT_STREAM)
        {
            if matches!(
                input_stream.state(),
                StreamState::Closed | StreamState::Error
            ) {
                let mut frames = client.pull_frames(input_stream.stream_id, 0, false).await?;
                let mut body = Vec::new();
                while let Some(frame) = frames.next().await {
                    let frame = frame.context("read input frame")?;
                    if frame.kind() == FrameKind::Data {
                        body.extend(frame.payload);
                    }
                }
                if body.is_empty() {
                    bail!("job '{job_id}' input.default closed without body");
                }
                return Ok(body);
            }
        }

        if tokio::time::Instant::now() >= deadline {
            bail!("job '{job_id}' timed out waiting for complete input.default stream");
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

pub(crate) fn upstream_path(interface_name: &str) -> Result<&'static str> {
    match interface_name {
        "llm.chat.v1" => Ok("chat/completions"),
        "llm.embed.v1" | "llm.embeddings.v1" => Ok("embeddings"),
        other => bail!("unsupported llm worker interface '{other}'"),
    }
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

pub(crate) fn persisted_outputs(content_type: &str, payload: Vec<u8>) -> Vec<ObjectRef> {
    if payload.is_empty() {
        return Vec::new();
    }

    vec![ObjectRef {
        reference: Some(object_ref::Reference::Inline(payload)),
        metadata: HashMap::from([("content_type".to_string(), content_type.to_string())]),
    }]
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
