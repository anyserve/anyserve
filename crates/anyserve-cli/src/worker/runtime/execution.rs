use std::collections::HashMap;

use anyhow::{Context, Result, bail};
use anyserve_client::execution::{Attempt, AttemptClient, v2};
use futures::StreamExt;
use reqwest::header::{CONTENT_TYPE, HeaderName, HeaderValue};
use tokio::sync::watch;

use super::WorkerContext;
use crate::worker::config::KIND_HTTP;
use crate::worker::helpers::truncate;

pub(crate) const FORWARD_HEADER_PREFIX: &str = "http.header.";
pub(crate) const HTTP_STATUS_KEY: &str = "http_status";
pub(crate) const CONTENT_TYPE_KEY: &str = "content_type";
const MAX_RESPONSE_BYTES: usize = 16 * 1024 * 1024;

pub(super) async fn run_attempt(
    context: WorkerContext,
    mut data: AttemptClient,
    assignment: v2::Assign,
    limits: v2::Limits,
    heartbeat_timeout: std::time::Duration,
    mut cancelled: watch::Receiver<bool>,
) -> Result<()> {
    // Dropping the request future aborts the HTTP body and releases its socket.
    // The control loop sends CancelAck only after this future returns.
    tokio::select! {
        biased;
        _ = cancellation(&mut cancelled) => Ok(()),
        result = execute(&context, &mut data, &assignment, &limits, heartbeat_timeout) => result,
    }
}

async fn cancellation(cancelled: &mut watch::Receiver<bool>) {
    loop {
        if *cancelled.borrow_and_update() {
            return;
        }
        if cancelled.changed().await.is_err() {
            return;
        }
    }
}

async fn execute(
    context: &WorkerContext,
    data: &mut AttemptClient,
    assignment: &v2::Assign,
    limits: &v2::Limits,
    heartbeat_timeout: std::time::Duration,
) -> Result<()> {
    let mut attempt = Attempt::open(data, assignment, limits, heartbeat_timeout).await?;
    if attempt.terminal_replay {
        return Ok(());
    }
    let result = request(context, assignment, &mut attempt).await;
    let (outputs, outcome) = match result {
        Ok(outputs) => (outputs, v2::finish::Outcome::Succeeded(v2::Succeeded {})),
        Err(error) => (Vec::new(), v2::finish::Outcome::Failed(error)),
    };
    attempt
        .commit(data, assignment, limits, &outputs, outcome)
        .await?;
    Ok(())
}

async fn request(
    context: &WorkerContext,
    assignment: &v2::Assign,
    attempt: &mut Attempt,
) -> Result<Vec<v2::Object>, v2::ErrorInfo> {
    let config = &context.config;
    let path = config
        .routes
        .get(&assignment.interface)
        .ok_or_else(|| failed("no route for assigned interface", false))?;
    let url = join_url(&config.base_url, path);
    let input = attempt
        .inputs
        .first()
        .ok_or_else(|| failed("HTTP worker requires inline input", false))?;
    let Some(v2::object::Body::Inline(body)) = &input.body else {
        return Err(failed("HTTP worker requires inline input", false));
    };
    let content_type = input
        .metadata
        .get(CONTENT_TYPE_KEY)
        .map(String::as_str)
        .unwrap_or("application/json");
    let mut upstream = context
        .http
        .post(&url)
        .header(CONTENT_TYPE, content_type)
        .header("x-anyserve-job-id", &assignment.job_id)
        .body(body.clone());
    for (name, value) in &input.metadata {
        if let Some(name) = name.strip_prefix(FORWARD_HEADER_PREFIX)
            && let (Ok(name), Ok(value)) = (
                HeaderName::from_bytes(name.as_bytes()),
                HeaderValue::from_str(value),
            )
        {
            upstream = upstream.header(name, value);
        }
    }
    let response = upstream
        .send()
        .await
        .map_err(|error| failed(format!("upstream request failed: {error}"), true))?;
    let status = response.status();
    if config.retryable_statuses.contains(&status.as_u16())
        || (!status.is_success() && config.kind != KIND_HTTP)
    {
        let retryable = config.retryable_statuses.contains(&status.as_u16());
        let body = read_bounded(response, 1024).await.unwrap_or_default();
        return Err(failed(
            format!(
                "upstream returned {status}: {}",
                truncate(&String::from_utf8_lossy(&body), 512)
            ),
            retryable,
        ));
    }
    let content_type = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("application/octet-stream")
        .to_owned();
    let metadata = HashMap::from([
        (CONTENT_TYPE_KEY.to_owned(), content_type.clone()),
        (HTTP_STATUS_KEY.to_owned(), status.as_u16().to_string()),
    ]);
    if content_type.contains("text/event-stream") {
        let mut body = response.bytes_stream();
        let mut buffered = Vec::new();
        let mut final_marker = DoneMarker::default();
        while let Some(chunk) = body.next().await {
            let chunk =
                chunk.map_err(|error| failed(format!("read upstream stream: {error}"), true))?;
            final_marker.push(&chunk);
            if config.kind == KIND_HTTP {
                if buffered.len().saturating_add(chunk.len()) > MAX_RESPONSE_BYTES {
                    return Err(failed("upstream response exceeds inline limit", false));
                }
                buffered.extend_from_slice(&chunk);
            }
            attempt
                .frame(chunk, metadata.clone(), false)
                .await
                .map_err(|error| failed(format!("send output: {error}"), true))?;
        }
        if config.kind != KIND_HTTP {
            if !final_marker.finish() {
                return Err(failed(
                    "upstream closed without the OpenAI [DONE] marker",
                    true,
                ));
            }
            return Ok(Vec::new());
        }
        Ok(vec![v2::Object {
            body: Some(v2::object::Body::Inline(buffered.into())),
            metadata,
        }])
    } else {
        let body = read_bounded(response, MAX_RESPONSE_BYTES)
            .await
            .map_err(|error| failed(format!("read upstream body: {error}"), true))?;
        Ok(vec![v2::Object {
            body: Some(v2::object::Body::Inline(body.into())),
            metadata,
        }])
    }
}

async fn read_bounded(response: reqwest::Response, limit: usize) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.context("read response chunk")?;
        if bytes.len().saturating_add(chunk.len()) > limit {
            bail!("upstream response exceeds {limit} bytes");
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

/// Recognise an SSE terminal line across arbitrary HTTP chunk boundaries.
/// Long non-terminal lines are skipped without buffering an entire event.
#[derive(Default)]
struct DoneMarker {
    line: Vec<u8>,
    overflow: bool,
    done: bool,
}
impl DoneMarker {
    fn push(&mut self, bytes: &[u8]) {
        for byte in bytes {
            if *byte == b'\n' {
                self.check_line();
                self.line.clear();
                self.overflow = false;
            } else if self.line.len() < 32 {
                self.line.push(*byte);
            } else {
                self.overflow = true;
            }
        }
    }
    fn check_line(&mut self) {
        self.done |= !self.overflow && self.line.trim_ascii() == b"data: [DONE]";
    }
    fn finish(mut self) -> bool {
        self.check_line();
        self.done
    }
}

fn failed(message: impl Into<String>, retryable: bool) -> v2::ErrorInfo {
    v2::ErrorInfo {
        code: "worker_failed".to_owned(),
        message: message.into(),
        retryable,
    }
}

fn join_url(base_url: &str, path: &str) -> String {
    if path.is_empty() {
        base_url.to_owned()
    } else {
        format!(
            "{}/{}",
            base_url.trim_end_matches('/'),
            path.trim_start_matches('/')
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{DoneMarker, join_url};
    #[test]
    fn sse_completion_handles_every_chunk_boundary_and_rejects_truncation() {
        let stream = b"data: {\"text\":\"data: [DONE]\"}\n\ndata: [DONE]\r\n\r\n";
        for split in 0..=stream.len() {
            let mut marker = DoneMarker::default();
            marker.push(&stream[..split]);
            marker.push(&stream[split..]);
            assert!(marker.finish());
        }
        let mut marker = DoneMarker::default();
        marker.push(b"data: {\"text\":\"data: [DONE]\"}\n\n");
        assert!(!marker.finish());
    }
    #[test]
    fn join_url_normalizes_slashes() {
        assert_eq!(join_url("http://a/v1/", "/chat"), "http://a/v1/chat");
        assert_eq!(
            join_url("http://a", "strategy_act"),
            "http://a/strategy_act"
        );
        assert_eq!(join_url("http://a/x", ""), "http://a/x");
    }
}
