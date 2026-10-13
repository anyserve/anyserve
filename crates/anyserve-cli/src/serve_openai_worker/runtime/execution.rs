use std::collections::HashMap;

use anyserve_client::{
    AnyserveClient, EventKind, FrameKind, FrameWrite, LeaseGrant, ObjectRef, object_ref,
};
use futures::StreamExt;
use reqwest::header::{CONTENT_TYPE, HeaderName, HeaderValue};
use tokio_stream::wrappers::ReceiverStream;

use super::WorkerContext;
use crate::serve_openai_worker::config::KIND_HTTP;
use crate::serve_openai_worker::helpers::{
    DEFAULT_OUTPUT_STREAM, frame_write_with_content_type, open_output_stream, read_request_body,
    truncate, upstream_path,
};

/// Input metadata keys with this prefix are forwarded to the upstream as headers.
pub(crate) const FORWARD_HEADER_PREFIX: &str = "http.header.";
pub(crate) const HTTP_STATUS_KEY: &str = "http_status";
pub(crate) const CONTENT_TYPE_KEY: &str = "content_type";

#[derive(Debug)]
pub(crate) struct ExecutionError {
    pub(crate) message: String,
    /// Whether another worker (possibly in another cluster) should retry the job.
    pub(crate) retryable: bool,
}

impl ExecutionError {
    fn retryable(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            retryable: true,
        }
    }

    fn fatal(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            retryable: false,
        }
    }
}

impl From<anyhow::Error> for ExecutionError {
    fn from(error: anyhow::Error) -> Self {
        Self::fatal(format!("{error:#}"))
    }
}

struct RequestBody {
    body: Vec<u8>,
    content_type: String,
    forward_headers: Vec<(String, String)>,
}

/// Runs one leased job against the local upstream.
///
/// The common case costs two control-plane round trips (the poll that delivered
/// the job and CompleteLease): the request arrives inline and the response goes
/// back inline. Only event-stream responses open an output stream, and those
/// frames are pipelined over one PushFrames call.
pub(super) async fn process_grant(
    context: &WorkerContext,
    client: &mut AnyserveClient,
    grant: LeaseGrant,
) -> Result<(), ExecutionError> {
    let config = &context.config;
    let job = grant.job;
    let lease_id = grant.lease.lease_id.clone();
    let attempt_id = grant.attempt.attempt_id.clone();
    let spec = job
        .spec
        .clone()
        .ok_or_else(|| ExecutionError::fatal("job is missing spec"))?;

    let url = if config.kind == KIND_HTTP {
        let path = config.routes.get(&spec.interface_name).ok_or_else(|| {
            ExecutionError::fatal(format!("no route for interface '{}'", spec.interface_name))
        })?;
        join_url(&config.base_url, path)
    } else {
        let path = upstream_path(&spec.interface_name)?;
        join_url(&config.base_url, path)
    };

    let request = load_request(client, &job.job_id, &spec, config.input_stream_wait).await?;
    report_started(context, client, &lease_id);

    let mut upstream = context
        .http_client
        .post(&url)
        .header(CONTENT_TYPE, request.content_type.as_str())
        .header("x-anyserve-job-id", job.job_id.as_str())
        .body(request.body);
    for (name, value) in &request.forward_headers {
        if let (Ok(name), Ok(value)) = (
            HeaderName::from_bytes(name.as_bytes()),
            HeaderValue::from_str(value),
        ) {
            upstream = upstream.header(name, value);
        }
    }
    let response = upstream.send().await.map_err(|error| {
        // Connection refused / timeout: let a healthy worker elsewhere retry.
        ExecutionError::retryable(format!("upstream request to {url} failed: {error}"))
    })?;

    let status = response.status();
    let content_type = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("application/octet-stream")
        .to_string();

    if config.retryable_statuses.contains(&status.as_u16()) {
        let body = response.text().await.unwrap_or_default();
        return Err(ExecutionError::retryable(format!(
            "upstream returned {status}: {}",
            truncate(&body, 512)
        )));
    }
    // LLM workers treat any non-2xx as failure (OpenAI gateway semantics);
    // HTTP workers pass upstream errors back to the caller verbatim.
    if !status.is_success() && config.kind != KIND_HTTP {
        let body = response.text().await.unwrap_or_default();
        return Err(ExecutionError::fatal(format!(
            "upstream returned {status}: {}",
            truncate(&body, 512)
        )));
    }

    let output_metadata = HashMap::from([
        (CONTENT_TYPE_KEY.to_string(), content_type.clone()),
        (HTTP_STATUS_KEY.to_string(), status.as_u16().to_string()),
    ]);

    let payload = if content_type.contains("text/event-stream") {
        stream_response(
            context,
            client,
            &job.job_id,
            &lease_id,
            &attempt_id,
            &content_type,
            response,
        )
        .await?
    } else {
        response
            .bytes()
            .await
            .map_err(|error| ExecutionError::retryable(format!("read upstream body: {error}")))?
            .to_vec()
    };

    client
        .complete_lease(
            context.worker_id.clone(),
            lease_id,
            vec![ObjectRef {
                reference: Some(object_ref::Reference::Inline(payload)),
                metadata: output_metadata.clone(),
            }],
            output_metadata,
        )
        .await?;
    Ok(())
}

async fn load_request(
    client: &mut AnyserveClient,
    job_id: &str,
    spec: &anyserve_client::controlplane::JobSpec,
    input_stream_wait: std::time::Duration,
) -> Result<RequestBody, ExecutionError> {
    if let Some(input) = spec.inputs.first()
        && let Some(object_ref::Reference::Inline(content)) = input.reference.as_ref()
    {
        let content_type = input
            .metadata
            .get(CONTENT_TYPE_KEY)
            .cloned()
            .unwrap_or_else(|| "application/json".to_string());
        let forward_headers = input
            .metadata
            .iter()
            .filter_map(|(key, value)| {
                key.strip_prefix(FORWARD_HEADER_PREFIX)
                    .map(|name| (name.to_string(), value.clone()))
            })
            .collect();
        return Ok(RequestBody {
            body: content.clone(),
            content_type,
            forward_headers,
        });
    }

    let body = read_request_body(client, job_id, &spec.params, input_stream_wait).await?;
    if body.is_empty() {
        return Err(ExecutionError::fatal(format!(
            "job '{job_id}' request body was empty"
        )));
    }
    Ok(RequestBody {
        body,
        content_type: "application/json".to_string(),
        forward_headers: Vec::new(),
    })
}

/// Best-effort Started event, sent off the critical path.
fn report_started(context: &WorkerContext, client: &AnyserveClient, lease_id: &str) {
    let mut client = client.clone();
    let worker_id = context.worker_id.clone();
    let lease_id = lease_id.to_string();
    let metadata = HashMap::from([
        ("provider".to_string(), context.config.provider.clone()),
        ("worker.kind".to_string(), context.config.kind.clone()),
        ("worker_id".to_string(), context.worker_id.clone()),
    ]);
    tokio::spawn(async move {
        let _ = client
            .report_event(
                worker_id,
                lease_id,
                EventKind::Started,
                Vec::new(),
                metadata,
            )
            .await;
    });
}

async fn stream_response(
    context: &WorkerContext,
    client: &mut AnyserveClient,
    job_id: &str,
    lease_id: &str,
    attempt_id: &str,
    content_type: &str,
    response: reqwest::Response,
) -> Result<Vec<u8>, ExecutionError> {
    let output_stream = client
        .open_stream(open_output_stream(
            job_id.to_string(),
            context.worker_id.clone(),
            lease_id.to_string(),
            attempt_id.to_string(),
        ))
        .await?;

    let (frames_tx, frames_rx) = tokio::sync::mpsc::channel::<FrameWrite>(256);
    let mut push_client = client.clone();
    let push_task = tokio::spawn({
        let stream_id = output_stream.stream_id.clone();
        let worker_id = context.worker_id.clone();
        let lease_id = lease_id.to_string();
        async move {
            push_client
                .push_frames_stream(
                    stream_id,
                    ReceiverStream::new(frames_rx),
                    Some(worker_id),
                    Some(lease_id),
                )
                .await
        }
    });

    let mut persisted = Vec::new();
    let mut body = response.bytes_stream();
    let mut stream_error = None;
    while let Some(chunk) = body.next().await {
        match chunk {
            Ok(chunk) if chunk.is_empty() => continue,
            Ok(chunk) => {
                persisted.extend_from_slice(&chunk);
                if frames_tx
                    .send(frame_write_with_content_type(
                        chunk.to_vec(),
                        content_type.to_string(),
                    ))
                    .await
                    .is_err()
                {
                    stream_error = Some("output frame pipeline closed".to_string());
                    break;
                }
            }
            Err(error) => {
                stream_error = Some(format!("read upstream event stream: {error}"));
                break;
            }
        }
    }

    let closing = match stream_error.as_ref() {
        Some(message) => FrameWrite::new(FrameKind::Error, message.clone().into_bytes()),
        None => FrameWrite::new(FrameKind::Close, Vec::new()).with_metadata(HashMap::from([(
            "stream_name".to_string(),
            DEFAULT_OUTPUT_STREAM.to_string(),
        )])),
    };
    let _ = frames_tx.send(closing).await;
    drop(frames_tx);
    push_task
        .await
        .map_err(|error| ExecutionError::fatal(format!("output push task panicked: {error}")))??;

    match stream_error {
        // Part of the answer already reached the caller, so do not retry.
        Some(message) => Err(ExecutionError::fatal(message)),
        None => Ok(persisted),
    }
}

fn join_url(base_url: &str, path: &str) -> String {
    if path.is_empty() {
        return base_url.to_string();
    }
    format!(
        "{}/{}",
        base_url.trim_end_matches('/'),
        path.trim_start_matches('/')
    )
}

#[cfg(test)]
mod tests {
    use super::join_url;

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
