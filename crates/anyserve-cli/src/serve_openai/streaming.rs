use std::io;
use std::time::Duration;

use anyserve_core::runtime::api::{EventKind, Execution, JobState, Object};
use axum::body::Body;
use axum::http::{StatusCode, header};
use axum::response::Response;
use futures::StreamExt;

use super::state::OpenAIError;

pub(super) async fn wait_for_output_bytes(
    timeout: Duration,
    execution: Execution,
) -> Result<axum::body::Bytes, OpenAIError> {
    let result = tokio::time::timeout(timeout, crate::serve_execution::collect(execution))
        .await
        .map_err(|_| OpenAIError::gateway_timeout("request execution deadline exceeded"))?
        .map_err(OpenAIError::runtime)?;
    if result.state != JobState::Succeeded {
        return Err(OpenAIError::execution(result.error.as_ref()));
    }
    match result.outputs.into_iter().next() {
        Some(Object::Inline { data, .. }) => Ok(data),
        _ => Err(OpenAIError::bad_gateway(
            "worker completed without an inline response",
        )),
    }
}

pub(super) async fn streaming_response(
    mut execution: Execution,
    timeout: Duration,
) -> Result<Response, OpenAIError> {
    let deadline = tokio::time::Instant::now() + timeout;
    // Admission, assignment and progress do not commit an HTTP success. Keep
    // pre-output failures on the same JSON/status path as buffered requests.
    let first = loop {
        let event = tokio::time::timeout_at(deadline, execution.next())
            .await
            .map_err(|_| OpenAIError::gateway_timeout("request execution deadline exceeded"))?
            .ok_or_else(|| OpenAIError::bad_gateway("execution ended without a terminal event"))?
            .map_err(OpenAIError::runtime)?;
        match event.kind {
            EventKind::Output {
                data,
                progress: false,
                ..
            } if !data.is_empty() => break data,
            EventKind::Finished {
                state: JobState::Succeeded,
                ..
            } => {
                return Err(OpenAIError::bad_gateway(
                    "worker completed without stream output",
                ));
            }
            EventKind::Finished { error, .. } => {
                return Err(OpenAIError::execution(error.as_ref()));
            }
            _ => {}
        }
    };
    let stream = async_stream::try_stream! {
        yield first;
        loop {
            let event = tokio::time::timeout_at(deadline, execution.next()).await
                .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "request execution deadline exceeded"))?
                .ok_or_else(|| io::Error::other("execution ended without a terminal event"))?
                .map_err(io::Error::other)?;
            match event.kind {
                EventKind::Output { data, progress: false, .. } => yield data,
                EventKind::Finished { state: JobState::Succeeded, .. } => break,
                EventKind::Finished { error, .. } => Err(io::Error::other(error.map(|error| error.message).unwrap_or_else(|| "execution cancelled".to_owned())))?,
                _ => {},
            }
        }
    };
    let stream: std::pin::Pin<
        Box<dyn futures::Stream<Item = io::Result<axum::body::Bytes>> + Send>,
    > = Box::pin(stream);
    let mut response = Response::new(Body::from_stream(stream));
    *response.status_mut() = StatusCode::OK;
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, "text/event-stream".parse().unwrap());
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, "no-cache".parse().unwrap());
    Ok(response)
}
