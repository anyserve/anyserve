use std::io;
use std::sync::Arc;
use std::time::Duration;

use async_stream::stream;
use axum::body::Body;
use axum::http::header::{CACHE_CONTROL, CONTENT_TYPE};
use axum::http::{HeaderValue, StatusCode};
use axum::response::Response;
use tokio::time::Instant;

use anyserve_core::model::JobState;

use super::super::state::{AppState, JobCancellationGuard};
use super::OUTPUT_STREAM;
use super::output::extract_data_chunks;
use super::waiters::{wait_for_job_or_stream_change, wait_for_stream_or_job_change};

pub(super) fn streaming_response(state: AppState, job_id: String) -> Response {
    let kernel = Arc::clone(&state.kernel);
    let deadline = Instant::now() + Duration::from_secs(state.config.request_timeout_secs);
    let poll_interval = Duration::from_millis(kernel.watch_poll_interval_ms());
    let stream = stream! {
        let mut cancel_guard = JobCancellationGuard::new(Arc::clone(&kernel), job_id.clone());
        let mut job_stream_updates = match kernel.subscribe_job_streams(&job_id).await {
            Ok(updates) => updates,
            Err(error) => {
                yield Err::<Vec<u8>, io::Error>(io::Error::other(format!("subscribe job streams: {error}")));
                return;
            }
        };
        let mut cluster_job_stream_updates = match kernel.subscribe_cluster_job_streams(&job_id).await {
            Ok(updates) => updates,
            Err(error) => {
                yield Err::<Vec<u8>, io::Error>(io::Error::other(format!("subscribe cluster job streams: {error}")));
                return;
            }
        };
        let mut job_event_updates = match kernel.subscribe_job_events(&job_id).await {
            Ok(updates) => updates,
            Err(error) => {
                yield Err::<Vec<u8>, io::Error>(io::Error::other(format!("subscribe job events: {error}")));
                return;
            }
        };
        let mut cluster_job_event_updates = match kernel.subscribe_cluster_job_events(&job_id).await {
            Ok(updates) => updates,
            Err(error) => {
                yield Err::<Vec<u8>, io::Error>(io::Error::other(format!("subscribe cluster job events: {error}")));
                return;
            }
        };
        let mut output_stream_id: Option<String> = None;
        let mut after_sequence = 0u64;

        loop {
            if Instant::now() > deadline {
                let _ = kernel.cancel_job(&job_id).await;
                cancel_guard.disarm();
                yield Err::<Vec<u8>, io::Error>(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "request timed out waiting for worker stream",
                ));
                break;
            }

            if output_stream_id.is_none() {
                let streams = match kernel.list_streams(&job_id).await {
                    Ok(streams) => streams,
                    Err(error) => {
                        yield Err(io::Error::other(format!("list streams: {error}")));
                        break;
                    }
                };
                output_stream_id = streams
                    .into_iter()
                    .find(|stream| stream.stream_name == OUTPUT_STREAM)
                    .map(|stream| stream.stream_id);
            }

            if let Some(stream_id) = output_stream_id.as_ref() {
                let frames = match kernel.pull_frames(stream_id, after_sequence).await {
                    Ok(frames) => frames,
                    Err(error) => {
                        yield Err(io::Error::other(format!("pull frames: {error}")));
                        break;
                    }
                };
                let chunks = match extract_data_chunks(frames, &mut after_sequence) {
                    Ok(chunks) => chunks,
                    Err(error) => {
                        yield Err(error);
                        return;
                    }
                };
                for chunk in chunks {
                    yield Ok(chunk);
                }

                let stream = match kernel.get_stream(stream_id).await {
                    Ok(stream) => stream,
                    Err(error) => {
                        yield Err(io::Error::other(format!("get stream: {error}")));
                        break;
                    }
                };
                let job = match kernel.get_job(&job_id).await {
                    Ok(job) => job,
                    Err(error) => {
                        yield Err(io::Error::other(format!("get job: {error}")));
                        break;
                    }
                };
                if job.state == JobState::Failed {
                    cancel_guard.disarm();
                    yield Err(io::Error::other(
                        job.last_error
                            .unwrap_or_else(|| "worker reported failure".to_string()),
                    ));
                    break;
                }
                if job.state == JobState::Cancelled {
                    cancel_guard.disarm();
                    yield Err(io::Error::other("job was cancelled"));
                    break;
                }
                if job.state.is_terminal() && stream.state.is_terminal() {
                    let remaining = match kernel.pull_frames(stream_id, after_sequence).await {
                        Ok(frames) => frames,
                        Err(error) => {
                            yield Err(io::Error::other(format!("pull frames: {error}")));
                            break;
                        }
                    };
                    let chunks = match extract_data_chunks(remaining, &mut after_sequence) {
                        Ok(chunks) => chunks,
                        Err(error) => {
                            yield Err(error);
                            return;
                        }
                    };
                    for chunk in chunks {
                        yield Ok(chunk);
                    }
                    cancel_guard.disarm();
                    break;
                }
            } else {
                let job = match kernel.get_job(&job_id).await {
                    Ok(job) => job,
                    Err(error) => {
                        yield Err(io::Error::other(format!("get job: {error}")));
                        break;
                    }
                };
                if job.state == JobState::Failed {
                    cancel_guard.disarm();
                    yield Err(io::Error::other(
                        job.last_error
                            .unwrap_or_else(|| "worker reported failure".to_string()),
                    ));
                    break;
                }
                if job.state == JobState::Cancelled {
                    cancel_guard.disarm();
                    yield Err(io::Error::other("job was cancelled"));
                    break;
                }
                if job.state.is_terminal() {
                    let streams = match kernel.list_streams(&job_id).await {
                        Ok(streams) => streams,
                        Err(error) => {
                            yield Err(io::Error::other(format!("list streams: {error}")));
                            break;
                        }
                    };
                    if let Some(stream_id) = streams
                        .into_iter()
                        .find(|stream| stream.stream_name == OUTPUT_STREAM)
                        .map(|stream| stream.stream_id)
                    {
                        output_stream_id = Some(stream_id);
                        continue;
                    }
                    cancel_guard.disarm();
                    yield Err(io::Error::other("job completed without output stream"));
                    break;
                }
            }

            let wait_result = if let Some(stream_id) = output_stream_id.as_ref() {
                wait_for_stream_or_job_change(
                    kernel.as_ref(),
                    stream_id,
                    after_sequence,
                    deadline,
                    poll_interval,
                    &mut job_event_updates,
                    &mut cluster_job_event_updates,
                    "request timed out waiting for worker stream",
                )
                .await
            } else {
                wait_for_job_or_stream_change(
                    deadline,
                    poll_interval,
                    &mut job_stream_updates,
                    &mut cluster_job_stream_updates,
                    &mut job_event_updates,
                    &mut cluster_job_event_updates,
                    "request timed out waiting for worker stream",
                )
                .await
            };

            if let Err(error) = wait_result {
                if error.kind() == io::ErrorKind::TimedOut {
                    let _ = kernel.cancel_job(&job_id).await;
                    cancel_guard.disarm();
                }
                yield Err(error);
                break;
            }
        }
    };

    let mut response = Response::new(Body::from_stream(stream));
    *response.status_mut() = StatusCode::OK;
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("text/event-stream"));
    response
        .headers_mut()
        .insert(CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    response
}
