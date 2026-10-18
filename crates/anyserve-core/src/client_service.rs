#![allow(
    clippy::result_large_err,
    reason = "tonic::Status is the transport error type"
)]
//! Protocol adapter for native clients. State transitions belong to Runtime.

use std::pin::Pin;
use std::time::Duration;

use anyserve_proto::v2::{self as pb, client_server::Client};
use futures::{Stream, StreamExt};
use tonic::{Request, Response, Status, Streaming};

use crate::runtime::{Runtime, api};

#[derive(Clone)]
pub struct ClientGrpcService {
    runtime: Runtime,
}
impl ClientGrpcService {
    pub fn new(runtime: Runtime) -> Self {
        Self { runtime }
    }
}
type EventStream = Pin<Box<dyn Stream<Item = Result<pb::ExecutionEvent, Status>> + Send>>;

#[tonic::async_trait]
impl Client for ClientGrpcService {
    async fn submit(
        &self,
        request: Request<pb::SubmitRequest>,
    ) -> Result<Response<pb::SubmitResponse>, Status> {
        let request = request.into_inner();
        let conflict = match pb::ConflictPolicy::try_from(request.conflict_policy) {
            Ok(pb::ConflictPolicy::Fail) => api::ConflictPolicy::Fail,
            Ok(pb::ConflictPolicy::UseExisting | pb::ConflictPolicy::Unspecified) => {
                api::ConflictPolicy::UseExisting
            }
            Err(_) => return Err(Status::invalid_argument("unknown conflict policy")),
        };
        let result = self
            .runtime
            .submit(api::SubmitRequest {
                job_id: nonempty(request.job_id),
                conflict,
                spec: spec_from_proto(
                    request
                        .spec
                        .ok_or_else(|| Status::invalid_argument("spec is required"))?,
                )?,
            })
            .await?;
        Ok(Response::new(pb::SubmitResponse {
            job: Some(job_to_proto(result.job)),
            created: result.created,
        }))
    }

    type InvokeStream = EventStream;
    async fn invoke(
        &self,
        request: Request<Streaming<pb::InvokeRequest>>,
    ) -> Result<Response<Self::InvokeStream>, Status> {
        let mut incoming = request.into_inner();
        let start = match incoming.message().await?.and_then(|message| message.msg) {
            Some(pb::invoke_request::Msg::Start(start)) => start,
            _ => {
                return Err(Status::invalid_argument(
                    "first invocation message must be Start",
                ));
            }
        };
        let mut execution = self
            .runtime
            .invoke(api::InvokeRequest {
                spec: spec_from_proto(
                    start
                        .spec
                        .ok_or_else(|| Status::invalid_argument("spec is required"))?,
                )?,
                input: if start.streaming_input {
                    api::InputMode::Streaming
                } else {
                    api::InputMode::Complete
                },
                delivery: api::Delivery::Streamed,
            })
            .await?;
        let input = execution.take_input();
        let stream = async_stream::try_stream! {
            // Execution owns the cancellation guard. Its Drop runs even if the
            // HTTP/2 caller disappears before this generator is first polled.
            let pump = pump_input(incoming, input);
            tokio::pin!(pump);
            let mut input_done = false;
            loop {
                enum Next {
                    Input(Result<(), Status>),
                    Event(Option<Result<api::ExecutionEvent, crate::error::RuntimeError>>),
                }
                let next = tokio::select! {
                    result = &mut pump, if !input_done => Next::Input(result),
                    next = execution.next() => Next::Event(next),
                };
                match next {
                    Next::Input(result) => { result?; input_done = true; }
                    Next::Event(next) => {
                        let event = next.ok_or_else(|| Status::unavailable("execution ended without a terminal event"))??;
                        let terminal = matches!(event.kind, api::EventKind::Finished { .. });
                        yield event_to_proto(event);
                        if terminal { break; }
                    }
                }
            }
        };
        Ok(Response::new(Box::pin(stream)))
    }

    type WatchStream = EventStream;
    async fn watch(
        &self,
        request: Request<pb::WatchRequest>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        let request = request.into_inner();
        let events = self
            .runtime
            .watch(&request.job_id, request.after_sequence)?;
        Ok(Response::new(Box::pin(events.map(|event| {
            event.map(event_to_proto).map_err(Status::from)
        }))))
    }

    async fn get_job(
        &self,
        request: Request<pb::GetJobRequest>,
    ) -> Result<Response<pb::Job>, Status> {
        let request = request.into_inner();
        Ok(Response::new(job_to_proto(
            self.runtime
                .get_job(&request.job_id, request.include_outputs)
                .await?,
        )))
    }
    async fn cancel(
        &self,
        request: Request<pb::CancelRequest>,
    ) -> Result<Response<pb::Job>, Status> {
        let request = request.into_inner();
        Ok(Response::new(job_to_proto(
            self.runtime.cancel(&request.job_id, request.reason).await?,
        )))
    }
    async fn list_jobs(
        &self,
        request: Request<pb::ListJobsRequest>,
    ) -> Result<Response<pb::ListJobsResponse>, Status> {
        let request = request.into_inner();
        let states = request
            .states
            .into_iter()
            .map(state_from_proto)
            .collect::<Result<_, _>>()?;
        let page = self
            .runtime
            .list_jobs(api::JobFilter {
                states,
                limit: request.limit as usize,
                page_token: nonempty(request.page_token),
                interface: None,
            })
            .await?;
        Ok(Response::new(pb::ListJobsResponse {
            jobs: page.jobs.into_iter().map(job_to_proto).collect(),
            next_page_token: page.next_page_token.unwrap_or_default(),
        }))
    }
}

async fn pump_input(
    mut incoming: Streaming<pb::InvokeRequest>,
    input: Option<api::InputSink>,
) -> Result<(), Status> {
    let mut ended = input.is_none();
    while let Some(message) = incoming.message().await? {
        match (message.msg, &input, ended) {
            (Some(pb::invoke_request::Msg::Input(chunk)), Some(input), false) => {
                input
                    .send(chunk.data, chunk.metadata.into_iter().collect())
                    .await?;
            }
            (Some(pb::invoke_request::Msg::InputEnd(_)), Some(input), false) => {
                input.finish().await?;
                ended = true;
            }
            _ => {
                return Err(Status::invalid_argument(
                    "unexpected invocation input message",
                ));
            }
        }
    }
    if !ended {
        return Err(Status::invalid_argument(
            "streaming input ended without InputEnd",
        ));
    }
    Ok(())
}

pub fn spec_from_proto(spec: pb::JobSpec) -> Result<api::JobSpec, Status> {
    let demand = spec
        .demand
        .ok_or_else(|| Status::invalid_argument("demand is required"))?;
    let policy = spec.policy.unwrap_or_default();
    let durability = match pb::Durability::try_from(policy.durability) {
        Ok(pb::Durability::Unspecified) => None,
        Ok(pb::Durability::Ephemeral) => Some(api::Durability::Ephemeral),
        Ok(pb::Durability::Durable) => Some(api::Durability::Durable),
        Err(_) => return Err(Status::invalid_argument("unknown durability")),
    };
    Ok(api::JobSpec {
        demand: api::Demand {
            interface: demand.interface,
            required_attributes: demand.required_attributes.into_iter().collect(),
            preferred_attributes: demand.preferred_attributes.into_iter().collect(),
            required_capacity: demand.required_capacity.into_iter().collect(),
        },
        inputs: spec
            .inputs
            .into_iter()
            .map(object_from_proto)
            .collect::<Result<_, _>>()?,
        params: spec.params,
        metadata: spec.metadata.into_iter().collect(),
        policy: api::ExecutionPolicy {
            durability,
            priority: policy.priority,
            queue_timeout: milliseconds(policy.queue_timeout_ms),
            execution_timeout: milliseconds(policy.execution_timeout_ms),
            total_timeout: milliseconds(policy.total_timeout_ms),
            spillover_after: milliseconds(policy.spillover_after_ms),
            max_attempts: (policy.max_attempts > 0).then_some(policy.max_attempts),
        },
    })
}

pub fn object_from_proto(object: pb::Object) -> Result<api::Object, Status> {
    let metadata = object.metadata.into_iter().collect();
    match object.body {
        Some(pb::object::Body::Inline(data)) => Ok(api::Object::Inline { data, metadata }),
        Some(pb::object::Body::Uri(uri)) => Ok(api::Object::Uri { uri, metadata }),
        None => Err(Status::invalid_argument("object body is required")),
    }
}
pub fn object_to_proto(object: api::Object) -> pb::Object {
    match object {
        api::Object::Inline { data, metadata } => pb::Object {
            body: Some(pb::object::Body::Inline(data)),
            metadata: metadata.into_iter().collect(),
        },
        api::Object::Uri { uri, metadata } => pb::Object {
            body: Some(pb::object::Body::Uri(uri)),
            metadata: metadata.into_iter().collect(),
        },
    }
}
pub fn job_to_proto(job: api::JobView) -> pb::Job {
    pb::Job {
        job_id: job.job_id,
        state: state_to_proto(job.state),
        durability: match job.durability {
            api::Durability::Ephemeral => pb::Durability::Ephemeral as i32,
            api::Durability::Durable => pb::Durability::Durable as i32,
        },
        interface: job.interface,
        attempt_count: job.attempt_count,
        current_attempt_id: job.current_attempt_id.unwrap_or_default(),
        current_worker_id: job.current_worker_id.unwrap_or_default(),
        created_at_ms: job.created_at_ms,
        updated_at_ms: job.updated_at_ms,
        error: job.error.map(error_to_proto),
        outputs: job
            .outputs
            .unwrap_or_default()
            .into_iter()
            .map(object_to_proto)
            .collect(),
        metadata: job.metadata.into_iter().collect(),
        spec_hash: job.spec_hash,
    }
}
pub fn event_to_proto(event: api::ExecutionEvent) -> pb::ExecutionEvent {
    let kind = match event.kind {
        api::EventKind::Accepted => pb::execution_event::Event::Accepted(pb::Accepted {}),
        api::EventKind::Assigned {
            worker_id,
            attempt_number,
        } => pb::execution_event::Event::Assigned(pb::Assigned {
            worker_id,
            attempt_number,
        }),
        api::EventKind::Output {
            data,
            metadata,
            progress,
        } => pb::execution_event::Event::Output(pb::Output {
            data,
            metadata: metadata.into_iter().collect(),
            progress,
        }),
        api::EventKind::AttemptRestarted {
            previous_attempt_id,
            reason,
        } => pb::execution_event::Event::AttemptRestarted(pb::AttemptRestarted {
            previous_attempt_id,
            reason: Some(error_to_proto(reason)),
        }),
        api::EventKind::Finished {
            state,
            outputs,
            error,
        } => pb::execution_event::Event::Finished(pb::ExecutionFinished {
            state: state_to_proto(state),
            outputs: outputs.into_iter().map(object_to_proto).collect(),
            error: error.map(error_to_proto),
        }),
    };
    pb::ExecutionEvent {
        job_id: event.job_id,
        sequence: event.sequence,
        attempt_id: event.attempt_id.unwrap_or_default(),
        at_ms: event.at_ms,
        event: Some(kind),
    }
}
pub fn error_to_proto(error: api::ErrorInfo) -> pb::ErrorInfo {
    pb::ErrorInfo {
        code: error.code,
        message: error.message,
        retryable: error.retryable,
    }
}
fn state_to_proto(state: api::JobState) -> i32 {
    (match state {
        api::JobState::Queued => pb::JobState::Queued,
        api::JobState::Running => pb::JobState::Running,
        api::JobState::Succeeded => pb::JobState::Succeeded,
        api::JobState::Failed => pb::JobState::Failed,
        api::JobState::Cancelled => pb::JobState::Cancelled,
    }) as i32
}
fn state_from_proto(state: i32) -> Result<api::JobState, Status> {
    Ok(match pb::JobState::try_from(state) {
        Ok(pb::JobState::Queued) => api::JobState::Queued,
        Ok(pb::JobState::Running) => api::JobState::Running,
        Ok(pb::JobState::Succeeded) => api::JobState::Succeeded,
        Ok(pb::JobState::Failed) => api::JobState::Failed,
        Ok(pb::JobState::Cancelled) => api::JobState::Cancelled,
        _ => return Err(Status::invalid_argument("unknown job state")),
    })
}
fn nonempty(value: String) -> Option<String> {
    (!value.is_empty()).then_some(value)
}
fn milliseconds(value: u64) -> Option<Duration> {
    (value > 0).then(|| Duration::from_millis(value))
}
