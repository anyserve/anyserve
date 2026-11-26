#![allow(
    clippy::result_large_err,
    reason = "tonic::Status is the transport error type"
)]
//! Protocol adapter for native clients. State transitions belong to Runtime.

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use anyserve_proto::v2::{self as pb, client_server::Client};
use futures::{Stream, StreamExt};
use tonic::{Request, Response, Status, Streaming};

use crate::auth::{
    AuthPolicy, DEFAULT_NAMESPACE, NAMESPACE_HEADER, Principal, validate_namespace,
    wait_optional_credential_invalidated,
};
use crate::runtime::{Runtime, api};

fn request_namespace<T>(request: &Request<T>) -> Result<String, Status> {
    if let Some(principal) = request.extensions().get::<Principal>() {
        return Ok(principal.namespace.clone());
    }
    match request
        .metadata()
        .get(NAMESPACE_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        Some(ns) => validate_namespace(ns).map(str::to_string),
        None => Ok(DEFAULT_NAMESPACE.to_string()),
    }
}

fn merge_grpc_trace_headers(
    spec: Option<&mut pb::JobSpec>,
    headers: &tonic::metadata::MetadataMap,
) {
    if let Some(spec) = spec {
        crate::trace::merge_trace_headers_into_proto(&mut spec.metadata, |key| {
            headers.get(key).and_then(|value| value.to_str().ok())
        });
    }
}

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
        self.runtime.ensure_active_leader()?;
        let namespace = request_namespace(&request)?;
        let (headers, _, mut inner) = request.into_parts();
        merge_grpc_trace_headers(inner.spec.as_mut(), &headers);
        submit_request(&self.runtime, namespace, inner)
            .await
            .map(Response::new)
    }

    type InvokeStream = EventStream;
    async fn invoke(
        &self,
        request: Request<Streaming<pb::InvokeRequest>>,
    ) -> Result<Response<Self::InvokeStream>, Status> {
        self.runtime.ensure_active_leader()?;
        let namespace = request_namespace(&request)?;
        let principal = request.extensions().get::<Principal>().cloned();
        let auth = request.extensions().get::<Arc<AuthPolicy>>().cloned();
        let (headers, _, mut incoming) = request.into_parts();
        let timeout = self.runtime.config().heartbeat_timeout;
        let first = tokio::time::timeout(timeout, incoming.message())
            .await
            .map_err(|_| Status::deadline_exceeded("no invocation Start received"))?;
        let mut start = match first?.and_then(|message| message.msg) {
            Some(pb::invoke_request::Msg::Start(start)) => start,
            _ => {
                return Err(Status::invalid_argument(
                    "first invocation message must be Start",
                ));
            }
        };
        if let (Some(auth), Some(principal)) = (auth.as_deref(), principal.as_ref())
            && auth.is_principal_invalidated(principal, crate::now_ms() / 1000)
        {
            auth.record_rejection();
            return Err(Status::unauthenticated(
                "bearer token has been revoked or expired",
            ));
        }
        merge_grpc_trace_headers(start.spec.as_mut(), &headers);
        let mut execution = start_invoke(&self.runtime, namespace, start).await?;
        let input = execution.take_input();
        let stream = async_stream::try_stream! {
            // Execution owns the cancellation guard. Its Drop runs even if the
            // HTTP/2 caller disappears before this generator is first polled.
            let pump = pump_input(incoming, input);
            let invalidated = wait_optional_credential_invalidated(auth.as_deref(), principal.as_ref());
            tokio::pin!(pump);
            tokio::pin!(invalidated);
            let mut input_done = false;
            loop {
                enum Next {
                    Invalidated,
                    Input(Result<(), Status>),
                    Event(Option<Result<api::ExecutionEvent, crate::error::RuntimeError>>),
                }
                let next = tokio::select! {
                    biased;
                    _ = &mut invalidated => Next::Invalidated,
                    result = &mut pump, if !input_done => Next::Input(result),
                    next = execution.next() => Next::Event(next),
                };
                match next {
                    Next::Invalidated => {
                        if let Some(auth) = &auth {
                            auth.record_rejection();
                        }
                        Err(Status::unauthenticated("bearer token has been revoked or expired"))?;
                    }
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
        self.runtime.ensure_active_leader()?;
        let namespace = request_namespace(&request)?;
        let principal = request.extensions().get::<Principal>().cloned();
        let auth = request.extensions().get::<Arc<AuthPolicy>>().cloned();
        if let (Some(auth), Some(principal)) = (auth.as_deref(), principal.as_ref())
            && auth.is_principal_invalidated(principal, crate::now_ms() / 1000)
        {
            auth.record_rejection();
            return Err(Status::unauthenticated(
                "bearer token has been revoked or expired",
            ));
        }
        let request = request.into_inner();
        let mut events =
            self.runtime
                .watch_in(&namespace, &request.job_id, request.after_sequence)?;
        if let (Some(auth), Some(principal)) = (auth, principal)
            && (principal.jti.is_some() || principal.exp.is_some())
        {
            let stream = async_stream::stream! {
                let invalidated = crate::auth::wait_credential_invalidated(&auth, &principal);
                tokio::pin!(invalidated);
                loop {
                    tokio::select! {
                        biased;
                        _ = &mut invalidated => {
                            auth.record_rejection();
                            yield Err(Status::unauthenticated(
                                "bearer token has been revoked or expired",
                            ));
                            break;
                        }
                        next = events.next() => match next {
                            Some(Ok(event)) => yield Ok(event_to_proto(event)),
                            Some(Err(err)) => {
                                yield Err(Status::from(err));
                                break;
                            }
                            None => break,
                        }
                    }
                }
            };
            return Ok(Response::new(Box::pin(stream)));
        }
        Ok(Response::new(Box::pin(events.map(|event| {
            event.map(event_to_proto).map_err(Status::from)
        }))))
    }

    async fn get_job(
        &self,
        request: Request<pb::GetJobRequest>,
    ) -> Result<Response<pb::Job>, Status> {
        self.runtime.ensure_active_leader()?;
        let namespace = request_namespace(&request)?;
        get_job_request(&self.runtime, &namespace, request.into_inner())
            .await
            .map(Response::new)
    }
    async fn cancel(
        &self,
        request: Request<pb::CancelRequest>,
    ) -> Result<Response<pb::Job>, Status> {
        self.runtime.ensure_active_leader()?;
        let namespace = request_namespace(&request)?;
        cancel_request(&self.runtime, &namespace, request.into_inner())
            .await
            .map(Response::new)
    }
    async fn list_jobs(
        &self,
        request: Request<pb::ListJobsRequest>,
    ) -> Result<Response<pb::ListJobsResponse>, Status> {
        self.runtime.ensure_active_leader()?;
        let namespace = request_namespace(&request)?;
        list_jobs_request(&self.runtime, namespace, request.into_inner())
            .await
            .map(Response::new)
    }
}

pub(crate) async fn submit_request(
    runtime: &Runtime,
    namespace: String,
    request: pb::SubmitRequest,
) -> Result<pb::SubmitResponse, Status> {
    let conflict = match pb::ConflictPolicy::try_from(request.conflict_policy) {
        Ok(pb::ConflictPolicy::Fail) => api::ConflictPolicy::Fail,
        Ok(pb::ConflictPolicy::UseExisting | pb::ConflictPolicy::Unspecified) => {
            api::ConflictPolicy::UseExisting
        }
        Err(_) => return Err(Status::invalid_argument("unknown conflict policy")),
    };
    let result = runtime
        .submit(api::SubmitRequest {
            job_id: nonempty(request.job_id),
            namespace: Some(namespace),
            conflict,
            spec: spec_from_proto(
                request
                    .spec
                    .ok_or_else(|| Status::invalid_argument("spec is required"))?,
            )?,
        })
        .await?;
    Ok(pb::SubmitResponse {
        job: Some(job_to_proto(result.job)),
        created: result.created,
    })
}

pub(crate) async fn start_invoke(
    runtime: &Runtime,
    namespace: String,
    start: pb::InvokeStart,
) -> Result<api::Execution, Status> {
    Ok(runtime
        .invoke(api::InvokeRequest {
            namespace: Some(namespace),
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
        })
        .await?)
}

pub(crate) async fn get_job_request(
    runtime: &Runtime,
    namespace: &str,
    request: pb::GetJobRequest,
) -> Result<pb::Job, Status> {
    Ok(job_to_proto(
        runtime
            .get_job_in(namespace, &request.job_id, request.include_outputs)
            .await?,
    ))
}

pub(crate) async fn cancel_request(
    runtime: &Runtime,
    namespace: &str,
    request: pb::CancelRequest,
) -> Result<pb::Job, Status> {
    Ok(job_to_proto(
        runtime
            .cancel_in(namespace, &request.job_id, request.reason)
            .await?,
    ))
}

pub(crate) async fn list_jobs_request(
    runtime: &Runtime,
    namespace: String,
    request: pb::ListJobsRequest,
) -> Result<pb::ListJobsResponse, Status> {
    let states = request
        .states
        .into_iter()
        .map(state_from_proto)
        .collect::<Result<_, _>>()?;
    let page = runtime
        .list_jobs(api::JobFilter {
            states,
            limit: request.limit as usize,
            page_token: nonempty(request.page_token),
            interface: None,
            namespace: Some(namespace),
        })
        .await?;
    Ok(pb::ListJobsResponse {
        jobs: page.jobs.into_iter().map(job_to_proto).collect(),
        next_page_token: page.next_page_token.unwrap_or_default(),
    })
}

pub(crate) async fn handle_invoke_message(
    message: pb::InvokeRequest,
    input: &Option<api::InputSink>,
    ended: &mut bool,
) -> Result<(), Status> {
    match (message.msg, input, *ended) {
        (Some(pb::invoke_request::Msg::Input(chunk)), Some(input), false) => {
            input
                .send(chunk.data, chunk.metadata.into_iter().collect())
                .await?;
            Ok(())
        }
        (Some(pb::invoke_request::Msg::InputEnd(_)), Some(input), false) => {
            input.finish().await?;
            *ended = true;
            Ok(())
        }
        _ => Err(Status::invalid_argument(
            "unexpected invocation input message",
        )),
    }
}

async fn pump_input(
    mut incoming: Streaming<pb::InvokeRequest>,
    input: Option<api::InputSink>,
) -> Result<(), Status> {
    let mut ended = input.is_none();
    while let Some(message) = incoming.message().await? {
        handle_invoke_message(message, &input, &mut ended).await?;
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
            affinity_key: demand.affinity_key,
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
            fairness_key: policy.fairness_key,
            concurrency_key: policy.concurrency_key,
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
        namespace: job.namespace,
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
            ..
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
pub fn error_from_proto(error: pb::ErrorInfo) -> api::ErrorInfo {
    api::ErrorInfo {
        code: error.code,
        message: error.message,
        retryable: error.retryable,
    }
}
pub fn spec_to_proto(spec: &api::JobSpec) -> pb::JobSpec {
    use crate::runtime::machine::{object_to_proto as ref_object_to_proto, to_proto_map};
    pb::JobSpec {
        demand: Some(pb::Demand {
            interface: spec.demand.interface.clone(),
            required_attributes: to_proto_map(&spec.demand.required_attributes),
            preferred_attributes: to_proto_map(&spec.demand.preferred_attributes),
            required_capacity: to_proto_map(&spec.demand.required_capacity),
            affinity_key: spec.demand.affinity_key.clone(),
        }),
        inputs: spec.inputs.iter().map(ref_object_to_proto).collect(),
        params: spec.params.clone(),
        metadata: to_proto_map(&spec.metadata),
        policy: Some(pb::ExecutionPolicy {
            durability: match spec.policy.durability {
                None => pb::Durability::Unspecified as i32,
                Some(api::Durability::Ephemeral) => pb::Durability::Ephemeral as i32,
                Some(api::Durability::Durable) => pb::Durability::Durable as i32,
            },
            priority: spec.policy.priority,
            queue_timeout_ms: spec
                .policy
                .queue_timeout
                .map(|d| d.as_millis().min(u64::MAX as u128) as u64)
                .unwrap_or(0),
            execution_timeout_ms: spec
                .policy
                .execution_timeout
                .map(|d| d.as_millis().min(u64::MAX as u128) as u64)
                .unwrap_or(0),
            total_timeout_ms: spec
                .policy
                .total_timeout
                .map(|d| d.as_millis().min(u64::MAX as u128) as u64)
                .unwrap_or(0),
            spillover_after_ms: spec
                .policy
                .spillover_after
                .map(|d| d.as_millis().min(u64::MAX as u128) as u64)
                .unwrap_or(0),
            max_attempts: spec.policy.max_attempts.unwrap_or(0),
            fairness_key: spec.policy.fairness_key.clone(),
            concurrency_key: spec.policy.concurrency_key.clone(),
        }),
    }
}
pub fn event_from_proto(event: pb::ExecutionEvent) -> Result<api::ExecutionEvent, Status> {
    let kind = match event
        .event
        .ok_or_else(|| Status::internal("missing execution event variant"))?
    {
        pb::execution_event::Event::Accepted(_) => api::EventKind::Accepted,
        pb::execution_event::Event::Assigned(assigned) => api::EventKind::Assigned {
            worker_id: assigned.worker_id,
            attempt_number: assigned.attempt_number,
        },
        pb::execution_event::Event::Output(output) => api::EventKind::Output {
            data: output.data,
            metadata: output.metadata.into_iter().collect(),
            progress: output.progress,
        },
        pb::execution_event::Event::AttemptRestarted(restarted) => {
            api::EventKind::AttemptRestarted {
                previous_attempt_id: restarted.previous_attempt_id,
                reason: error_from_proto(
                    restarted
                        .reason
                        .ok_or_else(|| Status::internal("missing restart reason"))?,
                ),
            }
        }
        pb::execution_event::Event::Finished(finished) => api::EventKind::Finished {
            state: state_from_proto(finished.state)?,
            outputs: finished
                .outputs
                .into_iter()
                .map(object_from_proto)
                .collect::<Result<_, _>>()?,
            error: finished.error.map(error_from_proto),
            handler_duration_ms: None,
        },
    };
    Ok(api::ExecutionEvent {
        job_id: event.job_id,
        sequence: event.sequence,
        attempt_id: nonempty(event.attempt_id),
        at_ms: event.at_ms,
        kind,
    })
}
fn nonempty(value: String) -> Option<String> {
    (!value.is_empty()).then_some(value)
}
fn milliseconds(value: u64) -> Option<Duration> {
    (value > 0).then(|| Duration::from_millis(value))
}
