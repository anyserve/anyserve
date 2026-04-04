use std::collections::BTreeSet;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::{Error, Result, bail};
use anyserve_proto::controlplane::{
    self as proto, control_plane_service_server::ControlPlaneService,
};
use async_stream::try_stream;
use futures::{Stream, StreamExt};
use tonic::{Request, Response, Status};

use crate::kernel::{Kernel, OpenStreamCommand};
use crate::model::{
    AttemptRecord, AttemptState, Attributes, Capacity, Demand, EventKind, ExecutionPolicy, Frame,
    FrameKind, JobEvent, JobRecord, JobSpec, JobState, LeaseAssignment, LeaseRecord, ObjectRef,
    StreamDirection, StreamRecord, StreamScope, StreamState, WorkerRecord, WorkerSpec,
    WorkerStatus,
};

type ResponseStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

#[derive(Clone)]
pub struct ControlPlaneGrpcService {
    kernel: Arc<Kernel>,
}

impl ControlPlaneGrpcService {
    pub fn new(kernel: Arc<Kernel>) -> Self {
        Self { kernel }
    }
}

#[tonic::async_trait]
impl ControlPlaneService for ControlPlaneGrpcService {
    type WatchJobStream = ResponseStream<proto::JobEvent>;
    type PullFramesStream = ResponseStream<proto::Frame>;

    async fn submit_job(
        &self,
        request: Request<proto::SubmitJobRequest>,
    ) -> Result<Response<proto::SubmitJobResponse>, Status> {
        let request = request.into_inner();
        let spec = request
            .spec
            .ok_or_else(|| Status::invalid_argument("job spec is required"))?;
        let job = self
            .kernel
            .submit_job(
                empty_to_none(request.job_id),
                job_spec_from_proto(spec).map_err(to_status)?,
            )
            .await
            .map_err(to_status)?;

        Ok(Response::new(proto::SubmitJobResponse {
            job: Some(job_record_to_proto(job)),
        }))
    }

    async fn watch_job(
        &self,
        request: Request<proto::WatchJobRequest>,
    ) -> Result<Response<Self::WatchJobStream>, Status> {
        let request = request.into_inner();
        if request.job_id.is_empty() {
            return Err(Status::invalid_argument("job_id is required"));
        }

        let kernel = Arc::clone(&self.kernel);
        let job_id = request.job_id;
        let stream = try_stream! {
            let mut cursor = request.after_sequence;
            let mut updates = kernel.subscribe_job_events(&job_id).await.map_err(to_status)?;

            loop {
                let events = kernel.watch_job(&job_id, cursor).await.map_err(to_status)?;
                for event in events {
                    cursor = event.sequence;
                    yield job_event_to_proto(event);
                }

                let job = kernel.get_job(&job_id).await.map_err(to_status)?;
                if job.state.is_terminal() {
                    let remaining = kernel.watch_job(&job_id, cursor).await.map_err(to_status)?;
                    if remaining.is_empty() {
                        break;
                    }
                    for event in remaining {
                        yield job_event_to_proto(event);
                    }
                    break;
                }

                if updates.changed().await.is_err() {
                    break;
                }
            }
        };

        Ok(Response::new(Box::pin(stream)))
    }

    async fn list_jobs(
        &self,
        _request: Request<proto::ListJobsRequest>,
    ) -> Result<Response<proto::ListJobsResponse>, Status> {
        let jobs = self.kernel.list_jobs().await.map_err(to_status)?;
        Ok(Response::new(proto::ListJobsResponse {
            jobs: jobs.into_iter().map(job_record_to_proto).collect(),
        }))
    }

    async fn get_job(
        &self,
        request: Request<proto::GetJobRequest>,
    ) -> Result<Response<proto::JobRecord>, Status> {
        let request = request.into_inner();
        if request.job_id.is_empty() {
            return Err(Status::invalid_argument("job_id is required"));
        }
        let job = self
            .kernel
            .get_job(&request.job_id)
            .await
            .map_err(to_status)?;
        Ok(Response::new(job_record_to_proto(job)))
    }

    async fn cancel_job(
        &self,
        request: Request<proto::CancelJobRequest>,
    ) -> Result<Response<proto::JobRecord>, Status> {
        let request = request.into_inner();
        if request.job_id.is_empty() {
            return Err(Status::invalid_argument("job_id is required"));
        }
        let job = self
            .kernel
            .cancel_job(&request.job_id)
            .await
            .map_err(to_status)?;
        Ok(Response::new(job_record_to_proto(job)))
    }

    async fn get_attempt(
        &self,
        request: Request<proto::GetAttemptRequest>,
    ) -> Result<Response<proto::AttemptRecord>, Status> {
        let request = request.into_inner();
        if request.attempt_id.is_empty() {
            return Err(Status::invalid_argument("attempt_id is required"));
        }
        let attempt = self
            .kernel
            .get_attempt(&request.attempt_id)
            .await
            .map_err(to_status)?;
        Ok(Response::new(attempt_record_to_proto(attempt)))
    }

    async fn list_attempts(
        &self,
        request: Request<proto::ListAttemptsRequest>,
    ) -> Result<Response<proto::ListAttemptsResponse>, Status> {
        let request = request.into_inner();
        if request.job_id.is_empty() {
            return Err(Status::invalid_argument("job_id is required"));
        }
        let attempts = self
            .kernel
            .list_attempts(&request.job_id)
            .await
            .map_err(to_status)?;
        Ok(Response::new(proto::ListAttemptsResponse {
            attempts: attempts.into_iter().map(attempt_record_to_proto).collect(),
        }))
    }

    async fn register_worker(
        &self,
        request: Request<proto::RegisterWorkerRequest>,
    ) -> Result<Response<proto::RegisterWorkerResponse>, Status> {
        let request = request.into_inner();
        let spec = request
            .spec
            .ok_or_else(|| Status::invalid_argument("worker spec is required"))?;
        let worker = self
            .kernel
            .register_worker(
                empty_to_none(request.worker_id),
                worker_spec_from_proto(spec).map_err(to_status)?,
            )
            .await
            .map_err(to_status)?;

        Ok(Response::new(proto::RegisterWorkerResponse {
            worker: Some(worker_record_to_proto(worker)),
            heartbeat_ttl_secs: self.kernel.heartbeat_ttl_secs(),
        }))
    }

    async fn heartbeat_worker(
        &self,
        request: Request<proto::HeartbeatWorkerRequest>,
    ) -> Result<Response<proto::HeartbeatWorkerResponse>, Status> {
        let request = request.into_inner();
        if request.worker_id.is_empty() {
            return Err(Status::invalid_argument("worker_id is required"));
        }
        let worker = self
            .kernel
            .heartbeat_worker(
                &request.worker_id,
                capacity_from_proto(request.available_capacity),
                request.active_leases,
                request.metadata.into_iter().collect(),
            )
            .await
            .map_err(to_status)?;

        Ok(Response::new(proto::HeartbeatWorkerResponse {
            worker: Some(worker_record_to_proto(worker)),
            heartbeat_ttl_secs: self.kernel.heartbeat_ttl_secs(),
        }))
    }

    async fn poll_lease(
        &self,
        request: Request<proto::PollLeaseRequest>,
    ) -> Result<Response<proto::PollLeaseResponse>, Status> {
        let request = request.into_inner();
        if request.worker_id.is_empty() {
            return Err(Status::invalid_argument("worker_id is required"));
        }

        let assignment = self
            .kernel
            .poll_lease(&request.worker_id)
            .await
            .map_err(to_status)?;

        Ok(Response::new(match assignment {
            Some(assignment) => poll_lease_response_to_proto(assignment),
            None => proto::PollLeaseResponse {
                lease: None,
                job: None,
                attempt: None,
            },
        }))
    }

    async fn renew_lease(
        &self,
        request: Request<proto::RenewLeaseRequest>,
    ) -> Result<Response<proto::RenewLeaseResponse>, Status> {
        let request = request.into_inner();
        if request.worker_id.is_empty() || request.lease_id.is_empty() {
            return Err(Status::invalid_argument(
                "worker_id and lease_id are required",
            ));
        }
        let lease = self
            .kernel
            .renew_lease(&request.worker_id, &request.lease_id)
            .await
            .map_err(to_status)?;
        Ok(Response::new(proto::RenewLeaseResponse {
            lease: Some(lease_record_to_proto(lease)),
        }))
    }

    async fn report_event(
        &self,
        request: Request<proto::ReportEventRequest>,
    ) -> Result<Response<()>, Status> {
        let request = request.into_inner();
        if request.worker_id.is_empty() || request.lease_id.is_empty() {
            return Err(Status::invalid_argument(
                "worker_id and lease_id are required",
            ));
        }
        self.kernel
            .report_event(
                &request.worker_id,
                &request.lease_id,
                event_kind_from_proto(request.kind()).map_err(to_status)?,
                request.payload,
                request.metadata.into_iter().collect(),
            )
            .await
            .map_err(to_status)?;
        Ok(Response::new(()))
    }

    async fn complete_lease(
        &self,
        request: Request<proto::CompleteLeaseRequest>,
    ) -> Result<Response<()>, Status> {
        let request = request.into_inner();
        if request.worker_id.is_empty() || request.lease_id.is_empty() {
            return Err(Status::invalid_argument(
                "worker_id and lease_id are required",
            ));
        }
        let outputs = request
            .outputs
            .into_iter()
            .map(object_ref_from_proto)
            .collect::<Result<Vec<_>>>()
            .map_err(to_status)?;
        self.kernel
            .complete_lease(
                &request.worker_id,
                &request.lease_id,
                outputs,
                request.metadata.into_iter().collect(),
            )
            .await
            .map_err(to_status)?;
        Ok(Response::new(()))
    }

    async fn fail_lease(
        &self,
        request: Request<proto::FailLeaseRequest>,
    ) -> Result<Response<()>, Status> {
        let request = request.into_inner();
        if request.worker_id.is_empty() || request.lease_id.is_empty() {
            return Err(Status::invalid_argument(
                "worker_id and lease_id are required",
            ));
        }
        self.kernel
            .fail_lease(
                &request.worker_id,
                &request.lease_id,
                request.reason,
                request.retryable,
                request.metadata.into_iter().collect(),
            )
            .await
            .map_err(to_status)?;
        Ok(Response::new(()))
    }

    async fn open_stream(
        &self,
        request: Request<proto::OpenStreamRequest>,
    ) -> Result<Response<proto::OpenStreamResponse>, Status> {
        let request = request.into_inner();
        if request.job_id.is_empty() {
            return Err(Status::invalid_argument("job_id is required"));
        }
        let scope = stream_scope_from_proto(request.scope()).map_err(to_status)?;
        let direction = stream_direction_from_proto(request.direction()).map_err(to_status)?;
        let stream = self
            .kernel
            .open_stream(OpenStreamCommand {
                job_id: request.job_id,
                attempt_id: empty_to_none(request.attempt_id),
                worker_id: empty_to_none(request.worker_id),
                lease_id: empty_to_none(request.lease_id),
                stream_name: request.stream_name,
                scope,
                direction,
                metadata: request.metadata.into_iter().collect(),
            })
            .await
            .map_err(to_status)?;
        Ok(Response::new(proto::OpenStreamResponse {
            stream: Some(stream_record_to_proto(stream)),
        }))
    }

    async fn get_stream(
        &self,
        request: Request<proto::GetStreamRequest>,
    ) -> Result<Response<proto::StreamRecord>, Status> {
        let request = request.into_inner();
        if request.stream_id.is_empty() {
            return Err(Status::invalid_argument("stream_id is required"));
        }
        let stream = self
            .kernel
            .get_stream(&request.stream_id)
            .await
            .map_err(to_status)?;
        Ok(Response::new(stream_record_to_proto(stream)))
    }

    async fn list_streams(
        &self,
        request: Request<proto::ListStreamsRequest>,
    ) -> Result<Response<proto::ListStreamsResponse>, Status> {
        let request = request.into_inner();
        if request.job_id.is_empty() {
            return Err(Status::invalid_argument("job_id is required"));
        }
        let streams = self
            .kernel
            .list_streams(&request.job_id)
            .await
            .map_err(to_status)?;
        Ok(Response::new(proto::ListStreamsResponse {
            streams: streams.into_iter().map(stream_record_to_proto).collect(),
        }))
    }

    async fn close_stream(
        &self,
        request: Request<proto::CloseStreamRequest>,
    ) -> Result<Response<proto::StreamRecord>, Status> {
        let request = request.into_inner();
        if request.stream_id.is_empty() {
            return Err(Status::invalid_argument("stream_id is required"));
        }
        let stream = self
            .kernel
            .close_stream(
                &request.stream_id,
                empty_to_none(request.worker_id),
                empty_to_none(request.lease_id),
                request.metadata.into_iter().collect(),
            )
            .await
            .map_err(to_status)?;
        Ok(Response::new(stream_record_to_proto(stream)))
    }

    async fn push_frames(
        &self,
        request: Request<tonic::Streaming<proto::PushFramesRequest>>,
    ) -> Result<Response<proto::PushFramesResponse>, Status> {
        let mut stream = request.into_inner();
        let mut target_stream_id: Option<String> = None;
        let mut last_sequence = 0;
        let mut written_frames = 0_u64;

        while let Some(item) = stream.next().await {
            let item = item.map_err(to_status)?;
            if item.stream_id.is_empty() {
                return Err(Status::invalid_argument("stream_id is required"));
            }
            if let Some(existing) = target_stream_id.as_ref() {
                if existing != &item.stream_id {
                    return Err(Status::invalid_argument(
                        "PushFrames may only target one stream per request",
                    ));
                }
            } else {
                target_stream_id = Some(item.stream_id.clone());
            }
            let frame_kind = frame_kind_from_proto(item.kind()).map_err(to_status)?;

            let frame = self
                .kernel
                .push_frame(
                    &item.stream_id,
                    empty_to_none(item.worker_id),
                    empty_to_none(item.lease_id),
                    frame_kind,
                    item.payload,
                    item.metadata.into_iter().collect(),
                )
                .await
                .map_err(to_status)?;
            last_sequence = frame.sequence;
            written_frames += 1;
        }

        let response = match target_stream_id {
            Some(stream_id) => proto::PushFramesResponse {
                stream: Some(stream_record_to_proto(
                    self.kernel
                        .get_stream(&stream_id)
                        .await
                        .map_err(to_status)?,
                )),
                last_sequence,
                written_frames,
            },
            None => proto::PushFramesResponse {
                stream: None,
                last_sequence: 0,
                written_frames: 0,
            },
        };
        Ok(Response::new(response))
    }

    async fn pull_frames(
        &self,
        request: Request<proto::PullFramesRequest>,
    ) -> Result<Response<Self::PullFramesStream>, Status> {
        let request = request.into_inner();
        if request.stream_id.is_empty() {
            return Err(Status::invalid_argument("stream_id is required"));
        }

        let kernel = Arc::clone(&self.kernel);
        let stream_id = request.stream_id;
        let follow = request.follow;
        let stream = try_stream! {
            let mut cursor = request.after_sequence;
            let mut updates = kernel
                .subscribe_stream_updates(&stream_id)
                .await
                .map_err(to_status)?;

            loop {
                let frames = kernel.pull_frames(&stream_id, cursor).await.map_err(to_status)?;
                for frame in frames {
                    cursor = frame.sequence;
                    yield frame_to_proto(frame);
                }

                let stream = kernel.get_stream(&stream_id).await.map_err(to_status)?;
                if !follow || stream.state.is_terminal() {
                    let remaining = kernel.pull_frames(&stream_id, cursor).await.map_err(to_status)?;
                    if remaining.is_empty() {
                        break;
                    }
                    for frame in remaining {
                        yield frame_to_proto(frame);
                    }
                    break;
                }

                if updates.changed().await.is_err() {
                    break;
                }
            }
        };

        Ok(Response::new(Box::pin(stream)))
    }
}

fn poll_lease_response_to_proto(assignment: LeaseAssignment) -> proto::PollLeaseResponse {
    proto::PollLeaseResponse {
        lease: Some(lease_record_to_proto(assignment.lease)),
        job: Some(job_record_to_proto(assignment.job)),
        attempt: Some(attempt_record_to_proto(assignment.attempt)),
    }
}

fn job_spec_from_proto(spec: proto::JobSpec) -> Result<JobSpec> {
    Ok(JobSpec {
        interface_name: spec.interface_name,
        inputs: spec
            .inputs
            .into_iter()
            .map(object_ref_from_proto)
            .collect::<Result<Vec<_>>>()?,
        params: spec.params,
        demand: spec.demand.map(demand_from_proto).unwrap_or_default(),
        policy: spec
            .policy
            .map(execution_policy_from_proto)
            .unwrap_or_default(),
        metadata: spec.metadata.into_iter().collect(),
    })
}

fn demand_from_proto(demand: proto::Demand) -> Demand {
    Demand {
        required_attributes: demand.required_attributes.into_iter().collect(),
        preferred_attributes: demand.preferred_attributes.into_iter().collect(),
        required_capacity: capacity_from_proto(demand.required_capacity),
    }
}

fn execution_policy_from_proto(policy: proto::ExecutionPolicy) -> ExecutionPolicy {
    ExecutionPolicy {
        profile: if policy.profile.is_empty() {
            "basic".to_string()
        } else {
            policy.profile
        },
        priority: policy.priority,
        lease_ttl_secs: policy.lease_ttl_secs as u64,
    }
}

fn worker_spec_from_proto(spec: proto::WorkerSpec) -> Result<WorkerSpec> {
    let interfaces: BTreeSet<String> = spec.interfaces.into_iter().collect();
    if interfaces.is_empty() {
        bail!("worker spec must include at least one interface");
    }

    Ok(WorkerSpec {
        interfaces,
        attributes: spec.attributes.into_iter().collect(),
        total_capacity: capacity_from_proto(spec.total_capacity),
        max_active_leases: spec.max_active_leases,
        metadata: spec.metadata.into_iter().collect(),
    })
}

fn object_ref_from_proto(reference: proto::ObjectRef) -> Result<ObjectRef> {
    let metadata: Attributes = reference.metadata.into_iter().collect();
    match reference.reference {
        Some(proto::object_ref::Reference::Inline(content)) => {
            Ok(ObjectRef::Inline { content, metadata })
        }
        Some(proto::object_ref::Reference::Uri(uri)) => Ok(ObjectRef::Uri { uri, metadata }),
        None => bail!("object reference is missing inline content or uri"),
    }
}

fn capacity_from_proto(entries: Vec<proto::ResourceQuantity>) -> Capacity {
    entries
        .into_iter()
        .map(|entry| (entry.name, entry.value))
        .collect()
}

fn job_record_to_proto(job: JobRecord) -> proto::JobRecord {
    proto::JobRecord {
        job_id: job.job_id,
        state: job_state_to_proto(job.state) as i32,
        spec: Some(job_spec_to_proto(job.spec)),
        outputs: job.outputs.into_iter().map(object_ref_to_proto).collect(),
        lease_id: job.lease_id.unwrap_or_default(),
        version: job.version,
        created_at_ms: job.created_at_ms,
        updated_at_ms: job.updated_at_ms,
        last_error: job.last_error.unwrap_or_default(),
        current_attempt_id: job.current_attempt_id.unwrap_or_default(),
    }
}

fn job_spec_to_proto(spec: JobSpec) -> proto::JobSpec {
    proto::JobSpec {
        interface_name: spec.interface_name,
        inputs: spec.inputs.into_iter().map(object_ref_to_proto).collect(),
        params: spec.params,
        demand: Some(proto::Demand {
            required_attributes: spec.demand.required_attributes.into_iter().collect(),
            preferred_attributes: spec.demand.preferred_attributes.into_iter().collect(),
            required_capacity: capacity_to_proto(spec.demand.required_capacity),
        }),
        policy: Some(proto::ExecutionPolicy {
            profile: spec.policy.profile,
            priority: spec.policy.priority,
            lease_ttl_secs: spec.policy.lease_ttl_secs as u32,
        }),
        metadata: spec.metadata.into_iter().collect(),
    }
}

fn object_ref_to_proto(object: ObjectRef) -> proto::ObjectRef {
    match object {
        ObjectRef::Inline { content, metadata } => proto::ObjectRef {
            reference: Some(proto::object_ref::Reference::Inline(content)),
            metadata: metadata.into_iter().collect(),
        },
        ObjectRef::Uri { uri, metadata } => proto::ObjectRef {
            reference: Some(proto::object_ref::Reference::Uri(uri)),
            metadata: metadata.into_iter().collect(),
        },
    }
}

fn capacity_to_proto(capacity: Capacity) -> Vec<proto::ResourceQuantity> {
    capacity
        .into_iter()
        .map(|(name, value)| proto::ResourceQuantity { name, value })
        .collect()
}

fn attempt_record_to_proto(attempt: AttemptRecord) -> proto::AttemptRecord {
    proto::AttemptRecord {
        attempt_id: attempt.attempt_id,
        job_id: attempt.job_id,
        worker_id: attempt.worker_id,
        lease_id: attempt.lease_id,
        state: attempt_state_to_proto(attempt.state) as i32,
        created_at_ms: attempt.created_at_ms,
        started_at_ms: attempt.started_at_ms.unwrap_or_default(),
        finished_at_ms: attempt.finished_at_ms.unwrap_or_default(),
        last_error: attempt.last_error.unwrap_or_default(),
        metadata: attempt.metadata.into_iter().collect(),
    }
}

fn worker_record_to_proto(worker: WorkerRecord) -> proto::WorkerRecord {
    proto::WorkerRecord {
        worker_id: worker.worker_id,
        spec: Some(proto::WorkerSpec {
            interfaces: worker.spec.interfaces.into_iter().collect(),
            attributes: worker.spec.attributes.into_iter().collect(),
            total_capacity: capacity_to_proto(worker.spec.total_capacity),
            max_active_leases: worker.spec.max_active_leases,
            metadata: worker.spec.metadata.into_iter().collect(),
        }),
        status: Some(worker_status_to_proto(worker.status)),
        registered_at_ms: worker.registered_at_ms,
        expires_at_ms: worker.expires_at_ms,
    }
}

fn worker_status_to_proto(status: WorkerStatus) -> proto::WorkerStatus {
    proto::WorkerStatus {
        available_capacity: capacity_to_proto(status.available_capacity),
        active_leases: status.active_leases,
        metadata: status.metadata.into_iter().collect(),
        last_seen_at_ms: status.last_seen_at_ms,
    }
}

fn lease_record_to_proto(lease: LeaseRecord) -> proto::LeaseRecord {
    proto::LeaseRecord {
        lease_id: lease.lease_id,
        job_id: lease.job_id,
        worker_id: lease.worker_id,
        issued_at_ms: lease.issued_at_ms,
        expires_at_ms: lease.expires_at_ms,
    }
}

fn stream_record_to_proto(stream: StreamRecord) -> proto::StreamRecord {
    proto::StreamRecord {
        stream_id: stream.stream_id,
        job_id: stream.job_id,
        attempt_id: stream.attempt_id.unwrap_or_default(),
        lease_id: stream.lease_id.unwrap_or_default(),
        stream_name: stream.stream_name,
        scope: stream_scope_to_proto(stream.scope) as i32,
        direction: stream_direction_to_proto(stream.direction) as i32,
        state: stream_state_to_proto(stream.state) as i32,
        metadata: stream.metadata.into_iter().collect(),
        created_at_ms: stream.created_at_ms,
        closed_at_ms: stream.closed_at_ms.unwrap_or_default(),
        last_sequence: stream.last_sequence,
    }
}

fn frame_to_proto(frame: Frame) -> proto::Frame {
    proto::Frame {
        stream_id: frame.stream_id,
        sequence: frame.sequence,
        kind: frame_kind_to_proto(frame.kind) as i32,
        payload: frame.payload,
        metadata: frame.metadata.into_iter().collect(),
        created_at_ms: frame.created_at_ms,
    }
}

fn job_event_to_proto(event: JobEvent) -> proto::JobEvent {
    proto::JobEvent {
        job_id: event.job_id,
        sequence: event.sequence,
        kind: event_kind_to_proto(event.kind) as i32,
        payload: event.payload,
        metadata: event.metadata.into_iter().collect(),
        created_at_ms: event.created_at_ms,
    }
}

fn job_state_to_proto(state: JobState) -> proto::JobState {
    match state {
        JobState::Pending => proto::JobState::Pending,
        JobState::Leased => proto::JobState::Leased,
        JobState::Running => proto::JobState::Running,
        JobState::Succeeded => proto::JobState::Succeeded,
        JobState::Failed => proto::JobState::Failed,
        JobState::Cancelled => proto::JobState::Cancelled,
    }
}

fn attempt_state_to_proto(state: AttemptState) -> proto::AttemptState {
    match state {
        AttemptState::Created => proto::AttemptState::Created,
        AttemptState::Leased => proto::AttemptState::Leased,
        AttemptState::Running => proto::AttemptState::Running,
        AttemptState::Succeeded => proto::AttemptState::Succeeded,
        AttemptState::Failed => proto::AttemptState::Failed,
        AttemptState::Expired => proto::AttemptState::Expired,
        AttemptState::Cancelled => proto::AttemptState::Cancelled,
    }
}

fn stream_scope_to_proto(scope: StreamScope) -> proto::StreamScope {
    match scope {
        StreamScope::Job => proto::StreamScope::Job,
        StreamScope::Attempt => proto::StreamScope::Attempt,
        StreamScope::Lease => proto::StreamScope::Lease,
    }
}

fn stream_direction_to_proto(direction: StreamDirection) -> proto::StreamDirection {
    match direction {
        StreamDirection::ClientToWorker => proto::StreamDirection::ClientToWorker,
        StreamDirection::WorkerToClient => proto::StreamDirection::WorkerToClient,
        StreamDirection::Bidirectional => proto::StreamDirection::Bidirectional,
        StreamDirection::Internal => proto::StreamDirection::Internal,
    }
}

fn stream_state_to_proto(state: StreamState) -> proto::StreamState {
    match state {
        StreamState::Open => proto::StreamState::Open,
        StreamState::Closing => proto::StreamState::Closing,
        StreamState::Closed => proto::StreamState::Closed,
        StreamState::Error => proto::StreamState::Error,
    }
}

fn frame_kind_to_proto(kind: FrameKind) -> proto::FrameKind {
    match kind {
        FrameKind::Open => proto::FrameKind::Open,
        FrameKind::Data => proto::FrameKind::Data,
        FrameKind::Close => proto::FrameKind::Close,
        FrameKind::Error => proto::FrameKind::Error,
        FrameKind::Checkpoint => proto::FrameKind::Checkpoint,
        FrameKind::Control => proto::FrameKind::Control,
    }
}

fn event_kind_to_proto(kind: EventKind) -> proto::EventKind {
    match kind {
        EventKind::Accepted => proto::EventKind::Accepted,
        EventKind::LeaseGranted => proto::EventKind::LeaseGranted,
        EventKind::Started => proto::EventKind::Started,
        EventKind::Progress => proto::EventKind::Progress,
        EventKind::OutputReady => proto::EventKind::OutputReady,
        EventKind::Succeeded => proto::EventKind::Succeeded,
        EventKind::Failed => proto::EventKind::Failed,
        EventKind::Cancelled => proto::EventKind::Cancelled,
        EventKind::LeaseExpired => proto::EventKind::LeaseExpired,
        EventKind::Requeued => proto::EventKind::Requeued,
    }
}

fn event_kind_from_proto(kind: proto::EventKind) -> Result<EventKind> {
    match kind {
        proto::EventKind::Accepted => Ok(EventKind::Accepted),
        proto::EventKind::LeaseGranted => Ok(EventKind::LeaseGranted),
        proto::EventKind::Started => Ok(EventKind::Started),
        proto::EventKind::Progress => Ok(EventKind::Progress),
        proto::EventKind::OutputReady => Ok(EventKind::OutputReady),
        proto::EventKind::Succeeded => Ok(EventKind::Succeeded),
        proto::EventKind::Failed => Ok(EventKind::Failed),
        proto::EventKind::Cancelled => Ok(EventKind::Cancelled),
        proto::EventKind::LeaseExpired => Ok(EventKind::LeaseExpired),
        proto::EventKind::Requeued => Ok(EventKind::Requeued),
        proto::EventKind::Unspecified => bail!("event kind is required"),
    }
}

fn stream_scope_from_proto(scope: proto::StreamScope) -> Result<StreamScope> {
    match scope {
        proto::StreamScope::Job => Ok(StreamScope::Job),
        proto::StreamScope::Attempt => Ok(StreamScope::Attempt),
        proto::StreamScope::Lease => Ok(StreamScope::Lease),
        proto::StreamScope::Unspecified => bail!("stream scope is required"),
    }
}

fn stream_direction_from_proto(direction: proto::StreamDirection) -> Result<StreamDirection> {
    match direction {
        proto::StreamDirection::ClientToWorker => Ok(StreamDirection::ClientToWorker),
        proto::StreamDirection::WorkerToClient => Ok(StreamDirection::WorkerToClient),
        proto::StreamDirection::Bidirectional => Ok(StreamDirection::Bidirectional),
        proto::StreamDirection::Internal => Ok(StreamDirection::Internal),
        proto::StreamDirection::Unspecified => bail!("stream direction is required"),
    }
}

fn frame_kind_from_proto(kind: proto::FrameKind) -> Result<FrameKind> {
    match kind {
        proto::FrameKind::Open => Ok(FrameKind::Open),
        proto::FrameKind::Data => Ok(FrameKind::Data),
        proto::FrameKind::Close => Ok(FrameKind::Close),
        proto::FrameKind::Error => Ok(FrameKind::Error),
        proto::FrameKind::Checkpoint => Ok(FrameKind::Checkpoint),
        proto::FrameKind::Control => Ok(FrameKind::Control),
        proto::FrameKind::Unspecified => bail!("frame kind is required"),
    }
}

fn empty_to_none(value: String) -> Option<String> {
    if value.trim().is_empty() {
        None
    } else {
        Some(value)
    }
}

fn to_status(error: impl Into<Error>) -> Status {
    let error = error.into();
    Status::internal(error.to_string())
}
