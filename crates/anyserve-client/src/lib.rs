use std::collections::HashMap;

use anyhow::{Context, Result, bail};
pub use anyserve_proto::controlplane;
pub use anyserve_proto::controlplane::{
    AttemptRecord, AttemptState, EventKind, Frame, FrameKind, JobEvent, JobRecord, JobState,
    LeaseRecord, ObjectRef, StreamDirection, StreamRecord, StreamScope, StreamState, WorkerRecord,
    object_ref,
};
use anyserve_proto::controlplane::{
    CloseStreamRequest, CompleteLeaseRequest, Demand, ExecutionPolicy, FailLeaseRequest,
    GetAttemptRequest, GetJobRequest, GetStreamRequest, HeartbeatWorkerRequest, JobSpec,
    ListAttemptsRequest, ListJobsRequest, ListStreamsRequest, OpenStreamRequest, PollLeaseRequest,
    PullFramesRequest, PushFramesRequest, RegisterWorkerRequest, RenewLeaseRequest,
    ReportEventRequest, ResourceQuantity, SubmitJobRequest, WatchJobRequest, WorkerSpec,
    control_plane_service_client::ControlPlaneServiceClient,
};
use tonic::transport::Channel;
use tonic::{Request, Streaming};

pub struct AnyserveClient {
    endpoint: String,
    inner: ControlPlaneServiceClient<Channel>,
}

#[derive(Clone, Debug)]
pub struct JobSubmission {
    pub job_id: Option<String>,
    pub interface_name: String,
    pub inputs: Vec<ObjectRef>,
    pub params: Vec<u8>,
    pub required_attributes: HashMap<String, String>,
    pub preferred_attributes: HashMap<String, String>,
    pub required_capacity: HashMap<String, i64>,
    pub metadata: HashMap<String, String>,
    pub profile: String,
    pub priority: i32,
    pub lease_ttl_secs: u32,
}

impl JobSubmission {
    pub fn new(interface_name: impl Into<String>) -> Self {
        Self {
            job_id: None,
            interface_name: interface_name.into(),
            inputs: Vec::new(),
            params: Vec::new(),
            required_attributes: HashMap::new(),
            preferred_attributes: HashMap::new(),
            required_capacity: HashMap::new(),
            metadata: HashMap::new(),
            profile: "basic".to_string(),
            priority: 0,
            lease_ttl_secs: 30,
        }
    }
}

#[derive(Clone, Debug)]
pub struct WorkerRegistration {
    pub worker_id: Option<String>,
    pub interfaces: Vec<String>,
    pub attributes: HashMap<String, String>,
    pub total_capacity: HashMap<String, i64>,
    pub max_active_leases: u32,
    pub metadata: HashMap<String, String>,
}

impl WorkerRegistration {
    pub fn new(interfaces: Vec<String>) -> Self {
        Self {
            worker_id: None,
            interfaces,
            attributes: HashMap::new(),
            total_capacity: HashMap::new(),
            max_active_leases: 1,
            metadata: HashMap::new(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct StreamOpen {
    pub job_id: String,
    pub stream_name: String,
    pub scope: StreamScope,
    pub direction: StreamDirection,
    pub metadata: HashMap<String, String>,
    pub attempt_id: Option<String>,
    pub worker_id: Option<String>,
    pub lease_id: Option<String>,
}

impl StreamOpen {
    pub fn job(
        job_id: impl Into<String>,
        stream_name: impl Into<String>,
        direction: StreamDirection,
    ) -> Self {
        Self {
            job_id: job_id.into(),
            stream_name: stream_name.into(),
            scope: StreamScope::Job,
            direction,
            metadata: HashMap::new(),
            attempt_id: None,
            worker_id: None,
            lease_id: None,
        }
    }

    pub fn with_scope(mut self, scope: StreamScope) -> Self {
        self.scope = scope;
        self
    }

    pub fn with_metadata(mut self, metadata: HashMap<String, String>) -> Self {
        self.metadata = metadata;
        self
    }

    pub fn with_attempt_id(mut self, attempt_id: impl Into<String>) -> Self {
        self.attempt_id = Some(attempt_id.into());
        self
    }

    pub fn with_worker_id(mut self, worker_id: impl Into<String>) -> Self {
        self.worker_id = Some(worker_id.into());
        self
    }

    pub fn with_lease_id(mut self, lease_id: impl Into<String>) -> Self {
        self.lease_id = Some(lease_id.into());
        self
    }
}

#[derive(Clone, Debug)]
pub struct FrameWrite {
    pub kind: FrameKind,
    pub payload: Vec<u8>,
    pub metadata: HashMap<String, String>,
}

impl FrameWrite {
    pub fn new(kind: FrameKind, payload: impl Into<Vec<u8>>) -> Self {
        Self {
            kind,
            payload: payload.into(),
            metadata: HashMap::new(),
        }
    }

    pub fn data(payload: impl Into<Vec<u8>>) -> Self {
        Self::new(FrameKind::Data, payload)
    }

    pub fn with_metadata(mut self, metadata: HashMap<String, String>) -> Self {
        self.metadata = metadata;
        self
    }
}

#[derive(Clone, Debug)]
pub struct PushSummary {
    pub stream: Option<StreamRecord>,
    pub last_sequence: u64,
    pub written_frames: u64,
}

#[derive(Clone, Debug)]
pub struct LeaseGrant {
    pub lease: LeaseRecord,
    pub attempt: AttemptRecord,
    pub job: JobRecord,
}

impl AnyserveClient {
    pub async fn connect(endpoint: impl Into<String>) -> Result<Self> {
        let endpoint = endpoint.into();
        let inner = ControlPlaneServiceClient::connect(endpoint.clone())
            .await
            .context("connect anyserve gRPC endpoint")?;
        Ok(Self { endpoint, inner })
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub fn inner_mut(&mut self) -> &mut ControlPlaneServiceClient<Channel> {
        &mut self.inner
    }

    pub fn into_inner(self) -> ControlPlaneServiceClient<Channel> {
        self.inner
    }

    pub async fn submit_job(&mut self, request: JobSubmission) -> Result<JobRecord> {
        let response = self
            .inner
            .submit_job(Request::new(SubmitJobRequest {
                job_id: request.job_id.unwrap_or_default(),
                spec: Some(JobSpec {
                    interface_name: request.interface_name,
                    inputs: request.inputs,
                    params: request.params,
                    demand: Some(Demand {
                        required_attributes: request.required_attributes,
                        preferred_attributes: request.preferred_attributes,
                        required_capacity: capacity_to_proto(request.required_capacity),
                    }),
                    policy: Some(ExecutionPolicy {
                        profile: request.profile,
                        priority: request.priority,
                        lease_ttl_secs: request.lease_ttl_secs,
                    }),
                    metadata: request.metadata,
                }),
            }))
            .await
            .context("submit job")?
            .into_inner();

        response.job.context("missing job in submit response")
    }

    pub async fn watch_job(
        &mut self,
        job_id: impl Into<String>,
        after_sequence: u64,
    ) -> Result<Streaming<JobEvent>> {
        Ok(self
            .inner
            .watch_job(Request::new(WatchJobRequest {
                job_id: job_id.into(),
                after_sequence,
            }))
            .await
            .context("watch job")?
            .into_inner())
    }

    pub async fn get_job(&mut self, job_id: impl Into<String>) -> Result<JobRecord> {
        Ok(self
            .inner
            .get_job(Request::new(GetJobRequest {
                job_id: job_id.into(),
            }))
            .await
            .context("get job")?
            .into_inner())
    }

    pub async fn list_jobs(&mut self) -> Result<Vec<JobRecord>> {
        Ok(self
            .inner
            .list_jobs(Request::new(ListJobsRequest {}))
            .await
            .context("list jobs")?
            .into_inner()
            .jobs)
    }

    pub async fn cancel_job(&mut self, job_id: impl Into<String>) -> Result<JobRecord> {
        Ok(self
            .inner
            .cancel_job(Request::new(controlplane::CancelJobRequest {
                job_id: job_id.into(),
            }))
            .await
            .context("cancel job")?
            .into_inner())
    }

    pub async fn get_attempt(&mut self, attempt_id: impl Into<String>) -> Result<AttemptRecord> {
        Ok(self
            .inner
            .get_attempt(Request::new(GetAttemptRequest {
                attempt_id: attempt_id.into(),
            }))
            .await
            .context("get attempt")?
            .into_inner())
    }

    pub async fn list_attempts(&mut self, job_id: impl Into<String>) -> Result<Vec<AttemptRecord>> {
        Ok(self
            .inner
            .list_attempts(Request::new(ListAttemptsRequest {
                job_id: job_id.into(),
            }))
            .await
            .context("list attempts")?
            .into_inner()
            .attempts)
    }

    pub async fn register_worker(&mut self, request: WorkerRegistration) -> Result<WorkerRecord> {
        let response = self
            .inner
            .register_worker(Request::new(RegisterWorkerRequest {
                worker_id: request.worker_id.unwrap_or_default(),
                spec: Some(WorkerSpec {
                    interfaces: request.interfaces,
                    attributes: request.attributes,
                    total_capacity: capacity_to_proto(request.total_capacity),
                    max_active_leases: request.max_active_leases,
                    metadata: request.metadata,
                }),
            }))
            .await
            .context("register worker")?
            .into_inner();

        response
            .worker
            .context("missing worker in register response")
    }

    pub async fn heartbeat_worker(
        &mut self,
        worker_id: impl Into<String>,
        available_capacity: HashMap<String, i64>,
        active_leases: u32,
        metadata: HashMap<String, String>,
    ) -> Result<WorkerRecord> {
        let response = self
            .inner
            .heartbeat_worker(Request::new(HeartbeatWorkerRequest {
                worker_id: worker_id.into(),
                available_capacity: capacity_to_proto(available_capacity),
                active_leases,
                metadata,
            }))
            .await
            .context("heartbeat worker")?
            .into_inner();

        response
            .worker
            .context("missing worker in heartbeat response")
    }

    pub async fn poll_lease(&mut self, worker_id: impl Into<String>) -> Result<Option<LeaseGrant>> {
        let response = self
            .inner
            .poll_lease(Request::new(PollLeaseRequest {
                worker_id: worker_id.into(),
            }))
            .await
            .context("poll lease")?
            .into_inner();

        match (response.lease, response.attempt, response.job) {
            (Some(lease), Some(attempt), Some(job)) => Ok(Some(LeaseGrant {
                lease,
                attempt,
                job,
            })),
            (None, None, None) => Ok(None),
            _ => bail!("lease grant response was incomplete"),
        }
    }

    pub async fn renew_lease(
        &mut self,
        worker_id: impl Into<String>,
        lease_id: impl Into<String>,
    ) -> Result<LeaseRecord> {
        let response = self
            .inner
            .renew_lease(Request::new(RenewLeaseRequest {
                worker_id: worker_id.into(),
                lease_id: lease_id.into(),
            }))
            .await
            .context("renew lease")?
            .into_inner();

        response.lease.context("missing lease in renew response")
    }

    pub async fn report_event(
        &mut self,
        worker_id: impl Into<String>,
        lease_id: impl Into<String>,
        kind: EventKind,
        payload: Vec<u8>,
        metadata: HashMap<String, String>,
    ) -> Result<()> {
        self.inner
            .report_event(Request::new(ReportEventRequest {
                worker_id: worker_id.into(),
                lease_id: lease_id.into(),
                kind: kind as i32,
                payload,
                metadata,
            }))
            .await
            .context("report event")?;
        Ok(())
    }

    pub async fn complete_lease(
        &mut self,
        worker_id: impl Into<String>,
        lease_id: impl Into<String>,
        outputs: Vec<ObjectRef>,
        metadata: HashMap<String, String>,
    ) -> Result<()> {
        self.inner
            .complete_lease(Request::new(CompleteLeaseRequest {
                worker_id: worker_id.into(),
                lease_id: lease_id.into(),
                outputs,
                metadata,
            }))
            .await
            .context("complete lease")?;
        Ok(())
    }

    pub async fn fail_lease(
        &mut self,
        worker_id: impl Into<String>,
        lease_id: impl Into<String>,
        reason: impl Into<String>,
        retryable: bool,
        metadata: HashMap<String, String>,
    ) -> Result<()> {
        self.inner
            .fail_lease(Request::new(FailLeaseRequest {
                worker_id: worker_id.into(),
                lease_id: lease_id.into(),
                reason: reason.into(),
                retryable,
                metadata,
            }))
            .await
            .context("fail lease")?;
        Ok(())
    }

    pub async fn open_stream(&mut self, request: StreamOpen) -> Result<StreamRecord> {
        let response = self
            .inner
            .open_stream(Request::new(OpenStreamRequest {
                job_id: request.job_id,
                attempt_id: request.attempt_id.unwrap_or_default(),
                worker_id: request.worker_id.unwrap_or_default(),
                lease_id: request.lease_id.unwrap_or_default(),
                stream_name: request.stream_name,
                scope: request.scope as i32,
                direction: request.direction as i32,
                metadata: request.metadata,
            }))
            .await
            .context("open stream")?
            .into_inner();

        response.stream.context("missing stream in open response")
    }

    pub async fn get_stream(&mut self, stream_id: impl Into<String>) -> Result<StreamRecord> {
        Ok(self
            .inner
            .get_stream(Request::new(GetStreamRequest {
                stream_id: stream_id.into(),
            }))
            .await
            .context("get stream")?
            .into_inner())
    }

    pub async fn list_streams(&mut self, job_id: impl Into<String>) -> Result<Vec<StreamRecord>> {
        Ok(self
            .inner
            .list_streams(Request::new(ListStreamsRequest {
                job_id: job_id.into(),
            }))
            .await
            .context("list streams")?
            .into_inner()
            .streams)
    }

    pub async fn close_stream(
        &mut self,
        stream_id: impl Into<String>,
        worker_id: Option<String>,
        lease_id: Option<String>,
        metadata: HashMap<String, String>,
    ) -> Result<StreamRecord> {
        Ok(self
            .inner
            .close_stream(Request::new(CloseStreamRequest {
                stream_id: stream_id.into(),
                worker_id: worker_id.unwrap_or_default(),
                lease_id: lease_id.unwrap_or_default(),
                metadata,
            }))
            .await
            .context("close stream")?
            .into_inner())
    }

    pub async fn push_frames(
        &mut self,
        stream_id: impl Into<String>,
        frames: impl IntoIterator<Item = FrameWrite>,
        worker_id: Option<String>,
        lease_id: Option<String>,
    ) -> Result<PushSummary> {
        let stream_id = stream_id.into();
        let worker_id = worker_id.unwrap_or_default();
        let lease_id = lease_id.unwrap_or_default();
        let requests = frames
            .into_iter()
            .map(|frame| PushFramesRequest {
                stream_id: stream_id.clone(),
                worker_id: worker_id.clone(),
                lease_id: lease_id.clone(),
                kind: frame.kind as i32,
                payload: frame.payload,
                metadata: frame.metadata,
            })
            .collect::<Vec<_>>();

        let response = self
            .inner
            .push_frames(Request::new(tokio_stream::iter(requests)))
            .await
            .context("push frames")?
            .into_inner();

        Ok(PushSummary {
            stream: response.stream,
            last_sequence: response.last_sequence,
            written_frames: response.written_frames,
        })
    }

    pub async fn pull_frames(
        &mut self,
        stream_id: impl Into<String>,
        after_sequence: u64,
        follow: bool,
    ) -> Result<Streaming<Frame>> {
        Ok(self
            .inner
            .pull_frames(Request::new(PullFramesRequest {
                stream_id: stream_id.into(),
                after_sequence,
                follow,
            }))
            .await
            .context("pull frames")?
            .into_inner())
    }
}

fn capacity_to_proto(capacity: HashMap<String, i64>) -> Vec<ResourceQuantity> {
    capacity
        .into_iter()
        .map(|(name, value)| ResourceQuantity { name, value })
        .collect()
}
