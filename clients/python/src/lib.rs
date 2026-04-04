use std::collections::HashMap;
use std::future::Future;

use anyhow::{Context, Result, bail};
use anyserve_proto::controlplane::control_plane_service_client::ControlPlaneServiceClient;
use anyserve_proto::controlplane::{
    AttemptRecord, CloseStreamRequest, CompleteLeaseRequest, Demand, EventKind, ExecutionPolicy,
    FailLeaseRequest, Frame, FrameKind, GetAttemptRequest, GetJobRequest, GetStreamRequest,
    HeartbeatWorkerRequest, JobEvent, JobRecord, JobSpec, ListAttemptsRequest, ListStreamsRequest,
    ObjectRef, OpenStreamRequest, PollLeaseRequest, PullFramesRequest, PushFramesRequest,
    RegisterWorkerRequest, RenewLeaseRequest, ReportEventRequest, ResourceQuantity,
    StreamDirection, StreamRecord, StreamScope, SubmitJobRequest, WatchJobRequest, WorkerRecord,
    WorkerSpec, WorkerStatus, object_ref,
};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};
use tokio::runtime::Builder;
use tokio_stream::StreamExt;
use tonic::Request;

#[derive(FromPyObject)]
struct PyObjectRef {
    #[pyo3(item("inline"))]
    inline: Option<Vec<u8>>,
    #[pyo3(item("uri"))]
    uri: Option<String>,
    #[pyo3(item("metadata"))]
    metadata: Option<HashMap<String, String>>,
}

#[pyclass]
#[derive(Clone)]
struct AnyserveClient {
    endpoint: String,
}

struct SubmitJobCall {
    interface_name: String,
    inputs: Vec<PyObjectRef>,
    params: Vec<u8>,
    required_attributes: HashMap<String, String>,
    preferred_attributes: HashMap<String, String>,
    required_capacity: HashMap<String, i64>,
    metadata: HashMap<String, String>,
    profile: String,
    priority: i32,
    lease_ttl_secs: u32,
    job_id: Option<String>,
}

struct RegisterWorkerCall {
    interfaces: Vec<String>,
    attributes: HashMap<String, String>,
    total_capacity: HashMap<String, i64>,
    max_active_leases: u32,
    metadata: HashMap<String, String>,
    worker_id: Option<String>,
}

struct OpenStreamCall {
    job_id: String,
    stream_name: String,
    scope: StreamScope,
    direction: StreamDirection,
    metadata: HashMap<String, String>,
    attempt_id: Option<String>,
    worker_id: Option<String>,
    lease_id: Option<String>,
}

#[pymethods]
impl AnyserveClient {
    #[new]
    fn new(endpoint: String) -> Self {
        Self { endpoint }
    }

    #[pyo3(signature = (
        interface_name,
        inputs=None,
        params=None,
        required_attributes=None,
        preferred_attributes=None,
        required_capacity=None,
        metadata=None,
        profile=None,
        priority=0,
        lease_ttl_secs=None,
        job_id=None
    ))]
    #[allow(clippy::too_many_arguments, reason = "Preserve the Python keyword API")]
    fn submit_job(
        &self,
        py: Python<'_>,
        interface_name: String,
        inputs: Option<Vec<PyObjectRef>>,
        params: Option<Vec<u8>>,
        required_attributes: Option<HashMap<String, String>>,
        preferred_attributes: Option<HashMap<String, String>>,
        required_capacity: Option<HashMap<String, i64>>,
        metadata: Option<HashMap<String, String>>,
        profile: Option<String>,
        priority: i32,
        lease_ttl_secs: Option<u32>,
        job_id: Option<String>,
    ) -> PyResult<Py<PyDict>> {
        let endpoint = self.endpoint.clone();
        let job = py
            .allow_threads(move || {
                submit_job_impl(
                    endpoint,
                    SubmitJobCall {
                        interface_name,
                        inputs: inputs.unwrap_or_default(),
                        params: params.unwrap_or_default(),
                        required_attributes: required_attributes.unwrap_or_default(),
                        preferred_attributes: preferred_attributes.unwrap_or_default(),
                        required_capacity: required_capacity.unwrap_or_default(),
                        metadata: metadata.unwrap_or_default(),
                        profile: profile.unwrap_or_else(|| "basic".to_string()),
                        priority,
                        lease_ttl_secs: lease_ttl_secs.unwrap_or(30),
                        job_id,
                    },
                )
            })
            .map_err(to_py_err)?;
        job_record_to_py(py, job)
    }

    #[pyo3(signature = (job_id, after_sequence=0))]
    fn watch_job(
        &self,
        py: Python<'_>,
        job_id: String,
        after_sequence: u64,
    ) -> PyResult<Vec<Py<PyDict>>> {
        let endpoint = self.endpoint.clone();
        let events = py
            .allow_threads(move || watch_job_impl(endpoint, job_id, after_sequence))
            .map_err(to_py_err)?;
        events
            .into_iter()
            .map(|event| job_event_to_py(py, event))
            .collect()
    }

    fn get_job(&self, py: Python<'_>, job_id: String) -> PyResult<Py<PyDict>> {
        let endpoint = self.endpoint.clone();
        let job = py
            .allow_threads(move || get_job_impl(endpoint, job_id))
            .map_err(to_py_err)?;
        job_record_to_py(py, job)
    }

    fn cancel_job(&self, py: Python<'_>, job_id: String) -> PyResult<Py<PyDict>> {
        let endpoint = self.endpoint.clone();
        let job = py
            .allow_threads(move || cancel_job_impl(endpoint, job_id))
            .map_err(to_py_err)?;
        job_record_to_py(py, job)
    }

    fn get_attempt(&self, py: Python<'_>, attempt_id: String) -> PyResult<Py<PyDict>> {
        let endpoint = self.endpoint.clone();
        let attempt = py
            .allow_threads(move || get_attempt_impl(endpoint, attempt_id))
            .map_err(to_py_err)?;
        attempt_record_to_py(py, attempt)
    }

    fn list_attempts(&self, py: Python<'_>, job_id: String) -> PyResult<Vec<Py<PyDict>>> {
        let endpoint = self.endpoint.clone();
        let attempts = py
            .allow_threads(move || list_attempts_impl(endpoint, job_id))
            .map_err(to_py_err)?;
        attempts
            .into_iter()
            .map(|attempt| attempt_record_to_py(py, attempt))
            .collect()
    }

    #[pyo3(signature = (
        interfaces,
        attributes=None,
        total_capacity=None,
        max_active_leases=1,
        metadata=None,
        worker_id=None
    ))]
    #[allow(clippy::too_many_arguments, reason = "Preserve the Python keyword API")]
    fn register_worker(
        &self,
        py: Python<'_>,
        interfaces: Vec<String>,
        attributes: Option<HashMap<String, String>>,
        total_capacity: Option<HashMap<String, i64>>,
        max_active_leases: u32,
        metadata: Option<HashMap<String, String>>,
        worker_id: Option<String>,
    ) -> PyResult<Py<PyDict>> {
        let endpoint = self.endpoint.clone();
        let worker = py
            .allow_threads(move || {
                register_worker_impl(
                    endpoint,
                    RegisterWorkerCall {
                        interfaces,
                        attributes: attributes.unwrap_or_default(),
                        total_capacity: total_capacity.unwrap_or_default(),
                        max_active_leases,
                        metadata: metadata.unwrap_or_default(),
                        worker_id,
                    },
                )
            })
            .map_err(to_py_err)?;
        worker_record_to_py(py, worker)
    }

    #[pyo3(signature = (worker_id, available_capacity=None, active_leases=0, metadata=None))]
    fn heartbeat_worker(
        &self,
        py: Python<'_>,
        worker_id: String,
        available_capacity: Option<HashMap<String, i64>>,
        active_leases: u32,
        metadata: Option<HashMap<String, String>>,
    ) -> PyResult<Py<PyDict>> {
        let endpoint = self.endpoint.clone();
        let worker = py
            .allow_threads(move || {
                heartbeat_worker_impl(
                    endpoint,
                    worker_id,
                    available_capacity.unwrap_or_default(),
                    active_leases,
                    metadata.unwrap_or_default(),
                )
            })
            .map_err(to_py_err)?;
        worker_record_to_py(py, worker)
    }

    fn poll_lease(&self, py: Python<'_>, worker_id: String) -> PyResult<Option<Py<PyDict>>> {
        let endpoint = self.endpoint.clone();
        let lease = py
            .allow_threads(move || poll_lease_impl(endpoint, worker_id))
            .map_err(to_py_err)?;
        lease
            .map(|(lease, attempt, job)| lease_grant_to_py(py, lease, attempt, job))
            .transpose()
    }

    fn renew_lease(
        &self,
        py: Python<'_>,
        worker_id: String,
        lease_id: String,
    ) -> PyResult<Py<PyDict>> {
        let endpoint = self.endpoint.clone();
        let lease = py
            .allow_threads(move || renew_lease_impl(endpoint, worker_id, lease_id))
            .map_err(to_py_err)?;
        lease_record_to_py(py, lease)
    }

    #[pyo3(signature = (worker_id, lease_id, kind, payload=None, metadata=None))]
    fn report_event(
        &self,
        py: Python<'_>,
        worker_id: String,
        lease_id: String,
        kind: String,
        payload: Option<Vec<u8>>,
        metadata: Option<HashMap<String, String>>,
    ) -> PyResult<()> {
        let endpoint = self.endpoint.clone();
        py.allow_threads(move || {
            report_event_impl(
                endpoint,
                worker_id,
                lease_id,
                parse_event_kind(&kind)?,
                payload.unwrap_or_default(),
                metadata.unwrap_or_default(),
            )
        })
        .map_err(to_py_err)?;
        Ok(())
    }

    #[pyo3(signature = (worker_id, lease_id, outputs=None, metadata=None))]
    fn complete_lease(
        &self,
        py: Python<'_>,
        worker_id: String,
        lease_id: String,
        outputs: Option<Vec<PyObjectRef>>,
        metadata: Option<HashMap<String, String>>,
    ) -> PyResult<()> {
        let endpoint = self.endpoint.clone();
        py.allow_threads(move || {
            complete_lease_impl(
                endpoint,
                worker_id,
                lease_id,
                outputs.unwrap_or_default(),
                metadata.unwrap_or_default(),
            )
        })
        .map_err(to_py_err)?;
        Ok(())
    }

    #[pyo3(signature = (worker_id, lease_id, reason, retryable=false, metadata=None))]
    fn fail_lease(
        &self,
        py: Python<'_>,
        worker_id: String,
        lease_id: String,
        reason: String,
        retryable: bool,
        metadata: Option<HashMap<String, String>>,
    ) -> PyResult<()> {
        let endpoint = self.endpoint.clone();
        py.allow_threads(move || {
            fail_lease_impl(
                endpoint,
                worker_id,
                lease_id,
                reason,
                retryable,
                metadata.unwrap_or_default(),
            )
        })
        .map_err(to_py_err)?;
        Ok(())
    }

    #[pyo3(signature = (
        job_id,
        stream_name,
        scope="job".to_string(),
        direction="client_to_worker".to_string(),
        metadata=None,
        attempt_id=None,
        worker_id=None,
        lease_id=None
    ))]
    #[allow(clippy::too_many_arguments, reason = "Preserve the Python keyword API")]
    fn open_stream(
        &self,
        py: Python<'_>,
        job_id: String,
        stream_name: String,
        scope: String,
        direction: String,
        metadata: Option<HashMap<String, String>>,
        attempt_id: Option<String>,
        worker_id: Option<String>,
        lease_id: Option<String>,
    ) -> PyResult<Py<PyDict>> {
        let endpoint = self.endpoint.clone();
        let stream = py
            .allow_threads(move || {
                open_stream_impl(
                    endpoint,
                    OpenStreamCall {
                        job_id,
                        stream_name,
                        scope: parse_stream_scope(&scope)?,
                        direction: parse_stream_direction(&direction)?,
                        metadata: metadata.unwrap_or_default(),
                        attempt_id,
                        worker_id,
                        lease_id,
                    },
                )
            })
            .map_err(to_py_err)?;
        stream_record_to_py(py, stream)
    }

    fn get_stream(&self, py: Python<'_>, stream_id: String) -> PyResult<Py<PyDict>> {
        let endpoint = self.endpoint.clone();
        let stream = py
            .allow_threads(move || get_stream_impl(endpoint, stream_id))
            .map_err(to_py_err)?;
        stream_record_to_py(py, stream)
    }

    fn list_streams(&self, py: Python<'_>, job_id: String) -> PyResult<Vec<Py<PyDict>>> {
        let endpoint = self.endpoint.clone();
        let streams = py
            .allow_threads(move || list_streams_impl(endpoint, job_id))
            .map_err(to_py_err)?;
        streams
            .into_iter()
            .map(|stream| stream_record_to_py(py, stream))
            .collect()
    }

    #[pyo3(signature = (stream_id, worker_id=None, lease_id=None, metadata=None))]
    fn close_stream(
        &self,
        py: Python<'_>,
        stream_id: String,
        worker_id: Option<String>,
        lease_id: Option<String>,
        metadata: Option<HashMap<String, String>>,
    ) -> PyResult<Py<PyDict>> {
        let endpoint = self.endpoint.clone();
        let stream = py
            .allow_threads(move || {
                close_stream_impl(
                    endpoint,
                    stream_id,
                    worker_id,
                    lease_id,
                    metadata.unwrap_or_default(),
                )
            })
            .map_err(to_py_err)?;
        stream_record_to_py(py, stream)
    }

    #[pyo3(signature = (stream_id, frames, worker_id=None, lease_id=None))]
    fn push_frames(
        &self,
        py: Python<'_>,
        stream_id: String,
        frames: Vec<(String, Vec<u8>, HashMap<String, String>)>,
        worker_id: Option<String>,
        lease_id: Option<String>,
    ) -> PyResult<Py<PyDict>> {
        let endpoint = self.endpoint.clone();
        let summary = py
            .allow_threads(move || {
                push_frames_impl(endpoint, stream_id, frames, worker_id, lease_id)
            })
            .map_err(to_py_err)?;
        push_summary_to_py(py, summary)
    }

    #[pyo3(signature = (stream_id, after_sequence=0, follow=false))]
    fn pull_frames(
        &self,
        py: Python<'_>,
        stream_id: String,
        after_sequence: u64,
        follow: bool,
    ) -> PyResult<Vec<Py<PyDict>>> {
        let endpoint = self.endpoint.clone();
        let frames = py
            .allow_threads(move || pull_frames_impl(endpoint, stream_id, after_sequence, follow))
            .map_err(to_py_err)?;
        frames
            .into_iter()
            .map(|frame| frame_to_py(py, frame))
            .collect()
    }

    fn __repr__(&self) -> String {
        format!("AnyserveClient(endpoint={:?})", self.endpoint)
    }
}

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<AnyserveClient>()?;
    Ok(())
}

fn submit_job_impl(endpoint: String, request: SubmitJobCall) -> Result<JobRecord> {
    let SubmitJobCall {
        interface_name,
        inputs,
        params,
        required_attributes,
        preferred_attributes,
        required_capacity,
        metadata,
        profile,
        priority,
        lease_ttl_secs,
        job_id,
    } = request;
    run_async(async move {
        let mut client = ControlPlaneServiceClient::connect(endpoint)
            .await
            .context("connect anyserve gRPC endpoint")?;

        let response = client
            .submit_job(Request::new(SubmitJobRequest {
                job_id: job_id.unwrap_or_default(),
                spec: Some(JobSpec {
                    interface_name,
                    inputs: py_objects_to_proto(inputs)?,
                    params,
                    demand: Some(Demand {
                        required_attributes,
                        preferred_attributes,
                        required_capacity: capacity_to_proto(required_capacity),
                    }),
                    policy: Some(ExecutionPolicy {
                        profile,
                        priority,
                        lease_ttl_secs,
                    }),
                    metadata,
                }),
            }))
            .await
            .context("submit job")?
            .into_inner();

        response.job.context("missing job in submit response")
    })
}

fn watch_job_impl(endpoint: String, job_id: String, after_sequence: u64) -> Result<Vec<JobEvent>> {
    run_async(async move {
        let mut client = ControlPlaneServiceClient::connect(endpoint)
            .await
            .context("connect anyserve gRPC endpoint")?;
        let mut stream = client
            .watch_job(Request::new(WatchJobRequest {
                job_id,
                after_sequence,
            }))
            .await
            .context("watch job")?
            .into_inner();

        let mut events = Vec::new();
        while let Some(item) = stream.next().await {
            events.push(item.context("receive job event")?);
        }
        Ok(events)
    })
}

fn get_job_impl(endpoint: String, job_id: String) -> Result<JobRecord> {
    run_async(async move {
        let mut client = ControlPlaneServiceClient::connect(endpoint)
            .await
            .context("connect anyserve gRPC endpoint")?;
        client
            .get_job(Request::new(GetJobRequest { job_id }))
            .await
            .context("get job")?
            .into_inner()
            .pipe(Ok)
    })
}

fn cancel_job_impl(endpoint: String, job_id: String) -> Result<JobRecord> {
    run_async(async move {
        let mut client = ControlPlaneServiceClient::connect(endpoint)
            .await
            .context("connect anyserve gRPC endpoint")?;
        client
            .cancel_job(Request::new(
                anyserve_proto::controlplane::CancelJobRequest { job_id },
            ))
            .await
            .context("cancel job")?
            .into_inner()
            .pipe(Ok)
    })
}

fn get_attempt_impl(endpoint: String, attempt_id: String) -> Result<AttemptRecord> {
    run_async(async move {
        let mut client = ControlPlaneServiceClient::connect(endpoint)
            .await
            .context("connect anyserve gRPC endpoint")?;
        client
            .get_attempt(Request::new(GetAttemptRequest { attempt_id }))
            .await
            .context("get attempt")?
            .into_inner()
            .pipe(Ok)
    })
}

fn list_attempts_impl(endpoint: String, job_id: String) -> Result<Vec<AttemptRecord>> {
    run_async(async move {
        let mut client = ControlPlaneServiceClient::connect(endpoint)
            .await
            .context("connect anyserve gRPC endpoint")?;
        Ok(client
            .list_attempts(Request::new(ListAttemptsRequest { job_id }))
            .await
            .context("list attempts")?
            .into_inner()
            .attempts)
    })
}

fn register_worker_impl(endpoint: String, request: RegisterWorkerCall) -> Result<WorkerRecord> {
    let RegisterWorkerCall {
        interfaces,
        attributes,
        total_capacity,
        max_active_leases,
        metadata,
        worker_id,
    } = request;
    run_async(async move {
        let mut client = ControlPlaneServiceClient::connect(endpoint)
            .await
            .context("connect anyserve gRPC endpoint")?;
        let response = client
            .register_worker(Request::new(RegisterWorkerRequest {
                worker_id: worker_id.unwrap_or_default(),
                spec: Some(WorkerSpec {
                    interfaces,
                    attributes,
                    total_capacity: capacity_to_proto(total_capacity),
                    max_active_leases,
                    metadata,
                }),
            }))
            .await
            .context("register worker")?
            .into_inner();

        response
            .worker
            .context("missing worker in register response")
    })
}

fn heartbeat_worker_impl(
    endpoint: String,
    worker_id: String,
    available_capacity: HashMap<String, i64>,
    active_leases: u32,
    metadata: HashMap<String, String>,
) -> Result<WorkerRecord> {
    run_async(async move {
        let mut client = ControlPlaneServiceClient::connect(endpoint)
            .await
            .context("connect anyserve gRPC endpoint")?;
        let response = client
            .heartbeat_worker(Request::new(HeartbeatWorkerRequest {
                worker_id,
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
    })
}

fn poll_lease_impl(
    endpoint: String,
    worker_id: String,
) -> Result<
    Option<(
        anyserve_proto::controlplane::LeaseRecord,
        AttemptRecord,
        JobRecord,
    )>,
> {
    run_async(async move {
        let mut client = ControlPlaneServiceClient::connect(endpoint)
            .await
            .context("connect anyserve gRPC endpoint")?;
        let response = client
            .poll_lease(Request::new(PollLeaseRequest { worker_id }))
            .await
            .context("poll lease")?
            .into_inner();

        match (response.lease, response.attempt, response.job) {
            (Some(lease), Some(attempt), Some(job)) => Ok(Some((lease, attempt, job))),
            (None, None, None) => Ok(None),
            _ => bail!("lease grant response was incomplete"),
        }
    })
}

fn renew_lease_impl(
    endpoint: String,
    worker_id: String,
    lease_id: String,
) -> Result<anyserve_proto::controlplane::LeaseRecord> {
    run_async(async move {
        let mut client = ControlPlaneServiceClient::connect(endpoint)
            .await
            .context("connect anyserve gRPC endpoint")?;
        let response = client
            .renew_lease(Request::new(RenewLeaseRequest {
                worker_id,
                lease_id,
            }))
            .await
            .context("renew lease")?
            .into_inner();

        response.lease.context("missing lease in renew response")
    })
}

fn report_event_impl(
    endpoint: String,
    worker_id: String,
    lease_id: String,
    kind: EventKind,
    payload: Vec<u8>,
    metadata: HashMap<String, String>,
) -> Result<()> {
    run_async(async move {
        let mut client = ControlPlaneServiceClient::connect(endpoint)
            .await
            .context("connect anyserve gRPC endpoint")?;
        client
            .report_event(Request::new(ReportEventRequest {
                worker_id,
                lease_id,
                kind: kind as i32,
                payload,
                metadata,
            }))
            .await
            .context("report event")?;
        Ok(())
    })
}

fn complete_lease_impl(
    endpoint: String,
    worker_id: String,
    lease_id: String,
    outputs: Vec<PyObjectRef>,
    metadata: HashMap<String, String>,
) -> Result<()> {
    run_async(async move {
        let mut client = ControlPlaneServiceClient::connect(endpoint)
            .await
            .context("connect anyserve gRPC endpoint")?;
        client
            .complete_lease(Request::new(CompleteLeaseRequest {
                worker_id,
                lease_id,
                outputs: py_objects_to_proto(outputs)?,
                metadata,
            }))
            .await
            .context("complete lease")?;
        Ok(())
    })
}

fn fail_lease_impl(
    endpoint: String,
    worker_id: String,
    lease_id: String,
    reason: String,
    retryable: bool,
    metadata: HashMap<String, String>,
) -> Result<()> {
    run_async(async move {
        let mut client = ControlPlaneServiceClient::connect(endpoint)
            .await
            .context("connect anyserve gRPC endpoint")?;
        client
            .fail_lease(Request::new(FailLeaseRequest {
                worker_id,
                lease_id,
                reason,
                retryable,
                metadata,
            }))
            .await
            .context("fail lease")?;
        Ok(())
    })
}

fn open_stream_impl(endpoint: String, request: OpenStreamCall) -> Result<StreamRecord> {
    let OpenStreamCall {
        job_id,
        stream_name,
        scope,
        direction,
        metadata,
        attempt_id,
        worker_id,
        lease_id,
    } = request;
    run_async(async move {
        let mut client = ControlPlaneServiceClient::connect(endpoint)
            .await
            .context("connect anyserve gRPC endpoint")?;
        let response = client
            .open_stream(Request::new(OpenStreamRequest {
                job_id,
                attempt_id: attempt_id.unwrap_or_default(),
                worker_id: worker_id.unwrap_or_default(),
                lease_id: lease_id.unwrap_or_default(),
                stream_name,
                scope: scope as i32,
                direction: direction as i32,
                metadata,
            }))
            .await
            .context("open stream")?
            .into_inner();

        response.stream.context("missing stream in open response")
    })
}

fn get_stream_impl(endpoint: String, stream_id: String) -> Result<StreamRecord> {
    run_async(async move {
        let mut client = ControlPlaneServiceClient::connect(endpoint)
            .await
            .context("connect anyserve gRPC endpoint")?;
        client
            .get_stream(Request::new(GetStreamRequest { stream_id }))
            .await
            .context("get stream")?
            .into_inner()
            .pipe(Ok)
    })
}

fn list_streams_impl(endpoint: String, job_id: String) -> Result<Vec<StreamRecord>> {
    run_async(async move {
        let mut client = ControlPlaneServiceClient::connect(endpoint)
            .await
            .context("connect anyserve gRPC endpoint")?;
        Ok(client
            .list_streams(Request::new(ListStreamsRequest { job_id }))
            .await
            .context("list streams")?
            .into_inner()
            .streams)
    })
}

fn close_stream_impl(
    endpoint: String,
    stream_id: String,
    worker_id: Option<String>,
    lease_id: Option<String>,
    metadata: HashMap<String, String>,
) -> Result<StreamRecord> {
    run_async(async move {
        let mut client = ControlPlaneServiceClient::connect(endpoint)
            .await
            .context("connect anyserve gRPC endpoint")?;
        client
            .close_stream(Request::new(CloseStreamRequest {
                stream_id,
                worker_id: worker_id.unwrap_or_default(),
                lease_id: lease_id.unwrap_or_default(),
                metadata,
            }))
            .await
            .context("close stream")?
            .into_inner()
            .pipe(Ok)
    })
}

struct PushSummary {
    stream: Option<StreamRecord>,
    last_sequence: u64,
    written_frames: u64,
}

fn push_frames_impl(
    endpoint: String,
    stream_id: String,
    frames: Vec<(String, Vec<u8>, HashMap<String, String>)>,
    worker_id: Option<String>,
    lease_id: Option<String>,
) -> Result<PushSummary> {
    run_async(async move {
        let mut client = ControlPlaneServiceClient::connect(endpoint)
            .await
            .context("connect anyserve gRPC endpoint")?;
        let worker_id = worker_id.unwrap_or_default();
        let lease_id = lease_id.unwrap_or_default();
        let requests = frames
            .into_iter()
            .map(|(kind, payload, metadata)| {
                let frame_kind = parse_frame_kind(&kind)?;
                Ok(PushFramesRequest {
                    stream_id: stream_id.clone(),
                    worker_id: worker_id.clone(),
                    lease_id: lease_id.clone(),
                    kind: frame_kind as i32,
                    payload,
                    metadata,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let response = client
            .push_frames(Request::new(tokio_stream::iter(requests)))
            .await
            .context("push frames")?
            .into_inner();

        Ok(PushSummary {
            stream: response.stream,
            last_sequence: response.last_sequence,
            written_frames: response.written_frames,
        })
    })
}

fn pull_frames_impl(
    endpoint: String,
    stream_id: String,
    after_sequence: u64,
    follow: bool,
) -> Result<Vec<Frame>> {
    run_async(async move {
        let mut client = ControlPlaneServiceClient::connect(endpoint)
            .await
            .context("connect anyserve gRPC endpoint")?;
        let mut stream = client
            .pull_frames(Request::new(PullFramesRequest {
                stream_id,
                after_sequence,
                follow,
            }))
            .await
            .context("pull frames")?
            .into_inner();

        let mut frames = Vec::new();
        while let Some(item) = stream.next().await {
            frames.push(item.context("receive frame")?);
        }
        Ok(frames)
    })
}

fn py_objects_to_proto(objects: Vec<PyObjectRef>) -> Result<Vec<ObjectRef>> {
    objects.into_iter().map(py_object_to_proto).collect()
}

fn py_object_to_proto(object: PyObjectRef) -> Result<ObjectRef> {
    let metadata = object.metadata.unwrap_or_default();
    match (object.inline, object.uri) {
        (Some(content), None) => Ok(ObjectRef {
            reference: Some(object_ref::Reference::Inline(content)),
            metadata,
        }),
        (None, Some(uri)) => Ok(ObjectRef {
            reference: Some(object_ref::Reference::Uri(uri)),
            metadata,
        }),
        (Some(_), Some(_)) => bail!("object cannot contain both inline and uri"),
        (None, None) => bail!("object must contain inline or uri"),
    }
}

fn capacity_to_proto(capacity: HashMap<String, i64>) -> Vec<ResourceQuantity> {
    capacity
        .into_iter()
        .map(|(name, value)| ResourceQuantity { name, value })
        .collect()
}

fn parse_event_kind(kind: &str) -> Result<EventKind> {
    match kind {
        "accepted" => Ok(EventKind::Accepted),
        "lease_granted" => Ok(EventKind::LeaseGranted),
        "started" => Ok(EventKind::Started),
        "progress" => Ok(EventKind::Progress),
        "output_ready" => Ok(EventKind::OutputReady),
        "succeeded" => Ok(EventKind::Succeeded),
        "failed" => Ok(EventKind::Failed),
        "cancelled" => Ok(EventKind::Cancelled),
        "lease_expired" => Ok(EventKind::LeaseExpired),
        "requeued" => Ok(EventKind::Requeued),
        other => bail!("unknown event kind: {other}"),
    }
}

fn parse_stream_scope(scope: &str) -> Result<StreamScope> {
    match scope {
        "job" => Ok(StreamScope::Job),
        "attempt" => Ok(StreamScope::Attempt),
        "lease" => Ok(StreamScope::Lease),
        other => bail!("unknown stream scope: {other}"),
    }
}

fn parse_stream_direction(direction: &str) -> Result<StreamDirection> {
    match direction {
        "client_to_worker" => Ok(StreamDirection::ClientToWorker),
        "worker_to_client" => Ok(StreamDirection::WorkerToClient),
        "bidirectional" => Ok(StreamDirection::Bidirectional),
        "internal" => Ok(StreamDirection::Internal),
        other => bail!("unknown stream direction: {other}"),
    }
}

fn parse_frame_kind(kind: &str) -> Result<FrameKind> {
    match kind {
        "open" => Ok(FrameKind::Open),
        "data" => Ok(FrameKind::Data),
        "close" => Ok(FrameKind::Close),
        "error" => Ok(FrameKind::Error),
        "checkpoint" => Ok(FrameKind::Checkpoint),
        "control" => Ok(FrameKind::Control),
        other => bail!("unknown frame kind: {other}"),
    }
}

fn job_record_to_py(py: Python<'_>, job: JobRecord) -> PyResult<Py<PyDict>> {
    let state = job.state();
    let dict = PyDict::new(py);
    dict.set_item("job_id", job.job_id)?;
    dict.set_item("state", job_state_name(state))?;
    dict.set_item("spec", job_spec_to_py(py, job.spec)?)?;
    dict.set_item("outputs", object_list_to_py(py, job.outputs)?)?;
    dict.set_item("lease_id", empty_to_none_py(job.lease_id))?;
    dict.set_item("version", job.version)?;
    dict.set_item("created_at_ms", job.created_at_ms)?;
    dict.set_item("updated_at_ms", job.updated_at_ms)?;
    dict.set_item("last_error", empty_to_none_py(job.last_error))?;
    dict.set_item(
        "current_attempt_id",
        empty_to_none_py(job.current_attempt_id),
    )?;
    Ok(dict.unbind())
}

fn attempt_record_to_py(py: Python<'_>, attempt: AttemptRecord) -> PyResult<Py<PyDict>> {
    let state = attempt.state();
    let dict = PyDict::new(py);
    dict.set_item("attempt_id", attempt.attempt_id)?;
    dict.set_item("job_id", attempt.job_id)?;
    dict.set_item("worker_id", attempt.worker_id)?;
    dict.set_item("lease_id", attempt.lease_id)?;
    dict.set_item("state", attempt_state_name(state))?;
    dict.set_item("created_at_ms", attempt.created_at_ms)?;
    dict.set_item("started_at_ms", zero_to_none_py(attempt.started_at_ms))?;
    dict.set_item("finished_at_ms", zero_to_none_py(attempt.finished_at_ms))?;
    dict.set_item("last_error", empty_to_none_py(attempt.last_error))?;
    dict.set_item("metadata", attempt.metadata)?;
    Ok(dict.unbind())
}

fn worker_record_to_py(py: Python<'_>, worker: WorkerRecord) -> PyResult<Py<PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("worker_id", worker.worker_id)?;
    dict.set_item("registered_at_ms", worker.registered_at_ms)?;
    dict.set_item("expires_at_ms", worker.expires_at_ms)?;
    dict.set_item("spec", worker_spec_to_py(py, worker.spec)?)?;
    dict.set_item("status", worker_status_to_py(py, worker.status)?)?;
    Ok(dict.unbind())
}

fn lease_grant_to_py(
    py: Python<'_>,
    lease: anyserve_proto::controlplane::LeaseRecord,
    attempt: AttemptRecord,
    job: JobRecord,
) -> PyResult<Py<PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("lease", lease_record_to_py(py, lease)?)?;
    dict.set_item("attempt", attempt_record_to_py(py, attempt)?)?;
    dict.set_item("job", job_record_to_py(py, job)?)?;
    Ok(dict.unbind())
}

fn lease_record_to_py(
    py: Python<'_>,
    lease: anyserve_proto::controlplane::LeaseRecord,
) -> PyResult<Py<PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("lease_id", lease.lease_id)?;
    dict.set_item("job_id", lease.job_id)?;
    dict.set_item("worker_id", lease.worker_id)?;
    dict.set_item("issued_at_ms", lease.issued_at_ms)?;
    dict.set_item("expires_at_ms", lease.expires_at_ms)?;
    Ok(dict.unbind())
}

fn stream_record_to_py(py: Python<'_>, stream: StreamRecord) -> PyResult<Py<PyDict>> {
    let scope = stream.scope();
    let direction = stream.direction();
    let state = stream.state();
    let dict = PyDict::new(py);
    dict.set_item("stream_id", stream.stream_id)?;
    dict.set_item("job_id", stream.job_id)?;
    dict.set_item("attempt_id", empty_to_none_py(stream.attempt_id))?;
    dict.set_item("lease_id", empty_to_none_py(stream.lease_id))?;
    dict.set_item("stream_name", stream.stream_name)?;
    dict.set_item("scope", stream_scope_name(scope))?;
    dict.set_item("direction", stream_direction_name(direction))?;
    dict.set_item("state", stream_state_name(state))?;
    dict.set_item("metadata", stream.metadata)?;
    dict.set_item("created_at_ms", stream.created_at_ms)?;
    dict.set_item("closed_at_ms", zero_to_none_py(stream.closed_at_ms))?;
    dict.set_item("last_sequence", stream.last_sequence)?;
    Ok(dict.unbind())
}

fn frame_to_py(py: Python<'_>, frame: Frame) -> PyResult<Py<PyDict>> {
    let kind = frame.kind();
    let dict = PyDict::new(py);
    dict.set_item("stream_id", frame.stream_id)?;
    dict.set_item("sequence", frame.sequence)?;
    dict.set_item("kind", frame_kind_name(kind))?;
    dict.set_item("payload", PyBytes::new(py, &frame.payload))?;
    dict.set_item("metadata", frame.metadata)?;
    dict.set_item("created_at_ms", frame.created_at_ms)?;
    Ok(dict.unbind())
}

fn push_summary_to_py(py: Python<'_>, summary: PushSummary) -> PyResult<Py<PyDict>> {
    let dict = PyDict::new(py);
    if let Some(stream) = summary.stream {
        dict.set_item("stream", stream_record_to_py(py, stream)?)?;
    } else {
        dict.set_item("stream", py.None())?;
    }
    dict.set_item("last_sequence", summary.last_sequence)?;
    dict.set_item("written_frames", summary.written_frames)?;
    Ok(dict.unbind())
}

fn job_event_to_py(py: Python<'_>, event: JobEvent) -> PyResult<Py<PyDict>> {
    let kind = event.kind();
    let dict = PyDict::new(py);
    dict.set_item("job_id", event.job_id)?;
    dict.set_item("sequence", event.sequence)?;
    dict.set_item("kind", event_kind_name(kind))?;
    dict.set_item("payload", PyBytes::new(py, &event.payload))?;
    dict.set_item("metadata", event.metadata)?;
    dict.set_item("created_at_ms", event.created_at_ms)?;
    Ok(dict.unbind())
}

fn job_spec_to_py(py: Python<'_>, spec: Option<JobSpec>) -> PyResult<Py<PyDict>> {
    let dict = PyDict::new(py);
    if let Some(spec) = spec {
        dict.set_item("interface_name", spec.interface_name)?;
        dict.set_item("inputs", object_list_to_py(py, spec.inputs)?)?;
        dict.set_item("params", PyBytes::new(py, &spec.params))?;
        dict.set_item(
            "required_attributes",
            spec.demand
                .as_ref()
                .map(|demand| demand.required_attributes.clone())
                .unwrap_or_default(),
        )?;
        dict.set_item(
            "preferred_attributes",
            spec.demand
                .as_ref()
                .map(|demand| demand.preferred_attributes.clone())
                .unwrap_or_default(),
        )?;
        dict.set_item(
            "required_capacity",
            capacity_to_py(
                spec.demand
                    .as_ref()
                    .map(|demand| demand.required_capacity.clone())
                    .unwrap_or_default(),
            ),
        )?;
        dict.set_item("policy", execution_policy_to_py(py, spec.policy)?)?;
        dict.set_item("metadata", spec.metadata)?;
    }
    Ok(dict.unbind())
}

fn worker_spec_to_py(py: Python<'_>, spec: Option<WorkerSpec>) -> PyResult<Py<PyDict>> {
    let dict = PyDict::new(py);
    if let Some(spec) = spec {
        dict.set_item("interfaces", spec.interfaces)?;
        dict.set_item("attributes", spec.attributes)?;
        dict.set_item("total_capacity", capacity_to_py(spec.total_capacity))?;
        dict.set_item("max_active_leases", spec.max_active_leases)?;
        dict.set_item("metadata", spec.metadata)?;
    }
    Ok(dict.unbind())
}

fn worker_status_to_py(py: Python<'_>, status: Option<WorkerStatus>) -> PyResult<Py<PyDict>> {
    let dict = PyDict::new(py);
    if let Some(status) = status {
        dict.set_item(
            "available_capacity",
            capacity_to_py(status.available_capacity),
        )?;
        dict.set_item("active_leases", status.active_leases)?;
        dict.set_item("metadata", status.metadata)?;
        dict.set_item("last_seen_at_ms", status.last_seen_at_ms)?;
    }
    Ok(dict.unbind())
}

fn execution_policy_to_py(py: Python<'_>, policy: Option<ExecutionPolicy>) -> PyResult<Py<PyDict>> {
    let dict = PyDict::new(py);
    if let Some(policy) = policy {
        dict.set_item("profile", policy.profile)?;
        dict.set_item("priority", policy.priority)?;
        dict.set_item("lease_ttl_secs", policy.lease_ttl_secs)?;
    }
    Ok(dict.unbind())
}

fn object_list_to_py(py: Python<'_>, objects: Vec<ObjectRef>) -> PyResult<Py<PyList>> {
    let list = PyList::empty(py);
    for object in objects {
        list.append(object_ref_to_py(py, object)?)?;
    }
    Ok(list.unbind())
}

fn object_ref_to_py(py: Python<'_>, object: ObjectRef) -> PyResult<Py<PyDict>> {
    let dict = PyDict::new(py);
    match object.reference {
        Some(object_ref::Reference::Inline(content)) => {
            dict.set_item("inline", PyBytes::new(py, &content))?;
        }
        Some(object_ref::Reference::Uri(uri)) => {
            dict.set_item("uri", uri)?;
        }
        None => {}
    }
    dict.set_item("metadata", object.metadata)?;
    Ok(dict.unbind())
}

fn capacity_to_py(capacity: Vec<ResourceQuantity>) -> HashMap<String, i64> {
    capacity
        .into_iter()
        .map(|entry| (entry.name, entry.value))
        .collect()
}

fn job_state_name(state: anyserve_proto::controlplane::JobState) -> &'static str {
    match state {
        anyserve_proto::controlplane::JobState::Pending => "pending",
        anyserve_proto::controlplane::JobState::Leased => "leased",
        anyserve_proto::controlplane::JobState::Running => "running",
        anyserve_proto::controlplane::JobState::Succeeded => "succeeded",
        anyserve_proto::controlplane::JobState::Failed => "failed",
        anyserve_proto::controlplane::JobState::Cancelled => "cancelled",
        anyserve_proto::controlplane::JobState::Unspecified => "unspecified",
    }
}

fn attempt_state_name(state: anyserve_proto::controlplane::AttemptState) -> &'static str {
    match state {
        anyserve_proto::controlplane::AttemptState::Created => "created",
        anyserve_proto::controlplane::AttemptState::Leased => "leased",
        anyserve_proto::controlplane::AttemptState::Running => "running",
        anyserve_proto::controlplane::AttemptState::Succeeded => "succeeded",
        anyserve_proto::controlplane::AttemptState::Failed => "failed",
        anyserve_proto::controlplane::AttemptState::Expired => "expired",
        anyserve_proto::controlplane::AttemptState::Cancelled => "cancelled",
        anyserve_proto::controlplane::AttemptState::Unspecified => "unspecified",
    }
}

fn stream_scope_name(scope: anyserve_proto::controlplane::StreamScope) -> &'static str {
    match scope {
        anyserve_proto::controlplane::StreamScope::Job => "job",
        anyserve_proto::controlplane::StreamScope::Attempt => "attempt",
        anyserve_proto::controlplane::StreamScope::Lease => "lease",
        anyserve_proto::controlplane::StreamScope::Unspecified => "unspecified",
    }
}

fn stream_direction_name(direction: anyserve_proto::controlplane::StreamDirection) -> &'static str {
    match direction {
        anyserve_proto::controlplane::StreamDirection::ClientToWorker => "client_to_worker",
        anyserve_proto::controlplane::StreamDirection::WorkerToClient => "worker_to_client",
        anyserve_proto::controlplane::StreamDirection::Bidirectional => "bidirectional",
        anyserve_proto::controlplane::StreamDirection::Internal => "internal",
        anyserve_proto::controlplane::StreamDirection::Unspecified => "unspecified",
    }
}

fn stream_state_name(state: anyserve_proto::controlplane::StreamState) -> &'static str {
    match state {
        anyserve_proto::controlplane::StreamState::Open => "open",
        anyserve_proto::controlplane::StreamState::Closing => "closing",
        anyserve_proto::controlplane::StreamState::Closed => "closed",
        anyserve_proto::controlplane::StreamState::Error => "error",
        anyserve_proto::controlplane::StreamState::Unspecified => "unspecified",
    }
}

fn frame_kind_name(kind: anyserve_proto::controlplane::FrameKind) -> &'static str {
    match kind {
        anyserve_proto::controlplane::FrameKind::Open => "open",
        anyserve_proto::controlplane::FrameKind::Data => "data",
        anyserve_proto::controlplane::FrameKind::Close => "close",
        anyserve_proto::controlplane::FrameKind::Error => "error",
        anyserve_proto::controlplane::FrameKind::Checkpoint => "checkpoint",
        anyserve_proto::controlplane::FrameKind::Control => "control",
        anyserve_proto::controlplane::FrameKind::Unspecified => "unspecified",
    }
}

fn event_kind_name(kind: EventKind) -> &'static str {
    match kind {
        EventKind::Accepted => "accepted",
        EventKind::LeaseGranted => "lease_granted",
        EventKind::Started => "started",
        EventKind::Progress => "progress",
        EventKind::OutputReady => "output_ready",
        EventKind::Succeeded => "succeeded",
        EventKind::Failed => "failed",
        EventKind::Cancelled => "cancelled",
        EventKind::LeaseExpired => "lease_expired",
        EventKind::Requeued => "requeued",
        EventKind::Unspecified => "unspecified",
    }
}

fn empty_to_none_py(value: String) -> Option<String> {
    if value.is_empty() { None } else { Some(value) }
}

fn zero_to_none_py(value: u64) -> Option<u64> {
    if value == 0 { None } else { Some(value) }
}

fn run_async<F, T>(future: F) -> Result<T>
where
    F: Future<Output = Result<T>>,
{
    Builder::new_current_thread()
        .enable_all()
        .build()
        .context("create tokio runtime")?
        .block_on(future)
}

fn to_py_err(error: anyhow::Error) -> PyErr {
    let message = error.to_string();
    if message.starts_with("unknown event kind:")
        || message.starts_with("unknown stream scope:")
        || message.starts_with("unknown stream direction:")
        || message.starts_with("unknown frame kind:")
    {
        PyValueError::new_err(message)
    } else {
        PyRuntimeError::new_err(message)
    }
}

trait Pipe: Sized {
    fn pipe<T>(self, op: impl FnOnce(Self) -> T) -> T {
        op(self)
    }
}

impl<T> Pipe for T {}
