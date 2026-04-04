use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use anyhow::{Context, Result, anyhow, bail};
use anyserve_client::controlplane::{
    ExecutionPolicy, JobSpec, ResourceQuantity, WorkerSpec, WorkerStatus,
};
use anyserve_client::{
    AnyserveClient as RustAnyserveClient, AttemptRecord, AttemptState, EventKind, Frame, FrameKind,
    FrameWrite, JobEvent, JobRecord, JobState, JobSubmission, LeaseGrant, LeaseRecord, ObjectRef,
    PushSummary as RustPushSummary, StreamDirection, StreamOpen, StreamRecord, StreamScope,
    StreamState, WorkerRecord, WorkerRegistration, object_ref,
};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};
use tokio::runtime::{Builder, Runtime};
use tonic::Streaming;

type SharedRuntime = Arc<Mutex<Runtime>>;
type SharedClient = Arc<Mutex<RustAnyserveClient>>;
type SharedJobEventStream = Arc<Mutex<Option<Streaming<JobEvent>>>>;
type SharedFrameStream = Arc<Mutex<Option<Streaming<Frame>>>>;

#[derive(FromPyObject)]
struct PyObjectRef {
    #[pyo3(item("inline"))]
    inline: Option<Vec<u8>>,
    #[pyo3(item("uri"))]
    uri: Option<String>,
    #[pyo3(item("metadata"))]
    metadata: Option<HashMap<String, String>>,
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

#[pyclass]
#[derive(Clone)]
struct AnyserveClient {
    endpoint: String,
    runtime: SharedRuntime,
    client: SharedClient,
}

#[pyclass]
struct JobEventIterator {
    runtime: SharedRuntime,
    stream: SharedJobEventStream,
}

#[pyclass]
struct FrameIterator {
    runtime: SharedRuntime,
    stream: SharedFrameStream,
}

#[pymethods]
impl AnyserveClient {
    #[new]
    fn new(endpoint: String) -> PyResult<Self> {
        let runtime = build_runtime().map_err(to_py_err)?;
        let client = runtime
            .block_on(RustAnyserveClient::connect(endpoint.clone()))
            .map_err(to_py_err)?;
        Ok(Self {
            endpoint,
            runtime: Arc::new(Mutex::new(runtime)),
            client: Arc::new(Mutex::new(client)),
        })
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
        let runtime = self.runtime.clone();
        let client = self.client.clone();
        let request = SubmitJobCall {
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
        };
        let job = py
            .allow_threads(move || {
                let request = submit_job_from_py(request)?;
                with_client(&runtime, &client, move |runtime, client| {
                    runtime.block_on(client.submit_job(request))
                })
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
    ) -> PyResult<JobEventIterator> {
        let runtime = self.runtime.clone();
        let stream_runtime = runtime.clone();
        let client = self.client.clone();
        py.allow_threads(move || {
            let stream = with_client(&runtime, &client, move |runtime, client| {
                runtime.block_on(client.watch_job(job_id, after_sequence))
            })?;
            Ok(JobEventIterator {
                runtime: stream_runtime,
                stream: Arc::new(Mutex::new(Some(stream))),
            })
        })
        .map_err(to_py_err)
    }

    fn list_jobs(&self, py: Python<'_>) -> PyResult<Vec<Py<PyDict>>> {
        let runtime = self.runtime.clone();
        let client = self.client.clone();
        let jobs = py
            .allow_threads(move || {
                with_client(&runtime, &client, |runtime, client| {
                    runtime.block_on(client.list_jobs())
                })
            })
            .map_err(to_py_err)?;
        jobs.into_iter()
            .map(|job| job_record_to_py(py, job))
            .collect()
    }

    fn get_job(&self, py: Python<'_>, job_id: String) -> PyResult<Py<PyDict>> {
        let runtime = self.runtime.clone();
        let client = self.client.clone();
        let job = py
            .allow_threads(move || {
                with_client(&runtime, &client, move |runtime, client| {
                    runtime.block_on(client.get_job(job_id))
                })
            })
            .map_err(to_py_err)?;
        job_record_to_py(py, job)
    }

    fn cancel_job(&self, py: Python<'_>, job_id: String) -> PyResult<Py<PyDict>> {
        let runtime = self.runtime.clone();
        let client = self.client.clone();
        let job = py
            .allow_threads(move || {
                with_client(&runtime, &client, move |runtime, client| {
                    runtime.block_on(client.cancel_job(job_id))
                })
            })
            .map_err(to_py_err)?;
        job_record_to_py(py, job)
    }

    fn get_attempt(&self, py: Python<'_>, attempt_id: String) -> PyResult<Py<PyDict>> {
        let runtime = self.runtime.clone();
        let client = self.client.clone();
        let attempt = py
            .allow_threads(move || {
                with_client(&runtime, &client, move |runtime, client| {
                    runtime.block_on(client.get_attempt(attempt_id))
                })
            })
            .map_err(to_py_err)?;
        attempt_record_to_py(py, attempt)
    }

    fn list_attempts(&self, py: Python<'_>, job_id: String) -> PyResult<Vec<Py<PyDict>>> {
        let runtime = self.runtime.clone();
        let client = self.client.clone();
        let attempts = py
            .allow_threads(move || {
                with_client(&runtime, &client, move |runtime, client| {
                    runtime.block_on(client.list_attempts(job_id))
                })
            })
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
        let runtime = self.runtime.clone();
        let client = self.client.clone();
        let request = RegisterWorkerCall {
            interfaces,
            attributes: attributes.unwrap_or_default(),
            total_capacity: total_capacity.unwrap_or_default(),
            max_active_leases,
            metadata: metadata.unwrap_or_default(),
            worker_id,
        };
        let worker = py
            .allow_threads(move || {
                let request = register_worker_from_py(request);
                with_client(&runtime, &client, move |runtime, client| {
                    runtime.block_on(client.register_worker(request))
                })
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
        let runtime = self.runtime.clone();
        let client = self.client.clone();
        let worker = py
            .allow_threads(move || {
                with_client(&runtime, &client, move |runtime, client| {
                    runtime.block_on(client.heartbeat_worker(
                        worker_id,
                        available_capacity.unwrap_or_default(),
                        active_leases,
                        metadata.unwrap_or_default(),
                    ))
                })
            })
            .map_err(to_py_err)?;
        worker_record_to_py(py, worker)
    }

    fn poll_lease(&self, py: Python<'_>, worker_id: String) -> PyResult<Option<Py<PyDict>>> {
        let runtime = self.runtime.clone();
        let client = self.client.clone();
        let lease = py
            .allow_threads(move || {
                with_client(&runtime, &client, move |runtime, client| {
                    runtime.block_on(client.poll_lease(worker_id))
                })
            })
            .map_err(to_py_err)?;
        lease.map(|grant| lease_grant_to_py(py, grant)).transpose()
    }

    fn renew_lease(
        &self,
        py: Python<'_>,
        worker_id: String,
        lease_id: String,
    ) -> PyResult<Py<PyDict>> {
        let runtime = self.runtime.clone();
        let client = self.client.clone();
        let lease = py
            .allow_threads(move || {
                with_client(&runtime, &client, move |runtime, client| {
                    runtime.block_on(client.renew_lease(worker_id, lease_id))
                })
            })
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
        let runtime = self.runtime.clone();
        let client = self.client.clone();
        py.allow_threads(move || {
            let kind = parse_event_kind(&kind)?;
            with_client(&runtime, &client, move |runtime, client| {
                runtime.block_on(client.report_event(
                    worker_id,
                    lease_id,
                    kind,
                    payload.unwrap_or_default(),
                    metadata.unwrap_or_default(),
                ))
            })
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
        let runtime = self.runtime.clone();
        let client = self.client.clone();
        py.allow_threads(move || {
            let outputs = py_objects_to_proto(outputs.unwrap_or_default())?;
            with_client(&runtime, &client, move |runtime, client| {
                runtime.block_on(client.complete_lease(
                    worker_id,
                    lease_id,
                    outputs,
                    metadata.unwrap_or_default(),
                ))
            })
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
        let runtime = self.runtime.clone();
        let client = self.client.clone();
        py.allow_threads(move || {
            with_client(&runtime, &client, move |runtime, client| {
                runtime.block_on(client.fail_lease(
                    worker_id,
                    lease_id,
                    reason,
                    retryable,
                    metadata.unwrap_or_default(),
                ))
            })
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
        let runtime = self.runtime.clone();
        let client = self.client.clone();
        let request = OpenStreamCall {
            job_id,
            stream_name,
            scope: parse_stream_scope(&scope).map_err(to_py_err)?,
            direction: parse_stream_direction(&direction).map_err(to_py_err)?,
            metadata: metadata.unwrap_or_default(),
            attempt_id,
            worker_id,
            lease_id,
        };
        let stream = py
            .allow_threads(move || {
                let request = open_stream_from_py(request);
                with_client(&runtime, &client, move |runtime, client| {
                    runtime.block_on(client.open_stream(request))
                })
            })
            .map_err(to_py_err)?;
        stream_record_to_py(py, stream)
    }

    fn get_stream(&self, py: Python<'_>, stream_id: String) -> PyResult<Py<PyDict>> {
        let runtime = self.runtime.clone();
        let client = self.client.clone();
        let stream = py
            .allow_threads(move || {
                with_client(&runtime, &client, move |runtime, client| {
                    runtime.block_on(client.get_stream(stream_id))
                })
            })
            .map_err(to_py_err)?;
        stream_record_to_py(py, stream)
    }

    fn list_streams(&self, py: Python<'_>, job_id: String) -> PyResult<Vec<Py<PyDict>>> {
        let runtime = self.runtime.clone();
        let client = self.client.clone();
        let streams = py
            .allow_threads(move || {
                with_client(&runtime, &client, move |runtime, client| {
                    runtime.block_on(client.list_streams(job_id))
                })
            })
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
        let runtime = self.runtime.clone();
        let client = self.client.clone();
        let stream = py
            .allow_threads(move || {
                with_client(&runtime, &client, move |runtime, client| {
                    runtime.block_on(client.close_stream(
                        stream_id,
                        worker_id,
                        lease_id,
                        metadata.unwrap_or_default(),
                    ))
                })
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
        let runtime = self.runtime.clone();
        let client = self.client.clone();
        let summary = py
            .allow_threads(move || {
                let frames = frame_writes_from_py(frames)?;
                with_client(&runtime, &client, move |runtime, client| {
                    runtime.block_on(client.push_frames(stream_id, frames, worker_id, lease_id))
                })
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
    ) -> PyResult<FrameIterator> {
        let runtime = self.runtime.clone();
        let stream_runtime = runtime.clone();
        let client = self.client.clone();
        py.allow_threads(move || {
            let stream = with_client(&runtime, &client, move |runtime, client| {
                runtime.block_on(client.pull_frames(stream_id, after_sequence, follow))
            })?;
            Ok(FrameIterator {
                runtime: stream_runtime,
                stream: Arc::new(Mutex::new(Some(stream))),
            })
        })
        .map_err(to_py_err)
    }

    fn __repr__(&self) -> String {
        format!("AnyserveClient(endpoint={:?})", self.endpoint)
    }
}

#[pymethods]
impl JobEventIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&self, py: Python<'_>) -> PyResult<Option<Py<PyDict>>> {
        let runtime = self.runtime.clone();
        let stream = self.stream.clone();
        let next = py
            .allow_threads(move || next_job_event(&runtime, &stream))
            .map_err(to_py_err)?;
        next.map(|event| job_event_to_py(py, event)).transpose()
    }

    fn __repr__(&self) -> &'static str {
        "JobEventIterator()"
    }
}

#[pymethods]
impl FrameIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&self, py: Python<'_>) -> PyResult<Option<Py<PyDict>>> {
        let runtime = self.runtime.clone();
        let stream = self.stream.clone();
        let next = py
            .allow_threads(move || next_frame(&runtime, &stream))
            .map_err(to_py_err)?;
        next.map(|frame| frame_to_py(py, frame)).transpose()
    }

    fn __repr__(&self) -> &'static str {
        "FrameIterator()"
    }
}

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<AnyserveClient>()?;
    m.add_class::<JobEventIterator>()?;
    m.add_class::<FrameIterator>()?;
    Ok(())
}

fn build_runtime() -> Result<Runtime> {
    Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .context("create tokio runtime")
}

fn with_client<F, T>(runtime: &SharedRuntime, client: &SharedClient, op: F) -> Result<T>
where
    F: FnOnce(&mut Runtime, &mut RustAnyserveClient) -> Result<T>,
{
    let mut runtime = lock_shared(runtime, "anyserve tokio runtime")?;
    let mut client = lock_shared(client, "anyserve client")?;
    op(&mut runtime, &mut client)
}

fn lock_shared<'a, T>(shared: &'a Arc<Mutex<T>>, label: &str) -> Result<MutexGuard<'a, T>> {
    shared.lock().map_err(|_| anyhow!("{label} lock poisoned"))
}

fn submit_job_from_py(request: SubmitJobCall) -> Result<JobSubmission> {
    Ok(JobSubmission {
        job_id: request.job_id,
        interface_name: request.interface_name,
        inputs: py_objects_to_proto(request.inputs)?,
        params: request.params,
        required_attributes: request.required_attributes,
        preferred_attributes: request.preferred_attributes,
        required_capacity: request.required_capacity,
        metadata: request.metadata,
        profile: request.profile,
        priority: request.priority,
        lease_ttl_secs: request.lease_ttl_secs,
    })
}

fn register_worker_from_py(request: RegisterWorkerCall) -> WorkerRegistration {
    WorkerRegistration {
        worker_id: request.worker_id,
        interfaces: request.interfaces,
        attributes: request.attributes,
        total_capacity: request.total_capacity,
        max_active_leases: request.max_active_leases,
        metadata: request.metadata,
    }
}

fn open_stream_from_py(request: OpenStreamCall) -> StreamOpen {
    StreamOpen {
        job_id: request.job_id,
        stream_name: request.stream_name,
        scope: request.scope,
        direction: request.direction,
        metadata: request.metadata,
        attempt_id: request.attempt_id,
        worker_id: request.worker_id,
        lease_id: request.lease_id,
    }
}

fn frame_writes_from_py(
    frames: Vec<(String, Vec<u8>, HashMap<String, String>)>,
) -> Result<Vec<FrameWrite>> {
    frames
        .into_iter()
        .map(|(kind, payload, metadata)| {
            Ok(FrameWrite {
                kind: parse_frame_kind(&kind)?,
                payload,
                metadata,
            })
        })
        .collect()
}

fn next_job_event(
    runtime: &SharedRuntime,
    stream: &SharedJobEventStream,
) -> Result<Option<JobEvent>> {
    let runtime = lock_shared(runtime, "anyserve tokio runtime")?;
    let mut stream = lock_shared(stream, "job event stream")?;
    let Some(inner) = stream.as_mut() else {
        return Ok(None);
    };
    let next = runtime
        .block_on(inner.message())
        .context("receive job event")?;
    if next.is_none() {
        *stream = None;
    }
    Ok(next)
}

fn next_frame(runtime: &SharedRuntime, stream: &SharedFrameStream) -> Result<Option<Frame>> {
    let runtime = lock_shared(runtime, "anyserve tokio runtime")?;
    let mut stream = lock_shared(stream, "frame stream")?;
    let Some(inner) = stream.as_mut() else {
        return Ok(None);
    };
    let next = runtime.block_on(inner.message()).context("receive frame")?;
    if next.is_none() {
        *stream = None;
    }
    Ok(next)
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
    let state = job_state_name(job.state());
    let dict = PyDict::new(py);
    dict.set_item("job_id", job.job_id)?;
    dict.set_item("state", state)?;
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
    let state = attempt_state_name(attempt.state());
    let dict = PyDict::new(py);
    dict.set_item("attempt_id", attempt.attempt_id)?;
    dict.set_item("job_id", attempt.job_id)?;
    dict.set_item("worker_id", attempt.worker_id)?;
    dict.set_item("lease_id", attempt.lease_id)?;
    dict.set_item("state", state)?;
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

fn lease_grant_to_py(py: Python<'_>, grant: LeaseGrant) -> PyResult<Py<PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("lease", lease_record_to_py(py, grant.lease)?)?;
    dict.set_item("attempt", attempt_record_to_py(py, grant.attempt)?)?;
    dict.set_item("job", job_record_to_py(py, grant.job)?)?;
    Ok(dict.unbind())
}

fn lease_record_to_py(py: Python<'_>, lease: LeaseRecord) -> PyResult<Py<PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("lease_id", lease.lease_id)?;
    dict.set_item("job_id", lease.job_id)?;
    dict.set_item("worker_id", lease.worker_id)?;
    dict.set_item("issued_at_ms", lease.issued_at_ms)?;
    dict.set_item("expires_at_ms", lease.expires_at_ms)?;
    Ok(dict.unbind())
}

fn stream_record_to_py(py: Python<'_>, stream: StreamRecord) -> PyResult<Py<PyDict>> {
    let scope = stream_scope_name(stream.scope());
    let direction = stream_direction_name(stream.direction());
    let state = stream_state_name(stream.state());
    let dict = PyDict::new(py);
    dict.set_item("stream_id", stream.stream_id)?;
    dict.set_item("job_id", stream.job_id)?;
    dict.set_item("attempt_id", empty_to_none_py(stream.attempt_id))?;
    dict.set_item("lease_id", empty_to_none_py(stream.lease_id))?;
    dict.set_item("stream_name", stream.stream_name)?;
    dict.set_item("scope", scope)?;
    dict.set_item("direction", direction)?;
    dict.set_item("state", state)?;
    dict.set_item("metadata", stream.metadata)?;
    dict.set_item("created_at_ms", stream.created_at_ms)?;
    dict.set_item("closed_at_ms", zero_to_none_py(stream.closed_at_ms))?;
    dict.set_item("last_sequence", stream.last_sequence)?;
    Ok(dict.unbind())
}

fn frame_to_py(py: Python<'_>, frame: Frame) -> PyResult<Py<PyDict>> {
    let kind = frame_kind_name(frame.kind());
    let dict = PyDict::new(py);
    dict.set_item("stream_id", frame.stream_id)?;
    dict.set_item("sequence", frame.sequence)?;
    dict.set_item("kind", kind)?;
    dict.set_item("payload", PyBytes::new(py, &frame.payload))?;
    dict.set_item("metadata", frame.metadata)?;
    dict.set_item("created_at_ms", frame.created_at_ms)?;
    Ok(dict.unbind())
}

fn push_summary_to_py(py: Python<'_>, summary: RustPushSummary) -> PyResult<Py<PyDict>> {
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
    let kind = event_kind_name(event.kind());
    let dict = PyDict::new(py);
    dict.set_item("job_id", event.job_id)?;
    dict.set_item("sequence", event.sequence)?;
    dict.set_item("kind", kind)?;
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

fn job_state_name(state: JobState) -> &'static str {
    match state {
        JobState::Pending => "pending",
        JobState::Leased => "leased",
        JobState::Running => "running",
        JobState::Succeeded => "succeeded",
        JobState::Failed => "failed",
        JobState::Cancelled => "cancelled",
        JobState::Unspecified => "unspecified",
    }
}

fn attempt_state_name(state: AttemptState) -> &'static str {
    match state {
        AttemptState::Created => "created",
        AttemptState::Leased => "leased",
        AttemptState::Running => "running",
        AttemptState::Succeeded => "succeeded",
        AttemptState::Failed => "failed",
        AttemptState::Expired => "expired",
        AttemptState::Cancelled => "cancelled",
        AttemptState::Unspecified => "unspecified",
    }
}

fn stream_scope_name(scope: StreamScope) -> &'static str {
    match scope {
        StreamScope::Job => "job",
        StreamScope::Attempt => "attempt",
        StreamScope::Lease => "lease",
        StreamScope::Unspecified => "unspecified",
    }
}

fn stream_direction_name(direction: StreamDirection) -> &'static str {
    match direction {
        StreamDirection::ClientToWorker => "client_to_worker",
        StreamDirection::WorkerToClient => "worker_to_client",
        StreamDirection::Bidirectional => "bidirectional",
        StreamDirection::Internal => "internal",
        StreamDirection::Unspecified => "unspecified",
    }
}

fn stream_state_name(state: StreamState) -> &'static str {
    match state {
        StreamState::Open => "open",
        StreamState::Closing => "closing",
        StreamState::Closed => "closed",
        StreamState::Error => "error",
        StreamState::Unspecified => "unspecified",
    }
}

fn frame_kind_name(kind: FrameKind) -> &'static str {
    match kind {
        FrameKind::Open => "open",
        FrameKind::Data => "data",
        FrameKind::Close => "close",
        FrameKind::Error => "error",
        FrameKind::Checkpoint => "checkpoint",
        FrameKind::Control => "control",
        FrameKind::Unspecified => "unspecified",
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

fn to_py_err(error: anyhow::Error) -> PyErr {
    let message = error.to_string();
    if message.starts_with("unknown event kind:")
        || message.starts_with("unknown stream scope:")
        || message.starts_with("unknown stream direction:")
        || message.starts_with("unknown frame kind:")
        || message.starts_with("object cannot contain both inline and uri")
        || message.starts_with("object must contain inline or uri")
        || message.starts_with("unsupported codec:")
    {
        PyValueError::new_err(message)
    } else {
        PyRuntimeError::new_err(message)
    }
}
