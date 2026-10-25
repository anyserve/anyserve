use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyserve_client::execution::{Attempt, AttemptClient, WorkerSession, v2};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};
use tokio::sync::{mpsc, watch};
use tonic::Streaming;

use crate::execution::{error, field, objects_from_py, objects_to_py};
use crate::{
    PyObjectRef, SharedRuntime, build_runtime, connection_error, lock_shared, status_error,
};

#[pyclass(module = "anyserve._native")]
pub struct WorkerConnection {
    runtime: SharedRuntime,
    control: mpsc::Sender<v2::WorkerMessage>,
    messages: Arc<Mutex<Option<Streaming<v2::ServerMessage>>>>,
    data: AttemptClient,
    limits: v2::Limits,
    heartbeat_timeout: Duration,
    heartbeat: tokio::task::JoinHandle<()>,
    #[pyo3(get)]
    session_id: String,
    #[pyo3(get)]
    generation: u64,
    #[pyo3(get)]
    resumed_lease_ids: Vec<String>,
}

#[pymethods]
impl WorkerConnection {
    #[new]
    #[pyo3(signature = (endpoint, hello, token=None))]
    fn new(
        py: Python<'_>,
        endpoint: String,
        hello: &Bound<'_, PyDict>,
        token: Option<String>,
    ) -> PyResult<Self> {
        let hello = v2::Hello {
            worker_id: field!(hello, "worker_id", String, String::new()),
            interfaces: field!(hello, "interfaces", Vec<String>, Vec::new()),
            attributes: field!(hello, "attributes", HashMap<String, String>, HashMap::new()),
            total_capacity: field!(hello, "capacity", HashMap<String, i64>, HashMap::new()),
            max_concurrency: field!(hello, "max_concurrency", u32, 1),
            resume_session_id: field!(hello, "resume_session_id", String, String::new()),
            held_lease_ids: field!(hello, "held_lease_ids", Vec<String>, Vec::new()),
            sdk_version: env!("CARGO_PKG_VERSION").to_owned(),
            metadata: field!(hello, "metadata", HashMap<String, String>, HashMap::new()),
        };
        let token = token.or_else(|| std::env::var(anyserve_client::TOKEN_ENV).ok());
        py.detach(move || {
            let runtime = Arc::new(build_runtime().map_err(error)?);
            let (session, welcome) = runtime.block_on(async {
                let mut session = WorkerSession::connect(&endpoint, token.as_deref(), hello)
                    .await
                    .map_err(connection_error)?;
                let welcome = match session.messages.message().await.map_err(status_error)? {
                    Some(v2::ServerMessage {
                        msg: Some(v2::server_message::Msg::Welcome(welcome)),
                    }) => welcome,
                    _ => return Err(error("worker session did not start with Welcome")),
                };
                Ok::<_, PyErr>((session, welcome))
            })?;
            let limits = welcome
                .limits
                .ok_or_else(|| error("Welcome has no data limits"))?;
            let control = session.control;
            let heartbeats = control.clone();
            let heartbeat = runtime.spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_millis(u64::from(
                    welcome.heartbeat_interval_ms.max(1),
                )));
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    interval.tick().await;
                    if heartbeats
                        .send(v2::WorkerMessage {
                            msg: Some(v2::worker_message::Msg::Heartbeat(v2::Heartbeat {
                                load: None,
                            })),
                        })
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            });
            Ok(Self {
                runtime,
                control,
                messages: Arc::new(Mutex::new(Some(session.messages))),
                data: session.data,
                limits,
                heartbeat_timeout: Duration::from_millis(u64::from(welcome.heartbeat_timeout_ms)),
                heartbeat,
                session_id: welcome.session_id,
                generation: welcome.generation,
                resumed_lease_ids: welcome.resumed_lease_ids,
            })
        })
    }

    #[pyo3(signature = (timeout_secs=1.0))]
    fn receive(&self, py: Python<'_>, timeout_secs: f64) -> PyResult<Option<Py<PyDict>>> {
        if !timeout_secs.is_finite() || timeout_secs <= 0.0 {
            return Err(error("timeout_secs must be finite and positive"));
        }
        let runtime = Arc::clone(&self.runtime);
        let messages = Arc::clone(&self.messages);
        let message = py.detach(move || {
            let mut messages = lock_shared(&messages, "worker messages").map_err(error)?;
            let Some(messages) = messages.as_mut() else {
                return Err(error("worker session is closed"));
            };
            runtime.block_on(async {
                match tokio::time::timeout(
                    Duration::from_secs_f64(timeout_secs),
                    messages.message(),
                )
                .await
                {
                    Err(_) => Ok(None),
                    Ok(Ok(Some(message))) => Ok(message.msg),
                    Ok(Ok(None)) => Err(error("worker session closed")),
                    Ok(Err(status)) => Err(status_error(status)),
                }
            })
        })?;
        let Some(message) = message else {
            return Ok(None);
        };
        let dict = PyDict::new(py);
        match message {
            v2::server_message::Msg::Assign(inner) => {
                dict.set_item("type", "assignment")?;
                dict.set_item("lease_id", &inner.lease_id)?;
                dict.set_item("job_id", &inner.job_id)?;
                let (cancelled, _) = watch::channel(false);
                dict.set_item(
                    "assignment",
                    Assignment {
                        runtime: Arc::clone(&self.runtime),
                        inner,
                        data: self.data.clone(),
                        limits: self.limits,
                        heartbeat_timeout: self.heartbeat_timeout,
                        cancelled,
                    },
                )?;
            }
            v2::server_message::Msg::Cancel(cancel) => {
                dict.set_item("type", "cancel")?;
                dict.set_item("lease_id", cancel.lease_id)?;
                dict.set_item("reason", cancel.reason)?;
            }
            v2::server_message::Msg::Fence(fence) => {
                dict.set_item("type", "fence")?;
                dict.set_item("lease_id", fence.lease_id)?;
                dict.set_item("reason", fence.reason)?;
            }
            v2::server_message::Msg::Drain(_) => dict.set_item("type", "drain")?,
            v2::server_message::Msg::Welcome(_) => return Err(error("duplicate Welcome")),
        }
        Ok(Some(dict.unbind()))
    }

    fn credit(&self, py: Python<'_>, free_slots: u32) -> PyResult<()> {
        self.send(
            py,
            v2::worker_message::Msg::Credit(v2::Credit { free_slots }),
        )
    }
    #[pyo3(signature = (lease_id, accepted=true, reason=String::new()))]
    fn accept(
        &self,
        py: Python<'_>,
        lease_id: String,
        accepted: bool,
        reason: String,
    ) -> PyResult<()> {
        self.send(
            py,
            v2::worker_message::Msg::AssignReply(v2::AssignReply {
                lease_id,
                accepted,
                reject_reason: reason,
            }),
        )
    }
    fn cancel_ack(&self, py: Python<'_>, lease_id: String) -> PyResult<()> {
        self.send(
            py,
            v2::worker_message::Msg::CancelAck(v2::CancelAck { lease_id }),
        )
    }
    fn drain(&self, py: Python<'_>) -> PyResult<()> {
        self.send(py, v2::worker_message::Msg::Draining(v2::Draining {}))
    }
    fn close(&self, py: Python<'_>) -> PyResult<()> {
        self.heartbeat.abort();
        let messages = Arc::clone(&self.messages);
        py.detach(move || {
            *lock_shared(&messages, "worker messages").map_err(error)? = None;
            Ok(())
        })
    }
}

impl WorkerConnection {
    fn send(&self, py: Python<'_>, msg: v2::worker_message::Msg) -> PyResult<()> {
        let control = self.control.clone();
        let runtime = Arc::clone(&self.runtime);
        py.detach(move || {
            runtime
                .block_on(control.send(v2::WorkerMessage { msg: Some(msg) }))
                .map_err(error)
        })
    }
}
impl Drop for WorkerConnection {
    fn drop(&mut self) {
        self.heartbeat.abort();
    }
}

#[pyclass(module = "anyserve._native")]
pub struct Assignment {
    runtime: SharedRuntime,
    inner: v2::Assign,
    data: AttemptClient,
    limits: v2::Limits,
    heartbeat_timeout: Duration,
    cancelled: watch::Sender<bool>,
}

#[pymethods]
impl Assignment {
    #[getter]
    fn job_id(&self) -> &str {
        &self.inner.job_id
    }
    #[getter]
    fn lease_id(&self) -> &str {
        &self.inner.lease_id
    }
    #[getter]
    fn attempt_id(&self) -> &str {
        &self.inner.attempt_id
    }
    #[getter]
    fn interface(&self) -> &str {
        &self.inner.interface
    }
    #[getter]
    fn cancelled(&self) -> bool {
        *self.cancelled.borrow()
    }
    #[getter]
    fn reserved_capacity(&self) -> HashMap<String, i64> {
        self.inner.reserved_capacity.clone()
    }
    fn cancel(&self) {
        self.cancelled.send_replace(true);
    }
    fn open(&self, py: Python<'_>) -> PyResult<WorkerAttempt> {
        self.open_mode(py, true)
    }
    fn open_stream(&self, py: Python<'_>) -> PyResult<WorkerAttempt> {
        self.open_mode(py, false)
    }
}
impl Assignment {
    fn open_mode(&self, py: Python<'_>, buffered: bool) -> PyResult<WorkerAttempt> {
        let runtime = Arc::clone(&self.runtime);
        let mut data = self.data.clone();
        let assignment = self.inner.clone();
        let limits = self.limits;
        let heartbeat_timeout = self.heartbeat_timeout;
        let mut cancelled = self.cancelled.subscribe();
        let attempt = py.detach(move || {
            runtime.block_on(async {
                tokio::select! {
                    attempt = async {
                        if buffered { Attempt::open(&mut data, &assignment, &limits, heartbeat_timeout).await }
                        else { Attempt::open_stream(&mut data, &assignment, &limits, heartbeat_timeout).await }
                    } => attempt.map_err(status_error),
                    _ = wait_for_cancel(&mut cancelled) => Err(error("attempt cancelled")),
                }
            })
        })?;
        let terminal_replay = attempt.terminal_replay;
        Ok(WorkerAttempt {
            terminal_replay,
            runtime: Arc::clone(&self.runtime),
            inner: Arc::new(Mutex::new(attempt)),
            assignment: self.inner.clone(),
            data: self.data.clone(),
            limits: self.limits,
            cancelled: self.cancelled.clone(),
        })
    }
}

#[pyclass(module = "anyserve._native")]
pub struct WorkerAttempt {
    #[pyo3(get)]
    terminal_replay: bool,
    runtime: SharedRuntime,
    inner: Arc<Mutex<Attempt>>,
    assignment: v2::Assign,
    data: AttemptClient,
    limits: v2::Limits,
    cancelled: watch::Sender<bool>,
}

#[pymethods]
impl WorkerAttempt {
    #[getter]
    fn job_id(&self) -> &str {
        &self.assignment.job_id
    }
    #[getter]
    fn attempt_id(&self) -> &str {
        &self.assignment.attempt_id
    }
    #[getter]
    fn lease_id(&self) -> &str {
        &self.assignment.lease_id
    }
    #[getter]
    fn metadata(&self) -> HashMap<String, String> {
        self.assignment.metadata.clone()
    }
    #[getter]
    fn inputs(&self, py: Python<'_>) -> PyResult<Py<pyo3::types::PyList>> {
        let inner = Arc::clone(&self.inner);
        let inputs = py.detach(move || {
            Ok::<_, PyErr>(
                lock_shared(&inner, "worker attempt")
                    .map_err(error)?
                    .inputs
                    .clone(),
            )
        })?;
        objects_to_py(py, &inputs)
    }
    #[getter]
    fn params(&self, py: Python<'_>) -> PyResult<Py<PyBytes>> {
        let inner = Arc::clone(&self.inner);
        let bytes = py.detach(move || {
            Ok::<_, PyErr>(
                lock_shared(&inner, "worker attempt")
                    .map_err(error)?
                    .params
                    .clone(),
            )
        })?;
        Ok(PyBytes::new(py, &bytes).unbind())
    }
    fn input_chunk(&self, py: Python<'_>) -> PyResult<Option<Py<PyDict>>> {
        let runtime = Arc::clone(&self.runtime);
        let inner = Arc::clone(&self.inner);
        let mut cancelled = self.cancelled.subscribe();
        let input = py.detach(move || {
            let mut inner = lock_shared(&inner, "worker attempt").map_err(error)?;
            runtime.block_on(async {
                tokio::select! {
                    input = inner.input_chunk() => input.map_err(status_error),
                    _ = wait_for_cancel(&mut cancelled) => Err(error("attempt cancelled")),
                }
            })
        })?;
        input
            .map(|input| {
                let dict = PyDict::new(py);
                dict.set_item("data", PyBytes::new(py, &input.data))?;
                dict.set_item("object_index", input.object_index)?;
                dict.set_item("uri", input.uri)?;
                dict.set_item("metadata", input.metadata)?;
                dict.set_item("parameter_chunk", input.parameter_chunk)?;
                dict.set_item("object_end", input.object_end)?;
                Ok(dict.unbind())
            })
            .transpose()
    }
    #[getter]
    fn cancelled(&self) -> bool {
        *self.cancelled.borrow()
    }
    #[pyo3(signature = (data, metadata=None, progress=false))]
    fn frame(
        &self,
        py: Python<'_>,
        data: Vec<u8>,
        metadata: Option<HashMap<String, String>>,
        progress: bool,
    ) -> PyResult<()> {
        let runtime = Arc::clone(&self.runtime);
        let inner = Arc::clone(&self.inner);
        let mut cancelled = self.cancelled.subscribe();
        py.detach(move || {
            let mut inner = lock_shared(&inner, "worker attempt").map_err(error)?;
            runtime.block_on(async {
                tokio::select! {
                    sent = inner.frame(data.into(), metadata.unwrap_or_default(), progress) => sent.map_err(status_error),
                    _ = wait_for_cancel(&mut cancelled) => Err(error("attempt cancelled")),
                }
            })
        })
    }
    #[pyo3(signature = (outputs=None))]
    fn complete(&self, py: Python<'_>, outputs: Option<Vec<PyObjectRef>>) -> PyResult<()> {
        self.finish(
            py,
            objects_from_py(outputs.unwrap_or_default())?,
            v2::finish::Outcome::Succeeded(v2::Succeeded {}),
        )
    }
    #[pyo3(signature = (message, retryable=false))]
    fn fail(&self, py: Python<'_>, message: String, retryable: bool) -> PyResult<()> {
        self.finish(
            py,
            Vec::new(),
            v2::finish::Outcome::Failed(v2::ErrorInfo {
                code: "worker_failed".to_owned(),
                message,
                retryable,
            }),
        )
    }
}
impl WorkerAttempt {
    fn finish(
        &self,
        py: Python<'_>,
        outputs: Vec<v2::Object>,
        outcome: v2::finish::Outcome,
    ) -> PyResult<()> {
        let runtime = Arc::clone(&self.runtime);
        let inner = Arc::clone(&self.inner);
        let assignment = self.assignment.clone();
        let limits = self.limits;
        let mut data = self.data.clone();
        let mut cancelled = self.cancelled.subscribe();
        py.detach(move || {
            let mut inner = lock_shared(&inner, "worker attempt").map_err(error)?;
            runtime.block_on(async {
                tokio::select! {
                    result = inner.commit(&mut data, &assignment, &limits, &outputs, outcome) => result.map_err(status_error),
                    _ = wait_for_cancel(&mut cancelled) => Err(error("attempt cancelled")),
                }
            })
        })
    }
}
async fn wait_for_cancel(cancelled: &mut watch::Receiver<bool>) {
    loop {
        if *cancelled.borrow_and_update() {
            return;
        }
        if cancelled.changed().await.is_err() {
            return;
        }
    }
}
