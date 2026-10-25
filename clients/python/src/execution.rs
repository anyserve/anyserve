use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use anyserve_client::execution::{ExecutionClient as Client, ExecutionEvents as ClientEvents, v2};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};

use crate::{
    PyObjectRef, SharedRuntime, build_runtime, connection_error, lock_shared, status_error,
};

macro_rules! field {
    ($dict:expr, $key:expr, $ty:ty, $default:expr) => {
        $dict
            .get_item($key)?
            .map(|value| value.extract::<$ty>())
            .transpose()?
            .unwrap_or($default)
    };
}
pub(crate) use field;

#[pyclass(module = "anyserve._native")]
pub struct ExecutionTransport {
    runtime: SharedRuntime,
    client: Client,
}

#[pymethods]
impl ExecutionTransport {
    #[new]
    #[pyo3(signature = (endpoint, token=None))]
    fn new(py: Python<'_>, endpoint: String, token: Option<String>) -> PyResult<Self> {
        let token = token.or_else(|| std::env::var(anyserve_client::TOKEN_ENV).ok());
        py.detach(move || {
            let runtime = Arc::new(build_runtime().map_err(error)?);
            let client = runtime
                .block_on(Client::connect_with_token(&endpoint, token.as_deref()))
                .map_err(connection_error)?;
            Ok(Self { runtime, client })
        })
    }

    #[pyo3(signature = (spec, job_id=None))]
    fn submit(
        &self,
        py: Python<'_>,
        spec: &Bound<'_, PyDict>,
        job_id: Option<String>,
    ) -> PyResult<Py<PyDict>> {
        let spec = parse_spec(spec)?;
        let runtime = Arc::clone(&self.runtime);
        let mut client = self.client.clone();
        let response = py.detach(move || {
            runtime
                .block_on(client.submit(job_id, spec))
                .map_err(status_error)
        })?;
        let dict = job_to_py(
            py,
            response
                .job
                .ok_or_else(|| PyRuntimeError::new_err("missing submitted job"))?,
        )?;
        dict.bind(py).set_item("created", response.created)?;
        Ok(dict)
    }

    fn invoke(&self, py: Python<'_>, spec: &Bound<'_, PyDict>) -> PyResult<ExecutionEvents> {
        let spec = parse_spec(spec)?;
        let runtime = Arc::clone(&self.runtime);
        let mut client = self.client.clone();
        let events =
            py.detach(move || runtime.block_on(client.invoke(spec)).map_err(status_error))?;
        Ok(ExecutionEvents::new(Arc::clone(&self.runtime), events))
    }

    fn invoke_stream(&self, py: Python<'_>, spec: &Bound<'_, PyDict>) -> PyResult<InputInvocation> {
        let spec = parse_spec(spec)?;
        let runtime = Arc::clone(&self.runtime);
        let mut client = self.client.clone();
        let invocation = py.detach(move || {
            runtime
                .block_on(client.invoke_stream(spec))
                .map_err(status_error)
        })?;
        Ok(InputInvocation {
            input: Arc::new(Mutex::new(Some(invocation.input))),
            runtime: Arc::clone(&self.runtime),
            events: Arc::new(Mutex::new(Some(invocation.events))),
            closed: tokio::sync::watch::channel(false).0,
        })
    }

    #[pyo3(signature = (job_id, after_sequence=0))]
    fn watch(
        &self,
        py: Python<'_>,
        job_id: String,
        after_sequence: u64,
    ) -> PyResult<ExecutionEvents> {
        let runtime = Arc::clone(&self.runtime);
        let mut client = self.client.clone();
        let events = py.detach(move || {
            runtime
                .block_on(client.watch(job_id, after_sequence))
                .map_err(status_error)
        })?;
        Ok(ExecutionEvents::new(Arc::clone(&self.runtime), events))
    }

    #[pyo3(signature = (job_id, include_outputs=true))]
    fn get(&self, py: Python<'_>, job_id: String, include_outputs: bool) -> PyResult<Py<PyDict>> {
        let runtime = Arc::clone(&self.runtime);
        let mut client = self.client.clone();
        let job = py.detach(move || {
            runtime
                .block_on(client.get(job_id, include_outputs))
                .map_err(status_error)
        })?;
        job_to_py(py, job)
    }

    #[pyo3(signature = (job_id, reason="caller cancelled".to_owned()))]
    fn cancel(&self, py: Python<'_>, job_id: String, reason: String) -> PyResult<Py<PyDict>> {
        let runtime = Arc::clone(&self.runtime);
        let mut client = self.client.clone();
        let job = py.detach(move || {
            runtime
                .block_on(client.cancel(job_id, reason))
                .map_err(status_error)
        })?;
        job_to_py(py, job)
    }
}

#[pyclass(module = "anyserve._native")]
pub struct InputInvocation {
    runtime: SharedRuntime,
    input: Arc<Mutex<Option<tokio::sync::mpsc::Sender<v2::InvokeRequest>>>>,
    events: Arc<Mutex<Option<ClientEvents>>>,
    closed: tokio::sync::watch::Sender<bool>,
}
#[pymethods]
impl InputInvocation {
    #[getter]
    fn events(&self) -> ExecutionEvents {
        ExecutionEvents {
            runtime: Arc::clone(&self.runtime),
            events: Arc::clone(&self.events),
            closed: self.closed.clone(),
        }
    }
    #[pyo3(signature = (data, metadata=None))]
    fn send(
        &self,
        py: Python<'_>,
        data: Vec<u8>,
        metadata: Option<HashMap<String, String>>,
    ) -> PyResult<()> {
        self.send_message(
            py,
            v2::invoke_request::Msg::Input(v2::InputChunk {
                data: data.into(),
                metadata: metadata.unwrap_or_default(),
            }),
        )
    }
    fn finish_input(&self, py: Python<'_>) -> PyResult<()> {
        self.send_message(py, v2::invoke_request::Msg::InputEnd(v2::InputEnd {}))
    }
    fn close(&self) -> PyResult<()> {
        self.closed.send_replace(true);
        *lock_shared(&self.input, "invocation input").map_err(error)? = None;
        if let Ok(mut events) = self.events.try_lock() {
            *events = None;
        }
        Ok(())
    }
}
impl InputInvocation {
    fn send_message(&self, py: Python<'_>, msg: v2::invoke_request::Msg) -> PyResult<()> {
        let runtime = Arc::clone(&self.runtime);
        let input = lock_shared(&self.input, "invocation input")
            .map_err(error)?
            .clone()
            .ok_or_else(|| error("invocation is closed"))?;
        let mut closed = self.closed.subscribe();
        py.detach(move || {
            runtime.block_on(async {
            if *closed.borrow_and_update() { return Err(error("invocation is closed")); }
            tokio::select! {
                result = input.send(v2::InvokeRequest { msg: Some(msg) }) => result.map_err(error),
                _ = closed.changed() => Err(error("invocation is closed")),
            }
        })
        })
    }
}

#[pyclass(module = "anyserve._native")]
pub struct ExecutionEvents {
    runtime: SharedRuntime,
    events: Arc<Mutex<Option<ClientEvents>>>,
    closed: tokio::sync::watch::Sender<bool>,
}

impl ExecutionEvents {
    fn new(runtime: SharedRuntime, events: ClientEvents) -> Self {
        Self {
            runtime,
            events: Arc::new(Mutex::new(Some(events))),
            closed: tokio::sync::watch::channel(false).0,
        }
    }
}

#[pymethods]
impl ExecutionEvents {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(&self, py: Python<'_>) -> PyResult<Option<Py<PyDict>>> {
        let runtime = Arc::clone(&self.runtime);
        let events = Arc::clone(&self.events);
        let mut closed = self.closed.subscribe();
        let next = py.detach(move || {
            let mut events = lock_shared(&events, "execution events").map_err(error)?;
            let Some(stream) = events.as_mut() else {
                return Ok(None);
            };
            let next = runtime
                .block_on(async {
                    if *closed.borrow_and_update() {
                        return Ok(None);
                    }
                    tokio::select! {
                        next = stream.message() => next,
                        _ = closed.changed() => Ok(None),
                    }
                })
                .map_err(status_error)?;
            if next.is_none() {
                *events = None;
            }
            Ok::<_, PyErr>(next)
        })?;
        next.map(|event| event_to_py(py, event)).transpose()
    }
    /// Release the HTTP/2 response immediately; Invoke then cancels its job.
    fn close(&self) -> PyResult<()> {
        self.closed.send_replace(true);
        if let Ok(mut events) = self.events.try_lock() {
            *events = None;
        }
        Ok(())
    }
}

fn parse_spec(dict: &Bound<'_, PyDict>) -> PyResult<v2::JobSpec> {
    let interface = field!(dict, "interface", String, String::new());
    if interface.trim().is_empty() {
        return Err(PyValueError::new_err("interface must be non-empty"));
    }
    let durability = field!(dict, "durability", String, "durable".to_owned());
    let durability = match durability.as_str() {
        "durable" => v2::Durability::Durable,
        "ephemeral" => v2::Durability::Ephemeral,
        _ => {
            return Err(PyValueError::new_err(
                "durability must be durable or ephemeral",
            ));
        }
    };
    let inputs = field!(dict, "inputs", Vec<PyObjectRef>, Vec::new());
    let params = field!(dict, "params", Vec<u8>, Vec::new());
    Ok(v2::JobSpec {
        demand: Some(v2::Demand {
            interface,
            required_attributes: field!(dict, "required_attributes", HashMap<String, String>, HashMap::new()),
            preferred_attributes: field!(dict, "preferred_attributes", HashMap<String, String>, HashMap::new()),
            required_capacity: field!(dict, "required_capacity", HashMap<String, i64>, HashMap::new()),
        }),
        inputs: objects_from_py(inputs)?,
        params: params.into(),
        metadata: field!(dict, "metadata", HashMap<String, String>, HashMap::new()),
        policy: Some(v2::ExecutionPolicy {
            durability: durability as i32,
            priority: field!(dict, "priority", u32, 0),
            queue_timeout_ms: field!(dict, "queue_timeout_ms", u64, 0),
            execution_timeout_ms: field!(dict, "execution_timeout_ms", u64, 0),
            total_timeout_ms: field!(dict, "total_timeout_ms", u64, 0),
            spillover_after_ms: field!(dict, "spillover_after_ms", u64, 0),
            max_attempts: field!(dict, "max_attempts", u32, 0),
        }),
    })
}

pub(crate) fn objects_from_py(objects: Vec<PyObjectRef>) -> PyResult<Vec<v2::Object>> {
    objects
        .into_iter()
        .map(|object| {
            let body = match (object.inline, object.uri) {
                (Some(data), None) => v2::object::Body::Inline(data.into()),
                (None, Some(uri)) => v2::object::Body::Uri(uri),
                _ => {
                    return Err(PyValueError::new_err(
                        "object requires exactly one of inline or uri",
                    ));
                }
            };
            Ok(v2::Object {
                body: Some(body),
                metadata: object.metadata.unwrap_or_default(),
            })
        })
        .collect()
}

pub(crate) fn objects_to_py(py: Python<'_>, objects: &[v2::Object]) -> PyResult<Py<PyList>> {
    let list = PyList::empty(py);
    for object in objects {
        let dict = PyDict::new(py);
        match &object.body {
            Some(v2::object::Body::Inline(data)) => {
                dict.set_item("inline", PyBytes::new(py, data))?
            }
            Some(v2::object::Body::Uri(uri)) => dict.set_item("uri", uri)?,
            None => {}
        }
        dict.set_item("metadata", &object.metadata)?;
        list.append(dict)?;
    }
    Ok(list.unbind())
}

fn job_to_py(py: Python<'_>, job: v2::Job) -> PyResult<Py<PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("job_id", job.job_id)?;
    dict.set_item("state", state_name(job.state))?;
    dict.set_item(
        "durability",
        if job.durability == v2::Durability::Durable as i32 {
            "durable"
        } else {
            "ephemeral"
        },
    )?;
    dict.set_item("interface", job.interface)?;
    dict.set_item("attempt_count", job.attempt_count)?;
    dict.set_item("current_attempt_id", job.current_attempt_id)?;
    dict.set_item("current_worker_id", job.current_worker_id)?;
    dict.set_item("created_at_ms", job.created_at_ms)?;
    dict.set_item("updated_at_ms", job.updated_at_ms)?;
    dict.set_item("outputs", objects_to_py(py, &job.outputs)?)?;
    dict.set_item("metadata", job.metadata)?;
    dict.set_item("spec_hash", job.spec_hash)?;
    dict.set_item(
        "error",
        job.error.map(|info| error_to_py(py, info)).transpose()?,
    )?;
    Ok(dict.unbind())
}

fn event_to_py(py: Python<'_>, event: v2::ExecutionEvent) -> PyResult<Py<PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("job_id", event.job_id)?;
    dict.set_item("sequence", event.sequence)?;
    dict.set_item("attempt_id", event.attempt_id)?;
    dict.set_item("at_ms", event.at_ms)?;
    match event.event {
        Some(v2::execution_event::Event::Accepted(_)) => dict.set_item("type", "accepted")?,
        Some(v2::execution_event::Event::Assigned(assignment)) => {
            dict.set_item("type", "assigned")?;
            dict.set_item("worker_id", assignment.worker_id)?;
            dict.set_item("attempt_number", assignment.attempt_number)?;
        }
        Some(v2::execution_event::Event::Output(output)) => {
            dict.set_item("type", "output")?;
            dict.set_item("data", PyBytes::new(py, &output.data))?;
            dict.set_item("metadata", output.metadata)?;
            dict.set_item("progress", output.progress)?;
        }
        Some(v2::execution_event::Event::AttemptRestarted(restart)) => {
            dict.set_item("type", "attempt_restarted")?;
            dict.set_item("previous_attempt_id", restart.previous_attempt_id)?;
            dict.set_item(
                "error",
                restart
                    .reason
                    .map(|info| error_to_py(py, info))
                    .transpose()?,
            )?;
        }
        Some(v2::execution_event::Event::Finished(finish)) => {
            dict.set_item("type", "finished")?;
            dict.set_item("state", state_name(finish.state))?;
            dict.set_item("outputs", objects_to_py(py, &finish.outputs)?)?;
            dict.set_item(
                "error",
                finish.error.map(|info| error_to_py(py, info)).transpose()?,
            )?;
        }
        None => return Err(PyRuntimeError::new_err("execution event has no body")),
    }
    Ok(dict.unbind())
}

fn error_to_py(py: Python<'_>, info: v2::ErrorInfo) -> PyResult<Py<PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("code", info.code)?;
    dict.set_item("message", info.message)?;
    dict.set_item("retryable", info.retryable)?;
    Ok(dict.unbind())
}
fn state_name(state: i32) -> &'static str {
    match v2::JobState::try_from(state) {
        Ok(v2::JobState::Queued) => "queued",
        Ok(v2::JobState::Running) => "running",
        Ok(v2::JobState::Succeeded) => "succeeded",
        Ok(v2::JobState::Failed) => "failed",
        Ok(v2::JobState::Cancelled) => "cancelled",
        _ => "unspecified",
    }
}
pub(crate) fn error(error: impl std::fmt::Display) -> PyErr {
    PyRuntimeError::new_err(error.to_string())
}
