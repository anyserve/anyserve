use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use anyserve_client::execution::{ExecutionClient as Client, ExecutionEvents as ClientEvents, v2};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyString};

use crate::{
    PyObjectRef, SharedRuntime, build_runtime, connection_error, extract_bytes, lock_shared,
    metadata_to_py, status_error,
};

macro_rules! field {
    ($dict:expr, $key:literal, $ty:ty, $default:expr) => {
        $dict
            .get_item(pyo3::intern!($dict.py(), $key))?
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
    #[pyo3(signature = (endpoint, token=None, namespace=None))]
    fn new(
        py: Python<'_>,
        endpoint: String,
        token: Option<String>,
        namespace: Option<String>,
    ) -> PyResult<Self> {
        let token = token.or_else(|| std::env::var(anyserve_client::TOKEN_ENV).ok());
        let namespace = namespace.or_else(|| std::env::var(anyserve_client::NAMESPACE_ENV).ok());
        py.detach(move || {
            let runtime = Arc::new(build_runtime().map_err(error)?);
            let client = runtime
                .block_on(Client::connect_with_options(
                    &endpoint,
                    token.as_deref(),
                    namespace.as_deref(),
                ))
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
        let mut client = self.client.clone();
        let response = py.detach(|| {
            self.runtime
                .block_on(client.submit(job_id, spec))
                .map_err(status_error)
        })?;
        let dict = job_to_py(
            py,
            response
                .job
                .ok_or_else(|| PyRuntimeError::new_err("missing submitted job"))?,
        )?;
        dict.bind(py)
            .set_item(intern!(py, "created"), response.created)?;
        Ok(dict)
    }

    fn invoke(&self, py: Python<'_>, spec: &Bound<'_, PyDict>) -> PyResult<ExecutionEvents> {
        let spec = parse_spec(spec)?;
        let mut client = self.client.clone();
        let events = py.detach(|| {
            self.runtime
                .block_on(client.invoke(spec))
                .map_err(status_error)
        })?;
        Ok(ExecutionEvents::new(Arc::clone(&self.runtime), events))
    }

    fn invoke_stream(&self, py: Python<'_>, spec: &Bound<'_, PyDict>) -> PyResult<InputInvocation> {
        let spec = parse_spec(spec)?;
        let mut client = self.client.clone();
        let invocation = py.detach(|| {
            self.runtime
                .block_on(client.invoke_stream(spec))
                .map_err(status_error)
        })?;
        let closed = tokio::sync::watch::channel(false).0;
        let closed_rx = closed.subscribe();
        Ok(InputInvocation {
            input: Arc::new(Mutex::new(Some(invocation.input))),
            runtime: Arc::clone(&self.runtime),
            events: Arc::new(Mutex::new(Some((invocation.events, closed_rx)))),
            closed,
        })
    }

    #[pyo3(signature = (job_id, after_sequence=0))]
    fn watch(
        &self,
        py: Python<'_>,
        job_id: String,
        after_sequence: u64,
    ) -> PyResult<ExecutionEvents> {
        let mut client = self.client.clone();
        let events = py.detach(|| {
            self.runtime
                .block_on(client.watch(job_id, after_sequence))
                .map_err(status_error)
        })?;
        Ok(ExecutionEvents::new(Arc::clone(&self.runtime), events))
    }

    #[pyo3(signature = (job_id, include_outputs=true))]
    fn get(&self, py: Python<'_>, job_id: String, include_outputs: bool) -> PyResult<Py<PyDict>> {
        let mut client = self.client.clone();
        let job = py.detach(|| {
            self.runtime
                .block_on(client.get(job_id, include_outputs))
                .map_err(status_error)
        })?;
        job_to_py(py, job)
    }

    #[pyo3(signature = (job_id, reason="caller cancelled".to_owned()))]
    fn cancel(&self, py: Python<'_>, job_id: String, reason: String) -> PyResult<Py<PyDict>> {
        let mut client = self.client.clone();
        let job = py.detach(|| {
            self.runtime
                .block_on(client.cancel(job_id, reason))
                .map_err(status_error)
        })?;
        job_to_py(py, job)
    }
}

type SharedEventStream = Arc<Mutex<Option<(ClientEvents, tokio::sync::watch::Receiver<bool>)>>>;

#[pyclass(module = "anyserve._native")]
pub struct InputInvocation {
    runtime: SharedRuntime,
    input: Arc<Mutex<Option<tokio::sync::mpsc::Sender<v2::InvokeRequest>>>>,
    events: SharedEventStream,
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
        data: &Bound<'_, PyAny>,
        metadata: Option<HashMap<String, String>>,
    ) -> PyResult<()> {
        let data = extract_bytes(data.as_borrowed())?;
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
        if *self.closed.borrow() {
            return Err(error("invocation is closed"));
        }
        let input = lock_shared(&self.input, "invocation input")
            .map_err(error)?
            .clone()
            .ok_or_else(|| error("invocation is closed"))?;
        let mut closed = self.closed.subscribe();
        py.detach(move || {
            self.runtime.block_on(async {
                if *closed.borrow_and_update() {
                    return Err(error("invocation is closed"));
                }
                tokio::select! {
                    biased;
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
    events: SharedEventStream,
    closed: tokio::sync::watch::Sender<bool>,
}

impl ExecutionEvents {
    fn new(runtime: SharedRuntime, events: ClientEvents) -> Self {
        let closed = tokio::sync::watch::channel(false).0;
        let closed_rx = closed.subscribe();
        Self {
            runtime,
            events: Arc::new(Mutex::new(Some((events, closed_rx)))),
            closed,
        }
    }
}

#[pymethods]
impl ExecutionEvents {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(&self, py: Python<'_>) -> PyResult<Option<Py<PyDict>>> {
        if *self.closed.borrow() {
            return Ok(None);
        }
        let next = py.detach(|| {
            let mut events = lock_shared(&self.events, "execution events").map_err(error)?;
            let Some((stream, closed)) = events.as_mut() else {
                return Ok(None);
            };
            let next = self
                .runtime
                .block_on(async {
                    if *closed.borrow_and_update() {
                        return Ok(None);
                    }
                    tokio::select! {
                        biased;
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

fn non_empty_map<K, V>(v: &Bound<'_, PyAny>) -> PyResult<HashMap<K, V>>
where
    for<'py> HashMap<K, V>: FromPyObjectOwned<'py>,
{
    if v.cast::<PyDict>().is_ok_and(|d| d.is_empty()) {
        return Ok(HashMap::new());
    }
    v.extract().map_err(Into::into)
}

fn parse_spec(dict: &Bound<'_, PyDict>) -> PyResult<v2::JobSpec> {
    let mut demand = v2::Demand::default();
    let mut policy = v2::ExecutionPolicy {
        durability: v2::Durability::Durable as i32,
        ..Default::default()
    };
    let mut inputs = Vec::new();
    let mut params = Vec::new();
    let mut metadata = HashMap::new();

    for (k, v) in dict.iter() {
        let Ok(key) = k.cast::<PyString>() else {
            continue;
        };
        match key.to_str()? {
            "interface" => demand.interface = v.extract()?,
            "inputs" => inputs = objects_from_py(&v)?,
            "durability" => {
                policy.durability = match v.cast::<PyString>()?.to_str()? {
                    "durable" => v2::Durability::Durable as i32,
                    "ephemeral" => v2::Durability::Ephemeral as i32,
                    _ => {
                        return Err(PyValueError::new_err(
                            "durability must be durable or ephemeral",
                        ));
                    }
                };
            }
            "params" => params = extract_bytes(v.as_borrowed())?,
            "required_attributes" => demand.required_attributes = non_empty_map(&v)?,
            "preferred_attributes" => demand.preferred_attributes = non_empty_map(&v)?,
            "required_capacity" => demand.required_capacity = non_empty_map(&v)?,
            "affinity_key" => demand.affinity_key = v.extract()?,
            "metadata" => metadata = non_empty_map(&v)?,
            "priority" => policy.priority = v.extract()?,
            "queue_timeout_ms" => policy.queue_timeout_ms = v.extract()?,
            "execution_timeout_ms" => policy.execution_timeout_ms = v.extract()?,
            "total_timeout_ms" => policy.total_timeout_ms = v.extract()?,
            "spillover_after_ms" => policy.spillover_after_ms = v.extract()?,
            "max_attempts" => policy.max_attempts = v.extract()?,
            "fairness_key" => policy.fairness_key = v.extract()?,
            "concurrency_key" => policy.concurrency_key = v.extract()?,
            _ => {}
        }
    }
    if demand.interface.trim().is_empty() {
        return Err(PyValueError::new_err("interface must be non-empty"));
    }
    Ok(v2::JobSpec {
        demand: Some(demand),
        inputs,
        params: params.into(),
        metadata,
        policy: Some(policy),
    })
}

pub(crate) fn objects_from_py(objects: &Bound<'_, PyAny>) -> PyResult<Vec<v2::Object>> {
    let mut out = Vec::with_capacity(objects.len().unwrap_or(0));
    for item in objects.try_iter()? {
        let object: PyObjectRef = item?.extract()?;
        let body = match (object.inline, object.uri) {
            (Some(data), None) => v2::object::Body::Inline(data.into()),
            (None, Some(uri)) if uri.is_empty() => {
                return Err(PyValueError::new_err("object URI must not be empty"));
            }
            (None, Some(uri)) => v2::object::Body::Uri(uri),
            _ => {
                return Err(PyValueError::new_err(
                    "object requires exactly one of inline or uri",
                ));
            }
        };
        out.push(v2::Object {
            body: Some(body),
            metadata: object.metadata.unwrap_or_default(),
        });
    }
    Ok(out)
}

pub(crate) fn objects_to_py(py: Python<'_>, objects: &[v2::Object]) -> PyResult<Py<PyList>> {
    let list = PyList::empty(py);
    for object in objects {
        let dict = PyDict::new(py);
        match &object.body {
            Some(v2::object::Body::Inline(data)) => {
                dict.set_item(intern!(py, "inline"), PyBytes::new(py, data))?
            }
            Some(v2::object::Body::Uri(uri)) => dict.set_item(intern!(py, "uri"), uri)?,
            None => {}
        }
        dict.set_item(
            intern!(py, "metadata"),
            metadata_to_py(py, &object.metadata)?,
        )?;
        list.append(dict)?;
    }
    Ok(list.unbind())
}

fn job_to_py(py: Python<'_>, job: v2::Job) -> PyResult<Py<PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item(intern!(py, "job_id"), job.job_id)?;
    dict.set_item(intern!(py, "namespace"), job.namespace)?;
    dict.set_item(intern!(py, "state"), state_name(py, job.state))?;
    dict.set_item(
        intern!(py, "durability"),
        if job.durability == v2::Durability::Durable as i32 {
            intern!(py, "durable")
        } else {
            intern!(py, "ephemeral")
        },
    )?;
    dict.set_item(intern!(py, "interface"), job.interface)?;
    dict.set_item(intern!(py, "attempt_count"), job.attempt_count)?;
    dict.set_item(intern!(py, "current_attempt_id"), job.current_attempt_id)?;
    dict.set_item(intern!(py, "current_worker_id"), job.current_worker_id)?;
    dict.set_item(intern!(py, "created_at_ms"), job.created_at_ms)?;
    dict.set_item(intern!(py, "updated_at_ms"), job.updated_at_ms)?;
    dict.set_item(intern!(py, "outputs"), objects_to_py(py, &job.outputs)?)?;
    dict.set_item(intern!(py, "metadata"), metadata_to_py(py, &job.metadata)?)?;
    dict.set_item(intern!(py, "spec_hash"), job.spec_hash)?;
    dict.set_item(
        intern!(py, "error"),
        job.error.map(|info| error_to_py(py, info)).transpose()?,
    )?;
    Ok(dict.unbind())
}

fn event_to_py(py: Python<'_>, event: v2::ExecutionEvent) -> PyResult<Py<PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item(intern!(py, "job_id"), event.job_id)?;
    dict.set_item(intern!(py, "sequence"), event.sequence)?;
    dict.set_item(intern!(py, "attempt_id"), event.attempt_id)?;
    dict.set_item(intern!(py, "at_ms"), event.at_ms)?;
    let kind_key = intern!(py, "type");
    match event.event {
        Some(v2::execution_event::Event::Accepted(_)) => {
            dict.set_item(kind_key, intern!(py, "accepted"))?
        }
        Some(v2::execution_event::Event::Assigned(assignment)) => {
            dict.set_item(kind_key, intern!(py, "assigned"))?;
            dict.set_item(intern!(py, "worker_id"), assignment.worker_id)?;
            dict.set_item(intern!(py, "attempt_number"), assignment.attempt_number)?;
        }
        Some(v2::execution_event::Event::Output(output)) => {
            dict.set_item(kind_key, intern!(py, "output"))?;
            dict.set_item(intern!(py, "data"), PyBytes::new(py, &output.data))?;
            dict.set_item(
                intern!(py, "metadata"),
                metadata_to_py(py, &output.metadata)?,
            )?;
            dict.set_item(intern!(py, "progress"), output.progress)?;
        }
        Some(v2::execution_event::Event::AttemptRestarted(restart)) => {
            dict.set_item(kind_key, intern!(py, "attempt_restarted"))?;
            dict.set_item(
                intern!(py, "previous_attempt_id"),
                restart.previous_attempt_id,
            )?;
            dict.set_item(
                intern!(py, "error"),
                restart
                    .reason
                    .map(|info| error_to_py(py, info))
                    .transpose()?,
            )?;
        }
        Some(v2::execution_event::Event::Finished(finish)) => {
            dict.set_item(kind_key, intern!(py, "finished"))?;
            dict.set_item(intern!(py, "state"), state_name(py, finish.state))?;
            dict.set_item(intern!(py, "outputs"), objects_to_py(py, &finish.outputs)?)?;
            dict.set_item(
                intern!(py, "error"),
                finish.error.map(|info| error_to_py(py, info)).transpose()?,
            )?;
        }
        None => return Err(PyRuntimeError::new_err("execution event has no body")),
    }
    Ok(dict.unbind())
}

fn error_to_py(py: Python<'_>, info: v2::ErrorInfo) -> PyResult<Py<PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item(intern!(py, "code"), info.code)?;
    dict.set_item(intern!(py, "message"), info.message)?;
    dict.set_item(intern!(py, "retryable"), info.retryable)?;
    Ok(dict.unbind())
}
fn state_name(py: Python<'_>, state: i32) -> &Bound<'_, PyString> {
    match v2::JobState::try_from(state) {
        Ok(v2::JobState::Queued) => intern!(py, "queued"),
        Ok(v2::JobState::Running) => intern!(py, "running"),
        Ok(v2::JobState::Succeeded) => intern!(py, "succeeded"),
        Ok(v2::JobState::Failed) => intern!(py, "failed"),
        Ok(v2::JobState::Cancelled) => intern!(py, "cancelled"),
        _ => intern!(py, "unspecified"),
    }
}
pub(crate) fn error(error: impl std::fmt::Display) -> PyErr {
    PyRuntimeError::new_err(error.to_string())
}
