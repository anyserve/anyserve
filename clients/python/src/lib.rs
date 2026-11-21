mod execution;
mod session;

use anyhow::{Context, Result, anyhow};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyString};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};
pyo3::create_exception!(_native, RpcError, PyRuntimeError);
use tokio::runtime::{Builder, Runtime};

type SharedRuntime = Arc<Runtime>;

pub(crate) struct PyObjectRef {
    pub(crate) inline: Option<Vec<u8>>,
    pub(crate) uri: Option<String>,
    pub(crate) metadata: Option<HashMap<String, String>>,
}

pub(crate) fn extract_bytes(value: Borrowed<'_, '_, PyAny>) -> PyResult<Vec<u8>> {
    if let Ok(b) = value.cast::<PyBytes>() {
        return Ok(b.as_bytes().to_vec());
    }
    value.extract::<Vec<u8>>()
}

pub(crate) fn metadata_to_py<'py>(
    py: Python<'py>,
    map: &HashMap<String, String>,
) -> PyResult<Bound<'py, PyDict>> {
    if map.is_empty() {
        Ok(PyDict::new(py))
    } else {
        Ok(map.into_pyobject(py)?)
    }
}

impl<'a, 'py> FromPyObject<'a, 'py> for PyObjectRef {
    type Error = PyErr;

    fn extract(object: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        let object = object.cast::<PyDict>()?;
        let mut out = Self {
            inline: None,
            uri: None,
            metadata: None,
        };
        for (k, v) in object.iter() {
            if v.is_none() {
                continue;
            }
            let Ok(key) = k.cast::<PyString>() else {
                continue;
            };
            match key.to_str()? {
                "inline" => out.inline = Some(extract_bytes(v.as_borrowed())?),
                "uri" => out.uri = Some(v.extract()?),
                "metadata" if !v.cast::<PyDict>().is_ok_and(|d| d.is_empty()) => {
                    out.metadata = Some(v.extract()?);
                }
                _ => {}
            }
        }
        Ok(out)
    }
}

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("RpcError", m.py().get_type::<RpcError>())?;
    m.add_class::<execution::ExecutionTransport>()?;
    m.add_class::<execution::ExecutionEvents>()?;
    m.add_class::<execution::InputInvocation>()?;
    m.add_class::<session::WorkerConnection>()?;
    m.add_class::<session::Assignment>()?;
    m.add_class::<session::WorkerAttempt>()?;
    Ok(())
}

fn build_runtime() -> Result<Runtime> {
    Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .context("create tokio runtime")
}

fn lock_shared<'a, T>(shared: &'a Arc<Mutex<T>>, label: &str) -> Result<MutexGuard<'a, T>> {
    shared.lock().map_err(|_| anyhow!("{label} lock poisoned"))
}

fn status_error(status: tonic::Status) -> PyErr {
    let error = RpcError::new_err(status.message().to_owned());
    Python::attach(|py| {
        let _ = error.value(py).setattr("status_code", status.code() as i32);
    });
    error
}
fn connection_error(error: anyhow::Error) -> PyErr {
    match error.downcast_ref::<tonic::Status>() {
        Some(status) => status_error(status.clone()),
        None => execution::error(error),
    }
}
