mod execution;
mod session;

use anyhow::{Context, Result, anyhow};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};
pyo3::create_exception!(_native, RpcError, PyRuntimeError);
use pyo3::types::PyDict;
use tokio::runtime::{Builder, Runtime};

type SharedRuntime = Arc<Runtime>;

struct PyObjectRef {
    inline: Option<Vec<u8>>,
    uri: Option<String>,
    metadata: Option<HashMap<String, String>>,
}

impl<'a, 'py> FromPyObject<'a, 'py> for PyObjectRef {
    type Error = PyErr;

    fn extract(object: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        let object = object.cast::<PyDict>()?;
        Ok(Self {
            inline: object
                .get_item("inline")?
                .map(|value| value.extract())
                .transpose()?
                .flatten(),
            uri: object
                .get_item("uri")?
                .map(|value| value.extract())
                .transpose()?
                .flatten(),
            metadata: object
                .get_item("metadata")?
                .map(|value| value.extract())
                .transpose()?
                .flatten(),
        })
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
