use std::collections::HashMap;
use std::future::Future;

use anyhow::{Context, Result};
use anyserve_proto::inference::grpc_inference_service_client::GrpcInferenceServiceClient;
use anyserve_proto::inference::{FetchInferRequest, InferCore, InferRequest, SendResponseRequest};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};
use tokio::runtime::Builder;
use tokio_stream::StreamExt;
use tonic::Request;

#[pyclass]
#[derive(Clone)]
struct AnyserveClient {
    endpoint: String,
}

#[pymethods]
impl AnyserveClient {
    #[new]
    fn new(endpoint: String) -> Self {
        Self { endpoint }
    }

    #[pyo3(signature = (content, metadata=None, queue=None, request_id=None))]
    fn infer(
        &self,
        py: Python<'_>,
        content: Vec<u8>,
        metadata: Option<HashMap<String, String>>,
        queue: Option<String>,
        request_id: Option<String>,
    ) -> PyResult<Vec<Py<PyDict>>> {
        let endpoint = self.endpoint.clone();
        let metadata = metadata.unwrap_or_default();
        let queue = queue.unwrap_or_else(|| "default".to_string());

        let chunks = py
            .allow_threads(move || infer_impl(endpoint, content, metadata, queue, request_id))
            .map_err(to_py_err)?;

        chunks
            .into_iter()
            .map(|(request_id, response)| infer_chunk_to_py(py, request_id, response))
            .collect()
    }

    #[pyo3(signature = (metadata=None, queue=None))]
    fn fetch_one(
        &self,
        py: Python<'_>,
        metadata: Option<HashMap<String, String>>,
        queue: Option<String>,
    ) -> PyResult<Option<Py<PyDict>>> {
        let endpoint = self.endpoint.clone();
        let metadata = metadata.unwrap_or_default();
        let queue = queue.unwrap_or_else(|| "default".to_string());

        let fetched = py
            .allow_threads(move || fetch_impl(endpoint, metadata, queue))
            .map_err(to_py_err)?;

        fetched
            .map(|(request_id, infer)| fetched_request_to_py(py, request_id, infer))
            .transpose()
    }

    fn send_responses(
        &self,
        py: Python<'_>,
        request_id: String,
        responses: Vec<(Vec<u8>, HashMap<String, String>)>,
    ) -> PyResult<()> {
        let endpoint = self.endpoint.clone();
        py.allow_threads(move || send_responses_impl(endpoint, request_id, responses))
            .map_err(to_py_err)?;
        Ok(())
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

fn infer_impl(
    endpoint: String,
    content: Vec<u8>,
    metadata: HashMap<String, String>,
    queue: String,
    request_id: Option<String>,
) -> Result<Vec<(String, InferCore)>> {
    run_async(async move {
        let mut client = GrpcInferenceServiceClient::connect(endpoint)
            .await
            .context("connect anyserve gRPC endpoint")?;

        let request = InferRequest {
            queue: Some(queue),
            infer: Some(InferCore { content, metadata }),
            request_id,
        };

        let mut stream = client
            .infer(Request::new(request))
            .await
            .context("send infer request")?
            .into_inner();

        let mut responses = Vec::new();
        while let Some(item) = stream.next().await {
            let item = item.context("receive infer response")?;
            let request_id = item.request_id;
            let response = item.response.context("infer response is missing payload")?;
            responses.push((request_id, response));
        }

        Ok(responses)
    })
}

fn fetch_impl(
    endpoint: String,
    metadata: HashMap<String, String>,
    queue: String,
) -> Result<Option<(String, InferCore)>> {
    run_async(async move {
        let mut client = GrpcInferenceServiceClient::connect(endpoint)
            .await
            .context("connect anyserve gRPC endpoint")?;

        let mut stream = client
            .fetch_infer(Request::new(FetchInferRequest {
                queue: Some(queue),
                metadata,
            }))
            .await
            .context("send fetch request")?
            .into_inner();

        if let Some(item) = stream.next().await {
            let item = item.context("receive fetch response")?;
            let infer = item.infer.context("fetched request is missing payload")?;
            Ok(Some((item.request_id, infer)))
        } else {
            Ok(None)
        }
    })
}

fn send_responses_impl(
    endpoint: String,
    request_id: String,
    responses: Vec<(Vec<u8>, HashMap<String, String>)>,
) -> Result<()> {
    run_async(async move {
        let mut client = GrpcInferenceServiceClient::connect(endpoint)
            .await
            .context("connect anyserve gRPC endpoint")?;

        let outbound = tokio_stream::iter(responses.into_iter().map(move |(content, metadata)| {
            SendResponseRequest {
                request_id: request_id.clone(),
                response: Some(InferCore { content, metadata }),
                metrics: HashMap::new(),
            }
        }));

        client
            .send_response(Request::new(outbound))
            .await
            .context("send worker responses")?;

        Ok(())
    })
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

fn infer_chunk_to_py(
    py: Python<'_>,
    request_id: String,
    response: InferCore,
) -> PyResult<Py<PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("request_id", request_id)?;
    dict.set_item("content", PyBytes::new(py, &response.content))?;
    dict.set_item("metadata", response.metadata)?;
    Ok(dict.unbind())
}

fn fetched_request_to_py(
    py: Python<'_>,
    request_id: String,
    infer: InferCore,
) -> PyResult<Py<PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("request_id", request_id)?;
    dict.set_item("content", PyBytes::new(py, &infer.content))?;
    dict.set_item("metadata", infer.metadata)?;
    Ok(dict.unbind())
}

fn to_py_err(error: anyhow::Error) -> PyErr {
    PyRuntimeError::new_err(error.to_string())
}
