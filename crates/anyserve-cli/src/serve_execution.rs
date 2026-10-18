//! Shared result collection and failure mapping for HTTP execution adapters.
use anyserve_core::error::{ErrorKind, RuntimeError};
use anyserve_core::runtime::api::{
    Demand, ErrorInfo, EventKind, Execution, JobState, Object, WorkerView,
};
use axum::http::StatusCode;
use futures::StreamExt;

pub(crate) struct ExecutionResult {
    pub worker_id: String,
    pub attempts: u32,
    pub state: JobState,
    pub outputs: Vec<Object>,
    pub error: Option<ErrorInfo>,
}

pub(crate) async fn collect(mut execution: Execution) -> Result<ExecutionResult, RuntimeError> {
    let mut worker_id = String::new();
    let mut attempts = 0;
    while let Some(event) = execution.next().await {
        let event = event?;
        match event.kind {
            EventKind::Assigned {
                worker_id: worker,
                attempt_number,
            } => {
                worker_id = worker;
                attempts = attempt_number;
            }
            EventKind::Finished {
                state,
                outputs,
                error,
            } => {
                return Ok(ExecutionResult {
                    worker_id,
                    attempts,
                    state,
                    outputs,
                    error,
                });
            }
            _ => {}
        }
    }
    Err(RuntimeError::unavailable(
        "execution ended without a terminal event",
    ))
}

pub(crate) fn runtime_status(error: &RuntimeError) -> StatusCode {
    match error.kind {
        ErrorKind::InvalidArgument => StatusCode::BAD_REQUEST,
        ErrorKind::NotFound => StatusCode::NOT_FOUND,
        ErrorKind::Conflict => StatusCode::CONFLICT,
        ErrorKind::ResourceExhausted => StatusCode::TOO_MANY_REQUESTS,
        ErrorKind::PersistenceUnavailable => StatusCode::SERVICE_UNAVAILABLE,
        ErrorKind::StaleLease => StatusCode::CONFLICT,
        ErrorKind::CursorExpired { .. } => StatusCode::GONE,
    }
}

pub(crate) fn execution_status(error: Option<&ErrorInfo>) -> StatusCode {
    match error.map(|error| error.code.as_str()) {
        Some("queue_timeout") => StatusCode::SERVICE_UNAVAILABLE,
        Some("total_timeout" | "execution_timeout" | "consumer_stalled") => {
            StatusCode::GATEWAY_TIMEOUT
        }
        _ => StatusCode::BAD_GATEWAY,
    }
}

/// Readiness checks capabilities, not whether every worker slot is idle.
pub(crate) fn worker_can_serve(
    worker: &WorkerView,
    interface: Option<&str>,
    demand: &Demand,
) -> bool {
    worker.connected
        && !worker.draining
        && worker
            .interfaces
            .iter()
            .any(|candidate| interface.is_none_or(|required| candidate == required))
        && demand
            .required_attributes
            .iter()
            .all(|(key, expected)| worker.attributes.get(key) == Some(expected))
        && demand.required_capacity.iter().all(|(key, required)| {
            worker.total_capacity.get(key).copied().unwrap_or_default() >= *required
        })
}
