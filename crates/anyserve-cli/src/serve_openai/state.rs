use std::sync::Arc;

use anyserve_core::kernel::Kernel;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Serialize;

use crate::serve_config::ServeOpenAIConfig;

#[derive(Clone)]
pub(super) struct AppState {
    pub(super) kernel: Arc<Kernel>,
    pub(super) config: Arc<ServeOpenAIConfig>,
}

#[derive(Serialize)]
pub(super) struct OpenAIErrorEnvelope {
    pub(super) error: OpenAIErrorBody,
}

#[derive(Serialize)]
pub(super) struct OpenAIErrorBody {
    pub(super) message: String,
    #[serde(rename = "type")]
    pub(super) error_type: String,
    pub(super) code: String,
}

#[derive(Debug)]
pub(super) struct OpenAIError {
    pub(super) status: StatusCode,
    pub(super) message: String,
    pub(super) error_type: &'static str,
    pub(super) code: &'static str,
}

pub(super) struct JobCancellationGuard {
    kernel: Arc<Kernel>,
    job_id: String,
    active: bool,
}

impl JobCancellationGuard {
    pub(super) fn new(kernel: Arc<Kernel>, job_id: String) -> Self {
        Self {
            kernel,
            job_id,
            active: true,
        }
    }

    pub(super) fn disarm(&mut self) {
        self.active = false;
    }
}

impl Drop for JobCancellationGuard {
    fn drop(&mut self) {
        if !self.active {
            return;
        }

        let kernel = Arc::clone(&self.kernel);
        let job_id = self.job_id.clone();
        tokio::spawn(async move {
            let _ = kernel.cancel_job(&job_id).await;
        });
    }
}

impl OpenAIError {
    pub(super) fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
            error_type: "invalid_request_error",
            code: "invalid_request",
        }
    }

    pub(super) fn gateway_timeout(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::GATEWAY_TIMEOUT,
            message: message.into(),
            error_type: "timeout_error",
            code: "gateway_timeout",
        }
    }

    pub(super) fn bad_gateway(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_GATEWAY,
            message: message.into(),
            error_type: "upstream_error",
            code: "bad_gateway",
        }
    }

    pub(super) fn internal(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: message.into(),
            error_type: "server_error",
            code: "internal_error",
        }
    }
}

impl IntoResponse for OpenAIError {
    fn into_response(self) -> Response {
        (
            self.status,
            axum::Json(OpenAIErrorEnvelope {
                error: OpenAIErrorBody {
                    message: self.message,
                    error_type: self.error_type.to_string(),
                    code: self.code.to_string(),
                },
            }),
        )
            .into_response()
    }
}
