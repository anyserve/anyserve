use std::sync::Arc;

use anyserve_core::runtime::Runtime;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Serialize;

use crate::serve_config::ServeOpenAIConfig;

#[derive(Clone)]
pub(super) struct AppState {
    pub(super) runtime: Runtime,
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

impl OpenAIError {
    pub(super) fn runtime(error: anyserve_core::error::RuntimeError) -> Self {
        Self {
            status: crate::serve_execution::runtime_status(&error),
            message: error.to_string(),
            error_type: "server_error",
            code: "runtime_error",
        }
    }
    pub(super) fn execution(error: Option<&anyserve_core::runtime::api::ErrorInfo>) -> Self {
        let mut response = Self::bad_gateway(
            error
                .map(|error| error.message.as_str())
                .unwrap_or("execution cancelled"),
        );
        response.status = crate::serve_execution::execution_status(error);
        response
    }
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
