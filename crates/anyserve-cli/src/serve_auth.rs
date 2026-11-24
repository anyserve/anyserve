//! Client-role authentication for the management HTTP API.

use std::sync::Arc;

use anyserve_core::auth::AuthPolicy;
use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

pub(crate) async fn require_client_token(
    State(policy): State<Arc<AuthPolicy>>,
    mut request: Request,
    next: Next,
) -> Response {
    let path = request.uri().path();
    if matches!(path, "/healthz" | "/readyz") {
        return next.run(request).await;
    }
    if path == "/metrics" {
        return match policy.authorize_operator_headers(request.headers()) {
            Ok(()) => next.run(request).await,
            Err(status) => auth_error_response(status),
        };
    }
    match policy.authorize_console_headers(request.headers()) {
        Ok(principal) => {
            request.extensions_mut().insert(principal);
            next.run(request).await
        }
        Err(status) => auth_error_response(status),
    }
}

fn auth_error_response(status: tonic::Status) -> Response {
    let code = if status.code() == tonic::Code::PermissionDenied {
        StatusCode::FORBIDDEN
    } else {
        StatusCode::UNAUTHORIZED
    };
    (
        code,
        axum::Json(serde_json::json!({ "error": status.message() })),
    )
        .into_response()
}
