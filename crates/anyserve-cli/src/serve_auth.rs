//! Client-role authentication for the management HTTP API.

use std::sync::Arc;

use anyserve_core::auth::AuthPolicy;
use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

pub(crate) async fn require_client_token(
    State(policy): State<Arc<AuthPolicy>>,
    request: Request,
    next: Next,
) -> Response {
    if request.uri().path() == "/healthz" {
        return next.run(request).await;
    }
    match policy.authorize_client_headers(request.headers()) {
        Ok(()) => next.run(request).await,
        Err(status) => {
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
    }
}
