//! Bearer-token authentication for the control-plane gRPC API.
//!
//! Edge workers dial the control plane across the WAN, so the API needs a
//! credential check before any request reaches the runtime. Tokens are split by
//! role: worker tokens open execution sessions and attempt data links; client
//! tokens invoke, submit, inspect and cancel jobs.

use std::collections::BTreeSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use http::HeaderMap;
use tonic::Status;
use tonic::body::BoxBody;
use tower::{Layer, Service};

const V2_CLIENT_PREFIX: &str = "/anyserve.v2.Client/";
const V2_WORKER_PREFIX: &str = "/anyserve.v2.Worker/";
const HEALTH_PREFIX: &str = "/grpc.health.v1.Health/";
const REFLECTION_PREFIX: &str = "/grpc.reflection.";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Client,
    Worker,
}

#[derive(Clone, Debug, Default)]
pub struct AuthPolicy {
    client_tokens: BTreeSet<String>,
    worker_tokens: BTreeSet<String>,
}

#[allow(clippy::result_large_err)] // tonic::Status is the natural error for gRPC auth.
impl AuthPolicy {
    pub fn new(
        client_tokens: impl IntoIterator<Item = String>,
        worker_tokens: impl IntoIterator<Item = String>,
    ) -> Self {
        let clean = |tokens: &mut dyn Iterator<Item = String>| -> BTreeSet<String> {
            tokens
                .map(|token| token.trim().to_string())
                .filter(|token| !token.is_empty())
                .collect()
        };
        Self {
            client_tokens: clean(&mut client_tokens.into_iter()),
            worker_tokens: clean(&mut worker_tokens.into_iter()),
        }
    }

    pub fn is_enabled(&self) -> bool {
        !self.client_tokens.is_empty() || !self.worker_tokens.is_empty()
    }

    /// Returns the role a bearer token grants, if any.
    pub fn role_for_token(&self, token: &str) -> Option<Role> {
        if contains_token(&self.worker_tokens, token) {
            Some(Role::Worker)
        } else if contains_token(&self.client_tokens, token) {
            Some(Role::Client)
        } else {
            None
        }
    }

    pub fn authorize_client_headers(&self, headers: &HeaderMap) -> Result<(), Status> {
        if !self.is_enabled() {
            return Ok(());
        }
        match bearer_token(headers).and_then(|token| self.role_for_token(token)) {
            Some(Role::Client) => Ok(()),
            Some(Role::Worker) => Err(Status::permission_denied(
                "worker token cannot be used for client requests",
            )),
            None => Err(Status::unauthenticated("missing or invalid bearer token")),
        }
    }

    pub fn authorize(&self, path: &str, headers: &HeaderMap) -> Result<(), Status> {
        if !self.is_enabled()
            || path.starts_with(HEALTH_PREFIX)
            || path.starts_with(REFLECTION_PREFIX)
        {
            return Ok(());
        }
        let role = bearer_token(headers)
            .and_then(|token| self.role_for_token(token))
            .ok_or_else(|| Status::unauthenticated("missing or invalid bearer token"))?;
        if (role == Role::Client && path.starts_with(V2_WORKER_PREFIX))
            || (role == Role::Worker && path.starts_with(V2_CLIENT_PREFIX))
        {
            return Err(Status::permission_denied(
                "token role cannot call this execution service",
            ));
        }
        Ok(())
    }
}

fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| {
            value
                .strip_prefix("Bearer ")
                .or_else(|| value.strip_prefix("bearer "))
        })
        .map(str::trim)
}

fn contains_token(tokens: &BTreeSet<String>, candidate: &str) -> bool {
    // Compare against every token without short-circuiting on content.
    tokens.iter().fold(false, |found, token| {
        found | constant_time_eq(token, candidate)
    })
}

fn constant_time_eq(left: &str, right: &str) -> bool {
    let (left, right) = (left.as_bytes(), right.as_bytes());
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0u8, |diff, (a, b)| diff | (a ^ b))
        == 0
}

#[derive(Clone)]
pub struct AuthLayer {
    policy: Arc<AuthPolicy>,
}

impl AuthLayer {
    pub fn new(policy: AuthPolicy) -> Self {
        Self {
            policy: Arc::new(policy),
        }
    }
}

impl<S> Layer<S> for AuthLayer {
    type Service = AuthService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        AuthService {
            inner,
            policy: Arc::clone(&self.policy),
        }
    }
}

#[derive(Clone)]
pub struct AuthService<S> {
    inner: S,
    policy: Arc<AuthPolicy>,
}

impl<S, B> Service<http::Request<B>> for AuthService<S>
where
    S: Service<http::Request<B>, Response = http::Response<BoxBody>>,
    S::Future: Send + 'static,
{
    type Response = http::Response<BoxBody>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut request: http::Request<B>) -> Self::Future {
        match self
            .policy
            .authorize(request.uri().path(), request.headers())
        {
            Ok(()) => {
                if self.policy.is_enabled()
                    && let Some(role) = bearer_token(request.headers())
                        .and_then(|token| self.policy.role_for_token(token))
                {
                    request.extensions_mut().insert(role);
                }
                Box::pin(self.inner.call(request))
            }
            Err(status) => Box::pin(async move { Ok(status.into_http()) }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers(token: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::AUTHORIZATION,
            format!("Bearer {token}").parse().unwrap(),
        );
        headers
    }

    fn path(method: &str) -> String {
        format!("{V2_CLIENT_PREFIX}{method}")
    }

    #[test]
    fn disabled_policy_allows_everything() {
        let policy = AuthPolicy::default();
        assert!(policy.authorize(&path("Submit"), &HeaderMap::new()).is_ok());
    }

    #[test]
    fn roles_are_enforced_per_service() {
        let policy = AuthPolicy::new(["client-secret".to_string()], ["worker-secret".to_string()]);
        for method in ["Submit", "Invoke", "Watch", "ListJobs", "GetJob", "Cancel"] {
            assert!(
                policy
                    .authorize(&path(method), &headers("client-secret"))
                    .is_ok()
            );
            assert_eq!(
                policy
                    .authorize(&path(method), &headers("worker-secret"))
                    .unwrap_err()
                    .code(),
                tonic::Code::PermissionDenied
            );
        }
        for method in ["Session", "Attempt"] {
            let path = format!("{V2_WORKER_PREFIX}{method}");
            assert!(policy.authorize(&path, &headers("worker-secret")).is_ok());
            assert_eq!(
                policy
                    .authorize(&path, &headers("client-secret"))
                    .unwrap_err()
                    .code(),
                tonic::Code::PermissionDenied
            );
        }
        for token in [None, Some("nope")] {
            let headers = token.map(headers).unwrap_or_default();
            assert_eq!(
                policy
                    .authorize(&path("GetJob"), &headers)
                    .unwrap_err()
                    .code(),
                tonic::Code::Unauthenticated
            );
        }
    }

    #[test]
    fn health_checks_skip_auth() {
        let policy = AuthPolicy::new(["c".to_string()], ["w".to_string()]);
        assert!(
            policy
                .authorize("/grpc.health.v1.Health/Check", &HeaderMap::new())
                .is_ok()
        );
    }
}
