//! Bearer-token authentication for the control plane.
//!
//! Edge workers dial the control plane across the WAN, so the API needs a
//! credential check before any request reaches the runtime. Tokens are split by
//! role and resolve into a [`Principal`] carrying the caller's `namespace`.

use std::collections::BTreeSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use http::HeaderMap;
use tonic::Status;
use tonic::body::BoxBody;
use tower::{Layer, Service};

pub const DEFAULT_NAMESPACE: &str = "default";
pub const NAMESPACE_HEADER: &str = "x-anyserve-namespace";

const V2_CLIENT_PREFIX: &str = "/anyserve.v2.Client/";
const V2_WORKER_PREFIX: &str = "/anyserve.v2.Worker/";
const HEALTH_PREFIX: &str = "/grpc.health.v1.Health/";
const REFLECTION_PREFIX: &str = "/grpc.reflection.";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Client,
    Worker,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Principal {
    pub namespace: String,
    pub role: Role,
    pub subject: String,
    /// Optional pool override bound to a worker credential so edge/spot workers
    /// cannot spoof a reserved tier.
    pub forced_pool: Option<String>,
    /// True only for operator-level client tokens (`[auth].client_tokens`) or
    /// when auth is disabled; `false` for tenant `[[auth.scoped_tokens]]` and workers.
    pub is_operator: bool,
}

impl Default for Principal {
    fn default() -> Self {
        Self {
            namespace: DEFAULT_NAMESPACE.to_string(),
            role: Role::Client,
            subject: "anonymous".to_string(),
            forced_pool: None,
            is_operator: true,
        }
    }
}

/// Validates a tenant namespace identifier (`1..=64` ASCII `[a-zA-Z0-9._-]`).
#[allow(clippy::result_large_err)] // tonic::Status is the natural error for gRPC auth.
pub fn validate_namespace(namespace: &str) -> Result<&str, Status> {
    if namespace.is_empty()
        || namespace.len() > 64
        || !namespace
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
    {
        return Err(Status::invalid_argument(
            "namespace must be 1..=64 ASCII characters in [a-zA-Z0-9._-]",
        ));
    }
    Ok(namespace)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScopedToken {
    pub token: String,
    pub role: Role,
    pub namespace: String,
    pub subject: String,
    pub forced_pool: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub struct AuthPolicy {
    client_tokens: BTreeSet<String>,
    worker_tokens: BTreeSet<String>,
    scoped_tokens: Vec<ScopedToken>,
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
            scoped_tokens: Vec::new(),
        }
    }

    pub fn with_scoped_tokens(mut self, tokens: impl IntoIterator<Item = ScopedToken>) -> Self {
        self.scoped_tokens = tokens
            .into_iter()
            .filter_map(|mut entry| {
                entry.token = entry.token.trim().to_string();
                (!entry.token.is_empty()).then_some(entry)
            })
            .collect();
        self
    }

    pub fn is_enabled(&self) -> bool {
        !self.client_tokens.is_empty()
            || !self.worker_tokens.is_empty()
            || !self.scoped_tokens.is_empty()
    }

    /// Returns the role a bearer token grants, if any.
    pub fn role_for_token(&self, token: &str) -> Option<Role> {
        self.principal_for_token(token).map(|p| p.role)
    }

    pub fn principal_for_token(&self, candidate: &str) -> Option<Principal> {
        let mut matched: Option<Principal> = None;
        for entry in &self.scoped_tokens {
            if constant_time_eq(&entry.token, candidate) && matched.is_none() {
                matched = Some(Principal {
                    namespace: entry.namespace.clone(),
                    role: entry.role,
                    subject: entry.subject.clone(),
                    forced_pool: entry.forced_pool.clone(),
                    is_operator: false,
                });
            }
        }
        if contains_token(&self.worker_tokens, candidate) && matched.is_none() {
            matched = Some(Principal {
                namespace: DEFAULT_NAMESPACE.to_string(),
                role: Role::Worker,
                subject: "worker-token".to_string(),
                forced_pool: None,
                is_operator: false,
            });
        }
        if contains_token(&self.client_tokens, candidate) && matched.is_none() {
            matched = Some(Principal {
                namespace: DEFAULT_NAMESPACE.to_string(),
                role: Role::Client,
                subject: "client-token".to_string(),
                forced_pool: None,
                is_operator: true,
            });
        }
        matched
    }

    pub fn authorize_client_headers(&self, headers: &HeaderMap) -> Result<(), Status> {
        self.verify_client_headers(headers).map(|_| ())
    }

    /// Verifies management/operator HTTP access: requires an operator-level
    /// client token when auth is enabled; tenant-scoped and worker tokens get `PermissionDenied`.
    pub fn authorize_operator_headers(&self, headers: &HeaderMap) -> Result<(), Status> {
        if !self.is_enabled() {
            return Ok(());
        }
        let token = bearer_token(headers)
            .ok_or_else(|| Status::unauthenticated("missing or invalid bearer token"))?;
        match self.principal_for_token(token) {
            Some(principal) if principal.role == Role::Client && principal.is_operator => Ok(()),
            Some(_) => Err(Status::permission_denied(
                "management API requires an operator client token",
            )),
            None => Err(Status::unauthenticated("missing or invalid bearer token")),
        }
    }

    pub fn authorize(&self, path: &str, headers: &HeaderMap) -> Result<(), Status> {
        self.verify(path, headers).map(|_| ())
    }

    fn unauthenticated_principal(role: Role, headers: &HeaderMap) -> Result<Principal, Status> {
        let namespace = match headers
            .get(NAMESPACE_HEADER)
            .and_then(|value| value.to_str().ok())
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            Some(ns) => validate_namespace(ns)?.to_string(),
            None => DEFAULT_NAMESPACE.to_string(),
        };
        Ok(Principal {
            namespace,
            role,
            subject: "anonymous".to_string(),
            forced_pool: None,
            is_operator: role == Role::Client,
        })
    }

    pub fn verify(&self, path: &str, headers: &HeaderMap) -> Result<Option<Principal>, Status> {
        if path.starts_with(HEALTH_PREFIX) || path.starts_with(REFLECTION_PREFIX) {
            return Ok(None);
        }
        let default_role = if path.starts_with(V2_WORKER_PREFIX) {
            Role::Worker
        } else {
            Role::Client
        };
        if !self.is_enabled() {
            return Self::unauthenticated_principal(default_role, headers).map(Some);
        }
        let principal = bearer_token(headers)
            .and_then(|token| self.principal_for_token(token))
            .ok_or_else(|| Status::unauthenticated("missing or invalid bearer token"))?;
        if (principal.role == Role::Client && path.starts_with(V2_WORKER_PREFIX))
            || (principal.role == Role::Worker && path.starts_with(V2_CLIENT_PREFIX))
        {
            return Err(Status::permission_denied(
                "token role cannot call this execution service",
            ));
        }
        Ok(Some(principal))
    }

    pub fn verify_client_headers(&self, headers: &HeaderMap) -> Result<Principal, Status> {
        self.verify_role_headers(headers, Role::Client)
    }

    pub fn verify_worker_headers(&self, headers: &HeaderMap) -> Result<Principal, Status> {
        self.verify_role_headers(headers, Role::Worker)
    }

    fn verify_role_headers(
        &self,
        headers: &HeaderMap,
        expected: Role,
    ) -> Result<Principal, Status> {
        if !self.is_enabled() {
            return Self::unauthenticated_principal(expected, headers);
        }
        match bearer_token(headers).and_then(|token| self.principal_for_token(token)) {
            Some(principal) if principal.role == expected => Ok(principal),
            Some(_) => Err(Status::permission_denied(match expected {
                Role::Client => "worker token cannot be used for client requests",
                Role::Worker => "client token cannot be used for worker requests",
            })),
            None => Err(Status::unauthenticated("missing or invalid bearer token")),
        }
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
        match self.policy.verify(request.uri().path(), request.headers()) {
            Ok(principal) => {
                if let Some(principal) = principal {
                    request.extensions_mut().insert(principal.role);
                    request.extensions_mut().insert(principal);
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
        let mut h = HeaderMap::new();
        h.insert(NAMESPACE_HEADER, "edge-robot-7".parse().unwrap());
        let principal = policy.verify(&path("Submit"), &h).unwrap().unwrap();
        assert_eq!(principal.namespace, "edge-robot-7");
    }

    #[test]
    fn scoped_tokens_derive_namespace_and_forced_pool_ignoring_header() {
        let policy = AuthPolicy::default().with_scoped_tokens([ScopedToken {
            token: "tok-robot".into(),
            role: Role::Worker,
            namespace: "factory-a".into(),
            subject: "jetson-1".into(),
            forced_pool: Some("jetson-local".into()),
        }]);
        let mut h = headers("tok-robot");
        h.insert(NAMESPACE_HEADER, "spoofed".parse().unwrap());
        let principal = policy
            .verify(&format!("{V2_WORKER_PREFIX}Session"), &h)
            .unwrap()
            .unwrap();
        assert_eq!(principal.namespace, "factory-a");
        assert_eq!(principal.forced_pool.as_deref(), Some("jetson-local"));
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

    #[test]
    fn management_requires_operator_token() {
        let policy = AuthPolicy::new(["op-client".into()], ["op-worker".into()])
            .with_scoped_tokens([
                ScopedToken {
                    token: "scoped-a-client".into(),
                    role: Role::Client,
                    namespace: "tenant-a".into(),
                    subject: "a".into(),
                    forced_pool: None,
                },
                ScopedToken {
                    token: "scoped-default-client".into(),
                    role: Role::Client,
                    namespace: "default".into(),
                    subject: "d".into(),
                    forced_pool: None,
                },
                ScopedToken {
                    token: "scoped-b-worker".into(),
                    role: Role::Worker,
                    namespace: "tenant-b".into(),
                    subject: "w".into(),
                    forced_pool: None,
                },
            ]);

        assert!(
            policy
                .authorize_operator_headers(&headers("op-client"))
                .is_ok()
        );
        for denied in [
            "op-worker",
            "scoped-a-client",
            "scoped-default-client",
            "scoped-b-worker",
        ] {
            assert_eq!(
                policy
                    .authorize_operator_headers(&headers(denied))
                    .unwrap_err()
                    .code(),
                tonic::Code::PermissionDenied
            );
        }
        assert_eq!(
            policy
                .authorize_operator_headers(&HeaderMap::new())
                .unwrap_err()
                .code(),
            tonic::Code::Unauthenticated
        );
    }

    #[test]
    fn verify_worker_headers_enforces_roles_and_namespaces() {
        let disabled = AuthPolicy::default();
        let p = disabled.verify_worker_headers(&HeaderMap::new()).unwrap();
        assert_eq!(p.role, Role::Worker);
        assert_eq!(p.namespace, DEFAULT_NAMESPACE);

        let mut ns_headers = HeaderMap::new();
        ns_headers.insert(NAMESPACE_HEADER, "edge-team".parse().unwrap());
        let p = disabled.verify_worker_headers(&ns_headers).unwrap();
        assert_eq!(p.namespace, "edge-team");

        let mut bad_ns_headers = HeaderMap::new();
        bad_ns_headers.insert(NAMESPACE_HEADER, "bad/ns".parse().unwrap());
        assert_eq!(
            disabled
                .verify_worker_headers(&bad_ns_headers)
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );

        let policy = AuthPolicy::new(["client-secret".into()], ["worker-secret".into()])
            .with_scoped_tokens([ScopedToken {
                token: "scoped-worker".into(),
                role: Role::Worker,
                namespace: "factory-a".into(),
                subject: "jetson-1".into(),
                forced_pool: Some("jetson-local".into()),
            }]);

        assert_eq!(
            policy
                .verify_worker_headers(&HeaderMap::new())
                .unwrap_err()
                .code(),
            tonic::Code::Unauthenticated
        );
        assert_eq!(
            policy
                .verify_worker_headers(&headers("unknown-token"))
                .unwrap_err()
                .code(),
            tonic::Code::Unauthenticated
        );
        assert_eq!(
            policy
                .verify_worker_headers(&headers("client-secret"))
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );

        let worker_principal = policy
            .verify_worker_headers(&headers("worker-secret"))
            .unwrap();
        assert_eq!(worker_principal.role, Role::Worker);
        assert_eq!(worker_principal.namespace, DEFAULT_NAMESPACE);

        let mut scoped_headers = headers("scoped-worker");
        scoped_headers.insert(NAMESPACE_HEADER, "spoofed".parse().unwrap());
        let scoped_principal = policy.verify_worker_headers(&scoped_headers).unwrap();
        assert_eq!(scoped_principal.role, Role::Worker);
        assert_eq!(scoped_principal.namespace, "factory-a");
        assert_eq!(
            scoped_principal.forced_pool.as_deref(),
            Some("jetson-local")
        );
    }
}
