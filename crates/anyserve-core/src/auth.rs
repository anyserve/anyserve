//! Bearer-token authentication and pluggable verifier traits for the control plane.
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
const CLUSTER_PREFIX: &str = "/anyserve.cluster.v1.";
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

#[allow(clippy::result_large_err)] // tonic::Status is the natural error for gRPC auth.
pub trait AuthVerifier: Send + Sync + 'static {
    fn is_enabled(&self) -> bool;
    fn verify(&self, path: &str, headers: &HeaderMap) -> Result<Option<Principal>, Status>;
    fn verify_client_headers(&self, headers: &HeaderMap) -> Result<Principal, Status>;
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
    peer_tokens: BTreeSet<String>,
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
            peer_tokens: BTreeSet::new(),
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

    pub fn with_peer_tokens(mut self, tokens: impl IntoIterator<Item = String>) -> Self {
        self.peer_tokens = tokens
            .into_iter()
            .map(|token| token.trim().to_string())
            .filter(|token| !token.is_empty())
            .collect();
        self
    }

    pub fn has_peer_tokens(&self) -> bool {
        !self.peer_tokens.is_empty()
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
            None if contains_token(&self.peer_tokens, token) => Err(Status::permission_denied(
                "peer token cannot access management API",
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
}

#[allow(clippy::result_large_err)]
impl AuthVerifier for AuthPolicy {
    fn is_enabled(&self) -> bool {
        AuthPolicy::is_enabled(self)
    }

    fn verify(&self, path: &str, headers: &HeaderMap) -> Result<Option<Principal>, Status> {
        if path.starts_with(HEALTH_PREFIX) || path.starts_with(REFLECTION_PREFIX) {
            return Ok(None);
        }
        if path.starts_with(CLUSTER_PREFIX) {
            if self.peer_tokens.is_empty() {
                return Err(Status::permission_denied(
                    "PeerMesh is disabled (configure [cluster].peer_token_env)",
                ));
            }
            let token = bearer_token(headers)
                .ok_or_else(|| Status::unauthenticated("missing peer bearer token"))?;
            if contains_token(&self.peer_tokens, token) {
                return Ok(None);
            }
            if self.principal_for_token(token).is_some() {
                return Err(Status::permission_denied(
                    "client or worker tokens cannot invoke PeerMesh",
                ));
            }
            return Err(Status::unauthenticated("invalid peer bearer token"));
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

    fn verify_client_headers(&self, headers: &HeaderMap) -> Result<Principal, Status> {
        if !self.is_enabled() {
            return Self::unauthenticated_principal(Role::Client, headers);
        }
        match bearer_token(headers).and_then(|token| self.principal_for_token(token)) {
            Some(principal) if principal.role == Role::Client => Ok(principal),
            Some(_) => Err(Status::permission_denied(
                "worker token cannot be used for client requests",
            )),
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
    verifier: Arc<dyn AuthVerifier>,
}

impl AuthLayer {
    pub fn new(policy: AuthPolicy) -> Self {
        Self::from_verifier(Arc::new(policy))
    }

    pub fn from_verifier(verifier: Arc<dyn AuthVerifier>) -> Self {
        Self { verifier }
    }
}

impl<S> Layer<S> for AuthLayer {
    type Service = AuthService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        AuthService {
            inner,
            verifier: Arc::clone(&self.verifier),
        }
    }
}

#[derive(Clone)]
pub struct AuthService<S> {
    inner: S,
    verifier: Arc<dyn AuthVerifier>,
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
            .verifier
            .verify(request.uri().path(), request.headers())
        {
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
    fn peermesh_requires_dedicated_peer_token_and_management_requires_operator_token() {
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
            ])
            .with_peer_tokens(["peer-secret".into()]);

        for method in [
            "/anyserve.cluster.v1.PeerMesh/SyncDirectory",
            "/anyserve.cluster.v1.PeerMesh/ForwardInvoke",
            "/anyserve.cluster.v1.PeerMesh/WatchRemote",
        ] {
            assert_eq!(
                policy
                    .authorize(method, &HeaderMap::new())
                    .unwrap_err()
                    .code(),
                tonic::Code::Unauthenticated
            );
            for bad in [
                "op-client",
                "op-worker",
                "scoped-a-client",
                "scoped-b-worker",
            ] {
                assert_eq!(
                    policy.authorize(method, &headers(bad)).unwrap_err().code(),
                    tonic::Code::PermissionDenied
                );
            }
            assert!(policy.authorize(method, &headers("peer-secret")).is_ok());
        }

        // Peer token must not work on public Client/Worker APIs.
        assert_eq!(
            policy
                .authorize(&path("GetJob"), &headers("peer-secret"))
                .unwrap_err()
                .code(),
            tonic::Code::Unauthenticated
        );

        // Management API only allows operator client tokens.
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
            "peer-secret",
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
}
