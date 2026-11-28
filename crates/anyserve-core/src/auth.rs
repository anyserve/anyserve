//! Bearer-token authentication for the control plane.
//!
//! Edge workers dial the control plane across the WAN, so the API needs a
//! credential check before any request reaches the runtime. Tokens are split by
//! role and resolve into a [`Principal`] carrying the caller's `namespace`.

use std::collections::{BTreeSet, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::task::{Context, Poll};
use std::time::Duration;

use anyhow::Context as _;
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use http::HeaderMap;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use tonic::Status;
use tonic::body::BoxBody;
use tower::{Layer, Service};

pub const DEFAULT_NAMESPACE: &str = "default";
pub const NAMESPACE_HEADER: &str = "x-anyserve-namespace";
pub const SIGNED_TOKEN_PREFIX: &str = "asv1.";
/// Comma-separated token signing keys, newest (signing) key first.
pub const SIGNING_KEYS_ENV: &str = "ANYSERVE_TOKEN_SIGNING_KEY";
/// Negotiated WebSocket subprotocol echoed on 101 upgrades for browser clients/workers.
pub const WS_SUBPROTOCOL_V2: &str = "anyserve.v2";
/// Prefix for base64url-encoded bearer token carried in `Sec-WebSocket-Protocol`
/// (browser WebSocket API cannot set custom `Authorization` headers).
pub const WS_BEARER_SUBPROTOCOL_PREFIX: &str = "anyserve.bearer.";

/// Splits a comma-separated signing-key list, dropping blank entries; the first key signs.
pub fn parse_signing_keys(raw: &str) -> Vec<Vec<u8>> {
    raw.split(',')
        .map(str::trim)
        .filter(|key| !key.is_empty())
        .map(|key| key.as_bytes().to_vec())
        .collect()
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignedTokenClaims {
    pub jti: String,
    pub ns: String,
    pub role: String,
    pub sub: String,
    pub iat: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exp: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fp: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RevocationSnapshot {
    pub version: u64,
    #[serde(default)]
    pub revoked_jtis: Vec<String>,
}

#[derive(Clone, Debug, Default)]
struct RevocationState {
    version: u64,
    revoked_jtis: HashSet<String>,
    last_sync_at_ms: Option<u64>,
}

fn now_secs() -> u64 {
    crate::now_ms() / 1000
}

pub fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    const BLOCK_LEN: usize = 64;
    let mut k0 = [0u8; BLOCK_LEN];
    if key.len() > BLOCK_LEN {
        let digest = Sha256::digest(key);
        k0[..32].copy_from_slice(&digest);
    } else {
        k0[..key.len()].copy_from_slice(key);
    }

    let mut ipad = [0x36u8; BLOCK_LEN];
    let mut opad = [0x5cu8; BLOCK_LEN];
    for i in 0..BLOCK_LEN {
        ipad[i] ^= k0[i];
        opad[i] ^= k0[i];
    }

    let mut inner = Sha256::new();
    inner.update(ipad);
    inner.update(data);
    let inner_hash = inner.finalize();

    let mut outer = Sha256::new();
    outer.update(opad);
    outer.update(inner_hash);
    outer.finalize().into()
}

fn parse_role(raw: &str) -> Option<Role> {
    match raw {
        "client" => Some(Role::Client),
        "worker" => Some(Role::Worker),
        _ => None,
    }
}

/// Signs claims into a stateless `asv1.<b64url-json>.<b64url-hmac>` bearer token.
pub fn sign_token(key: &[u8], claims: &SignedTokenClaims) -> anyhow::Result<String> {
    anyhow::ensure!(!key.is_empty(), "signing key must not be empty");
    anyhow::ensure!(!claims.jti.trim().is_empty(), "token jti must not be empty");
    validate_namespace(&claims.ns)
        .map_err(|s| anyhow::anyhow!("invalid token namespace: {}", s.message()))?;
    anyhow::ensure!(
        parse_role(&claims.role).is_some(),
        "token role must be 'client' or 'worker'"
    );
    let json = serde_json::to_vec(claims).context("serialize token claims")?;
    let payload = URL_SAFE_NO_PAD.encode(json);
    let signing_input = format!("{SIGNED_TOKEN_PREFIX}{payload}");
    let mac = hmac_sha256(key, signing_input.as_bytes());
    let sig = URL_SAFE_NO_PAD.encode(mac);
    Ok(format!("{signing_input}.{sig}"))
}

/// Verifies a `asv1.` signed token against all configured keys in constant time.
pub fn verify_signed_token(
    token: &str,
    keys: &[Vec<u8>],
    now_secs: u64,
) -> Option<SignedTokenClaims> {
    if keys.is_empty() {
        return None;
    }
    let (payload_b64, sig_b64) = split_signed_token(token)?;
    let sig = URL_SAFE_NO_PAD.decode(sig_b64).ok()?;
    if !signature_matches(keys, payload_b64, &sig) {
        return None;
    }
    let raw_claims = URL_SAFE_NO_PAD.decode(payload_b64).ok()?;
    let claims: SignedTokenClaims = serde_json::from_slice(&raw_claims).ok()?;
    if claims.jti.trim().is_empty()
        || validate_namespace(&claims.ns).is_err()
        || parse_role(&claims.role).is_none()
        || claims.exp.is_some_and(|exp| now_secs >= exp)
    {
        return None;
    }
    Some(claims)
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InspectedToken {
    pub claims: SignedTokenClaims,
    pub signature_valid: Option<bool>,
}

/// Splits `asv1.<payload>.<signature>` into its two base64 segments.
fn split_signed_token(token: &str) -> Option<(&str, &str)> {
    let (payload_b64, sig_b64) = token.strip_prefix(SIGNED_TOKEN_PREFIX)?.split_once('.')?;
    (!sig_b64.contains('.')).then_some((payload_b64, sig_b64))
}

/// Checks `sig` against every non-empty key without short-circuiting.
fn signature_matches(keys: &[Vec<u8>], payload_b64: &str, sig: &[u8]) -> bool {
    if sig.len() != 32 {
        return false;
    }
    let signing_input = format!("{SIGNED_TOKEN_PREFIX}{payload_b64}");
    keys.iter()
        .filter(|key| !key.is_empty())
        .fold(false, |ok, key| {
            ok | constant_time_bytes_eq(&hmac_sha256(key, signing_input.as_bytes()), sig)
        })
}

/// Decodes an `asv1.` token payload and optionally verifies its signature if `keys` is non-empty.
pub fn inspect_signed_token(token: &str, keys: &[Vec<u8>]) -> anyhow::Result<InspectedToken> {
    let (payload_b64, sig_b64) = split_signed_token(token).with_context(|| {
        format!("token must have the form '{SIGNED_TOKEN_PREFIX}<payload>.<signature>'")
    })?;
    let raw_claims = URL_SAFE_NO_PAD
        .decode(payload_b64)
        .context("decode token payload")?;
    let claims: SignedTokenClaims =
        serde_json::from_slice(&raw_claims).context("parse token claims JSON")?;
    let signature_valid = if keys.is_empty() {
        None
    } else {
        let sig = URL_SAFE_NO_PAD
            .decode(sig_b64)
            .context("decode token signature")?;
        Some(signature_matches(keys, payload_b64, &sig))
    };
    Ok(InspectedToken {
        claims,
        signature_valid,
    })
}

/// Derives the shared bearer secret used by internal plugin endpoints (e.g. `/internal/revocations`)
/// without sending the raw signing key over the wire.
pub fn internal_revocation_bearer(signing_key: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"anyserve-internal-revocations:");
    hasher.update(signing_key);
    let digest: [u8; 32] = hasher.finalize().into();
    let mut out = String::with_capacity(64);
    for b in digest {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{b:02x}");
    }
    out
}

pub fn constant_time_eq(left: &str, right: &str) -> bool {
    constant_time_bytes_eq(left.as_bytes(), right.as_bytes())
}

fn constant_time_bytes_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0u8, |diff, (a, b)| diff | (a ^ b))
        == 0
}

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
    /// Optional signed-token JTI (for targeted revocation enforcement).
    pub jti: Option<String>,
    /// Optional Unix timestamp in seconds when the signed credential expires.
    pub exp: Option<u64>,
}

impl Default for Principal {
    fn default() -> Self {
        Self {
            namespace: DEFAULT_NAMESPACE.to_string(),
            role: Role::Client,
            subject: "anonymous".to_string(),
            forced_pool: None,
            is_operator: true,
            jti: None,
            exp: None,
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

/// Generates a random `<prefix><32 lowercase hex>` bearer token using OS CSPRNG.
pub fn generate_token(prefix: &str) -> String {
    format!("{prefix}{}", uuid::Uuid::new_v4().simple())
}

#[derive(Clone, PartialEq, Eq)]
pub struct ScopedToken {
    pub token: String,
    pub role: Role,
    pub namespace: String,
    pub subject: String,
    pub forced_pool: Option<String>,
}

impl std::fmt::Debug for ScopedToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScopedToken")
            .field("token", &"[redacted]")
            .field("role", &self.role)
            .field("namespace", &self.namespace)
            .field("subject", &self.subject)
            .field("forced_pool", &self.forced_pool)
            .finish()
    }
}

#[derive(Clone)]
pub struct AuthPolicy {
    client_tokens: BTreeSet<String>,
    worker_tokens: BTreeSet<String>,
    scoped_tokens: Vec<ScopedToken>,
    signing_keys: Vec<Vec<u8>>,
    revocations: Arc<RwLock<RevocationState>>,
    revocation_tx: Arc<tokio::sync::watch::Sender<u64>>,
    auth_rejections_total: Arc<AtomicU64>,
}

impl Default for AuthPolicy {
    fn default() -> Self {
        Self::new(std::iter::empty(), std::iter::empty())
    }
}

impl std::fmt::Debug for AuthPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthPolicy")
            .field("client_token_count", &self.client_tokens.len())
            .field("worker_token_count", &self.worker_tokens.len())
            .field("scoped_tokens", &self.scoped_tokens)
            .field("signing_key_count", &self.signing_keys.len())
            .field("revocation_version", &self.revocation_version())
            .finish()
    }
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
        let (revocation_tx, _) = tokio::sync::watch::channel(0u64);
        Self {
            client_tokens: clean(&mut client_tokens.into_iter()),
            worker_tokens: clean(&mut worker_tokens.into_iter()),
            scoped_tokens: Vec::new(),
            signing_keys: Vec::new(),
            revocations: Arc::new(RwLock::new(RevocationState::default())),
            revocation_tx: Arc::new(revocation_tx),
            auth_rejections_total: Arc::new(AtomicU64::new(0)),
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

    pub fn with_signing_keys(mut self, keys: impl IntoIterator<Item = Vec<u8>>) -> Self {
        self.signing_keys = keys.into_iter().filter(|k| !k.is_empty()).collect();
        self
    }

    pub fn revocation_version(&self) -> u64 {
        self.revocations
            .read()
            .expect("revocations lock poisoned")
            .version
    }

    pub fn revocation_last_sync_at_ms(&self) -> Option<u64> {
        self.revocations
            .read()
            .expect("revocations lock poisoned")
            .last_sync_at_ms
    }

    pub fn mark_revocation_synced(&self) {
        let mut guard = self.revocations.write().expect("revocations lock poisoned");
        guard.last_sync_at_ms = Some(crate::now_ms());
    }

    pub fn apply_revocations(&self, snapshot: RevocationSnapshot) {
        {
            let mut guard = self.revocations.write().expect("revocations lock poisoned");
            guard.version = snapshot.version;
            guard.revoked_jtis = snapshot.revoked_jtis.into_iter().collect();
            guard.last_sync_at_ms = Some(crate::now_ms());
        }
        self.revocation_tx
            .send_modify(|seq| *seq = seq.wrapping_add(1));
    }

    pub fn subscribe_revocations(&self) -> tokio::sync::watch::Receiver<u64> {
        self.revocation_tx.subscribe()
    }

    pub fn signing_key_count(&self) -> usize {
        self.signing_keys.len()
    }

    /// Returns the derived internal revocation bearer for each configured signing key
    /// (newest/primary first), enabling rotation fallback in revocation pollers.
    pub fn revocation_bearers(&self) -> Vec<String> {
        self.signing_keys
            .iter()
            .map(|key| internal_revocation_bearer(key))
            .collect()
    }

    pub fn is_jti_revoked(&self, jti: &str) -> bool {
        self.revocations
            .read()
            .expect("revocations lock poisoned")
            .revoked_jtis
            .contains(jti)
    }

    pub(crate) fn is_principal_invalidated(&self, principal: &Principal, now_secs: u64) -> bool {
        principal.exp.is_some_and(|exp| exp <= now_secs)
            || principal
                .jti
                .as_deref()
                .is_some_and(|jti| self.is_jti_revoked(jti))
    }

    pub fn auth_rejections_total(&self) -> u64 {
        self.auth_rejections_total.load(Ordering::Relaxed)
    }

    pub fn record_rejection(&self) {
        self.auth_rejections_total.fetch_add(1, Ordering::Relaxed);
    }

    fn reject<T>(&self, status: Status) -> Result<T, Status> {
        self.record_rejection();
        Err(status)
    }

    pub fn is_enabled(&self) -> bool {
        !self.client_tokens.is_empty()
            || !self.worker_tokens.is_empty()
            || !self.scoped_tokens.is_empty()
            || !self.signing_keys.is_empty()
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
                    jti: None,
                    exp: None,
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
                jti: None,
                exp: None,
            });
        }
        if contains_token(&self.client_tokens, candidate) && matched.is_none() {
            matched = Some(Principal {
                namespace: DEFAULT_NAMESPACE.to_string(),
                role: Role::Client,
                subject: "client-token".to_string(),
                forced_pool: None,
                is_operator: true,
                jti: None,
                exp: None,
            });
        }
        if matched.is_none()
            && !self.signing_keys.is_empty()
            && let Some(claims) = verify_signed_token(candidate, &self.signing_keys, now_secs())
            && !self.is_jti_revoked(&claims.jti)
            && let Some(role) = parse_role(&claims.role)
        {
            matched = Some(Principal {
                namespace: claims.ns,
                role,
                subject: claims.sub,
                forced_pool: claims.fp,
                is_operator: false,
                jti: Some(claims.jti),
                exp: claims.exp,
            });
        }
        matched
    }

    pub fn authorize_client_headers(&self, headers: &HeaderMap) -> Result<(), Status> {
        self.verify_client_headers(headers).map(|_| ())
    }

    /// Verifies management/operator HTTP access (`/metrics`, global queries): requires an
    /// operator-level client token when auth is enabled; tenant tokens and workers get `PermissionDenied`.
    pub fn authorize_operator_headers(&self, headers: &HeaderMap) -> Result<(), Status> {
        if !self.is_enabled() {
            return Ok(());
        }
        let Some(token) = bearer_token(headers) else {
            return self.reject(Status::unauthenticated("missing or invalid bearer token"));
        };
        match self.principal_for_token(token) {
            Some(principal) if principal.role == Role::Client && principal.is_operator => Ok(()),
            Some(_) => self.reject(Status::permission_denied(
                "management API requires an operator client token",
            )),
            None => self.reject(Status::unauthenticated("missing or invalid bearer token")),
        }
    }

    /// Verifies Console `/api/*` access: allows both operator client tokens and tenant-scoped
    /// client tokens, returning the caller's [`Principal`] for namespace enforcement.
    pub fn authorize_console_headers(&self, headers: &HeaderMap) -> Result<Principal, Status> {
        if !self.is_enabled() {
            return Self::unauthenticated_principal(Role::Client, headers);
        }
        let Some(token) = bearer_token(headers) else {
            return self.reject(Status::unauthenticated("missing or invalid bearer token"));
        };
        match self.principal_for_token(token) {
            Some(principal) if principal.role == Role::Client => Ok(principal),
            Some(_) => self.reject(Status::permission_denied(
                "worker token cannot access console API",
            )),
            None => self.reject(Status::unauthenticated("missing or invalid bearer token")),
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
            jti: None,
            exp: None,
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
        let Some(principal) =
            bearer_token(headers).and_then(|token| self.principal_for_token(token))
        else {
            return self.reject(Status::unauthenticated("missing or invalid bearer token"));
        };
        if (principal.role == Role::Client && path.starts_with(V2_WORKER_PREFIX))
            || (principal.role == Role::Worker && path.starts_with(V2_CLIENT_PREFIX))
        {
            return self.reject(Status::permission_denied(
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

    /// Authenticates a WebSocket upgrade for a client endpoint (`/ws/v2/client/*`),
    /// accepting either `Authorization: Bearer <token>` or browser subprotocols
    /// `Sec-WebSocket-Protocol: anyserve.v2, anyserve.bearer.<base64url_nopad(token)>`.
    pub fn verify_ws_client_headers(&self, headers: &HeaderMap) -> Result<WsAuthResult, Status> {
        self.verify_ws_role_headers(headers, Role::Client)
    }

    /// Authenticates a WebSocket upgrade for a worker endpoint (`/ws/v2/worker/*`),
    /// accepting either `Authorization: Bearer <token>` or browser subprotocols
    /// `Sec-WebSocket-Protocol: anyserve.v2, anyserve.bearer.<base64url_nopad(token)>`.
    pub fn verify_ws_worker_headers(&self, headers: &HeaderMap) -> Result<WsAuthResult, Status> {
        self.verify_ws_role_headers(headers, Role::Worker)
    }

    fn verify_ws_role_headers(
        &self,
        headers: &HeaderMap,
        expected: Role,
    ) -> Result<WsAuthResult, Status> {
        let (selected_protocol, subprotocol_token) = match parse_ws_subprotocols(headers) {
            Ok(parsed) => parsed,
            Err(status) => return self.reject(status),
        };
        if !self.is_enabled() {
            let principal = Self::unauthenticated_principal(expected, headers)?;
            return Ok(WsAuthResult {
                principal,
                selected_protocol,
            });
        }
        let token = subprotocol_token
            .as_deref()
            .or_else(|| bearer_token(headers));
        let principal = self.verify_role_token(token, expected)?;
        Ok(WsAuthResult {
            principal,
            selected_protocol,
        })
    }

    fn verify_role_headers(
        &self,
        headers: &HeaderMap,
        expected: Role,
    ) -> Result<Principal, Status> {
        if !self.is_enabled() {
            return Self::unauthenticated_principal(expected, headers);
        }
        self.verify_role_token(bearer_token(headers), expected)
    }

    fn verify_role_token(&self, token: Option<&str>, expected: Role) -> Result<Principal, Status> {
        match token.and_then(|token| self.principal_for_token(token)) {
            Some(principal) if principal.role == expected => Ok(principal),
            Some(_) => self.reject(Status::permission_denied(match expected {
                Role::Client => "worker token cannot be used for client requests",
                Role::Worker => "client token cannot be used for worker requests",
            })),
            None => self.reject(Status::unauthenticated("missing or invalid bearer token")),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WsAuthResult {
    pub principal: Principal,
    pub selected_protocol: Option<&'static str>,
}

#[allow(clippy::result_large_err)]
fn parse_ws_subprotocols(
    headers: &HeaderMap,
) -> Result<(Option<&'static str>, Option<String>), Status> {
    let mut has_v2 = false;
    let mut bearer_encoded: Option<&str> = None;

    for raw in headers.get_all(http::header::SEC_WEBSOCKET_PROTOCOL) {
        let value = raw
            .to_str()
            .map_err(|_| Status::invalid_argument("invalid Sec-WebSocket-Protocol header"))?;
        for part in value.split(',') {
            let proto = part.trim();
            if proto.is_empty() {
                continue;
            }
            if proto == WS_SUBPROTOCOL_V2 {
                has_v2 = true;
            } else if let Some(encoded) = proto.strip_prefix(WS_BEARER_SUBPROTOCOL_PREFIX) {
                if bearer_encoded.is_some() {
                    return Err(Status::invalid_argument(
                        "duplicate WebSocket bearer subprotocol",
                    ));
                }
                bearer_encoded = Some(encoded);
            }
        }
    }

    let decoded_token = match bearer_encoded {
        Some(encoded) => {
            if headers.contains_key(http::header::AUTHORIZATION) {
                return Err(Status::invalid_argument(
                    "cannot supply both Authorization header and WebSocket bearer subprotocol",
                ));
            }
            if !has_v2 {
                return Err(Status::invalid_argument(
                    "WebSocket bearer subprotocol requires 'anyserve.v2' subprotocol",
                ));
            }
            if encoded.is_empty() {
                return Err(Status::invalid_argument(
                    "invalid WebSocket bearer subprotocol",
                ));
            }
            let bytes = URL_SAFE_NO_PAD
                .decode(encoded)
                .map_err(|_| Status::invalid_argument("invalid WebSocket bearer subprotocol"))?;
            let decoded = String::from_utf8(bytes)
                .map_err(|_| Status::invalid_argument("invalid WebSocket bearer subprotocol"))?;
            let trimmed = decoded.trim();
            if trimmed.is_empty() {
                return Err(Status::invalid_argument(
                    "invalid WebSocket bearer subprotocol",
                ));
            }
            Some(trimmed.to_owned())
        }
        None => None,
    };

    Ok((has_v2.then_some(WS_SUBPROTOCOL_V2), decoded_token))
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

const MAX_EXPIRY_SLEEP: Duration = Duration::from_secs(86_400);

async fn wait_for_expiry(exp: Option<u64>) {
    let Some(exp) = exp else {
        return std::future::pending().await;
    };
    let exp_ms = exp.saturating_mul(1000);
    loop {
        let now_ms = crate::now_ms();
        if now_ms >= exp_ms {
            return;
        }
        let wait = Duration::from_millis(exp_ms - now_ms).min(MAX_EXPIRY_SLEEP);
        tokio::time::sleep(wait).await;
    }
}

/// Resolves when `principal`'s signed credential is revoked or expires.
/// Stays pending forever when `principal` has neither `jti` nor `exp`.
pub(crate) async fn wait_credential_invalidated(auth: &AuthPolicy, principal: &Principal) {
    if principal.jti.is_none() && principal.exp.is_none() {
        return std::future::pending().await;
    }
    let mut rev_rx = auth.subscribe_revocations();
    let _ = rev_rx.borrow_and_update();
    if auth.is_principal_invalidated(principal, now_secs()) {
        return;
    }
    let wait_revocation = async {
        let Some(jti) = principal.jti.as_deref() else {
            return std::future::pending().await;
        };
        while rev_rx.changed().await.is_ok() {
            if auth.is_jti_revoked(jti) {
                return;
            }
        }
        std::future::pending().await
    };
    tokio::select! {
        _ = wait_for_expiry(principal.exp) => {}
        _ = wait_revocation => {}
    }
}

pub(crate) async fn wait_optional_credential_invalidated(
    auth: Option<&AuthPolicy>,
    principal: Option<&Principal>,
) {
    match (auth, principal) {
        (Some(auth), Some(principal)) => wait_credential_invalidated(auth, principal).await,
        _ => std::future::pending().await,
    }
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

pin_project_lite::pin_project! {
    #[project = AuthFutureProj]
    pub enum AuthFuture<F> {
        Inner { #[pin] fut: F },
        Reject { response: Option<http::Response<BoxBody>> },
    }
}

impl<F, E> Future for AuthFuture<F>
where
    F: Future<Output = Result<http::Response<BoxBody>, E>>,
{
    type Output = Result<http::Response<BoxBody>, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.project() {
            AuthFutureProj::Inner { fut } => fut.poll(cx),
            AuthFutureProj::Reject { response } => {
                Poll::Ready(Ok(response.take().expect("polled after completion")))
            }
        }
    }
}

impl<S, B> Service<http::Request<B>> for AuthService<S>
where
    S: Service<http::Request<B>, Response = http::Response<BoxBody>>,
    S::Future: Send + 'static,
{
    type Response = http::Response<BoxBody>;
    type Error = S::Error;
    type Future = AuthFuture<S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut request: http::Request<B>) -> Self::Future {
        // Fast path when auth is disabled and no explicit namespace header needs validation:
        // skip Principal/Extensions allocation and shared Arc<AuthPolicy> refcount contention.
        if !self.policy.is_enabled() && !request.headers().contains_key(NAMESPACE_HEADER) {
            return AuthFuture::Inner {
                fut: self.inner.call(request),
            };
        }
        match self.policy.verify(request.uri().path(), request.headers()) {
            Ok(principal) => {
                if let Some(principal) = principal {
                    request.extensions_mut().insert(principal.role);
                    request.extensions_mut().insert(principal);
                    if self.policy.is_enabled() {
                        request.extensions_mut().insert(Arc::clone(&self.policy));
                    }
                }
                AuthFuture::Inner {
                    fut: self.inner.call(request),
                }
            }
            Err(status) => AuthFuture::Reject {
                response: Some(status.into_http()),
            },
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

    #[test]
    fn debug_redacts_all_secret_tokens_in_scoped_token_and_auth_policy() {
        let scoped = ScopedToken {
            token: "super-secret-scoped-tok".into(),
            role: Role::Worker,
            namespace: "factory-a".into(),
            subject: "jetson-1".into(),
            forced_pool: Some("jetson-local".into()),
        };
        let scoped_dbg = format!("{scoped:?}");
        assert!(!scoped_dbg.contains("super-secret-scoped-tok"));
        assert!(scoped_dbg.contains("[redacted]"));
        assert!(scoped_dbg.contains("factory-a"));
        assert!(scoped_dbg.contains("jetson-1"));
        assert!(scoped_dbg.contains("jetson-local"));

        let policy = AuthPolicy::new(
            ["super-secret-client-tok".into()],
            ["super-secret-worker-tok".into()],
        )
        .with_scoped_tokens([scoped]);
        let policy_dbg = format!("{policy:?}");
        assert!(!policy_dbg.contains("super-secret-client-tok"));
        assert!(!policy_dbg.contains("super-secret-worker-tok"));
        assert!(!policy_dbg.contains("super-secret-scoped-tok"));
        assert!(policy_dbg.contains("[redacted]"));
        assert!(policy_dbg.contains("client_token_count: 1"));
        assert!(policy_dbg.contains("worker_token_count: 1"));
        assert!(policy_dbg.contains("factory-a"));
    }

    #[test]
    fn hmac_sha256_matches_rfc4231_vector() {
        let mac = hmac_sha256(b"Jefe", b"what do ya want for nothing?");
        let hex: String = mac.iter().map(|b| format!("{b:02x}")).collect();
        assert_eq!(
            hex,
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
    }

    #[test]
    fn signed_tokens_verify_rotate_expire_revoke_and_never_grant_operator() {
        let key1 = b"primary-signing-secret-32-bytes!!".to_vec();
        let key2 = b"rotated-signing-secret-32-bytes!!".to_vec();
        let claims = SignedTokenClaims {
            jti: "jti-1".into(),
            ns: "gh-42".into(),
            role: "client".into(),
            sub: "gh:octocat".into(),
            iat: 1_700_000_000,
            exp: Some(now_secs() + 3600),
            fp: Some("gpu-a".into()),
        };
        let token = sign_token(&key1, &claims).unwrap();
        assert!(token.starts_with(SIGNED_TOKEN_PREFIX));

        let policy = AuthPolicy::default().with_signing_keys([key2.clone(), key1.clone()]);
        assert!(policy.is_enabled());
        let p = policy.principal_for_token(&token).unwrap();
        assert_eq!(p.namespace, "gh-42");
        assert_eq!(p.role, Role::Client);
        assert_eq!(p.subject, "gh:octocat");
        assert_eq!(p.forced_pool.as_deref(), Some("gpu-a"));
        assert!(!p.is_operator);

        // Console API allows tenant client token; operator API (/metrics) rejects it.
        let ch = headers(&token);
        let cp = policy.authorize_console_headers(&ch).unwrap();
        assert_eq!(cp.namespace, "gh-42");
        assert!(!cp.is_operator);
        assert_eq!(
            policy.authorize_operator_headers(&ch).unwrap_err().code(),
            tonic::Code::PermissionDenied
        );

        // Tamper & wrong key reject cleanly.
        let mut tampered = token.clone();
        let last = tampered.pop().unwrap();
        tampered.push(if last == 'A' { 'B' } else { 'A' });
        assert!(policy.principal_for_token(&tampered).is_none());
        let wrong_policy = AuthPolicy::default().with_signing_keys([key2]);
        assert!(wrong_policy.principal_for_token(&token).is_none());

        // Expired token rejected.
        let expired = sign_token(
            &key1,
            &SignedTokenClaims {
                exp: Some(now_secs().saturating_sub(1)),
                ..claims.clone()
            },
        )
        .unwrap();
        assert!(policy.principal_for_token(&expired).is_none());

        // Worker signed token rejects Console API with PermissionDenied.
        let worker_tok = sign_token(
            &key1,
            &SignedTokenClaims {
                jti: "jti-w".into(),
                role: "worker".into(),
                ..claims
            },
        )
        .unwrap();
        assert_eq!(
            policy
                .authorize_console_headers(&headers(&worker_tok))
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );

        // Revocation snapshot blocks revoked JTI immediately and updates version.
        policy.apply_revocations(RevocationSnapshot {
            version: 7,
            revoked_jtis: vec!["jti-1".into()],
        });
        assert_eq!(policy.revocation_version(), 7);
        assert!(policy.principal_for_token(&token).is_none());
        assert!(policy.principal_for_token(&worker_tok).is_some());

        // Debug never exposes signing key bytes.
        let dbg = format!("{policy:?}");
        assert!(!dbg.contains("primary-signing-secret"));
        assert!(dbg.contains("signing_key_count: 2"));
        assert!(dbg.contains("revocation_version: 7"));
    }
}
