//! Optional `iroh`-backed direct data plane for static large objects (`anyserve+iroh://`).
//!
//! Enabled via the off-by-default `direct` Cargo feature on `anyserve-client`.
//! Zero changes are required on the Rust control plane, protobuf schema, or
//! Cloudflare Durable Object engine: large objects are passed opaquely through
//! existing `Object::Uri` and `ResultChunk::uri` fields using `anyserve.sha256`
//! and `anyserve.size_bytes` metadata.
//!
//! ## Trust boundary
//! - Without [`DirectConfig::allowed_peers`], payload bytes bypass control-plane
//!   forwarding/egress, but anyone possessing the capability URI during its TTL
//!   (including the control-plane operator) can fetch the object.
//! - With [`DirectConfig::allowed_peers`] configured (and persistent identities
//!   via [`DirectConfig::secret_key_path`]), `iroh`'s Ed25519 TLS handshake
//!   cryptographically rejects any peer not in the allowlist — including the
//!   control-plane operator.
//!
//! ## Scope (v1)
//! - Static large objects only (`Object::Uri` inputs and terminal `ResultChunk::uri`
//!   outputs). Live streaming URIs are intentionally out of scope.
//! - Auto-enabled only for live ephemeral `Invoke` (`ExecutionClient::invoke_direct`).
//!   `Submit` / durable jobs stay inline unless explicitly opted in, because direct
//!   object availability is tied to the holding process lifetime.

mod endpoint;
mod store;
mod uri;

#[cfg(test)]
mod tests;

use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;

pub use iroh::EndpointId;

pub use endpoint::{
    DIRECT_ALPN, DefaultPathClassifier, DirectEndpoint, LazyDirectEndpoint, ObservedPath,
    PathClassifier,
};
pub use store::{DirectObjectStore, InputLeaseGuard};
pub use uri::{ParsedDirectUri, decode_direct_uri, encode_direct_uri};

/// Optional environment variable pointing to a self-hosted `iroh-relay` URL.
pub const DIRECT_RELAY_URL_ENV: &str = "ANYSERVE_DIRECT_RELAY_URL";
/// Optional environment variable pointing to a `0600` Ed25519 key file for a stable `EndpointId`.
pub const DIRECT_SECRET_KEY_PATH_ENV: &str = "ANYSERVE_DIRECT_SECRET_KEY_PATH";

/// Default minimum payload size (1 MiB) staged via direct transfer.
pub const DEFAULT_DIRECT_THRESHOLD_BYTES: usize = 1024 * 1024;
/// Default maximum total input bytes eligible for automatic inline fallback on
/// public-relay-only networks (matches VPS `max_inline_object_bytes` = 8 MiB).
pub const DEFAULT_INLINE_FALLBACK_LIMIT_BYTES: usize = 8 * 1024 * 1024;
/// Default upgrade window to wait for direct UDP hole-punching when connected
/// via n0's rate-limited public relay (`*.relay.n0.iroh.link`).
pub const DEFAULT_UPGRADE_WINDOW: Duration = Duration::from_millis(1_500);
/// Default per-read/write idle timeout on direct QUIC streams.
pub const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
/// Default TTL for staged input tokens (also revoked immediately on terminal/drop).
pub const DEFAULT_INPUT_TOKEN_TTL: Duration = Duration::from_secs(300);
/// Default post-finish worker output LRU cache byte ceiling (512 MiB).
pub const DEFAULT_OUTPUT_CACHE_MAX_BYTES: usize = 512 * 1024 * 1024;
/// Default post-finish worker output LRU cache TTL (60 s; evicted early on client ACK).
pub const DEFAULT_OUTPUT_CACHE_TTL: Duration = Duration::from_secs(60);
/// Default hard cap on concurrent inbound iroh connections per endpoint.
pub const DEFAULT_MAX_CONCURRENT_CONNECTIONS: usize = 64;
/// Default hard cap on concurrent streams per inbound iroh connection.
pub const DEFAULT_MAX_STREAMS_PER_CONNECTION: usize = 8;
/// Default hard cap on concurrent fetches for a single object token.
pub const DEFAULT_MAX_CONCURRENT_FETCHES_PER_TOKEN: u32 = 2;
/// Default receiver safety cap on single direct object allocation (1 GiB).
pub const DEFAULT_MAX_OBJECT_BYTES: usize = 1024 * 1024 * 1024;
/// Upper bound on per-job input token fetch allowance (matches `RuntimeConfig::max_attempts_limit`).
pub const MAX_INPUT_FETCHES_LIMIT: u32 = 20;

/// Selected transport path observed for a direct transfer (SDK-reported).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DirectPath {
    Direct,
    Relay,
}

impl DirectPath {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Direct => "direct",
            Self::Relay => "relay",
        }
    }
}

/// Telemetry summary of one direct object fetch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DirectTransferReport {
    pub path: DirectPath,
    pub bytes: u64,
    pub sha256: String,
}

/// Cooperative cancellation signal bridged into active QUIC streams (`send.reset` / `recv.stop`).
#[derive(Clone, Debug)]
pub struct DirectCancel {
    tx: Arc<watch::Sender<bool>>,
    rx: watch::Receiver<bool>,
}

impl Default for DirectCancel {
    fn default() -> Self {
        Self::new()
    }
}

impl DirectCancel {
    pub fn new() -> Self {
        let (tx, rx) = watch::channel(false);
        Self {
            tx: Arc::new(tx),
            rx,
        }
    }

    pub fn cancel(&self) {
        let _ = self.tx.send(true);
    }

    pub fn is_cancelled(&self) -> bool {
        *self.rx.borrow()
    }

    pub async fn cancelled(&self) {
        let mut rx = self.rx.clone();
        if *rx.borrow_and_update() {
            return;
        }
        while rx.changed().await.is_ok() {
            if *rx.borrow() {
                return;
            }
        }
    }
}

/// Configuration for an optional `iroh` direct data-plane endpoint.
#[derive(Clone)]
pub struct DirectConfig {
    /// Custom self-hosted `iroh-relay` URL (`ANYSERVE_DIRECT_RELAY_URL` fallback).
    /// When `None` and `disable_relays == false`, uses `iroh`'s default n0 public
    /// relay strictly for hole-punch signaling (guarded by `upgrade_window`).
    pub relay_url: Option<String>,
    /// When `true`, disables all relays (`RelayMode::Disabled`, local/LAN direct IP only).
    pub disable_relays: bool,
    /// Optional path to a `0600` Ed25519 secret key file (`ANYSERVE_DIRECT_SECRET_KEY_PATH`
    /// fallback) so this endpoint keeps a stable `EndpointId` across restarts for `allowed_peers`.
    /// When unset, an ephemeral per-process identity is generated.
    pub secret_key_path: Option<PathBuf>,
    /// Optional allowlist of remote `EndpointId`s permitted to fetch objects from this endpoint.
    /// Enforced via Ed25519 TLS peer identity immediately on `accept()`.
    pub allowed_peers: Option<Vec<EndpointId>>,
    /// Include local/public `addr=<ip:port>` candidates in generated `anyserve+iroh://` URIs.
    /// Set to `false` to avoid exposing IP addresses to the control plane (hole-punching still
    /// works via `relay=`).
    pub advertise_direct_addrs: bool,
    /// Minimum inline payload bytes to stage over direct transfer.
    pub threshold_bytes: usize,
    /// Maximum total input bytes eligible for automatic inline fallback when the
    /// worker hits `direct_public_relay_throttled` (set to `0` to always fail fast).
    pub inline_fallback_limit_bytes: usize,
    /// How long to wait for direct IP upgrade when initially connected only via
    /// n0's rate-limited public relay before failing fast / falling back.
    pub upgrade_window: Duration,
    /// Per-read/write idle timeout on direct QUIC streams.
    pub idle_timeout: Duration,
    /// TTL ceiling for staged input object tokens (also revoked immediately on terminal/drop).
    pub input_token_ttl: Duration,
    /// Hard memory ceiling for the worker's post-finish output LRU cache.
    pub output_cache_max_bytes: usize,
    /// TTL for worker post-finish output entries (evicted early on client ACK).
    pub output_cache_ttl: Duration,
    /// Maximum concurrent inbound connections accepted by this endpoint.
    pub max_concurrent_connections: usize,
    /// Maximum concurrent bidirectional streams served per inbound connection.
    pub max_streams_per_connection: usize,
    /// Maximum concurrent fetches allowed per object token.
    pub max_concurrent_fetches_per_token: u32,
    /// Safety cap on single received object allocation.
    pub max_object_bytes: usize,
    /// Path classifier used to distinguish direct IP, custom relay, and throttled public relay.
    pub path_classifier: Arc<dyn PathClassifier>,
}

impl Default for DirectConfig {
    fn default() -> Self {
        let relay_url = std::env::var(DIRECT_RELAY_URL_ENV)
            .ok()
            .map(|v| v.trim().to_owned())
            .filter(|v| !v.is_empty());
        let secret_key_path = std::env::var(DIRECT_SECRET_KEY_PATH_ENV)
            .ok()
            .map(|v| v.trim().to_owned())
            .filter(|v| !v.is_empty())
            .map(PathBuf::from);
        Self {
            relay_url,
            disable_relays: false,
            secret_key_path,
            allowed_peers: None,
            advertise_direct_addrs: true,
            threshold_bytes: DEFAULT_DIRECT_THRESHOLD_BYTES,
            inline_fallback_limit_bytes: DEFAULT_INLINE_FALLBACK_LIMIT_BYTES,
            upgrade_window: DEFAULT_UPGRADE_WINDOW,
            idle_timeout: DEFAULT_IDLE_TIMEOUT,
            input_token_ttl: DEFAULT_INPUT_TOKEN_TTL,
            output_cache_max_bytes: DEFAULT_OUTPUT_CACHE_MAX_BYTES,
            output_cache_ttl: DEFAULT_OUTPUT_CACHE_TTL,
            max_concurrent_connections: DEFAULT_MAX_CONCURRENT_CONNECTIONS,
            max_streams_per_connection: DEFAULT_MAX_STREAMS_PER_CONNECTION,
            max_concurrent_fetches_per_token: DEFAULT_MAX_CONCURRENT_FETCHES_PER_TOKEN,
            max_object_bytes: DEFAULT_MAX_OBJECT_BYTES,
            path_classifier: Arc::new(DefaultPathClassifier),
        }
    }
}

impl fmt::Debug for DirectConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DirectConfig")
            .field("relay_url", &self.relay_url)
            .field("disable_relays", &self.disable_relays)
            .field(
                "secret_key_path",
                &self.secret_key_path.as_ref().map(|_| "[configured]"),
            )
            .field(
                "allowed_peers_count",
                &self.allowed_peers.as_ref().map(Vec::len),
            )
            .field("advertise_direct_addrs", &self.advertise_direct_addrs)
            .field("threshold_bytes", &self.threshold_bytes)
            .field(
                "inline_fallback_limit_bytes",
                &self.inline_fallback_limit_bytes,
            )
            .field("upgrade_window", &self.upgrade_window)
            .field("idle_timeout", &self.idle_timeout)
            .field("input_token_ttl", &self.input_token_ttl)
            .field("output_cache_max_bytes", &self.output_cache_max_bytes)
            .field("output_cache_ttl", &self.output_cache_ttl)
            .field(
                "max_concurrent_connections",
                &self.max_concurrent_connections,
            )
            .field(
                "max_streams_per_connection",
                &self.max_streams_per_connection,
            )
            .field(
                "max_concurrent_fetches_per_token",
                &self.max_concurrent_fetches_per_token,
            )
            .field("max_object_bytes", &self.max_object_bytes)
            .finish()
    }
}

/// Structured errors returned by the direct data plane.
#[derive(Debug, Clone)]
pub enum DirectError {
    InvalidUri(String),
    InvalidConfig(String),
    Bind(String),
    Connect(String),
    PeerRejected(String),
    NotFoundOrExpired,
    FetchLimitExceeded,
    ConcurrencyLimitExceeded,
    PublicRelayThrottled { detail: String },
    Sha256Mismatch { expected: String, actual: String },
    SizeMismatch { expected: u64, actual: u64 },
    ObjectTooLarge { size: u64, limit: usize },
    OutputCacheExceeded { size: usize, limit: usize },
    Cancelled,
    IdleTimeout,
    Protocol(String),
}

impl DirectError {
    /// Converts a pre-handler direct input error into a non-retryable attempt
    /// failure outcome so callers can inspect `ErrorInfo::code` cleanly.
    pub fn to_attempt_failure_outcome(&self) -> crate::v2::finish::Outcome {
        let code = match self {
            Self::PublicRelayThrottled { .. } => crate::ERROR_CODE_DIRECT_PUBLIC_RELAY_THROTTLED,
            Self::NotFoundOrExpired => "direct_token_expired",
            Self::FetchLimitExceeded => "direct_fetch_limit_exceeded",
            Self::Sha256Mismatch { .. } => "direct_sha256_mismatch",
            Self::SizeMismatch { .. } => "direct_size_mismatch",
            Self::Cancelled => "cancelled",
            Self::IdleTimeout => "direct_idle_timeout",
            _ => "direct_transfer_failed",
        };
        crate::v2::finish::Outcome::Failed(crate::v2::ErrorInfo {
            code: code.to_owned(),
            message: self.to_string(),
            retryable: false,
        })
    }

    pub fn into_status(self) -> tonic::Status {
        match &self {
            Self::InvalidUri(_) | Self::InvalidConfig(_) => {
                tonic::Status::invalid_argument(self.to_string())
            }
            Self::Cancelled => tonic::Status::cancelled(self.to_string()),
            Self::IdleTimeout => tonic::Status::deadline_exceeded(self.to_string()),
            Self::PublicRelayThrottled { .. }
            | Self::NotFoundOrExpired
            | Self::FetchLimitExceeded
            | Self::PeerRejected(_) => tonic::Status::failed_precondition(self.to_string()),
            Self::ConcurrencyLimitExceeded
            | Self::ObjectTooLarge { .. }
            | Self::OutputCacheExceeded { .. } => {
                tonic::Status::resource_exhausted(self.to_string())
            }
            Self::Sha256Mismatch { .. } | Self::SizeMismatch { .. } => {
                tonic::Status::data_loss(self.to_string())
            }
            Self::Bind(_) | Self::Connect(_) | Self::Protocol(_) => {
                tonic::Status::unavailable(self.to_string())
            }
        }
    }
}

impl fmt::Display for DirectError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidUri(msg) => write!(f, "invalid direct URI: {msg}"),
            Self::InvalidConfig(msg) => write!(f, "invalid direct config: {msg}"),
            Self::Bind(msg) => write!(f, "failed to bind direct iroh endpoint: {msg}"),
            Self::Connect(msg) => write!(f, "failed to connect to direct peer: {msg}"),
            Self::PeerRejected(msg) => write!(f, "direct peer rejected connection: {msg}"),
            Self::NotFoundOrExpired => write!(f, "direct object token not found or expired"),
            Self::FetchLimitExceeded => write!(f, "direct object token fetch limit exceeded"),
            Self::ConcurrencyLimitExceeded => {
                write!(f, "direct object token concurrent fetch limit exceeded")
            }
            Self::PublicRelayThrottled { detail } => write!(
                f,
                "direct transfer blocked on rate-limited public n0 relay ({detail}); configure {DIRECT_RELAY_URL_ENV} or unblock UDP"
            ),
            Self::Sha256Mismatch { expected, actual } => {
                write!(
                    f,
                    "direct object sha256 mismatch: expected {expected}, got {actual}"
                )
            }
            Self::SizeMismatch { expected, actual } => {
                write!(
                    f,
                    "direct object size mismatch: expected {expected} bytes, got {actual} bytes"
                )
            }
            Self::ObjectTooLarge { size, limit } => {
                write!(
                    f,
                    "direct object size {size} bytes exceeds configured max_object_bytes ({limit})"
                )
            }
            Self::OutputCacheExceeded { size, limit } => {
                write!(
                    f,
                    "direct output size {size} bytes exceeds output_cache_max_bytes ({limit})"
                )
            }
            Self::Cancelled => write!(f, "direct transfer cancelled"),
            Self::IdleTimeout => write!(f, "direct transfer idle timeout"),
            Self::Protocol(msg) => write!(f, "direct protocol error: {msg}"),
        }
    }
}

impl std::error::Error for DirectError {}

/// Derives per-job input token fetch allowance from `ExecutionPolicy::max_attempts`,
/// bounded to `3..=MAX_INPUT_FETCHES_LIMIT` (20) so attempt retries can re-read safely.
pub fn derive_input_max_fetches(policy_max_attempts: Option<u32>) -> u32 {
    policy_max_attempts
        .filter(|&n| n > 0)
        .unwrap_or(3)
        .clamp(3, MAX_INPUT_FETCHES_LIMIT)
}

/// Computes lowercase hex SHA-256 of `data`.
pub(crate) fn sha256_hex(data: &[u8]) -> String {
    use sha2::{Digest as _, Sha256};
    let digest = Sha256::digest(data);
    hex_lower(&digest)
}

pub(crate) fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}
