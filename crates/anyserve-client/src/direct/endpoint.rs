use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use iroh::endpoint::{Connection, RecvStream, SendStream, presets};
use iroh::{Endpoint, EndpointAddr, EndpointId, RelayMode, RelayUrl, SecretKey, TransportAddr};
use sha2::{Digest as _, Sha256};
use tokio::sync::{OnceCell, Semaphore};
use tokio::task::JoinHandle;

use super::store::{DirectObjectStore, EntryKind, InputLeaseGuard};
use super::uri::{decode_direct_uri, encode_direct_uri};
use super::{
    DirectCancel, DirectConfig, DirectError, DirectPath, DirectTransferReport, hex_lower,
    sha256_hex,
};
use crate::{
    DIRECT_CAPABILITY_KEY, DIRECT_CAPABILITY_VALUE, OBJECT_SHA256_METADATA_KEY,
    OBJECT_SIZE_METADATA_KEY, redact_direct_uri,
};

/// ALPN identifier for AnyServe's direct object transfer protocol over `iroh`.
pub const DIRECT_ALPN: &[u8] = b"anyserve/direct/1";

const PROTO_VERSION: u8 = 1;
const STATUS_OK: u8 = 0;
const STATUS_NOT_FOUND_OR_EXPIRED: u8 = 1;
const STATUS_FETCH_LIMIT_EXCEEDED: u8 = 2;
const STATUS_CONCURRENCY_LIMIT_EXCEEDED: u8 = 3;
const STATUS_INVALID_REQUEST: u8 = 4;
const ACK_VERIFIED: u8 = 1;

const CHUNK_BYTES: usize = 256 * 1024;
const MAX_TOKEN_WIRE_LEN: usize = 128;
const CANCEL_VARINT_CODE: u32 = 1;
const REJECTED_PEER_CODE: u32 = 2;
const ACK_WAIT_TIMEOUT: Duration = Duration::from_secs(2);
const PATH_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Path classification returned after inspecting a live `iroh` connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ObservedPath {
    DirectIp,
    CustomRelay,
    PublicRelayOnly,
}

/// Classifies an established `iroh` connection's active path. Injectable so
/// tests can exercise public-relay-only protection hermetically without WAN calls.
pub trait PathClassifier: Send + Sync + std::fmt::Debug {
    fn classify(&self, conn: &Connection, has_custom_relay: bool) -> ObservedPath;
}

/// Default path classifier inspecting `Connection::paths()`.
#[derive(Clone, Copy, Debug, Default)]
pub struct DefaultPathClassifier;

impl PathClassifier for DefaultPathClassifier {
    fn classify(&self, conn: &Connection, has_custom_relay: bool) -> ObservedPath {
        let mut saw_public_relay = false;
        let mut saw_custom_relay = false;
        for path in conn.paths().iter() {
            if !path.is_selected() {
                continue;
            }
            match path.remote_addr() {
                TransportAddr::Ip(_) => return ObservedPath::DirectIp,
                TransportAddr::Relay(url) => {
                    if has_custom_relay || !is_n0_public_relay(url) {
                        saw_custom_relay = true;
                    } else {
                        saw_public_relay = true;
                    }
                }
                _ => {}
            }
        }
        if saw_custom_relay {
            ObservedPath::CustomRelay
        } else if saw_public_relay {
            ObservedPath::PublicRelayOnly
        } else {
            ObservedPath::DirectIp
        }
    }
}

fn is_n0_public_relay(url: &RelayUrl) -> bool {
    let host = url.to_string();
    host.contains(".relay.n0.iroh.link") || host.contains(".iroh-relay.iroh.link")
}

/// Long-lived, reusable `iroh` endpoint serving and fetching `anyserve+iroh://`
/// static objects. Share a single instance per client or worker process so cold
/// `online()` startup is paid at most once.
pub struct DirectEndpoint {
    endpoint: Endpoint,
    config: DirectConfig,
    store: DirectObjectStore,
    has_custom_relay: bool,
    accept_task: JoinHandle<()>,
}

impl DirectEndpoint {
    /// Binds a new `iroh` endpoint using `presets::Minimal` (no n0 DNS/Pkarr
    /// discovery service), spawns its bounded accept loop, and logs the stable
    /// or ephemeral `EndpointId`.
    pub async fn bind(config: DirectConfig) -> Result<Arc<Self>, DirectError> {
        let (relay_mode, has_custom_relay) = resolve_relay_mode(&config)?;
        let secret_key = match config.secret_key_path.as_deref() {
            Some(path) => Some(load_or_create_secret_key(path)?),
            None => None,
        };

        let mut builder = Endpoint::builder(presets::Minimal)
            .relay_mode(relay_mode)
            .alpns(vec![DIRECT_ALPN.to_vec()]);
        if let Some(addr) = config.bind_addr {
            builder = builder.clear_ip_transports().bind_addr(addr).map_err(|e| {
                DirectError::InvalidConfig(format!("invalid bind_addr {addr}: {e}"))
            })?;
        }
        if let Some(sk) = secret_key {
            builder = builder.secret_key(sk);
        }
        let endpoint = builder
            .bind()
            .await
            .map_err(|e| DirectError::Bind(e.to_string()))?;

        if !config.disable_relays {
            let _ = tokio::time::timeout(Duration::from_secs(5), endpoint.online()).await;
        }

        let endpoint_id = endpoint.id();
        tracing::info!(
            endpoint_id = %endpoint_id,
            persistent_identity = config.secret_key_path.is_some(),
            allowed_peers_enabled = config.allowed_peers.is_some(),
            advertise_direct_addrs = config.advertise_direct_addrs,
            "anyserve direct iroh endpoint ready"
        );

        let store = DirectObjectStore::new(
            config.output_cache_max_bytes,
            config.max_concurrent_fetches_per_token,
        );
        let allowed_peers: Option<Arc<HashSet<EndpointId>>> = config
            .allowed_peers
            .as_ref()
            .map(|list| Arc::new(list.iter().copied().collect()));

        let conn_sem = Arc::new(Semaphore::new(config.max_concurrent_connections.max(1)));
        let max_streams = config.max_streams_per_connection.max(1);
        let idle_timeout = config.idle_timeout;
        let accept_ep = endpoint.clone();
        let accept_store = store.clone();

        let accept_task = tokio::spawn(async move {
            while let Some(incoming) = accept_ep.accept().await {
                let Ok(conn_permit) = conn_sem.clone().try_acquire_owned() else {
                    tracing::warn!("direct endpoint concurrent connection limit reached; dropping");
                    continue;
                };
                let store = accept_store.clone();
                let allowed = allowed_peers.clone();
                tokio::spawn(async move {
                    let _conn_permit = conn_permit;
                    let Ok(conn) = incoming.await else {
                        return;
                    };
                    let peer = conn.remote_id();
                    if let Some(allowlist) = &allowed
                        && !allowlist.contains(&peer)
                    {
                        tracing::warn!(remote_peer = %peer, "rejected unauthorized direct peer");
                        conn.close(REJECTED_PEER_CODE.into(), b"peer_not_allowed");
                        return;
                    }
                    serve_connection(conn, store, max_streams, idle_timeout).await;
                });
            }
        });

        Ok(Arc::new(Self {
            endpoint,
            config,
            store,
            has_custom_relay,
            accept_task,
        }))
    }

    pub fn id(&self) -> EndpointId {
        self.endpoint.id()
    }

    pub fn addr(&self) -> EndpointAddr {
        self.endpoint.addr()
    }

    pub fn config(&self) -> &DirectConfig {
        &self.config
    }

    pub fn store(&self) -> &DirectObjectStore {
        &self.store
    }

    /// Advertises `anyserve.direct = "1"` on a worker's `Hello.attributes` so
    /// the scheduler routes direct-input jobs only to direct-capable workers.
    ///
    /// Prefer [`crate::execution::DirectWorkerSession::connect`] +
    /// [`crate::execution::DirectWorkerSession::open_attempt`] (or
    /// [`crate::execution::Attempt::open_with_direct`]), which binds capability
    /// advertisement and pre-handler direct input resolution into a single path.
    pub fn apply_worker_attributes(hello: &mut crate::v2::Hello) {
        hello.attributes.insert(
            DIRECT_CAPABILITY_KEY.to_owned(),
            DIRECT_CAPABILITY_VALUE.to_owned(),
        );
    }

    /// Stages an input object for a live invocation and returns
    /// `(uri, sha256, size_bytes, InputLeaseGuard)`.
    pub fn stage_input(
        &self,
        data: Bytes,
        max_fetches: u32,
    ) -> Result<(String, String, u64, InputLeaseGuard), DirectError> {
        let size_bytes = data.len() as u64;
        let sha256 = sha256_hex(&data);
        let (token, guard) = self.store.stage_input(
            data,
            sha256.clone(),
            max_fetches,
            self.config.input_token_ttl,
        );
        let uri = encode_direct_uri(
            &self.endpoint.addr(),
            &token,
            self.config.advertise_direct_addrs,
        )?;
        Ok((uri, sha256, size_bytes, guard))
    }

    /// Stages a worker output object into the post-finish bounded LRU cache and
    /// returns `(uri, sha256, size_bytes)`.
    pub fn stage_output(&self, data: Bytes) -> Result<(String, String, u64), DirectError> {
        let size_bytes = data.len() as u64;
        let sha256 = sha256_hex(&data);
        let token = self
            .store
            .stage_output(data, sha256.clone(), self.config.output_cache_ttl)?;
        let uri = encode_direct_uri(
            &self.endpoint.addr(),
            &token,
            self.config.advertise_direct_addrs,
        )?;
        Ok((uri, sha256, size_bytes))
    }

    /// Fetches an `anyserve+iroh://` object from its remote provider, enforcing
    /// path protection (upgrade window vs rate-limited public n0 relay),
    /// cooperative cancellation (`reset`/`stop`), per-chunk idle timeout,
    /// `anyserve.size_bytes`, and streaming `anyserve.sha256` verification.
    pub async fn fetch_object(
        &self,
        raw_uri: &str,
        metadata: &HashMap<String, String>,
        cancel: &DirectCancel,
    ) -> Result<(Bytes, DirectTransferReport), DirectError> {
        if cancel.is_cancelled() {
            return Err(DirectError::Cancelled);
        }
        let parsed = decode_direct_uri(raw_uri)?;
        let expected_sha256 = metadata
            .get(OBJECT_SHA256_METADATA_KEY)
            .map(String::as_str)
            .filter(|s| !s.is_empty());
        let expected_size: Option<u64> = metadata
            .get(OBJECT_SIZE_METADATA_KEY)
            .and_then(|s| s.parse::<u64>().ok());

        if let Some(sz) = expected_size
            && sz > self.config.max_object_bytes as u64
        {
            return Err(DirectError::ObjectTooLarge {
                size: sz,
                limit: self.config.max_object_bytes,
            });
        }

        let conn = tokio::select! {
            _ = cancel.cancelled() => return Err(DirectError::Cancelled),
            res = tokio::time::timeout(
                self.config.idle_timeout,
                self.endpoint.connect(parsed.endpoint_addr(), DIRECT_ALPN),
            ) => match res {
                Ok(Ok(conn)) => conn,
                Ok(Err(e)) => return Err(DirectError::Connect(format!("{}: {e}", redact_direct_uri(raw_uri)))),
                Err(_) => return Err(DirectError::IdleTimeout),
            }
        };

        let observed_path = self.wait_for_acceptable_path(&conn, cancel).await?;
        let direct_path = match observed_path {
            ObservedPath::DirectIp => DirectPath::Direct,
            ObservedPath::CustomRelay => DirectPath::Relay,
            ObservedPath::PublicRelayOnly => {
                conn.close(CANCEL_VARINT_CODE.into(), b"public_relay_throttled");
                return Err(DirectError::PublicRelayThrottled {
                    detail: format!(
                        "still on public n0 relay after {}ms upgrade window",
                        self.config.upgrade_window.as_millis()
                    ),
                });
            }
        };

        let (mut send, mut recv) = tokio::select! {
            _ = cancel.cancelled() => {
                conn.close(CANCEL_VARINT_CODE.into(), b"cancelled");
                return Err(DirectError::Cancelled);
            }
            res = tokio::time::timeout(self.config.idle_timeout, conn.open_bi()) => match res {
                Ok(Ok(pair)) => pair,
                Ok(Err(e)) => return Err(map_stream_error(e.to_string())),
                Err(_) => return Err(DirectError::IdleTimeout),
            }
        };

        let result = self
            .exchange_object(
                &mut send,
                &mut recv,
                &parsed.token,
                expected_size,
                expected_sha256,
                direct_path,
                cancel,
            )
            .await;

        if matches!(result, Err(DirectError::Cancelled)) {
            let _ = send.reset(CANCEL_VARINT_CODE.into());
            let _ = recv.stop(CANCEL_VARINT_CODE.into());
            conn.close(CANCEL_VARINT_CODE.into(), b"cancelled");
        } else {
            conn.close(0u32.into(), b"done");
        }
        result
    }

    async fn wait_for_acceptable_path(
        &self,
        conn: &Connection,
        cancel: &DirectCancel,
    ) -> Result<ObservedPath, DirectError> {
        let mut observed = self
            .config
            .path_classifier
            .classify(conn, self.has_custom_relay);
        if observed != ObservedPath::PublicRelayOnly {
            return Ok(observed);
        }
        let deadline = Instant::now() + self.config.upgrade_window;
        while Instant::now() < deadline {
            let sleep_for =
                PATH_POLL_INTERVAL.min(deadline.saturating_duration_since(Instant::now()));
            tokio::select! {
                _ = cancel.cancelled() => return Err(DirectError::Cancelled),
                _ = tokio::time::sleep(sleep_for) => {}
            }
            observed = self
                .config
                .path_classifier
                .classify(conn, self.has_custom_relay);
            if observed != ObservedPath::PublicRelayOnly {
                return Ok(observed);
            }
        }
        Ok(ObservedPath::PublicRelayOnly)
    }

    #[allow(clippy::too_many_arguments)]
    async fn exchange_object(
        &self,
        send: &mut SendStream,
        recv: &mut RecvStream,
        token: &str,
        expected_size: Option<u64>,
        expected_sha256: Option<&str>,
        direct_path: DirectPath,
        cancel: &DirectCancel,
    ) -> Result<(Bytes, DirectTransferReport), DirectError> {
        let tok_bytes = token.as_bytes();
        let tok_len = u16::try_from(tok_bytes.len())
            .map_err(|_| DirectError::InvalidUri("token length overflow".into()))?;
        let mut req_buf = Vec::with_capacity(1 + 2 + tok_bytes.len() + 8);
        req_buf.push(PROTO_VERSION);
        req_buf.extend_from_slice(&tok_len.to_be_bytes());
        req_buf.extend_from_slice(tok_bytes);
        req_buf.extend_from_slice(&0u64.to_be_bytes()); // offset = 0

        write_all_checked(send, &req_buf, self.config.idle_timeout, cancel).await?;

        let mut status_buf = [0u8; 1];
        read_exact_checked(recv, &mut status_buf, self.config.idle_timeout, cancel).await?;
        match status_buf[0] {
            STATUS_OK => {}
            STATUS_NOT_FOUND_OR_EXPIRED => return Err(DirectError::NotFoundOrExpired),
            STATUS_FETCH_LIMIT_EXCEEDED => return Err(DirectError::FetchLimitExceeded),
            STATUS_CONCURRENCY_LIMIT_EXCEEDED => return Err(DirectError::ConcurrencyLimitExceeded),
            STATUS_INVALID_REQUEST => {
                return Err(DirectError::Protocol(
                    "peer rejected malformed request".into(),
                ));
            }
            other => {
                return Err(DirectError::Protocol(format!(
                    "unexpected direct response status {other}"
                )));
            }
        }

        let mut len_buf = [0u8; 8];
        read_exact_checked(recv, &mut len_buf, self.config.idle_timeout, cancel).await?;
        let total_len = u64::from_be_bytes(len_buf);
        if let Some(exp) = expected_size
            && total_len != exp
        {
            return Err(DirectError::SizeMismatch {
                expected: exp,
                actual: total_len,
            });
        }
        if total_len > self.config.max_object_bytes as u64 {
            return Err(DirectError::ObjectTooLarge {
                size: total_len,
                limit: self.config.max_object_bytes,
            });
        }

        let mut out = Vec::with_capacity(total_len as usize);
        let mut hasher = Sha256::new();
        let mut chunk = vec![0u8; CHUNK_BYTES];
        loop {
            let n = tokio::select! {
                _ = cancel.cancelled() => return Err(DirectError::Cancelled),
                res = tokio::time::timeout(self.config.idle_timeout, recv.read(&mut chunk)) => match res {
                    Ok(Ok(Some(n))) => n,
                    Ok(Ok(None)) => break,
                    Ok(Err(e)) => return Err(map_stream_error(e.to_string())),
                    Err(_) => return Err(DirectError::IdleTimeout),
                }
            };
            if (out.len() as u64).saturating_add(n as u64) > total_len {
                return Err(DirectError::SizeMismatch {
                    expected: total_len,
                    actual: (out.len() as u64).saturating_add(n as u64),
                });
            }
            hasher.update(&chunk[..n]);
            out.extend_from_slice(&chunk[..n]);
        }

        if out.len() as u64 != total_len {
            return Err(DirectError::SizeMismatch {
                expected: total_len,
                actual: out.len() as u64,
            });
        }

        let actual_sha256 = hex_lower(&hasher.finalize());
        if let Some(expected) = expected_sha256
            && !actual_sha256.eq_ignore_ascii_case(expected)
        {
            return Err(DirectError::Sha256Mismatch {
                expected: expected.to_owned(),
                actual: actual_sha256,
            });
        }

        // Acknowledge verified receipt so worker post-finish output cache evicts immediately.
        let _ = write_all_checked(send, &[ACK_VERIFIED], ACK_WAIT_TIMEOUT, cancel).await;
        let _ = send.finish();

        Ok((
            Bytes::from(out),
            DirectTransferReport {
                path: direct_path,
                bytes: total_len,
                sha256: actual_sha256,
            },
        ))
    }

    pub async fn close(&self) {
        self.accept_task.abort();
        self.endpoint.close().await;
    }
}

impl Drop for DirectEndpoint {
    fn drop(&mut self) {
        self.accept_task.abort();
    }
}

/// Lazily initialized singleton wrapper attached to [`crate::ExecutionClient`]
/// so `iroh` only binds when direct invocation is actually used.
pub struct LazyDirectEndpoint {
    config: DirectConfig,
    cell: OnceCell<Arc<DirectEndpoint>>,
}

impl LazyDirectEndpoint {
    pub fn new(config: DirectConfig) -> Self {
        Self {
            config,
            cell: OnceCell::new(),
        }
    }

    pub fn config(&self) -> &DirectConfig {
        &self.config
    }

    pub async fn get_or_init(&self) -> Result<Arc<DirectEndpoint>, DirectError> {
        self.cell
            .get_or_try_init(|| DirectEndpoint::bind(self.config.clone()))
            .await
            .map(Arc::clone)
    }
}

async fn serve_connection(
    conn: Connection,
    store: DirectObjectStore,
    max_streams: usize,
    idle_timeout: Duration,
) {
    let stream_sem = Arc::new(Semaphore::new(max_streams));
    while let Ok((send, recv)) = conn.accept_bi().await {
        let Ok(permit) = stream_sem.clone().try_acquire_owned() else {
            let mut s = send;
            let _ = s.write_all(&[STATUS_CONCURRENCY_LIMIT_EXCEEDED]).await;
            let _ = s.finish();
            continue;
        };
        let store = store.clone();
        tokio::spawn(async move {
            let _permit = permit;
            serve_stream(send, recv, store, idle_timeout).await;
        });
    }
}

async fn serve_stream(
    mut send: SendStream,
    mut recv: RecvStream,
    store: DirectObjectStore,
    idle_timeout: Duration,
) {
    let mut head = [0u8; 3];
    if !matches!(
        tokio::time::timeout(idle_timeout, recv.read_exact(&mut head)).await,
        Ok(Ok(()))
    ) {
        return;
    }
    let version = head[0];
    let tok_len = u16::from_be_bytes([head[1], head[2]]) as usize;
    if version != PROTO_VERSION || tok_len == 0 || tok_len > MAX_TOKEN_WIRE_LEN {
        let _ = send.write_all(&[STATUS_INVALID_REQUEST]).await;
        let _ = send.finish();
        return;
    }

    let mut tok_buf = vec![0u8; tok_len];
    if !matches!(
        tokio::time::timeout(idle_timeout, recv.read_exact(&mut tok_buf)).await,
        Ok(Ok(()))
    ) {
        return;
    }
    let mut offset_buf = [0u8; 8];
    if !matches!(
        tokio::time::timeout(idle_timeout, recv.read_exact(&mut offset_buf)).await,
        Ok(Ok(()))
    ) {
        return;
    }
    let Ok(token) = std::str::from_utf8(&tok_buf) else {
        let _ = send.write_all(&[STATUS_INVALID_REQUEST]).await;
        let _ = send.finish();
        return;
    };
    let offset = u64::from_be_bytes(offset_buf) as usize;

    let checked = match store.checkout(token) {
        Ok(obj) => obj,
        Err(DirectError::NotFoundOrExpired) => {
            let _ = send.write_all(&[STATUS_NOT_FOUND_OR_EXPIRED]).await;
            let _ = send.finish();
            return;
        }
        Err(DirectError::FetchLimitExceeded) => {
            let _ = send.write_all(&[STATUS_FETCH_LIMIT_EXCEEDED]).await;
            let _ = send.finish();
            return;
        }
        Err(DirectError::ConcurrencyLimitExceeded) => {
            let _ = send.write_all(&[STATUS_CONCURRENCY_LIMIT_EXCEEDED]).await;
            let _ = send.finish();
            return;
        }
        Err(_) => {
            let _ = send.write_all(&[STATUS_INVALID_REQUEST]).await;
            let _ = send.finish();
            return;
        }
    };

    if offset > checked.data.len() {
        let _ = send.write_all(&[STATUS_INVALID_REQUEST]).await;
        let _ = send.finish();
        return;
    }

    let slice = &checked.data[offset..];
    let mut resp_head = [0u8; 9];
    resp_head[0] = STATUS_OK;
    resp_head[1..9].copy_from_slice(&(slice.len() as u64).to_be_bytes());
    if !matches!(
        tokio::time::timeout(idle_timeout, send.write_all(&resp_head)).await,
        Ok(Ok(()))
    ) {
        return;
    }
    for chunk in slice.chunks(CHUNK_BYTES) {
        if !matches!(
            tokio::time::timeout(idle_timeout, send.write_all(chunk)).await,
            Ok(Ok(()))
        ) {
            return;
        }
    }
    let _ = send.finish();

    if checked.kind == EntryKind::Output {
        let mut ack = [0u8; 1];
        if matches!(
            tokio::time::timeout(ACK_WAIT_TIMEOUT, recv.read_exact(&mut ack)).await,
            Ok(Ok(()))
        ) && ack[0] == ACK_VERIFIED
        {
            store.ack_output(checked.digest);
        }
    }
}

async fn write_all_checked(
    send: &mut SendStream,
    buf: &[u8],
    timeout: Duration,
    cancel: &DirectCancel,
) -> Result<(), DirectError> {
    tokio::select! {
        _ = cancel.cancelled() => Err(DirectError::Cancelled),
        res = tokio::time::timeout(timeout, send.write_all(buf)) => match res {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(map_stream_error(e.to_string())),
            Err(_) => Err(DirectError::IdleTimeout),
        }
    }
}

async fn read_exact_checked(
    recv: &mut RecvStream,
    buf: &mut [u8],
    timeout: Duration,
    cancel: &DirectCancel,
) -> Result<(), DirectError> {
    tokio::select! {
        _ = cancel.cancelled() => Err(DirectError::Cancelled),
        res = tokio::time::timeout(timeout, recv.read_exact(buf)) => match res {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(map_stream_error(e.to_string())),
            Err(_) => Err(DirectError::IdleTimeout),
        }
    }
}

fn map_stream_error(msg: String) -> DirectError {
    if msg.contains("peer_not_allowed") || msg.contains("application closed") {
        DirectError::PeerRejected(msg)
    } else {
        DirectError::Protocol(msg)
    }
}

fn resolve_relay_mode(config: &DirectConfig) -> Result<(RelayMode, bool), DirectError> {
    if config.disable_relays {
        return Ok((RelayMode::Disabled, false));
    }
    if let Some(raw) = config.relay_url.as_deref() {
        let url: RelayUrl = raw
            .parse()
            .map_err(|e| DirectError::InvalidConfig(format!("invalid relay_url '{raw}': {e}")))?;
        return Ok((RelayMode::custom([url]), true));
    }
    Ok((RelayMode::Default, false))
}

fn load_or_create_secret_key(path: &Path) -> Result<SecretKey, DirectError> {
    if path.exists() {
        let raw = std::fs::read(path)
            .map_err(|e| DirectError::InvalidConfig(format!("cannot read secret_key_path: {e}")))?;
        if raw.len() == 32 {
            let bytes: [u8; 32] = raw.try_into().expect("checked 32 bytes");
            return Ok(SecretKey::from_bytes(&bytes));
        }
        let text = std::str::from_utf8(&raw)
            .map_err(|_| {
                DirectError::InvalidConfig("secret key file is not valid 32B raw or 64-hex".into())
            })?
            .trim();
        let decoded = decode_hex_32(text)?;
        return Ok(SecretKey::from_bytes(&decoded));
    }

    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent).map_err(|e| {
            DirectError::InvalidConfig(format!("cannot create secret key dir: {e}"))
        })?;
    }
    let key = SecretKey::generate();
    let encoded = format!("{}\n", hex_lower(&key.to_bytes()));

    #[cfg(unix)]
    {
        use std::io::Write as _;
        use std::os::unix::fs::OpenOptionsExt as _;
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(path)
            .map_err(|e| {
                DirectError::InvalidConfig(format!("cannot write secret_key_path: {e}"))
            })?;
        file.write_all(encoded.as_bytes()).map_err(|e| {
            DirectError::InvalidConfig(format!("cannot write secret_key_path: {e}"))
        })?;
        file.sync_all()
            .map_err(|e| DirectError::InvalidConfig(format!("cannot sync secret_key_path: {e}")))?;
    }
    #[cfg(not(unix))]
    {
        std::fs::write(path, encoded.as_bytes()).map_err(|e| {
            DirectError::InvalidConfig(format!("cannot write secret_key_path: {e}"))
        })?;
    }

    Ok(key)
}

fn decode_hex_32(hex: &str) -> Result<[u8; 32], DirectError> {
    if hex.len() != 64 {
        return Err(DirectError::InvalidConfig(
            "secret key file must contain 32 raw bytes or 64 hex characters".into(),
        ));
    }
    let mut out = [0u8; 32];
    let bytes = hex.as_bytes();
    for i in 0..32 {
        let hi = from_hex_nibble(bytes[i * 2])?;
        let lo = from_hex_nibble(bytes[i * 2 + 1])?;
        out[i] = (hi << 4) | lo;
    }
    Ok(out)
}

fn from_hex_nibble(b: u8) -> Result<u8, DirectError> {
    match b {
        b'0'..=b'9' => Ok(b - b'0'),
        b'a'..=b'f' => Ok(b - b'a' + 10),
        b'A'..=b'F' => Ok(b - b'A' + 10),
        _ => Err(DirectError::InvalidConfig(
            "secret key file contains non-hex characters".into(),
        )),
    }
}
