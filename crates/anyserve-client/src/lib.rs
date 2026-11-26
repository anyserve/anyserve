//! Native clients and outbound worker sessions for the execution protocol.
#[cfg(feature = "direct")]
pub mod direct;
pub mod execution;
pub(crate) mod ws_transport;
pub use anyserve_proto::v2;
pub use execution::ExecutionClient;

/// URI scheme for AnyServe's optional `iroh` peer-to-peer direct object transfer.
pub const DIRECT_URI_SCHEME: &str = "anyserve+iroh";
/// Capability flag carried in `Hello.attributes`, `Demand.required_attributes`,
/// and `JobSpec.metadata` to negotiate direct data-plane support without core changes.
pub const DIRECT_CAPABILITY_KEY: &str = "anyserve.direct";
pub const DIRECT_CAPABILITY_VALUE: &str = "1";
/// Content-addressed metadata keys matching `anyserve-core` and Cloudflare DO identity hashing.
pub const OBJECT_SHA256_METADATA_KEY: &str = "anyserve.sha256";
pub const OBJECT_SIZE_METADATA_KEY: &str = "anyserve.size_bytes";
/// SDK-reported observability metadata keys attached to `ResultChunk.metadata`.
pub const DIRECT_PATH_METADATA_KEY: &str = "anyserve.direct_path";
pub const DIRECT_BYTES_METADATA_KEY: &str = "anyserve.direct_bytes";
/// Non-retryable attempt error codes produced before handler execution.
pub const ERROR_CODE_DIRECT_UNSUPPORTED: &str = "direct_unsupported";
pub const ERROR_CODE_DIRECT_PUBLIC_RELAY_THROTTLED: &str = "direct_public_relay_throttled";

/// Returns `true` when `uri` uses the `anyserve+iroh://` scheme.
pub fn is_direct_uri(uri: &str) -> bool {
    uri.starts_with("anyserve+iroh://")
}

/// Redacts the capability token path segment and any `addr=` IP query values of
/// an `anyserve+iroh://` URI for safe logging and UI display while preserving
/// endpoint ID and `relay=` URLs. Non-direct URIs are returned unchanged.
pub fn redact_direct_uri(uri: &str) -> String {
    let Some(rest) = uri.strip_prefix("anyserve+iroh://") else {
        return uri.to_owned();
    };
    let (without_frag, frag) = match rest.find('#') {
        Some(idx) => (&rest[..idx], &rest[idx..]),
        None => (rest, ""),
    };
    let (authority_and_path, query) = match without_frag.find('?') {
        Some(idx) => (&without_frag[..idx], Some(&without_frag[idx + 1..])),
        None => (without_frag, None),
    };
    let redacted_authority_path = match authority_and_path.split_once('/') {
        Some((authority, path)) if !path.is_empty() => format!("{authority}/[redacted]"),
        _ => authority_and_path.to_owned(),
    };
    let redacted_query = query.map(|q| {
        q.split('&')
            .map(|pair| {
                if pair.starts_with("addr=") {
                    "addr=[redacted]".to_owned()
                } else {
                    pair.to_owned()
                }
            })
            .collect::<Vec<_>>()
            .join("&")
    });
    match redacted_query {
        Some(q) if !q.is_empty() => {
            format!("anyserve+iroh://{redacted_authority_path}?{q}{frag}")
        }
        _ => format!("anyserve+iroh://{redacted_authority_path}{frag}"),
    }
}

use anyhow::{Context, Result, anyhow};
use std::time::Duration;
use tonic::metadata::AsciiMetadataValue;
use tonic::service::Interceptor;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tonic::{Request, Status};

/// Environment variable consulted for a bearer token when none is passed explicitly.
pub const TOKEN_ENV: &str = "ANYSERVE_TOKEN";
/// Environment variable consulted for a tenant namespace when none is passed explicitly.
pub const NAMESPACE_ENV: &str = "ANYSERVE_NAMESPACE";

/// Attaches `authorization: Bearer <token>` and optional `x-anyserve-namespace`
/// to every request.
#[derive(Clone, Default)]
pub struct BearerAuth {
    header: Option<AsciiMetadataValue>,
    namespace: Option<AsciiMetadataValue>,
}

impl std::fmt::Debug for BearerAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BearerAuth")
            .field("header", &self.header.as_ref().map(|_| "[redacted]"))
            .field("namespace", &self.namespace_header())
            .finish()
    }
}

impl BearerAuth {
    pub fn new(token: Option<&str>) -> Result<Self> {
        Self::with_namespace(token, None)
    }

    pub fn with_namespace(token: Option<&str>, namespace: Option<&str>) -> Result<Self> {
        let header = token
            .map(str::trim)
            .filter(|token| !token.is_empty())
            .map(|token| {
                format!("Bearer {token}")
                    .parse::<AsciiMetadataValue>()
                    .map_err(|_| anyhow!("anyserve token contains invalid characters"))
            })
            .transpose()?;
        let namespace = namespace
            .map(str::trim)
            .filter(|ns| !ns.is_empty())
            .map(|ns| {
                ns.parse::<AsciiMetadataValue>()
                    .map_err(|_| anyhow!("anyserve namespace contains invalid characters"))
            })
            .transpose()?;
        Ok(Self { header, namespace })
    }

    /// Returns the formatted `authorization` header value (`Bearer <token>`), if configured.
    pub fn authorization_header(&self) -> Option<&str> {
        self.header.as_ref().and_then(|value| value.to_str().ok())
    }

    /// Returns the configured `x-anyserve-namespace` header value, if configured.
    pub fn namespace_header(&self) -> Option<&str> {
        self.namespace
            .as_ref()
            .and_then(|value| value.to_str().ok())
    }
}

impl Interceptor for BearerAuth {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        if let Some(header) = self.header.as_ref() {
            request
                .metadata_mut()
                .insert("authorization", header.clone());
        }
        if let Some(ns) = self.namespace.as_ref() {
            request
                .metadata_mut()
                .insert("x-anyserve-namespace", ns.clone());
        }
        Ok(request)
    }
}

pub(crate) fn is_ws_endpoint(endpoint: &str) -> bool {
    endpoint
        .parse::<http::Uri>()
        .ok()
        .and_then(|uri| {
            uri.scheme_str()
                .map(|s| s.eq_ignore_ascii_case("ws") || s.eq_ignore_ascii_case("wss"))
        })
        .unwrap_or(false)
}

pub(crate) fn parse_endpoints(raw: &str) -> Result<Vec<String>> {
    let endpoints: Vec<String> = raw
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .collect();
    if endpoints.is_empty() {
        return Err(anyhow!("anyserve endpoint must not be empty"));
    }
    for ep in &endpoints {
        let uri: http::Uri = ep
            .parse()
            .with_context(|| format!("parse anyserve endpoint '{ep}'"))?;
        let scheme = uri
            .scheme_str()
            .ok_or_else(|| anyhow!("anyserve endpoint '{ep}' is missing a scheme"))?;
        let authority = uri
            .authority()
            .ok_or_else(|| anyhow!("anyserve endpoint '{ep}' is missing an authority"))?;
        if authority.host().is_empty() {
            return Err(anyhow!("anyserve endpoint '{ep}' has an empty host"));
        }
        if scheme.eq_ignore_ascii_case("http") || scheme.eq_ignore_ascii_case("https") {
            Endpoint::from_shared(ep.clone())
                .with_context(|| format!("parse anyserve gRPC endpoint '{ep}'"))?;
        } else if !scheme.eq_ignore_ascii_case("ws") && !scheme.eq_ignore_ascii_case("wss") {
            return Err(anyhow!(
                "unsupported anyserve endpoint scheme '{scheme}' in '{ep}'"
            ));
        }
    }
    Ok(endpoints)
}

/// Open a channel to a single gRPC endpoint.
pub(crate) async fn connect_channel(endpoint: &str) -> Result<Channel> {
    let mut transport = Endpoint::from_shared(endpoint.to_owned())
        .context("parse anyserve gRPC endpoint")?
        .tcp_nodelay(true)
        .connect_timeout(Duration::from_secs(5))
        .initial_stream_window_size(4 * 1024 * 1024)
        .initial_connection_window_size(32 * 1024 * 1024)
        .http2_keep_alive_interval(Duration::from_secs(20))
        .keep_alive_while_idle(true);
    if endpoint
        .get(..8)
        .is_some_and(|scheme| scheme.eq_ignore_ascii_case("https://"))
    {
        transport = transport
            .tls_config(ClientTlsConfig::new().with_enabled_roots())
            .context("configure anyserve gRPC TLS")?;
    }
    transport
        .connect()
        .await
        .context("connect anyserve gRPC endpoint")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_redacts_bearer_token_while_keeping_namespace() {
        let auth = BearerAuth::with_namespace(Some("client-secret-123"), Some("tenant-x")).unwrap();
        let dbg = format!("{auth:?}");
        assert!(!dbg.contains("client-secret-123"));
        assert!(!dbg.contains("Bearer "));
        assert!(dbg.contains("[redacted]"));
        assert!(dbg.contains("tenant-x"));

        let anon = BearerAuth::with_namespace(None, Some("tenant-y")).unwrap();
        let anon_dbg = format!("{anon:?}");
        assert!(anon_dbg.contains("header: None"));
        assert!(anon_dbg.contains("tenant-y"));
    }
}
