//! Native clients and outbound worker sessions for the execution protocol.
pub mod execution;
pub use anyserve_proto::v2;
pub use execution::ExecutionClient;

use anyhow::{Context, Result, anyhow};
use std::time::Duration;
use tonic::metadata::AsciiMetadataValue;
use tonic::service::Interceptor;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tonic::{Request, Status};

/// Environment variable consulted for a bearer token when none is passed explicitly.
pub const TOKEN_ENV: &str = "ANYSERVE_TOKEN";

/// Attaches `authorization: Bearer <token>` to every request when a token is set.
#[derive(Clone, Debug, Default)]
pub struct BearerAuth {
    header: Option<AsciiMetadataValue>,
}

impl BearerAuth {
    pub fn new(token: Option<&str>) -> Result<Self> {
        let header = token
            .map(str::trim)
            .filter(|token| !token.is_empty())
            .map(|token| {
                format!("Bearer {token}")
                    .parse::<AsciiMetadataValue>()
                    .map_err(|_| anyhow!("anyserve token contains invalid characters"))
            })
            .transpose()?;
        Ok(Self { header })
    }
}

impl Interceptor for BearerAuth {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        if let Some(header) = self.header.as_ref() {
            request
                .metadata_mut()
                .insert("authorization", header.clone());
        }
        Ok(request)
    }
}

/// Each call establishes its own connection. Worker control and attempt data
/// deliberately use separate calls so HTTP/2 connection flow control is isolated.
pub(crate) async fn connect_channel(endpoint: &str) -> Result<Channel> {
    let mut transport = Endpoint::from_shared(endpoint.to_owned())
        .context("parse anyserve gRPC endpoint")?
        .tcp_nodelay(true)
        .connect_timeout(Duration::from_secs(5))
        .initial_stream_window_size(4 * 1024 * 1024)
        .initial_connection_window_size(32 * 1024 * 1024)
        .http2_keep_alive_interval(Duration::from_secs(20))
        .keep_alive_while_idle(true);
    if endpoint.starts_with("https://") {
        transport = transport
            .tls_config(ClientTlsConfig::new().with_enabled_roots())
            .context("configure anyserve gRPC TLS")?;
    }
    transport
        .connect()
        .await
        .context("connect anyserve gRPC endpoint")
}
