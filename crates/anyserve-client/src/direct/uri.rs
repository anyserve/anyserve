use std::fmt;
use std::net::SocketAddr;

use iroh::{EndpointAddr, EndpointId, RelayUrl, TransportAddr};
use url::Url;

use super::DirectError;
use crate::DIRECT_URI_SCHEME;

const MAX_TOKEN_BYTES: usize = 128;

/// Decoded `anyserve+iroh://<endpoint-id>/<token>?relay=<url>&addr=<ip:port>`.
#[derive(Clone, PartialEq, Eq)]
pub struct ParsedDirectUri {
    pub endpoint_id: EndpointId,
    pub token: String,
    pub addrs: Vec<TransportAddr>,
}

impl ParsedDirectUri {
    pub fn endpoint_addr(&self) -> EndpointAddr {
        EndpointAddr::from_parts(self.endpoint_id, self.addrs.clone())
    }
}

impl fmt::Debug for ParsedDirectUri {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let redacted_addrs: Vec<String> = self
            .addrs
            .iter()
            .map(|a| match a {
                TransportAddr::Ip(_) => "Ip([redacted])".to_owned(),
                TransportAddr::Relay(url) => format!("Relay({url})"),
                _ => "[other]".to_owned(),
            })
            .collect();
        f.debug_struct("ParsedDirectUri")
            .field("endpoint_id", &self.endpoint_id)
            .field("token", &"[redacted]")
            .field("addrs", &redacted_addrs)
            .finish()
    }
}

/// Encodes an `EndpointAddr` and capability token into an `anyserve+iroh://` URI.
/// When `advertise_direct_addrs` is `false`, `addr=` candidates are omitted so
/// client/worker IP addresses are not disclosed to the control plane.
pub fn encode_direct_uri(
    addr: &EndpointAddr,
    token: &str,
    advertise_direct_addrs: bool,
) -> Result<String, DirectError> {
    validate_token(token)?;
    let mut url = Url::parse(&format!("{DIRECT_URI_SCHEME}://{}/{token}", addr.id))
        .map_err(|e| DirectError::InvalidUri(e.to_string()))?;
    let mut has_query = false;
    {
        let mut q = url.query_pairs_mut();
        for relay in addr.relay_urls() {
            q.append_pair("relay", relay.as_str());
            has_query = true;
        }
        if advertise_direct_addrs {
            for ip in addr.ip_addrs() {
                q.append_pair("addr", &ip.to_string());
                has_query = true;
            }
        }
    }
    if !has_query {
        url.set_query(None);
    }
    Ok(url.into())
}

/// Decodes and validates an `anyserve+iroh://` URI.
pub fn decode_direct_uri(raw: &str) -> Result<ParsedDirectUri, DirectError> {
    let url = Url::parse(raw).map_err(|e| DirectError::InvalidUri(e.to_string()))?;
    if url.scheme() != DIRECT_URI_SCHEME {
        return Err(DirectError::InvalidUri(format!(
            "expected scheme '{DIRECT_URI_SCHEME}', got '{}'",
            url.scheme()
        )));
    }
    let host = url
        .host_str()
        .ok_or_else(|| DirectError::InvalidUri("missing endpoint id".into()))?;
    let endpoint_id: EndpointId = host
        .parse()
        .map_err(|e| DirectError::InvalidUri(format!("invalid endpoint id: {e}")))?;
    let token = url.path().trim_start_matches('/').to_owned();
    validate_token(&token)?;

    let mut addrs = Vec::new();
    for (k, v) in url.query_pairs() {
        match k.as_ref() {
            "relay" => {
                let relay: RelayUrl = v
                    .parse()
                    .map_err(|e| DirectError::InvalidUri(format!("invalid relay url: {e}")))?;
                addrs.push(TransportAddr::Relay(relay));
            }
            "addr" => {
                let sa: SocketAddr = v
                    .parse()
                    .map_err(|e| DirectError::InvalidUri(format!("invalid socket addr: {e}")))?;
                addrs.push(TransportAddr::Ip(sa));
            }
            _ => {}
        }
    }
    Ok(ParsedDirectUri {
        endpoint_id,
        token,
        addrs,
    })
}

fn validate_token(token: &str) -> Result<(), DirectError> {
    if token.is_empty()
        || token.len() > MAX_TOKEN_BYTES
        || !token
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'))
    {
        return Err(DirectError::InvalidUri(format!(
            "object token must be 1..={MAX_TOKEN_BYTES} ASCII [a-zA-Z0-9_-] characters"
        )));
    }
    Ok(())
}
