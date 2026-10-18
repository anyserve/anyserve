//! Request-level routing controls shared by the HTTP gateways.
//!
//! Callers steer placement with `x-anyserve-*` headers so a central service can
//! say "prefer my region, spill over after N ms, give up after M ms" per request
//! without the gateway knowing anything about clusters.

use std::sync::Arc;

use anyserve_core::auth::AuthPolicy;
use anyserve_core::runtime::api::{Demand, ExecutionPolicy, Metadata as Attributes};
use axum::extract::{Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use std::time::Duration;

pub(crate) const REQUIRE_HEADER: &str = "x-anyserve-require";
pub(crate) const PREFER_HEADER: &str = "x-anyserve-prefer";
pub(crate) const TIMEOUT_HEADER: &str = "x-anyserve-timeout-ms";
pub(crate) const QUEUE_TIMEOUT_HEADER: &str = "x-anyserve-queue-timeout-ms";
pub(crate) const SPILLOVER_HEADER: &str = "x-anyserve-spillover-ms";
pub(crate) const PRIORITY_HEADER: &str = "x-anyserve-priority";

/// Placement defaults applied when a request does not override them.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct RoutingDefaults {
    pub(crate) queue_timeout_ms: u64,
    pub(crate) spillover_after_ms: u64,
    pub(crate) execution_timeout_ms: u64,
    pub(crate) priority: u32,
}

impl RoutingDefaults {
    pub(crate) fn policy(&self) -> ExecutionPolicy {
        ExecutionPolicy {
            queue_timeout: milliseconds(self.queue_timeout_ms),
            spillover_after: milliseconds(self.spillover_after_ms),
            execution_timeout: milliseconds(self.execution_timeout_ms),
            priority: self.priority,
            ..ExecutionPolicy::default()
        }
    }
}

/// Applies `x-anyserve-*` overrides from `headers` to the demand and policy.
pub(crate) fn apply_routing_headers(
    headers: &HeaderMap,
    demand: &mut Demand,
    policy: &mut ExecutionPolicy,
) -> Result<(), String> {
    if let Some(value) = header_str(headers, REQUIRE_HEADER)? {
        demand.required_attributes.extend(parse_attributes(value)?);
    }
    if let Some(value) = header_str(headers, PREFER_HEADER)? {
        demand.preferred_attributes.extend(parse_attributes(value)?);
    }
    if let Some(value) = header_number::<u64>(headers, QUEUE_TIMEOUT_HEADER)? {
        policy.queue_timeout = milliseconds(value);
    }
    if let Some(value) = header_number::<u64>(headers, SPILLOVER_HEADER)? {
        policy.spillover_after = milliseconds(value);
    }
    if let Some(value) = header_number::<u32>(headers, PRIORITY_HEADER)? {
        policy.priority = value;
    }
    Ok(())
}

pub(crate) fn header_str<'a>(
    headers: &'a HeaderMap,
    name: &str,
) -> Result<Option<&'a str>, String> {
    headers
        .get(name)
        .map(|value| {
            value
                .to_str()
                .map(str::trim)
                .map_err(|_| format!("header {name} is not valid ASCII"))
        })
        .transpose()
}

pub(crate) fn header_number<T: std::str::FromStr>(
    headers: &HeaderMap,
    name: &str,
) -> Result<Option<T>, String> {
    header_str(headers, name)?
        .map(|value| {
            value
                .parse::<T>()
                .map_err(|_| format!("header {name} must be a number, got '{value}'"))
        })
        .transpose()
}

/// Parses `key=value,key2=value2`.
pub(crate) fn parse_attributes(raw: &str) -> Result<Attributes, String> {
    raw.split(',')
        .map(str::trim)
        .filter(|pair| !pair.is_empty())
        .map(|pair| {
            let (key, value) = pair
                .split_once('=')
                .ok_or_else(|| format!("expected key=value, got '{pair}'"))?;
            let (key, value) = (key.trim(), value.trim());
            if key.is_empty() {
                return Err(format!("empty attribute key in '{pair}'"));
            }
            Ok((key.to_string(), value.to_string()))
        })
        .collect()
}

/// Axum middleware: requires a client bearer token when auth is enabled.
pub(crate) async fn require_client_token(
    State(policy): State<Arc<AuthPolicy>>,
    request: Request,
    next: Next,
) -> Response {
    let path = request.uri().path();
    if path == "/healthz" || path == "/readyz" {
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

fn milliseconds(value: u64) -> Option<Duration> {
    (value > 0).then(|| Duration::from_millis(value))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_attribute_lists() {
        let attrs = parse_attributes("region=ap-south-1, accelerator = l4").unwrap();
        assert_eq!(attrs.get("region").map(String::as_str), Some("ap-south-1"));
        assert_eq!(attrs.get("accelerator").map(String::as_str), Some("l4"));
        assert!(parse_attributes("broken").is_err());
    }

    #[test]
    fn headers_override_defaults() {
        let mut headers = HeaderMap::new();
        headers.insert(PREFER_HEADER, "region=mumbai".parse().unwrap());
        headers.insert(SPILLOVER_HEADER, "40".parse().unwrap());
        headers.insert(QUEUE_TIMEOUT_HEADER, "2000".parse().unwrap());
        let defaults = RoutingDefaults {
            spillover_after_ms: 10,
            ..RoutingDefaults::default()
        };
        let mut demand = Demand::default();
        let mut policy = defaults.policy();

        apply_routing_headers(&headers, &mut demand, &mut policy).unwrap();

        assert_eq!(
            demand
                .preferred_attributes
                .get("region")
                .map(String::as_str),
            Some("mumbai")
        );
        assert_eq!(policy.spillover_after, Some(Duration::from_millis(40)));
        assert_eq!(policy.queue_timeout, Some(Duration::from_millis(2000)));
    }

    #[test]
    fn rejects_non_numeric_timeouts() {
        let mut headers = HeaderMap::new();
        headers.insert(SPILLOVER_HEADER, "soon".parse().unwrap());
        let error = apply_routing_headers(
            &headers,
            &mut Demand::default(),
            &mut ExecutionPolicy::default(),
        )
        .unwrap_err();
        assert!(error.contains(SPILLOVER_HEADER));
    }
}
