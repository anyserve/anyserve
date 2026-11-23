//! W3C Trace Context parsing and transport-header propagation.
//!
//! Propagates `traceparent`, `tracestate`, `baggage`, and `x-request-id` from
//! gRPC metadata / HTTP headers into `JobSpec.metadata` without affecting
//! idempotency fingerprints (`IDENTITY_EXCLUDED_METADATA_KEYS`).

use std::collections::HashMap;

use crate::runtime::api::IDENTITY_EXCLUDED_METADATA_KEYS;

/// Maximum UTF-8 byte length accepted for a propagated trace/request header.
pub const MAX_TRACE_HEADER_BYTES: usize = 512;

/// Parsed W3C `traceparent` header (`version-trace_id-parent_id-trace_flags`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct W3cTraceContext {
    pub version: String,
    pub trace_id: String,
    pub parent_id: String,
    pub trace_flags: String,
    pub sampled: bool,
    pub traceparent: String,
}

fn is_lower_hex(s: &str) -> bool {
    !s.is_empty()
        && s.bytes()
            .all(|b| b.is_ascii_digit() || matches!(b, b'a'..=b'f'))
}

/// Parses a W3C Trace Context `traceparent` value (`00-<32hex>-<16hex>-<2hex>`).
///
/// Rejects uppercase hex, version `ff`, all-zero `trace-id`, and all-zero `parent-id`
/// per the W3C Trace Context specification.
pub fn parse_traceparent(raw: &str) -> Option<W3cTraceContext> {
    let trimmed = raw.trim();
    let mut parts = trimmed.split('-');
    let version = parts.next()?;
    let trace_id = parts.next()?;
    let parent_id = parts.next()?;
    let trace_flags = parts.next()?;
    if parts.next().is_some() {
        return None;
    }
    if version.len() != 2 || trace_id.len() != 32 || parent_id.len() != 16 || trace_flags.len() != 2
    {
        return None;
    }
    if !is_lower_hex(version)
        || !is_lower_hex(trace_id)
        || !is_lower_hex(parent_id)
        || !is_lower_hex(trace_flags)
    {
        return None;
    }
    if version == "ff"
        || trace_id.bytes().all(|b| b == b'0')
        || parent_id.bytes().all(|b| b == b'0')
    {
        return None;
    }
    let flags = u8::from_str_radix(trace_flags, 16).ok()?;
    Some(W3cTraceContext {
        version: version.to_owned(),
        trace_id: trace_id.to_owned(),
        parent_id: parent_id.to_owned(),
        trace_flags: trace_flags.to_owned(),
        sampled: (flags & 0x01) != 0,
        traceparent: trimmed.to_owned(),
    })
}

/// Merges trace/request headers into protobuf `JobSpec.metadata`.
///
/// Explicit non-empty keys already present on `JobSpec.metadata` take precedence.
/// Empty values or values exceeding [`MAX_TRACE_HEADER_BYTES`] are ignored.
pub fn merge_trace_headers_into_proto<'a>(
    metadata: &mut HashMap<String, String>,
    get_header: impl Fn(&str) -> Option<&'a str>,
) {
    for &key in IDENTITY_EXCLUDED_METADATA_KEYS {
        if metadata.get(key).is_some_and(|v| !v.trim().is_empty()) {
            continue;
        }
        let Some(raw) = get_header(key) else {
            continue;
        };
        let trimmed = raw.trim();
        if !trimmed.is_empty() && trimmed.len() <= MAX_TRACE_HEADER_BYTES {
            metadata.insert(key.to_owned(), trimmed.to_owned());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_traceparent_accepts_valid_sampled_and_unsampled() {
        let sampled =
            parse_traceparent("  00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01 \n")
                .expect("valid sampled traceparent");
        assert_eq!(sampled.version, "00");
        assert_eq!(sampled.trace_id, "4bf92f3577b34da6a3ce929d0e0e4736");
        assert_eq!(sampled.parent_id, "00f067aa0ba902b7");
        assert_eq!(sampled.trace_flags, "01");
        assert!(sampled.sampled);
        assert_eq!(
            sampled.traceparent,
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
        );

        let unsampled =
            parse_traceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00")
                .expect("valid unsampled traceparent");
        assert!(!unsampled.sampled);

        let custom_flags =
            parse_traceparent("01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-03")
                .expect("valid future version");
        assert!(custom_flags.sampled);
    }

    #[test]
    fn parse_traceparent_rejects_invalid_values() {
        let cases = [
            "",
            "invalid",
            // Uppercase hex is invalid per W3C Trace Context
            "00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00F067AA0BA902B7-01",
            // Version ff forbidden
            "ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            // All-zero trace-id
            "00-00000000000000000000000000000000-00f067aa0ba902b7-01",
            // All-zero parent-id
            "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01",
            // Wrong field lengths / extra segment
            "00-4bf9-00f067aa0ba902b7-01",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-extra",
        ];
        for raw in cases {
            assert_eq!(parse_traceparent(raw), None, "expected None for {raw:?}");
        }
    }

    #[test]
    fn merge_trace_headers_preserves_explicit_metadata_and_caps_size() {
        let mut meta = HashMap::from([
            (
                "traceparent".to_owned(),
                "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-1111111111111111-01".to_owned(),
            ),
            ("tracestate".to_owned(), "   ".to_owned()),
            ("custom".to_owned(), "keep".to_owned()),
        ]);
        let oversized = "x".repeat(MAX_TRACE_HEADER_BYTES + 1);
        let headers = HashMap::from([
            (
                "traceparent",
                "00-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-2222222222222222-01",
            ),
            ("tracestate", "  vendor=1  "),
            ("baggage", oversized.as_str()),
            ("x-request-id", "req-42"),
            ("x-unrelated", "ignored"),
        ]);

        merge_trace_headers_into_proto(&mut meta, |k| headers.get(k).copied());

        assert_eq!(
            meta.get("traceparent").map(String::as_str),
            Some("00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-1111111111111111-01")
        );
        assert_eq!(meta.get("tracestate").map(String::as_str), Some("vendor=1"));
        assert!(!meta.contains_key("baggage"));
        assert_eq!(meta.get("x-request-id").map(String::as_str), Some("req-42"));
        assert!(!meta.contains_key("x-unrelated"));
        assert_eq!(meta.get("custom").map(String::as_str), Some("keep"));
    }
}
