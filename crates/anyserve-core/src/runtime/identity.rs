//! Canonical fingerprints: the idempotency hash of a job specification, the
//! digest of a worker's result, and the bijective `(namespace, job_id)` storage key.

use std::sync::OnceLock;

use sha2::{Digest, Sha256};

use super::api::{
    Durability, ErrorInfo, IDENTITY_EXCLUDED_METADATA_KEYS, JobSpec, OBJECT_SHA256_METADATA_KEY,
    OBJECT_SIZE_METADATA_KEY, Object,
};
use crate::auth::DEFAULT_NAMESPACE;

const SCOPED_PREFIX: &str = "@ns:";

/// Bijective mapping between `(namespace, raw_job_id)` and the internal
/// storage key so `"default"` namespace jobs keep raw IDs while other
/// namespaces are isolated across memory, SQLite, and PostgreSQL without
/// colliding or leaking job existence.
pub(crate) fn encode_scoped_id(namespace: &str, raw_job_id: &str) -> String {
    if namespace == DEFAULT_NAMESPACE && !raw_job_id.starts_with(SCOPED_PREFIX) {
        raw_job_id.to_string()
    } else {
        format!("{SCOPED_PREFIX}{namespace}:{raw_job_id}")
    }
}

pub(crate) fn decode_scoped_id(scoped_job_id: &str) -> (&str, &str) {
    if let Some(rest) = scoped_job_id.strip_prefix(SCOPED_PREFIX)
        && let Some((ns, raw)) = rest.split_once(':')
    {
        return (ns, raw);
    }
    (DEFAULT_NAMESPACE, scoped_job_id)
}

struct Canonical(Sha256);

impl Canonical {
    fn new(domain: &str) -> Self {
        let mut this = Self(Sha256::new());
        this.bytes(domain.as_bytes());
        this
    }

    fn from_prefix(prefix: &Sha256) -> Self {
        Self(prefix.clone())
    }

    fn bytes(&mut self, value: &[u8]) {
        self.0.update((value.len() as u64).to_le_bytes());
        self.0.update(value);
    }

    fn str(&mut self, value: &str) {
        self.bytes(value.as_bytes());
    }

    fn u64(&mut self, value: u64) {
        self.0.update(value.to_le_bytes());
    }

    fn map<'a>(&mut self, entries: impl ExactSizeIterator<Item = (&'a String, &'a String)>) {
        self.u64(entries.len() as u64);
        for (key, value) in entries {
            self.str(key);
            self.str(value);
        }
    }

    fn filtered_map<'a, F>(
        &mut self,
        entries: impl Iterator<Item = (&'a String, &'a String)> + Clone,
        include: F,
    ) where
        F: Fn(&str) -> bool,
    {
        let count = entries.clone().filter(|(k, _)| include(k.as_str())).count();
        self.u64(count as u64);
        for (key, value) in entries.filter(|(k, _)| include(k.as_str())) {
            self.str(key);
            self.str(value);
        }
    }

    fn object(&mut self, object: &Object) {
        match object {
            Object::Inline { data, metadata } => {
                self.str("inline");
                self.bytes(data);
                self.map(metadata.iter());
            }
            Object::Uri { uri, metadata } => {
                if let Some(digest) = metadata
                    .get(OBJECT_SHA256_METADATA_KEY)
                    .filter(|d| !d.is_empty())
                {
                    self.str("uri_digest");
                    self.str(digest);
                    self.str(
                        metadata
                            .get(OBJECT_SIZE_METADATA_KEY)
                            .map(String::as_str)
                            .unwrap_or(""),
                    );
                    self.filtered_map(metadata.iter(), |key| {
                        key != OBJECT_SHA256_METADATA_KEY && key != OBJECT_SIZE_METADATA_KEY
                    });
                } else {
                    self.str("uri");
                    self.str(uri);
                    self.map(metadata.iter());
                }
            }
        }
    }

    fn finish(self) -> String {
        hex(&self.0.finalize())
    }
}

fn spec_prefix() -> &'static Sha256 {
    static PREFIX: OnceLock<Sha256> = OnceLock::new();
    PREFIX.get_or_init(|| Canonical::new("anyserve.job.v1").0)
}

fn result_succeeded_prefix() -> &'static Sha256 {
    static PREFIX: OnceLock<Sha256> = OnceLock::new();
    PREFIX.get_or_init(|| {
        let mut c = Canonical::new("anyserve.result.v1");
        c.str("succeeded");
        c.0
    })
}

/// Pre-hashes the 65-byte prefix `("anyserve.result.v1", "succeeded", 1, "inline")`,
/// completing the first 64-byte SHA-256 block once at startup for the common
/// single-inline-output completion path.
fn result_succeeded_one_inline_prefix() -> &'static Sha256 {
    static PREFIX: OnceLock<Sha256> = OnceLock::new();
    PREFIX.get_or_init(|| {
        let mut c = Canonical::from_prefix(result_succeeded_prefix());
        c.u64(1);
        c.str("inline");
        c.0
    })
}

fn result_failed_prefix() -> &'static Sha256 {
    static PREFIX: OnceLock<Sha256> = OnceLock::new();
    PREFIX.get_or_init(|| {
        let mut c = Canonical::new("anyserve.result.v1");
        c.str("failed");
        c.0
    })
}

fn optional_ms(value: Option<std::time::Duration>) -> u64 {
    value.map(|value| value.as_millis() as u64).unwrap_or(0)
}

/// Fingerprint of everything that defines a job, except transport context and
/// scheduling hints (`affinity_key`, `fairness_key`, `concurrency_key`).
pub(crate) fn spec_hash(spec: &JobSpec, durability: Durability) -> String {
    let mut hash = Canonical::from_prefix(spec_prefix());
    let demand = &spec.demand;
    hash.str(&demand.interface);
    hash.map(demand.required_attributes.iter());
    hash.map(demand.preferred_attributes.iter());
    hash.u64(demand.required_capacity.len() as u64);
    for (name, value) in &demand.required_capacity {
        hash.str(name);
        hash.u64(*value as u64);
    }
    hash.u64(spec.inputs.len() as u64);
    for input in &spec.inputs {
        hash.object(input);
    }
    hash.bytes(&spec.params);
    let policy = &spec.policy;
    hash.str(match durability {
        Durability::Ephemeral => "ephemeral",
        Durability::Durable => "durable",
    });
    hash.u64(policy.priority as u64);
    hash.u64(optional_ms(policy.queue_timeout));
    hash.u64(optional_ms(policy.execution_timeout));
    hash.u64(optional_ms(policy.total_timeout));
    hash.u64(optional_ms(policy.spillover_after));
    hash.u64(policy.max_attempts.unwrap_or(0) as u64);
    hash.filtered_map(spec.metadata.iter(), |key| {
        !IDENTITY_EXCLUDED_METADATA_KEYS.contains(&key)
    });
    hash.finish()
}

/// Digest of an attempt's reported outcome, used to recognise a repeated
/// completion of the same lease.
pub(crate) fn outcome_digest(outputs: &[Object], error: Option<&ErrorInfo>) -> String {
    match error {
        None => {
            if let [Object::Inline { data, metadata }] = outputs {
                let mut hash = Canonical::from_prefix(result_succeeded_one_inline_prefix());
                hash.bytes(data);
                hash.map(metadata.iter());
                return hash.finish();
            }
            let mut hash = Canonical::from_prefix(result_succeeded_prefix());
            hash.u64(outputs.len() as u64);
            for output in outputs {
                hash.object(output);
            }
            hash.finish()
        }
        Some(error) => {
            let mut hash = Canonical::from_prefix(result_failed_prefix());
            hash.str(&error.code);
            hash.str(&error.message);
            hash.u64(error.retryable as u64);
            hash.finish()
        }
    }
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = Vec::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        out.push(DIGITS[(byte >> 4) as usize]);
        out.push(DIGITS[(byte & 0x0f) as usize]);
    }
    // SAFETY: `DIGITS` contains only valid ASCII hex bytes.
    String::from_utf8(out).expect("hex digits are ASCII")
}

#[cfg(test)]
mod tests {
    use super::super::api::Metadata;
    use super::*;

    fn spec() -> JobSpec {
        let mut spec = JobSpec::default();
        spec.demand.interface = "llm".to_string();
        spec.inputs.push(Object::inline(&b"hello"[..]));
        spec.metadata.insert("tenant".to_string(), "a".to_string());
        spec
    }

    #[test]
    fn transport_metadata_and_scheduling_hints_do_not_change_identity() {
        let base = spec_hash(&spec(), Durability::Durable);
        let mut traced = spec();
        traced
            .metadata
            .insert("traceparent".to_string(), "00-abc".to_string());
        traced.demand.affinity_key = "robot-cam-1".to_string();
        traced.policy.fairness_key = "fleet-a".to_string();
        traced.policy.concurrency_key = "gpu-group".to_string();
        assert_eq!(base, spec_hash(&traced, Durability::Durable));
    }

    #[test]
    fn uri_with_content_sha256_hashes_digest_not_transient_uri() {
        let mut a = spec();
        a.inputs = vec![Object::Uri {
            uri: "s3://bucket-1/tmp/upload-123".into(),
            metadata: Metadata::from([
                (OBJECT_SHA256_METADATA_KEY.into(), "deadbeef".into()),
                (OBJECT_SIZE_METADATA_KEY.into(), "1024".into()),
                ("mime".into(), "video/mp4".into()),
            ]),
        }];
        let mut b = a.clone();
        b.inputs = vec![Object::Uri {
            uri: "s3://bucket-1/tmp/upload-999?X-Amz-Signature=xyz".into(),
            metadata: Metadata::from([
                (OBJECT_SHA256_METADATA_KEY.into(), "deadbeef".into()),
                (OBJECT_SIZE_METADATA_KEY.into(), "1024".into()),
                ("mime".into(), "video/mp4".into()),
            ]),
        }];
        assert_eq!(
            spec_hash(&a, Durability::Durable),
            spec_hash(&b, Durability::Durable)
        );
    }

    #[test]
    fn scoped_id_roundtrip_is_bijective() {
        for (ns, raw) in [
            ("default", "job-1"),
            ("default", "@ns:weird:id"),
            ("robot-a", "job-1"),
            ("robot-b", "job-1"),
            ("tenant.prod", "a:b:c"),
        ] {
            let encoded = encode_scoped_id(ns, raw);
            assert_eq!(decode_scoped_id(&encoded), (ns, raw));
        }
        assert_ne!(
            encode_scoped_id("robot-a", "job-1"),
            encode_scoped_id("robot-b", "job-1")
        );
    }

    #[test]
    fn business_metadata_and_inputs_change_identity() {
        let base = spec_hash(&spec(), Durability::Durable);
        let mut other = spec();
        other.metadata.insert("tenant".to_string(), "b".to_string());
        assert_ne!(base, spec_hash(&other, Durability::Durable));
        let mut input_meta = spec();
        if let Object::Inline { metadata, .. } = &mut input_meta.inputs[0] {
            metadata.insert("content-type".to_string(), "text/plain".to_string());
        }
        assert_ne!(base, spec_hash(&input_meta, Durability::Durable));
        assert_ne!(base, spec_hash(&spec(), Durability::Ephemeral));
    }

    #[test]
    fn digest_distinguishes_outcomes() {
        let ok = outcome_digest(&[Object::inline(&b"a"[..])], None);
        assert_eq!(ok, outcome_digest(&[Object::inline(&b"a"[..])], None));
        assert_ne!(ok, outcome_digest(&[Object::inline(&b"b"[..])], None));
        let failed = ErrorInfo::new("x", "y", false);
        assert_ne!(ok, outcome_digest(&[], Some(&failed)));
    }
}
