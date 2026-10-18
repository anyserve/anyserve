//! Canonical fingerprints: the idempotency hash of a job specification and the
//! digest of a worker's result. Both are computed by the server only.

use sha2::{Digest, Sha256};

use super::api::{
    Durability, ErrorInfo, IDENTITY_EXCLUDED_METADATA_KEYS, JobSpec, Metadata, Object,
};

struct Canonical(Sha256);

impl Canonical {
    fn new(domain: &str) -> Self {
        let mut this = Self(Sha256::new());
        this.bytes(domain.as_bytes());
        this
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

    fn object(&mut self, object: &Object) {
        match object {
            Object::Inline { data, metadata } => {
                self.str("inline");
                self.bytes(data);
                self.map(metadata.iter());
            }
            Object::Uri { uri, metadata } => {
                self.str("uri");
                self.str(uri);
                self.map(metadata.iter());
            }
        }
    }

    fn finish(self) -> String {
        hex(&self.0.finalize())
    }
}

fn optional_ms(value: Option<std::time::Duration>) -> u64 {
    value.map(|value| value.as_millis() as u64).unwrap_or(0)
}

/// Fingerprint of everything that defines a job, except transport context.
pub(crate) fn spec_hash(spec: &JobSpec, durability: Durability) -> String {
    let mut hash = Canonical::new("anyserve.job.v1");
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
    let identity: Metadata = spec
        .metadata
        .iter()
        .filter(|(key, _)| !IDENTITY_EXCLUDED_METADATA_KEYS.contains(&key.as_str()))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    hash.map(identity.iter());
    hash.finish()
}

/// Digest of an attempt's reported outcome, used to recognise a repeated
/// completion of the same lease.
pub(crate) fn outcome_digest(outputs: &[Object], error: Option<&ErrorInfo>) -> String {
    let mut hash = Canonical::new("anyserve.result.v1");
    match error {
        None => {
            hash.str("succeeded");
            hash.u64(outputs.len() as u64);
            for output in outputs {
                hash.object(output);
            }
        }
        Some(error) => {
            hash.str("failed");
            hash.str(&error.code);
            hash.str(&error.message);
            hash.u64(error.retryable as u64);
        }
    }
    hash.finish()
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(DIGITS[(byte >> 4) as usize] as char);
        out.push(DIGITS[(byte & 0x0f) as usize] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec() -> JobSpec {
        let mut spec = JobSpec::default();
        spec.demand.interface = "llm".to_string();
        spec.inputs.push(Object::inline(&b"hello"[..]));
        spec.metadata.insert("tenant".to_string(), "a".to_string());
        spec
    }

    #[test]
    fn transport_metadata_does_not_change_identity() {
        let base = spec_hash(&spec(), Durability::Durable);
        let mut traced = spec();
        traced
            .metadata
            .insert("traceparent".to_string(), "00-abc".to_string());
        assert_eq!(base, spec_hash(&traced, Durability::Durable));
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
