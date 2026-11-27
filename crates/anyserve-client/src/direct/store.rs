use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use sha2::{Digest as _, Sha256};
use subtle::ConstantTimeEq as _;

use super::DirectError;

type TokenDigest = [u8; 32];

fn token_digest(token: &str) -> TokenDigest {
    Sha256::digest(token.as_bytes()).into()
}

fn generate_token() -> String {
    // 128-bit OS CSPRNG capability token (32 lowercase hex chars).
    uuid::Uuid::new_v4().simple().to_string()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum EntryKind {
    Input,
    Output,
}

struct InputEntry {
    token: String,
    data: Bytes,
    sha256: String,
    fetches: u32,
    max_fetches: u32,
    in_flight: u32,
    expires_at: Instant,
}

struct OutputEntry {
    token: String,
    data: Bytes,
    sha256: String,
    in_flight: u32,
    expires_at: Instant,
    seq: u64,
}

struct StoreInner {
    inputs: HashMap<TokenDigest, InputEntry>,
    outputs: HashMap<TokenDigest, OutputEntry>,
    output_bytes: usize,
    output_seq: u64,
    output_cache_max_bytes: usize,
    max_concurrent_fetches_per_token: u32,
}

impl StoreInner {
    fn purge_expired(&mut self, now: Instant) {
        self.inputs.retain(|_, entry| entry.expires_at > now);
        let mut removed_bytes = 0usize;
        self.outputs.retain(|_, entry| {
            if entry.expires_at <= now {
                removed_bytes += entry.data.len();
                false
            } else {
                true
            }
        });
        self.output_bytes = self.output_bytes.saturating_sub(removed_bytes);
    }

    fn evict_oldest_output(&mut self) -> bool {
        let Some((oldest_key, oldest_len)) = self
            .outputs
            .iter()
            .min_by_key(|(_, entry)| entry.seq)
            .map(|(k, entry)| (*k, entry.data.len()))
        else {
            return false;
        };
        if self.outputs.remove(&oldest_key).is_some() {
            self.output_bytes = self.output_bytes.saturating_sub(oldest_len);
            true
        } else {
            false
        }
    }
}

/// Thread-safe, bounded object store shared by a [`super::DirectEndpoint`].
///
/// - Input entries are multi-fetchable across retries up to `max_fetches` and
///   revoked immediately when their [`InputLeaseGuard`] drops or TTL expires.
/// - Output entries live in a bounded LRU cache evicted on capacity overflow,
///   TTL expiration, or immediately upon peer `ACK`.
#[derive(Clone)]
pub struct DirectObjectStore {
    inner: Arc<Mutex<StoreInner>>,
}

impl DirectObjectStore {
    pub fn new(output_cache_max_bytes: usize, max_concurrent_fetches_per_token: u32) -> Self {
        Self {
            inner: Arc::new(Mutex::new(StoreInner {
                inputs: HashMap::new(),
                outputs: HashMap::new(),
                output_bytes: 0,
                output_seq: 0,
                output_cache_max_bytes,
                max_concurrent_fetches_per_token: max_concurrent_fetches_per_token.max(1),
            })),
        }
    }

    /// Stages an input object and returns `(token, InputLeaseGuard)`. Dropping
    /// the guard immediately revokes the token.
    pub fn stage_input(
        &self,
        data: Bytes,
        sha256: String,
        max_fetches: u32,
        ttl: Duration,
    ) -> (String, InputLeaseGuard) {
        let token = generate_token();
        let digest = token_digest(&token);
        let now = Instant::now();
        let mut st = self.inner.lock().expect("direct store poisoned");
        st.purge_expired(now);
        st.inputs.insert(
            digest,
            InputEntry {
                token: token.clone(),
                data,
                sha256,
                fetches: 0,
                max_fetches: max_fetches.max(1),
                in_flight: 0,
                expires_at: now + ttl,
            },
        );
        let guard = InputLeaseGuard {
            store: self.clone(),
            digest: Some(digest),
        };
        (token, guard)
    }

    /// Stages a worker output object into the post-finish bounded LRU cache,
    /// evicting expired and oldest entries as needed to stay within
    /// `output_cache_max_bytes`.
    pub fn stage_output(
        &self,
        data: Bytes,
        sha256: String,
        ttl: Duration,
    ) -> Result<String, DirectError> {
        let len = data.len();
        let now = Instant::now();
        let mut st = self.inner.lock().expect("direct store poisoned");
        st.purge_expired(now);
        if len > st.output_cache_max_bytes {
            return Err(DirectError::OutputCacheExceeded {
                size: len,
                limit: st.output_cache_max_bytes,
            });
        }
        while st.output_bytes.saturating_add(len) > st.output_cache_max_bytes {
            if !st.evict_oldest_output() {
                break;
            }
        }
        let token = generate_token();
        let digest = token_digest(&token);
        st.output_seq = st.output_seq.wrapping_add(1);
        let seq = st.output_seq;
        st.output_bytes += len;
        st.outputs.insert(
            digest,
            OutputEntry {
                token: token.clone(),
                data,
                sha256,
                in_flight: 0,
                expires_at: now + ttl,
                seq,
            },
        );
        Ok(token)
    }

    /// Checks out an input or output object by capability token using a uniform
    /// digest key + constant-time token comparison.
    pub(crate) fn checkout(&self, token: &str) -> Result<CheckedOutObject, DirectError> {
        let digest = token_digest(token);
        let now = Instant::now();
        let mut st = self.inner.lock().expect("direct store poisoned");
        let max_conc = st.max_concurrent_fetches_per_token;

        if let Some(entry) = st.inputs.get_mut(&digest) {
            if entry.expires_at <= now {
                st.inputs.remove(&digest);
                return Err(DirectError::NotFoundOrExpired);
            }
            if !bool::from(entry.token.as_bytes().ct_eq(token.as_bytes())) {
                return Err(DirectError::NotFoundOrExpired);
            }
            if entry.fetches >= entry.max_fetches {
                return Err(DirectError::FetchLimitExceeded);
            }
            if entry.in_flight >= max_conc {
                return Err(DirectError::ConcurrencyLimitExceeded);
            }
            entry.fetches += 1;
            entry.in_flight += 1;
            return Ok(CheckedOutObject {
                store: self.clone(),
                digest,
                kind: EntryKind::Input,
                data: entry.data.clone(),
                _sha256: entry.sha256.clone(),
            });
        }

        if let Some(entry) = st.outputs.get_mut(&digest) {
            if entry.expires_at <= now {
                let len = entry.data.len();
                st.outputs.remove(&digest);
                st.output_bytes = st.output_bytes.saturating_sub(len);
                return Err(DirectError::NotFoundOrExpired);
            }
            if !bool::from(entry.token.as_bytes().ct_eq(token.as_bytes())) {
                return Err(DirectError::NotFoundOrExpired);
            }
            if entry.in_flight >= max_conc {
                return Err(DirectError::ConcurrencyLimitExceeded);
            }
            entry.in_flight += 1;
            return Ok(CheckedOutObject {
                store: self.clone(),
                digest,
                kind: EntryKind::Output,
                data: entry.data.clone(),
                _sha256: entry.sha256.clone(),
            });
        }

        Err(DirectError::NotFoundOrExpired)
    }

    /// Evicts a post-finish output entry early once the client acknowledges
    /// verified receipt (`1 B ACK`).
    pub(crate) fn ack_output(&self, digest: TokenDigest) -> bool {
        let mut st = self.inner.lock().expect("direct store poisoned");
        if let Some(entry) = st.outputs.remove(&digest) {
            st.output_bytes = st.output_bytes.saturating_sub(entry.data.len());
            true
        } else {
            false
        }
    }

    /// Removes expired entries and returns `(input_count, output_count, output_bytes)`.
    pub fn stats(&self) -> (usize, usize, usize) {
        let mut st = self.inner.lock().expect("direct store poisoned");
        st.purge_expired(Instant::now());
        (st.inputs.len(), st.outputs.len(), st.output_bytes)
    }

    fn revoke_input(&self, digest: TokenDigest) {
        let mut st = self.inner.lock().expect("direct store poisoned");
        st.inputs.remove(&digest);
    }

    fn release_in_flight(&self, digest: TokenDigest, kind: EntryKind) {
        let mut st = self.inner.lock().expect("direct store poisoned");
        match kind {
            EntryKind::Input => {
                if let Some(entry) = st.inputs.get_mut(&digest) {
                    entry.in_flight = entry.in_flight.saturating_sub(1);
                }
            }
            EntryKind::Output => {
                if let Some(entry) = st.outputs.get_mut(&digest) {
                    entry.in_flight = entry.in_flight.saturating_sub(1);
                }
            }
        }
    }
}

/// RAII lease revoking a staged input token when dropped (on terminal event,
/// cancellation, or client stream disconnect).
pub struct InputLeaseGuard {
    store: DirectObjectStore,
    digest: Option<TokenDigest>,
}

impl InputLeaseGuard {
    pub fn revoke_now(&mut self) {
        if let Some(digest) = self.digest.take() {
            self.store.revoke_input(digest);
        }
    }
}

impl Drop for InputLeaseGuard {
    fn drop(&mut self) {
        self.revoke_now();
    }
}

pub(crate) struct CheckedOutObject {
    store: DirectObjectStore,
    pub(crate) digest: TokenDigest,
    pub(crate) kind: EntryKind,
    pub(crate) data: Bytes,
    _sha256: String,
}

impl Drop for CheckedOutObject {
    fn drop(&mut self) {
        self.store.release_in_flight(self.digest, self.kind);
    }
}
