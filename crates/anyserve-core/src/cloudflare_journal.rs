//! Cloudflare Durable Object SQLite + R2 journal backend.
//!
//! Only the active leader holding the Durable Object's `scheduler_lease`
//! mutates durable state. Writes are grouped by a single background writer task
//! and committed via `POST /v1/commit_batch`. Payloads exceeding the DO SQLite
//! inline threshold (1 MiB by default) are spilled to R2 by the Durable Object
//! side while keeping the `JournalBackend` contract identical to `SqliteJournal`
//! and `PgJournal`.

use std::collections::{BTreeSet, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use bytes::Bytes;
use reqwest::StatusCode;
use reqwest::header::{AUTHORIZATION, HeaderMap, HeaderValue};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot, watch};

use crate::error::RuntimeError;
use crate::journal::{
    BoxFuture, CommitOutcome, JournalBackend, JournalOp, Leadership, LeaseHandle, Recovery,
    StoredJob, StoredJobPreview, StoredSpec, StoredSpecRef, decode_worker_records, state_text,
};
use crate::runtime::api::{
    CloudflareJournalConfig, INSPECTION_PREVIEW_BYTES, JobSpec, JobState, JournalConfig,
    JournalStats, Metadata, Object, ObjectPreview,
};
use crate::runtime::events::{ByteBudget, Permit, budgeted_bytes};
use crate::runtime::identity::{decode_scoped_id, encode_scoped_id};

const PURGE_BATCH: usize = 500;
const DEFAULT_HTTP_TIMEOUT: Duration = Duration::from_secs(30);

enum WriteRequest {
    Op {
        term: u64,
        op: JournalOp,
        reply: oneshot::Sender<CommitOutcome>,
    },
    Close {
        reply: oneshot::Sender<()>,
    },
}

type ActiveTermSender = Mutex<Option<(u64, watch::Sender<bool>)>>;

/// Single-leader journal backend talking to a Cloudflare Durable Object SQLite + R2 gateway.
pub(crate) struct CloudflareJournal {
    client: reqwest::Client,
    endpoint: String,
    config: CloudflareJournalConfig,
    holder_id: String,
    tx: mpsc::Sender<WriteRequest>,
    pending: Arc<AtomicUsize>,
    stats: Arc<Mutex<JournalStats>>,
    active_lost_tx: Arc<ActiveTermSender>,
    #[cfg(test)]
    pause_renewal: Arc<std::sync::atomic::AtomicBool>,
    #[cfg(test)]
    active_stop_renew_tx: Arc<Mutex<Option<watch::Sender<bool>>>>,
}

impl CloudflareJournal {
    pub(crate) async fn open(journal: &JournalConfig) -> Result<Arc<dyn JournalBackend>> {
        Ok(Self::open_concrete(journal).await?)
    }

    pub(crate) async fn open_concrete(journal: &JournalConfig) -> Result<Arc<Self>> {
        if journal.postgres.is_some() {
            bail!("cannot configure both Cloudflare and PostgreSQL journal backends");
        }
        let config = journal
            .cloudflare
            .as_ref()
            .context("Cloudflare journal config missing")?;
        let endpoint = normalize_endpoint(&config.endpoint)?;
        let lease_ms = u64::try_from(config.lease_duration.as_millis()).unwrap_or(u64::MAX);
        let poll_ms = u64::try_from(config.poll_interval.as_millis()).unwrap_or(u64::MAX);
        if !(50..=300_000).contains(&lease_ms)
            || poll_ms < 10
            || poll_ms > lease_ms / 2
            || journal.queue_capacity == 0
            || journal.max_batch == 0
            || !journal.full_sync
            || journal.max_bytes.is_some()
        {
            bail!(
                "invalid Cloudflare lease/poll/queue configuration; full_sync must be true and max_bytes is SQLite-only"
            );
        }

        let mut headers = HeaderMap::new();
        if let Some(token) = config
            .auth_token
            .as_deref()
            .map(str::trim)
            .filter(|t| !t.is_empty())
        {
            let mut value = HeaderValue::from_str(&format!("Bearer {token}"))
                .context("invalid Cloudflare journal auth_token")?;
            value.set_sensitive(true);
            headers.insert(AUTHORIZATION, value);
        }
        let client = reqwest::Client::builder()
            .default_headers(headers)
            .timeout(DEFAULT_HTTP_TIMEOUT)
            .build()
            .context("build Cloudflare journal HTTP client")?;

        let holder_id = uuid::Uuid::new_v4().to_string();
        let (tx, rx) = mpsc::channel(journal.queue_capacity.max(1));
        let pending = Arc::new(AtomicUsize::new(0));
        let stats = Arc::new(Mutex::new(JournalStats::default()));
        let active_lost_tx = Arc::new(Mutex::new(None));
        #[cfg(test)]
        let pause_renewal = Arc::new(std::sync::atomic::AtomicBool::new(false));
        #[cfg(test)]
        let active_stop_renew_tx = Arc::new(Mutex::new(None));

        tokio::spawn(writer_loop(
            client.clone(),
            endpoint.clone(),
            holder_id.clone(),
            rx,
            journal.max_batch.max(1),
            Arc::clone(&pending),
            Arc::clone(&stats),
            Arc::clone(&active_lost_tx),
        ));

        Ok(Arc::new(Self {
            client,
            endpoint,
            config: config.clone(),
            holder_id,
            tx,
            pending,
            stats,
            active_lost_tx,
            #[cfg(test)]
            pause_renewal,
            #[cfg(test)]
            active_stop_renew_tx,
        }))
    }

    #[cfg(test)]
    pub(crate) fn set_pause_renewal(&self, paused: bool) {
        self.pause_renewal.store(paused, Ordering::SeqCst);
    }

    #[cfg(test)]
    pub(crate) async fn abort_without_relinquish(&self) {
        self.pause_renewal.store(true, Ordering::SeqCst);
        if let Some(tx) = self
            .active_stop_renew_tx
            .lock()
            .expect("stop renew slot poisoned")
            .take()
        {
            tx.send_replace(true);
        }
        self.close().await;
    }
}

impl JournalBackend for CloudflareJournal {
    fn try_acquire_leadership(&self) -> BoxFuture<'_, Result<Option<Leadership>>> {
        Box::pin(async move {
            let lease_ms =
                u64::try_from(self.config.lease_duration.as_millis()).unwrap_or(u64::MAX);
            let started = Instant::now();
            let acquire_timeout = self
                .config
                .lease_duration
                .clamp(Duration::from_millis(500), Duration::from_secs(15));
            let resp: AcquireResponse = post_json(
                &self.client,
                &self.endpoint,
                "acquire",
                &AcquireRequest {
                    holder_id: &self.holder_id,
                    lease_duration_ms: lease_ms,
                },
                Some(acquire_timeout),
            )
            .await?;

            if !resp.acquired {
                return Ok(None);
            }
            let term = resp
                .term
                .context("missing term in /v1/acquire response when acquired=true")?;

            if started.elapsed() >= self.config.lease_duration {
                let _ = send_relinquish(&self.client, &self.endpoint, &self.holder_id, term).await;
                return Ok(None);
            }

            let active = resp
                .active
                .into_iter()
                .map(|job| job.into_stored_job(true, false))
                .collect::<Result<Vec<_>>>()?;
            let recovery = Recovery {
                generation: term,
                active,
                terminal: Vec::new(),
                workers: decode_worker_records(resp.workers),
            };

            let (lost_tx, lost_rx) = watch::channel(false);
            *self
                .active_lost_tx
                .lock()
                .expect("active lost sender poisoned") = Some((term, lost_tx.clone()));

            let (stop_renew_tx, stop_renew_rx) = watch::channel(false);
            #[cfg(test)]
            {
                *self
                    .active_stop_renew_tx
                    .lock()
                    .expect("stop renew slot poisoned") = Some(stop_renew_tx.clone());
            }

            tokio::spawn(renew_lease_loop(
                self.client.clone(),
                self.endpoint.clone(),
                self.holder_id.clone(),
                term,
                self.config.lease_duration,
                started + self.config.lease_duration,
                lost_tx,
                stop_renew_rx,
                #[cfg(test)]
                Arc::clone(&self.pause_renewal),
            ));

            let client = self.client.clone();
            let endpoint = self.endpoint.clone();
            let holder_id = self.holder_id.clone();
            let lease = LeaseHandle::new(lost_rx, move || {
                Box::pin(async move {
                    stop_renew_tx.send_replace(true);
                    let _ = send_relinquish(&client, &endpoint, &holder_id, term).await;
                })
            });

            Ok(Some(Leadership {
                term,
                recovery,
                lease,
            }))
        })
    }

    fn standby_poll_interval(&self) -> Duration {
        self.config.poll_interval
    }

    fn enqueue(
        &self,
        term: u64,
        op: JournalOp,
    ) -> Result<oneshot::Receiver<CommitOutcome>, RuntimeError> {
        let (reply, receiver) = oneshot::channel();
        self.pending.fetch_add(1, Ordering::Relaxed);
        match self.tx.try_send(WriteRequest::Op { term, op, reply }) {
            Ok(()) => Ok(receiver),
            Err(err) => {
                self.pending.fetch_sub(1, Ordering::Relaxed);
                Err(match err {
                    mpsc::error::TrySendError::Full(_) => {
                        RuntimeError::exhausted("Cloudflare journal commit queue is full")
                    }
                    mpsc::error::TrySendError::Closed(_) => {
                        RuntimeError::unavailable("Cloudflare journal is closed")
                    }
                })
            }
        }
    }

    fn spec_hash<'a>(&'a self, job_id: &'a str) -> BoxFuture<'a, Result<Option<String>>> {
        Box::pin(async move {
            let resp: SpecHashResponse = post_json(
                &self.client,
                &self.endpoint,
                "spec_hash",
                &SpecHashRequest { job_id },
                None,
            )
            .await?;
            Ok(resp.spec_hash)
        })
    }

    fn receipt<'a>(&'a self, lease_id: &'a str) -> BoxFuture<'a, Result<Option<(String, String)>>> {
        Box::pin(async move {
            let resp: ReceiptResponse = post_json(
                &self.client,
                &self.endpoint,
                "receipt",
                &ReceiptRequest { lease_id },
                None,
            )
            .await?;
            Ok(resp.receipt.map(|r| (r.job_id, r.result_digest)))
        })
    }

    fn load_job<'a>(
        &'a self,
        job_id: &'a str,
        include_outputs: bool,
        result_budget: Arc<ByteBudget>,
    ) -> BoxFuture<'a, Result<Option<StoredJob>>> {
        Box::pin(async move {
            let resp: LoadJobResponse = post_json(
                &self.client,
                &self.endpoint,
                "load_job",
                &LoadJobRequest {
                    job_id,
                    include_inputs: false,
                    include_outputs,
                    max_output_bytes: None,
                },
                None,
            )
            .await?;
            if resp.budget_exceeded {
                return Err(RuntimeError::exhausted(
                    "result budget is exhausted; retry the read later",
                )
                .into());
            }
            let Some(wire_job) = resp.job else {
                return Ok(None);
            };
            let mut job = wire_job.into_stored_job(false, include_outputs)?;
            if !include_outputs || job.state != JobState::Succeeded {
                job.outputs.clear();
                return Ok(Some(job));
            }
            let total_bytes: usize = job
                .outputs
                .iter()
                .filter_map(|obj| match obj {
                    Object::Inline { data, .. } => Some(data.len()),
                    Object::Uri { .. } => None,
                })
                .sum();
            let mut permit = Permit::new(result_budget);
            if !permit.grow(total_bytes) {
                return Err(RuntimeError::exhausted(
                    "result budget is exhausted; retry the read later",
                )
                .into());
            }
            for obj in &mut job.outputs {
                if let Object::Inline { data, .. } = obj {
                    let reserved = permit.split(data.len());
                    *data = budgeted_bytes(std::mem::take(data), reserved);
                }
            }
            Ok(Some(job))
        })
    }

    fn load_job_preview<'a>(
        &'a self,
        job_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<StoredJobPreview>>> {
        Box::pin(async move {
            let resp: LoadJobPreviewResponse = post_json(
                &self.client,
                &self.endpoint,
                "load_job_preview",
                &LoadJobPreviewRequest {
                    job_id,
                    preview_bytes: INSPECTION_PREVIEW_BYTES,
                },
                None,
            )
            .await?;
            let Some(wire_job) = resp.job else {
                return Ok(None);
            };
            let job = wire_job.into_stored_job(false, false)?;
            let previews = if job.state == JobState::Succeeded {
                resp.output_previews
                    .into_iter()
                    .map(WireObjectPreview::into_preview)
                    .collect::<Result<Vec<_>>>()?
            } else {
                Vec::new()
            };
            Ok(Some((job, previews)))
        })
    }

    fn scan_jobs<'a>(
        &'a self,
        from_ms: u64,
        to_ms: u64,
        before: Option<(u64, String)>,
        namespace: Option<&'a str>,
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<StoredJob>>> {
        Box::pin(async move {
            let before = before.map(|(created_at_ms, id)| WireScanCursor {
                created_at_ms,
                job_id: match namespace {
                    Some(ns) => encode_scoped_id(ns, &id),
                    None => id,
                },
            });
            let resp: ScanJobsResponse = post_json(
                &self.client,
                &self.endpoint,
                "scan_jobs",
                &ScanJobsRequest {
                    from_ms,
                    to_ms,
                    before,
                    namespace,
                    limit: limit.max(1),
                },
                None,
            )
            .await?;
            resp.jobs
                .into_iter()
                .map(|job| job.into_stored_job(false, false))
                .collect()
        })
    }

    fn purge<'a>(&'a self, term: u64, before_ms: u64) -> BoxFuture<'a, Result<usize>> {
        Box::pin(async move {
            let mut total = 0usize;
            loop {
                let resp: PurgeResponse = post_json(
                    &self.client,
                    &self.endpoint,
                    "purge",
                    &PurgeRequest {
                        holder_id: &self.holder_id,
                        term,
                        before_ms,
                        batch_limit: PURGE_BATCH,
                    },
                    None,
                )
                .await?;
                if resp.fenced {
                    signal_term_lost(&self.active_lost_tx, term);
                    bail!("Cloudflare scheduler lease lost during purge");
                }
                total += resp.deleted;
                if resp.deleted < PURGE_BATCH {
                    return Ok(total);
                }
            }
        })
    }

    fn stats(&self) -> JournalStats {
        let mut stats = self
            .stats
            .lock()
            .expect("Cloudflare journal stats poisoned")
            .clone();
        stats.pending_ops = self.pending.load(Ordering::Relaxed);
        stats
    }

    fn close(&self) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            let (reply, receiver) = oneshot::channel();
            if self.tx.send(WriteRequest::Close { reply }).await.is_ok() {
                let _ = tokio::time::timeout(Duration::from_secs(5), receiver).await;
            }
        })
    }
}

fn normalize_endpoint(raw: &str) -> Result<String> {
    let trimmed = raw.trim().trim_end_matches('/');
    if trimmed.is_empty() {
        bail!("Cloudflare journal endpoint must not be empty");
    }
    let uri: http::Uri = trimmed
        .parse()
        .context("invalid Cloudflare journal endpoint URI")?;
    let scheme = uri
        .scheme_str()
        .ok_or_else(|| anyhow!("Cloudflare journal endpoint must include http:// or https://"))?;
    if !matches!(scheme, "http" | "https") || uri.authority().is_none() {
        bail!("Cloudflare journal endpoint must be an http:// or https:// URL with authority");
    }
    Ok(trimmed.to_string())
}

fn signal_term_lost(slot: &ActiveTermSender, term: u64) {
    if let Some((active_term, tx)) = slot.lock().expect("lost slot poisoned").as_ref()
        && *active_term == term
    {
        tx.send_replace(true);
    }
}

async fn send_relinquish(
    client: &reqwest::Client,
    endpoint: &str,
    holder_id: &str,
    term: u64,
) -> Result<bool> {
    let resp: RelinquishResponse = post_json(
        client,
        endpoint,
        "relinquish",
        &RelinquishRequest { holder_id, term },
        Some(Duration::from_secs(2)),
    )
    .await?;
    Ok(resp.ok)
}

#[derive(Debug)]
struct HttpFailure {
    status: Option<StatusCode>,
    message: String,
}

impl HttpFailure {
    fn is_auth_or_client(&self) -> bool {
        self.status
            .is_some_and(|code| matches!(code, StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN))
    }
}

async fn post_json<Req: Serialize + ?Sized, Resp: for<'de> Deserialize<'de>>(
    client: &reqwest::Client,
    endpoint: &str,
    op: &str,
    body: &Req,
    timeout: Option<Duration>,
) -> Result<Resp> {
    post_json_detailed(client, endpoint, op, body, timeout)
        .await
        .map_err(|err| anyhow!(err.message))
}

async fn post_json_detailed<Req: Serialize + ?Sized, Resp: for<'de> Deserialize<'de>>(
    client: &reqwest::Client,
    endpoint: &str,
    op: &str,
    body: &Req,
    timeout: Option<Duration>,
) -> std::result::Result<Resp, HttpFailure> {
    let url = format!("{endpoint}/v1/{op}");
    let mut req = client.post(&url).json(body);
    if let Some(t) = timeout {
        req = req.timeout(t);
    }
    let response = req.send().await.map_err(|err| HttpFailure {
        status: err.status(),
        message: format!("POST /v1/{op} transport error: {err}"),
    })?;
    let status = response.status();
    if !status.is_success() {
        let text = response.text().await.unwrap_or_default();
        return Err(HttpFailure {
            status: Some(status),
            message: format!("POST /v1/{op} returned HTTP {status}: {text}"),
        });
    }
    response.json::<Resp>().await.map_err(|err| HttpFailure {
        status: Some(status),
        message: format!("decode /v1/{op} response JSON: {err}"),
    })
}

#[allow(clippy::too_many_arguments)]
async fn renew_lease_loop(
    client: reqwest::Client,
    endpoint: String,
    holder_id: String,
    term: u64,
    lease_duration: Duration,
    mut confirmed_until: Instant,
    lost_tx: watch::Sender<bool>,
    mut stop: watch::Receiver<bool>,
    #[cfg(test)] pause_renewal: Arc<std::sync::atomic::AtomicBool>,
) {
    let renew_every = Duration::from_millis((lease_duration.as_millis() as u64 * 3 / 10).max(10));
    let lease_ms = u64::try_from(lease_duration.as_millis()).unwrap_or(u64::MAX);
    let mut consecutive_failures: u32 = 0;

    loop {
        #[cfg(test)]
        if pause_renewal.load(Ordering::Acquire) {
            tokio::select! {
                res = stop.changed() => {
                    if res.is_err() || *stop.borrow() {
                        return;
                    }
                }
                _ = tokio::time::sleep(Duration::from_millis(20)) => {}
            }
            continue;
        }
        let sleep_until = confirmed_until.min(Instant::now() + renew_every);
        tokio::select! {
            res = stop.changed() => {
                if res.is_err() || *stop.borrow() {
                    return;
                }
            }
            _ = tokio::time::sleep_until(sleep_until.into()) => {}
        }
        if *stop.borrow() {
            return;
        }
        if Instant::now() >= confirmed_until {
            lost_tx.send_replace(true);
            return;
        }

        loop {
            if *stop.borrow() {
                return;
            }
            let now = Instant::now();
            if now >= confirmed_until {
                lost_tx.send_replace(true);
                return;
            }
            let remaining = confirmed_until.saturating_duration_since(now);
            let req_timeout = remaining.min(renew_every.max(Duration::from_millis(250)));
            let res = post_json_detailed::<RenewRequest, RenewResponse>(
                &client,
                &endpoint,
                "renew",
                &RenewRequest {
                    holder_id: &holder_id,
                    term,
                    lease_duration_ms: lease_ms,
                },
                Some(req_timeout),
            )
            .await;

            match res {
                Ok(RenewResponse { ok: true }) if Instant::now() < confirmed_until => {
                    confirmed_until = now + lease_duration;
                    consecutive_failures = 0;
                    break;
                }
                Ok(_) => {
                    // Explicitly lost or superseded on the Durable Object, or renewed past deadline.
                    lost_tx.send_replace(true);
                    return;
                }
                Err(err) if err.is_auth_or_client() => {
                    tracing::error!(error = %err.message, "lease renewal rejected");
                    lost_tx.send_replace(true);
                    return;
                }
                Err(err) => {
                    consecutive_failures = consecutive_failures.saturating_add(1);
                    let now = Instant::now();
                    if now >= confirmed_until {
                        tracing::error!(
                            consecutive_failures,
                            error = %err.message,
                            "lease renewal failed past lease expiration"
                        );
                        lost_tx.send_replace(true);
                        return;
                    }
                    tracing::warn!(
                        consecutive_failures,
                        error = %err.message,
                        "lease renewal failed; retrying before deadline"
                    );
                    let retry_delay = confirmed_until
                        .saturating_duration_since(now)
                        .min(Duration::from_millis(50));
                    tokio::select! {
                        res = stop.changed() => {
                            if res.is_err() || *stop.borrow() {
                                return;
                            }
                        }
                        _ = tokio::time::sleep(retry_delay) => {}
                    }
                }
            }
        }
    }
}

fn op_job_id(op: &JournalOp) -> Option<&str> {
    match op {
        JournalOp::Accept(r) => Some(&r.job_id),
        JournalOp::AttemptStart(r) => Some(&r.job_id),
        JournalOp::Terminal(r) => Some(&r.job_id),
        JournalOp::Probe | JournalOp::WorkerUpsert(_) | JournalOp::WorkerForget { .. } => None,
    }
}

fn drain_op_batch(
    first_term: u64,
    first_op: JournalOp,
    first_reply: oneshot::Sender<CommitOutcome>,
    rx: &mut mpsc::Receiver<WriteRequest>,
    max_batch: usize,
) -> (
    Vec<(JournalOp, oneshot::Sender<CommitOutcome>)>,
    Option<WriteRequest>,
) {
    let mut batch = vec![(first_op, first_reply)];
    let mut seen_ids: Option<HashSet<String>> = None;
    let mut deferred = None;
    while batch.len() < max_batch {
        match rx.try_recv() {
            Ok(WriteRequest::Op {
                term: next_term,
                op: next_op,
                reply: next_reply,
            }) => {
                let seen = seen_ids.get_or_insert_with(|| {
                    op_job_id(&batch[0].0)
                        .map(str::to_string)
                        .into_iter()
                        .collect()
                });
                let duplicate = op_job_id(&next_op).is_some_and(|id| seen.contains(id));
                if next_term != first_term || duplicate {
                    deferred = Some(WriteRequest::Op {
                        term: next_term,
                        op: next_op,
                        reply: next_reply,
                    });
                    break;
                }
                if let Some(id) = op_job_id(&next_op) {
                    seen.insert(id.to_string());
                }
                batch.push((next_op, next_reply));
            }
            Ok(other) => {
                deferred = Some(other);
                break;
            }
            Err(_) => break,
        }
    }
    (batch, deferred)
}

#[allow(clippy::too_many_arguments)]
async fn flush_write_batch(
    client: &reqwest::Client,
    endpoint: &str,
    holder_id: &str,
    term: u64,
    batch: Vec<(JournalOp, oneshot::Sender<CommitOutcome>)>,
    pending: &AtomicUsize,
    stats: &Mutex<JournalStats>,
    active_lost_tx: &ActiveTermSender,
) {
    let started = Instant::now();
    let ops: Vec<&JournalOp> = batch.iter().map(|(op, _)| op).collect();
    let outcomes = commit_batch(client, endpoint, holder_id, term, &ops, active_lost_tx).await;
    drop(ops);
    let elapsed = started.elapsed();
    let batch_len = batch.len();
    let committed = match &outcomes {
        None => batch_len as u64,
        Some(v) => v.iter().filter(|o| **o == CommitOutcome::Committed).count() as u64,
    };
    {
        let mut st = stats.lock().expect("Cloudflare stats poisoned");
        st.committed_batches += 1;
        st.committed_ops += committed;
        st.last_batch_size = batch_len;
        st.last_commit_micros = elapsed.as_micros() as u64;
    }
    pending.fetch_sub(batch_len, Ordering::Relaxed);
    match outcomes {
        None => {
            for (_, reply) in batch {
                let _ = reply.send(CommitOutcome::Committed);
            }
        }
        Some(outcomes) => {
            for ((_, reply), outcome) in batch.into_iter().zip(outcomes) {
                let _ = reply.send(outcome);
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn writer_loop(
    client: reqwest::Client,
    endpoint: String,
    holder_id: String,
    mut rx: mpsc::Receiver<WriteRequest>,
    max_batch: usize,
    pending: Arc<AtomicUsize>,
    stats: Arc<Mutex<JournalStats>>,
    active_lost_tx: Arc<ActiveTermSender>,
) {
    let mut deferred: Option<WriteRequest> = None;
    loop {
        let first = match deferred.take() {
            Some(req) => req,
            None => match rx.recv().await {
                Some(req) => req,
                None => break,
            },
        };
        match first {
            WriteRequest::Close { reply } => {
                let mut tail = deferred.take().or_else(|| rx.try_recv().ok());
                while let Some(req) = tail.take() {
                    if let WriteRequest::Op { term, op, reply } = req {
                        let (batch, next_deferred) =
                            drain_op_batch(term, op, reply, &mut rx, max_batch);
                        flush_write_batch(
                            &client,
                            &endpoint,
                            &holder_id,
                            term,
                            batch,
                            &pending,
                            &stats,
                            &active_lost_tx,
                        )
                        .await;
                        tail = next_deferred.or_else(|| rx.try_recv().ok());
                    } else {
                        tail = rx.try_recv().ok();
                    }
                }
                rx.close();
                let _ = reply.send(());
                return;
            }
            WriteRequest::Op { term, op, reply } => {
                let (batch, next_deferred) = drain_op_batch(term, op, reply, &mut rx, max_batch);
                deferred = next_deferred;
                flush_write_batch(
                    &client,
                    &endpoint,
                    &holder_id,
                    term,
                    batch,
                    &pending,
                    &stats,
                    &active_lost_tx,
                )
                .await;
            }
        }
    }
}

enum CommitHttpResult {
    Response(CommitBatchResponse),
    Rejected(String),
    Ambiguous(String),
}

async fn send_commit_batch(
    client: &reqwest::Client,
    endpoint: &str,
    holder_id: &str,
    term: u64,
    ops: &[&JournalOp],
) -> CommitHttpResult {
    let wire_ops = match ops
        .iter()
        .map(|op| WireCommitOp::try_from(*op))
        .collect::<Result<Vec<_>>>()
    {
        Ok(v) => v,
        Err(err) => return CommitHttpResult::Rejected(format!("encode commit op: {err:#}")),
    };

    let url = format!("{endpoint}/v1/commit_batch");
    let response = match client
        .post(&url)
        .json(&CommitBatchRequest {
            holder_id,
            term,
            ops: wire_ops,
        })
        .send()
        .await
    {
        Ok(resp) => resp,
        Err(err) => {
            return CommitHttpResult::Ambiguous(format!(
                "POST /v1/commit_batch transport error: {err}"
            ));
        }
    };

    let status = response.status();
    if status.is_success() {
        return match response.json::<CommitBatchResponse>().await {
            Ok(parsed) => CommitHttpResult::Response(parsed),
            Err(err) => {
                CommitHttpResult::Ambiguous(format!("decode /v1/commit_batch response JSON: {err}"))
            }
        };
    }

    let text = response.text().await.unwrap_or_default();
    if status.is_client_error() {
        CommitHttpResult::Rejected(format!("POST /v1/commit_batch HTTP {status}: {text}"))
    } else {
        CommitHttpResult::Ambiguous(format!("POST /v1/commit_batch HTTP {status}: {text}"))
    }
}

async fn verify_op(client: &reqwest::Client, endpoint: &str, op: &JournalOp) -> Result<bool> {
    match op {
        JournalOp::Accept(r) => {
            let resp: SpecHashResponse = post_json(
                client,
                endpoint,
                "spec_hash",
                &SpecHashRequest { job_id: &r.job_id },
                Some(Duration::from_secs(5)),
            )
            .await?;
            Ok(resp.spec_hash.is_some())
        }
        JournalOp::AttemptStart(r) => {
            let resp: LoadJobResponse = post_json(
                client,
                endpoint,
                "load_job",
                &LoadJobRequest {
                    job_id: &r.job_id,
                    include_inputs: false,
                    include_outputs: false,
                    max_output_bytes: None,
                },
                Some(Duration::from_secs(5)),
            )
            .await?;
            Ok(resp
                .job
                .is_some_and(|job| job.attempt_count >= r.attempt_number))
        }
        JournalOp::Terminal(r) => {
            let resp: LoadJobResponse = post_json(
                client,
                endpoint,
                "load_job",
                &LoadJobRequest {
                    job_id: &r.job_id,
                    include_inputs: false,
                    include_outputs: false,
                    max_output_bytes: None,
                },
                Some(Duration::from_secs(5)),
            )
            .await?;
            Ok(resp.job.is_some_and(|job| job.state == state_text(r.state)))
        }
        JournalOp::Probe => {
            let _: SpecHashResponse = post_json(
                client,
                endpoint,
                "spec_hash",
                &SpecHashRequest {
                    job_id: "__probe__",
                },
                Some(Duration::from_secs(5)),
            )
            .await?;
            Ok(false)
        }
        // Worker registry mutations are idempotent and best-effort; treat
        // ambiguous transport errors as committed so durable job traffic is
        // never stalled by a transient registry write glitch.
        JournalOp::WorkerUpsert(_) | JournalOp::WorkerForget { .. } => Ok(true),
    }
}

async fn commit_batch(
    client: &reqwest::Client,
    endpoint: &str,
    holder_id: &str,
    term: u64,
    ops: &[&JournalOp],
    active_lost_tx: &ActiveTermSender,
) -> Option<Vec<CommitOutcome>> {
    commit_batch_inner(client, endpoint, holder_id, term, ops, active_lost_tx, true).await
}

async fn commit_batch_inner(
    client: &reqwest::Client,
    endpoint: &str,
    holder_id: &str,
    term: u64,
    ops: &[&JournalOp],
    active_lost_tx: &ActiveTermSender,
    allow_split: bool,
) -> Option<Vec<CommitOutcome>> {
    match send_commit_batch(client, endpoint, holder_id, term, ops).await {
        CommitHttpResult::Response(resp) => match resp.outcome.as_str() {
            "committed" => None,
            "fenced" => {
                let reason = resp
                    .reason
                    .unwrap_or_else(|| "Cloudflare scheduler lease expired or superseded".into());
                signal_term_lost(active_lost_tx, term);
                Some(vec![CommitOutcome::Fenced(reason); ops.len()])
            }
            "rolled_back" => {
                let reason = resp
                    .reason
                    .unwrap_or_else(|| "Cloudflare journal commit rolled back".into());
                if !allow_split || ops.len() == 1 {
                    Some(vec![CommitOutcome::RolledBack(reason); ops.len()])
                } else {
                    let mut out = Vec::with_capacity(ops.len());
                    for op in ops {
                        let outcome = Box::pin(commit_batch_inner(
                            client,
                            endpoint,
                            holder_id,
                            term,
                            &[op],
                            active_lost_tx,
                            false,
                        ))
                        .await
                        .and_then(|mut v| v.pop())
                        .unwrap_or(CommitOutcome::Committed);
                        out.push(outcome);
                    }
                    Some(out)
                }
            }
            other => {
                let reason = format!(
                    "unexpected /v1/commit_batch outcome '{other}': {}",
                    resp.reason.unwrap_or_default()
                );
                signal_term_lost(active_lost_tx, term);
                Some(vec![CommitOutcome::Unknown(reason); ops.len()])
            }
        },
        CommitHttpResult::Rejected(err) => {
            if !allow_split || ops.len() == 1 {
                Some(vec![CommitOutcome::RolledBack(err); ops.len()])
            } else {
                let mut out = Vec::with_capacity(ops.len());
                for op in ops {
                    let outcome = Box::pin(commit_batch_inner(
                        client,
                        endpoint,
                        holder_id,
                        term,
                        &[op],
                        active_lost_tx,
                        false,
                    ))
                    .await
                    .and_then(|mut v| v.pop())
                    .unwrap_or(CommitOutcome::Committed);
                    out.push(outcome);
                }
                Some(out)
            }
        }
        CommitHttpResult::Ambiguous(err) => {
            let mut out = Vec::with_capacity(ops.len());
            let mut any_unknown = false;
            for op in ops {
                match verify_op(client, endpoint, op).await {
                    Ok(true) => out.push(CommitOutcome::Committed),
                    Ok(false) => out.push(CommitOutcome::RolledBack(err.clone())),
                    Err(verify_err) => {
                        any_unknown = true;
                        out.push(CommitOutcome::Unknown(format!(
                            "Cloudflare write failed ({err}) and could not be verified: {verify_err:#}"
                        )));
                    }
                }
            }
            if any_unknown {
                signal_term_lost(active_lost_tx, term);
            }
            Some(out)
        }
    }
}

fn state_from_text(text: &str) -> Result<JobState> {
    Ok(match text {
        "queued" => JobState::Queued,
        "running" => JobState::Running,
        "succeeded" => JobState::Succeeded,
        "failed" => JobState::Failed,
        "cancelled" => JobState::Cancelled,
        other => return Err(anyhow!("unknown Cloudflare journal job state '{other}'")),
    })
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WireObject {
    uri: Option<String>,
    data_b64: Option<String>,
    #[serde(default)]
    metadata: Metadata,
}

impl From<&Object> for WireObject {
    fn from(obj: &Object) -> Self {
        match obj {
            Object::Inline { data, metadata } => Self {
                uri: None,
                data_b64: Some(BASE64.encode(data)),
                metadata: metadata.clone(),
            },
            Object::Uri { uri, metadata } => Self {
                uri: Some(uri.clone()),
                data_b64: None,
                metadata: metadata.clone(),
            },
        }
    }
}

impl WireObject {
    fn into_object(self) -> Result<Object> {
        match self.uri {
            Some(uri) => Ok(Object::Uri {
                uri,
                metadata: self.metadata,
            }),
            None => {
                let data = match self.data_b64.as_deref() {
                    Some(b64) if !b64.is_empty() => {
                        Bytes::from(BASE64.decode(b64).context("decode base64 object payload")?)
                    }
                    _ => Bytes::new(),
                };
                Ok(Object::Inline {
                    data,
                    metadata: self.metadata,
                })
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WireObjectPreview {
    uri: Option<String>,
    head_b64: Option<String>,
    size_bytes: Option<usize>,
    #[serde(default)]
    metadata: Metadata,
}

impl WireObjectPreview {
    fn into_preview(self) -> Result<ObjectPreview> {
        match self.uri {
            Some(uri) => Ok(ObjectPreview {
                prefix: Object::Uri {
                    uri,
                    metadata: self.metadata,
                },
                size_bytes: None,
            }),
            None => {
                let data = match self.head_b64.as_deref() {
                    Some(b64) if !b64.is_empty() => {
                        Bytes::from(BASE64.decode(b64).context("decode base64 preview head")?)
                    }
                    _ => Bytes::new(),
                };
                let size_bytes = self.size_bytes.or(Some(data.len()));
                Ok(ObjectPreview {
                    prefix: Object::Inline {
                        data,
                        metadata: self.metadata,
                    },
                    size_bytes,
                })
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WireStoredJob {
    job_id: String,
    namespace: String,
    state: String,
    spec_hash: String,
    spec_json: String,
    #[serde(default)]
    inputs: Vec<WireObject>,
    #[serde(default)]
    params_b64: String,
    attempt_count: u32,
    max_attempts: u32,
    created_at_ms: u64,
    updated_at_ms: u64,
    #[serde(default)]
    error_json: Option<String>,
    #[serde(default)]
    outputs: Vec<WireObject>,
    #[serde(default)]
    last_attempt_id: Option<String>,
    #[serde(default)]
    last_worker_id: Option<String>,
    #[serde(default)]
    failed_workers: Vec<String>,
}

impl WireStoredJob {
    fn into_stored_job(self, include_inputs: bool, include_outputs: bool) -> Result<StoredJob> {
        let state = state_from_text(&self.state)?;
        let stored_spec: StoredSpec =
            serde_json::from_str(&self.spec_json).context("decode Cloudflare journal spec")?;
        let inputs = if include_inputs {
            self.inputs
                .into_iter()
                .map(WireObject::into_object)
                .collect::<Result<Vec<_>>>()?
        } else {
            Vec::new()
        };
        let params = if include_inputs && !self.params_b64.is_empty() {
            Bytes::from(
                BASE64
                    .decode(&self.params_b64)
                    .context("decode Cloudflare job params_b64")?,
            )
        } else {
            Bytes::new()
        };
        let outputs = if include_outputs {
            self.outputs
                .into_iter()
                .map(WireObject::into_object)
                .collect::<Result<Vec<_>>>()?
        } else {
            Vec::new()
        };
        let error = self
            .error_json
            .map(|json| serde_json::from_str(&json))
            .transpose()
            .context("decode Cloudflare journal error")?;
        let failed_workers: BTreeSet<String> = self.failed_workers.into_iter().collect();

        Ok(StoredJob {
            job_id: self.job_id,
            state,
            spec: JobSpec {
                demand: stored_spec.demand,
                inputs,
                params,
                policy: stored_spec.policy,
                metadata: stored_spec.metadata,
            },
            spec_hash: self.spec_hash,
            attempt_count: self.attempt_count,
            max_attempts: self.max_attempts,
            created_at_ms: self.created_at_ms,
            updated_at_ms: self.updated_at_ms,
            error,
            outputs,
            last_attempt_id: self.last_attempt_id,
            last_worker_id: self.last_worker_id,
            failed_workers,
        })
    }
}

#[derive(Serialize)]
struct AcquireRequest<'a> {
    holder_id: &'a str,
    lease_duration_ms: u64,
}

#[derive(Deserialize)]
struct AcquireResponse {
    acquired: bool,
    term: Option<u64>,
    #[serde(default)]
    active: Vec<WireStoredJob>,
    #[serde(default)]
    workers: Vec<String>,
}

#[derive(Serialize)]
struct RenewRequest<'a> {
    holder_id: &'a str,
    term: u64,
    lease_duration_ms: u64,
}

#[derive(Deserialize)]
struct RenewResponse {
    ok: bool,
}

#[derive(Serialize)]
struct RelinquishRequest<'a> {
    holder_id: &'a str,
    term: u64,
}

#[derive(Deserialize)]
struct RelinquishResponse {
    ok: bool,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum WireCommitOp {
    Accept {
        job_id: String,
        namespace: String,
        spec_hash: String,
        spec_json: String,
        inputs: Vec<WireObject>,
        params_b64: String,
        max_attempts: u32,
        created_at_ms: u64,
    },
    AttemptStart {
        job_id: String,
        attempt_number: u32,
        at_ms: u64,
        attempt_id: Option<String>,
        worker_id: Option<String>,
    },
    Terminal {
        job_id: String,
        state: String,
        outputs: Vec<WireObject>,
        error_json: Option<String>,
        attempt_id: Option<String>,
        worker_id: Option<String>,
        winning_lease_id: Option<String>,
        outcome_digest: Option<String>,
        at_ms: u64,
    },
    WorkerUpsert {
        namespace: String,
        worker_id: String,
        record_json: String,
        updated_at_ms: u64,
    },
    WorkerForget {
        namespace: String,
        worker_id: String,
    },
    Probe,
}

impl TryFrom<&JournalOp> for WireCommitOp {
    type Error = anyhow::Error;

    fn try_from(op: &JournalOp) -> Result<Self> {
        Ok(match op {
            JournalOp::Accept(r) => {
                let (ns, _) = decode_scoped_id(&r.job_id);
                let stored = StoredSpecRef {
                    demand: &r.spec.demand,
                    policy: &r.spec.policy,
                    metadata: &r.spec.metadata,
                };
                Self::Accept {
                    job_id: r.job_id.clone(),
                    namespace: ns.to_string(),
                    spec_hash: r.spec_hash.clone(),
                    spec_json: serde_json::to_string(&stored)?,
                    inputs: r.spec.inputs.iter().map(WireObject::from).collect(),
                    params_b64: if r.spec.params.is_empty() {
                        String::new()
                    } else {
                        BASE64.encode(&r.spec.params)
                    },
                    max_attempts: r.max_attempts,
                    created_at_ms: r.created_at_ms,
                }
            }
            JournalOp::AttemptStart(r) => Self::AttemptStart {
                job_id: r.job_id.clone(),
                attempt_number: r.attempt_number,
                at_ms: r.at_ms,
                attempt_id: r.attempt_id.clone(),
                worker_id: r.worker_id.clone(),
            },
            JournalOp::Terminal(r) => Self::Terminal {
                job_id: r.job_id.clone(),
                state: state_text(r.state).to_string(),
                outputs: r.outputs.iter().map(WireObject::from).collect(),
                error_json: r.error.as_ref().map(serde_json::to_string).transpose()?,
                attempt_id: r.attempt_id.clone(),
                worker_id: r.worker_id.clone(),
                winning_lease_id: r.winning_lease_id.clone(),
                outcome_digest: r.outcome_digest.clone(),
                at_ms: r.at_ms,
            },
            JournalOp::WorkerUpsert(r) => Self::WorkerUpsert {
                namespace: r.namespace.clone(),
                worker_id: r.worker_id.clone(),
                record_json: serde_json::to_string(r.as_ref())?,
                updated_at_ms: r.updated_at_ms(),
            },
            JournalOp::WorkerForget {
                namespace,
                worker_id,
            } => Self::WorkerForget {
                namespace: namespace.clone(),
                worker_id: worker_id.clone(),
            },
            JournalOp::Probe => Self::Probe,
        })
    }
}

#[derive(Serialize)]
struct CommitBatchRequest<'a> {
    holder_id: &'a str,
    term: u64,
    ops: Vec<WireCommitOp>,
}

#[derive(Deserialize)]
struct CommitBatchResponse {
    outcome: String,
    #[serde(default)]
    reason: Option<String>,
}

#[derive(Serialize)]
struct SpecHashRequest<'a> {
    job_id: &'a str,
}

#[derive(Deserialize)]
struct SpecHashResponse {
    spec_hash: Option<String>,
}

#[derive(Serialize)]
struct ReceiptRequest<'a> {
    lease_id: &'a str,
}

#[derive(Deserialize)]
struct WireReceipt {
    job_id: String,
    result_digest: String,
}

#[derive(Deserialize)]
struct ReceiptResponse {
    receipt: Option<WireReceipt>,
}

#[derive(Serialize)]
struct LoadJobRequest<'a> {
    job_id: &'a str,
    include_inputs: bool,
    include_outputs: bool,
    max_output_bytes: Option<u64>,
}

#[derive(Deserialize)]
struct LoadJobResponse {
    job: Option<WireStoredJob>,
    #[serde(default)]
    budget_exceeded: bool,
    #[serde(default)]
    #[allow(dead_code)]
    required_bytes: u64,
}

#[derive(Serialize)]
struct LoadJobPreviewRequest<'a> {
    job_id: &'a str,
    preview_bytes: usize,
}

#[derive(Deserialize)]
struct LoadJobPreviewResponse {
    job: Option<WireStoredJob>,
    #[serde(default)]
    #[allow(dead_code)]
    input_previews: Vec<WireObjectPreview>,
    #[serde(default)]
    #[allow(dead_code)]
    params_head_b64: String,
    #[serde(default)]
    #[allow(dead_code)]
    params_size_bytes: usize,
    #[serde(default)]
    output_previews: Vec<WireObjectPreview>,
}

#[derive(Serialize)]
struct WireScanCursor {
    created_at_ms: u64,
    job_id: String,
}

#[derive(Serialize)]
struct ScanJobsRequest<'a> {
    from_ms: u64,
    to_ms: u64,
    before: Option<WireScanCursor>,
    namespace: Option<&'a str>,
    limit: usize,
}

#[derive(Deserialize)]
struct ScanJobsResponse {
    jobs: Vec<WireStoredJob>,
}

#[derive(Serialize)]
struct PurgeRequest<'a> {
    holder_id: &'a str,
    term: u64,
    before_ms: u64,
    batch_limit: usize,
}

#[derive(Deserialize)]
struct PurgeResponse {
    fenced: bool,
    deleted: usize,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use anyserve_client::execution::{Attempt as ClientAttempt, WorkerSession};
    use anyserve_proto::v2 as pb;
    use anyserve_proto::v2::client_server::ClientServer;
    use axum::extract::State;
    use axum::http::{HeaderMap, StatusCode};
    use axum::routing::post;
    use axum::{Json, Router};
    use futures::StreamExt;
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::Server;

    use super::*;
    use crate::client_service::ClientGrpcService;
    use crate::error::ErrorKind;
    use crate::journal::{AcceptRecord, AttemptStartRecord, TerminalRecord};
    use crate::runtime::Runtime;
    use crate::runtime::api::{
        Durability, ErrorInfo, EventKind, JobScan, RuntimeConfig, SubmitRequest,
    };
    use crate::worker_service::WorkerGrpcService;

    #[derive(Clone)]
    struct MockLease {
        holder_id: String,
        term: u64,
        expires_at: Instant,
    }

    #[derive(Clone)]
    struct MockJobRow {
        job_id: String,
        namespace: String,
        state: String,
        spec_hash: String,
        spec_json: String,
        attempt_count: u32,
        max_attempts: u32,
        created_at_ms: u64,
        updated_at_ms: u64,
        error_json: Option<String>,
        winning_lease_id: Option<String>,
        result_digest: Option<String>,
        last_attempt_id: Option<String>,
        last_worker_id: Option<String>,
    }

    #[derive(Clone)]
    struct MockObjectRow {
        job_id: String,
        role: String,
        idx: usize,
        uri: Option<String>,
        inline_content: Option<Vec<u8>>,
        r2_key: Option<String>,
        size_bytes: usize,
        metadata: Metadata,
    }

    struct MockDb {
        lease: Option<MockLease>,
        last_term: u64,
        jobs: BTreeMap<String, MockJobRow>,
        objects: BTreeMap<(String, String, usize), MockObjectRow>,
        failed_workers: BTreeMap<String, BTreeSet<String>>,
        workers: BTreeMap<(String, String), (String, u64)>,
        r2: BTreeMap<String, Vec<u8>>,
        r2_spill_threshold: usize,
        expected_token: Option<String>,
        force_renew_false: bool,
        force_commit_500_after_apply: bool,
        force_commit_500_without_apply: bool,
        reject_all_http: bool,
        r2_range_reads: Vec<(String, usize, usize)>,
        r2_full_reads: Vec<String>,
    }

    impl MockDb {
        fn new(r2_spill_threshold: usize, expected_token: Option<String>) -> Self {
            Self {
                lease: None,
                last_term: 0,
                jobs: BTreeMap::new(),
                objects: BTreeMap::new(),
                failed_workers: BTreeMap::new(),
                workers: BTreeMap::new(),
                r2: BTreeMap::new(),
                r2_spill_threshold,
                expected_token,
                force_renew_false: false,
                force_commit_500_after_apply: false,
                force_commit_500_without_apply: false,
                reject_all_http: false,
                r2_range_reads: Vec::new(),
                r2_full_reads: Vec::new(),
            }
        }

        fn lease_valid(&self, holder_id: &str, term: u64) -> bool {
            self.lease.as_ref().is_some_and(|l| {
                l.holder_id == holder_id && l.term == term && l.expires_at > Instant::now()
            })
        }

        fn read_object(&mut self, obj: &MockObjectRow) -> WireObject {
            if let Some(uri) = &obj.uri {
                return WireObject {
                    uri: Some(uri.clone()),
                    data_b64: None,
                    metadata: obj.metadata.clone(),
                };
            }
            let bytes = if let Some(key) = &obj.r2_key {
                self.r2_full_reads.push(key.clone());
                self.r2.get(key).expect("r2 object present").clone()
            } else {
                obj.inline_content.clone().unwrap_or_default()
            };
            WireObject {
                uri: None,
                data_b64: Some(BASE64.encode(&bytes)),
                metadata: obj.metadata.clone(),
            }
        }

        fn read_preview(&mut self, obj: &MockObjectRow, preview_bytes: usize) -> WireObjectPreview {
            if let Some(uri) = &obj.uri {
                return WireObjectPreview {
                    uri: Some(uri.clone()),
                    head_b64: None,
                    size_bytes: None,
                    metadata: obj.metadata.clone(),
                };
            }
            let head = if let Some(key) = &obj.r2_key {
                let full = self.r2.get(key).expect("r2 object present");
                let len = full.len().min(preview_bytes);
                self.r2_range_reads.push((key.clone(), 0, len));
                full[..len].to_vec()
            } else {
                let inline = obj.inline_content.as_deref().unwrap_or(&[]);
                inline[..inline.len().min(preview_bytes)].to_vec()
            };
            WireObjectPreview {
                uri: None,
                head_b64: Some(BASE64.encode(&head)),
                size_bytes: Some(obj.size_bytes),
                metadata: obj.metadata.clone(),
            }
        }

        fn job_to_wire(
            &mut self,
            row: &MockJobRow,
            include_inputs: bool,
            include_outputs: bool,
        ) -> WireStoredJob {
            let mut inputs = Vec::new();
            let mut params_b64 = String::new();
            let mut outputs = Vec::new();
            let matching_keys: Vec<(String, String, usize)> = self
                .objects
                .keys()
                .filter(|(id, _, _)| id == &row.job_id)
                .cloned()
                .collect();
            for key in matching_keys {
                let obj = self.objects.get(&key).cloned().unwrap();
                match obj.role.as_str() {
                    "input" if include_inputs => inputs.push(self.read_object(&obj)),
                    "params" if include_inputs && obj.idx == 0 => {
                        let bytes = if let Some(r2_key) = &obj.r2_key {
                            self.r2_full_reads.push(r2_key.clone());
                            self.r2.get(r2_key).expect("r2 params").clone()
                        } else {
                            obj.inline_content.unwrap_or_default()
                        };
                        params_b64 = BASE64.encode(&bytes);
                    }
                    "output" if include_outputs => outputs.push(self.read_object(&obj)),
                    _ => {}
                }
            }
            WireStoredJob {
                job_id: row.job_id.clone(),
                namespace: row.namespace.clone(),
                state: row.state.clone(),
                spec_hash: row.spec_hash.clone(),
                spec_json: row.spec_json.clone(),
                inputs,
                params_b64,
                attempt_count: row.attempt_count,
                max_attempts: row.max_attempts,
                created_at_ms: row.created_at_ms,
                updated_at_ms: row.updated_at_ms,
                error_json: row.error_json.clone(),
                outputs,
                last_attempt_id: row.last_attempt_id.clone(),
                last_worker_id: row.last_worker_id.clone(),
                failed_workers: self
                    .failed_workers
                    .get(&row.job_id)
                    .map(|s| s.iter().cloned().collect())
                    .unwrap_or_default(),
            }
        }

        fn store_wire_object(
            r2_spill_threshold: usize,
            pending_r2_puts: &mut Vec<(String, Vec<u8>)>,
            job_id: &str,
            role: &str,
            idx: usize,
            obj: WireObject,
        ) -> MockObjectRow {
            if let Some(uri) = obj.uri {
                return MockObjectRow {
                    job_id: job_id.to_string(),
                    role: role.to_string(),
                    idx,
                    uri: Some(uri),
                    inline_content: None,
                    r2_key: None,
                    size_bytes: 0,
                    metadata: obj.metadata,
                };
            }
            let bytes = obj
                .data_b64
                .as_deref()
                .filter(|s| !s.is_empty())
                .map(|s| BASE64.decode(s).unwrap())
                .unwrap_or_default();
            let size_bytes = bytes.len();
            if size_bytes > r2_spill_threshold {
                let r2_key = format!("jobs/{job_id}/{role}/{idx}");
                pending_r2_puts.push((r2_key.clone(), bytes));
                MockObjectRow {
                    job_id: job_id.to_string(),
                    role: role.to_string(),
                    idx,
                    uri: None,
                    inline_content: None,
                    r2_key: Some(r2_key),
                    size_bytes,
                    metadata: obj.metadata,
                }
            } else {
                MockObjectRow {
                    job_id: job_id.to_string(),
                    role: role.to_string(),
                    idx,
                    uri: None,
                    inline_content: Some(bytes),
                    r2_key: None,
                    size_bytes,
                    metadata: obj.metadata,
                }
            }
        }
    }

    #[derive(Clone)]
    struct MockServer {
        db: Arc<tokio::sync::Mutex<MockDb>>,
        endpoint: String,
        _stop: Arc<tokio::sync::oneshot::Sender<()>>,
    }

    fn check_auth(db: &MockDb, headers: &HeaderMap) -> Result<(), (StatusCode, String)> {
        if db.reject_all_http {
            return Err((StatusCode::SERVICE_UNAVAILABLE, "simulated outage".into()));
        }
        if let Some(expected) = &db.expected_token {
            let auth = headers
                .get(AUTHORIZATION)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.strip_prefix("Bearer "));
            if auth != Some(expected.as_str()) {
                return Err((StatusCode::UNAUTHORIZED, "unauthorized".into()));
            }
        }
        Ok(())
    }

    #[derive(Deserialize)]
    struct MockAcquireReq {
        holder_id: String,
        lease_duration_ms: u64,
    }
    #[derive(Deserialize)]
    struct MockRenewReq {
        holder_id: String,
        term: u64,
        lease_duration_ms: u64,
    }
    #[derive(Deserialize)]
    struct MockRelinquishReq {
        holder_id: String,
        term: u64,
    }
    #[derive(Deserialize)]
    struct MockCommitBatchReq {
        holder_id: String,
        term: u64,
        ops: Vec<WireCommitOp>,
    }
    #[derive(Deserialize)]
    struct MockSpecHashReq {
        job_id: String,
    }
    #[derive(Deserialize)]
    struct MockReceiptReq {
        lease_id: String,
    }
    #[derive(Deserialize)]
    struct MockLoadJobReq {
        job_id: String,
        include_inputs: bool,
        include_outputs: bool,
        max_output_bytes: Option<u64>,
    }
    #[derive(Deserialize)]
    struct MockPreviewReq {
        job_id: String,
        preview_bytes: usize,
    }
    #[derive(Deserialize)]
    struct MockScanCursor {
        created_at_ms: u64,
        job_id: String,
    }
    #[derive(Deserialize)]
    struct MockScanReq {
        from_ms: u64,
        to_ms: u64,
        before: Option<MockScanCursor>,
        namespace: Option<String>,
        limit: usize,
    }
    #[derive(Deserialize)]
    struct MockPurgeReq {
        holder_id: String,
        term: u64,
        before_ms: u64,
        batch_limit: usize,
    }

    async fn handle_acquire(
        State(state): State<Arc<tokio::sync::Mutex<MockDb>>>,
        headers: HeaderMap,
        Json(req): Json<MockAcquireReq>,
    ) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
        let mut db = state.lock().await;
        check_auth(&db, &headers)?;
        let now = Instant::now();
        if let Some(current) = &db.lease
            && current.expires_at > now
            && current.holder_id != req.holder_id
        {
            return Ok(Json(serde_json::json!({ "acquired": false })));
        }
        db.last_term += 1;
        let term = db.last_term;
        db.lease = Some(MockLease {
            holder_id: req.holder_id,
            term,
            expires_at: now + Duration::from_millis(req.lease_duration_ms),
        });
        let mut queued: Vec<MockJobRow> = db
            .jobs
            .values()
            .filter(|j| j.state == "queued")
            .cloned()
            .collect();
        queued.sort_by(|a, b| (a.created_at_ms, &a.job_id).cmp(&(b.created_at_ms, &b.job_id)));
        let active: Vec<WireStoredJob> = queued
            .iter()
            .map(|row| db.job_to_wire(row, true, false))
            .collect();
        let mut worker_rows: Vec<(u64, String)> = db
            .workers
            .values()
            .map(|(json, updated)| (*updated, json.clone()))
            .collect();
        worker_rows.sort_by_key(|a| std::cmp::Reverse(a.0));
        let workers: Vec<String> = worker_rows.into_iter().map(|(_, json)| json).collect();
        Ok(Json(serde_json::json!({
            "acquired": true,
            "term": term,
            "active": active,
            "workers": workers,
        })))
    }

    async fn handle_renew(
        State(state): State<Arc<tokio::sync::Mutex<MockDb>>>,
        headers: HeaderMap,
        Json(req): Json<MockRenewReq>,
    ) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
        let mut db = state.lock().await;
        check_auth(&db, &headers)?;
        if db.force_renew_false {
            return Ok(Json(serde_json::json!({ "ok": false })));
        }
        if db.lease_valid(&req.holder_id, req.term) {
            if let Some(lease) = &mut db.lease {
                lease.expires_at = Instant::now() + Duration::from_millis(req.lease_duration_ms);
            }
            return Ok(Json(serde_json::json!({ "ok": true })));
        }
        Ok(Json(serde_json::json!({ "ok": false })))
    }

    async fn handle_relinquish(
        State(state): State<Arc<tokio::sync::Mutex<MockDb>>>,
        headers: HeaderMap,
        Json(req): Json<MockRelinquishReq>,
    ) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
        let mut db = state.lock().await;
        check_auth(&db, &headers)?;
        if db
            .lease
            .as_ref()
            .is_some_and(|l| l.holder_id == req.holder_id && l.term == req.term)
        {
            db.lease = None;
            return Ok(Json(serde_json::json!({ "ok": true })));
        }
        Ok(Json(serde_json::json!({ "ok": false })))
    }

    async fn handle_commit_batch(
        State(state): State<Arc<tokio::sync::Mutex<MockDb>>>,
        headers: HeaderMap,
        Json(req): Json<MockCommitBatchReq>,
    ) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
        let mut db = state.lock().await;
        check_auth(&db, &headers)?;
        if !db.lease_valid(&req.holder_id, req.term) {
            return Ok(Json(serde_json::json!({
                "outcome": "fenced",
                "reason": "lease expired or superseded"
            })));
        }
        if db.force_commit_500_without_apply {
            db.force_commit_500_without_apply = false;
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                "simulated commit failure".into(),
            ));
        }

        // Stage mutations atomically so any error rolls back the entire batch.
        let mut jobs = db.jobs.clone();
        let mut objects = db.objects.clone();
        let mut failed_workers = db.failed_workers.clone();
        let mut workers = db.workers.clone();
        let mut r2_puts = Vec::new();
        let threshold = db.r2_spill_threshold;

        for op in req.ops {
            match op {
                WireCommitOp::Accept {
                    job_id,
                    namespace,
                    spec_hash,
                    spec_json,
                    inputs,
                    params_b64,
                    max_attempts,
                    created_at_ms,
                } => {
                    if jobs.contains_key(&job_id) {
                        return Ok(Json(serde_json::json!({
                            "outcome": "rolled_back",
                            "reason": format!("duplicate job_id {job_id}")
                        })));
                    }
                    jobs.insert(
                        job_id.clone(),
                        MockJobRow {
                            job_id: job_id.clone(),
                            namespace,
                            state: "queued".into(),
                            spec_hash,
                            spec_json,
                            attempt_count: 0,
                            max_attempts,
                            created_at_ms,
                            updated_at_ms: created_at_ms,
                            error_json: None,
                            winning_lease_id: None,
                            result_digest: None,
                            last_attempt_id: None,
                            last_worker_id: None,
                        },
                    );
                    for (idx, input) in inputs.into_iter().enumerate() {
                        let row = MockDb::store_wire_object(
                            threshold,
                            &mut r2_puts,
                            &job_id,
                            "input",
                            idx,
                            input,
                        );
                        objects.insert((job_id.clone(), "input".into(), idx), row);
                    }
                    if !params_b64.is_empty() {
                        let row = MockDb::store_wire_object(
                            threshold,
                            &mut r2_puts,
                            &job_id,
                            "params",
                            0,
                            WireObject {
                                uri: None,
                                data_b64: Some(params_b64),
                                metadata: Metadata::new(),
                            },
                        );
                        objects.insert((job_id.clone(), "params".into(), 0), row);
                    }
                }
                WireCommitOp::AttemptStart {
                    job_id,
                    at_ms,
                    attempt_id,
                    worker_id,
                    ..
                } => {
                    let Some(job) = jobs.get_mut(&job_id) else {
                        return Ok(Json(serde_json::json!({
                            "outcome": "rolled_back",
                            "reason": format!("job {job_id} missing")
                        })));
                    };
                    if job.state != "queued" {
                        return Ok(Json(serde_json::json!({
                            "outcome": "rolled_back",
                            "reason": format!("job {job_id} is not queued")
                        })));
                    }
                    job.attempt_count += 1;
                    job.updated_at_ms = at_ms;
                    if attempt_id.is_some() {
                        job.last_attempt_id = attempt_id;
                    }
                    if let Some(w) = worker_id {
                        job.last_worker_id = Some(w.clone());
                        failed_workers.entry(job_id).or_default().insert(w);
                    }
                }
                WireCommitOp::Terminal {
                    job_id,
                    state,
                    outputs,
                    error_json,
                    attempt_id,
                    worker_id,
                    winning_lease_id,
                    outcome_digest,
                    at_ms,
                } => {
                    let Some(job) = jobs.get_mut(&job_id) else {
                        return Ok(Json(serde_json::json!({
                            "outcome": "rolled_back",
                            "reason": format!("job {job_id} missing")
                        })));
                    };
                    if job.state != "queued" {
                        return Ok(Json(serde_json::json!({
                            "outcome": "rolled_back",
                            "reason": format!("job {job_id} is not open")
                        })));
                    }
                    job.state = state;
                    job.updated_at_ms = at_ms;
                    job.error_json = error_json;
                    job.winning_lease_id = winning_lease_id;
                    job.result_digest = outcome_digest;
                    job.last_attempt_id = attempt_id;
                    job.last_worker_id = worker_id;
                    for (idx, output) in outputs.into_iter().enumerate() {
                        let row = MockDb::store_wire_object(
                            threshold,
                            &mut r2_puts,
                            &job_id,
                            "output",
                            idx,
                            output,
                        );
                        objects.insert((job_id.clone(), "output".into(), idx), row);
                    }
                }
                WireCommitOp::WorkerUpsert {
                    namespace,
                    worker_id,
                    record_json,
                    updated_at_ms,
                } => {
                    workers.insert((namespace, worker_id), (record_json, updated_at_ms));
                }
                WireCommitOp::WorkerForget {
                    namespace,
                    worker_id,
                } => {
                    workers.remove(&(namespace, worker_id));
                }
                WireCommitOp::Probe => {}
            }
        }

        for (key, bytes) in r2_puts {
            db.r2.insert(key, bytes);
        }
        db.jobs = jobs;
        db.objects = objects;
        db.failed_workers = failed_workers;
        db.workers = workers;

        if db.force_commit_500_after_apply {
            db.force_commit_500_after_apply = false;
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                "simulated response drop after commit".into(),
            ));
        }

        Ok(Json(serde_json::json!({ "outcome": "committed" })))
    }

    async fn handle_spec_hash(
        State(state): State<Arc<tokio::sync::Mutex<MockDb>>>,
        headers: HeaderMap,
        Json(req): Json<MockSpecHashReq>,
    ) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
        let db = state.lock().await;
        check_auth(&db, &headers)?;
        let hash = db.jobs.get(&req.job_id).map(|j| j.spec_hash.clone());
        Ok(Json(serde_json::json!({ "spec_hash": hash })))
    }

    async fn handle_receipt(
        State(state): State<Arc<tokio::sync::Mutex<MockDb>>>,
        headers: HeaderMap,
        Json(req): Json<MockReceiptReq>,
    ) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
        let db = state.lock().await;
        check_auth(&db, &headers)?;
        let receipt = db.jobs.values().find_map(|j| {
            if j.winning_lease_id.as_deref() == Some(req.lease_id.as_str()) {
                j.result_digest.as_ref().map(|digest| {
                    serde_json::json!({
                        "job_id": j.job_id,
                        "result_digest": digest,
                    })
                })
            } else {
                None
            }
        });
        Ok(Json(serde_json::json!({ "receipt": receipt })))
    }

    async fn handle_load_job(
        State(state): State<Arc<tokio::sync::Mutex<MockDb>>>,
        headers: HeaderMap,
        Json(req): Json<MockLoadJobReq>,
    ) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
        let mut db = state.lock().await;
        check_auth(&db, &headers)?;
        let Some(row) = db.jobs.get(&req.job_id).cloned() else {
            return Ok(Json(serde_json::json!({
                "job": null,
                "budget_exceeded": false,
                "required_bytes": 0,
            })));
        };
        let required_bytes: u64 = if req.include_outputs && row.state == "succeeded" {
            db.objects
                .values()
                .filter(|o| o.job_id == req.job_id && o.role == "output" && o.uri.is_none())
                .map(|o| o.size_bytes as u64)
                .sum()
        } else {
            0
        };
        if let Some(limit) = req.max_output_bytes
            && required_bytes > limit
        {
            return Ok(Json(serde_json::json!({
                "job": null,
                "budget_exceeded": true,
                "required_bytes": required_bytes,
            })));
        }
        let wire_job = db.job_to_wire(
            &row,
            req.include_inputs,
            req.include_outputs && row.state == "succeeded",
        );
        Ok(Json(serde_json::json!({
            "job": wire_job,
            "budget_exceeded": false,
            "required_bytes": required_bytes,
        })))
    }

    async fn handle_load_job_preview(
        State(state): State<Arc<tokio::sync::Mutex<MockDb>>>,
        headers: HeaderMap,
        Json(req): Json<MockPreviewReq>,
    ) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
        let mut db = state.lock().await;
        check_auth(&db, &headers)?;
        let Some(row) = db.jobs.get(&req.job_id).cloned() else {
            return Ok(Json(serde_json::json!({
                "job": null,
                "input_previews": [],
                "params_head_b64": "",
                "params_size_bytes": 0,
                "output_previews": [],
            })));
        };
        let wire_job = db.job_to_wire(&row, false, false);
        let obj_keys: Vec<(String, String, usize)> = db
            .objects
            .keys()
            .filter(|(id, _, _)| id == &req.job_id)
            .cloned()
            .collect();
        let mut input_previews = Vec::new();
        let mut params_head_b64 = String::new();
        let mut params_size_bytes = 0usize;
        let mut output_previews = Vec::new();
        for key in obj_keys {
            let obj = db.objects.get(&key).cloned().unwrap();
            match obj.role.as_str() {
                "input" => input_previews.push(db.read_preview(&obj, req.preview_bytes)),
                "params" if obj.idx == 0 => {
                    let preview = db.read_preview(&obj, req.preview_bytes);
                    params_head_b64 = preview.head_b64.unwrap_or_default();
                    params_size_bytes = preview.size_bytes.unwrap_or(0);
                }
                "output" if row.state == "succeeded" => {
                    output_previews.push(db.read_preview(&obj, req.preview_bytes));
                }
                _ => {}
            }
        }
        Ok(Json(serde_json::json!({
            "job": wire_job,
            "input_previews": input_previews,
            "params_head_b64": params_head_b64,
            "params_size_bytes": params_size_bytes,
            "output_previews": output_previews,
        })))
    }

    async fn handle_scan_jobs(
        State(state): State<Arc<tokio::sync::Mutex<MockDb>>>,
        headers: HeaderMap,
        Json(req): Json<MockScanReq>,
    ) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
        let mut db = state.lock().await;
        check_auth(&db, &headers)?;
        let mut rows: Vec<MockJobRow> =
            db.jobs
                .values()
                .filter(|j| {
                    matches!(j.state.as_str(), "succeeded" | "failed" | "cancelled")
                        && j.created_at_ms >= req.from_ms
                        && j.created_at_ms < req.to_ms
                        && req.namespace.as_deref().is_none_or(|ns| j.namespace == ns)
                        && req.before.as_ref().is_none_or(|b| {
                            (j.created_at_ms, &j.job_id) < (b.created_at_ms, &b.job_id)
                        })
                })
                .cloned()
                .collect();
        rows.sort_by(|a, b| (b.created_at_ms, &b.job_id).cmp(&(a.created_at_ms, &a.job_id)));
        rows.truncate(req.limit);
        let jobs: Vec<WireStoredJob> = rows
            .iter()
            .map(|r| db.job_to_wire(r, false, false))
            .collect();
        Ok(Json(serde_json::json!({ "jobs": jobs })))
    }

    async fn handle_purge(
        State(state): State<Arc<tokio::sync::Mutex<MockDb>>>,
        headers: HeaderMap,
        Json(req): Json<MockPurgeReq>,
    ) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
        let mut db = state.lock().await;
        check_auth(&db, &headers)?;
        if !db.lease_valid(&req.holder_id, req.term) {
            return Ok(Json(serde_json::json!({
                "fenced": true,
                "deleted": 0,
            })));
        }
        let victims: Vec<String> = db
            .jobs
            .values()
            .filter(|j| {
                matches!(j.state.as_str(), "succeeded" | "failed" | "cancelled")
                    && j.updated_at_ms < req.before_ms
            })
            .take(req.batch_limit)
            .map(|j| j.job_id.clone())
            .collect();
        let deleted = victims.len();
        for id in victims {
            db.jobs.remove(&id);
            db.failed_workers.remove(&id);
            let obj_keys: Vec<(String, String, usize)> = db
                .objects
                .keys()
                .filter(|(jid, _, _)| jid == &id)
                .cloned()
                .collect();
            for k in obj_keys {
                if let Some(row) = db.objects.remove(&k)
                    && let Some(r2_key) = row.r2_key
                {
                    db.r2.remove(&r2_key);
                }
            }
        }
        Ok(Json(serde_json::json!({
            "fenced": false,
            "deleted": deleted,
        })))
    }

    impl MockServer {
        async fn start(r2_spill_threshold: usize, expected_token: Option<String>) -> Self {
            let db = Arc::new(tokio::sync::Mutex::new(MockDb::new(
                r2_spill_threshold,
                expected_token,
            )));
            let app = Router::new()
                .route("/v1/acquire", post(handle_acquire))
                .route("/v1/renew", post(handle_renew))
                .route("/v1/relinquish", post(handle_relinquish))
                .route("/v1/commit_batch", post(handle_commit_batch))
                .route("/v1/spec_hash", post(handle_spec_hash))
                .route("/v1/receipt", post(handle_receipt))
                .route("/v1/load_job", post(handle_load_job))
                .route("/v1/load_job_preview", post(handle_load_job_preview))
                .route("/v1/scan_jobs", post(handle_scan_jobs))
                .route("/v1/purge", post(handle_purge))
                .layer(axum::extract::DefaultBodyLimit::max(128 * 1024 * 1024))
                .with_state(Arc::clone(&db));

            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let (tx, rx) = tokio::sync::oneshot::channel::<()>();
            tokio::spawn(async move {
                let _ = axum::serve(listener, app)
                    .with_graceful_shutdown(async move {
                        let _ = rx.await;
                    })
                    .await;
            });
            Self {
                db,
                endpoint: format!("http://{addr}"),
                _stop: Arc::new(tx),
            }
        }
    }

    struct GrpcHarness {
        runtime: Runtime,
        endpoint: String,
        _stop: tokio::sync::oneshot::Sender<()>,
    }

    async fn start_grpc(runtime: Runtime) -> GrpcHarness {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let client = ClientServer::new(ClientGrpcService::new(runtime.clone()))
            .max_decoding_message_size(anyserve_proto::MAX_MESSAGE_SIZE)
            .max_encoding_message_size(anyserve_proto::MAX_MESSAGE_SIZE);
        let worker = WorkerGrpcService::server(runtime.clone());
        tokio::spawn(async move {
            let _ = Server::builder()
                .add_service(client)
                .add_service(worker)
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                    let _ = rx.await;
                })
                .await;
        });
        GrpcHarness {
            runtime,
            endpoint: format!("http://{addr}"),
            _stop: tx,
        }
    }

    fn cf_journal_config(
        endpoint: &str,
        lease_ms: u64,
        poll_ms: u64,
        token: Option<&str>,
    ) -> JournalConfig {
        let mut cfg = JournalConfig::cloudflare(endpoint);
        let cf = cfg.cloudflare.as_mut().unwrap();
        cf.lease_duration = Duration::from_millis(lease_ms);
        cf.poll_interval = Duration::from_millis(poll_ms);
        cf.auth_token = token.map(str::to_string);
        cfg
    }

    fn durable_spec(interface: &str, input: Object) -> JobSpec {
        let mut spec = JobSpec::default();
        spec.demand.interface = interface.to_string();
        spec.policy.durability = Some(Durability::Durable);
        spec.inputs.push(input);
        spec.params = Bytes::from_static(b"params-v1");
        spec
    }

    #[test]
    fn cloudflare_config_debug_redacts_auth_token() {
        let mut cfg = CloudflareJournalConfig::new("http://journal.internal");
        assert!(format!("{cfg:?}").contains("None"));
        cfg.auth_token = Some("super-secret-token".into());
        let formatted = format!("{cfg:?}");
        assert!(!formatted.contains("super-secret-token"));
        assert!(formatted.contains("[redacted]"));
    }

    #[tokio::test]
    async fn standby_blocking_graceful_takeover_renewal_loss_and_stale_fencing() {
        let mock = MockServer::start(1024 * 1024, Some("cf-secret".into())).await;
        let make_rt = || RuntimeConfig {
            journal: Some(cf_journal_config(
                &mock.endpoint,
                400,
                30,
                Some("cf-secret"),
            )),
            ..Default::default()
        };

        let cp1 = Runtime::start(make_rt()).await.unwrap();
        let cp2 = Runtime::start(make_rt()).await.unwrap();
        assert!(cp1.is_leader_active());
        assert!(cp1.health().ready);
        assert!(!cp2.is_leader_active());
        assert!(cp2.health().live);
        assert!(!cp2.health().ready);

        // Standby rejects RPCs until promoted.
        assert_eq!(
            cp2.list_jobs(Default::default()).await.unwrap_err().kind,
            ErrorKind::PersistenceUnavailable
        );

        // Graceful shutdown relinquishes lease -> cp2 acquires term 2.
        let mut cp2_rx = cp2.subscribe_leadership();
        cp1.shutdown().await;
        tokio::time::timeout(Duration::from_secs(3), async {
            while !*cp2_rx.borrow_and_update() {
                cp2_rx.changed().await.unwrap();
            }
        })
        .await
        .expect("standby must promote after graceful relinquish");
        assert_eq!(cp2.generation(), 2);

        // Renewal loss (renew returns ok: false) triggers Runtime::mark_lease_lost.
        {
            mock.db.lock().await.force_renew_false = true;
        }
        tokio::time::timeout(Duration::from_secs(3), cp2.wait_for_lease_loss())
            .await
            .expect("renew ok:false must trigger mark_lease_lost");
        assert!(!cp2.health().live);
        assert!(!cp2.health().ready);

        // Reset mock and test stale-term fencing on commit_batch.
        {
            let mut db = mock.db.lock().await;
            db.force_renew_false = false;
            db.lease = None;
        }
        let j_stale = CloudflareJournal::open_concrete(&cf_journal_config(
            &mock.endpoint,
            250,
            25,
            Some("cf-secret"),
        ))
        .await
        .unwrap();
        let cp_stale = Runtime::start_with_backend(
            RuntimeConfig {
                journal: Some(cf_journal_config(
                    &mock.endpoint,
                    250,
                    25,
                    Some("cf-secret"),
                )),
                ..Default::default()
            },
            Some(Arc::clone(&j_stale) as Arc<dyn JournalBackend>),
        )
        .await
        .unwrap();
        assert_eq!(cp_stale.generation(), 3);

        // Pause cp_stale renewals and let cp_new take term 4 once lease expires.
        j_stale.set_pause_renewal(true);
        let cp_new = Runtime::start(RuntimeConfig {
            journal: Some(cf_journal_config(
                &mock.endpoint,
                400,
                25,
                Some("cf-secret"),
            )),
            ..Default::default()
        })
        .await
        .unwrap();
        let mut new_rx = cp_new.subscribe_leadership();
        tokio::time::timeout(Duration::from_secs(3), async {
            while !*new_rx.borrow_and_update() {
                new_rx.changed().await.unwrap();
            }
        })
        .await
        .expect("cp_new must acquire term 4 after cp_stale paused renewal");
        assert_eq!(cp_new.generation(), 4);

        // Stale leader tries to submit -> /v1/commit_batch fences it and marks lease lost.
        let err = cp_stale
            .submit(SubmitRequest {
                job_id: Some("stale-job".into()),
                spec: durable_spec("fence.v1", Object::inline(b"stale".as_slice())),
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert_eq!(err.kind, ErrorKind::PersistenceUnavailable);
        tokio::time::timeout(Duration::from_secs(2), cp_stale.wait_for_lease_loss())
            .await
            .expect("fenced commit_batch must trigger wait_for_lease_loss");
        assert_eq!(
            cp_new.get_job("stale-job", false).await.unwrap_err().kind,
            ErrorKind::NotFound
        );
        cp_new.shutdown().await;
    }

    #[tokio::test]
    async fn large_r2_spill_crash_recovery_preview_budget_dedup_scan_and_purge() {
        let mock = MockServer::start(1024 * 1024, None).await;
        let large_input = vec![b'A'; 2 * 1024 * 1024];
        let large_output = vec![b'Z'; 2 * 1024 * 1024];

        // 1. Start CP1, submit 2 MiB durable job, start an attempt on worker-bad, then crash CP1.
        let j1_cfg = cf_journal_config(&mock.endpoint, 1_000, 30, None);
        let j1 = CloudflareJournal::open_concrete(&j1_cfg).await.unwrap();
        let cp1 = start_grpc(
            Runtime::start_with_backend(
                RuntimeConfig {
                    journal: Some(j1_cfg),
                    default_max_attempts: 3,
                    ..Default::default()
                },
                Some(Arc::clone(&j1) as Arc<dyn JournalBackend>),
            )
            .await
            .unwrap(),
        )
        .await;

        let spec = durable_spec("r2.v1", Object::inline(large_input.clone()));
        let sub = cp1
            .runtime
            .submit(SubmitRequest {
                job_id: Some("r2-job-1".into()),
                spec: spec.clone(),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(sub.created);
        {
            let db = mock.db.lock().await;
            assert_eq!(db.r2.len(), 1, "2 MiB input must spill to R2");
            let obj = db
                .objects
                .get(&("r2-job-1".into(), "input".into(), 0))
                .unwrap();
            assert!(obj.inline_content.is_none());
            assert!(obj.r2_key.is_some());
        }

        // Start attempt on worker-bad and abandon CP1 without relinquish (simulating crash).
        let mut bad_worker = WorkerSession::connect(
            &cp1.endpoint,
            None,
            pb::Hello {
                worker_id: "worker-bad".into(),
                interfaces: vec!["r2.v1".into()],
                max_concurrency: 1,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let _ = bad_worker.messages.message().await.unwrap();
        let assign = loop {
            if let Some(pb::server_message::Msg::Assign(a)) =
                bad_worker.messages.message().await.unwrap().unwrap().msg
            {
                break a;
            }
        };
        assert_eq!(assign.job_id, "r2-job-1");
        let crashed_lease_id = assign.lease_id.clone();
        j1.abort_without_relinquish().await;
        drop(bad_worker);
        drop(cp1);

        // Wait for lease to expire and start CP2 -> recovers 2 MiB input, params, and failed_workers.
        tokio::time::sleep(Duration::from_millis(1_100)).await;
        let cp2 = start_grpc(
            Runtime::start(RuntimeConfig {
                journal: Some(cf_journal_config(&mock.endpoint, 2_000, 30, None)),
                max_terminal_jobs: 0,
                ..Default::default()
            })
            .await
            .unwrap(),
        )
        .await;
        assert!(cp2.runtime.is_leader_active());
        assert_eq!(cp2.runtime.generation(), 2);
        {
            let st = cp2.runtime.inner.lock();
            assert!(
                st.jobs["r2-job-1"].failed_workers.contains("worker-bad"),
                "recovered job must preserve failed_workers"
            );
        }

        // Connect worker-good -> receives recovered job with full 2 MiB input & params and returns 2 MiB output.
        let mut good_worker = WorkerSession::connect(
            &cp2.endpoint,
            None,
            pb::Hello {
                worker_id: "worker-good".into(),
                interfaces: vec!["r2.v1".into()],
                max_concurrency: 1,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let welcome = match good_worker.messages.message().await.unwrap().unwrap().msg {
            Some(pb::server_message::Msg::Welcome(w)) => w,
            other => panic!("expected Welcome: {other:?}"),
        };
        let limits = welcome.limits.unwrap();
        let hb = Duration::from_millis(u64::from(welcome.heartbeat_timeout_ms));
        let assign2 = loop {
            if let Some(pb::server_message::Msg::Assign(a)) =
                good_worker.messages.message().await.unwrap().unwrap().msg
            {
                break a;
            }
        };
        good_worker
            .control
            .send(pb::WorkerMessage {
                msg: Some(pb::worker_message::Msg::AssignReply(pb::AssignReply {
                    lease_id: assign2.lease_id.clone(),
                    accepted: true,
                    reject_reason: String::new(),
                })),
            })
            .await
            .unwrap();
        let mut att = ClientAttempt::open(&mut good_worker.data, &assign2, &limits, hb)
            .await
            .unwrap();
        assert_eq!(att.inputs.len(), 1);
        let Some(pb::object::Body::Inline(ref input_data)) = att.inputs[0].body else {
            panic!("expected inline input object, got {:?}", att.inputs[0]);
        };
        assert_eq!(input_data.as_ref(), large_input.as_slice());
        assert_eq!(att.params.as_ref(), b"params-v1");
        att.finish(
            &[
                pb::Object {
                    body: Some(pb::object::Body::Inline(Bytes::from(large_output.clone()))),
                    ..Default::default()
                },
                pb::Object {
                    body: Some(pb::object::Body::Uri("r2://extra/out.bin".into())),
                    ..Default::default()
                },
            ],
            pb::finish::Outcome::Succeeded(pb::Succeeded {}),
        )
        .await
        .unwrap();
        let winning_lease_id = assign2.lease_id.clone();

        // Wait until job finishes and is evicted (max_terminal_jobs = 0).
        let mut events = cp2.runtime.watch("r2-job-1", 0).unwrap();
        while let Some(ev) = events.next().await {
            if matches!(ev.unwrap().kind, EventKind::Finished { .. }) {
                break;
            }
        }
        assert!(
            cp2.runtime.spec("r2-job-1").is_err(),
            "job must be evicted from memory"
        );

        // 2. Preview reads only 4096 bytes via R2 Range without consuming result_budget.
        let before_reads = {
            let db = mock.db.lock().await;
            assert_eq!(db.r2.len(), 2, "2 MiB input + 2 MiB output in R2");
            db.r2_full_reads.len()
        };
        let before_budget = cp2.runtime.stats().result_bytes;
        let preview = cp2.runtime.inspect_job_preview("r2-job-1").await.unwrap();
        assert_eq!(preview.outputs.len(), 2);
        assert_eq!(preview.outputs[0].size_bytes, Some(2 * 1024 * 1024));
        assert_eq!(
            preview.outputs[0].prefix.inline_len(),
            INSPECTION_PREVIEW_BYTES
        );
        assert!(matches!(
            &preview.outputs[1].prefix,
            Object::Uri { uri, .. } if uri == "r2://extra/out.bin"
        ));
        assert_eq!(preview.outputs[1].size_bytes, None);
        assert_eq!(cp2.runtime.stats().result_bytes, before_budget);
        {
            let db = mock.db.lock().await;
            assert_eq!(db.r2_full_reads.len(), before_reads);
            assert!(
                db.r2_range_reads
                    .iter()
                    .any(|(k, off, len)| k.contains("output") && *off == 0 && *len == 4096),
                "expected 4096-byte R2 Range read, got {:?}",
                db.r2_range_reads
            );
        }

        // 3. Result budget enforcement on cold load_job.
        let journal = cp2.runtime.inner.journal.as_ref().unwrap();
        let small_budget = Arc::new(ByteBudget::new(2 * 1024 * 1024));
        let loaded = journal
            .load_job("r2-job-1", true, Arc::clone(&small_budget))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(small_budget.used(), 2 * 1024 * 1024);
        let rejected = journal
            .load_job("r2-job-1", true, Arc::clone(&small_budget))
            .await
            .unwrap_err();
        assert_eq!(
            rejected.downcast_ref::<RuntimeError>().unwrap().kind,
            ErrorKind::ResourceExhausted
        );
        drop(loaded);
        assert_eq!(small_budget.used(), 0);

        // 4. spec_hash idempotency + conflict + receipt dedup.
        let dup = cp2
            .runtime
            .submit(SubmitRequest {
                job_id: Some("r2-job-1".into()),
                spec,
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(!dup.created);
        assert_eq!(dup.job.state, JobState::Succeeded);

        let conflict = cp2
            .runtime
            .submit(SubmitRequest {
                job_id: Some("r2-job-1".into()),
                spec: durable_spec("r2.v1", Object::inline(b"other".as_slice())),
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert_eq!(conflict.kind, ErrorKind::Conflict);

        let receipt = journal.receipt(&winning_lease_id).await.unwrap().unwrap();
        assert_eq!(receipt.0, "r2-job-1");
        assert!(!receipt.1.is_empty());
        assert!(journal.receipt(&crashed_lease_id).await.unwrap().is_none());

        // 5. Multi-namespace scan_jobs cursor pagination + purge cleaning R2 objects.
        let from_ms = crate::now_ms().saturating_sub(60_000);
        let mut expected_ns_alpha = Vec::new();
        for i in 0..5 {
            let job_id = format!("alpha-job-{i}");
            cp2.runtime
                .submit(SubmitRequest {
                    namespace: Some("alpha".into()),
                    job_id: Some(job_id.clone()),
                    spec: durable_spec("r2.v1", Object::inline(b"tiny".as_slice())),
                    ..Default::default()
                })
                .await
                .unwrap();
            cp2.runtime
                .cancel_in("alpha", &job_id, "done".into())
                .await
                .unwrap();
            expected_ns_alpha.push(("alpha".to_string(), job_id));
        }
        let to_ms = crate::now_ms() + 60_000;
        expected_ns_alpha.sort();

        let mut cursor = None;
        let mut got_alpha = Vec::new();
        loop {
            let page = cp2
                .runtime
                .scan_job_metadata(JobScan {
                    archived: true,
                    from_ms,
                    to_ms,
                    limit: 2,
                    cursor,
                    namespace: Some("alpha".into()),
                })
                .await
                .unwrap();
            for j in page.jobs {
                got_alpha.push((j.namespace, j.job_id));
            }
            cursor = page.next_page_token;
            if cursor.is_none() {
                break;
            }
        }
        got_alpha.sort();
        assert_eq!(got_alpha, expected_ns_alpha);

        // Purge everything and confirm R2 objects were deleted too.
        let purged = journal
            .purge(cp2.runtime.generation(), crate::now_ms() + 60_000)
            .await
            .unwrap();
        assert!(purged >= 6);
        {
            let db = mock.db.lock().await;
            assert!(db.r2.is_empty(), "purge must delete R2 objects");
            assert!(db.jobs.is_empty());
        }

        drop(good_worker);
        cp2.runtime.shutdown().await;
    }

    #[tokio::test]
    async fn batch_isolation_and_transport_verification() {
        let mock = MockServer::start(1024 * 1024, None).await;
        let journal =
            CloudflareJournal::open_concrete(&cf_journal_config(&mock.endpoint, 5_000, 50, None))
                .await
                .unwrap();
        let leadership = journal.try_acquire_leadership().await.unwrap().unwrap();
        let term = leadership.term;

        let accept = |id: &str| {
            JournalOp::Accept(Box::new(AcceptRecord {
                job_id: id.into(),
                spec_hash: format!("hash-{id}"),
                spec: Arc::new(JobSpec::default()),
                max_attempts: 3,
                created_at_ms: 100,
            }))
        };

        assert_eq!(
            journal.enqueue(term, accept("j1")).unwrap().await.unwrap(),
            CommitOutcome::Committed
        );

        // Batch containing valid j2 + duplicate j1 -> rolls back batch, isolates j1, commits j2.
        let r_bad = journal.enqueue(term, accept("j1")).unwrap();
        let r_good = journal.enqueue(term, accept("j2")).unwrap();
        assert!(matches!(r_bad.await.unwrap(), CommitOutcome::RolledBack(_)));
        assert_eq!(r_good.await.unwrap(), CommitOutcome::Committed);
        assert_eq!(
            journal.spec_hash("j2").await.unwrap().as_deref(),
            Some("hash-j2")
        );

        // Server commits j3 then drops response with HTTP 500 -> verify_op resolves Committed.
        mock.db.lock().await.force_commit_500_after_apply = true;
        assert_eq!(
            journal.enqueue(term, accept("j3")).unwrap().await.unwrap(),
            CommitOutcome::Committed
        );

        // Server fails before commit with HTTP 500 -> verify_op resolves RolledBack.
        mock.db.lock().await.force_commit_500_without_apply = true;
        assert!(matches!(
            journal.enqueue(term, accept("j4")).unwrap().await.unwrap(),
            CommitOutcome::RolledBack(_)
        ));
        assert!(journal.spec_hash("j4").await.unwrap().is_none());

        // Total outage on commit -> verify_op also fails -> Unknown + lost_tx signaled.
        let mut lost_rx = leadership.lease.lost();
        mock.db.lock().await.reject_all_http = true;
        assert!(matches!(
            journal.enqueue(term, accept("j5")).unwrap().await.unwrap(),
            CommitOutcome::Unknown(_)
        ));
        tokio::time::timeout(Duration::from_secs(1), lost_rx.changed())
            .await
            .expect("Unknown outcome must signal lease loss")
            .unwrap();
        assert!(*lost_rx.borrow());

        mock.db.lock().await.reject_all_http = false;
        journal.close().await;
        leadership.lease.relinquish().await;
    }

    #[tokio::test]
    async fn batched_purge_loops_until_under_batch_limit() {
        let mock = MockServer::start(1024 * 1024, None).await;
        let journal =
            CloudflareJournal::open_concrete(&cf_journal_config(&mock.endpoint, 5_000, 50, None))
                .await
                .unwrap();
        let leadership = journal.try_acquire_leadership().await.unwrap().unwrap();
        let term = leadership.term;

        // Seed 520 expired terminal jobs (> PURGE_BATCH = 500) directly in MockDb.
        {
            let mut db = mock.db.lock().await;
            for i in 0..520 {
                let job_id = format!("old-{i:04}");
                db.jobs.insert(
                    job_id.clone(),
                    MockJobRow {
                        job_id,
                        namespace: "default".into(),
                        state: "succeeded".into(),
                        spec_hash: "h".into(),
                        spec_json: r#"{"demand":{"interface":"x","required_attributes":{},"preferred_attributes":{},"required_capacity":{},"affinity_key":""},"policy":{"durability":"Durable","priority":0,"queue_timeout":null,"execution_timeout":null,"total_timeout":null,"spillover_after":null,"max_attempts":null,"fairness_key":"","concurrency_key":""},"metadata":{}}"#.into(),
                        attempt_count: 1,
                        max_attempts: 3,
                        created_at_ms: 10,
                        updated_at_ms: 50,
                        error_json: None,
                        winning_lease_id: None,
                        result_digest: None,
                        last_attempt_id: None,
                        last_worker_id: None,
                    },
                );
            }
        }

        let deleted = journal.purge(term, 100).await.unwrap();
        assert_eq!(deleted, 520);
        assert!(mock.db.lock().await.jobs.is_empty());

        // Purge with stale term fences and signals lease loss.
        let mut lost_rx = leadership.lease.lost();
        assert!(journal.purge(term + 99, 100).await.is_err());
        // Calling with the active term after lease was dropped/fenced must signal lost.
        mock.db.lock().await.lease = None;
        assert!(journal.purge(term, 100).await.is_err());
        assert!(*lost_rx.borrow_and_update());

        // Also test TerminalRecord commit with AttemptStart + Terminal in sequence.
        let _ = journal.close().await;
        leadership.lease.relinquish().await;
    }

    #[tokio::test]
    async fn commit_batch_records_attempt_start_and_terminal_in_order() {
        let mock = MockServer::start(1024 * 1024, None).await;
        let journal =
            CloudflareJournal::open_concrete(&cf_journal_config(&mock.endpoint, 5_000, 50, None))
                .await
                .unwrap();
        let leadership = journal.try_acquire_leadership().await.unwrap().unwrap();
        let term = leadership.term;

        let accept = JournalOp::Accept(Box::new(AcceptRecord {
            job_id: encode_scoped_id("ns1", "job-a"),
            spec_hash: "spec-a".into(),
            spec: Arc::new(JobSpec::default()),
            max_attempts: 3,
            created_at_ms: 10,
        }));
        let start = JournalOp::AttemptStart(AttemptStartRecord {
            job_id: encode_scoped_id("ns1", "job-a"),
            attempt_number: 1,
            at_ms: 20,
            attempt_id: Some("att-1".into()),
            worker_id: Some("w-1".into()),
        });
        let terminal = JournalOp::Terminal(Box::new(TerminalRecord {
            job_id: encode_scoped_id("ns1", "job-a"),
            state: JobState::Failed,
            outputs: Vec::new(),
            error: Some(ErrorInfo::new("err", "boom", false)),
            attempt_id: Some("att-1".into()),
            worker_id: Some("w-1".into()),
            winning_lease_id: None,
            outcome_digest: None,
            at_ms: 30,
        }));

        // Enqueue all three back-to-back; batcher must split same job_id ops across batches!
        let r1 = journal.enqueue(term, accept).unwrap();
        let r2 = journal.enqueue(term, start).unwrap();
        let r3 = journal.enqueue(term, terminal).unwrap();
        assert_eq!(r1.await.unwrap(), CommitOutcome::Committed);
        assert_eq!(r2.await.unwrap(), CommitOutcome::Committed);
        assert_eq!(r3.await.unwrap(), CommitOutcome::Committed);

        let loaded = journal
            .load_job(
                &encode_scoped_id("ns1", "job-a"),
                false,
                Arc::new(ByteBudget::new(1024)),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded.state, JobState::Failed);
        assert_eq!(loaded.attempt_count, 1);
        assert_eq!(loaded.last_worker_id.as_deref(), Some("w-1"));
        assert!(loaded.failed_workers.contains("w-1"));

        let stats = journal.stats();
        assert_eq!(stats.committed_ops, 3);
        assert_eq!(stats.committed_batches, 3);
        assert_eq!(stats.pending_ops, 0);

        journal.close().await;
        leadership.lease.relinquish().await;
    }

    #[tokio::test]
    async fn runtime_start_selects_cloudflare_backend_and_handles_probe() {
        let mock = MockServer::start(1024 * 1024, None).await;
        let runtime = Runtime::start(RuntimeConfig {
            journal: Some(JournalConfig::cloudflare(&mock.endpoint)),
            ..Default::default()
        })
        .await
        .unwrap();
        assert!(runtime.is_leader_active());
        let probe_rx = runtime
            .inner
            .journal
            .as_ref()
            .unwrap()
            .enqueue(runtime.generation(), JournalOp::Probe)
            .unwrap();
        assert_eq!(probe_rx.await.unwrap(), CommitOutcome::Committed);

        runtime.shutdown().await;
    }

    #[tokio::test]
    async fn invalid_config_and_outage_renewal_loss_behave_accurately() {
        let mut bad_sync = JournalConfig::cloudflare("http://127.0.0.1:9");
        bad_sync.full_sync = false;
        assert!(CloudflareJournal::open(&bad_sync).await.is_err());

        let mut bad_bytes = JournalConfig::cloudflare("http://127.0.0.1:9");
        bad_bytes.max_bytes = Some(4096);
        assert!(CloudflareJournal::open(&bad_bytes).await.is_err());

        assert!(
            CloudflareJournal::open(&JournalConfig::cloudflare("ftp://invalid"))
                .await
                .is_err()
        );
        assert!(
            CloudflareJournal::open(&JournalConfig::cloudflare("   "))
                .await
                .is_err()
        );

        let mut both = JournalConfig::cloudflare("http://127.0.0.1:9");
        both.postgres = Some(crate::runtime::api::PostgresConfig::new("postgres://x/y"));
        assert!(CloudflareJournal::open(&both).await.is_err());

        // Wrong auth token must fail Runtime::start on /v1/acquire.
        let mock = MockServer::start(1024 * 1024, Some("right-token".into())).await;
        let wrong = Runtime::start(RuntimeConfig {
            journal: Some(cf_journal_config(&mock.endpoint, 200, 20, Some("wrong"))),
            ..Default::default()
        })
        .await;
        assert!(wrong.is_err());

        // Consecutive 503s on /v1/renew past lease_duration must trigger wait_for_lease_loss.
        let rt = Runtime::start(RuntimeConfig {
            journal: Some(cf_journal_config(
                &mock.endpoint,
                200,
                20,
                Some("right-token"),
            )),
            ..Default::default()
        })
        .await
        .unwrap();
        assert!(rt.is_leader_active());
        mock.db.lock().await.reject_all_http = true;
        tokio::time::timeout(Duration::from_secs(2), rt.wait_for_lease_loss())
            .await
            .expect("renewal outage past lease_duration must mark lease lost");
        assert!(!rt.health().live);
    }

    #[tokio::test]
    async fn cloudflare_worker_registry_persists_recovers_and_forgets() {
        let mock = MockServer::start(1024 * 1024, Some("cf-wk".into())).await;
        let cfg = cf_journal_config(&mock.endpoint, 5_000, 30, Some("cf-wk"));

        let j1 = CloudflareJournal::open(&cfg).await.unwrap();
        let l1 = j1.try_acquire_leadership().await.unwrap().unwrap();
        assert!(l1.recovery.workers.is_empty());

        let rec1 = crate::runtime::api::WorkerRecord {
            namespace: "default".into(),
            worker_id: "cf-gpu-1".into(),
            pool: "edge".into(),
            interfaces: vec!["demo.echo.v1".into()],
            max_concurrency: 4,
            first_seen_ms: 100,
            last_seen_ms: 200,
            last_session_id: "sess-1".into(),
            last_connected_at_ms: 100,
            offline_since_ms: Some(250),
            offline_reason: Some("disconnected".into()),
            ..Default::default()
        };
        let rec2 = crate::runtime::api::WorkerRecord {
            namespace: "tenant-a".into(),
            worker_id: "cf-gpu-2".into(),
            pool: "edge".into(),
            interfaces: vec!["demo.echo.v1".into()],
            max_concurrency: 2,
            first_seen_ms: 300,
            last_seen_ms: 400,
            last_session_id: "sess-2".into(),
            last_connected_at_ms: 300,
            offline_since_ms: Some(450),
            offline_reason: Some("worker session ended".into()),
            ..Default::default()
        };
        assert_eq!(
            j1.enqueue(l1.term, JournalOp::WorkerUpsert(Box::new(rec1.clone())))
                .unwrap()
                .await
                .unwrap(),
            CommitOutcome::Committed
        );
        assert_eq!(
            j1.enqueue(l1.term, JournalOp::WorkerUpsert(Box::new(rec2.clone())))
                .unwrap()
                .await
                .unwrap(),
            CommitOutcome::Committed
        );
        assert_eq!(
            j1.enqueue(
                l1.term,
                JournalOp::WorkerForget {
                    namespace: "tenant-a".into(),
                    worker_id: "cf-gpu-2".into(),
                }
            )
            .unwrap()
            .await
            .unwrap(),
            CommitOutcome::Committed
        );
        l1.lease.relinquish().await;
        j1.close().await;

        let j2 = CloudflareJournal::open(&cfg).await.unwrap();
        let l2 = j2.try_acquire_leadership().await.unwrap().unwrap();
        assert_eq!(l2.recovery.workers, vec![rec1]);
        l2.lease.relinquish().await;
        j2.close().await;
    }
}
