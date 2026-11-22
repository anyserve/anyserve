use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use anyserve_client::execution::{Attempt, AttemptClient, ExecutionClient, WorkerSession, v2};
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, header::AUTHORIZATION};
use axum::routing::post;
use axum::{Json, Router};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde::{Deserialize, Serialize};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, oneshot};
use tonic::Code;

const CLIENT_TOKEN: &str = "e2e-client-tok";
const WORKER_TOKEN: &str = "e2e-worker-tok";
const JOURNAL_TOKEN: &str = "e2e-journal-tok";
const R2_SPILL_THRESHOLD: usize = 1_048_576; // 1 MiB

static TEMP_SEQ: AtomicU64 = AtomicU64::new(0);

fn temp_config_path(label: &str) -> PathBuf {
    let seq = TEMP_SEQ.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "anyserve-e2e-{label}-{}-{seq}.toml",
        std::process::id()
    ))
}

async fn reserve_free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap().port()
}

// ---------------------------------------------------------------------------
// Contract C Cloudflare Durable Object SQLite + R2 HTTP Journal Mock
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WireObject {
    uri: Option<String>,
    data_b64: Option<String>,
    #[serde(default)]
    metadata: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WireObjectPreview {
    uri: Option<String>,
    head_b64: Option<String>,
    size_bytes: Option<usize>,
    #[serde(default)]
    metadata: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WireStoredJob {
    job_id: String,
    namespace: String,
    state: String,
    spec_hash: String,
    spec_json: String,
    inputs: Vec<WireObject>,
    params_b64: String,
    attempt_count: u32,
    max_attempts: u32,
    created_at_ms: u64,
    updated_at_ms: u64,
    error_json: Option<String>,
    outputs: Vec<WireObject>,
    last_attempt_id: Option<String>,
    last_worker_id: Option<String>,
    failed_workers: Vec<String>,
}

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
    metadata: BTreeMap<String, String>,
}

struct MockJournalState {
    lease: Option<MockLease>,
    last_term: u64,
    jobs: BTreeMap<String, MockJobRow>,
    objects: BTreeMap<(String, String, usize), MockObjectRow>,
    failed_workers: BTreeMap<String, BTreeSet<String>>,
    r2: BTreeMap<String, Vec<u8>>,
    r2_spill_threshold: usize,
    expected_token: String,
    r2_full_reads: Vec<String>,
    r2_range_reads: Vec<(String, usize, usize)>,
}

impl MockJournalState {
    fn new() -> Self {
        Self {
            lease: None,
            last_term: 0,
            jobs: BTreeMap::new(),
            objects: BTreeMap::new(),
            failed_workers: BTreeMap::new(),
            r2: BTreeMap::new(),
            r2_spill_threshold: R2_SPILL_THRESHOLD,
            expected_token: JOURNAL_TOKEN.to_owned(),
            r2_full_reads: Vec::new(),
            r2_range_reads: Vec::new(),
        }
    }

    fn lease_valid(&self, holder_id: &str, term: u64) -> bool {
        self.lease.as_ref().is_some_and(|lease| {
            lease.holder_id == holder_id && lease.term == term && lease.expires_at > Instant::now()
        })
    }

    fn read_wire_object(&mut self, obj: &MockObjectRow) -> WireObject {
        if let Some(uri) = &obj.uri {
            return WireObject {
                uri: Some(uri.clone()),
                data_b64: None,
                metadata: obj.metadata.clone(),
            };
        }
        let bytes = if let Some(key) = &obj.r2_key {
            self.r2_full_reads.push(key.clone());
            self.r2.get(key).expect("missing R2 key").clone()
        } else {
            obj.inline_content.clone().unwrap_or_default()
        };
        WireObject {
            uri: None,
            data_b64: Some(BASE64.encode(&bytes)),
            metadata: obj.metadata.clone(),
        }
    }

    fn read_wire_preview(
        &mut self,
        obj: &MockObjectRow,
        preview_bytes: usize,
    ) -> WireObjectPreview {
        if let Some(uri) = &obj.uri {
            return WireObjectPreview {
                uri: Some(uri.clone()),
                head_b64: None,
                size_bytes: None,
                metadata: obj.metadata.clone(),
            };
        }
        let head = if let Some(key) = &obj.r2_key {
            let data = self.r2.get(key).expect("missing R2 key");
            let len = data.len().min(preview_bytes);
            self.r2_range_reads.push((key.clone(), 0, len));
            data[..len].to_vec()
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
        let keys: Vec<(String, String, usize)> = self
            .objects
            .keys()
            .filter(|(id, _, _)| id == &row.job_id)
            .cloned()
            .collect();
        let mut inputs = Vec::new();
        let mut params_b64 = String::new();
        let mut outputs = Vec::new();
        for key in keys {
            let obj = self.objects.get(&key).cloned().unwrap();
            match obj.role.as_str() {
                "input" if include_inputs => inputs.push(self.read_wire_object(&obj)),
                "params" if include_inputs && obj.idx == 0 => {
                    let bytes = if let Some(r2_key) = &obj.r2_key {
                        self.r2_full_reads.push(r2_key.clone());
                        self.r2.get(r2_key).expect("missing R2 params").clone()
                    } else {
                        obj.inline_content.unwrap_or_default()
                    };
                    params_b64 = BASE64.encode(&bytes);
                }
                "output" if include_outputs => outputs.push(self.read_wire_object(&obj)),
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

    fn store_object(
        threshold: usize,
        r2_puts: &mut Vec<(String, Vec<u8>)>,
        job_id: &str,
        role: &str,
        idx: usize,
        obj: WireObject,
    ) -> MockObjectRow {
        if let Some(uri) = obj.uri {
            return MockObjectRow {
                job_id: job_id.to_owned(),
                role: role.to_owned(),
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
        if size_bytes > threshold {
            let key = format!("jobs/{job_id}/{role}/{idx}");
            r2_puts.push((key.clone(), bytes));
            MockObjectRow {
                job_id: job_id.to_owned(),
                role: role.to_owned(),
                idx,
                uri: None,
                inline_content: None,
                r2_key: Some(key),
                size_bytes,
                metadata: obj.metadata,
            }
        } else {
            MockObjectRow {
                job_id: job_id.to_owned(),
                role: role.to_owned(),
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

fn check_journal_auth(
    db: &MockJournalState,
    headers: &HeaderMap,
) -> Result<(), (StatusCode, String)> {
    let provided = headers
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    if provided != Some(db.expected_token.as_str()) {
        return Err((
            StatusCode::UNAUTHORIZED,
            "unauthorized journal caller".into(),
        ));
    }
    Ok(())
}

#[derive(Deserialize)]
struct AcquireReq {
    holder_id: String,
    lease_duration_ms: u64,
}

#[derive(Deserialize)]
struct RenewReq {
    holder_id: String,
    term: u64,
    lease_duration_ms: u64,
}

#[derive(Deserialize)]
struct RelinquishReq {
    holder_id: String,
    term: u64,
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum CommitOp {
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
        #[allow(dead_code)]
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
    Probe,
}

#[derive(Deserialize)]
struct CommitBatchReq {
    holder_id: String,
    term: u64,
    ops: Vec<CommitOp>,
}

#[derive(Deserialize)]
struct SpecHashReq {
    job_id: String,
}

#[derive(Deserialize)]
struct ReceiptReq {
    lease_id: String,
}

#[derive(Deserialize)]
struct LoadJobReq {
    job_id: String,
    include_inputs: bool,
    include_outputs: bool,
    max_output_bytes: Option<u64>,
}

#[derive(Deserialize)]
struct LoadJobPreviewReq {
    job_id: String,
    preview_bytes: usize,
}

#[derive(Deserialize)]
struct ScanCursor {
    created_at_ms: u64,
    job_id: String,
}

#[derive(Deserialize)]
struct ScanJobsReq {
    from_ms: u64,
    to_ms: u64,
    before: Option<ScanCursor>,
    namespace: Option<String>,
    limit: usize,
}

#[derive(Deserialize)]
struct PurgeReq {
    holder_id: String,
    term: u64,
    before_ms: u64,
    batch_limit: usize,
}

async fn j_acquire(
    State(state): State<Arc<Mutex<MockJournalState>>>,
    headers: HeaderMap,
    Json(req): Json<AcquireReq>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let mut db = state.lock().await;
    check_journal_auth(&db, &headers)?;
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
    Ok(Json(serde_json::json!({
        "acquired": true,
        "term": term,
        "active": active,
    })))
}

async fn j_renew(
    State(state): State<Arc<Mutex<MockJournalState>>>,
    headers: HeaderMap,
    Json(req): Json<RenewReq>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let mut db = state.lock().await;
    check_journal_auth(&db, &headers)?;
    if db.lease_valid(&req.holder_id, req.term) {
        if let Some(lease) = &mut db.lease {
            lease.expires_at = Instant::now() + Duration::from_millis(req.lease_duration_ms);
        }
        return Ok(Json(serde_json::json!({ "ok": true })));
    }
    Ok(Json(serde_json::json!({ "ok": false })))
}

async fn j_relinquish(
    State(state): State<Arc<Mutex<MockJournalState>>>,
    headers: HeaderMap,
    Json(req): Json<RelinquishReq>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let mut db = state.lock().await;
    check_journal_auth(&db, &headers)?;
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

async fn j_commit_batch(
    State(state): State<Arc<Mutex<MockJournalState>>>,
    headers: HeaderMap,
    Json(req): Json<CommitBatchReq>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let mut db = state.lock().await;
    check_journal_auth(&db, &headers)?;
    if !db.lease_valid(&req.holder_id, req.term) {
        return Ok(Json(serde_json::json!({
            "outcome": "fenced",
            "reason": "lease expired or superseded"
        })));
    }
    let mut jobs = db.jobs.clone();
    let mut objects = db.objects.clone();
    let mut failed_workers = db.failed_workers.clone();
    let mut r2_puts = Vec::new();
    let threshold = db.r2_spill_threshold;

    for op in req.ops {
        match op {
            CommitOp::Accept {
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
                        "reason": format!("duplicate job {job_id}")
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
                    let row = MockJournalState::store_object(
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
                    let row = MockJournalState::store_object(
                        threshold,
                        &mut r2_puts,
                        &job_id,
                        "params",
                        0,
                        WireObject {
                            uri: None,
                            data_b64: Some(params_b64),
                            metadata: BTreeMap::new(),
                        },
                    );
                    objects.insert((job_id.clone(), "params".into(), 0), row);
                }
            }
            CommitOp::AttemptStart {
                job_id,
                at_ms,
                attempt_id,
                worker_id,
                ..
            } => {
                let Some(job) = jobs.get_mut(&job_id) else {
                    return Ok(Json(serde_json::json!({
                        "outcome": "rolled_back",
                        "reason": format!("missing job {job_id}")
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
                if let Some(worker) = worker_id {
                    job.last_worker_id = Some(worker.clone());
                    failed_workers.entry(job_id).or_default().insert(worker);
                }
            }
            CommitOp::Terminal {
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
                        "reason": format!("missing job {job_id}")
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
                    let row = MockJournalState::store_object(
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
            CommitOp::Probe => {}
        }
    }

    for (key, bytes) in r2_puts {
        db.r2.insert(key, bytes);
    }
    db.jobs = jobs;
    db.objects = objects;
    db.failed_workers = failed_workers;
    Ok(Json(serde_json::json!({ "outcome": "committed" })))
}

async fn j_spec_hash(
    State(state): State<Arc<Mutex<MockJournalState>>>,
    headers: HeaderMap,
    Json(req): Json<SpecHashReq>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let db = state.lock().await;
    check_journal_auth(&db, &headers)?;
    let spec_hash = db.jobs.get(&req.job_id).map(|j| j.spec_hash.clone());
    Ok(Json(serde_json::json!({ "spec_hash": spec_hash })))
}

async fn j_receipt(
    State(state): State<Arc<Mutex<MockJournalState>>>,
    headers: HeaderMap,
    Json(req): Json<ReceiptReq>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let db = state.lock().await;
    check_journal_auth(&db, &headers)?;
    let receipt = db.jobs.values().find_map(|j| {
        (j.winning_lease_id.as_deref() == Some(req.lease_id.as_str())).then(|| {
            j.result_digest.as_ref().map(|digest| {
                serde_json::json!({
                    "job_id": j.job_id,
                    "result_digest": digest,
                })
            })
        })?
    });
    Ok(Json(serde_json::json!({ "receipt": receipt })))
}

async fn j_load_job(
    State(state): State<Arc<Mutex<MockJournalState>>>,
    headers: HeaderMap,
    Json(req): Json<LoadJobReq>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let mut db = state.lock().await;
    check_journal_auth(&db, &headers)?;
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
    let job = db.job_to_wire(
        &row,
        req.include_inputs,
        req.include_outputs && row.state == "succeeded",
    );
    Ok(Json(serde_json::json!({
        "job": job,
        "budget_exceeded": false,
        "required_bytes": required_bytes,
    })))
}

async fn j_load_job_preview(
    State(state): State<Arc<Mutex<MockJournalState>>>,
    headers: HeaderMap,
    Json(req): Json<LoadJobPreviewReq>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let mut db = state.lock().await;
    check_journal_auth(&db, &headers)?;
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
    let keys: Vec<(String, String, usize)> = db
        .objects
        .keys()
        .filter(|(id, _, _)| id == &req.job_id)
        .cloned()
        .collect();
    let mut input_previews = Vec::new();
    let mut params_head_b64 = String::new();
    let mut params_size_bytes = 0usize;
    let mut output_previews = Vec::new();
    for key in keys {
        let obj = db.objects.get(&key).cloned().unwrap();
        match obj.role.as_str() {
            "input" => input_previews.push(db.read_wire_preview(&obj, req.preview_bytes)),
            "params" if obj.idx == 0 => {
                let preview = db.read_wire_preview(&obj, req.preview_bytes);
                params_head_b64 = preview.head_b64.unwrap_or_default();
                params_size_bytes = preview.size_bytes.unwrap_or(0);
            }
            "output" if row.state == "succeeded" => {
                output_previews.push(db.read_wire_preview(&obj, req.preview_bytes));
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

async fn j_scan_jobs(
    State(state): State<Arc<Mutex<MockJournalState>>>,
    headers: HeaderMap,
    Json(req): Json<ScanJobsReq>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let mut db = state.lock().await;
    check_journal_auth(&db, &headers)?;
    let mut rows: Vec<MockJobRow> = db
        .jobs
        .values()
        .filter(|j| {
            matches!(j.state.as_str(), "succeeded" | "failed" | "cancelled")
                && j.created_at_ms >= req.from_ms
                && j.created_at_ms < req.to_ms
                && req.namespace.as_deref().is_none_or(|ns| j.namespace == ns)
                && req
                    .before
                    .as_ref()
                    .is_none_or(|b| (j.created_at_ms, &j.job_id) < (b.created_at_ms, &b.job_id))
        })
        .cloned()
        .collect();
    rows.sort_by(|a, b| (b.created_at_ms, &b.job_id).cmp(&(a.created_at_ms, &a.job_id)));
    rows.truncate(req.limit);
    let jobs: Vec<WireStoredJob> = rows
        .iter()
        .map(|row| db.job_to_wire(row, false, false))
        .collect();
    Ok(Json(serde_json::json!({ "jobs": jobs })))
}

async fn j_purge(
    State(state): State<Arc<Mutex<MockJournalState>>>,
    headers: HeaderMap,
    Json(req): Json<PurgeReq>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let mut db = state.lock().await;
    check_journal_auth(&db, &headers)?;
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
        for key in obj_keys {
            if let Some(row) = db.objects.remove(&key)
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

struct MockJournalGateway {
    db: Arc<Mutex<MockJournalState>>,
    endpoint: String,
    _stop: oneshot::Sender<()>,
}

impl MockJournalGateway {
    async fn start() -> Self {
        let db = Arc::new(Mutex::new(MockJournalState::new()));
        let app = Router::new()
            .route("/v1/acquire", post(j_acquire))
            .route("/v1/renew", post(j_renew))
            .route("/v1/relinquish", post(j_relinquish))
            .route("/v1/commit_batch", post(j_commit_batch))
            .route("/v1/spec_hash", post(j_spec_hash))
            .route("/v1/receipt", post(j_receipt))
            .route("/v1/load_job", post(j_load_job))
            .route("/v1/load_job_preview", post(j_load_job_preview))
            .route("/v1/scan_jobs", post(j_scan_jobs))
            .route("/v1/purge", post(j_purge))
            .layer(axum::extract::DefaultBodyLimit::max(128 * 1024 * 1024))
            .with_state(Arc::clone(&db));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = oneshot::channel::<()>();
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
            _stop: tx,
        }
    }
}

// ---------------------------------------------------------------------------
// Real `anyserve serve` single-port control-plane process harness
// ---------------------------------------------------------------------------

struct ControlPlaneProc {
    child: Child,
    config_path: PathBuf,
    port: u16,
    grpc_port: u16,
    http_base: String,
    ws_base: String,
}

impl ControlPlaneProc {
    async fn start(
        label: &str,
        journal_endpoint: &str,
        max_terminal_jobs: usize,
        expect_ready: bool,
    ) -> Self {
        let port = reserve_free_port().await;
        let grpc_port = reserve_free_port().await;
        let config_path = temp_config_path(label);
        let toml = format!(
            r#"[server]
grpc_enabled = false
grpc_port = {grpc_port}

[console]
enabled = true
listen = "127.0.0.1:{port}"
public_endpoint = "wss://edge.example.com"

[auth]
client_tokens = ["{CLIENT_TOKEN}"]
worker_tokens = ["{WORKER_TOKEN}"]

[journal]
enabled = true
backend = "cloudflare"
endpoint = "{journal_endpoint}"
token_env = "ANYSERVE_TEST_JOURNAL_TOKEN"
lease_duration_ms = 2000
poll_interval_ms = 50
r2_spill_threshold_bytes = {R2_SPILL_THRESHOLD}

[runtime]
heartbeat_timeout_ms = 2000
max_terminal_jobs = {max_terminal_jobs}
max_pending_bytes = 67108864
max_result_bytes = 67108864
max_event_bytes = 33554432
max_inline_object_bytes = 8388608
max_inline_total_bytes = 25165824
"#
        );
        std::fs::write(&config_path, toml).unwrap();

        let child = Command::new(env!("CARGO_BIN_EXE_anyserve"))
            .args(["serve", "--config", config_path.to_str().unwrap()])
            .env("ANYSERVE_TEST_JOURNAL_TOKEN", JOURNAL_TOKEN)
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn anyserve binary");

        let http_base = format!("http://127.0.0.1:{port}");
        let ws_base = format!("ws://127.0.0.1:{port}");
        let http = reqwest::Client::new();
        let target_path = if expect_ready { "/readyz" } else { "/healthz" };
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Ok(resp) = http.get(format!("{http_base}{target_path}")).send().await
                && resp.status() == reqwest::StatusCode::NO_CONTENT
            {
                break;
            }
            if Instant::now() >= deadline {
                panic!("control plane '{label}' did not become live at {http_base}{target_path}");
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        Self {
            child,
            config_path,
            port,
            grpc_port,
            http_base,
            ws_base,
        }
    }

    fn stop(mut self) {
        #[cfg(unix)]
        {
            let pid = self.child.id().to_string();
            let _ = Command::new("kill").args(["-TERM", &pid]).status();
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                if let Ok(Some(_)) = self.child.try_wait() {
                    return;
                }
                if Instant::now() >= deadline {
                    break;
                }
                std::thread::sleep(Duration::from_millis(25));
            }
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for ControlPlaneProc {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_file(&self.config_path);
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn job_spec(interface: &str, payload: Vec<u8>, durable: bool) -> v2::JobSpec {
    v2::JobSpec {
        demand: Some(v2::Demand {
            interface: interface.to_owned(),
            ..Default::default()
        }),
        inputs: if payload.is_empty() {
            Vec::new()
        } else {
            vec![v2::Object {
                body: Some(v2::object::Body::Inline(payload.into())),
                metadata: HashMap::new(),
            }]
        },
        policy: Some(v2::ExecutionPolicy {
            durability: (if durable {
                v2::Durability::Durable
            } else {
                v2::Durability::Ephemeral
            }) as i32,
            ..Default::default()
        }),
        ..Default::default()
    }
}

async fn connect_worker(
    endpoint: &str,
    worker_id: &str,
    interfaces: &[&str],
) -> (WorkerSession, v2::Limits, Duration) {
    let mut session = WorkerSession::connect(
        endpoint,
        Some(WORKER_TOKEN),
        v2::Hello {
            worker_id: worker_id.to_owned(),
            interfaces: interfaces.iter().map(|s| (*s).to_owned()).collect(),
            max_concurrency: 4,
            total_capacity: HashMap::from([("slot".to_owned(), 4)]),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let Some(v2::server_message::Msg::Welcome(welcome)) =
        session.messages.message().await.unwrap().unwrap().msg
    else {
        panic!("expected Welcome from worker session");
    };
    session
        .control
        .send(v2::WorkerMessage {
            msg: Some(v2::worker_message::Msg::Credit(v2::Credit {
                free_slots: 4,
            })),
        })
        .await
        .unwrap();
    let limits = welcome.limits.unwrap();
    let hb_timeout = Duration::from_millis(u64::from(welcome.heartbeat_timeout_ms));
    (session, limits, hb_timeout)
}

async fn spawn_counting_tcp_proxy(
    target_port: u16,
) -> (String, Arc<AtomicUsize>, oneshot::Sender<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_addr = listener.local_addr().unwrap();
    let target_addr = format!("127.0.0.1:{target_port}");
    let accepts = Arc::new(AtomicUsize::new(0));
    let accepts_bg = Arc::clone(&accepts);
    let (stop_tx, mut stop_rx) = oneshot::channel::<()>();

    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut stop_rx => break,
                accepted = listener.accept() => {
                    let Ok((mut inbound, _)) = accepted else {
                        break;
                    };
                    accepts_bg.fetch_add(1, Ordering::SeqCst);
                    let target = target_addr.clone();
                    tokio::spawn(async move {
                        if let Ok(mut outbound) = TcpStream::connect(&target).await {
                            let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
                        }
                    });
                }
            }
        }
    });

    (format!("ws://{proxy_addr}"), accepts, stop_tx)
}

// ---------------------------------------------------------------------------
// Cross-Stack E2E Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn single_port_ws_execution_streaming_cancel_large_payload_and_auth_roles() {
    let journal = MockJournalGateway::start().await;
    let cp = ControlPlaneProc::start("e2e-main", &journal.endpoint, 100, true).await;

    // 1. Verify gRPC port is NOT bound when grpc_enabled = false, while console overview reports memory_plus_cloudflare.
    assert!(
        TcpStream::connect(("127.0.0.1", cp.grpc_port))
            .await
            .is_err(),
        "gRPC port must not listen when grpc_enabled = false"
    );
    let http = reqwest::Client::new();
    let overview: serde_json::Value = http
        .get(format!("{}/api/overview", cp.http_base))
        .bearer_auth(CLIENT_TOKEN)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(overview["runtime"]["code"], "memory_plus_cloudflare");
    assert_eq!(overview["runtime"]["journal"], "cloudflare");
    assert_eq!(
        overview["control_plane"]["public_endpoint"],
        "wss://edge.example.com"
    );

    // 2. Auth role enforcement over single-port HTTP+WS:
    //    - Worker token on Client RPCs -> PermissionDenied
    //    - Client token on Worker WS -> PermissionDenied
    //    - Missing token -> Unauthenticated
    let mut worker_as_client = ExecutionClient::connect_with_token(&cp.ws_base, Some(WORKER_TOKEN))
        .await
        .unwrap();
    assert_eq!(
        worker_as_client
            .submit(None, job_spec("echo.unary", b"hi".to_vec(), false))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    assert_eq!(
        worker_as_client
            .get("missing", false)
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    assert_eq!(
        worker_as_client
            .cancel("missing", "nope")
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    assert_eq!(
        worker_as_client
            .list(v2::ListJobsRequest::default())
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    assert_eq!(
        worker_as_client
            .invoke(job_spec("echo.unary", b"hi".to_vec(), false))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    assert_eq!(
        worker_as_client
            .watch("missing", 0)
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );

    let client_as_worker_err = WorkerSession::connect(
        &cp.ws_base,
        Some(CLIENT_TOKEN),
        v2::Hello {
            worker_id: "bad-role".into(),
            interfaces: vec!["echo.unary".into()],
            max_concurrency: 1,
            ..Default::default()
        },
    )
    .await
    .err()
    .expect("client token must be denied on worker session WS");
    assert_eq!(
        client_as_worker_err
            .downcast_ref::<tonic::Status>()
            .unwrap()
            .code(),
        Code::PermissionDenied
    );

    let mut client_attempt = AttemptClient::connect(&cp.ws_base, Some(CLIENT_TOKEN))
        .await
        .unwrap();
    assert_eq!(
        client_attempt
            .open("lease-1".into(), 1, 0)
            .await
            .err()
            .expect("client token must be denied on worker attempt WS")
            .code(),
        Code::PermissionDenied
    );

    let mut unauth_client = ExecutionClient::connect_with_token(&cp.ws_base, None)
        .await
        .unwrap();
    assert_eq!(
        unauth_client
            .list(v2::ListJobsRequest::default())
            .await
            .unwrap_err()
            .code(),
        Code::Unauthenticated
    );

    // 3. Connect WS worker and run Submit, Cancel, ListJobs, Watch, Invoke, >3 MiB payload,
    //    and Invoke streaming + mid-stream CancelOnDrop!
    let (mut session, limits, hb_timeout) = connect_worker(
        &cp.ws_base,
        "ws-worker-e2e",
        &["echo.unary", "echo.stream", "echo.hold"],
    )
    .await;
    let cancel_seen = Arc::new(tokio::sync::Notify::new());
    let cancel_seen_bg = Arc::clone(&cancel_seen);
    let worker_task = tokio::spawn(async move {
        let mut active_cancels: HashMap<String, oneshot::Sender<()>> = HashMap::new();
        while let Ok(Some(msg)) = session.messages.message().await {
            let Some(inner) = msg.msg else { continue };
            match inner {
                v2::server_message::Msg::Cancel(cancel) => {
                    if let Some(tx) = active_cancels.remove(&cancel.lease_id) {
                        let _ = tx.send(());
                    }
                    let _ = session
                        .control
                        .send(v2::WorkerMessage {
                            msg: Some(v2::worker_message::Msg::CancelAck(v2::CancelAck {
                                lease_id: cancel.lease_id,
                            })),
                        })
                        .await;
                    cancel_seen_bg.notify_waiters();
                }
                v2::server_message::Msg::Assign(assign) => {
                    session
                        .control
                        .send(v2::WorkerMessage {
                            msg: Some(v2::worker_message::Msg::AssignReply(v2::AssignReply {
                                lease_id: assign.lease_id.clone(),
                                accepted: true,
                                reject_reason: String::new(),
                            })),
                        })
                        .await
                        .unwrap();
                    let mut data = session.data.clone();
                    let control = session.control.clone();
                    let (cancel_tx, cancel_rx) = oneshot::channel::<()>();
                    if assign.interface == "echo.hold" {
                        active_cancels.insert(assign.lease_id.clone(), cancel_tx);
                    }
                    tokio::spawn(async move {
                        match assign.interface.as_str() {
                            "echo.unary" => {
                                let mut attempt =
                                    Attempt::open(&mut data, &assign, &limits, hb_timeout)
                                        .await
                                        .unwrap();
                                let out = attempt.inputs.clone();
                                attempt
                                    .commit(
                                        &mut data,
                                        &assign,
                                        &limits,
                                        &out,
                                        v2::finish::Outcome::Succeeded(v2::Succeeded {}),
                                    )
                                    .await
                                    .unwrap();
                            }
                            "echo.stream" => {
                                let mut attempt =
                                    Attempt::open_stream(&mut data, &assign, &limits, hb_timeout)
                                        .await
                                        .unwrap();
                                let mut collected = Vec::new();
                                while let Some(chunk) = attempt.input_chunk().await.unwrap() {
                                    collected.extend_from_slice(&chunk.data);
                                    attempt
                                        .frame(chunk.data, HashMap::new(), true)
                                        .await
                                        .unwrap();
                                }
                                let out = vec![v2::Object {
                                    body: Some(v2::object::Body::Inline(collected.into())),
                                    metadata: HashMap::new(),
                                }];
                                attempt
                                    .commit(
                                        &mut data,
                                        &assign,
                                        &limits,
                                        &out,
                                        v2::finish::Outcome::Succeeded(v2::Succeeded {}),
                                    )
                                    .await
                                    .unwrap();
                            }
                            "echo.hold" => {
                                let mut attempt =
                                    Attempt::open_stream(&mut data, &assign, &limits, hb_timeout)
                                        .await
                                        .unwrap();
                                attempt
                                    .frame(b"holding".to_vec().into(), HashMap::new(), true)
                                    .await
                                    .unwrap();
                                let _ = cancel_rx.await;
                                let _ = attempt
                                    .commit(
                                        &mut data,
                                        &assign,
                                        &limits,
                                        &[],
                                        v2::finish::Outcome::Cancelled(v2::Cancelled {}),
                                    )
                                    .await;
                            }
                            other => panic!("unexpected interface {other}"),
                        }
                        let _ = control
                            .send(v2::WorkerMessage {
                                msg: Some(v2::worker_message::Msg::Credit(v2::Credit {
                                    free_slots: 1,
                                })),
                            })
                            .await;
                    });
                }
                _ => {}
            }
        }
    });

    let mut client = ExecutionClient::connect_with_token(&cp.ws_base, Some(CLIENT_TOKEN))
        .await
        .unwrap();

    // Submit + Watch + GetJob + > 3 MiB payload (3.5 MiB) over HTTP+WS!
    let big_payload = vec![0xA5u8; 3 * 1024 * 1024 + 512 * 1024];
    let sub = client
        .submit(
            Some("ws-big-submit".into()),
            job_spec("echo.unary", big_payload.clone(), true),
        )
        .await
        .unwrap();
    assert!(sub.created);
    let mut watch = client.watch("ws-big-submit", 0).await.unwrap();
    let mut watch_finished = None;
    while let Some(ev) = watch.message().await.unwrap() {
        if let Some(v2::execution_event::Event::Finished(fin)) = ev.event {
            watch_finished = Some(fin);
            break;
        }
    }
    let fin = watch_finished.expect("watch should deliver Finished");
    assert_eq!(fin.state, v2::JobState::Succeeded as i32);
    assert_eq!(
        fin.outputs[0].body,
        Some(v2::object::Body::Inline(big_payload.clone().into()))
    );

    let fetched = client.get("ws-big-submit", true).await.unwrap();
    assert_eq!(fetched.state, v2::JobState::Succeeded as i32);
    assert_eq!(
        fetched.outputs[0].body,
        Some(v2::object::Body::Inline(big_payload.clone().into()))
    );

    // Invoke with > 3 MiB payload over WS!
    let mut invoke_big = client
        .invoke(job_spec("echo.unary", big_payload.clone(), false))
        .await
        .unwrap();
    let mut invoke_out = None;
    while let Some(ev) = invoke_big.message().await.unwrap() {
        if let Some(v2::execution_event::Event::Finished(fin)) = ev.event {
            invoke_out = Some(fin);
            break;
        }
    }
    assert_eq!(
        invoke_out.unwrap().outputs[0].body,
        Some(v2::object::Body::Inline(big_payload.into()))
    );

    // Cancel + ListJobs over HTTP+WS!
    let cancel_sub = client
        .submit(
            Some("ws-cancel-target".into()),
            job_spec("unclaimed.iface", b"x".to_vec(), false),
        )
        .await
        .unwrap();
    assert!(cancel_sub.created);
    let cancelled = client
        .cancel("ws-cancel-target", "user abort")
        .await
        .unwrap();
    assert_eq!(cancelled.state, v2::JobState::Cancelled as i32);

    let listed = client
        .list(v2::ListJobsRequest {
            limit: 20,
            ..Default::default()
        })
        .await
        .unwrap();
    let ids: BTreeSet<&str> = listed.jobs.iter().map(|j| j.job_id.as_str()).collect();
    assert!(ids.contains("ws-big-submit"));
    assert!(ids.contains("ws-cancel-target"));

    // Invoke streaming round-trip!
    let mut inv_stream = client
        .invoke_stream(job_spec("echo.stream", Vec::new(), false))
        .await
        .unwrap();
    inv_stream.send(b"hello-".to_vec()).await.unwrap();
    inv_stream.send(b"ws".to_vec()).await.unwrap();
    inv_stream.finish_input().await.unwrap();
    let mut streamed_frames = Vec::new();
    let mut stream_final = None;
    while let Some(ev) = inv_stream.events.message().await.unwrap() {
        match ev.event.unwrap() {
            v2::execution_event::Event::Output(out) => streamed_frames.extend_from_slice(&out.data),
            v2::execution_event::Event::Finished(fin) => {
                stream_final = Some(fin);
                break;
            }
            _ => {}
        }
    }
    assert_eq!(streamed_frames, b"hello-ws");
    assert_eq!(
        stream_final.unwrap().outputs[0].body,
        Some(v2::object::Body::Inline(b"hello-ws".to_vec().into()))
    );

    // Invoke streaming mid-stream drop (CancelOnDrop)!
    let mut hold_inv = client
        .invoke_stream(job_spec("echo.hold", Vec::new(), false))
        .await
        .unwrap();
    let mut hold_job_id = None;
    while let Some(ev) = hold_inv.events.message().await.unwrap() {
        hold_job_id = Some(ev.job_id.clone());
        if matches!(ev.event, Some(v2::execution_event::Event::Output(_))) {
            break;
        }
    }
    let hold_job_id = hold_job_id.unwrap();
    drop(hold_inv);
    tokio::time::timeout(Duration::from_secs(3), cancel_seen.notified())
        .await
        .expect("mid-stream WS drop must trigger worker Cancel");
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let job = client.get(&hold_job_id, false).await.unwrap();
            if job.state == v2::JobState::Cancelled as i32 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
    })
    .await
    .expect("dropped streaming invoke must transition to Cancelled");

    worker_task.abort();
    cp.stop();
}

#[tokio::test]
async fn multi_endpoint_standby_to_leader_ws_failover_is_sticky() {
    let journal = MockJournalGateway::start().await;
    let leader = ControlPlaneProc::start("e2e-leader", &journal.endpoint, 50, true).await;
    let standby = ControlPlaneProc::start("e2e-standby", &journal.endpoint, 50, false).await;

    // Confirm standby itself rejects client RPCs with 503 / Code::Unavailable.
    let mut direct_standby =
        ExecutionClient::connect_with_token(&standby.ws_base, Some(CLIENT_TOKEN))
            .await
            .unwrap();
    assert_eq!(
        direct_standby
            .list(v2::ListJobsRequest::default())
            .await
            .unwrap_err()
            .code(),
        Code::Unavailable
    );

    // Wrap standby behind a counting TCP proxy so we can assert sticky failover.
    let (proxy_ws, standby_accepts, stop_proxy) = spawn_counting_tcp_proxy(standby.port).await;
    let endpoints = format!("{proxy_ws},{}", leader.ws_base);

    let mut client = ExecutionClient::connect_with_token(&endpoints, Some(CLIENT_TOKEN))
        .await
        .unwrap();
    let submitted = client
        .submit(
            Some("failover-job".into()),
            job_spec("failover.echo", b"ping".to_vec(), false),
        )
        .await
        .unwrap();
    assert!(submitted.created);
    let accepts_after_failover = standby_accepts.load(Ordering::SeqCst);
    assert!(
        accepts_after_failover >= 1,
        "initial probe/RPC should hit standby before failing over"
    );

    // Subsequent calls (including WS watch/get) must stay pinned to leader without touching standby again.
    let got = client.get("failover-job", false).await.unwrap();
    assert_eq!(got.job_id, "failover-job");
    let listed = client.list(v2::ListJobsRequest::default()).await.unwrap();
    assert!(listed.jobs.iter().any(|j| j.job_id == "failover-job"));
    assert_eq!(
        standby_accepts.load(Ordering::SeqCst),
        accepts_after_failover,
        "client must stick to leader after failover instead of re-probing standby"
    );

    // WorkerSession also fails over from standby (503) to leader!
    let (mut session, limits, hb_timeout) =
        connect_worker(&endpoints, "failover-worker", &["failover.echo"]).await;
    let Some(v2::server_message::Msg::Assign(assign)) =
        session.messages.message().await.unwrap().unwrap().msg
    else {
        panic!("expected assignment for failover-job");
    };
    session
        .control
        .send(v2::WorkerMessage {
            msg: Some(v2::worker_message::Msg::AssignReply(v2::AssignReply {
                lease_id: assign.lease_id.clone(),
                accepted: true,
                reject_reason: String::new(),
            })),
        })
        .await
        .unwrap();
    let mut attempt = Attempt::open(&mut session.data, &assign, &limits, hb_timeout)
        .await
        .unwrap();
    let out = attempt.inputs.clone();
    attempt
        .commit(
            &mut session.data,
            &assign,
            &limits,
            &out,
            v2::finish::Outcome::Succeeded(v2::Succeeded {}),
        )
        .await
        .unwrap();
    let done = client.get("failover-job", true).await.unwrap();
    assert_eq!(done.state, v2::JobState::Succeeded as i32);
    assert_eq!(
        done.outputs[0].body,
        Some(v2::object::Body::Inline(b"ping".to_vec().into()))
    );

    let _ = stop_proxy.send(());
    standby.stop();
    leader.stop();
}

#[tokio::test]
async fn durable_job_with_r2_spilled_payload_recovers_across_restart() {
    let journal = MockJournalGateway::start().await;
    let cp1 = ControlPlaneProc::start("e2e-recover-1", &journal.endpoint, 0, true).await;

    let large_input = vec![0x5Au8; 1_500_000]; // > 1 MiB R2 spill threshold
    let large_output = vec![0x7Cu8; 1_600_000]; // > 1 MiB R2 spill threshold

    let mut client1 = ExecutionClient::connect_with_token(&cp1.ws_base, Some(CLIENT_TOKEN))
        .await
        .unwrap();
    let accepted = client1
        .submit(
            Some("r2-durable-job".into()),
            job_spec("recover.echo", large_input.clone(), true),
        )
        .await
        .unwrap();
    assert!(accepted.created);

    {
        let db = journal.db.lock().await;
        assert!(
            db.r2.contains_key("jobs/r2-durable-job/input/0"),
            "input > 1 MiB must spill to R2"
        );
    }

    // Gracefully stop Process 1 before any worker executes the job.
    cp1.stop();

    // Start Process 2 (with max_terminal_jobs=0 so finished jobs read from CloudflareJournal/R2).
    let cp2 = ControlPlaneProc::start("e2e-recover-2", &journal.endpoint, 0, true).await;
    {
        let db = journal.db.lock().await;
        assert!(
            db.r2_full_reads
                .iter()
                .any(|k| k == "jobs/r2-durable-job/input/0"),
            "Process 2 recovery must materialize R2-spilled input"
        );
    }

    let (mut session, limits, hb_timeout) =
        connect_worker(&cp2.ws_base, "recover-worker", &["recover.echo"]).await;
    let Some(v2::server_message::Msg::Assign(assign)) =
        session.messages.message().await.unwrap().unwrap().msg
    else {
        panic!("expected recovered durable assignment");
    };
    assert_eq!(assign.job_id, "r2-durable-job");
    session
        .control
        .send(v2::WorkerMessage {
            msg: Some(v2::worker_message::Msg::AssignReply(v2::AssignReply {
                lease_id: assign.lease_id.clone(),
                accepted: true,
                reject_reason: String::new(),
            })),
        })
        .await
        .unwrap();
    let mut attempt = Attempt::open(&mut session.data, &assign, &limits, hb_timeout)
        .await
        .unwrap();
    assert_eq!(
        attempt.inputs[0].body,
        Some(v2::object::Body::Inline(large_input.into()))
    );
    let out_obj = v2::Object {
        body: Some(v2::object::Body::Inline(large_output.clone().into())),
        metadata: HashMap::new(),
    };
    attempt
        .commit(
            &mut session.data,
            &assign,
            &limits,
            std::slice::from_ref(&out_obj),
            v2::finish::Outcome::Succeeded(v2::Succeeded {}),
        )
        .await
        .unwrap();

    // Verify R2 output spill, GetJob full read, and /api/jobs preview Range read.
    {
        let db = journal.db.lock().await;
        assert!(
            db.r2.contains_key("jobs/r2-durable-job/output/0"),
            "output > 1 MiB must spill to R2"
        );
    }

    let mut client2 = ExecutionClient::connect_with_token(&cp2.ws_base, Some(CLIENT_TOKEN))
        .await
        .unwrap();
    let done = client2.get("r2-durable-job", true).await.unwrap();
    assert_eq!(done.state, v2::JobState::Succeeded as i32);
    assert_eq!(
        done.outputs[0].body,
        Some(v2::object::Body::Inline(large_output.clone().into()))
    );

    let detail: serde_json::Value = reqwest::Client::new()
        .get(format!("{}/api/jobs/r2-durable-job", cp2.http_base))
        .bearer_auth(CLIENT_TOKEN)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(detail["job"]["outputs"][0]["size_bytes"], 1_600_000);
    assert_eq!(detail["job"]["outputs"][0]["truncated"], true);

    {
        let db = journal.db.lock().await;
        assert!(
            db.r2_range_reads
                .iter()
                .any(|(key, offset, len)| key == "jobs/r2-durable-job/output/0"
                    && *offset == 0
                    && *len == 4096),
            "console preview must use 4096-byte R2 Range read"
        );
    }

    cp2.stop();
}
