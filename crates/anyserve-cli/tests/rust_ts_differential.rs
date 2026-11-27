use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyserve_client::execution::{Attempt, ExecutionClient, WorkerSession, v2};
use anyserve_proto::prost::Message as _;
use serde_json::Value;
use tokio::net::TcpListener;
use tokio::sync::Notify;
use tonic::{Code, Status};

const OPERATOR_CLIENT_TOKEN: &str = "diff-operator-tok";
const DEFAULT_WORKER_TOKEN: &str = "diff-worker-tok";
const TENANT_A_CLIENT_TOKEN: &str = "diff-tenant-a-client-tok";
const TENANT_A_WORKER_TOKEN: &str = "diff-tenant-a-worker-tok";
const TENANT_B_CLIENT_TOKEN: &str = "diff-tenant-b-client-tok";
const TENANT_B_WORKER_TOKEN: &str = "diff-tenant-b-worker-tok";

static TEMP_SEQ: AtomicU64 = AtomicU64::new(0);

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("resolve workspace root")
}

fn temp_path(label: &str, ext: &str) -> PathBuf {
    let seq = TEMP_SEQ.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "anyserve-diff-{label}-{}-{seq}.{ext}",
        std::process::id()
    ))
}

async fn reserve_free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap().port()
}

// ---------------------------------------------------------------------------
// Dual-Backend Harness: Real Rust `anyserve` vs Real TS `node-native-gateway`
// ---------------------------------------------------------------------------

struct RustServerProc {
    child: Child,
    config_path: PathBuf,
    journal_path: PathBuf,
    http_base: String,
    ws_base: String,
}

impl RustServerProc {
    async fn start() -> Self {
        Self::start_with_allowed_origins(None).await
    }

    async fn start_with_allowed_origins(allowed_origins: Option<&str>) -> Self {
        let port = reserve_free_port().await;
        let grpc_port = reserve_free_port().await;
        let config_path = temp_path("rust-cfg", "toml");
        let journal_path = temp_path("rust-journal", "sqlite");
        let origins_toml = match allowed_origins {
            Some(origin) => format!("allowed_origins = [\"{origin}\"]\n"),
            None => String::new(),
        };

        let scoped_toml = format!(
            r#"
[[auth.scoped_tokens]]
token = "{TENANT_A_CLIENT_TOKEN}"
role = "client"
namespace = "tenant-a"
subject = "tenant-a-client"

[[auth.scoped_tokens]]
token = "{TENANT_A_WORKER_TOKEN}"
role = "worker"
namespace = "tenant-a"
subject = "tenant-a-worker"

[[auth.scoped_tokens]]
token = "{TENANT_B_CLIENT_TOKEN}"
role = "client"
namespace = "tenant-b"
subject = "tenant-b-client"

[[auth.scoped_tokens]]
token = "{TENANT_B_WORKER_TOKEN}"
role = "worker"
namespace = "tenant-b"
subject = "tenant-b-worker"
"#
        );

        let toml = format!(
            r#"[server]
grpc_enabled = false
grpc_port = {grpc_port}

[console]
enabled = true
listen = "127.0.0.1:{port}"
public_endpoint = "wss://diff.anyserve.example.com"
{origins_toml}
[journal]
enabled = true
backend = "sqlite"
path = "{journal}"
full_sync = false

[auth]
client_tokens = ["{OPERATOR_CLIENT_TOKEN}"]
worker_tokens = ["{DEFAULT_WORKER_TOKEN}"]
{scoped_toml}

[runtime]
heartbeat_interval_ms = 30000
heartbeat_timeout_ms = 90000
max_queue = 2000
max_pending_jobs = 2000
"#,
            journal = journal_path.display()
        );
        std::fs::write(&config_path, toml).expect("write diff rust server config");

        let child = Command::new(env!("CARGO_BIN_EXE_anyserve"))
            .args(["serve", "--config", config_path.to_str().unwrap()])
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn anyserve binary");

        let http_base = format!("http://127.0.0.1:{port}");
        let ws_base = format!("ws://127.0.0.1:{port}");
        wait_readyz("rust", &http_base).await;

        Self {
            child,
            config_path,
            journal_path,
            http_base,
            ws_base,
        }
    }
}

impl Drop for RustServerProc {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_file(&self.config_path);
        let _ = std::fs::remove_file(&self.journal_path);
        let wal = PathBuf::from(format!("{}-wal", self.journal_path.display()));
        let shm = PathBuf::from(format!("{}-shm", self.journal_path.display()));
        let _ = std::fs::remove_file(wal);
        let _ = std::fs::remove_file(shm);
    }
}

struct TsGatewayProc {
    child: Child,
    http_base: String,
    ws_base: String,
}

impl TsGatewayProc {
    async fn start() -> Self {
        Self::start_with_allowed_origins(None).await
    }

    async fn start_with_allowed_origins(allowed_origins: Option<&str>) -> Self {
        let root = workspace_root();
        let script = root.join("deploy/cloudflare/src/node-native-gateway.ts");
        let scoped_tokens_json = serde_json::json!([
            {
                "token": TENANT_A_CLIENT_TOKEN,
                "role": "client",
                "namespace": "tenant-a",
                "subject": "tenant-a-client"
            },
            {
                "token": TENANT_A_WORKER_TOKEN,
                "role": "worker",
                "namespace": "tenant-a",
                "subject": "tenant-a-worker"
            },
            {
                "token": TENANT_B_CLIENT_TOKEN,
                "role": "client",
                "namespace": "tenant-b",
                "subject": "tenant-b-client"
            },
            {
                "token": TENANT_B_WORKER_TOKEN,
                "role": "worker",
                "namespace": "tenant-b",
                "subject": "tenant-b-worker"
            }
        ])
        .to_string();

        let mut cmd = Command::new("node");
        cmd.arg("--experimental-strip-types")
            .arg(&script)
            .current_dir(&root)
            .env("HOST", "127.0.0.1")
            .env("PORT", "0")
            .env("ANYSERVE_CLIENT_TOKENS", OPERATOR_CLIENT_TOKEN)
            .env("ANYSERVE_WORKER_TOKENS", DEFAULT_WORKER_TOKEN)
            .env("ANYSERVE_SCOPED_TOKENS", scoped_tokens_json)
            .env(
                "ANYSERVE_PUBLIC_ENDPOINT",
                "wss://diff.anyserve.example.com",
            )
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if let Some(origin) = allowed_origins {
            cmd.env("ANYSERVE_ALLOWED_ORIGINS", origin);
        }
        let mut child = cmd.spawn().expect("spawn node-native-gateway.ts");

        let stdout = child.stdout.take().expect("gateway stdout");
        let stderr = child.stderr.take().expect("gateway stderr");

        std::thread::spawn(move || {
            let reader = BufReader::new(stderr);
            for line in reader.lines() {
                if line.is_err() {
                    break;
                }
            }
        });

        let (listen_tx, listen_rx) = std::sync::mpsc::channel::<(String, String)>();
        std::thread::spawn(move || {
            let reader = BufReader::new(stdout);
            for line in reader.lines() {
                let Ok(text) = line else { break };
                if let Some(rest) = text.strip_prefix("ANYSERVE_NATIVE_GATEWAY_LISTENING ") {
                    let mut parts = rest.split_whitespace();
                    if let (Some(http_url), Some(ws_url)) = (parts.next(), parts.next()) {
                        let _ = listen_tx.send((http_url.to_owned(), ws_url.to_owned()));
                    }
                }
            }
        });

        let (http_base, ws_base) = listen_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("node-native-gateway did not emit listening line within 10s");
        wait_readyz("ts", &http_base).await;

        Self {
            child,
            http_base,
            ws_base,
        }
    }
}

impl Drop for TsGatewayProc {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

async fn wait_readyz(label: &str, http_base: &str) {
    let http = reqwest::Client::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(resp) = http.get(format!("{http_base}/readyz")).send().await
            && resp.status() == reqwest::StatusCode::NO_CONTENT
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{label} backend did not pass /readyz at {http_base}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

struct DualBackend {
    rust: RustServerProc,
    ts: TsGatewayProc,
    http: reqwest::Client,
}

impl DualBackend {
    async fn start() -> Self {
        let (rust, ts) = tokio::join!(RustServerProc::start(), TsGatewayProc::start());
        Self {
            rust,
            ts,
            http: reqwest::Client::new(),
        }
    }

    async fn client_rust(&self, token: Option<&str>) -> ExecutionClient {
        ExecutionClient::connect_with_token(&self.rust.ws_base, token)
            .await
            .expect("connect rust ExecutionClient")
    }

    async fn client_ts(&self, token: Option<&str>) -> ExecutionClient {
        ExecutionClient::connect_with_token(&self.ts.ws_base, token)
            .await
            .expect("connect ts ExecutionClient")
    }
}

// ---------------------------------------------------------------------------
// Shared Protobuf, Worker & Normalization Helpers
// ---------------------------------------------------------------------------

fn make_spec(
    interface: &str,
    inputs: Vec<v2::Object>,
    durable: bool,
    max_attempts: u32,
) -> v2::JobSpec {
    v2::JobSpec {
        demand: Some(v2::Demand {
            interface: interface.to_owned(),
            ..Default::default()
        }),
        inputs,
        policy: Some(v2::ExecutionPolicy {
            durability: (if durable {
                v2::Durability::Durable
            } else {
                v2::Durability::Ephemeral
            }) as i32,
            max_attempts,
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn inline_obj(bytes: &[u8]) -> v2::Object {
    v2::Object {
        body: Some(v2::object::Body::Inline(bytes.to_vec().into())),
        metadata: HashMap::new(),
    }
}

fn uri_obj(uri: &str, meta: HashMap<String, String>) -> v2::Object {
    v2::Object {
        body: Some(v2::object::Body::Uri(uri.to_owned())),
        metadata: meta,
    }
}

#[allow(clippy::result_large_err)]
async fn raw_submit(
    http: &reqwest::Client,
    http_base: &str,
    token: Option<&str>,
    namespace: Option<&str>,
    req: &v2::SubmitRequest,
    extra_headers: &[(&str, &str)],
) -> Result<v2::SubmitResponse, Status> {
    let mut builder = http
        .post(format!("{http_base}/rpc/v2/client/submit"))
        .header(reqwest::header::CONTENT_TYPE, "application/x-protobuf")
        .body(req.encode_to_vec());
    if let Some(tok) = token {
        builder = builder.bearer_auth(tok);
    }
    if let Some(ns) = namespace {
        builder = builder.header("x-anyserve-namespace", ns);
    }
    for (k, v) in extra_headers {
        builder = builder.header(*k, *v);
    }
    let resp = builder
        .send()
        .await
        .expect("send raw /rpc/v2/client/submit");
    let status = resp.status();
    let headers = resp.headers().clone();
    let body = resp.bytes().await.expect("read submit response body");
    if !status.is_success() {
        let code = headers
            .get("x-anyserve-grpc-code")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<i32>().ok())
            .map(Code::from_i32)
            .unwrap_or(Code::Unknown);
        let msg = String::from_utf8_lossy(&body).into_owned();
        return Err(Status::new(code, msg));
    }
    v2::SubmitResponse::decode(body).map_err(|e| Status::internal(e.to_string()))
}

async fn console_get(
    http: &reqwest::Client,
    http_base: &str,
    token: Option<&str>,
    path: &str,
) -> (u16, Value) {
    let mut builder = http.get(format!("{http_base}{path}"));
    if let Some(tok) = token {
        builder = builder.bearer_auth(tok);
    }
    let resp = builder.send().await.expect("console GET");
    let code = resp.status().as_u16();
    let val = resp.json::<Value>().await.unwrap_or(Value::Null);
    (code, val)
}

async fn connect_worker(
    ws_base: &str,
    token: &str,
    hello: v2::Hello,
    initial_credits: u32,
) -> (WorkerSession, v2::Welcome) {
    let mut session = WorkerSession::connect(ws_base, Some(token), hello)
        .await
        .expect("connect WorkerSession");
    let Some(v2::server_message::Msg::Welcome(welcome)) = session
        .messages
        .message()
        .await
        .expect("read welcome")
        .expect("welcome frame")
        .msg
    else {
        panic!("expected Welcome");
    };
    if initial_credits > 0 {
        session
            .control
            .send(v2::WorkerMessage {
                msg: Some(v2::worker_message::Msg::Credit(v2::Credit {
                    free_slots: initial_credits,
                })),
            })
            .await
            .expect("send credit");
    }
    (session, welcome)
}

#[derive(Debug, PartialEq, Eq)]
enum NormObj {
    Inline(Vec<u8>, BTreeMap<String, String>),
    Uri(String, BTreeMap<String, String>),
}

#[derive(Debug, PartialEq, Eq)]
struct NormJob {
    job_id: String,
    namespace: String,
    state: i32,
    durability: i32,
    interface: String,
    attempt_count: u32,
    spec_hash: String,
    metadata: BTreeMap<String, String>,
    error: Option<(String, bool)>,
    outputs: Vec<NormObj>,
}

fn norm_obj(o: &v2::Object) -> NormObj {
    let meta: BTreeMap<String, String> = o.metadata.clone().into_iter().collect();
    match &o.body {
        Some(v2::object::Body::Inline(b)) => NormObj::Inline(b.to_vec(), meta),
        Some(v2::object::Body::Uri(u)) => NormObj::Uri(u.clone(), meta),
        None => NormObj::Inline(Vec::new(), meta),
    }
}

fn norm_job(job: &v2::Job) -> NormJob {
    NormJob {
        job_id: job.job_id.clone(),
        namespace: job.namespace.clone(),
        state: job.state,
        durability: job.durability,
        interface: job.interface.clone(),
        attempt_count: job.attempt_count,
        spec_hash: job.spec_hash.clone(),
        metadata: job.metadata.clone().into_iter().collect(),
        error: job.error.as_ref().map(|e| (e.code.clone(), e.retryable)),
        outputs: job.outputs.iter().map(norm_obj).collect(),
    }
}

fn assert_job_parity(ctx: &str, rust: &v2::Job, ts: &v2::Job) {
    assert_eq!(
        norm_job(rust),
        norm_job(ts),
        "{ctx}: Rust vs TS job mismatch"
    );
}

async fn wait_terminal_job(label: &str, client: &mut ExecutionClient, job_id: &str) -> v2::Job {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let job = client.get(job_id, true).await.expect("get_job");
        if matches!(
            v2::JobState::try_from(job.state).ok(),
            Some(v2::JobState::Succeeded | v2::JobState::Failed | v2::JobState::Cancelled)
        ) {
            return job;
        }
        assert!(
            Instant::now() < deadline,
            "[{label}] timed out waiting for terminal state on {job_id}: current state={} attempt_count={}",
            job.state,
            job.attempt_count
        );
        tokio::time::sleep(Duration::from_millis(15)).await;
    }
}

// ---------------------------------------------------------------------------
// 1. Submit Idempotency, Conflicts, Spec Hash & Input Validation Parity
// ---------------------------------------------------------------------------

#[tokio::test]
async fn diff_submit_idempotency_conflicts_and_validation() {
    let b = DualBackend::start().await;
    let mut r_cli = b.client_rust(Some(OPERATOR_CLIENT_TOKEN)).await;
    let mut t_cli = b.client_ts(Some(OPERATOR_CLIENT_TOKEN)).await;

    // 1a. Identical spec_hash even with different excluded trace keys + anyserve.sha256 URI digest
    let mut spec1 = make_spec(
        "diff.hash.v1",
        vec![
            inline_obj(b"hello-world"),
            uri_obj(
                "https://storage.example/presigned-1?sig=aaa",
                HashMap::from([
                    ("anyserve.sha256".into(), "deadbeef".into()),
                    ("anyserve.size_bytes".into(), "1024".into()),
                    ("kind".into(), "tensor".into()),
                ]),
            ),
        ],
        true,
        3,
    );
    spec1.metadata.insert(
        "traceparent".into(),
        "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-1111111111111111-01".into(),
    );
    spec1.metadata.insert("custom".into(), "v1".into());

    let r1 = r_cli
        .submit(Some("job-idem-1".into()), spec1.clone())
        .await
        .unwrap();
    let t1 = t_cli
        .submit(Some("job-idem-1".into()), spec1.clone())
        .await
        .unwrap();
    assert!(r1.created && t1.created);
    assert_job_parity(
        "initial submit",
        r1.job.as_ref().unwrap(),
        t1.job.as_ref().unwrap(),
    );

    // Replay with different excluded traceparent + different presigned URI query string (same sha256)
    let mut spec1_replay = make_spec(
        "diff.hash.v1",
        vec![
            inline_obj(b"hello-world"),
            uri_obj(
                "https://storage.example/presigned-2?sig=bbb",
                HashMap::from([
                    ("anyserve.sha256".into(), "deadbeef".into()),
                    ("anyserve.size_bytes".into(), "1024".into()),
                    ("kind".into(), "tensor".into()),
                ]),
            ),
        ],
        true,
        3,
    );
    spec1_replay
        .metadata
        .insert("x-request-id".into(), "req-other".into());
    spec1_replay.metadata.insert("custom".into(), "v1".into());

    let r1_rep = r_cli
        .submit(Some("job-idem-1".into()), spec1_replay.clone())
        .await
        .unwrap();
    let t1_rep = t_cli
        .submit(Some("job-idem-1".into()), spec1_replay)
        .await
        .unwrap();
    assert!(!r1_rep.created && !t1_rep.created);
    assert_eq!(
        r1_rep.job.as_ref().unwrap().spec_hash,
        t1_rep.job.as_ref().unwrap().spec_hash
    );

    // 1b. Ephemeral submit parity
    let eph = make_spec("diff.eph.v1", vec![inline_obj(b"eph")], false, 1);
    let r_eph = r_cli
        .submit(Some("job-eph-1".into()), eph.clone())
        .await
        .unwrap();
    let t_eph = t_cli.submit(Some("job-eph-1".into()), eph).await.unwrap();
    assert_job_parity(
        "ephemeral submit",
        r_eph.job.as_ref().unwrap(),
        t_eph.job.as_ref().unwrap(),
    );

    // 1c. ConflictPolicy::Fail on existing job -> Code::AlreadyExists on both
    let conflict_req = v2::SubmitRequest {
        job_id: "job-idem-1".into(),
        conflict_policy: v2::ConflictPolicy::Fail as i32,
        spec: Some(spec1),
    };
    let r_cf = raw_submit(
        &b.http,
        &b.rust.http_base,
        Some(OPERATOR_CLIENT_TOKEN),
        None,
        &conflict_req,
        &[],
    )
    .await
    .unwrap_err();
    let t_cf = raw_submit(
        &b.http,
        &b.ts.http_base,
        Some(OPERATOR_CLIENT_TOKEN),
        None,
        &conflict_req,
        &[],
    )
    .await
    .unwrap_err();
    assert_eq!(r_cf.code(), Code::AlreadyExists);
    assert_eq!(t_cf.code(), Code::AlreadyExists);

    // UseExisting with mismatched spec also fails with Code::AlreadyExists
    let mismatched = make_spec("diff.hash.v1", vec![inline_obj(b"changed")], true, 3);
    assert_eq!(
        r_cli
            .submit(Some("job-idem-1".into()), mismatched.clone())
            .await
            .unwrap_err()
            .code(),
        Code::AlreadyExists
    );
    assert_eq!(
        t_cli
            .submit(Some("job-idem-1".into()), mismatched)
            .await
            .unwrap_err()
            .code(),
        Code::AlreadyExists
    );

    // 1d. Validation: >64-byte interface rejected as InvalidArgument; missing job -> NotFound
    let too_long_iface = "a".repeat(65);
    let bad_spec = make_spec(&too_long_iface, Vec::new(), true, 1);
    assert_eq!(
        r_cli
            .submit(None, bad_spec.clone())
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    assert_eq!(
        t_cli.submit(None, bad_spec).await.unwrap_err().code(),
        Code::InvalidArgument
    );

    assert_eq!(
        r_cli.get("missing-job", false).await.unwrap_err().code(),
        Code::NotFound
    );
    assert_eq!(
        t_cli.get("missing-job", false).await.unwrap_err().code(),
        Code::NotFound
    );
}

// ---------------------------------------------------------------------------
// 2. Auth, Scoped Tenant Isolation & Console API Parity
// ---------------------------------------------------------------------------

#[tokio::test]
async fn diff_auth_namespace_isolation_and_console_parity() {
    let b = DualBackend::start().await;

    // Unauthenticated & wrong-role RPC rejections
    let mut r_unauth = b.client_rust(None).await;
    let mut t_unauth = b.client_ts(None).await;
    assert_eq!(
        r_unauth
            .list(v2::ListJobsRequest::default())
            .await
            .unwrap_err()
            .code(),
        Code::Unauthenticated
    );
    assert_eq!(
        t_unauth
            .list(v2::ListJobsRequest::default())
            .await
            .unwrap_err()
            .code(),
        Code::Unauthenticated
    );

    let mut r_wrong = b.client_rust(Some(DEFAULT_WORKER_TOKEN)).await;
    let mut t_wrong = b.client_ts(Some(DEFAULT_WORKER_TOKEN)).await;
    let s = make_spec("diff.auth.v1", Vec::new(), true, 1);
    assert_eq!(
        r_wrong.submit(None, s.clone()).await.unwrap_err().code(),
        Code::PermissionDenied
    );
    assert_eq!(
        t_wrong.submit(None, s.clone()).await.unwrap_err().code(),
        Code::PermissionDenied
    );

    let r_w_wrong = WorkerSession::connect(
        &b.rust.ws_base,
        Some(OPERATOR_CLIENT_TOKEN),
        v2::Hello {
            worker_id: "bad".into(),
            interfaces: vec!["diff.auth.v1".into()],
            max_concurrency: 1,
            ..Default::default()
        },
    )
    .await
    .err()
    .unwrap()
    .downcast::<Status>()
    .unwrap();
    let t_w_wrong = WorkerSession::connect(
        &b.ts.ws_base,
        Some(OPERATOR_CLIENT_TOKEN),
        v2::Hello {
            worker_id: "bad".into(),
            interfaces: vec!["diff.auth.v1".into()],
            max_concurrency: 1,
            ..Default::default()
        },
    )
    .await
    .err()
    .unwrap()
    .downcast::<Status>()
    .unwrap();
    assert_eq!(r_w_wrong.code(), Code::PermissionDenied);
    assert_eq!(t_w_wrong.code(), Code::PermissionDenied);

    // Console auth: unauthenticated -> 401; scoped tenant -> 403 on /metrics, 200 (scoped) on /api/overview, 403 on cross-namespace
    assert_eq!(
        console_get(&b.http, &b.rust.http_base, None, "/api/overview")
            .await
            .0,
        401
    );
    assert_eq!(
        console_get(&b.http, &b.ts.http_base, None, "/api/overview")
            .await
            .0,
        401
    );
    assert_eq!(
        console_get(
            &b.http,
            &b.rust.http_base,
            Some(TENANT_A_CLIENT_TOKEN),
            "/metrics"
        )
        .await
        .0,
        403
    );
    assert_eq!(
        console_get(
            &b.http,
            &b.ts.http_base,
            Some(TENANT_A_CLIENT_TOKEN),
            "/metrics"
        )
        .await
        .0,
        403
    );
    assert_eq!(
        console_get(
            &b.http,
            &b.rust.http_base,
            Some(TENANT_A_CLIENT_TOKEN),
            "/api/jobs?namespace=tenant-b"
        )
        .await
        .0,
        403
    );
    assert_eq!(
        console_get(
            &b.http,
            &b.ts.http_base,
            Some(TENANT_A_CLIENT_TOKEN),
            "/api/jobs?namespace=tenant-b"
        )
        .await
        .0,
        403
    );

    // Tenant A and Tenant B submit same raw job_id in isolated namespaces
    let mut r_a = b.client_rust(Some(TENANT_A_CLIENT_TOKEN)).await;
    let mut t_a = b.client_ts(Some(TENANT_A_CLIENT_TOKEN)).await;
    let mut r_b = b.client_rust(Some(TENANT_B_CLIENT_TOKEN)).await;
    let mut t_b = b.client_ts(Some(TENANT_B_CLIENT_TOKEN)).await;

    let r_ja = r_a
        .submit(Some("shared-raw-id".into()), s.clone())
        .await
        .unwrap()
        .job
        .unwrap();
    let t_ja = t_a
        .submit(Some("shared-raw-id".into()), s.clone())
        .await
        .unwrap()
        .job
        .unwrap();
    assert_job_parity("tenant-a submit", &r_ja, &t_ja);
    assert_eq!(r_ja.namespace, "tenant-a");

    let mut s_b = s.clone();
    s_b.inputs = vec![inline_obj(b"tenant-b-only")];
    let r_jb = r_b
        .submit(Some("shared-raw-id".into()), s_b.clone())
        .await
        .unwrap()
        .job
        .unwrap();
    let t_jb = t_b
        .submit(Some("shared-raw-id".into()), s_b)
        .await
        .unwrap()
        .job
        .unwrap();
    assert_job_parity("tenant-b submit", &r_jb, &t_jb);
    assert_eq!(r_jb.namespace, "tenant-b");

    // Cross-tenant GetJob/Cancel isolated (NotFound for non-existent job-only-in-a)
    let only_a = make_spec("diff.auth.v1", vec![inline_obj(b"only-a")], true, 1);
    r_a.submit(Some("only-a-id".into()), only_a.clone())
        .await
        .unwrap();
    t_a.submit(Some("only-a-id".into()), only_a).await.unwrap();
    assert_eq!(
        r_b.get("only-a-id", false).await.unwrap_err().code(),
        Code::NotFound
    );
    assert_eq!(
        t_b.get("only-a-id", false).await.unwrap_err().code(),
        Code::NotFound
    );
    assert_eq!(
        r_b.cancel("only-a-id", "x").await.unwrap_err().code(),
        Code::NotFound
    );
    assert_eq!(
        t_b.cancel("only-a-id", "x").await.unwrap_err().code(),
        Code::NotFound
    );

    // Console /api/overview & /api/jobs namespace filter parity
    let (rs, r_ov) = console_get(
        &b.http,
        &b.rust.http_base,
        Some(OPERATOR_CLIENT_TOKEN),
        "/api/overview",
    )
    .await;
    let (ts, t_ov) = console_get(
        &b.http,
        &b.ts.http_base,
        Some(OPERATOR_CLIENT_TOKEN),
        "/api/overview",
    )
    .await;
    assert_eq!((rs, ts), (200, 200));
    assert_eq!(r_ov["queue"], t_ov["queue"]);
    assert_eq!(
        r_ov["control_plane"]["auth_enabled"],
        t_ov["control_plane"]["auth_enabled"]
    );

    let (rs, r_jobs_a) = console_get(
        &b.http,
        &b.rust.http_base,
        Some(OPERATOR_CLIENT_TOKEN),
        "/api/jobs?namespace=tenant-a",
    )
    .await;
    let (ts, t_jobs_a) = console_get(
        &b.http,
        &b.ts.http_base,
        Some(OPERATOR_CLIENT_TOKEN),
        "/api/jobs?namespace=tenant-a",
    )
    .await;
    assert_eq!((rs, ts), (200, 200));
    assert_eq!(r_jobs_a["total"], 2);
    assert_eq!(t_jobs_a["total"], 2);

    // Tenant A Console isolation parity on /api/overview & /api/jobs
    let (rs, r_ten_ov) = console_get(
        &b.http,
        &b.rust.http_base,
        Some(TENANT_A_CLIENT_TOKEN),
        "/api/overview",
    )
    .await;
    let (ts, t_ten_ov) = console_get(
        &b.http,
        &b.ts.http_base,
        Some(TENANT_A_CLIENT_TOKEN),
        "/api/overview",
    )
    .await;
    assert_eq!((rs, ts), (200, 200));
    assert_eq!(r_ten_ov["scope"]["namespace"], "tenant-a");
    assert_eq!(t_ten_ov["scope"]["namespace"], "tenant-a");
    assert_eq!(r_ten_ov["scope"]["is_operator"], false);
    assert_eq!(t_ten_ov["scope"]["is_operator"], false);
    assert_eq!(r_ten_ov["queue"]["queued"], 2);
    assert_eq!(t_ten_ov["queue"]["queued"], 2);

    let (rs, r_ten_jobs) = console_get(
        &b.http,
        &b.rust.http_base,
        Some(TENANT_A_CLIENT_TOKEN),
        "/api/jobs",
    )
    .await;
    let (ts, t_ten_jobs) = console_get(
        &b.http,
        &b.ts.http_base,
        Some(TENANT_A_CLIENT_TOKEN),
        "/api/jobs",
    )
    .await;
    assert_eq!((rs, ts), (200, 200));
    assert_eq!(r_ten_jobs["total"], 2);
    assert_eq!(t_ten_jobs["total"], 2);

    // Connect one worker per backend and compare /api/workers
    let (_rw, _) = connect_worker(
        &b.rust.ws_base,
        DEFAULT_WORKER_TOKEN,
        v2::Hello {
            worker_id: "w-console-1".into(),
            interfaces: vec!["diff.auth.v1".into()],
            max_concurrency: 2,
            pool: "gpu-pool".into(),
            ..Default::default()
        },
        2,
    )
    .await;
    let (_tw, _) = connect_worker(
        &b.ts.ws_base,
        DEFAULT_WORKER_TOKEN,
        v2::Hello {
            worker_id: "w-console-1".into(),
            interfaces: vec!["diff.auth.v1".into()],
            max_concurrency: 2,
            pool: "gpu-pool".into(),
            ..Default::default()
        },
        2,
    )
    .await;

    let (_, r_wk) = console_get(
        &b.http,
        &b.rust.http_base,
        Some(OPERATOR_CLIENT_TOKEN),
        "/api/workers",
    )
    .await;
    let (_, t_wk) = console_get(
        &b.http,
        &b.ts.http_base,
        Some(OPERATOR_CLIENT_TOKEN),
        "/api/workers",
    )
    .await;
    assert_eq!(r_wk["total"], t_wk["total"]);
    assert_eq!(r_wk["healthy"], t_wk["healthy"]);
    assert_eq!(
        r_wk["workers"][0]["worker_id"],
        t_wk["workers"][0]["worker_id"]
    );
    assert_eq!(r_wk["workers"][0]["pool"], t_wk["workers"][0]["pool"]);
}

// ---------------------------------------------------------------------------
// 3. Scheduling: Priority, Attributes, Capacity, Concurrency Key & Affinity
// ---------------------------------------------------------------------------

async fn run_priority_order(ws_base: &str) -> Vec<String> {
    let mut cli = ExecutionClient::connect_with_token(ws_base, Some(OPERATOR_CLIENT_TOKEN))
        .await
        .unwrap();

    // Queue all 3 jobs before connecting the worker so priority ordering is tested deterministically.
    for (id, prio) in [("p-low", 0), ("p-mid", 1), ("p-high", 3)] {
        let mut s = make_spec("diff.prio.v1", vec![inline_obj(id.as_bytes())], true, 1);
        s.policy.as_mut().unwrap().priority = prio;
        cli.submit(Some(id.into()), s).await.unwrap();
    }

    let (mut wk, welcome) = connect_worker(
        ws_base,
        DEFAULT_WORKER_TOKEN,
        v2::Hello {
            worker_id: "w-prio".into(),
            interfaces: vec!["diff.prio.v1".into()],
            max_concurrency: 1,
            total_capacity: HashMap::from([("slot".into(), 1)]),
            ..Default::default()
        },
        1,
    )
    .await;
    let limits = welcome.limits.unwrap();
    let hb_timeout = Duration::from_millis(u64::from(welcome.heartbeat_timeout_ms));

    let mut order = Vec::new();
    for step in 0..3 {
        if step > 0 {
            wk.control
                .send(v2::WorkerMessage {
                    msg: Some(v2::worker_message::Msg::Credit(v2::Credit {
                        free_slots: 1,
                    })),
                })
                .await
                .unwrap();
        }
        let assign = loop {
            if let Some(v2::server_message::Msg::Assign(a)) =
                wk.messages.message().await.unwrap().unwrap().msg
            {
                break a;
            }
        };
        wk.control
            .send(v2::WorkerMessage {
                msg: Some(v2::worker_message::Msg::AssignReply(v2::AssignReply {
                    lease_id: assign.lease_id.clone(),
                    accepted: true,
                    reject_reason: String::new(),
                })),
            })
            .await
            .unwrap();
        order.push(assign.job_id.clone());
        let mut data = wk.data.clone();
        let mut att = Attempt::open(&mut data, &assign, &limits, hb_timeout)
            .await
            .unwrap();
        let out = att.inputs.clone();
        att.commit(
            &mut data,
            &assign,
            &limits,
            &out,
            v2::finish::Outcome::Succeeded(v2::Succeeded {}),
        )
        .await
        .unwrap();
    }
    order
}

async fn run_attr_cap_concurrency_affinity(ws_base: &str) -> (String, usize, String) {
    let mut cli = ExecutionClient::connect_with_token(ws_base, Some(OPERATOR_CLIENT_TOKEN))
        .await
        .unwrap();

    let (w_cpu, w_cpu_wel) = connect_worker(
        ws_base,
        DEFAULT_WORKER_TOKEN,
        v2::Hello {
            worker_id: "w-cpu".into(),
            interfaces: vec!["diff.sched.v1".into()],
            attributes: HashMap::from([("tier".into(), "cpu".into())]),
            total_capacity: HashMap::from([("gpu".into(), 0), ("slot".into(), 2)]),
            max_concurrency: 2,
            ..Default::default()
        },
        2,
    )
    .await;
    let (w_gpu, w_gpu_wel) = connect_worker(
        ws_base,
        DEFAULT_WORKER_TOKEN,
        v2::Hello {
            worker_id: "w-gpu".into(),
            interfaces: vec!["diff.sched.v1".into()],
            attributes: HashMap::from([("tier".into(), "gpu".into())]),
            total_capacity: HashMap::from([("gpu".into(), 2), ("slot".into(), 2)]),
            max_concurrency: 2,
            ..Default::default()
        },
        2,
    )
    .await;

    let active_serial = Arc::new(Mutex::new(0usize));
    let max_seen_serial = Arc::new(Mutex::new(0usize));
    let affinity_attempts: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let aff_done = Arc::new(Notify::new());

    let spawn_loop = |mut wk: WorkerSession,
                      wel: v2::Welcome,
                      wid: &'static str,
                      active_serial: Arc<Mutex<usize>>,
                      max_seen_serial: Arc<Mutex<usize>>,
                      affinity_attempts: Arc<Mutex<Vec<String>>>,
                      aff_done: Arc<Notify>| {
        let limits = wel.limits.unwrap();
        let hb_timeout = Duration::from_millis(u64::from(wel.heartbeat_timeout_ms));
        tokio::spawn(async move {
            while let Ok(Some(msg)) = wk.messages.message().await {
                let Some(v2::server_message::Msg::Assign(assign)) = msg.msg else {
                    continue;
                };
                wk.control
                    .send(v2::WorkerMessage {
                        msg: Some(v2::worker_message::Msg::AssignReply(v2::AssignReply {
                            lease_id: assign.lease_id.clone(),
                            accepted: true,
                            reject_reason: String::new(),
                        })),
                    })
                    .await
                    .unwrap();
                let mut data = wk.data.clone();
                let ctrl = wk.control.clone();
                let active_s = Arc::clone(&active_serial);
                let max_s = Arc::clone(&max_seen_serial);
                let aff_a = Arc::clone(&affinity_attempts);
                let aff_n = Arc::clone(&aff_done);
                tokio::spawn(async move {
                    let mut att = Attempt::open(&mut data, &assign, &limits, hb_timeout)
                        .await
                        .unwrap();
                    if assign.job_id.starts_with("serial-") {
                        {
                            let mut cur = active_s.lock().unwrap();
                            *cur += 1;
                            let mut max_v = max_s.lock().unwrap();
                            if *cur > *max_v {
                                *max_v = *cur;
                            }
                        }
                        tokio::time::sleep(Duration::from_millis(25)).await;
                        {
                            let mut cur = active_s.lock().unwrap();
                            *cur -= 1;
                        }
                        att.commit(
                            &mut data,
                            &assign,
                            &limits,
                            &[inline_obj(wid.as_bytes())],
                            v2::finish::Outcome::Succeeded(v2::Succeeded {}),
                        )
                        .await
                        .unwrap();
                    } else if assign.job_id == "aff-retry-job" {
                        let first = {
                            let mut vec = aff_a.lock().unwrap();
                            vec.push(wid.to_owned());
                            vec.len() == 1
                        };
                        if first {
                            att.commit(
                                &mut data,
                                &assign,
                                &limits,
                                &[],
                                v2::finish::Outcome::Failed(v2::ErrorInfo {
                                    code: "retry_other".into(),
                                    message: "fail first worker".into(),
                                    retryable: true,
                                }),
                            )
                            .await
                            .unwrap();
                        } else {
                            att.commit(
                                &mut data,
                                &assign,
                                &limits,
                                &[inline_obj(wid.as_bytes())],
                                v2::finish::Outcome::Succeeded(v2::Succeeded {}),
                            )
                            .await
                            .unwrap();
                            aff_n.notify_waiters();
                        }
                    } else {
                        att.commit(
                            &mut data,
                            &assign,
                            &limits,
                            &[inline_obj(wid.as_bytes())],
                            v2::finish::Outcome::Succeeded(v2::Succeeded {}),
                        )
                        .await
                        .unwrap();
                    }
                    let _ = ctrl
                        .send(v2::WorkerMessage {
                            msg: Some(v2::worker_message::Msg::Credit(v2::Credit {
                                free_slots: 1,
                            })),
                        })
                        .await;
                });
            }
        })
    };

    let h1 = spawn_loop(
        w_cpu,
        w_cpu_wel,
        "w-cpu",
        Arc::clone(&active_serial),
        Arc::clone(&max_seen_serial),
        Arc::clone(&affinity_attempts),
        Arc::clone(&aff_done),
    );
    let h2 = spawn_loop(
        w_gpu,
        w_gpu_wel,
        "w-gpu",
        Arc::clone(&active_serial),
        Arc::clone(&max_seen_serial),
        Arc::clone(&affinity_attempts),
        Arc::clone(&aff_done),
    );

    // (a) Attribute + capacity routing: requires tier=gpu & gpu=2 -> only w-gpu matches
    let mut gpu_spec = make_spec("diff.sched.v1", vec![inline_obj(b"gpu")], true, 1);
    gpu_spec
        .demand
        .as_mut()
        .unwrap()
        .required_attributes
        .insert("tier".into(), "gpu".into());
    gpu_spec
        .demand
        .as_mut()
        .unwrap()
        .required_capacity
        .insert("gpu".into(), 2);
    cli.submit(Some("gpu-job".into()), gpu_spec).await.unwrap();
    let gpu_done = wait_terminal_job("sched-gpu", &mut cli, "gpu-job").await;
    let gpu_worker = match &gpu_done.outputs[0].body {
        Some(v2::object::Body::Inline(bytes)) => String::from_utf8_lossy(bytes).into_owned(),
        _ => String::new(),
    };

    // (b) concurrency_key serialization across 2 healthy workers
    for i in 0..3 {
        let mut s = make_spec("diff.sched.v1", vec![inline_obj(b"ser")], true, 1);
        s.policy.as_mut().unwrap().concurrency_key = "serial-key".into();
        cli.submit(Some(format!("serial-{i}")), s).await.unwrap();
    }
    for i in 0..3 {
        let j = wait_terminal_job("sched-serial", &mut cli, &format!("serial-{i}")).await;
        assert_eq!(j.state, v2::JobState::Succeeded as i32);
    }
    let max_concurrent = *max_seen_serial.lock().unwrap();

    // (c) affinity_key sticky failure exclusion: attempt 1 fails -> attempt 2 routes to the OTHER worker
    let mut aff = make_spec("diff.sched.v1", vec![inline_obj(b"aff")], true, 2);
    aff.demand.as_mut().unwrap().affinity_key = "robot-cam-1".into();
    cli.submit(Some("aff-retry-job".into()), aff).await.unwrap();
    let aff_job = wait_terminal_job("sched-aff", &mut cli, "aff-retry-job").await;
    assert_eq!(aff_job.state, v2::JobState::Succeeded as i32);
    let attempts = affinity_attempts.lock().unwrap().clone();
    assert_eq!(attempts.len(), 2);
    assert_ne!(
        attempts[0], attempts[1],
        "failed worker must be excluded on retry"
    );
    let sticky_pair = format!("{}->{}", attempts[0], attempts[1]);

    h1.abort();
    h2.abort();
    (gpu_worker, max_concurrent, sticky_pair)
}

#[tokio::test]
async fn diff_scheduling_priority_attributes_capacity_concurrency_and_affinity() {
    let b = DualBackend::start().await;

    let (r_prio, t_prio) = tokio::join!(
        run_priority_order(&b.rust.ws_base),
        run_priority_order(&b.ts.ws_base),
    );
    assert_eq!(r_prio, vec!["p-high", "p-mid", "p-low"]);
    assert_eq!(r_prio, t_prio);

    let (r_sched, t_sched) = tokio::join!(
        run_attr_cap_concurrency_affinity(&b.rust.ws_base),
        run_attr_cap_concurrency_affinity(&b.ts.ws_base),
    );
    assert_eq!(r_sched.0, "w-gpu");
    assert_eq!(r_sched.0, t_sched.0);
    assert_eq!(r_sched.1, 1);
    assert_eq!(t_sched.1, 1);
    // Rendezvous hash chooses the exact same primary and backup workers on both backends!
    assert_eq!(r_sched.2, t_sched.2);
}

// ---------------------------------------------------------------------------
// 4. Lifecycle: Retry, Exposed Output Interruption, Cancel & Full-Duplex Invoke
// ---------------------------------------------------------------------------

async fn run_lifecycle_suite(
    ws_base: &str,
) -> (Vec<&'static str>, v2::Job, v2::Job, v2::Job, Vec<Vec<u8>>) {
    let mut cli = ExecutionClient::connect_with_token(ws_base, Some(OPERATOR_CLIENT_TOKEN))
        .await
        .unwrap();
    let (mut wk, welcome) = connect_worker(
        ws_base,
        DEFAULT_WORKER_TOKEN,
        v2::Hello {
            worker_id: "w-life".into(),
            interfaces: vec![
                "life.retry".into(),
                "life.expose".into(),
                "life.hold".into(),
                "life.duplex".into(),
            ],
            max_concurrency: 4,
            ..Default::default()
        },
        4,
    )
    .await;
    let limits = welcome.limits.unwrap();
    let hb_timeout = Duration::from_millis(u64::from(welcome.heartbeat_timeout_ms));
    let retry_count = Arc::new(AtomicU64::new(0));
    let exposed_seen = Arc::new(Notify::new());
    let exposed_seen_wk = Arc::clone(&exposed_seen);
    let cancel_map: Arc<Mutex<HashMap<String, tokio::sync::oneshot::Sender<()>>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let cancel_map_wk = Arc::clone(&cancel_map);

    let wk_task = tokio::spawn(async move {
        while let Ok(Some(msg)) = wk.messages.message().await {
            match msg.msg {
                Some(v2::server_message::Msg::Cancel(cancel)) => {
                    if let Some(tx) = cancel_map_wk.lock().unwrap().remove(&cancel.lease_id) {
                        let _ = tx.send(());
                    }
                    let _ = wk
                        .control
                        .send(v2::WorkerMessage {
                            msg: Some(v2::worker_message::Msg::CancelAck(v2::CancelAck {
                                lease_id: cancel.lease_id,
                            })),
                        })
                        .await;
                }
                Some(v2::server_message::Msg::Assign(assign)) => {
                    wk.control
                        .send(v2::WorkerMessage {
                            msg: Some(v2::worker_message::Msg::AssignReply(v2::AssignReply {
                                lease_id: assign.lease_id.clone(),
                                accepted: true,
                                reject_reason: String::new(),
                            })),
                        })
                        .await
                        .unwrap();
                    let mut data = wk.data.clone();
                    let ctrl = wk.control.clone();
                    let rc = Arc::clone(&retry_count);
                    let es = Arc::clone(&exposed_seen_wk);
                    let (ctx_tx, ctx_rx) = tokio::sync::oneshot::channel::<()>();
                    if assign.interface == "life.hold" {
                        cancel_map_wk
                            .lock()
                            .unwrap()
                            .insert(assign.lease_id.clone(), ctx_tx);
                    }
                    tokio::spawn(async move {
                        match assign.interface.as_str() {
                            "life.retry" => {
                                let mut att =
                                    Attempt::open(&mut data, &assign, &limits, hb_timeout)
                                        .await
                                        .unwrap();
                                let n = rc.fetch_add(1, Ordering::SeqCst) + 1;
                                att.frame(b"prog".to_vec().into(), HashMap::new(), true)
                                    .await
                                    .unwrap();
                                if n == 1 {
                                    att.commit(
                                        &mut data,
                                        &assign,
                                        &limits,
                                        &[],
                                        v2::finish::Outcome::Failed(v2::ErrorInfo {
                                            code: "transient".into(),
                                            message: "first failed".into(),
                                            retryable: true,
                                        }),
                                    )
                                    .await
                                    .unwrap();
                                } else {
                                    att.commit(
                                        &mut data,
                                        &assign,
                                        &limits,
                                        &[inline_obj(b"retry-ok")],
                                        v2::finish::Outcome::Succeeded(v2::Succeeded {}),
                                    )
                                    .await
                                    .unwrap();
                                }
                            }
                            "life.expose" => {
                                let mut att =
                                    Attempt::open(&mut data, &assign, &limits, hb_timeout)
                                        .await
                                        .unwrap();
                                att.frame(b"partial-chunk".to_vec().into(), HashMap::new(), false)
                                    .await
                                    .unwrap();
                                es.notified().await;
                                att.commit(
                                    &mut data,
                                    &assign,
                                    &limits,
                                    &[],
                                    v2::finish::Outcome::Failed(v2::ErrorInfo {
                                        code: "crash".into(),
                                        message: "boom".into(),
                                        retryable: true,
                                    }),
                                )
                                .await
                                .unwrap();
                            }
                            "life.hold" => {
                                let mut att =
                                    Attempt::open(&mut data, &assign, &limits, hb_timeout)
                                        .await
                                        .unwrap();
                                att.frame(b"holding".to_vec().into(), HashMap::new(), true)
                                    .await
                                    .unwrap();
                                let _ = ctx_rx.await;
                                let _ = att
                                    .commit(
                                        &mut data,
                                        &assign,
                                        &limits,
                                        &[],
                                        v2::finish::Outcome::Cancelled(v2::Cancelled {}),
                                    )
                                    .await;
                            }
                            "life.duplex" => {
                                let mut att =
                                    Attempt::open_stream(&mut data, &assign, &limits, hb_timeout)
                                        .await
                                        .unwrap();
                                let mut acc = Vec::new();
                                while let Some(chunk) = att.input_chunk().await.unwrap() {
                                    acc.extend_from_slice(&chunk.data);
                                    att.frame(chunk.data, HashMap::new(), false).await.unwrap();
                                }
                                att.commit(
                                    &mut data,
                                    &assign,
                                    &limits,
                                    &[inline_obj(&acc)],
                                    v2::finish::Outcome::Succeeded(v2::Succeeded {}),
                                )
                                .await
                                .unwrap();
                            }
                            other => panic!("unexpected {other}"),
                        }
                        let _ = ctrl
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

    // (a) Retryable failure -> AttemptRestarted -> Succeeded
    cli.submit(
        Some("life-retry".into()),
        make_spec("life.retry", vec![inline_obj(b"a")], true, 3),
    )
    .await
    .unwrap();
    let mut watch = cli.watch("life-retry", 0).await.unwrap();
    let mut retry_kinds = Vec::new();
    while let Some(ev) = watch.message().await.unwrap() {
        let k = match ev.event {
            Some(v2::execution_event::Event::Accepted(_)) => "accepted",
            Some(v2::execution_event::Event::Assigned(_)) => "assigned",
            Some(v2::execution_event::Event::AttemptRestarted(_)) => "attempt_restarted",
            Some(v2::execution_event::Event::Output(o)) => {
                if o.progress {
                    "progress"
                } else {
                    "output"
                }
            }
            Some(v2::execution_event::Event::Finished(_)) => "finished",
            None => "none",
        };
        retry_kinds.push(k);
    }
    let retry_job = wait_terminal_job("life-retry", &mut cli, "life-retry").await;

    // (b) Exposed non-progress output + retryable failure -> output_interrupted
    let mut inv = cli
        .invoke(make_spec("life.expose", vec![inline_obj(b"b")], false, 3))
        .await
        .unwrap();
    let mut expose_job_id = String::new();
    while let Some(ev) = inv.message().await.unwrap() {
        if expose_job_id.is_empty() {
            expose_job_id = ev.job_id.clone();
        }
        if let Some(v2::execution_event::Event::Output(o)) = ev.event
            && !o.progress
        {
            exposed_seen.notify_waiters();
        }
    }
    let expose_job = wait_terminal_job("life-expose", &mut cli, &expose_job_id).await;

    // (c) Running cancel
    cli.submit(
        Some("life-hold".into()),
        make_spec("life.hold", Vec::new(), true, 1),
    )
    .await
    .unwrap();
    let mut hold_watch = cli.watch("life-hold", 0).await.unwrap();
    while let Some(ev) = hold_watch.message().await.unwrap() {
        if matches!(ev.event, Some(v2::execution_event::Event::Output(_))) {
            break;
        }
    }
    cli.cancel("life-hold", "user cancelled").await.unwrap();
    let cancelled_job = wait_terminal_job("life-hold", &mut cli, "life-hold").await;

    // (d) Full-duplex streaming Invoke
    let mut duplex = cli
        .invoke_stream(make_spec("life.duplex", Vec::new(), false, 1))
        .await
        .unwrap();
    duplex.send(b"part-1".as_slice()).await.unwrap();
    duplex.send(b"-part-2".as_slice()).await.unwrap();
    duplex.finish_input().await.unwrap();
    let mut duplex_outs = Vec::new();
    while let Some(ev) = duplex.events.message().await.unwrap() {
        match ev.event {
            Some(v2::execution_event::Event::Output(o)) if !o.progress => {
                duplex_outs.push(o.data.to_vec());
            }
            Some(v2::execution_event::Event::Finished(f)) => {
                assert_eq!(f.state, v2::JobState::Succeeded as i32);
                break;
            }
            _ => {}
        }
    }

    wk_task.abort();
    (
        retry_kinds,
        retry_job,
        expose_job,
        cancelled_job,
        duplex_outs,
    )
}

#[tokio::test]
async fn diff_lifecycle_retry_cancel_duplex_and_output_interrupted() {
    let b = DualBackend::start().await;
    let (r, t) = tokio::join!(
        run_lifecycle_suite(&b.rust.ws_base),
        run_lifecycle_suite(&b.ts.ws_base),
    );
    assert_eq!(
        r.0,
        vec![
            "accepted",
            "assigned",
            "progress",
            "attempt_restarted",
            "assigned",
            "progress",
            "finished"
        ]
    );
    assert_eq!(r.0, t.0);
    assert_job_parity("retry job", &r.1, &t.1);
    assert_eq!(r.1.attempt_count, 2);

    assert_eq!(r.2.error.as_ref().unwrap().code, "output_interrupted");
    assert_eq!(r.2.attempt_count, 1);
    assert_eq!(norm_job(&r.2).error, norm_job(&t.2).error);
    assert_eq!(r.2.state, t.2.state);
    assert_eq!(r.2.attempt_count, t.2.attempt_count);

    assert_eq!(r.3.state, v2::JobState::Cancelled as i32);
    assert_eq!(r.3.state, t.3.state);
    assert_eq!(r.4, vec![b"part-1".to_vec(), b"-part-2".to_vec()]);
    assert_eq!(r.4, t.4);
}

// ---------------------------------------------------------------------------
// 5. W3C Trace Propagation & Console Waterfall API Parity
// ---------------------------------------------------------------------------

async fn run_trace_and_waterfall(
    http: &reqwest::Client,
    http_base: &str,
    ws_base: &str,
) -> (v2::Job, v2::Job, Value, Value) {
    let (mut wk, welcome) = connect_worker(
        ws_base,
        DEFAULT_WORKER_TOKEN,
        v2::Hello {
            worker_id: "w-trace".into(),
            interfaces: vec!["diff.trace.v1".into()],
            max_concurrency: 4,
            ..Default::default()
        },
        4,
    )
    .await;
    let limits = welcome.limits.unwrap();
    let hb_timeout = Duration::from_millis(u64::from(welcome.heartbeat_timeout_ms));

    let wk_task = tokio::spawn(async move {
        while let Ok(Some(msg)) = wk.messages.message().await {
            let Some(v2::server_message::Msg::Assign(assign)) = msg.msg else {
                continue;
            };
            wk.control
                .send(v2::WorkerMessage {
                    msg: Some(v2::worker_message::Msg::AssignReply(v2::AssignReply {
                        lease_id: assign.lease_id.clone(),
                        accepted: true,
                        reject_reason: String::new(),
                    })),
                })
                .await
                .unwrap();
            let mut data = wk.data.clone();
            let mut att = Attempt::open(&mut data, &assign, &limits, hb_timeout)
                .await
                .unwrap();
            att.frame(b"step-1".to_vec().into(), HashMap::new(), true)
                .await
                .unwrap();
            att.frame(b"stream-out".to_vec().into(), HashMap::new(), false)
                .await
                .unwrap();
            att.commit(
                &mut data,
                &assign,
                &limits,
                &[inline_obj(b"trace-done")],
                v2::finish::Outcome::Succeeded(v2::Succeeded {}),
            )
            .await
            .unwrap();
        }
    });

    // Job 1: traceparent & tracestate delivered only via HTTP headers
    let tp_header = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    let req1 = v2::SubmitRequest {
        job_id: "trace-hdr-job".into(),
        conflict_policy: v2::ConflictPolicy::UseExisting as i32,
        spec: Some(make_spec(
            "diff.trace.v1",
            vec![inline_obj(b"in-1")],
            true,
            2,
        )),
    };
    let sub1 = raw_submit(
        http,
        http_base,
        Some(OPERATOR_CLIENT_TOKEN),
        None,
        &req1,
        &[("traceparent", tp_header), ("tracestate", "vendor=diff")],
    )
    .await
    .unwrap();
    assert!(sub1.created);

    // Job 2: explicit spec.metadata["traceparent"] overrides header
    let tp_explicit = "00-99999999999999999999999999999999-1111111111111111-01";
    let mut spec2 = make_spec("diff.trace.v1", vec![inline_obj(b"in-1")], true, 2);
    spec2
        .metadata
        .insert("traceparent".into(), tp_explicit.into());
    let req2 = v2::SubmitRequest {
        job_id: "trace-override-job".into(),
        conflict_policy: v2::ConflictPolicy::UseExisting as i32,
        spec: Some(spec2),
    };
    let sub2 = raw_submit(
        http,
        http_base,
        Some(OPERATOR_CLIENT_TOKEN),
        None,
        &req2,
        &[("traceparent", tp_header), ("x-request-id", "req-override")],
    )
    .await
    .unwrap();
    assert!(sub2.created);

    let mut cli = ExecutionClient::connect_with_token(ws_base, Some(OPERATOR_CLIENT_TOKEN))
        .await
        .unwrap();
    let j1 = wait_terminal_job("trace-1", &mut cli, "trace-hdr-job").await;
    let j2 = wait_terminal_job("trace-2", &mut cli, "trace-override-job").await;

    let (qs, search_json) = console_get(
        http,
        http_base,
        Some(OPERATOR_CLIENT_TOKEN),
        "/api/jobs?q=4bf92f3577b34da6a3ce929d0e0e4736",
    )
    .await;
    assert_eq!(qs, 200);

    let (ds, detail_json) = console_get(
        http,
        http_base,
        Some(OPERATOR_CLIENT_TOKEN),
        "/api/jobs/trace-hdr-job",
    )
    .await;
    assert_eq!(ds, 200);

    wk_task.abort();
    (j1, j2, search_json, detail_json)
}

#[tokio::test]
async fn diff_w3c_trace_propagation_and_console_waterfall_parity() {
    let b = DualBackend::start().await;
    let ((rj1, rj2, r_search, r_detail), (tj1, tj2, t_search, t_detail)) = tokio::join!(
        run_trace_and_waterfall(&b.http, &b.rust.http_base, &b.rust.ws_base),
        run_trace_and_waterfall(&b.http, &b.ts.http_base, &b.ts.ws_base),
    );

    assert_job_parity("trace header job", &rj1, &tj1);
    assert_job_parity("trace override job", &rj2, &tj2);
    // Header propagation must NOT alter spec_hash vs explicit traceparent
    assert_eq!(rj1.spec_hash, rj2.spec_hash);
    assert_eq!(tj1.spec_hash, tj2.spec_hash);
    assert_eq!(
        rj1.metadata.get("traceparent").map(String::as_str),
        Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
    );
    assert_eq!(
        rj2.metadata.get("traceparent").map(String::as_str),
        Some("00-99999999999999999999999999999999-1111111111111111-01")
    );
    assert_eq!(
        rj2.metadata.get("x-request-id").map(String::as_str),
        Some("req-override")
    );

    // Search by trace_id finds trace-hdr-job on both backends
    assert_eq!(r_search["total"], 1);
    assert_eq!(t_search["total"], 1);
    assert_eq!(r_search["jobs"][0]["job_id"], "trace-hdr-job");
    assert_eq!(t_search["jobs"][0]["job_id"], "trace-hdr-job");

    // Console detail attempts & events (including attempt_id on attempt events)
    let r_att_states: Vec<&str> = r_detail["attempts"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| a["state"].as_str().unwrap())
        .collect();
    let t_att_states: Vec<&str> = t_detail["attempts"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| a["state"].as_str().unwrap())
        .collect();
    assert_eq!(r_att_states, vec!["succeeded"]);
    assert_eq!(r_att_states, t_att_states);

    let extract_ev = |v: &Value| -> Vec<(String, bool)> {
        v["events"]
            .as_array()
            .unwrap()
            .iter()
            .map(|e| {
                let kind = e["kind"].as_str().unwrap().to_owned();
                let has_att = e
                    .get("attempt_id")
                    .and_then(Value::as_str)
                    .is_some_and(|s| !s.is_empty());
                (kind, has_att)
            })
            .collect()
    };
    let r_evs = extract_ev(&r_detail);
    let t_evs = extract_ev(&t_detail);
    assert_eq!(
        r_evs,
        vec![
            ("accepted".into(), false),
            ("assigned".into(), true),
            ("progress".into(), true),
            ("output".into(), true),
            ("finished".into(), true),
        ]
    );
    assert_eq!(r_evs, t_evs);
}

// ---------------------------------------------------------------------------
// 6. Seeded Randomized Differential Workload (`ANYSERVE_DIFF_SEEDS`)
// ---------------------------------------------------------------------------

struct SplitMix64(u64);
impl SplitMix64 {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
    fn range(&mut self, n: usize) -> usize {
        (self.next_u64() as usize) % n
    }
}

#[derive(Clone)]
enum WorkerPlan {
    Succeed(Vec<u8>),
    RetryOnceThenSucceed(Vec<u8>),
    FailFatal(String),
}

fn spawn_planned_worker(
    label: &str,
    ws_base: &str,
    worker_id: &str,
    plans: Arc<Mutex<HashMap<String, WorkerPlan>>>,
) -> tokio::task::JoinHandle<()> {
    let label = label.to_owned();
    let ws = ws_base.to_owned();
    let wid = worker_id.to_owned();
    tokio::spawn(async move {
        let (mut session, welcome) = connect_worker(
            &ws,
            DEFAULT_WORKER_TOKEN,
            v2::Hello {
                worker_id: wid,
                interfaces: vec!["rand.alpha.v1".into(), "rand.beta.v1".into()],
                max_concurrency: 4,
                ..Default::default()
            },
            4,
        )
        .await;
        let limits = welcome.limits.unwrap();
        let hb_timeout = Duration::from_millis(u64::from(welcome.heartbeat_timeout_ms));
        let seen_attempts: Arc<Mutex<HashMap<String, usize>>> =
            Arc::new(Mutex::new(HashMap::new()));

        while let Ok(Some(msg)) = session.messages.message().await {
            match msg.msg {
                Some(v2::server_message::Msg::Cancel(c)) => {
                    let _ = session
                        .control
                        .send(v2::WorkerMessage {
                            msg: Some(v2::worker_message::Msg::CancelAck(v2::CancelAck {
                                lease_id: c.lease_id,
                            })),
                        })
                        .await;
                }
                Some(v2::server_message::Msg::Assign(assign)) => {
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
                    let ctrl = session.control.clone();
                    let plans_c = Arc::clone(&plans);
                    let seen_c = Arc::clone(&seen_attempts);
                    let lbl = label.clone();
                    tokio::spawn(async move {
                        let mut att =
                            match Attempt::open(&mut data, &assign, &limits, hb_timeout).await {
                                Ok(att) => att,
                                Err(e) => {
                                    eprintln!(
                                        "[{lbl}] Attempt::open failed for {}: {e}",
                                        assign.job_id
                                    );
                                    return;
                                }
                            };
                        let plan = plans_c
                            .lock()
                            .unwrap()
                            .get(&assign.job_id)
                            .cloned()
                            .unwrap_or(WorkerPlan::Succeed(b"default".to_vec()));
                        let attempt_idx = {
                            let mut m = seen_c.lock().unwrap();
                            let e = m.entry(assign.job_id.clone()).or_insert(0);
                            *e += 1;
                            *e
                        };
                        let res = match plan {
                            WorkerPlan::Succeed(bytes) => {
                                att.commit(
                                    &mut data,
                                    &assign,
                                    &limits,
                                    &[inline_obj(&bytes)],
                                    v2::finish::Outcome::Succeeded(v2::Succeeded {}),
                                )
                                .await
                            }
                            WorkerPlan::RetryOnceThenSucceed(bytes) => {
                                if attempt_idx == 1 {
                                    att.commit(
                                        &mut data,
                                        &assign,
                                        &limits,
                                        &[],
                                        v2::finish::Outcome::Failed(v2::ErrorInfo {
                                            code: "flaky".into(),
                                            message: "retry me".into(),
                                            retryable: true,
                                        }),
                                    )
                                    .await
                                } else {
                                    att.commit(
                                        &mut data,
                                        &assign,
                                        &limits,
                                        &[inline_obj(&bytes)],
                                        v2::finish::Outcome::Succeeded(v2::Succeeded {}),
                                    )
                                    .await
                                }
                            }
                            WorkerPlan::FailFatal(code) => {
                                att.commit(
                                    &mut data,
                                    &assign,
                                    &limits,
                                    &[],
                                    v2::finish::Outcome::Failed(v2::ErrorInfo {
                                        code,
                                        message: "fatal".into(),
                                        retryable: false,
                                    }),
                                )
                                .await
                            }
                        };
                        if let Err(e) = res {
                            eprintln!("[{lbl}] Attempt::commit failed for {}: {e}", assign.job_id);
                        }
                        let _ = ctrl
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
    })
}

fn parse_diff_seeds() -> Vec<u64> {
    if let Ok(raw) = std::env::var("ANYSERVE_DIFF_SEEDS") {
        let parsed: Vec<u64> = raw
            .split(',')
            .filter_map(|s| s.trim().parse::<u64>().ok())
            .collect();
        if !parsed.is_empty() {
            return parsed;
        }
    }
    vec![1, 2, 3]
}

#[tokio::test]
async fn diff_seeded_randomized_workload_parity() {
    let seeds = parse_diff_seeds();
    let b = DualBackend::start().await;

    for seed in seeds {
        let mut rng = SplitMix64(seed);
        let plans: Arc<Mutex<HashMap<String, WorkerPlan>>> = Arc::new(Mutex::new(HashMap::new()));
        let rw = spawn_planned_worker(
            "rust",
            &b.rust.ws_base,
            &format!("w-rand-{seed}"),
            Arc::clone(&plans),
        );
        let tw = spawn_planned_worker(
            "ts",
            &b.ts.ws_base,
            &format!("w-rand-{seed}"),
            Arc::clone(&plans),
        );

        let mut r_cli = b.client_rust(Some(OPERATOR_CLIENT_TOKEN)).await;
        let mut t_cli = b.client_ts(Some(OPERATOR_CLIENT_TOKEN)).await;
        let mut submitted: Vec<(String, v2::JobSpec)> = Vec::new();

        for step in 0..20usize {
            let op_roll = rng.range(10);
            if submitted.is_empty() || op_roll < 5 {
                let job_id = format!("rand-s{seed}-{step}");
                let iface = if rng.range(2) == 0 {
                    "rand.alpha.v1"
                } else {
                    "rand.beta.v1"
                };
                let durable = rng.range(3) != 0;
                let prio = rng.range(4) as u32;
                let payload = format!("payload-{seed}-{step}").into_bytes();
                let mut spec = make_spec(iface, vec![inline_obj(&payload)], durable, 3);
                spec.policy.as_mut().unwrap().priority = prio;
                if rng.range(2) == 0 {
                    spec.policy.as_mut().unwrap().concurrency_key = format!("ck-{}", rng.range(2));
                }
                let trace_hdr = format!(
                    "00-{:032x}-{:016x}-01",
                    seed as u128 + step as u128 + 1,
                    step + 1
                );
                let plan = match rng.range(5) {
                    0 => {
                        WorkerPlan::RetryOnceThenSucceed(format!("ok-retried-{step}").into_bytes())
                    }
                    1 => WorkerPlan::FailFatal(format!("fatal_{step}")),
                    _ => WorkerPlan::Succeed(format!("ok-{step}").into_bytes()),
                };
                plans.lock().unwrap().insert(job_id.clone(), plan);

                let req = v2::SubmitRequest {
                    job_id: job_id.clone(),
                    conflict_policy: v2::ConflictPolicy::UseExisting as i32,
                    spec: Some(spec.clone()),
                };
                let hdrs = [("traceparent", trace_hdr.as_str())];
                let (r_res, t_res) = tokio::join!(
                    raw_submit(
                        &b.http,
                        &b.rust.http_base,
                        Some(OPERATOR_CLIENT_TOKEN),
                        None,
                        &req,
                        &hdrs
                    ),
                    raw_submit(
                        &b.http,
                        &b.ts.http_base,
                        Some(OPERATOR_CLIENT_TOKEN),
                        None,
                        &req,
                        &hdrs
                    ),
                );
                let r_sub = r_res
                    .unwrap_or_else(|e| panic!("seed={seed} step={step} op=submit rust err: {e}"));
                let t_sub = t_res
                    .unwrap_or_else(|e| panic!("seed={seed} step={step} op=submit ts err: {e}"));
                assert_eq!(
                    r_sub.created, t_sub.created,
                    "seed={seed} step={step} op=submit created"
                );
                assert_eq!(
                    r_sub.job.as_ref().unwrap().spec_hash,
                    t_sub.job.as_ref().unwrap().spec_hash,
                    "seed={seed} step={step} op=submit spec_hash"
                );
                submitted.push((job_id, spec));
            } else if op_roll < 7 {
                // Idempotent replay
                let idx = rng.range(submitted.len());
                let (jid, spec) = submitted[idx].clone();
                let (r_res, t_res) = tokio::join!(
                    r_cli.submit(Some(jid.clone()), spec.clone()),
                    t_cli.submit(Some(jid.clone()), spec),
                );
                let r_rep = r_res
                    .unwrap_or_else(|e| panic!("seed={seed} step={step} op=replay rust err: {e}"));
                let t_rep = t_res
                    .unwrap_or_else(|e| panic!("seed={seed} step={step} op=replay ts err: {e}"));
                assert!(
                    !r_rep.created && !t_rep.created,
                    "seed={seed} step={step} op=replay created"
                );
                assert_eq!(
                    r_rep.job.as_ref().unwrap().spec_hash,
                    t_rep.job.as_ref().unwrap().spec_hash,
                    "seed={seed} step={step} op=replay spec_hash"
                );
            } else if op_roll == 7 {
                // Conflicting submit with ConflictPolicy::Fail
                let idx = rng.range(submitted.len());
                let (jid, mut spec) = submitted[idx].clone();
                spec.inputs = vec![inline_obj(b"conflicting-mutation")];
                let req = v2::SubmitRequest {
                    job_id: jid.clone(),
                    conflict_policy: v2::ConflictPolicy::Fail as i32,
                    spec: Some(spec),
                };
                let (r_err, t_err) = tokio::join!(
                    raw_submit(
                        &b.http,
                        &b.rust.http_base,
                        Some(OPERATOR_CLIENT_TOKEN),
                        None,
                        &req,
                        &[]
                    ),
                    raw_submit(
                        &b.http,
                        &b.ts.http_base,
                        Some(OPERATOR_CLIENT_TOKEN),
                        None,
                        &req,
                        &[]
                    ),
                );
                assert_eq!(
                    r_err.unwrap_err().code(),
                    Code::AlreadyExists,
                    "seed={seed} step={step} op=conflict rust"
                );
                assert_eq!(
                    t_err.unwrap_err().code(),
                    Code::AlreadyExists,
                    "seed={seed} step={step} op=conflict ts"
                );
            } else if op_roll == 8 {
                // Console /api/jobs query parity (wait for active items so transient in-flight differences don't race)
                for (jid, _) in &submitted {
                    let _ = tokio::join!(
                        wait_terminal_job("rand-rust", &mut r_cli, jid),
                        wait_terminal_job("rand-ts", &mut t_cli, jid),
                    );
                }
                let (r_st, r_list) = console_get(
                    &b.http,
                    &b.rust.http_base,
                    Some(OPERATOR_CLIENT_TOKEN),
                    "/api/jobs?limit=100",
                )
                .await;
                let (t_st, t_list) = console_get(
                    &b.http,
                    &b.ts.http_base,
                    Some(OPERATOR_CLIENT_TOKEN),
                    "/api/jobs?limit=100",
                )
                .await;
                assert_eq!(
                    (r_st, t_st),
                    (200, 200),
                    "seed={seed} step={step} op=console_jobs"
                );
                let r_ids: BTreeSet<String> = r_list["jobs"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|j| j["job_id"].as_str().unwrap().to_owned())
                    .collect();
                let t_ids: BTreeSet<String> = t_list["jobs"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|j| j["job_id"].as_str().unwrap().to_owned())
                    .collect();
                assert_eq!(r_ids, t_ids, "seed={seed} step={step} op=console_jobs ids");
            } else {
                // Cancel a job after settling or while active; then compare settled states
                let idx = rng.range(submitted.len());
                let jid = submitted[idx].0.clone();
                let _ = tokio::join!(
                    wait_terminal_job("rand-rust-cancel", &mut r_cli, &jid),
                    wait_terminal_job("rand-ts-cancel", &mut t_cli, &jid),
                );
                let (r_c, t_c) = tokio::join!(
                    r_cli.cancel(&jid, "rand cancel"),
                    t_cli.cancel(&jid, "rand cancel"),
                );
                assert_job_parity(
                    &format!("seed={seed} step={step} op=cancel({jid})"),
                    &r_c.unwrap(),
                    &t_c.unwrap(),
                );
            }
        }

        // Final full convergence check across every submitted job in this seed
        for (jid, _) in &submitted {
            let (rj, tj) = tokio::join!(
                wait_terminal_job("rand-rust-final", &mut r_cli, jid),
                wait_terminal_job("rand-ts-final", &mut t_cli, jid),
            );
            assert_job_parity(&format!("seed={seed} final({jid})"), &rj, &tj);
        }

        rw.abort();
        tw.abort();
    }
}

#[tokio::test]
async fn differential_cors_and_ws_subprotocol_bearer_auth_match_between_rust_and_ts() {
    use base64::Engine as _;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let http = reqwest::Client::new();

    // 1. Empty allowed_origins: neither Rust nor TS emits Vary or Access-Control-* headers.
    let (rust_empty, ts_empty) = tokio::join!(
        RustServerProc::start_with_allowed_origins(None),
        TsGatewayProc::start_with_allowed_origins(None)
    );
    for (label, path, is_post) in [
        ("overview", "/api/overview", false),
        ("rpc_list", "/rpc/v2/client/list_jobs", true),
    ] {
        let req = |base: &str| {
            let b = if is_post {
                http.post(format!("{base}{path}"))
                    .header("content-type", "application/x-protobuf")
                    .body(Vec::<u8>::new())
            } else {
                http.get(format!("{base}{path}"))
            };
            b.bearer_auth(OPERATOR_CLIENT_TOKEN)
                .header("origin", "https://browser.example.test")
                .send()
        };
        let (rr, tr) = tokio::join!(req(&rust_empty.http_base), req(&ts_empty.http_base));
        let (rr, tr) = (rr.unwrap(), tr.unwrap());
        assert_eq!(
            (rr.status().as_u16(), tr.status().as_u16()),
            (200, 200),
            "{label}"
        );
        for hdr in [
            "vary",
            "access-control-allow-origin",
            "access-control-expose-headers",
        ] {
            assert_eq!(
                (
                    rr.headers().get(hdr).is_some(),
                    tr.headers().get(hdr).is_some()
                ),
                (false, false),
                "empty allowed_origins {label} header {hdr}"
            );
        }
    }
    drop((rust_empty, ts_empty));

    // 2. Configured allowed_origins: Rust & TS match on preflight, /api/*, /rpc/v2/client/* (200 & 403), and disallowed origin.
    let allowed_origin = "https://browser.example.test";
    let (rust, ts) = tokio::join!(
        RustServerProc::start_with_allowed_origins(Some(allowed_origin)),
        TsGatewayProc::start_with_allowed_origins(Some(allowed_origin))
    );

    let header_val = |resp: &reqwest::Response, name: &str| -> Option<String> {
        resp.headers()
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned)
    };

    let (r_pre, t_pre) = tokio::join!(
        http.request(
            reqwest::Method::OPTIONS,
            format!("{}/rpc/v2/client/list_jobs", rust.http_base),
        )
        .header("origin", allowed_origin)
        .send(),
        http.request(
            reqwest::Method::OPTIONS,
            format!("{}/rpc/v2/client/list_jobs", ts.http_base),
        )
        .header("origin", allowed_origin)
        .send(),
    );
    let (r_pre, t_pre) = (r_pre.unwrap(), t_pre.unwrap());
    assert_eq!(
        (r_pre.status().as_u16(), t_pre.status().as_u16()),
        (204, 204)
    );
    for hdr in [
        "vary",
        "access-control-allow-origin",
        "access-control-allow-methods",
        "access-control-allow-headers",
        "access-control-expose-headers",
    ] {
        assert_eq!(
            header_val(&r_pre, hdr),
            header_val(&t_pre, hdr),
            "preflight header {hdr}"
        );
    }
    assert_eq!(
        header_val(&r_pre, "access-control-allow-origin").as_deref(),
        Some(allowed_origin)
    );
    assert_eq!(header_val(&r_pre, "access-control-expose-headers"), None);

    for (label, token, origin, expect_status, expect_grpc_code, expect_cors) in [
        (
            "rpc_ok",
            OPERATOR_CLIENT_TOKEN,
            allowed_origin,
            200u16,
            None,
            true,
        ),
        (
            "rpc_forbidden",
            DEFAULT_WORKER_TOKEN,
            allowed_origin,
            403u16,
            Some("7"),
            true,
        ),
        (
            "rpc_disallowed_origin",
            OPERATOR_CLIENT_TOKEN,
            "https://evil.example.test",
            200u16,
            None,
            false,
        ),
    ] {
        let send_rpc = |base: &str| {
            http.post(format!("{base}/rpc/v2/client/list_jobs"))
                .bearer_auth(token)
                .header("origin", origin)
                .header("content-type", "application/x-protobuf")
                .body(Vec::<u8>::new())
                .send()
        };
        let (rr, tr) = tokio::join!(send_rpc(&rust.http_base), send_rpc(&ts.http_base));
        let (rr, tr) = (rr.unwrap(), tr.unwrap());
        assert_eq!(
            (rr.status().as_u16(), tr.status().as_u16()),
            (expect_status, expect_status),
            "{label}"
        );
        assert_eq!(
            header_val(&rr, "x-anyserve-grpc-code").as_deref(),
            expect_grpc_code,
            "{label} rust grpc"
        );
        assert_eq!(
            header_val(&tr, "x-anyserve-grpc-code").as_deref(),
            expect_grpc_code,
            "{label} ts grpc"
        );
        assert_eq!(
            header_val(&rr, "vary"),
            header_val(&tr, "vary"),
            "{label} vary"
        );
        assert_eq!(
            header_val(&rr, "access-control-allow-origin"),
            header_val(&tr, "access-control-allow-origin"),
            "{label} acao"
        );
        assert_eq!(
            header_val(&rr, "access-control-expose-headers"),
            header_val(&tr, "access-control-expose-headers"),
            "{label} aceh"
        );
        if expect_cors {
            assert_eq!(
                header_val(&rr, "access-control-allow-origin").as_deref(),
                Some(allowed_origin),
                "{label}"
            );
            assert_eq!(
                header_val(&rr, "access-control-expose-headers").as_deref(),
                Some("x-anyserve-grpc-code"),
                "{label}"
            );
        } else {
            assert_eq!(
                header_val(&rr, "access-control-allow-origin"),
                None,
                "{label}"
            );
            assert_eq!(
                header_val(&rr, "access-control-expose-headers"),
                None,
                "{label}"
            );
        }
    }

    // 3. Raw HTTP/1.1 WebSocket upgrade parity for Sec-WebSocket-Protocol bearer auth.
    let raw_ws_upgrade = |http_base: String,
                          path: String,
                          auth_hdr: Option<String>,
                          proto_hdr: Option<String>| async move {
        let addr = http_base.strip_prefix("http://").unwrap().to_owned();
        let mut tcp = tokio::net::TcpStream::connect(&addr).await.unwrap();
        let mut req = format!(
            "GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
        );
        if let Some(a) = auth_hdr {
            req.push_str(&format!("Authorization: {a}\r\n"));
        }
        if let Some(p) = proto_hdr {
            req.push_str(&format!("Sec-WebSocket-Protocol: {p}\r\n"));
        }
        req.push_str("\r\n");
        tcp.write_all(req.as_bytes()).await.unwrap();
        let mut buf = vec![0u8; 2048];
        let mut total = 0usize;
        while total < buf.len() {
            let n = tcp.read(&mut buf[total..]).await.unwrap();
            if n == 0 {
                break;
            }
            total += n;
            if buf[..total].windows(4).any(|w| w == b"\r\n\r\n") {
                break;
            }
        }
        let head = String::from_utf8_lossy(&buf[..total]).into_owned();
        let mut lines = head.split("\r\n");
        let status_line = lines.next().unwrap_or("");
        let status: u16 = status_line
            .split_whitespace()
            .nth(1)
            .unwrap()
            .parse()
            .unwrap();
        let mut headers = HashMap::<String, String>::new();
        for line in lines {
            if line.is_empty() {
                break;
            }
            if let Some((k, v)) = line.split_once(':') {
                headers.insert(k.trim().to_ascii_lowercase(), v.trim().to_owned());
            }
        }
        (status, headers)
    };

    let wrk_b64 = URL_SAFE_NO_PAD.encode(DEFAULT_WORKER_TOKEN);
    let cli_b64 = URL_SAFE_NO_PAD.encode(OPERATOR_CLIENT_TOKEN);
    let bad_b64 = URL_SAFE_NO_PAD.encode("wrong-token");

    for (label, path, auth_hdr, proto_hdr, expect_status, expect_proto, expect_grpc) in [
        (
            "worker_ok",
            "/ws/v2/worker/session",
            None,
            Some(format!("anyserve.v2, anyserve.bearer.{wrk_b64}")),
            101u16,
            Some("anyserve.v2"),
            None,
        ),
        (
            "client_ok",
            "/ws/v2/client/invoke",
            None,
            Some(format!("anyserve.v2, anyserve.bearer.{cli_b64}")),
            101u16,
            Some("anyserve.v2"),
            None,
        ),
        (
            "cross_role_403",
            "/ws/v2/worker/session",
            None,
            Some(format!("anyserve.v2, anyserve.bearer.{cli_b64}")),
            403u16,
            None,
            Some("7"),
        ),
        (
            "bad_token_401",
            "/ws/v2/worker/session",
            None,
            Some(format!("anyserve.v2, anyserve.bearer.{bad_b64}")),
            401u16,
            None,
            Some("16"),
        ),
        (
            "conflict_400",
            "/ws/v2/worker/session",
            Some(format!("Bearer {DEFAULT_WORKER_TOKEN}")),
            Some(format!("anyserve.v2, anyserve.bearer.{wrk_b64}")),
            400u16,
            None,
            Some("3"),
        ),
        (
            "missing_v2_400",
            "/ws/v2/worker/session",
            None,
            Some(format!("anyserve.bearer.{wrk_b64}")),
            400u16,
            None,
            Some("3"),
        ),
        (
            "bad_b64_400",
            "/ws/v2/worker/session",
            None,
            Some("anyserve.v2, anyserve.bearer.bad+chars=".to_string()),
            400u16,
            None,
            Some("3"),
        ),
    ] {
        let (r_res, t_res) = tokio::join!(
            raw_ws_upgrade(
                rust.http_base.clone(),
                path.to_owned(),
                auth_hdr.clone(),
                proto_hdr.clone(),
            ),
            raw_ws_upgrade(
                ts.http_base.clone(),
                path.to_owned(),
                auth_hdr.clone(),
                proto_hdr.clone(),
            ),
        );
        assert_eq!(
            (r_res.0, t_res.0),
            (expect_status, expect_status),
            "ws {label}"
        );
        assert_eq!(
            r_res.1.get("sec-websocket-protocol").map(String::as_str),
            expect_proto,
            "ws {label} rust proto"
        );
        assert_eq!(
            t_res.1.get("sec-websocket-protocol").map(String::as_str),
            expect_proto,
            "ws {label} ts proto"
        );
        assert_eq!(
            r_res.1.get("x-anyserve-grpc-code").map(String::as_str),
            expect_grpc,
            "ws {label} rust grpc"
        );
        assert_eq!(
            t_res.1.get("x-anyserve-grpc-code").map(String::as_str),
            expect_grpc,
            "ws {label} ts grpc"
        );
    }
}
