use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use crate::serve_config::{RuntimeMode, ServeConsoleConfig};
use crate::test_support::{Server, Worker, temporary_path};
use anyserve_client::execution::{Attempt, ExecutionClient, WorkerSession, v2};
use anyserve_core::auth::{AuthPolicy, Role, ScopedToken};
use anyserve_core::runtime::{
    Runtime,
    api::{
        Demand, Durability, ExecutionPolicy, InterfacePolicy, JobSpec, JobState, JournalConfig,
        Metadata, Object, PoolTier, RuntimeConfig, SubmitRequest,
    },
};
use reqwest::{Client, Method, Response};
use serde_json::{Value, json};

use super::api::{AppState, ControlPlaneStatic, runtime_mode_summary};
use super::handlers::{parse_states, router};
use super::{ControlPlaneMetadata, format_host_port, run_server};

struct ConsoleClient {
    http: Client,
    base: String,
    handle: tokio::task::JoinHandle<()>,
}

impl Drop for ConsoleClient {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

impl ConsoleClient {
    async fn spawn(runtime: Runtime, auth: AuthPolicy, meta: ControlPlaneMetadata) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let mode = match runtime.config().journal.as_ref() {
            Some(j) if j.postgres.is_some() => RuntimeMode::MemoryPlusPostgres,
            Some(_) => RuntimeMode::MemoryPlusSqlite,
            None => RuntimeMode::Memory,
        };
        let state = AppState {
            runtime,
            allowed_origins: Arc::new(BTreeSet::from(["http://console.test".to_string()])),
            mode: runtime_mode_summary(mode),
            control_plane: Arc::new(ControlPlaneStatic {
                grpc_listen: meta.grpc_listen,
                grpc_tls_enabled: meta.grpc_tls_enabled,
                console_listen: addr.to_string(),
                auth_enabled: auth.is_enabled(),
            }),
        };
        let app = router(state, Arc::new(auth));
        let handle = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Self {
            http: Client::new(),
            base: format!("http://{addr}"),
            handle,
        }
    }

    async fn send(&self, method: Method, path: &str, token: Option<&str>) -> Response {
        let mut req = self.http.request(method, format!("{}{}", self.base, path));
        if let Some(t) = token {
            req = req.bearer_auth(t);
        }
        req.send().await.unwrap()
    }

    async fn status(&self, method: Method, path: &str, token: Option<&str>) -> u16 {
        self.send(method, path, token).await.status().as_u16()
    }

    async fn json(&self, method: Method, path: &str, token: Option<&str>) -> Value {
        let resp = self.send(method, path, token).await;
        assert_eq!(resp.status().as_u16(), 200);
        resp.json().await.unwrap()
    }

    async fn text(&self, path: &str, token: Option<&str>) -> String {
        let resp = self.send(Method::GET, path, token).await;
        assert_eq!(resp.status().as_u16(), 200);
        resp.text().await.unwrap()
    }
}

async fn test_runtime() -> Runtime {
    Runtime::start(RuntimeConfig::default()).await.unwrap()
}

async fn spawn_console(runtime: Runtime) -> ConsoleClient {
    spawn_console_auth(runtime, AuthPolicy::default()).await
}

async fn spawn_console_auth(runtime: Runtime, auth: AuthPolicy) -> ConsoleClient {
    let meta = ControlPlaneMetadata {
        grpc_listen: "127.0.0.1:50052".into(),
        grpc_tls_enabled: false,
    };
    ConsoleClient::spawn(runtime, auth, meta).await
}

fn spec(interface: &str, durability: Durability) -> JobSpec {
    JobSpec {
        demand: Demand {
            interface: interface.into(),
            ..Default::default()
        },
        policy: ExecutionPolicy {
            durability: Some(durability),
            ..Default::default()
        },
        inputs: vec![Object::inline(b"hello".as_slice())],
        params: b"world".as_slice().into(),
        metadata: Metadata::from([("source".to_string(), "runtime".to_string())]),
    }
}

async fn submit(runtime: &Runtime, ns: Option<&str>, spec: JobSpec) -> String {
    let req = SubmitRequest {
        namespace: ns.map(str::to_owned),
        spec,
        ..Default::default()
    };
    runtime.submit(req).await.unwrap().job.job_id
}

async fn send_msg(session: &mut WorkerSession, msg: v2::worker_message::Msg) {
    session
        .control
        .send(v2::WorkerMessage { msg: Some(msg) })
        .await
        .unwrap();
}

async fn connect_session(
    endpoint: &str,
    ns: Option<&str>,
    hello: v2::Hello,
    credits: Option<u32>,
) -> (WorkerSession, v2::Limits, Duration) {
    let mut s = WorkerSession::connect_with_namespace(endpoint, None, ns, hello)
        .await
        .unwrap();
    let Some(v2::server_message::Msg::Welcome(w)) =
        s.messages.message().await.unwrap().unwrap().msg
    else {
        panic!("missing Welcome");
    };
    let (limits, hb) = (
        w.limits.unwrap(),
        Duration::from_millis(u64::from(w.heartbeat_timeout_ms)),
    );
    if let Some(free_slots) = credits {
        send_msg(
            &mut s,
            v2::worker_message::Msg::Credit(v2::Credit { free_slots }),
        )
        .await;
    }
    (s, limits, hb)
}

async fn accept_assign(session: &mut WorkerSession) -> v2::Assign {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Some(v2::server_message::Msg::Assign(a)) =
                session.messages.message().await.unwrap().unwrap().msg
            {
                let reply = v2::AssignReply {
                    lease_id: a.lease_id.clone(),
                    accepted: true,
                    reject_reason: String::new(),
                };
                send_msg(session, v2::worker_message::Msg::AssignReply(reply)).await;
                return a;
            }
        }
    })
    .await
    .unwrap()
}

async fn wait_until(mut cond: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while !cond() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}

fn cleanup_sqlite(path: &Path) {
    for suffix in ["", "-wal", "-shm", ".lock"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
    }
}

#[test]
fn parse_states_rejects_invalid_values() {
    let error = parse_states(Some("queued,nope")).expect_err("should reject invalid state");
    assert_eq!(error.status, axum::http::StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn console_requires_client_auth_for_reads_and_cancellation() {
    let runtime = test_runtime().await;
    let job_id = submit(&runtime, None, spec("demo.auth.v1", Durability::Ephemeral)).await;
    let c = spawn_console_auth(
        runtime.clone(),
        AuthPolicy::new(["client-secret".into()], ["worker-secret".into()]),
    )
    .await;
    let detail = format!("/api/jobs/{job_id}");
    let cancel = format!("/api/jobs/{job_id}/cancel");
    let paths = [
        "/metrics",
        "/api/overview",
        "/api/jobs",
        "/api/workers",
        "/api/analytics/jobs?source=retained&from_ms=0&to_ms=1",
        &detail,
    ];

    for (token, expected) in [
        (None, 401),
        (Some("wrong"), 401),
        (Some("worker-secret"), 403),
    ] {
        for path in paths {
            assert_eq!(c.status(Method::GET, path, token).await, expected);
        }
        assert_eq!(c.status(Method::POST, &cancel, token).await, expected);
        assert_eq!(
            runtime.get_job(&job_id, false).await.unwrap().state,
            JobState::Queued
        );
    }
    assert_eq!(c.status(Method::GET, "/healthz", None).await, 204);
    let preflight = c
        .http
        .request(Method::OPTIONS, format!("{}/api/jobs", c.base))
        .header("origin", "http://console.test")
        .header("access-control-request-headers", "authorization")
        .send()
        .await
        .unwrap();
    assert_eq!(preflight.status().as_u16(), 204);
    assert_eq!(
        preflight.headers()["access-control-allow-origin"],
        "http://console.test"
    );
    assert!(
        preflight.headers()["access-control-allow-headers"]
            .to_str()
            .unwrap()
            .contains("Authorization")
    );
    assert_eq!(
        c.status(Method::GET, "/api/jobs", Some("client-secret"))
            .await,
        200
    );
    assert_eq!(
        c.status(Method::POST, &cancel, Some("client-secret")).await,
        200
    );
    assert_eq!(
        runtime.get_job(&job_id, false).await.unwrap().state,
        JobState::Cancelled
    );
}

#[tokio::test]
async fn console_rejects_scoped_tenant_tokens() {
    let runtime = test_runtime().await;
    let job_id = submit(
        &runtime,
        Some("tenant-a"),
        spec("demo.auth.v1", Durability::Ephemeral),
    )
    .await;
    let policy = AuthPolicy::new(["operator-client".into()], ["operator-worker".into()])
        .with_scoped_tokens([
            ScopedToken {
                token: "scoped-a-client".into(),
                role: Role::Client,
                namespace: "tenant-a".into(),
                subject: "tenant-a".into(),
                forced_pool: None,
            },
            ScopedToken {
                token: "scoped-default-client".into(),
                role: Role::Client,
                namespace: "default".into(),
                subject: "default".into(),
                forced_pool: None,
            },
        ]);
    let c = spawn_console_auth(runtime, policy).await;
    let detail = format!("/api/jobs/{job_id}");
    let cancel = format!("/api/jobs/{job_id}/cancel");
    let paths = [
        "/metrics",
        "/api/overview",
        "/api/jobs",
        "/api/workers",
        "/api/analytics/jobs?source=retained&from_ms=0&to_ms=1",
        &detail,
    ];

    for token in [
        "scoped-a-client",
        "scoped-default-client",
        "operator-worker",
    ] {
        for path in paths {
            assert_eq!(
                c.status(Method::GET, path, Some(token)).await,
                403,
                "token {token} on {path}"
            );
        }
        assert_eq!(
            c.status(Method::POST, &cancel, Some(token)).await,
            403,
            "token {token} cancel"
        );
    }
    assert_eq!(
        c.status(Method::GET, "/api/jobs", Some("operator-client"))
            .await,
        200
    );
}

#[tokio::test]
async fn analytics_pages_metadata_and_validates_ranges_and_sources() {
    let runtime = test_runtime().await;
    let before = anyserve_core::now_ms().saturating_sub(1000);
    let mut expected = Vec::new();
    for i in 0..5 {
        let mut s = spec("analytics.v1", Durability::Ephemeral);
        s.metadata.insert("tag".into(), format!("value-{i}"));
        s.params = b"private request body".to_vec().into();
        expected.push(submit(&runtime, None, s).await);
    }
    runtime
        .cancel(&expected[0], "failed selection".into())
        .await
        .unwrap();
    let end = anyserve_core::now_ms() + 1;
    let c = spawn_console(runtime).await;
    let (mut cursor, mut received) = (None::<String>, Vec::new());
    loop {
        let mut path =
            format!("/api/analytics/jobs?source=retained&from_ms={before}&to_ms={end}&limit=2");
        if let Some(cur) = cursor {
            path.push_str(&format!("&cursor={cur}"));
        }
        let body = c.json(Method::GET, &path, None).await;
        let rows = body["jobs"].as_array().unwrap();
        assert!(rows.len() <= 2);
        for row in rows {
            assert_eq!(row["interface_name"], "analytics.v1");
            assert_eq!(row["durability"], "ephemeral");
            assert!(row["metadata"]["tag"].is_string());
            assert!(row.get("params").is_none() && row.get("outputs").is_none());
            received.push(row["job_id"].as_str().unwrap().to_owned());
        }
        cursor = body["next_cursor"].as_str().map(str::to_owned);
        if cursor.is_none() {
            break;
        }
    }
    expected.sort();
    received.sort();
    assert_eq!(received, expected);
    for q in [
        "source=archive&from_ms=0&to_ms=100",
        "source=unknown&from_ms=0&to_ms=100",
        "from_ms=0&to_ms=100",
        "source=retained&from_ms=10&to_ms=10",
        "source=retained&from_ms=0&to_ms=9999999999",
        "source=retained&from_ms=0&to_ms=100&cursor=invalid",
    ] {
        assert_eq!(
            c.status(Method::GET, &format!("/api/analytics/jobs?{q}"), None)
                .await,
            400
        );
    }
}

#[tokio::test]
async fn overview_reports_runtime_queue_history_and_workers() {
    let server = Server::start(RuntimeConfig::default(), AuthPolicy::default()).await;
    let runtime = server.runtime.clone();
    let _worker =
        Worker::connect(&server, "worker-overview", vec!["demo.worker.v1".into()], 4).await;
    submit(&runtime, None, spec("demo.queue.v1", Durability::Ephemeral)).await;
    let cancelled = submit(
        &runtime,
        None,
        spec("demo.history.v1", Durability::Ephemeral),
    )
    .await;
    runtime
        .cancel(&cancelled, "test cancellation".into())
        .await
        .unwrap();

    let c = spawn_console(runtime).await;
    let body = c.json(Method::GET, "/api/overview", None).await;
    assert_eq!(body["runtime"]["code"], "memory");
    assert_eq!(body["queue"]["queued"], 1);
    assert_eq!(body["history"]["cancelled"], 1);
    assert_eq!(body["workers"]["healthy"], 1);
    assert_eq!(body["workers"]["draining"], 0);
    let interfaces = body["interfaces"].as_array().unwrap();
    let find = |name: &str| interfaces.iter().find(|i| i["name"] == name).unwrap();
    assert_eq!(find("demo.queue.v1")["queued"], 1);
    assert_eq!(find("demo.worker.v1")["sessions"], 1);
    assert_eq!(find("demo.worker.v1")["free_slots"], 4);
}

#[tokio::test]
async fn jobs_endpoint_filters_and_paginates() {
    let runtime = test_runtime().await;
    let first_id = submit(&runtime, None, spec("demo.first.v1", Durability::Ephemeral)).await;
    let second_id = submit(
        &runtime,
        None,
        spec("demo.second.v1", Durability::Ephemeral),
    )
    .await;
    runtime
        .cancel(&second_id, "test cancellation".into())
        .await
        .unwrap();
    submit(&runtime, None, spec("demo.third.v1", Durability::Ephemeral)).await;

    let c = spawn_console(runtime).await;
    let body = c
        .json(
            Method::GET,
            "/api/jobs?states=cancelled&limit=1&offset=0",
            None,
        )
        .await;
    assert_eq!(body["total"], 1);
    assert!(body["next_page_token"].is_null());
    assert_eq!(body["jobs"].as_array().unwrap().len(), 1);
    assert_eq!(body["jobs"][0]["state"], "cancelled");
    assert_eq!(body["jobs"][0]["job_id"], second_id);
    assert_ne!(body["jobs"][0]["job_id"], first_id);
}

#[tokio::test]
async fn jobs_endpoint_supports_search_worker_time_window_total_and_histogram() {
    let server = Server::start(RuntimeConfig::default(), AuthPolicy::default()).await;
    let (mut worker_alpha, limits_a, hb_a) = connect_session(
        &server.endpoint,
        None,
        v2::Hello {
            worker_id: "worker-alpha".into(),
            interfaces: vec!["embed.v1".into()],
            max_concurrency: 2,
            ..Default::default()
        },
        Some(2),
    )
    .await;
    let (mut worker_beta, limits_b, hb_b) = connect_session(
        &server.endpoint,
        None,
        v2::Hello {
            worker_id: "worker-beta".into(),
            interfaces: vec!["chat.v1".into()],
            max_concurrency: 2,
            ..Default::default()
        },
        Some(2),
    )
    .await;

    let mut s1 = spec("embed.v1", Durability::Ephemeral);
    s1.metadata.insert("source".into(), "mobile-ios".into());
    s1.metadata.insert("trace".into(), "tr-ALPHA-99".into());
    let id1 = submit(&server.runtime, None, s1).await;
    let a1 = accept_assign(&mut worker_alpha).await;
    Attempt::open(&mut worker_alpha.data, &a1, &limits_a, hb_a)
        .await
        .unwrap()
        .finish(&[], v2::finish::Outcome::Succeeded(v2::Succeeded {}))
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(25)).await;
    let split_ts = anyserve_core::now_ms();
    tokio::time::sleep(Duration::from_millis(5)).await;

    let mut s2 = spec("chat.v1", Durability::Ephemeral);
    s2.metadata.insert("source".into(), "web-console".into());
    let id2 = submit(&server.runtime, None, s2).await;
    let a2 = accept_assign(&mut worker_beta).await;
    Attempt::open(&mut worker_beta.data, &a2, &limits_b, hb_b)
        .await
        .unwrap()
        .finish(
            &[],
            v2::finish::Outcome::Failed(v2::ErrorInfo {
                code: "rate_limited".into(),
                message: "Quota Exceeded on GPU".into(),
                retryable: false,
            }),
        )
        .await
        .unwrap();

    let mut s3 = spec("embed.v1", Durability::Ephemeral);
    s3.metadata.insert("source".into(), "cli-batch".into());
    let id3 = submit(&server.runtime, None, s3).await;
    let a3 = accept_assign(&mut worker_alpha).await;
    Attempt::open(&mut worker_alpha.data, &a3, &limits_a, hb_a)
        .await
        .unwrap()
        .finish(&[], v2::finish::Outcome::Succeeded(v2::Succeeded {}))
        .await
        .unwrap();

    let c = spawn_console(server.runtime.clone()).await;

    // Exact total persists across paginated requests.
    let page1 = c
        .json(Method::GET, "/api/jobs?limit=2&offset=0&buckets=16", None)
        .await;
    assert_eq!(page1["total"], 3);
    assert_eq!(page1["next_page_token"], "2");
    assert_eq!(page1["jobs"].as_array().unwrap().len(), 2);

    let histogram = &page1["histogram"];
    let buckets = histogram["buckets"].as_array().unwrap();
    assert_eq!(buckets.len(), 16);
    let mut bucket_sum = 0u64;
    let mut non_empty_bucket: Option<(u64, u64, u64)> = None;
    for b in buckets {
        let count = b["active"].as_u64().unwrap()
            + b["succeeded"].as_u64().unwrap()
            + b["failed"].as_u64().unwrap()
            + b["cancelled"].as_u64().unwrap();
        bucket_sum += count;
        if count > 0 && non_empty_bucket.is_none() {
            non_empty_bucket = Some((
                b["start_ms"].as_u64().unwrap(),
                b["end_ms"].as_u64().unwrap(),
                count,
            ));
        }
    }
    assert_eq!(bucket_sum, 3);

    // Clicking a single bucket yields the exact same matching count via from_ms/to_ms.
    let (b_start, b_end, b_count) = non_empty_bucket.unwrap();
    let clicked = c
        .json(
            Method::GET,
            &format!("/api/jobs?from_ms={b_start}&to_ms={b_end}&buckets=16"),
            None,
        )
        .await;
    assert_eq!(clicked["total"].as_u64().unwrap(), b_count);

    // Worker filter narrows results accurately.
    let by_worker = c
        .json(Method::GET, "/api/jobs?worker=worker-beta", None)
        .await;
    assert_eq!(by_worker["total"], 1);
    assert_eq!(by_worker["jobs"][0]["job_id"], id2);

    // Server-side search matches metadata value, error message, and job_id (case-insensitive).
    let by_meta = c.json(Method::GET, "/api/jobs?q=alpha-99", None).await;
    assert_eq!(by_meta["total"], 1);
    assert_eq!(by_meta["jobs"][0]["job_id"], id1);

    let by_error = c
        .json(Method::GET, "/api/jobs?q=quota%20exceeded", None)
        .await;
    assert_eq!(by_error["total"], 1);
    assert_eq!(by_error["jobs"][0]["job_id"], id2);

    // Time window filter + search compose together cleanly.
    let after_split = c
        .json(
            Method::GET,
            &format!("/api/jobs?from_ms={split_ts}&worker=worker-alpha"),
            None,
        )
        .await;
    assert_eq!(after_split["total"], 1);
    assert_eq!(after_split["jobs"][0]["job_id"], id3);

    let recent = c
        .json(Method::GET, "/api/jobs?window_ms=60000&buckets=8", None)
        .await;
    assert_eq!(recent["total"], 3);

    // Invalid ranges/queries return 400.
    assert_eq!(
        c.status(Method::GET, "/api/jobs?from_ms=100&to_ms=100", None)
            .await,
        400
    );
    assert_eq!(
        c.status(Method::GET, "/api/jobs?window_ms=0", None).await,
        400
    );
    let long_q = "x".repeat(257);
    assert_eq!(
        c.status(Method::GET, &format!("/api/jobs?q={long_q}"), None)
            .await,
        400
    );

    drop(c);
    drop(worker_alpha);
    drop(worker_beta);
    server.stop().await;
}

#[tokio::test]
async fn job_detail_and_cancel_endpoint_return_payloads() {
    let runtime = test_runtime().await;
    let job_id = submit(
        &runtime,
        None,
        spec("demo.detail.v1", Durability::Ephemeral),
    )
    .await;
    let c = spawn_console(runtime).await;
    let body = c
        .json(Method::GET, &format!("/api/jobs/{job_id}"), None)
        .await;
    assert!(!body["events"].as_array().unwrap().is_empty());
    assert_eq!(body["history_incomplete"], false);
    assert!(body.get("streams").is_none());
    assert_eq!(body["job"]["params_size_bytes"], 5);
    assert_eq!(body["job"]["inputs"][0]["kind"], "inline");
    assert!(body["job"]["inputs"][0].get("content").is_none());

    let cancel_body = c
        .json(Method::POST, &format!("/api/jobs/{job_id}/cancel"), None)
        .await;
    assert_eq!(cancel_body["job"]["state"], "cancelled");
}

#[tokio::test]
async fn workers_endpoint_reports_health() {
    let server = Server::start(RuntimeConfig::default(), AuthPolicy::default()).await;
    let _worker = Worker::connect(&server, "worker-health", vec!["demo.worker.v1".into()], 4).await;
    let c = spawn_console(server.runtime.clone()).await;
    let body = c.json(Method::GET, "/api/workers", None).await;
    assert_eq!(body["total"], 1);
    assert_eq!(body["healthy"], 1);
    assert_eq!(body["workers"][0]["worker_id"], "worker-health");
    assert_eq!(body["workers"][0]["healthy"], true);
}

#[tokio::test]
async fn evicted_durable_details_show_missing_spec_and_incomplete_history() {
    let path = temporary_path("console-journal.sqlite");
    let cfg = RuntimeConfig {
        journal: Some(JournalConfig::new(&path)),
        max_terminal_jobs: 0,
        ..Default::default()
    };
    let server = Server::start(cfg, AuthPolicy::default()).await;
    let mut worker = Worker::connect(&server, "cold-console", vec!["demo.cold.v1".into()], 1).await;
    let job_id = submit(
        &server.runtime,
        None,
        spec("demo.cold.v1", Durability::Durable),
    )
    .await;
    let _assignment = worker.assignment().await;
    server.runtime.cancel(&job_id, "test".into()).await.unwrap();
    wait_until(|| server.runtime.spec(&job_id).is_err()).await;

    let c = spawn_console(server.runtime.clone()).await;
    let listed = c.json(Method::GET, "/api/jobs", None).await;
    assert!(listed["jobs"].as_array().unwrap().is_empty());
    assert!(listed["next_page_token"].is_null());

    let body = c
        .json(Method::GET, &format!("/api/jobs/{job_id}"), None)
        .await;
    assert_eq!(body["job"]["state"], "cancelled");
    assert!(body["job"]["demand"].is_null());
    assert!(body["job"]["policy"].is_null());
    assert!(body["job"]["inputs"].as_array().unwrap().is_empty());
    assert_eq!(body["history_incomplete"], true);
    assert!(body["attempts"].as_array().unwrap().is_empty());
    assert!(body.get("streams").is_none());
    drop(c);
    server.stop().await;
    cleanup_sqlite(&path);
}

#[tokio::test]
async fn cold_result_preview_is_bounded_and_works_with_an_exhausted_result_budget() {
    const SIZE: usize = 5 * 1024 * 1024;
    const BINARY_SIZE: usize = 8192;
    let path = temporary_path("console-result.sqlite");
    let cfg = RuntimeConfig {
        journal: Some(JournalConfig::new(&path)),
        max_terminal_jobs: 0,
        max_result_bytes: (SIZE + BINARY_SIZE + 128) as u64,
        ..Default::default()
    };
    let server = Server::start(cfg, AuthPolicy::default()).await;
    let mut worker = Worker::connect(&server, "cold-result", vec!["demo.cold.v1".into()], 1).await;
    let job_id = submit(
        &server.runtime,
        None,
        spec("demo.cold.v1", Durability::Durable),
    )
    .await;
    let assignment = worker.assignment().await;
    let mut attempt = Attempt::open(
        &mut worker.session.data,
        &assignment,
        &worker.limits,
        worker.heartbeat_timeout,
    )
    .await
    .unwrap();
    attempt
        .finish(
            &[
                v2::Object {
                    body: Some(v2::object::Body::Inline(vec![b'a'; SIZE].into())),
                    metadata: [("content_type".into(), "text/plain".into())].into(),
                },
                v2::Object {
                    body: Some(v2::object::Body::Inline(vec![0; BINARY_SIZE].into())),
                    ..Default::default()
                },
                v2::Object {
                    body: Some(v2::object::Body::Uri("s3://media/video.mp4".into())),
                    metadata: [("content_type".into(), "video/mp4".into())].into(),
                },
                v2::Object {
                    body: Some(v2::object::Body::Inline(Vec::new().into())),
                    ..Default::default()
                },
            ],
            v2::finish::Outcome::Succeeded(v2::Succeeded {}),
        )
        .await
        .unwrap();
    drop(attempt);
    wait_until(|| server.runtime.stats().result_bytes == 0).await;

    // A business reader holds the full result. Another full read must fail,
    // but concurrent management previews must still work and leave that budget alone.
    let held = server.runtime.get_job(&job_id, true).await.unwrap();
    assert_eq!(held.outputs.as_ref().unwrap()[0].inline_len(), SIZE);
    assert_eq!(
        server.runtime.stats().result_bytes,
        (SIZE + BINARY_SIZE) as u64
    );
    assert_eq!(
        server
            .runtime
            .get_job(&job_id, true)
            .await
            .unwrap_err()
            .kind,
        anyserve_core::error::ErrorKind::ResourceExhausted
    );

    let c = spawn_console(server.runtime.clone()).await;
    let detail_path = format!("/api/jobs/{job_id}");
    for body in
        futures::future::join_all((0..8).map(|_| c.json(Method::GET, &detail_path, None))).await
    {
        assert_eq!(body["job"]["output_preview"]["preview"], "a".repeat(4096));
        assert_eq!(
            body["job"]["output_preview"]["size_bytes"],
            SIZE + BINARY_SIZE
        );
        assert_eq!(body["job"]["output_preview"]["truncated"], true);
        assert_eq!(body["job"]["output_preview"]["content_type"], "text/plain");
        let outputs = &body["job"]["outputs"];
        assert_eq!(outputs[0]["size_bytes"], SIZE);
        assert_eq!(outputs[0]["preview"], "a".repeat(4096));
        assert_eq!(outputs[0]["truncated"], true);
        assert_eq!(outputs[1]["size_bytes"], BINARY_SIZE);
        assert!(outputs[1]["preview"].is_null());
        assert_eq!(outputs[2]["uri"], "s3://media/video.mp4");
        assert_eq!(outputs[2]["content_type"], "video/mp4");
        assert!(outputs[2]["size_bytes"].is_null());
        assert_eq!(outputs[3]["size_bytes"], 0);
        assert_eq!(outputs[3]["truncated"], false);
        assert!(outputs[3]["preview"].is_null());
        assert_eq!(
            server.runtime.stats().result_bytes,
            (SIZE + BINARY_SIZE) as u64
        );
    }
    drop(held);
    assert_eq!(server.runtime.stats().result_bytes, 0);
    drop(c);
    server.stop().await;
    cleanup_sqlite(&path);
}

#[tokio::test]
async fn management_metrics_follow_native_jobs_and_worker_reservations() {
    let path = temporary_path("metrics.sqlite");
    let cfg = RuntimeConfig {
        journal: Some(JournalConfig::new(&path)),
        ..Default::default()
    };
    let server = Server::start(cfg, AuthPolicy::default()).await;
    let c = spawn_console(server.runtime.clone()).await;
    let mut client = ExecutionClient::connect_with_token(&server.endpoint, None)
        .await
        .unwrap();
    client
        .submit(
            None,
            v2::JobSpec {
                demand: Some(v2::Demand {
                    interface: "demo.metrics.v1".into(),
                    ..Default::default()
                }),
                inputs: vec![v2::Object {
                    body: Some(v2::object::Body::Inline(b"payload".to_vec().into())),
                    ..Default::default()
                }],
                policy: Some(v2::ExecutionPolicy {
                    durability: v2::Durability::Durable as i32,
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let resp = c.send(Method::GET, "/metrics", None).await;
    assert_eq!(resp.headers()["content-type"], "text/plain; version=0.0.4");
    let queued = resp.text().await.unwrap();
    for line in [
        "anyserve_jobs{state=\"queued\"} 1\n",
        "anyserve_live_workers 0\n",
        "anyserve_budget_bytes{budget=\"input\"} 7\n",
        "anyserve_interface_jobs{interface=\"demo.metrics.v1\",state=\"queued\"} 1\n",
    ] {
        assert!(queued.contains(line), "missing {line}");
    }

    let mut worker =
        Worker::connect(&server, "metrics-worker", vec!["demo.metrics.v1".into()], 2).await;
    let assignment = worker.assignment().await;
    let running = c.text("/metrics", None).await;
    for line in [
        "anyserve_jobs{state=\"running\"} 1\n",
        "anyserve_live_workers 1\n",
        "anyserve_active_leases 1\n",
        "anyserve_interface_free_slots{interface=\"demo.metrics.v1\"} 1\n",
    ] {
        assert!(running.contains(line), "missing {line}");
    }

    let mut attempt = Attempt::open(
        &mut worker.session.data,
        &assignment,
        &worker.limits,
        worker.heartbeat_timeout,
    )
    .await
    .unwrap();
    attempt
        .finish(&[], v2::finish::Outcome::Succeeded(v2::Succeeded {}))
        .await
        .unwrap();
    let terminal = c.text("/metrics", None).await;
    for line in [
        "anyserve_jobs{state=\"succeeded\"} 1\n",
        "anyserve_active_leases 0\n",
        "anyserve_budget_bytes{budget=\"input\"} 0\n",
        "# TYPE anyserve_journal_committed_ops_total counter\n",
        "anyserve_journal_committed_ops_total 3\n",
        "# TYPE anyserve_journal_committed_batches_total counter\n",
        "# TYPE anyserve_journal_last_commit_duration_seconds gauge\n",
    ] {
        assert!(terminal.contains(line), "missing {line}");
    }
    assert!(!terminal.contains("anyserve_journal_last_commit_seconds"));
    for line in terminal.lines().filter(|l| l.starts_with("# TYPE ")) {
        let family = line.split_whitespace().nth(2).unwrap();
        assert!(
            terminal
                .lines()
                .any(|l| l.starts_with(&format!("# HELP {family} "))),
            "missing HELP for {family}"
        );
    }
    drop(c);
    server.stop().await;
    cleanup_sqlite(&path);
}

#[tokio::test]
async fn management_liveness_and_readiness_are_separate() {
    let runtime = test_runtime().await;
    let c = spawn_console_auth(
        runtime.clone(),
        AuthPolicy::new(["client".into()], ["worker".into()]),
    )
    .await;
    assert_eq!(c.status(Method::GET, "/readyz", None).await, 204);
    runtime.shutdown().await;
    assert_eq!(c.status(Method::GET, "/healthz", None).await, 204);
    assert_eq!(c.status(Method::GET, "/readyz", None).await, 503);
    assert!(
        c.text("/metrics", Some("client"))
            .await
            .contains("anyserve_runtime_ready 0\n")
    );
}

#[test]
fn format_host_port_handles_ipv4_hostnames_and_ipv6() {
    assert_eq!(format_host_port("127.0.0.1", 50052), "127.0.0.1:50052");
    assert_eq!(format_host_port("localhost", 50052), "localhost:50052");
    assert_eq!(format_host_port("::1", 50052), "[::1]:50052");
    assert_eq!(format_host_port("[::1]", 50052), "[::1]:50052");
}

#[tokio::test]
async fn overview_exposes_health_control_plane_budgets_journal_policies_and_pools() {
    let path = temporary_path("overview-enriched.sqlite");
    let cfg = RuntimeConfig {
        journal: Some(JournalConfig::new(&path)),
        max_sessions: 128,
        priority_levels: 8,
        default_max_attempts: 5,
        heartbeat_interval: Duration::from_secs(2),
        heartbeat_timeout: Duration::from_secs(9),
        reconnect_grace: Some(Duration::from_secs(4)),
        max_pending_bytes: 1_000_000,
        max_result_bytes: 2_000_000,
        max_event_bytes: 3_000_000,
        interfaces: BTreeMap::from([
            (
                "alpha.idle.v1".into(),
                InterfacePolicy {
                    max_queue: Some(50),
                    queue_timeout: Some(Duration::from_millis(1200)),
                    max_in_flight_per_key: Some(4),
                    pool_tiers: vec![PoolTier {
                        pool: "gpu-primary".into(),
                        spillover_after: Duration::from_millis(500),
                        reconnect_grace: Some(Duration::from_secs(2)),
                    }],
                },
            ),
            (
                "zebra.active.v1".into(),
                InterfacePolicy {
                    max_queue: Some(10),
                    pool_tiers: vec![PoolTier {
                        pool: "gpu-primary".into(),
                        spillover_after: Duration::from_millis(0),
                        reconnect_grace: None,
                    }],
                    ..Default::default()
                },
            ),
        ]),
        ..Default::default()
    };
    let server = Server::start(cfg, AuthPolicy::default()).await;
    let hello = v2::Hello {
        worker_id: "pool-worker".into(),
        pool: "gpu-primary".into(),
        interfaces: vec!["zebra.active.v1".into(), "dynamic.unconfigured.v1".into()],
        max_concurrency: 3,
        total_capacity: [("gpu".into(), 1)].into(),
        ..Default::default()
    };
    let (mut session, _, _) = connect_session(&server.endpoint, None, hello, Some(3)).await;
    wait_until(|| !server.runtime.list_workers().is_empty()).await;

    let mut first = spec("zebra.active.v1", Durability::Durable);
    first.inputs = vec![Object::inline(b"hello world".as_slice())];
    submit(&server.runtime, None, first).await;
    accept_assign(&mut session).await;

    // Exhaust free slots and create queued demand for desired_additional_slots > 0.
    for _ in 0..4 {
        let mut s = spec("zebra.active.v1", Durability::Durable);
        s.inputs = vec![Object::inline(b"hello world".as_slice())];
        s.demand
            .required_attributes
            .insert("region".into(), "unreachable".into());
        submit(&server.runtime, None, s).await;
    }

    let c = ConsoleClient::spawn(
        server.runtime.clone(),
        AuthPolicy::new(["op-secret".into()], []),
        ControlPlaneMetadata {
            grpc_listen: "0.0.0.0:50062".into(),
            grpc_tls_enabled: true,
        },
    )
    .await;
    let body = c
        .json(Method::GET, "/api/overview", Some("op-secret"))
        .await;

    assert_eq!(body["runtime"]["code"], "memory_plus_sqlite");
    assert_eq!(body["runtime"]["journal"], "sqlite");
    assert_eq!(
        body["health"],
        json!({"live": true, "ready": true, "journal": {"status": "healthy", "message": null}})
    );

    let cp = &body["control_plane"];
    assert_eq!(cp["grpc_listen"], "0.0.0.0:50062");
    assert_eq!(cp["grpc_tls_enabled"], true);
    assert!(
        cp["console_listen"]
            .as_str()
            .unwrap()
            .starts_with("127.0.0.1:")
    );
    assert_eq!(cp["auth_enabled"], true);
    assert!(cp["generation"].as_u64().unwrap() >= 1);
    for (k, v) in [
        ("retained_jobs", 0),
        ("sessions", 1),
        ("max_sessions", 128),
        ("priority_levels", 8),
        ("default_max_attempts", 5),
        ("heartbeat_interval_ms", 2000),
        ("heartbeat_timeout_ms", 9000),
        ("reconnect_grace_ms", 4000),
    ] {
        assert_eq!(cp[k], v, "cp.{k}");
    }

    let budgets = &body["budgets"];
    for (k, v) in [
        ("input_bytes", 80),
        ("max_pending_bytes", 1_000_000),
        ("result_bytes", 0),
        ("max_result_bytes", 2_000_000),
        ("max_event_bytes", 3_000_000),
    ] {
        assert_eq!(budgets[k], v, "budgets.{k}");
    }
    assert!(budgets["event_bytes"].as_u64().unwrap() > 0);
    assert!(body["journal_stats"]["committed_batches"].as_u64().unwrap() >= 1);
    assert!(body["journal_stats"]["committed_ops"].as_u64().unwrap() >= 5);

    let ifaces = body["interfaces"].as_array().unwrap();
    let names: Vec<&str> = ifaces.iter().map(|i| i["name"].as_str().unwrap()).collect();
    assert_eq!(
        names,
        [
            "alpha.idle.v1",
            "dynamic.unconfigured.v1",
            "zebra.active.v1"
        ]
    );
    assert_eq!(ifaces[0]["queued"], 0);
    assert_eq!(ifaces[0]["sessions"], 0);
    assert_eq!(ifaces[0]["policy"]["max_queue"], 50);
    assert_eq!(ifaces[0]["policy"]["queue_timeout_ms"], 1200);
    assert_eq!(ifaces[0]["policy"]["max_in_flight_per_key"], 4);
    assert_eq!(
        ifaces[0]["policy"]["pool_tiers"][0],
        json!({"pool": "gpu-primary", "spillover_after_ms": 500, "reconnect_grace_ms": 2000})
    );
    assert!(ifaces[1]["policy"].is_null());
    assert_eq!(ifaces[1]["sessions"], 1);
    assert_eq!(ifaces[1]["free_slots"], 2);
    assert_eq!(ifaces[2]["queued"], 4);
    assert_eq!(ifaces[2]["running"], 1);
    assert_eq!(ifaces[2]["policy"]["max_queue"], 10);

    let pool = body["pools"]
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p["interface"] == "zebra.active.v1" && p["pool"] == "gpu-primary")
        .unwrap();
    assert_eq!(
        pool,
        &json!({"namespace": "default", "interface": "zebra.active.v1", "pool": "gpu-primary", "sessions": 1, "free_slots": 2, "active_leases": 1, "desired_additional_slots": 2})
    );

    drop(c);
    drop(session);
    server.stop().await;
    cleanup_sqlite(&path);
}

#[tokio::test]
async fn overview_memory_mode_reports_disabled_journal_and_null_journal_stats() {
    let c = spawn_console(test_runtime().await).await;
    let body = c.json(Method::GET, "/api/overview", None).await;
    assert_eq!(
        body["health"],
        json!({"live": true, "ready": true, "journal": {"status": "disabled", "message": null}})
    );
    assert!(body["journal_stats"].is_null());
    assert_eq!(body["control_plane"]["auth_enabled"], false);
    assert!(body["control_plane"]["reconnect_grace_ms"].is_null());
    assert!(body["pools"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn run_server_serves_control_plane_metadata_and_shuts_down_cleanly() {
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let cfg = ServeConsoleConfig {
        listen: "127.0.0.1:0".into(),
        allowed_origins: vec![" http://console.test ".into(), "".into()],
    };
    let meta = ControlPlaneMetadata {
        grpc_listen: "127.0.0.1:50099".into(),
        grpc_tls_enabled: true,
    };
    let task = tokio::spawn(run_server(
        addr,
        test_runtime().await,
        cfg,
        meta,
        RuntimeMode::Memory,
        Arc::new(AuthPolicy::default()),
        shutdown_rx,
    ));
    let _ = shutdown_tx.send(true);
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn workers_endpoint_reports_session_pool_namespace_load_draining_and_disconnected() {
    let cfg = RuntimeConfig {
        reconnect_grace: Some(Duration::from_secs(30)),
        ..Default::default()
    };
    let server = Server::start(cfg, AuthPolicy::default()).await;

    let (mut healthy, _, _) = connect_session(
        &server.endpoint,
        Some("team-alpha"),
        v2::Hello {
            worker_id: "w-healthy".into(),
            pool: "spot".into(),
            interfaces: vec!["demo.v1".into()],
            max_concurrency: 4,
            ..Default::default()
        },
        Some(4),
    )
    .await;
    send_msg(
        &mut healthy,
        v2::worker_message::Msg::Heartbeat(v2::Heartbeat {
            load: Some(v2::Load {
                queued: 2,
                running: 1,
                utilization: 0.75,
                custom: [("vram_used_mb".into(), 4096.0)].into(),
            }),
        }),
    )
    .await;

    let (mut draining, _, _) = connect_session(
        &server.endpoint,
        None,
        v2::Hello {
            worker_id: "w-draining".into(),
            pool: "ondemand".into(),
            interfaces: vec!["demo.v1".into()],
            max_concurrency: 2,
            ..Default::default()
        },
        None,
    )
    .await;
    send_msg(
        &mut draining,
        v2::worker_message::Msg::Draining(v2::Draining { preempting: false }),
    )
    .await;

    let (mut disconnected, _, _) = connect_session(
        &server.endpoint,
        None,
        v2::Hello {
            worker_id: "w-disconnected".into(),
            pool: "edge".into(),
            interfaces: vec!["demo.disconnect.v1".into()],
            max_concurrency: 1,
            ..Default::default()
        },
        Some(1),
    )
    .await;
    wait_until(|| {
        server
            .runtime
            .list_workers()
            .iter()
            .any(|w| w.worker_id == "w-disconnected")
    })
    .await;
    submit(
        &server.runtime,
        None,
        spec("demo.disconnect.v1", Durability::Ephemeral),
    )
    .await;
    accept_assign(&mut disconnected).await;
    wait_until(|| {
        server
            .runtime
            .list_workers()
            .iter()
            .any(|w| w.worker_id == "w-disconnected" && w.active_leases == 1)
    })
    .await;
    drop(disconnected);

    wait_until(|| {
        let ws = server.runtime.list_workers();
        ws.len() == 3
            && ws
                .iter()
                .any(|w| w.worker_id == "w-healthy" && w.load.queued == 2)
            && ws.iter().any(|w| w.worker_id == "w-draining" && w.draining)
            && ws
                .iter()
                .any(|w| w.worker_id == "w-disconnected" && !w.connected)
    })
    .await;

    let c = spawn_console(server.runtime.clone()).await;
    let body = c.json(Method::GET, "/api/workers", None).await;
    assert_eq!(body["total"], 3);
    assert_eq!(body["healthy"], 1);
    assert_eq!(body["draining"], 1);
    assert_eq!(body["disconnected"], 1);

    let w_h = body["workers"]
        .as_array()
        .unwrap()
        .iter()
        .find(|w| w["worker_id"] == "w-healthy")
        .unwrap();
    assert!(!w_h["session_id"].as_str().unwrap().is_empty());
    assert_eq!(w_h["namespace"], "team-alpha");
    assert_eq!(w_h["pool"], "spot");
    assert!(w_h["connected_at_ms"].as_u64().unwrap() > 0);
    assert_eq!(w_h["utilization"], 0.75);
    assert_eq!(
        w_h["load"],
        json!({"queued": 2, "running": 1, "utilization": 0.75, "custom": {"vram_used_mb": 4096.0}})
    );

    let overview = c.json(Method::GET, "/api/overview", None).await;
    assert_eq!(overview["workers"]["total"], 3);
    assert_eq!(overview["workers"]["healthy"], 1);
    assert_eq!(overview["workers"]["draining"], 1);
    assert_eq!(overview["workers"]["unavailable"], 2);

    drop(c);
    drop(healthy);
    drop(draining);
    server.stop().await;
}

#[tokio::test]
async fn jobs_endpoints_filter_and_expose_enriched_summary_and_detail_fields() {
    let server = Server::start(RuntimeConfig::default(), AuthPolicy::default()).await;
    let (mut worker_beta, limits, hb) = connect_session(
        &server.endpoint,
        Some("team-beta"),
        v2::Hello {
            worker_id: "shared-worker-id".into(),
            pool: "beta-pool".into(),
            interfaces: vec!["iface.a".into()],
            max_concurrency: 2,
            ..Default::default()
        },
        Some(2),
    )
    .await;
    // Also connect a default-namespace worker with the SAME worker_id to verify namespace isolation.
    let _worker_default =
        Worker::connect(&server, "shared-worker-id", vec!["iface.b".into()], 1).await;

    let before = anyserve_core::now_ms().saturating_sub(1000);
    let mut beta_spec = spec("iface.a", Durability::Ephemeral);
    beta_spec.demand.affinity_key = "aff-1".into();
    beta_spec.policy.fairness_key = "fair-1".into();
    beta_spec.policy.concurrency_key = "conc-1".into();
    let beta_failed_id = submit(&server.runtime, Some("team-beta"), beta_spec).await;

    let assign = accept_assign(&mut worker_beta).await;
    let mut attempt = Attempt::open(&mut worker_beta.data, &assign, &limits, hb)
        .await
        .unwrap();
    attempt
        .finish(
            &[],
            v2::finish::Outcome::Failed(v2::ErrorInfo {
                code: "bad_input".into(),
                message: "bad payload".into(),
                retryable: false,
            }),
        )
        .await
        .unwrap();

    let mut unreach_a = spec("iface.a", Durability::Ephemeral);
    unreach_a
        .demand
        .required_attributes
        .insert("unreachable".into(), "true".into());
    let beta_cancel_id = submit(&server.runtime, Some("team-beta"), unreach_a).await;

    let mut unreach_b = spec("iface.b", Durability::Ephemeral);
    unreach_b
        .demand
        .required_attributes
        .insert("unreachable".into(), "true".into());
    let default_id = submit(&server.runtime, None, unreach_b).await;

    let end = anyserve_core::now_ms() + 1;
    let c = spawn_console(server.runtime.clone()).await;

    assert_eq!(
        c.json(Method::GET, "/api/jobs?interface=iface.a", None)
            .await["jobs"]
            .as_array()
            .unwrap()
            .len(),
        2
    );

    let by_ns = c
        .json(
            Method::GET,
            "/api/jobs?namespace=team-beta&states=failed",
            None,
        )
        .await;
    let row = &by_ns["jobs"].as_array().unwrap()[0];
    assert_eq!(by_ns["jobs"].as_array().unwrap().len(), 1);
    assert_eq!(row["job_id"], beta_failed_id);
    assert_eq!(row["namespace"], "team-beta");
    assert_eq!(row["durability"], "ephemeral");
    assert_eq!(row["last_error"], "bad payload");
    assert_eq!(row["error_code"], "bad_input");
    assert_eq!(row["error_retryable"], false);

    assert_eq!(
        c.json(Method::GET, "/api/jobs?interface=%20%20&namespace=", None)
            .await["jobs"]
            .as_array()
            .unwrap()
            .len(),
        3
    );

    let detail = c
        .json(
            Method::GET,
            &format!("/api/jobs/{beta_failed_id}?namespace=team-beta"),
            None,
        )
        .await;
    assert_eq!(detail["job"]["namespace"], "team-beta");
    assert_eq!(detail["job"]["durability"], "ephemeral");
    assert_eq!(detail["job"]["error_code"], "bad_input");
    assert_eq!(detail["job"]["error_retryable"], false);
    assert_eq!(detail["job"]["demand"]["affinity_key"], "aff-1");
    assert_eq!(detail["job"]["policy"]["fairness_key"], "fair-1");
    assert_eq!(detail["job"]["policy"]["concurrency_key"], "conc-1");
    assert_eq!(detail["latest_worker"]["namespace"], "team-beta");
    assert_eq!(detail["latest_worker"]["pool"], "beta-pool");

    assert_eq!(
        c.status(Method::GET, &format!("/api/jobs/{beta_failed_id}"), None)
            .await,
        404
    );
    assert_eq!(
        c.status(Method::GET, &format!("/api/jobs/{default_id}"), None)
            .await,
        200
    );

    let cancelled = c
        .json(
            Method::POST,
            &format!("/api/jobs/{beta_cancel_id}/cancel?namespace=team-beta"),
            None,
        )
        .await;
    assert_eq!(cancelled["job"]["state"], "cancelled");
    assert_eq!(cancelled["job"]["namespace"], "team-beta");
    assert_eq!(cancelled["job"]["error_code"], "cancelled");

    let analytics = c.json(Method::GET, &format!("/api/analytics/jobs?source=retained&from_ms={before}&to_ms={end}&namespace=team-beta"), None).await;
    let rows = analytics["jobs"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert!(
        rows.iter()
            .all(|j| j["namespace"] == "team-beta" && j["durability"] == "ephemeral")
    );

    drop(c);
    drop(worker_beta);
    server.stop().await;
}

#[tokio::test]
async fn cors_middleware_allows_only_configured_origins() {
    let c = spawn_console(test_runtime().await).await;
    let allowed = c
        .http
        .get(format!("{}/api/overview", c.base))
        .header("origin", "http://console.test")
        .send()
        .await
        .unwrap();
    assert_eq!(
        allowed.headers()["access-control-allow-origin"],
        "http://console.test"
    );
    let disallowed = c
        .http
        .get(format!("{}/api/overview", c.base))
        .header("origin", "http://evil.test")
        .send()
        .await
        .unwrap();
    assert!(
        disallowed
            .headers()
            .get("access-control-allow-origin")
            .is_none()
    );
}
