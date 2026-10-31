use std::collections::BTreeSet;
use std::sync::Arc;

use crate::test_support::{Server, Worker};
use anyserve_core::auth::AuthPolicy;
use anyserve_core::runtime::{
    Runtime,
    api::{
        Demand, Durability, ExecutionPolicy, JobSpec, JobState, Metadata, Object, RuntimeConfig,
        SubmitRequest,
    },
};
use serde_json::Value;

use crate::serve_config::RuntimeMode;

use super::api::{AppState, runtime_mode_summary};
use super::handlers::{parse_states, router};

#[test]
fn parse_states_rejects_invalid_values() {
    let error = parse_states(Some("queued,nope")).expect_err("should reject invalid state");
    assert_eq!(error.status, axum::http::StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn console_requires_client_auth_for_reads_and_cancellation() {
    let runtime = test_runtime().await;
    let job = runtime
        .submit(SubmitRequest {
            spec: test_job_spec("demo.auth.v1"),
            ..Default::default()
        })
        .await
        .unwrap()
        .job;
    let policy = AuthPolicy::new(["client-secret".into()], ["worker-secret".into()]);
    let (base_url, handle) = spawn_test_server_with_auth(runtime.clone(), policy).await;
    let client = reqwest::Client::new();

    for (token, expected) in [
        (None, 401),
        (Some("wrong"), 401),
        (Some("worker-secret"), 403),
    ] {
        for path in [
            "/metrics",
            "/api/overview",
            "/api/jobs",
            "/api/workers",
            &format!("/api/jobs/{}", job.job_id),
        ] {
            let mut request = client.get(format!("{base_url}{path}"));
            if let Some(token) = token {
                request = request.bearer_auth(token);
            }
            assert_eq!(request.send().await.unwrap().status().as_u16(), expected);
        }
        let mut request = client.post(format!("{base_url}/api/jobs/{}/cancel", job.job_id));
        if let Some(token) = token {
            request = request.bearer_auth(token);
        }
        assert_eq!(request.send().await.unwrap().status().as_u16(), expected);
        assert_eq!(
            runtime.get_job(&job.job_id, false).await.unwrap().state,
            JobState::Queued
        );
    }
    assert_eq!(
        client
            .get(format!("{base_url}/healthz"))
            .send()
            .await
            .unwrap()
            .status()
            .as_u16(),
        204
    );
    let preflight = client
        .request(reqwest::Method::OPTIONS, format!("{base_url}/api/jobs"))
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
        client
            .get(format!("{base_url}/api/jobs"))
            .bearer_auth("client-secret")
            .send()
            .await
            .unwrap()
            .status()
            .as_u16(),
        200
    );
    assert_eq!(
        client
            .post(format!("{base_url}/api/jobs/{}/cancel", job.job_id))
            .bearer_auth("client-secret")
            .send()
            .await
            .unwrap()
            .status()
            .as_u16(),
        200
    );
    assert_eq!(
        runtime.get_job(&job.job_id, false).await.unwrap().state,
        JobState::Cancelled
    );
    handle.abort();
}

#[tokio::test]
async fn overview_reports_runtime_queue_history_and_workers() {
    let server = Server::start(RuntimeConfig::default(), AuthPolicy::default()).await;
    let runtime = server.runtime.clone();
    let _worker =
        Worker::connect(&server, "worker-overview", vec!["demo.worker.v1".into()], 4).await;
    runtime
        .submit(SubmitRequest {
            spec: test_job_spec("demo.queue.v1"),
            ..Default::default()
        })
        .await
        .unwrap();
    let cancelled = runtime
        .submit(SubmitRequest {
            spec: test_job_spec("demo.history.v1"),
            ..Default::default()
        })
        .await
        .unwrap()
        .job;
    runtime
        .cancel(&cancelled.job_id, "test cancellation".into())
        .await
        .unwrap();

    let (base_url, handle) = spawn_test_server(runtime).await;
    let response = reqwest::get(format!("{base_url}/api/overview"))
        .await
        .unwrap();
    assert_eq!(response.status(), axum::http::StatusCode::OK);

    let body: Value = response.json().await.unwrap();
    assert_eq!(body["runtime"]["code"], "memory");
    assert_eq!(body["queue"]["queued"], 1);
    assert_eq!(body["history"]["cancelled"], 1);
    assert_eq!(body["workers"]["healthy"], 1);
    let interfaces = body["interfaces"].as_array().unwrap();
    let find = |name: &str| interfaces.iter().find(|i| i["name"] == name).unwrap();
    assert_eq!(find("demo.queue.v1")["queued"], 1);
    assert_eq!(find("demo.worker.v1")["sessions"], 1);
    assert_eq!(find("demo.worker.v1")["free_slots"], 4);

    handle.abort();
}

#[tokio::test]
async fn jobs_endpoint_filters_and_paginates() {
    let runtime = test_runtime().await;
    let first = runtime
        .submit(SubmitRequest {
            spec: test_job_spec("demo.first.v1"),
            ..Default::default()
        })
        .await
        .unwrap()
        .job;
    let second = runtime
        .submit(SubmitRequest {
            spec: test_job_spec("demo.second.v1"),
            ..Default::default()
        })
        .await
        .unwrap()
        .job;
    runtime
        .cancel(&second.job_id, "test cancellation".into())
        .await
        .unwrap();
    runtime
        .submit(SubmitRequest {
            spec: test_job_spec("demo.third.v1"),
            ..Default::default()
        })
        .await
        .unwrap();

    let (base_url, handle) = spawn_test_server(runtime).await;
    let response = reqwest::get(format!(
        "{base_url}/api/jobs?states=cancelled&limit=1&offset=0"
    ))
    .await
    .unwrap();
    assert_eq!(response.status(), axum::http::StatusCode::OK);

    let body: Value = response.json().await.unwrap();
    assert!(body["next_page_token"].is_null());
    assert_eq!(body["jobs"].as_array().unwrap().len(), 1);
    assert_eq!(body["jobs"][0]["state"], "cancelled");
    assert_eq!(body["jobs"][0]["job_id"], second.job_id);
    assert_ne!(body["jobs"][0]["job_id"], first.job_id);

    handle.abort();
}

#[tokio::test]
async fn job_detail_and_cancel_endpoint_return_payloads() {
    let runtime = test_runtime().await;
    let job = runtime
        .submit(SubmitRequest {
            spec: test_job_spec("demo.detail.v1"),
            ..Default::default()
        })
        .await
        .unwrap()
        .job;

    let (base_url, handle) = spawn_test_server(runtime.clone()).await;
    let client = reqwest::Client::new();

    let detail = client
        .get(format!("{base_url}/api/jobs/{}", job.job_id))
        .send()
        .await
        .unwrap();
    assert_eq!(detail.status(), axum::http::StatusCode::OK);
    let body: Value = detail.json().await.unwrap();
    assert!(!body["events"].as_array().unwrap().is_empty());
    assert_eq!(body["history_incomplete"], false);
    assert!(body.get("streams").is_none());
    assert_eq!(body["job"]["params_size_bytes"], 5);
    assert_eq!(body["job"]["inputs"][0]["kind"], "inline");
    assert!(body["job"]["inputs"][0].get("content").is_none());

    let cancel = client
        .post(format!("{base_url}/api/jobs/{}/cancel", job.job_id))
        .send()
        .await
        .unwrap();
    assert_eq!(cancel.status(), axum::http::StatusCode::OK);
    let cancel_body: Value = cancel.json().await.unwrap();
    assert_eq!(cancel_body["job"]["state"], "cancelled");

    handle.abort();
}

#[tokio::test]
async fn workers_endpoint_reports_health() {
    let server = Server::start(RuntimeConfig::default(), AuthPolicy::default()).await;
    let runtime = server.runtime.clone();
    let _worker = Worker::connect(&server, "worker-health", vec!["demo.worker.v1".into()], 4).await;

    let (base_url, handle) = spawn_test_server(runtime).await;
    let response = reqwest::get(format!("{base_url}/api/workers"))
        .await
        .unwrap();
    assert_eq!(response.status(), axum::http::StatusCode::OK);

    let body: Value = response.json().await.unwrap();
    assert_eq!(body["total"], 1);
    assert_eq!(body["healthy"], 1);
    assert_eq!(body["workers"][0]["worker_id"], "worker-health");
    assert_eq!(body["workers"][0]["healthy"], true);

    handle.abort();
}

#[tokio::test]
async fn evicted_durable_details_show_missing_spec_and_incomplete_history() {
    use crate::test_support::temporary_path;
    use anyserve_core::runtime::api::JournalConfig;
    use std::time::Duration;
    let path = temporary_path("console-journal.sqlite");
    let server = Server::start(
        RuntimeConfig {
            journal: Some(JournalConfig::new(&path)),
            max_terminal_jobs: 0,
            ..Default::default()
        },
        AuthPolicy::default(),
    )
    .await;
    let mut worker = Worker::connect(&server, "cold-console", vec!["demo.cold.v1".into()], 1).await;
    let mut spec = test_job_spec("demo.cold.v1");
    spec.policy.durability = Some(Durability::Durable);
    let job = server
        .runtime
        .submit(SubmitRequest {
            spec,
            ..Default::default()
        })
        .await
        .unwrap()
        .job;
    let _assignment = worker.assignment().await;
    server
        .runtime
        .cancel(&job.job_id, "test".into())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while server.runtime.spec(&job.job_id).is_ok() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let (url, handle) = spawn_test_server(server.runtime.clone()).await;
    let listed = reqwest::get(format!("{url}/api/jobs")).await.unwrap();
    assert_eq!(listed.status().as_u16(), 200);
    let listed: Value = listed.json().await.unwrap();
    assert!(listed["jobs"].as_array().unwrap().is_empty());
    assert!(listed["next_page_token"].is_null());

    let response = reqwest::get(format!("{url}/api/jobs/{}", job.job_id))
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 200);
    let body: Value = response.json().await.unwrap();
    assert_eq!(body["job"]["state"], "cancelled");
    assert!(body["job"]["demand"].is_null());
    assert!(body["job"]["policy"].is_null());
    assert!(body["job"]["inputs"].as_array().unwrap().is_empty());
    assert_eq!(body["history_incomplete"], true);
    assert!(body["attempts"].as_array().unwrap().is_empty());
    assert!(body.get("streams").is_none());
    handle.abort();
    server.stop().await;
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn cold_result_preview_fits_a_single_result_budget() {
    use anyserve_client::execution::{Attempt, v2};
    use anyserve_core::runtime::api::JournalConfig;
    use std::time::Duration;

    let path = crate::test_support::temporary_path("console-result.sqlite");
    let server = Server::start(
        RuntimeConfig {
            journal: Some(JournalConfig::new(&path)),
            max_terminal_jobs: 0,
            max_result_bytes: 128,
            ..Default::default()
        },
        AuthPolicy::default(),
    )
    .await;
    let mut worker = Worker::connect(&server, "cold-result", vec!["demo.cold.v1".into()], 1).await;
    let mut spec = test_job_spec("demo.cold.v1");
    spec.policy.durability = Some(Durability::Durable);
    let job = server
        .runtime
        .submit(SubmitRequest {
            spec,
            ..Default::default()
        })
        .await
        .unwrap()
        .job;
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
            &[v2::Object {
                body: Some(v2::object::Body::Inline(vec![b'a'; 128].into())),
                ..Default::default()
            }],
            v2::finish::Outcome::Succeeded(v2::Succeeded {}),
        )
        .await
        .unwrap();
    drop(attempt);
    tokio::time::timeout(Duration::from_secs(2), async {
        while server.runtime.stats().result_bytes != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let (url, handle) = spawn_test_server(server.runtime.clone()).await;
    for _ in 0..2 {
        let response = reqwest::get(format!("{url}/api/jobs/{}", job.job_id))
            .await
            .unwrap();
        assert_eq!(response.status().as_u16(), 200);
        let body: Value = response.json().await.unwrap();
        assert_eq!(body["job"]["output_preview"]["preview"], "a".repeat(128));
        assert_eq!(body["job"]["outputs"][0]["size_bytes"], 128);
        assert_eq!(server.runtime.stats().result_bytes, 0);
    }
    handle.abort();
    server.stop().await;
    let _ = std::fs::remove_file(path);
}

async fn test_runtime() -> Runtime {
    Runtime::start(RuntimeConfig::default()).await.unwrap()
}

async fn spawn_test_server(runtime: Runtime) -> (String, tokio::task::JoinHandle<()>) {
    spawn_test_server_with_auth(runtime, AuthPolicy::default()).await
}

async fn spawn_test_server_with_auth(
    runtime: Runtime,
    auth: AuthPolicy,
) -> (String, tokio::task::JoinHandle<()>) {
    let state = AppState {
        runtime,
        allowed_origins: Arc::new(BTreeSet::from(["http://console.test".to_string()])),
        mode: runtime_mode_summary(RuntimeMode::Memory),
    };
    let app = router(state, Arc::new(auth));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}"), handle)
}

fn test_job_spec(interface_name: &str) -> JobSpec {
    JobSpec {
        demand: Demand {
            interface: interface_name.into(),
            ..Default::default()
        },
        policy: ExecutionPolicy {
            durability: Some(Durability::Ephemeral),
            ..Default::default()
        },
        inputs: vec![Object::inline(b"hello".as_slice())],
        params: b"world".as_slice().into(),
        metadata: Metadata::from([("source".to_string(), "runtime".to_string())]),
    }
}

#[tokio::test]
async fn management_metrics_follow_native_jobs_and_worker_reservations() {
    use anyserve_client::execution::{Attempt, ExecutionClient, v2};

    let path = crate::test_support::temporary_path("metrics.sqlite");
    let server = Server::start(
        RuntimeConfig {
            journal: Some(anyserve_core::runtime::api::JournalConfig::new(&path)),
            ..Default::default()
        },
        AuthPolicy::default(),
    )
    .await;
    let (url, handle) = spawn_test_server(server.runtime.clone()).await;
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
    let response = reqwest::get(format!("{url}/metrics")).await.unwrap();
    assert_eq!(
        response.headers()["content-type"],
        "text/plain; version=0.0.4"
    );
    let queued = response.text().await.unwrap();
    assert!(queued.contains("anyserve_jobs{state=\"queued\"} 1\n"));
    assert!(queued.contains("anyserve_live_workers 0\n"));
    assert!(queued.contains("anyserve_budget_bytes{budget=\"input\"} 7\n"));
    assert!(
        queued.contains(
            "anyserve_interface_jobs{interface=\"demo.metrics.v1\",state=\"queued\"} 1\n"
        )
    );

    let mut worker =
        Worker::connect(&server, "metrics-worker", vec!["demo.metrics.v1".into()], 2).await;
    let assignment = worker.assignment().await;
    let running = reqwest::get(format!("{url}/metrics"))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(running.contains("anyserve_jobs{state=\"running\"} 1\n"));
    assert!(running.contains("anyserve_live_workers 1\n"));
    assert!(running.contains("anyserve_active_leases 1\n"));
    assert!(running.contains("anyserve_interface_free_slots{interface=\"demo.metrics.v1\"} 1\n"));
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
    let terminal = reqwest::get(format!("{url}/metrics"))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(terminal.contains("anyserve_jobs{state=\"succeeded\"} 1\n"));
    assert!(terminal.contains("anyserve_active_leases 0\n"));
    assert!(terminal.contains("anyserve_budget_bytes{budget=\"input\"} 0\n"));
    assert!(terminal.contains("# TYPE anyserve_journal_committed_ops_total counter\n"));
    assert!(terminal.contains("anyserve_journal_committed_ops_total 3\n"));
    assert!(terminal.contains("# TYPE anyserve_journal_committed_batches_total counter\n"));
    assert!(terminal.contains("# TYPE anyserve_journal_last_commit_duration_seconds gauge\n"));
    assert!(!terminal.contains("anyserve_journal_last_commit_seconds"));
    for line in terminal.lines().filter(|line| line.starts_with("# TYPE ")) {
        let family = line.split_whitespace().nth(2).unwrap();
        assert!(
            terminal
                .lines()
                .any(|line| line.starts_with(&format!("# HELP {family} "))),
            "missing HELP for {family}"
        );
    }
    handle.abort();
    server.stop().await;
    for suffix in ["", "-wal", "-shm", ".lock"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
    }
}

#[tokio::test]
async fn management_liveness_and_readiness_are_separate() {
    let runtime = test_runtime().await;
    let (url, handle) = spawn_test_server_with_auth(
        runtime.clone(),
        AuthPolicy::new(["client".into()], ["worker".into()]),
    )
    .await;
    assert_eq!(
        reqwest::get(format!("{url}/readyz"))
            .await
            .unwrap()
            .status()
            .as_u16(),
        204
    );
    runtime.shutdown().await;
    assert_eq!(
        reqwest::get(format!("{url}/healthz"))
            .await
            .unwrap()
            .status()
            .as_u16(),
        204
    );
    assert_eq!(
        reqwest::get(format!("{url}/readyz"))
            .await
            .unwrap()
            .status()
            .as_u16(),
        503
    );
    let metrics = reqwest::Client::new()
        .get(format!("{url}/metrics"))
        .bearer_auth("client")
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(metrics.contains("anyserve_runtime_ready 0\n"));
    handle.abort();
}
