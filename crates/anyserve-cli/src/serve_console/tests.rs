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

use super::api::{AppState, ControlPlaneStatic, runtime_mode_summary};
use super::handlers::{parse_states, router};
use super::{ControlPlaneMetadata, format_host_port, run_server};

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
            "/api/analytics/jobs?source=retained&from_ms=0&to_ms=1",
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
async fn console_rejects_scoped_tenant_tokens() {
    use anyserve_core::auth::{Role, ScopedToken};

    let runtime = test_runtime().await;
    let job = runtime
        .submit(SubmitRequest {
            namespace: Some("tenant-a".into()),
            spec: test_job_spec("demo.auth.v1"),
            ..Default::default()
        })
        .await
        .unwrap()
        .job;
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
    let (base_url, handle) = spawn_test_server_with_auth(runtime, policy).await;
    let client = reqwest::Client::new();

    for token in [
        "scoped-a-client",
        "scoped-default-client",
        "operator-worker",
    ] {
        for path in [
            "/metrics",
            "/api/overview",
            "/api/jobs",
            "/api/workers",
            "/api/analytics/jobs?source=retained&from_ms=0&to_ms=1",
            &format!("/api/jobs/{}", job.job_id),
        ] {
            let status = client
                .get(format!("{base_url}{path}"))
                .bearer_auth(token)
                .send()
                .await
                .unwrap()
                .status()
                .as_u16();
            assert_eq!(status, 403, "token {token} must be forbidden on {path}");
        }
        let status = client
            .post(format!("{base_url}/api/jobs/{}/cancel", job.job_id))
            .bearer_auth(token)
            .send()
            .await
            .unwrap()
            .status()
            .as_u16();
        assert_eq!(status, 403, "token {token} cancel must be forbidden");
    }

    let ok = client
        .get(format!("{base_url}/api/jobs"))
        .bearer_auth("operator-client")
        .send()
        .await
        .unwrap()
        .status()
        .as_u16();
    assert_eq!(ok, 200);
    handle.abort();
}

#[tokio::test]
async fn analytics_pages_metadata_and_validates_ranges_and_sources() {
    let runtime = test_runtime().await;
    let before = anyserve_core::now_ms().saturating_sub(1000);
    let mut expected = Vec::new();
    for index in 0..5 {
        let mut spec = test_job_spec("analytics.v1");
        spec.metadata.insert("tag".into(), format!("value-{index}"));
        spec.params = b"private request body".to_vec().into();
        let job = runtime
            .submit(SubmitRequest {
                spec,
                ..Default::default()
            })
            .await
            .unwrap()
            .job;
        expected.push(job.job_id);
    }
    runtime
        .cancel(&expected[0], "failed selection".into())
        .await
        .unwrap();
    let end = anyserve_core::now_ms() + 1;
    let (base, handle) = spawn_test_server(runtime).await;
    let client = reqwest::Client::new();
    let mut cursor = None;
    let mut received = Vec::new();
    loop {
        let mut query = vec![
            ("source", "retained".to_string()),
            ("from_ms", before.to_string()),
            ("to_ms", end.to_string()),
            ("limit", "2".to_string()),
        ];
        if let Some(cursor) = cursor {
            query.push(("cursor", cursor));
        }
        let response = client
            .get(format!("{base}/api/analytics/jobs"))
            .query(&query)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status().as_u16(), 200);
        let body: Value = response.json().await.unwrap();
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
    for query in [
        "source=archive&from_ms=0&to_ms=100",
        "source=unknown&from_ms=0&to_ms=100",
        "from_ms=0&to_ms=100",
        "source=retained&from_ms=10&to_ms=10",
        "source=retained&from_ms=0&to_ms=9999999999",
        "source=retained&from_ms=0&to_ms=100&cursor=invalid",
    ] {
        assert_eq!(
            client
                .get(format!("{base}/api/analytics/jobs?{query}"))
                .send()
                .await
                .unwrap()
                .status()
                .as_u16(),
            400
        );
    }
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
async fn cold_result_preview_is_bounded_and_works_with_an_exhausted_result_budget() {
    use anyserve_client::execution::{Attempt, v2};
    use anyserve_core::runtime::api::JournalConfig;
    use std::time::Duration;

    const SIZE: usize = 5 * 1024 * 1024;
    const BINARY_SIZE: usize = 8192;
    let path = crate::test_support::temporary_path("console-result.sqlite");
    let server = Server::start(
        RuntimeConfig {
            journal: Some(JournalConfig::new(&path)),
            max_terminal_jobs: 0,
            max_result_bytes: (SIZE + BINARY_SIZE + 128) as u64,
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
    tokio::time::timeout(Duration::from_secs(2), async {
        while server.runtime.stats().result_bytes != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    // A business reader holds the full result. Another full read must fail,
    // but concurrent management previews must still work and leave that budget alone.
    let held = server.runtime.get_job(&job.job_id, true).await.unwrap();
    assert_eq!(held.outputs.as_ref().unwrap()[0].inline_len(), SIZE);
    assert_eq!(
        server.runtime.stats().result_bytes,
        (SIZE + BINARY_SIZE) as u64
    );
    assert_eq!(
        server
            .runtime
            .get_job(&job.job_id, true)
            .await
            .unwrap_err()
            .kind,
        anyserve_core::error::ErrorKind::ResourceExhausted
    );
    let (url, handle) = spawn_test_server(server.runtime.clone()).await;
    let responses = futures::future::join_all(
        (0..8).map(|_| reqwest::get(format!("{url}/api/jobs/{}", job.job_id))),
    )
    .await;
    for response in responses {
        let response = response.unwrap();
        assert_eq!(response.status().as_u16(), 200);
        let body: Value = response.json().await.unwrap();
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
    spawn_test_server_with_metadata(
        runtime,
        auth,
        ControlPlaneMetadata {
            grpc_listen: "127.0.0.1:50052".into(),
            grpc_tls_enabled: false,
        },
    )
    .await
}

async fn spawn_test_server_with_metadata(
    runtime: Runtime,
    auth: AuthPolicy,
    control_plane: ControlPlaneMetadata,
) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mode = match runtime.config().journal.as_ref() {
        Some(journal) if journal.postgres.is_some() => RuntimeMode::MemoryPlusPostgres,
        Some(_) => RuntimeMode::MemoryPlusSqlite,
        None => RuntimeMode::Memory,
    };
    let state = AppState {
        runtime,
        allowed_origins: Arc::new(BTreeSet::from(["http://console.test".to_string()])),
        mode: runtime_mode_summary(mode),
        control_plane: Arc::new(ControlPlaneStatic {
            grpc_listen: control_plane.grpc_listen,
            grpc_tls_enabled: control_plane.grpc_tls_enabled,
            console_listen: addr.to_string(),
            auth_enabled: auth.is_enabled(),
        }),
    };
    let app = router(state, Arc::new(auth));
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

#[test]
fn format_host_port_handles_ipv4_hostnames_and_ipv6() {
    assert_eq!(format_host_port("127.0.0.1", 50052), "127.0.0.1:50052");
    assert_eq!(format_host_port("localhost", 50052), "localhost:50052");
    assert_eq!(format_host_port("::1", 50052), "[::1]:50052");
    assert_eq!(format_host_port("[::1]", 50052), "[::1]:50052");
}

#[tokio::test]
async fn overview_exposes_health_control_plane_budgets_journal_policies_and_pools() {
    use crate::test_support::temporary_path;
    use anyserve_client::execution::{WorkerSession, v2};
    use anyserve_core::runtime::api::{InterfacePolicy, JournalConfig, PoolTier};
    use std::collections::BTreeMap;
    use std::time::Duration;

    let path = temporary_path("overview-enriched.sqlite");
    let config = RuntimeConfig {
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
    let server = Server::start(config, AuthPolicy::default()).await;
    let mut session = WorkerSession::connect(
        &server.endpoint,
        None,
        v2::Hello {
            worker_id: "pool-worker".into(),
            pool: "gpu-primary".into(),
            interfaces: vec!["zebra.active.v1".into(), "dynamic.unconfigured.v1".into()],
            max_concurrency: 3,
            total_capacity: [("gpu".into(), 1)].into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let _welcome = session.messages.message().await.unwrap().unwrap();
    session
        .control
        .send(v2::WorkerMessage {
            msg: Some(v2::worker_message::Msg::Credit(v2::Credit {
                free_slots: 3,
            })),
        })
        .await
        .unwrap();

    // Wait for worker session to register before submitting jobs so first job leases cleanly.
    tokio::time::timeout(Duration::from_secs(2), async {
        while server.runtime.list_workers().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    let mut first_spec = test_job_spec("zebra.active.v1");
    first_spec.policy.durability = Some(Durability::Durable);
    first_spec.inputs = vec![Object::inline(b"hello world".as_slice())];
    server
        .runtime
        .submit(SubmitRequest {
            spec: first_spec,
            ..Default::default()
        })
        .await
        .unwrap();

    // Accept the first assignment so 1 lease is active and 2 slots remain free.
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Some(v2::server_message::Msg::Assign(assign)) =
                session.messages.message().await.unwrap().unwrap().msg
            {
                session
                    .control
                    .send(v2::WorkerMessage {
                        msg: Some(v2::worker_message::Msg::AssignReply(v2::AssignReply {
                            lease_id: assign.lease_id,
                            accepted: true,
                            reject_reason: String::new(),
                        })),
                    })
                    .await
                    .unwrap();
                break;
            }
        }
    })
    .await
    .unwrap();

    // Exhaust free slots and create queued demand for desired_additional_slots > 0.
    for _ in 0..4 {
        let mut spec = test_job_spec("zebra.active.v1");
        spec.policy.durability = Some(Durability::Durable);
        spec.inputs = vec![Object::inline(b"hello world".as_slice())];
        spec.demand
            .required_attributes
            .insert("region".into(), "unreachable".into());
        server
            .runtime
            .submit(SubmitRequest {
                spec,
                ..Default::default()
            })
            .await
            .unwrap();
    }

    let auth = AuthPolicy::new(["op-secret".into()], []);
    let (base_url, handle) = spawn_test_server_with_metadata(
        server.runtime.clone(),
        auth,
        ControlPlaneMetadata {
            grpc_listen: "0.0.0.0:50062".into(),
            grpc_tls_enabled: true,
        },
    )
    .await;

    let response = reqwest::Client::new()
        .get(format!("{base_url}/api/overview"))
        .bearer_auth("op-secret")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 200);
    let body: Value = response.json().await.unwrap();

    assert_eq!(body["runtime"]["code"], "memory_plus_sqlite");
    assert_eq!(body["runtime"]["journal"], "sqlite");
    assert_eq!(body["health"]["live"], true);
    assert_eq!(body["health"]["ready"], true);
    assert_eq!(body["health"]["journal"]["status"], "healthy");
    assert!(body["health"]["journal"]["message"].is_null());

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
    assert_eq!(cp["retained_jobs"], 0);
    assert_eq!(cp["sessions"], 1);
    assert_eq!(cp["max_sessions"], 128);
    assert_eq!(cp["priority_levels"], 8);
    assert_eq!(cp["default_max_attempts"], 5);
    assert_eq!(cp["heartbeat_interval_ms"], 2000);
    assert_eq!(cp["heartbeat_timeout_ms"], 9000);
    assert_eq!(cp["reconnect_grace_ms"], 4000);

    let budgets = &body["budgets"];
    assert_eq!(budgets["input_bytes"], 80);
    assert_eq!(budgets["max_pending_bytes"], 1_000_000);
    assert_eq!(budgets["result_bytes"], 0);
    assert_eq!(budgets["max_result_bytes"], 2_000_000);
    assert!(budgets["event_bytes"].as_u64().unwrap() > 0);
    assert_eq!(budgets["max_event_bytes"], 3_000_000);

    let journal_stats = &body["journal_stats"];
    assert!(journal_stats.is_object());
    assert!(journal_stats["committed_batches"].as_u64().unwrap() >= 1);
    assert!(journal_stats["committed_ops"].as_u64().unwrap() >= 5);

    let interfaces = body["interfaces"].as_array().unwrap();
    let names: Vec<&str> = interfaces
        .iter()
        .map(|item| item["name"].as_str().unwrap())
        .collect();
    assert_eq!(
        names,
        vec![
            "alpha.idle.v1",
            "dynamic.unconfigured.v1",
            "zebra.active.v1"
        ]
    );
    assert_eq!(interfaces[0]["queued"], 0);
    assert_eq!(interfaces[0]["sessions"], 0);
    assert_eq!(interfaces[0]["policy"]["max_queue"], 50);
    assert_eq!(interfaces[0]["policy"]["queue_timeout_ms"], 1200);
    assert_eq!(interfaces[0]["policy"]["max_in_flight_per_key"], 4);
    assert_eq!(
        interfaces[0]["policy"]["pool_tiers"][0]["pool"],
        "gpu-primary"
    );
    assert_eq!(
        interfaces[0]["policy"]["pool_tiers"][0]["spillover_after_ms"],
        500
    );
    assert_eq!(
        interfaces[0]["policy"]["pool_tiers"][0]["reconnect_grace_ms"],
        2000
    );
    assert!(interfaces[1]["policy"].is_null());
    assert_eq!(interfaces[1]["sessions"], 1);
    assert_eq!(interfaces[1]["free_slots"], 2);
    assert_eq!(interfaces[2]["queued"], 4);
    assert_eq!(interfaces[2]["running"], 1);
    assert_eq!(interfaces[2]["policy"]["max_queue"], 10);

    let pools = body["pools"].as_array().unwrap();
    let zebra_pool = pools
        .iter()
        .find(|p| p["interface"] == "zebra.active.v1" && p["pool"] == "gpu-primary")
        .unwrap();
    assert_eq!(zebra_pool["namespace"], "default");
    assert_eq!(zebra_pool["sessions"], 1);
    assert_eq!(zebra_pool["free_slots"], 2);
    assert_eq!(zebra_pool["active_leases"], 1);
    assert_eq!(zebra_pool["desired_additional_slots"], 2);

    handle.abort();
    drop(session);
    server.stop().await;
    for suffix in ["", "-wal", "-shm", ".lock"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
    }
}

#[tokio::test]
async fn overview_memory_mode_reports_disabled_journal_and_null_journal_stats() {
    let runtime = test_runtime().await;
    let (base_url, handle) = spawn_test_server(runtime).await;
    let body: Value = reqwest::get(format!("{base_url}/api/overview"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(body["health"]["live"], true);
    assert_eq!(body["health"]["ready"], true);
    assert_eq!(body["health"]["journal"]["status"], "disabled");
    assert!(body["health"]["journal"]["message"].is_null());
    assert!(body["journal_stats"].is_null());
    assert_eq!(body["control_plane"]["auth_enabled"], false);
    assert!(body["control_plane"]["reconnect_grace_ms"].is_null());
    assert!(body["pools"].as_array().unwrap().is_empty());
    handle.abort();
}

#[tokio::test]
async fn run_server_serves_control_plane_metadata_and_shuts_down_cleanly() {
    let runtime = test_runtime().await;
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let task = tokio::spawn(run_server(
        addr,
        runtime,
        crate::serve_config::ServeConsoleConfig {
            listen: "127.0.0.1:0".into(),
            allowed_origins: vec![" http://console.test ".into(), "".into()],
        },
        ControlPlaneMetadata {
            grpc_listen: "127.0.0.1:50099".into(),
            grpc_tls_enabled: true,
        },
        RuntimeMode::Memory,
        Arc::new(AuthPolicy::default()),
        shutdown_rx,
    ));
    let _ = shutdown_tx.send(true);
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn workers_endpoint_reports_session_pool_namespace_load_draining_and_disconnected() {
    use anyserve_client::execution::{WorkerSession, v2};
    use std::time::Duration;

    let server = Server::start(
        RuntimeConfig {
            reconnect_grace: Some(Duration::from_secs(30)),
            ..Default::default()
        },
        AuthPolicy::default(),
    )
    .await;

    let mut healthy = WorkerSession::connect_with_namespace(
        &server.endpoint,
        None,
        Some("team-alpha"),
        v2::Hello {
            worker_id: "w-healthy".into(),
            pool: "spot".into(),
            interfaces: vec!["demo.v1".into()],
            max_concurrency: 4,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let _ = healthy.messages.message().await.unwrap();
    healthy
        .control
        .send(v2::WorkerMessage {
            msg: Some(v2::worker_message::Msg::Credit(v2::Credit {
                free_slots: 4,
            })),
        })
        .await
        .unwrap();
    healthy
        .control
        .send(v2::WorkerMessage {
            msg: Some(v2::worker_message::Msg::Heartbeat(v2::Heartbeat {
                load: Some(v2::Load {
                    queued: 2,
                    running: 1,
                    utilization: 0.75,
                    custom: [("vram_used_mb".into(), 4096.0)].into(),
                }),
            })),
        })
        .await
        .unwrap();

    let mut draining = WorkerSession::connect(
        &server.endpoint,
        None,
        v2::Hello {
            worker_id: "w-draining".into(),
            pool: "ondemand".into(),
            interfaces: vec!["demo.v1".into()],
            max_concurrency: 2,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let _ = draining.messages.message().await.unwrap();
    draining
        .control
        .send(v2::WorkerMessage {
            msg: Some(v2::worker_message::Msg::Draining(v2::Draining {
                preempting: false,
            })),
        })
        .await
        .unwrap();

    let mut disconnected = WorkerSession::connect(
        &server.endpoint,
        None,
        v2::Hello {
            worker_id: "w-disconnected".into(),
            pool: "edge".into(),
            interfaces: vec!["demo.disconnect.v1".into()],
            max_concurrency: 1,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let _ = disconnected.messages.message().await.unwrap();
    disconnected
        .control
        .send(v2::WorkerMessage {
            msg: Some(v2::worker_message::Msg::Credit(v2::Credit {
                free_slots: 1,
            })),
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while !server
            .runtime
            .list_workers()
            .iter()
            .any(|w| w.worker_id == "w-disconnected")
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    server
        .runtime
        .submit(SubmitRequest {
            spec: test_job_spec("demo.disconnect.v1"),
            ..Default::default()
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Some(v2::server_message::Msg::Assign(assign)) =
                disconnected.messages.message().await.unwrap().unwrap().msg
            {
                disconnected
                    .control
                    .send(v2::WorkerMessage {
                        msg: Some(v2::worker_message::Msg::AssignReply(v2::AssignReply {
                            lease_id: assign.lease_id,
                            accepted: true,
                            reject_reason: String::new(),
                        })),
                    })
                    .await
                    .unwrap();
                break;
            }
        }
    })
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while !server
            .runtime
            .list_workers()
            .iter()
            .any(|w| w.worker_id == "w-disconnected" && w.active_leases == 1)
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    drop(disconnected);

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let workers = server.runtime.list_workers();
            let ready = workers.len() == 3
                && workers
                    .iter()
                    .any(|w| w.worker_id == "w-healthy" && w.load.queued == 2)
                && workers
                    .iter()
                    .any(|w| w.worker_id == "w-draining" && w.draining)
                && workers
                    .iter()
                    .any(|w| w.worker_id == "w-disconnected" && !w.connected);
            if ready {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();

    let (base_url, handle) = spawn_test_server(server.runtime.clone()).await;
    let body: Value = reqwest::get(format!("{base_url}/api/workers"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(body["total"], 3);
    assert_eq!(body["healthy"], 1);
    assert_eq!(body["draining"], 1);
    assert_eq!(body["disconnected"], 1);

    let workers = body["workers"].as_array().unwrap();
    let w_healthy = workers
        .iter()
        .find(|w| w["worker_id"] == "w-healthy")
        .unwrap();
    assert!(!w_healthy["session_id"].as_str().unwrap().is_empty());
    assert_eq!(w_healthy["namespace"], "team-alpha");
    assert_eq!(w_healthy["pool"], "spot");
    assert!(w_healthy["connected_at_ms"].as_u64().unwrap() > 0);
    assert_eq!(w_healthy["utilization"], 0.75);
    assert_eq!(w_healthy["load"]["queued"], 2);
    assert_eq!(w_healthy["load"]["running"], 1);
    assert_eq!(w_healthy["load"]["utilization"], 0.75);
    assert_eq!(w_healthy["load"]["custom"]["vram_used_mb"], 4096.0);

    handle.abort();
    drop(healthy);
    drop(draining);
    server.stop().await;
}

#[tokio::test]
async fn jobs_endpoints_filter_and_expose_enriched_summary_and_detail_fields() {
    use anyserve_client::execution::{Attempt, WorkerSession, v2};
    use std::time::Duration;

    let server = Server::start(RuntimeConfig::default(), AuthPolicy::default()).await;

    let mut worker_beta = WorkerSession::connect_with_namespace(
        &server.endpoint,
        None,
        Some("team-beta"),
        v2::Hello {
            worker_id: "shared-worker-id".into(),
            pool: "beta-pool".into(),
            interfaces: vec!["iface.a".into()],
            max_concurrency: 2,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let Some(v2::server_message::Msg::Welcome(welcome)) =
        worker_beta.messages.message().await.unwrap().unwrap().msg
    else {
        panic!("missing Welcome");
    };
    let limits = welcome.limits.unwrap();
    let hb_timeout = Duration::from_millis(u64::from(welcome.heartbeat_timeout_ms));
    worker_beta
        .control
        .send(v2::WorkerMessage {
            msg: Some(v2::worker_message::Msg::Credit(v2::Credit {
                free_slots: 2,
            })),
        })
        .await
        .unwrap();

    // Also connect a default-namespace worker with the SAME worker_id to ensure detail matches namespace!
    let _worker_default =
        Worker::connect(&server, "shared-worker-id", vec!["iface.b".into()], 1).await;

    let before = anyserve_core::now_ms().saturating_sub(1000);
    let mut beta_spec = test_job_spec("iface.a");
    beta_spec.demand.affinity_key = "aff-1".into();
    beta_spec.policy.fairness_key = "fair-1".into();
    beta_spec.policy.concurrency_key = "conc-1".into();
    let beta_failed_job = server
        .runtime
        .submit(SubmitRequest {
            namespace: Some("team-beta".into()),
            spec: beta_spec,
            ..Default::default()
        })
        .await
        .unwrap()
        .job;

    let assign = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Some(v2::server_message::Msg::Assign(assign)) =
                worker_beta.messages.message().await.unwrap().unwrap().msg
            {
                worker_beta
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
                return assign;
            }
        }
    })
    .await
    .unwrap();
    let mut attempt = Attempt::open(&mut worker_beta.data, &assign, &limits, hb_timeout)
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

    // Submit a second queued job in team-beta to test cancel?namespace=team-beta.
    let beta_cancel_job = server
        .runtime
        .submit(SubmitRequest {
            namespace: Some("team-beta".into()),
            spec: {
                let mut s = test_job_spec("iface.a");
                s.demand
                    .required_attributes
                    .insert("unreachable".into(), "true".into());
                s
            },
            ..Default::default()
        })
        .await
        .unwrap()
        .job;

    // Submit a default-namespace job on iface.b.
    let default_job = server
        .runtime
        .submit(SubmitRequest {
            spec: {
                let mut s = test_job_spec("iface.b");
                s.demand
                    .required_attributes
                    .insert("unreachable".into(), "true".into());
                s
            },
            ..Default::default()
        })
        .await
        .unwrap()
        .job;

    let end = anyserve_core::now_ms() + 1;
    let (base_url, handle) = spawn_test_server(server.runtime.clone()).await;
    let client = reqwest::Client::new();

    // 1. Filter by interface=iface.a -> returns both beta jobs, not default_job.
    let by_iface: Value = client
        .get(format!("{base_url}/api/jobs?interface=iface.a"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(by_iface["jobs"].as_array().unwrap().len(), 2);

    // 2. Filter by namespace=team-beta&states=failed -> returns beta_failed_job with error_code/retryable.
    let by_ns: Value = client
        .get(format!(
            "{base_url}/api/jobs?namespace=team-beta&states=failed"
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let failed_rows = by_ns["jobs"].as_array().unwrap();
    assert_eq!(failed_rows.len(), 1);
    assert_eq!(failed_rows[0]["job_id"], beta_failed_job.job_id);
    assert_eq!(failed_rows[0]["namespace"], "team-beta");
    assert_eq!(failed_rows[0]["durability"], "ephemeral");
    assert_eq!(failed_rows[0]["last_error"], "bad payload");
    assert_eq!(failed_rows[0]["error_code"], "bad_input");
    assert_eq!(failed_rows[0]["error_retryable"], false);

    // 3. Empty/whitespace interface & namespace filters are treated as None (all 3 jobs returned).
    let all_jobs: Value = client
        .get(format!("{base_url}/api/jobs?interface=%20%20&namespace="))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(all_jobs["jobs"].as_array().unwrap().len(), 3);

    // 4. Detail for non-default namespace job exposes affinity_key, fairness_key, concurrency_key,
    //    and resolves latest_worker in the job's namespace ("beta-pool", not default).
    let detail: Value = client
        .get(format!(
            "{base_url}/api/jobs/{}?namespace=team-beta",
            beta_failed_job.job_id
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(detail["job"]["namespace"], "team-beta");
    assert_eq!(detail["job"]["durability"], "ephemeral");
    assert_eq!(detail["job"]["error_code"], "bad_input");
    assert_eq!(detail["job"]["error_retryable"], false);
    assert_eq!(detail["job"]["demand"]["affinity_key"], "aff-1");
    assert_eq!(detail["job"]["policy"]["fairness_key"], "fair-1");
    assert_eq!(detail["job"]["policy"]["concurrency_key"], "conc-1");
    assert_eq!(detail["latest_worker"]["namespace"], "team-beta");
    assert_eq!(detail["latest_worker"]["pool"], "beta-pool");

    // 5. Default-namespace lookup for beta job returns 404; default job returns 200 without ?namespace.
    assert_eq!(
        client
            .get(format!("{base_url}/api/jobs/{}", beta_failed_job.job_id))
            .send()
            .await
            .unwrap()
            .status()
            .as_u16(),
        404
    );
    assert_eq!(
        client
            .get(format!("{base_url}/api/jobs/{}", default_job.job_id))
            .send()
            .await
            .unwrap()
            .status()
            .as_u16(),
        200
    );

    // 6. Cancel in non-default namespace via ?namespace=team-beta.
    let cancelled: Value = client
        .post(format!(
            "{base_url}/api/jobs/{}/cancel?namespace=team-beta",
            beta_cancel_job.job_id
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(cancelled["job"]["state"], "cancelled");
    assert_eq!(cancelled["job"]["namespace"], "team-beta");
    assert_eq!(cancelled["job"]["error_code"], "cancelled");

    // 7. Analytics with ?namespace=team-beta returns single durability & enriched fields.
    let analytics: Value = client
        .get(format!(
            "{base_url}/api/analytics/jobs?source=retained&from_ms={before}&to_ms={end}&namespace=team-beta"
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(analytics["jobs"].as_array().unwrap().len(), 2);
    assert!(
        analytics["jobs"]
            .as_array()
            .unwrap()
            .iter()
            .all(|j| j["namespace"] == "team-beta" && j["durability"] == "ephemeral")
    );

    handle.abort();
    drop(worker_beta);
    server.stop().await;
}

#[tokio::test]
async fn cors_middleware_allows_only_configured_origins() {
    let runtime = test_runtime().await;
    let (base_url, handle) = spawn_test_server(runtime).await;
    let client = reqwest::Client::new();

    let allowed = client
        .get(format!("{base_url}/api/overview"))
        .header("origin", "http://console.test")
        .send()
        .await
        .unwrap();
    assert_eq!(
        allowed.headers()["access-control-allow-origin"],
        "http://console.test"
    );

    let disallowed = client
        .get(format!("{base_url}/api/overview"))
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

    handle.abort();
}
