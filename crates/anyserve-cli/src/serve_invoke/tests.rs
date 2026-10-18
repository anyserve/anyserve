use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use anyserve_core::auth::AuthPolicy;
use anyserve_core::runtime::api::{Demand, RuntimeConfig};
use axum::body::{Body, Bytes};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;

use super::router;
use crate::edge_routing::RoutingDefaults;
use crate::serve_config::ServeInvokeConfig;
use crate::test_support::{Backend, BuiltinWorker, Server};

fn config() -> ServeInvokeConfig {
    ServeInvokeConfig {
        listen: "127.0.0.1:0".into(),
        default_timeout_ms: 3_000,
        max_body_bytes: 16 * 1024 * 1024,
        forward_headers: vec!["x-request-id".into()],
        demand: Demand::default(),
        defaults: RoutingDefaults::default(),
    }
}
const WORKER: &str = "kind='http'\nmax_active_leases=4\n[routes]\n'policy.v1'='/act'\n";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn invoke_relays_large_payload_status_content_type_and_selected_worker() {
    let server = Server::start(RuntimeConfig::default(), AuthPolicy::default()).await;
    let backend = Backend::start(
        axum::Router::new()
            .route(
                "/act",
                axum::routing::post(|headers: HeaderMap, body: Bytes| async move {
                    assert_eq!(headers["x-request-id"], "request-123");
                    (
                        StatusCode::CREATED,
                        [("content-type", "application/custom")],
                        body,
                    )
                }),
            )
            .layer(axum::extract::DefaultBodyLimit::max(16 * 1024 * 1024)),
    )
    .await;
    let worker = BuiltinWorker::start(&server, &backend, WORKER).await;
    let gateway = Backend::start(router(
        server.runtime.clone(),
        config(),
        Arc::new(AuthPolicy::default()),
    ))
    .await;
    let payload = vec![42; 5 * 1024 * 1024];
    let response = reqwest::Client::new()
        .post(format!("{}/v1/invoke/policy.v1", gateway.url))
        .header("x-request-id", "request-123")
        .body(payload.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    assert_eq!(response.headers()["content-type"], "application/custom");
    assert_eq!(
        response.headers()["x-anyserve-worker-id"].to_str().unwrap(),
        worker.worker_id
    );
    assert_eq!(response.headers()["x-anyserve-attempts"], "1");
    assert_eq!(response.bytes().await.unwrap().as_ref(), payload);
    drop(worker);
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retryable_upstream_failure_uses_one_retry_budget_and_client_errors_pass_through() {
    let server = Server::start(RuntimeConfig::default(), AuthPolicy::default()).await;
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let backend = Backend::start(axum::Router::new().route(
        "/act",
        axum::routing::post(move |body: Bytes| {
            let count = count.clone();
            async move {
                if body.as_ref() == b"invalid" {
                    return (StatusCode::UNPROCESSABLE_ENTITY, "bad input").into_response();
                }
                if count.fetch_add(1, Ordering::SeqCst) == 0 {
                    return StatusCode::SERVICE_UNAVAILABLE.into_response();
                }
                body.into_response()
            }
        }),
    ))
    .await;
    let worker = BuiltinWorker::start(&server, &backend, WORKER).await;
    let gateway = Backend::start(router(
        server.runtime.clone(),
        config(),
        Arc::new(AuthPolicy::default()),
    ))
    .await;
    let client = reqwest::Client::new();
    let response = client
        .post(format!("{}/v1/invoke/policy.v1", gateway.url))
        .body("ok")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["x-anyserve-attempts"], "2");
    assert_eq!(response.text().await.unwrap(), "ok");
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    let response = client
        .post(format!("{}/v1/invoke/policy.v1", gateway.url))
        .body("invalid")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(response.text().await.unwrap(), "bad input");
    drop(worker);
    server.stop().await;
}

#[tokio::test]
async fn queue_timeout_is_explicit_when_no_worker_can_serve() {
    let server = Server::start(RuntimeConfig::default(), AuthPolicy::default()).await;
    let gateway = Backend::start(router(
        server.runtime.clone(),
        config(),
        Arc::new(AuthPolicy::default()),
    ))
    .await;
    let response = reqwest::Client::new()
        .post(format!("{}/v1/invoke/missing", gateway.url))
        .header("x-anyserve-queue-timeout-ms", "30")
        .body("input")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["error"]["code"], "queue_timeout");
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn caller_deadline_aborts_the_backend_body_and_releases_the_worker_slot() {
    struct OnDrop(Arc<AtomicBool>);
    impl Drop for OnDrop {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    let cancelled = Arc::new(AtomicBool::new(false));
    let observed = cancelled.clone();
    let backend = Backend::start(axum::Router::new().route("/act", axum::routing::post(move || {
        let observed = observed.clone();
        async move {
            let stream = async_stream::stream! {
                let _guard = OnDrop(observed);
                loop { tokio::time::sleep(Duration::from_millis(5)).await; yield Ok::<_, std::convert::Infallible>(vec![0u8; 16]); }
            };
            Body::from_stream(stream)
        }
    }))).await;
    let server = Server::start(RuntimeConfig::default(), AuthPolicy::default()).await;
    let worker = BuiltinWorker::start(&server, &backend, WORKER).await;
    let gateway = Backend::start(router(
        server.runtime.clone(),
        config(),
        Arc::new(AuthPolicy::default()),
    ))
    .await;
    let response = reqwest::Client::new()
        .post(format!("{}/v1/invoke/policy.v1", gateway.url))
        .header("x-anyserve-timeout-ms", "100")
        .body("input")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
    tokio::time::timeout(Duration::from_secs(2), async {
        while !cancelled.load(Ordering::SeqCst)
            || server
                .runtime
                .list_workers()
                .iter()
                .any(|worker| worker.active_leases != 0)
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("cancellation did not propagate to the backend");
    drop(worker);
    server.stop().await;
}

#[tokio::test]
async fn http_gateway_requires_client_role_but_health_is_public() {
    let server = Server::start(RuntimeConfig::default(), AuthPolicy::default()).await;
    let auth = Arc::new(AuthPolicy::new(
        ["client".to_owned()],
        ["worker".to_owned()],
    ));
    let gateway = Backend::start(router(server.runtime.clone(), config(), auth)).await;
    let client = reqwest::Client::new();
    let url = format!("{}/v1/invoke/missing", gateway.url);
    assert_eq!(
        client.post(&url).send().await.unwrap().status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        client
            .post(&url)
            .bearer_auth("worker")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        client
            .get(format!("{}/metrics", gateway.url))
            .bearer_auth("client")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        client
            .get(format!("{}/healthz", gateway.url))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    server.stop().await;
}

#[tokio::test]
async fn readiness_matches_demand_and_live_metrics_exclude_draining_and_disconnected_sessions() {
    use crate::test_support::Worker;
    use anyserve_client::execution::v2;
    let server = Server::start(RuntimeConfig::default(), AuthPolicy::default()).await;
    let worker = Worker::connect(&server, "ready-worker", vec!["policy.v1".into()], 1).await;
    worker.credit(0).await;
    tokio::time::timeout(Duration::from_secs(2), async {
        while server.runtime.list_workers()[0].free_slots != 0 {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    let gateway = Backend::start(router(
        server.runtime.clone(),
        config(),
        Arc::new(AuthPolicy::default()),
    ))
    .await;
    assert_eq!(
        reqwest::get(format!("{}/readyz", gateway.url))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    for required_attributes in [false, true] {
        let mut config = config();
        if required_attributes {
            config
                .demand
                .required_attributes
                .insert("region".into(), "near".into());
        } else {
            config.demand.required_capacity.insert("slot".into(), 2);
        }
        let unmatched = Backend::start(router(
            server.runtime.clone(),
            config,
            Arc::new(AuthPolicy::default()),
        ))
        .await;
        assert_eq!(
            reqwest::get(format!("{}/readyz", unmatched.url))
                .await
                .unwrap()
                .status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }
    let metrics = reqwest::get(format!("{}/metrics", gateway.url))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(
        metrics
            .lines()
            .any(|line| line == "anyserve_live_workers 1")
    );
    worker
        .session
        .control
        .send(v2::WorkerMessage {
            msg: Some(v2::worker_message::Msg::Draining(v2::Draining {})),
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while !server.runtime.list_workers()[0].draining {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        reqwest::get(format!("{}/readyz", gateway.url))
            .await
            .unwrap()
            .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert!(
        reqwest::get(format!("{}/metrics", gateway.url))
            .await
            .unwrap()
            .text()
            .await
            .unwrap()
            .lines()
            .any(|line| line == "anyserve_live_workers 0")
    );
    drop(worker);
    tokio::time::timeout(Duration::from_secs(2), async {
        while server
            .runtime
            .list_workers()
            .iter()
            .any(|worker| worker.connected)
        {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    // An idle draining session may be removed immediately. Keep a real lease
    // on another session to exercise the disconnected grace-period case.
    let mut grace = Worker::connect(&server, "grace-worker", vec!["policy.v1".into()], 1).await;
    use anyserve_core::runtime::api::{Durability, ExecutionPolicy, JobSpec, SubmitRequest};
    server
        .runtime
        .submit(SubmitRequest {
            spec: JobSpec {
                demand: Demand {
                    interface: "policy.v1".into(),
                    ..Default::default()
                },
                policy: ExecutionPolicy {
                    durability: Some(Durability::Ephemeral),
                    ..Default::default()
                },
                ..Default::default()
            },
            ..Default::default()
        })
        .await
        .unwrap();
    let _assignment = grace.assignment().await;
    drop(grace);
    tokio::time::timeout(Duration::from_secs(2), async {
        while server
            .runtime
            .list_workers()
            .iter()
            .any(|worker| worker.connected)
        {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    assert!(
        server.runtime.stats().sessions > 0,
        "leased session should remain during its grace period"
    );
    assert!(
        reqwest::get(format!("{}/metrics", gateway.url))
            .await
            .unwrap()
            .text()
            .await
            .unwrap()
            .lines()
            .any(|line| line == "anyserve_live_workers 0")
    );
    server.runtime.shutdown().await;
    assert_eq!(
        reqwest::get(format!("{}/readyz", gateway.url))
            .await
            .unwrap()
            .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
}
