use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use anyserve_core::auth::{AuthLayer, AuthPolicy};
use anyserve_core::frame::MemoryFramePlane;
use anyserve_core::kernel::Kernel;
use anyserve_core::model::Demand;
use anyserve_core::notify::NoopClusterNotifier;
use anyserve_core::scheduler::BasicScheduler;
use anyserve_core::service::ControlPlaneGrpcService;
use anyserve_core::store::MemoryStateStore;
use anyserve_proto::controlplane::control_plane_service_server::ControlPlaneServiceServer;
use axum::http::StatusCode;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

use super::router;
use crate::edge_routing::RoutingDefaults;
use crate::serve_config::ServeInvokeConfig;
use crate::serve_openai_worker::{ServeOpenAIWorkerArgs, run as run_worker};

const CLIENT_TOKEN: &str = "client-secret";
const WORKER_TOKEN: &str = "worker-secret";

struct Cluster {
    grpc_endpoint: String,
    invoke_url: String,
}

async fn spawn_cluster(auth: AuthPolicy) -> Cluster {
    let kernel = Arc::new(Kernel::new(
        Arc::new(MemoryStateStore::new()),
        Arc::new(MemoryFramePlane::new()),
        Arc::new(NoopClusterNotifier),
        Arc::new(BasicScheduler),
        30,
        30,
        250,
    ));
    kernel.start_background().await;

    let grpc_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let grpc_addr = grpc_listener.local_addr().unwrap();
    let grpc_kernel = Arc::clone(&kernel);
    let grpc_auth = auth.clone();
    tokio::spawn(async move {
        Server::builder()
            .layer(AuthLayer::new(grpc_auth))
            .add_service(ControlPlaneServiceServer::new(
                ControlPlaneGrpcService::new(grpc_kernel),
            ))
            .serve_with_incoming(TcpListenerStream::new(grpc_listener))
            .await
            .unwrap();
    });

    let config = ServeInvokeConfig {
        listen: "127.0.0.1:0".to_string(),
        default_timeout_ms: 5_000,
        max_body_bytes: 1024 * 1024,
        forward_headers: vec!["x-tenant".to_string()],
        demand: Demand::default(),
        defaults: RoutingDefaults::default(),
    };
    let invoke_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let invoke_addr = invoke_listener.local_addr().unwrap();
    let app = router(kernel, config, Arc::new(auth));
    tokio::spawn(async move { axum::serve(invoke_listener, app).await.unwrap() });

    Cluster {
        grpc_endpoint: format!("http://{grpc_addr}"),
        invoke_url: format!("http://{invoke_addr}"),
    }
}

#[derive(Clone)]
struct Upstream {
    addr: SocketAddr,
    calls: Arc<AtomicUsize>,
    healthy: Arc<AtomicBool>,
}

/// Mock inference service: `/act` echoes the body tagged with `name`,
/// `/reject` returns 422, `/healthz` follows the `healthy` flag.
async fn spawn_upstream(name: &'static str) -> Upstream {
    let calls = Arc::new(AtomicUsize::new(0));
    let healthy = Arc::new(AtomicBool::new(true));
    let app = axum::Router::new()
        .route(
            "/act",
            axum::routing::post({
                let calls = Arc::clone(&calls);
                move |headers: axum::http::HeaderMap, body: axum::body::Bytes| {
                    let calls = Arc::clone(&calls);
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        let tenant = headers
                            .get("x-tenant")
                            .and_then(|value| value.to_str().ok())
                            .unwrap_or("-")
                            .to_string();
                        axum::Json(serde_json::json!({
                            "served_by": name,
                            "tenant": tenant,
                            "echo": String::from_utf8_lossy(&body),
                        }))
                    }
                }
            }),
        )
        .route(
            "/reject",
            axum::routing::post(|| async { (StatusCode::UNPROCESSABLE_ENTITY, "bad observation") }),
        )
        .route(
            "/healthz",
            axum::routing::get({
                let healthy = Arc::clone(&healthy);
                move || {
                    let healthy = Arc::clone(&healthy);
                    async move {
                        if healthy.load(Ordering::SeqCst) {
                            StatusCode::OK
                        } else {
                            StatusCode::SERVICE_UNAVAILABLE
                        }
                    }
                }
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    Upstream {
        addr,
        calls,
        healthy,
    }
}

fn write_worker_config(name: &str, upstream: &Upstream, region: &str, extra: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "anyserve-invoke-test-{}-{name}.toml",
        std::process::id()
    ));
    std::fs::write(
        &path,
        format!(
            r#"
kind = "http"
worker_id = "{name}"
base_url = "http://{addr}"
max_active_leases = 4
poll_interval_secs = 5
heartbeat_interval_secs = 1
health_url = "http://{addr}/healthz"
health_interval_secs = 1
token = "{WORKER_TOKEN}"
{extra}

[routes]
"demo.act.v1" = "/act"
"demo.reject.v1" = "/reject"

[attributes]
region = "{region}"
"#,
            addr = upstream.addr,
        ),
    )
    .unwrap();
    path
}

fn spawn_worker(endpoint: &str, config: PathBuf) {
    let endpoint = endpoint.to_string();
    tokio::spawn(async move {
        let _ = run_worker(
            &endpoint,
            ServeOpenAIWorkerArgs {
                config,
                worker_id: None,
            },
        )
        .await;
    });
}

async fn wait_ready(cluster: &Cluster, live_workers: u64) {
    let client = reqwest::Client::new();
    for _ in 0..100 {
        if let Ok(response) = client
            .get(format!("{}/readyz", cluster.invoke_url))
            .send()
            .await
            && let Ok(body) = response.json::<serde_json::Value>().await
            && body["live_workers"].as_u64() == Some(live_workers)
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("workers never became ready");
}

fn auth_policy() -> AuthPolicy {
    AuthPolicy::new([CLIENT_TOKEN.to_string()], [WORKER_TOKEN.to_string()])
}

async fn invoke(
    cluster: &Cluster,
    interface: &str,
    headers: &[(&str, &str)],
    body: &str,
) -> reqwest::Response {
    let mut request = reqwest::Client::new()
        .post(format!("{}/v1/invoke/{interface}", cluster.invoke_url))
        .bearer_auth(CLIENT_TOKEN)
        .header("content-type", "application/json")
        .body(body.to_string());
    for (name, value) in headers {
        request = request.header(*name, *value);
    }
    request.send().await.unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn invoke_round_trips_through_a_long_polling_edge_worker() {
    let cluster = spawn_cluster(auth_policy()).await;
    let upstream = spawn_upstream("edge-a").await;
    spawn_worker(
        &cluster.grpc_endpoint,
        write_worker_config("edge-a", &upstream, "ap-south-1", ""),
    );
    wait_ready(&cluster, 1).await;
    // Let the worker settle into its long-poll.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let started = Instant::now();
    let response = invoke(
        &cluster,
        "demo.act.v1",
        &[("x-tenant", "b2c")],
        r#"{"hand":"AsKd"}"#,
    )
    .await;
    let elapsed = started.elapsed();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()["x-anyserve-worker-id"].to_str().unwrap(),
        "edge-a"
    );
    assert_eq!(
        response.headers()["x-anyserve-attempts"].to_str().unwrap(),
        "1"
    );
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["served_by"], "edge-a");
    assert_eq!(body["tenant"], "b2c");
    assert_eq!(body["echo"], r#"{"hand":"AsKd"}"#);
    // poll_interval_secs = 5: only a long-poll can answer this fast.
    assert!(
        elapsed < Duration::from_millis(500),
        "invoke took {elapsed:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn invoke_passes_upstream_client_errors_through() {
    let cluster = spawn_cluster(auth_policy()).await;
    let upstream = spawn_upstream("edge-a").await;
    spawn_worker(
        &cluster.grpc_endpoint,
        write_worker_config("edge-reject", &upstream, "ap-south-1", ""),
    );
    wait_ready(&cluster, 1).await;

    let response = invoke(&cluster, "demo.reject.v1", &[], "{}").await;
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(response.text().await.unwrap(), "bad observation");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn invoke_prefers_local_cluster_and_spills_over_when_it_drains() {
    let cluster = spawn_cluster(auth_policy()).await;
    let near = spawn_upstream("near").await;
    let far = spawn_upstream("far").await;
    spawn_worker(
        &cluster.grpc_endpoint,
        write_worker_config("near-worker", &near, "near", ""),
    );
    spawn_worker(
        &cluster.grpc_endpoint,
        write_worker_config("far-worker", &far, "far", ""),
    );
    wait_ready(&cluster, 2).await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let placement = [
        ("x-anyserve-prefer", "region=near"),
        ("x-anyserve-spillover-ms", "300"),
    ];
    for _ in 0..10 {
        let response = invoke(&cluster, "demo.act.v1", &placement, "{}").await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()["x-anyserve-worker-id"].to_str().unwrap(),
            "near-worker"
        );
    }
    assert_eq!(near.calls.load(Ordering::SeqCst), 10);
    assert_eq!(far.calls.load(Ordering::SeqCst), 0);

    // Near cluster's backend goes unhealthy; its worker stops leasing.
    near.healthy.store(false, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(1_500)).await;

    let started = Instant::now();
    let response = invoke(&cluster, "demo.act.v1", &placement, "{}").await;
    let elapsed = started.elapsed();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()["x-anyserve-worker-id"].to_str().unwrap(),
        "far-worker"
    );
    assert!(
        elapsed >= Duration::from_millis(280),
        "spilled too early: {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_millis(1_000),
        "spilled too late: {elapsed:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn invoke_fails_fast_with_queue_timeout_when_no_worker_can_serve() {
    let cluster = spawn_cluster(auth_policy()).await;

    let started = Instant::now();
    let response = invoke(
        &cluster,
        "demo.nobody.v1",
        &[("x-anyserve-queue-timeout-ms", "150")],
        "{}",
    )
    .await;
    let elapsed = started.elapsed();

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        response.headers()["x-anyserve-error"].to_str().unwrap(),
        "queue_timeout"
    );
    assert!(
        elapsed < Duration::from_millis(1_000),
        "queue timeout took {elapsed:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn invoke_and_worker_rpcs_require_matching_tokens() {
    let cluster = spawn_cluster(auth_policy()).await;

    let response = reqwest::Client::new()
        .post(format!("{}/v1/invoke/demo.act.v1", cluster.invoke_url))
        .body("{}")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    let response = reqwest::Client::new()
        .post(format!("{}/v1/invoke/demo.act.v1", cluster.invoke_url))
        .bearer_auth(WORKER_TOKEN)
        .body("{}")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);

    let mut client = anyserve_client::AnyserveClient::connect_with_token(
        cluster.grpc_endpoint.clone(),
        Some(CLIENT_TOKEN),
    )
    .await
    .unwrap();
    let error = client
        .register_worker(anyserve_client::WorkerRegistration::new(vec![
            "demo.act.v1".to_string(),
        ]))
        .await
        .unwrap_err();
    assert!(format!("{error:#}").contains("cannot call worker method"));
}
