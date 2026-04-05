use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use anyserve_core::frame::MemoryFramePlane;
use anyserve_core::kernel::Kernel;
use anyserve_core::model::{Attributes, JobSpec, WorkerSpec};
use anyserve_core::notify::NoopClusterNotifier;
use anyserve_core::scheduler::BasicScheduler;
use anyserve_core::store::MemoryStateStore;
use serde_json::Value;

use crate::serve_config::RuntimeMode;

use super::api::{AppState, runtime_mode_summary};
use super::handlers::{parse_states, router};

#[test]
fn parse_states_rejects_invalid_values() {
    let error = parse_states(Some("pending,nope")).expect_err("should reject invalid state");
    assert_eq!(error.status, axum::http::StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn overview_reports_runtime_queue_history_and_workers() {
    let kernel = test_kernel();
    register_worker(&kernel, "worker-overview").await;
    kernel
        .submit_job(None, test_job_spec("demo.queue.v1"))
        .await
        .unwrap();
    let cancelled = kernel
        .submit_job(None, test_job_spec("demo.history.v1"))
        .await
        .unwrap();
    kernel.cancel_job(&cancelled.job_id).await.unwrap();

    let (base_url, handle) = spawn_test_server(kernel).await;
    let response = reqwest::get(format!("{base_url}/api/overview"))
        .await
        .unwrap();
    assert_eq!(response.status(), axum::http::StatusCode::OK);

    let body: Value = response.json().await.unwrap();
    assert_eq!(body["runtime"]["code"], "memory");
    assert_eq!(body["queue"]["pending"], 1);
    assert_eq!(body["history"]["cancelled"], 1);
    assert_eq!(body["workers"]["healthy"], 1);

    handle.abort();
}

#[tokio::test]
async fn jobs_endpoint_filters_and_paginates() {
    let kernel = test_kernel();
    let first = kernel
        .submit_job(None, test_job_spec("demo.first.v1"))
        .await
        .unwrap();
    let second = kernel
        .submit_job(None, test_job_spec("demo.second.v1"))
        .await
        .unwrap();
    kernel.cancel_job(&second.job_id).await.unwrap();
    kernel
        .submit_job(None, test_job_spec("demo.third.v1"))
        .await
        .unwrap();

    let (base_url, handle) = spawn_test_server(kernel).await;
    let response = reqwest::get(format!(
        "{base_url}/api/jobs?states=cancelled&limit=1&offset=0"
    ))
    .await
    .unwrap();
    assert_eq!(response.status(), axum::http::StatusCode::OK);

    let body: Value = response.json().await.unwrap();
    assert_eq!(body["total"], 1);
    assert_eq!(body["jobs"].as_array().unwrap().len(), 1);
    assert_eq!(body["jobs"][0]["state"], "cancelled");
    assert_eq!(body["jobs"][0]["job_id"], second.job_id);
    assert_ne!(body["jobs"][0]["job_id"], first.job_id);

    handle.abort();
}

#[tokio::test]
async fn job_detail_and_cancel_endpoint_return_payloads() {
    let kernel = test_kernel();
    let job = kernel
        .submit_job(None, test_job_spec("demo.detail.v1"))
        .await
        .unwrap();

    let (base_url, handle) = spawn_test_server(kernel.clone()).await;
    let client = reqwest::Client::new();

    let detail = client
        .get(format!("{base_url}/api/jobs/{}", job.job_id))
        .send()
        .await
        .unwrap();
    assert_eq!(detail.status(), axum::http::StatusCode::OK);
    let body: Value = detail.json().await.unwrap();
    assert!(body["events"].as_array().unwrap().len() >= 1);
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
    let kernel = test_kernel();
    register_worker(&kernel, "worker-health").await;

    let (base_url, handle) = spawn_test_server(kernel).await;
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

fn test_kernel() -> Arc<Kernel> {
    Arc::new(Kernel::new(
        Arc::new(MemoryStateStore::new()),
        Arc::new(MemoryFramePlane::new()),
        Arc::new(NoopClusterNotifier),
        Arc::new(BasicScheduler),
        30,
        30,
        250,
    ))
}

async fn spawn_test_server(kernel: Arc<Kernel>) -> (String, tokio::task::JoinHandle<()>) {
    let state = AppState {
        kernel,
        allowed_origins: Arc::new(BTreeSet::new()),
        runtime: runtime_mode_summary(RuntimeMode::Memory),
    };
    let app = router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}"), handle)
}

fn test_job_spec(interface_name: &str) -> JobSpec {
    JobSpec {
        interface_name: interface_name.to_string(),
        inputs: vec![anyserve_core::model::ObjectRef::inline(b"hello".to_vec())],
        params: b"world".to_vec(),
        metadata: Attributes::from([("source".to_string(), "runtime".to_string())]),
        ..JobSpec::default()
    }
}

async fn register_worker(kernel: &Arc<Kernel>, worker_id: &str) {
    let worker = kernel
        .register_worker(
            Some(worker_id.to_string()),
            WorkerSpec {
                interfaces: BTreeSet::from(["demo.queue.v1".to_string()]),
                attributes: BTreeMap::from([("region".to_string(), "local".to_string())]),
                total_capacity: BTreeMap::from([("slot".to_string(), 4)]),
                max_active_leases: 4,
                metadata: BTreeMap::new(),
            },
        )
        .await
        .unwrap();
    kernel
        .heartbeat_worker(
            &worker.worker_id,
            BTreeMap::from([("slot".to_string(), 4)]),
            0,
            BTreeMap::new(),
        )
        .await
        .unwrap();
}
