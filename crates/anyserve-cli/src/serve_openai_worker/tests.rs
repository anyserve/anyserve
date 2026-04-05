use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use anyserve_client::{AnyserveClient, FrameWrite, object_ref};
use anyserve_core::frame::MemoryFramePlane;
use anyserve_core::kernel::{Kernel, OpenStreamCommand};
use anyserve_core::model::{Attributes, JobSpec, StreamDirection, StreamScope};
use anyserve_core::notify::NoopClusterNotifier;
use anyserve_core::scheduler::BasicScheduler;
use anyserve_core::service::ControlPlaneGrpcService;
use anyserve_core::store::MemoryStateStore;
use anyserve_proto::controlplane::control_plane_service_server::ControlPlaneServiceServer;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

use super::helpers::{
    DEFAULT_INPUT_STREAM, available_capacity, persisted_outputs, read_request_body, truncate,
    upstream_path,
};

#[test]
fn llm_worker_maps_supported_interfaces() {
    assert_eq!(upstream_path("llm.chat.v1").unwrap(), "chat/completions");
    assert_eq!(upstream_path("llm.embed.v1").unwrap(), "embeddings");
    assert_eq!(upstream_path("llm.embeddings.v1").unwrap(), "embeddings");
}

#[test]
fn llm_worker_reduces_slot_capacity_for_active_lease() {
    let available = available_capacity(&HashMap::from([("slot".to_string(), 4)]), 2);
    assert_eq!(available.get("slot").copied(), Some(2));
}

#[test]
fn truncate_keeps_short_values() {
    assert_eq!(truncate("hello", 10), "hello");
}

#[test]
fn persisted_outputs_store_inline_payload_and_content_type() {
    let outputs = persisted_outputs("application/json", br#"{"ok":true}"#.to_vec());
    assert_eq!(outputs.len(), 1);
    assert_eq!(
        outputs[0].reference,
        Some(object_ref::Reference::Inline(br#"{"ok":true}"#.to_vec()))
    );
    assert_eq!(
        outputs[0].metadata.get("content_type").map(String::as_str),
        Some("application/json")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn read_request_body_waits_for_terminal_input_stream() -> Result<()> {
    let (endpoint, shutdown_tx, handle, kernel) = spawn_test_server().await?;
    let job = kernel
        .submit_job(
            None,
            JobSpec {
                interface_name: "llm.chat.v1".to_string(),
                ..JobSpec::default()
            },
        )
        .await?;
    let stream = kernel
        .open_stream(OpenStreamCommand {
            job_id: job.job_id.clone(),
            attempt_id: None,
            worker_id: None,
            lease_id: None,
            stream_name: DEFAULT_INPUT_STREAM.to_string(),
            scope: StreamScope::Job,
            direction: StreamDirection::ClientToWorker,
            metadata: Attributes::new(),
        })
        .await?;

    let mut writer = AnyserveClient::connect(endpoint.clone()).await?;
    writer
        .push_frames(
            stream.stream_id.clone(),
            vec![FrameWrite {
                kind: anyserve_client::FrameKind::Data,
                payload: b"hello streamed body".to_vec(),
                metadata: HashMap::new(),
            }],
            None,
            None,
        )
        .await?;

    let mut reader = AnyserveClient::connect(endpoint).await?;
    let read_job = reader.get_job(job.job_id.clone()).await?;
    let read_task = tokio::spawn(async move {
        read_request_body(
            &mut reader,
            &read_job.job_id,
            &read_job.spec.as_ref().unwrap().params,
            Duration::from_secs(1),
        )
        .await
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!read_task.is_finished());

    writer
        .close_stream(stream.stream_id, None, None, HashMap::new())
        .await?;

    let body = read_task.await.context("join read_request_body task")??;
    assert_eq!(body, b"hello streamed body".to_vec());

    let _ = shutdown_tx.send(());
    handle.await.context("join grpc test server")??;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn read_request_body_rejects_empty_terminal_input_stream() -> Result<()> {
    let (endpoint, shutdown_tx, handle, kernel) = spawn_test_server().await?;
    let job = kernel
        .submit_job(
            None,
            JobSpec {
                interface_name: "llm.chat.v1".to_string(),
                ..JobSpec::default()
            },
        )
        .await?;
    let stream = kernel
        .open_stream(OpenStreamCommand {
            job_id: job.job_id.clone(),
            attempt_id: None,
            worker_id: None,
            lease_id: None,
            stream_name: DEFAULT_INPUT_STREAM.to_string(),
            scope: StreamScope::Job,
            direction: StreamDirection::ClientToWorker,
            metadata: Attributes::new(),
        })
        .await?;

    let mut writer = AnyserveClient::connect(endpoint.clone()).await?;
    writer
        .close_stream(stream.stream_id, None, None, HashMap::new())
        .await?;

    let mut reader = AnyserveClient::connect(endpoint).await?;
    let read_job = reader.get_job(job.job_id.clone()).await?;
    let error = read_request_body(
        &mut reader,
        &read_job.job_id,
        &read_job.spec.as_ref().unwrap().params,
        Duration::from_secs(1),
    )
    .await
    .expect_err("empty terminal input stream should fail");
    assert!(error.to_string().contains("closed without body"));

    let _ = shutdown_tx.send(());
    handle.await.context("join grpc test server")??;
    Ok(())
}

async fn spawn_test_server() -> Result<(
    String,
    oneshot::Sender<()>,
    tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
    Arc<Kernel>,
)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .context("bind grpc test listener")?;
    let addr = listener.local_addr().context("resolve grpc test addr")?;
    let kernel = Arc::new(Kernel::new(
        Arc::new(MemoryStateStore::new()),
        Arc::new(MemoryFramePlane::new()),
        Arc::new(NoopClusterNotifier),
        Arc::new(BasicScheduler),
        30,
        30,
        250,
    ));
    let service = ControlPlaneGrpcService::new(Arc::clone(&kernel));
    let incoming = TcpListenerStream::new(listener);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let handle = tokio::spawn(async move {
        Server::builder()
            .add_service(ControlPlaneServiceServer::new(service))
            .serve_with_incoming_shutdown(incoming, async move {
                let _ = shutdown_rx.await;
            })
            .await
    });

    Ok((format!("http://{addr}"), shutdown_tx, handle, kernel))
}
