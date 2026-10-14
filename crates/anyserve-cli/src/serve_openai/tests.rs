use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use tokio::sync::watch;

use anyserve_core::frame::{FramePlane, MemoryFramePlane};
use anyserve_core::kernel::Kernel;
use anyserve_core::model::{
    Attributes, Demand, Frame, FrameKind, JobRecord, JobSpec, JobState, ObjectRef, StreamRecord,
};
use anyserve_core::notify::NoopClusterNotifier;
use anyserve_core::scheduler::BasicScheduler;
use anyserve_core::store::MemoryStateStore;

use super::handlers::submit_request;
use super::state::AppState;
use super::streaming::read_output_from_job;
use crate::serve_config::ServeOpenAIConfig;

#[test]
fn read_output_from_job_prefers_inline_output() {
    let job = JobRecord {
        spec: JobSpec::default(),
        outputs: vec![ObjectRef::Inline {
            content: br#"{"ok":true}"#.to_vec(),
            metadata: Attributes::from([(
                "content_type".to_string(),
                "application/json".to_string(),
            )]),
        }],
        ..JobRecord::default()
    };

    let output = read_output_from_job(&job).expect("inline output should be supported");
    assert_eq!(output, Some(br#"{"ok":true}"#.to_vec()));
}

#[tokio::test]
async fn streamed_submit_cancels_job_when_input_upload_fails() {
    let state = test_state(Arc::new(FailingAppendFramePlane {
        inner: MemoryFramePlane::new(),
    }));

    let error = submit_request(
        &state,
        &axum::http::HeaderMap::new(),
        "llm.chat.v1",
        b"hello".to_vec(),
    )
    .await
    .expect_err("streamed submit should fail");
    assert!(error.message.contains("push input frame"));

    let jobs = state.kernel.list_jobs().await.unwrap();
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].state, JobState::Cancelled);

    let streams = state.kernel.list_streams(&jobs[0].job_id).await.unwrap();
    assert_eq!(streams.len(), 1);
    assert!(streams[0].state.is_terminal());
}

fn test_state(frame_plane: Arc<dyn FramePlane>) -> AppState {
    AppState {
        kernel: Arc::new(Kernel::new(
            Arc::new(MemoryStateStore::new()),
            frame_plane,
            Arc::new(NoopClusterNotifier),
            Arc::new(BasicScheduler),
            30,
            30,
            250,
        )),
        config: Arc::new(ServeOpenAIConfig {
            listen: "127.0.0.1:8080".to_string(),
            models: vec!["demo".to_string()],
            chat_interface: "llm.chat.v1".to_string(),
            embeddings_interface: "llm.embed.v1".to_string(),
            request_timeout_secs: 30,
            inline_request_limit_bytes: 1,
            demand: Demand::default(),
            policy: Default::default(),
        }),
    }
}

struct FailingAppendFramePlane {
    inner: MemoryFramePlane,
}

#[async_trait]
impl FramePlane for FailingAppendFramePlane {
    async fn stream_created(&self, stream: &StreamRecord) -> Result<()> {
        self.inner.stream_created(stream).await
    }

    async fn stream_updated(&self, stream: &StreamRecord) -> Result<()> {
        self.inner.stream_updated(stream).await
    }

    async fn append_frame(
        &self,
        _stream: &StreamRecord,
        _kind: FrameKind,
        _payload: Vec<u8>,
        _metadata: Attributes,
        _now_ms: u64,
    ) -> Result<Frame> {
        Err(anyhow!("synthetic append failure"))
    }

    async fn frames_after(&self, stream_id: &str, after_sequence: u64) -> Result<Vec<Frame>> {
        self.inner.frames_after(stream_id, after_sequence).await
    }

    async fn wait_for_frames(
        &self,
        stream_id: &str,
        after_sequence: u64,
        timeout: Duration,
    ) -> Result<Vec<Frame>> {
        self.inner
            .wait_for_frames(stream_id, after_sequence, timeout)
            .await
    }

    async fn latest_sequence(&self, stream_id: &str) -> Result<Option<u64>> {
        self.inner.latest_sequence(stream_id).await
    }

    async fn subscribe_stream_updates(&self, stream_id: &str) -> watch::Receiver<u64> {
        self.inner.subscribe_stream_updates(stream_id).await
    }
}

mod edge_streaming {
    use std::sync::Arc;
    use std::time::Duration;

    use anyserve_core::auth::AuthPolicy;
    use anyserve_core::frame::MemoryFramePlane;
    use anyserve_core::kernel::Kernel;
    use anyserve_core::model::Demand;
    use anyserve_core::notify::NoopClusterNotifier;
    use anyserve_core::scheduler::BasicScheduler;
    use anyserve_core::service::ControlPlaneGrpcService;
    use anyserve_core::store::MemoryStateStore;
    use anyserve_proto::controlplane::control_plane_service_server::ControlPlaneServiceServer;
    use axum::response::IntoResponse;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::Server;

    use crate::serve_config::ServeOpenAIConfig;
    use crate::serve_openai::router;
    use crate::serve_openai_worker::{ServeOpenAIWorkerArgs, run as run_worker};

    const CHUNKS: [&str; 3] = ["Hel", "lo", "!"];

    /// OpenAI-compatible upstream: SSE chunks with gaps, or one JSON body.
    async fn spawn_upstream() -> String {
        let app = axum::Router::new().route(
            "/v1/chat/completions",
            axum::routing::post(|body: axum::Json<serde_json::Value>| async move {
                if body.get("stream").and_then(|value| value.as_bool()) != Some(true) {
                    return axum::Json(
                        serde_json::json!({"choices": [{"message": {"content": "Hello!"}}]}),
                    )
                    .into_response();
                }
                let events = async_stream::stream! {
                    for chunk in CHUNKS {
                        tokio::time::sleep(Duration::from_millis(30)).await;
                        let event = serde_json::json!({"choices": [{"delta": {"content": chunk}}]});
                        yield Ok::<_, std::convert::Infallible>(format!("data: {event}\n\n"));
                    }
                    yield Ok("data: [DONE]\n\n".to_string());
                };
                (
                    [(axum::http::header::CONTENT_TYPE, "text/event-stream")],
                    axum::body::Body::from_stream(events),
                )
                    .into_response()
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{addr}/v1")
    }

    async fn spawn_center() -> (String, String) {
        let kernel = Arc::new(Kernel::new(
            Arc::new(MemoryStateStore::new()),
            Arc::new(MemoryFramePlane::new()),
            Arc::new(NoopClusterNotifier),
            Arc::new(BasicScheduler),
            30,
            30,
            50,
        ));
        kernel.start_background().await;
        let grpc = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let grpc_addr = grpc.local_addr().unwrap();
        let grpc_kernel = Arc::clone(&kernel);
        tokio::spawn(async move {
            Server::builder()
                .add_service(ControlPlaneServiceServer::new(
                    ControlPlaneGrpcService::new(grpc_kernel),
                ))
                .serve_with_incoming(TcpListenerStream::new(grpc))
                .await
                .unwrap();
        });
        let config = ServeOpenAIConfig {
            listen: "127.0.0.1:0".to_string(),
            models: vec!["demo".to_string()],
            chat_interface: "llm.chat.v1".to_string(),
            embeddings_interface: "llm.embed.v1".to_string(),
            request_timeout_secs: 10,
            inline_request_limit_bytes: 64 * 1024,
            demand: Demand::default(),
            policy: Default::default(),
        };
        let http = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_addr = http.local_addr().unwrap();
        let app = router(kernel, config, Arc::new(AuthPolicy::default()));
        tokio::spawn(async move { axum::serve(http, app).await.unwrap() });
        (format!("http://{grpc_addr}"), format!("http://{http_addr}"))
    }

    fn spawn_llm_worker(endpoint: String, upstream: &str) {
        let config = std::env::temp_dir().join(format!(
            "anyserve-openai-stream-{}.toml",
            std::process::id()
        ));
        std::fs::write(
            &config,
            format!("base_url = \"{upstream}\"\nworker_id = \"edge-llm\"\nmax_active_leases = 4\n"),
        )
        .unwrap();
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn chat_streams_tokens_through_edge_worker_and_returns_json() {
        let upstream = spawn_upstream().await;
        let (grpc, http) = spawn_center().await;
        spawn_llm_worker(grpc, &upstream);
        tokio::time::sleep(Duration::from_millis(300)).await;
        let client = reqwest::Client::new();

        let body = client
            .post(format!("{http}/v1/chat/completions"))
            .json(&serde_json::json!({"model": "demo", "stream": true, "messages": []}))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        let positions: Vec<usize> = CHUNKS
            .iter()
            .map(|chunk| body.find(&format!("\"content\":\"{chunk}\"")).expect(chunk))
            .collect();
        assert!(
            positions.windows(2).all(|pair| pair[0] < pair[1]),
            "out of order: {body}"
        );
        assert!(body.trim_end().ends_with("data: [DONE]"));

        let reply: serde_json::Value = client
            .post(format!("{http}/v1/chat/completions"))
            .json(&serde_json::json!({"model": "demo", "messages": []}))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(reply["choices"][0]["message"]["content"], "Hello!");
    }
}
