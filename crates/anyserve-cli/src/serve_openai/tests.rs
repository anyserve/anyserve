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

    let error = submit_request(&state, "llm.chat.v1", b"hello".to_vec())
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
