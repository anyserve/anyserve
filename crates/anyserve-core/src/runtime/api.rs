//! Stable types of the execution runtime API. Native clients and the client gRPC
//! service depend only on these and on [`super::Runtime`].

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use futures::Stream;
use serde::{Deserialize, Serialize};

use crate::error::RuntimeError;

pub type Metadata = BTreeMap<String, String>;
pub type Capacity = BTreeMap<String, i64>;

/// Metadata keys that carry transport context rather than job meaning. They
/// are excluded from the idempotency fingerprint; every other key counts.
pub const IDENTITY_EXCLUDED_METADATA_KEYS: &[&str] =
    &["traceparent", "tracestate", "baggage", "x-request-id"];

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Object {
    Inline { data: Bytes, metadata: Metadata },
    Uri { uri: String, metadata: Metadata },
}

impl Object {
    pub fn inline(data: impl Into<Bytes>) -> Self {
        Self::Inline {
            data: data.into(),
            metadata: Metadata::new(),
        }
    }

    pub fn metadata(&self) -> &Metadata {
        match self {
            Self::Inline { metadata, .. } | Self::Uri { metadata, .. } => metadata,
        }
    }

    /// Payload bytes held by the control plane for this object.
    pub fn inline_len(&self) -> usize {
        match self {
            Self::Inline { data, .. } => data.len(),
            Self::Uri { uri, .. } => uri.len(),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Demand {
    pub interface: String,
    pub required_attributes: Metadata,
    pub preferred_attributes: Metadata,
    pub required_capacity: Capacity,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Durability {
    Ephemeral,
    Durable,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionPolicy {
    /// `None` takes the entry point's default: `Submit` is durable, `Invoke`
    /// is always ephemeral.
    pub durability: Option<Durability>,
    /// Higher runs first; clamped to `RuntimeConfig::priority_levels - 1`.
    pub priority: u32,
    /// Longest time a job may wait queued before an attempt starts.
    pub queue_timeout: Option<Duration>,
    /// Longest run of one attempt; expiry fails the attempt (retryable).
    pub execution_timeout: Option<Duration>,
    /// Longest time from acceptance to a terminal state.
    pub total_timeout: Option<Duration>,
    /// Workers missing a preferred attribute may take the job only after it
    /// has been queued this long.
    pub spillover_after: Option<Duration>,
    /// Total attempts including the first; `None` uses the runtime default.
    /// Whether a failed attempt is retried at all is the worker's call
    /// (`ErrorInfo::retryable`).
    pub max_attempts: Option<u32>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct JobSpec {
    pub demand: Demand,
    pub inputs: Vec<Object>,
    pub params: Bytes,
    pub policy: ExecutionPolicy,
    pub metadata: Metadata,
}

/// What `Submit` does when the job id already exists.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ConflictPolicy {
    /// Same fingerprint returns the existing job; a different one conflicts.
    #[default]
    UseExisting,
    /// Any existing job with this id conflicts.
    Fail,
}

#[derive(Clone, Debug, Default)]
pub struct SubmitRequest {
    pub job_id: Option<String>,
    pub conflict: ConflictPolicy,
    pub spec: JobSpec,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum InputMode {
    /// `spec.inputs` and `spec.params` are the complete input.
    #[default]
    Complete,
    /// Further input arrives through [`Execution::input`] after start.
    Streaming,
}

#[derive(Clone, Debug, Default)]
pub struct InvokeRequest {
    pub spec: JobSpec,
    pub input: InputMode,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubmitOutcome {
    pub job: JobView,
    pub created: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum JobState {
    Queued,
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

impl JobState {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorInfo {
    pub code: String,
    pub message: String,
    pub retryable: bool,
}

impl ErrorInfo {
    pub fn new(code: &str, message: impl Into<String>, retryable: bool) -> Self {
        Self {
            code: code.to_string(),
            message: message.into(),
            retryable,
        }
    }
}

/// Stable `ErrorInfo::code` values produced by the runtime. Workers may report
/// their own codes in addition.
pub mod codes {
    pub const QUEUE_TIMEOUT: &str = "queue_timeout";
    pub const EXECUTION_TIMEOUT: &str = "execution_timeout";
    pub const TOTAL_TIMEOUT: &str = "total_timeout";
    pub const CANCELLED: &str = "cancelled";
    pub const WORKER_FAILED: &str = "worker_failed";
    pub const WORKER_LOST: &str = "worker_lost";
    pub const WORKER_REJECTED: &str = "worker_rejected";
    pub const ATTEMPTS_EXHAUSTED: &str = "attempts_exhausted";
    pub const OUTPUT_INTERRUPTED: &str = "output_interrupted";
    pub const OUTPUT_TOO_LARGE: &str = "output_too_large";
    pub const CONSUMER_STALLED: &str = "consumer_stalled";
    pub const RECOVERED: &str = "control_plane_restarted";
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JobView {
    pub job_id: String,
    pub state: JobState,
    pub durability: Durability,
    pub interface: String,
    /// Attempts started so far, including rejected and lost ones.
    pub attempt_count: u32,
    pub current_attempt_id: Option<String>,
    pub current_worker_id: Option<String>,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    pub error: Option<ErrorInfo>,
    /// Filled only when requested and the job succeeded.
    pub outputs: Option<Vec<Object>>,
    pub metadata: Metadata,
    pub spec_hash: String,
}

/// A job's detail and retained events. Journal replay shares the result buffers
/// with `job`; request payloads are available only while retained in memory.
#[derive(Clone, Debug)]
pub struct JobInspection {
    pub job: JobView,
    pub spec: Option<Arc<JobSpec>>,
    pub events: Vec<ExecutionEvent>,
}

/// Maximum inline bytes per object in a management preview.
pub const INSPECTION_PREVIEW_BYTES: usize = 4 * 1024;

/// An object's original size and a bounded prefix, never a complete-result API.
/// URI objects and metadata are preserved without fetching external content.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectPreview {
    pub prefix: Object,
    /// Original inline length; unknown for a URI.
    pub size_bytes: Option<usize>,
}

impl From<&Object> for ObjectPreview {
    fn from(object: &Object) -> Self {
        match object {
            Object::Inline { data, metadata } => Self {
                prefix: Object::Inline {
                    data: Bytes::copy_from_slice(&data[..data.len().min(INSPECTION_PREVIEW_BYTES)]),
                    metadata: metadata.clone(),
                },
                size_bytes: Some(data.len()),
            },
            Object::Uri { .. } => Self {
                prefix: object.clone(),
                size_bytes: None,
            },
        }
    }
}

/// Management detail with bounded output prefixes. `job.outputs` is absent;
/// callers needing full results must use `get_job` or `inspect_job` instead.
/// Live events share retained buffers; cold replay contains no output payloads.
#[derive(Clone, Debug)]
pub struct JobPreview {
    pub job: JobView,
    pub spec: Option<Arc<JobSpec>>,
    pub events: Vec<ExecutionEvent>,
    pub outputs: Vec<ObjectPreview>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutionEvent {
    pub job_id: String,
    /// Strictly increasing per job, also across control-plane restarts.
    pub sequence: u64,
    pub attempt_id: Option<String>,
    pub at_ms: u64,
    pub kind: EventKind,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EventKind {
    Accepted,
    Assigned {
        worker_id: String,
        attempt_number: u32,
    },
    Output {
        data: Bytes,
        metadata: Metadata,
        progress: bool,
    },
    /// The previous attempt ended without exposing output and a new one will
    /// be scheduled.
    AttemptRestarted {
        previous_attempt_id: String,
        reason: ErrorInfo,
    },
    /// Always the last event of a job.
    Finished {
        state: JobState,
        outputs: Vec<Object>,
        error: Option<ErrorInfo>,
    },
}

/// A stream of job events that ends after `Finished`.
pub struct EventStream {
    inner: Pin<Box<dyn Stream<Item = Result<ExecutionEvent, RuntimeError>> + Send>>,
}

impl EventStream {
    pub(crate) fn new(
        inner: impl Stream<Item = Result<ExecutionEvent, RuntimeError>> + Send + 'static,
    ) -> Self {
        Self {
            inner: Box::pin(inner),
        }
    }
}

impl Stream for EventStream {
    type Item = Result<ExecutionEvent, RuntimeError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

/// A live execution started by `Invoke`. Dropping it before the job finishes
/// cancels the job.
pub struct Execution {
    pub(crate) job_id: String,
    pub(crate) events: EventStream,
    pub(crate) input: Option<InputSink>,
    pub(crate) _guard: Box<dyn Send + Sync>,
}

impl Execution {
    pub fn job_id(&self) -> &str {
        &self.job_id
    }

    /// The input sink for `InputMode::Streaming`; `None` otherwise or after it
    /// was taken.
    pub fn take_input(&mut self) -> Option<InputSink> {
        self.input.take()
    }
}

impl Stream for Execution {
    type Item = Result<ExecutionEvent, RuntimeError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.events).poll_next(cx)
    }
}

/// Streaming input of one ephemeral execution. The whole input is retained
/// (bounded by `Limits::max_inline_total_bytes`) so a retry can replay it.
#[derive(Clone)]
pub struct InputSink {
    pub(crate) log: Arc<crate::runtime::input::InputLog>,
}

impl InputSink {
    pub async fn send(&self, data: Bytes, metadata: Metadata) -> Result<(), RuntimeError> {
        self.log.push(data, metadata)
    }

    /// Ends the input. Further `send` calls fail.
    pub async fn finish(&self) -> Result<(), RuntimeError> {
        self.log.finish();
        Ok(())
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct JobFilter {
    /// Empty matches every state.
    pub states: Vec<JobState>,
    pub interface: Option<String>,
    /// 0 uses a default page size.
    pub limit: usize,
    pub page_token: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct JobPage {
    pub jobs: Vec<JobView>,
    pub next_page_token: Option<String>,
}

/// Bounded metadata scan in descending (creation time, job id) order.
/// Archived scans include completed durable jobs; retained scans include live
/// and recently completed jobs. Neither reads request or result bodies.
#[derive(Clone, Debug)]
pub struct JobScan {
    pub archived: bool,
    pub from_ms: u64,
    /// Exclusive upper bound, fixed across pages by the caller.
    pub to_ms: u64,
    pub limit: usize,
    pub cursor: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct WorkerLoad {
    pub queued: u32,
    pub running: u32,
    /// 0..1; the worker decides what it measures.
    pub utilization: f64,
    pub custom: BTreeMap<String, f64>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct WorkerView {
    pub session_id: String,
    pub worker_id: String,
    pub interfaces: Vec<String>,
    pub attributes: Metadata,
    pub total_capacity: Capacity,
    pub max_concurrency: u32,
    pub active_leases: u32,
    pub free_slots: u32,
    pub load: WorkerLoad,
    pub draining: bool,
    pub connected: bool,
    pub connected_at_ms: u64,
    pub last_seen_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JournalHealth {
    Disabled,
    Healthy,
    Unavailable(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Health {
    pub live: bool,
    /// Accepting new work; false while shutting down or when the journal is
    /// unavailable.
    pub ready: bool,
    pub journal: JournalHealth,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct InterfaceStats {
    pub queued: usize,
    pub running: usize,
    pub oldest_queued_age_ms: u64,
    pub sessions: usize,
    pub free_slots: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct JournalStats {
    pub pending_ops: usize,
    pub committed_batches: u64,
    pub committed_ops: u64,
    pub last_batch_size: usize,
    pub last_commit_micros: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RuntimeStats {
    pub generation: u64,
    pub queued: usize,
    pub running: usize,
    /// Terminal jobs still held in memory, by outcome.
    pub succeeded: usize,
    pub failed: usize,
    pub cancelled: usize,
    pub retained_jobs: usize,
    pub sessions: usize,
    pub interfaces: BTreeMap<String, InterfaceStats>,
    pub journal: Option<JournalStats>,
    /// Bytes held against `max_pending_bytes`.
    pub input_bytes: u64,
    /// Bytes held against `max_result_bytes`.
    pub result_bytes: u64,
    /// Bytes held against `max_event_bytes`.
    pub event_bytes: u64,
}

/// Size limits. Every inline budget stays below the 64 MiB gRPC message limit
/// with room for metadata and framing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Limits {
    pub max_control_message_bytes: u32,
    pub max_chunk_bytes: u32,
    pub attempt_window_bytes: u32,
    /// Inputs plus params up to this size travel inside `Assign`.
    pub assign_inline_limit: u32,
    pub max_inline_object_bytes: u64,
    /// Inputs plus params, or all outputs, of one job.
    pub max_inline_total_bytes: u64,
    /// Keys plus values of job metadata, attributes and output metadata,
    /// plus interface names.
    pub max_metadata_bytes: u32,
    /// Input objects of a job, result objects of an attempt, and interfaces
    /// of a worker.
    pub max_objects: u32,
    /// Job ids, interface names and worker ids.
    pub max_identifier_bytes: u32,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_control_message_bytes: 256 * 1024,
            max_chunk_bytes: 64 * 1024,
            attempt_window_bytes: 4 * 1024 * 1024,
            assign_inline_limit: 64 * 1024,
            max_inline_object_bytes: 16 * 1024 * 1024,
            max_inline_total_bytes: 32 * 1024 * 1024,
            max_metadata_bytes: 32 * 1024,
            max_objects: 1_024,
            max_identifier_bytes: 256,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct InterfacePolicy {
    pub max_queue: Option<usize>,
    pub queue_timeout: Option<Duration>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeConfig {
    pub heartbeat_interval: Duration,
    /// A worker session silent this long is closed; its leases survive this
    /// long again for a reconnect before their attempts are lost.
    pub heartbeat_timeout: Duration,
    pub default_max_attempts: u32,
    /// Upper bound for a job's own `ExecutionPolicy::max_attempts`. It also
    /// bounds the lifecycle events that may exceed `max_event_bytes`:
    /// `(max_pending_jobs + max_terminal_jobs) × (2 × max_attempts_limit + 1)
    /// × 96 B`, about 433 MB with the defaults.
    pub max_attempts_limit: u32,
    pub default_initial_backoff: Duration,
    pub default_max_backoff: Duration,
    pub priority_levels: u32,
    pub default_max_queue: usize,
    /// Unfinished jobs across all interfaces, including those being accepted.
    pub max_pending_jobs: usize,
    /// Input bytes (static inputs, params and streaming input) of unfinished
    /// jobs, from acceptance until the job ends.
    pub max_pending_bytes: u64,
    pub interfaces: BTreeMap<String, InterfacePolicy>,
    /// A streamed consumer that stops reading this long fails the job.
    pub consumer_stall_timeout: Duration,
    /// How long a cancelled lease keeps its reservation without an ack.
    pub cancel_grace: Duration,
    /// Terminal jobs stay in memory this long (durable ones remain in the
    /// journal afterwards).
    pub terminal_retention: Duration,
    pub max_terminal_jobs: usize,
    pub event_log_max_events: usize,
    /// Per job. A streamed `Invoke` consumer that falls this far behind
    /// back-pressures the worker; other logs drop their oldest events.
    pub event_log_max_bytes: usize,
    /// Event bytes across all jobs, enforced the same way as the per-job bound.
    pub max_event_bytes: u64,
    /// Result payload bytes alive anywhere in the control plane, from the
    /// start of an upload until the last reference is dropped. When an upload
    /// does not fit, the oldest terminal jobs leave memory once and the upload
    /// is retried; if it still does not fit it is refused with
    /// `RESOURCE_EXHAUSTED`.
    pub max_result_bytes: u64,
    pub max_sessions: usize,
    pub limits: Limits,
    pub journal: Option<JournalConfig>,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            heartbeat_interval: Duration::from_secs(5),
            heartbeat_timeout: Duration::from_secs(15),
            default_max_attempts: 3,
            max_attempts_limit: 20,
            default_initial_backoff: Duration::from_millis(200),
            default_max_backoff: Duration::from_secs(10),
            priority_levels: 4,
            default_max_queue: 10_000,
            max_pending_jobs: 100_000,
            max_pending_bytes: 2 * 1024 * 1024 * 1024,
            interfaces: BTreeMap::new(),
            consumer_stall_timeout: Duration::from_secs(30),
            cancel_grace: Duration::from_secs(15),
            terminal_retention: Duration::from_secs(600),
            max_terminal_jobs: 10_000,
            event_log_max_events: 8_192,
            event_log_max_bytes: 8 * 1024 * 1024,
            max_event_bytes: 1024 * 1024 * 1024,
            max_result_bytes: 2 * 1024 * 1024 * 1024,
            max_sessions: 4_096,
            limits: Limits::default(),
            journal: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JournalConfig {
    pub path: PathBuf,
    /// `synchronous=FULL` when true (default), `NORMAL` otherwise.
    pub full_sync: bool,
    pub queue_capacity: usize,
    pub max_batch: usize,
    /// Terminal durable jobs are deleted after this long.
    pub retention: Duration,
    /// Caps the database size; writes beyond it roll back and durable
    /// admission pauses while ephemeral work continues.
    pub max_bytes: Option<u64>,
}

impl JournalConfig {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            full_sync: true,
            queue_capacity: 4_096,
            max_batch: 256,
            retention: Duration::from_secs(7 * 24 * 3600),
            max_bytes: None,
        }
    }
}
