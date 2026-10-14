use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

pub type Attributes = BTreeMap<String, String>;
pub type Capacity = BTreeMap<String, i64>;

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct Demand {
    pub required_attributes: Attributes,
    pub preferred_attributes: Attributes,
    pub required_capacity: Capacity,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct ExecutionPolicy {
    pub priority: i32,
    pub lease_ttl_secs: u64,
    /// Fail the job if it is still pending this long after submission. 0 disables.
    #[serde(default)]
    pub queue_timeout_ms: u64,
    /// Locality hold: workers that do not match every preferred attribute may only
    /// lease the job once it has been pending this long. 0 disables.
    #[serde(default)]
    pub spillover_after_ms: u64,
}

impl Default for ExecutionPolicy {
    fn default() -> Self {
        Self {
            priority: 0,
            lease_ttl_secs: 30,
            queue_timeout_ms: 0,
            spillover_after_ms: 0,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub enum ObjectRef {
    Inline {
        content: Vec<u8>,
        metadata: Attributes,
    },
    Uri {
        uri: String,
        metadata: Attributes,
    },
}

impl ObjectRef {
    pub fn inline(content: Vec<u8>) -> Self {
        Self::Inline {
            content,
            metadata: Attributes::new(),
        }
    }

    pub fn metadata(&self) -> &Attributes {
        match self {
            Self::Inline { metadata, .. } | Self::Uri { metadata, .. } => metadata,
        }
    }
}

impl Default for ObjectRef {
    fn default() -> Self {
        Self::Inline {
            content: Vec::new(),
            metadata: Attributes::new(),
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct JobSpec {
    pub interface_name: String,
    pub inputs: Vec<ObjectRef>,
    pub params: Vec<u8>,
    pub demand: Demand,
    pub policy: ExecutionPolicy,
    pub metadata: Attributes,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum JobState {
    #[default]
    Pending,
    Leased,
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

impl ExecutionPolicy {
    /// Earliest time a worker outside the preferred locality may lease a job
    /// created at `created_at_ms`, or `None` when no hold applies.
    pub fn spillover_at_ms(&self, demand: &Demand, created_at_ms: u64) -> Option<u64> {
        if self.spillover_after_ms == 0 || demand.preferred_attributes.is_empty() {
            return None;
        }
        Some(created_at_ms.saturating_add(self.spillover_after_ms))
    }

    pub fn queue_deadline_ms(&self, created_at_ms: u64) -> Option<u64> {
        (self.queue_timeout_ms > 0).then(|| created_at_ms.saturating_add(self.queue_timeout_ms))
    }
}

impl JobState {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled)
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct JobRecord {
    pub job_id: String,
    pub state: JobState,
    pub spec: JobSpec,
    pub outputs: Vec<ObjectRef>,
    pub lease_id: Option<String>,
    pub version: u64,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    pub last_error: Option<String>,
    pub current_attempt_id: Option<String>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AttemptState {
    #[default]
    Created,
    Leased,
    Running,
    Succeeded,
    Failed,
    Expired,
    Cancelled,
}

impl AttemptState {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Expired | Self::Cancelled
        )
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct AttemptRecord {
    pub attempt_id: String,
    pub job_id: String,
    pub worker_id: String,
    pub lease_id: String,
    pub state: AttemptState,
    pub created_at_ms: u64,
    pub started_at_ms: Option<u64>,
    pub finished_at_ms: Option<u64>,
    pub last_error: Option<String>,
    pub metadata: Attributes,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    #[default]
    Accepted,
    LeaseGranted,
    Started,
    Progress,
    OutputReady,
    Succeeded,
    Failed,
    Cancelled,
    LeaseExpired,
    Requeued,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct JobEvent {
    pub job_id: String,
    pub sequence: u64,
    pub kind: EventKind,
    pub payload: Vec<u8>,
    pub metadata: Attributes,
    pub created_at_ms: u64,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct WorkerSpec {
    pub interfaces: BTreeSet<String>,
    pub attributes: Attributes,
    pub total_capacity: Capacity,
    pub max_active_leases: u32,
    pub metadata: Attributes,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct WorkerStatus {
    pub available_capacity: Capacity,
    pub active_leases: u32,
    pub metadata: Attributes,
    pub last_seen_at_ms: u64,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct WorkerRecord {
    pub worker_id: String,
    pub spec: WorkerSpec,
    pub status: WorkerStatus,
    pub registered_at_ms: u64,
    pub expires_at_ms: u64,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct LeaseRecord {
    pub lease_id: String,
    pub job_id: String,
    pub worker_id: String,
    pub issued_at_ms: u64,
    pub expires_at_ms: u64,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct LeaseAssignment {
    pub lease: LeaseRecord,
    pub attempt: AttemptRecord,
    pub job: JobRecord,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StreamScope {
    #[default]
    Job,
    Attempt,
    Lease,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StreamDirection {
    #[default]
    ClientToWorker,
    WorkerToClient,
    Bidirectional,
    Internal,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StreamState {
    #[default]
    Open,
    Closing,
    Closed,
    Error,
}

impl StreamState {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Closed | Self::Error)
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FrameKind {
    Open,
    #[default]
    Data,
    Close,
    Error,
    Checkpoint,
    Control,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct StreamRecord {
    pub stream_id: String,
    pub job_id: String,
    pub attempt_id: Option<String>,
    pub lease_id: Option<String>,
    pub stream_name: String,
    pub scope: StreamScope,
    pub direction: StreamDirection,
    pub state: StreamState,
    pub metadata: Attributes,
    pub created_at_ms: u64,
    pub closed_at_ms: Option<u64>,
    pub last_sequence: u64,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct Frame {
    pub stream_id: String,
    pub sequence: u64,
    pub kind: FrameKind,
    pub payload: Vec<u8>,
    pub metadata: Attributes,
    pub created_at_ms: u64,
}
