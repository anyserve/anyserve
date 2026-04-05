use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};

use anyserve_core::kernel::Kernel;
use anyserve_core::model::{
    AttemptState, Demand, EventKind, ExecutionPolicy, JobState, StreamDirection, StreamScope,
    StreamState,
};

use crate::serve_config::RuntimeMode;

#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) kernel: Arc<Kernel>,
    pub(crate) allowed_origins: Arc<BTreeSet<String>>,
    pub(crate) runtime: RuntimeModeSummary,
}

#[derive(Debug)]
pub(crate) struct ApiError {
    pub(crate) status: StatusCode,
    pub(crate) message: String,
}

impl ApiError {
    pub(crate) fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }

    pub(crate) fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: message.into(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(ErrorResponse {
                error: ErrorBody {
                    message: self.message,
                },
            }),
        )
            .into_response()
    }
}

#[derive(Serialize)]
pub(crate) struct ErrorResponse {
    pub(crate) error: ErrorBody,
}

#[derive(Serialize)]
pub(crate) struct ErrorBody {
    pub(crate) message: String,
}

#[derive(Clone, Serialize)]
pub(crate) struct RuntimeModeSummary {
    pub(crate) code: &'static str,
    pub(crate) label: &'static str,
    pub(crate) storage_backend: &'static str,
    pub(crate) frame_backend: &'static str,
}

#[derive(Serialize)]
pub(crate) struct QueueSummary {
    pub(crate) pending: usize,
    pub(crate) leased: usize,
    pub(crate) running: usize,
    pub(crate) active: usize,
}

#[derive(Serialize)]
pub(crate) struct HistorySummary {
    pub(crate) succeeded: usize,
    pub(crate) failed: usize,
    pub(crate) cancelled: usize,
    pub(crate) total: usize,
}

#[derive(Serialize)]
pub(crate) struct WorkerRollup {
    pub(crate) total: usize,
    pub(crate) healthy: usize,
    pub(crate) expired: usize,
    pub(crate) active_leases: u64,
    pub(crate) total_capacity: BTreeMap<String, i64>,
    pub(crate) available_capacity: BTreeMap<String, i64>,
    pub(crate) last_seen_at_ms: Option<u64>,
}

#[derive(Serialize)]
pub(crate) struct JobsResponse {
    pub(crate) jobs: Vec<JobSummary>,
    pub(crate) total: usize,
    pub(crate) limit: usize,
    pub(crate) offset: usize,
}

#[derive(Serialize)]
pub(crate) struct JobSummary {
    pub(crate) job_id: String,
    pub(crate) state: JobState,
    pub(crate) interface_name: String,
    pub(crate) source: Option<String>,
    pub(crate) created_at_ms: u64,
    pub(crate) updated_at_ms: u64,
    pub(crate) duration_ms: Option<u64>,
    pub(crate) current_attempt_id: Option<String>,
    pub(crate) latest_worker_id: Option<String>,
    pub(crate) attempt_count: usize,
    pub(crate) last_error: Option<String>,
    pub(crate) metadata: BTreeMap<String, String>,
}

#[derive(Serialize)]
pub(crate) struct JobDetailResponse {
    pub(crate) job: JobDetail,
    pub(crate) attempts: Vec<AttemptSummary>,
    pub(crate) streams: Vec<StreamSummary>,
    pub(crate) events: Vec<JobEventSummary>,
    pub(crate) latest_worker: Option<WorkerSummary>,
}

#[derive(Serialize)]
pub(crate) struct JobDetail {
    pub(crate) job_id: String,
    pub(crate) state: JobState,
    pub(crate) interface_name: String,
    pub(crate) source: Option<String>,
    pub(crate) created_at_ms: u64,
    pub(crate) updated_at_ms: u64,
    pub(crate) duration_ms: Option<u64>,
    pub(crate) current_attempt_id: Option<String>,
    pub(crate) lease_id: Option<String>,
    pub(crate) version: u64,
    pub(crate) last_error: Option<String>,
    pub(crate) metadata: BTreeMap<String, String>,
    pub(crate) demand: Demand,
    pub(crate) policy: ExecutionPolicy,
    pub(crate) params_size_bytes: usize,
    pub(crate) params_preview: Option<String>,
    pub(crate) params_truncated: bool,
    pub(crate) output_preview: Option<InlinePreviewSummary>,
    pub(crate) inputs: Vec<ObjectRefSummary>,
    pub(crate) outputs: Vec<ObjectRefSummary>,
}

#[derive(Serialize)]
pub(crate) struct InlinePreviewSummary {
    pub(crate) size_bytes: usize,
    pub(crate) content_type: Option<String>,
    pub(crate) preview: Option<String>,
    pub(crate) truncated: bool,
}

#[derive(Serialize)]
pub(crate) struct ObjectRefSummary {
    pub(crate) kind: &'static str,
    pub(crate) size_bytes: Option<usize>,
    pub(crate) uri: Option<String>,
    pub(crate) content_type: Option<String>,
    pub(crate) preview: Option<String>,
    pub(crate) truncated: bool,
    pub(crate) metadata: BTreeMap<String, String>,
}

#[derive(Serialize)]
pub(crate) struct AttemptSummary {
    pub(crate) attempt_id: String,
    pub(crate) state: AttemptState,
    pub(crate) worker_id: Option<String>,
    pub(crate) lease_id: Option<String>,
    pub(crate) created_at_ms: u64,
    pub(crate) started_at_ms: Option<u64>,
    pub(crate) finished_at_ms: Option<u64>,
    pub(crate) duration_ms: Option<u64>,
    pub(crate) last_error: Option<String>,
    pub(crate) metadata: BTreeMap<String, String>,
}

#[derive(Serialize)]
pub(crate) struct StreamSummary {
    pub(crate) stream_id: String,
    pub(crate) stream_name: String,
    pub(crate) scope: StreamScope,
    pub(crate) direction: StreamDirection,
    pub(crate) state: StreamState,
    pub(crate) attempt_id: Option<String>,
    pub(crate) lease_id: Option<String>,
    pub(crate) created_at_ms: u64,
    pub(crate) closed_at_ms: Option<u64>,
    pub(crate) last_sequence: u64,
    pub(crate) metadata: BTreeMap<String, String>,
}

#[derive(Serialize)]
pub(crate) struct JobEventSummary {
    pub(crate) sequence: u64,
    pub(crate) kind: EventKind,
    pub(crate) created_at_ms: u64,
    pub(crate) payload_size_bytes: usize,
    pub(crate) metadata: BTreeMap<String, String>,
}

#[derive(Serialize)]
pub(crate) struct WorkersResponse {
    pub(crate) workers: Vec<WorkerSummary>,
    pub(crate) total: usize,
    pub(crate) healthy: usize,
}

#[derive(Clone, Serialize)]
pub(crate) struct WorkerSummary {
    pub(crate) worker_id: String,
    pub(crate) healthy: bool,
    pub(crate) interfaces: Vec<String>,
    pub(crate) attributes: BTreeMap<String, String>,
    pub(crate) total_capacity: BTreeMap<String, i64>,
    pub(crate) available_capacity: BTreeMap<String, i64>,
    pub(crate) max_active_leases: u32,
    pub(crate) active_leases: u32,
    pub(crate) registered_at_ms: u64,
    pub(crate) last_seen_at_ms: u64,
    pub(crate) expires_at_ms: u64,
    pub(crate) spec_metadata: BTreeMap<String, String>,
    pub(crate) status_metadata: BTreeMap<String, String>,
}

#[derive(Serialize)]
pub(crate) struct CancelJobResponse {
    pub(crate) job: JobSummary,
}

#[derive(Deserialize)]
pub(crate) struct JobsQuery {
    pub(crate) states: Option<String>,
    pub(crate) limit: Option<usize>,
    pub(crate) offset: Option<usize>,
}

pub(crate) fn runtime_mode_summary(mode: RuntimeMode) -> RuntimeModeSummary {
    match mode {
        RuntimeMode::Memory => RuntimeModeSummary {
            code: mode.code(),
            label: mode.label(),
            storage_backend: "memory",
            frame_backend: "memory",
        },
        RuntimeMode::MemoryPlusPostgres => RuntimeModeSummary {
            code: mode.code(),
            label: mode.label(),
            storage_backend: "postgresql",
            frame_backend: "memory",
        },
        RuntimeMode::MemoryPlusSqlite => RuntimeModeSummary {
            code: mode.code(),
            label: mode.label(),
            storage_backend: "sqlite",
            frame_backend: "memory",
        },
        RuntimeMode::PostgresAndRedis => RuntimeModeSummary {
            code: mode.code(),
            label: mode.label(),
            storage_backend: "postgresql",
            frame_backend: "redis",
        },
    }
}
