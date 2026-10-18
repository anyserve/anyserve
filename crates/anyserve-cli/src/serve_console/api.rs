use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};

use crate::serve_config::RuntimeMode;
use anyserve_core::runtime::{
    Runtime,
    api::{Demand, ExecutionPolicy},
};

#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) runtime: Runtime,
    pub(crate) allowed_origins: Arc<BTreeSet<String>>,
    pub(crate) mode: RuntimeModeSummary,
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
}
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(serde_json::json!({ "error": { "message": self.message } })),
        )
            .into_response()
    }
}

#[derive(Clone, Serialize)]
pub(crate) struct RuntimeModeSummary {
    pub(crate) code: &'static str,
    pub(crate) label: &'static str,
    pub(crate) journal: &'static str,
}
#[derive(Serialize)]
pub(crate) struct JobsResponse {
    pub(crate) jobs: Vec<JobSummary>,
    pub(crate) next_page_token: Option<String>,
}
#[derive(Serialize)]
pub(crate) struct JobSummary {
    pub(crate) job_id: String,
    pub(crate) state: &'static str,
    pub(crate) interface_name: String,
    pub(crate) source: Option<String>,
    pub(crate) created_at_ms: u64,
    pub(crate) updated_at_ms: u64,
    pub(crate) duration_ms: Option<u64>,
    pub(crate) current_attempt_id: Option<String>,
    pub(crate) latest_worker_id: Option<String>,
    pub(crate) attempt_count: u32,
    pub(crate) last_error: Option<String>,
    pub(crate) metadata: BTreeMap<String, String>,
}
#[derive(Serialize)]
pub(crate) struct JobDetailResponse {
    pub(crate) job: JobDetail,
    pub(crate) attempts: Vec<AttemptSummary>,
    pub(crate) events: Vec<JobEventSummary>,
    pub(crate) history_incomplete: bool,
    pub(crate) latest_worker: Option<WorkerSummary>,
}
#[derive(Serialize)]
pub(crate) struct JobDetail {
    #[serde(flatten)]
    pub(crate) summary: JobSummary,
    pub(crate) demand: Option<Demand>,
    pub(crate) policy: Option<ExecutionPolicy>,
    pub(crate) params_size_bytes: Option<usize>,
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
    pub(crate) state: &'static str,
    pub(crate) worker_id: String,
    pub(crate) created_at_ms: u64,
    pub(crate) finished_at_ms: Option<u64>,
    pub(crate) duration_ms: Option<u64>,
    pub(crate) last_error: Option<String>,
}
#[derive(Serialize)]
pub(crate) struct JobEventSummary {
    pub(crate) sequence: u64,
    pub(crate) kind: &'static str,
    pub(crate) created_at_ms: u64,
    pub(crate) payload_size_bytes: usize,
}
#[derive(Clone, Serialize)]
pub(crate) struct WorkerSummary {
    pub(crate) worker_id: String,
    pub(crate) healthy: bool,
    pub(crate) interfaces: Vec<String>,
    pub(crate) attributes: BTreeMap<String, String>,
    pub(crate) total_capacity: BTreeMap<String, i64>,
    pub(crate) max_concurrency: u32,
    pub(crate) active_leases: u32,
    pub(crate) free_slots: u32,
    pub(crate) utilization: f64,
    pub(crate) connected: bool,
    pub(crate) draining: bool,
    pub(crate) last_seen_at_ms: u64,
}
#[derive(Serialize)]
pub(crate) struct WorkersResponse {
    pub(crate) workers: Vec<WorkerSummary>,
    pub(crate) total: usize,
    pub(crate) healthy: usize,
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
    RuntimeModeSummary {
        code: mode.code(),
        label: mode.label(),
        journal: match mode {
            RuntimeMode::Memory => "disabled",
            RuntimeMode::MemoryPlusSqlite => "sqlite",
        },
    }
}
