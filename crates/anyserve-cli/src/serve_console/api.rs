use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};

use crate::serve_config::RuntimeMode;
use anyserve_core::auth::AuthPolicy;
use anyserve_core::runtime::{
    Runtime,
    api::{Demand, ExecutionPolicy},
};

#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) runtime: Runtime,
    pub(crate) auth: Arc<AuthPolicy>,
    pub(crate) revocation_sync_ok: Option<Arc<AtomicBool>>,
    pub(crate) allowed_origins: Arc<BTreeSet<String>>,
    pub(crate) mode: RuntimeModeSummary,
    pub(crate) control_plane: Arc<ControlPlaneStatic>,
}

#[derive(Clone, Debug)]
pub(crate) struct ControlPlaneStatic {
    pub(crate) grpc_enabled: bool,
    pub(crate) grpc_listen: String,
    pub(crate) grpc_tls_enabled: bool,
    pub(crate) console_listen: String,
    pub(crate) public_endpoint: Option<String>,
    pub(crate) auth_portal_url: Option<String>,
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

    pub(crate) fn forbidden(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::FORBIDDEN,
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

#[derive(Clone, Serialize)]
pub(crate) struct ConsoleScopeSummary {
    pub(crate) is_operator: bool,
    pub(crate) namespace: String,
    pub(crate) subject: String,
}

#[derive(Serialize)]
pub(crate) struct InfoResponse {
    pub(crate) auth_enabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) auth_portal_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) public_endpoint: Option<String>,
    pub(crate) grpc_enabled: bool,
}

#[derive(Serialize)]
pub(crate) struct DrainWorkerResponse {
    pub(crate) session_id: String,
    pub(crate) draining: bool,
}

#[derive(Serialize)]
pub(crate) struct OverviewResponse {
    pub(crate) runtime: RuntimeModeSummary,
    pub(crate) health: HealthSummary,
    pub(crate) control_plane: ControlPlaneSummary,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) scope: Option<ConsoleScopeSummary>,
    pub(crate) budgets: BudgetsSummary,
    pub(crate) journal_stats: Option<JournalStatsSummary>,
    pub(crate) queue: QueueSummary,
    pub(crate) history: HistorySummary,
    pub(crate) workers: WorkerRollup,
    pub(crate) interfaces: Vec<InterfaceSummary>,
    pub(crate) pools: Vec<PoolSummary>,
    pub(crate) refreshed_at_ms: u64,
}

#[derive(Serialize)]
pub(crate) struct HealthSummary {
    pub(crate) live: bool,
    pub(crate) ready: bool,
    pub(crate) journal: JournalHealthSummary,
}

#[derive(Serialize)]
pub(crate) struct JournalHealthSummary {
    pub(crate) status: &'static str,
    pub(crate) message: Option<String>,
}

#[derive(Serialize)]
pub(crate) struct ControlPlaneSummary {
    pub(crate) grpc_enabled: bool,
    pub(crate) grpc_listen: String,
    pub(crate) grpc_tls_enabled: bool,
    pub(crate) console_listen: String,
    pub(crate) public_endpoint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) auth_portal_url: Option<String>,
    pub(crate) auth_enabled: bool,
    pub(crate) generation: u64,
    pub(crate) retained_jobs: usize,
    pub(crate) sessions: usize,
    pub(crate) max_sessions: usize,
    pub(crate) priority_levels: u32,
    pub(crate) default_max_attempts: u32,
    pub(crate) heartbeat_interval_ms: u64,
    pub(crate) heartbeat_timeout_ms: u64,
    pub(crate) reconnect_grace_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) recommended_poll_ms: Option<u64>,
}

#[derive(Serialize)]
pub(crate) struct BudgetsSummary {
    pub(crate) input_bytes: u64,
    pub(crate) max_pending_bytes: u64,
    pub(crate) result_bytes: u64,
    pub(crate) max_result_bytes: u64,
    pub(crate) event_bytes: u64,
    pub(crate) max_event_bytes: u64,
}

#[derive(Serialize)]
pub(crate) struct JournalStatsSummary {
    pub(crate) pending_ops: usize,
    pub(crate) committed_batches: u64,
    pub(crate) committed_ops: u64,
    pub(crate) last_batch_size: usize,
    pub(crate) last_commit_micros: u64,
}

#[derive(Serialize)]
pub(crate) struct QueueSummary {
    pub(crate) queued: usize,
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
    pub(crate) draining: usize,
    pub(crate) unavailable: usize,
    pub(crate) active_leases: u64,
    pub(crate) total_capacity: BTreeMap<String, i64>,
    pub(crate) last_seen_at_ms: Option<u64>,
}

#[derive(Serialize)]
pub(crate) struct PoolTierSummary {
    pub(crate) pool: String,
    pub(crate) spillover_after_ms: u64,
    pub(crate) reconnect_grace_ms: Option<u64>,
}

#[derive(Serialize)]
pub(crate) struct InterfacePolicySummary {
    pub(crate) max_queue: Option<usize>,
    pub(crate) queue_timeout_ms: Option<u64>,
    pub(crate) max_in_flight_per_key: Option<usize>,
    pub(crate) pool_tiers: Vec<PoolTierSummary>,
}

#[derive(Serialize)]
pub(crate) struct InterfaceSummary {
    pub(crate) name: String,
    pub(crate) queued: usize,
    pub(crate) running: usize,
    pub(crate) oldest_queued_age_ms: u64,
    pub(crate) sessions: usize,
    pub(crate) free_slots: u64,
    pub(crate) policy: Option<InterfacePolicySummary>,
}

#[derive(Serialize)]
pub(crate) struct PoolSummary {
    pub(crate) namespace: String,
    pub(crate) interface: String,
    pub(crate) pool: String,
    pub(crate) sessions: usize,
    pub(crate) free_slots: u64,
    pub(crate) active_leases: usize,
    pub(crate) desired_additional_slots: usize,
}

#[derive(Serialize)]
pub(crate) struct JobsHistogramBucketSummary {
    pub(crate) start_ms: u64,
    pub(crate) end_ms: u64,
    pub(crate) active: usize,
    pub(crate) succeeded: usize,
    pub(crate) failed: usize,
    pub(crate) cancelled: usize,
}

#[derive(Serialize)]
pub(crate) struct JobsHistogramSummary {
    pub(crate) start_ms: u64,
    pub(crate) end_ms: u64,
    pub(crate) bucket_ms: u64,
    pub(crate) buckets: Vec<JobsHistogramBucketSummary>,
}

#[derive(Serialize)]
pub(crate) struct JobsResponse {
    pub(crate) jobs: Vec<JobSummary>,
    pub(crate) total: usize,
    pub(crate) next_page_token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) histogram: Option<JobsHistogramSummary>,
}
#[derive(Serialize)]
pub(crate) struct JobSummary {
    pub(crate) job_id: String,
    pub(crate) namespace: String,
    pub(crate) state: &'static str,
    pub(crate) durability: &'static str,
    pub(crate) interface_name: String,
    pub(crate) source: Option<String>,
    pub(crate) created_at_ms: u64,
    pub(crate) updated_at_ms: u64,
    pub(crate) duration_ms: Option<u64>,
    pub(crate) current_attempt_id: Option<String>,
    pub(crate) latest_worker_id: Option<String>,
    pub(crate) attempt_count: u32,
    pub(crate) last_error: Option<String>,
    pub(crate) error_code: Option<String>,
    pub(crate) error_retryable: Option<bool>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) handler_duration_ms: Option<u64>,
    pub(crate) last_error: Option<String>,
}
#[derive(Serialize)]
pub(crate) struct JobEventSummary {
    pub(crate) sequence: u64,
    pub(crate) kind: &'static str,
    pub(crate) created_at_ms: u64,
    pub(crate) payload_size_bytes: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) attempt_id: Option<String>,
}
#[derive(Serialize)]
pub(crate) struct WorkerLoadSummary {
    pub(crate) queued: u32,
    pub(crate) running: u32,
    pub(crate) utilization: f64,
    pub(crate) custom: BTreeMap<String, f64>,
}
#[derive(Serialize)]
pub(crate) struct WorkerSummary {
    pub(crate) session_id: String,
    pub(crate) worker_id: String,
    pub(crate) namespace: String,
    pub(crate) pool: String,
    pub(crate) healthy: bool,
    pub(crate) interfaces: Vec<String>,
    pub(crate) attributes: BTreeMap<String, String>,
    pub(crate) total_capacity: BTreeMap<String, i64>,
    pub(crate) max_concurrency: u32,
    pub(crate) active_leases: u32,
    pub(crate) free_slots: u32,
    pub(crate) utilization: f64,
    pub(crate) load: WorkerLoadSummary,
    pub(crate) connected: bool,
    pub(crate) draining: bool,
    pub(crate) connected_at_ms: u64,
    pub(crate) last_seen_at_ms: u64,
}
#[derive(Serialize)]
pub(crate) struct WorkersResponse {
    pub(crate) workers: Vec<WorkerSummary>,
    pub(crate) total: usize,
    pub(crate) healthy: usize,
    pub(crate) draining: usize,
    pub(crate) disconnected: usize,
}
#[derive(Serialize)]
pub(crate) struct CancelJobResponse {
    pub(crate) job: JobSummary,
}
#[derive(Deserialize)]
pub(crate) struct JobsQuery {
    pub(crate) states: Option<String>,
    pub(crate) interface: Option<String>,
    pub(crate) namespace: Option<String>,
    pub(crate) worker: Option<String>,
    pub(crate) q: Option<String>,
    pub(crate) from_ms: Option<u64>,
    pub(crate) to_ms: Option<u64>,
    pub(crate) window_ms: Option<u64>,
    pub(crate) buckets: Option<usize>,
    pub(crate) limit: Option<usize>,
    pub(crate) offset: Option<usize>,
    pub(crate) page_token: Option<String>,
}

#[derive(Deserialize)]
pub(crate) struct NamespaceQuery {
    pub(crate) namespace: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AnalyticsSource {
    Archive,
    Retained,
}

#[derive(Deserialize)]
pub(crate) struct AnalyticsQuery {
    pub(crate) source: AnalyticsSource,
    pub(crate) from_ms: u64,
    pub(crate) to_ms: u64,
    pub(crate) limit: Option<usize>,
    pub(crate) cursor: Option<String>,
    pub(crate) namespace: Option<String>,
}

#[derive(Serialize)]
pub(crate) struct AnalyticsResponse {
    pub(crate) jobs: Vec<JobSummary>,
    pub(crate) next_cursor: Option<String>,
}
pub(crate) fn runtime_mode_summary(mode: RuntimeMode) -> RuntimeModeSummary {
    RuntimeModeSummary {
        code: mode.code(),
        label: mode.label(),
        journal: match mode {
            RuntimeMode::Memory => "disabled",
            RuntimeMode::MemoryPlusSqlite => "sqlite",
            RuntimeMode::MemoryPlusPostgres => "postgres",
            RuntimeMode::MemoryPlusCloudflare => "cloudflare",
        },
    }
}
