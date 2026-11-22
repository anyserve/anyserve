export type JobState = "queued" | "running" | "succeeded" | "failed" | "cancelled";
export type AttemptState = "assigned" | "restarted" | "succeeded" | "failed" | "cancelled";
export type EventKind = "accepted" | "assigned" | "output" | "progress" | "attempt_restarted" | "finished";
export type CapacityMap = Record<string, number>;
export type MetadataMap = Record<string, string>;
export type AnalyticsSource = "archive" | "retained";

export interface RuntimeModeSummary {
  code: "memory" | "memory_plus_sqlite" | "memory_plus_postgres" | "memory_plus_cloudflare";
  label: string;
  journal: "disabled" | "sqlite" | "postgres" | "cloudflare";
}

export interface QueueSummary { queued: number; running: number; active: number; }
export interface HistorySummary { succeeded: number; failed: number; cancelled: number; total: number; }
export interface RustDuration { secs: number; nanos: number; }
export interface HealthSummary { live: boolean; ready: boolean; journal: { status: "disabled" | "healthy" | "unavailable"; message: string | null }; }

export interface WorkerRollup {
  total: number; healthy: number; unavailable: number; draining?: number;
  active_leases: number; total_capacity: CapacityMap; last_seen_at_ms: number | null;
}

export interface PoolTierPolicy { pool: string; spillover_after_ms: number; reconnect_grace_ms: number | null; }
export interface InterfacePolicySummary {
  max_queue: number | null; queue_timeout_ms: number | null;
  max_in_flight_per_key: number | null; pool_tiers: PoolTierPolicy[];
}

export interface InterfaceSummary {
  name: string; queued: number; running: number; oldest_queued_age_ms: number;
  sessions: number; free_slots: number; policy?: InterfacePolicySummary | null;
}

export interface ControlPlaneSummary {
  grpc_listen: string; grpc_tls_enabled: boolean; console_listen: string; auth_enabled: boolean;
  public_endpoint?: string | null;
  generation: number; retained_jobs: number; sessions: number; max_sessions: number;
  priority_levels: number; default_max_attempts: number;
  heartbeat_interval_ms: number; heartbeat_timeout_ms: number; reconnect_grace_ms: number | null;
}

export interface BudgetsSummary {
  input_bytes: number; max_pending_bytes: number;
  result_bytes: number; max_result_bytes: number;
  event_bytes: number; max_event_bytes: number;
}

export interface JournalStatsSummary {
  pending_ops: number; committed_batches: number; committed_ops: number;
  last_batch_size: number; last_commit_micros: number;
}

export interface PoolSummary {
  namespace: string; interface: string; pool: string;
  sessions: number; free_slots: number; active_leases: number; desired_additional_slots: number;
}

export interface OverviewResponse {
  runtime: RuntimeModeSummary; queue: QueueSummary; history: HistorySummary;
  workers: WorkerRollup; interfaces: InterfaceSummary[]; refreshed_at_ms: number;
  health?: HealthSummary; control_plane?: ControlPlaneSummary; budgets?: BudgetsSummary;
  journal_stats?: JournalStatsSummary | null; pools?: PoolSummary[];
}

export interface JobSummary {
  job_id: string; state: JobState; interface_name: string; namespace?: string;
  durability?: "durable" | "ephemeral"; source: string | null;
  created_at_ms: number; updated_at_ms: number; duration_ms: number | null;
  current_attempt_id: string | null; latest_worker_id: string | null; attempt_count: number;
  last_error: string | null; error_code?: string | null; error_retryable?: boolean | null;
  metadata: MetadataMap;
}

export interface JobsHistogramBucket {
  start_ms: number;
  end_ms: number;
  active: number;
  succeeded: number;
  failed: number;
  cancelled: number;
}

export interface JobsHistogram {
  start_ms: number;
  end_ms: number;
  bucket_ms: number;
  buckets: JobsHistogramBucket[];
}

export interface JobsResponse {
  jobs: JobSummary[];
  total?: number;
  next_page_token: string | null;
  histogram?: JobsHistogram | null;
}
export interface AnalyticsJob extends JobSummary { durability: "durable" | "ephemeral"; }
export interface AnalyticsResponse { jobs: AnalyticsJob[]; next_cursor: string | null; }

export interface ObjectRefSummary {
  kind: "inline" | "uri"; size_bytes: number | null; uri: string | null;
  content_type: string | null; preview: string | null; truncated: boolean; metadata: MetadataMap;
}

export interface InlinePreviewSummary {
  size_bytes: number; content_type: string | null; preview: string | null; truncated: boolean;
}

export interface Demand {
  interface: string; required_attributes: MetadataMap; preferred_attributes: MetadataMap;
  required_capacity: CapacityMap; affinity_key?: string;
}

export interface ExecutionPolicy {
  priority: number; max_attempts: number | null; durability: "Ephemeral" | "Durable" | null;
  queue_timeout: RustDuration | null; execution_timeout: RustDuration | null;
  total_timeout: RustDuration | null; spillover_after: RustDuration | null;
  fairness_key?: string; concurrency_key?: string;
}

export interface JobDetail extends Omit<JobSummary, "latest_worker_id" | "attempt_count"> {
  demand: Demand | null; policy: ExecutionPolicy | null;
  params_size_bytes: number | null; params_preview: string | null; params_truncated: boolean;
  output_preview: InlinePreviewSummary | null; inputs: ObjectRefSummary[]; outputs: ObjectRefSummary[];
}

export interface AttemptSummary {
  attempt_id: string; state: AttemptState; worker_id: string | null;
  created_at_ms: number; finished_at_ms: number | null; duration_ms: number | null; last_error: string | null;
}

export interface JobEventSummary {
  sequence: number; kind: EventKind; created_at_ms: number; payload_size_bytes: number;
}

export interface WorkerLoadSummary {
  queued: number; running: number; utilization: number; custom: Record<string, number>;
}

export interface WorkerSummary {
  worker_id: string; session_id?: string; namespace?: string; pool?: string;
  healthy: boolean; interfaces: string[]; attributes: MetadataMap; total_capacity: CapacityMap;
  max_concurrency: number; free_slots: number; utilization: number; connected: boolean; draining: boolean;
  active_leases: number; connected_at_ms?: number; last_seen_at_ms: number; load?: WorkerLoadSummary;
}

export interface WorkersResponse {
  workers: WorkerSummary[]; total: number; healthy: number; draining?: number; disconnected?: number;
}

export interface JobDetailResponse {
  job: JobDetail; attempts: AttemptSummary[]; history_incomplete: boolean;
  events: JobEventSummary[]; latest_worker: WorkerSummary | null;
}

export interface CancelJobResponse { job: JobSummary; }
