export type JobState = "queued" | "running" | "succeeded" | "failed" | "cancelled";
export type AttemptState = "assigned" | "restarted" | "succeeded" | "failed" | "cancelled";
export type EventKind = "accepted" | "assigned" | "output" | "progress" | "attempt_restarted" | "finished";

export type CapacityMap = Record<string, number>;
export type MetadataMap = Record<string, string>;

export interface RuntimeModeSummary {
  code:
    | "memory"
    | "memory_plus_sqlite"
    | "postgres_cluster";
  label: string;
  journal: "disabled" | "sqlite" | "postgres";
}

export interface QueueSummary {
  queued: number;
  running: number;
  active: number;
}

export interface HistorySummary {
  succeeded: number;
  failed: number;
  cancelled: number;
  total: number;
}

export interface WorkerRollup {
  total: number;
  healthy: number;
  unavailable: number;
  active_leases: number;
  total_capacity: CapacityMap;
  last_seen_at_ms: number | null;
}

export interface InterfaceSummary {
  name: string;
  queued: number;
  running: number;
  oldest_queued_age_ms: number;
  sessions: number;
  free_slots: number;
}

export interface OverviewResponse {
  runtime: RuntimeModeSummary;
  queue: QueueSummary;
  history: HistorySummary;
  workers: WorkerRollup;
  interfaces: InterfaceSummary[];
  refreshed_at_ms: number;
}

export interface JobSummary {
  job_id: string;
  state: JobState;
  interface_name: string;
  source: string | null;
  created_at_ms: number;
  updated_at_ms: number;
  duration_ms: number | null;
  current_attempt_id: string | null;
  latest_worker_id: string | null;
  attempt_count: number;
  last_error: string | null;
  metadata: MetadataMap;
}

export interface JobsResponse {
  jobs: JobSummary[];
  next_page_token: string | null;
}

export type AnalyticsSource = "archive" | "retained";
export interface AnalyticsJob extends JobSummary {
  durability: "durable" | "ephemeral";
}
export interface AnalyticsResponse {
  jobs: AnalyticsJob[];
  next_cursor: string | null;
}

export interface ObjectRefSummary {
  kind: "inline" | "uri";
  size_bytes: number | null;
  uri: string | null;
  content_type: string | null;
  preview: string | null;
  truncated: boolean;
  metadata: MetadataMap;
}

export interface InlinePreviewSummary {
  size_bytes: number;
  content_type: string | null;
  preview: string | null;
  truncated: boolean;
}

export interface Demand {
  interface: string;
  required_attributes: MetadataMap;
  preferred_attributes: MetadataMap;
  required_capacity: CapacityMap;
}

export interface ExecutionPolicy {
  priority: number;
  max_attempts: number | null;
  durability: "Ephemeral" | "Durable" | null;
  queue_timeout: { secs: number; nanos: number } | null;
  execution_timeout: { secs: number; nanos: number } | null;
  total_timeout: { secs: number; nanos: number } | null;
  spillover_after: { secs: number; nanos: number } | null;
}

export interface JobDetail {
  job_id: string;
  state: JobState;
  interface_name: string;
  source: string | null;
  created_at_ms: number;
  updated_at_ms: number;
  duration_ms: number | null;
  current_attempt_id: string | null;
  last_error: string | null;
  metadata: MetadataMap;
  demand: Demand | null;
  policy: ExecutionPolicy | null;
  params_size_bytes: number | null;
  params_preview: string | null;
  params_truncated: boolean;
  output_preview: InlinePreviewSummary | null;
  inputs: ObjectRefSummary[];
  outputs: ObjectRefSummary[];
}

export interface AttemptSummary {
  attempt_id: string;
  state: AttemptState;
  worker_id: string | null;
  created_at_ms: number;
  finished_at_ms: number | null;
  duration_ms: number | null;
  last_error: string | null;
}

export interface JobEventSummary {
  sequence: number;
  kind: EventKind;
  created_at_ms: number;
  payload_size_bytes: number;
}

export interface WorkerSummary {
  worker_id: string;
  healthy: boolean;
  interfaces: string[];
  attributes: MetadataMap;
  total_capacity: CapacityMap;
  max_concurrency: number;
  free_slots: number;
  utilization: number;
  connected: boolean;
  draining: boolean;
  active_leases: number;
  last_seen_at_ms: number;
}

export interface WorkersResponse {
  workers: WorkerSummary[];
  total: number;
  healthy: number;
}

export interface JobDetailResponse {
  job: JobDetail;
  attempts: AttemptSummary[];
  history_incomplete: boolean;
  events: JobEventSummary[];
  latest_worker: WorkerSummary | null;
}

export interface CancelJobResponse {
  job: JobSummary;
}
