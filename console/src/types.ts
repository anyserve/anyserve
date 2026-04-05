export type JobState =
  | "pending"
  | "leased"
  | "running"
  | "succeeded"
  | "failed"
  | "cancelled";

export type AttemptState =
  | "created"
  | "leased"
  | "running"
  | "succeeded"
  | "failed"
  | "expired"
  | "cancelled";

export type StreamScope = "job" | "attempt" | "lease";
export type StreamDirection =
  | "client_to_worker"
  | "worker_to_client"
  | "bidirectional"
  | "internal";
export type StreamState = "open" | "closing" | "closed" | "error";
export type EventKind =
  | "accepted"
  | "lease_granted"
  | "started"
  | "progress"
  | "output_ready"
  | "succeeded"
  | "failed"
  | "cancelled"
  | "lease_expired"
  | "requeued";

export type CapacityMap = Record<string, number>;
export type MetadataMap = Record<string, string>;

export interface RuntimeModeSummary {
  code:
    | "memory"
    | "memory_plus_postgres"
    | "memory_plus_sqlite"
    | "postgres_and_redis";
  label: string;
  storage_backend: string;
  frame_backend: string;
}

export interface QueueSummary {
  pending: number;
  leased: number;
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
  expired: number;
  active_leases: number;
  total_capacity: CapacityMap;
  available_capacity: CapacityMap;
  last_seen_at_ms: number | null;
}

export interface OverviewResponse {
  runtime: RuntimeModeSummary;
  queue: QueueSummary;
  history: HistorySummary;
  workers: WorkerRollup;
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
  total: number;
  limit: number;
  offset: number;
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
  required_attributes: MetadataMap;
  preferred_attributes: MetadataMap;
  required_capacity: CapacityMap;
}

export interface ExecutionPolicy {
  profile: string;
  priority: number;
  lease_ttl_secs: number;
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
  lease_id: string | null;
  version: number;
  last_error: string | null;
  metadata: MetadataMap;
  demand: Demand;
  policy: ExecutionPolicy;
  params_size_bytes: number;
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
  lease_id: string | null;
  created_at_ms: number;
  started_at_ms: number | null;
  finished_at_ms: number | null;
  duration_ms: number | null;
  last_error: string | null;
  metadata: MetadataMap;
}

export interface StreamSummary {
  stream_id: string;
  stream_name: string;
  scope: StreamScope;
  direction: StreamDirection;
  state: StreamState;
  attempt_id: string | null;
  lease_id: string | null;
  created_at_ms: number;
  closed_at_ms: number | null;
  last_sequence: number;
  metadata: MetadataMap;
}

export interface JobEventSummary {
  sequence: number;
  kind: EventKind;
  created_at_ms: number;
  payload_size_bytes: number;
  metadata: MetadataMap;
}

export interface WorkerSummary {
  worker_id: string;
  healthy: boolean;
  interfaces: string[];
  attributes: MetadataMap;
  total_capacity: CapacityMap;
  available_capacity: CapacityMap;
  max_active_leases: number;
  active_leases: number;
  registered_at_ms: number;
  last_seen_at_ms: number;
  expires_at_ms: number;
  spec_metadata: MetadataMap;
  status_metadata: MetadataMap;
}

export interface WorkersResponse {
  workers: WorkerSummary[];
  total: number;
  healthy: number;
}

export interface JobDetailResponse {
  job: JobDetail;
  attempts: AttemptSummary[];
  streams: StreamSummary[];
  events: JobEventSummary[];
  latest_worker: WorkerSummary | null;
}

export interface CancelJobResponse {
  job: JobSummary;
}
