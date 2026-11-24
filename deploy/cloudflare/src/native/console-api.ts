// Management & Console HTTP API for AnyserveNativeDO (section 6.2, 7 of docs/cloudflare-native-do-plan.md).
// Matches console/src/types.ts and crates/anyserve-cli/src/serve_console/{api,handlers,snapshot,metrics}.rs.

import { GrpcCode, GrpcStatusError, type AuthPolicy, type Principal } from "./auth";
import { DEFAULT_NAMESPACE, encodeScopedId, validateNamespaceSafe } from "./engine";
import {
  MAX_IDENTIFIER_BYTES,
  MAX_INTERFACE_BYTES,
  PREVIEW_MAX_BYTES,
  isValidInterfaceName,
  type InterfacePolicyConfig,
  type NativeEnvConfig,
} from "./host";
import {
  formatInlinePreview,
  type DurabilityName,
  type JobStateName,
  type ObjectPreviewData,
  type StoredDemand,
  type StoredErrorInfo,
  type StoredExecutionPolicy,
} from "./store";
import { utf8ByteLen } from "./utf8";

export interface ConsoleJobSummaryData {
  job_id: string;
  namespace: string;
  state: JobStateName;
  durability: DurabilityName;
  interface_name: string;
  source: string | null;
  created_at_ms: number;
  updated_at_ms: number;
  duration_ms: number | null;
  current_attempt_id: string | null;
  latest_worker_id: string | null;
  attempt_count: number;
  last_error: string | null;
  error_code: string | null;
  error_retryable: boolean | null;
  metadata: Record<string, string>;
}

export interface ConsoleEventItem {
  sequence: number;
  attemptId: string | null;
  atMs: number;
  kind: "accepted" | "assigned" | "output" | "progress" | "attempt_restarted" | "finished";
  payloadSizeBytes: number;
  payloadHead?: Uint8Array;
  payloadMeta?: Record<string, string>;
  workerId?: string;
  previousAttemptId?: string;
  reasonMessage?: string;
  terminalState?: JobStateName;
  terminalErrorMessage?: string | null;
}

export interface ConsoleWorkerData {
  session_id: string;
  worker_id: string;
  namespace: string;
  pool: string;
  healthy: boolean;
  interfaces: string[];
  attributes: Record<string, string>;
  total_capacity: Record<string, number>;
  max_concurrency: number;
  active_leases: number;
  free_slots: number;
  utilization: number;
  load: {
    queued: number;
    running: number;
    utilization: number;
    custom: Record<string, number>;
  };
  connected: boolean;
  draining: boolean;
  connected_at_ms: number;
  last_seen_at_ms: number;
}

export interface ConsoleJobDetailResult {
  summary: ConsoleJobSummaryData;
  demand: StoredDemand | null;
  policy: StoredExecutionPolicy | null;
  paramsSizeBytes: number | null;
  paramsHead: Uint8Array | null;
  inputPreviews: ObjectPreviewData[];
  outputPreviews: ObjectPreviewData[];
  events: ConsoleEventItem[];
  latestWorker: ConsoleWorkerData | null;
}

export interface ConsoleJobsQuery {
  states: JobStateName[];
  interfaceName: string | null;
  namespace: string | null;
  workerId: string | null;
  search: string | null;
  fromMs: number | null;
  toMs: number | null;
  limit: number;
  offset: number;
  histogramBuckets: number;
}

export interface ConsoleHistogramBucket {
  start_ms: number;
  end_ms: number;
  active: number;
  succeeded: number;
  failed: number;
  cancelled: number;
}

export interface ConsoleHistogram {
  start_ms: number;
  end_ms: number;
  bucket_ms: number;
  buckets: ConsoleHistogramBucket[];
}

export interface ConsoleJobsQueryResult {
  jobs: ConsoleJobSummaryData[];
  total: number;
  next_page_token: string | null;
  histogram: ConsoleHistogram | null;
}

export interface ConsoleAnalyticsScan {
  archived: boolean;
  fromMs: number;
  toMs: number;
  limit: number;
  before: { createdAtMs: number; cursorId: string } | null;
  namespace: string | null;
}

export interface ConsoleAnalyticsResult {
  jobs: ConsoleJobSummaryData[];
  next_cursor: string | null;
}

export interface ConsoleOverviewSnapshot {
  live: boolean;
  ready: boolean;
  journalMessage: string | null;
  generation: number;
  bootId: string;
  constructorRuns: number;
  droppedCustomLoadEntries: number;
  retainedJobs: number;
  sessions: number;
  maxSessions: number;
  priorityLevels: number;
  defaultMaxAttempts: number;
  heartbeatIntervalMs: number;
  heartbeatTimeoutMs: number;
  reconnectGraceMs: number | null;
  inputBytes: number;
  maxPendingBytes: number;
  resultBytes: number;
  maxResultBytes: number;
  eventBytes: number;
  maxEventBytes: number;
  committedBatches: number;
  committedOps: number;
  queued: number;
  running: number;
  succeeded: number;
  failed: number;
  cancelled: number;
  workersRollup: {
    total: number;
    healthy: number;
    draining: number;
    unavailable: number;
    active_leases: number;
    total_capacity: Record<string, number>;
    last_seen_at_ms: number | null;
  };
  interfaces: Array<{
    name: string;
    queued: number;
    running: number;
    oldest_queued_age_ms: number;
    sessions: number;
    free_slots: number;
    policy: InterfacePolicyConfig | null;
  }>;
  pools: Array<{
    namespace: string;
    interface: string;
    pool: string;
    sessions: number;
    free_slots: number;
    active_leases: number;
    desired_additional_slots: number;
  }>;
  quotaDayUtc: string;
  quotaMonthUtc: string;
  quotaRowsWritten: number;
  quotaRowsRead: number;
  quotaDoRequests: number;
  quotaWsMessages: number;
  quotaR2ClassA: number;
  quotaR2ClassB: number;
  quotaLargeJobs: number;
  databaseSizeBytes: number;
  reservedRetainedBytes: number;
}

export interface AdminResetResult {
  generation: number;
  reset_at_ms: number;
  fenced_leases: number;
  requeued_jobs: number;
  failed_jobs: number;
  cancelled_jobs: number;
}

export interface ConsoleBackend {
  readonly auth: AuthPolicy;
  readonly env: NativeEnvConfig;
  getHealth(): { live: boolean; ready: boolean; message: string | null };
  getOverviewSnapshot(nowMs: number, namespace?: string): ConsoleOverviewSnapshot;
  queryJobsForConsole(query: ConsoleJobsQuery, nowMs: number): ConsoleJobsQueryResult;
  getJobDetailForConsole(
    namespace: string,
    rawJobId: string,
    nowMs: number,
  ): Promise<ConsoleJobDetailResult | null>;
  cancelJobForConsole(
    namespace: string,
    rawJobId: string,
    reason: string,
    nowMs: number,
  ): Promise<ConsoleJobSummaryData | null>;
  scanAnalyticsJobsForConsole(
    scan: ConsoleAnalyticsScan,
    nowMs: number,
  ): ConsoleAnalyticsResult;
  listWorkersForConsole(nowMs: number): ConsoleWorkerData[];
  adminReset(nowMs: number): Promise<AdminResetResult>;
}


function parseAllowedOrigins(env: NativeEnvConfig): Set<string> {
  const raw = env.ANYSERVE_ALLOWED_ORIGINS;
  if (!raw) {
    return new Set();
  }
  return new Set(
    raw
      .split(",")
      .map((s) => s.trim())
      .filter((s) => s.length > 0),
  );
}

export function withCorsHeaders(
  request: Request,
  response: Response,
  env: NativeEnvConfig,
): Response {
  const allowed = parseAllowedOrigins(env);
  const headers = new Headers(response.headers);
  headers.set("Vary", "Origin");
  headers.set("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  headers.set(
    "Access-Control-Allow-Headers",
    "Accept, Content-Type, Authorization, x-anyserve-namespace, traceparent, tracestate, baggage, x-request-id",
  );
  const reqOrigin = request.headers.get("Origin");
  if (reqOrigin && allowed.size > 0 && allowed.has(reqOrigin)) {
    headers.set("Access-Control-Allow-Origin", reqOrigin);
  }
  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers,
  });
}

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json; charset=utf-8" },
  });
}

function apiErrorResponse(status: number, message: string): Response {
  return jsonResponse({ error: { message } }, status);
}

function authMiddlewareErrorResponse(err: unknown): Response {
  if (err instanceof GrpcStatusError) {
    if (err.code === GrpcCode.PermissionDenied) {
      return jsonResponse({ error: err.message }, 403);
    }
    if (err.code === GrpcCode.InvalidArgument) {
      return apiErrorResponse(400, err.message);
    }
    return jsonResponse({ error: err.message }, 401);
  }
  return jsonResponse({ error: "unauthorized" }, 401);
}

export function resolvePublicEndpoint(request: Request, env: NativeEnvConfig): string {
  const configured = env.ANYSERVE_PUBLIC_ENDPOINT?.trim();
  if (configured) {
    return configured;
  }
  const host = new URL(request.url).host;
  return `wss://${host}`;
}

export function resolveRecommendedPollMs(env: NativeEnvConfig): number {
  const raw = env.ANYSERVE_CONSOLE_POLL_MS?.trim();
  if (raw === undefined || raw === "") {
    return 10_000;
  }
  const parsed = Number(raw);
  if (!Number.isFinite(parsed) || parsed < 0) {
    return 10_000;
  }
  return Math.trunc(parsed);
}

function normalizeOptionalFilter(raw: string | null): string | null {
  if (raw === null) {
    return null;
  }
  const trimmed = raw.trim();
  return trimmed.length > 0 ? trimmed : null;
}

function parseStatesParam(raw: string | null): JobStateName[] | string {
  if (!raw || raw.trim().length === 0) {
    return [];
  }
  const out: JobStateName[] = [];
  for (const part of raw.split(",")) {
    const trimmed = part.trim();
    if (!trimmed) {
      continue;
    }
    if (
      trimmed !== "queued" &&
      trimmed !== "running" &&
      trimmed !== "succeeded" &&
      trimmed !== "failed" &&
      trimmed !== "cancelled"
    ) {
      return `unsupported state filter '${trimmed}'`;
    }
    if (!out.includes(trimmed)) {
      out.push(trimmed);
    }
  }
  return out;
}

function parseOptionalUint(raw: string | null): number | null | "invalid" {
  if (raw === null || raw.trim() === "") {
    return null;
  }
  const n = Number(raw);
  if (!Number.isInteger(n) || n < 0) {
    return "invalid";
  }
  return n;
}

function clamp(n: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, n));
}

function metadataValue(
  metadata: Record<string, string>,
  key: string,
): string | null {
  const v = metadata[key];
  return v && v.trim().length > 0 ? v : null;
}

export function buildJobSummary(params: {
  jobId: string;
  namespace: string;
  state: JobStateName;
  durability: DurabilityName;
  interfaceName: string;
  createdAtMs: number;
  updatedAtMs: number;
  currentAttemptId: string | null;
  latestWorkerId: string | null;
  attemptCount: number;
  error: StoredErrorInfo | null;
  metadata: Record<string, string>;
}): ConsoleJobSummaryData {
  const isTerminal =
    params.state === "succeeded" ||
    params.state === "failed" ||
    params.state === "cancelled";
  return {
    job_id: params.jobId,
    namespace: params.namespace,
    state: params.state,
    durability: params.durability,
    interface_name: params.interfaceName,
    source: metadataValue(params.metadata, "source"),
    created_at_ms: params.createdAtMs,
    updated_at_ms: params.updatedAtMs,
    duration_ms: isTerminal
      ? Math.max(0, params.updatedAtMs - params.createdAtMs)
      : null,
    current_attempt_id: params.currentAttemptId,
    latest_worker_id: params.latestWorkerId,
    attempt_count: params.attemptCount,
    last_error: params.error ? params.error.message : null,
    error_code: params.error ? params.error.code : null,
    error_retryable: params.error ? params.error.retryable : null,
    metadata: params.metadata,
  };
}

function durationFromMs(ms: number | null): { secs: number; nanos: number } | null {
  if (ms === null || ms <= 0) {
    return null;
  }
  return {
    secs: Math.floor(ms / 1000),
    nanos: (ms % 1000) * 1_000_000,
  };
}

function escapePromLabel(value: string): string {
  return value
    .replace(/\\/g, "\\\\")
    .replace(/\n/g, "\\n")
    .replace(/"/g, '\\"');
}

export function renderPrometheusMetrics(snap: ConsoleOverviewSnapshot): string {
  const lines: string[] = [];
  lines.push(
    "# HELP anyserve_runtime_ready Whether the runtime accepts new work.",
    "# TYPE anyserve_runtime_ready gauge",
    `anyserve_runtime_ready ${snap.ready ? 1 : 0}`,
    "# HELP anyserve_jobs Jobs currently retained in memory, by state.",
    "# TYPE anyserve_jobs gauge",
    `anyserve_jobs{state="queued"} ${snap.queued}`,
    `anyserve_jobs{state="running"} ${snap.running}`,
    `anyserve_jobs{state="succeeded"} ${snap.succeeded}`,
    `anyserve_jobs{state="failed"} ${snap.failed}`,
    `anyserve_jobs{state="cancelled"} ${snap.cancelled}`,
    "# HELP anyserve_live_workers Connected, non-draining workers.",
    "# TYPE anyserve_live_workers gauge",
    `anyserve_live_workers ${snap.workersRollup.healthy}`,
    "# HELP anyserve_sessions Retained worker sessions, including reconnect grace.",
    "# TYPE anyserve_sessions gauge",
    `anyserve_sessions ${snap.sessions}`,
    "# HELP anyserve_active_leases Active worker leases, including reconnect grace.",
    "# TYPE anyserve_active_leases gauge",
    `anyserve_active_leases ${snap.workersRollup.active_leases}`,
    "# HELP anyserve_budget_bytes Bytes currently reserved in each shared Runtime budget.",
    "# TYPE anyserve_budget_bytes gauge",
    "# HELP anyserve_budget_limit_bytes Configured byte limit for each shared Runtime budget.",
    "# TYPE anyserve_budget_limit_bytes gauge",
    `anyserve_budget_bytes{budget="input"} ${snap.inputBytes}`,
    `anyserve_budget_limit_bytes{budget="input"} ${snap.maxPendingBytes}`,
    `anyserve_budget_bytes{budget="result"} ${snap.resultBytes}`,
    `anyserve_budget_limit_bytes{budget="result"} ${snap.maxResultBytes}`,
    `anyserve_budget_bytes{budget="event"} ${snap.eventBytes}`,
    `anyserve_budget_limit_bytes{budget="event"} ${snap.maxEventBytes}`,
    "# HELP anyserve_interface_jobs Queued or running jobs retained per interface.",
    "# TYPE anyserve_interface_jobs gauge",
    "# HELP anyserve_interface_oldest_queue_age_seconds Age in seconds of the oldest queued job per interface.",
    "# TYPE anyserve_interface_oldest_queue_age_seconds gauge",
    "# HELP anyserve_interface_sessions Retained sessions, including reconnect grace.",
    "# TYPE anyserve_interface_sessions gauge",
    "# HELP anyserve_interface_free_slots Available execution slots per interface after credits and concurrency limits.",
    "# TYPE anyserve_interface_free_slots gauge",
  );

  for (const iface of snap.interfaces) {
    const esc = escapePromLabel(iface.name);
    lines.push(
      `anyserve_interface_jobs{interface="${esc}",state="queued"} ${iface.queued}`,
      `anyserve_interface_jobs{interface="${esc}",state="running"} ${iface.running}`,
      `anyserve_interface_oldest_queue_age_seconds{interface="${esc}"} ${iface.oldest_queued_age_ms / 1000}`,
      `anyserve_interface_sessions{interface="${esc}"} ${iface.sessions}`,
      `anyserve_interface_free_slots{interface="${esc}"} ${iface.free_slots}`,
    );
  }

  if (snap.pools.length > 0) {
    lines.push(
      "# HELP anyserve_pool_sessions Retained worker sessions per namespace, interface, and pool.",
      "# TYPE anyserve_pool_sessions gauge",
      "# HELP anyserve_pool_free_slots Available execution slots per namespace, interface, and pool.",
      "# TYPE anyserve_pool_free_slots gauge",
      "# HELP anyserve_pool_active_leases Active execution leases per namespace, interface, and pool.",
      "# TYPE anyserve_pool_active_leases gauge",
      "# HELP anyserve_pool_desired_additional_slots Additional worker slots needed for queued demand per pool.",
      "# TYPE anyserve_pool_desired_additional_slots gauge",
    );
    for (const p of snap.pools) {
      const labels = `namespace="${escapePromLabel(p.namespace)}",interface="${escapePromLabel(p.interface)}",pool="${escapePromLabel(p.pool)}"`;
      lines.push(
        `anyserve_pool_sessions{${labels}} ${p.sessions}`,
        `anyserve_pool_free_slots{${labels}} ${p.free_slots}`,
        `anyserve_pool_active_leases{${labels}} ${p.active_leases}`,
        `anyserve_pool_desired_additional_slots{${labels}} ${p.desired_additional_slots}`,
      );
    }
  }

  lines.push(
    "# HELP anyserve_native_constructor_runs Number of Durable Object constructor runs in this isolate.",
    "# TYPE anyserve_native_constructor_runs counter",
    `anyserve_native_constructor_runs ${snap.constructorRuns}`,
    "# HELP anyserve_native_boot_info Current Durable Object boot identifier.",
    "# TYPE anyserve_native_boot_info gauge",
    `anyserve_native_boot_info{boot_id="${escapePromLabel(snap.bootId)}"} 1`,
    "# HELP anyserve_native_dropped_custom_load_entries_total Custom heartbeat load entries dropped above the 64-entry / 8 KiB cap.",
    "# TYPE anyserve_native_dropped_custom_load_entries_total counter",
    `anyserve_native_dropped_custom_load_entries_total ${snap.droppedCustomLoadEntries}`,
    "# HELP anyserve_native_quota_rows_written Daily metered SQLite rows written.",
    "# TYPE anyserve_native_quota_rows_written gauge",
    `anyserve_native_quota_rows_written ${snap.quotaRowsWritten}`,
    "# HELP anyserve_native_quota_rows_read Daily metered SQLite rows read.",
    "# TYPE anyserve_native_quota_rows_read gauge",
    `anyserve_native_quota_rows_read ${snap.quotaRowsRead}`,
    "# HELP anyserve_native_quota_do_requests Daily metered DO requests.",
    "# TYPE anyserve_native_quota_do_requests gauge",
    `anyserve_native_quota_do_requests ${snap.quotaDoRequests}`,
    "# HELP anyserve_native_quota_ws_messages Daily inbound WebSocket messages.",
    "# TYPE anyserve_native_quota_ws_messages gauge",
    `anyserve_native_quota_ws_messages ${snap.quotaWsMessages}`,
    "# HELP anyserve_native_quota_r2_class_a Monthly metered R2 Class A operations.",
    "# TYPE anyserve_native_quota_r2_class_a gauge",
    `anyserve_native_quota_r2_class_a ${snap.quotaR2ClassA}`,
    "# HELP anyserve_native_quota_r2_class_b Monthly metered R2 Class B operations.",
    "# TYPE anyserve_native_quota_r2_class_b gauge",
    `anyserve_native_quota_r2_class_b ${snap.quotaR2ClassB}`,
    "# HELP anyserve_native_quota_large_jobs Daily metered multi-chunk or R2-spilled jobs.",
    "# TYPE anyserve_native_quota_large_jobs gauge",
    `anyserve_native_quota_large_jobs ${snap.quotaLargeJobs}`,
    "# HELP anyserve_native_database_size_bytes Durable Object SQLite database size in bytes.",
    "# TYPE anyserve_native_database_size_bytes gauge",
    `anyserve_native_database_size_bytes ${snap.databaseSizeBytes}`,
    "# HELP anyserve_native_reserved_retained_bytes Reserved retained SQLite bytes for accepted work.",
    "# TYPE anyserve_native_reserved_retained_bytes gauge",
    `anyserve_native_reserved_retained_bytes ${snap.reservedRetainedBytes}`,
    "",
  );
  return lines.join("\n");
}

export async function handleConsoleOrHealthRequest(
  request: Request,
  backend: ConsoleBackend,
  nowMs: number,
): Promise<Response | null> {
  const url = new URL(request.url);
  const path = url.pathname;
  const method = request.method.toUpperCase();

  const isKnownConsolePath =
    path === "/healthz" ||
    path === "/readyz" ||
    path === "/metrics" ||
    path === "/api" ||
    path.startsWith("/api/");

  if (!isKnownConsolePath) {
    if (method === "OPTIONS") {
      return withCorsHeaders(request, new Response(null, { status: 204 }), backend.env);
    }
    return null;
  }

  if (method === "OPTIONS") {
    return withCorsHeaders(request, new Response(null, { status: 204 }), backend.env);
  }

  // /healthz and /readyz skip auth (mirrors crates/anyserve-cli/src/serve_auth.rs)
  if (path === "/healthz" && method === "GET") {
    const h = backend.getHealth();
    return withCorsHeaders(
      request,
      new Response(null, { status: h.live ? 204 : 503 }),
      backend.env,
    );
  }
  if (path === "/readyz" && method === "GET") {
    const h = backend.getHealth();
    return withCorsHeaders(
      request,
      new Response(null, { status: h.ready ? 204 : 503 }),
      backend.env,
    );
  }

  // /metrics and /api/admin/reset remain strictly Operator-only.
  if (path === "/metrics" || path === "/api/admin/reset") {
    try {
      backend.auth.authorizeOperatorHeaders(request.headers);
    } catch (err) {
      return withCorsHeaders(request, authMiddlewareErrorResponse(err), backend.env);
    }
    const res = await dispatchAuthorizedConsoleRoute(
      request,
      url,
      path,
      method,
      backend,
      null,
      nowMs,
    );
    return withCorsHeaders(request, res, backend.env);
  }

  // Remaining Console /api/* routes permit both operator and tenant client tokens.
  let principal: Principal;
  try {
    principal = backend.auth.authorizeConsoleHeaders(request.headers);
  } catch (err) {
    return withCorsHeaders(request, authMiddlewareErrorResponse(err), backend.env);
  }

  const res = await dispatchAuthorizedConsoleRoute(
    request,
    url,
    path,
    method,
    backend,
    principal,
    nowMs,
  );
  return withCorsHeaders(request, res, backend.env);
}

function enforceConsoleNamespace(
  principal: Principal,
  requested: string | null,
  allowAllKeyword: boolean,
): string | null {
  if (principal.isOperator) {
    return requested;
  }
  if (
    requested !== null &&
    !(allowAllKeyword && requested.toLowerCase() === "all") &&
    requested !== principal.namespace
  ) {
    throw new GrpcStatusError(
      GrpcCode.PermissionDenied,
      "token cannot access another namespace",
    );
  }
  return principal.namespace;
}

async function dispatchAuthorizedConsoleRoute(
  request: Request,
  url: URL,
  path: string,
  method: string,
  backend: ConsoleBackend,
  principal: Principal | null,
  nowMs: number,
): Promise<Response> {
  if (path === "/metrics" && method === "GET") {
    const snap = backend.getOverviewSnapshot(nowMs);
    return new Response(renderPrometheusMetrics(snap), {
      status: 200,
      headers: { "Content-Type": "text/plain; version=0.0.4" },
    });
  }

  if (path === "/api/overview" && method === "GET") {
    const isOperator = principal?.isOperator ?? true;
    const snap = backend.getOverviewSnapshot(
      nowMs,
      isOperator ? undefined : principal?.namespace,
    );
    const body: Record<string, unknown> = {
      runtime: {
        code: "memory_plus_cloudflare",
        label: "Memory + Cloudflare DO",
        journal: "cloudflare",
      },
      health: {
        live: snap.live,
        ready: snap.ready,
        journal: {
          status: snap.journalMessage ? "unavailable" : "healthy",
          message: snap.journalMessage,
        },
      },
      control_plane: {
        grpc_listen: "disabled",
        grpc_tls_enabled: false,
        console_listen: "cloudflare-native-do",
        public_endpoint: resolvePublicEndpoint(request, backend.env),
        auth_enabled: backend.auth.isEnabled(),
        generation: snap.generation,
        retained_jobs: snap.retainedJobs,
        sessions: snap.sessions,
        max_sessions: snap.maxSessions,
        priority_levels: snap.priorityLevels,
        default_max_attempts: snap.defaultMaxAttempts,
        heartbeat_interval_ms: snap.heartbeatIntervalMs,
        heartbeat_timeout_ms: snap.heartbeatTimeoutMs,
        reconnect_grace_ms: snap.reconnectGraceMs,
        recommended_poll_ms: resolveRecommendedPollMs(backend.env),
        boot_id: snap.bootId,
        constructor_runs: snap.constructorRuns,
      },
      budgets: isOperator
        ? {
            input_bytes: snap.inputBytes,
            max_pending_bytes: snap.maxPendingBytes,
            result_bytes: snap.resultBytes,
            max_result_bytes: snap.maxResultBytes,
            event_bytes: snap.eventBytes,
            max_event_bytes: snap.maxEventBytes,
          }
        : {
            input_bytes: 0,
            max_pending_bytes: 0,
            result_bytes: 0,
            max_result_bytes: 0,
            event_bytes: 0,
            max_event_bytes: 0,
          },
      journal_stats: isOperator
        ? {
            pending_ops: 0,
            committed_batches: snap.committedBatches,
            committed_ops: snap.committedOps,
            last_batch_size: 1,
            last_commit_micros: 0,
          }
        : null,
      queue: {
        queued: snap.queued,
        running: snap.running,
        active: snap.queued + snap.running,
      },
      history: {
        succeeded: snap.succeeded,
        failed: snap.failed,
        cancelled: snap.cancelled,
        total: snap.succeeded + snap.failed + snap.cancelled,
      },
      workers: snap.workersRollup,
      interfaces: snap.interfaces.map((iface) => ({
        name: iface.name,
        queued: iface.queued,
        running: iface.running,
        oldest_queued_age_ms: iface.oldest_queued_age_ms,
        sessions: iface.sessions,
        free_slots: iface.free_slots,
        policy: iface.policy
          ? {
              max_queue: iface.policy.maxQueue,
              queue_timeout_ms: iface.policy.queueTimeoutMs,
              max_in_flight_per_key: iface.policy.maxInFlightPerKey,
              pool_tiers: iface.policy.poolTiers.map((t) => ({
                pool: t.pool,
                spillover_after_ms: t.spilloverAfterMs,
                reconnect_grace_ms: t.reconnectGraceMs,
              })),
            }
          : null,
      })),
      pools: snap.pools,
      refreshed_at_ms: nowMs,
    };
    if (backend.auth.isEnabled() && principal) {
      body.scope = {
        is_operator: principal.isOperator,
        namespace: principal.namespace,
        subject: principal.subject,
      };
    }
    if (isOperator) {
      body.quota = {
        day_utc: snap.quotaDayUtc,
        month_utc: snap.quotaMonthUtc,
        do_requests: snap.quotaDoRequests,
        ws_messages: snap.quotaWsMessages,
        rows_written: snap.quotaRowsWritten,
        rows_read: snap.quotaRowsRead,
        r2_class_a: snap.quotaR2ClassA,
        r2_class_b: snap.quotaR2ClassB,
        large_jobs: snap.quotaLargeJobs,
        database_size_bytes: snap.databaseSizeBytes,
        reserved_retained_bytes: snap.reservedRetainedBytes,
      };
    }
    return jsonResponse(body);
  }

  if (path === "/api/workers" && method === "GET") {
    const allWorkers = backend.listWorkersForConsole(nowMs);
    const workers =
      principal && !principal.isOperator
        ? allWorkers.filter((w) => w.namespace === principal.namespace)
        : allWorkers;
    let healthy = 0;
    let draining = 0;
    let disconnected = 0;
    for (const w of workers) {
      if (w.healthy) healthy += 1;
      if (w.draining) draining += 1;
      if (!w.connected) disconnected += 1;
    }
    return jsonResponse({
      workers,
      total: workers.length,
      healthy,
      draining,
      disconnected,
    });
  }

  if (path === "/api/jobs" && method === "GET") {
    const statesOrErr = parseStatesParam(url.searchParams.get("states"));
    if (typeof statesOrErr === "string") {
      return apiErrorResponse(400, statesOrErr);
    }
    const search = normalizeOptionalFilter(url.searchParams.get("q"));
    if (search && utf8ByteLen(search) > 256) {
      return apiErrorResponse(400, "search query exceeds 256 bytes");
    }
    const rawFrom = parseOptionalUint(url.searchParams.get("from_ms"));
    const rawTo = parseOptionalUint(url.searchParams.get("to_ms"));
    const rawWindow = parseOptionalUint(url.searchParams.get("window_ms"));
    const rawBuckets = parseOptionalUint(url.searchParams.get("buckets"));
    const rawLimit = parseOptionalUint(url.searchParams.get("limit"));
    const rawOffset = parseOptionalUint(url.searchParams.get("offset"));
    if (
      rawFrom === "invalid" ||
      rawTo === "invalid" ||
      rawWindow === "invalid" ||
      rawBuckets === "invalid" ||
      rawLimit === "invalid" ||
      rawOffset === "invalid"
    ) {
      return apiErrorResponse(400, "invalid numeric query parameter");
    }
    if (rawWindow === 0) {
      return apiErrorResponse(400, "window_ms must be positive");
    }
    let fromMs: number | null = null;
    let toMs: number | null = null;
    if (rawFrom !== null || rawTo !== null) {
      fromMs = rawFrom;
      toMs = rawTo;
      if (fromMs !== null && toMs !== null && fromMs >= toMs) {
        return apiErrorResponse(400, "from_ms must be less than to_ms");
      }
    } else if (rawWindow !== null) {
      const buckets = clamp(rawBuckets ?? 32, 8, 96);
      const step = clamp(Math.floor(rawWindow / buckets), 1_000, 60_000);
      const end = (Math.floor(nowMs / step) + 1) * step;
      fromMs = Math.max(0, end - rawWindow);
      toMs = end;
    }

    const pageToken =
      normalizeOptionalFilter(url.searchParams.get("page_token")) ??
      (rawOffset !== null && rawOffset > 0 ? String(rawOffset) : null);
    let offset = 0;
    if (pageToken !== null) {
      const parsedOffset = Number(pageToken);
      if (!Number.isInteger(parsedOffset) || parsedOffset < 0) {
        return apiErrorResponse(400, "invalid page_token");
      }
      offset = parsedOffset;
    }

    const rawWorker = normalizeOptionalFilter(url.searchParams.get("worker"));
    const workerId =
      rawWorker && rawWorker.toLowerCase() !== "all" ? rawWorker : null;
    const requestedNs = normalizeOptionalFilter(url.searchParams.get("namespace"));
    if (requestedNs !== null && !validateNamespaceSafe(requestedNs)) {
      return apiErrorResponse(
        400,
        "namespace must be 1..=64 ASCII characters in [a-zA-Z0-9._-]",
      );
    }
    let nsFilter: string | null = requestedNs;
    if (principal) {
      try {
        nsFilter = enforceConsoleNamespace(principal, requestedNs, true);
      } catch (err) {
        return authMiddlewareErrorResponse(err);
      }
    }
    const rawIface = normalizeOptionalFilter(url.searchParams.get("interface"));
    const ifaceFilter =
      rawIface && rawIface.toLowerCase() !== "all" ? rawIface : null;
    if (ifaceFilter !== null && !isValidInterfaceName(ifaceFilter)) {
      return apiErrorResponse(
        400,
        `interface must be 1..=${MAX_INTERFACE_BYTES} bytes with no ASCII whitespace or control characters`,
      );
    }

    const result = backend.queryJobsForConsole(
      {
        states: statesOrErr,
        interfaceName: ifaceFilter,
        namespace: nsFilter,
        workerId,
        search,
        fromMs,
        toMs,
        limit: clamp(rawLimit ?? 50, 1, 200),
        offset,
        histogramBuckets: rawBuckets ?? 0,
      },
      nowMs,
    );

    const out: Record<string, unknown> = {
      jobs: result.jobs,
      total: result.total,
      next_page_token: result.next_page_token,
    };
    if (result.histogram !== null) {
      out.histogram = result.histogram;
    }
    return jsonResponse(out);
  }

  if (path === "/api/analytics/jobs" && method === "GET") {
    const source = url.searchParams.get("source");
    if (source !== "archive" && source !== "retained") {
      return apiErrorResponse(400, "source must be 'archive' or 'retained'");
    }
    const fromMs = parseOptionalUint(url.searchParams.get("from_ms"));
    const toMs = parseOptionalUint(url.searchParams.get("to_ms"));
    const rawLimit = parseOptionalUint(url.searchParams.get("limit"));
    if (
      fromMs === null ||
      fromMs === "invalid" ||
      toMs === null ||
      toMs === "invalid" ||
      rawLimit === "invalid"
    ) {
      return apiErrorResponse(400, "from_ms and to_ms are required");
    }
    if (fromMs >= toMs || toMs - fromMs > 31 * 24 * 60 * 60 * 1000) {
      return apiErrorResponse(400, "choose a time range of at most 31 days");
    }
    const requestedNs = normalizeOptionalFilter(url.searchParams.get("namespace"));
    if (requestedNs !== null && !validateNamespaceSafe(requestedNs)) {
      return apiErrorResponse(
        400,
        "namespace must be 1..=64 ASCII characters in [a-zA-Z0-9._-]",
      );
    }
    let nsFilter: string | null = requestedNs;
    if (principal) {
      try {
        nsFilter = enforceConsoleNamespace(principal, requestedNs, false);
      } catch (err) {
        return authMiddlewareErrorResponse(err);
      }
    }
    const cursorRaw = normalizeOptionalFilter(url.searchParams.get("cursor"));
    let before: { createdAtMs: number; cursorId: string } | null = null;
    if (cursorRaw !== null) {
      if (cursorRaw.length > 2048) {
        return apiErrorResponse(400, "invalid metadata cursor");
      }
      let parsed: unknown;
      try {
        parsed = JSON.parse(cursorRaw);
      } catch {
        return apiErrorResponse(400, "invalid metadata cursor");
      }
      if (
        !Array.isArray(parsed) ||
        parsed.length !== 2 ||
        typeof parsed[0] !== "number" ||
        !Number.isInteger(parsed[0]) ||
        typeof parsed[1] !== "string"
      ) {
        return apiErrorResponse(400, "invalid metadata cursor");
      }
      const maxCursorIdLen =
        nsFilter !== null ? MAX_IDENTIFIER_BYTES : MAX_IDENTIFIER_BYTES + 70;
      if (
        parsed[0] < fromMs ||
        parsed[0] >= toMs ||
        parsed[1].length === 0 ||
        parsed[1].length > maxCursorIdLen
      ) {
        return apiErrorResponse(400, "metadata cursor is outside the time range");
      }
      before = { createdAtMs: parsed[0], cursorId: parsed[1] };
    }

    const page = backend.scanAnalyticsJobsForConsole(
      {
        archived: source === "archive",
        fromMs,
        toMs,
        limit: clamp(rawLimit ?? 250, 1, 250),
        before,
        namespace: nsFilter,
      },
      nowMs,
    );
    return jsonResponse(page);
  }

  if (path === "/api/admin/reset" && method === "POST") {
    const res = await backend.adminReset(nowMs);
    return jsonResponse(res);
  }

  // /api/jobs/:job_id and /api/jobs/:job_id/cancel
  const cancelMatch = /^\/api\/jobs\/([^/]+)\/cancel$/.exec(path);
  if (cancelMatch && method === "POST") {
    let rawJobId: string;
    try {
      rawJobId = decodeURIComponent(cancelMatch[1]!);
    } catch {
      return apiErrorResponse(400, "invalid job_id encoding");
    }
    const requestedNs = normalizeOptionalFilter(url.searchParams.get("namespace"));
    if (requestedNs !== null && !validateNamespaceSafe(requestedNs)) {
      return apiErrorResponse(
        400,
        "namespace must be 1..=64 ASCII characters in [a-zA-Z0-9._-]",
      );
    }
    let ns = requestedNs ?? DEFAULT_NAMESPACE;
    if (principal) {
      try {
        ns = enforceConsoleNamespace(principal, requestedNs, false) ?? DEFAULT_NAMESPACE;
      } catch (err) {
        return authMiddlewareErrorResponse(err);
      }
    }
    const job = await backend.cancelJobForConsole(
      ns,
      rawJobId,
      "cancelled from console",
      nowMs,
    );
    if (!job) {
      return apiErrorResponse(404, `job '${rawJobId}' does not exist`);
    }
    return jsonResponse({ job });
  }

  const detailMatch = /^\/api\/jobs\/([^/]+)$/.exec(path);
  if (detailMatch && method === "GET") {
    let rawJobId: string;
    try {
      rawJobId = decodeURIComponent(detailMatch[1]!);
    } catch {
      return apiErrorResponse(400, "invalid job_id encoding");
    }
    const requestedNs = normalizeOptionalFilter(url.searchParams.get("namespace"));
    if (requestedNs !== null && !validateNamespaceSafe(requestedNs)) {
      return apiErrorResponse(
        400,
        "namespace must be 1..=64 ASCII characters in [a-zA-Z0-9._-]",
      );
    }
    let ns = requestedNs ?? DEFAULT_NAMESPACE;
    if (principal) {
      try {
        ns = enforceConsoleNamespace(principal, requestedNs, false) ?? DEFAULT_NAMESPACE;
      } catch (err) {
        return authMiddlewareErrorResponse(err);
      }
    }
    const detail = await backend.getJobDetailForConsole(ns, rawJobId, nowMs);
    if (!detail) {
      return apiErrorResponse(404, `job '${rawJobId}' does not exist`);
    }
    return jsonResponse(formatJobDetailResponse(detail));
  }

  return apiErrorResponse(404, `not found: ${method} ${path}`);
}

function formatObjectPreviewSummary(p: ObjectPreviewData) {
  if (p.kind === "uri") {
    return {
      kind: "uri" as const,
      size_bytes: null,
      uri: p.uri,
      content_type: metadataValue(p.metadata, "content_type"),
      preview: null,
      truncated: false,
      metadata: p.metadata,
    };
  }
  const size = p.sizeBytes ?? 0;
  return {
    kind: "inline" as const,
    size_bytes: size,
    uri: null,
    content_type: metadataValue(p.metadata, "content_type"),
    preview: formatInlinePreview(p.headBytes, PREVIEW_MAX_BYTES),
    truncated: size > PREVIEW_MAX_BYTES,
    metadata: p.metadata,
  };
}

function formatJobDetailResponse(detail: ConsoleJobDetailResult) {
  // Reconstruct attempts from events (mirrors serve_console/snapshot.rs::attempts_from_events)
  const attempts: Array<{
    attempt_id: string;
    state: "assigned" | "restarted" | "succeeded" | "failed" | "cancelled";
    worker_id: string;
    created_at_ms: number;
    finished_at_ms: number | null;
    duration_ms: number | null;
    last_error: string | null;
  }> = [];

  for (const ev of detail.events) {
    if (ev.kind === "assigned" && ev.attemptId) {
      attempts.push({
        attempt_id: ev.attemptId,
        state: "assigned",
        worker_id: ev.workerId ?? "",
        created_at_ms: ev.atMs,
        finished_at_ms: null,
        duration_ms: null,
        last_error: null,
      });
    } else if (ev.kind === "attempt_restarted" && ev.previousAttemptId) {
      for (let i = attempts.length - 1; i >= 0; i--) {
        if (attempts[i]!.attempt_id === ev.previousAttemptId) {
          attempts[i]!.state = "restarted";
          attempts[i]!.finished_at_ms = ev.atMs;
          attempts[i]!.duration_ms = Math.max(0, ev.atMs - attempts[i]!.created_at_ms);
          attempts[i]!.last_error = ev.reasonMessage ?? null;
          break;
        }
      }
    } else if (ev.kind === "finished" && ev.attemptId && ev.terminalState) {
      for (let i = attempts.length - 1; i >= 0; i--) {
        if (attempts[i]!.attempt_id === ev.attemptId) {
          const st = ev.terminalState;
          if (st === "succeeded" || st === "failed" || st === "cancelled") {
            attempts[i]!.state = st;
          }
          attempts[i]!.finished_at_ms = ev.atMs;
          attempts[i]!.duration_ms = Math.max(0, ev.atMs - attempts[i]!.created_at_ms);
          attempts[i]!.last_error = ev.terminalErrorMessage ?? null;
          break;
        }
      }
    }
  }

  const historyIncomplete =
    attempts.length < detail.summary.attempt_count ||
    (detail.events.length > 0 && detail.events[0]!.kind !== "accepted");

  // Compute combined output_preview (mirrors snapshot.rs::output_preview)
  let outSize = 0;
  const outChunks: Uint8Array[] = [];
  let outChunksBytes = 0;
  let outContentType: string | null = null;
  let hasFrames = false;

  const appendPreviewPiece = (
    head: Uint8Array | null | undefined,
    meta: Record<string, string>,
    origSize: number,
  ) => {
    outSize += origSize;
    if (outContentType === null) {
      outContentType = metadataValue(meta, "content_type");
    }
    const remaining = PREVIEW_MAX_BYTES - outChunksBytes;
    if (remaining > 0 && head && head.byteLength > 0) {
      const slice = head.subarray(0, Math.min(head.byteLength, remaining));
      outChunks.push(slice);
      outChunksBytes += slice.byteLength;
    }
  };

  for (const ev of detail.events) {
    if (ev.kind === "output") {
      hasFrames = true;
      appendPreviewPiece(ev.payloadHead, ev.payloadMeta ?? {}, ev.payloadSizeBytes);
    }
  }
  if (!hasFrames) {
    for (const op of detail.outputPreviews) {
      if (op.kind === "inline") {
        appendPreviewPiece(
          op.headBytes,
          op.metadata,
          op.sizeBytes ?? (op.headBytes?.byteLength ?? 0),
        );
      }
    }
  }

  let outputPreview: {
    size_bytes: number;
    content_type: string | null;
    preview: string | null;
    truncated: boolean;
  } | null = null;
  if (outSize > 0) {
    const merged = new Uint8Array(outChunksBytes);
    let pos = 0;
    for (const c of outChunks) {
      merged.set(c, pos);
      pos += c.byteLength;
    }
    outputPreview = {
      size_bytes: outSize,
      content_type: outContentType,
      preview: formatInlinePreview(merged, PREVIEW_MAX_BYTES),
      truncated: outSize > PREVIEW_MAX_BYTES,
    };
  }

  const demandJson = detail.demand
    ? {
        interface: detail.demand.interface,
        required_attributes: detail.demand.required_attributes,
        preferred_attributes: detail.demand.preferred_attributes,
        required_capacity: Object.fromEntries(
          Object.entries(detail.demand.required_capacity).map(([k, v]) => [
            k,
            Number(v),
          ]),
        ),
        affinity_key: detail.demand.affinity_key,
      }
    : null;

  const policyJson = detail.policy
    ? {
        priority: detail.policy.priority,
        max_attempts: detail.policy.max_attempts,
        durability:
          detail.policy.durability === "durable"
            ? ("Durable" as const)
            : detail.policy.durability === "ephemeral"
              ? ("Ephemeral" as const)
              : null,
        queue_timeout: durationFromMs(detail.policy.queue_timeout_ms),
        execution_timeout: durationFromMs(detail.policy.execution_timeout_ms),
        total_timeout: durationFromMs(detail.policy.total_timeout_ms),
        spillover_after: durationFromMs(detail.policy.spillover_after_ms),
        fairness_key: detail.policy.fairness_key,
        concurrency_key: detail.policy.concurrency_key,
      }
    : null;

  return {
    job: {
      ...detail.summary,
      demand: demandJson,
      policy: policyJson,
      params_size_bytes: detail.paramsSizeBytes,
      params_preview: formatInlinePreview(detail.paramsHead, PREVIEW_MAX_BYTES),
      params_truncated:
        detail.paramsSizeBytes !== null && detail.paramsSizeBytes > PREVIEW_MAX_BYTES,
      output_preview: outputPreview,
      inputs: detail.inputPreviews.map(formatObjectPreviewSummary),
      outputs: detail.outputPreviews.map(formatObjectPreviewSummary),
    },
    attempts,
    events: detail.events.map((ev) => ({
      sequence: ev.sequence,
      kind: ev.kind,
      created_at_ms: ev.atMs,
      payload_size_bytes: ev.payloadSizeBytes,
      ...(ev.attemptId ? { attempt_id: ev.attemptId } : {}),
    })),
    history_incomplete: historyIncomplete,
    latest_worker: detail.latestWorker,
  };
}

export function formatAnalyticsNextCursor(
  lastJob: ConsoleJobSummaryData | undefined,
  hasMore: boolean,
  namespaceFilter: string | null,
): string | null {
  if (!hasMore || !lastJob) {
    return null;
  }
  const cursorId =
    namespaceFilter !== null
      ? lastJob.job_id
      : encodeScopedId(lastJob.namespace, lastJob.job_id);
  return JSON.stringify([lastJob.created_at_ms, cursorId]);
}
