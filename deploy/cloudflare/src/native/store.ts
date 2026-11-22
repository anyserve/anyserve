// Native DO SQLite + R2 storage layer (sections 4, 6.4, 8, 9 of docs/cloudflare-native-do-plan.md).
// Zero cloudflare:workers or node:* imports; never calls JournalStorage.

import {
  BUNDLE_INLINE_MAX_BYTES,
  BUNDLE_MANIFEST_MAX_BYTES,
  DEFAULT_R2_RECONCILE_SAFETY_AGE_MS,
  DEFAULT_R2_STAGING_TTL_MS,
  PREVIEW_MAX_BYTES,
  QUOTA_HARD_DO_REQUESTS_PER_DAY,
  QUOTA_HARD_ROWS_READ_PER_DAY,
  QUOTA_HARD_ROW_WRITES_PER_DAY,
  QUOTA_MAX_DB_PLUS_RESERVED_BYTES,
  QUOTA_SOFT_DO_REQUESTS_PER_DAY,
  QUOTA_SOFT_LARGE_JOBS_PER_DAY,
  QUOTA_SOFT_R2_CLASS_A_PER_MONTH,
  QUOTA_SOFT_R2_CLASS_B_PER_MONTH,
  QUOTA_SOFT_ROWS_READ_PER_DAY,
  QUOTA_SOFT_ROW_WRITES_PER_DAY,
  R2_PACK_MAX_BYTES,
  type NativeDurableStorage,
  type NativeR2Bucket,
  type NativeRuntimeOverrides,
} from "./host";

const utf8Encoder = new TextEncoder();
const utf8Decoder = new TextDecoder();

export type JobStateName =
  | "queued"
  | "running"
  | "succeeded"
  | "failed"
  | "cancelled";

export type DurabilityName = "durable" | "ephemeral";

export interface StoredErrorInfo {
  code: string;
  message: string;
  retryable: boolean;
}

export interface StoredObject {
  kind: "inline" | "uri";
  data?: Uint8Array;
  uri?: string;
  metadata: Record<string, string>;
}

export interface StoredDemand {
  interface: string;
  required_attributes: Record<string, string>;
  preferred_attributes: Record<string, string>;
  required_capacity: Record<string, bigint>;
  affinity_key: string;
}

export interface StoredExecutionPolicy {
  durability: DurabilityName | null;
  priority: number;
  queue_timeout_ms: number | null;
  execution_timeout_ms: number | null;
  total_timeout_ms: number | null;
  spillover_after_ms: number | null;
  max_attempts: number | null;
  fairness_key: string;
  concurrency_key: string;
}

export interface StoredSpecMetadata {
  demand: StoredDemand;
  policy: StoredExecutionPolicy;
  metadata: Record<string, string>;
  params_len: number;
  input_count: number;
}

export type SegmentSliceRef =
  | { k: string; o: number; l: number } // R2 blob/pack slice
  | { o: number; l: number }; // Inline blob slice

export type BundleManifestEntry =
  | {
      t: "i"; // inline slice in this row's pack_blob
      o: number;
      l: number;
      m?: Record<string, string>;
    }
  | {
      t: "u"; // opaque URI object
      u: string;
      m?: Record<string, string>;
    }
  | {
      t: "r2"; // slice in an immutable R2 pack or single-object R2 blob
      k: string;
      o: number;
      l: number;
      m?: Record<string, string>;
    }
  | {
      t: "segs"; // multi-segment uploaded object (R2 segments + optional final inline slice)
      l: number;
      s: SegmentSliceRef[];
      m?: Record<string, string>;
    }
  | {
      t: "p"; // params inline in pack_blob
      o: number;
      l: number;
    }
  | {
      t: "pr2"; // params in R2 pack/blob
      k: string;
      o: number;
      l: number;
    };

export interface CompletedAttemptObjectRef {
  kind: "inline" | "uri" | "segments";
  metadata: Record<string, string>;
  uri?: string;
  inlineOffset?: number;
  byteLen: number;
  segments?: Array<{ r2Key: string | null; segIndex: number; byteLen: number }>;
}

export interface ObjectPreviewData {
  kind: "inline" | "uri";
  sizeBytes: number | null;
  uri: string | null;
  headBytes: Uint8Array | null;
  metadata: Record<string, string>;
}

export interface SplitU64 {
  hi: number;
  lo: number;
}

export function splitU64(value: bigint): SplitU64 {
  const clamped = value < 0n ? 0n : value;
  return {
    hi: Number((clamped >> 32n) & 0xffffffffn),
    lo: Number(clamped & 0xffffffffn),
  };
}

export function joinU64(hi: number | null | undefined, lo: number | null | undefined): bigint {
  if (hi === null || hi === undefined || lo === null || lo === undefined) {
    return 0n;
  }
  return ((BigInt(hi >>> 0) << 32n) | BigInt(lo >>> 0));
}

export function normalizeBlob(val: unknown): Uint8Array {
  if (val instanceof Uint8Array) {
    return val;
  }
  if (val instanceof ArrayBuffer) {
    return new Uint8Array(val);
  }
  if (ArrayBuffer.isView(val)) {
    return new Uint8Array(val.buffer, val.byteOffset, val.byteLength);
  }
  if (typeof val === "string") {
    return utf8Encoder.encode(val);
  }
  return new Uint8Array(0);
}

interface RustDurationJson {
  secs: number;
  nanos: number;
}

function durationToRustJson(ms: number | null): RustDurationJson | null {
  if (ms === null || ms <= 0) {
    return null;
  }
  const secs = Math.floor(ms / 1000);
  const nanos = (ms % 1000) * 1_000_000;
  return { secs, nanos };
}

function parseDurationField(val: unknown): number | null {
  if (val === null || val === undefined) {
    return null;
  }
  if (typeof val === "number" && Number.isFinite(val) && val > 0) {
    return Math.trunc(val);
  }
  if (typeof val === "object") {
    const d = val as Partial<RustDurationJson>;
    if (typeof d.secs === "number") {
      const ms = d.secs * 1000 + Math.floor((d.nanos ?? 0) / 1_000_000);
      return ms > 0 ? ms : null;
    }
  }
  return null;
}

/**
 * Serializes StoredSpecMetadata in a format compatible with Rust `StoredSpec` (`spec_json`),
 * and parses both old Rust `spec_json` and native `spec_json` losslessly (including >2^53 capacities).
 */
export function serializeSpecJson(spec: StoredSpecMetadata): string {
  const capObj: Record<string, number | string> = {};
  for (const [k, v] of Object.entries(spec.demand.required_capacity)) {
    if (v >= BigInt(Number.MIN_SAFE_INTEGER) && v <= BigInt(Number.MAX_SAFE_INTEGER)) {
      capObj[k] = Number(v);
    } else {
      capObj[k] = v.toString();
    }
  }
  const doc = {
    demand: {
      interface: spec.demand.interface,
      required_attributes: spec.demand.required_attributes,
      preferred_attributes: spec.demand.preferred_attributes,
      required_capacity: capObj,
      affinity_key: spec.demand.affinity_key,
    },
    policy: {
      durability:
        spec.policy.durability === "durable"
          ? "Durable"
          : spec.policy.durability === "ephemeral"
            ? "Ephemeral"
            : null,
      priority: spec.policy.priority,
      queue_timeout: durationToRustJson(spec.policy.queue_timeout_ms),
      execution_timeout: durationToRustJson(spec.policy.execution_timeout_ms),
      total_timeout: durationToRustJson(spec.policy.total_timeout_ms),
      spillover_after: durationToRustJson(spec.policy.spillover_after_ms),
      max_attempts: spec.policy.max_attempts,
      fairness_key: spec.policy.fairness_key,
      concurrency_key: spec.policy.concurrency_key,
    },
    metadata: spec.metadata,
    params_len: spec.params_len,
    input_count: spec.input_count,
  };
  return JSON.stringify(doc);
}

export function parseSpecJson(raw: string): StoredSpecMetadata {
  const parsed = JSON.parse(raw) as Record<string, any>;
  const demandRaw = parsed.demand ?? {};
  const policyRaw = parsed.policy ?? {};
  const cap: Record<string, bigint> = {};
  if (demandRaw.required_capacity && typeof demandRaw.required_capacity === "object") {
    for (const [k, v] of Object.entries(demandRaw.required_capacity)) {
      if (typeof v === "bigint") {
        cap[k] = v;
      } else if (typeof v === "number" && Number.isFinite(v)) {
        cap[k] = BigInt(Math.trunc(v));
      } else if (typeof v === "string" && v.trim().length > 0) {
        cap[k] = BigInt(v.trim());
      }
    }
  }
  let durability: DurabilityName | null = null;
  if (typeof policyRaw.durability === "string") {
    const low = policyRaw.durability.toLowerCase();
    if (low === "durable") {
      durability = "durable";
    } else if (low === "ephemeral") {
      durability = "ephemeral";
    }
  }
  return {
    demand: {
      interface: String(demandRaw.interface ?? ""),
      required_attributes: (demandRaw.required_attributes as Record<string, string>) ?? {},
      preferred_attributes: (demandRaw.preferred_attributes as Record<string, string>) ?? {},
      required_capacity: cap,
      affinity_key: String(demandRaw.affinity_key ?? ""),
    },
    policy: {
      durability,
      priority: typeof policyRaw.priority === "number" ? policyRaw.priority : 0,
      queue_timeout_ms:
        parseDurationField(policyRaw.queue_timeout_ms) ??
        parseDurationField(policyRaw.queue_timeout),
      execution_timeout_ms:
        parseDurationField(policyRaw.execution_timeout_ms) ??
        parseDurationField(policyRaw.execution_timeout),
      total_timeout_ms:
        parseDurationField(policyRaw.total_timeout_ms) ??
        parseDurationField(policyRaw.total_timeout),
      spillover_after_ms:
        parseDurationField(policyRaw.spillover_after_ms) ??
        parseDurationField(policyRaw.spillover_after),
      max_attempts:
        typeof policyRaw.max_attempts === "number" && policyRaw.max_attempts > 0
          ? policyRaw.max_attempts
          : null,
      fairness_key: String(policyRaw.fairness_key ?? ""),
      concurrency_key: String(policyRaw.concurrency_key ?? ""),
    },
    metadata: (parsed.metadata as Record<string, string>) ?? {},
    params_len: typeof parsed.params_len === "number" ? parsed.params_len : 0,
    input_count: typeof parsed.input_count === "number" ? parsed.input_count : 0,
  };
}

export function serializeCapacityJson(cap: Record<string, bigint>): string {
  const entries = Object.entries(cap);
  if (entries.length === 0) {
    return "{}";
  }
  const out: Record<string, string> = {};
  for (const [k, v] of entries) {
    out[k] = v.toString();
  }
  return JSON.stringify(out);
}

export function parseCapacityJson(raw: string | null | undefined): Record<string, bigint> {
  if (!raw || raw === "{}") {
    return {};
  }
  const parsed = JSON.parse(raw) as Record<string, unknown>;
  const out: Record<string, bigint> = {};
  for (const [k, v] of Object.entries(parsed)) {
    if (typeof v === "string") {
      out[k] = BigInt(v);
    } else if (typeof v === "number") {
      out[k] = BigInt(Math.trunc(v));
    }
  }
  return out;
}

const SCHEMA_STATEMENTS: readonly string[] = [
  `CREATE TABLE IF NOT EXISTS native_runtime_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
  ) WITHOUT ROWID`,
  `CREATE TABLE IF NOT EXISTS native_quota_meter (
    bucket_key TEXT PRIMARY KEY,
    day_utc TEXT NOT NULL,
    month_utc TEXT NOT NULL,
    do_requests INTEGER NOT NULL,
    ws_messages INTEGER NOT NULL,
    rows_written INTEGER NOT NULL,
    rows_read INTEGER NOT NULL,
    r2_class_a INTEGER NOT NULL,
    r2_class_b INTEGER NOT NULL,
    large_jobs INTEGER NOT NULL
  ) WITHOUT ROWID`,
  `CREATE TABLE IF NOT EXISTS native_sessions (
    session_id TEXT PRIMARY KEY,
    worker_id TEXT NOT NULL,
    namespace TEXT NOT NULL,
    pool TEXT NOT NULL,
    forced_pool TEXT,
    interfaces_json TEXT NOT NULL,
    attributes_json TEXT NOT NULL,
    metadata_json TEXT NOT NULL,
    total_capacity_json TEXT NOT NULL,
    reserved_capacity_json TEXT NOT NULL,
    active_leases_json TEXT NOT NULL,
    max_concurrency INTEGER NOT NULL,
    reported_free INTEGER NOT NULL,
    assigned_since_report INTEGER NOT NULL,
    draining INTEGER NOT NULL,
    connected INTEGER NOT NULL,
    current_conn_id INTEGER NOT NULL,
    connected_at_ms INTEGER NOT NULL,
    last_seen_ms INTEGER NOT NULL,
    grace_deadline_ms INTEGER,
    consecutive_failures INTEGER NOT NULL,
    cooldown_until_ms INTEGER NOT NULL,
    queued INTEGER NOT NULL DEFAULT 0,
    running INTEGER NOT NULL DEFAULT 0,
    utilization REAL NOT NULL DEFAULT 0
  ) WITHOUT ROWID`,
  `CREATE TABLE IF NOT EXISTS native_jobs (
    scoped_job_id TEXT PRIMARY KEY,
    raw_job_id TEXT NOT NULL,
    namespace TEXT NOT NULL,
    interface_name TEXT NOT NULL,
    state TEXT NOT NULL,
    durability TEXT NOT NULL,
    spec_hash TEXT NOT NULL,
    spec_json TEXT NOT NULL,
    priority INTEGER NOT NULL,
    fairness_key TEXT NOT NULL,
    concurrency_key TEXT NOT NULL,
    affinity_key TEXT NOT NULL,
    max_attempts INTEGER NOT NULL,
    attempt_count INTEGER NOT NULL,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    queued_since_ms INTEGER NOT NULL,
    not_before_ms INTEGER NOT NULL,
    queue_deadline_ms INTEGER,
    total_deadline_ms INTEGER,
    spillover_at_ms INTEGER,
    caller_conn_id INTEGER,
    streaming_input INTEGER NOT NULL,
    input_ended INTEGER NOT NULL,
    stream_chunk_count INTEGER NOT NULL DEFAULT 0,
    total_input_bytes INTEGER NOT NULL DEFAULT 0,
    retained_bytes_est INTEGER NOT NULL DEFAULT 0,
    cancel_requested TEXT,
    error_json TEXT,
    winning_lease_id TEXT,
    result_digest TEXT,
    last_attempt_id TEXT,
    last_worker_id TEXT,
    failed_workers_json TEXT NOT NULL,
    rejected_sessions_json TEXT NOT NULL,
    last_seq_hi INTEGER NOT NULL,
    last_seq_lo INTEGER NOT NULL,
    evicted_seq_hi INTEGER NOT NULL,
    evicted_seq_lo INTEGER NOT NULL,
    primary_seq_hi INTEGER,
    primary_seq_lo INTEGER
  ) WITHOUT ROWID`,
  `CREATE INDEX IF NOT EXISTS idx_native_jobs_state
    ON native_jobs(namespace, state, updated_at_ms DESC, scoped_job_id ASC)`,
  `CREATE INDEX IF NOT EXISTS idx_native_jobs_created
    ON native_jobs(namespace, created_at_ms DESC, scoped_job_id DESC)`,
  `CREATE INDEX IF NOT EXISTS idx_native_jobs_winner
    ON native_jobs(winning_lease_id) WHERE winning_lease_id IS NOT NULL`,
  `CREATE TABLE IF NOT EXISTS native_active_leases (
    lease_id TEXT PRIMARY KEY,
    scoped_job_id TEXT NOT NULL,
    raw_job_id TEXT NOT NULL,
    namespace TEXT NOT NULL,
    attempt_id TEXT NOT NULL,
    attempt_number INTEGER NOT NULL,
    session_id TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    generation_text TEXT NOT NULL,
    reserved_capacity_json TEXT NOT NULL,
    started_at_ms INTEGER NOT NULL,
    execution_deadline_ms INTEGER,
    inputs_via_attempt INTEGER NOT NULL,
    accepted INTEGER NOT NULL,
    cancelling INTEGER NOT NULL,
    cancel_deadline_ms INTEGER,
    attempt_conn_id INTEGER,
    last_frame_seq_hi INTEGER NOT NULL,
    last_frame_seq_lo INTEGER NOT NULL,
    exposed INTEGER NOT NULL,
    concurrency_counted INTEGER NOT NULL,
    input_stage INTEGER NOT NULL,
    input_item_idx INTEGER NOT NULL,
    input_byte_off INTEGER NOT NULL,
    input_seq_hi INTEGER NOT NULL,
    input_seq_lo INTEGER NOT NULL,
    cur_obj_index INTEGER,
    cur_obj_meta_json TEXT,
    cur_obj_uri TEXT,
    cur_obj_bytes INTEGER NOT NULL DEFAULT 0,
    next_obj_index INTEGER NOT NULL DEFAULT 0,
    tail_bytes INTEGER NOT NULL DEFAULT 0,
    tail_next_chunk_seq INTEGER NOT NULL DEFAULT 0,
    next_seg_index INTEGER NOT NULL DEFAULT 0,
    total_result_bytes INTEGER NOT NULL DEFAULT 0,
    acked_result_bytes INTEGER NOT NULL DEFAULT 0,
    result_too_large INTEGER NOT NULL DEFAULT 0,
    completed_objs_json TEXT NOT NULL DEFAULT '[]',
    inline_result_blob BLOB
  ) WITHOUT ROWID`,
  `CREATE TABLE IF NOT EXISTS native_object_bundles (
    scoped_job_id TEXT NOT NULL,
    role TEXT NOT NULL,
    seq INTEGER NOT NULL,
    manifest TEXT NOT NULL,
    pack_blob BLOB,
    PRIMARY KEY (scoped_job_id, role, seq)
  ) WITHOUT ROWID`,
  `CREATE TABLE IF NOT EXISTS native_upload_tail (
    lease_id TEXT NOT NULL,
    attempt_conn_id INTEGER NOT NULL,
    obj_index INTEGER NOT NULL,
    chunk_seq INTEGER NOT NULL,
    data BLOB NOT NULL,
    PRIMARY KEY (lease_id, attempt_conn_id, obj_index, chunk_seq)
  ) WITHOUT ROWID`,
  `CREATE TABLE IF NOT EXISTS native_upload_segments (
    lease_id TEXT NOT NULL,
    attempt_conn_id INTEGER NOT NULL,
    obj_index INTEGER NOT NULL,
    seg_index INTEGER NOT NULL,
    byte_len INTEGER NOT NULL,
    r2_key TEXT,
    blob BLOB,
    PRIMARY KEY (lease_id, attempt_conn_id, obj_index, seg_index)
  ) WITHOUT ROWID`,
  `CREATE TABLE IF NOT EXISTS native_job_events (
    scoped_job_id TEXT NOT NULL,
    seq_hi INTEGER NOT NULL,
    seq_lo INTEGER NOT NULL,
    attempt_id TEXT,
    at_ms INTEGER NOT NULL,
    kind TEXT NOT NULL,
    meta_json TEXT,
    payload BLOB,
    size_bytes INTEGER NOT NULL,
    PRIMARY KEY (scoped_job_id, seq_hi, seq_lo)
  ) WITHOUT ROWID`,
  `CREATE TABLE IF NOT EXISTS native_watchers (
    watch_conn_id INTEGER PRIMARY KEY,
    scoped_job_id TEXT NOT NULL,
    raw_job_id TEXT NOT NULL,
    namespace TEXT NOT NULL,
    after_seq_hi INTEGER NOT NULL,
    after_seq_lo INTEGER NOT NULL,
    delivered_seq_hi INTEGER NOT NULL,
    delivered_seq_lo INTEGER NOT NULL,
    finished INTEGER NOT NULL
  )`,
  `CREATE TABLE IF NOT EXISTS native_receipts (
    lease_id TEXT PRIMARY KEY,
    scoped_job_id TEXT NOT NULL,
    namespace TEXT NOT NULL,
    outcome_digest TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL
  ) WITHOUT ROWID`,
  `CREATE TABLE IF NOT EXISTS native_r2_gc (
    upload_id TEXT PRIMARY KEY,
    r2_key TEXT NOT NULL,
    state TEXT NOT NULL,
    expires_at_ms INTEGER NOT NULL,
    retry_after_ms INTEGER NOT NULL,
    created_at_ms INTEGER NOT NULL
  ) WITHOUT ROWID`,
];

export interface QuotaSnapshot {
  dayUtc: string;
  monthUtc: string;
  doRequests: number;
  wsMessages: number;
  rowsWritten: number;
  rowsRead: number;
  r2ClassA: number;
  r2ClassB: number;
  largeJobs: number;
  databaseSizeBytes: number;
  reservedRetainedBytes: number;
}

const METER_ROW_WRITE_BLOCK = 500;
const METER_REQUEST_BLOCK = 1_000;

function utcDayAndMonth(nowMs: number): { dayUtc: string; monthUtc: string } {
  const iso = new Date(nowMs).toISOString(); // YYYY-MM-DDTHH:mm:ss.sssZ
  return {
    dayUtc: iso.slice(0, 10),
    monthUtc: iso.slice(0, 7),
  };
}

export interface StagedR2Upload {
  uploadId: string;
  r2Key: string;
  byteLen: number;
}

export interface PreparedBundleRow {
  seq: number;
  manifest: string;
  packBlob: Uint8Array | null;
}

export interface StagedBundlePlan {
  rows: PreparedBundleRow[];
  stagedUploads: StagedR2Upload[];
}

/**
 * Storage and quota manager for AnyserveNativeDO.
 * Tracks SQLite rowsRead/rowsWritten, R2 Class A/B operations, databaseSize + reserved bytes,
 * packed object bundles, upload tails/segments, and CAS R2 garbage collection.
 */
export class NativeStore {
  readonly doId: string;
  readonly storage: NativeDurableStorage;
  readonly r2: NativeR2Bucket | null;
  readonly overrides: NativeRuntimeOverrides;

  // Live quota counters (loaded from native_quota_meter on init, flushed in blocks inside write txns)
  private dayUtc = "";
  private monthUtc = "";
  private doRequests = 0;
  private wsMessages = 0;
  private wsMsgRemainder = 0;
  private rowsWritten = 0;
  private rowsRead = 0;
  private r2ClassA = 0;
  private r2ClassB = 0;
  private largeJobs = 0;

  private flushedRowsWritten = 0;
  private flushedDoRequests = 0;
  private inTransactionDepth = 0;
  private flushingMeter = false;
  private pendingGcHint = false;

  // Exact cumulative counters since this instance booted (useful for per-step row accounting tests!)
  bootRowsWritten = 0;
  bootRowsRead = 0;
  bootAlarmWrites = 0;
  bootR2ClassA = 0;
  bootR2ClassB = 0;

  reservedRetainedBytes = 0;

  constructor(
    doId: string,
    storage: NativeDurableStorage,
    r2: NativeR2Bucket | null,
    overrides?: NativeRuntimeOverrides,
  ) {
    this.doId = doId;
    this.storage = storage;
    this.r2 = r2;
    this.overrides = overrides ?? {};
  }

  r2BlobPrefix(): string {
    return `native/v1/${this.doId}/blobs/`;
  }

  newR2BlobKey(): string {
    return `${this.r2BlobPrefix()}${crypto.randomUUID()}`;
  }

  get databaseSizeBytes(): number {
    return this.storage.sql.databaseSize ?? 0;
  }

  /**
   * Checks whether schema already exists using a pure SELECT first so cold-wake
   * rehydration performs ZERO SQLite writes! Runs DDL only on brand-new DO DB.
   */
  initOrLoadMeta(nowMs: number): {
    generation: bigint;
    connSeq: number;
    queueSeq: bigint;
    freshInit: boolean;
  } {
    const { dayUtc, monthUtc } = utcDayAndMonth(nowMs);
    this.dayUtc = dayUtc;
    this.monthUtc = monthUtc;

    let existingMeta: Array<{ key: string; value: string }> | null = null;
    try {
      existingMeta = this.exec<{ key: string; value: string }>(
        `SELECT key, value FROM native_runtime_meta`,
      );
    } catch {
      existingMeta = null;
    }

    let freshInit = false;
    if (!existingMeta || existingMeta.length === 0) {
      freshInit = true;
      this.transactionSync(() => {
        for (const ddl of SCHEMA_STATEMENTS) {
          this.exec(ddl);
        }
        this.exec(
          `INSERT OR IGNORE INTO native_runtime_meta (key, value) VALUES ('generation', '1')`,
        );
        this.exec(
          `INSERT OR IGNORE INTO native_runtime_meta (key, value) VALUES ('conn_seq', '0')`,
        );
        this.exec(
          `INSERT OR IGNORE INTO native_runtime_meta (key, value) VALUES ('queue_seq', '0')`,
        );
      });
      existingMeta = this.exec<{ key: string; value: string }>(
        `SELECT key, value FROM native_runtime_meta`,
      );
    }

    let generation = 1n;
    let connSeq = 0;
    let queueSeq = 0n;
    for (const row of existingMeta) {
      if (row.key === "generation") {
        generation = BigInt(row.value);
      } else if (row.key === "conn_seq") {
        connSeq = Number(row.value);
      } else if (row.key === "queue_seq") {
        queueSeq = BigInt(row.value);
      }
    }

    const meterRow = this.exec<{
      day_utc: string;
      month_utc: string;
      do_requests: number;
      ws_messages: number;
      rows_written: number;
      rows_read: number;
      r2_class_a: number;
      r2_class_b: number;
      large_jobs: number;
    }>(`SELECT * FROM native_quota_meter WHERE bucket_key = 'current'`)[0];

    if (meterRow) {
      if (meterRow.day_utc === dayUtc) {
        this.doRequests = meterRow.do_requests;
        this.wsMessages = meterRow.ws_messages;
        this.rowsWritten = meterRow.rows_written;
        this.rowsRead = meterRow.rows_read;
        this.largeJobs = meterRow.large_jobs;
      }
      if (meterRow.month_utc === monthUtc) {
        this.r2ClassA = meterRow.r2_class_a;
        this.r2ClassB = meterRow.r2_class_b;
      }
      this.flushedRowsWritten = this.rowsWritten;
      this.flushedDoRequests = this.doRequests;
    }

    if (!freshInit && this.r2) {
      this.pendingGcHint = true;
    }

    return { generation, connSeq, queueSeq, freshInit };
  }

  rolloverQuotaClock(nowMs: number): void {
    const { dayUtc, monthUtc } = utcDayAndMonth(nowMs);
    if (dayUtc !== this.dayUtc) {
      this.dayUtc = dayUtc;
      this.doRequests = 0;
      this.wsMessages = 0;
      this.wsMsgRemainder = 0;
      this.rowsWritten = 0;
      this.rowsRead = 0;
      this.largeJobs = 0;
      this.flushedRowsWritten = 0;
      this.flushedDoRequests = 0;
    }
    if (monthUtc !== this.monthUtc) {
      this.monthUtc = monthUtc;
      this.r2ClassA = 0;
      this.r2ClassB = 0;
    }
  }

  recordHttpOrAlarmRequest(nowMs: number): void {
    this.rolloverQuotaClock(nowMs);
    this.doRequests += 1;
  }

  /**
   * Incoming WebSocket messages count 20:1 against DO requests (section 6.1 / 9.1).
   * Pure in-memory counter: NEVER writes to SQLite on its own!
   */
  recordWsInboundMessage(nowMs: number): void {
    this.rolloverQuotaClock(nowMs);
    this.wsMessages += 1;
    this.wsMsgRemainder += 1;
    if (this.wsMsgRemainder >= 20) {
      this.wsMsgRemainder -= 20;
      this.doRequests += 1;
    }
  }

  recordAlarmSet(nowMs: number): void {
    this.rolloverQuotaClock(nowMs);
    this.rowsWritten += 1;
    this.bootRowsWritten += 1;
    this.bootAlarmWrites += 1;
  }

  recordR2ClassA(nowMs: number, count = 1): void {
    this.rolloverQuotaClock(nowMs);
    this.r2ClassA += count;
    this.bootR2ClassA += count;
  }

  recordR2ClassB(nowMs: number, count = 1): void {
    this.rolloverQuotaClock(nowMs);
    this.r2ClassB += count;
    this.bootR2ClassB += count;
  }

  recordLargeJob(nowMs: number): void {
    this.rolloverQuotaClock(nowMs);
    this.largeJobs += 1;
  }

  quotaSnapshot(nowMs: number): QuotaSnapshot {
    this.rolloverQuotaClock(nowMs);
    return {
      dayUtc: this.dayUtc,
      monthUtc: this.monthUtc,
      doRequests: this.doRequests,
      wsMessages: this.wsMessages,
      rowsWritten: this.rowsWritten,
      rowsRead: this.rowsRead,
      r2ClassA: this.r2ClassA,
      r2ClassB: this.r2ClassB,
      largeJobs: this.largeJobs,
      databaseSizeBytes: this.databaseSizeBytes,
      reservedRetainedBytes: this.reservedRetainedBytes,
    };
  }

  /**
   * Checks admission limits for accepting NEW work (`Submit` / `Invoke`).
   * Leaves 10,000 row writes/day and 10,000 DO requests/day headroom so already-accepted
   * jobs can finish, retry, cancel, run alarms, and purge cleanly!
   */
  checkNewJobAdmission(
    nowMs: number,
    estimatedRetainedBytes: number,
    usesMultiChunkOrR2: boolean,
  ): string | null {
    this.rolloverQuotaClock(nowMs);
    const softRowWrites =
      this.overrides.quotaSoftRowWritesPerDay ?? QUOTA_SOFT_ROW_WRITES_PER_DAY;
    if (this.rowsWritten >= softRowWrites) {
      return `daily SQLite row-write admission cap reached (${this.rowsWritten}/${softRowWrites}); shedding new load to preserve headroom for accepted jobs`;
    }
    const softRequests =
      this.overrides.quotaSoftDoRequestsPerDay ?? QUOTA_SOFT_DO_REQUESTS_PER_DAY;
    if (this.doRequests >= softRequests) {
      return `daily DO request admission cap reached (${this.doRequests}/${softRequests})`;
    }
    const softReads =
      this.overrides.quotaSoftRowsReadPerDay ?? QUOTA_SOFT_ROWS_READ_PER_DAY;
    if (this.rowsRead >= softReads) {
      return `daily SQLite row-read admission cap reached (${this.rowsRead}/${softReads})`;
    }
    const maxDbPlusReserved =
      this.overrides.quotaMaxDbPlusReservedBytes ?? QUOTA_MAX_DB_PLUS_RESERVED_BYTES;
    const projectedBytes =
      this.databaseSizeBytes + this.reservedRetainedBytes + estimatedRetainedBytes;
    if (projectedBytes > maxDbPlusReserved) {
      return `DO SQLite storage + reserved retained budget exceeded (${projectedBytes} > ${maxDbPlusReserved} bytes)`;
    }
    if (usesMultiChunkOrR2) {
      const softLarge =
        this.overrides.quotaSoftLargeJobsPerDay ?? QUOTA_SOFT_LARGE_JOBS_PER_DAY;
      if (this.largeJobs >= softLarge) {
        return `daily large/multi-chunk job admission cap reached (${this.largeJobs}/${softLarge})`;
      }
      const softClassA =
        this.overrides.quotaSoftR2ClassAPerMonth ?? QUOTA_SOFT_R2_CLASS_A_PER_MONTH;
      if (this.r2ClassA >= softClassA) {
        return `monthly R2 Class A admission cap reached (${this.r2ClassA}/${softClassA})`;
      }
    }
    const softClassB =
      this.overrides.quotaSoftR2ClassBPerMonth ?? QUOTA_SOFT_R2_CLASS_B_PER_MONTH;
    if (this.r2ClassB >= softClassB) {
      return `monthly R2 Class B admission cap reached (${this.r2ClassB}/${softClassB})`;
    }
    return null;
  }

  /**
   * Read-path admission check (Watch / Get / List) against hard daily ceilings.
   */
  checkReadAdmission(nowMs: number): string | null {
    this.rolloverQuotaClock(nowMs);
    const hardRequests =
      this.overrides.quotaHardDoRequestsPerDay ?? QUOTA_HARD_DO_REQUESTS_PER_DAY;
    if (this.doRequests >= hardRequests) {
      return `daily DO request ceiling reached (${this.doRequests}/${hardRequests})`;
    }
    const hardReads =
      this.overrides.quotaHardRowsReadPerDay ?? QUOTA_HARD_ROWS_READ_PER_DAY;
    if (this.rowsRead >= hardReads) {
      return `daily SQLite row-read ceiling reached (${this.rowsRead}/${hardReads})`;
    }
    return null;
  }

  checkHardWriteCeiling(nowMs: number): string | null {
    this.rolloverQuotaClock(nowMs);
    const hardWrites =
      this.overrides.quotaHardRowWritesPerDay ?? QUOTA_HARD_ROW_WRITES_PER_DAY;
    if (this.rowsWritten >= hardWrites) {
      return `daily SQLite hard row-write ceiling reached (${this.rowsWritten}/${hardWrites})`;
    }
    return null;
  }

  exec<T extends Record<string, any> = Record<string, any>>(
    query: string,
    ...bindings: unknown[]
  ): T[] {
    const cursor = this.storage.sql.exec<T>(query, ...bindings);
    const arr = cursor.toArray();
    const rr = typeof cursor.rowsRead === "number" ? cursor.rowsRead : arr.length;
    const rw = typeof cursor.rowsWritten === "number" ? cursor.rowsWritten : 0;
    this.rowsRead += rr;
    this.rowsWritten += rw;
    this.bootRowsRead += rr;
    this.bootRowsWritten += rw;
    return arr;
  }

  /**
   * Executes a synchronous SQLite transaction and piggybacks a batched `native_quota_meter`
   * flush ONLY when the transaction already wrote rows AND an in-memory credit block crossed!
   */
  transactionSync<T>(closure: () => T): T {
    if (this.inTransactionDepth > 0) {
      return closure();
    }
    this.inTransactionDepth += 1;
    const writesBefore = this.rowsWritten;
    try {
      return this.storage.transactionSync(() => {
        const res = closure();
        const wroteInTxn = this.rowsWritten > writesBefore;
        if (
          wroteInTxn &&
          !this.flushingMeter &&
          (this.rowsWritten - this.flushedRowsWritten >= METER_ROW_WRITE_BLOCK ||
            this.doRequests - this.flushedDoRequests >= METER_REQUEST_BLOCK)
        ) {
          this.flushQuotaMeterInTxn();
        }
        return res;
      });
    } finally {
      this.inTransactionDepth -= 1;
    }
  }

  flushQuotaMeterInTxn(): void {
    this.flushingMeter = true;
    try {
      // Pre-reserve +1 write for the meter row itself so persisted value matches.
      const projectedWrites = this.rowsWritten + 1;
      this.exec(
        `INSERT INTO native_quota_meter (
          bucket_key, day_utc, month_utc, do_requests, ws_messages,
          rows_written, rows_read, r2_class_a, r2_class_b, large_jobs
        ) VALUES ('current', ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(bucket_key) DO UPDATE SET
          day_utc = excluded.day_utc,
          month_utc = excluded.month_utc,
          do_requests = excluded.do_requests,
          ws_messages = excluded.ws_messages,
          rows_written = excluded.rows_written,
          rows_read = excluded.rows_read,
          r2_class_a = excluded.r2_class_a,
          r2_class_b = excluded.r2_class_b,
          large_jobs = excluded.large_jobs`,
        this.dayUtc,
        this.monthUtc,
        this.doRequests,
        this.wsMessages,
        projectedWrites,
        this.rowsRead,
        this.r2ClassA,
        this.r2ClassB,
        this.largeJobs,
      );
      this.flushedRowsWritten = this.rowsWritten;
      this.flushedDoRequests = this.doRequests;
    } finally {
      this.flushingMeter = false;
    }
  }

  setMeta(key: string, value: string): void {
    this.exec(
      `INSERT INTO native_runtime_meta (key, value) VALUES (?, ?)
       ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
      key,
      value,
    );
  }

  // ---------------------------------------------------------------------------
  // R2 Staging & CAS Garbage Collection (Section 4.5)
  // ---------------------------------------------------------------------------

  async stageR2Blob(bytes: Uint8Array, nowMs: number): Promise<StagedR2Upload> {
    if (!this.r2) {
      throw new Error("R2 bucket binding ANYSERVE_R2 is unavailable");
    }
    const uploadId = crypto.randomUUID();
    const r2Key = this.newR2BlobKey();
    const ttlMs = this.overrides.r2StagingTtlMs ?? DEFAULT_R2_STAGING_TTL_MS;
    const expiresAtMs = nowMs + ttlMs;

    this.transactionSync(() => {
      this.exec(
        `INSERT INTO native_r2_gc (
          upload_id, r2_key, state, expires_at_ms, retry_after_ms, created_at_ms
        ) VALUES (?, ?, 'staging', ?, 0, ?)`,
        uploadId,
        r2Key,
        expiresAtMs,
        nowMs,
      );
      this.pendingGcHint = true;
    });

    try {
      this.recordR2ClassA(nowMs, 1);
      await this.r2.put(r2Key, bytes);
    } catch (err) {
      // Leave in GC as pending_delete so any partial/late arrival is cleaned up.
      this.transactionSync(() => {
        this.exec(
          `UPDATE native_r2_gc SET state = 'pending_delete', retry_after_ms = ?
           WHERE upload_id = ? AND state = 'staging'`,
          nowMs,
          uploadId,
        );
      });
      throw err;
    }

    return { uploadId, r2Key, byteLen: bytes.byteLength };
  }

  /**
   * Inside a commit `transactionSync`, claims all staged uploads via CAS:
   * `DELETE FROM native_r2_gc WHERE upload_id = ? AND state = 'staging' RETURNING upload_id`.
   * Returns false if ANY upload was already claimed by the GC sweeper!
   */
  claimStagedUploadsInTxn(uploads: readonly StagedR2Upload[]): boolean {
    for (const u of uploads) {
      const claimed = this.exec<{ upload_id: string }>(
        `DELETE FROM native_r2_gc WHERE upload_id = ? AND state = 'staging' RETURNING upload_id`,
        u.uploadId,
      );
      if (claimed.length === 0) {
        return false;
      }
    }
    return true;
  }

  /**
   * Compensating cleanup when a commit transaction aborts or loses the staging race.
   */
  async compensateUnpublishedUploads(
    uploads: readonly StagedR2Upload[],
    nowMs: number,
  ): Promise<void> {
    if (!this.r2 || uploads.length === 0) {
      return;
    }
    for (const u of uploads) {
      // Remove staging row if still present, or leave deleting row to sweeper; either way delete R2 key.
      this.transactionSync(() => {
        this.exec(
          `DELETE FROM native_r2_gc WHERE upload_id = ? AND state = 'staging'`,
          u.uploadId,
        );
      });
      try {
        await this.r2.delete(u.r2Key);
      } catch {
        this.transactionSync(() => {
          this.queuePendingDeleteInTxn([u.r2Key], nowMs);
        });
      }
    }
  }

  queuePendingDeleteInTxn(r2Keys: readonly string[], nowMs: number): void {
    for (const key of r2Keys) {
      if (!key) {
        continue;
      }
      this.exec(
        `INSERT INTO native_r2_gc (
          upload_id, r2_key, state, expires_at_ms, retry_after_ms, created_at_ms
        ) VALUES (?, ?, 'pending_delete', ?, ?, ?)`,
        crypto.randomUUID(),
        key,
        nowMs,
        nowMs,
        nowMs,
      );
      this.pendingGcHint = true;
    }
  }

  /**
   * Returns the earliest due timestamp across `native_r2_gc` when GC entries exist,
   * performing ZERO SQL reads in steady state when `pendingGcHint` is false!
   */
  nextR2GcDueMs(): number | null {
    if (!this.pendingGcHint || !this.r2) {
      return null;
    }
    const row = this.exec<{ due_ms: number | null }>(
      `SELECT MIN(CASE WHEN state = 'staging' THEN expires_at_ms ELSE retry_after_ms END) AS due_ms FROM native_r2_gc`,
    )[0];
    const due = row?.due_ms ?? null;
    if (due === null) {
      this.pendingGcHint = false;
    }
    return due;
  }

  /**
   * Sweeps expired `staging` uploads (via conditional state='staging' -> 'deleting' CAS)
   * and `pending_delete` rows, retrying failed deletes.
   */
  async sweepR2Gc(nowMs: number, batchLimit = 64): Promise<number> {
    if (!this.r2) {
      return 0;
    }
    const candidates = this.exec<{
      upload_id: string;
      r2_key: string;
      state: string;
    }>(
      `SELECT upload_id, r2_key, state FROM native_r2_gc
       WHERE (state = 'staging' AND expires_at_ms <= ?)
          OR (state IN ('pending_delete', 'deleting') AND retry_after_ms <= ?)
       LIMIT ?`,
      nowMs,
      nowMs,
      batchLimit,
    );
    if (candidates.length === 0) {
      return 0;
    }

    const claimed: Array<{ uploadId: string; r2Key: string }> = [];
    this.transactionSync(() => {
      for (const c of candidates) {
        if (c.state === "staging") {
          const rows = this.exec<{ upload_id: string; r2_key: string }>(
            `UPDATE native_r2_gc
             SET state = 'deleting', retry_after_ms = ?
             WHERE upload_id = ? AND state = 'staging' AND expires_at_ms <= ?
             RETURNING upload_id, r2_key`,
            nowMs + 30_000,
            c.upload_id,
            nowMs,
          );
          if (rows[0]) {
            claimed.push({ uploadId: rows[0].upload_id, r2Key: rows[0].r2_key });
          }
        } else {
          const rows = this.exec<{ upload_id: string; r2_key: string }>(
            `UPDATE native_r2_gc
             SET state = 'deleting', retry_after_ms = ?
             WHERE upload_id = ? AND state IN ('pending_delete', 'deleting') AND retry_after_ms <= ?
             RETURNING upload_id, r2_key`,
            nowMs + 30_000,
            c.upload_id,
            nowMs,
          );
          if (rows[0]) {
            claimed.push({ uploadId: rows[0].upload_id, r2Key: rows[0].r2_key });
          }
        }
      }
    });

    let deleted = 0;
    for (const item of claimed) {
      try {
        await this.r2.delete(item.r2Key);
        this.transactionSync(() => {
          this.exec(`DELETE FROM native_r2_gc WHERE upload_id = ?`, item.uploadId);
        });
        deleted += 1;
      } catch {
        this.transactionSync(() => {
          this.exec(
            `UPDATE native_r2_gc
             SET state = 'pending_delete', retry_after_ms = ?
             WHERE upload_id = ?`,
            nowMs + 15_000,
            item.uploadId,
          );
        });
      }
    }
    return deleted;
  }

  collectAllReferencedR2KeysSync(): Set<string> {
    const keys = new Set<string>();
    const bundleRows = this.exec<{ manifest: string }>(
      `SELECT manifest FROM native_object_bundles`,
    );
    for (const row of bundleRows) {
      for (const k of extractR2KeysFromManifest(row.manifest)) {
        keys.add(k);
      }
    }
    const segRows = this.exec<{ r2_key: string | null }>(
      `SELECT r2_key FROM native_upload_segments WHERE r2_key IS NOT NULL`,
    );
    for (const row of segRows) {
      if (row.r2_key) {
        keys.add(row.r2_key);
      }
    }
    const gcRows = this.exec<{ r2_key: string; state: string }>(
      `SELECT r2_key, state FROM native_r2_gc WHERE state = 'staging'`,
    );
    for (const row of gcRows) {
      keys.add(row.r2_key);
    }
    return keys;
  }

  /**
   * Paged prefix reconciliation for `native/v1/<do_id>/blobs/` (section 4.5).
   * Never touches another deployment's prefix; deletes only unreachable keys older than safety age.
   */
  async reconcileR2Prefix(
    nowMs: number,
    options?: { cursor?: string; pageLimit?: number; maxPages?: number },
  ): Promise<{ deletedKeys: string[]; nextCursor?: string }> {
    if (!this.r2 || typeof this.r2.list !== "function") {
      return { deletedKeys: [] };
    }
    const prefix = this.r2BlobPrefix();
    const safetyAgeMs =
      this.overrides.r2ReconcileSafetyAgeMs ?? DEFAULT_R2_RECONCILE_SAFETY_AGE_MS;
    const pageLimit = options?.pageLimit ?? 100;
    const maxPages = options?.maxPages ?? 4;
    let cursor = options?.cursor;
    const deletedKeys: string[] = [];

    for (let page = 0; page < maxPages; page++) {
      this.recordR2ClassA(nowMs, 1);
      const listed = await this.r2.list({ prefix, cursor, limit: pageLimit });
      // Snapshot reachable set AFTER listing so any concurrently staged/committed key is protected!
      const reachable = this.collectAllReferencedR2KeysSync();

      const toDelete: string[] = [];
      for (const obj of listed.objects) {
        if (!obj.key.startsWith(prefix)) {
          // Defensive check: NEVER touch keys outside our DO prefix!
          continue;
        }
        if (reachable.has(obj.key)) {
          continue;
        }
        const uploadedMs = obj.uploaded instanceof Date ? obj.uploaded.getTime() : 0;
        if (nowMs - uploadedMs < safetyAgeMs) {
          continue;
        }
        toDelete.push(obj.key);
      }

      if (toDelete.length > 0) {
        await this.r2.delete(toDelete);
        deletedKeys.push(...toDelete);
      }

      if (!listed.truncated || !listed.cursor) {
        cursor = undefined;
        break;
      }
      cursor = listed.cursor;
    }

    return { deletedKeys, nextCursor: cursor };
  }

  // ---------------------------------------------------------------------------
  // Packed Object Bundles (Section 4.3)
  // ---------------------------------------------------------------------------

  /**
   * Packs static inputs + optional params (or completed outputs) into bounded bundle rows.
   * - <= 512 KiB total (`manifest + pack_blob`): ONE row, zero R2 PUTs!
   * - Overflow (e.g. 48 x 256 KiB = 12 MiB): packs consecutive <= 512 KiB items into
   *   immutable R2 packs (<= 4 MiB each, referenced as `{ t: "r2", k, o, l }`),
   *   single objects > 512 KiB get their own R2 blob, and manifests > 256 KiB split across `seq` rows.
   */
  async prepareObjectBundle(
    objects: readonly StoredObject[],
    params: Uint8Array | null,
    nowMs: number,
  ): Promise<StagedBundlePlan> {
    let totalInlineBytes = params ? params.byteLength : 0;
    for (const obj of objects) {
      if (obj.kind === "inline" && obj.data) {
        totalInlineBytes += obj.data.byteLength;
      }
    }

    // Fast path: everything fits inline in ONE 512 KiB bundle row!
    if (totalInlineBytes <= BUNDLE_INLINE_MAX_BYTES) {
      const packBlob = new Uint8Array(totalInlineBytes);
      let offset = 0;
      const entries: BundleManifestEntry[] = [];
      for (const obj of objects) {
        const meta =
          Object.keys(obj.metadata).length > 0 ? obj.metadata : undefined;
        if (obj.kind === "uri") {
          entries.push({ t: "u", u: obj.uri ?? "", ...(meta ? { m: meta } : {}) });
        } else {
          const data = obj.data ?? new Uint8Array(0);
          if (data.byteLength > 0) {
            packBlob.set(data, offset);
          }
          entries.push({
            t: "i",
            o: offset,
            l: data.byteLength,
            ...(meta ? { m: meta } : {}),
          });
          offset += data.byteLength;
        }
      }
      if (params && params.byteLength > 0) {
        packBlob.set(params, offset);
        entries.push({ t: "p", o: offset, l: params.byteLength });
        offset += params.byteLength;
      }
      const manifest = JSON.stringify(entries);
      const manifestByteLen = utf8Encoder.encode(manifest).byteLength;
      if (
        manifestByteLen <= BUNDLE_MANIFEST_MAX_BYTES &&
        manifestByteLen + packBlob.byteLength <= BUNDLE_INLINE_MAX_BYTES
      ) {
        return {
          rows: [
            {
              seq: 0,
              manifest,
              packBlob: packBlob.byteLength > 0 ? packBlob : null,
            },
          ],
          stagedUploads: [],
        };
      }
    }

    // Overflow path: multi-pack (R2 packs <= 4 MiB when R2 is bound, else multi-row <= 512 KiB SQLite packs)
    const stagedUploads: StagedR2Upload[] = [];
    try {
      if (this.r2) {
        const allEntries: BundleManifestEntry[] = [];
        let pendingPackParts: Array<{
          entryIndex: number;
          isParams: boolean;
          data: Uint8Array;
          meta?: Record<string, string>;
        }> = [];
        let pendingPackBytes = 0;

        const flushPendingPackToR2 = async () => {
          if (pendingPackParts.length === 0) {
            return;
          }
          const packBuf = new Uint8Array(pendingPackBytes);
          let off = 0;
          for (const part of pendingPackParts) {
            packBuf.set(part.data, off);
            off += part.data.byteLength;
          }
          const uploaded = await this.stageR2Blob(packBuf, nowMs);
          stagedUploads.push(uploaded);
          let entryOff = 0;
          for (const part of pendingPackParts) {
            if (part.isParams) {
              allEntries[part.entryIndex] = {
                t: "pr2",
                k: uploaded.r2Key,
                o: entryOff,
                l: part.data.byteLength,
              };
            } else {
              allEntries[part.entryIndex] = {
                t: "r2",
                k: uploaded.r2Key,
                o: entryOff,
                l: part.data.byteLength,
                ...(part.meta ? { m: part.meta } : {}),
              };
            }
            entryOff += part.data.byteLength;
          }
          pendingPackParts = [];
          pendingPackBytes = 0;
        };

        for (const obj of objects) {
          const meta =
            Object.keys(obj.metadata).length > 0 ? obj.metadata : undefined;
          if (obj.kind === "uri") {
            allEntries.push({
              t: "u",
              u: obj.uri ?? "",
              ...(meta ? { m: meta } : {}),
            });
            continue;
          }
          const data = obj.data ?? new Uint8Array(0);
          if (data.byteLength === 0) {
            allEntries.push({
              t: "i",
              o: 0,
              l: 0,
              ...(meta ? { m: meta } : {}),
            });
            continue;
          }
          if (data.byteLength > BUNDLE_INLINE_MAX_BYTES) {
            // Single object > 512 KiB gets its own R2 blob; flush open pack first to preserve R2 pack grouping.
            await flushPendingPackToR2();
            const uploaded = await this.stageR2Blob(data, nowMs);
            stagedUploads.push(uploaded);
            allEntries.push({
              t: "r2",
              k: uploaded.r2Key,
              o: 0,
              l: data.byteLength,
              ...(meta ? { m: meta } : {}),
            });
            continue;
          }
          if (pendingPackBytes + data.byteLength > R2_PACK_MAX_BYTES) {
            await flushPendingPackToR2();
          }
          const placeholderIdx = allEntries.length;
          allEntries.push({ t: "i", o: 0, l: 0 });
          pendingPackParts.push({
            entryIndex: placeholderIdx,
            isParams: false,
            data,
            meta,
          });
          pendingPackBytes += data.byteLength;
        }

        if (params && params.byteLength > 0) {
          if (params.byteLength > BUNDLE_INLINE_MAX_BYTES) {
            await flushPendingPackToR2();
            const uploaded = await this.stageR2Blob(params, nowMs);
            stagedUploads.push(uploaded);
            allEntries.push({
              t: "pr2",
              k: uploaded.r2Key,
              o: 0,
              l: params.byteLength,
            });
          } else {
            if (pendingPackBytes + params.byteLength > R2_PACK_MAX_BYTES) {
              await flushPendingPackToR2();
            }
            const placeholderIdx = allEntries.length;
            allEntries.push({ t: "p", o: 0, l: 0 });
            pendingPackParts.push({
              entryIndex: placeholderIdx,
              isParams: true,
              data: params,
            });
            pendingPackBytes += params.byteLength;
          }
        }

        // Flush remaining pack to R2 if > 512 KiB, or keep inline if it fits in the last seq row!
        let trailingBlob: Uint8Array | null = null;
        if (pendingPackParts.length > 0) {
          if (pendingPackBytes > BUNDLE_INLINE_MAX_BYTES) {
            await flushPendingPackToR2();
          } else {
            // Check whether keeping trailing pack inline fits alongside manifest; if not, flush to R2.
            const estManifest = utf8Encoder.encode(JSON.stringify(allEntries)).byteLength + 64 * pendingPackParts.length;
            if (
              estManifest <= BUNDLE_MANIFEST_MAX_BYTES &&
              estManifest + pendingPackBytes <= BUNDLE_INLINE_MAX_BYTES
            ) {
              trailingBlob = new Uint8Array(pendingPackBytes);
              let off = 0;
              for (const part of pendingPackParts) {
                trailingBlob.set(part.data, off);
                if (part.isParams) {
                  allEntries[part.entryIndex] = {
                    t: "p",
                    o: off,
                    l: part.data.byteLength,
                  };
                } else {
                  allEntries[part.entryIndex] = {
                    t: "i",
                    o: off,
                    l: part.data.byteLength,
                    ...(part.meta ? { m: part.meta } : {}),
                  };
                }
                off += part.data.byteLength;
              }
              pendingPackParts = [];
              pendingPackBytes = 0;
            } else {
              await flushPendingPackToR2();
            }
          }
        }

        const rows = splitManifestRows(allEntries, trailingBlob);
        return { rows, stagedUploads };
      }

      // Fallback when R2 is null: split across multiple <= 512 KiB SQLite seq rows
      const rows = packInlineMultiRows(objects, params);
      return { rows, stagedUploads: [] };
    } catch (err) {
      await this.compensateUnpublishedUploads(stagedUploads, nowMs);
      throw err;
    }
  }

  writeBundleRowsInTxn(
    scopedJobId: string,
    role: "static_in" | "stream_in" | "output",
    rows: readonly PreparedBundleRow[],
  ): void {
    for (const r of rows) {
      this.exec(
        `INSERT INTO native_object_bundles (scoped_job_id, role, seq, manifest, pack_blob)
         VALUES (?, ?, ?, ?, ?)`,
        scopedJobId,
        role,
        r.seq,
        r.manifest,
        r.packBlob,
      );
    }
  }

  /**
   * Appends one streaming `InputChunk` durably into `native_object_bundles(role='stream_in')`
   * (updating the open `seq` row while `manifest + pack_blob <= 512 KiB`, else opening `seq + 1`
   * or spilling > 512 KiB to R2).
   */
  async appendStreamInputChunk(
    scopedJobId: string,
    data: Uint8Array,
    metadata: Record<string, string>,
    nowMs: number,
    guardInTxn: () => boolean,
  ): Promise<boolean> {
    const meta = Object.keys(metadata).length > 0 ? metadata : undefined;
    if (data.byteLength > BUNDLE_INLINE_MAX_BYTES && this.r2) {
      const staged = await this.stageR2Blob(data, nowMs);
      let committed = false;
      this.transactionSync(() => {
        if (!guardInTxn() || !this.claimStagedUploadsInTxn([staged])) {
          return;
        }
        const lastRow = this.exec<{ seq: number }>(
          `SELECT seq FROM native_object_bundles
           WHERE scoped_job_id = ? AND role = 'stream_in'
           ORDER BY seq DESC LIMIT 1`,
          scopedJobId,
        )[0];
        const nextSeq = lastRow ? lastRow.seq + 1 : 0;
        const entry: BundleManifestEntry = {
          t: "r2",
          k: staged.r2Key,
          o: 0,
          l: data.byteLength,
          ...(meta ? { m: meta } : {}),
        };
        this.exec(
          `INSERT INTO native_object_bundles (scoped_job_id, role, seq, manifest, pack_blob)
           VALUES (?, 'stream_in', ?, ?, NULL)`,
          scopedJobId,
          nextSeq,
          JSON.stringify([entry]),
        );
        committed = true;
      });
      if (!committed) {
        await this.compensateUnpublishedUploads([staged], nowMs);
        return false;
      }
      return true;
    }

    return this.transactionSync(() => {
      if (!guardInTxn()) {
        return false;
      }
      const last = this.exec<{
        seq: number;
        manifest: string;
        pack_blob: Uint8Array | null;
      }>(
        `SELECT seq, manifest, pack_blob FROM native_object_bundles
         WHERE scoped_job_id = ? AND role = 'stream_in'
         ORDER BY seq DESC LIMIT 1`,
        scopedJobId,
      )[0];

      if (last) {
        const entries = JSON.parse(last.manifest) as BundleManifestEntry[];
        const prevBlob = last.pack_blob ? normalizeBlob(last.pack_blob) : new Uint8Array(0);
        const newOffset = prevBlob.byteLength;
        entries.push({
          t: "i",
          o: newOffset,
          l: data.byteLength,
          ...(meta ? { m: meta } : {}),
        });
        const newManifest = JSON.stringify(entries);
        const newManifestBytes = utf8Encoder.encode(newManifest).byteLength;
        const newBlobBytes = prevBlob.byteLength + data.byteLength;
        if (
          newManifestBytes <= BUNDLE_MANIFEST_MAX_BYTES &&
          newManifestBytes + newBlobBytes <= BUNDLE_INLINE_MAX_BYTES
        ) {
          const merged = new Uint8Array(newBlobBytes);
          merged.set(prevBlob, 0);
          merged.set(data, prevBlob.byteLength);
          this.exec(
            `UPDATE native_object_bundles
             SET manifest = ?, pack_blob = ?
             WHERE scoped_job_id = ? AND role = 'stream_in' AND seq = ?`,
            newManifest,
            merged.byteLength > 0 ? merged : null,
            scopedJobId,
            last.seq,
          );
          return true;
        }
      }

      const nextSeq = last ? last.seq + 1 : 0;
      const entry: BundleManifestEntry = {
        t: "i",
        o: 0,
        l: data.byteLength,
        ...(meta ? { m: meta } : {}),
      };
      this.exec(
        `INSERT INTO native_object_bundles (scoped_job_id, role, seq, manifest, pack_blob)
         VALUES (?, 'stream_in', ?, ?, ?)`,
        scopedJobId,
        nextSeq,
        JSON.stringify([entry]),
        data.byteLength > 0 ? data : null,
      );
      return true;
    });
  }

  /**
   * Reads only the bundle manifests (no `pack_blob` payloads!) for streaming reads or previews.
   */
  readBundleManifestsSync(
    scopedJobId: string,
    role: "static_in" | "stream_in" | "output",
  ): Array<{ seq: number; entries: BundleManifestEntry[] }> {
    const rows = this.exec<{ seq: number; manifest: string }>(
      `SELECT seq, manifest FROM native_object_bundles
       WHERE scoped_job_id = ? AND role = ?
       ORDER BY seq ASC`,
      scopedJobId,
      role,
    );
    return rows.map((r) => ({
      seq: r.seq,
      entries: JSON.parse(r.manifest) as BundleManifestEntry[],
    }));
  }

  /**
   * Reads a slice `[sliceOffset, sliceOffset + sliceLen)` of an inline `pack_blob`
   * directly in SQLite via `SUBSTR(pack_blob, ?, ?)` without loading the rest of the blob!
   */
  readInlinePackSliceSync(
    scopedJobId: string,
    role: "static_in" | "stream_in" | "output",
    seq: number,
    sliceOffset: number,
    sliceLen: number,
  ): Uint8Array {
    if (sliceLen <= 0) {
      return new Uint8Array(0);
    }
    const row = this.exec<{ sub: Uint8Array | null }>(
      `SELECT SUBSTR(pack_blob, ?, ?) AS sub
       FROM native_object_bundles
       WHERE scoped_job_id = ? AND role = ? AND seq = ?`,
      sliceOffset + 1,
      sliceLen,
      scopedJobId,
      role,
      seq,
    )[0];
    return row?.sub ? normalizeBlob(row.sub) : new Uint8Array(0);
  }

  async readR2Slice(
    r2Key: string,
    offset: number,
    length: number,
    nowMs: number,
  ): Promise<Uint8Array> {
    if (length <= 0) {
      return new Uint8Array(0);
    }
    if (!this.r2) {
      throw new Error(`R2 bucket unavailable while reading ${r2Key}`);
    }
    this.recordR2ClassB(nowMs, 1);
    const body = await this.r2.get(r2Key, { range: { offset, length } });
    if (!body) {
      throw new Error(`Missing R2 blob: ${r2Key}`);
    }
    return new Uint8Array(await body.arrayBuffer());
  }

  /**
   * Reads a `[byteOffset, byteOffset + maxLen)` slice of any `BundleManifestEntry`
   * (inline, R2 pack, or multi-segment R2/inline output) without materialising the whole object.
   */
  async readEntrySlice(
    scopedJobId: string,
    role: "static_in" | "stream_in" | "output",
    seq: number,
    entry: BundleManifestEntry,
    byteOffset: number,
    maxLen: number,
    nowMs: number,
  ): Promise<Uint8Array> {
    if (entry.t === "u") {
      return new Uint8Array(0);
    }
    if (byteOffset >= entry.l || maxLen <= 0) {
      return new Uint8Array(0);
    }
    const want = Math.min(maxLen, entry.l - byteOffset);
    if (entry.t === "i" || entry.t === "p") {
      return this.readInlinePackSliceSync(
        scopedJobId,
        role,
        seq,
        entry.o + byteOffset,
        want,
      );
    }
    if (entry.t === "r2" || entry.t === "pr2") {
      return await this.readR2Slice(entry.k, entry.o + byteOffset, want, nowMs);
    }
    // entry.t === "segs"
    const out = new Uint8Array(want);
    let written = 0;
    let curLogicalOff = 0;
    const targetStart = byteOffset;
    const targetEnd = byteOffset + want;
    for (const seg of entry.s) {
      const segStart = curLogicalOff;
      const segEnd = curLogicalOff + seg.l;
      curLogicalOff = segEnd;
      if (segEnd <= targetStart) {
        continue;
      }
      if (segStart >= targetEnd) {
        break;
      }
      const overlapStart = Math.max(segStart, targetStart);
      const overlapEnd = Math.min(segEnd, targetEnd);
      const segLocalOff = overlapStart - segStart;
      const segWant = overlapEnd - overlapStart;
      let piece: Uint8Array;
      if ("k" in seg) {
        piece = await this.readR2Slice(seg.k, seg.o + segLocalOff, segWant, nowMs);
      } else {
        piece = this.readInlinePackSliceSync(
          scopedJobId,
          role,
          seq,
          seg.o + segLocalOff,
          segWant,
        );
      }
      out.set(piece, written);
      written += piece.byteLength;
    }
    return written === want ? out : out.subarray(0, written);
  }

  /**
   * Materialises all objects (and optional params) of a bundle role.
   */
  async loadBundleObjects(
    scopedJobId: string,
    role: "static_in" | "stream_in" | "output",
    nowMs: number,
  ): Promise<{ objects: StoredObject[]; params: Uint8Array }> {
    const rows = this.exec<{
      seq: number;
      manifest: string;
      pack_blob: Uint8Array | null;
    }>(
      `SELECT seq, manifest, pack_blob FROM native_object_bundles
       WHERE scoped_job_id = ? AND role = ?
       ORDER BY seq ASC`,
      scopedJobId,
      role,
    );
    const objects: StoredObject[] = [];
    let params: Uint8Array = new Uint8Array(0);

    // Cache R2 pack reads within one bundle load if multiple small objects share the same R2 pack!
    let cachedPackKey: string | null = null;
    let cachedPackBuf: Uint8Array | null = null;

    for (const row of rows) {
      const entries = JSON.parse(row.manifest) as BundleManifestEntry[];
      const blob = row.pack_blob ? normalizeBlob(row.pack_blob) : new Uint8Array(0);
      // Pre-count how many entries share each R2 key in this row so we do 1 GET per R2 pack instead of N!
      const r2KeyCounts = new Map<string, number>();
      for (const e of entries) {
        if (e.t === "r2" || e.t === "pr2") {
          r2KeyCounts.set(e.k, (r2KeyCounts.get(e.k) ?? 0) + 1);
        }
      }

      for (const e of entries) {
        if (e.t === "u") {
          objects.push({
            kind: "uri",
            uri: e.u,
            metadata: e.m ?? {},
          });
        } else if (e.t === "i") {
          objects.push({
            kind: "inline",
            data: blob.slice(e.o, e.o + e.l),
            metadata: e.m ?? {},
          });
        } else if (e.t === "p") {
          params = blob.slice(e.o, e.o + e.l);
        } else if (e.t === "r2" || e.t === "pr2") {
          let data: Uint8Array;
          if ((r2KeyCounts.get(e.k) ?? 0) > 1) {
            if (cachedPackKey !== e.k) {
              if (!this.r2) {
                throw new Error(`R2 unavailable reading pack ${e.k}`);
              }
              this.recordR2ClassB(nowMs, 1);
              const full = await this.r2.get(e.k);
              if (!full) {
                throw new Error(`Missing R2 pack ${e.k}`);
              }
              cachedPackKey = e.k;
              cachedPackBuf = new Uint8Array(await full.arrayBuffer());
            }
            data = cachedPackBuf!.slice(e.o, e.o + e.l);
          } else {
            data = await this.readR2Slice(e.k, e.o, e.l, nowMs);
          }
          if (e.t === "pr2") {
            params = data;
          } else {
            objects.push({
              kind: "inline",
              data,
              metadata: e.m ?? {},
            });
          }
        } else if (e.t === "segs") {
          const buf = new Uint8Array(e.l);
          let pos = 0;
          for (const seg of e.s) {
            let piece: Uint8Array;
            if ("k" in seg) {
              piece = await this.readR2Slice(seg.k, seg.o, seg.l, nowMs);
            } else {
              piece = blob.slice(seg.o, seg.o + seg.l);
            }
            buf.set(piece, pos);
            pos += piece.byteLength;
          }
          objects.push({
            kind: "inline",
            data: buf,
            metadata: e.m ?? {},
          });
        }
      }
    }

    return { objects, params };
  }

  /**
   * Loads bounded 4,096-byte previews via SQLite `SUBSTR` (inline packs) and R2 `Range` GET
   * (R2 packs / segments) without reading whole objects!
   */
  async loadBundlePreviews(
    scopedJobId: string,
    role: "static_in" | "output",
    nowMs: number,
    previewLimit = PREVIEW_MAX_BYTES,
  ): Promise<{
    previews: ObjectPreviewData[];
    paramsSizeBytes: number;
    paramsHead: Uint8Array | null;
  }> {
    const manifests = this.readBundleManifestsSync(scopedJobId, role);
    const previews: ObjectPreviewData[] = [];
    let paramsSizeBytes = 0;
    let paramsHead: Uint8Array | null = null;

    for (const { seq, entries } of manifests) {
      for (const e of entries) {
        if (e.t === "u") {
          previews.push({
            kind: "uri",
            sizeBytes: null,
            uri: e.u,
            headBytes: null,
            metadata: e.m ?? {},
          });
          continue;
        }
        if (e.t === "p" || e.t === "pr2") {
          paramsSizeBytes = e.l;
          paramsHead = await this.readEntrySlice(
            scopedJobId,
            role,
            seq,
            e,
            0,
            Math.min(e.l, previewLimit),
            nowMs,
          );
          continue;
        }
        const head = await this.readEntrySlice(
          scopedJobId,
          role,
          seq,
          e,
          0,
          Math.min(e.l, previewLimit),
          nowMs,
        );
        previews.push({
          kind: "inline",
          sizeBytes: e.l,
          uri: null,
          headBytes: head,
          metadata: e.m ?? {},
        });
      }
    }

    return { previews, paramsSizeBytes, paramsHead };
  }
}

export function extractR2KeysFromManifest(manifestJson: string): string[] {
  const keys: string[] = [];
  try {
    const entries = JSON.parse(manifestJson) as BundleManifestEntry[];
    for (const e of entries) {
      if (e.t === "r2" || e.t === "pr2") {
        keys.push(e.k);
      } else if (e.t === "segs") {
        for (const s of e.s) {
          if ("k" in s) {
            keys.push(s.k);
          }
        }
      }
    }
  } catch {
    // Ignore malformed JSON
  }
  return keys;
}

function splitManifestRows(
  allEntries: readonly BundleManifestEntry[],
  trailingInlineBlob: Uint8Array | null,
): PreparedBundleRow[] {
  const fullManifest = JSON.stringify(allEntries);
  const fullBytes = utf8Encoder.encode(fullManifest).byteLength;
  const blobLen = trailingInlineBlob?.byteLength ?? 0;
  if (
    fullBytes <= BUNDLE_MANIFEST_MAX_BYTES &&
    fullBytes + blobLen <= BUNDLE_INLINE_MAX_BYTES
  ) {
    return [
      {
        seq: 0,
        manifest: fullManifest,
        packBlob: trailingInlineBlob,
      },
    ];
  }

  // Split large manifests across ordered seq rows (<= 256 KiB each); attach trailingInlineBlob only to the final row.
  const rows: PreparedBundleRow[] = [];
  let current: BundleManifestEntry[] = [];
  let currentBytes = 2; // "[]"
  let seq = 0;

  for (let i = 0; i < allEntries.length; i++) {
    const entry = allEntries[i]!;
    const itemBytes = utf8Encoder.encode(JSON.stringify(entry)).byteLength + 1;
    const isInlineEntry = entry.t === "i" || entry.t === "p";
    // Keep all inline entries pointing into trailingInlineBlob together on the last row.
    if (
      current.length > 0 &&
      (currentBytes + itemBytes > BUNDLE_MANIFEST_MAX_BYTES ||
        (isInlineEntry && current.some((e) => e.t !== "i" && e.t !== "p") &&
          currentBytes + itemBytes + blobLen > BUNDLE_INLINE_MAX_BYTES))
    ) {
      rows.push({
        seq: seq++,
        manifest: JSON.stringify(current),
        packBlob: null,
      });
      current = [];
      currentBytes = 2;
    }
    current.push(entry);
    currentBytes += itemBytes;
  }

  if (current.length > 0 || rows.length === 0) {
    rows.push({
      seq: seq++,
      manifest: JSON.stringify(current),
      packBlob: trailingInlineBlob,
    });
  }
  return rows;
}

function packInlineMultiRows(
  objects: readonly StoredObject[],
  params: Uint8Array | null,
): PreparedBundleRow[] {
  const rows: PreparedBundleRow[] = [];
  let seq = 0;
  let curEntries: BundleManifestEntry[] = [];
  let curParts: Uint8Array[] = [];
  let curBlobBytes = 0;
  let curManifestBytes = 2;

  const flush = () => {
    if (curEntries.length === 0) {
      return;
    }
    const blob =
      curBlobBytes > 0 ? concatBytes(curParts, curBlobBytes) : null;
    rows.push({
      seq: seq++,
      manifest: JSON.stringify(curEntries),
      packBlob: blob,
    });
    curEntries = [];
    curParts = [];
    curBlobBytes = 0;
    curManifestBytes = 2;
  };

  for (const obj of objects) {
    const meta = Object.keys(obj.metadata).length > 0 ? obj.metadata : undefined;
    if (obj.kind === "uri") {
      const entry: BundleManifestEntry = {
        t: "u",
        u: obj.uri ?? "",
        ...(meta ? { m: meta } : {}),
      };
      const entryLen = utf8Encoder.encode(JSON.stringify(entry)).byteLength + 1;
      if (
        curEntries.length > 0 &&
        (curManifestBytes + entryLen > BUNDLE_MANIFEST_MAX_BYTES ||
          curManifestBytes + entryLen + curBlobBytes > BUNDLE_INLINE_MAX_BYTES)
      ) {
        flush();
      }
      curEntries.push(entry);
      curManifestBytes += entryLen;
      continue;
    }
    const data = obj.data ?? new Uint8Array(0);
    const entry: BundleManifestEntry = {
      t: "i",
      o: curBlobBytes,
      l: data.byteLength,
      ...(meta ? { m: meta } : {}),
    };
    const entryLen = utf8Encoder.encode(JSON.stringify(entry)).byteLength + 1;
    if (
      curEntries.length > 0 &&
      (curManifestBytes + entryLen > BUNDLE_MANIFEST_MAX_BYTES ||
        curManifestBytes + entryLen + curBlobBytes + data.byteLength >
          BUNDLE_INLINE_MAX_BYTES)
    ) {
      flush();
      entry.o = 0;
    }
    curEntries.push(entry);
    if (data.byteLength > 0) {
      curParts.push(data);
      curBlobBytes += data.byteLength;
    }
    curManifestBytes += entryLen;
  }

  if (params && params.byteLength > 0) {
    const entry: BundleManifestEntry = {
      t: "p",
      o: curBlobBytes,
      l: params.byteLength,
    };
    const entryLen = utf8Encoder.encode(JSON.stringify(entry)).byteLength + 1;
    if (
      curEntries.length > 0 &&
      (curManifestBytes + entryLen > BUNDLE_MANIFEST_MAX_BYTES ||
        curManifestBytes + entryLen + curBlobBytes + params.byteLength >
          BUNDLE_INLINE_MAX_BYTES)
    ) {
      flush();
      entry.o = 0;
    }
    curEntries.push(entry);
    curParts.push(params);
    curBlobBytes += params.byteLength;
    curManifestBytes += entryLen;
  }

  if (curEntries.length > 0 || rows.length === 0) {
    flush();
  }
  return rows;
}

export function concatBytes(parts: readonly Uint8Array[], totalLen?: number): Uint8Array {
  const total =
    totalLen ?? parts.reduce((acc, p) => acc + p.byteLength, 0);
  const out = new Uint8Array(total);
  let off = 0;
  for (const p of parts) {
    out.set(p, off);
    off += p.byteLength;
  }
  return out;
}

export function formatInlinePreview(
  headBytes: Uint8Array | null,
  limit = PREVIEW_MAX_BYTES,
): string | null {
  if (!headBytes || headBytes.byteLength === 0) {
    return null;
  }
  const sampleLen = Math.min(512, headBytes.byteLength);
  let binary = 0;
  for (let i = 0; i < sampleLen; i++) {
    const b = headBytes[i]!;
    if ((b >= 0 && b <= 8) || b === 11 || b === 12 || (b >= 14 && b <= 31)) {
      binary += 1;
    }
  }
  if (binary * 10 > sampleLen) {
    return null;
  }
  const slice = headBytes.subarray(0, Math.min(limit, headBytes.byteLength));
  return utf8Decoder.decode(slice).trim();
}
