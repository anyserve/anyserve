export const DEFAULT_R2_SPILL_THRESHOLD_BYTES = 1_048_576;
export const DEFAULT_PREVIEW_BYTES = 4_096;
export const MAX_SCAN_LIMIT = 251;
export const MAX_PURGE_BATCH_LIMIT = 500;
export const MAX_R2_DELETE_CHUNK = 1_000;
export const MAX_RECOVERED_WORKERS = 20_000;

export type JobStateText =
  | "queued"
  | "running"
  | "succeeded"
  | "failed"
  | "cancelled";

export interface WireObject {
  uri: string | null;
  data_b64: string | null;
  metadata: Record<string, string>;
}

export interface WireObjectPreview {
  uri: string | null;
  head_b64: string | null;
  size_bytes: number | null;
  metadata: Record<string, string>;
}

export interface WireStoredJob {
  job_id: string;
  namespace: string;
  state: JobStateText;
  spec_hash: string;
  spec_json: string;
  inputs: WireObject[];
  params_b64: string;
  attempt_count: number;
  max_attempts: number;
  created_at_ms: number;
  updated_at_ms: number;
  error_json: string | null;
  outputs: WireObject[];
  last_attempt_id: string | null;
  last_worker_id: string | null;
  failed_workers: string[];
}

export interface AcquireRequest {
  holder_id: string;
  lease_duration_ms: number;
}

export type AcquireResponse =
  | { acquired: false }
  | { acquired: true; term: number; active: WireStoredJob[]; workers: string[] };

export interface RenewRequest {
  holder_id: string;
  term: number;
  lease_duration_ms: number;
}

export interface RenewResponse {
  ok: boolean;
}

export interface RelinquishRequest {
  holder_id: string;
  term: number;
}

export interface RelinquishResponse {
  ok: boolean;
}

export type CommitOp =
  | {
      type: "accept";
      job_id: string;
      namespace: string;
      spec_hash: string;
      spec_json: string;
      inputs: WireObject[];
      params_b64: string;
      max_attempts: number;
      created_at_ms: number;
    }
  | {
      type: "attempt_start";
      job_id: string;
      attempt_number: number;
      at_ms: number;
      attempt_id: string | null;
      worker_id: string | null;
    }
  | {
      type: "terminal";
      job_id: string;
      state: "succeeded" | "failed" | "cancelled";
      outputs: WireObject[];
      error_json: string | null;
      attempt_id: string | null;
      worker_id: string | null;
      winning_lease_id: string | null;
      outcome_digest: string | null;
      at_ms: number;
    }
  | {
      type: "worker_upsert";
      namespace: string;
      worker_id: string;
      record_json: string;
      updated_at_ms: number;
    }
  | {
      type: "worker_forget";
      namespace: string;
      worker_id: string;
    }
  | {
      type: "probe";
    };

export interface CommitBatchRequest {
  holder_id: string;
  term: number;
  ops: CommitOp[];
}

export interface CommitBatchResponse {
  outcome: "committed" | "fenced" | "rolled_back";
  reason?: string;
}

export interface SpecHashRequest {
  job_id: string;
}

export interface SpecHashResponse {
  spec_hash: string | null;
}

export interface ReceiptRequest {
  lease_id: string;
}

export interface ReceiptResponse {
  receipt: { job_id: string; result_digest: string } | null;
}

export interface LoadJobRequest {
  job_id: string;
  include_inputs: boolean;
  include_outputs: boolean;
  max_output_bytes: number | null;
}

export interface LoadJobResponse {
  job: WireStoredJob | null;
  budget_exceeded: boolean;
  required_bytes: number;
}

export interface LoadJobPreviewRequest {
  job_id: string;
  preview_bytes?: number;
}

export interface LoadJobPreviewResponse {
  job: WireStoredJob | null;
  input_previews: WireObjectPreview[];
  params_head_b64: string;
  params_size_bytes: number;
  output_previews: WireObjectPreview[];
}

export interface ScanJobsRequest {
  from_ms: number;
  to_ms: number;
  before: { created_at_ms: number; job_id: string } | null;
  namespace: string | null;
  limit: number;
}

export interface ScanJobsResponse {
  jobs: WireStoredJob[];
}

export interface PurgeRequest {
  holder_id: string;
  term: number;
  before_ms: number;
  batch_limit: number;
}

export interface PurgeResponse {
  fenced: boolean;
  deleted: number;
}

export interface SqlCursorLike<T extends Record<string, any> = Record<string, any>> {
  toArray(): T[];
  [Symbol.iterator]?(): IterableIterator<T>;
}

export interface SqlStorageLike {
  exec<T extends Record<string, any>>(
    query: string,
    ...bindings: any[]
  ): SqlCursorLike<T>;
}

export interface DurableObjectStorageLike {
  sql: SqlStorageLike;
  transactionSync<T>(closure: () => T): T;
}

export interface R2ObjectBodyLike {
  arrayBuffer(): Promise<ArrayBuffer>;
}

export type R2RangeLike =
  | { offset: number; length?: number }
  | { offset?: number; length: number };

export interface R2BucketLike {
  get(
    key: string,
    options?: { range?: R2RangeLike },
  ): Promise<R2ObjectBodyLike | null>;
  put(
    key: string,
    value:
      | ReadableStream
      | ArrayBuffer
      | ArrayBufferView
      | string
      | Blob
      | null,
  ): Promise<unknown>;
  delete(keys: string | string[]): Promise<void>;
}

export interface JournalStorageOptions {
  r2SpillThresholdBytes?: number;
  nowMs?: () => number;
  authSecret?: string;
}

const B64_ALPHABET =
  "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
const B64_DECODE = new Int16Array(256).fill(-1);
for (let i = 0; i < B64_ALPHABET.length; i++) {
  B64_DECODE[B64_ALPHABET.charCodeAt(i)] = i;
}

export function encodeBase64(bytes: Uint8Array): string {
  if (bytes.byteLength === 0) {
    return "";
  }
  const chunks: string[] = [];
  const len = bytes.byteLength;
  const maxChunkParts = 16_384;
  let parts: string[] = [];
  for (let i = 0; i < len; i += 3) {
    const b0 = bytes[i]!;
    const hasB1 = i + 1 < len;
    const hasB2 = i + 2 < len;
    const b1 = hasB1 ? bytes[i + 1]! : 0;
    const b2 = hasB2 ? bytes[i + 2]! : 0;
    const triplet = (b0 << 16) | (b1 << 8) | b2;
    parts.push(
      B64_ALPHABET[(triplet >> 18) & 0x3f]! +
        B64_ALPHABET[(triplet >> 12) & 0x3f]! +
        (hasB1 ? B64_ALPHABET[(triplet >> 6) & 0x3f]! : "=") +
        (hasB2 ? B64_ALPHABET[triplet & 0x3f]! : "="),
    );
    if (parts.length >= maxChunkParts) {
      chunks.push(parts.join(""));
      parts = [];
    }
  }
  if (parts.length > 0) {
    chunks.push(parts.join(""));
  }
  return chunks.join("");
}

export function decodeBase64(input: string): Uint8Array {
  const cleaned = input.replace(/\s+/g, "");
  if (cleaned.length === 0) {
    return new Uint8Array(0);
  }
  if (cleaned.length % 4 !== 0) {
    throw new BadRequestError("invalid base64 length");
  }
  let padding = 0;
  if (cleaned.endsWith("==")) {
    padding = 2;
  } else if (cleaned.endsWith("=")) {
    padding = 1;
  }
  const outLen = (cleaned.length / 4) * 3 - padding;
  const out = new Uint8Array(outLen);
  let outIdx = 0;
  for (let i = 0; i < cleaned.length; i += 4) {
    const c0 = cleaned.charCodeAt(i);
    const c1 = cleaned.charCodeAt(i + 1);
    const c2 = cleaned.charCodeAt(i + 2);
    const c3 = cleaned.charCodeAt(i + 3);
    const v0 = c0 < 256 ? B64_DECODE[c0]! : -1;
    const v1 = c1 < 256 ? B64_DECODE[c1]! : -1;
    const isPad2 = c2 === 61;
    const isPad3 = c3 === 61;
    const v2 = isPad2 ? 0 : c2 < 256 ? B64_DECODE[c2]! : -1;
    const v3 = isPad3 ? 0 : c3 < 256 ? B64_DECODE[c3]! : -1;
    if (
      v0 < 0 ||
      v1 < 0 ||
      v2 < 0 ||
      v3 < 0 ||
      (isPad2 && !isPad3) ||
      ((isPad2 || isPad3) && i + 4 !== cleaned.length)
    ) {
      throw new BadRequestError("invalid base64 character");
    }
    const triplet = (v0 << 18) | (v1 << 12) | (v2 << 6) | v3;
    out[outIdx++] = (triplet >> 16) & 0xff;
    if (!isPad2 && outIdx < outLen) {
      out[outIdx++] = (triplet >> 8) & 0xff;
    }
    if (!isPad3 && outIdx < outLen) {
      out[outIdx++] = triplet & 0xff;
    }
  }
  return out;
}

export function objectR2Key(
  jobId: string,
  role: "input" | "params" | "output",
  idx: number,
): string {
  return `jobs/${encodeURIComponent(jobId)}/${role}/${idx}`;
}

class BadRequestError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "BadRequestError";
  }
}

class RollbackError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "RollbackError";
  }
}

function toUint8Array(value: unknown): Uint8Array {
  if (value === null || value === undefined) {
    return new Uint8Array(0);
  }
  if (value instanceof Uint8Array) {
    return value;
  }
  if (value instanceof ArrayBuffer) {
    return new Uint8Array(value);
  }
  if (ArrayBuffer.isView(value)) {
    return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
  }
  throw new Error(`expected binary BLOB value, got ${typeof value}`);
}

function parseMetadataJson(raw: string): Record<string, string> {
  const parsed: unknown = JSON.parse(raw);
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    return {};
  }
  const out: Record<string, string> = {};
  for (const [k, v] of Object.entries(parsed)) {
    out[k] = String(v);
  }
  return out;
}

function normalizeMetadata(
  input: Record<string, string> | undefined | null,
): string {
  if (!input || typeof input !== "object" || Array.isArray(input)) {
    return "{}";
  }
  const keys = Object.keys(input);
  if (keys.length === 0) {
    return "{}";
  }
  return JSON.stringify(input);
}

function constantTimeEqual(a: string, b: string): boolean {
  const enc = new TextEncoder();
  const aBytes = enc.encode(a);
  const bBytes = enc.encode(b);
  const maxLen = Math.max(aBytes.length, bBytes.length);
  let diff = aBytes.length ^ bBytes.length;
  for (let i = 0; i < maxLen; i++) {
    diff |= (aBytes[i] ?? 0) ^ (bBytes[i] ?? 0);
  }
  return diff === 0;
}

function assertNonEmptyString(value: unknown, field: string): string {
  if (typeof value !== "string" || value.length === 0) {
    throw new BadRequestError(`${field} must be a non-empty string`);
  }
  return value;
}

function assertString(value: unknown, field: string): string {
  if (typeof value !== "string") {
    throw new BadRequestError(`${field} must be a string`);
  }
  return value;
}

function assertNullableString(value: unknown, field: string): string | null {
  if (value === null || value === undefined) {
    return null;
  }
  if (typeof value !== "string") {
    throw new BadRequestError(`${field} must be a string or null`);
  }
  return value;
}

function assertNonNegativeInt(value: unknown, field: string): number {
  if (
    typeof value !== "number" ||
    !Number.isFinite(value) ||
    !Number.isInteger(value) ||
    value < 0
  ) {
    throw new BadRequestError(`${field} must be a non-negative integer`);
  }
  return value;
}

function assertPositiveInt(value: unknown, field: string): number {
  const n = assertNonNegativeInt(value, field);
  if (n <= 0) {
    throw new BadRequestError(`${field} must be positive`);
  }
  return n;
}

interface JobRow {
  job_id: string;
  namespace: string;
  state: JobStateText;
  spec_hash: string;
  spec_json: string;
  attempt_count: number;
  max_attempts: number;
  created_at_ms: number;
  updated_at_ms: number;
  error_json: string | null;
  last_attempt_id: string | null;
  last_worker_id: string | null;
}

interface ObjectRow {
  job_id: string;
  role: "input" | "params" | "output";
  idx: number;
  uri: string | null;
  content: unknown;
  r2_key: string | null;
  size_bytes: number | null;
  metadata_json: string;
}

interface PreviewObjectRow {
  role: "input" | "params" | "output";
  idx: number;
  uri: string | null;
  r2_key: string | null;
  size_bytes: number | null;
  head: unknown;
  metadata_json: string;
}

interface PreparedWireObject {
  idx: number;
  uri: string | null;
  bytes: Uint8Array | null;
  r2Key: string | null;
  sizeBytes: number | null;
  metadataJson: string;
}

type PreparedOp =
  | {
      type: "accept";
      jobId: string;
      namespace: string;
      specHash: string;
      specJson: string;
      inputs: PreparedWireObject[];
      paramsBytes: Uint8Array;
      paramsR2Key: string | null;
      maxAttempts: number;
      createdAtMs: number;
    }
  | {
      type: "attempt_start";
      jobId: string;
      attemptNumber: number;
      atMs: number;
      attemptId: string | null;
      workerId: string | null;
    }
  | {
      type: "terminal";
      jobId: string;
      state: "succeeded" | "failed" | "cancelled";
      outputs: PreparedWireObject[];
      errorJson: string | null;
      attemptId: string | null;
      workerId: string | null;
      winningLeaseId: string | null;
      outcomeDigest: string | null;
      atMs: number;
    }
  | {
      type: "worker_upsert";
      namespace: string;
      workerId: string;
      recordJson: string;
      updatedAtMs: number;
    }
  | {
      type: "worker_forget";
      namespace: string;
      workerId: string;
    }
  | {
      type: "probe";
    };

const SCHEMA_STATEMENTS: readonly string[] = [
  `CREATE TABLE IF NOT EXISTS scheduler_lease (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    holder_id TEXT NOT NULL,
    generation INTEGER NOT NULL,
    expires_at_ms INTEGER NOT NULL
  )`,
  `CREATE TABLE IF NOT EXISTS journal_meta (
    key TEXT PRIMARY KEY,
    value INTEGER NOT NULL
  )`,
  `INSERT OR IGNORE INTO journal_meta (key, value) VALUES ('schema_version', 1)`,
  `CREATE TABLE IF NOT EXISTS journal_jobs (
    job_id TEXT PRIMARY KEY,
    namespace TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')),
    spec_hash TEXT NOT NULL,
    spec_json TEXT NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    error_json TEXT,
    winning_lease_id TEXT,
    result_digest TEXT,
    last_attempt_id TEXT,
    last_worker_id TEXT
  )`,
  `CREATE INDEX IF NOT EXISTS journal_jobs_recovery
    ON journal_jobs(created_at_ms, job_id)
    WHERE state = 'queued'`,
  `CREATE UNIQUE INDEX IF NOT EXISTS journal_jobs_winner
    ON journal_jobs(winning_lease_id)
    WHERE winning_lease_id IS NOT NULL`,
  `CREATE INDEX IF NOT EXISTS journal_jobs_history
    ON journal_jobs(created_at_ms DESC, job_id DESC)
    WHERE state IN ('succeeded', 'failed', 'cancelled')`,
  `CREATE INDEX IF NOT EXISTS journal_jobs_ns_history
    ON journal_jobs(namespace, created_at_ms DESC, job_id DESC)
    WHERE state IN ('succeeded', 'failed', 'cancelled')`,
  `CREATE INDEX IF NOT EXISTS journal_jobs_retention
    ON journal_jobs(updated_at_ms)
    WHERE state IN ('succeeded', 'failed', 'cancelled')`,
  `CREATE TABLE IF NOT EXISTS journal_objects (
    job_id TEXT NOT NULL,
    role TEXT NOT NULL,
    idx INTEGER NOT NULL,
    uri TEXT,
    content BLOB,
    r2_key TEXT,
    size_bytes INTEGER,
    metadata_json TEXT NOT NULL,
    PRIMARY KEY (job_id, role, idx)
  )`,
  `CREATE TABLE IF NOT EXISTS journal_failed_workers (
    job_id TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    PRIMARY KEY (job_id, worker_id)
  )`,
  `CREATE TABLE IF NOT EXISTS journal_workers (
    namespace TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    record_json TEXT NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    PRIMARY KEY (namespace, worker_id)
  )`,
  `CREATE INDEX IF NOT EXISTS journal_workers_updated
    ON journal_workers(updated_at_ms DESC)`,
];

const JOB_SELECT_COLUMNS = `job_id, namespace, state, spec_hash, spec_json,
  attempt_count, max_attempts, created_at_ms, updated_at_ms, error_json,
  last_attempt_id, last_worker_id`;

export class JournalStorage {
  private readonly storage: DurableObjectStorageLike;
  private readonly r2: R2BucketLike;
  private readonly r2SpillThresholdBytes: number;
  private readonly nowMs: () => number;
  private readonly authSecret: string | undefined;
  private schemaReady = false;

  constructor(
    storage: DurableObjectStorageLike,
    r2: R2BucketLike,
    options: JournalStorageOptions = {},
  ) {
    this.storage = storage;
    this.r2 = r2;
    this.r2SpillThresholdBytes =
      options.r2SpillThresholdBytes ?? DEFAULT_R2_SPILL_THRESHOLD_BYTES;
    this.nowMs = options.nowMs ?? (() => Date.now());
    this.authSecret =
      options.authSecret && options.authSecret.length > 0
        ? options.authSecret
        : undefined;
  }

  ensureSchema(): void {
    if (this.schemaReady) {
      return;
    }
    for (const stmt of SCHEMA_STATEMENTS) {
      this.storage.sql.exec(stmt);
    }
    this.schemaReady = true;
  }

  async handleRequest(request: Request): Promise<Response> {
    if (this.authSecret !== undefined) {
      const authHeader = request.headers.get("authorization") ?? "";
      const match = /^Bearer\s+(.+)$/i.exec(authHeader.trim());
      if (!match || !constantTimeEqual(match[1]!, this.authSecret)) {
        return Response.json({ error: "unauthorized" }, { status: 401 });
      }
    }

    if (request.method !== "POST") {
      return Response.json({ error: "method not allowed" }, { status: 405 });
    }

    const pathname = new URL(request.url).pathname;

    let body: unknown;
    try {
      body = await request.json();
    } catch {
      return Response.json({ error: "invalid json body" }, { status: 400 });
    }
    if (!body || typeof body !== "object" || Array.isArray(body)) {
      return Response.json({ error: "json body must be an object" }, { status: 400 });
    }

    try {
      switch (pathname) {
        case "/v1/acquire":
          return Response.json(
            await this.acquire(body as Record<string, unknown>),
          );
        case "/v1/renew":
          return Response.json(this.renew(body as Record<string, unknown>));
        case "/v1/relinquish":
          return Response.json(
            this.relinquish(body as Record<string, unknown>),
          );
        case "/v1/commit_batch":
          return Response.json(
            await this.commitBatch(body as Record<string, unknown>),
          );
        case "/v1/spec_hash":
          return Response.json(this.specHash(body as Record<string, unknown>));
        case "/v1/receipt":
          return Response.json(this.receipt(body as Record<string, unknown>));
        case "/v1/load_job":
          return Response.json(
            await this.loadJob(body as Record<string, unknown>),
          );
        case "/v1/load_job_preview":
          return Response.json(
            await this.loadJobPreview(body as Record<string, unknown>),
          );
        case "/v1/scan_jobs":
          return Response.json(this.scanJobs(body as Record<string, unknown>));
        case "/v1/purge":
          return Response.json(
            await this.purge(body as Record<string, unknown>),
          );
        default:
          return Response.json({ error: "not found" }, { status: 404 });
      }
    } catch (error) {
      if (error instanceof BadRequestError) {
        return Response.json({ error: error.message }, { status: 400 });
      }
      const message = error instanceof Error ? error.message : String(error);
      return Response.json({ error: message }, { status: 500 });
    }
  }

  async acquire(raw: Record<string, unknown>): Promise<AcquireResponse> {
    const holderId = assertNonEmptyString(raw.holder_id, "holder_id");
    const leaseDurationMs = assertPositiveInt(
      raw.lease_duration_ms,
      "lease_duration_ms",
    );
    this.ensureSchema();

    const syncResult = this.storage.transactionSync(() => {
      const now = this.nowMs();
      const expiresAtMs = now + leaseDurationMs;
      const leaseRows = this.storage.sql
        .exec<{ generation: number }>(
          `INSERT INTO scheduler_lease (id, holder_id, generation, expires_at_ms)
           VALUES (1, ?, 1, ?)
           ON CONFLICT(id) DO UPDATE SET
             holder_id = excluded.holder_id,
             generation = scheduler_lease.generation + 1,
             expires_at_ms = excluded.expires_at_ms
           WHERE scheduler_lease.expires_at_ms <= ?
           RETURNING generation`,
          holderId,
          expiresAtMs,
          now,
        )
        .toArray();
      if (leaseRows.length !== 1) {
        return null;
      }
      const term = leaseRows[0]!.generation;
      const jobRows = this.storage.sql
        .exec<JobRow>(
          `SELECT ${JOB_SELECT_COLUMNS}
           FROM journal_jobs
           WHERE state = 'queued'
           ORDER BY created_at_ms ASC, job_id ASC`,
        )
        .toArray();

      const jobs = jobRows.map((row) => {
        const objects = this.storage.sql
          .exec<ObjectRow>(
            `SELECT job_id, role, idx, uri, content, r2_key, size_bytes, metadata_json
             FROM journal_objects
             WHERE job_id = ? AND role IN ('input', 'params')
             ORDER BY role ASC, idx ASC`,
            row.job_id,
          )
          .toArray();
        const failedWorkers = this.loadFailedWorkersSync(row.job_id);
        return { row, objects, failedWorkers };
      });
      const workers = this.storage.sql
        .exec<{ record_json: string }>(
          `SELECT record_json
           FROM journal_workers
           ORDER BY updated_at_ms DESC
           LIMIT ?`,
          MAX_RECOVERED_WORKERS,
        )
        .toArray()
        .map((r) => r.record_json);
      return { term, jobs, workers };
    });

    if (syncResult === null) {
      return { acquired: false };
    }

    let fetchedFromR2 = false;
    const active: WireStoredJob[] = [];
    for (const item of syncResult.jobs) {
      const inputs: WireObject[] = [];
      let paramsB64 = "";
      for (const obj of item.objects) {
        if (obj.role === "input") {
          if (obj.r2_key !== null) {
            fetchedFromR2 = true;
          }
          inputs.push(await this.materializeWireObject(obj));
        } else if (obj.role === "params" && obj.idx === 0) {
          if (obj.r2_key !== null) {
            fetchedFromR2 = true;
            const bytes = await this.readR2Required(obj.r2_key);
            paramsB64 = encodeBase64(bytes);
          } else {
            paramsB64 = encodeBase64(toUint8Array(obj.content));
          }
        }
      }
      active.push(
        this.toWireStoredJob(
          item.row,
          inputs,
          paramsB64,
          [],
          item.failedWorkers,
        ),
      );
    }

    if (fetchedFromR2) {
      const stillValid = this.storage.transactionSync(() =>
        this.hasValidLeaseSync(holderId, syncResult.term, this.nowMs()),
      );
      if (!stillValid) {
        return { acquired: false };
      }
    }

    return {
      acquired: true,
      term: syncResult.term,
      active,
      workers: syncResult.workers,
    };
  }

  renew(raw: Record<string, unknown>): RenewResponse {
    const holderId = assertNonEmptyString(raw.holder_id, "holder_id");
    const term = assertPositiveInt(raw.term, "term");
    const leaseDurationMs = assertPositiveInt(
      raw.lease_duration_ms,
      "lease_duration_ms",
    );
    this.ensureSchema();

    const ok = this.storage.transactionSync(() => {
      const now = this.nowMs();
      const rows = this.storage.sql
        .exec<{ generation: number }>(
          `UPDATE scheduler_lease
           SET expires_at_ms = ?
           WHERE id = 1 AND holder_id = ? AND generation = ? AND expires_at_ms > ?
           RETURNING generation`,
          now + leaseDurationMs,
          holderId,
          term,
          now,
        )
        .toArray();
      return rows.length === 1;
    });
    return { ok };
  }

  relinquish(raw: Record<string, unknown>): RelinquishResponse {
    const holderId = assertNonEmptyString(raw.holder_id, "holder_id");
    const term = assertPositiveInt(raw.term, "term");
    this.ensureSchema();

    const ok = this.storage.transactionSync(() => {
      const now = this.nowMs();
      const rows = this.storage.sql
        .exec<{ generation: number }>(
          `UPDATE scheduler_lease
           SET expires_at_ms = ?
           WHERE id = 1 AND holder_id = ? AND generation = ?
           RETURNING generation`,
          now,
          holderId,
          term,
        )
        .toArray();
      return rows.length === 1;
    });
    return { ok };
  }

  async commitBatch(
    raw: Record<string, unknown>,
  ): Promise<CommitBatchResponse> {
    const holderId = assertNonEmptyString(raw.holder_id, "holder_id");
    const term = assertPositiveInt(raw.term, "term");
    if (!Array.isArray(raw.ops)) {
      throw new BadRequestError("ops must be an array");
    }
    this.ensureSchema();

    const preparedOps = this.prepareOps(raw.ops);

    // Pre-check lease and job eligibility synchronously before spilling payloads to R2.
    const preflight = this.storage.transactionSync(() => {
      if (!this.hasValidLeaseSync(holderId, term, this.nowMs())) {
        return { fenced: true as const, spillIndices: new Set<number>() };
      }
      const pendingQueued = new Set<string>();
      const spillIndices = new Set<number>();
      for (let i = 0; i < preparedOps.length; i++) {
        const op = preparedOps[i]!;
        if (op.type === "accept") {
          const exists = this.storage.sql
            .exec<{ job_id: string }>(
              "SELECT job_id FROM journal_jobs WHERE job_id = ?",
              op.jobId,
            )
            .toArray();
          if (exists.length === 0 && !pendingQueued.has(op.jobId)) {
            pendingQueued.add(op.jobId);
            spillIndices.add(i);
          }
        } else if (op.type === "terminal") {
          const row = this.storage.sql
            .exec<{ state: string }>(
              "SELECT state FROM journal_jobs WHERE job_id = ?",
              op.jobId,
            )
            .toArray()[0];
          const isQueued =
            row?.state === "queued" || pendingQueued.has(op.jobId);
          if (isQueued) {
            pendingQueued.delete(op.jobId);
            spillIndices.add(i);
          }
        }
      }
      return { fenced: false as const, spillIndices };
    });

    if (preflight.fenced) {
      return {
        outcome: "fenced",
        reason: "scheduler lease expired or superseded",
      };
    }

    // Spill large inline payloads (> threshold) to R2 BEFORE transactionSync.
    const uploadedKeys: Array<{ key: string; jobId: string }> = [];
    for (let i = 0; i < preparedOps.length; i++) {
      if (!preflight.spillIndices.has(i)) {
        continue;
      }
      const op = preparedOps[i]!;
      if (op.type === "accept") {
        for (const obj of op.inputs) {
          if (obj.r2Key !== null && obj.bytes !== null) {
            await this.r2.put(obj.r2Key, obj.bytes);
            uploadedKeys.push({ key: obj.r2Key, jobId: op.jobId });
          }
        }
        if (op.paramsR2Key !== null) {
          await this.r2.put(op.paramsR2Key, op.paramsBytes);
          uploadedKeys.push({ key: op.paramsR2Key, jobId: op.jobId });
        }
      } else if (op.type === "terminal") {
        for (const obj of op.outputs) {
          if (obj.r2Key !== null && obj.bytes !== null) {
            await this.r2.put(obj.r2Key, obj.bytes);
            uploadedKeys.push({ key: obj.r2Key, jobId: op.jobId });
          }
        }
      }
    }

    // Commit strictly synchronously inside transactionSync.
    try {
      const result = this.storage.transactionSync((): CommitBatchResponse => {
        if (!this.hasValidLeaseSync(holderId, term, this.nowMs())) {
          return {
            outcome: "fenced",
            reason: "scheduler lease expired or superseded",
          };
        }
        for (const op of preparedOps) {
          this.applyPreparedOpSync(op);
        }
        return { outcome: "committed" };
      });
      return result;
    } catch (error) {
      if (uploadedKeys.length > 0) {
        const orphanKeys = this.storage.transactionSync(() => {
          const toDelete: string[] = [];
          for (const item of uploadedKeys) {
            const exists = this.storage.sql
              .exec<{ job_id: string }>(
                "SELECT job_id FROM journal_jobs WHERE job_id = ?",
                item.jobId,
              )
              .toArray();
            if (exists.length === 0) {
              toDelete.push(item.key);
            }
          }
          return toDelete;
        });
        await this.deleteR2KeysInChunks(orphanKeys);
      }
      const reason = error instanceof Error ? error.message : String(error);
      return { outcome: "rolled_back", reason };
    }
  }

  specHash(raw: Record<string, unknown>): SpecHashResponse {
    const jobId = assertNonEmptyString(raw.job_id, "job_id");
    this.ensureSchema();
    const row = this.storage.sql
      .exec<{ spec_hash: string }>(
        "SELECT spec_hash FROM journal_jobs WHERE job_id = ?",
        jobId,
      )
      .toArray()[0];
    return { spec_hash: row?.spec_hash ?? null };
  }

  receipt(raw: Record<string, unknown>): ReceiptResponse {
    const leaseId = assertNonEmptyString(raw.lease_id, "lease_id");
    this.ensureSchema();
    const row = this.storage.sql
      .exec<{ job_id: string; result_digest: string }>(
        `SELECT job_id, result_digest
         FROM journal_jobs
         WHERE winning_lease_id = ? AND result_digest IS NOT NULL`,
        leaseId,
      )
      .toArray()[0];
    return {
      receipt: row
        ? { job_id: row.job_id, result_digest: row.result_digest }
        : null,
    };
  }

  async loadJob(raw: Record<string, unknown>): Promise<LoadJobResponse> {
    const jobId = assertNonEmptyString(raw.job_id, "job_id");
    const includeInputs = Boolean(raw.include_inputs);
    const includeOutputs = Boolean(raw.include_outputs);
    const maxOutputBytes =
      raw.max_output_bytes === null || raw.max_output_bytes === undefined
        ? null
        : assertNonNegativeInt(raw.max_output_bytes, "max_output_bytes");
    this.ensureSchema();

    const snapshot = this.storage.transactionSync(() => {
      const row = this.storage.sql
        .exec<JobRow>(
          `SELECT ${JOB_SELECT_COLUMNS} FROM journal_jobs WHERE job_id = ?`,
          jobId,
        )
        .toArray()[0];
      if (!row) {
        return { kind: "missing" as const };
      }
      const failedWorkers = this.loadFailedWorkersSync(jobId);

      let requiredBytes = 0;
      let outputRows: ObjectRow[] = [];
      if (includeOutputs && row.state === "succeeded") {
        const sumRow = this.storage.sql
          .exec<{ total_bytes: number }>(
            `SELECT COALESCE(SUM(size_bytes), 0) AS total_bytes
             FROM journal_objects
             WHERE job_id = ? AND role = 'output' AND uri IS NULL`,
            jobId,
          )
          .toArray()[0];
        requiredBytes = Number(sumRow?.total_bytes ?? 0);
        if (maxOutputBytes !== null && requiredBytes > maxOutputBytes) {
          return {
            kind: "budget_exceeded" as const,
            requiredBytes,
          };
        }
        outputRows = this.storage.sql
          .exec<ObjectRow>(
            `SELECT job_id, role, idx, uri, content, r2_key, size_bytes, metadata_json
             FROM journal_objects
             WHERE job_id = ? AND role = 'output'
             ORDER BY idx ASC`,
            jobId,
          )
          .toArray();
      }

      let inputRows: ObjectRow[] = [];
      let paramsRow: ObjectRow | undefined;
      if (includeInputs) {
        inputRows = this.storage.sql
          .exec<ObjectRow>(
            `SELECT job_id, role, idx, uri, content, r2_key, size_bytes, metadata_json
             FROM journal_objects
             WHERE job_id = ? AND role = 'input'
             ORDER BY idx ASC`,
            jobId,
          )
          .toArray();
        paramsRow = this.storage.sql
          .exec<ObjectRow>(
            `SELECT job_id, role, idx, uri, content, r2_key, size_bytes, metadata_json
             FROM journal_objects
             WHERE job_id = ? AND role = 'params' AND idx = 0`,
            jobId,
          )
          .toArray()[0];
      }

      return {
        kind: "ok" as const,
        row,
        failedWorkers,
        requiredBytes,
        outputRows,
        inputRows,
        paramsRow,
      };
    });

    if (snapshot.kind === "missing") {
      return { job: null, budget_exceeded: false, required_bytes: 0 };
    }
    if (snapshot.kind === "budget_exceeded") {
      return {
        job: null,
        budget_exceeded: true,
        required_bytes: snapshot.requiredBytes,
      };
    }

    try {
      const inputs: WireObject[] = [];
      for (const obj of snapshot.inputRows) {
        inputs.push(await this.materializeWireObject(obj));
      }
      let paramsB64 = "";
      if (snapshot.paramsRow) {
        if (snapshot.paramsRow.r2_key !== null) {
          paramsB64 = encodeBase64(
            await this.readR2Required(snapshot.paramsRow.r2_key),
          );
        } else {
          paramsB64 = encodeBase64(toUint8Array(snapshot.paramsRow.content));
        }
      }
      const outputs: WireObject[] = [];
      for (const obj of snapshot.outputRows) {
        outputs.push(await this.materializeWireObject(obj));
      }
      return {
        job: this.toWireStoredJob(
          snapshot.row,
          inputs,
          paramsB64,
          outputs,
          snapshot.failedWorkers,
        ),
        budget_exceeded: false,
        required_bytes: snapshot.requiredBytes,
      };
    } catch (error) {
      // Handle race with concurrent purge gracefully.
      const stillPresent = this.storage.sql
        .exec<{ job_id: string }>(
          "SELECT job_id FROM journal_jobs WHERE job_id = ?",
          jobId,
        )
        .toArray();
      if (stillPresent.length === 0) {
        return { job: null, budget_exceeded: false, required_bytes: 0 };
      }
      throw error;
    }
  }

  async loadJobPreview(
    raw: Record<string, unknown>,
  ): Promise<LoadJobPreviewResponse> {
    const jobId = assertNonEmptyString(raw.job_id, "job_id");
    const previewBytes =
      raw.preview_bytes === undefined || raw.preview_bytes === null
        ? DEFAULT_PREVIEW_BYTES
        : assertNonNegativeInt(raw.preview_bytes, "preview_bytes");
    this.ensureSchema();

    const snapshot = this.storage.transactionSync(() => {
      const row = this.storage.sql
        .exec<JobRow>(
          `SELECT ${JOB_SELECT_COLUMNS} FROM journal_jobs WHERE job_id = ?`,
          jobId,
        )
        .toArray()[0];
      if (!row) {
        return null;
      }
      const failedWorkers = this.loadFailedWorkersSync(jobId);
      const objectRows = this.storage.sql
        .exec<PreviewObjectRow>(
          `SELECT role, idx, uri, r2_key, size_bytes,
                  SUBSTR(content, 1, ?) AS head, metadata_json
           FROM journal_objects
           WHERE job_id = ?
           ORDER BY role ASC, idx ASC`,
          previewBytes,
          jobId,
        )
        .toArray();
      return { row, failedWorkers, objectRows };
    });

    if (snapshot === null) {
      return {
        job: null,
        input_previews: [],
        params_head_b64: "",
        params_size_bytes: 0,
        output_previews: [],
      };
    }

    const inputPreviews: WireObjectPreview[] = [];
    const outputPreviews: WireObjectPreview[] = [];
    let paramsHeadB64 = "";
    let paramsSizeBytes = 0;

    try {
      for (const obj of snapshot.objectRows) {
        if (obj.role === "params" && obj.idx === 0) {
          paramsSizeBytes = Number(obj.size_bytes ?? 0);
          if (previewBytes === 0) {
            paramsHeadB64 = "";
          } else if (obj.r2_key !== null) {
            const head = await this.readR2Range(obj.r2_key, previewBytes);
            paramsHeadB64 = encodeBase64(head);
          } else {
            paramsHeadB64 = encodeBase64(toUint8Array(obj.head));
          }
          continue;
        }
        const preview = await this.materializeWireObjectPreview(
          obj,
          previewBytes,
        );
        if (obj.role === "input") {
          inputPreviews.push(preview);
        } else if (obj.role === "output") {
          outputPreviews.push(preview);
        }
      }
    } catch (error) {
      const stillPresent = this.storage.sql
        .exec<{ job_id: string }>(
          "SELECT job_id FROM journal_jobs WHERE job_id = ?",
          jobId,
        )
        .toArray();
      if (stillPresent.length === 0) {
        return {
          job: null,
          input_previews: [],
          params_head_b64: "",
          params_size_bytes: 0,
          output_previews: [],
        };
      }
      throw error;
    }

    return {
      job: this.toWireStoredJob(
        snapshot.row,
        [],
        "",
        [],
        snapshot.failedWorkers,
      ),
      input_previews: inputPreviews,
      params_head_b64: paramsHeadB64,
      params_size_bytes: paramsSizeBytes,
      output_previews: outputPreviews,
    };
  }

  scanJobs(raw: Record<string, unknown>): ScanJobsResponse {
    const fromMs = assertNonNegativeInt(raw.from_ms, "from_ms");
    const toMs = assertNonNegativeInt(raw.to_ms, "to_ms");
    const namespace = assertNullableString(raw.namespace, "namespace");
    const rawLimit = assertNonNegativeInt(raw.limit, "limit");
    const limit = Math.max(1, Math.min(MAX_SCAN_LIMIT, rawLimit));

    let before: { created_at_ms: number; job_id: string } | null = null;
    if (raw.before !== null && raw.before !== undefined) {
      if (typeof raw.before !== "object" || Array.isArray(raw.before)) {
        throw new BadRequestError("before must be an object or null");
      }
      const b = raw.before as Record<string, unknown>;
      before = {
        created_at_ms: assertNonNegativeInt(
          b.created_at_ms,
          "before.created_at_ms",
        ),
        job_id: assertNonEmptyString(b.job_id, "before.job_id"),
      };
    }

    this.ensureSchema();

    const conditions = [
      "state IN ('succeeded', 'failed', 'cancelled')",
      "created_at_ms >= ?",
      "created_at_ms < ?",
    ];
    const params: unknown[] = [fromMs, toMs];
    if (namespace !== null) {
      conditions.push("namespace = ?");
      params.push(namespace);
    }
    if (before !== null) {
      conditions.push("(created_at_ms, job_id) < (?, ?)");
      params.push(before.created_at_ms, before.job_id);
    }
    params.push(limit);

    const rows = this.storage.sql
      .exec<JobRow>(
        `SELECT ${JOB_SELECT_COLUMNS}
         FROM journal_jobs
         WHERE ${conditions.join(" AND ")}
         ORDER BY created_at_ms DESC, job_id DESC
         LIMIT ?`,
        ...params,
      )
      .toArray();

    return {
      jobs: rows.map((row) => this.toWireStoredJob(row, [], "", [], [])),
    };
  }

  async purge(raw: Record<string, unknown>): Promise<PurgeResponse> {
    const holderId = assertNonEmptyString(raw.holder_id, "holder_id");
    const term = assertPositiveInt(raw.term, "term");
    const beforeMs = assertNonNegativeInt(raw.before_ms, "before_ms");
    const rawBatchLimit =
      raw.batch_limit === undefined || raw.batch_limit === null
        ? MAX_PURGE_BATCH_LIMIT
        : assertPositiveInt(raw.batch_limit, "batch_limit");
    const batchLimit = Math.min(MAX_PURGE_BATCH_LIMIT, rawBatchLimit);
    this.ensureSchema();

    const syncResult = this.storage.transactionSync(() => {
      if (!this.hasValidLeaseSync(holderId, term, this.nowMs())) {
        return { fenced: true, deleted: 0, r2Keys: [] as string[] };
      }
      const victims = this.storage.sql
        .exec<{ job_id: string }>(
          `SELECT job_id
           FROM journal_jobs
           WHERE state IN ('succeeded', 'failed', 'cancelled')
             AND updated_at_ms < ?
           ORDER BY updated_at_ms ASC, job_id ASC
           LIMIT ?`,
          beforeMs,
          batchLimit,
        )
        .toArray();
      if (victims.length === 0) {
        return { fenced: false, deleted: 0, r2Keys: [] as string[] };
      }

      const r2Keys: string[] = [];
      for (const { job_id: jobId } of victims) {
        const keyRows = this.storage.sql
          .exec<{ r2_key: string }>(
            "SELECT r2_key FROM journal_objects WHERE job_id = ? AND r2_key IS NOT NULL",
            jobId,
          )
          .toArray();
        for (const { r2_key } of keyRows) {
          r2Keys.push(r2_key);
        }
        this.storage.sql.exec(
          "DELETE FROM journal_objects WHERE job_id = ?",
          jobId,
        );
        this.storage.sql.exec(
          "DELETE FROM journal_failed_workers WHERE job_id = ?",
          jobId,
        );
        this.storage.sql.exec(
          "DELETE FROM journal_jobs WHERE job_id = ?",
          jobId,
        );
      }

      return { fenced: false, deleted: victims.length, r2Keys };
    });

    if (syncResult.fenced) {
      return { fenced: true, deleted: 0 };
    }

    await this.deleteR2KeysInChunks(syncResult.r2Keys);
    return { fenced: false, deleted: syncResult.deleted };
  }

  private prepareOps(rawOps: unknown[]): PreparedOp[] {
    return rawOps.map((rawOp, opIdx) => {
      if (!rawOp || typeof rawOp !== "object" || Array.isArray(rawOp)) {
        throw new BadRequestError(`ops[${opIdx}] must be an object`);
      }
      const op = rawOp as Record<string, unknown>;
      const opType = assertNonEmptyString(op.type, `ops[${opIdx}].type`);
      switch (opType) {
        case "accept": {
          const jobId = assertNonEmptyString(
            op.job_id,
            `ops[${opIdx}].job_id`,
          );
          const namespace = assertString(
            op.namespace,
            `ops[${opIdx}].namespace`,
          );
          const specHash = assertString(
            op.spec_hash,
            `ops[${opIdx}].spec_hash`,
          );
          const specJson = assertString(
            op.spec_json,
            `ops[${opIdx}].spec_json`,
          );
          if (!Array.isArray(op.inputs)) {
            throw new BadRequestError(`ops[${opIdx}].inputs must be an array`);
          }
          const inputs = this.prepareWireObjects(
            jobId,
            "input",
            op.inputs,
            `ops[${opIdx}].inputs`,
          );
          const paramsBytes = decodeBase64(
            assertString(op.params_b64 ?? "", `ops[${opIdx}].params_b64`),
          );
          const paramsR2Key =
            paramsBytes.byteLength > this.r2SpillThresholdBytes
              ? objectR2Key(jobId, "params", 0)
              : null;
          const maxAttempts = assertNonNegativeInt(
            op.max_attempts,
            `ops[${opIdx}].max_attempts`,
          );
          const createdAtMs = assertNonNegativeInt(
            op.created_at_ms,
            `ops[${opIdx}].created_at_ms`,
          );
          return {
            type: "accept",
            jobId,
            namespace,
            specHash,
            specJson,
            inputs,
            paramsBytes,
            paramsR2Key,
            maxAttempts,
            createdAtMs,
          };
        }
        case "attempt_start": {
          return {
            type: "attempt_start",
            jobId: assertNonEmptyString(op.job_id, `ops[${opIdx}].job_id`),
            attemptNumber: assertPositiveInt(
              op.attempt_number,
              `ops[${opIdx}].attempt_number`,
            ),
            atMs: assertNonNegativeInt(op.at_ms, `ops[${opIdx}].at_ms`),
            attemptId: assertNullableString(
              op.attempt_id,
              `ops[${opIdx}].attempt_id`,
            ),
            workerId: assertNullableString(
              op.worker_id,
              `ops[${opIdx}].worker_id`,
            ),
          };
        }
        case "terminal": {
          const jobId = assertNonEmptyString(
            op.job_id,
            `ops[${opIdx}].job_id`,
          );
          const state = assertNonEmptyString(op.state, `ops[${opIdx}].state`);
          if (
            state !== "succeeded" &&
            state !== "failed" &&
            state !== "cancelled"
          ) {
            throw new BadRequestError(
              `ops[${opIdx}].state must be succeeded, failed, or cancelled`,
            );
          }
          if (!Array.isArray(op.outputs)) {
            throw new BadRequestError(`ops[${opIdx}].outputs must be an array`);
          }
          const outputs = this.prepareWireObjects(
            jobId,
            "output",
            op.outputs,
            `ops[${opIdx}].outputs`,
          );
          return {
            type: "terminal",
            jobId,
            state,
            outputs,
            errorJson: assertNullableString(
              op.error_json,
              `ops[${opIdx}].error_json`,
            ),
            attemptId: assertNullableString(
              op.attempt_id,
              `ops[${opIdx}].attempt_id`,
            ),
            workerId: assertNullableString(
              op.worker_id,
              `ops[${opIdx}].worker_id`,
            ),
            winningLeaseId: assertNullableString(
              op.winning_lease_id,
              `ops[${opIdx}].winning_lease_id`,
            ),
            outcomeDigest: assertNullableString(
              op.outcome_digest,
              `ops[${opIdx}].outcome_digest`,
            ),
            atMs: assertNonNegativeInt(op.at_ms, `ops[${opIdx}].at_ms`),
          };
        }
        case "worker_upsert":
          return {
            type: "worker_upsert",
            namespace: assertNonEmptyString(
              op.namespace,
              `ops[${opIdx}].namespace`,
            ),
            workerId: assertNonEmptyString(
              op.worker_id,
              `ops[${opIdx}].worker_id`,
            ),
            recordJson: assertNonEmptyString(
              op.record_json,
              `ops[${opIdx}].record_json`,
            ),
            updatedAtMs: assertNonNegativeInt(
              op.updated_at_ms,
              `ops[${opIdx}].updated_at_ms`,
            ),
          };
        case "worker_forget":
          return {
            type: "worker_forget",
            namespace: assertNonEmptyString(
              op.namespace,
              `ops[${opIdx}].namespace`,
            ),
            workerId: assertNonEmptyString(
              op.worker_id,
              `ops[${opIdx}].worker_id`,
            ),
          };
        case "probe":
          return { type: "probe" };
        default:
          throw new BadRequestError(`unknown commit op type '${opType}'`);
      }
    });
  }

  private prepareWireObjects(
    jobId: string,
    role: "input" | "output",
    rawObjects: unknown[],
    fieldPath: string,
  ): PreparedWireObject[] {
    return rawObjects.map((rawObj, idx) => {
      if (!rawObj || typeof rawObj !== "object" || Array.isArray(rawObj)) {
        throw new BadRequestError(`${fieldPath}[${idx}] must be an object`);
      }
      const obj = rawObj as Record<string, unknown>;
      const uri = assertNullableString(obj.uri, `${fieldPath}[${idx}].uri`);
      const metadataJson = normalizeMetadata(
        obj.metadata as Record<string, string> | undefined,
      );
      if (uri !== null) {
        return {
          idx,
          uri,
          bytes: null,
          r2Key: null,
          sizeBytes: null,
          metadataJson,
        };
      }
      const dataB64 = assertNullableString(
        obj.data_b64,
        `${fieldPath}[${idx}].data_b64`,
      );
      const bytes = decodeBase64(dataB64 ?? "");
      const r2Key =
        bytes.byteLength > this.r2SpillThresholdBytes
          ? objectR2Key(jobId, role, idx)
          : null;
      return {
        idx,
        uri: null,
        bytes,
        r2Key,
        sizeBytes: bytes.byteLength,
        metadataJson,
      };
    });
  }

  private applyPreparedOpSync(op: PreparedOp): void {
    switch (op.type) {
      case "accept": {
        this.storage.sql.exec(
          `INSERT INTO journal_jobs (
             job_id, namespace, state, spec_hash, spec_json,
             attempt_count, max_attempts, created_at_ms, updated_at_ms
           )
           VALUES (?, ?, 'queued', ?, ?, 0, ?, ?, ?)`,
          op.jobId,
          op.namespace,
          op.specHash,
          op.specJson,
          op.maxAttempts,
          op.createdAtMs,
          op.createdAtMs,
        );
        for (const obj of op.inputs) {
          this.insertObjectSync(op.jobId, "input", obj);
        }
        if (op.paramsBytes.byteLength > 0) {
          this.storage.sql.exec(
            `INSERT INTO journal_objects (
               job_id, role, idx, uri, content, r2_key, size_bytes, metadata_json
             )
             VALUES (?, 'params', 0, NULL, ?, ?, ?, '{}')`,
            op.jobId,
            op.paramsR2Key === null ? op.paramsBytes : null,
            op.paramsR2Key,
            op.paramsBytes.byteLength,
          );
        }
        break;
      }
      case "attempt_start": {
        const updated = this.storage.sql
          .exec<{ job_id: string }>(
            `UPDATE journal_jobs
             SET attempt_count = attempt_count + 1,
                 updated_at_ms = ?,
                 last_attempt_id = COALESCE(?, last_attempt_id),
                 last_worker_id = COALESCE(?, last_worker_id)
             WHERE job_id = ? AND state = 'queued'
             RETURNING job_id`,
            op.atMs,
            op.attemptId,
            op.workerId,
            op.jobId,
          )
          .toArray();
        if (updated.length !== 1) {
          throw new RollbackError(
            `job '${op.jobId}' is not open in the journal`,
          );
        }
        if (op.workerId !== null && op.workerId.length > 0) {
          this.storage.sql.exec(
            `INSERT OR IGNORE INTO journal_failed_workers (job_id, worker_id)
             VALUES (?, ?)`,
            op.jobId,
            op.workerId,
          );
        }
        break;
      }
      case "terminal": {
        const updated = this.storage.sql
          .exec<{ job_id: string }>(
            `UPDATE journal_jobs
             SET state = ?,
                 updated_at_ms = ?,
                 error_json = ?,
                 winning_lease_id = ?,
                 result_digest = ?,
                 last_attempt_id = ?,
                 last_worker_id = ?
             WHERE job_id = ? AND state = 'queued'
             RETURNING job_id`,
            op.state,
            op.atMs,
            op.errorJson,
            op.winningLeaseId,
            op.outcomeDigest,
            op.attemptId,
            op.workerId,
            op.jobId,
          )
          .toArray();
        if (updated.length !== 1) {
          throw new RollbackError(
            `job '${op.jobId}' is not open in the journal`,
          );
        }
        for (const obj of op.outputs) {
          this.insertObjectSync(op.jobId, "output", obj);
        }
        break;
      }
      case "worker_upsert": {
        this.storage.sql.exec(
          `INSERT INTO journal_workers (namespace, worker_id, record_json, updated_at_ms)
           VALUES (?, ?, ?, ?)
           ON CONFLICT(namespace, worker_id) DO UPDATE SET
             record_json = excluded.record_json,
             updated_at_ms = excluded.updated_at_ms`,
          op.namespace,
          op.workerId,
          op.recordJson,
          op.updatedAtMs,
        );
        break;
      }
      case "worker_forget": {
        this.storage.sql.exec(
          `DELETE FROM journal_workers WHERE namespace = ? AND worker_id = ?`,
          op.namespace,
          op.workerId,
        );
        break;
      }
      case "probe": {
        this.storage.sql.exec(
          "UPDATE journal_meta SET value = value WHERE key = 'schema_version'",
        );
        break;
      }
    }
  }

  private insertObjectSync(
    jobId: string,
    role: "input" | "output",
    obj: PreparedWireObject,
  ): void {
    this.storage.sql.exec(
      `INSERT INTO journal_objects (
         job_id, role, idx, uri, content, r2_key, size_bytes, metadata_json
       )
       VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
      jobId,
      role,
      obj.idx,
      obj.uri,
      obj.uri === null && obj.r2Key === null ? obj.bytes : null,
      obj.r2Key,
      obj.sizeBytes,
      obj.metadataJson,
    );
  }

  private hasValidLeaseSync(
    holderId: string,
    term: number,
    nowMs: number,
  ): boolean {
    const rows = this.storage.sql
      .exec<{ ok: number }>(
        `SELECT 1 AS ok
         FROM scheduler_lease
         WHERE id = 1 AND holder_id = ? AND generation = ? AND expires_at_ms > ?`,
        holderId,
        term,
        nowMs,
      )
      .toArray();
    return rows.length === 1;
  }

  private loadFailedWorkersSync(jobId: string): string[] {
    return this.storage.sql
      .exec<{ worker_id: string }>(
        "SELECT worker_id FROM journal_failed_workers WHERE job_id = ? ORDER BY worker_id ASC",
        jobId,
      )
      .toArray()
      .map((r) => r.worker_id);
  }

  private async materializeWireObject(obj: ObjectRow): Promise<WireObject> {
    const metadata = parseMetadataJson(obj.metadata_json);
    if (obj.uri !== null) {
      return { uri: obj.uri, data_b64: null, metadata };
    }
    const bytes =
      obj.r2_key !== null
        ? await this.readR2Required(obj.r2_key)
        : toUint8Array(obj.content);
    return {
      uri: null,
      data_b64: encodeBase64(bytes),
      metadata,
    };
  }

  private async materializeWireObjectPreview(
    obj: PreviewObjectRow,
    previewBytes: number,
  ): Promise<WireObjectPreview> {
    const metadata = parseMetadataJson(obj.metadata_json);
    if (obj.uri !== null) {
      return {
        uri: obj.uri,
        head_b64: null,
        size_bytes: null,
        metadata,
      };
    }
    let headBytes: Uint8Array;
    if (previewBytes === 0) {
      headBytes = new Uint8Array(0);
    } else if (obj.r2_key !== null) {
      headBytes = await this.readR2Range(obj.r2_key, previewBytes);
    } else {
      headBytes = toUint8Array(obj.head);
    }
    return {
      uri: null,
      head_b64: encodeBase64(headBytes),
      size_bytes: Number(obj.size_bytes ?? headBytes.byteLength),
      metadata,
    };
  }

  private async readR2Required(key: string): Promise<Uint8Array> {
    const obj = await this.r2.get(key);
    if (!obj) {
      throw new Error(`missing R2 journal object '${key}'`);
    }
    return new Uint8Array(await obj.arrayBuffer());
  }

  private async readR2Range(
    key: string,
    length: number,
  ): Promise<Uint8Array> {
    if (length <= 0) {
      return new Uint8Array(0);
    }
    const obj = await this.r2.get(key, {
      range: { offset: 0, length },
    });
    if (!obj) {
      throw new Error(`missing R2 journal object '${key}'`);
    }
    const bytes = new Uint8Array(await obj.arrayBuffer());
    return bytes.byteLength > length ? bytes.subarray(0, length) : bytes;
  }

  private async deleteR2KeysInChunks(keys: string[]): Promise<void> {
    for (let i = 0; i < keys.length; i += MAX_R2_DELETE_CHUNK) {
      const chunk = keys.slice(i, i + MAX_R2_DELETE_CHUNK);
      if (chunk.length > 0) {
        await this.r2.delete(chunk);
      }
    }
  }

  private toWireStoredJob(
    row: JobRow,
    inputs: WireObject[],
    paramsB64: string,
    outputs: WireObject[],
    failedWorkers: string[],
  ): WireStoredJob {
    return {
      job_id: row.job_id,
      namespace: row.namespace,
      state: row.state,
      spec_hash: row.spec_hash,
      spec_json: row.spec_json,
      inputs,
      params_b64: paramsB64,
      attempt_count: Number(row.attempt_count),
      max_attempts: Number(row.max_attempts),
      created_at_ms: Number(row.created_at_ms),
      updated_at_ms: Number(row.updated_at_ms),
      error_json: row.error_json,
      outputs,
      last_attempt_id: row.last_attempt_id,
      last_worker_id: row.last_worker_id,
      failed_workers: failedWorkers,
    };
  }
}
