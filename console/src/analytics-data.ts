import { Float64, Int32, Table, Utf8, makeData, makeVector, tableToIPC } from "apache-arrow";
import type { AnalyticsJob } from "./types";

const utf8Encoder = new TextEncoder();
const UTF8_TYPE = new Utf8();
const FLOAT64_TYPE = new Float64();
const INT32_TYPE = new Int32();

/** Zero-allocation exact UTF-8 byte length (matches TextEncoder). */
export function utf8ByteLen(str: string): number {
  const len = str.length;
  let bytes = len;
  for (let i = 0; i < len; i++) {
    const c = str.charCodeAt(i);
    if (c < 0x80) continue;
    if (c < 0x800) bytes += 1;
    else if (c >= 0xd800 && c <= 0xdbff && i + 1 < len && (str.charCodeAt(i + 1) & 0xfc00) === 0xdc00) {
      bytes += 2;
      i += 1;
    } else {
      bytes += 2;
    }
  }
  return bytes;
}

/** Fast byte-size estimate of a JSON-serialized `AnalyticsJob` without `JSON.stringify` + `TextEncoder.encode`. */
export function estimateJobJsonBytes(job: AnalyticsJob): number {
  let bytes =
    260 +
    utf8ByteLen(job.job_id) +
    utf8ByteLen(job.state) +
    utf8ByteLen(job.interface_name) +
    (job.namespace ? utf8ByteLen(job.namespace) : 7) +
    (job.source ? utf8ByteLen(job.source) : 4) +
    (job.durability ? utf8ByteLen(job.durability) : 4) +
    (job.current_attempt_id ? utf8ByteLen(job.current_attempt_id) : 4) +
    (job.latest_worker_id ? utf8ByteLen(job.latest_worker_id) : 4) +
    (job.last_error ? utf8ByteLen(job.last_error) : 4);
  const meta = job.metadata;
  for (const k in meta) {
    bytes += utf8ByteLen(k) + utf8ByteLen(meta[k]!) + 6;
  }
  return bytes;
}

/** Fast byte-size estimate of a JSON-serialized result row without `JSON.stringify` + `TextEncoder.encode`. */
export function estimateRowJsonBytes(row: readonly (string | null)[]): number {
  let bytes = 3; // "[" + "]" + ","
  for (let i = 0; i < row.length; i++) {
    const cell = row[i];
    bytes += cell === null ? 5 : utf8ByteLen(cell) + 3;
  }
  return bytes;
}

/**
 * Direct Arrow `Utf8` column builder using pre-sized typed arrays, an inline ASCII fast path
 * (zero per-string `TextEncoder` calls or temporary `Uint8Array`s), and lazy `nullBitmap`
 * allocation only when a null value actually occurs.
 */
class Utf8ColumnBuilder {
  private offsets: Int32Array;
  private bytes: Uint8Array;
  private bytePos = 0;
  private len = 0;
  private nullCount = 0;
  private nullBitmap: Uint8Array | undefined;

  constructor(initialRows: number, initialByteCap: number) {
    this.offsets = new Int32Array(Math.max(1, initialRows) + 1);
    this.bytes = new Uint8Array(Math.max(64, initialByteCap));
  }

  private ensureRowCapacity(nextLen: number): void {
    if (nextLen < this.offsets.length) return;
    const nextCap = Math.max(nextLen + 1, this.offsets.length * 2);
    const grown = new Int32Array(nextCap);
    grown.set(this.offsets);
    this.offsets = grown;
    if (this.nullBitmap !== undefined) {
      const nextBitmap = new Uint8Array((nextCap + 7) >>> 3);
      nextBitmap.set(this.nullBitmap);
      this.nullBitmap = nextBitmap;
    }
  }

  private ensureByteCapacity(needed: number): void {
    if (needed <= this.bytes.byteLength) return;
    let nextCap = this.bytes.byteLength * 2;
    while (nextCap < needed) nextCap *= 2;
    const grown = new Uint8Array(nextCap);
    grown.set(this.bytes.subarray(0, this.bytePos));
    this.bytes = grown;
  }

  append(value: string | null | undefined): void {
    const idx = this.len;
    this.ensureRowCapacity(idx + 1);
    if (value == null) {
      if (this.nullBitmap === undefined) {
        const bm = new Uint8Array((this.offsets.length + 7) >>> 3);
        const fullBytes = idx >>> 3;
        if (fullBytes > 0) bm.fill(0xff, 0, fullBytes);
        const remBits = idx & 7;
        if (remBits > 0) bm[fullBytes] = (1 << remBits) - 1;
        this.nullBitmap = bm;
      }
      this.nullCount += 1;
      this.len = idx + 1;
      this.offsets[this.len] = this.bytePos;
      return;
    }

    if (this.nullBitmap !== undefined) {
      this.nullBitmap[idx >>> 3] |= 1 << (idx & 7);
    }

    const n = value.length;
    if (n > 0) {
      this.ensureByteCapacity(this.bytePos + n);
      const buf = this.bytes;
      let p = this.bytePos;
      let i = 0;
      for (; i < n; i++) {
        const c = value.charCodeAt(i);
        if (c >= 0x80) break;
        buf[p++] = c;
      }
      if (i === n) {
        this.bytePos = p;
      } else {
        // Non-ASCII fallback: ensure worst-case 3 UTF-8 bytes per remaining UTF-16 unit.
        const maxRemain = (n - i) * 3;
        this.ensureByteCapacity(p + maxRemain);
        this.bytePos = p + utf8Encoder.encodeInto(value.slice(i), this.bytes.subarray(p)).written;
      }
    }
    this.len = idx + 1;
    this.offsets[this.len] = this.bytePos;
  }

  finish() {
    const length = this.len;
    const offsets =
      this.offsets.length === length + 1
        ? this.offsets
        : this.offsets.subarray(0, length + 1);
    const data = this.bytes.subarray(0, this.bytePos);
    const nullBitmap =
      this.nullBitmap !== undefined
        ? this.nullBitmap.subarray(0, (length + 7) >>> 3)
        : undefined;
    return makeVector(
      makeData({
        type: UTF8_TYPE,
        length,
        nullCount: this.nullCount,
        data,
        valueOffsets: offsets,
        nullBitmap,
      }),
    );
  }
}

function buildFloat64Vector(
  values: Float64Array,
  nullBitmap: Uint8Array | undefined,
  nullCount: number,
) {
  return makeVector(
    makeData({
      type: FLOAT64_TYPE,
      length: values.length,
      nullCount,
      data: values,
      nullBitmap,
    }),
  );
}

function buildInt32Vector(values: Int32Array) {
  return makeVector(
    makeData({
      type: INT32_TYPE,
      length: values.length,
      nullCount: 0,
      data: values,
    }),
  );
}

function serializeMetadataJson(meta: Record<string, string>): string {
  for (const _ in meta) {
    return JSON.stringify(meta);
  }
  return "{}";
}

export const MAX_DATA_BYTES = 32 * 1024 * 1024;
export const MAX_RESULT_ROWS = 1_000;
export const MAX_RESULT_COLUMNS = 100;
export const MAX_RESULT_BYTES = 4 * 1024 * 1024;
export const QUERY_TIMEOUT_MS = 15_000;

export const JOB_SCHEMA = `CREATE TABLE jobs (
  job_id VARCHAR, state VARCHAR, interface_name VARCHAR, namespace VARCHAR, source VARCHAR,
  created_at_ms BIGINT, updated_at_ms BIGINT, duration_ms BIGINT,
  current_attempt_id VARCHAR, latest_worker_id VARCHAR, attempt_count INTEGER,
  last_error VARCHAR, metadata VARCHAR, durability VARCHAR
);
CREATE TABLE job_metadata (job_id VARCHAR, namespace VARCHAR, key VARCHAR, value VARCHAR)`;

export const ENGINE_SETTINGS = `
  SET memory_limit = '256MB';
  SET threads = 1;
  SET preserve_insertion_order = false;
  SET max_temp_directory_size = '0B';
  SET autoinstall_known_extensions = false;
  SET autoload_known_extensions = false;
  SET enable_external_access = false;
  SET lock_configuration = true;
`;

export const QUERY_PRESETS = [
  { name: "Recent jobs", sql: `SELECT job_id, namespace, state, interface_name, latest_worker_id,
       duration_ms, attempt_count, created_at_ms
FROM jobs
ORDER BY created_at_ms DESC, job_id DESC` },
  { name: "Failures and retries", sql: `SELECT job_id, namespace, state, interface_name, latest_worker_id,
       attempt_count, last_error
FROM jobs
WHERE state = 'failed' OR attempt_count > 1
ORDER BY created_at_ms DESC` },
  { name: "Interface performance", sql: `SELECT interface_name, count(*) AS jobs,
       count(*) FILTER (WHERE state = 'failed') AS failed,
       round(avg(duration_ms), 2) AS avg_ms,
       quantile_cont(duration_ms, 0.95) AS p95_ms,
       max(duration_ms) AS max_ms
FROM jobs
GROUP BY interface_name
ORDER BY jobs DESC` },
  { name: "Workers", sql: `SELECT latest_worker_id, state, count(*) AS jobs,
       round(avg(duration_ms), 2) AS avg_ms
FROM jobs
GROUP BY latest_worker_id, state
ORDER BY jobs DESC` },
  { name: "Minute buckets", sql: `SELECT floor(created_at_ms / 60000) * 60000 AS minute_ms,
       state, count(*) AS jobs
FROM jobs
GROUP BY minute_ms, state
ORDER BY minute_ms DESC, state` },
  { name: "Metadata values", sql: `SELECT key, value, count(*) AS jobs
FROM job_metadata
GROUP BY key, value
ORDER BY jobs DESC` },
] as const;

// Explicit Arrow types also handle empty or entirely-null columns consistently
// across batches. Timestamp milliseconds fit exactly in JavaScript numbers.
export function jobsToArrow(jobs: AnalyticsJob[]): Table {
  const n = jobs.length;
  const jobId = new Utf8ColumnBuilder(n, n * 24);
  const state = new Utf8ColumnBuilder(n, n * 10);
  const interfaceName = new Utf8ColumnBuilder(n, n * 20);
  const namespace = new Utf8ColumnBuilder(n, n * 10);
  const source = new Utf8ColumnBuilder(n, n * 12);
  const currentAttemptId = new Utf8ColumnBuilder(n, Math.max(64, n * 8));
  const latestWorkerId = new Utf8ColumnBuilder(n, n * 16);
  const lastError = new Utf8ColumnBuilder(n, Math.max(64, n * 8));
  const metadata = new Utf8ColumnBuilder(n, n * 32);
  const durability = new Utf8ColumnBuilder(n, n * 10);

  const createdAt = new Float64Array(n);
  const updatedAt = new Float64Array(n);
  const duration = new Float64Array(n);
  let durationNullBitmap: Uint8Array | undefined;
  let durationNullCount = 0;
  const attemptCount = new Int32Array(n);

  for (let i = 0; i < n; i++) {
    const job = jobs[i]!;
    jobId.append(job.job_id);
    state.append(job.state);
    interfaceName.append(job.interface_name);
    namespace.append(job.namespace ?? "default");
    source.append(job.source);
    createdAt[i] = job.created_at_ms;
    updatedAt[i] = job.updated_at_ms;
    const dur = job.duration_ms;
    if (dur == null) {
      if (durationNullBitmap === undefined) {
        durationNullBitmap = new Uint8Array((n + 7) >>> 3);
        const fullBytes = i >>> 3;
        if (fullBytes > 0) durationNullBitmap.fill(0xff, 0, fullBytes);
        const remBits = i & 7;
        if (remBits > 0) durationNullBitmap[fullBytes] = (1 << remBits) - 1;
      }
      durationNullCount += 1;
    } else {
      duration[i] = dur;
      if (durationNullBitmap !== undefined) {
        durationNullBitmap[i >>> 3] |= 1 << (i & 7);
      }
    }
    currentAttemptId.append(job.current_attempt_id);
    latestWorkerId.append(job.latest_worker_id);
    attemptCount[i] = job.attempt_count;
    lastError.append(job.last_error);
    metadata.append(serializeMetadataJson(job.metadata));
    durability.append(job.durability);
  }

  return new Table({
    job_id: jobId.finish(),
    state: state.finish(),
    interface_name: interfaceName.finish(),
    namespace: namespace.finish(),
    source: source.finish(),
    created_at_ms: buildFloat64Vector(createdAt, undefined, 0),
    updated_at_ms: buildFloat64Vector(updatedAt, undefined, 0),
    duration_ms: buildFloat64Vector(duration, durationNullBitmap, durationNullCount),
    current_attempt_id: currentAttemptId.finish(),
    latest_worker_id: latestWorkerId.finish(),
    attempt_count: buildInt32Vector(attemptCount),
    last_error: lastError.finish(),
    metadata: metadata.finish(),
    durability: durability.finish(),
  });
}

export function jobsToIPC(jobs: AnalyticsJob[]): Uint8Array {
  return tableToIPC(jobsToArrow(jobs), "stream");
}

export function metadataToIPC(jobs: AnalyticsJob[]): Uint8Array | null {
  let rowCount = 0;
  for (let i = 0; i < jobs.length; i++) {
    const meta = jobs[i]!.metadata;
    for (const _ in meta) rowCount += 1;
  }
  if (rowCount === 0) return null;

  const jobId = new Utf8ColumnBuilder(rowCount, rowCount * 24);
  const namespace = new Utf8ColumnBuilder(rowCount, rowCount * 10);
  const keyCol = new Utf8ColumnBuilder(rowCount, rowCount * 12);
  const valueCol = new Utf8ColumnBuilder(rowCount, rowCount * 16);

  for (let i = 0; i < jobs.length; i++) {
    const job = jobs[i]!;
    const meta = job.metadata;
    const ns = job.namespace ?? "default";
    for (const key in meta) {
      jobId.append(job.job_id);
      namespace.append(ns);
      keyCol.append(key);
      valueCol.append(meta[key]!);
    }
  }

  return tableToIPC(
    new Table({
      job_id: jobId.finish(),
      namespace: namespace.finish(),
      key: keyCol.finish(),
      value: valueCol.finish(),
    }),
    "stream",
  );
}

// Preparing this derived-table SELECT rejects DDL and multiple statements in
// DuckDB's parser. Do not concatenate it into query(), which accepts scripts.
export function boundedQuery(sql: string): string {
  const query = sql.trim().replace(/;\s*$/, "");
  if (!query || utf8ByteLen(query) > 32_768) throw new Error("Enter a SELECT query of up to 32 KiB.");
  return `SELECT * FROM (\n${query}\n) AS console_query LIMIT ${MAX_RESULT_ROWS + 1}`;
}

export interface QueryResult {
  columns: string[];
  rows: (string | null)[][];
  truncated: boolean;
  clippedCells: number;
  elapsedMs: number;
}

export function cellText(value: unknown): string | null {
  if (value == null) return null;
  if (typeof value === "object") {
    return JSON.stringify(value, (_, item) => typeof item === "bigint" ? item.toString() : item);
  }
  return String(value);
}

export function resultCsv(result: QueryResult): string {
  const quote = (value: string | null) => {
    // Spreadsheet applications can execute formulas in CSV cells.
    const text = value != null && /^[=+@\-\t\r]/.test(value) ? `'${value}` : value ?? "";
    return `"${text.replace(/"/g, '""')}"`;
  };
  return [result.columns, ...result.rows].map(row => row.map(quote).join(",")).join("\r\n");
}
