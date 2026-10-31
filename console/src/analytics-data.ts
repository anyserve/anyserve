import { Float64, Int32, Table, Utf8, tableToIPC, vectorFromArray } from "apache-arrow";
import type { AnalyticsJob } from "./types";

export const MAX_DATA_BYTES = 32 * 1024 * 1024;
export const MAX_RESULT_ROWS = 1_000;
export const MAX_RESULT_COLUMNS = 100;
export const MAX_RESULT_BYTES = 4 * 1024 * 1024;
export const QUERY_TIMEOUT_MS = 15_000;

export const JOB_SCHEMA = `CREATE TABLE jobs (
  job_id VARCHAR, state VARCHAR, interface_name VARCHAR, source VARCHAR,
  created_at_ms BIGINT, updated_at_ms BIGINT, duration_ms BIGINT,
  current_attempt_id VARCHAR, latest_worker_id VARCHAR, attempt_count INTEGER,
  last_error VARCHAR, metadata VARCHAR, durability VARCHAR
);
CREATE TABLE job_metadata (job_id VARCHAR, key VARCHAR, value VARCHAR)`;

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
  { name: "Recent jobs", sql: `SELECT job_id, state, interface_name, latest_worker_id,
       duration_ms, attempt_count, created_at_ms
FROM jobs
ORDER BY created_at_ms DESC, job_id DESC` },
  { name: "Failures and retries", sql: `SELECT job_id, state, interface_name, latest_worker_id,
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
  const string = (key: "job_id" | "state" | "interface_name" | "source" | "current_attempt_id" | "latest_worker_id" | "last_error" | "durability") =>
    vectorFromArray(jobs.map(job => job[key]), new Utf8());
  const time = (key: "created_at_ms" | "updated_at_ms" | "duration_ms") =>
    vectorFromArray(jobs.map(job => job[key]), new Float64());
  return new Table({
    job_id: string("job_id"), state: string("state"), interface_name: string("interface_name"), source: string("source"),
    created_at_ms: time("created_at_ms"), updated_at_ms: time("updated_at_ms"), duration_ms: time("duration_ms"),
    current_attempt_id: string("current_attempt_id"), latest_worker_id: string("latest_worker_id"),
    attempt_count: vectorFromArray(jobs.map(job => job.attempt_count), new Int32()),
    last_error: string("last_error"),
    metadata: vectorFromArray(jobs.map(job => JSON.stringify(job.metadata)), new Utf8()),
    durability: string("durability"),
  });
}

export function jobsToIPC(jobs: AnalyticsJob[]): Uint8Array {
  return tableToIPC(jobsToArrow(jobs), "stream");
}

export function metadataToIPC(jobs: AnalyticsJob[]): Uint8Array | null {
  const rows = jobs.flatMap(job => Object.entries(job.metadata).map(([key, value]) => ({ job_id: job.job_id, key, value })));
  if (!rows.length) return null;
  return tableToIPC(new Table({
    job_id: vectorFromArray(rows.map(row => row.job_id), new Utf8()),
    key: vectorFromArray(rows.map(row => row.key), new Utf8()),
    value: vectorFromArray(rows.map(row => row.value), new Utf8()),
  }), "stream");
}

// Preparing this derived-table SELECT rejects DDL and multiple statements in
// DuckDB's parser. Do not concatenate it into query(), which accepts scripts.
export function boundedQuery(sql: string): string {
  const query = sql.trim().replace(/;\s*$/, "");
  if (!query || new TextEncoder().encode(query).byteLength > 32_768) throw new Error("Enter a SELECT query of up to 32 KiB.");
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
