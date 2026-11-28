import * as duckdb from "@duckdb/duckdb-wasm";
import wasmEh from "@duckdb/duckdb-wasm/dist/duckdb-eh.wasm?url";
import workerEh from "@duckdb/duckdb-wasm/dist/duckdb-browser-eh.worker.js?url";
import {
  boundedQuery, cellText, ENGINE_SETTINGS, estimateRowJsonBytes, JOB_SCHEMA, jobsToIPC, metadataToIPC,
  MAX_RESULT_BYTES, MAX_RESULT_COLUMNS, MAX_RESULT_ROWS, QUERY_TIMEOUT_MS, type QueryResult,
} from "./analytics-data";
import type { AnalyticsJob } from "./types";

export function resolveDuckdbWasmUrl(overrideUrl = import.meta.env.VITE_DUCKDB_WASM_URL): string {
  const trimmed = typeof overrideUrl === "string" ? overrideUrl.trim() : "";
  return trimmed.length > 0 ? trimmed : wasmEh;
}

export function isGzippedWasmUrl(wasmUrl: string): boolean {
  return wasmUrl.split(/[?#]/, 1)[0].endsWith(".wasm.bin");
}

export interface PreparedWasmAsset {
  url: string;
  cleanup: () => void;
}

export async function prepareDuckdbWasmUrl(
  signal: AbortSignal,
  wasmUrl = resolveDuckdbWasmUrl(),
): Promise<PreparedWasmAsset> {
  if (!isGzippedWasmUrl(wasmUrl)) {
    return { url: wasmUrl, cleanup: () => {} };
  }
  signal.throwIfAborted();
  const response = await fetch(wasmUrl, { signal });
  if (!response.ok) {
    throw new Error(`Failed to fetch DuckDB WASM asset (${response.status} ${response.statusText || "HTTP error"}).`);
  }
  if (!response.body) {
    throw new Error("DuckDB WASM asset response had an empty body.");
  }
  if (typeof DecompressionStream === "undefined") {
    throw new Error("Explore requires a browser with DecompressionStream support. Update your browser and try again.");
  }
  const decompressedStream = response.body.pipeThrough(
    new DecompressionStream("gzip") as unknown as ReadableWritablePair<Uint8Array, Uint8Array>,
  );
  const bytes = await new Response(decompressedStream).arrayBuffer();
  signal.throwIfAborted();
  const blob = new Blob([bytes], { type: "application/wasm" });
  const objectUrl = URL.createObjectURL(blob);
  let revoked = false;
  return {
    url: objectUrl,
    cleanup: () => {
      if (revoked) return;
      revoked = true;
      URL.revokeObjectURL(objectUrl);
    },
  };
}

export async function instantiateDuckdb(
  db: Pick<duckdb.AsyncDuckDB, "instantiate">,
  signal: AbortSignal,
  wasmUrl = resolveDuckdbWasmUrl(),
): Promise<void> {
  const prepared = await prepareDuckdbWasmUrl(signal, wasmUrl);
  try {
    await db.instantiate(prepared.url);
  } finally {
    prepared.cleanup();
  }
}

export class AnalyticsEngine {
  private connection!: duckdb.AsyncDuckDBConnection;
  private stopped = false;

  private constructor(private db: duckdb.AsyncDuckDB, private worker: Worker) {}

  static async create(signal: AbortSignal): Promise<AnalyticsEngine> {
    // The EH bundle provides reliable SQL error recovery and avoids shipping
    // an additional 40 MiB legacy bundle. Unsupported browsers keep the other
    // Console pages available.
    const platform = await duckdb.getPlatformFeatures();
    if (!platform.wasmExceptions) throw new Error("Explore requires a browser with WebAssembly exception handling. Update your browser and try again.");
    signal.throwIfAborted();
    const worker = new Worker(workerEh);
    const db = new duckdb.AsyncDuckDB(new duckdb.VoidLogger(), worker);
    const engine = new AnalyticsEngine(db, worker);
    let cleanupWasmUrl = () => {};
    try {
      await engine.guard(async () => {
        const prepared = await prepareDuckdbWasmUrl(signal);
        cleanupWasmUrl = prepared.cleanup;
        await db.instantiate(prepared.url);
        cleanupWasmUrl();
        await db.open({ path: ":memory:", maximumThreads: 1, query: { queryPollingInterval: 10 } });
        engine.connection = await db.connect();
        await engine.connection.query(ENGINE_SETTINGS);
        await engine.connection.query(JOB_SCHEMA);
      }, signal, 60_000);
      return engine;
    } catch (error) { engine.dispose(); throw error; }
    finally { cleanupWasmUrl(); }
  }

  get closed(): boolean { return this.stopped; }

  dispose(): void {
    if (this.stopped) return;
    this.stopped = true;
    this.worker.terminate();
    void this.db.terminate().catch(() => {});
  }

  private async guard<T>(work: () => Promise<T>, signal: AbortSignal, timeout = QUERY_TIMEOUT_MS): Promise<T> {
    signal.throwIfAborted();
    if (this.stopped) throw new Error("Load data to start a new query session.");
    let timer: ReturnType<typeof setTimeout>;
    let abort: () => void;
    const interrupted = new Promise<never>((_, reject) => {
      abort = () => { this.dispose(); reject(new DOMException("Cancelled. Load data to resume.", "AbortError")); };
      signal.addEventListener("abort", abort, { once: true });
      timer = setTimeout(() => {
        this.dispose();
        reject(new Error("Query timed out. Load data again and use a narrower query."));
      }, timeout);
    });
    try { return await Promise.race([work(), interrupted]); }
    finally { clearTimeout(timer!); signal.removeEventListener("abort", abort!); }
  }

  async append(jobs: AnalyticsJob[], signal: AbortSignal): Promise<void> {
    await this.guard(async () => {
      await this.connection.insertArrowFromIPCStream(jobsToIPC(jobs), { name: "jobs", create: false });
      const metadata = metadataToIPC(jobs);
      if (metadata) await this.connection.insertArrowFromIPCStream(metadata, { name: "job_metadata", create: false });
    }, signal);
  }

  async query(sql: string, signal: AbortSignal): Promise<QueryResult> {
    return this.guard(async () => {
      const started = performance.now();
      const statement = await this.connection.prepare(boundedQuery(sql));
      try {
        const reader = await statement.send();
        await reader.open();
        if (reader.schema.fields.length > MAX_RESULT_COLUMNS) {
          await this.connection.cancelSent();
          throw new Error(`Select at most ${MAX_RESULT_COLUMNS} result columns.`);
        }
        const result: QueryResult = { columns: reader.schema.fields.map(field => field.name), rows: [], truncated: false, clippedCells: 0, elapsedMs: 0 };
        const colCount = result.columns.length;
        let bytes = estimateRowJsonBytes(result.columns);
        for await (const batch of reader) {
          const cols = Array.from({ length: colCount }, (_, c) => batch.getChildAt(c));
          for (let index = 0; index < batch.numRows; index++) {
            if (result.rows.length === MAX_RESULT_ROWS) { result.truncated = true; break; }
            const row: (string | null)[] = new Array(colCount);
            for (let column = 0; column < colCount; column++) {
              let text = cellText(cols[column]?.get(index));
              if (text && text.length > 8_192) { text = text.slice(0, 8_192) + "…"; result.clippedCells++; }
              row[column] = text;
            }
            bytes += estimateRowJsonBytes(row);
            if (bytes > MAX_RESULT_BYTES) { result.truncated = true; break; }
            result.rows.push(row);
          }
          if (result.truncated) { await this.connection.cancelSent(); break; }
        }
        result.elapsedMs = performance.now() - started;
        return result;
      } finally { if (!this.stopped) await statement.close(); }
    }, signal);
  }
}
