import * as duckdb from "@duckdb/duckdb-wasm";
import wasmEh from "@duckdb/duckdb-wasm/dist/duckdb-eh.wasm?url";
import workerEh from "@duckdb/duckdb-wasm/dist/duckdb-browser-eh.worker.js?url";
import {
  boundedQuery, cellText, ENGINE_SETTINGS, JOB_SCHEMA, jobsToIPC, metadataToIPC,
  MAX_RESULT_BYTES, MAX_RESULT_COLUMNS, MAX_RESULT_ROWS, QUERY_TIMEOUT_MS, type QueryResult,
} from "./analytics-data";
import type { AnalyticsJob } from "./types";

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
    try {
      await engine.guard(async () => {
        await db.instantiate(wasmEh);
        await db.open({ path: ":memory:", maximumThreads: 1, query: { queryPollingInterval: 10 } });
        engine.connection = await db.connect();
        await engine.connection.query(ENGINE_SETTINGS);
        await engine.connection.query(JOB_SCHEMA);
      }, signal, 60_000);
      return engine;
    } catch (error) { engine.dispose(); throw error; }
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
        const encoder = new TextEncoder();
        let bytes = encoder.encode(JSON.stringify(result.columns)).byteLength;
        for await (const batch of reader) {
          for (let index = 0; index < batch.numRows; index++) {
            if (result.rows.length === MAX_RESULT_ROWS) { result.truncated = true; break; }
            const row = batch.schema.fields.map((_, column) => {
              let text = cellText(batch.getChildAt(column)?.get(index));
              if (text && text.length > 8_192) { text = text.slice(0, 8_192) + "…"; result.clippedCells++; }
              return text;
            });
            bytes += encoder.encode(JSON.stringify(row)).byteLength + 1;
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
