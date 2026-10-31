// Run with Node 24+: npm run bench:analytics. No browser automation or native
// DuckDB: this measures the same WASM module and Arrow IPC used by Explore.
import { createRequire } from "node:module";
import { cpus, platform, arch } from "node:os";
import { performance } from "node:perf_hooks";
import { writeFileSync } from "node:fs";
import { boundedQuery, ENGINE_SETTINGS, JOB_SCHEMA, jobsToIPC, metadataToIPC, QUERY_PRESETS } from "../src/analytics-data.ts";
import type { AnalyticsJob } from "../src/types.ts";

const require = createRequire(import.meta.url);
const duckdb = require("@duckdb/duckdb-wasm/blocking");
if (!(await duckdb.getPlatformFeatures()).wasmExceptions) throw new Error("WebAssembly exception handling required");
const init = performance.now();
const db = await duckdb.createDuckDB({
  mvp: { mainModule: require.resolve("@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm"), mainWorker: "" },
  eh: { mainModule: require.resolve("@duckdb/duckdb-wasm/dist/duckdb-eh.wasm"), mainWorker: "" },
}, new duckdb.VoidLogger(), duckdb.NODE_RUNTIME);
await db.instantiate(); db.open({ path: ":memory:" });
const connection = db.connect(); connection.query(ENGINE_SETTINGS);
const initializationMs = performance.now() - init;
const measurements: object[] = [];
try {
  for (const rows of [10_000, 100_000]) {
    for (let repeat = 1; repeat <= 3; repeat++) {
      connection.query("DROP TABLE IF EXISTS jobs; DROP TABLE IF EXISTS job_metadata"); connection.query(JOB_SCHEMA);
      const started = performance.now();
      let ipcBytes = 0;
      for (let offset = 0; offset < rows; offset += 250) {
        const jobs: AnalyticsJob[] = Array.from({ length: Math.min(250, rows - offset) }, (_, index) => {
          const i = offset + index;
          return {
            job_id: `job-${String(i).padStart(8, "0")}`, state: i % 10 ? "succeeded" : "failed",
            interface_name: `interface-${i % 20}`, source: "benchmark", durability: "durable",
            created_at_ms: 1_700_000_000_000 + i, updated_at_ms: 1_700_000_000_010 + i + i % 100,
            duration_ms: 10 + i % 100, current_attempt_id: null, latest_worker_id: `worker-${i % 32}`,
            attempt_count: i % 10 ? 1 : 2, last_error: i % 10 ? null : "synthetic failure", metadata: { region: `region-${i % 4}` },
          };
        });
        const ipc = jobsToIPC(jobs); ipcBytes += ipc.byteLength;
        connection.insertArrowFromIPCStream(ipc, { name: "jobs", create: false });
        const metadata = metadataToIPC(jobs)!; ipcBytes += metadata.byteLength;
        connection.insertArrowFromIPCStream(metadata, { name: "job_metadata", create: false });
      }
      const importMs = performance.now() - started;
      const count = Number(connection.query("SELECT count(*) AS n FROM jobs").get(0).n);
      if (count !== rows) throw new Error(`Expected ${rows} jobs, loaded ${count}`);
      const queries = QUERY_PRESETS.map(preset => {
        const started = performance.now();
        const statement = connection.prepare(boundedQuery(preset.sql));
        try { return { name: preset.name, rows: statement.query().numRows, elapsed_ms: performance.now() - started }; }
        finally { statement.close(); }
      });
      measurements.push({ rows, repeat, import_ms: importMs, arrow_ipc_bytes: ipcBytes, queries, node_rss_mib: process.memoryUsage().rss / 1048576 });
    }
  }
} finally { connection.close(); await db.reset(); }
const result = {
  environment: { node: process.version, platform: platform(), arch: arch(), cpu: cpus()[0]?.model, logical_cpus: cpus().length },
  scope: "DuckDB-WASM EH under Node, same 250-row Arrow IPC batches as Explore; synthetic metadata; three repetitions; import includes data generation/serialization; query includes preparation and materialization capped at 1001 rows; excludes network, browser Worker messaging and rendering. 100k-row case exercises the engine independently of the browser's 32 MiB JSON dataset cap.",
  initialization_ms: initializationMs, measurements,
};
const json = JSON.stringify(result, null, 2) + "\n";
if (process.argv[2]) writeFileSync(process.argv[2], json);
else process.stdout.write(json);
