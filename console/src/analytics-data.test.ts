import { createRequire } from "node:module";
import { afterAll, beforeAll, expect, it } from "vitest";
import { createDuckDB, NODE_RUNTIME, VoidLogger, type DuckDBConnection } from "@duckdb/duckdb-wasm/dist/duckdb-node-blocking";
import { boundedQuery, ENGINE_SETTINGS, JOB_SCHEMA, jobsToIPC, metadataToIPC, QUERY_PRESETS, resultCsv } from "./analytics-data";
import type { AnalyticsJob } from "./types";

const require = createRequire(import.meta.url);
let db: Awaited<ReturnType<typeof createDuckDB>>;
let connection: DuckDBConnection;
const job = (id: string, state: AnalyticsJob["state"] = "succeeded"): AnalyticsJob => ({
  job_id: id, state, interface_name: "echo", source: null, created_at_ms: 1_700_000_000_001,
  updated_at_ms: 1_700_000_000_026, duration_ms: 25, current_attempt_id: null,
  latest_worker_id: null, attempt_count: 1, last_error: null, metadata: { tag: "测试" }, durability: "durable",
});

beforeAll(async () => {
  db = await createDuckDB({ mvp: { mainModule: require.resolve("@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm"), mainWorker: "" }, eh: { mainModule: require.resolve("@duckdb/duckdb-wasm/dist/duckdb-eh.wasm"), mainWorker: "" } }, new VoidLogger(), NODE_RUNTIME);
  await db.instantiate();
  db.open({ path: ":memory:" });
  connection = db.connect();
  connection.query(ENGINE_SETTINGS);
  connection.query(JOB_SCHEMA);
  connection.insertArrowFromIPCStream(jobsToIPC([job("A"), job("B", "failed")]), { name: "jobs", create: false });
  connection.insertArrowFromIPCStream(jobsToIPC([{ ...job("C"), source: "test", latest_worker_id: "worker-1", duration_ms: null }]), { name: "jobs", create: false });
  connection.insertArrowFromIPCStream(metadataToIPC([job("A"), job("B"), job("C")])!, { name: "job_metadata", create: false });
}, 30_000);
afterAll(async () => { connection?.close(); await db?.reset(); });

it("runs real WASM queries over typed Arrow batches with nulls and metadata", () => {
  const row = connection.query("SELECT * FROM jobs WHERE job_id = 'C'").get(0)!;
  expect(row.source).toBe("test"); expect(row.latest_worker_id).toBe("worker-1"); expect(row.duration_ms).toBeNull();
  expect(Number(row.created_at_ms)).toBe(1_700_000_000_001);
  expect(JSON.parse(row.metadata)).toEqual({ tag: "测试" });
  expect(connection.query("SELECT jobs.job_id FROM jobs JOIN job_metadata USING(job_id) WHERE key = 'tag' AND value = '测试'").numRows).toBe(3);
  for (const preset of QUERY_PRESETS) {
    const statement = connection.prepare(boundedQuery(preset.sql));
    expect(statement.query().numRows).toBeGreaterThan(0); statement.close();
  }
  const rows = connection.query("SELECT state, count(*) AS n FROM jobs GROUP BY state ORDER BY state");
  expect(rows.toArray().map(row => [row.state, Number(row.n)])).toEqual([["failed", 1], ["succeeded", 2]]);
});

it("rejects mutation, multiple statements, and external data access", () => {
  for (const sql of ["DELETE FROM jobs", "SELECT * FROM jobs); DELETE FROM jobs; SELECT * FROM (jobs", "SELECT * FROM read_csv('https://example.com/private.csv')"]) {
    expect(() => { const stmt = connection.prepare(boundedQuery(sql)); try { stmt.query(); } finally { stmt.close(); } }).toThrow();
  }
  expect(Number(connection.query("SELECT count(*) AS n FROM jobs").get(0)!.n)).toBe(3);
  const stmt = connection.prepare(boundedQuery("WITH a AS (SELECT * FROM jobs) SELECT * FROM a;"));
  expect(stmt.query().numRows).toBe(3); stmt.close();
});

it("bounds result rows and safely encodes CSV exports", () => {
  const stmt = connection.prepare(boundedQuery("SELECT * FROM range(100000)"));
  expect(stmt.query().numRows).toBe(1001); stmt.close();
  expect(resultCsv({ columns: ["value"], rows: [["=1+1"], ['a,"b'], [null]], elapsedMs: 0, truncated: false, clippedCells: 0 }))
    .toBe('"value"\r\n"\'=1+1"\r\n"a,""b"\r\n""');
});
