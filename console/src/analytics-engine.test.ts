import { createRequire } from "node:module";
import { Worker as NodeWorker } from "node:worker_threads";
import { afterEach, expect, it, vi } from "vitest";
import { AnalyticsEngine } from "./analytics-engine";
import type { AnalyticsJob } from "./types";

// Exercise the production async client, query bounds and cancellation with a
// real WASM Worker. Only the browser Worker transport and asset URLs differ.
vi.mock("@duckdb/duckdb-wasm/dist/duckdb-eh.wasm?url", async () => {
  const { createRequire } = await import("node:module");
  return { default: createRequire(import.meta.url).resolve("@duckdb/duckdb-wasm/dist/duckdb-eh.wasm") };
});
const require = createRequire(import.meta.url);
class TestWorker {
  node = new NodeWorker(`
    const { parentPort, workerData } = require('node:worker_threads');
    globalThis.postMessage = (message, transfer) => parentPort.postMessage(message, transfer);
    parentPort.on('message', data => globalThis.onmessage({ data }));
    require(workerData);
  `, { eval: true, workerData: require.resolve("@duckdb/duckdb-wasm/dist/duckdb-node-eh.worker.cjs") });
  listeners = new Map<Function, (value: unknown) => void>();
  addEventListener(name: string, callback: (event: unknown) => void) {
    const listener = (value: unknown) => callback(name === "message" ? { data: value } : value);
    this.listeners.set(callback, listener); this.node.on(name, listener);
  }
  removeEventListener(name: string, callback: Function) {
    const listener = this.listeners.get(callback);
    if (listener) this.node.off(name, listener);
    this.listeners.delete(callback);
  }
  postMessage(value: unknown, transfer: ArrayBuffer[]) { this.node.postMessage(value, transfer); }
  terminate() { void this.node.terminate(); }
}
vi.stubGlobal("Worker", TestWorker);
let engine: AnalyticsEngine | null = null;
afterEach(() => { engine?.dispose(); engine = null; });
const job: AnalyticsJob = {
  job_id: "async-job", state: "succeeded", interface_name: "echo", source: null,
  created_at_ms: 1, updated_at_ms: 2, duration_ms: 1, current_attempt_id: null,
  latest_worker_id: null, attempt_count: 1, last_error: null, metadata: { tag: "测试" }, durability: "durable",
};

it("imports and queries through a real Worker, including result bounds and error recovery", async () => {
  const signal = new AbortController().signal;
  engine = await AnalyticsEngine.create(signal);
  await engine.append([job], signal);
  const result = await engine.query("SELECT job_id, duration_ms FROM jobs", signal);
  expect(result.columns).toEqual(["job_id", "duration_ms"]); expect(result.rows).toEqual([["async-job", "1"]]);
  expect((await engine.query("SELECT jobs.job_id FROM jobs JOIN job_metadata USING(job_id) WHERE key = 'tag' AND value = '测试'", signal)).rows).toEqual([["async-job"]]);
  await expect(engine.query("DELETE FROM jobs", signal)).rejects.toThrow();
  const limited = await engine.query("SELECT * FROM range(2000)", signal);
  expect(limited.rows).toHaveLength(1000); expect(limited.truncated).toBe(true);
  const after = await engine.query("SELECT count(*) AS n, NULL AS empty FROM jobs", signal);
  expect(after.rows).toEqual([["1", null]]);
  const wide = await engine.query("SELECT repeat('x', 10000) AS text", signal);
  expect(wide.clippedCells).toBe(1); expect(wide.rows[0][0]?.length).toBe(8193);
  const empty = await engine.query("SELECT job_id FROM jobs WHERE false", signal);
  expect(empty.columns).toEqual(["job_id"]); expect(empty.rows).toEqual([]);
  await expect(engine.query(`SELECT ${Array.from({ length: 101 }, (_, index) => `${index} AS c${index}`).join(",")}`, signal)).rejects.toThrow(/100 result columns/);
  const bytes = await engine.query("SELECT repeat('x', 8000) AS text FROM range(1000)", signal);
  expect(bytes.truncated).toBe(true); expect(bytes.rows.length).toBeLessThan(600);
}, 30_000);

it("terminates an expensive query immediately when cancelled", async () => {
  const controller = new AbortController();
  engine = await AnalyticsEngine.create(controller.signal);
  const query = engine.query("SELECT sum(ascii(md5(i::VARCHAR))) FROM range(1000000000) t(i)", controller.signal);
  const started = performance.now();
  const timer = setTimeout(() => controller.abort(), 50);
  try { await expect(query).rejects.toThrow(/Cancelled/); }
  finally { clearTimeout(timer); }
  expect(engine.closed).toBe(true);
  expect(performance.now() - started).toBeLessThan(2_000);
}, 30_000);

it("stops an unbounded query at the configured deadline", async () => {
  const signal = new AbortController().signal;
  engine = await AnalyticsEngine.create(signal);
  await expect(engine.query("SELECT sum(ascii(md5(i::VARCHAR))) FROM range(1000000000) t(i)", signal)).rejects.toThrow(/timed out/);
  expect(engine.closed).toBe(true);
}, 25_000);
