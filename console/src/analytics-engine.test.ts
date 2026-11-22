import { createRequire } from "node:module";
import { Worker as NodeWorker } from "node:worker_threads";
import zlib from "node:zlib";
import * as duckdb from "@duckdb/duckdb-wasm";
import wasmEh from "@duckdb/duckdb-wasm/dist/duckdb-eh.wasm?url";
import workerEh from "@duckdb/duckdb-wasm/dist/duckdb-browser-eh.worker.js?url";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  AnalyticsEngine,
  instantiateDuckdb,
  isGzippedWasmUrl,
  resolveDuckdbWasmUrl,
} from "./analytics-engine";
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
afterEach(() => {
  engine?.dispose();
  engine = null;
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
  vi.stubGlobal("Worker", TestWorker);
});
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

describe("WASM URL resolution and .wasm.bin gzip decompression", () => {
  it("resolves default wasmEh and supports VITE_DUCKDB_WASM_URL overrides", () => {
    expect(resolveDuckdbWasmUrl()).toBe(wasmEh);
    expect(resolveDuckdbWasmUrl("   ")).toBe(wasmEh);

    vi.stubEnv("VITE_DUCKDB_WASM_URL", "  https://cdn.example.com/assets/duckdb-eh-custom.wasm.bin?v=2  ");
    expect(resolveDuckdbWasmUrl()).toBe("https://cdn.example.com/assets/duckdb-eh-custom.wasm.bin?v=2");

    expect(isGzippedWasmUrl("/assets/duckdb-eh-abc123.wasm.bin")).toBe(true);
    expect(isGzippedWasmUrl("https://cdn.example.com/duckdb-eh.wasm.bin?v=1#frag")).toBe(true);
    expect(isGzippedWasmUrl("/assets/duckdb-eh-abc123.wasm")).toBe(false);
    expect(isGzippedWasmUrl("/assets/duckdb-eh.wasm?v=1#ignored.wasm.bin")).toBe(false);
  });

  it("fetches .wasm.bin, decompresses gzip, passes an application/wasm Blob URL, and revokes it in finally", async () => {
    const rawWasm = new Uint8Array([0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00, 42, 99]);
    const gzipped = zlib.gzipSync(rawWasm, { level: 9 });
    const wasmUrl = "/assets/duckdb-eh-CfdE9-rk.wasm.bin?cache=1#eh";
    const controller = new AbortController();

    const fetchSpy = vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(gzipped, { status: 200, headers: { "Content-Type": "application/octet-stream" } }),
    );
    let capturedBlob: Blob | null = null;
    const createUrlSpy = vi.spyOn(URL, "createObjectURL").mockImplementation((obj) => {
      capturedBlob = obj as Blob;
      return "blob:https://console.example/duckdb-wasm-1";
    });
    const revokeUrlSpy = vi.spyOn(URL, "revokeObjectURL").mockImplementation(() => {});

    const callOrder: string[] = [];
    const db = {
      instantiate: vi.fn(async (url: string) => {
        callOrder.push(`instantiate:${url}`);
        expect(revokeUrlSpy).not.toHaveBeenCalled();
        return null;
      }),
    };
    revokeUrlSpy.mockImplementation((url) => {
      callOrder.push(`revoke:${url}`);
    });

    await instantiateDuckdb(db, controller.signal, wasmUrl);

    expect(fetchSpy).toHaveBeenCalledWith(wasmUrl, { signal: controller.signal });
    expect(createUrlSpy).toHaveBeenCalledTimes(1);
    expect(capturedBlob).toBeInstanceOf(Blob);
    expect(capturedBlob!.type).toBe("application/wasm");
    expect(new Uint8Array(await capturedBlob!.arrayBuffer())).toEqual(rawWasm);
    expect(db.instantiate).toHaveBeenCalledWith("blob:https://console.example/duckdb-wasm-1");
    expect(revokeUrlSpy).toHaveBeenCalledWith("blob:https://console.example/duckdb-wasm-1");
    expect(callOrder).toEqual([
      "instantiate:blob:https://console.example/duckdb-wasm-1",
      "revoke:blob:https://console.example/duckdb-wasm-1",
    ]);
  });

  it("passes non-.wasm.bin URLs straight to db.instantiate without fetch or Blob URLs", async () => {
    const fetchSpy = vi.spyOn(globalThis, "fetch");
    const createUrlSpy = vi.spyOn(URL, "createObjectURL");
    const db = { instantiate: vi.fn(async () => null) };

    await instantiateDuckdb(db, new AbortController().signal, "/assets/duckdb-eh-plain.wasm?v=1");

    expect(fetchSpy).not.toHaveBeenCalled();
    expect(createUrlSpy).not.toHaveBeenCalled();
    expect(db.instantiate).toHaveBeenCalledWith("/assets/duckdb-eh-plain.wasm?v=1");
  });

  it("revokes Blob URL in finally even when db.instantiate fails", async () => {
    const gzipped = zlib.gzipSync(new Uint8Array([0, 97, 115, 109]));
    vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response(gzipped, { status: 200 }));
    vi.spyOn(URL, "createObjectURL").mockReturnValue("blob:https://console.example/failing-wasm");
    const revokeUrlSpy = vi.spyOn(URL, "revokeObjectURL").mockImplementation(() => {});
    const db = {
      instantiate: vi.fn(async () => {
        throw new Error("WebAssembly.instantiate failed");
      }),
    };

    await expect(
      instantiateDuckdb(db, new AbortController().signal, "/assets/duckdb-eh-fail.wasm.bin"),
    ).rejects.toThrow("WebAssembly.instantiate failed");

    expect(revokeUrlSpy).toHaveBeenCalledTimes(1);
    expect(revokeUrlSpy).toHaveBeenCalledWith("blob:https://console.example/failing-wasm");
  });

  it("rejects on non-OK HTTP responses, empty bodies, or invalid gzip without leaking object URLs", async () => {
    const createUrlSpy = vi.spyOn(URL, "createObjectURL");
    const revokeUrlSpy = vi.spyOn(URL, "revokeObjectURL");
    const db = { instantiate: vi.fn(async () => null) };
    const signal = new AbortController().signal;

    vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(
      new Response("Not Found", { status: 404, statusText: "Not Found" }),
    );
    await expect(instantiateDuckdb(db, signal, "/assets/missing.wasm.bin")).rejects.toThrow(/404/);

    vi.spyOn(globalThis, "fetch").mockResolvedValueOnce({
      ok: true,
      status: 200,
      statusText: "OK",
      body: null,
    } as Response);
    await expect(instantiateDuckdb(db, signal, "/assets/empty.wasm.bin")).rejects.toThrow(/empty body/i);

    vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(
      new Response(new Uint8Array([1, 2, 3, 4]), { status: 200 }),
    );
    await expect(instantiateDuckdb(db, signal, "/assets/corrupt.wasm.bin")).rejects.toThrow();

    expect(db.instantiate).not.toHaveBeenCalled();
    expect(createUrlSpy).not.toHaveBeenCalled();
    expect(revokeUrlSpy).not.toHaveBeenCalled();
  });

  it("uses VITE_DUCKDB_WASM_URL in AnalyticsEngine.create, keeps worker same-origin, and revokes on abort", async () => {
    const workerUrls: string[] = [];
    const fakeWorker = {
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
      postMessage: vi.fn(),
      terminate: vi.fn(),
    };
    vi.stubGlobal(
      "Worker",
      vi.fn(function (this: unknown, url: string) {
        workerUrls.push(url);
        return fakeWorker;
      }),
    );

    const rawWasm = new Uint8Array([0x00, 0x61, 0x73, 0x6d, 1, 2, 3, 4]);
    const gzipped = zlib.gzipSync(rawWasm);
    vi.stubEnv("VITE_DUCKDB_WASM_URL", "https://edge.example.com/assets/duckdb-eh-override.wasm.bin?v=9");
    vi.spyOn(globalThis, "fetch").mockImplementation(async () => new Response(gzipped, { status: 200 }));

    const blobs: Blob[] = [];
    vi.spyOn(URL, "createObjectURL").mockImplementation((blob) => {
      blobs.push(blob as Blob);
      return `blob:https://console.example/wasm-${blobs.length}`;
    });
    const revokeUrlSpy = vi.spyOn(URL, "revokeObjectURL").mockImplementation(() => {});

    const instantiateSpy = vi
      .spyOn(duckdb.AsyncDuckDB.prototype, "instantiate")
      .mockResolvedValue(null);
    vi.spyOn(duckdb.AsyncDuckDB.prototype, "open").mockResolvedValue(undefined);
    const mockConn = { query: vi.fn().mockResolvedValue(undefined) } as unknown as duckdb.AsyncDuckDBConnection;
    vi.spyOn(duckdb.AsyncDuckDB.prototype, "connect").mockResolvedValue(mockConn);
    vi.spyOn(duckdb.AsyncDuckDB.prototype, "terminate").mockResolvedValue(undefined);

    const controller = new AbortController();
    engine = await AnalyticsEngine.create(controller.signal);

    expect(workerUrls).toEqual([workerEh]);
    expect(blobs).toHaveLength(1);
    expect(blobs[0].type).toBe("application/wasm");
    expect(instantiateSpy).toHaveBeenCalledWith("blob:https://console.example/wasm-1");
    expect(revokeUrlSpy).toHaveBeenCalledTimes(1);
    expect(revokeUrlSpy).toHaveBeenCalledWith("blob:https://console.example/wasm-1");

    // Verify abort while instantiate is pending still terminates worker and revokes the Blob URL.
    instantiateSpy.mockImplementationOnce(() => new Promise(() => {}));
    const abortController = new AbortController();
    const pendingCreate = AnalyticsEngine.create(abortController.signal);
    await vi.waitFor(() => expect(revokeUrlSpy).toHaveBeenCalledTimes(1));
    await vi.waitFor(() => expect(instantiateSpy).toHaveBeenCalledTimes(2));
    abortController.abort();

    await expect(pendingCreate).rejects.toThrow(/Cancelled/);
    expect(fakeWorker.terminate).toHaveBeenCalled();
    expect(revokeUrlSpy).toHaveBeenCalledTimes(2);
    expect(revokeUrlSpy).toHaveBeenLastCalledWith("blob:https://console.example/wasm-2");
  });
});
