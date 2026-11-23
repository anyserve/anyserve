import { DatabaseSync } from "node:sqlite";
import { describe, expect, it, vi } from "vitest";

vi.mock("cloudflare:workers", () => {
  class DurableObject<Env = unknown> {
    protected ctx: unknown;
    protected env: Env;
    constructor(ctx: unknown, env: Env) {
      this.ctx = ctx;
      this.env = env;
    }
  }
  return { DurableObject };
});

import {
  MAX_TRACE_HEADER_BYTES,
  extractTraceHeaders,
  mergeTraceHeaders,
  specHash,
} from "../src/native/identity";
import {
  WS_READY_STATE_OPEN,
  type NativeDurableStorage,
  type NativeHost,
  type NativeSqlCursor,
  type NativeWebSocket,
  type NativeWebSocketPairResult,
} from "../src/native/host";
import { NativeEngine } from "../src/native/engine";
import {
  Durability,
  decodeExecutionEvent,
  decodeServerMessage,
  decodeSubmitResponse,
  decodeWsFrame,
  encodeAttemptUp,
  encodeCredit,
  encodeHello,
  encodeInvokeRequest,
  encodeSubmitRequest,
  encodeWorkerMessage,
} from "../src/native/proto";

class MemSqliteStorage implements NativeDurableStorage {
  readonly db = new DatabaseSync(":memory:");
  readonly sql = {
    exec: <T extends Record<string, any> = Record<string, any>>(
      query: string,
      ...bindings: unknown[]
    ): NativeSqlCursor<T> => {
      const stmt = this.db.prepare(query);
      const trimmed = query.trimStart().toUpperCase();
      if (trimmed.startsWith("SELECT") || trimmed.startsWith("PRAGMA")) {
        const rows = stmt.all(...(bindings as any[])) as T[];
        return {
          toArray: () => rows,
          one: () => rows[0] as T,
          [Symbol.iterator]: () => rows[Symbol.iterator](),
          rowsRead: rows.length,
          rowsWritten: 0,
        };
      }
      const info = stmt.run(...(bindings as any[]));
      return {
        toArray: () => [],
        one: () => ({}) as T,
        [Symbol.iterator]: () => [].values(),
        rowsRead: 0,
        rowsWritten: Number(info.changes ?? 0),
      };
    },
    get databaseSize(): number {
      return 4096;
    },
  };

  transactionSync<T>(fn: () => T): T {
    this.db.exec("BEGIN IMMEDIATE");
    try {
      const out = fn();
      this.db.exec("COMMIT");
      return out;
    } catch (err) {
      this.db.exec("ROLLBACK");
      throw err;
    }
  }
}

class MemWebSocket implements NativeWebSocket {
  readyState = WS_READY_STATE_OPEN;
  bufferedAmount = 0;
  private attachment: unknown = null;
  readonly sentFrames: Uint8Array[] = [];

  send(message: ArrayBuffer | ArrayBufferView | string): void {
    if (message instanceof Uint8Array) {
      this.sentFrames.push(new Uint8Array(message));
    } else if (ArrayBuffer.isView(message)) {
      this.sentFrames.push(
        new Uint8Array(message.buffer.slice(message.byteOffset, message.byteOffset + message.byteLength)),
      );
    } else if (message instanceof ArrayBuffer) {
      this.sentFrames.push(new Uint8Array(message));
    }
  }

  close(): void {
    this.readyState = 3;
  }

  serializeAttachment(attachment: unknown): void {
    this.attachment = structuredClone(attachment);
  }

  deserializeAttachment(): unknown {
    return this.attachment === null ? null : structuredClone(this.attachment);
  }
}

class MemUpgradeResponse extends Response {
  readonly serverSocket: MemWebSocket;
  private readonly upgradeStatus = 101;
  constructor(serverSocket: MemWebSocket) {
    super(null, { status: 200 });
    this.serverSocket = serverSocket;
  }
  override get status(): number {
    return this.upgradeStatus;
  }
}

function createMemEngine(): { engine: NativeEngine; openWs: (path: string, headers?: Record<string, string>) => Promise<MemWebSocket> } {
  const storage = new MemSqliteStorage();
  const sockets: MemWebSocket[] = [];
  let now = 1_700_000_000_000;
  let alarm: number | null = null;
  const host: NativeHost = {
    doId: "trace-test-do",
    storage,
    r2: null,
    env: {},
    acceptWebSocket: (ws) => {
      sockets.push(ws as MemWebSocket);
    },
    getWebSockets: () => sockets.filter((s) => s.readyState === WS_READY_STATE_OPEN),
    createWebSocketPair: (): NativeWebSocketPairResult => {
      const client = new MemWebSocket();
      const server = new MemWebSocket();
      return {
        client,
        server,
        toUpgradeResponse: () => new MemUpgradeResponse(server),
      };
    },
    nowMs: () => now,
    setAlarm: async (t) => {
      alarm = t;
    },
    getAlarm: async () => alarm,
    deleteAlarm: async () => {
      alarm = null;
    },
  };
  const engine = new NativeEngine(host);
  return {
    engine,
    openWs: async (path: string, headers?: Record<string, string>) => {
      const res = await engine.fetch(
        new Request(`https://anyserve.example.com${path}`, {
          method: "GET",
          headers: { Upgrade: "websocket", ...(headers ?? {}) },
        }),
      );
      expect(res.status).toBe(101);
      return (res as MemUpgradeResponse).serverSocket;
    },
  };
}

describe("W3C trace propagation and console event attempt_id", () => {
  it("extracts bounded trace headers and preserves explicit metadata precedence", () => {
    const oversized = "a".repeat(MAX_TRACE_HEADER_BYTES + 1);
    const headers = new Headers({
      traceparent: "  00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01  ",
      tracestate: "vendor=1",
      baggage: oversized,
      "x-request-id": "req-123",
      "x-ignored": "nope",
    });

    const extracted = extractTraceHeaders(headers);
    expect(extracted).toEqual({
      traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
      tracestate: "vendor=1",
      "x-request-id": "req-123",
    });

    const metadata: Record<string, string> = {
      traceparent: "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-1111111111111111-01",
      tracestate: "   ",
      custom: "keep",
    };
    mergeTraceHeaders(metadata, extracted);
    expect(metadata).toEqual({
      traceparent: "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-1111111111111111-01",
      tracestate: "vendor=1",
      "x-request-id": "req-123",
      custom: "keep",
    });
  });

  it("propagates HTTP trace headers on /rpc/v2/client/submit without altering spec_hash", async () => {
    const { engine } = createMemEngine();
    const traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    const baseSpec = {
      demand: { interface: "trace.demo.v1" },
      inputs: [{ body: { case: "inline" as const, value: new TextEncoder().encode("hi") } }],
      policy: { durability: Durability.DURABLE },
    };
    const expectedHash = specHash(baseSpec, Durability.DURABLE);
    const body = encodeSubmitRequest({ jobId: "trace-job-1", spec: baseSpec });

    const res = await engine.fetch(
      new Request("https://anyserve.example.com/rpc/v2/client/submit", {
        method: "POST",
        headers: {
          "content-type": "application/x-protobuf",
          traceparent,
          tracestate: "congo=t61rcWkgMzE",
          "x-request-id": "req-trace-1",
        },
        body: body.slice().buffer,
      }),
    );

    expect(res.status).toBe(200);
    const submitResp = decodeSubmitResponse(new Uint8Array(await res.arrayBuffer()));
    expect(submitResp.created).toBe(true);
    expect(submitResp.job?.specHash).toBe(expectedHash);
    expect(submitResp.job?.metadata.traceparent).toBe(traceparent);
    expect(submitResp.job?.metadata.tracestate).toBe("congo=t61rcWkgMzE");
    expect(submitResp.job?.metadata["x-request-id"]).toBe("req-trace-1");

    const detailResp = await engine.fetch(
      new Request("https://anyserve.example.com/api/jobs/trace-job-1"),
    );
    expect(detailResp.status).toBe(200);
    const detailJson = (await detailResp.json()) as {
      job: { metadata: Record<string, string> };
      events: Array<{ kind: string; attempt_id?: string }>;
    };
    expect(detailJson.job.metadata.traceparent).toBe(traceparent);
    expect(detailJson.events[0]?.kind).toBe("accepted");
    expect(detailJson.events[0]?.attempt_id).toBeUndefined();
  });

  it("propagates WebSocket upgrade trace headers on /ws/v2/client/invoke and emits attempt_id in console events", async () => {
    const { engine, openWs } = createMemEngine();

    const workerWs = await openWs("/ws/v2/worker/session");
    await engine.webSocketMessage(
      workerWs,
      encodeWorkerMessage({
        msg: {
          case: "hello",
          value: {
            workerId: "w-trace-1",
            interfaces: ["trace.ws.v1"],
            maxConcurrency: 1,
          },
        },
      }),
    );
    await engine.webSocketMessage(
      workerWs,
      encodeWorkerMessage({ msg: { case: "credit", value: { freeSlots: 1 } } }),
    );
    workerWs.sentFrames.length = 0;

    const traceparent = "00-11111111111111111111111111111111-2222222222222222-01";
    const invokeWs = await openWs("/ws/v2/client/invoke", {
      traceparent,
      "x-request-id": "ws-req-9",
    });
    await engine.webSocketMessage(
      invokeWs,
      encodeInvokeRequest({
        msg: {
          case: "start",
          value: {
            spec: {
              demand: { interface: "trace.ws.v1" },
              inputs: [
                { body: { case: "inline", value: new TextEncoder().encode("ping") } },
              ],
            },
          },
        },
      }),
    );

    const assignFrame = decodeWsFrame(workerWs.sentFrames[0]!);
    expect(assignFrame.kind).toBe("data");
    if (assignFrame.kind !== "data") throw new Error("unexpected error frame");
    const assignMsg = decodeServerMessage(assignFrame.payload);
    expect(assignMsg.msg?.case).toBe("assign");
    const assign = assignMsg.msg!.case === "assign" ? assignMsg.msg!.value : null!;

    await engine.webSocketMessage(
      workerWs,
      encodeWorkerMessage({
        msg: {
          case: "assignReply",
          value: { leaseId: assign.leaseId, accepted: true },
        },
      }),
    );

    const attemptWs = await openWs("/ws/v2/worker/attempt");
    await engine.webSocketMessage(
      attemptWs,
      encodeAttemptUp({
        msg: {
          case: "open",
          value: { leaseId: assign.leaseId, generation: assign.generation },
        },
      }),
    );
    await engine.webSocketMessage(
      attemptWs,
      encodeAttemptUp({
        msg: {
          case: "result",
          value: {
            index: 0,
            data: new TextEncoder().encode("pong"),
            last: true,
          },
        },
      }),
    );
    await engine.webSocketMessage(
      attemptWs,
      encodeAttemptUp({
        msg: {
          case: "finish",
          value: { outcome: { case: "succeeded", value: {} } },
        },
      }),
    );

    const firstEventFrame = decodeWsFrame(invokeWs.sentFrames[0]!);
    expect(firstEventFrame.kind).toBe("data");
    if (firstEventFrame.kind !== "data") throw new Error("unexpected error frame");
    const firstEvent = decodeExecutionEvent(firstEventFrame.payload);

    const detailResp = await engine.fetch(
      new Request(`https://anyserve.example.com/api/jobs/${encodeURIComponent(firstEvent.jobId)}`),
    );
    expect(detailResp.status).toBe(200);
    const detailJson = (await detailResp.json()) as {
      job: { metadata: Record<string, string> };
      events: Array<{ kind: string; attempt_id?: string }>;
    };
    expect(detailJson.job.metadata.traceparent).toBe(traceparent);
    expect(detailJson.job.metadata["x-request-id"]).toBe("ws-req-9");

    const assignedEv = detailJson.events.find((e) => e.kind === "assigned");
    const finishedEv = detailJson.events.find((e) => e.kind === "finished");
    expect(assignedEv?.attempt_id).toBe(assign.attemptId);
    expect(finishedEv?.attempt_id).toBe(assign.attemptId);
  });

  it("releases concurrencyKey on terminal success so serialized jobs dispatch sequentially", async () => {
    const { engine, openWs } = createMemEngine();

    const workerWs = await openWs("/ws/v2/worker/session");
    await engine.webSocketMessage(
      workerWs,
      encodeWorkerMessage({
        msg: {
          case: "hello",
          value: {
            workerId: "w-serial-1",
            interfaces: ["serial.v1"],
            maxConcurrency: 2,
          },
        },
      }),
    );
    await engine.webSocketMessage(
      workerWs,
      encodeWorkerMessage({ msg: { case: "credit", value: { freeSlots: 2 } } }),
    );
    workerWs.sentFrames.length = 0;

    for (const jobId of ["serial-job-1", "serial-job-2"]) {
      const body = encodeSubmitRequest({
        jobId,
        spec: {
          demand: { interface: "serial.v1" },
          inputs: [
            { body: { case: "inline", value: new TextEncoder().encode(jobId) } },
          ],
          policy: { durability: Durability.DURABLE, concurrencyKey: "serial-key" },
        },
      });
      const res = await engine.fetch(
        new Request("https://anyserve.example.com/rpc/v2/client/submit", {
          method: "POST",
          headers: { "content-type": "application/x-protobuf" },
          body: body.slice().buffer,
        }),
      );
      expect(res.status).toBe(200);
    }

    // Only one job may run concurrently for the shared concurrencyKey.
    expect(workerWs.sentFrames).toHaveLength(1);
    const frame1 = decodeWsFrame(workerWs.sentFrames[0]!);
    if (frame1.kind !== "data") throw new Error("expected data frame for first assign");
    const msg1 = decodeServerMessage(frame1.payload);
    expect(msg1.msg?.case).toBe("assign");
    const assign1 = msg1.msg!.case === "assign" ? msg1.msg!.value : null!;
    expect(assign1.jobId).toBe("serial-job-1");
    expect(engine.inFlightConcurrency.get("default")?.get("serial-key")).toBe(1);

    await engine.webSocketMessage(
      workerWs,
      encodeWorkerMessage({
        msg: {
          case: "assignReply",
          value: { leaseId: assign1.leaseId, accepted: true },
        },
      }),
    );

    const attemptWs1 = await openWs("/ws/v2/worker/attempt");
    await engine.webSocketMessage(
      attemptWs1,
      encodeAttemptUp({
        msg: {
          case: "open",
          value: { leaseId: assign1.leaseId, generation: assign1.generation },
        },
      }),
    );
    await engine.webSocketMessage(
      attemptWs1,
      encodeAttemptUp({
        msg: {
          case: "result",
          value: { index: 0, data: new TextEncoder().encode("ok-1"), last: true },
        },
      }),
    );
    workerWs.sentFrames.length = 0;
    await engine.webSocketMessage(
      attemptWs1,
      encodeAttemptUp({
        msg: { case: "finish", value: { outcome: { case: "succeeded", value: {} } } },
      }),
    );

    // Completing job-1 releases the concurrencyKey and immediately dispatches job-2.
    expect(workerWs.sentFrames).toHaveLength(1);
    const frame2 = decodeWsFrame(workerWs.sentFrames[0]!);
    if (frame2.kind !== "data") throw new Error("expected data frame for second assign");
    const msg2 = decodeServerMessage(frame2.payload);
    expect(msg2.msg?.case).toBe("assign");
    const assign2 = msg2.msg!.case === "assign" ? msg2.msg!.value : null!;
    expect(assign2.jobId).toBe("serial-job-2");
    expect(engine.inFlightConcurrency.get("default")?.get("serial-key")).toBe(1);

    await engine.webSocketMessage(
      workerWs,
      encodeWorkerMessage({
        msg: {
          case: "assignReply",
          value: { leaseId: assign2.leaseId, accepted: true },
        },
      }),
    );
    const attemptWs2 = await openWs("/ws/v2/worker/attempt");
    await engine.webSocketMessage(
      attemptWs2,
      encodeAttemptUp({
        msg: {
          case: "open",
          value: { leaseId: assign2.leaseId, generation: assign2.generation },
        },
      }),
    );
    await engine.webSocketMessage(
      attemptWs2,
      encodeAttemptUp({
        msg: {
          case: "result",
          value: { index: 0, data: new TextEncoder().encode("ok-2"), last: true },
        },
      }),
    );
    await engine.webSocketMessage(
      attemptWs2,
      encodeAttemptUp({
        msg: { case: "finish", value: { outcome: { case: "succeeded", value: {} } } },
      }),
    );

    expect(engine.inFlightConcurrency.size).toBe(0);
    for (const jobId of ["serial-job-1", "serial-job-2"]) {
      const detailResp = await engine.fetch(
        new Request(`https://anyserve.example.com/api/jobs/${jobId}`),
      );
      expect(detailResp.status).toBe(200);
      const detailJson = (await detailResp.json()) as { job: { state: string } };
      expect(detailJson.job.state).toBe("succeeded");
    }
  });
});
