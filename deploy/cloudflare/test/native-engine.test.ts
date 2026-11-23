import { DatabaseSync } from "node:sqlite";
import { describe, expect, it, vi } from "vitest";
import { GrpcCode } from "../src/native/auth";
import { CONN_SEQ_BLOCK, NativeEngine, RUNTIME_CODES, rendezvousHash64 } from "../src/native/engine";
import { utf8ByteLen } from "../src/native/utf8";
import { parseSpecJson, serializeSpecJson } from "../src/native/store";
import {
  ATTACHMENT_MAX_BYTES,
  ISOLATE_MEMORY_CAP_BYTES,
  LARGE_SEND_CAP_BYTES,
  UPLOAD_SEGMENT_BYTES,
  WELCOME_MAX_CHUNK_BYTES,
  readSocketAttachment,
  writeSocketAttachment,
  type SocketAttachment,
  WS_MAX_ENCODED_BYTES,
  WS_READY_STATE_CLOSED,
  WS_READY_STATE_OPEN,
  type NativeDurableStorage,
  type NativeEnvConfig,
  type NativeHost,
  type NativeR2Bucket,
  type NativeR2ListResult,
  type NativeR2ObjectBody,
  type NativeR2Range,
  type NativeRuntimeOverrides,
  type NativeSqlCursor,
  type NativeWebSocket,
  type NativeWebSocketPairResult,
} from "../src/native/host";
import {
  DEFAULT_NAMESPACE,
  encodeScopedId,
  outcomeDigest,
  specHash,
} from "../src/native/identity";
import * as pb from "../src/native/proto";

// Counts WorkerMessage decodes so tests can assert the engine decodes each inbound control frame ONCE.
const decodeCounters = vi.hoisted(() => ({ workerMessage: 0 }));
vi.mock("../src/native/proto", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../src/native/proto")>();
  return {
    ...actual,
    decodeWorkerMessage: (bytes: Uint8Array) => {
      decodeCounters.workerMessage += 1;
      return actual.decodeWorkerMessage(bytes);
    },
  };
});

// Mock `cloudflare:workers` so `src/native-do.ts` (`AnyserveNativeDO`) can be exercised directly in Node 24 Vitest.
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

const utf8Encoder = new TextEncoder();
const utf8Decoder = new TextDecoder();

// ---------------------------------------------------------------------------
// 1. Node 24 SQLite + R2 + Hibernatable WebSocket Simulation Harness
//    Enforces Cloudflare DO SQLite invariants (sync-only txn, <=100 params,
//    <=100 KB SQL, <=2 MiB row/BLOB) and tracks table + secondary-index writes.
// ---------------------------------------------------------------------------

class UpgradeResponse extends Response {
  readonly webSocket: FakeClientWebSocket;
  private readonly upgradeStatus: number;

  constructor(client: FakeClientWebSocket, headers?: HeadersInit) {
    super(null, { status: 200, headers });
    this.webSocket = client;
    this.upgradeStatus = 101;
  }

  override get status(): number {
    return this.upgradeStatus;
  }
}

export class TrackedSqliteStorage implements NativeDurableStorage {
  readonly db: DatabaseSync;
  private txnDepth = 0;
  private savepointSeq = 0;
  totalRowsWritten = 0;
  totalRowsRead = 0;
  simulatedBaseDbBytes = 0;

  readonly sql: {
    exec: <T extends Record<string, any> = Record<string, any>>(
      query: string,
      ...bindings: unknown[]
    ) => NativeSqlCursor<T>;
    readonly databaseSize: number;
  };

  constructor(db?: DatabaseSync) {
    this.db = db ?? new DatabaseSync(":memory:");
    const self = this;
    this.sql = {
      exec<T extends Record<string, any> = Record<string, any>>(
        query: string,
        ...bindings: unknown[]
      ): NativeSqlCursor<T> {
        return self.execInternal<T>(query, bindings);
      },
      get databaseSize(): number {
        const pc = Number(
          (self.db.prepare("PRAGMA page_count").get() as { page_count?: number })
            ?.page_count ?? 0,
        );
        const ps = Number(
          (self.db.prepare("PRAGMA page_size").get() as { page_size?: number })
            ?.page_size ?? 4096,
        );
        return self.simulatedBaseDbBytes + pc * ps;
      },
    };
  }

  private execInternal<T extends Record<string, any>>(
    query: string,
    bindings: unknown[],
  ): NativeSqlCursor<T> {
    const sqlBytes = utf8Encoder.encode(query).byteLength;
    if (sqlBytes > 100 * 1024) {
      throw new Error(`SQL query exceeds 100 KB limit (${sqlBytes} bytes)`);
    }
    if (bindings.length > 100) {
      throw new Error(
        `SQL query exceeds 100 bound parameters (${bindings.length})`,
      );
    }
    const normalizedBindings = bindings.map((b) => {
      if (b === undefined) return null;
      if (b instanceof Uint8Array) {
        if (b.byteLength > 2 * 1024 * 1024) {
          throw new Error(
            `SQLite BLOB binding exceeds 2 MiB DO row limit (${b.byteLength} bytes)`,
          );
        }
        return b;
      }
      if (b instanceof ArrayBuffer) {
        if (b.byteLength > 2 * 1024 * 1024) {
          throw new Error(
            `SQLite ArrayBuffer binding exceeds 2 MiB DO row limit (${b.byteLength} bytes)`,
          );
        }
        return new Uint8Array(b);
      }
      if (typeof b === "string") {
        const len = utf8Encoder.encode(b).byteLength;
        if (len > 2 * 1024 * 1024) {
          throw new Error(`SQLite TEXT binding exceeds 2 MiB DO row limit`);
        }
      }
      return b as number | bigint | string | Uint8Array | null;
    });

    const trimmed = query.trim();
    const upper = trimmed.toUpperCase();
    const isSelect = upper.startsWith("SELECT") || upper.startsWith("PRAGMA");
    const isReturning = /\bRETURNING\b/i.test(trimmed);
    const isCreateOrDrop =
      upper.startsWith("CREATE ") || upper.startsWith("DROP ");

    if (isCreateOrDrop) {
      this.db.exec(query);
      return {
        toArray: () => [],
        rowsRead: 0,
        rowsWritten: 1,
      };
    }

    const stmt = this.db.prepare(query);
    let rows: T[] = [];
    let baseChanges = 0;

    if (isSelect) {
      rows = stmt.all(...normalizedBindings) as T[];
    } else if (isReturning) {
      rows = stmt.all(...normalizedBindings) as T[];
      baseChanges = rows.length;
    } else {
      const info = stmt.run(...normalizedBindings);
      baseChanges = Number(info.changes ?? 0);
    }

    // Account secondary index maintenance on `native_jobs` (2 full indexes + 1 partial winner index)
    let indexWrites = 0;
    if (!isSelect && baseChanges > 0 && /\bnative_jobs\b/i.test(trimmed)) {
      if (upper.startsWith("INSERT")) {
        indexWrites = 2 * baseChanges;
      } else if (upper.startsWith("DELETE")) {
        indexWrites = 2 * baseChanges;
      } else if (upper.startsWith("UPDATE")) {
        if (/\bstate\s*=/i.test(trimmed) || /\bupdated_at_ms\s*=/i.test(trimmed)) {
          indexWrites += baseChanges;
        }
        if (/\bwinning_lease_id\s*=/i.test(trimmed)) {
          indexWrites += baseChanges;
        }
      }
    }

    const rowsWritten = isSelect ? 0 : baseChanges + indexWrites;
    const rowsRead = isSelect ? Math.max(1, rows.length) : baseChanges;

    this.totalRowsWritten += rowsWritten;
    this.totalRowsRead += rowsRead;

    return {
      toArray: () => rows,
      rowsRead,
      rowsWritten,
      [Symbol.iterator]: () => rows[Symbol.iterator](),
    };
  }

  transactionSync<T>(closure: () => T): T {
    const sp = `sp_${++this.savepointSeq}`;
    const savedWrites = this.totalRowsWritten;
    this.txnDepth += 1;
    this.db.exec(`SAVEPOINT ${sp}`);
    try {
      const result = closure();
      if (
        result !== null &&
        (typeof result === "object" || typeof result === "function") &&
        typeof (result as { then?: unknown }).then === "function"
      ) {
        throw new Error(
          "transactionSync closure must be synchronous and must not return a Promise",
        );
      }
      this.db.exec(`RELEASE SAVEPOINT ${sp}`);
      return result;
    } catch (err) {
      this.db.exec(`ROLLBACK TO SAVEPOINT ${sp}`);
      this.db.exec(`RELEASE SAVEPOINT ${sp}`);
      this.totalRowsWritten = savedWrites;
      throw err;
    } finally {
      this.txnDepth -= 1;
    }
  }
}

interface StoredR2Entry {
  bytes: Uint8Array;
  uploadedAtMs: number;
}

export class SimulatedR2Bucket implements NativeR2Bucket {
  readonly objects = new Map<string, StoredR2Entry>();
  private readonly nowFn: () => number;
  putCount = 0;
  getCount = 0;
  rangeGetCalls: Array<{ key: string; offset: number; length?: number }> = [];
  deleteCount = 0;
  listCount = 0;
  beforePutHook: ((key: string, bytes: Uint8Array) => Promise<void> | void) | null =
    null;

  constructor(nowFn: () => number) {
    this.nowFn = nowFn;
  }

  seedObject(key: string, bytes: Uint8Array, uploadedAtMs: number): void {
    this.objects.set(key, {
      bytes: new Uint8Array(bytes),
      uploadedAtMs,
    });
  }

  async put(
    key: string,
    value: ReadableStream | ArrayBuffer | ArrayBufferView | string | Blob | null,
  ): Promise<unknown> {
    let bytes: Uint8Array;
    if (value === null) {
      bytes = new Uint8Array(0);
    } else if (value instanceof Uint8Array) {
      bytes = new Uint8Array(value);
    } else if (value instanceof ArrayBuffer) {
      bytes = new Uint8Array(value.slice(0));
    } else if (ArrayBuffer.isView(value)) {
      bytes = new Uint8Array(
        value.buffer.slice(value.byteOffset, value.byteOffset + value.byteLength),
      );
    } else if (typeof value === "string") {
      bytes = utf8Encoder.encode(value);
    } else if (value instanceof Blob) {
      bytes = new Uint8Array(await value.arrayBuffer());
    } else {
      const reader = value.getReader();
      const chunks: Uint8Array[] = [];
      let total = 0;
      while (true) {
        const { done, value: chunk } = await reader.read();
        if (done) break;
        const u = chunk instanceof Uint8Array ? chunk : new Uint8Array(chunk);
        chunks.push(u);
        total += u.byteLength;
      }
      bytes = new Uint8Array(total);
      let pos = 0;
      for (const c of chunks) {
        bytes.set(c, pos);
        pos += c.byteLength;
      }
    }

    if (this.beforePutHook) {
      await this.beforePutHook(key, bytes);
    }
    this.putCount += 1;
    this.objects.set(key, {
      bytes,
      uploadedAtMs: this.nowFn(),
    });
    return { key, size: bytes.byteLength };
  }

  async get(
    key: string,
    options?: { range?: NativeR2Range },
  ): Promise<NativeR2ObjectBody | null> {
    this.getCount += 1;
    const entry = this.objects.get(key);
    if (!entry) return null;
    let slice = entry.bytes;
    if (options?.range) {
      const off = options.range.offset ?? 0;
      const len =
        options.range.length !== undefined
          ? options.range.length
          : Math.max(0, entry.bytes.byteLength - off);
      this.rangeGetCalls.push({ key, offset: off, length: options.range.length });
      slice = entry.bytes.subarray(off, Math.min(entry.bytes.byteLength, off + len));
    }
    const copy = new Uint8Array(slice.byteLength);
    copy.set(slice);
    return {
      arrayBuffer: async () => copy.buffer,
    };
  }

  async delete(keys: string | string[]): Promise<void> {
    this.deleteCount += 1;
    const arr = Array.isArray(keys) ? keys : [keys];
    for (const k of arr) {
      this.objects.delete(k);
    }
  }

  async list(options?: {
    prefix?: string;
    cursor?: string;
    limit?: number;
  }): Promise<NativeR2ListResult> {
    this.listCount += 1;
    const prefix = options?.prefix ?? "";
    const afterKey = options?.cursor;
    const sortedKeys = [...this.objects.keys()]
      .filter((k) => k.startsWith(prefix) && (afterKey === undefined || k > afterKey))
      .sort();
    const limit = options?.limit ?? 1000;
    const pageKeys = sortedKeys.slice(0, limit);
    const truncated = pageKeys.length < sortedKeys.length;
    const nextCursor =
      truncated && pageKeys.length > 0
        ? pageKeys[pageKeys.length - 1]
        : undefined;
    return {
      objects: pageKeys.map((k) => {
        const e = this.objects.get(k)!;
        return {
          key: k,
          size: e.bytes.byteLength,
          uploaded: new Date(e.uploadedAtMs),
        };
      }),
      truncated,
      cursor: nextCursor,
    };
  }
}

export class FakeServerWebSocket implements NativeWebSocket {
  readyState = WS_READY_STATE_OPEN;
  bufferedAmount = 0;
  peakBufferedAmount = 0;
  acceptedTags: string[] = [];
  private serializedAttachmentJson: string | null = null;
  peer!: FakeClientWebSocket;
  platform!: SimulatedNativePlatform;
  closeCalls: Array<{ code?: number; reason?: string }> = [];

  send(message: ArrayBuffer | ArrayBufferView | string): void {
    if (this.readyState !== WS_READY_STATE_OPEN) {
      throw new Error("WebSocket is not open");
    }
    let frame: Uint8Array;
    if (typeof message === "string") {
      frame = utf8Encoder.encode(message);
    } else if (message instanceof Uint8Array) {
      frame = new Uint8Array(message);
    } else if (message instanceof ArrayBuffer) {
      frame = new Uint8Array(message.slice(0));
    } else {
      frame = new Uint8Array(
        message.buffer.slice(
          message.byteOffset,
          message.byteOffset + message.byteLength,
        ),
      );
    }

    if (this.peer.autoDrain) {
      this.peer.receivedFrames.push(frame);
    } else {
      this.bufferedAmount += frame.byteLength;
      if (this.bufferedAmount > this.peakBufferedAmount) {
        this.peakBufferedAmount = this.bufferedAmount;
      }
      this.peer.pendingNetworkQueue.push(frame);
    }
  }

  close(code?: number, reason?: string): void {
    if (code === 1005 || code === 1006) {
      throw new Error(`Application must never pass reserved close code ${code}`);
    }
    this.closeCalls.push({ code, reason });
    if (this.readyState === WS_READY_STATE_CLOSED) {
      return;
    }
    this.readyState = WS_READY_STATE_CLOSED;
    this.peer.serverClosed = { code: code ?? 1005, reason: reason ?? "" };
    if (this.peer.autoDrain && this.peer.pendingNetworkQueue.length === 0) {
      this.peer.readyState = WS_READY_STATE_CLOSED;
    }
  }

  serializeAttachment(attachment: unknown): void {
    const json = JSON.stringify(attachment);
    const byteLen = utf8Encoder.encode(json).byteLength;
    if (byteLen > ATTACHMENT_MAX_BYTES) {
      throw new Error(
        `Attachment exceeds ${ATTACHMENT_MAX_BYTES} bytes (${byteLen})`,
      );
    }
    this.serializedAttachmentJson = json;
  }

  deserializeAttachment(): unknown {
    if (this.serializedAttachmentJson === null) return null;
    return JSON.parse(this.serializedAttachmentJson);
  }
}

export class FakeClientWebSocket {
  readyState = WS_READY_STATE_OPEN;
  autoDrain = true;
  receivedFrames: Uint8Array[] = [];
  pendingNetworkQueue: Uint8Array[] = [];
  serverClosed: { code: number; reason: string } | null = null;
  platformClose: { code: number; reason: string } | null = null;
  server!: FakeServerWebSocket;
  platform!: SimulatedNativePlatform;

  /**
   * Sends a binary frame through the simulated Cloudflare edge (32 MiB platform ceiling -> 1009 close)
   * and awaits `engine.webSocketMessage`.
   */
  async sendBinary(bytes: Uint8Array): Promise<void> {
    if (this.readyState !== WS_READY_STATE_OPEN) {
      throw new Error("Client socket is closed");
    }
    if (bytes.byteLength > WS_MAX_ENCODED_BYTES) {
      // Cloudflare edge closes with 1009 Message Too Big before DO sees the payload
      this.readyState = WS_READY_STATE_CLOSED;
      this.server.readyState = WS_READY_STATE_CLOSED;
      this.platformClose = { code: 1009, reason: "Message too big" };
      await this.platform.engine.webSocketClose(
        this.server,
        1009,
        "Message too big",
        false,
      );
      return;
    }
    await this.platform.engine.webSocketMessage(this.server, bytes);
  }

  /**
   * Bypasses edge 1009 interception to test the engine's own encoded-length defence directly.
   */
  async sendDirectToEngine(bytes: Uint8Array | string): Promise<void> {
    await this.platform.engine.webSocketMessage(this.server, bytes);
  }

  /**
   * Drains up to `maxBytes` (or everything if omitted) from `pendingNetworkQueue` into `receivedFrames`
   * and notifies the active `engine` that `server.bufferedAmount` decreased.
   */
  drain(maxBytes?: number): number {
    let drained = 0;
    while (this.pendingNetworkQueue.length > 0) {
      const head = this.pendingNetworkQueue[0]!;
      if (maxBytes !== undefined && drained > 0 && drained + head.byteLength > maxBytes) {
        break;
      }
      this.pendingNetworkQueue.shift();
      this.receivedFrames.push(head);
      this.server.bufferedAmount = Math.max(
        0,
        this.server.bufferedAmount - head.byteLength,
      );
      drained += head.byteLength;
      if (maxBytes !== undefined && drained >= maxBytes) {
        break;
      }
    }
    if (this.pendingNetworkQueue.length === 0 && this.serverClosed !== null) {
      this.readyState = WS_READY_STATE_CLOSED;
    }
    this.platform.engine.onSocketBufferDrain(this.server);
    return drained;
  }

  drainAll(): number {
    return this.drain();
  }

  async closeClean(code = 1000, reason = ""): Promise<void> {
    this.readyState = WS_READY_STATE_CLOSED;
    this.server.readyState = WS_READY_STATE_CLOSED;
    await this.platform.engine.webSocketClose(this.server, code, reason, true);
  }

  /**
   * Peer sends a Close frame with no status code (workerd delivers `code = 1005` while server readyState may still be OPEN).
   */
  async closeNoStatus1005(): Promise<void> {
    this.readyState = WS_READY_STATE_CLOSED;
    await this.platform.engine.webSocketClose(this.server, 1005, "", true);
  }

  /**
   * Abnormal TCP drop with no Close frame (`code = 1006`, `wasClean = false`).
   */
  async dropAbnormal1006(): Promise<void> {
    this.readyState = WS_READY_STATE_CLOSED;
    this.server.readyState = WS_READY_STATE_CLOSED;
    await this.platform.engine.webSocketClose(
      this.server,
      1006,
      "abnormal closure",
      false,
    );
  }

  popDecodedFrames(): Array<
    | { kind: "data"; payload: Uint8Array }
    | { kind: "error"; code: number; message: string }
  > {
    const out = this.receivedFrames.map((f) => pb.decodeWsFrame(f));
    this.receivedFrames.length = 0;
    return out;
  }

  peekDecodedFrames(): Array<
    | { kind: "data"; payload: Uint8Array }
    | { kind: "error"; code: number; message: string }
  > {
    return this.receivedFrames.map((f) => pb.decodeWsFrame(f));
  }
}

export class SimulatedNativePlatform {
  readonly doId: string;
  readonly storage: TrackedSqliteStorage;
  readonly r2: SimulatedR2Bucket | null;
  readonly env: NativeEnvConfig;
  readonly overrides: NativeRuntimeOverrides;
  private currentNowMs: number;
  private scheduledAlarmMs: number | null = null;
  private constructorCounter = 0;
  private readonly acceptedSockets: FakeServerWebSocket[] = [];

  failNextSetAlarm = false;
  setAlarmCallCount = 0;
  engine: NativeEngine;

  constructor(options?: {
    doId?: string;
    withR2?: boolean;
    startMs?: number;
    env?: NativeEnvConfig;
    overrides?: NativeRuntimeOverrides;
  }) {
    this.doId = options?.doId ?? "do-native-test-01";
    this.currentNowMs = options?.startMs ?? 1_700_000_000_000;
    this.storage = new TrackedSqliteStorage();
    this.r2 =
      options?.withR2 === false
        ? null
        : new SimulatedR2Bucket(() => this.currentNowMs);
    this.env = { ...(options?.env ?? {}) };
    this.overrides = { ...(options?.overrides ?? {}) };
    this.engine = this.instantiateEngine();
  }

  private buildHost(): NativeHost {
    return {
      doId: this.doId,
      storage: this.storage,
      r2: this.r2,
      env: this.env,
      overrides: this.overrides,
      acceptWebSocket: (ws: NativeWebSocket, tags?: string[]) => {
        const fws = ws as FakeServerWebSocket;
        fws.acceptedTags = tags ? [...tags] : [];
        if (!this.acceptedSockets.includes(fws)) {
          this.acceptedSockets.push(fws);
        }
      },
      getWebSockets: (tag?: string): NativeWebSocket[] => {
        return this.acceptedSockets.filter(
          (ws) =>
            ws.readyState === WS_READY_STATE_OPEN &&
            (tag === undefined || ws.acceptedTags.includes(tag)),
        );
      },
      createWebSocketPair: (): NativeWebSocketPairResult => {
        const client = new FakeClientWebSocket();
        const server = new FakeServerWebSocket();
        client.server = server;
        server.peer = client;
        client.platform = this;
        server.platform = this;
        return {
          client,
          server,
          toUpgradeResponse: (headers?: HeadersInit) =>
            new UpgradeResponse(client, headers),
        };
      },
      nowMs: () => this.currentNowMs,
      setAlarm: async (scheduledTimeMs: number) => {
        this.setAlarmCallCount += 1;
        if (this.failNextSetAlarm) {
          this.failNextSetAlarm = false;
          throw new Error("simulated interruption before setAlarm persisted");
        }
        this.scheduledAlarmMs = scheduledTimeMs;
      },
      getAlarm: async () => this.scheduledAlarmMs,
      deleteAlarm: async () => {
        this.scheduledAlarmMs = null;
      },
      nextConstructorRun: () => {
        this.constructorCounter += 1;
        return this.constructorCounter;
      },
    };
  }

  private instantiateEngine(): NativeEngine {
    return new NativeEngine(this.buildHost());
  }

  nowMs(): number {
    return this.currentNowMs;
  }

  getAlarmMs(): number | null {
    return this.scheduledAlarmMs;
  }

  advanceMs(deltaMs: number): void {
    this.currentNowMs += deltaMs;
  }

  setNowMs(ms: number): void {
    this.currentNowMs = ms;
  }

  /**
   * Advances the clock by `deltaMs`, firing due DO alarms in chronological order just like workerd.
   */
  async advanceAndFireAlarms(deltaMs: number): Promise<number> {
    const targetMs = this.currentNowMs + deltaMs;
    let fired = 0;
    while (
      this.scheduledAlarmMs !== null &&
      this.scheduledAlarmMs <= targetMs &&
      fired < 100
    ) {
      if (this.scheduledAlarmMs > this.currentNowMs) {
        this.currentNowMs = this.scheduledAlarmMs;
      }
      this.scheduledAlarmMs = null;
      fired += 1;
      await this.engine.alarm();
    }
    this.currentNowMs = targetMs;
    return fired;
  }

  /**
   * Simulates Durable Object hibernation eviction + cold wake:
   * Drops the entire `NativeEngine` JS object graph and constructs a brand-new `NativeEngine`
   * on the same SQLite DB, R2 bucket, and open hibernated WebSockets!
   * Verifies `bootId` and `constructorRuns` changed.
   */
  simulateHibernation(): {
    prevBootId: string;
    newBootId: string;
    constructorRuns: number;
  } {
    const prevBootId = this.engine.bootId;
    const prevRuns = this.engine.constructorRuns;
    this.engine.dispose();
    this.engine = this.instantiateEngine();
    expect(this.engine.bootId).not.toBe(prevBootId);
    expect(this.engine.constructorRuns).toBe(prevRuns + 1);
    return {
      prevBootId,
      newBootId: this.engine.bootId,
      constructorRuns: this.engine.constructorRuns,
    };
  }

  /**
   * Simulates a silent socket disappearance while hibernated (no `webSocketClose` delivered before wake).
   */
  vanishSocketDuringHibernation(client: FakeClientWebSocket): void {
    client.readyState = WS_READY_STATE_CLOSED;
    client.server.readyState = WS_READY_STATE_CLOSED;
  }

  async openWs(
    path: string,
    headers?: Record<string, string>,
  ): Promise<{ status: number; response: Response; client?: FakeClientWebSocket }> {
    const res = await this.engine.fetch(
      new Request(`https://anyserve.example.com${path}`, {
        method: "GET",
        headers: {
          Upgrade: "websocket",
          ...(headers ?? {}),
        },
      }),
    );
    if (res.status === 101 && res instanceof UpgradeResponse) {
      return { status: 101, response: res, client: res.webSocket };
    }
    return { status: res.status, response: res };
  }

  async unaryPost(
    path: string,
    body: Uint8Array,
    headers?: Record<string, string>,
  ): Promise<{ status: number; grpcCode: number | null; bytes: Uint8Array; text: string }> {
    const ab = new ArrayBuffer(body.byteLength);
    new Uint8Array(ab).set(body);
    const res = await this.engine.fetch(
      new Request(`https://anyserve.example.com${path}`, {
        method: "POST",
        headers: {
          "Content-Type": "application/x-protobuf",
          ...(headers ?? {}),
        },
        body: ab,
      }),
    );
    const outBytes = new Uint8Array(await res.arrayBuffer());
    const codeHdr = res.headers.get("x-anyserve-grpc-code");
    return {
      status: res.status,
      grpcCode: codeHdr !== null ? Number(codeHdr) : null,
      bytes: outBytes,
      text: res.status === 200 ? "" : utf8Decoder.decode(outBytes),
    };
  }
}

// ---------------------------------------------------------------------------
// Deterministic Payload & Protocol Helpers
// ---------------------------------------------------------------------------

function makeDeterministicBytes(byteLength: number, seed = 17): Uint8Array {
  const out = new Uint8Array(byteLength);
  // Fill in 4-byte words via DataView / Uint32Array for sub-millisecond 8-21 MiB buffer creation
  let x = (seed ^ 0x9e3779b9) >>> 0;
  const wordCount = byteLength >>> 2;
  const u32 = new Uint32Array(out.buffer, 0, wordCount);
  for (let i = 0; i < wordCount; i++) {
    x ^= x << 13;
    x ^= x >>> 17;
    x ^= x << 5;
    u32[i] = x;
  }
  for (let i = wordCount << 2; i < byteLength; i++) {
    out[i] = (seed + i * 31) & 0xff;
  }
  return out;
}

function buffersEqual(a: Uint8Array, b: Uint8Array): boolean {
  if (a.byteLength !== b.byteLength) return false;
  // Fast 32-bit comparison
  const words = a.byteLength >>> 2;
  const ua = new Uint32Array(a.buffer, a.byteOffset, words);
  const ub = new Uint32Array(b.buffer, b.byteOffset, words);
  for (let i = 0; i < words; i++) {
    if (ua[i] !== ub[i]) return false;
  }
  for (let i = words << 2; i < a.byteLength; i++) {
    if (a[i] !== b[i]) return false;
  }
  return true;
}

async function connectWorkerSession(
  platform: SimulatedNativePlatform,
  opts?: {
    workerId?: string;
    interfaces?: string[];
    maxConcurrency?: number;
    pool?: string;
    attributes?: Record<string, string>;
    totalCapacity?: Record<string, bigint>;
    resumeSessionId?: string;
    heldLeaseIds?: string[];
    headers?: Record<string, string>;
  },
): Promise<{
  ws: FakeClientWebSocket;
  welcome: pb.Welcome;
  initialMessages: pb.ServerMessage[];
}> {
  const upg = await platform.openWs("/ws/v2/worker/session", opts?.headers);
  expect(upg.status).toBe(101);
  const ws = upg.client!;
  await ws.sendBinary(
    pb.encodeWorkerMessage({
      msg: {
        case: "hello",
        value: {
          workerId: opts?.workerId ?? "worker-1",
          interfaces: opts?.interfaces ?? ["echo.v1"],
          attributes: opts?.attributes ?? {},
          totalCapacity: opts?.totalCapacity ?? {},
          maxConcurrency: opts?.maxConcurrency ?? 4,
          resumeSessionId: opts?.resumeSessionId ?? "",
          heldLeaseIds: opts?.heldLeaseIds ?? [],
          metadata: {},
          pool: opts?.pool ?? "",
        },
      },
    }),
  );
  expect(ws.receivedFrames.length).toBeGreaterThanOrEqual(1);
  const firstRaw = ws.receivedFrames.shift()!;
  const firstFrame = pb.decodeWsFrame(firstRaw);
  expect(firstFrame.kind).toBe("data");
  if (firstFrame.kind !== "data") throw new Error("unexpected error frame");
  const firstMsg = pb.decodeServerMessage(firstFrame.payload);
  // Plan 5.3: Welcome MUST always be the first control reply!
  expect(firstMsg.msg?.case).toBe("welcome");
  const welcome = (firstMsg.msg as { case: "welcome"; value: pb.Welcome }).value;
  const initialMessages = ws.receivedFrames.map((raw) => {
    const f = pb.decodeWsFrame(raw);
    if (f.kind !== "data") throw new Error("unexpected error frame");
    return pb.decodeServerMessage(f.payload);
  });
  return { ws, welcome, initialMessages };
}

function extractAssigns(ws: FakeClientWebSocket): pb.Assign[] {
  const assigns: pb.Assign[] = [];
  const keep: Uint8Array[] = [];
  for (const raw of ws.receivedFrames) {
    const f = pb.decodeWsFrame(raw);
    if (f.kind === "data") {
      const m = pb.decodeServerMessage(f.payload);
      if (m.msg?.case === "assign") {
        assigns.push(m.msg.value);
        continue;
      }
    }
    keep.push(raw);
  }
  ws.receivedFrames = keep;
  return assigns;
}

function decodeAttemptDownMessages(ws: FakeClientWebSocket): pb.AttemptDown[] {
  return ws.popDecodedFrames().map((f) => {
    if (f.kind !== "data") {
      throw new Error(`Expected AttemptDown data frame, got error(${f.code}): ${f.message}`);
    }
    return pb.decodeAttemptDown(f.payload);
  });
}

function decodeExecutionEvents(ws: FakeClientWebSocket): pb.ExecutionEvent[] {
  return ws.popDecodedFrames().map((f) => {
    if (f.kind !== "data") {
      throw new Error(`Expected ExecutionEvent data frame, got error(${f.code}): ${f.message}`);
    }
    return pb.decodeExecutionEvent(f.payload);
  });
}

// ---------------------------------------------------------------------------
// Test Suites
// ---------------------------------------------------------------------------

describe("AnyserveNativeDO class & constructor wake invariants (4.1 - 4.3)", () => {
  it("delegates fetch, hibernated WebSockets, and alarm through AnyserveNativeDO without JournalStorage", async () => {
    const { AnyserveNativeDO } = await import("../src/native-do");

    const OrigResponse = globalThis.Response;
    const OrigPair = (globalThis as Record<string, unknown>).WebSocketPair;
    const sqlStorage = new TrackedSqliteStorage();
    const r2 = new SimulatedR2Bucket(() => 1_700_000_000_000);
    const liveSockets: FakeServerWebSocket[] = [];
    let alarmMs: number | null = null;

    class DoMockPair {
      0: FakeClientWebSocket;
      1: FakeServerWebSocket;
      constructor() {
        const c = new FakeClientWebSocket();
        const s = new FakeServerWebSocket();
        c.server = s;
        s.peer = c;
        this[0] = c;
        this[1] = s;
      }
    }

    class ShimmedResponse extends OrigResponse {
      readonly webSocket?: unknown;
      private readonly rawStatus?: number;
      constructor(
        body?: BodyInit | null,
        init?: ResponseInit & { webSocket?: unknown },
      ) {
        if (init?.status === 101 && init.webSocket) {
          const { status: _st, webSocket, ...rest } = init;
          super(body, { ...rest, status: 200 });
          this.rawStatus = 101;
          this.webSocket = webSocket;
        } else {
          super(body, init);
        }
      }
      override get status(): number {
        return this.rawStatus ?? super.status;
      }
    }

    (globalThis as Record<string, unknown>).WebSocketPair = DoMockPair;
    globalThis.Response = ShimmedResponse as unknown as typeof Response;

    try {
      const ctx = {
        id: { toString: () => "native-do-smoke-id" },
        storage: {
          sql: sqlStorage.sql,
          transactionSync: <T>(fn: () => T) => sqlStorage.transactionSync(fn),
          setAlarm: async (ms: number) => {
            alarmMs = ms;
          },
          getAlarm: async () => alarmMs,
          deleteAlarm: async () => {
            alarmMs = null;
          },
        },
        acceptWebSocket: (ws: unknown, tags?: string[]) => {
          const s = ws as FakeServerWebSocket;
          s.acceptedTags = tags ?? [];
          liveSockets.push(s);
        },
        getWebSockets: () =>
          liveSockets.filter((s) => s.readyState === WS_READY_STATE_OPEN),
      };

      const doInstance1 = new AnyserveNativeDO(
        ctx as unknown as DurableObjectState,
        {
          ANYSERVE_R2: r2 as unknown as R2Bucket,
          ANYSERVE_PUBLIC_ENDPOINT: "wss://public.example.dev",
        } as any,
      );

      const healthRes = await doInstance1.fetch(
        new Request("https://do.internal/healthz"),
      );
      expect(healthRes.status).toBe(204);

      const ov1 = await (
        await doInstance1.fetch(new Request("https://do.internal/api/overview"))
      ).json() as Record<string, any>;
      expect(ov1.runtime.code).toBe("memory_plus_cloudflare");
      expect(ov1.control_plane.public_endpoint).toBe("wss://public.example.dev");
      expect(ov1.control_plane.constructor_runs).toBe(1);
      expect(ov1.control_plane.generation).toBe(1);

      // Open worker session through AnyserveNativeDO.fetch + webSocketMessage
      const upgRes = await doInstance1.fetch(
        new Request("https://do.internal/ws/v2/worker/session", {
          headers: { Upgrade: "websocket" },
        }),
      );
      expect(upgRes.status).toBe(101);
      const serverWs = liveSockets[0]!;
      const helloBytes = pb.encodeWorkerMessage({
        msg: {
          case: "hello",
          value: {
            workerId: "w-do-1",
            interfaces: ["demo.v1"],
            attributes: {},
            totalCapacity: {},
            maxConcurrency: 2,
            resumeSessionId: "",
            heldLeaseIds: [],
            metadata: {},
            pool: "",
          },
        },
      });
      await doInstance1.webSocketMessage(
        serverWs as unknown as WebSocket,
        helloBytes.buffer.slice(
          helloBytes.byteOffset,
          helloBytes.byteOffset + helloBytes.byteLength,
        ),
      );
      expect(serverWs.peer.receivedFrames.length).toBe(1);

      // Re-instantiate AnyserveNativeDO on the same ctx (simulated hibernation wake!)
      const doInstance2 = new AnyserveNativeDO(
        ctx as unknown as DurableObjectState,
        { ANYSERVE_R2: r2 as unknown as R2Bucket } as any,
      );
      const ov2 = await (
        await doInstance2.fetch(new Request("https://do.internal/api/overview"))
      ).json() as Record<string, any>;
      expect(ov2.control_plane.constructor_runs).toBe(2);
      expect(ov2.control_plane.boot_id).not.toBe(ov1.control_plane.boot_id);
      expect(ov2.control_plane.generation).toBe(1); // Wake NEVER bumps generation!
      expect(ov2.control_plane.sessions).toBe(1);

      await doInstance2.alarm();
      await doInstance2.webSocketClose(
        serverWs as unknown as WebSocket,
        1000,
        "bye",
        true,
      );
    } finally {
      globalThis.Response = OrigResponse;
      (globalThis as Record<string, unknown>).WebSocketPair = OrigPair;
    }
  });
});

describe("8 MiB ResultChunk upload pause + simulateHibernation on same socket & AttemptOpen resend (4.4, 10)", () => {
  const EIGHT_MIB = 8 * 1024 * 1024; // 8,388,608 B = 128 x 64 KiB chunks
  const CHUNK_SIZE = WELCOME_MAX_CHUNK_BYTES; // 65,536 B
  const TOTAL_CHUNKS = EIGHT_MIB / CHUNK_SIZE; // 128

  async function run8MiBResultWithPause(
    pauseMode:
      | "after_first_seg_ack" // (a) after chunk 32 (0-based 31) ACK
      | "chunk_33_before_ack" // (b) after chunk 33 (0-based 32) processed, BEFORE any ACK for it
      | "mid_segment" // (c) mid-segment (after chunk 16 AND again after chunk 50 => >=2 constructor runs!)
      | "before_last", // (d) after chunk 127 (0-based 126), right before final chunk 128 (last=true)
  ) {
    const platform = new SimulatedNativePlatform();
    const { ws: sessionWs, welcome } = await connectWorkerSession(platform, {
      interfaces: ["blob.v1"],
    });
    expect(welcome.limits?.maxChunkBytes).toBe(65_536);
    expect(welcome.limits?.attemptWindowBytes).toBe(4_194_304);

    const payload = makeDeterministicBytes(EIGHT_MIB, 101);
    const outMeta = { "content-type": "application/octet-stream", tag: pauseMode };
    const expectedDigest = outcomeDigest(
      [{ kind: "inline", data: payload, metadata: outMeta }],
      null,
    );

    const subRes = await platform.unaryPost(
      "/rpc/v2/client/submit",
      pb.encodeSubmitRequest({
        jobId: `job-8m-${pauseMode}`,
        spec: {
          demand: { interface: "blob.v1" },
          inputs: [],
          params: new Uint8Array(0),
          metadata: {},
        },
        conflictPolicy: pb.ConflictPolicy.FAIL,
      }),
    );
    expect(subRes.status).toBe(200);
    const submitDecoded = pb.decodeSubmitResponse(subRes.bytes);
    expect(submitDecoded.created).toBe(true);

    const assigns = extractAssigns(sessionWs);
    expect(assigns).toHaveLength(1);
    const assign = assigns[0]!;

    // Open Attempt data socket
    const attUpg = await platform.openWs("/ws/v2/worker/attempt");
    expect(attUpg.status).toBe(101);
    const attemptWs = attUpg.client!;
    await attemptWs.sendBinary(
      pb.encodeAttemptUp({
        msg: {
          case: "open",
          value: { leaseId: assign.leaseId, generation: assign.generation },
        },
      }),
    );
    const openMsgs = decodeAttemptDownMessages(attemptWs);
    expect(openMsgs).toHaveLength(1);
    expect(openMsgs[0]!.msg).toEqual({
      case: "opened",
      value: { resumeFromSequence: 0n, terminalReplay: false },
    });

    const initialRuns = platform.engine.constructorRuns;

    for (let i = 0; i < TOTAL_CHUNKS; i++) {
      const start = i * CHUNK_SIZE;
      const slice = payload.subarray(start, start + CHUNK_SIZE);
      const isLast = i === TOTAL_CHUNKS - 1;
      await attemptWs.sendBinary(
        pb.encodeAttemptUp({
          msg: {
            case: "result",
            value: {
              index: 0,
              data: slice,
              last: isLast,
              metadata: i === 0 ? outMeta : {},
              uri: "",
            },
          },
        }),
      );

      // (a) Right after first 2 MiB segment (32 chunks, i === 31) is sealed and ACKed
      if (pauseMode === "after_first_seg_ack" && i === 31) {
        const acks = decodeAttemptDownMessages(attemptWs);
        expect(acks).toHaveLength(1);
        expect(acks[0]!.msg?.case).toBe("ack");
        const ackVal = (acks[0]!.msg as { case: "ack"; value: pb.Ack }).value;
        expect(ackVal.resultBytes).toBeGreaterThanOrEqual(BigInt(UPLOAD_SEGMENT_BYTES));
        // Tail table must be empty right after segment 0 sealed!
        const tailCnt = platform.storage.sql
          .exec<{ c: number }>(`SELECT COUNT(*) AS c FROM native_upload_tail`)
          .toArray()[0]!.c;
        expect(tailCnt).toBe(0);

        platform.simulateHibernation();
      }

      // (b) Right after chunk 33 (1-based = i === 32) is processed and BEFORE any ACK for it!
      if (pauseMode === "chunk_33_before_ack" && i === 31) {
        // Consume segment 0 ACK first so we can prove chunk 33 itself emitted NO ACK
        const seg0Ack = decodeAttemptDownMessages(attemptWs);
        expect(seg0Ack).toHaveLength(1);
      }
      if (pauseMode === "chunk_33_before_ack" && i === 32) {
        // Chunk 33 must NOT have emitted an ACK yet, AND must already be persisted in native_upload_tail!
        expect(attemptWs.receivedFrames).toHaveLength(0);
        const tailRows = platform.storage.sql
          .exec<{ chunk_seq: number; len: number }>(
            `SELECT chunk_seq, LENGTH(data) AS len FROM native_upload_tail`,
          )
          .toArray();
        expect(tailRows).toEqual([{ chunk_seq: 32, len: CHUNK_SIZE }]);

        platform.simulateHibernation();
      }

      // (c) Middle of a segment — hibernate TWICE on the same socket (>=2 constructor re-runs mid-upload!)
      if (pauseMode === "mid_segment" && (i === 15 || i === 49)) {
        const tailCnt = platform.storage.sql
          .exec<{ c: number }>(`SELECT COUNT(*) AS c FROM native_upload_tail`)
          .toArray()[0]!.c;
        expect(tailCnt).toBeGreaterThan(0);
        platform.simulateHibernation();
      }

      // (d) Right before `last` (after chunk 127, i === 126; 31 chunks sitting unsealed in native_upload_tail)
      if (pauseMode === "before_last" && i === TOTAL_CHUNKS - 2) {
        const tailCnt = platform.storage.sql
          .exec<{ c: number }>(`SELECT COUNT(*) AS c FROM native_upload_tail`)
          .toArray()[0]!.c;
        expect(tailCnt).toBe(31);
        platform.simulateHibernation();
      }
    }

    expect(platform.engine.constructorRuns).toBeGreaterThan(initialRuns);

    // Send Finish on the SAME Attempt socket (never reopened!)
    await attemptWs.sendBinary(
      pb.encodeAttemptUp({
        msg: {
          case: "finish",
          value: { outcome: { case: "succeeded", value: {} } },
        },
      }),
    );

    const finalMsgs = decodeAttemptDownMessages(attemptWs);
    const finishedMsg = finalMsgs.find((m) => m.msg?.case === "finished");
    expect(finishedMsg).toBeDefined();
    expect(
      (finishedMsg!.msg as { case: "finished"; value: pb.AttemptFinished }).value
        .status,
    ).toBe(pb.AttemptFinishedStatus.COMMITTED);

    // Verify server recorded exact outcomeDigest and single terminal state + all 8 MiB bytes!
    const dbJob = platform.storage.sql
      .exec<{ state: string; result_digest: string; winning_lease_id: string }>(
        `SELECT state, result_digest, winning_lease_id FROM native_jobs WHERE raw_job_id = ?`,
        `job-8m-${pauseMode}`,
      )
      .toArray()[0]!;
    expect(dbJob.state).toBe("succeeded");
    expect(dbJob.winning_lease_id).toBe(assign.leaseId);
    expect(dbJob.result_digest).toBe(expectedDigest);

    const finishedEventCount = platform.storage.sql
      .exec<{ c: number }>(
        `SELECT COUNT(*) AS c FROM native_job_events WHERE scoped_job_id = ? AND kind = 'finished'`,
        `job-8m-${pauseMode}`,
      )
      .toArray()[0]!.c;
    expect(finishedEventCount).toBe(1);

    const getRes = await platform.unaryPost(
      "/rpc/v2/client/get_job",
      pb.encodeGetJobRequest({
        jobId: `job-8m-${pauseMode}`,
        includeOutputs: true,
      }),
    );
    expect(getRes.status).toBe(200);
    const gotJob = pb.decodeJob(getRes.bytes);
    expect(gotJob.state).toBe(pb.JobState.SUCCEEDED);
    expect(gotJob.outputs).toHaveLength(1);
    expect(gotJob.outputs[0]!.body?.case).toBe("inline");
    const gotBytes = (
      gotJob.outputs[0]!.body as { case: "inline"; value: Uint8Array }
    ).value;
    expect(gotBytes.byteLength).toBe(EIGHT_MIB);
    expect(buffersEqual(gotBytes, payload)).toBe(true);
    expect(gotJob.outputs[0]!.metadata).toEqual(outMeta);
  }

  it("resumes 8 MiB upload across hibernation on the same socket at (a) after first segment ACK", async () => {
    await run8MiBResultWithPause("after_first_seg_ack");
  });

  it("resumes 8 MiB upload across hibernation on the same socket at (b) after chunk 33 before its ACK", async () => {
    await run8MiBResultWithPause("chunk_33_before_ack");
  });

  it("resumes 8 MiB upload across >=2 hibernations on the same socket at (c) mid-segment", async () => {
    await run8MiBResultWithPause("mid_segment");
  });

  it("resumes 8 MiB upload across hibernation on the same socket at (d) right before last chunk", async () => {
    await run8MiBResultWithPause("before_last");
  });

  it("discards uncommitted tail & R2 segments on a new AttemptOpen and resends cleanly from object 0, plus multi-4MiB window upload", async () => {
    const platform = new SimulatedNativePlatform();
    const { ws: sessionWs } = await connectWorkerSession(platform, {
      interfaces: ["multi.v1"],
    });

    await platform.unaryPost(
      "/rpc/v2/client/submit",
      pb.encodeSubmitRequest({
        jobId: "job-reopen-and-multiwin",
        spec: {
          demand: { interface: "multi.v1" },
          inputs: [],
          params: new Uint8Array(0),
          metadata: {},
        },
        conflictPolicy: pb.ConflictPolicy.FAIL,
      }),
    );
    const assign = extractAssigns(sessionWs)[0]!;

    // First Attempt socket: upload 48 x 64 KiB = 3 MiB (1 sealed R2 segment + 16 tail chunks), then abandon!
    const att1 = (await platform.openWs("/ws/v2/worker/attempt")).client!;
    await att1.sendBinary(
      pb.encodeAttemptUp({
        msg: {
          case: "open",
          value: { leaseId: assign.leaseId, generation: assign.generation },
        },
      }),
    );
    decodeAttemptDownMessages(att1);

    const staleChunk = makeDeterministicBytes(CHUNK_SIZE, 77);
    for (let i = 0; i < 48; i++) {
      await att1.sendBinary(
        pb.encodeAttemptUp({
          msg: {
            case: "result",
            value: {
              index: 0,
              data: staleChunk,
              last: false,
              metadata: i === 0 ? { stale: "1" } : {},
              uri: "",
            },
          },
        }),
      );
    }
    expect(platform.r2!.objects.size).toBe(1);
    const staleR2Key = [...platform.r2!.objects.keys()][0]!;

    // Open a NEW Attempt socket with AttemptOpen: must discard uncommitted R2 segment + tail and reset to object 0!
    const att2 = (await platform.openWs("/ws/v2/worker/attempt")).client!;
    await att2.sendBinary(
      pb.encodeAttemptUp({
        msg: {
          case: "open",
          value: { leaseId: assign.leaseId, generation: assign.generation },
        },
      }),
    );
    const opened2 = decodeAttemptDownMessages(att2)[0]!;
    expect(opened2.msg?.case).toBe("opened");

    const gcPending = platform.storage.sql
      .exec<{ r2_key: string; state: string }>(
        `SELECT r2_key, state FROM native_r2_gc WHERE r2_key = ?`,
        staleR2Key,
      )
      .toArray();
    expect(gcPending).toEqual([{ r2_key: staleR2Key, state: "pending_delete" }]);

    // Now upload 3 x 6 MiB = 18 MiB across multiple 4 MiB windows from object 0!
    const sixMiB = 6 * 1024 * 1024; // 96 chunks each
    const objs = [
      makeDeterministicBytes(sixMiB, 11),
      makeDeterministicBytes(sixMiB, 22),
      makeDeterministicBytes(sixMiB, 33),
    ];

    for (let objIdx = 0; objIdx < objs.length; objIdx++) {
      const data = objs[objIdx]!;
      const chunks = data.byteLength / CHUNK_SIZE;
      for (let c = 0; c < chunks; c++) {
        await att2.sendBinary(
          pb.encodeAttemptUp({
            msg: {
              case: "result",
              value: {
                index: objIdx,
                data: data.subarray(c * CHUNK_SIZE, (c + 1) * CHUNK_SIZE),
                last: c === chunks - 1,
                metadata: c === 0 ? { idx: String(objIdx) } : {},
                uri: "",
              },
            },
          }),
        );
      }
    }

    await att2.sendBinary(
      pb.encodeAttemptUp({
        msg: {
          case: "finish",
          value: { outcome: { case: "succeeded", value: {} } },
        },
      }),
    );
    const fin = decodeAttemptDownMessages(att2).find(
      (m) => m.msg?.case === "finished",
    )!;
    expect(
      (fin.msg as { case: "finished"; value: pb.AttemptFinished }).value.status,
    ).toBe(pb.AttemptFinishedStatus.COMMITTED);

    // Run alarm so R2 GC sweeps the discarded stale segment without touching the 9 committed R2 segments!
    await platform.advanceAndFireAlarms(5_000);
    expect(platform.r2!.objects.has(staleR2Key)).toBe(false);

    const got = pb.decodeJob(
      (
        await platform.unaryPost(
          "/rpc/v2/client/get_job",
          pb.encodeGetJobRequest({
            jobId: "job-reopen-and-multiwin",
            includeOutputs: true,
          }),
        )
      ).bytes,
    );
    expect(got.outputs).toHaveLength(3);
    for (let i = 0; i < 3; i++) {
      const val = (
        got.outputs[i]!.body as { case: "inline"; value: Uint8Array }
      ).value;
      expect(buffersEqual(val, objs[i]!)).toBe(true);
    }
  });
});

describe("Params, ordinary static object, streaming Invoke, and server->worker InputData pause + hibernation (4.4, 5.3, 10)", () => {
  it("pumps static objects (incl. empty & URI), params, streaming Invoke chunks, and trailer in exact order across mid-upload & mid-download hibernations with zero duplicate/missing slices", async () => {
    const platform = new SimulatedNativePlatform();
    const { ws: sessionWs } = await connectWorkerSession(platform, {
      interfaces: ["stream.v1"],
    });

    const CHUNK = WELCOME_MAX_CHUNK_BYTES; // 64 KiB
    const staticObj2 = makeDeterministicBytes(35 * CHUNK, 51); // 35 chunks (> 2 MiB)
    const paramsBuf = makeDeterministicBytes(35 * CHUNK, 61); // 35 chunks (> 2 MiB)
    const streamTotalChunks = 36; // 36 x 64 KiB = 2.25 MiB streamed via Invoke

    // Open client Invoke socket
    const invWs = (await platform.openWs("/ws/v2/client/invoke")).client!;
    await invWs.sendBinary(
      pb.encodeInvokeRequest({
        msg: {
          case: "start",
          value: {
            streamingInput: true,
            spec: {
              demand: { interface: "stream.v1" },
              inputs: [
                // Static object 0: empty inline with metadata
                { body: { case: "inline", value: new Uint8Array(0) }, metadata: { empty: "yes" } },
                // Static object 1: opaque URI with metadata
                { body: { case: "uri", value: "s3://bucket/input-1.bin" }, metadata: { kind: "uri-obj" } },
                // Static object 2: 35-chunk (2.1875 MiB) inline object
                { body: { case: "inline", value: staticObj2 }, metadata: { big: "static" } },
              ],
              params: paramsBuf,
              metadata: { job: "full-input-matrix" },
            },
          },
        },
      }),
    );

    const assign = extractAssigns(sessionWs)[0]!;
    expect(assign.inputsViaAttempt).toBe(true);
    expect(assign.streamingInput).toBe(true);

    // Open Attempt socket in slow-reader mode (`autoDrain = false`) so we pause InputData at exact points!
    const attWs = (await platform.openWs("/ws/v2/worker/attempt")).client!;
    attWs.autoDrain = false;

    await attWs.sendBinary(
      pb.encodeAttemptUp({
        msg: {
          case: "open",
          value: { leaseId: assign.leaseId, generation: assign.generation },
        },
      }),
    );

    // Drain first batch (AttemptOpened + up to 16 InputData frames <= 1 MiB)
    attWs.drainAll();
    const collectedDown: pb.AttemptDown[] = decodeAttemptDownMessages(attWs);
    // Plan 5.3: AttemptOpened MUST precede any InputData!
    expect(collectedDown[0]!.msg?.case).toBe("opened");
    expect(collectedDown.length).toBeGreaterThan(2);

    // Pause (a) after first batch on Attempt socket -> simulateHibernation on SAME Attempt & Invoke sockets!
    platform.simulateHibernation();

    // Drain second batch (~chunks up to 32) and 1 more chunk so we hit chunk 33, then hibernate again!
    await Promise.resolve();
    await new Promise((r) => setTimeout(r, 5));
    attWs.drain(CHUNK * 17);
    await new Promise((r) => setTimeout(r, 5));
    collectedDown.push(...decodeAttemptDownMessages(attWs));

    // Pause (b)/(c) mid-static-object & mid-params -> simulateHibernation again (>= 2 constructor runs on same Attempt socket!)
    platform.simulateHibernation();

    // Drain through all static inputs + params (stages 0 and 1 complete; pump now waits at stage 2 for streaming InputChunks)
    for (let step = 0; step < 8; step++) {
      await new Promise((r) => setTimeout(r, 5));
      attWs.drainAll();
      collectedDown.push(...decodeAttemptDownMessages(attWs));
      if (step === 2) {
        // Hibernate right in the middle of `params` (parameterChunk=true) download!
        platform.simulateHibernation();
      }
    }

    // Now stream 36 chunks from client `Invoke` socket, pausing & hibernating on the SAME `Invoke` socket at:
    // - after chunk 16 (mid-segment)
    // - after chunk 32 (2 MiB)
    // - after chunk 33
    // - right before InputEnd (after chunk 36)
    const streamExpectedPieces: Uint8Array[] = [];
    for (let s = 0; s < streamTotalChunks; s++) {
      const piece = makeDeterministicBytes(CHUNK, 200 + s);
      streamExpectedPieces.push(piece);
      await invWs.sendBinary(
        pb.encodeInvokeRequest({
          msg: {
            case: "input",
            value: {
              data: piece,
              metadata: s === 0 ? { stream_first: "true" } : {},
            },
          },
        }),
      );

      if (s === 15 || s === 31 || s === 32 || s === streamTotalChunks - 1) {
        platform.simulateHibernation();
      }

      // Let worker drain streamed InputData progressively
      attWs.drainAll();
      await new Promise((r) => setTimeout(r, 2));
      collectedDown.push(...decodeAttemptDownMessages(attWs));
    }

    // Send InputEnd after hibernating right before last!
    await invWs.sendBinary(
      pb.encodeInvokeRequest({
        msg: { case: "inputEnd", value: {} },
      }),
    );

    // Hibernate once more right before draining final trailer (`last = true`)!
    platform.simulateHibernation();
    for (let step = 0; step < 6; step++) {
      await new Promise((r) => setTimeout(r, 5));
      attWs.drainAll();
      collectedDown.push(...decodeAttemptDownMessages(attWs));
    }

    // Verify contiguous sequence 1..N and exact wire ordering!
    const inputFrames = collectedDown
      .slice(1)
      .map((m) => {
        expect(m.msg?.case).toBe("input");
        return (m.msg as { case: "input"; value: pb.InputData }).value;
      });

    expect(inputFrames.length).toBeGreaterThan(100);
    for (let i = 0; i < inputFrames.length; i++) {
      expect(inputFrames[i]!.sequence).toBe(BigInt(i + 1));
    }

    // 1. First InputData: static object 0 (empty inline, objectIndex === 0, objectEnd === true)
    expect(inputFrames[0]!.objectIndex).toBe(0);
    expect(inputFrames[0]!.data.byteLength).toBe(0);
    expect(inputFrames[0]!.metadata).toEqual({ empty: "yes" });
    expect(inputFrames[0]!.objectEnd).toBe(true);
    expect(inputFrames[0]!.parameterChunk).toBe(false);

    // 2. Second InputData: static object 1 (URI, objectIndex === 1, uri === "s3://...", objectEnd === true)
    expect(inputFrames[1]!.objectIndex).toBe(1);
    expect(inputFrames[1]!.uri).toBe("s3://bucket/input-1.bin");
    expect(inputFrames[1]!.data.byteLength).toBe(0);
    expect(inputFrames[1]!.metadata).toEqual({ kind: "uri-obj" });
    expect(inputFrames[1]!.objectEnd).toBe(true);

    // 3. Reassemble static object 2 (objectIndex === 2, 35 chunks)
    const obj2Frames = inputFrames.filter((f) => f.objectIndex === 2);
    expect(obj2Frames).toHaveLength(35);
    expect(obj2Frames[0]!.metadata).toEqual({ big: "static" });
    expect(obj2Frames[1]!.metadata).toEqual({});
    expect(obj2Frames.slice(0, -1).every((f) => !f.objectEnd)).toBe(true);
    expect(obj2Frames[obj2Frames.length - 1]!.objectEnd).toBe(true);
    const gotStatic2 = new Uint8Array(staticObj2.byteLength);
    let pos = 0;
    for (const f of obj2Frames) {
      gotStatic2.set(f.data, pos);
      pos += f.data.byteLength;
    }
    expect(buffersEqual(gotStatic2, staticObj2)).toBe(true);

    // 4. Reassemble params (`objectIndex === undefined`, `parameterChunk === true`) — MUST come after static objects and before stream chunks!
    const paramFrames = inputFrames.filter((f) => f.parameterChunk);
    expect(paramFrames).toHaveLength(35);
    expect(paramFrames.every((f) => f.objectIndex === undefined)).toBe(true);
    expect(paramFrames[paramFrames.length - 1]!.objectEnd).toBe(true);
    const firstParamSeq = paramFrames[0]!.sequence;
    const lastStaticSeq = obj2Frames[obj2Frames.length - 1]!.sequence;
    expect(firstParamSeq).toBe(lastStaticSeq + 1n);

    const gotParams = new Uint8Array(paramsBuf.byteLength);
    pos = 0;
    for (const f of paramFrames) {
      gotParams.set(f.data, pos);
      pos += f.data.byteLength;
    }
    expect(buffersEqual(gotParams, paramsBuf)).toBe(true);

    // 5. Streaming chunks (`objectIndex === 3 .. 3 + 35`)
    for (let s = 0; s < streamTotalChunks; s++) {
      const sFrames = inputFrames.filter((f) => f.objectIndex === 3 + s);
      expect(sFrames).toHaveLength(1);
      expect(sFrames[0]!.objectEnd).toBe(true);
      expect(buffersEqual(sFrames[0]!.data, streamExpectedPieces[s]!)).toBe(true);
    }

    // 6. Final trailer (`objectIndex === undefined`, `last === true`, `parameterChunk === false`)
    const trailer = inputFrames[inputFrames.length - 1]!;
    expect(trailer.last).toBe(true);
    expect(trailer.objectIndex).toBeUndefined();
    expect(trailer.parameterChunk).toBe(false);
    expect(trailer.data.byteLength).toBe(0);

    // Complete attempt and verify Invoke client receives ExecutionFinished + clean close(1000)
    attWs.autoDrain = true;
    await attWs.sendBinary(
      pb.encodeAttemptUp({
        msg: {
          case: "result",
          value: {
            index: 0,
            data: utf8Encoder.encode("all-inputs-verified"),
            last: true,
            metadata: {},
            uri: "",
          },
        },
      }),
    );
    await attWs.sendBinary(
      pb.encodeAttemptUp({
        msg: {
          case: "finish",
          value: { outcome: { case: "succeeded", value: {} } },
        },
      }),
    );

    const invokeEvents = decodeExecutionEvents(invWs);
    const finEv = invokeEvents[invokeEvents.length - 1]!;
    expect(finEv.event?.case).toBe("finished");
    expect(invWs.serverClosed?.code).toBe(1000);
  });
});

describe("Packed object bundles & 4096-byte previews via SUBSTR / R2 Range (4.3, 8, 10)", () => {
  it("writes the exact same number of SQLite rows for 24 x 1B inputs as for 1 x 1B input (1 bundle row)", async () => {
    const p1 = new SimulatedNativePlatform();
    const before1 = p1.storage.totalRowsWritten;
    const res1 = await p1.unaryPost(
      "/rpc/v2/client/submit",
      pb.encodeSubmitRequest({
        jobId: "job-1-in",
        spec: {
          demand: { interface: "pack.v1" },
          inputs: [
            { body: { case: "inline", value: new Uint8Array([0x41]) }, metadata: {} },
          ],
          params: new Uint8Array(0),
          metadata: {},
        },
        conflictPolicy: pb.ConflictPolicy.FAIL,
      }),
    );
    expect(res1.status).toBe(200);
    const rowsFor1Input = p1.storage.totalRowsWritten - before1;
    const bundles1 = p1.storage.sql
      .exec<{ c: number }>(`SELECT COUNT(*) AS c FROM native_object_bundles`)
      .toArray()[0]!.c;
    expect(bundles1).toBe(1);

    const p24 = new SimulatedNativePlatform();
    const before24 = p24.storage.totalRowsWritten;
    const twentyFourInputs: pb.ProtoObjectInput[] = Array.from(
      { length: 24 },
      (_, i) => ({
        body: { case: "inline" as const, value: new Uint8Array([i & 0xff]) },
        metadata: {},
      }),
    );
    const res24 = await p24.unaryPost(
      "/rpc/v2/client/submit",
      pb.encodeSubmitRequest({
        jobId: "job-24-in",
        spec: {
          demand: { interface: "pack.v1" },
          inputs: twentyFourInputs,
          params: new Uint8Array(0),
          metadata: {},
        },
        conflictPolicy: pb.ConflictPolicy.FAIL,
      }),
    );
    expect(res24.status).toBe(200);
    const rowsFor24Inputs = p24.storage.totalRowsWritten - before24;
    const bundles24 = p24.storage.sql
      .exec<{ c: number }>(`SELECT COUNT(*) AS c FROM native_object_bundles`)
      .toArray()[0]!.c;
    expect(bundles24).toBe(1);
    expect(rowsFor24Inputs).toBe(rowsFor1Input);
    expect(p24.r2!.putCount).toBe(0);
  });

  it("splits 48 x 256 KiB (12 MiB) inputs into ordered <= 4 MiB R2 packs and serves 4096B Range previews", async () => {
    const platform = new SimulatedNativePlatform();
    const itemLen = 256 * 1024; // 256 KiB
    const inputs: Uint8Array[] = [];
    const protoInputs: pb.ProtoObjectInput[] = [];

    for (let i = 0; i < 48; i++) {
      const buf = new Uint8Array(itemLen);
      const header = utf8Encoder.encode(`ITEM-${String(i).padStart(2, "0")}:` + "A".repeat(5000));
      buf.set(header, 0);
      for (let j = header.byteLength; j < itemLen; j++) {
        buf[j] = (i * 7 + j) & 0xff;
      }
      inputs.push(buf);
      protoInputs.push({
        body: { case: "inline", value: buf },
        metadata: { idx: String(i), content_type: "text/plain" },
      });
    }

    const sub = await platform.unaryPost(
      "/rpc/v2/client/submit",
      pb.encodeSubmitRequest({
        jobId: "job-48x256k",
        spec: {
          demand: { interface: "pack12m.v1" },
          inputs: protoInputs,
          params: utf8Encoder.encode("P".repeat(6000)),
          metadata: {},
        },
        conflictPolicy: pb.ConflictPolicy.FAIL,
      }),
    );
    expect(sub.status).toBe(200);

    // 48 * 256 KiB = 12 MiB -> 3 R2 packs of 4 MiB each (plus small trailing inline params in the 1 SQLite row!)
    expect(platform.r2!.objects.size).toBe(3);
    for (const entry of platform.r2!.objects.values()) {
      expect(entry.bytes.byteLength).toBeLessThanOrEqual(4 * 1024 * 1024);
    }
    const bundleRowCount = platform.storage.sql
      .exec<{ c: number }>(
        `SELECT COUNT(*) AS c FROM native_object_bundles WHERE scoped_job_id = 'job-48x256k'`,
      )
      .toArray()[0]!.c;
    expect(bundleRowCount).toBe(1);

    // Verify /api/jobs/:id fetches 4096-byte R2 Range slices rather than full 4 MiB packs!
    platform.r2!.rangeGetCalls = [];
    const detailRes = await platform.engine.fetch(
      new Request("https://anyserve.example.com/api/jobs/job-48x256k"),
    );
    expect(detailRes.status).toBe(200);
    const detail = (await detailRes.json()) as Record<string, any>;
    expect(detail.job.inputs).toHaveLength(48);
    expect(detail.job.inputs[0].size_bytes).toBe(itemLen);
    expect(detail.job.inputs[0].truncated).toBe(true);
    expect(detail.job.inputs[0].preview.startsWith("ITEM-00:")).toBe(true);
    expect(detail.job.inputs[0].preview.length).toBeLessThanOrEqual(4096);
    expect(detail.job.inputs[47].preview.startsWith("ITEM-47:")).toBe(true);
    expect(detail.job.params_truncated).toBe(true);
    expect(detail.job.params_preview.length).toBe(4096);

    expect(platform.r2!.rangeGetCalls).toHaveLength(48);
    expect(platform.r2!.rangeGetCalls.every((c) => c.length === 4096)).toBe(true);

    // Also connect a worker and verify all 48 x 256 KiB objects stream in exact order!
    const { ws: sessionWs } = await connectWorkerSession(platform, {
      interfaces: ["pack12m.v1"],
    });
    const assign = extractAssigns(sessionWs)[0]!;
    const attWs = (await platform.openWs("/ws/v2/worker/attempt")).client!;
    await attWs.sendBinary(
      pb.encodeAttemptUp({
        msg: {
          case: "open",
          value: { leaseId: assign.leaseId, generation: assign.generation },
        },
      }),
    );
    // Allow async pump batches to complete
    for (let i = 0; i < 20; i++) {
      await new Promise((r) => setTimeout(r, 2));
    }
    const down = decodeAttemptDownMessages(attWs);
    const inMsgs = down
      .slice(1)
      .map((m) => (m.msg as { case: "input"; value: pb.InputData }).value);
    expect(inMsgs[inMsgs.length - 1]!.last).toBe(true);

    for (let idx = 0; idx < 48; idx++) {
      const pieces = inMsgs.filter((m) => m.objectIndex === idx);
      expect(pieces).toHaveLength(4); // 256 KiB / 64 KiB = 4 chunks
      const rebuilt = new Uint8Array(itemLen);
      let off = 0;
      for (const p of pieces) {
        rebuilt.set(p.data, off);
        off += p.data.byteLength;
      }
      expect(buffersEqual(rebuilt, inputs[idx]!)).toBe(true);
    }
  });
});

describe("IsolateMemoryBudget, slow readers, and two simultaneous 3 x 7 MiB completions (5.1, 5.2, 10)", () => {
  it("serializes two simultaneous 3 x 7 MiB (~21 MiB) ExecutionFinished sends via the 56 MiB large-send budget without blocking unrelated RPCs, recording peak bufferedAmount and budget", async () => {
    const platform = new SimulatedNativePlatform();
    const { ws: sessionWs } = await connectWorkerSession(platform, {
      interfaces: ["heavy.v1"],
      maxConcurrency: 4,
    });

    const SEVEN_MIB = 7 * 1024 * 1024; // 7,340,032 B = 112 x 64 KiB chunks
    const CHUNK = WELCOME_MAX_CHUNK_BYTES;

    const job1Objs = [
      makeDeterministicBytes(SEVEN_MIB, 1),
      makeDeterministicBytes(SEVEN_MIB, 2),
      makeDeterministicBytes(SEVEN_MIB, 3),
    ];
    const job2Objs = [
      makeDeterministicBytes(SEVEN_MIB, 4),
      makeDeterministicBytes(SEVEN_MIB, 5),
      makeDeterministicBytes(SEVEN_MIB, 6),
    ];

    // Open two slow-reader Invoke sockets (`autoDrain = false` right before Finish!)
    const inv1 = (await platform.openWs("/ws/v2/client/invoke")).client!;
    const inv2 = (await platform.openWs("/ws/v2/client/invoke")).client!;

    await inv1.sendBinary(
      pb.encodeInvokeRequest({
        msg: {
          case: "start",
          value: {
            streamingInput: false,
            spec: {
              demand: { interface: "heavy.v1" },
              inputs: [],
              params: new Uint8Array(0),
              metadata: { slot: "1" },
            },
          },
        },
      }),
    );
    await inv2.sendBinary(
      pb.encodeInvokeRequest({
        msg: {
          case: "start",
          value: {
            streamingInput: false,
            spec: {
              demand: { interface: "heavy.v1" },
              inputs: [],
              params: new Uint8Array(0),
              metadata: { slot: "2" },
            },
          },
        },
      }),
    );

    // Clear small Accepted/Assigned events, then freeze client reading (`autoDrain = false`)!
    inv1.popDecodedFrames();
    inv2.popDecodedFrames();
    inv1.autoDrain = false;
    inv2.autoDrain = false;

    const assigns = extractAssigns(sessionWs);
    expect(assigns).toHaveLength(2);

    const uploadAllChunks = async (
      assign: pb.Assign,
      objs: Uint8Array[],
    ): Promise<FakeClientWebSocket> => {
      const att = (await platform.openWs("/ws/v2/worker/attempt")).client!;
      await att.sendBinary(
        pb.encodeAttemptUp({
          msg: {
            case: "open",
            value: { leaseId: assign.leaseId, generation: assign.generation },
          },
        }),
      );
      decodeAttemptDownMessages(att);
      for (let oi = 0; oi < objs.length; oi++) {
        const buf = objs[oi]!;
        const n = buf.byteLength / CHUNK;
        for (let c = 0; c < n; c++) {
          await att.sendBinary(
            pb.encodeAttemptUp({
              msg: {
                case: "result",
                value: {
                  index: oi,
                  data: buf.subarray(c * CHUNK, (c + 1) * CHUNK),
                  last: c === n - 1,
                  metadata: c === 0 ? { part: String(oi) } : {},
                  uri: "",
                },
              },
            }),
          );
        }
      }
      return att;
    };

    const att1 = await uploadAllChunks(assigns[0]!, job1Objs);
    const att2 = await uploadAllChunks(assigns[1]!, job2Objs);

    // Trigger BOTH 21 MiB Finish completions simultaneously!
    const finish1Promise = att1.sendBinary(
      pb.encodeAttemptUp({
        msg: {
          case: "finish",
          value: { outcome: { case: "succeeded", value: {} } },
        },
      }),
    );
    const finish2Promise = att2.sendBinary(
      pb.encodeAttemptUp({
        msg: {
          case: "finish",
          value: { outcome: { case: "succeeded", value: {} } },
        },
      }),
    );

    // Give microtasks/R2 reads a tick so Finish #1 enqueues (~21 MiB sitting in inv1.server.bufferedAmount)
    // while Finish #2 parks asynchronously waiting for the 56 MiB large-send semaphore!
    await new Promise((r) => setTimeout(r, 15));

    const budget = platform.engine.isolateBudget;
    expect(budget.largeSendWaitCount).toBeGreaterThanOrEqual(1);
    expect(inv1.server.bufferedAmount + inv2.server.bufferedAmount).toBeGreaterThan(
      20 * 1024 * 1024,
    );
    expect(inv1.server.bufferedAmount + inv2.server.bufferedAmount).toBeLessThan(
      30 * 1024 * 1024,
    ); // Only ONE 21 MiB frame is in native send queues at once!

    // Unrelated HTTP traffic MUST NOT be blocked while Finish #2 waits on large-send budget!
    const listDuringWait = await platform.unaryPost(
      "/rpc/v2/client/list_jobs",
      pb.encodeListJobsRequest({ states: [], limit: 10, pageToken: "" }),
    );
    expect(listDuringWait.status).toBe(200);

    // Drain slow reader #1 -> releases large-send budget -> Finish #2 immediately proceeds!
    inv1.drainAll();
    await new Promise((r) => setTimeout(r, 15));
    inv2.drainAll();

    await Promise.all([finish1Promise, finish2Promise]);

    // Verify peak memory & bufferedAmount stayed strictly inside the 56 MiB / 80 MiB caps!
    expect(budget.peakLargeSendBytes).toBeGreaterThan(40 * 1024 * 1024);
    expect(budget.peakLargeSendBytes).toBeLessThanOrEqual(LARGE_SEND_CAP_BYTES);
    expect(budget.peakIsolateBytes).toBeLessThanOrEqual(ISOLATE_MEMORY_CAP_BYTES);
    expect(budget.peakBufferedAmount).toBeGreaterThan(20 * 1024 * 1024);
    expect(budget.currentIsolateBytes).toBe(0);
    expect(budget.currentLargeSendBytes).toBe(0);

    // Verify BOTH slow readers received full 3 x 7 MiB ExecutionFinished frames + clean 1000 EOF!
    for (const [inv, expectedObjs] of [
      [inv1, job1Objs],
      [inv2, job2Objs],
    ] as const) {
      const evs = decodeExecutionEvents(inv);
      expect(evs).toHaveLength(1);
      expect(evs[0]!.event?.case).toBe("finished");
      const fin = (
        evs[0]!.event as { case: "finished"; value: pb.ExecutionFinished }
      ).value;
      expect(fin.state).toBe(pb.JobState.SUCCEEDED);
      expect(fin.outputs).toHaveLength(3);
      for (let k = 0; k < 3; k++) {
        const data = (
          fin.outputs[k]!.body as { case: "inline"; value: Uint8Array }
        ).value;
        expect(buffersEqual(data, expectedObjs[k]!)).toBe(true);
      }
      expect(inv.readyState).toBe(WS_READY_STATE_CLOSED);
      expect(inv.serverClosed?.code).toBe(1000);
    }
  });

  it("fails a stalled primary Invoke reader at consumer_stall_timeout while dropping a slow Watch without stalling workers", async () => {
    const platform = new SimulatedNativePlatform({
      overrides: {
        perJobEventCapBytes: 128 * 1024, // 128 KiB event cap for fast deterministic test
        consumerStallTimeoutMs: 5_000,
      },
    });
    const { ws: sessionWs } = await connectWorkerSession(platform, {
      interfaces: ["stream-out.v1"],
      maxConcurrency: 2,
    });

    // Case A: Slow Watch on a durable job is dropped with ResourceExhausted without stalling worker!
    await platform.unaryPost(
      "/rpc/v2/client/submit",
      pb.encodeSubmitRequest({
        jobId: "job-slow-watch",
        spec: {
          demand: { interface: "stream-out.v1" },
          inputs: [],
          params: new Uint8Array(0),
          metadata: {},
        },
        conflictPolicy: pb.ConflictPolicy.FAIL,
      }),
    );
    const assignWatch = extractAssigns(sessionWs)[0]!;
    const slowWatch = (await platform.openWs("/ws/v2/client/watch")).client!;
    await slowWatch.sendBinary(
      pb.encodeWatchRequest({ jobId: "job-slow-watch", afterSequence: 0n }),
    );
    slowWatch.popDecodedFrames();
    slowWatch.autoDrain = false;

    const attA = (await platform.openWs("/ws/v2/worker/attempt")).client!;
    await attA.sendBinary(
      pb.encodeAttemptUp({
        msg: {
          case: "open",
          value: { leaseId: assignWatch.leaseId, generation: assignWatch.generation },
        },
      }),
    );
    decodeAttemptDownMessages(attA);

    const frameChunk = makeDeterministicBytes(60 * 1024, 9);
    for (let seq = 1n; seq <= 4n; seq++) {
      await attA.sendBinary(
        pb.encodeAttemptUp({
          msg: {
            case: "frame",
            value: { sequence: seq, data: frameChunk, metadata: {}, progress: false },
          },
        }),
      );
    }
    // Slow watcher exceeded 128 KiB bufferedAmount -> dropped with ResourceExhausted(8) while worker continues!
    slowWatch.drainAll();
    const watchFrames = slowWatch.popDecodedFrames();
    const watchErr = watchFrames.find((f) => f.kind === "error");
    expect(watchErr).toBeDefined();
    expect((watchErr as { kind: "error"; code: number }).code).toBe(
      GrpcCode.ResourceExhausted,
    );

    await attA.sendBinary(
      pb.encodeAttemptUp({
        msg: {
          case: "finish",
          value: { outcome: { case: "succeeded", value: {} } },
        },
      }),
    );
    expect(
      decodeAttemptDownMessages(attA).some(
        (m) =>
          m.msg?.case === "finished" &&
          m.msg.value.status === pb.AttemptFinishedStatus.COMMITTED,
      ),
    ).toBe(true);

    // Case B: Stalled primary Invoke reader delays worker and fails with `consumer_stalled` at consumerStallTimeoutMs!
    const stalledInvoke = (await platform.openWs("/ws/v2/client/invoke")).client!;
    await stalledInvoke.sendBinary(
      pb.encodeInvokeRequest({
        msg: {
          case: "start",
          value: {
            streamingInput: false,
            spec: {
              demand: { interface: "stream-out.v1" },
              inputs: [],
              params: new Uint8Array(0),
              metadata: {},
            },
          },
        },
      }),
    );
    stalledInvoke.popDecodedFrames();
    stalledInvoke.autoDrain = false;

    const assignInvoke = extractAssigns(sessionWs)[0]!;
    const attB = (await platform.openWs("/ws/v2/worker/attempt")).client!;
    await attB.sendBinary(
      pb.encodeAttemptUp({
        msg: {
          case: "open",
          value: { leaseId: assignInvoke.leaseId, generation: assignInvoke.generation },
        },
      }),
    );
    decodeAttemptDownMessages(attB);

    await attB.sendBinary(
      pb.encodeAttemptUp({
        msg: {
          case: "frame",
          value: { sequence: 1n, data: frameChunk, metadata: {}, progress: false },
        },
      }),
    );
    await attB.sendBinary(
      pb.encodeAttemptUp({
        msg: {
          case: "frame",
          value: { sequence: 2n, data: frameChunk, metadata: {}, progress: false },
        },
      }),
    );
    // Frame 3 exceeds 128 KiB primary Invoke buffer -> arms stall timer!
    await attB.sendBinary(
      pb.encodeAttemptUp({
        msg: {
          case: "frame",
          value: { sequence: 3n, data: frameChunk, metadata: {}, progress: false },
        },
      }),
    );

    await platform.advanceAndFireAlarms(5_100);
    stalledInvoke.drainAll();
    const invEvents = decodeExecutionEvents(stalledInvoke);
    const termEv = invEvents.find((e) => e.event?.case === "finished");
    expect(termEv).toBeDefined();
    const finVal = (
      termEv!.event as { case: "finished"; value: pb.ExecutionFinished }
    ).value;
    expect(finVal.state).toBe(pb.JobState.FAILED);
    expect(finVal.error?.code).toBe(RUNTIME_CODES.CONSUMER_STALLED);
  });
});

describe("Reference-job per-step row accounting & cold-wake zero-write Heartbeat (6.3, 6.4, 10)", () => {
  it("writes 0 SQLite rows and sets 0 alarms on Heartbeat, including immediately after cold-wake hibernation", async () => {
    const platform = new SimulatedNativePlatform();
    const { ws: sessionWs } = await connectWorkerSession(platform, {
      workerId: "w-hb",
      interfaces: ["hb.v1"],
    });

    // Warm heartbeat: 0 SQL writes, 0 setAlarm calls
    const writesBeforeWarm = platform.storage.totalRowsWritten;
    const alarmsBeforeWarm = platform.setAlarmCallCount;
    await sessionWs.sendBinary(
      pb.encodeWorkerMessage({
        msg: {
          case: "heartbeat",
          value: {
            load: { queued: 1, running: 2, utilization: 0.5, custom: { gpu_temp: 62 } },
          },
        },
      }),
    );
    expect(platform.storage.totalRowsWritten - writesBeforeWarm).toBe(0);
    expect(platform.setAlarmCallCount - alarmsBeforeWarm).toBe(0);

    // Cold-wake hibernation + 10 heartbeats across multiple wakes: STILL 0 SQL writes, 0 setAlarm calls!
    for (let cycle = 0; cycle < 3; cycle++) {
      platform.advanceMs(30_000);
      platform.simulateHibernation();
      await new Promise((r) => setTimeout(r, 2)); // allow async wake alarm sync to settle

      const writesBeforeCold = platform.storage.totalRowsWritten;
      const alarmsBeforeCold = platform.setAlarmCallCount;
      const bootWritesBefore = platform.engine.store.bootRowsWritten;

      await sessionWs.sendBinary(
        pb.encodeWorkerMessage({
          msg: {
            case: "heartbeat",
            value: {
              load: {
                queued: cycle,
                running: 1,
                utilization: 0.25,
                custom: { cycle },
              },
            },
          },
        }),
      );

      expect(platform.storage.totalRowsWritten - writesBeforeCold).toBe(0);
      expect(platform.engine.store.bootRowsWritten - bootWritesBefore).toBe(0);
      expect(platform.setAlarmCallCount - alarmsBeforeCold).toBe(0);
    }
  });

  it("measures full-lifecycle reference-job row writes per step (~24-28 rows incl. indexes, alarms, and 24h purge)", async () => {
    const platform = new SimulatedNativePlatform();
    const { ws: sessionWs } = await connectWorkerSession(platform, {
      workerId: "w-ref",
      interfaces: ["ref.v1"],
    });

    // Settle initial worker connection writes before measuring the reference job lifecycle
    const snap = () =>
      platform.storage.totalRowsWritten + platform.setAlarmCallCount;

    const t0 = snap();
    const readsStart = platform.storage.totalRowsRead;
    const refInputs: pb.ProtoObjectInput[] = Array.from({ length: 8 }, (_, i) => ({
      body: { case: "inline" as const, value: utf8Encoder.encode(`in-${i}`) },
      metadata: {},
    }));

    // Step 1: Submit + immediate dispatch/Assign
    const sub = await platform.unaryPost(
      "/rpc/v2/client/submit",
      pb.encodeSubmitRequest({
        jobId: "ref-job-1",
        spec: {
          demand: { interface: "ref.v1" },
          inputs: refInputs,
          params: utf8Encoder.encode("{}"),
          metadata: { env: "prod" },
        },
        conflictPolicy: pb.ConflictPolicy.FAIL,
      }),
    );
    expect(sub.status).toBe(200);
    const tAfterSubmitAssign = snap();
    const submitAndAssignWrites = tAfterSubmitAssign - t0;

    // Step 2: Attempt handshake (`openWs` conn_seq + `AttemptOpen` lease update)
    const assign = extractAssigns(sessionWs)[0]!;
    const attWs = (await platform.openWs("/ws/v2/worker/attempt")).client!;
    await attWs.sendBinary(
      pb.encodeAttemptUp({
        msg: {
          case: "open",
          value: { leaseId: assign.leaseId, generation: assign.generation },
        },
      }),
    );
    decodeAttemptDownMessages(attWs);
    const tAfterOpen = snap();
    const openWrites = tAfterOpen - tAfterSubmitAssign;

    // Step 3: Single-chunk output (`<= 16 KiB` stored inline on lease, 0 upload_tail rows!) + Finish commit
    await attWs.sendBinary(
      pb.encodeAttemptUp({
        msg: {
          case: "result",
          value: {
            index: 0,
            data: utf8Encoder.encode("small-reference-output"),
            last: true,
            metadata: {},
            uri: "",
          },
        },
      }),
    );
    const tailCount = platform.storage.sql
      .exec<{ c: number }>(`SELECT COUNT(*) AS c FROM native_upload_tail`)
      .toArray()[0]!.c;
    expect(tailCount).toBe(0);

    await attWs.sendBinary(
      pb.encodeAttemptUp({
        msg: {
          case: "finish",
          value: { outcome: { case: "succeeded", value: {} } },
        },
      }),
    );
    const tAfterFinish = snap();
    const resultAndFinishWrites = tAfterFinish - tAfterOpen;

    // Disconnect idle worker cleanly so we isolate the 24h retention purge alarm
    await sessionWs.closeClean(1000, "");
    const tBeforePurge = snap();

    // Step 4: Advance past 24h retention and run purge alarm
    platform.advanceMs(86_400_000 + 10_000);
    await platform.engine.alarm();
    const tAfterPurge = snap();
    const purgeWrites = tAfterPurge - tBeforePurge;

    const totalLifecycleWrites =
      submitAndAssignWrites + openWrites + resultAndFinishWrites + purgeWrites;

    // Verify job, bundles, events, and receipt were completely purged
    expect(
      platform.storage.sql
        .exec<{ c: number }>(`SELECT COUNT(*) AS c FROM native_jobs`)
        .toArray()[0]!.c,
    ).toBe(0);
    expect(
      platform.storage.sql
        .exec<{ c: number }>(`SELECT COUNT(*) AS c FROM native_object_bundles`)
        .toArray()[0]!.c,
    ).toBe(0);

    // Reference job full lifecycle (incl. secondary index maintenance & alarms & purge) stays in ~22-30 rows!
    expect(submitAndAssignWrites).toBeGreaterThanOrEqual(8);
    // conn_seq is block-allocated (one meta write per CONN_SEQ_BLOCK ids), so the attempt upgrade writes
    // only the lease row (previously 2 = lease + standalone conn_seq upsert).
    expect(openWrites).toBeLessThanOrEqual(1);
    expect(resultAndFinishWrites).toBeLessThanOrEqual(9);
    expect(purgeWrites).toBeLessThanOrEqual(10);
    expect(totalLifecycleWrites).toBeGreaterThanOrEqual(20);
    expect(totalLifecycleWrites).toBeLessThanOrEqual(29); // baseline before optimisation: 30
    expect(platform.storage.totalRowsRead - readsStart).toBeLessThanOrEqual(30); // baseline: 32
    expect(platform.r2!.putCount).toBe(0);
  });
});

describe("Watchers & cursors across hibernation incl. after == oldest - 1 and OutOfRange(11) (4.3, 4.4, 10)", () => {
  it("resumes two watchers at different cursors across hibernation and enforces after == oldest - 1 vs OutOfRange(11)", async () => {
    const platform = new SimulatedNativePlatform({
      overrides: { eventLogMaxEvents: 5 },
    });
    const { ws: sessionWs } = await connectWorkerSession(platform, {
      interfaces: ["watch.v1"],
    });

    await platform.unaryPost(
      "/rpc/v2/client/submit",
      pb.encodeSubmitRequest({
        jobId: "job-watch-cursors",
        spec: {
          demand: { interface: "watch.v1" },
          inputs: [],
          params: new Uint8Array(0),
          metadata: {},
        },
        conflictPolicy: pb.ConflictPolicy.FAIL,
      }),
    );
    const assign = extractAssigns(sessionWs)[0]!;
    const attWs = (await platform.openWs("/ws/v2/worker/attempt")).client!;
    await attWs.sendBinary(
      pb.encodeAttemptUp({
        msg: {
          case: "open",
          value: { leaseId: assign.leaseId, generation: assign.generation },
        },
      }),
    );
    decodeAttemptDownMessages(attWs);

    // Emit Frame 1 so we have: Accepted (seq base+1), Assigned (seq base+2), Output#1 (seq base+3)
    await attWs.sendBinary(
      pb.encodeAttemptUp({
        msg: {
          case: "frame",
          value: {
            sequence: 1n,
            data: utf8Encoder.encode("f1"),
            metadata: {},
            progress: false,
          },
        },
      }),
    );

    // Watcher 1 starts from 0; Watcher 2 starts after Assigned (base + 2)
    const w1 = (await platform.openWs("/ws/v2/client/watch")).client!;
    await w1.sendBinary(
      pb.encodeWatchRequest({ jobId: "job-watch-cursors", afterSequence: 0n }),
    );
    const w1Initial = decodeExecutionEvents(w1);
    expect(w1Initial).toHaveLength(3); // Accepted, Assigned, Output#1
    const seqAssigned = w1Initial[1]!.sequence;

    const w2 = (await platform.openWs("/ws/v2/client/watch")).client!;
    await w2.sendBinary(
      pb.encodeWatchRequest({
        jobId: "job-watch-cursors",
        afterSequence: seqAssigned,
      }),
    );
    const w2Initial = decodeExecutionEvents(w2);
    expect(w2Initial).toHaveLength(1); // Only Output#1

    // Hibernate with BOTH watchers open at their own persisted `delivered_seq`!
    platform.simulateHibernation();

    // Emit Frames 2..6 (evicting early events past `eventLogMaxEvents = 5`)!
    for (let s = 2n; s <= 6n; s++) {
      await attWs.sendBinary(
        pb.encodeAttemptUp({
          msg: {
            case: "frame",
            value: {
              sequence: s,
              data: utf8Encoder.encode(`f${s}`),
              metadata: {},
              progress: false,
            },
          },
        }),
      );
    }

    const w1More = decodeExecutionEvents(w1);
    const w2More = decodeExecutionEvents(w2);
    expect(w1More).toHaveLength(5);
    expect(w2More).toHaveLength(5);
    expect(w1More.map((e) => e.sequence)).toEqual(w2More.map((e) => e.sequence));

    // Check evictedThrough (`oldest - 1`) boundary!
    const jobRow = platform.storage.sql
      .exec<{ evicted_seq_hi: number; evicted_seq_lo: number }>(
        `SELECT evicted_seq_hi, evicted_seq_lo FROM native_jobs WHERE raw_job_id = 'job-watch-cursors'`,
      )
      .toArray()[0]!;
    const evictedThrough =
      (BigInt(jobRow.evicted_seq_hi >>> 0) << 32n) |
      BigInt(jobRow.evicted_seq_lo >>> 0);
    const oldestAvailable = evictedThrough + 1n;
    expect(evictedThrough).toBeGreaterThan(0n);

    // `after == oldest - 1` (`evictedThrough`) MUST SUCCEED!
    const wExactBoundary = (await platform.openWs("/ws/v2/client/watch")).client!;
    await wExactBoundary.sendBinary(
      pb.encodeWatchRequest({
        jobId: "job-watch-cursors",
        afterSequence: evictedThrough,
      }),
    );
    const boundaryEvs = decodeExecutionEvents(wExactBoundary);
    expect(boundaryEvs[0]!.sequence).toBe(oldestAvailable);

    // `0 < after < oldest - 1` (`evictedThrough - 1n`) MUST FAIL with OutOfRange(11) carrying `oldest`!
    const wExpired = (await platform.openWs("/ws/v2/client/watch")).client!;
    await wExpired.sendBinary(
      pb.encodeWatchRequest({
        jobId: "job-watch-cursors",
        afterSequence: evictedThrough - 1n,
      }),
    );
    const expFrames = wExpired.popDecodedFrames();
    expect(expFrames).toHaveLength(1);
    expect(expFrames[0]!.kind).toBe("error");
    if (expFrames[0]!.kind === "error") {
      expect(expFrames[0]!.code).toBe(GrpcCode.OutOfRange);
      expect(expFrames[0]!.message).toContain(oldestAvailable.toString());
    }

    // Finish job -> active watchers receive Finished + close(1000) and are cleaned from native_watchers!
    await attWs.sendBinary(
      pb.encodeAttemptUp({
        msg: {
          case: "finish",
          value: { outcome: { case: "succeeded", value: {} } },
        },
      }),
    );
    expect(decodeExecutionEvents(w1).some((e) => e.event?.case === "finished")).toBe(
      true,
    );
    expect(decodeExecutionEvents(w2).some((e) => e.event?.case === "finished")).toBe(
      true,
    );
    expect(
      platform.storage.sql
        .exec<{ c: number }>(`SELECT COUNT(*) AS c FROM native_watchers`)
        .toArray()[0]!.c,
    ).toBe(0);
  });
});

describe("Session rules: oversized Load.custom, non-extending grace across wakes, stale Close, 1005/1006, handshake timeouts, caps & auth (4.1-4.3, 5.3, 10)", () => {
  it("bounds oversized Heartbeat.Load.custom in memory (<=64 / <=8 KiB), keeps attachment <=256B, and clears custom on wake", async () => {
    const platform = new SimulatedNativePlatform();
    const { ws: sessionWs } = await connectWorkerSession(platform, {
      workerId: "w-custom",
      interfaces: ["custom.v1"],
    });

    // Build 100 custom entries (legal <= 256 KiB control message, > 64 entries)
    const bigCustom: Record<string, number> = {};
    for (let i = 0; i < 100; i++) {
      bigCustom[`metric_${String(i).padStart(3, "0")}_${"x".repeat(40)}`] = i * 1.5;
    }

    await sessionWs.sendBinary(
      pb.encodeWorkerMessage({
        msg: {
          case: "heartbeat",
          value: {
            load: { queued: 3, running: 1, utilization: 0.75, custom: bigCustom },
          },
        },
      }),
    );

    const workers1 = (await (
      await platform.engine.fetch(
        new Request("https://anyserve.example.com/api/workers"),
      )
    ).json()) as { workers: Array<Record<string, any>> };
    const keptKeys = Object.keys(workers1.workers[0]!.load.custom);
    expect(keptKeys.length).toBe(64);

    const metricsText = await (
      await platform.engine.fetch(
        new Request("https://anyserve.example.com/metrics"),
      )
    ).text();
    expect(metricsText).toContain(
      "anyserve_native_dropped_custom_load_entries_total 36",
    );

    // After hibernation wake, scalar load (queued/running/utilization) survives via attachment, while customLoad resets to {}
    platform.simulateHibernation();
    const workers2 = (await (
      await platform.engine.fetch(
        new Request("https://anyserve.example.com/api/workers"),
      )
    ).json()) as { workers: Array<Record<string, any>> };
    expect(workers2.workers[0]!.load.queued).toBe(3);
    expect(workers2.workers[0]!.load.running).toBe(1);
    expect(workers2.workers[0]!.load.utilization).toBe(0.75);
    expect(workers2.workers[0]!.load.custom).toEqual({});
  });

  it("never extends reconnect_grace across repeated hibernation wakes, ignores stale Close of replaced session, and handles 1005/1006 + missing Hello/Open", async () => {
    const platform = new SimulatedNativePlatform({
      overrides: {
        reconnectGraceMs: 30_000,
        heartbeatTimeoutMs: 90_000,
        defaultMaxAttempts: 2,
      },
    });

    const { ws: ws1, welcome: w1 } = await connectWorkerSession(platform, {
      workerId: "w-grace",
      interfaces: ["grace.v1"],
    });

    // Replace session with new connection BEFORE closing old socket -> old socket's late Close must be ignored!
    const { ws: ws2, welcome: w2 } = await connectWorkerSession(platform, {
      workerId: "w-grace",
      interfaces: ["grace.v1"],
      resumeSessionId: w1.sessionId,
    });
    expect(w2.sessionId).toBe(w1.sessionId);
    await ws1.dropAbnormal1006(); // Stale close on superseded connection!
    expect(platform.engine.listWorkersForConsole(platform.nowMs())[0]!.connected).toBe(
      true,
    );

    // Assign a job so session holds an active lease, then drop ws2 abnormally with 1006!
    await platform.unaryPost(
      "/rpc/v2/client/submit",
      pb.encodeSubmitRequest({
        jobId: "job-grace-1",
        spec: {
          demand: { interface: "grace.v1" },
          inputs: [],
          params: new Uint8Array(0),
          metadata: {},
        },
        conflictPolicy: pb.ConflictPolicy.FAIL,
      }),
    );
    expect(extractAssigns(ws2)).toHaveLength(1);

    const disconnectAt = platform.nowMs();
    await ws2.dropAbnormal1006();
    // Server must NEVER echo 1006!
    expect(ws2.server.closeCalls).toHaveLength(0);

    const expectedGraceDeadline = disconnectAt + 30_000;
    const graceRow1 = platform.storage.sql
      .exec<{ grace_deadline_ms: number }>(
        `SELECT grace_deadline_ms FROM native_sessions WHERE session_id = ?`,
        w1.sessionId,
      )
      .toArray()[0]!;
    expect(graceRow1.grace_deadline_ms).toBe(expectedGraceDeadline);

    // Wake repeatedly at +10s and +20s -> grace_deadline_ms MUST NOT move!
    platform.advanceMs(10_000);
    platform.simulateHibernation();
    platform.advanceMs(10_000);
    platform.simulateHibernation();

    const graceRow2 = platform.storage.sql
      .exec<{ grace_deadline_ms: number }>(
        `SELECT grace_deadline_ms FROM native_sessions WHERE session_id = ?`,
        w1.sessionId,
      )
      .toArray()[0]!;
    expect(graceRow2.grace_deadline_ms).toBe(expectedGraceDeadline);

    // At +30s total (+10s more), reconnect_grace expires -> session removed and job re-queued!
    await platform.advanceAndFireAlarms(10_000);
    expect(platform.engine.listWorkersForConsole(platform.nowMs())).toHaveLength(0);
    const reQueued = pb.decodeJob(
      (
        await platform.unaryPost(
          "/rpc/v2/client/get_job",
          pb.encodeGetJobRequest({ jobId: "job-grace-1", includeOutputs: false }),
        )
      ).bytes,
    );
    expect(reQueued.state).toBe(pb.JobState.QUEUED);

    // Test 1005 plain close completion (never sends reserved 1005) & missing Hello/Open DeadlineExceeded(4)!
    const silentSession = (await platform.openWs("/ws/v2/worker/session")).client!;
    const silentAttempt = (await platform.openWs("/ws/v2/worker/attempt")).client!;
    const plainCloseWatch = (await platform.openWs("/ws/v2/client/watch")).client!;
    await plainCloseWatch.closeNoStatus1005();
    expect(plainCloseWatch.server.closeCalls).toEqual([
      { code: undefined, reason: undefined },
    ]);

    await platform.advanceAndFireAlarms(90_001);
    for (const s of [silentSession, silentAttempt]) {
      const f = s.popDecodedFrames()[0]!;
      expect(f.kind).toBe("error");
      if (f.kind === "error") {
        expect(f.code).toBe(GrpcCode.DeadlineExceeded);
      }
    }
  });

  it("enforces pre-upgrade auth (401/16, 403/7, 400/3) and concurrent socket caps (429/8)", async () => {
    const platform = new SimulatedNativePlatform({
      env: {
        ANYSERVE_CLIENT_TOKENS: "client-secret-token",
        ANYSERVE_WORKER_TOKENS: "worker-secret-token",
      },
      overrides: {
        maxWatchSockets: 2,
      },
    });

    // Missing token -> 401 / Unauthenticated(16)
    const r401 = await platform.openWs("/ws/v2/client/watch");
    expect(r401.status).toBe(401);
    expect(r401.response.headers.get("x-anyserve-grpc-code")).toBe("16");

    // Worker token on client endpoint -> 403 / PermissionDenied(7)
    const r403 = await platform.openWs("/ws/v2/client/watch", {
      Authorization: "Bearer worker-secret-token",
    });
    expect(r403.status).toBe(403);
    expect(r403.response.headers.get("x-anyserve-grpc-code")).toBe("7");

    // Invalid namespace in anonymous header mode -> 400 / InvalidArgument(3)
    const anonPlatform = new SimulatedNativePlatform();
    const r400 = await anonPlatform.openWs("/ws/v2/client/watch", {
      "x-anyserve-namespace": "bad/namespace!",
    });
    expect(r400.status).toBe(400);
    expect(r400.response.headers.get("x-anyserve-grpc-code")).toBe("3");

    // Open 2 watch sockets -> 3rd hits cap and returns 429 / ResourceExhausted(8)
    const ok1 = await platform.openWs("/ws/v2/client/watch", {
      Authorization: "Bearer client-secret-token",
    });
    const ok2 = await platform.openWs("/ws/v2/client/watch", {
      Authorization: "Bearer client-secret-token",
    });
    expect(ok1.status).toBe(101);
    expect(ok2.status).toBe(101);

    const r429 = await platform.openWs("/ws/v2/client/watch", {
      Authorization: "Bearer client-secret-token",
    });
    expect(r429.status).toBe(429);
    expect(r429.response.headers.get("x-anyserve-grpc-code")).toBe("8");
  });
});

describe("Cancel, retry, interruption before setAlarm, 200ms timer precision, admin reset & winner replay (4.5, 4.6, 6.3, 10)", () => {
  it("keeps slot reservation on cancelling=1 across wake until late Finish (FENCED), then dispatches queued work", async () => {
    const platform = new SimulatedNativePlatform();
    const { ws: sessionWs } = await connectWorkerSession(platform, {
      workerId: "w-1slot",
      interfaces: ["cancel.v1"],
      maxConcurrency: 1, // Only 1 slot!
      totalCapacity: { vram_mb: 16_384n },
    });

    // Submit Job 1 (consumes the 1 slot + 16 GiB VRAM) and Job 2 (waits in queue)
    for (const jid of ["cancel-j1", "cancel-j2"]) {
      await platform.unaryPost(
        "/rpc/v2/client/submit",
        pb.encodeSubmitRequest({
          jobId: jid,
          spec: {
            demand: {
              interface: "cancel.v1",
              requiredAttributes: {},
              preferredAttributes: {},
              requiredCapacity: { vram_mb: 16_384n },
              affinityKey: "",
            },
            inputs: [],
            params: new Uint8Array(0),
            metadata: {},
          },
          conflictPolicy: pb.ConflictPolicy.FAIL,
        }),
      );
    }

    const assign1 = extractAssigns(sessionWs)[0]!;
    expect(assign1.jobId).toBe("cancel-j1");

    const att1 = (await platform.openWs("/ws/v2/worker/attempt")).client!;
    await att1.sendBinary(
      pb.encodeAttemptUp({
        msg: {
          case: "open",
          value: { leaseId: assign1.leaseId, generation: assign1.generation },
        },
      }),
    );
    decodeAttemptDownMessages(att1);

    // Client cancels Job 1 -> Job 1 becomes CANCELLED immediately, Cancel sent to worker,
    // AND even if worker reports Credit{freeSlots: 1}, server's cancelling=1 lease keeps maxConcurrency & VRAM reserved!
    const cancelRes = await platform.unaryPost(
      "/rpc/v2/client/cancel",
      pb.encodeCancelRequest({ jobId: "cancel-j1", reason: "user abort" }),
    );
    expect(pb.decodeJob(cancelRes.bytes).state).toBe(pb.JobState.CANCELLED);
    await sessionWs.sendBinary(
      pb.encodeWorkerMessage({
        msg: { case: "credit", value: { freeSlots: 1 } },
      }),
    );
    expect(extractAssigns(sessionWs)).toHaveLength(0);

    // Simulate hibernation while lease is `cancelling = 1`:
    // After wake, `running` count is 0, while worker slot reservation is STILL kept!
    platform.simulateHibernation();
    const snapAfterWake = platform.engine.getOverviewSnapshot(platform.nowMs());
    expect(snapAfterWake.running).toBe(0);
    expect(snapAfterWake.queued).toBe(1);
    expect(snapAfterWake.workersRollup.active_leases).toBe(1);

    // Worker sends late Finish on Job 1 -> receives AttemptFinished::FENCED, releases slot, and Job 2 dispatches!
    await att1.sendBinary(
      pb.encodeAttemptUp({
        msg: {
          case: "finish",
          value: { outcome: { case: "succeeded", value: {} } },
        },
      }),
    );
    const att1Down = decodeAttemptDownMessages(att1);
    expect(att1Down[0]!.msg).toEqual({
      case: "finished",
      value: {
        status: pb.AttemptFinishedStatus.FENCED,
        reason: "the job was cancelled",
      },
    });

    const assign2 = extractAssigns(sessionWs);
    expect(assign2).toHaveLength(1);
    expect(assign2[0]!.jobId).toBe("cancel-j2");
  });

  it("enforces exact 200ms retry backoff (< 2s total_timeout, never 5s-bucketed), handles interruption before setAlarm, and is idempotent on duplicate alarms", async () => {
    const platform = new SimulatedNativePlatform();
    const { ws: ws1 } = await connectWorkerSession(platform, {
      workerId: "w-fail-1",
      interfaces: ["retry.v1"],
    });
    const { ws: ws2 } = await connectWorkerSession(platform, {
      workerId: "w-good-2",
      interfaces: ["retry.v1"],
    });

    // Part 1: Exact 200ms retry timer vs 2000ms total_timeout
    const submitAt = platform.nowMs();
    await platform.unaryPost(
      "/rpc/v2/client/submit",
      pb.encodeSubmitRequest({
        jobId: "job-200ms-retry",
        spec: {
          demand: { interface: "retry.v1", affinityKey: "pick-w1" },
          inputs: [],
          params: new Uint8Array(0),
          metadata: {},
          policy: {
            durability: pb.Durability.DURABLE,
            priority: 1,
            queueTimeoutMs: 0n,
            executionTimeoutMs: 0n,
            totalTimeoutMs: 2_000n, // 2s total timeout!
            spilloverAfterMs: 0n,
            maxAttempts: 3,
            fairnessKey: "",
            concurrencyKey: "",
          },
        },
        conflictPolicy: pb.ConflictPolicy.FAIL,
      }),
    );

    const firstAssign = [...extractAssigns(ws1), ...extractAssigns(ws2)][0]!;
    platform.advanceMs(100);
    const failAt = platform.nowMs();

    const att1 = (await platform.openWs("/ws/v2/worker/attempt")).client!;
    await att1.sendBinary(
      pb.encodeAttemptUp({
        msg: {
          case: "open",
          value: {
            leaseId: firstAssign.leaseId,
            generation: firstAssign.generation,
          },
        },
      }),
    );
    decodeAttemptDownMessages(att1);

    await att1.sendBinary(
      pb.encodeAttemptUp({
        msg: {
          case: "finish",
          value: {
            outcome: {
              case: "failed",
              value: { code: "transient", message: "try again", retryable: true },
            },
          },
        },
      }),
    );

    // Exact business timer MUST be armed at `failAt + 200ms`, NOT at a 5,000ms bucket!
    expect(platform.getAlarmMs()).toBe(failAt + 200);

    // Advance 200ms and fire alarm (plus duplicate alarm calls to prove idempotency!)
    await platform.advanceAndFireAlarms(200);
    await platform.engine.alarm();
    await platform.engine.alarm();

    const retryAssign = [...extractAssigns(ws1), ...extractAssigns(ws2)][0]!;
    expect(retryAssign).toBeDefined();
    expect(retryAssign.attemptNumber).toBe(2);
    expect(platform.nowMs() - submitAt).toBe(300); // Well inside the 2,000ms total timeout!

    const att2 = (await platform.openWs("/ws/v2/worker/attempt")).client!;
    await att2.sendBinary(
      pb.encodeAttemptUp({
        msg: {
          case: "open",
          value: {
            leaseId: retryAssign.leaseId,
            generation: retryAssign.generation,
          },
        },
      }),
    );
    decodeAttemptDownMessages(att2);
    await att2.sendBinary(
      pb.encodeAttemptUp({
        msg: {
          case: "finish",
          value: { outcome: { case: "succeeded", value: {} } },
        },
      }),
    );

    const doneJob = pb.decodeJob(
      (
        await platform.unaryPost(
          "/rpc/v2/client/get_job",
          pb.encodeGetJobRequest({ jobId: "job-200ms-retry", includeOutputs: false }),
        )
      ).bytes,
    );
    expect(doneJob.state).toBe(pb.JobState.SUCCEEDED);

    // Part 2: Simulated interruption between retry commit and `setAlarm`!
    await platform.unaryPost(
      "/rpc/v2/client/submit",
      pb.encodeSubmitRequest({
        jobId: "job-interrupted-alarm",
        spec: {
          demand: { interface: "retry.v1" },
          inputs: [],
          params: new Uint8Array(0),
          metadata: {},
        },
        conflictPolicy: pb.ConflictPolicy.FAIL,
      }),
    );
    const aInt1 = [...extractAssigns(ws1), ...extractAssigns(ws2)][0]!;
    const attInt1 = (await platform.openWs("/ws/v2/worker/attempt")).client!;
    await attInt1.sendBinary(
      pb.encodeAttemptUp({
        msg: {
          case: "open",
          value: { leaseId: aInt1.leaseId, generation: aInt1.generation },
        },
      }),
    );
    decodeAttemptDownMessages(attInt1);

    // Inject failure on `setAlarm` right when retryable Finish commits!
    platform.failNextSetAlarm = true;
    await attInt1.sendBinary(
      pb.encodeAttemptUp({
        msg: {
          case: "finish",
          value: {
            outcome: {
              case: "failed",
              value: { code: "net_blip", message: "retry me", retryable: true },
            },
          },
        },
      }),
    );

    // Advance 250ms so `retry_at_ms` is overdue and wake from hibernation -> `rehydrateAfterWake` MUST scan and dispatch it!
    platform.advanceMs(250);
    platform.simulateHibernation();
    const aInt2 = [...extractAssigns(ws1), ...extractAssigns(ws2)][0]!;
    expect(aInt2).toBeDefined();
    expect(aInt2.jobId).toBe("job-interrupted-alarm");
    expect(aInt2.attemptNumber).toBe(2);
  });

  it("replays durable winner after 10-min receipt expiry AND after POST /api/admin/reset (matching = COMMITTED, conflicting = REJECTED), and enforces admin reset exposure/exhaustion rules", async () => {
    const platform = new SimulatedNativePlatform();
    const { ws: sessionWs } = await connectWorkerSession(platform, {
      workerId: "w-reset",
      interfaces: ["reset.v1"],
      maxConcurrency: 4,
    });

    // 1. Complete a durable winner job ("job-winner")
    await platform.unaryPost(
      "/rpc/v2/client/submit",
      pb.encodeSubmitRequest({
        jobId: "job-winner",
        spec: {
          demand: { interface: "reset.v1" },
          inputs: [],
          params: new Uint8Array(0),
          metadata: {},
        },
        conflictPolicy: pb.ConflictPolicy.FAIL,
      }),
    );
    const winAssign = extractAssigns(sessionWs)[0]!;
    const winBytes = utf8Encoder.encode("canonical-winner-payload");
    const attWin = (await platform.openWs("/ws/v2/worker/attempt")).client!;
    await attWin.sendBinary(
      pb.encodeAttemptUp({
        msg: {
          case: "open",
          value: { leaseId: winAssign.leaseId, generation: winAssign.generation },
        },
      }),
    );
    decodeAttemptDownMessages(attWin);
    await attWin.sendBinary(
      pb.encodeAttemptUp({
        msg: {
          case: "result",
          value: {
            index: 0,
            data: winBytes,
            last: true,
            metadata: { v: "1" },
            uri: "",
          },
        },
      }),
    );
    await attWin.sendBinary(
      pb.encodeAttemptUp({
        msg: {
          case: "finish",
          value: { outcome: { case: "succeeded", value: {} } },
        },
      }),
    );
    expect(
      decodeAttemptDownMessages(attWin).some(
        (m) =>
          m.msg?.case === "finished" &&
          m.msg.value.status === pb.AttemptFinishedStatus.COMMITTED,
      ),
    ).toBe(true);

    // Advance 11 minutes (> 10 min `receipt_retention_ms`) while sending heartbeats so worker stays alive, then run alarm!
    for (let m = 0; m < 15; m++) {
      platform.advanceMs(45_000);
      await sessionWs.sendBinary(
        pb.encodeWorkerMessage({ msg: { case: "heartbeat", value: {} } }),
      );
    }
    await platform.engine.alarm();

    // Verify the 10-minute `native_receipts` row HAS expired and been deleted!
    expect(
      platform.storage.sql
        .exec<{ c: number }>(`SELECT COUNT(*) AS c FROM native_receipts`)
        .toArray()[0]!.c,
    ).toBe(0);

    // 2. Start three active jobs before calling POST /api/admin/reset:
    //    - Exposed Invoke job -> must fail with `output_interrupted`!
    //    - Exhausted durable job (`maxAttempts = 1`, already on attempt 1) -> must fail with `attempts_exhausted`!
    //    - Unexposed durable job (`maxAttempts = 3`, on attempt 1) -> must re-queue under generation 2!
    const exposedInv = (await platform.openWs("/ws/v2/client/invoke")).client!;
    await exposedInv.sendBinary(
      pb.encodeInvokeRequest({
        msg: {
          case: "start",
          value: {
            streamingInput: false,
            spec: {
              demand: { interface: "reset.v1" },
              inputs: [],
              params: new Uint8Array(0),
              metadata: {},
            },
          },
        },
      }),
    );
    await platform.unaryPost(
      "/rpc/v2/client/submit",
      pb.encodeSubmitRequest({
        jobId: "job-exhausted",
        spec: {
          demand: { interface: "reset.v1" },
          inputs: [],
          params: new Uint8Array(0),
          metadata: {},
          policy: {
            durability: pb.Durability.DURABLE,
            priority: 0,
            queueTimeoutMs: 0n,
            executionTimeoutMs: 0n,
            totalTimeoutMs: 0n,
            spilloverAfterMs: 0n,
            maxAttempts: 1,
            fairnessKey: "",
            concurrencyKey: "",
          },
        },
        conflictPolicy: pb.ConflictPolicy.FAIL,
      }),
    );
    await platform.unaryPost(
      "/rpc/v2/client/submit",
      pb.encodeSubmitRequest({
        jobId: "job-requeue",
        spec: {
          demand: { interface: "reset.v1" },
          inputs: [],
          params: new Uint8Array(0),
          metadata: {},
          policy: {
            durability: pb.Durability.DURABLE,
            priority: 0,
            queueTimeoutMs: 0n,
            executionTimeoutMs: 0n,
            totalTimeoutMs: 0n,
            spilloverAfterMs: 0n,
            maxAttempts: 3,
            fairnessKey: "",
            concurrencyKey: "",
          },
        },
        conflictPolicy: pb.ConflictPolicy.FAIL,
      }),
    );

    const activeAssigns = extractAssigns(sessionWs);
    expect(activeAssigns).toHaveLength(3);
    const invokeAssign = activeAssigns.find(
      (a) => a.jobId !== "job-exhausted" && a.jobId !== "job-requeue",
    )!;

    // Emit one non-progress Frame on the Invoke job so `exposed = 1`!
    const attExp = (await platform.openWs("/ws/v2/worker/attempt")).client!;
    await attExp.sendBinary(
      pb.encodeAttemptUp({
        msg: {
          case: "open",
          value: {
            leaseId: invokeAssign.leaseId,
            generation: invokeAssign.generation,
          },
        },
      }),
    );
    decodeAttemptDownMessages(attExp);
    await attExp.sendBinary(
      pb.encodeAttemptUp({
        msg: {
          case: "frame",
          value: {
            sequence: 1n,
            data: utf8Encoder.encode("partial-live-token"),
            metadata: {},
            progress: false,
          },
        },
      }),
    );
    exposedInv.popDecodedFrames();

    // 3. Call POST /api/admin/reset (the ONLY place generation increments: 1 -> 2!)
    const resetRes = await platform.engine.fetch(
      new Request("https://anyserve.example.com/api/admin/reset", {
        method: "POST",
      }),
    );
    expect(resetRes.status).toBe(200);
    const resetJson = (await resetRes.json()) as Record<string, number>;
    expect(resetJson.generation).toBe(2);
    expect(resetJson.fenced_leases).toBe(3);
    expect(resetJson.requeued_jobs).toBe(1);
    expect(resetJson.failed_jobs).toBe(2);

    // Verify exposed Invoke received `output_interrupted`!
    const expTerm = decodeExecutionEvents(exposedInv).find(
      (e) => e.event?.case === "finished",
    )!;
    expect(
      (expTerm.event as { case: "finished"; value: pb.ExecutionFinished }).value
        .error?.code,
    ).toBe(RUNTIME_CODES.OUTPUT_INTERRUPTED);

    // Verify exhausted job failed with `attempts_exhausted` while unexposed durable job re-queued!
    const exhJob = pb.decodeJob(
      (
        await platform.unaryPost(
          "/rpc/v2/client/get_job",
          pb.encodeGetJobRequest({ jobId: "job-exhausted", includeOutputs: false }),
        )
      ).bytes,
    );
    expect(exhJob.state).toBe(pb.JobState.FAILED);
    expect(exhJob.error?.code).toBe(RUNTIME_CODES.ATTEMPTS_EXHAUSTED);

    const reqJob = pb.decodeJob(
      (
        await platform.unaryPost(
          "/rpc/v2/client/get_job",
          pb.encodeGetJobRequest({ jobId: "job-requeue", includeOutputs: false }),
        )
      ).bytes,
    );
    expect([pb.JobState.QUEUED, pb.JobState.RUNNING]).toContain(reqJob.state);

    // 4. Replay the durable winner from generation 1 AFTER 10-min receipt expiry AND AFTER admin reset (generation 2)!
    // 4a. Matching digest -> AttemptOpened{terminalReplay: true} -> AttemptFinished::COMMITTED!
    const replayMatch = (await platform.openWs("/ws/v2/worker/attempt")).client!;
    await replayMatch.sendBinary(
      pb.encodeAttemptUp({
        msg: {
          case: "open",
          value: { leaseId: winAssign.leaseId, generation: 1n },
        },
      }),
    );
    const repOpen1 = decodeAttemptDownMessages(replayMatch)[0]!;
    expect(repOpen1.msg).toEqual({
      case: "opened",
      value: { resumeFromSequence: 0n, terminalReplay: true },
    });
    await replayMatch.sendBinary(
      pb.encodeAttemptUp({
        msg: {
          case: "result",
          value: {
            index: 0,
            data: winBytes,
            last: true,
            metadata: { v: "1" },
            uri: "",
          },
        },
      }),
    );
    decodeAttemptDownMessages(replayMatch);
    await replayMatch.sendBinary(
      pb.encodeAttemptUp({
        msg: {
          case: "finish",
          value: { outcome: { case: "succeeded", value: {} } },
        },
      }),
    );
    expect(decodeAttemptDownMessages(replayMatch)[0]!.msg).toEqual({
      case: "finished",
      value: { status: pb.AttemptFinishedStatus.COMMITTED, reason: "" },
    });

    // 4b. Conflicting digest -> AttemptOpened{terminalReplay: true} -> AttemptFinished::REJECTED!
    const replayConflict = (await platform.openWs("/ws/v2/worker/attempt")).client!;
    await replayConflict.sendBinary(
      pb.encodeAttemptUp({
        msg: {
          case: "open",
          value: { leaseId: winAssign.leaseId, generation: 1n },
        },
      }),
    );
    decodeAttemptDownMessages(replayConflict);
    await replayConflict.sendBinary(
      pb.encodeAttemptUp({
        msg: {
          case: "result",
          value: {
            index: 0,
            data: utf8Encoder.encode("tampered-different-bytes"),
            last: true,
            metadata: { v: "1" },
            uri: "",
          },
        },
      }),
    );
    decodeAttemptDownMessages(replayConflict);
    await replayConflict.sendBinary(
      pb.encodeAttemptUp({
        msg: {
          case: "finish",
          value: { outcome: { case: "succeeded", value: {} } },
        },
      }),
    );
    const rejMsg = decodeAttemptDownMessages(replayConflict)[0]!;
    expect(
      (rejMsg.msg as { case: "finished"; value: pb.AttemptFinished }).value.status,
    ).toBe(pb.AttemptFinishedStatus.REJECTED);
  });
});

describe("R2 staging vs GC mutual exclusion & prefix reconciliation isolation (4.5, 10)", () => {
  it("prevents publishing an R2 key when staging expires & GC claims it mid-PUT, and reconciles only this DO's prefix", async () => {
    const platform = new SimulatedNativePlatform({
      doId: "do-alpha",
      overrides: {
        r2StagingTtlMs: 5_000,
        r2ReconcileSafetyAgeMs: 10_000,
      },
    });

    // 1. Race a late R2 PUT against staging expiry + GC sweeper!
    const bigInput = makeDeterministicBytes(600 * 1024, 88); // > 512 KiB -> spills to R2
    platform.r2!.beforePutHook = async () => {
      // Simulate slow PUT: staging TTL expires and GC sweeper claims `state='staging' -> 'deleting'`!
      platform.advanceMs(6_000);
      await platform.engine.store.sweepR2Gc(platform.nowMs());
    };

    const raceRes = await platform.unaryPost(
      "/rpc/v2/client/submit",
      pb.encodeSubmitRequest({
        jobId: "job-lost-staging-race",
        spec: {
          demand: { interface: "r2.v1" },
          inputs: [{ body: { case: "inline", value: bigInput }, metadata: {} }],
          params: new Uint8Array(0),
          metadata: {},
        },
        conflictPolicy: pb.ConflictPolicy.FAIL,
      }),
    );
    platform.r2!.beforePutHook = null;

    // Writer's `DELETE ... WHERE state='staging' RETURNING` returned empty -> refused with Unavailable(14)!
    expect(raceRes.status).toBe(503);
    expect(raceRes.grpcCode).toBe(GrpcCode.Unavailable);
    // Job MUST NOT exist and compensating delete MUST have removed the late-arriving R2 object!
    expect(
      platform.storage.sql
        .exec<{ c: number }>(`SELECT COUNT(*) AS c FROM native_jobs`)
        .toArray()[0]!.c,
    ).toBe(0);
    expect(platform.r2!.objects.size).toBe(0);

    // 2. Prefix reconciliation: seed reachable key, old orphan under our prefix, young staging key, and another DO's key!
    const okSub = await platform.unaryPost(
      "/rpc/v2/client/submit",
      pb.encodeSubmitRequest({
        jobId: "job-live-r2",
        spec: {
          demand: { interface: "r2.v1" },
          inputs: [{ body: { case: "inline", value: bigInput }, metadata: {} }],
          params: new Uint8Array(0),
          metadata: {},
        },
        conflictPolicy: pb.ConflictPolicy.FAIL,
      }),
    );
    expect(okSub.status).toBe(200);
    const liveKey = [...platform.r2!.objects.keys()][0]!;
    expect(liveKey.startsWith("native/v1/do-alpha/blobs/")).toBe(true);

    const oldAt = platform.nowMs() - 60_000;
    const orphan1 = "native/v1/do-alpha/blobs/orphan-1";
    const orphan2 = "native/v1/do-alpha/blobs/orphan-2";
    const youngUnref = "native/v1/do-alpha/blobs/young-keep";
    const otherDeploymentKey = "native/v1/do-beta-other/blobs/foreign-keep";

    platform.r2!.seedObject(orphan1, new Uint8Array([1, 2]), oldAt);
    platform.r2!.seedObject(orphan2, new Uint8Array([3, 4]), oldAt);
    platform.r2!.seedObject(youngUnref, new Uint8Array([5]), platform.nowMs() - 1_000);
    platform.r2!.seedObject(otherDeploymentKey, new Uint8Array([9, 9]), oldAt);

    const rec = await platform.engine.store.reconcileR2Prefix(platform.nowMs(), {
      pageLimit: 2,
      maxPages: 5,
    });
    expect(rec.deletedKeys.sort()).toEqual([orphan1, orphan2]);
    expect(platform.r2!.objects.has(liveKey)).toBe(true);
    expect(platform.r2!.objects.has(youngUnref)).toBe(true);
    expect(platform.r2!.objects.has(otherDeploymentKey)).toBe(true); // NEVER touched!
  });
});

describe("Size boundaries (2 MiB Invoke, 8 MiB object, 32 MiB WS boundary), admission shedding, scoped-id bijection, scheduling & Console API (5.1, 7, 9, 10)", () => {
  it("accepts 2 MiB single-message Invoke & 8 MiB object, and enforces 32 MiB encoded WS boundary (both app rejection & platform 1009)", async () => {
    const platform = new SimulatedNativePlatform();
    const { ws: sessionWs } = await connectWorkerSession(platform, {
      interfaces: ["limits.v1"],
    });

    // 1. 2 MiB single-message InvokeStart
    const twoMiB = makeDeterministicBytes(2 * 1024 * 1024, 31);
    const inv2m = (await platform.openWs("/ws/v2/client/invoke")).client!;
    await inv2m.sendBinary(
      pb.encodeInvokeRequest({
        msg: {
          case: "start",
          value: {
            streamingInput: false,
            spec: {
              demand: { interface: "limits.v1" },
              inputs: [{ body: { case: "inline", value: twoMiB }, metadata: {} }],
              params: new Uint8Array(0),
              metadata: {},
            },
          },
        },
      }),
    );
    expect(extractAssigns(sessionWs)).toHaveLength(1);

    // 2. Exact 8 MiB inline object accepted; 8 MiB + 1 byte rejected with InvalidArgument(3)
    const eightMiB = makeDeterministicBytes(8 * 1024 * 1024, 41);
    const ok8m = await platform.unaryPost(
      "/rpc/v2/client/submit",
      pb.encodeSubmitRequest({
        jobId: "job-exact-8m",
        spec: {
          demand: { interface: "limits.v1" },
          inputs: [{ body: { case: "inline", value: eightMiB }, metadata: {} }],
          params: new Uint8Array(0),
          metadata: {},
        },
        conflictPolicy: pb.ConflictPolicy.FAIL,
      }),
    );
    expect(ok8m.status).toBe(200);

    const over8m = await platform.unaryPost(
      "/rpc/v2/client/submit",
      pb.encodeSubmitRequest({
        jobId: "job-over-8m",
        spec: {
          demand: { interface: "limits.v1" },
          inputs: [
            {
              body: { case: "inline", value: new Uint8Array(8 * 1024 * 1024 + 1) },
              metadata: {},
            },
          ],
          params: new Uint8Array(0),
          metadata: {},
        },
        conflictPolicy: pb.ConflictPolicy.FAIL,
      }),
    );
    expect(over8m.grpcCode).toBe(GrpcCode.InvalidArgument);

    // 3. 32 MiB (33,554,432 bytes) encoded WS boundary:
    //    3a. Exact 33,554,432 B encoded frame passes encoded-length check (rejected by 24 MiB logical check -> InvalidArgument, NOT 1009 / ResourceExhausted)
    const wsExact32 = (await platform.openWs("/ws/v2/client/invoke")).client!;
    // Pad unknown skipped bytes (field 99 wire type 2) to produce an EXACT 33,554,432 B valid InvokeStart with small logical payload!
    const baseStart = pb.encodeInvokeRequest({
      msg: {
        case: "start",
        value: {
          streamingInput: false,
          spec: {
            demand: { interface: "limits.v1" },
            inputs: [],
            params: new Uint8Array(16),
            metadata: {},
          },
        },
      },
    });
    const exact32Frame = new Uint8Array(WS_MAX_ENCODED_BYTES);
    exact32Frame.set(baseStart, 0);
    // Append unknown field tag `(99 << 3) | 2 = 794` (2-byte varint: 0x9a, 0x06) + 4-byte varint length for remaining bytes
    const remAfterTag = WS_MAX_ENCODED_BYTES - baseStart.byteLength - 2 - 4;
    let p = baseStart.byteLength;
    exact32Frame[p++] = 0x9a;
    exact32Frame[p++] = 0x06;
    // 4-byte varint for remAfterTag (~33.55M -> 4 bytes since < 2^28 = 268,435,456)
    let v = remAfterTag >>> 0;
    while (v >= 0x80) {
      exact32Frame[p++] = (v & 0x7f) | 0x80;
      v >>>= 7;
    }
    exact32Frame[p++] = v;
    expect(p + remAfterTag).toBe(WS_MAX_ENCODED_BYTES);

    await wsExact32.sendBinary(exact32Frame);
    expect(wsExact32.platformClose).toBeNull();
    const exact32Evs = decodeExecutionEvents(wsExact32);
    expect(exact32Evs[0]!.event?.case).toBe("accepted");

    //    3b. 33,554,433 B (32 MiB + 1 B): application check rejects with 0x01 || ResourceExhausted(8)!
    const wsApp32Plus1 = (await platform.openWs("/ws/v2/client/invoke")).client!;
    const over32Bytes = new Uint8Array(WS_MAX_ENCODED_BYTES + 1);
    await wsApp32Plus1.sendDirectToEngine(over32Bytes);
    const appRej = wsApp32Plus1.popDecodedFrames()[0]!;
    expect(appRej).toEqual({
      kind: "error",
      code: GrpcCode.ResourceExhausted,
      message: expect.stringContaining(String(WS_MAX_ENCODED_BYTES)),
    });

    //    3c. 33,554,433 B via platform edge closes with 1009 Message Too Big!
    const wsPlat1009 = (await platform.openWs("/ws/v2/client/invoke")).client!;
    await wsPlat1009.sendBinary(over32Bytes);
    expect(wsPlat1009.platformClose?.code).toBe(1009);
    expect(wsPlat1009.readyState).toBe(WS_READY_STATE_CLOSED);
  });

  it("sheds new load at quotaSoftRowWritesPerDay (and 850 MiB storage cap) while already-accepted jobs still complete end-to-end", async () => {
    const platform = new SimulatedNativePlatform({
      overrides: {
        quotaSoftRowWritesPerDay: 40,
        quotaHardRowWritesPerDay: 200,
      },
    });
    const { ws: sessionWs } = await connectWorkerSession(platform, {
      interfaces: ["shed.v1"],
    });

    // Accept Job A before crossing soft row-write admission limit
    const accRes = await platform.unaryPost(
      "/rpc/v2/client/submit",
      pb.encodeSubmitRequest({
        jobId: "accepted-before-shed",
        spec: {
          demand: { interface: "shed.v1" },
          inputs: [],
          params: new Uint8Array(0),
          metadata: {},
        },
        conflictPolicy: pb.ConflictPolicy.FAIL,
      }),
    );
    expect(accRes.status).toBe(200);
    const assignA = extractAssigns(sessionWs)[0]!;

    // Submit until we cross the soft row-write limit (40 writes) and record the FIRST rejected job!
    let firstRejectedStatus: { status: number; grpcCode: number | null; text: string } | null =
      null;
    for (let i = 0; i < 10; i++) {
      const r = await platform.unaryPost(
        "/rpc/v2/client/submit",
        pb.encodeSubmitRequest({
          jobId: `candidate-${i}`,
          spec: {
            demand: { interface: "shed.v1" },
            inputs: [],
            params: new Uint8Array(0),
            metadata: {},
          },
          conflictPolicy: pb.ConflictPolicy.FAIL,
        }),
      );
      if (r.status !== 200) {
        firstRejectedStatus = r;
        break;
      }
    }
    expect(firstRejectedStatus).not.toBeNull();
    expect(firstRejectedStatus!.status).toBe(429);
    expect(firstRejectedStatus!.grpcCode).toBe(GrpcCode.ResourceExhausted);
    expect(firstRejectedStatus!.text).toContain("row-write admission cap");

    // Already-accepted Job A MUST still open its Attempt stream, upload output, and finish using headroom!
    const attA = (await platform.openWs("/ws/v2/worker/attempt")).client!;
    await attA.sendBinary(
      pb.encodeAttemptUp({
        msg: {
          case: "open",
          value: { leaseId: assignA.leaseId, generation: assignA.generation },
        },
      }),
    );
    decodeAttemptDownMessages(attA);
    await attA.sendBinary(
      pb.encodeAttemptUp({
        msg: {
          case: "result",
          value: {
            index: 0,
            data: utf8Encoder.encode("completed-under-headroom"),
            last: true,
            metadata: {},
            uri: "",
          },
        },
      }),
    );
    await attA.sendBinary(
      pb.encodeAttemptUp({
        msg: {
          case: "finish",
          value: { outcome: { case: "succeeded", value: {} } },
        },
      }),
    );
    expect(
      decodeAttemptDownMessages(attA).some(
        (m) =>
          m.msg?.case === "finished" &&
          m.msg.value.status === pb.AttemptFinishedStatus.COMMITTED,
      ),
    ).toBe(true);

    // Also verify 850 MiB `databaseSize + reservedRetainedBytes` cap sheds new work!
    platform.advanceMs(86_400_000 + 1_000); // next UTC day resets daily row counter
    platform.storage.simulatedBaseDbBytes = 850 * 1024 * 1024;
    const storageShed = await platform.unaryPost(
      "/rpc/v2/client/submit",
      pb.encodeSubmitRequest({
        jobId: "storage-shed-job",
        spec: {
          demand: { interface: "shed.v1" },
          inputs: [],
          params: new Uint8Array(1024),
          metadata: {},
        },
        conflictPolicy: pb.ConflictPolicy.FAIL,
      }),
    );
    expect(storageShed.status).toBe(429);
    expect(storageShed.grpcCode).toBe(GrpcCode.ResourceExhausted);
    expect(storageShed.text).toContain("storage + reserved retained budget");
  });

  it("isolates ('default', '@ns:team:j') vs ('team', 'j') end-to-end across submit, dedupe, conflict, watch, results, detail, and analytics paging", async () => {
    expect(encodeScopedId(DEFAULT_NAMESPACE, "@ns:team:j")).toBe(
      "@ns:default:@ns:team:j",
    );
    expect(encodeScopedId("team", "j")).toBe("@ns:team:j");

    const platform = new SimulatedNativePlatform();
    const { ws: wDefault } = await connectWorkerSession(platform, {
      workerId: "w-def",
      interfaces: ["ns.v1"],
    });
    const { ws: wTeam } = await connectWorkerSession(platform, {
      workerId: "w-team",
      interfaces: ["ns.v1"],
      headers: { "x-anyserve-namespace": "team" },
    });

    // Submit ("default", "@ns:team:j") with params "from-default"
    const sDef1 = await platform.unaryPost(
      "/rpc/v2/client/submit",
      pb.encodeSubmitRequest({
        jobId: "@ns:team:j",
        spec: {
          demand: { interface: "ns.v1" },
          inputs: [],
          params: utf8Encoder.encode("from-default"),
          metadata: { owner: "default-ns" },
        },
        conflictPolicy: pb.ConflictPolicy.USE_EXISTING,
      }),
    );
    expect(sDef1.status).toBe(200);
    expect(pb.decodeSubmitResponse(sDef1.bytes).created).toBe(true);

    // Submit ("team", "j") with DIFFERENT params "from-team" -> MUST NOT collide!
    const sTeam1 = await platform.unaryPost(
      "/rpc/v2/client/submit",
      pb.encodeSubmitRequest({
        jobId: "j",
        spec: {
          demand: { interface: "ns.v1" },
          inputs: [],
          params: utf8Encoder.encode("from-team"),
          metadata: { owner: "team-ns" },
        },
        conflictPolicy: pb.ConflictPolicy.USE_EXISTING,
      }),
      { "x-anyserve-namespace": "team" },
    );
    expect(sTeam1.status).toBe(200);
    expect(pb.decodeSubmitResponse(sTeam1.bytes).created).toBe(true);

    // Idempotent repeat of ("default", "@ns:team:j") returns created=false; different spec returns AlreadyExists(6)!
    const sDefDedupe = await platform.unaryPost(
      "/rpc/v2/client/submit",
      pb.encodeSubmitRequest({
        jobId: "@ns:team:j",
        spec: {
          demand: { interface: "ns.v1" },
          inputs: [],
          params: utf8Encoder.encode("from-default"),
          metadata: { owner: "default-ns" },
        },
        conflictPolicy: pb.ConflictPolicy.USE_EXISTING,
      }),
    );
    expect(pb.decodeSubmitResponse(sDefDedupe.bytes).created).toBe(false);

    const sDefConflict = await platform.unaryPost(
      "/rpc/v2/client/submit",
      pb.encodeSubmitRequest({
        jobId: "@ns:team:j",
        spec: {
          demand: { interface: "ns.v1" },
          inputs: [],
          params: utf8Encoder.encode("mutated"),
          metadata: {},
        },
        conflictPolicy: pb.ConflictPolicy.USE_EXISTING,
      }),
    );
    expect(sDefConflict.grpcCode).toBe(GrpcCode.AlreadyExists);

    // Complete both jobs on their respective namespace workers with distinct outputs!
    for (const [sessWs, nsHdr, outStr] of [
      [wDefault, undefined, "out-default"],
      [wTeam, { "x-anyserve-namespace": "team" }, "out-team"],
    ] as const) {
      const a = extractAssigns(sessWs)[0]!;
      const att = (await platform.openWs("/ws/v2/worker/attempt", nsHdr)).client!;
      await att.sendBinary(
        pb.encodeAttemptUp({
          msg: {
            case: "open",
            value: { leaseId: a.leaseId, generation: a.generation },
          },
        }),
      );
      decodeAttemptDownMessages(att);
      await att.sendBinary(
        pb.encodeAttemptUp({
          msg: {
            case: "result",
            value: {
              index: 0,
              data: utf8Encoder.encode(outStr),
              last: true,
              metadata: {},
              uri: "",
            },
          },
        }),
      );
      await att.sendBinary(
        pb.encodeAttemptUp({
          msg: {
            case: "finish",
            value: { outcome: { case: "succeeded", value: {} } },
          },
        }),
      );
    }

    // Verify GetJob(includeOutputs=true) and Watch isolate each namespace cleanly
    const gDef = pb.decodeJob(
      (
        await platform.unaryPost(
          "/rpc/v2/client/get_job",
          pb.encodeGetJobRequest({ jobId: "@ns:team:j", includeOutputs: true }),
        )
      ).bytes,
    );
    const gTeam = pb.decodeJob(
      (
        await platform.unaryPost(
          "/rpc/v2/client/get_job",
          pb.encodeGetJobRequest({ jobId: "j", includeOutputs: true }),
          { "x-anyserve-namespace": "team" },
        )
      ).bytes,
    );
    expect(
      utf8Decoder.decode(
        (gDef.outputs[0]!.body as { case: "inline"; value: Uint8Array }).value,
      ),
    ).toBe("out-default");
    expect(
      utf8Decoder.decode(
        (gTeam.outputs[0]!.body as { case: "inline"; value: Uint8Array }).value,
      ),
    ).toBe("out-team");

    // Verify `/api/analytics/jobs` archive cursor pagination across both jobs without collision!
    const fromMs = platform.nowMs() - 10_000;
    const toMs = platform.nowMs() + 10_000;
    const page1 = (await (
      await platform.engine.fetch(
        new Request(
          `https://anyserve.example.com/api/analytics/jobs?source=archive&from_ms=${fromMs}&to_ms=${toMs}&limit=1`,
        ),
      )
    ).json()) as { jobs: Array<{ namespace: string; job_id: string }>; next_cursor: string | null };
    expect(page1.jobs).toHaveLength(1);
    expect(page1.next_cursor).not.toBeNull();

    const page2 = (await (
      await platform.engine.fetch(
        new Request(
          `https://anyserve.example.com/api/analytics/jobs?source=archive&from_ms=${fromMs}&to_ms=${toMs}&limit=1&cursor=${encodeURIComponent(page1.next_cursor!)}`,
        ),
      )
    ).json()) as { jobs: Array<{ namespace: string; job_id: string }>; next_cursor: string | null };
    expect(page2.jobs).toHaveLength(1);
    expect(page2.next_cursor).toBeNull();
    const seenPairs = [...page1.jobs, ...page2.jobs]
      .map((j) => `${j.namespace}:${j.job_id}`)
      .sort();
    expect(seenPairs).toEqual(["default:@ns:team:j", "team:j"]);
  });

  it("mirrors Rust scheduling (priority, fairness round-robin, preferred/pool spillover, concurrency keys, affinity) & Console API routes", async () => {
    const platform = new SimulatedNativePlatform({
      env: {
        ANYSERVE_CLIENT_TOKENS: "op-secret",
        ANYSERVE_WORKER_TOKENS: "worker-secret",
        ANYSERVE_CONSOLE_POLL_MS: "0",
        ANYSERVE_ALLOWED_ORIGINS: "https://console.example.com",
      },
      overrides: {
        interfaces: {
          "sched.v1": {
            maxQueue: 100,
            queueTimeoutMs: null,
            maxInFlightPerKey: 1,
            poolTiers: [
              { pool: "hot", spilloverAfterMs: 0, reconnectGraceMs: 10_000 },
              { pool: "warm", spilloverAfterMs: 1_000, reconnectGraceMs: 10_000 },
            ],
          },
        },
      },
    });
    const authHdr = { Authorization: "Bearer op-secret" };
    const workerAuthHdr = { Authorization: "Bearer worker-secret" };

    // Queue 3 jobs while no worker is connected:
    // - tenantA-1 (prio 1, fairness A, concurrency_key "ck1")
    // - tenantA-2 (prio 1, fairness A, concurrency_key "ck1") -> blocked by ck1 while tenantA-1 runs!
    // - tenantB-1 (prio 1, fairness B) -> round-robins ahead of tenantA-2!
    // - highPrio  (prio 3, preferred zone=us-east, spilloverAfterMs=500)
    const submitSched = async (
      jobId: string,
      opts: {
        prio: number;
        fairness?: string;
        ckey?: string;
        pref?: Record<string, string>;
        spillMs?: bigint;
      },
    ) => {
      const r = await platform.unaryPost(
        "/rpc/v2/client/submit",
        pb.encodeSubmitRequest({
          jobId,
          spec: {
            demand: {
              interface: "sched.v1",
              requiredAttributes: {},
              preferredAttributes: opts.pref ?? {},
              requiredCapacity: {},
              affinityKey: "",
            },
            inputs: [],
            params: utf8Encoder.encode("hello-preview"),
            metadata: { tag: jobId },
            policy: {
              durability: pb.Durability.DURABLE,
              priority: opts.prio,
              queueTimeoutMs: 0n,
              executionTimeoutMs: 0n,
              totalTimeoutMs: 0n,
              spilloverAfterMs: opts.spillMs ?? 0n,
              maxAttempts: 2,
              fairnessKey: opts.fairness ?? "",
              concurrencyKey: opts.ckey ?? "",
            },
          },
          conflictPolicy: pb.ConflictPolicy.FAIL,
        }),
        authHdr,
      );
      expect(r.status).toBe(200);
    };

    await submitSched("tenantA-1", { prio: 1, fairness: "A", ckey: "ck1" });
    await submitSched("tenantA-2", { prio: 1, fairness: "A", ckey: "ck1" });
    await submitSched("tenantB-1", { prio: 1, fairness: "B" });
    await submitSched("high-spill", {
      prio: 3,
      pref: { zone: "us-east" },
      spillMs: 500n,
    });

    // Connect a "warm" pool worker in "us-west" -> at t=0 neither tier ("warm" unlocks at 1000ms) nor preferred ("us-east" spills at 500ms) is open yet!
    const { ws: warmWs } = await connectWorkerSession(platform, {
      workerId: "w-warm",
      interfaces: ["sched.v1"],
      pool: "warm",
      attributes: { zone: "us-west" },
      maxConcurrency: 2,
      headers: workerAuthHdr,
    });
    expect(extractAssigns(warmWs)).toHaveLength(0);

    // Advance 1,000ms -> BOTH preferred spillover (500ms) AND "warm" pool tier (1,000ms) unlock!
    await platform.advanceAndFireAlarms(1_000);
    const batch1 = extractAssigns(warmWs).map((a) => a.jobId);
    // Priority 3 ("high-spill") dispatches FIRST, followed by "tenantA-1"! ("tenantA-2" is held back by maxInFlightPerKey=1 on "ck1")
    expect(batch1).toEqual(["high-spill", "tenantA-1"]);

    // Verify Console API: operator auth check, CORS headers, /api/overview (`recommended_poll_ms === 0`, default fallback wss://<host>), /api/jobs histogram + cancel!
    const unauthOv = await platform.engine.fetch(
      new Request("https://anyserve.example.com/api/overview"),
    );
    expect(unauthOv.status).toBe(401);

    const ovRes = await platform.engine.fetch(
      new Request("https://anyserve.example.com/api/overview", {
        headers: {
          ...authHdr,
          Origin: "https://console.example.com",
        },
      }),
    );
    expect(ovRes.status).toBe(200);
    expect(ovRes.headers.get("Access-Control-Allow-Origin")).toBe(
      "https://console.example.com",
    );
    const ov = (await ovRes.json()) as Record<string, any>;
    expect(ov.runtime).toEqual({
      code: "memory_plus_cloudflare",
      label: "Memory + Cloudflare DO",
      journal: "cloudflare",
    });
    expect(ov.control_plane.public_endpoint).toBe("wss://anyserve.example.com");
    expect(ov.control_plane.recommended_poll_ms).toBe(0);
    expect(ov.queue).toEqual({ queued: 2, running: 2, active: 4 });

    const jobsRes = await platform.engine.fetch(
      new Request(
        "https://anyserve.example.com/api/jobs?window_ms=60000&buckets=12&limit=2",
        { headers: authHdr },
      ),
    );
    expect(jobsRes.status).toBe(200);
    const jobsJson = (await jobsRes.json()) as Record<string, any>;
    expect(jobsJson.total).toBe(4);
    expect(jobsJson.jobs).toHaveLength(2);
    expect(jobsJson.next_page_token).toBe("2");
    expect(jobsJson.histogram.buckets).toHaveLength(12);

    // Cancel queued "tenantB-1" via Console POST /api/jobs/:id/cancel
    const cRes = await platform.engine.fetch(
      new Request("https://anyserve.example.com/api/jobs/tenantB-1/cancel", {
        method: "POST",
        headers: authHdr,
      }),
    );
    expect(cRes.status).toBe(200);
    expect(((await cRes.json()) as any).job.state).toBe("cancelled");

    // Verify `specHash` matches Rust normalization on `GetJob`
    const expectedHash = specHash(
      {
        demand: { interface: "sched.v1" },
        inputs: [],
        params: utf8Encoder.encode("hello-preview"),
        metadata: { tag: "tenantA-1" },
        policy: {
          durability: pb.Durability.DURABLE,
          priority: 1,
          queueTimeoutMs: 0n,
          executionTimeoutMs: 0n,
          totalTimeoutMs: 0n,
          spilloverAfterMs: 0n,
          maxAttempts: 2,
          fairnessKey: "A",
          concurrencyKey: "ck1",
        },
      },
      "durable",
    );
    const gA1 = pb.decodeJob(
      (
        await platform.unaryPost(
          "/rpc/v2/client/get_job",
          pb.encodeGetJobRequest({ jobId: "tenantA-1", includeOutputs: false }),
          authHdr,
        )
      ).bytes,
    );
    expect(gA1.specHash).toBe(expectedHash);
  });

  it("preserves old Rust spec_json compatibility (>2^53 capacities & Duration structs), 8 MiB params hibernation, fairness round-robin, and affinity rendezvous routing", async () => {
    // 1. Bidirectional Rust `spec_json` compatibility (including > 2^53 u64 capacity!)
    const bigCap = (1n << 55n) + 12345n;
    const rustLegacyJson = JSON.stringify({
      demand: {
        interface: "legacy.v1",
        required_attributes: { arch: "x86_64" },
        preferred_attributes: { rack: "r1" },
        required_capacity: { vram_bytes: bigCap.toString() },
        affinity_key: "model-llama-70b",
      },
      policy: {
        durability: "Durable",
        priority: 2,
        queue_timeout: { secs: 12, nanos: 500_000_000 },
        execution_timeout: { secs: 60, nanos: 0 },
        total_timeout: null,
        spillover_after: { secs: 1, nanos: 250_000_000 },
        max_attempts: 4,
        fairness_key: "org-9",
        concurrency_key: "ck-legacy",
      },
      metadata: { imported: "rust" },
      params_len: 128,
      input_count: 2,
    });
    const parsedLegacy = parseSpecJson(rustLegacyJson);
    expect(parsedLegacy.demand.required_capacity.vram_bytes).toBe(bigCap);
    expect(parsedLegacy.policy.durability).toBe("durable");
    expect(parsedLegacy.policy.queue_timeout_ms).toBe(12_500);
    expect(parsedLegacy.policy.execution_timeout_ms).toBe(60_000);
    expect(parsedLegacy.policy.spillover_after_ms).toBe(1_250);
    const reserialized = parseSpecJson(serializeSpecJson(parsedLegacy));
    expect(reserialized).toEqual(parsedLegacy);

    // 2. Fairness round-robin & bounded-load Rendezvous Hashing (`affinity_key`)
    const platform = new SimulatedNativePlatform();
    for (const [jid, fkey] of [
      ["fa-1", "tenant-A"],
      ["fa-2", "tenant-A"],
      ["fa-3", "tenant-A"],
      ["fb-1", "tenant-B"],
    ] as const) {
      await platform.unaryPost(
        "/rpc/v2/client/submit",
        pb.encodeSubmitRequest({
          jobId: jid,
          spec: {
            demand: { interface: "fair.v1" },
            inputs: [],
            params: new Uint8Array(0),
            metadata: {},
            policy: {
              durability: pb.Durability.DURABLE,
              priority: 1,
              queueTimeoutMs: 0n,
              executionTimeoutMs: 0n,
              totalTimeoutMs: 0n,
              spilloverAfterMs: 0n,
              maxAttempts: 1,
              fairnessKey: fkey,
              concurrencyKey: "",
            },
          },
          conflictPolicy: pb.ConflictPolicy.FAIL,
        }),
      );
    }

    // Worker with 2 slots: fairness ring must pick `fa-1` (from tenant-A) and `fb-1` (from tenant-B) ahead of `fa-2`!
    const { ws: fairWs } = await connectWorkerSession(platform, {
      workerId: "w-fair",
      interfaces: ["fair.v1"],
      maxConcurrency: 2,
    });
    expect(extractAssigns(fairWs).map((a) => a.jobId)).toEqual(["fa-1", "fb-1"]);

    // Affinity routing across two idle workers (< 0.85 load)
    const { ws: aff1 } = await connectWorkerSession(platform, {
      workerId: "worker-alpha",
      interfaces: ["aff.v1"],
      maxConcurrency: 4,
    });
    const { ws: aff2 } = await connectWorkerSession(platform, {
      workerId: "worker-beta",
      interfaces: ["aff.v1"],
      maxConcurrency: 4,
    });
    const affKey = "sticky-model-weights-v2";
    const s1 = rendezvousHash64(affKey, "worker-alpha");
    const s2 = rendezvousHash64(affKey, "worker-beta");
    const expectedWinnerWs = s1 > s2 ? aff1 : aff2;

    for (let k = 0; k < 2; k++) {
      await platform.unaryPost(
        "/rpc/v2/client/submit",
        pb.encodeSubmitRequest({
          jobId: `aff-job-${k}`,
          spec: {
            demand: { interface: "aff.v1", affinityKey: affKey },
            inputs: [],
            params: new Uint8Array(0),
            metadata: {},
          },
          conflictPolicy: pb.ConflictPolicy.FAIL,
        }),
      );
    }
    expect(extractAssigns(expectedWinnerWs)).toHaveLength(2);

    // 3. Exact 8 MiB `params` object paused mid-download across `simulateHibernation()` on the same Attempt socket!
    const eightMiBParams = makeDeterministicBytes(8 * 1024 * 1024, 91);
    const pSub = await platform.unaryPost(
      "/rpc/v2/client/submit",
      pb.encodeSubmitRequest({
        jobId: "job-8m-params",
        spec: {
          demand: { interface: "aff.v1", affinityKey: affKey },
          inputs: [],
          params: eightMiBParams,
          metadata: {},
        },
        conflictPolicy: pb.ConflictPolicy.FAIL,
      }),
    );
    expect(pSub.status).toBe(200);
    const pAssign = extractAssigns(expectedWinnerWs)[0]!;
    const pAtt = (await platform.openWs("/ws/v2/worker/attempt")).client!;
    pAtt.autoDrain = false;
    await pAtt.sendBinary(
      pb.encodeAttemptUp({
        msg: {
          case: "open",
          value: { leaseId: pAssign.leaseId, generation: pAssign.generation },
        },
      }),
    );

    const paramMsgs: pb.AttemptDown[] = [];
    // Pause & hibernate after first 1 MiB batch, after chunk 33, and right before last!
    pAtt.drainAll();
    paramMsgs.push(...decodeAttemptDownMessages(pAtt));
    platform.simulateHibernation();

    for (let step = 0; step < 12; step++) {
      await new Promise((r) => setTimeout(r, 3));
      pAtt.drainAll();
      paramMsgs.push(...decodeAttemptDownMessages(pAtt));
      if (step === 1 || step === 6) {
        platform.simulateHibernation();
      }
    }

    const pChunks = paramMsgs
      .slice(1)
      .map((m) => (m.msg as { case: "input"; value: pb.InputData }).value);
    expect(pChunks[pChunks.length - 1]!.last).toBe(true);
    const onlyParams = pChunks.filter((c) => c.parameterChunk);
    expect(onlyParams).toHaveLength(128);
    const rebuiltParams = new Uint8Array(8 * 1024 * 1024);
    let pOff = 0;
    for (const c of onlyParams) {
      expect(c.objectIndex).toBeUndefined();
      rebuiltParams.set(c.data, pOff);
      pOff += c.data.byteLength;
    }
    expect(buffersEqual(rebuiltParams, eightMiBParams)).toBe(true);
  });

  it("schedules connected-session liveness alarm at lastSeen + heartbeatTimeoutMs (not 5s loop) and exposes full quota & databaseSize metrics", async () => {
    const platform = new SimulatedNativePlatform();
    const { ws } = await connectWorkerSession(platform, {
      workerId: "w-hibernatable",
      interfaces: ["hib.v1"],
    });

    // With Free defaults (heartbeatTimeoutMs = 90_000, maintenanceBucketMs = 5_000),
    // the armed alarm MUST be >= nowMs + 90_000 so the DO can hibernate across 30s+ idle!
    const armed = (platform as unknown as { scheduledAlarmMs: number | null }).scheduledAlarmMs;
    expect(armed).not.toBeNull();
    expect(armed! - platform.nowMs()).toBeGreaterThanOrEqual(90_000);

    const rwBefore = platform.engine.store.quotaSnapshot(platform.nowMs()).rowsWritten;
    await ws.sendBinary(
      pb.encodeWorkerMessage({
        msg: {
          case: "heartbeat",
          value: { load: { queued: 0, running: 1, utilization: 0.5, custom: {} } },
        },
      }),
    );
    expect(platform.engine.store.quotaSnapshot(platform.nowMs()).rowsWritten).toBe(rwBefore);

    const ov = (await (
      await platform.engine.fetch(new Request("https://anyserve.example.com/api/overview"))
    ).json()) as Record<string, any>;
    expect(ov.quota).toBeDefined();
    expect(typeof ov.quota.database_size_bytes).toBe("number");
    expect(typeof ov.quota.r2_class_a).toBe("number");
    expect(typeof ov.quota.r2_class_b).toBe("number");

    const metricsTxt = await (
      await platform.engine.fetch(new Request("https://anyserve.example.com/metrics"))
    ).text();
    expect(metricsTxt).toContain("anyserve_native_database_size_bytes ");
    expect(metricsTxt).toContain("anyserve_native_quota_r2_class_a ");
    expect(metricsTxt).toContain("anyserve_native_quota_r2_class_b ");
  });
});


// ---------------------------------------------------------------------------
// Hot-path optimisations (attachment cache, conn_seq blocks, single decode, inline-assign cache,
// static_in manifest cache, zero-alloc UTF-8 length)
// ---------------------------------------------------------------------------

function sqlStatementLog(platform: SimulatedNativePlatform): Array<{ query: string; bindings: unknown[] }> {
  const log: Array<{ query: string; bindings: unknown[] }> = [];
  const sql = platform.storage.sql as unknown as {
    exec: (query: string, ...bindings: unknown[]) => unknown;
  };
  const orig = sql.exec.bind(sql);
  sql.exec = (query: string, ...bindings: unknown[]) => {
    log.push({ query, bindings });
    return orig(query, ...bindings);
  };
  return log;
}

function metaConnSeq(platform: SimulatedNativePlatform): number {
  return Number(
    platform.storage.sql
      .exec<{ value: string }>(`SELECT value FROM native_runtime_meta WHERE key = 'conn_seq'`)
      .toArray()[0]!.value,
  );
}

describe("zero-allocation utf8ByteLen", () => {
  it("matches TextEncoder byte length for ASCII, 2/3/4-byte code points and lone surrogates", () => {
    const samples = ["", "abc", "héllo", "日本語", "😀 emoji", "a\ud800b", "\udc00", "x\ud83d", "\ud83d\ude00\ud83d", "k".repeat(1000)];
    for (const str of samples) {
      expect(utf8ByteLen(str), JSON.stringify(str)).toBe(utf8Encoder.encode(str).byteLength);
    }
  });
});

describe("SocketAttachment cache (host.ts)", () => {
  function makeWrapper(shared: { json: string | null }) {
    const counters = { serialize: 0, deserialize: 0 };
    const ws = {
      serializeAttachment(a: unknown) {
        counters.serialize += 1;
        shared.json = JSON.stringify(a);
      },
      deserializeAttachment() {
        counters.deserialize += 1;
        return shared.json === null ? null : JSON.parse(shared.json);
      },
      readyState: WS_READY_STATE_OPEN,
      bufferedAmount: 0,
      send() {},
      close() {},
    };
    return { ws: ws as unknown as NativeWebSocket, counters };
  }
  const att: SocketAttachment = {
    kind: "session",
    connId: 7,
    id: "s-1",
    namespace: "default",
    forcedPool: null,
    handshakeDone: 1,
    openedAtMs: 10,
    lastSeenAtMs: 20,
    queued: 1,
    running: 2,
    utilization: 0.1234567891,
    replay: 0,
  };

  it("serves reads from the cache after a write and equals the deserialized round-trip", () => {
    const shared = { json: null as string | null };
    const { ws, counters } = makeWrapper(shared);
    writeSocketAttachment(ws, att);
    const first = readSocketAttachment(ws)!;
    for (let i = 0; i < 50; i++) {
      expect(readSocketAttachment(ws)).toBe(first);
    }
    expect(counters.deserialize).toBe(0);
    // canonical form: identical to what a cold deserialize would produce (utilization rounded to 6 dp)
    const cold = makeWrapper(shared);
    expect(readSocketAttachment(cold.ws)).toEqual(first);
    expect(first.utilization).toBe(0.123457);
    expect(Object.isFrozen(first)).toBe(true);
  });

  it("falls back to deserialize on a recreated wrapper (post-hibernation), fills once, and follows later writes", () => {
    const shared = { json: null as string | null };
    const a = makeWrapper(shared);
    writeSocketAttachment(a.ws, att);

    const b = makeWrapper(shared); // wrapper object recreated after hibernation: cache miss
    const viaB = readSocketAttachment(b.ws)!;
    expect(b.counters.deserialize).toBe(1);
    expect(viaB.connId).toBe(7);
    expect(readSocketAttachment(b.ws)).toBe(viaB);
    expect(b.counters.deserialize).toBe(1);

    writeSocketAttachment(b.ws, { ...att, lastSeenAtMs: 99, running: 0 });
    expect(readSocketAttachment(b.ws)!.lastSeenAtMs).toBe(99);
    expect(readSocketAttachment(b.ws)!.running).toBe(0);
    expect(b.counters.deserialize).toBe(1);

    const c = makeWrapper({ json: null });
    expect(readSocketAttachment(c.ws)).toBeNull();
  });

  it("still enforces the 256-byte attachment cap and never caches a rejected write", () => {
    const shared = { json: null as string | null };
    const { ws } = makeWrapper(shared);
    writeSocketAttachment(ws, att);
    expect(() => writeSocketAttachment(ws, { ...att, id: "x".repeat(300) })).toThrow(/exceeds/);
    expect(readSocketAttachment(ws)!.id).toBe("s-1");
  });

  it("Heartbeats and console reads never call deserializeAttachment once the socket attachment is cached", async () => {
    const platform = new SimulatedNativePlatform();
    const { ws: sessionWs } = await connectWorkerSession(platform, { workerId: "w-cache", interfaces: ["c.v1"] });
    const deserialize = vi.spyOn(sessionWs.server, "deserializeAttachment");
    for (let i = 0; i < 5; i++) {
      await sessionWs.sendBinary(
        pb.encodeWorkerMessage({
          msg: { case: "heartbeat", value: { load: { queued: i, running: 1, utilization: 0.5, custom: {} } } },
        }),
      );
    }
    const ov = await platform.engine.fetch(new Request("https://anyserve.example.com/api/overview"));
    expect(ov.status).toBe(200);
    await platform.engine.fetch(new Request("https://anyserve.example.com/api/workers"));
    expect(deserialize).not.toHaveBeenCalled();
    // attachment reflects the latest heartbeat
    expect(readSocketAttachment(sessionWs.server)!.queued).toBe(4);
  });
});

describe("conn_seq block allocation", () => {
  it("persists conn_seq once per CONN_SEQ_BLOCK ids and never reuses ids across hibernation", async () => {
    expect(CONN_SEQ_BLOCK).toBe(64);
    const platform = new SimulatedNativePlatform();
    expect(metaConnSeq(platform)).toBe(0);
    const writesBefore = platform.storage.totalRowsWritten;
    const ids: number[] = [];
    for (let i = 0; i < CONN_SEQ_BLOCK + 1; i++) {
      const up = await platform.openWs("/ws/v2/client/watch");
      expect(up.status).toBe(101);
      ids.push(readSocketAttachment(up.client!.server)!.connId);
      await up.client!.closeClean();
      if (i === 0) {
        expect(metaConnSeq(platform)).toBe(CONN_SEQ_BLOCK);
      }
    }
    expect(ids).toEqual(Array.from({ length: CONN_SEQ_BLOCK + 1 }, (_, i) => i + 1));
    // 65 upgrades => exactly 2 meta writes (ids 1 and 65 cross the ceiling), not 65
    expect(platform.storage.totalRowsWritten - writesBefore).toBe(2);
    expect(metaConnSeq(platform)).toBe(2 * CONN_SEQ_BLOCK);

    // Wake: ids resume strictly above the persisted ceiling (all earlier ids may have been issued).
    platform.simulateHibernation();
    const up = await platform.openWs("/ws/v2/client/watch");
    const idAfterWake = readSocketAttachment(up.client!.server)!.connId;
    expect(idAfterWake).toBeGreaterThan(2 * CONN_SEQ_BLOCK);
    expect(metaConnSeq(platform)).toBeGreaterThanOrEqual(idAfterWake);
    const up2 = await platform.openWs("/ws/v2/client/watch");
    expect(readSocketAttachment(up2.client!.server)!.connId).toBe(idAfterWake + 1);
  });

  it("resumes above the highest live attachment/session/lease connection id even if the meta ceiling is stale", async () => {
    const platform = new SimulatedNativePlatform();
    const { ws: sessionWs } = await connectWorkerSession(platform, { interfaces: ["cs.v1"] });
    const live = readSocketAttachment(sessionWs.server)!.connId;
    // Simulate a lost meta write: force the persisted ceiling below a live id.
    platform.storage.sql.exec(`UPDATE native_runtime_meta SET value = '0' WHERE key = 'conn_seq'`);
    platform.simulateHibernation();
    const up = await platform.openWs("/ws/v2/client/watch");
    expect(readSocketAttachment(up.client!.server)!.connId).toBeGreaterThan(live);
  });
});

describe("single WorkerMessage decode per inbound session frame", () => {
  it("decodes Hello, Heartbeat and non-heartbeat control messages exactly once", async () => {
    const platform = new SimulatedNativePlatform();
    const before = decodeCounters.workerMessage;
    const { ws } = await connectWorkerSession(platform, { workerId: "w-once", interfaces: ["once.v1"] });
    expect(decodeCounters.workerMessage - before).toBe(1); // Hello

    const hb = decodeCounters.workerMessage;
    await ws.sendBinary(
      pb.encodeWorkerMessage({ msg: { case: "heartbeat", value: { load: { queued: 0, running: 0, utilization: 0, custom: {} } } } }),
    );
    expect(decodeCounters.workerMessage - hb).toBe(1);

    const credit = decodeCounters.workerMessage;
    await ws.sendBinary(pb.encodeWorkerMessage({ msg: { case: "credit", value: { freeSlots: 3 } } }));
    expect(decodeCounters.workerMessage - credit).toBe(1);
    const sessionRow = platform.storage.sql
      .exec<{ reported_free: number }>(`SELECT reported_free FROM native_sessions`)
      .toArray()[0]!;
    expect(sessionRow.reported_free).toBe(3); // the single decode was still delivered to the handler
  });
});

describe("protoOkResponse body handling", () => {
  it("returns exact bytes for buffer-spanning arrays and for subarray views", async () => {
    const platform = new SimulatedNativePlatform();
    const respond = (bytes: Uint8Array) =>
      (platform.engine as unknown as { protoOkResponse(b: Uint8Array): Response }).protoOkResponse(bytes);
    const spanning = Uint8Array.from([1, 2, 3, 4, 5]);
    const r1 = respond(spanning);
    expect(r1.status).toBe(200);
    expect(r1.headers.get("content-type")).toBe("application/x-protobuf");
    expect(Array.from(new Uint8Array(await r1.arrayBuffer()))).toEqual([1, 2, 3, 4, 5]);

    const backing = Uint8Array.from([9, 9, 1, 2, 3, 9, 9]);
    const view = backing.subarray(2, 5);
    const r2 = respond(view);
    expect(Array.from(new Uint8Array(await r2.arrayBuffer()))).toEqual([1, 2, 3]);

    const big = makeDeterministicBytes(3 * 1024 * 1024, 5);
    const r3 = respond(big);
    expect(buffersEqual(new Uint8Array(await r3.arrayBuffer()), big)).toBe(true);
  });
});

describe("transient small-inline-input cache on queued jobs", () => {
  const inputs24: pb.ProtoObjectInput[] = Array.from({ length: 24 }, (_, i) => ({
    body: { case: "inline" as const, value: Uint8Array.of(65 + (i % 26)) },
    metadata: { i: String(i) },
  }));
  const submit24 = (platform: SimulatedNativePlatform, jobId: string) =>
    platform.unaryPost(
      "/rpc/v2/client/submit",
      pb.encodeSubmitRequest({
        jobId,
        spec: {
          demand: { interface: "inl.v1" },
          inputs: inputs24,
          params: utf8Encoder.encode("{\"p\":1}"),
          metadata: {},
        },
        conflictPolicy: pb.ConflictPolicy.FAIL,
      }),
    );
  const expectAssignInputs = (assign: pb.Assign) => {
    expect(assign.inputsViaAttempt).toBe(false);
    expect(assign.inlineInputs).toHaveLength(24);
    assign.inlineInputs.forEach((inp, i) => {
      expect(inp.body.case).toBe("inline");
      expect(Array.from((inp.body as { value: Uint8Array }).value)).toEqual([65 + (i % 26)]);
      expect(inp.metadata).toEqual({ i: String(i) });
    });
    expect(utf8Decoder.decode(assign.params)).toBe("{\"p\":1}");
  };
  const bundleSelects = (log: Array<{ query: string }>) =>
    log.filter((e) => /native_object_bundles/.test(e.query) && /^\s*SELECT/i.test(e.query));

  it("dispatches a freshly accepted small job without re-reading its bundle, then drops the cache", async () => {
    const platform = new SimulatedNativePlatform();
    const { ws: sessionWs } = await connectWorkerSession(platform, { interfaces: ["inl.v1"] });
    const log = sqlStatementLog(platform);
    const res = await submit24(platform, "inl-hot");
    expect(res.status).toBe(200);
    expect(bundleSelects(log)).toHaveLength(0);
    expectAssignInputs(extractAssigns(sessionWs)[0]!);
    expect(platform.engine.inlineAssignCacheBytes).toBe(0);
  });

  it("holds the payload only while queued, loses it on hibernation, and falls back to SQLite identically", async () => {
    const platform = new SimulatedNativePlatform();
    expect((await submit24(platform, "inl-queued")).status).toBe(200); // no worker => stays queued
    expect(platform.engine.inlineAssignCacheBytes).toBeGreaterThan(0);

    platform.simulateHibernation(); // cache is memory-only
    expect(platform.engine.inlineAssignCacheBytes).toBe(0);
    const log = sqlStatementLog(platform);
    const { ws: sessionWs } = await connectWorkerSession(platform, { interfaces: ["inl.v1"] });
    expect(bundleSelects(log).length).toBeGreaterThan(0); // SQLite fallback read
    expectAssignInputs(extractAssigns(sessionWs)[0]!);
  });

  it("releases the cached bytes when a queued job is cancelled and bounds total cached bytes", async () => {
    const platform = new SimulatedNativePlatform();
    expect((await submit24(platform, "inl-cancel")).status).toBe(200);
    const held = platform.engine.inlineAssignCacheBytes;
    expect(held).toBeGreaterThan(0);
    const cancel = await platform.unaryPost(
      "/rpc/v2/client/cancel",
      pb.encodeCancelRequest({ jobId: "inl-cancel", reason: "test" }),
    );
    expect(pb.decodeJob(cancel.bytes).state).toBe(pb.JobState.CANCELLED);
    expect(platform.engine.inlineAssignCacheBytes).toBe(0);

    // many queued 60 KiB jobs cannot grow the transient cache beyond INLINE_ASSIGN_CACHE_MAX_BYTES
    const blob = makeDeterministicBytes(60 * 1024, 9);
    for (let i = 0; i < 100; i++) {
      const r = await platform.unaryPost(
        "/rpc/v2/client/submit",
        pb.encodeSubmitRequest({
          jobId: `inl-big-${i}`,
          spec: {
            demand: { interface: "inl.v1" },
            inputs: [{ body: { case: "inline" as const, value: blob }, metadata: {} }],
            params: new Uint8Array(0),
            metadata: {},
          },
          conflictPolicy: pb.ConflictPolicy.FAIL,
        }),
      );
      expect(r.status).toBe(200);
    }
    expect(platform.engine.inlineAssignCacheBytes).toBeLessThanOrEqual(4 * 1024 * 1024);
    expect(platform.engine.inlineAssignCacheBytes).toBeGreaterThan(0);
  });
});

describe("static_in manifests are read once per input pump run", () => {
  it("reads static_in manifests a single time across multiple <=1 MiB InputData batches", async () => {
    const platform = new SimulatedNativePlatform();
    const { ws: sessionWs } = await connectWorkerSession(platform, { interfaces: ["pump.v1"] });
    const CHUNK = WELCOME_MAX_CHUNK_BYTES;
    const objs = [0, 1, 2].map((i) => makeDeterministicBytes(8 * CHUNK, 70 + i)); // 3 x 512 KiB = 24 chunks
    const sub = await platform.unaryPost(
      "/rpc/v2/client/submit",
      pb.encodeSubmitRequest({
        jobId: "pump-once",
        spec: {
          demand: { interface: "pump.v1" },
          inputs: objs.map((o) => ({ body: { case: "inline" as const, value: o }, metadata: {} })),
          params: new Uint8Array(0),
          metadata: {},
        },
        conflictPolicy: pb.ConflictPolicy.FAIL,
      }),
    );
    expect(sub.status).toBe(200);
    const assign = extractAssigns(sessionWs)[0]!;
    expect(assign.inputsViaAttempt).toBe(true);

    const attWs = (await platform.openWs("/ws/v2/worker/attempt")).client!;
    const log = sqlStatementLog(platform);
    await attWs.sendBinary(
      pb.encodeAttemptUp({
        msg: { case: "open", value: { leaseId: assign.leaseId, generation: assign.generation } },
      }),
    );
    for (let i = 0; i < 50 && platform.engine.leases.get(assign.leaseId)?.inputStage !== 4; i++) {
      await new Promise((r) => setTimeout(r, 1));
    }
    expect(decodeAttemptDownMessages(attWs).length).toBeGreaterThanOrEqual(25);
    const lease = platform.engine.leases.get(assign.leaseId)!;
    expect(lease.inputStage).toBe(4);
    expect(lease.inputSeq).toBe(25n); // 24 data chunks + trailer => 2 batches (16 + 9 frames)
    const staticReads = log.filter(
      (e) =>
        /FROM native_object_bundles/.test(e.query) && e.bindings.includes("static_in"),
    );
    expect(staticReads).toHaveLength(1);
  });
});

describe("interface name 1..=64 byte bound on Hello, Submit, Invoke, and Console query", () => {
  it("accepts a 64-byte interface and rejects 65-byte, empty, whitespace, and control-char interfaces with GrpcCode.InvalidArgument (3)", async () => {
    const platform = new SimulatedNativePlatform();
    const exact64 = `domain.action.${"a".repeat(47)}.v1`;
    expect(utf8ByteLen(exact64)).toBe(64);
    const over65 = `domain.action.${"a".repeat(48)}.v1`;
    expect(utf8ByteLen(over65)).toBe(65);

    // 1. 64-char interface succeeds on Hello, Submit, and Invoke
    const { ws: sessionWs } = await connectWorkerSession(platform, {
      workerId: "w-iface-64",
      interfaces: [exact64],
    });

    const okSub = await platform.unaryPost(
      "/rpc/v2/client/submit",
      pb.encodeSubmitRequest({
        jobId: `j-${"x".repeat(254)}`,
        spec: {
          demand: { interface: exact64 },
          inputs: [{ body: { case: "inline", value: utf8Encoder.encode("ok") }, metadata: {} }],
          params: new Uint8Array(0),
          metadata: {},
        },
        conflictPolicy: pb.ConflictPolicy.FAIL,
      }),
    );
    expect(okSub.status).toBe(200);
    expect(pb.decodeSubmitResponse(okSub.bytes).job?.interface).toBe(exact64);

    const okInv = (await platform.openWs("/ws/v2/client/invoke")).client!;
    await okInv.sendBinary(
      pb.encodeInvokeRequest({
        msg: {
          case: "start",
          value: {
            streamingInput: false,
            spec: {
              demand: { interface: exact64 },
              inputs: [],
              params: new Uint8Array(0),
              metadata: {},
            },
          },
        },
      }),
    );
    expect(okInv.serverClosed).toBeNull();
    expect(decodeExecutionEvents(okInv)[0]!.event?.case).toBe("accepted");
    expect(extractAssigns(sessionWs).length).toBeGreaterThanOrEqual(1);

    const okConsoleRes = await platform.engine.fetch(
      new Request(`https://anyserve.example.com/api/jobs?interface=${encodeURIComponent(exact64)}`),
    );
    expect(okConsoleRes.status).toBe(200);

    // 2. 65-char, empty, whitespace, and ASCII control interfaces fail with InvalidArgument (3)
    for (const bad of [
      over65,
      "",
      "   ",
      "domain action.v1",
      "domain\taction.v1",
      "domain\naction.v1",
      "domain\x00action.v1",
      "domain\x7faction.v1",
    ]) {
      const badHello = (await platform.openWs("/ws/v2/worker/session")).client!;
      await badHello.sendBinary(
        pb.encodeWorkerMessage({
          msg: {
            case: "hello",
            value: {
              workerId: "w-bad",
              interfaces: [bad],
              maxConcurrency: 1,
            },
          },
        }),
      );
      expect(badHello.popDecodedFrames()[0]).toMatchObject({
        kind: "error",
        code: GrpcCode.InvalidArgument,
      });
      expect(badHello.readyState).toBe(WS_READY_STATE_CLOSED);

      const badSub = await platform.unaryPost(
        "/rpc/v2/client/submit",
        pb.encodeSubmitRequest({
          jobId: "",
          spec: {
            demand: { interface: bad },
            inputs: [],
            params: new Uint8Array(0),
            metadata: {},
          },
          conflictPolicy: pb.ConflictPolicy.FAIL,
        }),
      );
      expect(badSub.grpcCode).toBe(GrpcCode.InvalidArgument);

      const badInv = (await platform.openWs("/ws/v2/client/invoke")).client!;
      await badInv.sendBinary(
        pb.encodeInvokeRequest({
          msg: {
            case: "start",
            value: {
              streamingInput: false,
              spec: {
                demand: { interface: bad },
                inputs: [],
                params: new Uint8Array(0),
                metadata: {},
              },
            },
          },
        }),
      );
      expect(badInv.popDecodedFrames()[0]).toMatchObject({
        kind: "error",
        code: GrpcCode.InvalidArgument,
      });
      expect(badInv.readyState).toBe(WS_READY_STATE_CLOSED);
    }

    const badConsoleRes = await platform.engine.fetch(
      new Request(`https://anyserve.example.com/api/jobs?interface=${encodeURIComponent(over65)}`),
    );
    expect(badConsoleRes.status).toBe(400);
  });
});
