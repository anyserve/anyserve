// Zero-external-dependency Node 24 HTTP + RFC 6455 WebSocket Native Gateway
// (`node --experimental-strip-types deploy/cloudflare/src/node-native-gateway.ts`)
// Wraps the real `NativeEngine` / `AnyserveNativeDO` control plane over `node:sqlite`,
// an in-memory Cloudflare R2 bucket, and `node:http` with full RFC 6455 WebSockets (`ws://`).

import { createHash } from "node:crypto";
import { mkdirSync, readFileSync } from "node:fs";
import {
  STATUS_CODES,
  createServer,
  type IncomingMessage,
  type Server,
  type ServerResponse,
} from "node:http";
import { registerHooks, stripTypeScriptTypes } from "node:module";
import type { Socket } from "node:net";
import { dirname } from "node:path";
import { DatabaseSync, type SQLInputValue, type StatementSync } from "node:sqlite";
import type { Duplex } from "node:stream";
import { fileURLToPath } from "node:url";

import type { NativeEngine } from "./native/engine";
import type {
  NativeDurableStorage,
  NativeEnvConfig,
  NativeHost,
  NativeR2Bucket,
  NativeR2ListResult,
  NativeR2ObjectBody,
  NativeR2Range,
  NativeRuntimeOverrides,
  NativeSqlCursor,
  NativeWebSocket,
  NativeWebSocketPairResult,
} from "./native/host";

const WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
const WS_MAX_MESSAGE_BYTES = 33_554_432; // 32 MiB exact (platform ceiling -> 1009)
const ATTACHMENT_LIMIT_BYTES = 256;
const SQL_MAX_QUERY_BYTES = 100 * 1024;
const SQL_MAX_BOUND_PARAMS = 100;
const SQL_MAX_VALUE_BYTES = 2 * 1024 * 1024;

const READY_STATE_OPEN = 1;
const READY_STATE_CLOSING = 2;
const READY_STATE_CLOSED = 3;

const OPCODE_CONTINUATION = 0x0;
const OPCODE_TEXT = 0x1;
const OPCODE_BINARY = 0x2;
const OPCODE_CLOSE = 0x8;
const OPCODE_PING = 0x9;
const OPCODE_PONG = 0xa;

const utf8Encoder = new TextEncoder();
const utf8FatalDecoder = new TextDecoder("utf-8", { fatal: true });

let hooksInstalled = false;

function ensureNodeTsHooks(): void {
  if (hooksInstalled) {
    return;
  }
  hooksInstalled = true;
  if ("__vitest_worker__" in globalThis) {
    return;
  }
  registerHooks({
    resolve(specifier, context, nextResolve) {
      if (specifier === "cloudflare:workers") {
        return {
          url: "data:text/javascript,export class DurableObject { constructor(ctx, env) { this.ctx = ctx; this.env = env; } }",
          shortCircuit: true,
        };
      }
      try {
        return nextResolve(specifier, context);
      } catch (err) {
        if (
          (specifier.startsWith("./") || specifier.startsWith("../")) &&
          !specifier.endsWith(".ts") &&
          !specifier.endsWith(".js")
        ) {
          return nextResolve(`${specifier}.ts`, context);
        }
        throw err;
      }
    },
    load(url, context, nextLoad) {
      if (url.startsWith("file://") && url.endsWith(".ts")) {
        const source = readFileSync(fileURLToPath(url), "utf8");
        return {
          format: "module",
          source: stripTypeScriptTypes(source, {
            mode: "transform",
            sourceUrl: url,
          }),
          shortCircuit: true,
        };
      }
      return nextLoad(url, context);
    },
  });
}

// ---------------------------------------------------------------------------
// Node SQLite Storage Adapter (enforces Cloudflare DO SQLite invariants)
// ---------------------------------------------------------------------------

interface CachedSqlEntry {
  stmt: StatementSync | null;
  isSelect: boolean;
  isReturning: boolean;
  isDdl: boolean;
  indexWriteFactor: number;
}

const MAX_UTF16_LEN_UNDER_2MIB = Math.floor(SQL_MAX_VALUE_BYTES / 3);
const MAX_UTF16_LEN_UNDER_QUERY_LIMIT = Math.floor(SQL_MAX_QUERY_BYTES / 3);

export class NodeNativeSqliteStorage implements NativeDurableStorage {
  readonly db: DatabaseSync;
  private savepointSeq = 0;
  private readonly stmtCache = new Map<string, CachedSqlEntry>();
  private readonly pageCountStmt: StatementSync;
  private readonly pageSizeStmt: StatementSync;
  totalRowsWritten = 0;
  totalRowsRead = 0;

  readonly sql: {
    exec: <T extends Record<string, unknown> = Record<string, unknown>>(
      query: string,
      ...bindings: unknown[]
    ) => NativeSqlCursor<T>;
    readonly databaseSize: number;
  };

  constructor(db: DatabaseSync) {
    this.db = db;
    this.pageCountStmt = db.prepare("PRAGMA page_count");
    this.pageSizeStmt = db.prepare("PRAGMA page_size");
    const self = this;
    this.sql = {
      exec<T extends Record<string, unknown> = Record<string, unknown>>(
        query: string,
        ...bindings: unknown[]
      ): NativeSqlCursor<T> {
        return self.execInternal<T>(query, bindings);
      },
      get databaseSize(): number {
        const pc = Number(
          (self.pageCountStmt.get() as { page_count?: number } | undefined)
            ?.page_count ?? 0,
        );
        const ps = Number(
          (self.pageSizeStmt.get() as { page_size?: number } | undefined)
            ?.page_size ?? 4096,
        );
        return pc * ps;
      },
    };
  }

  private getOrPrepareSql(query: string): CachedSqlEntry {
    const cached = this.stmtCache.get(query);
    if (cached !== undefined) {
      return cached;
    }
    if (query.length > MAX_UTF16_LEN_UNDER_QUERY_LIMIT) {
      const sqlBytes = Buffer.byteLength(query, "utf8");
      if (sqlBytes > SQL_MAX_QUERY_BYTES) {
        throw new Error(
          `Cloudflare DO SQLite limit exceeded: ${sqlBytes} SQL bytes (max ${SQL_MAX_QUERY_BYTES})`,
        );
      }
    }
    const trimmed = query.trim();
    const upper = trimmed.toUpperCase();
    const isSelect = upper.startsWith("SELECT") || upper.startsWith("PRAGMA");
    const isReturning = !isSelect && /\bRETURNING\b/i.test(trimmed);
    const isDdl =
      !isSelect &&
      (upper.startsWith("CREATE ") ||
        upper.startsWith("DROP ") ||
        upper.startsWith("ALTER "));

    let indexWriteFactor = 0;
    if (!isSelect && !isDdl && /\bnative_jobs\b/i.test(trimmed)) {
      if (upper.startsWith("INSERT") || upper.startsWith("DELETE")) {
        indexWriteFactor = 2;
      } else if (upper.startsWith("UPDATE")) {
        if (
          /\bstate\s*=/i.test(trimmed) ||
          /\bupdated_at_ms\s*=/i.test(trimmed)
        ) {
          indexWriteFactor += 1;
        }
        if (/\bwinning_lease_id\s*=/i.test(trimmed)) {
          indexWriteFactor += 1;
        }
      }
    }

    const entry: CachedSqlEntry = {
      stmt: isDdl ? null : this.db.prepare(query),
      isSelect,
      isReturning,
      isDdl,
      indexWriteFactor,
    };
    if (!isDdl && this.stmtCache.size < 256) {
      this.stmtCache.set(query, entry);
    }
    return entry;
  }

  private execInternal<T extends Record<string, unknown>>(
    query: string,
    bindings: unknown[],
  ): NativeSqlCursor<T> {
    if (bindings.length > SQL_MAX_BOUND_PARAMS) {
      throw new Error(
        `Cloudflare DO SQLite limit exceeded: ${bindings.length} bound parameters (max ${SQL_MAX_BOUND_PARAMS})`,
      );
    }

    const meta = this.getOrPrepareSql(query);
    if (meta.isDdl) {
      this.stmtCache.clear();
      this.db.exec(query);
      this.totalRowsWritten += 1;
      return {
        toArray: () => [],
        rowsRead: 0,
        rowsWritten: 1,
      };
    }

    const n = bindings.length;
    const normalized: SQLInputValue[] = new Array(n);
    for (let i = 0; i < n; i++) {
      const b = bindings[i];
      if (b === undefined || b === null) {
        normalized[i] = null;
      } else if (typeof b === "number" || typeof b === "bigint") {
        normalized[i] = b;
      } else if (typeof b === "string") {
        if (b.length > MAX_UTF16_LEN_UNDER_2MIB) {
          const byteLen = Buffer.byteLength(b, "utf8");
          if (byteLen > SQL_MAX_VALUE_BYTES) {
            throw new Error(
              `SQLite TEXT binding exceeds 2 MiB DO row limit (${byteLen} bytes)`,
            );
          }
        }
        normalized[i] = b;
      } else if (b instanceof Uint8Array) {
        if (b.byteLength > SQL_MAX_VALUE_BYTES) {
          throw new Error(
            `SQLite BLOB binding exceeds 2 MiB DO row limit (${b.byteLength} bytes)`,
          );
        }
        normalized[i] = b;
      } else if (b instanceof ArrayBuffer) {
        if (b.byteLength > SQL_MAX_VALUE_BYTES) {
          throw new Error(
            `SQLite ArrayBuffer binding exceeds 2 MiB DO row limit (${b.byteLength} bytes)`,
          );
        }
        normalized[i] = new Uint8Array(b);
      } else if (ArrayBuffer.isView(b)) {
        const view = new Uint8Array(b.buffer, b.byteOffset, b.byteLength);
        if (view.byteLength > SQL_MAX_VALUE_BYTES) {
          throw new Error(
            `SQLite view binding exceeds 2 MiB DO row limit (${view.byteLength} bytes)`,
          );
        }
        normalized[i] = view;
      } else {
        throw new TypeError(`Unsupported SQL binding type: ${typeof b}`);
      }
    }

    const stmt = meta.stmt!;
    let rows: T[] = [];
    let baseChanges = 0;

    if (meta.isSelect) {
      rows = stmt.all(...normalized) as T[];
    } else if (meta.isReturning) {
      rows = stmt.all(...normalized) as T[];
      baseChanges = rows.length;
    } else {
      const info = stmt.run(...normalized);
      baseChanges = Number(info.changes ?? 0);
    }

    const indexWrites =
      !meta.isSelect && baseChanges > 0 ? meta.indexWriteFactor * baseChanges : 0;
    const rowsWritten = meta.isSelect ? 0 : baseChanges + indexWrites;
    const rowsRead = meta.isSelect ? Math.max(1, rows.length) : baseChanges;
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
    this.db.exec(`SAVEPOINT ${sp}`);
    try {
      const result = closure();
      if (
        result !== null &&
        (typeof result === "object" || typeof result === "function") &&
        typeof (result as { then?: unknown }).then === "function"
      ) {
        throw new Error(
          "transactionSync callback returned a Promise/thenable; async inside transactionSync is forbidden",
        );
      }
      this.db.exec(`RELEASE SAVEPOINT ${sp}`);
      return result;
    } catch (err) {
      try {
        this.db.exec(`ROLLBACK TO SAVEPOINT ${sp}`);
        this.db.exec(`RELEASE SAVEPOINT ${sp}`);
      } catch {
        // Ignore rollback failure if already aborted.
      }
      this.totalRowsWritten = savedWrites;
      throw err;
    }
  }
}

// ---------------------------------------------------------------------------
// In-Memory R2 Bucket (Range GET, PUT, DELETE, paged LIST)
// ---------------------------------------------------------------------------

interface StoredR2Object {
  bytes: Uint8Array;
  uploadedAtMs: number;
}

export class InMemoryNativeR2Bucket implements NativeR2Bucket {
  readonly objects = new Map<string, StoredR2Object>();
  private readonly nowFn: () => number;
  putCount = 0;
  getCount = 0;
  rangeGetCount = 0;
  deleteCount = 0;
  listCount = 0;

  constructor(nowFn: () => number) {
    this.nowFn = nowFn;
  }

  get size(): number {
    return this.objects.size;
  }

  async put(
    key: string,
    value:
      | ReadableStream
      | ArrayBuffer
      | ArrayBufferView
      | string
      | Blob
      | null,
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
      const resp = new Response(value);
      bytes = new Uint8Array(await resp.arrayBuffer());
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
    if (!entry) {
      return null;
    }
    let slice = entry.bytes;
    if (options?.range) {
      this.rangeGetCount += 1;
      const offset = Math.max(0, options.range.offset ?? 0);
      const end =
        options.range.length !== undefined
          ? Math.min(
              entry.bytes.byteLength,
              offset + Math.max(0, options.range.length),
            )
          : entry.bytes.byteLength;
      slice = entry.bytes.subarray(offset, end);
    }
    const copy = new Uint8Array(slice.byteLength);
    copy.set(slice);
    return {
      arrayBuffer: async () => copy.buffer,
    };
  }

  async delete(keys: string | string[]): Promise<void> {
    this.deleteCount += 1;
    const list = Array.isArray(keys) ? keys : [keys];
    for (const k of list) {
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
    const sorted = [...this.objects.keys()]
      .filter(
        (k) =>
          k.startsWith(prefix) && (afterKey === undefined || k > afterKey),
      )
      .sort();
    const limit = options?.limit ?? 1000;
    const page = sorted.slice(0, limit);
    const truncated = page.length < sorted.length;
    return {
      objects: page.map((k) => {
        const obj = this.objects.get(k)!;
        return {
          key: k,
          size: obj.bytes.byteLength,
          uploaded: new Date(obj.uploadedAtMs),
        };
      }),
      truncated,
      cursor:
        truncated && page.length > 0 ? page[page.length - 1] : undefined,
    };
  }
}

// ---------------------------------------------------------------------------
// RFC 6455 Framing Helpers (Fast 32-bit Unmasking, 7/16/64-bit Lengths)
// ---------------------------------------------------------------------------

function unmaskInPlace(payload: Buffer, mask: Buffer): void {
  const len = payload.length;
  if (len === 0) {
    return;
  }
  const m0 = mask[0]!;
  const m1 = mask[1]!;
  const m2 = mask[2]!;
  const m3 = mask[3]!;
  if ((m0 | m1 | m2 | m3) === 0) {
    return;
  }
  // Word-aligned 4-byte XOR fast path when byteOffset is 4-byte aligned, else safe byte loop.
  let i = 0;
  if ((payload.byteOffset & 3) === 0 && len >= 16) {
    const maskWord =
      (m0 | (m1 << 8) | (m2 << 16) | (m3 << 24)) >>> 0; // LE word order on x86_64/arm64 LE
    const isLittleEndian =
      new Uint8Array(new Uint16Array([0x0102]).buffer)[0] === 0x02;
    if (isLittleEndian) {
      const words = new Uint32Array(
        payload.buffer,
        payload.byteOffset,
        len >>> 2,
      );
      for (let w = 0; w < words.length; w++) {
        words[w] = (words[w]! ^ maskWord) >>> 0;
      }
      i = words.length << 2;
    }
  }
  for (; i < len; i++) {
    payload[i] = payload[i]! ^ mask[i & 3]!;
  }
}

function encodeWsFrameHeader(opcode: number, payloadLen: number): Buffer {
  const firstByte = 0x80 | (opcode & 0x0f);
  if (payloadLen < 126) {
    const hdr = Buffer.allocUnsafe(2);
    hdr[0] = firstByte;
    hdr[1] = payloadLen;
    return hdr;
  }
  if (payloadLen <= 0xffff) {
    const hdr = Buffer.allocUnsafe(4);
    hdr[0] = firstByte;
    hdr[1] = 126;
    hdr.writeUInt16BE(payloadLen, 2);
    return hdr;
  }
  const hdr = Buffer.allocUnsafe(10);
  hdr[0] = firstByte;
  hdr[1] = 127;
  const hi = Math.floor(payloadLen / 4_294_967_296);
  const lo = payloadLen >>> 0;
  hdr.writeUInt32BE(hi, 2);
  hdr.writeUInt32BE(lo, 6);
  return hdr;
}

function encodeWsControlFrame(opcode: number, payload?: Uint8Array): Buffer {
  const body = payload ?? new Uint8Array(0);
  const hdr = encodeWsFrameHeader(opcode, body.byteLength);
  if (body.byteLength === 0) {
    return hdr;
  }
  return Buffer.concat([hdr, Buffer.from(body.buffer, body.byteOffset, body.byteLength)]);
}

function encodeCloseFrame(code?: number, reason?: string): Buffer {
  if (code === undefined) {
    return encodeWsControlFrame(OPCODE_CLOSE);
  }
  const reasonBytes = reason ? utf8Encoder.encode(reason) : new Uint8Array(0);
  const cappedReason =
    reasonBytes.byteLength > 123 ? reasonBytes.subarray(0, 123) : reasonBytes;
  const payload = new Uint8Array(2 + cappedReason.byteLength);
  payload[0] = (code >>> 8) & 0xff;
  payload[1] = code & 0xff;
  payload.set(cappedReason, 2);
  return encodeWsControlFrame(OPCODE_CLOSE, payload);
}

function isValidPeerEchoCloseCode(code: number): boolean {
  if (code < 1000 || code >= 5000) {
    return false;
  }
  if (code === 1004 || code === 1005 || code === 1006 || code === 1015) {
    return false;
  }
  if (code >= 1016 && code < 3000) {
    return false;
  }
  return true;
}

// ---------------------------------------------------------------------------
// UpgradeResponse & NodeServerWebSocket (RFC 6455 over Duplex/Socket)
// ---------------------------------------------------------------------------

class GatewayUpgradeResponse extends Response {
  readonly serverWebSocket: NodeServerWebSocket;
  private readonly upgradeHeaders: Headers;

  constructor(serverWebSocket: NodeServerWebSocket, headers?: HeadersInit) {
    super(null, { status: 200, headers });
    this.serverWebSocket = serverWebSocket;
    this.upgradeHeaders = new Headers(headers);
  }

  override get status(): number {
    return 101;
  }

  get extraHeaders(): Headers {
    return this.upgradeHeaders;
  }
}

class ChunkAccumulator {
  private chunks: Buffer[] = [];
  private totalLen = 0;

  get length(): number {
    return this.totalLen;
  }

  push(chunk: Buffer): void {
    if (chunk.length === 0) {
      return;
    }
    this.chunks.push(chunk);
    this.totalLen += chunk.length;
  }

  peekHeader(minBytes: number): Buffer | null {
    if (this.totalLen < minBytes) {
      return null;
    }
    const first = this.chunks[0]!;
    if (first.length >= minBytes) {
      return first;
    }
    const out = Buffer.allocUnsafe(minBytes);
    let copied = 0;
    for (const c of this.chunks) {
      const n = Math.min(c.length, minBytes - copied);
      c.copy(out, copied, 0, n);
      copied += n;
      if (copied >= minBytes) {
        break;
      }
    }
    return out;
  }

  consume(byteCount: number): Buffer {
    if (byteCount === 0) {
      return Buffer.alloc(0);
    }
    if (byteCount > this.totalLen) {
      throw new Error("ChunkAccumulator underflow");
    }
    const first = this.chunks[0]!;
    if (first.length === byteCount) {
      this.chunks.shift();
      this.totalLen -= byteCount;
      return first;
    }
    if (first.length > byteCount) {
      const slice = first.subarray(0, byteCount);
      this.chunks[0] = first.subarray(byteCount);
      this.totalLen -= byteCount;
      return slice;
    }
    const out = Buffer.allocUnsafe(byteCount);
    let copied = 0;
    while (copied < byteCount) {
      const head = this.chunks[0]!;
      const need = byteCount - copied;
      if (head.length <= need) {
        head.copy(out, copied);
        copied += head.length;
        this.chunks.shift();
      } else {
        head.copy(out, copied, 0, need);
        this.chunks[0] = head.subarray(need);
        copied += need;
      }
    }
    this.totalLen -= byteCount;
    return out;
  }

  clear(): void {
    this.chunks = [];
    this.totalLen = 0;
  }
}

export class NodeServerWebSocket implements NativeWebSocket {
  readyState: number = READY_STATE_OPEN;
  acceptedTags: string[] = [];

  private socket: Duplex | null = null;
  private readonly controller: NativeGatewayController;
  private serializedAttachmentJson: string | null = null;
  private pendingWriteBytes = 0;
  private preAttachWrites: Array<{
    header: Buffer;
    payload: Uint8Array;
    closeAfter?: { code?: number; reason?: string };
  }> = [];

  private readonly rxBuf = new ChunkAccumulator();
  private fragOpcode: number | null = null;
  private fragChunks: Buffer[] = [];
  private fragTotalBytes = 0;

  private closeFrameSent = false;
  private peerCloseReceived = false;
  private closeCallbackDelivered = false;
  private discardingInput = false;
  private destroyTimer: ReturnType<typeof setTimeout> | null = null;

  private readonly inboundQueue: Array<
    | { type: "message"; data: Uint8Array | string }
    | { type: "close"; code: number; reason: string; wasClean: boolean }
  > = [];
  private pumpingInbound = false;

  constructor(controller: NativeGatewayController) {
    this.controller = controller;
  }

  get bufferedAmount(): number {
    const sockLen = this.socket ? this.socket.writableLength : 0;
    return Math.max(this.pendingWriteBytes, sockLen);
  }

  serializeAttachment(attachment: unknown): void {
    const json = JSON.stringify(attachment);
    const byteLen = Buffer.byteLength(json, "utf8");
    if (byteLen > ATTACHMENT_LIMIT_BYTES) {
      throw new Error(
        `WebSocket attachment exceeds ${ATTACHMENT_LIMIT_BYTES} bytes (${byteLen} bytes)`,
      );
    }
    this.serializedAttachmentJson = json;
  }

  deserializeAttachment(): unknown {
    if (this.serializedAttachmentJson === null) {
      return null;
    }
    return JSON.parse(this.serializedAttachmentJson);
  }

  send(message: ArrayBuffer | ArrayBufferView | string): void {
    if (this.readyState !== READY_STATE_OPEN || this.closeFrameSent) {
      throw new Error("WebSocket is not open");
    }

    let opcode: number;
    let payload: Uint8Array;
    if (typeof message === "string") {
      opcode = OPCODE_TEXT;
      payload = utf8Encoder.encode(message);
    } else if (message instanceof Uint8Array) {
      opcode = OPCODE_BINARY;
      payload = message;
    } else if (message instanceof ArrayBuffer) {
      opcode = OPCODE_BINARY;
      payload = new Uint8Array(message);
    } else {
      opcode = OPCODE_BINARY;
      payload = new Uint8Array(
        message.buffer,
        message.byteOffset,
        message.byteLength,
      );
    }

    const header = encodeWsFrameHeader(opcode, payload.byteLength);
    if (!this.socket) {
      this.pendingWriteBytes += header.length + payload.byteLength;
      this.preAttachWrites.push({ header, payload: new Uint8Array(payload) });
      return;
    }

    this.writeFrameToSocket(header, payload);
  }

  close(code?: number, reason?: string): void {
    if (code === 1005 || code === 1006) {
      throw new Error(`Application must never pass reserved close code ${code}`);
    }
    if (this.readyState === READY_STATE_CLOSED) {
      return;
    }
    this.readyState = READY_STATE_CLOSED;
    if (this.closeFrameSent) {
      return;
    }
    this.closeFrameSent = true;
    const closeBuf = encodeCloseFrame(code, reason);
    if (!this.socket) {
      this.preAttachWrites.push({
        header: closeBuf,
        payload: new Uint8Array(0),
        closeAfter: { code, reason },
      });
      return;
    }
    this.endSocketGracefully(closeBuf);
  }

  attachTcpSocket(socket: Duplex, initialChunks: Buffer[]): void {
    this.socket = socket;
    const netSock = socket as Socket;
    if (typeof netSock.setNoDelay === "function") {
      netSock.setNoDelay(true);
    }

    socket.on("drain", () => {
      this.controller.notifySocketDrain(this);
    });

    socket.on("data", (chunk: Buffer) => {
      if (this.discardingInput) {
        return;
      }
      this.rxBuf.push(chunk);
      this.parseFrames();
    });

    socket.on("end", () => {
      if (!this.peerCloseReceived && !this.closeCallbackDelivered) {
        if (this.closeFrameSent) {
          this.enqueueInbound({
            type: "close",
            code: 1000,
            reason: "",
            wasClean: true,
          });
        } else {
          this.readyState = READY_STATE_CLOSED;
          this.enqueueInbound({
            type: "close",
            code: 1006,
            reason: "abnormal closure",
            wasClean: false,
          });
        }
      }
    });

    socket.on("close", () => {
      this.clearDestroyTimer();
      this.pendingWriteBytes = 0;
      this.readyState = READY_STATE_CLOSED;
      if (!this.closeCallbackDelivered) {
        this.enqueueInbound({
          type: "close",
          code: this.peerCloseReceived || this.closeFrameSent ? 1000 : 1006,
          reason: this.peerCloseReceived || this.closeFrameSent ? "" : "abnormal closure",
          wasClean: this.peerCloseReceived || this.closeFrameSent,
        });
      }
      this.controller.notifySocketDrain(this);
    });

    socket.on("error", () => {
      this.pendingWriteBytes = 0;
      this.readyState = READY_STATE_CLOSED;
      if (!this.closeCallbackDelivered) {
        this.enqueueInbound({
          type: "close",
          code: 1006,
          reason: "socket error",
          wasClean: false,
        });
      }
    });

    // Flush any frames queued synchronously during upgrade
    if (this.preAttachWrites.length > 0) {
      const queued = this.preAttachWrites;
      this.preAttachWrites = [];
      this.pendingWriteBytes = 0;
      for (const item of queued) {
        if (item.closeAfter !== undefined) {
          this.endSocketGracefully(item.header);
        } else {
          this.writeFrameToSocket(item.header, item.payload);
        }
      }
    }

    for (const c of initialChunks) {
      if (c.length > 0 && !this.discardingInput) {
        this.rxBuf.push(c);
      }
    }
    if (this.rxBuf.length > 0 && !this.discardingInput) {
      this.parseFrames();
    }
  }

  forceDestroyTcp(): void {
    this.clearDestroyTimer();
    if (this.socket && !this.socket.destroyed) {
      this.socket.destroy();
    }
  }

  private clearDestroyTimer(): void {
    if (this.destroyTimer !== null) {
      clearTimeout(this.destroyTimer);
      this.destroyTimer = null;
    }
  }

  private writeFrameToSocket(header: Buffer, payload: Uint8Array): void {
    const sock = this.socket;
    if (!sock || sock.destroyed || !sock.writable) {
      return;
    }
    const frameBytes = header.length + payload.byteLength;
    this.pendingWriteBytes += frameBytes;
    sock.cork();
    if (payload.byteLength === 0) {
      sock.write(header, () => {
        this.pendingWriteBytes = Math.max(0, this.pendingWriteBytes - frameBytes);
        this.controller.notifySocketDrain(this);
      });
    } else {
      sock.write(header);
      const bodyBuf = Buffer.from(
        payload.buffer,
        payload.byteOffset,
        payload.byteLength,
      );
      sock.write(bodyBuf, () => {
        this.pendingWriteBytes = Math.max(0, this.pendingWriteBytes - frameBytes);
        this.controller.notifySocketDrain(this);
      });
    }
    sock.uncork();
  }

  private endSocketGracefully(closeFrame: Buffer): void {
    const sock = this.socket;
    if (!sock || sock.destroyed) {
      return;
    }
    if (sock.writable) {
      sock.end(closeFrame);
    }
    // Wait until all queued outbound data + closeFrame are flushed ("finish") before starting
    // a fallback destroy timer, so multi-MiB frames (e.g. 8 MiB / 21 MiB Finished) never truncate!
    sock.once("finish", () => {
      this.clearDestroyTimer();
      this.destroyTimer = setTimeout(() => {
        if (this.socket && !this.socket.destroyed) {
          this.socket.destroy();
        }
      }, 2_000);
      this.destroyTimer.unref();
    });
  }

  private failProtocol(closeCode: number, reason: string, notifyCode?: number): void {
    this.discardingInput = true;
    this.rxBuf.clear();
    this.fragChunks = [];
    this.fragTotalBytes = 0;
    this.fragOpcode = null;
    this.readyState = READY_STATE_CLOSED;
    if (!this.closeFrameSent) {
      this.closeFrameSent = true;
      const closeBuf = encodeCloseFrame(closeCode, reason);
      if (this.socket && !this.socket.destroyed && this.socket.writable) {
        this.socket.end(closeBuf);
        this.clearDestroyTimer();
        this.destroyTimer = setTimeout(() => {
          if (this.socket && !this.socket.destroyed) {
            this.socket.destroy();
          }
        }, 250);
        this.destroyTimer.unref();
      }
    }
    if (!this.closeCallbackDelivered) {
      this.enqueueInbound({
        type: "close",
        code: notifyCode ?? closeCode,
        reason,
        wasClean: false,
      });
    }
  }

  private parseFrames(): void {
    while (!this.discardingInput && this.rxBuf.length >= 2) {
      const hdr2 = this.rxBuf.peekHeader(2)!;
      const b0 = hdr2[0]!;
      const b1 = hdr2[1]!;

      const fin = (b0 & 0x80) !== 0;
      const rsv = b0 & 0x70;
      const opcode = b0 & 0x0f;
      const masked = (b1 & 0x80) !== 0;
      const len7 = b1 & 0x7f;

      if (rsv !== 0) {
        this.failProtocol(1002, "Unexpected RSV bits");
        return;
      }
      if (!masked) {
        this.failProtocol(1002, "Client frames must be masked");
        return;
      }

      const isControl = (opcode & 0x08) !== 0;
      if (isControl && (!fin || len7 > 125)) {
        this.failProtocol(1002, "Invalid WebSocket control frame");
        return;
      }

      let headerLen = 2;
      let payloadLen = len7;
      if (len7 === 126) {
        headerLen = 4;
        const hdr4 = this.rxBuf.peekHeader(4);
        if (!hdr4) {
          return;
        }
        payloadLen = hdr4.readUInt16BE(2);
      } else if (len7 === 127) {
        headerLen = 10;
        const hdr10 = this.rxBuf.peekHeader(10);
        if (!hdr10) {
          return;
        }
        const hi = hdr10.readUInt32BE(2);
        const lo = hdr10.readUInt32BE(6);
        if ((hi & 0x8000_0000) !== 0) {
          this.failProtocol(1002, "Invalid 64-bit WebSocket length");
          return;
        }
        payloadLen = hi * 4_294_967_296 + lo;
      }

      // Platform 32 MiB ceiling check -> Close 1009 BEFORE buffering > 32 MiB!
      if (!isControl) {
        const projectedBytes =
          (opcode === OPCODE_CONTINUATION ? this.fragTotalBytes : 0) +
          payloadLen;
        if (projectedBytes > WS_MAX_MESSAGE_BYTES) {
          this.failProtocol(1009, "Message too big", 1009);
          return;
        }
      }

      const totalFrameBytes = headerLen + 4 + payloadLen;
      if (this.rxBuf.length < totalFrameBytes) {
        return;
      }

      this.rxBuf.consume(headerLen);
      const maskKey = this.rxBuf.consume(4);
      const payloadBuf = this.rxBuf.consume(payloadLen);
      unmaskInPlace(payloadBuf, maskKey);

      if (opcode === OPCODE_PING) {
        if (!this.closeFrameSent && this.socket && !this.socket.destroyed && this.socket.writable) {
          const pong = encodeWsControlFrame(OPCODE_PONG, payloadBuf);
          this.socket.write(pong);
        }
        continue;
      }

      if (opcode === OPCODE_PONG) {
        continue;
      }

      if (opcode === OPCODE_CLOSE) {
        this.peerCloseReceived = true;
        if (payloadBuf.length === 1) {
          this.failProtocol(1002, "Invalid 1-byte close payload", 1002);
          return;
        }
        let closeCode = 1005;
        let closeReason = "";
        if (payloadBuf.length >= 2) {
          closeCode = payloadBuf.readUInt16BE(0);
          if (!isValidPeerEchoCloseCode(closeCode)) {
            this.failProtocol(1002, "Invalid close code", 1002);
            return;
          }
          if (payloadBuf.length > 2) {
            try {
              closeReason = utf8FatalDecoder.decode(payloadBuf.subarray(2));
            } catch {
              this.failProtocol(1007, "Invalid UTF-8 in close reason", 1007);
              return;
            }
          }
        }

        if (!this.closeFrameSent) {
          this.closeFrameSent = true;
          this.readyState = READY_STATE_CLOSED;
          const replyBuf =
            closeCode === 1005
              ? encodeCloseFrame()
              : encodeCloseFrame(closeCode);
          this.endSocketGracefully(replyBuf);
        } else {
          this.readyState = READY_STATE_CLOSED;
        }

        this.discardingInput = true;
        this.rxBuf.clear();
        this.enqueueInbound({
          type: "close",
          code: closeCode,
          reason: closeReason,
          wasClean: true,
        });
        return;
      }

      if (opcode === OPCODE_BINARY || opcode === OPCODE_TEXT) {
        if (this.fragOpcode !== null) {
          this.failProtocol(1002, "Expected continuation frame");
          return;
        }
        if (fin) {
          this.deliverDataMessage(opcode, payloadBuf);
        } else {
          this.fragOpcode = opcode;
          this.fragChunks = [Buffer.from(payloadBuf)];
          this.fragTotalBytes = payloadBuf.length;
        }
        continue;
      }

      if (opcode === OPCODE_CONTINUATION) {
        if (this.fragOpcode === null) {
          this.failProtocol(1002, "Unexpected continuation frame");
          return;
        }
        this.fragChunks.push(Buffer.from(payloadBuf));
        this.fragTotalBytes += payloadBuf.length;
        if (fin) {
          const msgOpcode = this.fragOpcode;
          const combined = Buffer.concat(this.fragChunks, this.fragTotalBytes);
          this.fragOpcode = null;
          this.fragChunks = [];
          this.fragTotalBytes = 0;
          this.deliverDataMessage(msgOpcode, combined);
        }
        continue;
      }

      this.failProtocol(1002, `Unsupported opcode ${opcode}`);
      return;
    }
  }

  private deliverDataMessage(opcode: number, buf: Buffer): void {
    if (opcode === OPCODE_TEXT) {
      let text: string;
      try {
        text = utf8FatalDecoder.decode(buf);
      } catch {
        this.failProtocol(1007, "Invalid UTF-8 text frame", 1007);
        return;
      }
      this.enqueueInbound({ type: "message", data: text });
      return;
    }
    const copy = new Uint8Array(buf.length);
    copy.set(buf);
    this.enqueueInbound({ type: "message", data: copy });
  }

  private enqueueInbound(
    item:
      | { type: "message"; data: Uint8Array | string }
      | { type: "close"; code: number; reason: string; wasClean: boolean },
  ): void {
    if (item.type === "close") {
      if (this.closeCallbackDelivered) {
        return;
      }
      this.closeCallbackDelivered = true;
    }
    this.inboundQueue.push(item);
    if (!this.pumpingInbound) {
      void this.pumpInbound();
    }
  }

  private async pumpInbound(): Promise<void> {
    if (this.pumpingInbound) {
      return;
    }
    this.pumpingInbound = true;
    try {
      while (this.inboundQueue.length > 0) {
        const item = this.inboundQueue.shift()!;
        if (item.type === "message") {
          await this.controller.dispatchWsMessage(this, item.data);
        } else {
          await this.controller.dispatchWsClose(
            this,
            item.code,
            item.reason,
            item.wasClean,
          );
        }
      }
    } finally {
      this.pumpingInbound = false;
    }
  }
}

// ---------------------------------------------------------------------------
// NativeGatewayController (Manages Engine/DO Lifecycle, Alarms & Hibernation)
// ---------------------------------------------------------------------------

class NativeGatewayController {
  readonly doId: string;
  readonly storage: NodeNativeSqliteStorage;
  readonly r2: InMemoryNativeR2Bucket;
  readonly env: NativeEnvConfig;
  readonly overrides: NativeRuntimeOverrides;
  private readonly EngineClass: typeof NativeEngine;

  private clockOffsetMs = 0;
  private scheduledAlarmMs: number | null = null;
  private alarmTimer: ReturnType<typeof setTimeout> | null = null;
  private constructorCounter = 0;
  private readonly acceptedSockets = new Set<NodeServerWebSocket>();
  private readonly activeHandlers = new Set<Promise<unknown>>();
  private hibernationChain: Promise<unknown> = Promise.resolve();
  private stopped = false;

  engine: NativeEngine;

  constructor(params: {
    doId: string;
    storage: NodeNativeSqliteStorage;
    r2: InMemoryNativeR2Bucket;
    env: NativeEnvConfig;
    overrides: NativeRuntimeOverrides;
    EngineClass: typeof NativeEngine;
  }) {
    this.doId = params.doId;
    this.storage = params.storage;
    this.r2 = params.r2;
    this.env = params.env;
    this.overrides = params.overrides;
    this.EngineClass = params.EngineClass;
    this.engine = this.instantiateEngine();
  }

  nowMs(): number {
    return Date.now() + this.clockOffsetMs;
  }

  get openWebSocketCount(): number {
    this.pruneClosedSockets();
    return this.acceptedSockets.size;
  }

  private pruneClosedSockets(): void {
    for (const ws of this.acceptedSockets) {
      if (ws.readyState === READY_STATE_CLOSED) {
        this.acceptedSockets.delete(ws);
      }
    }
  }

  private buildHost(): NativeHost {
    return {
      doId: this.doId,
      storage: this.storage,
      r2: this.r2,
      env: this.env,
      overrides: this.overrides,
      acceptWebSocket: (ws: NativeWebSocket, tags?: string[]) => {
        const nws = ws as NodeServerWebSocket;
        nws.acceptedTags = tags ? [...tags] : [];
        this.acceptedSockets.add(nws);
      },
      getWebSockets: (tag?: string): NativeWebSocket[] => {
        this.pruneClosedSockets();
        const out: NativeWebSocket[] = [];
        for (const ws of this.acceptedSockets) {
          if (
            ws.readyState === READY_STATE_OPEN &&
            (tag === undefined || ws.acceptedTags.includes(tag))
          ) {
            out.push(ws);
          }
        }
        return out;
      },
      createWebSocketPair: (): NativeWebSocketPairResult => {
        const server = new NodeServerWebSocket(this);
        return {
          client: server,
          server,
          toUpgradeResponse: (headers?: HeadersInit) =>
            new GatewayUpgradeResponse(server, headers),
        };
      },
      nowMs: () => this.nowMs(),
      setAlarm: async (scheduledTimeMs: number) => {
        this.scheduledAlarmMs = scheduledTimeMs;
        this.scheduleNodeAlarmTimer();
      },
      getAlarm: async () => this.scheduledAlarmMs,
      deleteAlarm: async () => {
        this.scheduledAlarmMs = null;
        this.clearNodeAlarmTimer();
      },
      nextConstructorRun: () => {
        this.constructorCounter += 1;
        return this.constructorCounter;
      },
    };
  }

  private instantiateEngine(): NativeEngine {
    return new this.EngineClass(this.buildHost());
  }

  private clearNodeAlarmTimer(): void {
    if (this.alarmTimer !== null) {
      clearTimeout(this.alarmTimer);
      this.alarmTimer = null;
    }
  }

  private scheduleNodeAlarmTimer(): void {
    this.clearNodeAlarmTimer();
    if (this.stopped || this.scheduledAlarmMs === null) {
      return;
    }
    const delayMs = Math.max(0, this.scheduledAlarmMs - this.nowMs());
    this.alarmTimer = setTimeout(() => {
      this.alarmTimer = null;
      void this.fireDueAlarmFromTimer();
    }, delayMs);
    this.alarmTimer.unref();
  }

  private async trackHandler<T>(fn: () => Promise<T>): Promise<T> {
    await this.hibernationChain;
    const p = fn();
    this.activeHandlers.add(p);
    try {
      return await p;
    } finally {
      this.activeHandlers.delete(p);
    }
  }

  private async fireDueAlarmFromTimer(): Promise<void> {
    if (this.stopped || this.scheduledAlarmMs === null) {
      return;
    }
    if (this.scheduledAlarmMs > this.nowMs()) {
      this.scheduleNodeAlarmTimer();
      return;
    }
    this.scheduledAlarmMs = null;
    await this.trackHandler(async () => {
      await this.engine.alarm();
    });
  }

  async fetch(request: Request): Promise<Response> {
    return await this.trackHandler(async () => {
      return await this.engine.fetch(request);
    });
  }

  async dispatchWsMessage(
    ws: NodeServerWebSocket,
    data: Uint8Array | string,
  ): Promise<void> {
    if (this.stopped) {
      return;
    }
    await this.trackHandler(async () => {
      if (this.stopped) {
        return;
      }
      await this.engine.webSocketMessage(ws, data);
    });
  }

  async dispatchWsClose(
    ws: NodeServerWebSocket,
    code: number,
    reason: string,
    wasClean: boolean,
  ): Promise<void> {
    this.acceptedSockets.delete(ws);
    if (this.stopped) {
      return;
    }
    await this.trackHandler(async () => {
      if (this.stopped) {
        return;
      }
      await this.engine.webSocketClose(ws, code, reason, wasClean);
    });
  }

  notifySocketDrain(ws: NodeServerWebSocket): void {
    if (this.stopped) {
      return;
    }
    this.engine.onSocketBufferDrain(ws);
  }

  async simulateHibernation(): Promise<{
    prev_boot_id: string;
    boot_id: string;
    constructor_runs: number;
    open_websockets: number;
  }> {
    const run = this.hibernationChain.then(async () => {
      while (this.activeHandlers.size > 0) {
        await Promise.allSettled([...this.activeHandlers]);
      }
      const prevBootId = this.engine.bootId;
      this.engine.dispose();
      this.engine = this.instantiateEngine();
      this.scheduleNodeAlarmTimer();
      return {
        prev_boot_id: prevBootId,
        boot_id: this.engine.bootId,
        constructor_runs: this.engine.constructorRuns,
        open_websockets: this.openWebSocketCount,
      };
    });
    this.hibernationChain = run.catch(() => {});
    return await run;
  }

  async advanceClockAndFireAlarms(deltaMs: number): Promise<{
    now_ms: number;
    alarms_fired: number;
    boot_id: string;
    constructor_runs: number;
  }> {
    const step = Math.max(0, Math.trunc(deltaMs));
    const startNow = this.nowMs();
    const targetMs = startNow + step;
    let alarmsFired = 0;

    while (
      this.scheduledAlarmMs !== null &&
      this.scheduledAlarmMs <= targetMs &&
      alarmsFired < 100
    ) {
      const due = this.scheduledAlarmMs;
      const cur = this.nowMs();
      if (due > cur) {
        this.clockOffsetMs += due - cur;
      }
      this.scheduledAlarmMs = null;
      this.clearNodeAlarmTimer();
      alarmsFired += 1;
      await this.trackHandler(async () => {
        await this.engine.alarm();
      });
    }

    const curAfter = this.nowMs();
    if (targetMs > curAfter) {
      this.clockOffsetMs += targetMs - curAfter;
    }
    this.scheduleNodeAlarmTimer();

    return {
      now_ms: this.nowMs(),
      alarms_fired: alarmsFired,
      boot_id: this.engine.bootId,
      constructor_runs: this.engine.constructorRuns,
    };
  }

  async stop(): Promise<void> {
    this.stopped = true;
    this.clearNodeAlarmTimer();
    this.engine.dispose();
    for (const ws of this.acceptedSockets) {
      ws.forceDestroyTcp();
    }
    this.acceptedSockets.clear();
    while (this.activeHandlers.size > 0) {
      await Promise.allSettled([...this.activeHandlers]);
    }
  }
}

// ---------------------------------------------------------------------------
// Public Server API & CLI Entrypoint
// ---------------------------------------------------------------------------

export interface NodeNativeGatewayOptions {
  dbPath?: string;
  doId?: string;
  host?: string;
  port?: number;
  env?: NativeEnvConfig;
  overrides?: NativeRuntimeOverrides;
  enableTestHooks?: boolean;
}

export interface NodeNativeGatewayHandle {
  server: Server;
  httpUrl: string;
  wsUrl: string;
  port: number;
  db: DatabaseSync;
  storage: NodeNativeSqliteStorage;
  r2: InMemoryNativeR2Bucket;
  simulateHibernation: () => Promise<{
    prev_boot_id: string;
    boot_id: string;
    constructor_runs: number;
    open_websockets: number;
  }>;
  advanceClock: (deltaMs: number) => Promise<{
    now_ms: number;
    alarms_fired: number;
    boot_id: string;
    constructor_runs: number;
  }>;
  getBootStats: () => {
    boot_id: string;
    constructor_runs: number;
    open_websockets: number;
  };
  close: () => Promise<void>;
}

async function readNodeRequestBody(req: IncomingMessage): Promise<Buffer> {
  const chunks: Buffer[] = [];
  for await (const chunk of req) {
    chunks.push(typeof chunk === "string" ? Buffer.from(chunk) : chunk);
  }
  return Buffer.concat(chunks);
}

function buildWebHeaders(req: IncomingMessage): Headers {
  const headers = new Headers();
  for (const [key, value] of Object.entries(req.headers)) {
    if (typeof value === "string") {
      headers.set(key, value);
    } else if (Array.isArray(value)) {
      headers.set(key, value.join(", "));
    }
  }
  return headers;
}

async function writeNodeResponse(
  res: ServerResponse,
  webRes: Response,
): Promise<void> {
  res.statusCode = webRes.status;
  webRes.headers.forEach((value, key) => {
    res.setHeader(key, value);
  });
  const body = Buffer.from(await webRes.arrayBuffer());
  res.end(body);
}

async function writeRawHttpOnSocket(
  socket: Duplex,
  webRes: Response,
): Promise<void> {
  const body = Buffer.from(await webRes.arrayBuffer());
  const statusText = STATUS_CODES[webRes.status] ?? "Error";
  const lines = [
    `HTTP/1.1 ${webRes.status} ${statusText}`,
    "Connection: close",
    `Content-Length: ${body.length}`,
  ];
  webRes.headers.forEach((value, key) => {
    const lower = key.toLowerCase();
    if (lower !== "connection" && lower !== "content-length" && lower !== "transfer-encoding") {
      lines.push(`${key}: ${value}`);
    }
  });
  lines.push("", "");
  const headBuf = Buffer.from(lines.join("\r\n"), "utf8");
  if (!socket.destroyed && socket.writable) {
    socket.end(body.length > 0 ? Buffer.concat([headBuf, body]) : headBuf);
  }
}

function writeRawBadRequestOnSocket(socket: Duplex, message: string): void {
  const body = Buffer.from(message, "utf8");
  const raw =
    `HTTP/1.1 400 Bad Request\r\n` +
    `Connection: close\r\n` +
    `Content-Type: text/plain; charset=utf-8\r\n` +
    `Content-Length: ${body.length}\r\n\r\n`;
  if (!socket.destroyed && socket.writable) {
    socket.end(Buffer.concat([Buffer.from(raw, "utf8"), body]));
  }
}

export async function startNodeNativeGatewayServer(
  options: NodeNativeGatewayOptions = {},
): Promise<NodeNativeGatewayHandle> {
  ensureNodeTsHooks();
  const { NativeEngine: EngineClass } = await import("./native/engine");

  const dbPath = options.dbPath ?? ":memory:";
  if (dbPath !== ":memory:") {
    mkdirSync(dirname(dbPath), { recursive: true });
  }
  const db = new DatabaseSync(dbPath);
  const storage = new NodeNativeSqliteStorage(db);

  let controllerRef: NativeGatewayController | null = null;
  const r2 = new InMemoryNativeR2Bucket(() =>
    controllerRef ? controllerRef.nowMs() : Date.now(),
  );

  const env: NativeEnvConfig = {
    ANYSERVE_CLIENT_TOKENS: process.env.ANYSERVE_CLIENT_TOKENS,
    ANYSERVE_WORKER_TOKENS: process.env.ANYSERVE_WORKER_TOKENS,
    ANYSERVE_SCOPED_TOKENS: process.env.ANYSERVE_SCOPED_TOKENS,
    ANYSERVE_TOKEN_SIGNING_KEY: process.env.ANYSERVE_TOKEN_SIGNING_KEY,
    ANYSERVE_AUTH_REVOCATION_URL: process.env.ANYSERVE_AUTH_REVOCATION_URL,
    ANYSERVE_AUTH_REVOCATION_POLL_INTERVAL_MS:
      process.env.ANYSERVE_AUTH_REVOCATION_POLL_INTERVAL_MS,
    ANYSERVE_CONSOLE_AUTH_PORTAL_URL:
      process.env.ANYSERVE_CONSOLE_AUTH_PORTAL_URL,
    ANYSERVE_MEMORY_LIMIT_MB: process.env.ANYSERVE_MEMORY_LIMIT_MB,
    ANYSERVE_PUBLIC_ENDPOINT: process.env.ANYSERVE_PUBLIC_ENDPOINT,
    ANYSERVE_CONSOLE_POLL_MS: process.env.ANYSERVE_CONSOLE_POLL_MS,
    ANYSERVE_ALLOWED_ORIGINS: process.env.ANYSERVE_ALLOWED_ORIGINS,
    ...(options.env ?? {}),
  };

  let envOverrides: NativeRuntimeOverrides = {};
  if (process.env.ANYSERVE_RUNTIME_OVERRIDES_JSON) {
    envOverrides = JSON.parse(
      process.env.ANYSERVE_RUNTIME_OVERRIDES_JSON,
    ) as NativeRuntimeOverrides;
  }

  const controller = new NativeGatewayController({
    doId: options.doId ?? process.env.ANYSERVE_DO_ID ?? "singleton-leader",
    storage,
    r2,
    env,
    overrides: { ...envOverrides, ...(options.overrides ?? {}) },
    EngineClass,
  });
  controllerRef = controller;

  const enableTestHooks =
    options.enableTestHooks ??
    (process.env.ANYSERVE_TEST_HOOKS === "1" ||
      process.env.ANYSERVE_TEST_HOOKS === "true");

  const host = options.host ?? "127.0.0.1";
  const requestedPort = options.port ?? 0;
  let actualPort = requestedPort;

  const activeHttpSockets = new Set<Socket>();

  const server = createServer(async (req, res) => {
    try {
      const reqUrl = new URL(
        req.url ?? "/",
        `http://${req.headers.host ?? `${host}:${actualPort}`}`,
      );
      const method = (req.method ?? "GET").toUpperCase();

      if (enableTestHooks && reqUrl.pathname.startsWith("/__test/")) {
        if (reqUrl.pathname === "/__test/hibernate" && method === "POST") {
          await readNodeRequestBody(req);
          const result = await controller.simulateHibernation();
          res.statusCode = 200;
          res.setHeader("content-type", "application/json");
          res.end(JSON.stringify(result));
          return;
        }
        if (reqUrl.pathname === "/__test/advance" && method === "POST") {
          const raw = await readNodeRequestBody(req);
          const parsed =
            raw.length > 0
              ? (JSON.parse(raw.toString("utf8")) as { ms?: number })
              : {};
          const result = await controller.advanceClockAndFireAlarms(
            Number(parsed.ms ?? 0),
          );
          res.statusCode = 200;
          res.setHeader("content-type", "application/json");
          res.end(JSON.stringify(result));
          return;
        }
        if (reqUrl.pathname === "/__test/stats" && method === "GET") {
          res.statusCode = 200;
          res.setHeader("content-type", "application/json");
          res.end(
            JSON.stringify({
              boot_id: controller.engine.bootId,
              constructor_runs: controller.engine.constructorRuns,
              open_websockets: controller.openWebSocketCount,
              r2_objects: r2.size,
              r2_put_count: r2.putCount,
              r2_get_count: r2.getCount,
              r2_range_get_count: r2.rangeGetCount,
              r2_delete_count: r2.deleteCount,
              sql_rows_written: storage.totalRowsWritten,
              sql_rows_read: storage.totalRowsRead,
            }),
          );
          return;
        }
      }

      const headers = buildWebHeaders(req);
      const body =
        method === "GET" || method === "HEAD"
          ? undefined
          : new Uint8Array(await readNodeRequestBody(req));
      const webReq = new Request(reqUrl.toString(), {
        method,
        headers,
        body,
      });
      const webRes = await controller.fetch(webReq);
      await writeNodeResponse(res, webRes);
    } catch (error) {
      res.statusCode = 500;
      res.setHeader("content-type", "application/json");
      res.end(
        JSON.stringify({
          error: error instanceof Error ? error.message : String(error),
        }),
      );
    }
  });

  server.on("connection", (sock: Socket) => {
    sock.setNoDelay(true);
    activeHttpSockets.add(sock);
    sock.on("close", () => {
      activeHttpSockets.delete(sock);
    });
  });

  server.on("upgrade", (req: IncomingMessage, socket: Duplex, head: Buffer) => {
    const earlyChunks: Buffer[] = [];
    if (head.length > 0) {
      earlyChunks.push(Buffer.from(head));
    }
    const onEarlyData = (chunk: Buffer) => {
      earlyChunks.push(Buffer.from(chunk));
    };
    const onEarlyError = () => {
      // Prevent unhandled socket error before upgrade settles.
    };
    socket.on("data", onEarlyData);
    socket.on("error", onEarlyError);

    void (async () => {
      try {
        const wsKeyHeader = req.headers["sec-websocket-key"];
        const wsVersionHeader = req.headers["sec-websocket-version"];
        const wsKey =
          typeof wsKeyHeader === "string" ? wsKeyHeader.trim() : "";
        const wsVersion =
          typeof wsVersionHeader === "string" ? wsVersionHeader.trim() : "";

        if (!wsKey || wsVersion !== "13") {
          socket.removeListener("data", onEarlyData);
          writeRawBadRequestOnSocket(
            socket,
            "Invalid Sec-WebSocket-Key or Sec-WebSocket-Version",
          );
          return;
        }

        const reqUrl = new URL(
          req.url ?? "/",
          `http://${req.headers.host ?? `${host}:${actualPort}`}`,
        );
        const headers = buildWebHeaders(req);
        headers.set("Upgrade", "websocket");
        headers.set("Connection", "Upgrade");

        const webReq = new Request(reqUrl.toString(), {
          method: req.method ?? "GET",
          headers,
        });

        const webRes = await controller.fetch(webReq);
        socket.removeListener("data", onEarlyData);

        if (!(webRes instanceof GatewayUpgradeResponse)) {
          // Pre-upgrade HTTP rejection (401/403/400/429 + x-anyserve-grpc-code)
          await writeRawHttpOnSocket(socket, webRes);
          socket.removeListener("error", onEarlyError);
          return;
        }

        if (socket.destroyed || !socket.writable) {
          socket.removeListener("error", onEarlyError);
          await controller.dispatchWsClose(
            webRes.serverWebSocket,
            1006,
            "client disconnected during upgrade",
            false,
          );
          return;
        }

        const acceptHash = createHash("sha1")
          .update(wsKey + WS_GUID)
          .digest("base64");

        const respLines = [
          "HTTP/1.1 101 Switching Protocols",
          "Upgrade: websocket",
          "Connection: Upgrade",
          `Sec-WebSocket-Accept: ${acceptHash}`,
        ];
        webRes.extraHeaders.forEach((value, key) => {
          const lower = key.toLowerCase();
          if (
            lower !== "upgrade" &&
            lower !== "connection" &&
            lower !== "sec-websocket-accept"
          ) {
            respLines.push(`${key}: ${value}`);
          }
        });
        respLines.push("", "");

        socket.write(respLines.join("\r\n"));
        socket.removeListener("error", onEarlyError);
        webRes.serverWebSocket.attachTcpSocket(socket, earlyChunks);
      } catch (err) {
        socket.removeListener("data", onEarlyData);
        socket.removeListener("error", onEarlyError);
        const msg = err instanceof Error ? err.message : String(err);
        writeRawBadRequestOnSocket(socket, msg);
      }
    })();
  });

  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(requestedPort, host, () => {
      server.removeListener("error", reject);
      resolve();
    });
  });

  const addr = server.address();
  if (!addr || typeof addr === "string") {
    throw new Error("Unexpected server address format");
  }
  actualPort = addr.port;
  const httpUrl = `http://${host}:${actualPort}`;
  const wsUrl = `ws://${host}:${actualPort}`;

  return {
    server,
    httpUrl,
    wsUrl,
    port: actualPort,
    db,
    storage,
    r2,
    simulateHibernation: () => controller.simulateHibernation(),
    advanceClock: (deltaMs: number) =>
      controller.advanceClockAndFireAlarms(deltaMs),
    getBootStats: () => ({
      boot_id: controller.engine.bootId,
      constructor_runs: controller.engine.constructorRuns,
      open_websockets: controller.openWebSocketCount,
    }),
    close: async () => {
      await controller.stop();
      for (const s of activeHttpSockets) {
        s.destroy();
      }
      activeHttpSockets.clear();
      await new Promise<void>((resolve) => {
        server.close(() => resolve());
      });
      db.close();
    },
  };
}

if (
  typeof process !== "undefined" &&
  process.argv[1] &&
  import.meta.url.endsWith(process.argv[1].replace(/\\/g, "/"))
) {
  const port = Number(process.env.PORT ?? "8789");
  const host = process.env.HOST ?? "127.0.0.1";
  const dbPath = process.env.ANYSERVE_DEV_SQLITE ?? ":memory:";
  const handle = await startNodeNativeGatewayServer({
    host,
    port,
    dbPath,
  });
  console.log(
    `ANYSERVE_NATIVE_GATEWAY_LISTENING ${handle.httpUrl} ${handle.wsUrl}`,
  );
  const shutdown = () => {
    void handle.close().finally(() => {
      process.exit(0);
    });
  };
  process.once("SIGTERM", shutdown);
  process.once("SIGINT", shutdown);
}
