// Pure Web-standard host interfaces, limits, attachment codec, and IsolateMemoryBudget.
// NO cloudflare:workers or node:* imports so this runs identically in workerd and Node 24.

export const WS_MAX_ENCODED_BYTES = 33_554_432; // 32 MiB exact
export const HTTP_MAX_BODY_BYTES = 67_108_864; // 64 MiB exact
export const MAX_INLINE_OBJECT_BYTES = 8_388_608; // 8 MiB
export const MAX_INLINE_TOTAL_BYTES = 25_165_824; // 24 MiB
export const MAX_METADATA_BYTES = 32_768; // 32 KiB
export const MAX_OBJECTS = 1_024;
export const MAX_IDENTIFIER_BYTES = 256;
export const MAX_INTERFACE_BYTES = 64;

export const WELCOME_MAX_CONTROL_BYTES = 262_144; // 256 KiB
export const WELCOME_MAX_CHUNK_BYTES = 65_536; // 64 KiB
export const WELCOME_ATTEMPT_WINDOW_BYTES = 4_194_304; // 4 MiB
export const WELCOME_ASSIGN_INLINE_LIMIT = 65_536; // 64 KiB

export const UPLOAD_SEGMENT_BYTES = 2_097_152; // 2 MiB
export const UPLOAD_WINDOW_SLACK_THRESHOLD = 1_048_576; // 1 MiB
export const INPUT_BATCH_MAX_BYTES = 1_048_576; // 1 MiB
export const INPUT_BATCH_MAX_CHUNKS = 16;

export const BUNDLE_INLINE_MAX_BYTES = 524_288; // 512 KiB
export const BUNDLE_MANIFEST_MAX_BYTES = 262_144; // 256 KiB
export const R2_PACK_MAX_BYTES = 4_194_304; // 4 MiB
export const PREVIEW_MAX_BYTES = 4_096; // 4 KiB

export const ISOLATE_MEMORY_CAP_BYTES = 83_886_080; // 80 MiB
export const LARGE_SEND_CAP_BYTES = 58_720_256; // 56 MiB
export const LARGE_SEND_THRESHOLD_BYTES = 1_048_576; // 1 MiB
export const R2_READ_BUFFER_BYTES = 2_097_152; // 2 MiB
export const PER_JOB_EVENT_CAP_BYTES = 8_388_608; // 8 MiB
export const EVENT_OVERHEAD_BYTES = 96;

export const ATTACHMENT_MAX_BYTES = 256;
export const LOAD_CUSTOM_MAX_ENTRIES = 64;
export const LOAD_CUSTOM_MAX_BYTES = 8_192;

export const MAX_SESSION_SOCKETS = 256;
export const MAX_ATTEMPT_SOCKETS = 256;
export const MAX_INVOKE_SOCKETS = 128;
export const MAX_WATCH_SOCKETS = 256;
export const MAX_PENDING_HANDSHAKE_SOCKETS = 64;

export const DEFAULT_HEARTBEAT_INTERVAL_MS = 30_000;
export const DEFAULT_HEARTBEAT_TIMEOUT_MS = 90_000;
export const DEFAULT_RECONNECT_GRACE_MS = 30_000;
export const DEFAULT_CANCEL_GRACE_MS = 15_000;
export const DEFAULT_RECEIPT_RETENTION_MS = 600_000;
export const DEFAULT_TERMINAL_RETENTION_MS = 86_400_000;
export const DEFAULT_INITIAL_BACKOFF_MS = 200;
export const DEFAULT_MAX_BACKOFF_MS = 10_000;
export const DEFAULT_CONSUMER_STALL_TIMEOUT_MS = 30_000;
export const MAINTENANCE_BUCKET_MS = 5_000;
export const DEFAULT_R2_STAGING_TTL_MS = 300_000;
export const DEFAULT_R2_RECONCILE_SAFETY_AGE_MS = 600_000;

export const QUOTA_SOFT_ROW_WRITES_PER_DAY = 90_000;
export const QUOTA_HARD_ROW_WRITES_PER_DAY = 100_000;
export const QUOTA_SOFT_DO_REQUESTS_PER_DAY = 90_000;
export const QUOTA_HARD_DO_REQUESTS_PER_DAY = 100_000;
export const QUOTA_SOFT_ROWS_READ_PER_DAY = 4_500_000;
export const QUOTA_HARD_ROWS_READ_PER_DAY = 5_000_000;
export const QUOTA_MAX_DB_PLUS_RESERVED_BYTES = 850 * 1024 * 1024; // 850 MiB
export const QUOTA_SOFT_R2_CLASS_A_PER_MONTH = 900_000;
export const QUOTA_SOFT_R2_CLASS_B_PER_MONTH = 9_000_000;
export const QUOTA_SOFT_LARGE_JOBS_PER_DAY = 500;

export const WS_READY_STATE_CONNECTING = 0;
export const WS_READY_STATE_OPEN = 1;
export const WS_READY_STATE_CLOSING = 2;
export const WS_READY_STATE_CLOSED = 3;

export type SocketKind = "session" | "attempt" | "invoke" | "watch";

export interface SocketAttachment {
  kind: SocketKind;
  connId: number;
  id: string;
  namespace: string;
  forcedPool: string | null;
  handshakeDone: 0 | 1;
  openedAtMs: number;
  lastSeenAtMs: number;
  queued: number;
  running: number;
  utilization: number;
  replay: 0 | 1;
}

interface CompactAttachment {
  k: "s" | "a" | "i" | "w";
  c: number;
  id: string;
  ns: string;
  fp?: string;
  h: 0 | 1;
  oa: number;
  ls: number;
  q?: number;
  r?: number;
  u?: number;
  rp?: 1;
}

import { utf8ByteLen } from "./utf8";

const utf8Encoder = new TextEncoder();

export function isValidInterfaceName(name: string): boolean {
  if (name.length === 0 || utf8ByteLen(name) > MAX_INTERFACE_BYTES) {
    return false;
  }
  for (let i = 0; i < name.length; i++) {
    const c = name.charCodeAt(i);
    if (c <= 0x20 || c === 0x7f) {
      return false;
    }
  }
  return true;
}

export function encodeCompactAttachment(att: SocketAttachment): CompactAttachment {
  const kindMap: Record<SocketKind, CompactAttachment["k"]> = {
    session: "s",
    attempt: "a",
    invoke: "i",
    watch: "w",
  };
  const out: CompactAttachment = {
    k: kindMap[att.kind],
    c: att.connId,
    id: att.id,
    ns: att.namespace,
    h: att.handshakeDone,
    oa: att.openedAtMs,
    ls: att.lastSeenAtMs,
  };
  if (att.forcedPool !== null && att.forcedPool.length > 0) {
    out.fp = att.forcedPool;
  }
  if (att.queued !== 0) {
    out.q = att.queued;
  }
  if (att.running !== 0) {
    out.r = att.running;
  }
  if (att.utilization !== 0) {
    out.u = Number(att.utilization.toFixed(6));
  }
  if (att.replay === 1) {
    out.rp = 1;
  }
  return out;
}

export function decodeCompactAttachment(raw: unknown): SocketAttachment | null {
  if (!raw || typeof raw !== "object") {
    return null;
  }
  const obj = raw as Partial<CompactAttachment>;
  const kindRev: Record<string, SocketKind> = {
    s: "session",
    a: "attempt",
    i: "invoke",
    w: "watch",
  };
  const kind = typeof obj.k === "string" ? kindRev[obj.k] : undefined;
  if (
    !kind ||
    typeof obj.c !== "number" ||
    typeof obj.id !== "string" ||
    typeof obj.ns !== "string"
  ) {
    return null;
  }
  return {
    kind,
    connId: obj.c,
    id: obj.id,
    namespace: obj.ns,
    forcedPool: typeof obj.fp === "string" ? obj.fp : null,
    handshakeDone: obj.h === 1 ? 1 : 0,
    openedAtMs: typeof obj.oa === "number" ? obj.oa : 0,
    lastSeenAtMs: typeof obj.ls === "number" ? obj.ls : 0,
    queued: typeof obj.q === "number" ? obj.q : 0,
    running: typeof obj.r === "number" ? obj.r : 0,
    utilization: typeof obj.u === "number" ? obj.u : 0,
    replay: obj.rp === 1 ? 1 : 0,
  };
}

export function attachmentByteLength(att: SocketAttachment): number {
  return utf8ByteLen(JSON.stringify(encodeCompactAttachment(att)));
}

/**
 * Per-socket-object cache of the decoded attachment. Updated by `writeSocketAttachment` and filled
 * on the first `readSocketAttachment` miss (e.g. a wrapper object recreated after hibernation), so
 * hot paths never repeat `deserializeAttachment()` + `JSON.parse`. Cached values are frozen and
 * always equal `decodeCompactAttachment(encodeCompactAttachment(att))`.
 */
const attachmentCache = new WeakMap<object, SocketAttachment>();

export function writeSocketAttachment(ws: NativeWebSocket, att: SocketAttachment): void {
  const compact = encodeCompactAttachment(att);
  const bytes = utf8ByteLen(JSON.stringify(compact));
  if (bytes > ATTACHMENT_MAX_BYTES) {
    throw new Error(
      `WebSocket attachment exceeds ${ATTACHMENT_MAX_BYTES} bytes (${bytes} bytes)`,
    );
  }
  ws.serializeAttachment(compact);
  const canonical = decodeCompactAttachment(compact);
  if (canonical) {
    attachmentCache.set(ws, Object.freeze(canonical));
  } else {
    attachmentCache.delete(ws);
  }
}

export function readSocketAttachment(ws: NativeWebSocket): SocketAttachment | null {
  const cached = attachmentCache.get(ws);
  if (cached) {
    return cached;
  }
  const decoded = decodeCompactAttachment(ws.deserializeAttachment());
  if (decoded) {
    attachmentCache.set(ws, Object.freeze(decoded));
  }
  return decoded;
}

export function boundCustomLoad(
  raw: ReadonlyMap<string, number> | Record<string, number> | undefined | null,
): { custom: Record<string, number>; dropped: number } {
  if (!raw) {
    return { custom: {}, dropped: 0 };
  }
  const entries: Array<[string, number]> =
    raw instanceof Map ? [...raw.entries()] : Object.entries(raw);
  if (entries.length === 0) {
    return { custom: {}, dropped: 0 };
  }
  // Sort deterministically by UTF-8 byte order so retained subset is reproducible.
  entries.sort(([a], [b]) => compareUtf8Strings(a, b));
  const custom: Record<string, number> = {};
  let kept = 0;
  let usedBytes = 0;
  let dropped = 0;
  for (const [key, val] of entries) {
    if (!Number.isFinite(val)) {
      dropped += 1;
      continue;
    }
    const entryBytes = utf8ByteLen(key) + 8;
    if (kept >= LOAD_CUSTOM_MAX_ENTRIES || usedBytes + entryBytes > LOAD_CUSTOM_MAX_BYTES) {
      dropped += 1;
      continue;
    }
    custom[key] = val;
    kept += 1;
    usedBytes += entryBytes;
  }
  return { custom, dropped };
}

export function compareUtf8Strings(a: string, b: string): number {
  if (a === b) {
    return 0;
  }
  const ba = utf8Encoder.encode(a);
  const bb = utf8Encoder.encode(b);
  const min = Math.min(ba.byteLength, bb.byteLength);
  for (let i = 0; i < min; i++) {
    const diff = ba[i]! - bb[i]!;
    if (diff !== 0) {
      return diff;
    }
  }
  return ba.byteLength - bb.byteLength;
}

export interface NativeSqlCursor<T extends Record<string, any> = Record<string, any>> {
  toArray(): T[];
  readonly rowsRead?: number;
  readonly rowsWritten?: number;
  [Symbol.iterator]?(): IterableIterator<T>;
}

export interface NativeSqlStorage {
  exec<T extends Record<string, any> = Record<string, any>>(
    query: string,
    ...bindings: unknown[]
  ): NativeSqlCursor<T>;
  readonly databaseSize?: number;
}

export interface NativeDurableStorage {
  readonly sql: NativeSqlStorage;
  transactionSync<T>(closure: () => T): T;
}

export type NativeR2Range =
  | { offset: number; length?: number }
  | { offset?: number; length: number };

export interface NativeR2ObjectBody {
  arrayBuffer(): Promise<ArrayBuffer>;
}

export interface NativeR2ListedObject {
  key: string;
  size: number;
  uploaded: Date;
}

export interface NativeR2ListResult {
  objects: NativeR2ListedObject[];
  truncated: boolean;
  cursor?: string;
}

export interface NativeR2Bucket {
  get(key: string, options?: { range?: NativeR2Range }): Promise<NativeR2ObjectBody | null>;
  put(
    key: string,
    value: ReadableStream | ArrayBuffer | ArrayBufferView | string | Blob | null,
  ): Promise<unknown>;
  delete(keys: string | string[]): Promise<void>;
  list?(options?: {
    prefix?: string;
    cursor?: string;
    limit?: number;
  }): Promise<NativeR2ListResult>;
}

export interface NativeWebSocket {
  send(message: ArrayBuffer | ArrayBufferView | string): void;
  close(code?: number, reason?: string): void;
  serializeAttachment(attachment: unknown): void;
  deserializeAttachment(): unknown;
  readonly readyState: number;
  readonly bufferedAmount: number;
}

export interface NativeWebSocketPairResult {
  client: unknown;
  server: NativeWebSocket;
  toUpgradeResponse(headers?: HeadersInit): Response;
}

export interface PoolTierConfig {
  pool: string;
  spilloverAfterMs: number;
  reconnectGraceMs: number | null;
}

export interface InterfacePolicyConfig {
  maxQueue: number | null;
  queueTimeoutMs: number | null;
  maxInFlightPerKey: number | null;
  poolTiers: PoolTierConfig[];
}

export interface NativeRuntimeOverrides {
  heartbeatIntervalMs?: number;
  heartbeatTimeoutMs?: number;
  reconnectGraceMs?: number | null;
  cancelGraceMs?: number;
  receiptRetentionMs?: number;
  terminalRetentionMs?: number;
  defaultInitialBackoffMs?: number;
  defaultMaxBackoffMs?: number;
  consumerStallTimeoutMs?: number;
  maintenanceBucketMs?: number;
  priorityLevels?: number;
  defaultMaxAttempts?: number;
  maxAttemptsLimit?: number;
  defaultMaxQueue?: number;
  defaultMaxInFlightPerKey?: number | null;
  maxPendingJobs?: number;
  maxTerminalJobs?: number;
  maxPendingBytes?: number;
  maxResultBytes?: number;
  maxEventBytes?: number;
  perJobEventCapBytes?: number;
  eventLogMaxEvents?: number;
  maxInlineObjectBytes?: number;
  maxInlineTotalBytes?: number;
  maxControlMessageBytes?: number;
  maxChunkBytes?: number;
  attemptWindowBytes?: number;
  assignInlineLimit?: number;
  maxSessionSockets?: number;
  maxAttemptSockets?: number;
  maxInvokeSockets?: number;
  maxWatchSockets?: number;
  maxPendingHandshakeSockets?: number;
  r2StagingTtlMs?: number;
  r2ReconcileSafetyAgeMs?: number;
  isolateMemoryCapBytes?: number;
  largeSendCapBytes?: number;
  largeSendThresholdBytes?: number;
  quotaSoftRowWritesPerDay?: number;
  quotaHardRowWritesPerDay?: number;
  quotaSoftDoRequestsPerDay?: number;
  quotaHardDoRequestsPerDay?: number;
  quotaSoftRowsReadPerDay?: number;
  quotaHardRowsReadPerDay?: number;
  quotaMaxDbPlusReservedBytes?: number;
  quotaSoftR2ClassAPerMonth?: number;
  quotaSoftR2ClassBPerMonth?: number;
  quotaSoftLargeJobsPerDay?: number;
  interfaces?: Record<string, InterfacePolicyConfig>;
}

export interface NativeEnvConfig {
  ANYSERVE_CLIENT_TOKENS?: string;
  ANYSERVE_WORKER_TOKENS?: string;
  ANYSERVE_SCOPED_TOKENS?: string;
  ANYSERVE_TOKEN_SIGNING_KEY?: string;
  ANYSERVE_AUTH_REVOCATION_URL?: string;
  ANYSERVE_AUTH_REVOCATION_POLL_INTERVAL_MS?: string;
  ANYSERVE_CONSOLE_AUTH_PORTAL_URL?: string;
  ANYSERVE_MEMORY_LIMIT_MB?: string;
  ANYSERVE_PUBLIC_ENDPOINT?: string;
  ANYSERVE_CONSOLE_POLL_MS?: string;
  ANYSERVE_ALLOWED_ORIGINS?: string;
  ANYSERVE_RUNTIME_OVERRIDES_JSON?: string;
}

export interface NativeHost {
  readonly doId: string;
  readonly storage: NativeDurableStorage;
  readonly r2: NativeR2Bucket | null;
  readonly env: NativeEnvConfig;
  readonly overrides?: NativeRuntimeOverrides;
  acceptWebSocket(ws: NativeWebSocket, tags?: string[]): void;
  getWebSockets(tag?: string): NativeWebSocket[];
  createWebSocketPair(): NativeWebSocketPairResult;
  nowMs(): number;
  setAlarm(scheduledTimeMs: number): Promise<void>;
  getAlarm(): Promise<number | null>;
  deleteAlarm?(): Promise<void>;
  nextConstructorRun?(): number;
}

export interface OutboundSendReservation {
  completeSend(): void;
  abort(): void;
}

interface Waiter {
  totalNeeded: number;
  largeNeeded: number;
  isLarge: boolean;
  resolve: () => void;
  reject: (err: Error) => void;
}

interface SocketQueueReservation {
  reservedTotal: number;
  reservedLarge: number;
}

/**
 * Isolate-wide memory budget (section 5.2):
 * - 80 MiB total payload cap (`isolateCapBytes`)
 * - 56 MiB large-send cap (`largeSendCapBytes`) for outbound messages with L > 1 MiB
 * - Reserves `2 * L + r2ReadBuffer` BEFORE allocating JS buffer
 * - On `ws.send()`, drops the JS buffer (`L`) + R2 buffer, keeping `max(ws.bufferedAmount, L)`
 *   until `ws.bufferedAmount` drains or the socket closes.
 */
export class IsolateMemoryBudget {
  readonly isolateCapBytes: number;
  readonly largeSendCapBytes: number;
  readonly largeSendThresholdBytes: number;

  private inFlightTotal = 0;
  private inFlightLarge = 0;
  private readonly socketQueues = new Map<NativeWebSocket, SocketQueueReservation>();
  private readonly waiters: Waiter[] = [];

  peakIsolateBytes = 0;
  peakLargeSendBytes = 0;
  peakBufferedAmount = 0;
  largeSendWaitCount = 0;
  waitCount = 0;

  constructor(options?: {
    isolateCapBytes?: number;
    largeSendCapBytes?: number;
    largeSendThresholdBytes?: number;
  }) {
    this.isolateCapBytes = options?.isolateCapBytes ?? ISOLATE_MEMORY_CAP_BYTES;
    this.largeSendCapBytes = options?.largeSendCapBytes ?? LARGE_SEND_CAP_BYTES;
    this.largeSendThresholdBytes =
      options?.largeSendThresholdBytes ?? LARGE_SEND_THRESHOLD_BYTES;
  }

  get currentIsolateBytes(): number {
    this.refreshSockets();
    return this.inFlightTotal;
  }

  get currentLargeSendBytes(): number {
    this.refreshSockets();
    return this.inFlightLarge;
  }

  get pendingWaiters(): number {
    return this.waiters.length;
  }

  private recordPeaks(): void {
    if (this.inFlightTotal > this.peakIsolateBytes) {
      this.peakIsolateBytes = this.inFlightTotal;
    }
    if (this.inFlightLarge > this.peakLargeSendBytes) {
      this.peakLargeSendBytes = this.inFlightLarge;
    }
  }

  /**
   * Reconciles per-socket native send-queue reservations with current `ws.bufferedAmount`.
   * Call whenever sockets drain, close, or before reserving new memory.
   */
  refreshSockets(): void {
    let freedAny = false;
    for (const [ws, entry] of this.socketQueues) {
      if (ws.readyState !== WS_READY_STATE_OPEN) {
        this.inFlightTotal = Math.max(0, this.inFlightTotal - entry.reservedTotal);
        this.inFlightLarge = Math.max(0, this.inFlightLarge - entry.reservedLarge);
        this.socketQueues.delete(ws);
        freedAny = true;
        continue;
      }
      const buffered = Math.max(0, ws.bufferedAmount);
      if (buffered > this.peakBufferedAmount) {
        this.peakBufferedAmount = buffered;
      }
      if (buffered === 0) {
        this.inFlightTotal = Math.max(0, this.inFlightTotal - entry.reservedTotal);
        this.inFlightLarge = Math.max(0, this.inFlightLarge - entry.reservedLarge);
        this.socketQueues.delete(ws);
        freedAny = true;
      } else if (buffered < entry.reservedTotal) {
        const deltaTotal = entry.reservedTotal - buffered;
        this.inFlightTotal = Math.max(0, this.inFlightTotal - deltaTotal);
        entry.reservedTotal = buffered;
        if (entry.reservedLarge > buffered) {
          const deltaLarge = entry.reservedLarge - buffered;
          this.inFlightLarge = Math.max(0, this.inFlightLarge - deltaLarge);
          entry.reservedLarge = buffered;
        }
        freedAny = true;
      }
    }
    if (freedAny) {
      this.wakeWaiters();
    }
  }

  releaseSocket(ws: NativeWebSocket): void {
    const entry = this.socketQueues.get(ws);
    if (!entry) {
      return;
    }
    this.inFlightTotal = Math.max(0, this.inFlightTotal - entry.reservedTotal);
    this.inFlightLarge = Math.max(0, this.inFlightLarge - entry.reservedLarge);
    this.socketQueues.delete(ws);
    this.wakeWaiters();
  }

  private canFit(totalNeeded: number, largeNeeded: number, isLarge: boolean): boolean {
    if (this.inFlightTotal + totalNeeded > this.isolateCapBytes) {
      return false;
    }
    if (isLarge && this.inFlightLarge + largeNeeded > this.largeSendCapBytes) {
      return false;
    }
    return true;
  }

  private wakeWaiters(): void {
    while (this.waiters.length > 0) {
      const head = this.waiters[0]!;
      if (!this.canFit(head.totalNeeded, head.largeNeeded, head.isLarge)) {
        break;
      }
      this.waiters.shift();
      this.inFlightTotal += head.totalNeeded;
      this.inFlightLarge += head.largeNeeded;
      this.recordPeaks();
      head.resolve();
    }
  }

  reserveBufferSync(bytes: number): { release(): void } | null {
    this.refreshSockets();
    if (bytes <= 0) {
      return { release: () => {} };
    }
    if (this.inFlightTotal + bytes > this.isolateCapBytes) {
      return null;
    }
    this.inFlightTotal += bytes;
    this.recordPeaks();
    let released = false;
    return {
      release: () => {
        if (released) {
          return;
        }
        released = true;
        this.inFlightTotal = Math.max(0, this.inFlightTotal - bytes);
        this.wakeWaiters();
      },
    };
  }

  tryAcquireOutboundSync(
    ws: NativeWebSocket,
    encodedLen: number,
  ): OutboundSendReservation | null {
    this.refreshSockets();
    const isLarge = encodedLen > this.largeSendThresholdBytes;
    const totalNeeded = 2 * encodedLen;
    const largeNeeded = isLarge ? 2 * encodedLen : 0;
    if (!this.canFit(totalNeeded, largeNeeded, isLarge)) {
      return null;
    }
    this.inFlightTotal += totalNeeded;
    this.inFlightLarge += largeNeeded;
    this.recordPeaks();
    return this.buildHandle(ws, encodedLen, 0, totalNeeded, largeNeeded, isLarge);
  }

  async acquireOutboundSend(
    ws: NativeWebSocket,
    encodedLen: number,
    r2ReadBufferBytes = 0,
  ): Promise<OutboundSendReservation> {
    this.refreshSockets();
    const isLarge = encodedLen > this.largeSendThresholdBytes;
    const totalNeeded = 2 * encodedLen + r2ReadBufferBytes;
    const largeNeeded = isLarge ? 2 * encodedLen : 0;
    if (
      totalNeeded > this.isolateCapBytes ||
      (isLarge && largeNeeded > this.largeSendCapBytes)
    ) {
      throw new Error(
        `Outbound frame of ${encodedLen} bytes exceeds isolate memory budget`,
      );
    }

    if (this.waiters.length === 0 && this.canFit(totalNeeded, largeNeeded, isLarge)) {
      this.inFlightTotal += totalNeeded;
      this.inFlightLarge += largeNeeded;
      this.recordPeaks();
      return this.buildHandle(
        ws,
        encodedLen,
        r2ReadBufferBytes,
        totalNeeded,
        largeNeeded,
        isLarge,
      );
    }

    this.waitCount += 1;
    if (isLarge && this.inFlightLarge + largeNeeded > this.largeSendCapBytes) {
      this.largeSendWaitCount += 1;
    }
    await new Promise<void>((resolve, reject) => {
      this.waiters.push({
        totalNeeded,
        largeNeeded,
        isLarge,
        resolve,
        reject,
      });
    });
    return this.buildHandle(
      ws,
      encodedLen,
      r2ReadBufferBytes,
      totalNeeded,
      largeNeeded,
      isLarge,
    );
  }

  private buildHandle(
    ws: NativeWebSocket,
    encodedLen: number,
    r2ReadBufferBytes: number,
    totalReserved: number,
    largeReserved: number,
    isLarge: boolean,
  ): OutboundSendReservation {
    let settled = false;
    return {
      completeSend: () => {
        if (settled) {
          return;
        }
        settled = true;
        const buffered = Math.max(0, ws.bufferedAmount);
        if (buffered > this.peakBufferedAmount) {
          this.peakBufferedAmount = buffered;
        }
        // Release the JS buffer (L) and R2 read buffer; keep max(bufferedAmount, L) while open & queued.
        this.inFlightTotal = Math.max(0, this.inFlightTotal - totalReserved);
        this.inFlightLarge = Math.max(0, this.inFlightLarge - largeReserved);

        if (ws.readyState === WS_READY_STATE_OPEN && buffered > 0) {
          const prev = this.socketQueues.get(ws);
          if (prev) {
            this.inFlightTotal = Math.max(0, this.inFlightTotal - prev.reservedTotal);
            this.inFlightLarge = Math.max(0, this.inFlightLarge - prev.reservedLarge);
          }
          const keepTotal = Math.max(buffered, encodedLen);
          const keepLarge = isLarge
            ? Math.max(prev?.reservedLarge ?? 0, Math.min(keepTotal, encodedLen))
            : (prev?.reservedLarge ?? 0);
          this.socketQueues.set(ws, {
            reservedTotal: keepTotal,
            reservedLarge: keepLarge,
          });
          this.inFlightTotal += keepTotal;
          this.inFlightLarge += keepLarge;
          this.recordPeaks();
        }
        this.wakeWaiters();
      },
      abort: () => {
        if (settled) {
          return;
        }
        settled = true;
        this.inFlightTotal = Math.max(0, this.inFlightTotal - totalReserved);
        this.inFlightLarge = Math.max(0, this.inFlightLarge - largeReserved);
        this.wakeWaiters();
      },
    };
  }
}
