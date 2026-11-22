// Native Durable Object Execution Engine (sections 4, 5, 6, 7, 8, 9 of docs/cloudflare-native-do-plan.md v4).
// Mirrors crates/anyserve-core/src/runtime/* and http_ws_service.rs without cloudflare:workers or node:* imports.

import {
  AuthPolicy,
  GrpcCode,
  GrpcStatusError,
  grpcErrorResponse,
  validateNamespace,
  type Principal,
} from "./auth";
import {
  buildJobSummary,
  formatAnalyticsNextCursor,
  handleConsoleOrHealthRequest,
  type AdminResetResult,
  type ConsoleAnalyticsResult,
  type ConsoleAnalyticsScan,
  type ConsoleBackend,
  type ConsoleEventItem,
  type ConsoleHistogram,
  type ConsoleHistogramBucket,
  type ConsoleJobDetailResult,
  type ConsoleJobSummaryData,
  type ConsoleJobsQuery,
  type ConsoleJobsQueryResult,
  type ConsoleOverviewSnapshot,
  type ConsoleWorkerData,
} from "./console-api";
import {
  BUNDLE_INLINE_MAX_BYTES,
  DEFAULT_CANCEL_GRACE_MS,
  DEFAULT_CONSUMER_STALL_TIMEOUT_MS,
  DEFAULT_HEARTBEAT_INTERVAL_MS,
  DEFAULT_HEARTBEAT_TIMEOUT_MS,
  DEFAULT_INITIAL_BACKOFF_MS,
  DEFAULT_MAX_BACKOFF_MS,
  DEFAULT_RECEIPT_RETENTION_MS,
  DEFAULT_RECONNECT_GRACE_MS,
  DEFAULT_TERMINAL_RETENTION_MS,
  EVENT_OVERHEAD_BYTES,
  HTTP_MAX_BODY_BYTES,
  INPUT_BATCH_MAX_BYTES,
  INPUT_BATCH_MAX_CHUNKS,
  IsolateMemoryBudget,
  MAINTENANCE_BUCKET_MS,
  MAX_ATTEMPT_SOCKETS,
  MAX_IDENTIFIER_BYTES,
  MAX_INLINE_OBJECT_BYTES,
  MAX_INLINE_TOTAL_BYTES,
  MAX_INVOKE_SOCKETS,
  MAX_METADATA_BYTES,
  MAX_OBJECTS,
  MAX_PENDING_HANDSHAKE_SOCKETS,
  MAX_SESSION_SOCKETS,
  MAX_WATCH_SOCKETS,
  PER_JOB_EVENT_CAP_BYTES,
  PREVIEW_MAX_BYTES,
  R2_READ_BUFFER_BYTES,
  UPLOAD_SEGMENT_BYTES,
  UPLOAD_WINDOW_SLACK_THRESHOLD,
  WELCOME_ASSIGN_INLINE_LIMIT,
  WELCOME_ATTEMPT_WINDOW_BYTES,
  WELCOME_MAX_CHUNK_BYTES,
  WELCOME_MAX_CONTROL_BYTES,
  WS_MAX_ENCODED_BYTES,
  WS_READY_STATE_OPEN,
  boundCustomLoad,
  readSocketAttachment,
  writeSocketAttachment,
  type InterfacePolicyConfig,
  type NativeEnvConfig,
  type NativeHost,
  type NativeWebSocket,
  type PoolTierConfig,
  type SocketAttachment,
  type SocketKind,
} from "./host";
import {
  DEFAULT_NAMESPACE,
  IncrementalSha256,
  OBJECT_OVERHEAD,
  OBJECT_SHA256_METADATA_KEY,
  OBJECT_SIZE_METADATA_KEY,
  OUTCOME_DIGEST_DOMAIN,
  decodeScopedId,
  encodeScopedId,
  outcomeDigest,
  outcomeDigestStream,
  resultObjectAccountedBytes,
  sortMapByUtf8Bytes,
  specHash,
  type CanonicalObjectInput,
  type StreamingCanonicalObjectInput,
} from "./identity";
import * as pb from "./proto";
import {
  NativeStore,
  concatBytes,
  extractR2KeysFromManifest,
  joinU64,
  normalizeBlob,
  parseCapacityJson,
  parseSpecJson,
  serializeCapacityJson,
  serializeSpecJson,
  splitU64,
  type BundleManifestEntry,
  type CompletedAttemptObjectRef,
  type DurabilityName,
  type JobStateName,
  type SegmentSliceRef,
  type StoredDemand,
  type StoredErrorInfo,
  type StoredExecutionPolicy,
  type StoredObject,
  type StoredSpecMetadata,
} from "./store";

export { DEFAULT_NAMESPACE, decodeScopedId, encodeScopedId };

export function validateNamespaceSafe(ns: string): boolean {
  try {
    validateNamespace(ns);
    return true;
  } catch {
    return false;
  }
}

export const RUNTIME_CODES = {
  QUEUE_TIMEOUT: "queue_timeout",
  EXECUTION_TIMEOUT: "execution_timeout",
  TOTAL_TIMEOUT: "total_timeout",
  CANCELLED: "cancelled",
  WORKER_FAILED: "worker_failed",
  WORKER_LOST: "worker_lost",
  WORKER_REJECTED: "worker_rejected",
  WORKER_PREEMPTED: "worker_preempted",
  ATTEMPTS_EXHAUSTED: "attempts_exhausted",
  OUTPUT_INTERRUPTED: "output_interrupted",
  OUTPUT_TOO_LARGE: "output_too_large",
  CONSUMER_STALLED: "consumer_stalled",
  RECOVERED: "control_plane_restarted",
} as const;

const PROTOBUF_CONTENT_TYPE = "application/x-protobuf";
const DEFAULT_FAIRNESS_KEY = "_default";
const DISPATCH_SCAN = 256;

const utf8Encoder = new TextEncoder();

function utf8Len(s: string): number {
  return utf8Encoder.encode(s).byteLength;
}

function metadataByteLen(m: Record<string, string>): number {
  let sum = 0;
  for (const [k, v] of Object.entries(m)) {
    sum += utf8Len(k) + utf8Len(v);
  }
  return sum;
}

function toNumberClamped(v: bigint): number {
  if (v <= 0n) return 0;
  if (v > BigInt(Number.MAX_SAFE_INTEGER)) return Number.MAX_SAFE_INTEGER;
  return Number(v);
}

// ---------------------------------------------------------------------------
// Exact 64-bit xorshift64* & Rendezvous Hash (mirrors machine.rs lines 1638-1668)
// ---------------------------------------------------------------------------

const MASK_64 = 0xffff_ffff_ffff_ffffn;

function nextRandom64(rng: { state: bigint }): bigint {
  let x = rng.state === 0n ? 0x9e37_79b9_7f4a_7c15n : rng.state & MASK_64;
  x = (x ^ (x >> 12n)) & MASK_64;
  x = (x ^ ((x << 25n) & MASK_64)) & MASK_64;
  x = (x ^ (x >> 27n)) & MASK_64;
  rng.state = x;
  return (x * 0x2545_f491_4f6c_dd1dn) & MASK_64;
}

export function rendezvousHash64(affinityKey: string, workerId: string): bigint {
  let hash = 0xcbf2_9ce4_8422_2325n;
  const keyBytes = utf8Encoder.encode(affinityKey);
  const workerBytes = utf8Encoder.encode(workerId);
  for (let i = 0; i < keyBytes.byteLength; i++) {
    hash = ((hash ^ BigInt(keyBytes[i]!)) * 0x0100_0000_01b3n) & MASK_64;
  }
  hash = ((hash ^ 0xffn) * 0x0100_0000_01b3n) & MASK_64;
  for (let i = 0; i < workerBytes.byteLength; i++) {
    hash = ((hash ^ BigInt(workerBytes[i]!)) * 0x0100_0000_01b3n) & MASK_64;
  }
  hash = ((hash ^ (hash >> 30n)) * 0xbf58_476d_1ce4_e5b9n) & MASK_64;
  hash = ((hash ^ (hash >> 27n)) * 0x94d0_49bb_1331_11ebn) & MASK_64;
  hash = (hash ^ (hash >> 31n)) & MASK_64;
  return hash;
}

// ---------------------------------------------------------------------------
// Resolved Configuration
// ---------------------------------------------------------------------------

interface ResolvedEngineConfig {
  heartbeatIntervalMs: number;
  heartbeatTimeoutMs: number;
  reconnectGraceMs: number | null;
  cancelGraceMs: number;
  receiptRetentionMs: number;
  terminalRetentionMs: number;
  defaultInitialBackoffMs: number;
  defaultMaxBackoffMs: number;
  consumerStallTimeoutMs: number;
  maintenanceBucketMs: number;
  priorityLevels: number;
  defaultMaxAttempts: number;
  maxAttemptsLimit: number;
  defaultMaxQueue: number;
  defaultMaxInFlightPerKey: number | null;
  maxPendingJobs: number;
  maxTerminalJobs: number;
  maxPendingBytes: number;
  maxResultBytes: number;
  maxEventBytes: number;
  perJobEventCapBytes: number;
  eventLogMaxEvents: number;
  maxInlineObjectBytes: number;
  maxInlineTotalBytes: number;
  maxControlMessageBytes: number;
  maxChunkBytes: number;
  attemptWindowBytes: number;
  assignInlineLimit: number;
  maxSessionSockets: number;
  maxAttemptSockets: number;
  maxInvokeSockets: number;
  maxWatchSockets: number;
  maxPendingHandshakeSockets: number;
  interfaces: Record<string, InterfacePolicyConfig>;
}

function resolveEngineConfig(host: NativeHost): ResolvedEngineConfig {
  const o = host.overrides ?? {};
  return {
    heartbeatIntervalMs: o.heartbeatIntervalMs ?? DEFAULT_HEARTBEAT_INTERVAL_MS,
    heartbeatTimeoutMs: o.heartbeatTimeoutMs ?? DEFAULT_HEARTBEAT_TIMEOUT_MS,
    reconnectGraceMs:
      o.reconnectGraceMs !== undefined
        ? o.reconnectGraceMs
        : DEFAULT_RECONNECT_GRACE_MS,
    cancelGraceMs: o.cancelGraceMs ?? DEFAULT_CANCEL_GRACE_MS,
    receiptRetentionMs: o.receiptRetentionMs ?? DEFAULT_RECEIPT_RETENTION_MS,
    terminalRetentionMs: o.terminalRetentionMs ?? DEFAULT_TERMINAL_RETENTION_MS,
    defaultInitialBackoffMs:
      o.defaultInitialBackoffMs ?? DEFAULT_INITIAL_BACKOFF_MS,
    defaultMaxBackoffMs: o.defaultMaxBackoffMs ?? DEFAULT_MAX_BACKOFF_MS,
    consumerStallTimeoutMs:
      o.consumerStallTimeoutMs ?? DEFAULT_CONSUMER_STALL_TIMEOUT_MS,
    maintenanceBucketMs: o.maintenanceBucketMs ?? MAINTENANCE_BUCKET_MS,
    priorityLevels: Math.max(1, o.priorityLevels ?? 4),
    defaultMaxAttempts: Math.max(1, o.defaultMaxAttempts ?? 3),
    maxAttemptsLimit: Math.max(1, o.maxAttemptsLimit ?? 20),
    defaultMaxQueue: Math.max(1, o.defaultMaxQueue ?? 2_000),
    defaultMaxInFlightPerKey: o.defaultMaxInFlightPerKey ?? null,
    maxPendingJobs: Math.max(1, o.maxPendingJobs ?? 2_000),
    maxTerminalJobs: Math.max(1, o.maxTerminalJobs ?? 3_000),
    maxPendingBytes: o.maxPendingBytes ?? 67_108_864,
    maxResultBytes: o.maxResultBytes ?? 67_108_864,
    maxEventBytes: o.maxEventBytes ?? 33_554_432,
    perJobEventCapBytes: o.perJobEventCapBytes ?? PER_JOB_EVENT_CAP_BYTES,
    eventLogMaxEvents: Math.max(2, o.eventLogMaxEvents ?? 8_192),
    maxInlineObjectBytes: o.maxInlineObjectBytes ?? MAX_INLINE_OBJECT_BYTES,
    maxInlineTotalBytes: o.maxInlineTotalBytes ?? MAX_INLINE_TOTAL_BYTES,
    maxControlMessageBytes:
      o.maxControlMessageBytes ?? WELCOME_MAX_CONTROL_BYTES,
    maxChunkBytes: o.maxChunkBytes ?? WELCOME_MAX_CHUNK_BYTES,
    attemptWindowBytes: o.attemptWindowBytes ?? WELCOME_ATTEMPT_WINDOW_BYTES,
    assignInlineLimit: o.assignInlineLimit ?? WELCOME_ASSIGN_INLINE_LIMIT,
    maxSessionSockets: o.maxSessionSockets ?? MAX_SESSION_SOCKETS,
    maxAttemptSockets: o.maxAttemptSockets ?? MAX_ATTEMPT_SOCKETS,
    maxInvokeSockets: o.maxInvokeSockets ?? MAX_INVOKE_SOCKETS,
    maxWatchSockets: o.maxWatchSockets ?? MAX_WATCH_SOCKETS,
    maxPendingHandshakeSockets:
      o.maxPendingHandshakeSockets ?? MAX_PENDING_HANDSHAKE_SOCKETS,
    interfaces: o.interfaces ?? {},
  };
}

// ---------------------------------------------------------------------------
// In-Memory Scheduling & Metadata Structures (Rehydrated on Wake in < 5ms)
// ---------------------------------------------------------------------------

interface SessionMem {
  sessionId: string;
  workerId: string;
  namespace: string;
  pool: string;
  forcedPool: string | null;
  interfaces: string[];
  attributes: Record<string, string>;
  metadata: Record<string, string>;
  totalCapacity: Record<string, bigint>;
  reservedCapacity: Record<string, bigint>;
  activeLeases: Set<string>;
  maxConcurrency: number;
  reportedFree: number;
  assignedSinceReport: number;
  draining: boolean;
  connected: boolean;
  currentConnId: number;
  connectedAtMs: number;
  lastSeenMs: number;
  graceDeadlineMs: number | null;
  consecutiveFailures: number;
  cooldownUntilMs: number;
  queued: number;
  running: number;
  utilization: number;
  customLoad: Record<string, number>; // memory-only, bounded 64 entries / 8 KiB
}

interface LeaseMem {
  leaseId: string;
  scopedJobId: string;
  rawJobId: string;
  namespace: string;
  attemptId: string;
  attemptNumber: number;
  sessionId: string;
  workerId: string;
  generation: bigint;
  reservedCapacity: Record<string, bigint>;
  startedAtMs: number;
  executionDeadlineMs: number | null;
  inputsViaAttempt: boolean;
  accepted: boolean;
  cancelling: boolean;
  cancelDeadlineMs: number | null;
  attemptConnId: number | null;
  lastFrameSeq: bigint;
  exposed: boolean;
  concurrencyCounted: boolean;
  inputStage: number; // 0=static, 1=params, 2=stream, 3=trailer, 4=done
  inputItemIdx: number;
  inputByteOff: number;
  inputSeq: bigint;
  curObjIndex: number | null;
  curObjMeta: Record<string, string>;
  curObjUri: string;
  curObjBytes: number;
  nextObjIndex: number;
  tailBytes: number;
  tailNextChunkSeq: number;
  nextSegIndex: number;
  totalResultBytes: number;
  ackedResultBytes: number;
  resultTooLarge: boolean;
  completedObjs: CompletedAttemptObjectRef[];
  inlineResultBlob: Uint8Array;
}

interface JobMem {
  scopedJobId: string;
  rawJobId: string;
  namespace: string;
  interfaceName: string;
  state: JobStateName;
  durability: DurabilityName;
  specHash: string;
  spec: StoredSpecMetadata;
  priority: number;
  fairnessKey: string;
  concurrencyKey: string;
  affinityKey: string;
  maxAttempts: number;
  attemptCount: number;
  createdAtMs: number;
  updatedAtMs: number;
  queuedSinceMs: number;
  notBeforeMs: number;
  queueDeadlineMs: number | null;
  totalDeadlineMs: number | null;
  spilloverAtMs: number | null;
  callerConnId: number | null;
  streamingInput: boolean;
  inputEnded: boolean;
  streamChunkCount: number;
  totalInputBytes: number;
  retainedBytesEst: number;
  cancelRequested: string | null;
  error: StoredErrorInfo | null;
  winningLeaseId: string | null;
  resultDigest: string | null;
  lastAttemptId: string | null;
  lastWorkerId: string | null;
  currentLeaseId: string | null;
  failedWorkers: Set<string>;
  rejectedSessions: Set<string>;
  lastSequence: bigint;
  evictedThrough: bigint;
  primaryConsumed: bigint | null;
  queueSeq: bigint | null;
  eventLogBytes: number;
  eventLogCount: number;
  stalledSinceMs: number | null;
}

interface WatcherMem {
  watchConnId: number;
  scopedJobId: string;
  rawJobId: string;
  namespace: string;
  afterSeq: bigint;
  deliveredSeq: bigint;
  finished: boolean;
}

interface ReplayUploadState {
  leaseId: string;
  namespace: string;
  recordedDigest: string;
  lastFrameSeq: bigint;
  curObjIndex: number | null;
  curObjMeta: Record<string, string>;
  curObjUri: string;
  curObjBytes: number;
  curObjSha: IncrementalSha256 | null;
  nextObjIndex: number;
  totalResultBytes: bigint;
  tooLarge: boolean;
  objectHashes: Array<
    | { kind: "inline_prehashed"; byteLen: number; sha256Bytes: Uint8Array; metadata: Record<string, string> }
    | { kind: "uri"; uri: string; metadata: Record<string, string> }
  >;
}

interface QueueEntry {
  seq: bigint;
  scopedJobId: string;
}

class FairPriorityQueue {
  private readonly levels: Array<{
    ring: string[];
    subqueues: Map<string, QueueEntry[]>;
    count: number;
  }> = [];
  totalLen = 0;

  insert(priorityLevels: number, rawPriority: number, fairnessKey: string, entry: QueueEntry): void {
    const numLevels = Math.max(1, priorityLevels);
    while (this.levels.length < numLevels) {
      this.levels.push({ ring: [], subqueues: new Map(), count: 0 });
    }
    const idx = Math.min(Math.max(0, rawPriority), numLevels - 1);
    const level = this.levels[idx]!;
    const fkey = fairnessKey || DEFAULT_FAIRNESS_KEY;
    let sub = level.subqueues.get(fkey);
    if (!sub) {
      sub = [];
      level.subqueues.set(fkey, sub);
      level.ring.push(fkey);
    }
    if (sub.some((e) => e.scopedJobId === entry.scopedJobId)) {
      return;
    }
    // Insert sorted by (seq ASC, scopedJobId ASC)
    let pos = sub.length;
    while (pos > 0) {
      const prev = sub[pos - 1]!;
      if (
        prev.seq < entry.seq ||
        (prev.seq === entry.seq && prev.scopedJobId < entry.scopedJobId)
      ) {
        break;
      }
      pos -= 1;
    }
    sub.splice(pos, 0, entry);
    level.count += 1;
    this.totalLen += 1;
  }

  remove(rawPriority: number, fairnessKey: string, scopedJobId: string): boolean {
    if (this.levels.length === 0) {
      return false;
    }
    const idx = Math.min(Math.max(0, rawPriority), this.levels.length - 1);
    const level = this.levels[idx];
    if (!level) {
      return false;
    }
    const fkey = fairnessKey || DEFAULT_FAIRNESS_KEY;
    const sub = level.subqueues.get(fkey);
    if (!sub) {
      return false;
    }
    const pos = sub.findIndex((e) => e.scopedJobId === scopedJobId);
    if (pos < 0) {
      return false;
    }
    sub.splice(pos, 1);
    level.count -= 1;
    this.totalLen -= 1;
    if (sub.length === 0) {
      level.subqueues.delete(fkey);
      const rIdx = level.ring.indexOf(fkey);
      if (rIdx >= 0) {
        level.ring.splice(rIdx, 1);
      }
    }
    return true;
  }

  rotateKeyToBack(rawPriority: number, fairnessKey: string): void {
    if (this.levels.length === 0) {
      return;
    }
    const idx = Math.min(Math.max(0, rawPriority), this.levels.length - 1);
    const level = this.levels[idx];
    if (!level || level.ring.length <= 1) {
      return;
    }
    const fkey = fairnessKey || DEFAULT_FAIRNESS_KEY;
    if (level.ring[0] === fkey) {
      level.ring.push(level.ring.shift()!);
    }
  }

  collectCandidates(limit: number): QueueEntry[] {
    const out: QueueEntry[] = [];
    for (let idx = this.levels.length - 1; idx >= 0 && out.length < limit; idx--) {
      const level = this.levels[idx]!;
      if (level.count === 0) {
        continue;
      }
      const need = limit - out.length;
      if (level.ring.length === 1) {
        const sub = level.subqueues.get(level.ring[0]!) ?? [];
        for (let i = 0; i < sub.length && out.length < limit; i++) {
          out.push(sub[i]!);
        }
        continue;
      }
      const cursors = level.ring
        .map((k) => level.subqueues.get(k))
        .filter((s): s is QueueEntry[] => Boolean(s && s.length > 0))
        .map((sub) => ({ sub, pos: 0 }));
      let added = 0;
      while (added < need && cursors.length > 0) {
        let progressed = false;
        for (let i = 0; i < cursors.length && added < need; i++) {
          const c = cursors[i]!;
          if (c.pos < c.sub.length) {
            out.push(c.sub[c.pos]!);
            c.pos += 1;
            added += 1;
            progressed = true;
          }
        }
        if (!progressed) {
          break;
        }
      }
    }
    return out;
  }

  collectAllOrdered(): QueueEntry[] {
    return this.collectCandidates(this.totalLen || 1);
  }
}

// ---------------------------------------------------------------------------
// Single-Buffer Protobuf Wire Helpers for Streaming Large `ExecutionFinished`
// ---------------------------------------------------------------------------

function varintLen32(v: number): number {
  const u = v >>> 0;
  if (u < 0x80) return 1;
  if (u < 0x4000) return 2;
  if (u < 0x200000) return 3;
  if (u < 0x10000000) return 4;
  return 5;
}

function varintLen64(v: bigint): number {
  let u = BigInt.asUintN(64, v);
  let n = 1;
  while (u >= 0x80n) {
    u >>= 7n;
    n += 1;
  }
  return n;
}

function writeVarint32At(buf: Uint8Array, pos: number, v: number): number {
  let u = v >>> 0;
  while (u >= 0x80) {
    buf[pos++] = (u & 0x7f) | 0x80;
    u >>>= 7;
  }
  buf[pos++] = u;
  return pos;
}

function writeVarint64At(buf: Uint8Array, pos: number, v: bigint): number {
  let u = BigInt.asUintN(64, v);
  while (u >= 0x80n) {
    buf[pos++] = Number(u & 0x7fn) | 0x80;
    u >>= 7n;
  }
  buf[pos++] = Number(u);
  return pos;
}

function lenDelimitedSize(tagBytes: number, bodyLen: number): number {
  return tagBytes + varintLen32(bodyLen) + bodyLen;
}

function stringMapEncodedSize(
  tagBytes: number,
  map: Record<string, string> | undefined,
): number {
  if (!map) return 0;
  const sorted = sortMapByUtf8Bytes(map);
  let total = 0;
  for (const e of sorted) {
    const valBytes = utf8Len(e.value);
    const entryLen =
      (e.keyBytes.byteLength > 0 ? lenDelimitedSize(1, e.keyBytes.byteLength) : 0) +
      (valBytes > 0 ? lenDelimitedSize(1, valBytes) : 0);
    total += lenDelimitedSize(tagBytes, entryLen);
  }
  return total;
}

function writeStringMapAt(
  buf: Uint8Array,
  pos: number,
  fieldTag: number,
  map: Record<string, string> | undefined,
): number {
  if (!map) return pos;
  const sorted = sortMapByUtf8Bytes(map);
  for (const e of sorted) {
    const valBytes = utf8Encoder.encode(e.value);
    const entryLen =
      (e.keyBytes.byteLength > 0 ? lenDelimitedSize(1, e.keyBytes.byteLength) : 0) +
      (valBytes.byteLength > 0 ? lenDelimitedSize(1, valBytes.byteLength) : 0);
    pos = writeVarint32At(buf, pos, fieldTag);
    pos = writeVarint32At(buf, pos, entryLen);
    if (e.keyBytes.byteLength > 0) {
      pos = writeVarint32At(buf, pos, (1 << 3) | 2);
      pos = writeVarint32At(buf, pos, e.keyBytes.byteLength);
      buf.set(e.keyBytes, pos);
      pos += e.keyBytes.byteLength;
    }
    if (valBytes.byteLength > 0) {
      pos = writeVarint32At(buf, pos, (2 << 3) | 2);
      pos = writeVarint32At(buf, pos, valBytes.byteLength);
      buf.set(valBytes, pos);
      pos += valBytes.byteLength;
    }
  }
  return pos;
}

interface OutputManifestItemRef {
  seq: number;
  entry: BundleManifestEntry;
}

function encodedObjectLenFromManifestEntry(entry: BundleManifestEntry): number {
  if (entry.t === "u") {
    const uLen = utf8Len(entry.u);
    return lenDelimitedSize(1, uLen) + stringMapEncodedSize(1, entry.m);
  }
  const meta = "m" in entry ? entry.m : undefined;
  return lenDelimitedSize(1, entry.l) + stringMapEncodedSize(1, meta);
}

// ---------------------------------------------------------------------------
// NativeEngine Class
// ---------------------------------------------------------------------------

export class NativeEngine implements ConsoleBackend {
  readonly host: NativeHost;
  readonly config: ResolvedEngineConfig;
  readonly store: NativeStore;
  readonly auth: AuthPolicy;
  readonly isolateBudget: IsolateMemoryBudget;
  readonly bootId: string;
  readonly constructorRuns: number;
  readonly constructorDurationMs: number;

  private generation = 1n;
  private disposed = false;
  private connSeq = 0;
  private queueSeq = 0n;
  private rng = { state: 0x5dee_ce66_d1ce_4e5bn };
  private armedAlarmMs: number | null = null;
  private committedBatches = 0;
  private committedOps = 0;
  droppedCustomLoadEntries = 0;

  // In-memory state rebuilt on wake from SQLite + getWebSockets()
  private readonly sessions = new Map<string, SessionMem>();
  private readonly sessionsByInterface = new Map<string, Map<string, Set<string>>>();
  private readonly leases = new Map<string, LeaseMem>();
  private readonly jobs = new Map<string, JobMem>();
  private readonly queues = new Map<string, Map<string, FairPriorityQueue>>();
  private readonly inFlightConcurrency = new Map<string, Map<string, number>>();
  private readonly watchers = new Map<number, WatcherMem>();
  private readonly watchersByJob = new Map<string, Set<number>>();
  private readonly replayStates = new Map<number, ReplayUploadState>();

  // Live sockets indexed by connId
  private readonly socketsByConnId = new Map<number, NativeWebSocket>();
  private readonly socketChains = new Map<number, Promise<void>>();
  private readonly pumpingAttempts = new Set<string>();

  // Shared runtime byte trackers (for /metrics and /api/overview parity)
  private totalInputBytesHeld = 0;
  private totalResultBytesHeld = 0;
  private totalEventBytesHeld = 0;

  constructor(host: NativeHost) {
    const t0 = host.nowMs();
    this.host = host;
    this.config = resolveEngineConfig(host);
    this.bootId = crypto.randomUUID();
    this.constructorRuns = host.nextConstructorRun ? host.nextConstructorRun() : 1;
    this.auth = AuthPolicy.fromEnv(host.env);
    this.isolateBudget = new IsolateMemoryBudget({
      isolateCapBytes: host.overrides?.isolateMemoryCapBytes,
      largeSendCapBytes: host.overrides?.largeSendCapBytes,
      largeSendThresholdBytes: host.overrides?.largeSendThresholdBytes,
    });
    this.store = new NativeStore(
      host.doId,
      host.storage,
      host.r2,
      host.overrides,
    );

    this.rehydrateAfterWake(t0);
    this.constructorDurationMs = Math.max(0, host.nowMs() - t0);
  }

  get env(): NativeEnvConfig {
    return this.host.env;
  }

  get currentGeneration(): bigint {
    return this.generation;
  }

  /**
   * Marks this in-memory engine instance as evicted (used by `simulateHibernation()` in single-process tests
   * so background async loops from the pre-hibernation instance stop immediately, matching workerd eviction).
   */
  dispose(): void {
    this.disposed = true;
  }

  // ---------------------------------------------------------------------------
  // Constructor / Cold-Wake Rehydration (Section 4.1, 4.3, 4.6)
  // Bounded metadata load (< 5ms, zero R2 I/O, zero payload reads, zero writes
  // when nothing vanished and no retry is overdue!)
  // ---------------------------------------------------------------------------

  private rehydrateAfterWake(nowMs: number): void {
    const meta = this.store.initOrLoadMeta(nowMs);
    this.generation = meta.generation;
    this.connSeq = meta.connSeq;
    this.queueSeq = meta.queueSeq;
    this.rng.state = (this.generation ^ 0x5dee_ce66_d1ce_4e5bn) & MASK_64;

    // 1. Index all live open WebSockets from host.getWebSockets()
    for (const ws of this.host.getWebSockets()) {
      if (ws.readyState !== WS_READY_STATE_OPEN) {
        continue;
      }
      const att = readSocketAttachment(ws);
      if (!att) {
        continue;
      }
      if (att.connId > this.connSeq) {
        this.connSeq = att.connId;
      }
      this.socketsByConnId.set(att.connId, ws);
    }

    // 2. Load native_sessions
    const sessionRows = this.store.exec<Record<string, any>>(
      `SELECT * FROM native_sessions`,
    );
    for (const r of sessionRows) {
      const connId = Number(r.current_conn_id);
      const liveWs = this.socketsByConnId.get(connId);
      const liveAtt = liveWs ? readSocketAttachment(liveWs) : null;
      const session: SessionMem = {
        sessionId: String(r.session_id),
        workerId: String(r.worker_id),
        namespace: String(r.namespace),
        pool: String(r.pool),
        forcedPool: r.forced_pool !== null ? String(r.forced_pool) : null,
        interfaces: JSON.parse(String(r.interfaces_json)) as string[],
        attributes: JSON.parse(String(r.attributes_json)) as Record<string, string>,
        metadata: JSON.parse(String(r.metadata_json)) as Record<string, string>,
        totalCapacity: parseCapacityJson(r.total_capacity_json),
        reservedCapacity: parseCapacityJson(r.reserved_capacity_json),
        activeLeases: new Set(JSON.parse(String(r.active_leases_json)) as string[]),
        maxConcurrency: Number(r.max_concurrency),
        reportedFree: Number(r.reported_free),
        assignedSinceReport: Number(r.assigned_since_report),
        draining: Number(r.draining) === 1,
        connected: Number(r.connected) === 1,
        currentConnId: connId,
        connectedAtMs: Number(r.connected_at_ms),
        lastSeenMs: liveAtt
          ? Math.max(Number(r.last_seen_ms), liveAtt.lastSeenAtMs)
          : Number(r.last_seen_ms),
        graceDeadlineMs:
          r.grace_deadline_ms !== null ? Number(r.grace_deadline_ms) : null,
        consecutiveFailures: Number(r.consecutive_failures),
        cooldownUntilMs: Number(r.cooldown_until_ms),
        queued: liveAtt ? liveAtt.queued : Number(r.queued ?? 0),
        running: liveAtt ? liveAtt.running : Number(r.running ?? 0),
        utilization: liveAtt ? liveAtt.utilization : Number(r.utilization ?? 0),
        customLoad: {}, // Load.custom is memory-only and empty right after wake until next heartbeat
      };
      this.sessions.set(session.sessionId, session);
      this.indexSessionInMemory(session);
    }

    // 3. Load native_active_leases (without reading upload_tail or upload_segments blobs!)
    const leaseRows = this.store.exec<Record<string, any>>(
      `SELECT * FROM native_active_leases`,
    );
    for (const r of leaseRows) {
      const attemptConnId =
        r.attempt_conn_id !== null ? Number(r.attempt_conn_id) : null;
      const liveAttemptConn =
        attemptConnId !== null && this.socketsByConnId.has(attemptConnId)
          ? attemptConnId
          : null;
      const inlineBlob = r.inline_result_blob
        ? normalizeBlob(r.inline_result_blob)
        : new Uint8Array(0);
      const lease: LeaseMem = {
        leaseId: String(r.lease_id),
        scopedJobId: String(r.scoped_job_id),
        rawJobId: String(r.raw_job_id),
        namespace: String(r.namespace),
        attemptId: String(r.attempt_id),
        attemptNumber: Number(r.attempt_number),
        sessionId: String(r.session_id),
        workerId: String(r.worker_id),
        generation: BigInt(String(r.generation_text)),
        reservedCapacity: parseCapacityJson(r.reserved_capacity_json),
        startedAtMs: Number(r.started_at_ms),
        executionDeadlineMs:
          r.execution_deadline_ms !== null ? Number(r.execution_deadline_ms) : null,
        inputsViaAttempt: Number(r.inputs_via_attempt) === 1,
        accepted: Number(r.accepted) === 1,
        cancelling: Number(r.cancelling) === 1,
        cancelDeadlineMs:
          r.cancel_deadline_ms !== null ? Number(r.cancel_deadline_ms) : null,
        attemptConnId: liveAttemptConn,
        lastFrameSeq: joinU64(r.last_frame_seq_hi, r.last_frame_seq_lo),
        exposed: Number(r.exposed) === 1,
        concurrencyCounted: Number(r.concurrency_counted) === 1,
        inputStage: Number(r.input_stage),
        inputItemIdx: Number(r.input_item_idx),
        inputByteOff: Number(r.input_byte_off),
        inputSeq: joinU64(r.input_seq_hi, r.input_seq_lo),
        curObjIndex: r.cur_obj_index !== null ? Number(r.cur_obj_index) : null,
        curObjMeta: r.cur_obj_meta_json
          ? (JSON.parse(String(r.cur_obj_meta_json)) as Record<string, string>)
          : {},
        curObjUri: r.cur_obj_uri !== null ? String(r.cur_obj_uri) : "",
        curObjBytes: Number(r.cur_obj_bytes ?? 0),
        nextObjIndex: Number(r.next_obj_index ?? 0),
        tailBytes: Number(r.tail_bytes ?? 0),
        tailNextChunkSeq: Number(r.tail_next_chunk_seq ?? 0),
        nextSegIndex: Number(r.next_seg_index ?? 0),
        totalResultBytes: Number(r.total_result_bytes ?? 0),
        ackedResultBytes: Number(r.acked_result_bytes ?? 0),
        resultTooLarge: Number(r.result_too_large ?? 0) === 1,
        completedObjs: JSON.parse(
          String(r.completed_objs_json ?? "[]"),
        ) as CompletedAttemptObjectRef[],
        inlineResultBlob: inlineBlob,
      };
      this.leases.set(lease.leaseId, lease);
      this.totalResultBytesHeld += lease.totalResultBytes;
    }

    // 4. Load bounded job metadata (all unfinished jobs + retained terminal jobs; no bundle payloads!)
    const jobRows = this.store.exec<Record<string, any>>(
      `SELECT * FROM native_jobs ORDER BY created_at_ms ASC, scoped_job_id ASC`,
    );
    const eventStatRows = this.store.exec<{
      scoped_job_id: string;
      cnt: number;
      bytes: number;
    }>(
      `SELECT scoped_job_id, COUNT(*) AS cnt, COALESCE(SUM(size_bytes), 0) AS bytes
       FROM native_job_events GROUP BY scoped_job_id`,
    );
    const eventStatsByJob = new Map<string, { cnt: number; bytes: number }>();
    for (const es of eventStatRows) {
      eventStatsByJob.set(es.scoped_job_id, {
        cnt: Number(es.cnt),
        bytes: Number(es.bytes),
      });
    }

    this.store.reservedRetainedBytes = 0;
    for (const r of jobRows) {
      const scopedJobId = String(r.scoped_job_id);
      const spec = parseSpecJson(String(r.spec_json));
      const state = String(r.state) as JobStateName;
      const es = eventStatsByJob.get(scopedJobId) ?? { cnt: 0, bytes: 0 };
      const job: JobMem = {
        scopedJobId,
        rawJobId: String(r.raw_job_id),
        namespace: String(r.namespace),
        interfaceName: String(r.interface_name),
        state,
        durability: String(r.durability) as DurabilityName,
        specHash: String(r.spec_hash),
        spec,
        priority: Number(r.priority),
        fairnessKey: String(r.fairness_key),
        concurrencyKey: String(r.concurrency_key),
        affinityKey: String(r.affinity_key),
        maxAttempts: Number(r.max_attempts),
        attemptCount: Number(r.attempt_count),
        createdAtMs: Number(r.created_at_ms),
        updatedAtMs: Number(r.updated_at_ms),
        queuedSinceMs: Number(r.queued_since_ms),
        notBeforeMs: Number(r.not_before_ms),
        queueDeadlineMs:
          r.queue_deadline_ms !== null ? Number(r.queue_deadline_ms) : null,
        totalDeadlineMs:
          r.total_deadline_ms !== null ? Number(r.total_deadline_ms) : null,
        spilloverAtMs:
          r.spillover_at_ms !== null ? Number(r.spillover_at_ms) : null,
        callerConnId:
          r.caller_conn_id !== null ? Number(r.caller_conn_id) : null,
        streamingInput: Number(r.streaming_input) === 1,
        inputEnded: Number(r.input_ended) === 1,
        streamChunkCount: Number(r.stream_chunk_count ?? 0),
        totalInputBytes: Number(r.total_input_bytes ?? 0),
        retainedBytesEst: Number(r.retained_bytes_est ?? 0),
        cancelRequested:
          r.cancel_requested !== null ? String(r.cancel_requested) : null,
        error: r.error_json
          ? (JSON.parse(String(r.error_json)) as StoredErrorInfo)
          : null,
        winningLeaseId:
          r.winning_lease_id !== null ? String(r.winning_lease_id) : null,
        resultDigest:
          r.result_digest !== null ? String(r.result_digest) : null,
        lastAttemptId:
          r.last_attempt_id !== null ? String(r.last_attempt_id) : null,
        lastWorkerId:
          r.last_worker_id !== null ? String(r.last_worker_id) : null,
        currentLeaseId: null,
        failedWorkers: new Set(
          JSON.parse(String(r.failed_workers_json)) as string[],
        ),
        rejectedSessions: new Set(
          JSON.parse(String(r.rejected_sessions_json)) as string[],
        ),
        lastSequence: joinU64(r.last_seq_hi, r.last_seq_lo),
        evictedThrough: joinU64(r.evicted_seq_hi, r.evicted_seq_lo),
        primaryConsumed:
          r.primary_seq_hi !== null
            ? joinU64(r.primary_seq_hi, r.primary_seq_lo)
            : null,
        queueSeq: null,
        eventLogBytes: es.bytes,
        eventLogCount: es.cnt,
        stalledSinceMs: null,
      };
      this.jobs.set(scopedJobId, job);
      this.store.reservedRetainedBytes += job.retainedBytesEst;
      this.totalEventBytesHeld += es.bytes;
      if (state === "queued" || state === "running") {
        this.totalInputBytesHeld += job.totalInputBytes;
      }
      if (state === "queued") {
        this.queueSeq += 1n;
        job.queueSeq = this.queueSeq;
        this.getOrCreateQueue(job.namespace, job.interfaceName).insert(
          this.config.priorityLevels,
          job.priority,
          job.fairnessKey,
          { seq: job.queueSeq, scopedJobId: job.scopedJobId },
        );
      }
    }

    // Link non-cancelling active leases back to their running job & concurrency counters!
    // Note (section 4.1 & 4.6): cancelling=1 leases do NOT count as running work,
    // while keeping their session slot/capacity reservation until resolved!
    for (const lease of this.leases.values()) {
      if (!lease.cancelling) {
        const job = this.jobs.get(lease.scopedJobId);
        if (job && job.state === "running") {
          job.currentLeaseId = lease.leaseId;
          if (lease.concurrencyCounted && job.concurrencyKey) {
            this.incConcurrencyKey(job.namespace, job.concurrencyKey);
          }
        }
      }
    }

    // 5. Load native_watchers
    const watcherRows = this.store.exec<Record<string, any>>(
      `SELECT * FROM native_watchers`,
    );
    for (const r of watcherRows) {
      const watchConnId = Number(r.watch_conn_id);
      const w: WatcherMem = {
        watchConnId,
        scopedJobId: String(r.scoped_job_id),
        rawJobId: String(r.raw_job_id),
        namespace: String(r.namespace),
        afterSeq: joinU64(r.after_seq_hi, r.after_seq_lo),
        deliveredSeq: joinU64(r.delivered_seq_hi, r.delivered_seq_lo),
        finished: Number(r.finished) === 1,
      };
      this.watchers.set(watchConnId, w);
      let set = this.watchersByJob.get(w.scopedJobId);
      if (!set) {
        set = new Set();
        this.watchersByJob.set(w.scopedJobId, set);
      }
      set.add(watchConnId);
    }

    // Restore terminal_replay upload state if an open attempt socket with rp=1 exists across wake
    for (const [connId, ws] of this.socketsByConnId) {
      const att = readSocketAttachment(ws);
      if (att && att.kind === "attempt" && att.handshakeDone === 1 && att.replay === 1 && att.id) {
        const recorded = this.lookupRecordedReceiptSync(att.id, att.namespace);
        if (recorded !== null) {
          this.replayStates.set(connId, {
            leaseId: att.id,
            namespace: att.namespace,
            recordedDigest: recorded,
            lastFrameSeq: 0n,
            curObjIndex: null,
            curObjMeta: {},
            curObjUri: "",
            curObjBytes: 0,
            curObjSha: null,
            nextObjIndex: 0,
            totalResultBytes: 0n,
            tooLarge: false,
            objectHashes: [],
          });
        }
      }
    }

    // 6. Reconcile vanished sockets (ONLY writes SQL if something actually vanished or overdue retry dispatches!)
    const vanishedSessions: SessionMem[] = [];
    for (const session of this.sessions.values()) {
      // ONLY connected === true -> false transitions start grace!
      // Already-disconnected sessions NEVER have grace_deadline_ms extended on wake!
      if (session.connected && !this.socketsByConnId.has(session.currentConnId)) {
        vanishedSessions.push(session);
      }
    }
    const vanishedInvokes: JobMem[] = [];
    for (const job of this.jobs.values()) {
      if (
        job.callerConnId !== null &&
        (job.state === "queued" || job.state === "running") &&
        !this.socketsByConnId.has(job.callerConnId)
      ) {
        vanishedInvokes.push(job);
      }
    }
    const vanishedWatchers: number[] = [];
    for (const w of this.watchers.values()) {
      if (!this.socketsByConnId.has(w.watchConnId)) {
        vanishedWatchers.push(w.watchConnId);
      }
    }

    const hasOverdueQueued = [...this.jobs.values()].some(
      (j) =>
        (j.state === "queued" &&
          ((j.notBeforeMs <= nowMs && this.hasMatchingFreeWorker(j, nowMs)) ||
            (j.queueDeadlineMs !== null && j.queueDeadlineMs <= nowMs) ||
            (j.totalDeadlineMs !== null && j.totalDeadlineMs <= nowMs))) ||
        (j.state === "running" && j.totalDeadlineMs !== null && j.totalDeadlineMs <= nowMs),
    );

    const rehydrateNotifyJobs = new Set<string>();
    if (
      vanishedSessions.length > 0 ||
      vanishedInvokes.length > 0 ||
      vanishedWatchers.length > 0 ||
      hasOverdueQueued
    ) {
      let needsAlarmUpdate = false;
      this.store.transactionSync(() => {
        for (const s of vanishedSessions) {
          this.disconnectSessionInTxn(s.sessionId, s.currentConnId, nowMs);
          needsAlarmUpdate = true;
        }
        for (const j of vanishedInvokes) {
          this.cancelJobInTxn(j.scopedJobId, "caller disconnected", nowMs);
          rehydrateNotifyJobs.add(j.scopedJobId);
          needsAlarmUpdate = true;
        }
        for (const wid of vanishedWatchers) {
          this.removeWatcherInTxn(wid);
        }
        if (hasOverdueQueued) {
          if (this.dispatchInTxn(nowMs, rehydrateNotifyJobs) > 0 || rehydrateNotifyJobs.size > 0) {
            needsAlarmUpdate = true;
          }
        }
      });
      for (const jid of rehydrateNotifyJobs) {
        void this.notifyJobFollowers(jid);
      }
      if (needsAlarmUpdate) {
        void this.refreshAlarm(nowMs);
      }
    } else if (this.computeNextAlarmDueMs(nowMs) !== null) {
      // Sync in-memory alarm marker with platform alarm without writing SQLite rows on cold-wake heartbeats;
      // ONLY calls setAlarm if a prior setAlarm was interrupted before hibernation!
      void this.syncPlatformAlarmOnWake();
    }

    // Resume server->worker InputData pumping on any live open attempt socket that still has input pending!
    for (const lease of this.leases.values()) {
      if (
        !lease.cancelling &&
        lease.inputsViaAttempt &&
        lease.attemptConnId !== null &&
        lease.inputStage < 4
      ) {
        const ws = this.socketsByConnId.get(lease.attemptConnId);
        if (ws && ws.readyState === WS_READY_STATE_OPEN && ws.bufferedAmount === 0) {
          void this.pumpAttemptInputs(lease.leaseId);
        }
      }
    }
  }

  private async syncPlatformAlarmOnWake(): Promise<void> {
    try {
      const existing = await this.host.getAlarm();
      if (this.disposed) return;
      const nowMs = this.host.nowMs();
      const due = this.computeNextAlarmDueMs(nowMs);
      if (due === null) {
        this.armedAlarmMs = existing;
        return;
      }
      if (existing !== null && existing <= due) {
        this.armedAlarmMs = existing;
        return;
      }
      await this.refreshAlarm(nowMs);
    } catch {
      // ignore getAlarm errors
    }
  }

  private hasMatchingFreeWorker(job: JobMem, _nowMs: number): boolean {
    const set = this.sessionsByInterface.get(job.namespace)?.get(job.interfaceName);
    if (!set) return false;
    for (const sid of set) {
      const s = this.sessions.get(sid);
      if (s && this.sessionFreeSlots(s) > 0) {
        return true;
      }
    }
    return false;
  }

  // ---------------------------------------------------------------------------
  // Entry Points: HTTP Fetch, WebSocket Upgrade, Hibernation Handlers, Alarm
  // ---------------------------------------------------------------------------

  async fetch(request: Request): Promise<Response> {
    const nowMs = this.host.nowMs();
    this.store.recordHttpOrAlarmRequest(nowMs);

    const consoleRes = await handleConsoleOrHealthRequest(request, this, nowMs);
    if (consoleRes !== null) {
      return consoleRes;
    }

    const url = new URL(request.url);
    const path = url.pathname;
    const method = request.method.toUpperCase();

    // WebSocket streaming routes (/ws/v2/*)
    if (path.startsWith("/ws/v2/")) {
      return await this.handleWebSocketUpgrade(request, path, nowMs);
    }

    // Unary protobuf routes (/rpc/v2/client/*)
    if (path.startsWith("/rpc/v2/client/") && method === "POST") {
      return await this.handleUnaryClientRpc(request, path, nowMs);
    }

    return new Response("Not Found", { status: 404 });
  }

  private countOpenSocketsByKind(): {
    session: number;
    attempt: number;
    invoke: number;
    watch: number;
    pendingHandshake: number;
  } {
    let session = 0;
    let attempt = 0;
    let invoke = 0;
    let watch = 0;
    let pendingHandshake = 0;
    for (const [connId, ws] of this.socketsByConnId) {
      if (ws.readyState !== WS_READY_STATE_OPEN) {
        this.socketsByConnId.delete(connId);
        continue;
      }
      const att = readSocketAttachment(ws);
      if (!att) continue;
      if (att.handshakeDone === 0) {
        pendingHandshake += 1;
      }
      switch (att.kind) {
        case "session":
          session += 1;
          break;
        case "attempt":
          attempt += 1;
          break;
        case "invoke":
          invoke += 1;
          break;
        case "watch":
          watch += 1;
          break;
      }
    }
    return { session, attempt, invoke, watch, pendingHandshake };
  }

  private async handleWebSocketUpgrade(
    request: Request,
    path: string,
    nowMs: number,
  ): Promise<Response> {
    let kind: SocketKind;
    let isWorker: boolean;
    switch (path) {
      case "/ws/v2/worker/session":
        kind = "session";
        isWorker = true;
        break;
      case "/ws/v2/worker/attempt":
        kind = "attempt";
        isWorker = true;
        break;
      case "/ws/v2/client/invoke":
        kind = "invoke";
        isWorker = false;
        break;
      case "/ws/v2/client/watch":
        kind = "watch";
        isWorker = false;
        break;
      default:
        return grpcErrorResponse(GrpcCode.Unimplemented, `unknown websocket endpoint: ${path}`);
    }

    // Pre-upgrade authentication (HTTP 401/16, 403/7, 400/3)
    let principal: Principal;
    try {
      principal = isWorker
        ? this.auth.verifyWorkerHeaders(request.headers)
        : this.auth.verifyClientHeaders(request.headers);
    } catch (err) {
      if (err instanceof GrpcStatusError) {
        return grpcErrorResponse(err);
      }
      return grpcErrorResponse(GrpcCode.Internal, String(err));
    }

    if (request.headers.get("upgrade")?.toLowerCase() !== "websocket") {
      return grpcErrorResponse(GrpcCode.InvalidArgument, "expected WebSocket upgrade");
    }

    // Enforce concurrent connection caps (section 4.3: 256/256/128/256 + handshake-pending cap -> HTTP 429/8)
    const counts = this.countOpenSocketsByKind();
    if (counts.pendingHandshake >= this.config.maxPendingHandshakeSockets) {
      return grpcErrorResponse(
        GrpcCode.ResourceExhausted,
        `too many pending WebSocket handshakes (${counts.pendingHandshake})`,
      );
    }
    if (kind === "session" && counts.session >= this.config.maxSessionSockets) {
      return grpcErrorResponse(
        GrpcCode.ResourceExhausted,
        `worker session socket cap (${this.config.maxSessionSockets}) reached`,
      );
    }
    if (kind === "attempt" && counts.attempt >= this.config.maxAttemptSockets) {
      return grpcErrorResponse(
        GrpcCode.ResourceExhausted,
        `worker attempt socket cap (${this.config.maxAttemptSockets}) reached`,
      );
    }
    if (kind === "invoke") {
      if (counts.invoke >= this.config.maxInvokeSockets) {
        return grpcErrorResponse(
          GrpcCode.ResourceExhausted,
          `client invoke socket cap (${this.config.maxInvokeSockets}) reached`,
        );
      }
      const shedReason = this.store.checkNewJobAdmission(nowMs, 4096, false);
      if (shedReason !== null) {
        return grpcErrorResponse(GrpcCode.ResourceExhausted, shedReason);
      }
    }
    if (kind === "watch") {
      if (counts.watch >= this.config.maxWatchSockets) {
        return grpcErrorResponse(
          GrpcCode.ResourceExhausted,
          `client watch socket cap (${this.config.maxWatchSockets}) reached`,
        );
      }
      const readShed = this.store.checkReadAdmission(nowMs);
      if (readShed !== null) {
        return grpcErrorResponse(GrpcCode.ResourceExhausted, readShed);
      }
    }

    const connId = this.allocateConnId(nowMs);
    const pair = this.host.createWebSocketPair();
    const att: SocketAttachment = {
      kind,
      connId,
      id: "",
      namespace: principal.namespace,
      forcedPool: principal.forcedPool,
      handshakeDone: 0,
      openedAtMs: nowMs,
      lastSeenAtMs: nowMs,
      queued: 0,
      running: 0,
      utilization: 0,
      replay: 0,
    };
    writeSocketAttachment(pair.server, att);
    this.host.acceptWebSocket(pair.server, [kind]);
    this.socketsByConnId.set(connId, pair.server);

    // Worker session/attempt handshakes expire at heartbeatTimeoutMs if Hello/Open never arrives!
    if (kind === "session" || kind === "attempt") {
      await this.refreshAlarm(nowMs);
    }

    return pair.toUpgradeResponse();
  }

  private allocateConnId(_nowMs: number): number {
    this.connSeq += 1;
    const id = this.connSeq;
    this.store.transactionSync(() => {
      this.store.setMeta("conn_seq", String(id));
    });
    return id;
  }

  private async handleUnaryClientRpc(
    request: Request,
    path: string,
    nowMs: number,
  ): Promise<Response> {
    let principal: Principal;
    try {
      principal = this.auth.verifyClientHeaders(request.headers);
    } catch (err) {
      if (err instanceof GrpcStatusError) {
        return grpcErrorResponse(err);
      }
      return grpcErrorResponse(GrpcCode.Internal, String(err));
    }

    const contentLengthHeader = request.headers.get("content-length");
    if (contentLengthHeader) {
      const declaredLen = Number(contentLengthHeader);
      if (Number.isFinite(declaredLen) && declaredLen > HTTP_MAX_BODY_BYTES) {
        return grpcErrorResponse(
          GrpcCode.ResourceExhausted,
          `HTTP protobuf body exceeds ${HTTP_MAX_BODY_BYTES} bytes`,
        );
      }
    }

    const rawBody = new Uint8Array(await request.arrayBuffer());
    if (rawBody.byteLength > HTTP_MAX_BODY_BYTES) {
      return grpcErrorResponse(
        GrpcCode.ResourceExhausted,
        `HTTP protobuf body of ${rawBody.byteLength} bytes exceeds ${HTTP_MAX_BODY_BYTES} bytes`,
      );
    }

    try {
      switch (path) {
        case "/rpc/v2/client/submit": {
          const req = pb.decodeSubmitRequest(rawBody);
          const resp = await this.handleSubmitRpc(principal.namespace, req, nowMs);
          return this.protoOkResponse(pb.encodeSubmitResponse(resp));
        }
        case "/rpc/v2/client/get_job": {
          const readShed = this.store.checkReadAdmission(nowMs);
          if (readShed !== null) {
            throw GrpcStatusError.resourceExhausted(readShed);
          }
          const req = pb.decodeGetJobRequest(rawBody);
          const resp = await this.handleGetJobRpc(principal.namespace, req, nowMs);
          return this.protoOkResponse(pb.encodeJob(resp));
        }
        case "/rpc/v2/client/cancel": {
          const req = pb.decodeCancelRequest(rawBody);
          const resp = await this.handleCancelRpc(principal.namespace, req, nowMs);
          return this.protoOkResponse(pb.encodeJob(resp));
        }
        case "/rpc/v2/client/list_jobs": {
          const readShed = this.store.checkReadAdmission(nowMs);
          if (readShed !== null) {
            throw GrpcStatusError.resourceExhausted(readShed);
          }
          const req = pb.decodeListJobsRequest(rawBody);
          const resp = this.handleListJobsRpc(principal.namespace, req, nowMs);
          return this.protoOkResponse(pb.encodeListJobsResponse(resp));
        }
        default:
          return grpcErrorResponse(GrpcCode.Unimplemented, `unknown unary route: ${path}`);
      }
    } catch (err) {
      if (err instanceof pb.ProtoDecodeError) {
        return grpcErrorResponse(
          GrpcCode.InvalidArgument,
          `invalid protobuf request: ${err.message}`,
        );
      }
      if (err instanceof GrpcStatusError) {
        return grpcErrorResponse(err);
      }
      return grpcErrorResponse(
        GrpcCode.Internal,
        err instanceof Error ? err.message : String(err),
      );
    }
  }

  private protoOkResponse(bytes: Uint8Array): Response {
    const body = new ArrayBuffer(bytes.byteLength);
    new Uint8Array(body).set(bytes);
    return new Response(body, {
      status: 200,
      headers: { "Content-Type": PROTOBUF_CONTENT_TYPE },
    });
  }

  onSocketBufferDrain(ws: NativeWebSocket): void {
    this.isolateBudget.refreshSockets();
    const att = readSocketAttachment(ws);
    if (!att || ws.readyState !== WS_READY_STATE_OPEN) {
      return;
    }
    if (att.kind === "attempt" && att.id && att.replay === 0) {
      const lease = this.leases.get(att.id);
      if (
        lease &&
        !lease.cancelling &&
        lease.attemptConnId === att.connId &&
        lease.inputsViaAttempt &&
        lease.inputStage < 4
      ) {
        void this.pumpAttemptInputs(lease.leaseId);
      }
    } else if (att.kind === "invoke" && att.id) {
      const job = this.jobs.get(att.id);
      if (job) {
        job.stalledSinceMs = null;
        void this.deliverPrimaryInvokeEvents(job.scopedJobId);
      }
    } else if (att.kind === "watch" && att.id) {
      void this.deliverWatcherEvents(att.connId);
    }
  }

  async webSocketMessage(
    ws: NativeWebSocket,
    message: ArrayBuffer | Uint8Array | string,
  ): Promise<void> {
    const nowMs = this.host.nowMs();
    this.store.recordWsInboundMessage(nowMs);

    const att = readSocketAttachment(ws);
    if (!att) {
      ws.close(1000, "");
      return;
    }

    if (typeof message === "string") {
      this.sendWsErrorAndClose(ws, GrpcCode.InvalidArgument, "expected binary protobuf frame");
      return;
    }

    const bytes =
      message instanceof Uint8Array ? message : new Uint8Array(message);

    // Application-level encoded length check at 32 MiB (33,554,432 bytes)
    if (bytes.byteLength > WS_MAX_ENCODED_BYTES) {
      this.sendWsErrorAndClose(
        ws,
        GrpcCode.ResourceExhausted,
        `WebSocket frame of ${bytes.byteLength} bytes exceeds ${WS_MAX_ENCODED_BYTES} bytes`,
      );
      return;
    }

    // Fast path for Worker Heartbeat: pure synchronous attachment-only update,
    // ZERO SQL reads/writes, ZERO setAlarm calls!
    if (att.kind === "session" && att.handshakeDone === 1) {
      if (bytes.byteLength > this.config.maxControlMessageBytes) {
        this.sendWsErrorAndClose(
          ws,
          GrpcCode.InvalidArgument,
          `control message of ${bytes.byteLength} bytes exceeds ${this.config.maxControlMessageBytes}`,
        );
        return;
      }
      // Peek if it's a Heartbeat before queuing async work
      let decodedControl: pb.WorkerMessage | null = null;
      try {
        decodedControl = pb.decodeWorkerMessage(bytes);
      } catch (err) {
        this.sendWsErrorAndClose(
          ws,
          GrpcCode.InvalidArgument,
          `invalid protobuf WorkerMessage: ${err instanceof Error ? err.message : String(err)}`,
        );
        return;
      }
      if (!decodedControl.msg || decodedControl.msg.case === "heartbeat") {
        this.handleSessionHeartbeatSync(
          ws,
          att,
          decodedControl.msg?.case === "heartbeat"
            ? decodedControl.msg.value
            : undefined,
          nowMs,
        );
        return;
      }
    }

    // Serialize per-connection async execution so pipelined chunks + Finish execute in exact wire order
    // AND webSocketMessage never resolves before SQLite persistence completes!
    const prevChain = this.socketChains.get(att.connId) ?? Promise.resolve();
    const currentTask = prevChain.then(async () => {
      if (ws.readyState !== WS_READY_STATE_OPEN) {
        return;
      }
      const freshAtt = readSocketAttachment(ws);
      if (!freshAtt) {
        return;
      }
      const stepNow = this.host.nowMs();
      await this.dispatchSocketMessage(ws, freshAtt, bytes, stepNow);
    });
    const safeChain = currentTask.catch(() => {});
    this.socketChains.set(att.connId, safeChain);
    try {
      await currentTask;
    } finally {
      if (this.socketChains.get(att.connId) === safeChain) {
        this.socketChains.delete(att.connId);
      }
    }
  }

  /**
   * Close handling (section 5.3):
   * - `1005` (no status code from peer): complete handshake with plain `ws.close()` (never pass 1005/1006!)
   * - `1006` (abnormal loss, no Close frame): run loss/cancel/reconnect logic, never echo!
   */
  async webSocketClose(
    ws: NativeWebSocket,
    code: number,
    _reason: string,
    _wasClean: boolean,
  ): Promise<void> {
    const nowMs = this.host.nowMs();
    this.isolateBudget.releaseSocket(ws);
    const att = readSocketAttachment(ws);
    if (att) {
      this.socketsByConnId.delete(att.connId);
      this.replayStates.delete(att.connId);
    }

    // Complete close handshake cleanly for peer 1005 (plain close without reserved status code)
    if (code === 1005 && ws.readyState === WS_READY_STATE_OPEN) {
      try {
        ws.close();
      } catch {
        // ignore
      }
    }

    if (!att || att.handshakeDone === 0 || !att.id) {
      return;
    }

    let needsAlarm = false;
    if (att.kind === "session") {
      const session = this.sessions.get(att.id);
      // Late close of a replaced connection (`att.connId !== session.currentConnId`) is IGNORED!
      if (session && session.currentConnId === att.connId && session.connected) {
        this.store.transactionSync(() => {
          this.disconnectSessionInTxn(session.sessionId, att.connId, nowMs);
        });
        needsAlarm = true;
      }
    } else if (att.kind === "attempt") {
      const lease = this.leases.get(att.id);
      if (lease && lease.attemptConnId === att.connId) {
        lease.attemptConnId = null;
        this.store.transactionSync(() => {
          this.store.exec(
            `UPDATE native_active_leases SET attempt_conn_id = NULL
             WHERE lease_id = ? AND attempt_conn_id = ?`,
            lease.leaseId,
            att.connId,
          );
        });
      }
    } else if (att.kind === "invoke") {
      const job = this.jobs.get(att.id);
      if (
        job &&
        job.callerConnId === att.connId &&
        (job.state === "queued" || job.state === "running")
      ) {
        this.store.transactionSync(() => {
          this.cancelJobInTxn(job.scopedJobId, "caller disconnected", nowMs);
          this.dispatchInTxn(nowMs);
        });
        needsAlarm = true;
      }
    } else if (att.kind === "watch") {
      if (this.watchers.has(att.connId)) {
        this.store.transactionSync(() => {
          this.removeWatcherInTxn(att.connId);
        });
      }
    }

    if (needsAlarm) {
      await this.refreshAlarm(nowMs);
    }
  }

  // ---------------------------------------------------------------------------
  // Alarm & Timer Scheduling (Section 6.3)
  // ---------------------------------------------------------------------------

  async alarm(): Promise<void> {
    const nowMs = this.host.nowMs();
    // Clear in-memory marker FIRST (section 6.3)
    this.armedAlarmMs = null;
    this.store.recordHttpOrAlarmRequest(nowMs);
    this.isolateBudget.refreshSockets();

    // 1. Check live socket attachments for handshake timeouts & silent session heartbeat timeouts
    const silentSessions: Array<{ sessionId: string; connId: number; ws: NativeWebSocket }> = [];
    const expiredHandshakes: Array<{ ws: NativeWebSocket; kind: SocketKind; connId: number }> = [];

    for (const [connId, ws] of this.socketsByConnId) {
      if (ws.readyState !== WS_READY_STATE_OPEN) {
        this.socketsByConnId.delete(connId);
        continue;
      }
      const att = readSocketAttachment(ws);
      if (!att) continue;
      if (att.handshakeDone === 0 && (att.kind === "session" || att.kind === "attempt")) {
        if (nowMs - att.openedAtMs >= this.config.heartbeatTimeoutMs) {
          expiredHandshakes.push({ ws, kind: att.kind, connId });
        }
      } else if (att.kind === "session" && att.handshakeDone === 1) {
        const session = this.sessions.get(att.id);
        if (session && session.connected && session.currentConnId === connId) {
          session.lastSeenMs = Math.max(session.lastSeenMs, att.lastSeenAtMs);
          session.queued = att.queued;
          session.running = att.running;
          session.utilization = att.utilization;
          if (nowMs - session.lastSeenMs >= this.config.heartbeatTimeoutMs) {
            silentSessions.push({ sessionId: session.sessionId, connId, ws });
          }
        }
      }
    }

    for (const h of expiredHandshakes) {
      this.socketsByConnId.delete(h.connId);
      this.sendWsErrorAndClose(
        h.ws,
        GrpcCode.DeadlineExceeded,
        h.kind === "session" ? "no Hello received" : "no Open received",
      );
    }

    for (const s of silentSessions) {
      this.socketsByConnId.delete(s.connId);
      try {
        s.ws.close(1000, "heartbeat timeout");
      } catch {
        // ignore
      }
    }

    // 2. Process exact business timers + overdue retries + dispatch inside bounded transactionSync
    const notifyJobs = new Set<string>();
    this.store.transactionSync(() => {
      for (const s of silentSessions) {
        this.disconnectSessionInTxn(s.sessionId, s.connId, nowMs);
      }

      // Session reconnect_grace expiry
      for (const session of [...this.sessions.values()]) {
        if (
          !session.connected &&
          session.graceDeadlineMs !== null &&
          session.graceDeadlineMs <= nowMs
        ) {
          const affected = this.expireDisconnectedSessionInTxn(
            session.sessionId,
            session.currentConnId,
            nowMs,
          );
          for (const jid of affected) notifyJobs.add(jid);
        }
      }

      // Lease cancel_grace expiry
      for (const lease of [...this.leases.values()]) {
        if (
          lease.cancelling &&
          lease.cancelDeadlineMs !== null &&
          lease.cancelDeadlineMs <= nowMs
        ) {
          const sessionId = lease.sessionId;
          this.releaseLeaseInTxn(lease.leaseId, nowMs);
          this.sendControlMessageToSession(sessionId, {
            msg: {
              case: "fence",
              value: {
                leaseId: lease.leaseId,
                reason: "cancel_not_acknowledged",
              },
            },
          }, nowMs);
        }
      }

      // Job queue / execution / total / consumer_stall deadlines
      let handled = 0;
      for (const job of [...this.jobs.values()]) {
        if (handled >= 128) break;
        if (job.state !== "queued" && job.state !== "running") {
          continue;
        }
        if (job.totalDeadlineMs !== null && job.totalDeadlineMs <= nowMs) {
          this.failJobInTxn(
            job.scopedJobId,
            RUNTIME_CODES.TOTAL_TIMEOUT,
            "job exceeded its total timeout",
            nowMs,
          );
          notifyJobs.add(job.scopedJobId);
          handled += 1;
          continue;
        }
        if (
          job.state === "queued" &&
          job.queueDeadlineMs !== null &&
          job.queueDeadlineMs <= nowMs
        ) {
          this.failJobInTxn(
            job.scopedJobId,
            RUNTIME_CODES.QUEUE_TIMEOUT,
            "no worker took the job within its queue timeout",
            nowMs,
          );
          notifyJobs.add(job.scopedJobId);
          handled += 1;
          continue;
        }
        if (job.state === "running" && job.currentLeaseId) {
          const lease = this.leases.get(job.currentLeaseId);
          if (
            lease &&
            !lease.cancelling &&
            lease.executionDeadlineMs !== null &&
            lease.executionDeadlineMs <= nowMs
          ) {
            this.endAttemptWithErrorInTxn(
              job.scopedJobId,
              {
                code: RUNTIME_CODES.EXECUTION_TIMEOUT,
                message: "attempt exceeded its execution timeout",
                retryable: true,
              },
              { mode: "cancel", reason: RUNTIME_CODES.EXECUTION_TIMEOUT },
              null,
              nowMs,
            );
            notifyJobs.add(job.scopedJobId);
            handled += 1;
            continue;
          }
          if (
            job.stalledSinceMs !== null &&
            nowMs - job.stalledSinceMs >= this.config.consumerStallTimeoutMs
          ) {
            this.failJobInTxn(
              job.scopedJobId,
              RUNTIME_CODES.CONSUMER_STALLED,
              `consumer did not read output for ${Math.floor(this.config.consumerStallTimeoutMs / 1000)}s`,
              nowMs,
            );
            notifyJobs.add(job.scopedJobId);
            handled += 1;
            continue;
          }
        }
      }

      // Receipt retention sweep (10 min default)
      const receiptCutoff = nowMs - this.config.receiptRetentionMs;
      this.store.exec(
        `DELETE FROM native_receipts WHERE created_at_ms <= ?`,
        receiptCutoff,
      );

      // Terminal job retention purge (24h default, bounded batch)
      this.purgeExpiredTerminalJobsInTxn(nowMs, 64);

      // Scan & dispatch all runnable queued jobs (including overdue retry_at_ms & unlocked spillovers!)
      const dispatched = this.dispatchInTxn(nowMs, notifyJobs);
      void dispatched;
    });

    // 3. Sweep R2 GC outside transactionSync
    await this.store.sweepR2Gc(nowMs, 32);

    // 4. Deliver pending events to Invoke/Watch subscribers for any affected jobs
    for (const jid of notifyJobs) {
      await this.notifyJobFollowers(jid);
    }

    // 5. Re-arm next earliest due alarm
    await this.refreshAlarm(nowMs);
  }

  private computeNextAlarmDueMs(nowMs: number): number | null {
    let earliestBusiness: number | null = null;
    const considerBusiness = (atMs: number | null | undefined) => {
      if (atMs === null || atMs === undefined) return;
      const clamped = Math.max(nowMs, atMs);
      if (earliestBusiness === null || clamped < earliestBusiness) {
        earliestBusiness = clamped;
      }
    };

    // Exact business timers (NEVER bucketed!)
    for (const [connId, ws] of this.socketsByConnId) {
      if (ws.readyState !== WS_READY_STATE_OPEN) continue;
      const att = readSocketAttachment(ws);
      if (att && att.handshakeDone === 0 && (att.kind === "session" || att.kind === "attempt")) {
        considerBusiness(att.openedAtMs + this.config.heartbeatTimeoutMs);
      } else {
        void connId;
      }
    }

    for (const s of this.sessions.values()) {
      if (!s.connected && s.graceDeadlineMs !== null) {
        considerBusiness(s.graceDeadlineMs);
      }
      if (s.connected && s.cooldownUntilMs > nowMs) {
        considerBusiness(s.cooldownUntilMs);
      }
    }

    for (const l of this.leases.values()) {
      if (l.cancelling && l.cancelDeadlineMs !== null) {
        considerBusiness(l.cancelDeadlineMs);
      } else if (!l.cancelling && l.executionDeadlineMs !== null) {
        considerBusiness(l.executionDeadlineMs);
      }
    }

    for (const j of this.jobs.values()) {
      if (j.state !== "queued" && j.state !== "running") {
        continue;
      }
      considerBusiness(j.totalDeadlineMs);
      if (j.state === "queued") {
        considerBusiness(j.queueDeadlineMs);
        if (j.notBeforeMs > nowMs) {
          considerBusiness(j.notBeforeMs);
        }
        if (j.spilloverAtMs !== null && j.spilloverAtMs > nowMs) {
          considerBusiness(j.spilloverAtMs);
        }
        const ifacePolicy = this.config.interfaces[j.interfaceName];
        if (ifacePolicy) {
          for (const tier of ifacePolicy.poolTiers) {
            const unlockAt = j.queuedSinceMs + tier.spilloverAfterMs;
            if (tier.spilloverAfterMs > 0 && unlockAt > nowMs) {
              considerBusiness(unlockAt);
            }
          }
        }
      } else if (j.stalledSinceMs !== null) {
        considerBusiness(j.stalledSinceMs + this.config.consumerStallTimeoutMs);
      }
    }

    // Background maintenance (>= 5s bucket): schedule connected-session liveness
    // checks at `effectiveLastSeen + heartbeatTimeoutMs` (bucketed to maintenanceBucketMs,
    // or exact when heartbeatTimeoutMs < maintenanceBucketMs) rather than waking every 5s!
    const b = this.config.maintenanceBucketMs;
    let maintenanceDue: number | null = null;
    const considerMaintenance = (rawAtMs: number) => {
      const bucketed = Math.ceil(Math.max(nowMs + 1, rawAtMs) / b) * b;
      if (maintenanceDue === null || bucketed < maintenanceDue) {
        maintenanceDue = bucketed;
      }
    };

    for (const s of this.sessions.values()) {
      if (s.connected) {
        const ws = this.socketsByConnId.get(s.currentConnId);
        const att =
          ws && ws.readyState === WS_READY_STATE_OPEN
            ? readSocketAttachment(ws)
            : null;
        const effectiveLastSeen =
          att && att.kind === "session" && att.connId === s.currentConnId
            ? Math.max(s.lastSeenMs, att.lastSeenAtMs)
            : s.lastSeenMs;
        const rawTimeoutAt = effectiveLastSeen + this.config.heartbeatTimeoutMs;
        if (this.config.heartbeatTimeoutMs < b) {
          considerBusiness(rawTimeoutAt);
        } else {
          considerMaintenance(rawTimeoutAt);
        }
      }
    }

    const r2GcDueMs = this.store.nextR2GcDueMs();
    if (r2GcDueMs !== null) {
      considerMaintenance(r2GcDueMs);
    }

    if (maintenanceDue === null && this.jobs.size > 0) {
      // Schedule retention purge for retained terminal jobs
      for (const j of this.jobs.values()) {
        if (
          j.state === "succeeded" ||
          j.state === "failed" ||
          j.state === "cancelled"
        ) {
          considerMaintenance(j.updatedAtMs + this.config.terminalRetentionMs);
        }
      }
    }

    if (earliestBusiness !== null && maintenanceDue !== null) {
      return Math.min(earliestBusiness, maintenanceDue);
    }
    return earliestBusiness ?? maintenanceDue;
  }

  private async refreshAlarm(nowMs: number): Promise<void> {
    const due = this.computeNextAlarmDueMs(nowMs);
    if (due === null) {
      return;
    }
    // Re-arm when missing or when a business transition created an earlier due time!
    if (this.armedAlarmMs !== null && this.armedAlarmMs <= due && this.armedAlarmMs > nowMs) {
      return;
    }
    this.armedAlarmMs = due;
    this.store.recordAlarmSet(nowMs);
    try {
      await this.host.setAlarm(due);
    } catch {
      // If interrupted before setAlarm completes (tested in section 4.6 / 10!), clear marker;
      // rehydrateAfterWake() and alarm() scan overdue retry_at_ms on next wake!
      this.armedAlarmMs = null;
    }
  }

  // ---------------------------------------------------------------------------
  // WebSocket Message Dispatch by Socket Kind
  // ---------------------------------------------------------------------------

  private async dispatchSocketMessage(
    ws: NativeWebSocket,
    att: SocketAttachment,
    bytes: Uint8Array,
    nowMs: number,
  ): Promise<void> {
    try {
      switch (att.kind) {
        case "session":
          await this.handleSessionSocketMessage(ws, att, bytes, nowMs);
          break;
        case "attempt":
          await this.handleAttemptSocketMessage(ws, att, bytes, nowMs);
          break;
        case "invoke":
          await this.handleInvokeSocketMessage(ws, att, bytes, nowMs);
          break;
        case "watch":
          await this.handleWatchSocketMessage(ws, att, bytes, nowMs);
          break;
      }
    } catch (err) {
      if (err instanceof pb.ProtoDecodeError) {
        this.sendWsErrorAndClose(
          ws,
          GrpcCode.InvalidArgument,
          `invalid protobuf frame: ${err.message}`,
        );
      } else if (err instanceof GrpcStatusError) {
        this.sendWsErrorAndClose(ws, err.code, err.message);
      } else {
        this.sendWsErrorAndClose(
          ws,
          GrpcCode.Internal,
          err instanceof Error ? err.message : String(err),
        );
      }
    }
  }

  // ---------------------------------------------------------------------------
  // 1. Worker Control Session (`/ws/v2/worker/session`)
  // ---------------------------------------------------------------------------

  private handleSessionHeartbeatSync(
    ws: NativeWebSocket,
    att: SocketAttachment,
    hb: pb.Heartbeat | undefined,
    nowMs: number,
  ): void {
    const session = this.sessions.get(att.id);
    if (!session || !session.connected || session.currentConnId !== att.connId) {
      return;
    }
    session.lastSeenMs = nowMs;
    if (hb?.load) {
      session.queued = hb.load.queued;
      session.running = hb.load.running;
      session.utilization = Number.isFinite(hb.load.utilization)
        ? hb.load.utilization
        : 0;
      const bounded = boundCustomLoad(hb.load.custom);
      session.customLoad = bounded.custom;
      this.droppedCustomLoadEntries += bounded.dropped;
    }
    writeSocketAttachment(ws, {
      ...att,
      lastSeenAtMs: nowMs,
      queued: session.queued,
      running: session.running,
      utilization: session.utilization,
    });
  }

  private async handleSessionSocketMessage(
    ws: NativeWebSocket,
    att: SocketAttachment,
    bytes: Uint8Array,
    nowMs: number,
  ): Promise<void> {
    if (bytes.byteLength > this.config.maxControlMessageBytes) {
      throw GrpcStatusError.invalidArgument(
        `control message of ${bytes.byteLength} bytes exceeds ${this.config.maxControlMessageBytes}`,
      );
    }
    const decoded = pb.decodeWorkerMessage(bytes);
    if (att.handshakeDone === 0) {
      if (decoded.msg?.case !== "hello") {
        throw GrpcStatusError.invalidArgument("a session must start with Hello");
      }
      await this.handleSessionHello(ws, att, decoded.msg.value, nowMs);
      return;
    }

    if (!decoded.msg) {
      return;
    }
    if (decoded.msg.case === "hello") {
      throw GrpcStatusError.invalidArgument("Hello may only open a session");
    }
    if (decoded.msg.case === "heartbeat") {
      this.handleSessionHeartbeatSync(ws, att, decoded.msg.value, nowMs);
      return;
    }

    const session = this.sessions.get(att.id);
    // Stale connection check: ignore messages from superseded control connections!
    if (!session || !session.connected || session.currentConnId !== att.connId) {
      return;
    }

    session.lastSeenMs = nowMs;
    writeSocketAttachment(ws, { ...att, lastSeenAtMs: nowMs });

    const notifyJobs = new Set<string>();
    const msg = decoded.msg;
    this.store.transactionSync(() => {
      let mayFreeSlot = false;
      switch (msg.case) {
        case "credit": {
          session.reportedFree = msg.value.freeSlots;
          session.assignedSinceReport = 0;
          this.persistSessionRowInTxn(session);
          mayFreeSlot = true;
          break;
        }
        case "draining": {
          session.draining = true;
          this.persistSessionRowInTxn(session);
          if (msg.value.preempting) {
            for (const leaseId of [...session.activeLeases]) {
              const lease = this.leases.get(leaseId);
              if (!lease || lease.cancelling) continue;
              const job = this.jobs.get(lease.scopedJobId);
              if (
                job &&
                job.state === "running" &&
                job.currentLeaseId === leaseId &&
                !lease.exposed
              ) {
                this.endAttemptWithErrorInTxn(
                  job.scopedJobId,
                  {
                    code: RUNTIME_CODES.WORKER_PREEMPTED,
                    message: "worker preempted while draining",
                    retryable: true,
                  },
                  { mode: "cancel", reason: RUNTIME_CODES.WORKER_PREEMPTED },
                  null,
                  nowMs,
                );
                notifyJobs.add(job.scopedJobId);
                mayFreeSlot = true;
              }
            }
          }
          break;
        }
        case "cancelAck": {
          const lease = this.leases.get(msg.value.leaseId);
          if (lease && lease.sessionId === session.sessionId && lease.cancelling) {
            this.releaseLeaseInTxn(lease.leaseId, nowMs);
            mayFreeSlot = true;
          }
          break;
        }
        case "assignReply": {
          const lease = this.leases.get(msg.value.leaseId);
          if (!lease || lease.sessionId !== session.sessionId || lease.cancelling) {
            // Stale lease reply is ignored cleanly (matches http_ws_service.rs line 469)
            break;
          }
          const job = this.jobs.get(lease.scopedJobId);
          if (!job || job.currentLeaseId !== lease.leaseId || job.state !== "running") {
            break;
          }
          if (msg.value.accepted) {
            if (!lease.accepted) {
              lease.accepted = true;
              this.store.exec(
                `UPDATE native_active_leases SET accepted = 1 WHERE lease_id = ?`,
                lease.leaseId,
              );
            }
          } else {
            job.rejectedSessions.add(session.sessionId);
            const reason = msg.value.rejectReason || "worker rejected the assignment";
            this.endAttemptWithErrorInTxn(
              job.scopedJobId,
              {
                code: RUNTIME_CODES.WORKER_REJECTED,
                message: reason,
                retryable: true,
              },
              { mode: "release" },
              null,
              nowMs,
            );
            notifyJobs.add(job.scopedJobId);
            mayFreeSlot = true;
          }
          break;
        }
      }
      if (mayFreeSlot) {
        this.dispatchInTxn(nowMs, notifyJobs);
      }
    });

    for (const jid of notifyJobs) {
      await this.notifyJobFollowers(jid);
    }
    await this.refreshAlarm(nowMs);
  }

  private async handleSessionHello(
    ws: NativeWebSocket,
    att: SocketAttachment,
    hello: pb.Hello,
    nowMs: number,
  ): Promise<void> {
    if (hello.interfaces.length === 0) {
      throw GrpcStatusError.invalidArgument("Hello.interfaces is required");
    }
    if (
      hello.interfaces.length > MAX_OBJECTS ||
      hello.interfaces.some((i) => i.length === 0 || utf8Len(i) > MAX_IDENTIFIER_BYTES) ||
      utf8Len(hello.workerId) > MAX_IDENTIFIER_BYTES ||
      utf8Len(hello.resumeSessionId) > MAX_IDENTIFIER_BYTES ||
      utf8Len(hello.pool) > MAX_IDENTIFIER_BYTES
    ) {
      throw GrpcStatusError.invalidArgument(
        `Hello identifiers must be 1..=${MAX_IDENTIFIER_BYTES} bytes and at most ${MAX_OBJECTS} interfaces`,
      );
    }
    if (hello.heldLeaseIds.length > MAX_OBJECTS) {
      throw GrpcStatusError.invalidArgument("too many held leases");
    }
    const ifaceBytes = hello.interfaces.reduce((s, i) => s + utf8Len(i), 0);
    if (
      metadataByteLen(hello.attributes) + metadataByteLen(hello.metadata) + ifaceBytes >
      MAX_METADATA_BYTES
    ) {
      throw GrpcStatusError.invalidArgument(
        `worker attributes and metadata exceed ${MAX_METADATA_BYTES} bytes`,
      );
    }

    const workerId = hello.workerId.trim() || crypto.randomUUID();
    const interfaces = [...new Set(hello.interfaces)].sort();
    const maxConcurrency = Math.max(1, hello.maxConcurrency);
    const namespace = att.namespace;
    const pool = att.forcedPool ?? hello.pool;

    const existingSession = hello.resumeSessionId
      ? this.sessions.get(hello.resumeSessionId)
      : undefined;
    const isResume = Boolean(
      existingSession &&
        existingSession.workerId === workerId &&
        existingSession.namespace === namespace,
    );

    if (!isResume && this.sessions.size >= this.config.maxSessionSockets) {
      throw GrpcStatusError.resourceExhausted("worker session limit reached");
    }

    const sessionId = isResume ? existingSession!.sessionId : crypto.randomUUID();
    const oldConnIdToClose =
      isResume && existingSession!.connected && existingSession!.currentConnId !== att.connId
        ? existingSession!.currentConnId
        : null;

    const resumedLeaseIds: string[] = [];
    const fencedLeaseIds: string[] = [];
    const notifyJobs = new Set<string>();

    // Update attachment BEFORE Welcome/Assign sends
    const updatedAtt: SocketAttachment = {
      ...att,
      id: sessionId,
      handshakeDone: 1,
      lastSeenAtMs: nowMs,
    };
    writeSocketAttachment(ws, updatedAtt);

    this.store.transactionSync(() => {
      let session: SessionMem;
      if (isResume && existingSession) {
        this.unindexSessionInMemory(existingSession);
        session = existingSession;
        session.pool = pool;
        session.forcedPool = att.forcedPool;
        session.interfaces = interfaces;
        session.attributes = { ...hello.attributes };
        session.metadata = { ...hello.metadata };
        session.totalCapacity = { ...hello.totalCapacity };
        session.maxConcurrency = maxConcurrency;
        session.reportedFree = maxConcurrency;
        session.assignedSinceReport = 0;
        session.draining = false;
        session.connected = true;
        session.currentConnId = att.connId;
        session.graceDeadlineMs = null;
        session.lastSeenMs = nowMs;
      } else {
        session = {
          sessionId,
          workerId,
          namespace,
          pool,
          forcedPool: att.forcedPool,
          interfaces,
          attributes: { ...hello.attributes },
          metadata: { ...hello.metadata },
          totalCapacity: { ...hello.totalCapacity },
          reservedCapacity: {},
          activeLeases: new Set(),
          maxConcurrency,
          reportedFree: maxConcurrency,
          assignedSinceReport: 0,
          draining: false,
          connected: true,
          currentConnId: att.connId,
          connectedAtMs: nowMs,
          lastSeenMs: nowMs,
          graceDeadlineMs: null,
          consecutiveFailures: 0,
          cooldownUntilMs: 0,
          queued: 0,
          running: 0,
          utilization: 0,
          customLoad: {},
        };
        this.sessions.set(sessionId, session);
      }
      this.indexSessionInMemory(session);

      // Validate held_lease_ids (must belong to same worker_id AND namespace in current generation and !cancelling)
      const heldSet = new Set(hello.heldLeaseIds);
      for (const leaseId of heldSet) {
        const lease = this.leases.get(leaseId);
        const owner = lease ? this.sessions.get(lease.sessionId) : undefined;
        const movable = Boolean(
          lease &&
            !lease.cancelling &&
            lease.generation === this.generation &&
            owner &&
            owner.workerId === workerId &&
            owner.namespace === namespace,
        );
        if (!movable || !lease) {
          fencedLeaseIds.push(leaseId);
          continue;
        }
        if (lease.sessionId !== sessionId) {
          const prevOwner = this.sessions.get(lease.sessionId);
          if (prevOwner) {
            this.detachLeaseFromSessionMem(prevOwner, leaseId, lease.reservedCapacity);
            this.persistSessionRowInTxn(prevOwner);
          }
          lease.sessionId = sessionId;
          this.attachLeaseToSessionMem(session, leaseId, lease.reservedCapacity);
          this.store.exec(
            `UPDATE native_active_leases SET session_id = ? WHERE lease_id = ?`,
            sessionId,
            leaseId,
          );
        }
        resumedLeaseIds.push(leaseId);
      }

      // Leases previously on this session that the worker no longer holds are lost!
      const forgotten = [...session.activeLeases].filter((lid) => !heldSet.has(lid));
      for (const lid of forgotten) {
        const jid = this.leaseLostInTxn(lid, nowMs);
        if (jid) notifyJobs.add(jid);
      }

      this.persistSessionRowInTxn(session);
    });

    if (oldConnIdToClose !== null) {
      const oldWs = this.socketsByConnId.get(oldConnIdToClose);
      this.socketsByConnId.delete(oldConnIdToClose);
      if (oldWs && oldWs.readyState === WS_READY_STATE_OPEN) {
        try {
          oldWs.close(1000, "session replaced");
        } catch {
          // ignore
        }
      }
    }

    // Section 4.2 & 5.3: Send Welcome FIRST, then Fence for invalid held leases, BEFORE any Assign!
    this.sendWsProtoSync(
      ws,
      pb.encodeServerMessage({
        msg: {
          case: "welcome",
          value: {
            sessionId,
            generation: this.generation,
            heartbeatIntervalMs: this.config.heartbeatIntervalMs,
            heartbeatTimeoutMs: this.config.heartbeatTimeoutMs,
            limits: {
              maxControlMessageBytes: this.config.maxControlMessageBytes,
              maxChunkBytes: this.config.maxChunkBytes,
              attemptWindowBytes: this.config.attemptWindowBytes,
              assignInlineLimit: this.config.assignInlineLimit,
            },
            resumedLeaseIds,
          },
        },
      }),
    );

    for (const fencedId of fencedLeaseIds) {
      this.sendWsProtoSync(
        ws,
        pb.encodeServerMessage({
          msg: {
            case: "fence",
            value: {
              leaseId: fencedId,
              reason: "lease is not held by this worker in the current generation",
            },
          },
        }),
      );
    }

    // Now dispatch queued work to this newly ready session!
    this.store.transactionSync(() => {
      this.dispatchInTxn(nowMs, notifyJobs);
    });

    for (const jid of notifyJobs) {
      await this.notifyJobFollowers(jid);
    }
    await this.refreshAlarm(nowMs);
  }

  // ---------------------------------------------------------------------------
  // 2. Worker Attempt Data Stream (`/ws/v2/worker/attempt`)
  // ---------------------------------------------------------------------------

  private async handleAttemptSocketMessage(
    ws: NativeWebSocket,
    att: SocketAttachment,
    bytes: Uint8Array,
    nowMs: number,
  ): Promise<void> {
    const up = pb.decodeAttemptUp(bytes);
    if (att.handshakeDone === 0) {
      if (up.msg?.case !== "open") {
        throw GrpcStatusError.invalidArgument("an attempt stream must start with Open");
      }
      await this.handleAttemptOpen(ws, att, up.msg.value, nowMs);
      return;
    }

    if (!up.msg) {
      return;
    }
    if (up.msg.case === "open") {
      throw GrpcStatusError.invalidArgument("Open was already received");
    }

    if (att.replay === 1) {
      await this.handleReplayAttemptMessage(ws, att, up.msg, nowMs);
      return;
    }

    switch (up.msg.case) {
      case "frame":
        await this.handleLiveAttemptFrame(ws, att, up.msg.value, nowMs);
        break;
      case "result":
        await this.handleLiveAttemptResultChunk(ws, att, up.msg.value, nowMs);
        break;
      case "finish":
        await this.handleLiveAttemptFinish(ws, att, up.msg.value, nowMs);
        break;
    }
  }

  private lookupRecordedReceiptSync(leaseId: string, namespace: string): string | null {
    // 1. Check 10-min native_receipts table by (lease_id, namespace)
    const rec = this.store.exec<{ outcome_digest: string }>(
      `SELECT outcome_digest FROM native_receipts WHERE lease_id = ? AND namespace = ?`,
      leaseId,
      namespace,
    )[0];
    if (rec) {
      return rec.outcome_digest;
    }
    // 2. Check durable winner on native_jobs by (winning_lease_id, namespace) — lives for full 24h job retention!
    const win = this.store.exec<{ result_digest: string | null }>(
      `SELECT result_digest FROM native_jobs WHERE winning_lease_id = ? AND namespace = ?`,
      leaseId,
      namespace,
    )[0];
    if (win && win.result_digest !== null) {
      return win.result_digest;
    }
    return null;
  }

  private async handleAttemptOpen(
    ws: NativeWebSocket,
    att: SocketAttachment,
    open: pb.AttemptOpen,
    nowMs: number,
  ): Promise<void> {
    const lease = this.leases.get(open.leaseId);
    const isLive = Boolean(
      lease &&
        !lease.cancelling &&
        open.generation === this.generation &&
        lease.namespace === att.namespace &&
        this.jobs.get(lease.scopedJobId)?.currentLeaseId === lease.leaseId,
    );

    if (isLive && lease) {
      const prevAttemptConnId = lease.attemptConnId;
      // Section 4.4: A new AttemptOpen (new attempt_conn_id) discards uncommitted tail & segments!
      this.store.transactionSync(() => {
        const segRows = this.store.exec<{ r2_key: string | null }>(
          `SELECT r2_key FROM native_upload_segments WHERE lease_id = ? AND r2_key IS NOT NULL`,
          lease.leaseId,
        );
        const r2Keys = segRows.map((r) => r.r2_key!).filter(Boolean);
        if (r2Keys.length > 0) {
          this.store.queuePendingDeleteInTxn(r2Keys, nowMs);
        }
        this.store.exec(`DELETE FROM native_upload_tail WHERE lease_id = ?`, lease.leaseId);
        this.store.exec(`DELETE FROM native_upload_segments WHERE lease_id = ?`, lease.leaseId);

        this.totalResultBytesHeld = Math.max(
          0,
          this.totalResultBytesHeld - lease.totalResultBytes,
        );
        lease.attemptConnId = att.connId;
        lease.accepted = true;
        lease.inputStage = lease.inputsViaAttempt ? 0 : 4;
        lease.inputItemIdx = 0;
        lease.inputByteOff = 0;
        lease.inputSeq = 0n;
        lease.curObjIndex = null;
        lease.curObjMeta = {};
        lease.curObjUri = "";
        lease.curObjBytes = 0;
        lease.nextObjIndex = 0;
        lease.tailBytes = 0;
        lease.tailNextChunkSeq = 0;
        lease.nextSegIndex = 0;
        lease.totalResultBytes = 0;
        lease.ackedResultBytes = 0;
        lease.resultTooLarge = false;
        lease.completedObjs = [];
        lease.inlineResultBlob = new Uint8Array(0);
        this.persistLeaseRowInTxn(lease);
      });

      if (prevAttemptConnId !== null && prevAttemptConnId !== att.connId) {
        const oldWs = this.socketsByConnId.get(prevAttemptConnId);
        this.socketsByConnId.delete(prevAttemptConnId);
        if (oldWs && oldWs.readyState === WS_READY_STATE_OPEN) {
          try {
            oldWs.close(1000, "attempt reopened");
          } catch {
            // ignore
          }
        }
      }

      writeSocketAttachment(ws, {
        ...att,
        id: lease.leaseId,
        handshakeDone: 1,
        lastSeenAtMs: nowMs,
        replay: 0,
      });

      // Section 5.3: AttemptOpened MUST precede any InputData!
      this.sendWsProtoSync(
        ws,
        pb.encodeAttemptDown({
          msg: {
            case: "opened",
            value: {
              resumeFromSequence: lease.lastFrameSeq,
              terminalReplay: false,
            },
          },
        }),
      );

      if (lease.inputsViaAttempt) {
        await this.pumpAttemptInputs(lease.leaseId);
      }
      return;
    }

    // Section 4.2 & 4.5: terminal_replay lookup by (lease_id, namespace), independent of generation or active lease!
    const recordedDigest = this.lookupRecordedReceiptSync(open.leaseId, att.namespace);
    if (recordedDigest !== null) {
      writeSocketAttachment(ws, {
        ...att,
        id: open.leaseId,
        handshakeDone: 1,
        lastSeenAtMs: nowMs,
        replay: 1,
      });
      this.replayStates.set(att.connId, {
        leaseId: open.leaseId,
        namespace: att.namespace,
        recordedDigest,
        lastFrameSeq: 0n,
        curObjIndex: null,
        curObjMeta: {},
        curObjUri: "",
        curObjBytes: 0,
        curObjSha: null,
        nextObjIndex: 0,
        totalResultBytes: 0n,
        tooLarge: false,
        objectHashes: [],
      });
      this.sendWsProtoSync(
        ws,
        pb.encodeAttemptDown({
          msg: {
            case: "opened",
            value: {
              resumeFromSequence: 0n,
              terminalReplay: true,
            },
          },
        }),
      );
      return;
    }

    // Expired/fenced Open with no receipt gets AttemptFinished::FENCED in a normal 0x00 envelope, then close(1000)!
    this.sendWsProtoSync(
      ws,
      pb.encodeAttemptDown({
        msg: {
          case: "finished",
          value: {
            status: pb.AttemptFinishedStatus.FENCED,
            reason: "lease is not active in this generation and has no recorded outcome",
          },
        },
      }),
    );
    ws.close(1000, "");
  }

  /**
   * Server-to-worker `InputData` pump (sections 4.4 & 5.3):
   * - Sends synchronous batches (<= 1 MiB / 16 chunks while `bufferedAmount` allows)
   * - Order: static objects (`objectIndex = i`, empty inline & URI included, metadata on first piece,
   *   `objectEnd` on last piece), non-empty params (`objectIndex = undefined`, `parameterChunk = true`,
   *   `objectEnd` on last piece), streaming chunks (`objectIndex = staticCount + s`), trailer (`objectIndex = undefined`, `last = true`).
   * - Writes `(input_stage, input_item_idx, input_byte_off, input_seq)` to `native_active_leases`
   *   in the EXACT SAME synchronous turn as `ws.send()` (ZERO `await` in between)!
   */
  private async pumpAttemptInputs(leaseId: string): Promise<void> {
    if (this.pumpingAttempts.has(leaseId)) {
      return;
    }
    this.pumpingAttempts.add(leaseId);
    try {
      while (!this.disposed) {
        const lease = this.leases.get(leaseId);
        if (
          !lease ||
          lease.cancelling ||
          !lease.inputsViaAttempt ||
          lease.attemptConnId === null ||
          lease.inputStage >= 4
        ) {
          return;
        }
        const ws = this.socketsByConnId.get(lease.attemptConnId);
        if (!ws || ws.readyState !== WS_READY_STATE_OPEN) {
          return;
        }
        // Pause pumping while socket buffer already holds >= 1 MiB unconsumed!
        if (ws.bufferedAmount >= INPUT_BATCH_MAX_BYTES) {
          return;
        }
        const job = this.jobs.get(lease.scopedJobId);
        if (!job) {
          return;
        }

        const nowMs = this.host.nowMs();
        const chunkLimit = Math.max(1, this.config.maxChunkBytes);

        // Snapshot current cursor BEFORE async R2/SQLite read
        const snapConnId = lease.attemptConnId;
        const snapStage = lease.inputStage;
        const snapItemIdx = lease.inputItemIdx;
        const snapByteOff = lease.inputByteOff;
        const snapInputSeq = lease.inputSeq;

        // Gather manifests (fast sync metadata read)
        const staticManifests = this.store.readBundleManifestsSync(
          job.scopedJobId,
          "static_in",
        );
        const staticInputs: Array<{ seq: number; entry: BundleManifestEntry }> = [];
        let paramsRef: { seq: number; entry: BundleManifestEntry } | null = null;
        for (const row of staticManifests) {
          for (const e of row.entries) {
            if (e.t === "p" || e.t === "pr2") {
              paramsRef = { seq: row.seq, entry: e };
            } else {
              staticInputs.push({ seq: row.seq, entry: e });
            }
          }
        }

        const streamManifests = job.streamingInput
          ? this.store.readBundleManifestsSync(job.scopedJobId, "stream_in")
          : [];
        const streamInputs: Array<{ seq: number; entry: BundleManifestEntry }> = [];
        for (const row of streamManifests) {
          for (const e of row.entries) {
            streamInputs.push({ seq: row.seq, entry: e });
          }
        }

        // Pre-prepare up to <= 16 frames / <= 1 MiB asynchronously BEFORE entering synchronous send+SQL turn!
        interface PlannedInputFrame {
          encodedFrame: Uint8Array;
          nextStage: number;
          nextItemIdx: number;
          nextByteOff: number;
          nextSeq: bigint;
        }

        const planned: PlannedInputFrame[] = [];
        let curStage = snapStage;
        let curIdx = snapItemIdx;
        let curOff = snapByteOff;
        let curSeq = snapInputSeq;
        let batchPayloadBytes = 0;

        while (
          planned.length < INPUT_BATCH_MAX_CHUNKS &&
          batchPayloadBytes < INPUT_BATCH_MAX_BYTES &&
          curStage < 4
        ) {
          if (curStage === 0) {
            if (curIdx >= staticInputs.length) {
              curStage = 1;
              curIdx = 0;
              curOff = 0;
              continue;
            }
            const item = staticInputs[curIdx]!;
            const entry = item.entry;
            if (entry.t === "u") {
              curSeq += 1n;
              const nextIdx = curIdx + 1;
              const nextStage = nextIdx >= staticInputs.length ? 1 : 0;
              const frame = pb.encodeWsDataFrame(
                pb.encodeAttemptDown({
                  msg: {
                    case: "input",
                    value: {
                      sequence: curSeq,
                      data: new Uint8Array(0),
                      last: false,
                      metadata: entry.m ?? {},
                      objectIndex: curIdx,
                      uri: entry.u,
                      parameterChunk: false,
                      objectEnd: true,
                    },
                  },
                }),
              );
              planned.push({
                encodedFrame: frame,
                nextStage,
                nextItemIdx: nextStage === 0 ? nextIdx : 0,
                nextByteOff: 0,
                nextSeq: curSeq,
              });
              curStage = nextStage;
              curIdx = nextStage === 0 ? nextIdx : 0;
              curOff = 0;
              continue;
            }

            const totalLen = entry.l;
            const takeLen = Math.min(
              chunkLimit,
              INPUT_BATCH_MAX_BYTES - batchPayloadBytes,
              Math.max(0, totalLen - curOff),
            );
            const slice =
              takeLen > 0
                ? await this.store.readEntrySlice(
                    job.scopedJobId,
                    "static_in",
                    item.seq,
                    entry,
                    curOff,
                    takeLen,
                    nowMs,
                  )
                : new Uint8Array(0);
            const newOff = curOff + slice.byteLength;
            const isEnd = newOff >= totalLen;
            curSeq += 1n;
            const meta =
              curOff === 0 && "m" in entry && entry.m ? entry.m : {};
            const frame = pb.encodeWsDataFrame(
              pb.encodeAttemptDown({
                msg: {
                  case: "input",
                  value: {
                    sequence: curSeq,
                    data: slice,
                    last: false,
                    metadata: meta,
                    objectIndex: curIdx,
                    uri: "",
                    parameterChunk: false,
                    objectEnd: isEnd,
                  },
                },
              }),
            );
            batchPayloadBytes += slice.byteLength;
            if (isEnd) {
              const nextIdx = curIdx + 1;
              const nextStage = nextIdx >= staticInputs.length ? 1 : 0;
              curStage = nextStage;
              curIdx = nextStage === 0 ? nextIdx : 0;
              curOff = 0;
            } else {
              curOff = newOff;
            }
            planned.push({
              encodedFrame: frame,
              nextStage: curStage,
              nextItemIdx: curIdx,
              nextByteOff: curOff,
              nextSeq: curSeq,
            });
            continue;
          }

          if (curStage === 1) {
            if (!paramsRef || paramsRef.entry.t === "u" || paramsRef.entry.l === 0) {
              curStage = job.streamingInput ? 2 : 3;
              curIdx = 0;
              curOff = 0;
              continue;
            }
            const totalLen = paramsRef.entry.l;
            const takeLen = Math.min(
              chunkLimit,
              INPUT_BATCH_MAX_BYTES - batchPayloadBytes,
              Math.max(0, totalLen - curOff),
            );
            const slice = await this.store.readEntrySlice(
              job.scopedJobId,
              "static_in",
              paramsRef.seq,
              paramsRef.entry,
              curOff,
              takeLen,
              nowMs,
            );
            const newOff = curOff + slice.byteLength;
            const isEnd = newOff >= totalLen;
            curSeq += 1n;
            const frame = pb.encodeWsDataFrame(
              pb.encodeAttemptDown({
                msg: {
                  case: "input",
                  value: {
                    sequence: curSeq,
                    data: slice,
                    last: false,
                    metadata: {},
                    objectIndex: undefined, // params carry NO object_index!
                    uri: "",
                    parameterChunk: true,
                    objectEnd: isEnd,
                  },
                },
              }),
            );
            batchPayloadBytes += slice.byteLength;
            if (isEnd) {
              curStage = job.streamingInput ? 2 : 3;
              curIdx = 0;
              curOff = 0;
            } else {
              curOff = newOff;
            }
            planned.push({
              encodedFrame: frame,
              nextStage: curStage,
              nextItemIdx: curIdx,
              nextByteOff: curOff,
              nextSeq: curSeq,
            });
            continue;
          }

          if (curStage === 2) {
            if (curIdx >= streamInputs.length) {
              if (job.inputEnded) {
                curStage = 3;
                curOff = 0;
                continue;
              }
              // Wait for more streaming InputChunks or InputEnd!
              break;
            }
            const item = streamInputs[curIdx]!;
            const entry = item.entry;
            const totalLen = entry.t === "u" ? 0 : entry.l;
            const takeLen = Math.min(
              chunkLimit,
              INPUT_BATCH_MAX_BYTES - batchPayloadBytes,
              Math.max(0, totalLen - curOff),
            );
            const slice =
              takeLen > 0
                ? await this.store.readEntrySlice(
                    job.scopedJobId,
                    "stream_in",
                    item.seq,
                    entry,
                    curOff,
                    takeLen,
                    nowMs,
                  )
                : new Uint8Array(0);
            const newOff = curOff + slice.byteLength;
            const isEnd = newOff >= totalLen;
            curSeq += 1n;
            const meta =
              curOff === 0 && "m" in entry && entry.m ? entry.m : {};
            const logicalObjIndex = staticInputs.length + curIdx;
            const frame = pb.encodeWsDataFrame(
              pb.encodeAttemptDown({
                msg: {
                  case: "input",
                  value: {
                    sequence: curSeq,
                    data: slice,
                    last: false,
                    metadata: meta,
                    objectIndex: logicalObjIndex,
                    uri: "",
                    parameterChunk: false,
                    objectEnd: isEnd,
                  },
                },
              }),
            );
            batchPayloadBytes += slice.byteLength;
            if (isEnd) {
              curIdx += 1;
              curOff = 0;
            } else {
              curOff = newOff;
            }
            planned.push({
              encodedFrame: frame,
              nextStage: curStage,
              nextItemIdx: curIdx,
              nextByteOff: curOff,
              nextSeq: curSeq,
            });
            continue;
          }

          if (curStage === 3) {
            // Final trailer (`objectIndex = undefined`, `last = true`)
            curSeq += 1n;
            const frame = pb.encodeWsDataFrame(
              pb.encodeAttemptDown({
                msg: {
                  case: "input",
                  value: {
                    sequence: curSeq,
                    data: new Uint8Array(0),
                    last: true,
                    metadata: {},
                    objectIndex: undefined,
                    uri: "",
                    parameterChunk: false,
                    objectEnd: false,
                  },
                },
              }),
            );
            curStage = 4;
            planned.push({
              encodedFrame: frame,
              nextStage: 4,
              nextItemIdx: curIdx,
              nextByteOff: 0,
              nextSeq: curSeq,
            });
            break;
          }
        }

        if (planned.length === 0) {
          // If curStage transitioned without frames (e.g. empty static inputs + empty params + waiting for stream), persist stage
          if (curStage !== snapStage) {
            const liveLease = this.leases.get(leaseId);
            if (
              liveLease &&
              !liveLease.cancelling &&
              liveLease.attemptConnId === snapConnId &&
              liveLease.inputSeq === snapInputSeq
            ) {
              this.store.transactionSync(() => {
                liveLease.inputStage = curStage;
                liveLease.inputItemIdx = curIdx;
                liveLease.inputByteOff = curOff;
                this.persistLeaseInputCursorInTxn(liveLease);
              });
            }
          }
          return;
        }

        // CRITICAL (section 4.4): Send frames AND write cursor to SQLite in the SAME synchronous turn (ZERO await)!
        if (this.disposed) {
          return;
        }
        let sentCount = 0;
        const liveLease = this.leases.get(leaseId);
        if (
          !liveLease ||
          liveLease.cancelling ||
          liveLease.attemptConnId !== snapConnId ||
          liveLease.inputSeq !== snapInputSeq ||
          ws.readyState !== WS_READY_STATE_OPEN
        ) {
          return;
        }

        let lastSentPlan: PlannedInputFrame | null = null;
        for (const p of planned) {
          if (
            sentCount > 0 &&
            ws.bufferedAmount + p.encodedFrame.byteLength > INPUT_BATCH_MAX_BYTES
          ) {
            break;
          }
          const permit = this.isolateBudget.tryAcquireOutboundSync(
            ws,
            p.encodedFrame.byteLength,
          );
          if (!permit) {
            break;
          }
          try {
            ws.send(p.encodedFrame);
            permit.completeSend();
            lastSentPlan = p;
            sentCount += 1;
          } catch {
            permit.abort();
            break;
          }
        }

        if (lastSentPlan !== null) {
          const committedPlan = lastSentPlan;
          this.store.transactionSync(() => {
            liveLease.inputStage = committedPlan.nextStage;
            liveLease.inputItemIdx = committedPlan.nextItemIdx;
            liveLease.inputByteOff = committedPlan.nextByteOff;
            liveLease.inputSeq = committedPlan.nextSeq;
            this.persistLeaseInputCursorInTxn(liveLease);
          });
        }

        if (sentCount === 0 || liveLease.inputStage >= 4 || ws.bufferedAmount >= INPUT_BATCH_MAX_BYTES) {
          return;
        }
        // Yield macrotask/microtask between <= 1 MiB batches so interleaving messages or hibernation tests can run!
        await Promise.resolve();
      }
    } finally {
      this.pumpingAttempts.delete(leaseId);
    }
  }

  private async handleLiveAttemptFrame(
    ws: NativeWebSocket,
    att: SocketAttachment,
    frame: pb.Frame,
    nowMs: number,
  ): Promise<void> {
    if (frame.data.byteLength > this.config.maxChunkBytes) {
      throw GrpcStatusError.invalidArgument(
        `frame of ${frame.data.byteLength} bytes exceeds max_chunk_bytes ${this.config.maxChunkBytes}`,
      );
    }
    if (metadataByteLen(frame.metadata) > MAX_METADATA_BYTES) {
      throw GrpcStatusError.invalidArgument("frame metadata is too large");
    }

    const lease = this.leases.get(att.id);
    if (!lease || lease.cancelling || lease.attemptConnId !== att.connId) {
      this.sendWsProtoSync(
        ws,
        pb.encodeAttemptDown({
          msg: {
            case: "finished",
            value: {
              status: pb.AttemptFinishedStatus.FENCED,
              reason: "lease is no longer active",
            },
          },
        }),
      );
      ws.close(1000, "");
      return;
    }

    const job = this.jobs.get(lease.scopedJobId);
    if (!job || job.currentLeaseId !== lease.leaseId || job.state !== "running") {
      this.sendWsProtoSync(
        ws,
        pb.encodeAttemptDown({
          msg: {
            case: "finished",
            value: {
              status: pb.AttemptFinishedStatus.FENCED,
              reason: "lease is not the job's current attempt",
            },
          },
        }),
      );
      ws.close(1000, "");
      return;
    }

    // Duplicate frame re-acks contiguous high-water mark; gap is InvalidArgument!
    if (frame.sequence <= lease.lastFrameSeq) {
      this.sendWsProtoSync(
        ws,
        pb.encodeAttemptDown({
          msg: {
            case: "ack",
            value: {
              upToSequence: lease.lastFrameSeq,
              resultBytes: BigInt(lease.ackedResultBytes),
            },
          },
        }),
      );
      return;
    }
    if (frame.sequence !== lease.lastFrameSeq + 1n) {
      throw GrpcStatusError.invalidArgument(
        `frame sequence ${frame.sequence} does not follow ${lease.lastFrameSeq}`,
      );
    }

    const eventSize =
      EVENT_OVERHEAD_BYTES + frame.data.byteLength + metadataByteLen(frame.metadata);

    // Primary Invoke consumer back-pressure (section 5.2: 8 MiB per-job event cap; stalls worker ACK and fails at consumer_stall_timeout!)
    if (job.callerConnId !== null) {
      const invokeWs = this.socketsByConnId.get(job.callerConnId);
      const consumerOverloaded =
        job.eventLogBytes + eventSize > this.config.perJobEventCapBytes ||
        (invokeWs !== undefined &&
          invokeWs.bufferedAmount >= this.config.perJobEventCapBytes);
      if (consumerOverloaded) {
        if (job.stalledSinceMs === null) {
          job.stalledSinceMs = nowMs;
          await this.refreshAlarm(nowMs);
        }
        if (nowMs - job.stalledSinceMs >= this.config.consumerStallTimeoutMs) {
          this.store.transactionSync(() => {
            this.failJobInTxn(
              job.scopedJobId,
              RUNTIME_CODES.CONSUMER_STALLED,
              `consumer did not read output for ${Math.floor(this.config.consumerStallTimeoutMs / 1000)}s`,
              nowMs,
            );
          });
          await this.notifyJobFollowers(job.scopedJobId);
          this.sendWsProtoSync(
            ws,
            pb.encodeAttemptDown({
              msg: {
                case: "finished",
                value: {
                  status: pb.AttemptFinishedStatus.FENCED,
                  reason: "the job's consumer stopped reading",
                },
              },
            }),
          );
          ws.close(1000, "");
          return;
        }
        // Wait briefly for consumer drain up to stall deadline
        const waitMs = Math.min(250, this.config.consumerStallTimeoutMs - (nowMs - job.stalledSinceMs));
        await new Promise<void>((r) => setTimeout(r, Math.max(10, waitMs)));
        // Re-evaluate after wait
        const reNow = this.host.nowMs();
        if (
          !this.leases.has(lease.leaseId) ||
          lease.cancelling ||
          lease.attemptConnId !== att.connId
        ) {
          return;
        }
        if (job.eventLogBytes + eventSize > this.config.perJobEventCapBytes) {
          if (reNow - (job.stalledSinceMs ?? nowMs) >= this.config.consumerStallTimeoutMs) {
            this.store.transactionSync(() => {
              this.failJobInTxn(
                job.scopedJobId,
                RUNTIME_CODES.CONSUMER_STALLED,
                `consumer did not read output for ${Math.floor(this.config.consumerStallTimeoutMs / 1000)}s`,
                reNow,
              );
            });
            await this.notifyJobFollowers(job.scopedJobId);
            this.sendWsProtoSync(
              ws,
              pb.encodeAttemptDown({
                msg: {
                  case: "finished",
                  value: {
                    status: pb.AttemptFinishedStatus.FENCED,
                    reason: "the job's consumer stopped reading",
                  },
                },
              }),
            );
            ws.close(1000, "");
            return;
          }
          throw GrpcStatusError.resourceExhausted("primary consumer event buffer full");
        }
      }
    }

    // Persist Frame event, last_frame_seq, and exposed BEFORE sending Ack or ExecutionEvent!
    this.store.transactionSync(() => {
      lease.lastFrameSeq = frame.sequence;
      if (job.callerConnId !== null && !frame.progress) {
        lease.exposed = true;
      }
      const seqSplit = splitU64(lease.lastFrameSeq);
      this.store.exec(
        `UPDATE native_active_leases
         SET last_frame_seq_hi = ?, last_frame_seq_lo = ?, exposed = ?
         WHERE lease_id = ?`,
        seqSplit.hi,
        seqSplit.lo,
        lease.exposed ? 1 : 0,
        lease.leaseId,
      );
      this.appendJobEventInTxn(
        job,
        lease.attemptId,
        frame.progress ? "progress" : "output",
        { metadata: frame.metadata, progress: frame.progress },
        frame.data,
        eventSize,
        nowMs,
      );
      const lastSplit = splitU64(job.lastSequence);
      this.store.exec(
        `UPDATE native_jobs SET last_seq_hi = ?, last_seq_lo = ? WHERE scoped_job_id = ?`,
        lastSplit.hi,
        lastSplit.lo,
        job.scopedJobId,
      );
    });

    this.sendWsProtoSync(
      ws,
      pb.encodeAttemptDown({
        msg: {
          case: "ack",
          value: {
            upToSequence: lease.lastFrameSeq,
            resultBytes: BigInt(lease.ackedResultBytes),
          },
        },
      }),
    );

    await this.notifyJobFollowers(job.scopedJobId);
  }

  /**
   * Worker `ResultChunk` upload (section 4.4):
   * - Single-chunk object (`chunk.last` on first chunk, <= 512 KiB): stored inline on `native_active_leases`
   *   before `Ack` is sent (zero `native_upload_tail` / `native_upload_segments` rows!).
   * - Multi-chunk object: EVERY chunk persisted to `native_upload_tail` BEFORE `webSocketMessage` returns!
   * - Seals at `UPLOAD_SEGMENT_BYTES` (2 MiB), `chunk.last`, or `< 1 MiB` window slack into R2 segment
   *   (or inline BLOB for short final piece), deletes sealed tail rows, updates `acked_result_bytes`, then sends `Ack`!
   */
  private async handleLiveAttemptResultChunk(
    ws: NativeWebSocket,
    att: SocketAttachment,
    chunk: pb.ResultChunk,
    nowMs: number,
  ): Promise<void> {
    if (chunk.data.byteLength > this.config.maxChunkBytes) {
      throw GrpcStatusError.invalidArgument(
        `result chunk of ${chunk.data.byteLength} bytes exceeds max_chunk_bytes ${this.config.maxChunkBytes}`,
      );
    }
    if (chunk.uri.length > 0 && chunk.data.byteLength > 0) {
      throw GrpcStatusError.invalidArgument("a URI result carries no data");
    }

    const lease = this.leases.get(att.id);
    if (!lease || lease.cancelling || lease.attemptConnId !== att.connId) {
      this.sendWsProtoSync(
        ws,
        pb.encodeAttemptDown({
          msg: {
            case: "finished",
            value: {
              status: pb.AttemptFinishedStatus.FENCED,
              reason: "lease is no longer active",
            },
          },
        }),
      );
      ws.close(1000, "");
      return;
    }

    const expectedIdx =
      lease.curObjIndex !== null ? lease.curObjIndex : lease.nextObjIndex;
    if (chunk.index !== expectedIdx) {
      throw GrpcStatusError.invalidArgument(
        `result chunk for object ${chunk.index} while object ${expectedIdx} is expected`,
      );
    }

    const isStartingNewObject = lease.curObjIndex === null;
    const canStoreSingleChunkInline =
      isStartingNewObject &&
      chunk.last &&
      lease.inlineResultBlob.byteLength + chunk.data.byteLength <=
        BUNDLE_INLINE_MAX_BYTES;

    if (canStoreSingleChunkInline) {
      // Single-chunk fast path (reference job!): persist inline on lease row in 1 write, then Ack!
      this.store.transactionSync(() => {
        const metaBytes = metadataByteLen(chunk.metadata);
        const addedAccounted = Number(
          resultObjectAccountedBytes({
            metadata: chunk.metadata,
            uri: chunk.uri,
            dataByteLength: chunk.data.byteLength,
          }),
        );
        lease.totalResultBytes += addedAccounted;
        this.totalResultBytesHeld += addedAccounted;
        if (
          lease.nextObjIndex >= MAX_OBJECTS ||
          metaBytes > MAX_METADATA_BYTES ||
          lease.totalResultBytes > this.config.maxInlineTotalBytes ||
          chunk.data.byteLength > this.config.maxInlineObjectBytes
        ) {
          lease.resultTooLarge = true;
          lease.completedObjs = [];
          lease.inlineResultBlob = new Uint8Array(0);
        } else if (chunk.uri.length > 0) {
          lease.completedObjs.push({
            kind: "uri",
            uri: chunk.uri,
            metadata: { ...chunk.metadata },
            byteLen: 0,
          });
        } else {
          const offset = lease.inlineResultBlob.byteLength;
          if (chunk.data.byteLength > 0) {
            const merged = new Uint8Array(offset + chunk.data.byteLength);
            merged.set(lease.inlineResultBlob, 0);
            merged.set(chunk.data, offset);
            lease.inlineResultBlob = merged;
          }
          lease.completedObjs.push({
            kind: "inline",
            metadata: { ...chunk.metadata },
            inlineOffset: offset,
            byteLen: chunk.data.byteLength,
          });
        }
        lease.nextObjIndex = chunk.index + 1;
        lease.ackedResultBytes = lease.totalResultBytes;
        this.persistLeaseResultStateInTxn(lease);
      });

      this.sendWsProtoSync(
        ws,
        pb.encodeAttemptDown({
          msg: {
            case: "ack",
            value: {
              upToSequence: lease.lastFrameSeq,
              resultBytes: BigInt(lease.ackedResultBytes),
            },
          },
        }),
      );
      return;
    }

    // Multi-chunk object path:
    // Step 1: IMMEDIATELY persist chunk to `native_upload_tail` inside transactionSync BEFORE returning or awaiting R2!
    if (isStartingNewObject && chunk.data.byteLength > 0) {
      this.store.recordLargeJob(nowMs);
    }

    let sealNeeded = false;
    let sealUpToChunkSeq = 0;
    let isOverflow = false;

    this.store.transactionSync(() => {
      if (lease.curObjIndex === null) {
        const metaBytes = metadataByteLen(chunk.metadata);
        const headerAccounted =
          OBJECT_OVERHEAD + metaBytes + utf8Len(chunk.uri);
        lease.totalResultBytes += headerAccounted;
        this.totalResultBytesHeld += headerAccounted;
        lease.curObjIndex = chunk.index;
        lease.curObjMeta = { ...chunk.metadata };
        lease.curObjUri = chunk.uri;
        lease.curObjBytes = 0;
        lease.tailBytes = 0;
        lease.tailNextChunkSeq = 0;
        lease.nextSegIndex = 0;
        if (
          lease.nextObjIndex >= MAX_OBJECTS ||
          metaBytes > MAX_METADATA_BYTES
        ) {
          lease.resultTooLarge = true;
        }
      }

      lease.curObjBytes += chunk.data.byteLength;
      lease.totalResultBytes += chunk.data.byteLength;
      this.totalResultBytesHeld += chunk.data.byteLength;

      if (
        lease.totalResultBytes > this.config.maxInlineTotalBytes ||
        lease.curObjBytes > this.config.maxInlineObjectBytes
      ) {
        lease.resultTooLarge = true;
      }

      isOverflow = lease.resultTooLarge;
      const chunkSeq = lease.tailNextChunkSeq;
      lease.tailNextChunkSeq += 1;
      lease.tailBytes += chunk.data.byteLength;

      if (!isOverflow && chunk.data.byteLength > 0) {
        this.store.exec(
          `INSERT INTO native_upload_tail (
            lease_id, attempt_conn_id, obj_index, chunk_seq, data
          ) VALUES (?, ?, ?, ?, ?)`,
          lease.leaseId,
          att.connId,
          chunk.index,
          chunkSeq,
          chunk.data,
        );
      }

      const unackedBytes = lease.totalResultBytes - lease.ackedResultBytes;
      const windowSlack = this.config.attemptWindowBytes - unackedBytes;
      if (
        lease.tailBytes >= UPLOAD_SEGMENT_BYTES ||
        chunk.last ||
        windowSlack < UPLOAD_WINDOW_SLACK_THRESHOLD
      ) {
        sealNeeded = true;
        sealUpToChunkSeq = chunkSeq;
      }

      this.persistLeaseResultStateInTxn(lease);
    });

    if (!sealNeeded) {
      // Chunk is safely persisted in `native_upload_tail`! No Ack yet until segment seals or slack < 1 MiB.
      return;
    }

    // Step 2: Seal tail into an immutable R2 segment (or inline BLOB for short final piece <= 512 KiB)
    if (isOverflow) {
      this.store.transactionSync(() => {
        const segRows = this.store.exec<{ r2_key: string | null }>(
          `SELECT r2_key FROM native_upload_segments WHERE lease_id = ? AND r2_key IS NOT NULL`,
          lease.leaseId,
        );
        const r2Keys = segRows.map((r) => r.r2_key!).filter(Boolean);
        if (r2Keys.length > 0) {
          this.store.queuePendingDeleteInTxn(r2Keys, nowMs);
        }
        this.store.exec(`DELETE FROM native_upload_tail WHERE lease_id = ?`, lease.leaseId);
        this.store.exec(`DELETE FROM native_upload_segments WHERE lease_id = ?`, lease.leaseId);
        lease.completedObjs = [];
        lease.inlineResultBlob = new Uint8Array(0);
        lease.tailBytes = 0;
        lease.ackedResultBytes = lease.totalResultBytes;
        if (chunk.last) {
          lease.curObjIndex = null;
          lease.nextObjIndex = chunk.index + 1;
        }
        this.persistLeaseResultStateInTxn(lease);
      });
      this.sendWsProtoSync(
        ws,
        pb.encodeAttemptDown({
          msg: {
            case: "ack",
            value: {
              upToSequence: lease.lastFrameSeq,
              resultBytes: BigInt(lease.ackedResultBytes),
            },
          },
        }),
      );
      return;
    }

    const tailRows = this.store.exec<{ chunk_seq: number; data: Uint8Array }>(
      `SELECT chunk_seq, data FROM native_upload_tail
       WHERE lease_id = ? AND attempt_conn_id = ? AND obj_index = ? AND chunk_seq <= ?
       ORDER BY chunk_seq ASC`,
      lease.leaseId,
      att.connId,
      chunk.index,
      sealUpToChunkSeq,
    );
    const tailParts = tailRows.map((r) => normalizeBlob(r.data));
    const segData = concatBytes(tailParts);
    const storeInlineSeg =
      !this.host.r2 ||
      (chunk.last &&
        lease.nextSegIndex === 0 &&
        segData.byteLength <= BUNDLE_INLINE_MAX_BYTES);

    const stagedUpload =
      !storeInlineSeg && segData.byteLength > 0
        ? await this.store.stageR2Blob(segData, nowMs)
        : null;

    let committedSeal = false;
    this.store.transactionSync(() => {
      // Section 4.2: Re-check generation, lease, cancelling, and attempt_conn_id after await!
      const curLease = this.leases.get(lease.leaseId);
      if (
        !curLease ||
        curLease.cancelling ||
        curLease.generation !== this.generation ||
        curLease.attemptConnId !== att.connId ||
        curLease.curObjIndex !== chunk.index
      ) {
        return;
      }
      if (stagedUpload && !this.store.claimStagedUploadsInTxn([stagedUpload])) {
        return;
      }

      const segIdx = curLease.nextSegIndex;
      curLease.nextSegIndex += 1;

      if (segData.byteLength > 0) {
        this.store.exec(
          `INSERT INTO native_upload_segments (
            lease_id, attempt_conn_id, obj_index, seg_index, byte_len, r2_key, blob
          ) VALUES (?, ?, ?, ?, ?, ?, ?)`,
          curLease.leaseId,
          att.connId,
          chunk.index,
          segIdx,
          segData.byteLength,
          stagedUpload ? stagedUpload.r2Key : null,
          stagedUpload ? null : segData,
        );
      }

      this.store.exec(
        `DELETE FROM native_upload_tail
         WHERE lease_id = ? AND attempt_conn_id = ? AND obj_index = ? AND chunk_seq <= ?`,
        curLease.leaseId,
        att.connId,
        chunk.index,
        sealUpToChunkSeq,
      );

      curLease.tailBytes = Math.max(0, curLease.tailBytes - segData.byteLength);
      curLease.ackedResultBytes = curLease.totalResultBytes - curLease.tailBytes;

      if (chunk.last) {
        const allSegs = this.store.exec<{
          seg_index: number;
          byte_len: number;
          r2_key: string | null;
        }>(
          `SELECT seg_index, byte_len, r2_key FROM native_upload_segments
           WHERE lease_id = ? AND attempt_conn_id = ? AND obj_index = ?
           ORDER BY seg_index ASC`,
          curLease.leaseId,
          att.connId,
          chunk.index,
        );
        curLease.completedObjs.push({
          kind: "segments",
          metadata: { ...curLease.curObjMeta },
          byteLen: curLease.curObjBytes,
          segments: allSegs.map((s) => ({
            r2Key: s.r2_key,
            segIndex: Number(s.seg_index),
            byteLen: Number(s.byte_len),
          })),
        });
        curLease.curObjIndex = null;
        curLease.curObjMeta = {};
        curLease.curObjUri = "";
        curLease.curObjBytes = 0;
        curLease.nextObjIndex = chunk.index + 1;
        curLease.ackedResultBytes = curLease.totalResultBytes;
      }

      this.persistLeaseResultStateInTxn(curLease);
      committedSeal = true;
    });

    if (!committedSeal) {
      if (stagedUpload) {
        await this.store.compensateUnpublishedUploads([stagedUpload], nowMs);
      }
      if (!this.leases.has(lease.leaseId) || lease.cancelling) {
        return;
      }
      throw GrpcStatusError.unavailable("result segment staging expired; retry upload");
    }

    if (ws.readyState === WS_READY_STATE_OPEN) {
      this.sendWsProtoSync(
        ws,
        pb.encodeAttemptDown({
          msg: {
            case: "ack",
            value: {
              upToSequence: lease.lastFrameSeq,
              resultBytes: BigInt(lease.ackedResultBytes),
            },
          },
        }),
      );
    }
  }

  private async handleLiveAttemptFinish(
    ws: NativeWebSocket,
    att: SocketAttachment,
    finish: pb.Finish,
    nowMs: number,
  ): Promise<void> {
    if (!finish.outcome) {
      throw GrpcStatusError.invalidArgument("Finish requires an outcome");
    }
    const lease = this.leases.get(att.id);
    if (!lease) {
      // Maybe already finished on another call; check receipt!
      const recorded = this.lookupRecordedReceiptSync(att.id, att.namespace);
      if (recorded !== null) {
        const err = this.finishOutcomeError(finish.outcome, false);
        const digest = outcomeDigest([], err);
        const status =
          digest === recorded
            ? pb.AttemptFinishedStatus.COMMITTED
            : pb.AttemptFinishedStatus.REJECTED;
        this.sendWsProtoSync(
          ws,
          pb.encodeAttemptDown({
            msg: {
              case: "finished",
              value: {
                status,
                reason:
                  status === pb.AttemptFinishedStatus.COMMITTED
                    ? ""
                    : "a different outcome was already recorded for this lease",
              },
            },
          }),
        );
        ws.close(1000, "");
        return;
      }
      this.sendWsProtoSync(
        ws,
        pb.encodeAttemptDown({
          msg: {
            case: "finished",
            value: {
              status: pb.AttemptFinishedStatus.FENCED,
              reason: "lease is no longer active",
            },
          },
        }),
      );
      ws.close(1000, "");
      return;
    }

    if (lease.curObjIndex !== null) {
      throw GrpcStatusError.invalidArgument(
        "Finish arrived before the last result chunk",
      );
    }

    // Section 4.6: Late Finish on a cancelling=1 lease releases its slot/capacity reservation and returns FENCED!
    if (lease.cancelling) {
      const notifyJobs = new Set<string>();
      this.store.transactionSync(() => {
        this.cleanupLeaseUploadsInTxn(lease.leaseId, true, nowMs);
        this.releaseLeaseInTxn(lease.leaseId, nowMs);
        this.dispatchInTxn(nowMs, notifyJobs);
      });
      for (const jid of notifyJobs) {
        await this.notifyJobFollowers(jid);
      }
      await this.refreshAlarm(nowMs);
      this.sendWsProtoSync(
        ws,
        pb.encodeAttemptDown({
          msg: {
            case: "finished",
            value: {
              status: pb.AttemptFinishedStatus.FENCED,
              reason: "the job was cancelled",
            },
          },
        }),
      );
      ws.close(1000, "");
      return;
    }

    if (lease.attemptConnId !== att.connId) {
      this.sendWsProtoSync(
        ws,
        pb.encodeAttemptDown({
          msg: {
            case: "finished",
            value: {
              status: pb.AttemptFinishedStatus.FENCED,
              reason: "attempt connection was superseded",
            },
          },
        }),
      );
      ws.close(1000, "");
      return;
    }

    let errorInfo = this.finishOutcomeError(finish.outcome, lease.resultTooLarge);
    if (errorInfo && errorInfo.code.length === 0) {
      errorInfo = {
        code: RUNTIME_CODES.WORKER_FAILED,
        message: errorInfo.message,
        retryable: errorInfo.retryable,
      };
    }

    // Compute canonical outcomeDigest streaming across R2 segments (zero full-object concatenation!)
    const digest = await this.computeLiveLeaseOutcomeDigest(lease, errorInfo, nowMs);

    // Re-check lease after async streaming digest!
    const curLease = this.leases.get(lease.leaseId);
    if (!curLease || curLease.generation !== this.generation) {
      this.sendWsProtoSync(
        ws,
        pb.encodeAttemptDown({
          msg: {
            case: "finished",
            value: {
              status: pb.AttemptFinishedStatus.FENCED,
              reason: "lease is no longer active",
            },
          },
        }),
      );
      ws.close(1000, "");
      return;
    }
    if (curLease.cancelling) {
      const notifyJobs = new Set<string>();
      this.store.transactionSync(() => {
        this.cleanupLeaseUploadsInTxn(curLease.leaseId, true, nowMs);
        this.releaseLeaseInTxn(curLease.leaseId, nowMs);
        this.dispatchInTxn(nowMs, notifyJobs);
      });
      for (const jid of notifyJobs) {
        await this.notifyJobFollowers(jid);
      }
      await this.refreshAlarm(nowMs);
      this.sendWsProtoSync(
        ws,
        pb.encodeAttemptDown({
          msg: {
            case: "finished",
            value: {
              status: pb.AttemptFinishedStatus.FENCED,
              reason: "the job was cancelled",
            },
          },
        }),
      );
      ws.close(1000, "");
      return;
    }

    const job = this.jobs.get(curLease.scopedJobId);
    if (!job || job.currentLeaseId !== curLease.leaseId || job.state !== "running") {
      this.sendWsProtoSync(
        ws,
        pb.encodeAttemptDown({
          msg: {
            case: "finished",
            value: {
              status: pb.AttemptFinishedStatus.FENCED,
              reason: "lease is not the job's current attempt",
            },
          },
        }),
      );
      ws.close(1000, "");
      return;
    }

    const commitNow = this.host.nowMs();
    const notifyJobs = new Set<string>([job.scopedJobId]);
    let armedRetryBeforeReply = false;

    // Section 4.5 & 4.6: Commit terminal state / retry + output bundle + receipt + lease release
    // in ONE `transactionSync` BEFORE `AttemptFinished::COMMITTED` is sent!
    this.store.transactionSync(() => {
      if (errorInfo === null) {
        // Succeeded! Reset session consecutive_failures and build output bundle row from completedObjs!
        const session = this.sessions.get(curLease.sessionId);
        if (session) {
          session.consecutiveFailures = 0;
          session.cooldownUntilMs = 0;
        }
        this.writeCompletedLeaseOutputsToBundleInTxn(job.scopedJobId, curLease, att.connId);
        this.cleanupLeaseUploadsInTxn(curLease.leaseId, false, commitNow); // false = keep R2 blobs now owned by output bundle!
        this.insertReceiptInTxn(curLease.leaseId, job.scopedJobId, job.namespace, digest, commitNow);
        this.applyJobTerminalInTxn(
          job,
          "succeeded",
          null,
          curLease.leaseId,
          digest,
          commitNow,
        );
        this.releaseLeaseInTxn(curLease.leaseId, commitNow);
        this.dispatchInTxn(commitNow, notifyJobs);
      } else {
        // Failed or Cancelled: discard any uploaded output segments -> pending_delete
        this.cleanupLeaseUploadsInTxn(curLease.leaseId, true, commitNow);
        const retried = this.endAttemptWithErrorInTxn(
          job.scopedJobId,
          errorInfo,
          { mode: "release" },
          { leaseId: curLease.leaseId, digest },
          commitNow,
        );
        if (retried) {
          armedRetryBeforeReply = true;
        }
        this.dispatchInTxn(commitNow, notifyJobs);
      }
    });

    // Section 4.6: Wake-up for retry_at_ms MUST be armed BEFORE COMMITTED is sent!
    if (armedRetryBeforeReply) {
      await this.refreshAlarm(commitNow);
    }

    if (ws.readyState === WS_READY_STATE_OPEN) {
      this.sendWsProtoSync(
        ws,
        pb.encodeAttemptDown({
          msg: {
            case: "finished",
            value: {
              status: pb.AttemptFinishedStatus.COMMITTED,
              reason: "",
            },
          },
        }),
      );
      ws.close(1000, "");
    }

    for (const jid of notifyJobs) {
      await this.notifyJobFollowers(jid);
    }
    if (!armedRetryBeforeReply) {
      await this.refreshAlarm(commitNow);
    }
  }

  private finishOutcomeError(
    outcome: pb.FinishOutcome,
    tooLarge: boolean,
  ): StoredErrorInfo | null {
    switch (outcome.case) {
      case "succeeded":
        if (tooLarge) {
          return {
            code: RUNTIME_CODES.OUTPUT_TOO_LARGE,
            message: `result exceeds the inline budget (${MAX_OBJECTS} objects, ${this.config.maxInlineObjectBytes} bytes per object, ${this.config.maxInlineTotalBytes} in total)`,
            retryable: false,
          };
        }
        return null;
      case "failed":
        return {
          code: outcome.value.code,
          message: outcome.value.message,
          retryable: outcome.value.retryable,
        };
      case "cancelled":
        return {
          code: RUNTIME_CODES.CANCELLED,
          message: "worker cancelled the attempt",
          retryable: false,
        };
    }
  }

  private async computeLiveLeaseOutcomeDigest(
    lease: LeaseMem,
    error: StoredErrorInfo | null,
    nowMs: number,
  ): Promise<string> {
    if (error !== null) {
      return outcomeDigest([], error);
    }
    const hasSegments = lease.completedObjs.some((o) => o.kind === "segments");
    if (!hasSegments) {
      const syncObjs: CanonicalObjectInput[] = lease.completedObjs.map((o) => {
        if (o.kind === "uri") {
          return { kind: "uri", uri: o.uri ?? "", metadata: o.metadata };
        }
        const off = o.inlineOffset ?? 0;
        return {
          kind: "inline",
          data: lease.inlineResultBlob.subarray(off, off + o.byteLen),
          metadata: o.metadata,
        };
      });
      return outcomeDigest(syncObjs, null);
    }

    const streamObjs: StreamingCanonicalObjectInput[] = lease.completedObjs.map(
      (o, objIdx) => {
        if (o.kind === "uri") {
          return { kind: "uri", uri: o.uri ?? "", metadata: o.metadata };
        }
        if (o.kind === "inline") {
          const off = o.inlineOffset ?? 0;
          return {
            kind: "inline",
            data: lease.inlineResultBlob.subarray(off, off + o.byteLen),
            metadata: o.metadata,
          };
        }
        const segs = o.segments ?? [];
        const store = this.store;
        const leaseId = lease.leaseId;
        const attemptConnId = lease.attemptConnId!;
        return {
          kind: "inline_stream",
          byteLength: o.byteLen,
          metadata: o.metadata,
          chunks: (async function* () {
            for (const s of segs) {
              if (s.r2Key) {
                yield await store.readR2Slice(s.r2Key, 0, s.byteLen, nowMs);
              } else {
                const row = store.exec<{ blob: Uint8Array | null }>(
                  `SELECT blob FROM native_upload_segments
                   WHERE lease_id = ? AND attempt_conn_id = ? AND obj_index = ? AND seg_index = ?`,
                  leaseId,
                  attemptConnId,
                  objIdx,
                  s.segIndex,
                )[0];
                yield row?.blob ? normalizeBlob(row.blob) : new Uint8Array(0);
              }
            }
          })(),
        };
      },
    );
    return await outcomeDigestStream(streamObjs, null);
  }

  private writeCompletedLeaseOutputsToBundleInTxn(
    scopedJobId: string,
    lease: LeaseMem,
    attemptConnId: number,
  ): void {
    if (lease.completedObjs.length === 0) {
      return;
    }
    const inlineParts: Uint8Array[] = [];
    let inlineTotal = 0;
    const entries: BundleManifestEntry[] = [];

    for (let objIdx = 0; objIdx < lease.completedObjs.length; objIdx++) {
      const obj = lease.completedObjs[objIdx]!;
      const meta = Object.keys(obj.metadata).length > 0 ? obj.metadata : undefined;
      if (obj.kind === "uri") {
        entries.push({
          t: "u",
          u: obj.uri ?? "",
          ...(meta ? { m: meta } : {}),
        });
      } else if (obj.kind === "inline") {
        const srcOff = obj.inlineOffset ?? 0;
        const slice = lease.inlineResultBlob.subarray(srcOff, srcOff + obj.byteLen);
        entries.push({
          t: "i",
          o: inlineTotal,
          l: slice.byteLength,
          ...(meta ? { m: meta } : {}),
        });
        if (slice.byteLength > 0) {
          inlineParts.push(slice);
          inlineTotal += slice.byteLength;
        }
      } else {
        // Multi-segment object: reference already-uploaded R2 segments directly without re-uploading!
        const segRefs: SegmentSliceRef[] = [];
        for (const s of obj.segments ?? []) {
          if (s.r2Key) {
            segRefs.push({ k: s.r2Key, o: 0, l: s.byteLen });
          } else {
            const row = this.store.exec<{ blob: Uint8Array | null }>(
              `SELECT blob FROM native_upload_segments
               WHERE lease_id = ? AND attempt_conn_id = ? AND obj_index = ? AND seg_index = ?`,
              lease.leaseId,
              attemptConnId,
              objIdx,
              s.segIndex,
            )[0];
            const blobBytes = row?.blob ? normalizeBlob(row.blob) : new Uint8Array(0);
            segRefs.push({ o: inlineTotal, l: blobBytes.byteLength });
            if (blobBytes.byteLength > 0) {
              inlineParts.push(blobBytes);
              inlineTotal += blobBytes.byteLength;
            }
          }
        }
        if (segRefs.length === 1 && "k" in segRefs[0]!) {
          entries.push({
            t: "r2",
            k: segRefs[0]!.k,
            o: 0,
            l: obj.byteLen,
            ...(meta ? { m: meta } : {}),
          });
        } else {
          entries.push({
            t: "segs",
            l: obj.byteLen,
            s: segRefs,
            ...(meta ? { m: meta } : {}),
          });
        }
      }
    }

    const packBlob = inlineTotal > 0 ? concatBytes(inlineParts, inlineTotal) : null;
    this.store.writeBundleRowsInTxn(scopedJobId, "output", [
      {
        seq: 0,
        manifest: JSON.stringify(entries),
        packBlob,
      },
    ]);
  }

  private async handleReplayAttemptMessage(
    ws: NativeWebSocket,
    att: SocketAttachment,
    msg: Exclude<pb.AttemptUpVariant, { case: "open" }>,
    _nowMs: number,
  ): Promise<void> {
    const replay = this.replayStates.get(att.connId);
    if (!replay) {
      this.sendWsProtoSync(
        ws,
        pb.encodeAttemptDown({
          msg: {
            case: "finished",
            value: {
              status: pb.AttemptFinishedStatus.FENCED,
              reason: "replay state expired",
            },
          },
        }),
      );
      ws.close(1000, "");
      return;
    }

    if (msg.case === "frame") {
      if (msg.value.sequence > replay.lastFrameSeq) {
        replay.lastFrameSeq = msg.value.sequence;
      }
      this.sendWsProtoSync(
        ws,
        pb.encodeAttemptDown({
          msg: {
            case: "ack",
            value: {
              upToSequence: replay.lastFrameSeq,
              resultBytes: replay.totalResultBytes,
            },
          },
        }),
      );
      return;
    }

    if (msg.case === "result") {
      const chunk = msg.value;
      const expected =
        replay.curObjIndex !== null ? replay.curObjIndex : replay.nextObjIndex;
      if (chunk.index !== expected) {
        throw GrpcStatusError.invalidArgument(
          `result chunk for object ${chunk.index} while object ${expected} is expected`,
        );
      }
      if (chunk.uri.length > 0 && chunk.data.byteLength > 0) {
        throw GrpcStatusError.invalidArgument("a URI result carries no data");
      }
      if (replay.curObjIndex === null) {
        const metaBytes = metadataByteLen(chunk.metadata);
        replay.totalResultBytes += BigInt(
          OBJECT_OVERHEAD + metaBytes + utf8Len(chunk.uri),
        );
        if (replay.nextObjIndex >= MAX_OBJECTS || metaBytes > MAX_METADATA_BYTES) {
          replay.tooLarge = true;
        }
        replay.curObjIndex = chunk.index;
        replay.curObjMeta = { ...chunk.metadata };
        replay.curObjUri = chunk.uri;
        replay.curObjBytes = 0;
        replay.curObjSha = chunk.uri.length === 0 ? new IncrementalSha256() : null;
      }
      replay.curObjBytes += chunk.data.byteLength;
      replay.totalResultBytes += BigInt(chunk.data.byteLength);
      if (
        replay.totalResultBytes > BigInt(this.config.maxInlineTotalBytes) ||
        replay.curObjBytes > this.config.maxInlineObjectBytes
      ) {
        replay.tooLarge = true;
      }
      // Stream into canonical SHA-256? Wait: outcomeDigest hashes length-prefixed data inside one global Sha256!
      // For replay, feed chunks into a streaming canonical hasher on replay state!
      if (!replay.tooLarge && chunk.uri.length === 0) {
        // Keep chunk list or feed IncrementalSha256? Note: global outcomeDigest hashes `"inline" || u64_le(obj.byteLen) || raw_bytes || map`!
        // Because u64_le(obj.byteLen) comes BEFORE raw_bytes, only multi-MiB replay needs chunks; store <= 64 KiB chunks in array (bounded by 24 MiB)
        if (!("_replayChunks" in replay)) {
          (replay as unknown as { _replayChunks: Uint8Array[][] })._replayChunks = [];
        }
        const lists = (replay as unknown as { _replayChunks: Uint8Array[][] })._replayChunks;
        if (!lists[chunk.index]) lists[chunk.index] = [];
        if (chunk.data.byteLength > 0) {
          lists[chunk.index]!.push(new Uint8Array(chunk.data));
        }
      }
      if (chunk.last) {
        if (!replay.tooLarge) {
          if (replay.curObjUri.length > 0) {
            replay.objectHashes.push({
              kind: "uri",
              uri: replay.curObjUri,
              metadata: replay.curObjMeta,
            });
          } else {
            replay.objectHashes.push({
              kind: "inline_prehashed",
              byteLen: replay.curObjBytes,
              sha256Bytes: new Uint8Array(0),
              metadata: replay.curObjMeta,
            });
          }
        }
        replay.curObjIndex = null;
        replay.nextObjIndex = chunk.index + 1;
      }
      this.sendWsProtoSync(
        ws,
        pb.encodeAttemptDown({
          msg: {
            case: "ack",
            value: {
              upToSequence: replay.lastFrameSeq,
              resultBytes: replay.totalResultBytes,
            },
          },
        }),
      );
      return;
    }

    // msg.case === "finish"
    if (replay.curObjIndex !== null) {
      throw GrpcStatusError.invalidArgument(
        "Finish arrived before the last result chunk",
      );
    }
    if (!msg.value.outcome) {
      throw GrpcStatusError.invalidArgument("Finish requires an outcome");
    }
    let err = this.finishOutcomeError(msg.value.outcome, replay.tooLarge);
    if (err && err.code.length === 0) {
      err = {
        code: RUNTIME_CODES.WORKER_FAILED,
        message: err.message,
        retryable: err.retryable,
      };
    }

    const chunkLists =
      (replay as unknown as { _replayChunks?: Uint8Array[][] })._replayChunks ?? [];
    const canonicalObjs: CanonicalObjectInput[] =
      err === null
        ? replay.objectHashes.map((o, idx) => {
            if (o.kind === "uri") {
              return { kind: "uri", uri: o.uri, metadata: o.metadata };
            }
            return {
              kind: "inline_chunks",
              byteLength: o.byteLen,
              chunks: chunkLists[idx] ?? [],
              metadata: o.metadata,
            };
          })
        : [];
    const computedDigest = outcomeDigest(canonicalObjs, err);
    const matches = computedDigest === replay.recordedDigest;
    this.replayStates.delete(att.connId);

    this.sendWsProtoSync(
      ws,
      pb.encodeAttemptDown({
        msg: {
          case: "finished",
          value: {
            status: matches
              ? pb.AttemptFinishedStatus.COMMITTED
              : pb.AttemptFinishedStatus.REJECTED,
            reason: matches
              ? ""
              : "a different outcome was already recorded for this lease",
          },
        },
      }),
    );
    ws.close(1000, "");
  }

  // ---------------------------------------------------------------------------
  // 3. Client Invoke Stream (`/ws/v2/client/invoke`)
  // ---------------------------------------------------------------------------

  private async handleInvokeSocketMessage(
    ws: NativeWebSocket,
    att: SocketAttachment,
    bytes: Uint8Array,
    nowMs: number,
  ): Promise<void> {
    const req = pb.decodeInvokeRequest(bytes);
    if (att.handshakeDone === 0) {
      if (req.msg?.case !== "start") {
        throw GrpcStatusError.invalidArgument(
          "first invocation message must be Start",
        );
      }
      await this.handleInvokeStart(ws, att, req.msg.value, nowMs);
      return;
    }

    const job = this.jobs.get(att.id);
    if (!job || !job.streamingInput || job.inputEnded) {
      throw GrpcStatusError.invalidArgument("unexpected invocation input message");
    }

    if (req.msg?.case === "input") {
      const chunk = req.msg.value;
      if (chunk.data.byteLength > this.config.maxInlineObjectBytes) {
        throw GrpcStatusError.invalidArgument(
          `streaming input chunk of ${chunk.data.byteLength} bytes exceeds ${this.config.maxInlineObjectBytes}`,
        );
      }
      if (metadataByteLen(chunk.metadata) > MAX_METADATA_BYTES) {
        throw GrpcStatusError.invalidArgument("streaming input metadata is too large");
      }
      const nextTotal = job.totalInputBytes + chunk.data.byteLength;
      if (nextTotal > this.config.maxInlineTotalBytes) {
        throw GrpcStatusError.invalidArgument(
          `inputs and params total ${nextTotal} bytes, above ${this.config.maxInlineTotalBytes}`,
        );
      }
      if (this.totalInputBytesHeld + chunk.data.byteLength > this.config.maxPendingBytes) {
        throw GrpcStatusError.resourceExhausted("input budget is exhausted");
      }

      // Section 4.4: Persist streaming InputChunk BEFORE webSocketMessage returns!
      const ok = await this.store.appendStreamInputChunk(
        job.scopedJobId,
        chunk.data,
        chunk.metadata,
        nowMs,
        () => {
          const cur = this.jobs.get(job.scopedJobId);
          if (
            !cur ||
            cur.callerConnId !== att.connId ||
            cur.inputEnded ||
            (cur.state !== "queued" && cur.state !== "running")
          ) {
            return false;
          }
          cur.streamChunkCount += 1;
          cur.totalInputBytes += chunk.data.byteLength;
          this.totalInputBytesHeld += chunk.data.byteLength;
          this.store.exec(
            `UPDATE native_jobs
             SET stream_chunk_count = ?, total_input_bytes = ?
             WHERE scoped_job_id = ?`,
            cur.streamChunkCount,
            cur.totalInputBytes,
            cur.scopedJobId,
          );
          return true;
        },
      );
      if (!ok) {
        return;
      }
      if (job.currentLeaseId) {
        await this.pumpAttemptInputs(job.currentLeaseId);
      }
      return;
    }

    if (req.msg?.case === "inputEnd") {
      // Section 4.4: Persist InputEnd BEFORE webSocketMessage returns!
      this.store.transactionSync(() => {
        const cur = this.jobs.get(job.scopedJobId);
        if (!cur || cur.callerConnId !== att.connId) return;
        cur.inputEnded = true;
        this.store.exec(
          `UPDATE native_jobs SET input_ended = 1 WHERE scoped_job_id = ?`,
          cur.scopedJobId,
        );
      });
      if (job.currentLeaseId) {
        await this.pumpAttemptInputs(job.currentLeaseId);
      }
      return;
    }

    throw GrpcStatusError.invalidArgument("unexpected invocation input message");
  }

  private async handleInvokeStart(
    ws: NativeWebSocket,
    att: SocketAttachment,
    start: pb.InvokeStart,
    nowMs: number,
  ): Promise<void> {
    if (!start.spec) {
      throw GrpcStatusError.invalidArgument("spec is required");
    }
    const normalized = this.normalizeJobSpec(start.spec, "ephemeral");
    const usesLarge = this.specUsesMultiChunkOrR2(normalized.inputs, normalized.params, start.streamingInput);
    const estRetained = 4_096 + normalized.totalPayloadBytes;
    const shedReason = this.store.checkNewJobAdmission(nowMs, estRetained, usesLarge);
    if (shedReason !== null) {
      throw GrpcStatusError.resourceExhausted(shedReason);
    }

    this.checkQueueAndInputBudgets(att.namespace, normalized.spec.demand.interface, normalized.totalPayloadBytes);

    const rawJobId = crypto.randomUUID();
    const scopedJobId = encodeScopedId(att.namespace, rawJobId);
    const bundlePlan = await this.store.prepareObjectBundle(
      normalized.inputs,
      normalized.params,
      nowMs,
    );

    if (usesLarge || bundlePlan.stagedUploads.length > 0) {
      this.store.recordLargeJob(nowMs);
    }

    const notifyJobs = new Set<string>([scopedJobId]);
    let committed = false;
    this.store.transactionSync(() => {
      if (ws.readyState !== WS_READY_STATE_OPEN) {
        return;
      }
      if (!this.store.claimStagedUploadsInTxn(bundlePlan.stagedUploads)) {
        return;
      }
      writeSocketAttachment(ws, {
        ...att,
        id: scopedJobId,
        handshakeDone: 1,
        lastSeenAtMs: nowMs,
      });
      this.insertAcceptedJobInTxn({
        scopedJobId,
        rawJobId,
        namespace: att.namespace,
        durability: "ephemeral",
        specHash: "",
        spec: normalized.spec,
        maxAttempts: normalized.maxAttempts,
        totalInputBytes: normalized.totalPayloadBytes,
        retainedBytesEst: estRetained,
        callerConnId: att.connId,
        streamingInput: start.streamingInput,
        bundleRows: bundlePlan.rows,
        nowMs,
      });
      this.dispatchInTxn(nowMs, notifyJobs);
      committed = true;
    });

    if (!committed) {
      await this.store.compensateUnpublishedUploads(bundlePlan.stagedUploads, nowMs);
      throw GrpcStatusError.unavailable("failed to commit InvokeStart");
    }

    for (const jid of notifyJobs) {
      await this.notifyJobFollowers(jid);
    }
    await this.refreshAlarm(nowMs);
  }

  // ---------------------------------------------------------------------------
  // 4. Client Watch Stream (`/ws/v2/client/watch`)
  // ---------------------------------------------------------------------------

  private async handleWatchSocketMessage(
    ws: NativeWebSocket,
    att: SocketAttachment,
    bytes: Uint8Array,
    nowMs: number,
  ): Promise<void> {
    if (att.handshakeDone === 1) {
      throw GrpcStatusError.invalidArgument("unexpected watch message");
    }
    const req = pb.decodeWatchRequest(bytes);
    const scopedJobId = encodeScopedId(att.namespace, req.jobId);
    const job = this.jobs.get(scopedJobId);
    if (!job) {
      throw GrpcStatusError.notFound(`job '${req.jobId}' does not exist`);
    }

    // Cursor check (section 4.3 & 4.4: after == 0 or after >= oldest - 1 is valid; older returns OutOfRange(11) with oldest!)
    if (req.afterSequence !== 0n && req.afterSequence < job.evictedThrough) {
      const oldest = job.evictedThrough + 1n;
      throw GrpcStatusError.outOfRange(
        `cursor expired; oldest available sequence is ${oldest}`,
      );
    }

    writeSocketAttachment(ws, {
      ...att,
      id: scopedJobId,
      handshakeDone: 1,
      lastSeenAtMs: nowMs,
    });

    const watcher: WatcherMem = {
      watchConnId: att.connId,
      scopedJobId,
      rawJobId: job.rawJobId,
      namespace: att.namespace,
      afterSeq: req.afterSequence,
      deliveredSeq: req.afterSequence,
      finished: false,
    };

    this.store.transactionSync(() => {
      this.watchers.set(att.connId, watcher);
      let set = this.watchersByJob.get(scopedJobId);
      if (!set) {
        set = new Set();
        this.watchersByJob.set(scopedJobId, set);
      }
      set.add(att.connId);
      const aSplit = splitU64(watcher.afterSeq);
      const dSplit = splitU64(watcher.deliveredSeq);
      this.store.exec(
        `INSERT INTO native_watchers (
          watch_conn_id, scoped_job_id, raw_job_id, namespace,
          after_seq_hi, after_seq_lo, delivered_seq_hi, delivered_seq_lo, finished
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)`,
        att.connId,
        scopedJobId,
        job.rawJobId,
        att.namespace,
        aSplit.hi,
        aSplit.lo,
        dSplit.hi,
        dSplit.lo,
      );
    });

    await this.deliverWatcherEvents(att.connId);
  }

  // ---------------------------------------------------------------------------
  // 5. Unary Client RPCs (`Submit`, `GetJob`, `Cancel`, `ListJobs`)
  // ---------------------------------------------------------------------------

  private async handleSubmitRpc(
    namespace: string,
    req: pb.SubmitRequest,
    nowMs: number,
  ): Promise<pb.SubmitResponse> {
    if (!req.spec) {
      throw GrpcStatusError.invalidArgument("spec is required");
    }
    if (
      req.conflictPolicy !== pb.ConflictPolicy.CONFLICT_POLICY_UNSPECIFIED &&
      req.conflictPolicy !== pb.ConflictPolicy.FAIL &&
      req.conflictPolicy !== pb.ConflictPolicy.USE_EXISTING
    ) {
      throw GrpcStatusError.invalidArgument("unknown conflict policy");
    }
    const durability: DurabilityName =
      req.spec.policy?.durability === pb.Durability.EPHEMERAL
        ? "ephemeral"
        : "durable";
    const normalized = this.normalizeJobSpec(req.spec, durability);

    const requestedId = req.jobId.trim();
    if (requestedId && utf8Len(requestedId) > MAX_IDENTIFIER_BYTES) {
      throw GrpcStatusError.invalidArgument("job_id is too long");
    }

    const computedHash =
      requestedId.length > 0 || durability === "durable"
        ? specHash(normalized.protoForHash, durability)
        : "";

    const rawJobId = requestedId.length > 0 ? requestedId : crypto.randomUUID();
    const scopedJobId = encodeScopedId(namespace, rawJobId);

    // Idempotency / conflict check before staging any R2 uploads!
    const existing = this.jobs.get(scopedJobId);
    if (existing) {
      this.checkSubmitConflict(rawJobId, existing.specHash, computedHash, req.conflictPolicy);
      return {
        job: await this.jobMemToProto(existing, false, nowMs),
        created: false,
      };
    }

    const usesLarge = this.specUsesMultiChunkOrR2(normalized.inputs, normalized.params, false);
    const estRetained = 4_096 + (usesLarge ? 2_048 : normalized.totalPayloadBytes);
    const shedReason = this.store.checkNewJobAdmission(nowMs, estRetained, usesLarge);
    if (shedReason !== null) {
      throw GrpcStatusError.resourceExhausted(shedReason);
    }

    this.checkQueueAndInputBudgets(
      namespace,
      normalized.spec.demand.interface,
      normalized.totalPayloadBytes,
    );

    const bundlePlan = await this.store.prepareObjectBundle(
      normalized.inputs,
      normalized.params,
      nowMs,
    );
    if (usesLarge || bundlePlan.stagedUploads.length > 0) {
      this.store.recordLargeJob(nowMs);
    }

    const notifyJobs = new Set<string>();
    let raceExisting: JobMem | null = null;
    let createdJob: JobMem | null = null;

    try {
      this.store.transactionSync(() => {
        const curExisting = this.jobs.get(scopedJobId);
        if (curExisting) {
          this.checkSubmitConflict(
            rawJobId,
            curExisting.specHash,
            computedHash,
            req.conflictPolicy,
          );
          raceExisting = curExisting;
          return;
        }
        if (!this.store.claimStagedUploadsInTxn(bundlePlan.stagedUploads)) {
          throw GrpcStatusError.unavailable("input staging expired; retry submission");
        }
        createdJob = this.insertAcceptedJobInTxn({
          scopedJobId,
          rawJobId,
          namespace,
          durability,
          specHash: computedHash,
          spec: normalized.spec,
          maxAttempts: normalized.maxAttempts,
          totalInputBytes: normalized.totalPayloadBytes,
          retainedBytesEst: estRetained,
          callerConnId: null,
          streamingInput: false,
          bundleRows: bundlePlan.rows,
          nowMs,
        });
        this.dispatchInTxn(nowMs, notifyJobs);
      });
    } catch (err) {
      await this.store.compensateUnpublishedUploads(bundlePlan.stagedUploads, nowMs);
      throw err;
    }

    if (raceExisting !== null) {
      await this.store.compensateUnpublishedUploads(bundlePlan.stagedUploads, nowMs);
      return {
        job: await this.jobMemToProto(raceExisting, false, nowMs),
        created: false,
      };
    }

    for (const jid of notifyJobs) {
      await this.notifyJobFollowers(jid);
    }
    await this.refreshAlarm(nowMs);

    return {
      job: await this.jobMemToProto(createdJob!, false, nowMs),
      created: true,
    };
  }

  private checkSubmitConflict(
    rawJobId: string,
    existingHash: string,
    incomingHash: string,
    policy: pb.ConflictPolicy,
  ): void {
    if (!existingHash) {
      throw GrpcStatusError.alreadyExists(
        `job '${rawJobId}' exists and was not created by an idempotent submission`,
      );
    }
    if (existingHash !== incomingHash) {
      throw GrpcStatusError.alreadyExists(
        `job '${rawJobId}' exists with a different specification`,
      );
    }
    if (policy === pb.ConflictPolicy.FAIL) {
      throw GrpcStatusError.alreadyExists(`job '${rawJobId}' already exists`);
    }
  }

  private async handleGetJobRpc(
    namespace: string,
    req: pb.GetJobRequest,
    nowMs: number,
  ): Promise<pb.Job> {
    const scopedJobId = encodeScopedId(namespace, req.jobId);
    const job = this.jobs.get(scopedJobId);
    if (!job) {
      throw GrpcStatusError.notFound(`job '${req.jobId}' does not exist`);
    }
    return await this.jobMemToProto(job, req.includeOutputs, nowMs);
  }

  private async handleCancelRpc(
    namespace: string,
    req: pb.CancelRequest,
    nowMs: number,
  ): Promise<pb.Job> {
    const scopedJobId = encodeScopedId(namespace, req.jobId);
    const job = this.jobs.get(scopedJobId);
    if (!job) {
      throw GrpcStatusError.notFound(`job '${req.jobId}' does not exist`);
    }
    if (
      job.state === "succeeded" ||
      job.state === "failed" ||
      job.state === "cancelled"
    ) {
      return await this.jobMemToProto(job, false, nowMs);
    }
    const reason = req.reason.trim() || "cancelled";
    const notifyJobs = new Set<string>([scopedJobId]);
    this.store.transactionSync(() => {
      this.cancelJobInTxn(scopedJobId, reason, nowMs);
      this.dispatchInTxn(nowMs, notifyJobs);
    });
    for (const jid of notifyJobs) {
      await this.notifyJobFollowers(jid);
    }
    await this.refreshAlarm(nowMs);
    return await this.jobMemToProto(job, false, nowMs);
  }

  private handleListJobsRpc(
    namespace: string,
    req: pb.ListJobsRequest,
    nowMs: number,
  ): pb.ListJobsResponse {
    const states: JobStateName[] = [];
    for (const s of req.states) {
      const name = protoStateToName(s);
      if (!name) {
        throw GrpcStatusError.invalidArgument("unknown job state");
      }
      if (!states.includes(name)) {
        states.push(name);
      }
    }
    let offset = 0;
    if (req.pageToken.trim().length > 0) {
      const parsed = Number(req.pageToken);
      if (!Number.isInteger(parsed) || parsed < 0) {
        throw GrpcStatusError.invalidArgument("invalid page_token");
      }
      offset = parsed;
    }
    const limit = req.limit === 0 ? 100 : req.limit;
    const res = this.queryJobsInternal({
      states,
      interfaceName: null,
      namespace,
      workerId: null,
      search: null,
      fromMs: null,
      toMs: null,
      limit,
      offset,
      histogramBuckets: 0,
    }, nowMs);

    return {
      jobs: res.matchedPage.map((j) => this.jobMemToProtoSyncWithoutOutputs(j)),
      nextPageToken: res.nextPageToken ?? "",
    };
  }

  // ---------------------------------------------------------------------------
  // Core State Machine Transitions (All run inside `store.transactionSync`)
  // ---------------------------------------------------------------------------

  private normalizeJobSpec(
    protoSpec: pb.JobSpec,
    durability: DurabilityName,
  ): {
    spec: StoredSpecMetadata;
    inputs: StoredObject[];
    params: Uint8Array;
    maxAttempts: number;
    totalPayloadBytes: number;
    protoForHash: pb.JobSpecInput;
  } {
    if (!protoSpec.demand) {
      throw GrpcStatusError.invalidArgument("demand is required");
    }
    const iface = protoSpec.demand.interface;
    if (!iface || iface.trim().length === 0) {
      throw GrpcStatusError.invalidArgument("demand.interface is required");
    }
    if (utf8Len(iface) > MAX_IDENTIFIER_BYTES) {
      throw GrpcStatusError.invalidArgument(
        `demand.interface exceeds ${MAX_IDENTIFIER_BYTES} bytes`,
      );
    }
    if (protoSpec.inputs.length > MAX_OBJECTS) {
      throw GrpcStatusError.invalidArgument(
        `a job may carry at most ${MAX_OBJECTS} input objects`,
      );
    }

    let totalPayload = protoSpec.params.byteLength;
    const inputs: StoredObject[] = [];
    let inputMetaBytes = 0;

    for (const inp of protoSpec.inputs) {
      if (!inp.body) {
        throw GrpcStatusError.invalidArgument("object body is required");
      }
      let inlineSize = 0;
      if (inp.body.case === "uri") {
        if (inp.body.value.length === 0) {
          throw GrpcStatusError.invalidArgument("object URI must not be empty");
        }
        const uriBytes = utf8Len(inp.body.value);
        if (uriBytes > MAX_METADATA_BYTES) {
          throw GrpcStatusError.invalidArgument(
            `object URI of ${uriBytes} bytes exceeds ${MAX_METADATA_BYTES} bytes`,
          );
        }
        inputs.push({
          kind: "uri",
          uri: inp.body.value,
          metadata: { ...inp.metadata },
        });
      } else {
        inlineSize = inp.body.value.byteLength;
        inputs.push({
          kind: "inline",
          data: inp.body.value,
          metadata: { ...inp.metadata },
        });
      }
      if (inlineSize > this.config.maxInlineObjectBytes) {
        throw GrpcStatusError.invalidArgument(
          `input object of ${inlineSize} bytes exceeds ${this.config.maxInlineObjectBytes} bytes`,
        );
      }
      totalPayload += inlineSize;
      inputMetaBytes += metadataByteLen(inp.metadata);
    }

    if (protoSpec.params.byteLength > this.config.maxInlineObjectBytes) {
      throw GrpcStatusError.invalidArgument(
        `params of ${protoSpec.params.byteLength} bytes exceeds ${this.config.maxInlineObjectBytes} bytes`,
      );
    }
    if (totalPayload > this.config.maxInlineTotalBytes) {
      throw GrpcStatusError.invalidArgument(
        `inputs and params total ${totalPayload} bytes, above ${this.config.maxInlineTotalBytes}`,
      );
    }

    let capKeyBytes = 0;
    for (const [k, v] of Object.entries(protoSpec.demand.requiredCapacity)) {
      if (v < 0n) {
        throw GrpcStatusError.invalidArgument(
          "required capacity cannot be negative",
        );
      }
      capKeyBytes += utf8Len(k);
    }

    const totalMeta =
      utf8Len(iface) +
      metadataByteLen(protoSpec.metadata) +
      metadataByteLen(protoSpec.demand.requiredAttributes) +
      metadataByteLen(protoSpec.demand.preferredAttributes) +
      capKeyBytes +
      inputMetaBytes;
    if (totalMeta > MAX_METADATA_BYTES) {
      throw GrpcStatusError.invalidArgument(
        `metadata and attributes total ${totalMeta} bytes, above ${MAX_METADATA_BYTES}`,
      );
    }

    const pol = protoSpec.policy;
    if (
      pol &&
      pol.durability !== pb.Durability.DURABILITY_UNSPECIFIED &&
      pol.durability !== pb.Durability.EPHEMERAL &&
      pol.durability !== pb.Durability.DURABLE
    ) {
      throw GrpcStatusError.invalidArgument("unknown durability");
    }
    const clampedPriority = Math.min(
      pol?.priority ?? 0,
      this.config.priorityLevels - 1,
    );
    const rawMaxAttempts =
      pol && pol.maxAttempts > 0 ? pol.maxAttempts : this.config.defaultMaxAttempts;
    const maxAttempts = Math.min(
      Math.max(1, rawMaxAttempts),
      this.config.maxAttemptsLimit,
    );
    const policyMaxAttemptsNormalized =
      pol && pol.maxAttempts > 0 ? pol.maxAttempts : null;

    const queueTimeoutMs =
      pol && pol.queueTimeoutMs > 0n ? toNumberClamped(pol.queueTimeoutMs) : null;
    const execTimeoutMs =
      pol && pol.executionTimeoutMs > 0n
        ? toNumberClamped(pol.executionTimeoutMs)
        : null;
    const totalTimeoutMs =
      pol && pol.totalTimeoutMs > 0n ? toNumberClamped(pol.totalTimeoutMs) : null;
    const spilloverAfterMs =
      pol && pol.spilloverAfterMs > 0n
        ? toNumberClamped(pol.spilloverAfterMs)
        : null;

    const storedSpec: StoredSpecMetadata = {
      demand: {
        interface: iface,
        required_attributes: { ...protoSpec.demand.requiredAttributes },
        preferred_attributes: { ...protoSpec.demand.preferredAttributes },
        required_capacity: { ...protoSpec.demand.requiredCapacity },
        affinity_key: protoSpec.demand.affinityKey,
      },
      policy: {
        durability,
        priority: clampedPriority,
        queue_timeout_ms: queueTimeoutMs,
        execution_timeout_ms: execTimeoutMs,
        total_timeout_ms: totalTimeoutMs,
        spillover_after_ms: spilloverAfterMs,
        max_attempts: policyMaxAttemptsNormalized,
        fairness_key: pol?.fairnessKey ?? "",
        concurrency_key: pol?.concurrencyKey ?? "",
      },
      metadata: { ...protoSpec.metadata },
      params_len: protoSpec.params.byteLength,
      input_count: inputs.length,
    };

    // Normalized proto spec for specHash (matchesRuntime::normalize before identity::spec_hash!)
    const protoForHash: pb.JobSpecInput = {
      demand: protoSpec.demand,
      inputs: protoSpec.inputs,
      params: protoSpec.params,
      metadata: protoSpec.metadata,
      policy: {
        durability:
          durability === "durable" ? pb.Durability.DURABLE : pb.Durability.EPHEMERAL,
        priority: clampedPriority,
        queueTimeoutMs: queueTimeoutMs !== null ? BigInt(queueTimeoutMs) : 0n,
        executionTimeoutMs: execTimeoutMs !== null ? BigInt(execTimeoutMs) : 0n,
        totalTimeoutMs: totalTimeoutMs !== null ? BigInt(totalTimeoutMs) : 0n,
        spilloverAfterMs: spilloverAfterMs !== null ? BigInt(spilloverAfterMs) : 0n,
        maxAttempts: policyMaxAttemptsNormalized ?? 0,
        fairnessKey: pol?.fairnessKey ?? "",
        concurrencyKey: pol?.concurrencyKey ?? "",
      },
    };

    return {
      spec: storedSpec,
      inputs,
      params: protoSpec.params,
      maxAttempts,
      totalPayloadBytes: totalPayload,
      protoForHash,
    };
  }

  private specUsesMultiChunkOrR2(
    inputs: readonly StoredObject[],
    params: Uint8Array,
    streamingInput: boolean,
  ): boolean {
    if (streamingInput || params.byteLength > this.config.maxChunkBytes) {
      return true;
    }
    let sum = params.byteLength;
    for (const o of inputs) {
      if (o.kind === "inline" && o.data) {
        if (o.data.byteLength > this.config.maxChunkBytes) return true;
        sum += o.data.byteLength;
      }
    }
    return sum > BUNDLE_INLINE_MAX_BYTES;
  }

  private checkQueueAndInputBudgets(
    namespace: string,
    interfaceName: string,
    payloadBytes: number,
  ): void {
    let unfinished = 0;
    for (const j of this.jobs.values()) {
      if (j.state === "queued" || j.state === "running") {
        unfinished += 1;
      }
    }
    if (unfinished >= this.config.maxPendingJobs) {
      throw GrpcStatusError.resourceExhausted("unfinished job limit reached");
    }
    const maxQueue =
      this.config.interfaces[interfaceName]?.maxQueue ?? this.config.defaultMaxQueue;
    const curQueued =
      this.queues.get(namespace)?.get(interfaceName)?.totalLen ?? 0;
    if (curQueued >= maxQueue) {
      throw GrpcStatusError.resourceExhausted(
        `queue for interface '${interfaceName}' is full`,
      );
    }
    if (this.totalInputBytesHeld + payloadBytes > this.config.maxPendingBytes) {
      throw GrpcStatusError.resourceExhausted("input budget is exhausted");
    }
  }

  private insertAcceptedJobInTxn(params: {
    scopedJobId: string;
    rawJobId: string;
    namespace: string;
    durability: DurabilityName;
    specHash: string;
    spec: StoredSpecMetadata;
    maxAttempts: number;
    totalInputBytes: number;
    retainedBytesEst: number;
    callerConnId: number | null;
    streamingInput: boolean;
    bundleRows: readonly { seq: number; manifest: string; packBlob: Uint8Array | null }[];
    nowMs: number;
  }): JobMem {
    const baseSeq = this.generation << 32n;
    const acceptedSeq = baseSeq + 1n;
    const queueTimeoutMs =
      params.spec.policy.queue_timeout_ms ??
      this.config.interfaces[params.spec.demand.interface]?.queueTimeoutMs ??
      null;
    const queueDeadlineMs =
      queueTimeoutMs !== null ? params.nowMs + queueTimeoutMs : null;
    const totalDeadlineMs =
      params.spec.policy.total_timeout_ms !== null
        ? params.nowMs + params.spec.policy.total_timeout_ms
        : null;
    const hasPreferred =
      Object.keys(params.spec.demand.preferred_attributes).length > 0;
    const spilloverAtMs =
      hasPreferred && params.spec.policy.spillover_after_ms !== null
        ? params.nowMs + params.spec.policy.spillover_after_ms
        : null;

    this.queueSeq += 1n;
    const qSeq = this.queueSeq;

    const job: JobMem = {
      scopedJobId: params.scopedJobId,
      rawJobId: params.rawJobId,
      namespace: params.namespace,
      interfaceName: params.spec.demand.interface,
      state: "queued",
      durability: params.durability,
      specHash: params.specHash,
      spec: params.spec,
      priority: params.spec.policy.priority,
      fairnessKey: params.spec.policy.fairness_key,
      concurrencyKey: params.spec.policy.concurrency_key,
      affinityKey: params.spec.demand.affinity_key,
      maxAttempts: params.maxAttempts,
      attemptCount: 0,
      createdAtMs: params.nowMs,
      updatedAtMs: params.nowMs,
      queuedSinceMs: params.nowMs,
      notBeforeMs: 0,
      queueDeadlineMs,
      totalDeadlineMs,
      spilloverAtMs,
      callerConnId: params.callerConnId,
      streamingInput: params.streamingInput,
      inputEnded: !params.streamingInput,
      streamChunkCount: 0,
      totalInputBytes: params.totalInputBytes,
      retainedBytesEst: params.retainedBytesEst,
      cancelRequested: null,
      error: null,
      winningLeaseId: null,
      resultDigest: null,
      lastAttemptId: null,
      lastWorkerId: null,
      currentLeaseId: null,
      failedWorkers: new Set(),
      rejectedSessions: new Set(),
      lastSequence: acceptedSeq,
      evictedThrough: 0n,
      primaryConsumed: params.callerConnId !== null ? 0n : null,
      queueSeq: qSeq,
      eventLogBytes: EVENT_OVERHEAD_BYTES,
      eventLogCount: 1,
      stalledSinceMs: null,
    };

    const lastSplit = splitU64(job.lastSequence);
    const primSplit =
      job.primaryConsumed !== null ? splitU64(job.primaryConsumed) : null;

    this.store.exec(
      `INSERT INTO native_jobs (
        scoped_job_id, raw_job_id, namespace, interface_name, state, durability,
        spec_hash, spec_json, priority, fairness_key, concurrency_key, affinity_key,
        max_attempts, attempt_count, created_at_ms, updated_at_ms, queued_since_ms,
        not_before_ms, queue_deadline_ms, total_deadline_ms, spillover_at_ms,
        caller_conn_id, streaming_input, input_ended, stream_chunk_count,
        total_input_bytes, retained_bytes_est, cancel_requested, error_json,
        winning_lease_id, result_digest, last_attempt_id, last_worker_id,
        failed_workers_json, rejected_sessions_json,
        last_seq_hi, last_seq_lo, evicted_seq_hi, evicted_seq_lo,
        primary_seq_hi, primary_seq_lo
      ) VALUES (
        ?, ?, ?, ?, 'queued', ?,
        ?, ?, ?, ?, ?, ?,
        ?, 0, ?, ?, ?,
        0, ?, ?, ?,
        ?, ?, ?, 0,
        ?, ?, NULL, NULL,
        NULL, NULL, NULL, NULL,
        '[]', '[]',
        ?, ?, 0, 0,
        ?, ?
      )`,
      job.scopedJobId,
      job.rawJobId,
      job.namespace,
      job.interfaceName,
      job.durability,
      job.specHash,
      serializeSpecJson(job.spec),
      job.priority,
      job.fairnessKey,
      job.concurrencyKey,
      job.affinityKey,
      job.maxAttempts,
      job.createdAtMs,
      job.updatedAtMs,
      job.queuedSinceMs,
      job.queueDeadlineMs,
      job.totalDeadlineMs,
      job.spilloverAtMs,
      job.callerConnId,
      job.streamingInput ? 1 : 0,
      job.inputEnded ? 1 : 0,
      job.totalInputBytes,
      job.retainedBytesEst,
      lastSplit.hi,
      lastSplit.lo,
      primSplit ? primSplit.hi : null,
      primSplit ? primSplit.lo : null,
    );

    if (params.bundleRows.length > 0 && (params.spec.input_count > 0 || params.spec.params_len > 0)) {
      this.store.writeBundleRowsInTxn(
        job.scopedJobId,
        "static_in",
        params.bundleRows,
      );
    }

    // Persist Accepted event (1 row)
    this.store.exec(
      `INSERT INTO native_job_events (
        scoped_job_id, seq_hi, seq_lo, attempt_id, at_ms, kind, meta_json, payload, size_bytes
      ) VALUES (?, ?, ?, NULL, ?, 'accepted', NULL, NULL, ?)`,
      job.scopedJobId,
      lastSplit.hi,
      lastSplit.lo,
      params.nowMs,
      EVENT_OVERHEAD_BYTES,
    );

    this.jobs.set(job.scopedJobId, job);
    this.store.reservedRetainedBytes += job.retainedBytesEst;
    this.totalInputBytesHeld += job.totalInputBytes;
    this.totalEventBytesHeld += EVENT_OVERHEAD_BYTES;

    this.getOrCreateQueue(job.namespace, job.interfaceName).insert(
      this.config.priorityLevels,
      job.priority,
      job.fairnessKey,
      { seq: qSeq, scopedJobId: job.scopedJobId },
    );
    this.committedBatches += 1;
    this.committedOps += 1;
    return job;
  }

  // ---------------------------------------------------------------------------
  // Scheduling & Dispatch (Mirrors machine.rs::dispatch_locked & pick_session)
  // ---------------------------------------------------------------------------

  private dispatchInTxn(nowMs: number, notifyJobs?: Set<string>): number {
    let dispatchedCount = 0;

    for (const [namespace, ifaceQueues] of this.queues) {
      for (const [interfaceName, queue] of ifaceQueues) {
        if (queue.totalLen === 0) continue;
        const ifaceSessions = this.sessionsByInterface
          .get(namespace)
          ?.get(interfaceName);
        if (!ifaceSessions || ifaceSessions.size === 0) continue;

        const candidates: string[] = [];
        for (const sid of ifaceSessions) {
          const s = this.sessions.get(sid);
          if (s && this.sessionFreeSlots(s) > 0) {
            candidates.push(sid);
          }
        }
        if (candidates.length === 0) continue;

        const policy = this.config.interfaces[interfaceName];
        const entries =
          queue.totalLen <= DISPATCH_SCAN
            ? queue.collectCandidates(DISPATCH_SCAN)
            : queue.collectAllOrdered();

        for (const qEntry of entries) {
          if (candidates.length === 0) break;
          const job = this.jobs.get(qEntry.scopedJobId);
          if (!job || job.state !== "queued") continue;

          // Lazy deadline checks before dispatching!
          if (job.totalDeadlineMs !== null && job.totalDeadlineMs <= nowMs) {
            this.failJobInTxn(
              job.scopedJobId,
              RUNTIME_CODES.TOTAL_TIMEOUT,
              "job exceeded its total timeout",
              nowMs,
            );
            notifyJobs?.add(job.scopedJobId);
            continue;
          }
          if (job.queueDeadlineMs !== null && job.queueDeadlineMs <= nowMs) {
            this.failJobInTxn(
              job.scopedJobId,
              RUNTIME_CODES.QUEUE_TIMEOUT,
              "no worker took the job within its queue timeout",
              nowMs,
            );
            notifyJobs?.add(job.scopedJobId);
            continue;
          }

          if (job.notBeforeMs > nowMs) {
            continue;
          }

          // Concurrency key check
          if (job.concurrencyKey.length > 0) {
            const cap =
              policy?.maxInFlightPerKey ??
              this.config.defaultMaxInFlightPerKey ??
              1;
            if (this.getConcurrencyKeyCount(namespace, job.concurrencyKey) >= cap) {
              continue;
            }
          }

          const pickedSessionId = this.pickSessionForJob(
            job,
            policy,
            candidates,
            nowMs,
          );
          if (!pickedSessionId) {
            continue;
          }

          this.beginAttemptInTxn(job, pickedSessionId, nowMs);
          queue.rotateKeyToBack(job.priority, job.fairnessKey);
          notifyJobs?.add(job.scopedJobId);
          dispatchedCount += 1;

          const pickedSession = this.sessions.get(pickedSessionId);
          if (!pickedSession || this.sessionFreeSlots(pickedSession) === 0) {
            const cIdx = candidates.indexOf(pickedSessionId);
            if (cIdx >= 0) {
              candidates.splice(cIdx, 1);
            }
          }
        }
      }
    }

    return dispatchedCount;
  }

  private pickSessionForJob(
    job: JobMem,
    policy: InterfacePolicyConfig | undefined,
    candidates: string[],
    nowMs: number,
  ): string | null {
    const demand = job.spec.demand;
    const hasPreferred = Object.keys(demand.preferred_attributes).length > 0;
    const spillOpen =
      !hasPreferred ||
      job.spilloverAtMs === null ||
      nowMs >= job.spilloverAtMs;
    const tiers: PoolTierConfig[] = policy?.poolTiers ?? [];
    const elapsedMs = Math.max(0, nowMs - job.queuedSinceMs);

    let best: SessionMem[] = [];
    let bestHealthy = false;
    let bestNotCooled = false;
    let bestTierIdx = Number.MAX_SAFE_INTEGER; // smaller tier index is higher rank!
    let bestMatches = -1;

    const isBetterRank = (
      h: boolean,
      nc: boolean,
      tIdx: number,
      m: number,
    ): number => {
      if (h !== bestHealthy) return h ? 1 : -1;
      if (nc !== bestNotCooled) return nc ? 1 : -1;
      if (tIdx !== bestTierIdx) return tIdx < bestTierIdx ? 1 : -1;
      if (m !== bestMatches) return m > bestMatches ? 1 : -1;
      return 0;
    };

    for (const sid of candidates) {
      const s = this.sessions.get(sid);
      if (!s || this.sessionFreeSlots(s) === 0) continue;

      const tIdx = this.tierIndexForSession(tiers, s.pool, elapsedMs);
      if (tIdx === null) continue;

      if (!this.matchesRequiredAttributes(demand.required_attributes, s.attributes)) {
        continue;
      }
      const prefKeys = Object.entries(demand.preferred_attributes);
      let matches = 0;
      for (const [k, v] of prefKeys) {
        if (s.attributes[k] === v) matches += 1;
      }
      if (!spillOpen && matches < prefKeys.length) {
        continue;
      }
      if (!this.sessionFitsCapacity(s, demand.required_capacity)) {
        continue;
      }

      const healthy =
        !job.failedWorkers.has(s.workerId) && !job.rejectedSessions.has(s.sessionId);
      const notCooled = nowMs >= s.cooldownUntilMs;

      if (best.length === 0) {
        best = [s];
        bestHealthy = healthy;
        bestNotCooled = notCooled;
        bestTierIdx = tIdx;
        bestMatches = matches;
      } else {
        const cmp = isBetterRank(healthy, notCooled, tIdx, matches);
        if (cmp > 0) {
          best = [s];
          bestHealthy = healthy;
          bestNotCooled = notCooled;
          bestTierIdx = tIdx;
          bestMatches = matches;
        } else if (cmp === 0) {
          best.push(s);
        }
      }
    }

    if (best.length === 0) {
      return null;
    }

    // Wait up to 5s if a healthy / non-cooldown peer exists on the interface (currently busy)
    if ((!bestHealthy || !bestNotCooled) && elapsedMs < 5_000) {
      const allSids =
        this.sessionsByInterface.get(job.namespace)?.get(job.interfaceName) ??
        new Set<string>();
      for (const peerSid of allSids) {
        const peer = this.sessions.get(peerSid);
        if (!peer || !peer.connected || peer.draining) continue;
        if (this.tierIndexForSession(tiers, peer.pool, elapsedMs) === null) continue;
        if (!this.matchesRequiredAttributes(demand.required_attributes, peer.attributes)) {
          continue;
        }
        const totalCapOk = Object.entries(demand.required_capacity).every(
          ([name, needed]) =>
            needed <= 0n || (peer.totalCapacity[name] ?? 0n) >= needed,
        );
        if (!totalCapOk) continue;
        const peerBetter = !bestHealthy
          ? !job.failedWorkers.has(peer.workerId) &&
            !job.rejectedSessions.has(peer.sessionId)
          : nowMs >= peer.cooldownUntilMs;
        if (peerBetter) {
          return null;
        }
      }
    }

    if (best.length === 1) {
      return best[0]!.sessionId;
    }

    // Bounded-load Rendezvous Hashing for `affinity_key` (< 0.85 utilization)
    if (demand.affinity_key.length > 0) {
      const underLoad = best.filter(
        (s) => s.activeLeases.size / Math.max(1, s.maxConcurrency) < 0.85,
      );
      if (underLoad.length > 0) {
        let winner = underLoad[0]!;
        let winnerScore = rendezvousHash64(demand.affinity_key, winner.workerId);
        for (let i = 1; i < underLoad.length; i++) {
          const cand = underLoad[i]!;
          const score = rendezvousHash64(demand.affinity_key, cand.workerId);
          if (
            score > winnerScore ||
            (score === winnerScore && cand.workerId > winner.workerId)
          ) {
            winner = cand;
            winnerScore = score;
          }
        }
        return winner.sessionId;
      }
    }

    // Power-of-two-choices by load_score
    const n = BigInt(best.length);
    const first = Number(nextRandom64(this.rng) % n);
    let second = Number(nextRandom64(this.rng) % (n - 1n));
    if (second >= first) {
      second += 1;
    }
    const a = best[first]!;
    const b = best[second]!;
    return this.sessionLoadScore(b) < this.sessionLoadScore(a)
      ? b.sessionId
      : a.sessionId;
  }

  private tierIndexForSession(
    tiers: readonly PoolTierConfig[],
    pool: string,
    elapsedMs: number,
  ): number | null {
    if (tiers.length === 0) {
      return 0;
    }
    const idx = tiers.findIndex((t) => t.pool === pool);
    if (idx < 0) {
      return null;
    }
    return elapsedMs >= tiers[idx]!.spilloverAfterMs ? idx : null;
  }

  private matchesRequiredAttributes(
    required: Record<string, string>,
    actual: Record<string, string>,
  ): boolean {
    for (const [k, v] of Object.entries(required)) {
      if (actual[k] !== v) return false;
    }
    return true;
  }

  private sessionLoadScore(s: SessionMem): number {
    const conc = Math.max(1, s.maxConcurrency);
    return (
      s.activeLeases.size / conc +
      s.utilization * 0.5 +
      (s.queued / conc) * 0.25
    );
  }

  private beginAttemptInTxn(
    job: JobMem,
    sessionId: string,
    nowMs: number,
  ): void {
    this.dequeueJobInMemory(job);
    const session = this.sessions.get(sessionId)!;
    const leaseId = crypto.randomUUID();
    const attemptId = crypto.randomUUID();
    const attemptNumber = job.attemptCount + 1;

    // Inline Assign vs inputsViaAttempt check (section 5.1 & 5.3)
    let inlineInputs: pb.ProtoObjectInput[] = [];
    let inlineParams: Uint8Array = new Uint8Array(0);
    let inputsViaAttempt =
      job.streamingInput || job.totalInputBytes > this.config.assignInlineLimit;

    if (!inputsViaAttempt && (job.spec.input_count > 0 || job.spec.params_len > 0)) {
      const rows = this.store.exec<{
        seq: number;
        manifest: string;
        pack_blob: Uint8Array | null;
      }>(
        `SELECT seq, manifest, pack_blob FROM native_object_bundles
         WHERE scoped_job_id = ? AND role = 'static_in'
         ORDER BY seq ASC`,
        job.scopedJobId,
      );
      let assignInlineBytes = 0;
      let hasR2Ref = false;
      const loadedInputs: pb.ProtoObjectInput[] = [];
      let loadedParams: Uint8Array = new Uint8Array(0);

      for (const r of rows) {
        const entries = JSON.parse(r.manifest) as BundleManifestEntry[];
        const blob = r.pack_blob ? normalizeBlob(r.pack_blob) : new Uint8Array(0);
        for (const e of entries) {
          if (e.t === "r2" || e.t === "pr2" || e.t === "segs") {
            hasR2Ref = true;
            break;
          }
          if (e.t === "u") {
            const meta = e.m ?? {};
            assignInlineBytes += utf8Len(e.u) + metadataByteLen(meta);
            loadedInputs.push({
              body: { case: "uri", value: e.u },
              metadata: meta,
            });
          } else if (e.t === "i") {
            const meta = e.m ?? {};
            const slice = blob.slice(e.o, e.o + e.l);
            assignInlineBytes += slice.byteLength + metadataByteLen(meta);
            loadedInputs.push({
              body: { case: "inline", value: slice },
              metadata: meta,
            });
          } else if (e.t === "p") {
            loadedParams = blob.slice(e.o, e.o + e.l);
            assignInlineBytes += loadedParams.byteLength;
          }
        }
      }

      if (hasR2Ref || assignInlineBytes > this.config.assignInlineLimit) {
        inputsViaAttempt = true;
      } else {
        inlineInputs = loadedInputs;
        inlineParams = loadedParams;
      }
    }

    const execDeadlineMs =
      job.spec.policy.execution_timeout_ms !== null
        ? nowMs + job.spec.policy.execution_timeout_ms
        : null;

    this.attachLeaseToSessionMem(session, leaseId, job.spec.demand.required_capacity);
    session.assignedSinceReport += 1;
    this.persistSessionRowInTxn(session);

    const concurrencyCounted = job.concurrencyKey.length > 0;
    if (concurrencyCounted) {
      this.incConcurrencyKey(job.namespace, job.concurrencyKey);
    }

    const lease: LeaseMem = {
      leaseId,
      scopedJobId: job.scopedJobId,
      rawJobId: job.rawJobId,
      namespace: job.namespace,
      attemptId,
      attemptNumber,
      sessionId: session.sessionId,
      workerId: session.workerId,
      generation: this.generation,
      reservedCapacity: { ...job.spec.demand.required_capacity },
      startedAtMs: nowMs,
      executionDeadlineMs: execDeadlineMs,
      inputsViaAttempt,
      accepted: false,
      cancelling: false,
      cancelDeadlineMs: null,
      attemptConnId: null,
      lastFrameSeq: 0n,
      exposed: false,
      concurrencyCounted,
      inputStage: inputsViaAttempt ? 0 : 4,
      inputItemIdx: 0,
      inputByteOff: 0,
      inputSeq: 0n,
      curObjIndex: null,
      curObjMeta: {},
      curObjUri: "",
      curObjBytes: 0,
      nextObjIndex: 0,
      tailBytes: 0,
      tailNextChunkSeq: 0,
      nextSegIndex: 0,
      totalResultBytes: 0,
      ackedResultBytes: 0,
      resultTooLarge: false,
      completedObjs: [],
      inlineResultBlob: new Uint8Array(0),
    };
    this.leases.set(leaseId, lease);
    this.insertLeaseRowInTxn(lease);

    job.state = "running";
    job.attemptCount = attemptNumber;
    job.currentLeaseId = leaseId;
    job.lastAttemptId = attemptId;
    job.lastWorkerId = session.workerId;
    job.updatedAtMs = nowMs;

    this.appendJobEventInTxn(
      job,
      attemptId,
      "assigned",
      { workerId: session.workerId, attemptNumber },
      null,
      EVENT_OVERHEAD_BYTES,
      nowMs,
    );

    const lastSplit = splitU64(job.lastSequence);
    this.store.exec(
      `UPDATE native_jobs
       SET state = 'running', attempt_count = ?, updated_at_ms = ?,
           last_attempt_id = ?, last_worker_id = ?,
           last_seq_hi = ?, last_seq_lo = ?
       WHERE scoped_job_id = ?`,
      job.attemptCount,
      nowMs,
      attemptId,
      session.workerId,
      lastSplit.hi,
      lastSplit.lo,
      job.scopedJobId,
    );

    this.committedBatches += 1;
    this.committedOps += 1;

    // Send Assign AFTER attempt count & lease reservation are committed!
    this.sendControlMessageToSession(
      session.sessionId,
      {
        msg: {
          case: "assign",
          value: {
            leaseId,
            attemptId,
            jobId: job.rawJobId,
            attemptNumber,
            generation: this.generation,
            interface: job.interfaceName,
            params: inputsViaAttempt ? new Uint8Array(0) : inlineParams,
            reservedCapacity: job.spec.demand.required_capacity,
            metadata: job.spec.metadata,
            inlineInputs: inputsViaAttempt ? [] : inlineInputs,
            inputsViaAttempt,
            streamingInput: job.streamingInput,
            executionDeadlineMs: execDeadlineMs !== null ? BigInt(execDeadlineMs) : 0n,
            totalDeadlineMs:
              job.totalDeadlineMs !== null ? BigInt(job.totalDeadlineMs) : 0n,
          },
        },
      },
      nowMs,
    );
  }

  // ---------------------------------------------------------------------------
  // Retry, Failure, Cancellation & Administrative Reset (Section 4.1, 4.5, 4.6)
  // ---------------------------------------------------------------------------

  private endAttemptWithErrorInTxn(
    scopedJobId: string,
    error: StoredErrorInfo,
    leaseEnd: { mode: "release" } | { mode: "cancel"; reason: string },
    receipt: { leaseId: string; digest: string } | null,
    nowMs: number,
  ): boolean {
    const job = this.jobs.get(scopedJobId);
    if (!job || !job.currentLeaseId) {
      return false;
    }
    const lease = this.leases.get(job.currentLeaseId);
    if (!lease) {
      job.currentLeaseId = null;
      return false;
    }

    if (lease.concurrencyCounted && job.concurrencyKey.length > 0) {
      lease.concurrencyCounted = false;
      this.decConcurrencyKey(job.namespace, job.concurrencyKey);
    }

    job.failedWorkers.add(lease.workerId);
    job.currentLeaseId = null;

    // Session cooldown after 3 consecutive failures
    if (
      error.code === RUNTIME_CODES.WORKER_FAILED ||
      error.code === RUNTIME_CODES.WORKER_REJECTED ||
      error.code === RUNTIME_CODES.EXECUTION_TIMEOUT ||
      error.code === RUNTIME_CODES.WORKER_LOST
    ) {
      const session = this.sessions.get(lease.sessionId);
      if (session) {
        session.consecutiveFailures += 1;
        if (session.consecutiveFailures >= 3) {
          session.cooldownUntilMs = nowMs + 2_000;
        }
        this.persistSessionRowInTxn(session);
      }
    }

    if (receipt) {
      this.insertReceiptInTxn(
        receipt.leaseId,
        job.scopedJobId,
        job.namespace,
        receipt.digest,
        nowMs,
      );
    }

    const canRetry =
      error.retryable &&
      !lease.exposed &&
      job.cancelRequested === null &&
      job.attemptCount < job.maxAttempts;

    if (canRetry) {
      const backoffMs =
        error.code === RUNTIME_CODES.WORKER_PREEMPTED
          ? 0
          : this.computeRetryBackoffMs(job.attemptCount);
      job.state = "queued";
      job.queuedSinceMs = nowMs;
      job.updatedAtMs = nowMs;
      job.notBeforeMs = nowMs + backoffMs;
      const queueTimeoutMs =
        job.spec.policy.queue_timeout_ms ??
        this.config.interfaces[job.interfaceName]?.queueTimeoutMs ??
        null;
      job.queueDeadlineMs =
        queueTimeoutMs !== null ? nowMs + queueTimeoutMs : null;

      this.appendJobEventInTxn(
        job,
        lease.attemptId,
        "attempt_restarted",
        { previousAttemptId: lease.attemptId, reason: error },
        null,
        EVENT_OVERHEAD_BYTES,
        nowMs,
      );

      this.queueSeq += 1n;
      job.queueSeq = this.queueSeq;
      this.getOrCreateQueue(job.namespace, job.interfaceName).insert(
        this.config.priorityLevels,
        job.priority,
        job.fairnessKey,
        { seq: job.queueSeq, scopedJobId: job.scopedJobId },
      );

      const lastSplit = splitU64(job.lastSequence);
      this.store.exec(
        `UPDATE native_jobs
         SET state = 'queued', updated_at_ms = ?, queued_since_ms = ?,
             not_before_ms = ?, queue_deadline_ms = ?,
             failed_workers_json = ?, rejected_sessions_json = ?,
             last_seq_hi = ?, last_seq_lo = ?
         WHERE scoped_job_id = ?`,
        nowMs,
        nowMs,
        job.notBeforeMs,
        job.queueDeadlineMs,
        JSON.stringify([...job.failedWorkers]),
        JSON.stringify([...job.rejectedSessions]),
        lastSplit.hi,
        lastSplit.lo,
        job.scopedJobId,
      );

      if (leaseEnd.mode === "release") {
        this.releaseLeaseInTxn(lease.leaseId, nowMs);
      } else {
        this.cancelLeaseInTxn(lease.leaseId, leaseEnd.reason, nowMs);
      }
      return true;
    }

    // Terminal failure!
    let finalError: StoredErrorInfo = error;
    if (lease.exposed && error.retryable) {
      finalError = {
        code: RUNTIME_CODES.OUTPUT_INTERRUPTED,
        message: `attempt failed after its output was delivered: ${error.message}`,
        retryable: false,
      };
    } else if (error.retryable && job.attemptCount >= job.maxAttempts) {
      finalError = {
        code: RUNTIME_CODES.ATTEMPTS_EXHAUSTED,
        message: `${error.message} (${error.code}) after ${job.attemptCount} attempts`,
        retryable: false,
      };
    }

    this.applyJobTerminalInTxn(
      job,
      "failed",
      finalError,
      receipt ? receipt.leaseId : null,
      receipt ? receipt.digest : null,
      nowMs,
    );

    if (leaseEnd.mode === "release") {
      this.releaseLeaseInTxn(lease.leaseId, nowMs);
    } else {
      this.cancelLeaseInTxn(lease.leaseId, leaseEnd.reason, nowMs);
    }
    return false;
  }

  private computeRetryBackoffMs(attemptCount: number): number {
    const exp = Math.min(16, Math.max(0, attemptCount - 1));
    return Math.min(
      this.config.defaultMaxBackoffMs,
      this.config.defaultInitialBackoffMs * (1 << exp),
    );
  }

  private cancelJobInTxn(scopedJobId: string, reason: string, nowMs: number): void {
    const job = this.jobs.get(scopedJobId);
    if (
      !job ||
      job.state === "succeeded" ||
      job.state === "failed" ||
      job.state === "cancelled"
    ) {
      return;
    }
    if (job.cancelRequested === null) {
      job.cancelRequested = reason;
    }
    const activeLeaseId = job.currentLeaseId;
    if (activeLeaseId) {
      const lease = this.leases.get(activeLeaseId);
      if (lease && lease.concurrencyCounted && job.concurrencyKey.length > 0) {
        lease.concurrencyCounted = false;
        this.decConcurrencyKey(job.namespace, job.concurrencyKey);
      }
      job.currentLeaseId = null;
    } else {
      this.dequeueJobInMemory(job);
    }

    const cancelReason = job.cancelRequested || reason;
    this.applyJobTerminalInTxn(
      job,
      "cancelled",
      {
        code: RUNTIME_CODES.CANCELLED,
        message: cancelReason,
        retryable: false,
      },
      null,
      null,
      nowMs,
    );

    if (activeLeaseId) {
      this.cancelLeaseInTxn(activeLeaseId, cancelReason, nowMs);
    }
  }

  private failJobInTxn(
    scopedJobId: string,
    code: string,
    message: string,
    nowMs: number,
  ): void {
    const job = this.jobs.get(scopedJobId);
    if (
      !job ||
      job.state === "succeeded" ||
      job.state === "failed" ||
      job.state === "cancelled"
    ) {
      return;
    }
    const activeLeaseId = job.currentLeaseId;
    if (activeLeaseId) {
      const lease = this.leases.get(activeLeaseId);
      if (lease && lease.concurrencyCounted && job.concurrencyKey.length > 0) {
        lease.concurrencyCounted = false;
        this.decConcurrencyKey(job.namespace, job.concurrencyKey);
      }
      job.currentLeaseId = null;
    } else {
      this.dequeueJobInMemory(job);
    }

    this.applyJobTerminalInTxn(
      job,
      "failed",
      { code, message, retryable: false },
      null,
      null,
      nowMs,
    );

    if (activeLeaseId) {
      this.cancelLeaseInTxn(activeLeaseId, code, nowMs);
    }
  }

  private applyJobTerminalInTxn(
    job: JobMem,
    terminalState: "succeeded" | "failed" | "cancelled",
    error: StoredErrorInfo | null,
    winningLeaseId: string | null,
    resultDigest: string | null,
    nowMs: number,
  ): void {
    this.dequeueJobInMemory(job);
    this.totalInputBytesHeld = Math.max(0, this.totalInputBytesHeld - job.totalInputBytes);

    // Drop stream_in bundle rows on terminal (queueing any R2 keys for GC); keep static_in previews for durable jobs or drop if ephemeral?
    // Note: keeping static_in for retained jobs lets /api/jobs/:id show input previews until retention expiry!
    if (job.streamingInput) {
      const streamRows = this.store.exec<{ manifest: string }>(
        `SELECT manifest FROM native_object_bundles WHERE scoped_job_id = ? AND role = 'stream_in'`,
        job.scopedJobId,
      );
      const r2Keys: string[] = [];
      for (const r of streamRows) {
        r2Keys.push(...extractR2KeysFromManifest(r.manifest));
      }
      if (r2Keys.length > 0) {
        this.store.queuePendingDeleteInTxn(r2Keys, nowMs);
      }
      this.store.exec(
        `DELETE FROM native_object_bundles WHERE scoped_job_id = ? AND role = 'stream_in'`,
        job.scopedJobId,
      );
    }

    job.state = terminalState;
    job.updatedAtMs = nowMs;
    job.error = error;
    job.winningLeaseId = winningLeaseId;
    job.resultDigest = resultDigest;
    job.queueDeadlineMs = null;
    job.totalDeadlineMs = null;
    job.stalledSinceMs = null;

    this.appendJobEventInTxn(
      job,
      job.lastAttemptId,
      "finished",
      { state: terminalState, error },
      null,
      EVENT_OVERHEAD_BYTES,
      nowMs,
    );

    const lastSplit = splitU64(job.lastSequence);
    const evSplit = splitU64(job.evictedThrough);
    this.store.exec(
      `UPDATE native_jobs
       SET state = ?, updated_at_ms = ?, error_json = ?,
           winning_lease_id = ?, result_digest = ?,
           failed_workers_json = ?, rejected_sessions_json = ?,
           last_seq_hi = ?, last_seq_lo = ?,
           evicted_seq_hi = ?, evicted_seq_lo = ?
       WHERE scoped_job_id = ?`,
      terminalState,
      nowMs,
      error ? JSON.stringify(error) : null,
      winningLeaseId,
      resultDigest,
      JSON.stringify([...job.failedWorkers]),
      JSON.stringify([...job.rejectedSessions]),
      lastSplit.hi,
      lastSplit.lo,
      evSplit.hi,
      evSplit.lo,
      job.scopedJobId,
    );

    this.committedBatches += 1;
    this.committedOps += 1;

    // Enforce maxTerminalJobs bound
    this.enforceMaxTerminalJobsInTxn(nowMs);
  }

  /**
   * Administrative Reset (`POST /api/admin/reset`, sections 4.1 & 4.6):
   * - ONLY place where `generation` increments!
   * - Fences all active leases, releases session reservations, cleans up uncommitted uploads
   * - Fails exposed `Invoke` jobs with `output_interrupted` (or cancels if caller socket gone)
   * - Re-queues unexposed/durable unfinished jobs (or fails with `attempts_exhausted` when `attempt_count >= max_attempts`)
   * - Preserves durable winners & `native_receipts` so `terminal_replay` works across reset!
   */
  async adminReset(nowMs: number): Promise<AdminResetResult> {
    let fencedLeases = 0;
    let requeuedJobs = 0;
    let failedJobs = 0;
    let cancelledJobs = 0;
    const notifyJobs = new Set<string>();

    this.store.transactionSync(() => {
      this.generation += 1n;
      this.rng.state = (this.generation ^ 0x5dee_ce66_d1ce_4e5bn) & MASK_64;
      this.store.setMeta("generation", this.generation.toString());

      // Track which jobs had an exposed active attempt before fencing
      const exposedByJob = new Map<string, boolean>();
      for (const lease of [...this.leases.values()]) {
        if (!lease.cancelling && lease.exposed) {
          exposedByJob.set(lease.scopedJobId, true);
        }
        fencedLeases += 1;
        this.cleanupLeaseUploadsInTxn(lease.leaseId, true, nowMs);
        const session = this.sessions.get(lease.sessionId);
        if (session) {
          this.detachLeaseFromSessionMem(session, lease.leaseId, lease.reservedCapacity);
          this.sendControlMessageToSession(
            session.sessionId,
            {
              msg: {
                case: "fence",
                value: {
                  leaseId: lease.leaseId,
                  reason: "administrative_reset",
                },
              },
            },
            nowMs,
          );
        }
        if (lease.attemptConnId !== null) {
          const aws = this.socketsByConnId.get(lease.attemptConnId);
          this.socketsByConnId.delete(lease.attemptConnId);
          if (aws && aws.readyState === WS_READY_STATE_OPEN) {
            try {
              this.sendWsProtoSync(
                aws,
                pb.encodeAttemptDown({
                  msg: {
                    case: "finished",
                    value: {
                      status: pb.AttemptFinishedStatus.FENCED,
                      reason: "administrative_reset",
                    },
                  },
                }),
              );
              aws.close(1000, "");
            } catch {
              // ignore
            }
          }
        }
      }
      this.leases.clear();
      this.inFlightConcurrency.clear();
      this.store.exec(`DELETE FROM native_active_leases`);

      for (const session of this.sessions.values()) {
        session.activeLeases.clear();
        session.reservedCapacity = {};
        session.assignedSinceReport = 0;
        this.persistSessionRowInTxn(session);
      }

      // Recover unfinished jobs under the new generation
      const newGenBase = this.generation << 32n;
      for (const job of this.jobs.values()) {
        if (job.state !== "queued" && job.state !== "running") {
          continue;
        }
        const wasRunning = job.state === "running";
        job.currentLeaseId = null;
        if (wasRunning && job.lastWorkerId) {
          job.failedWorkers.add(job.lastWorkerId);
        }
        // Advance sequence base to new generation so event sequences stay strictly monotonic!
        if (job.lastSequence < newGenBase) {
          job.lastSequence = newGenBase;
        }

        // Invoke jobs: caller gone => cancel; exposed => fail with output_interrupted!
        if (job.callerConnId !== null) {
          const callerOpen = this.socketsByConnId.has(job.callerConnId);
          if (!callerOpen) {
            this.cancelJobInTxn(job.scopedJobId, "caller disconnected", nowMs);
            cancelledJobs += 1;
            notifyJobs.add(job.scopedJobId);
            continue;
          }
          if (exposedByJob.get(job.scopedJobId)) {
            this.failJobInTxn(
              job.scopedJobId,
              RUNTIME_CODES.OUTPUT_INTERRUPTED,
              "control plane reset after output was exposed to Invoke caller",
              nowMs,
            );
            failedJobs += 1;
            notifyJobs.add(job.scopedJobId);
            continue;
          }
        }

        if (job.attemptCount >= job.maxAttempts) {
          this.failJobInTxn(
            job.scopedJobId,
            RUNTIME_CODES.ATTEMPTS_EXHAUSTED,
            `${job.attemptCount} attempts were started before the control plane restarted`,
            nowMs,
          );
          failedJobs += 1;
          notifyJobs.add(job.scopedJobId);
          continue;
        }

        // Re-queue!
        if (wasRunning) {
          job.state = "queued";
          job.queuedSinceMs = nowMs;
          job.updatedAtMs = nowMs;
          job.notBeforeMs = 0;
          this.queueSeq += 1n;
          job.queueSeq = this.queueSeq;
          this.getOrCreateQueue(job.namespace, job.interfaceName).insert(
            this.config.priorityLevels,
            job.priority,
            job.fairnessKey,
            { seq: job.queueSeq, scopedJobId: job.scopedJobId },
          );
        }
        const lastSplit = splitU64(job.lastSequence);
        this.store.exec(
          `UPDATE native_jobs
           SET state = 'queued', updated_at_ms = ?, queued_since_ms = ?,
               not_before_ms = 0, failed_workers_json = ?,
               last_seq_hi = ?, last_seq_lo = ?
           WHERE scoped_job_id = ?`,
          nowMs,
          job.queuedSinceMs,
          JSON.stringify([...job.failedWorkers]),
          lastSplit.hi,
          lastSplit.lo,
          job.scopedJobId,
        );
        requeuedJobs += 1;
        notifyJobs.add(job.scopedJobId);
      }

      this.dispatchInTxn(nowMs, notifyJobs);
    });

    for (const jid of notifyJobs) {
      await this.notifyJobFollowers(jid);
    }
    await this.refreshAlarm(nowMs);

    return {
      generation: Number(this.generation),
      reset_at_ms: nowMs,
      fenced_leases: fencedLeases,
      requeued_jobs: requeuedJobs,
      failed_jobs: failedJobs,
      cancelled_jobs: cancelledJobs,
    };
  }

  // ---------------------------------------------------------------------------
  // Session & Lease Persistence Helpers
  // ---------------------------------------------------------------------------

  private disconnectSessionInTxn(
    sessionId: string,
    connId: number,
    nowMs: number,
  ): void {
    const session = this.sessions.get(sessionId);
    if (!session || !session.connected || session.currentConnId !== connId) {
      return;
    }
    // Section 4.3: grace_deadline_ms is set ONLY on the connected 1 -> 0 transition!
    session.connected = false;
    session.customLoad = {};
    if (session.activeLeases.size === 0) {
      this.removeSessionInTxn(sessionId);
      return;
    }
    const graceMs = this.resolveSessionReconnectGraceMs(session);
    if (graceMs <= 0) {
      this.expireDisconnectedSessionInTxn(sessionId, connId, nowMs);
      return;
    }
    session.graceDeadlineMs = nowMs + graceMs;
    this.persistSessionRowInTxn(session);
  }

  private resolveSessionReconnectGraceMs(session: SessionMem): number {
    let overrideGrace: number | null = null;
    for (const iface of session.interfaces) {
      const tier = this.config.interfaces[iface]?.poolTiers.find(
        (t) => t.pool === session.pool,
      );
      if (tier && tier.reconnectGraceMs !== null) {
        overrideGrace =
          overrideGrace === null
            ? tier.reconnectGraceMs
            : Math.min(overrideGrace, tier.reconnectGraceMs);
      }
    }
    if (overrideGrace !== null) {
      return overrideGrace;
    }
    return this.config.reconnectGraceMs ?? this.config.heartbeatTimeoutMs;
  }

  private expireDisconnectedSessionInTxn(
    sessionId: string,
    connId: number,
    nowMs: number,
  ): string[] {
    const session = this.sessions.get(sessionId);
    if (!session || session.connected || session.currentConnId !== connId) {
      return [];
    }
    const affectedJobs: string[] = [];
    for (const leaseId of [...session.activeLeases]) {
      const jid = this.leaseLostInTxn(leaseId, nowMs);
      if (jid) affectedJobs.push(jid);
    }
    this.removeSessionInTxn(sessionId);
    return affectedJobs;
  }

  private leaseLostInTxn(leaseId: string, nowMs: number): string | null {
    const lease = this.leases.get(leaseId);
    if (!lease) return null;
    this.cleanupLeaseUploadsInTxn(leaseId, true, nowMs);
    if (lease.cancelling) {
      this.releaseLeaseInTxn(leaseId, nowMs);
      return null;
    }
    const job = this.jobs.get(lease.scopedJobId);
    if (job && job.state === "running" && job.currentLeaseId === leaseId) {
      this.endAttemptWithErrorInTxn(
        job.scopedJobId,
        {
          code: RUNTIME_CODES.WORKER_LOST,
          message: "worker session ended",
          retryable: true,
        },
        { mode: "release" },
        null,
        nowMs,
      );
      return job.scopedJobId;
    }
    this.releaseLeaseInTxn(leaseId, nowMs);
    return null;
  }

  private cancelLeaseInTxn(leaseId: string, reason: string, nowMs: number): void {
    const lease = this.leases.get(leaseId);
    if (!lease) return;
    if (lease.cancelling) return;
    lease.cancelling = true;
    lease.cancelDeadlineMs = nowMs + this.config.cancelGraceMs;
    this.store.exec(
      `UPDATE native_active_leases
       SET cancelling = 1, cancel_deadline_ms = ?, concurrency_counted = 0
       WHERE lease_id = ?`,
      lease.cancelDeadlineMs,
      leaseId,
    );
    this.sendControlMessageToSession(
      lease.sessionId,
      {
        msg: {
          case: "cancel",
          value: { leaseId, reason },
        },
      },
      nowMs,
    );
  }

  private releaseLeaseInTxn(leaseId: string, _nowMs: number): void {
    const lease = this.leases.get(leaseId);
    if (!lease) return;
    this.leases.delete(leaseId);
    this.totalResultBytesHeld = Math.max(
      0,
      this.totalResultBytesHeld - lease.totalResultBytes,
    );
    this.store.exec(`DELETE FROM native_active_leases WHERE lease_id = ?`, leaseId);
    const session = this.sessions.get(lease.sessionId);
    if (session) {
      this.detachLeaseFromSessionMem(session, leaseId, lease.reservedCapacity);
      if (!session.connected && session.activeLeases.size === 0) {
        this.removeSessionInTxn(session.sessionId);
      } else {
        this.persistSessionRowInTxn(session);
      }
    }
  }

  private cleanupLeaseUploadsInTxn(
    leaseId: string,
    deleteR2Segments: boolean,
    nowMs: number,
  ): void {
    if (deleteR2Segments) {
      const segRows = this.store.exec<{ r2_key: string | null }>(
        `SELECT r2_key FROM native_upload_segments WHERE lease_id = ? AND r2_key IS NOT NULL`,
        leaseId,
      );
      const r2Keys = segRows.map((r) => r.r2_key!).filter(Boolean);
      if (r2Keys.length > 0) {
        this.store.queuePendingDeleteInTxn(r2Keys, nowMs);
      }
    }
    const lease = this.leases.get(leaseId);
    if (lease && (lease.tailNextChunkSeq > 0 || lease.nextSegIndex > 0)) {
      this.store.exec(`DELETE FROM native_upload_tail WHERE lease_id = ?`, leaseId);
      this.store.exec(`DELETE FROM native_upload_segments WHERE lease_id = ?`, leaseId);
    }
  }

  private insertReceiptInTxn(
    leaseId: string,
    scopedJobId: string,
    namespace: string,
    digest: string,
    nowMs: number,
  ): void {
    this.store.exec(
      `INSERT INTO native_receipts (lease_id, scoped_job_id, namespace, outcome_digest, created_at_ms)
       VALUES (?, ?, ?, ?, ?)
       ON CONFLICT(lease_id) DO NOTHING`,
      leaseId,
      scopedJobId,
      namespace,
      digest,
      nowMs,
    );
  }

  private sessionFreeSlots(s: SessionMem): number {
    if (!s.connected || s.draining) return 0;
    const byServer = Math.max(0, s.maxConcurrency - s.activeLeases.size);
    const byWorker = Math.max(0, s.reportedFree - s.assignedSinceReport);
    return Math.min(byServer, byWorker);
  }

  private sessionFitsCapacity(
    s: SessionMem,
    demand: Record<string, bigint>,
  ): boolean {
    for (const [name, needed] of Object.entries(demand)) {
      if (needed <= 0n) continue;
      const total = s.totalCapacity[name];
      if (total === undefined) return false;
      const reserved = s.reservedCapacity[name] ?? 0n;
      if (total - reserved < needed) return false;
    }
    return true;
  }

  private attachLeaseToSessionMem(
    s: SessionMem,
    leaseId: string,
    cap: Record<string, bigint>,
  ): void {
    if (s.activeLeases.has(leaseId)) return;
    s.activeLeases.add(leaseId);
    for (const [k, v] of Object.entries(cap)) {
      if (v === 0n) continue;
      s.reservedCapacity[k] = (s.reservedCapacity[k] ?? 0n) + v;
    }
  }

  private detachLeaseFromSessionMem(
    s: SessionMem,
    leaseId: string,
    cap: Record<string, bigint>,
  ): void {
    if (!s.activeLeases.delete(leaseId)) return;
    for (const [k, v] of Object.entries(cap)) {
      const cur = s.reservedCapacity[k];
      if (cur !== undefined) {
        const next = cur - v;
        if (next <= 0n) {
          delete s.reservedCapacity[k];
        } else {
          s.reservedCapacity[k] = next;
        }
      }
    }
  }

  private indexSessionInMemory(s: SessionMem): void {
    let nsMap = this.sessionsByInterface.get(s.namespace);
    if (!nsMap) {
      nsMap = new Map();
      this.sessionsByInterface.set(s.namespace, nsMap);
    }
    for (const iface of s.interfaces) {
      let set = nsMap.get(iface);
      if (!set) {
        set = new Set();
        nsMap.set(iface, set);
      }
      set.add(s.sessionId);
    }
  }

  private unindexSessionInMemory(s: SessionMem): void {
    const nsMap = this.sessionsByInterface.get(s.namespace);
    if (!nsMap) return;
    for (const iface of s.interfaces) {
      const set = nsMap.get(iface);
      if (set) {
        set.delete(s.sessionId);
        if (set.size === 0) nsMap.delete(iface);
      }
    }
    if (nsMap.size === 0) {
      this.sessionsByInterface.delete(s.namespace);
    }
  }

  private removeSessionInTxn(sessionId: string): void {
    const s = this.sessions.get(sessionId);
    if (s) {
      this.unindexSessionInMemory(s);
      this.sessions.delete(sessionId);
      this.store.exec(`DELETE FROM native_sessions WHERE session_id = ?`, sessionId);
    }
  }

  private persistSessionRowInTxn(s: SessionMem): void {
    this.store.exec(
      `INSERT INTO native_sessions (
        session_id, worker_id, namespace, pool, forced_pool,
        interfaces_json, attributes_json, metadata_json,
        total_capacity_json, reserved_capacity_json, active_leases_json,
        max_concurrency, reported_free, assigned_since_report,
        draining, connected, current_conn_id, connected_at_ms, last_seen_ms,
        grace_deadline_ms, consecutive_failures, cooldown_until_ms,
        queued, running, utilization
      ) VALUES (
        ?, ?, ?, ?, ?,
        ?, ?, ?,
        ?, ?, ?,
        ?, ?, ?,
        ?, ?, ?, ?, ?,
        ?, ?, ?,
        ?, ?, ?
      )
      ON CONFLICT(session_id) DO UPDATE SET
        pool = excluded.pool,
        forced_pool = excluded.forced_pool,
        interfaces_json = excluded.interfaces_json,
        attributes_json = excluded.attributes_json,
        metadata_json = excluded.metadata_json,
        total_capacity_json = excluded.total_capacity_json,
        reserved_capacity_json = excluded.reserved_capacity_json,
        active_leases_json = excluded.active_leases_json,
        max_concurrency = excluded.max_concurrency,
        reported_free = excluded.reported_free,
        assigned_since_report = excluded.assigned_since_report,
        draining = excluded.draining,
        connected = excluded.connected,
        current_conn_id = excluded.current_conn_id,
        last_seen_ms = excluded.last_seen_ms,
        grace_deadline_ms = excluded.grace_deadline_ms,
        consecutive_failures = excluded.consecutive_failures,
        cooldown_until_ms = excluded.cooldown_until_ms,
        queued = excluded.queued,
        running = excluded.running,
        utilization = excluded.utilization`,
      s.sessionId,
      s.workerId,
      s.namespace,
      s.pool,
      s.forcedPool,
      JSON.stringify(s.interfaces),
      JSON.stringify(s.attributes),
      JSON.stringify(s.metadata),
      serializeCapacityJson(s.totalCapacity),
      serializeCapacityJson(s.reservedCapacity),
      JSON.stringify([...s.activeLeases]),
      s.maxConcurrency,
      s.reportedFree,
      s.assignedSinceReport,
      s.draining ? 1 : 0,
      s.connected ? 1 : 0,
      s.currentConnId,
      s.connectedAtMs,
      s.lastSeenMs,
      s.graceDeadlineMs,
      s.consecutiveFailures,
      s.cooldownUntilMs,
      s.queued,
      s.running,
      s.utilization,
    );
  }

  private insertLeaseRowInTxn(l: LeaseMem): void {
    const fSplit = splitU64(l.lastFrameSeq);
    const iSplit = splitU64(l.inputSeq);
    this.store.exec(
      `INSERT INTO native_active_leases (
        lease_id, scoped_job_id, raw_job_id, namespace, attempt_id, attempt_number,
        session_id, worker_id, generation_text, reserved_capacity_json,
        started_at_ms, execution_deadline_ms, inputs_via_attempt, accepted,
        cancelling, cancel_deadline_ms, attempt_conn_id,
        last_frame_seq_hi, last_frame_seq_lo, exposed, concurrency_counted,
        input_stage, input_item_idx, input_byte_off, input_seq_hi, input_seq_lo,
        cur_obj_index, cur_obj_meta_json, cur_obj_uri, cur_obj_bytes,
        next_obj_index, tail_bytes, tail_next_chunk_seq, next_seg_index,
        total_result_bytes, acked_result_bytes, result_too_large,
        completed_objs_json, inline_result_blob
      ) VALUES (
        ?, ?, ?, ?, ?, ?,
        ?, ?, ?, ?,
        ?, ?, ?, ?,
        ?, ?, ?,
        ?, ?, ?, ?,
        ?, ?, ?, ?, ?,
        ?, ?, ?, ?,
        ?, ?, ?, ?,
        ?, ?, ?,
        ?, ?
      )`,
      l.leaseId,
      l.scopedJobId,
      l.rawJobId,
      l.namespace,
      l.attemptId,
      l.attemptNumber,
      l.sessionId,
      l.workerId,
      l.generation.toString(),
      serializeCapacityJson(l.reservedCapacity),
      l.startedAtMs,
      l.executionDeadlineMs,
      l.inputsViaAttempt ? 1 : 0,
      l.accepted ? 1 : 0,
      l.cancelling ? 1 : 0,
      l.cancelDeadlineMs,
      l.attemptConnId,
      fSplit.hi,
      fSplit.lo,
      l.exposed ? 1 : 0,
      l.concurrencyCounted ? 1 : 0,
      l.inputStage,
      l.inputItemIdx,
      l.inputByteOff,
      iSplit.hi,
      iSplit.lo,
      l.curObjIndex,
      l.curObjIndex !== null ? JSON.stringify(l.curObjMeta) : null,
      l.curObjIndex !== null ? l.curObjUri : null,
      l.curObjBytes,
      l.nextObjIndex,
      l.tailBytes,
      l.tailNextChunkSeq,
      l.nextSegIndex,
      l.totalResultBytes,
      l.ackedResultBytes,
      l.resultTooLarge ? 1 : 0,
      JSON.stringify(l.completedObjs),
      l.inlineResultBlob.byteLength > 0 ? l.inlineResultBlob : null,
    );
  }

  private persistLeaseRowInTxn(l: LeaseMem): void {
    const fSplit = splitU64(l.lastFrameSeq);
    const iSplit = splitU64(l.inputSeq);
    this.store.exec(
      `UPDATE native_active_leases SET
        accepted = ?, attempt_conn_id = ?,
        last_frame_seq_hi = ?, last_frame_seq_lo = ?, exposed = ?,
        input_stage = ?, input_item_idx = ?, input_byte_off = ?,
        input_seq_hi = ?, input_seq_lo = ?,
        cur_obj_index = ?, cur_obj_meta_json = ?, cur_obj_uri = ?, cur_obj_bytes = ?,
        next_obj_index = ?, tail_bytes = ?, tail_next_chunk_seq = ?, next_seg_index = ?,
        total_result_bytes = ?, acked_result_bytes = ?, result_too_large = ?,
        completed_objs_json = ?, inline_result_blob = ?
       WHERE lease_id = ?`,
      l.accepted ? 1 : 0,
      l.attemptConnId,
      fSplit.hi,
      fSplit.lo,
      l.exposed ? 1 : 0,
      l.inputStage,
      l.inputItemIdx,
      l.inputByteOff,
      iSplit.hi,
      iSplit.lo,
      l.curObjIndex,
      l.curObjIndex !== null ? JSON.stringify(l.curObjMeta) : null,
      l.curObjIndex !== null ? l.curObjUri : null,
      l.curObjBytes,
      l.nextObjIndex,
      l.tailBytes,
      l.tailNextChunkSeq,
      l.nextSegIndex,
      l.totalResultBytes,
      l.ackedResultBytes,
      l.resultTooLarge ? 1 : 0,
      JSON.stringify(l.completedObjs),
      l.inlineResultBlob.byteLength > 0 ? l.inlineResultBlob : null,
      l.leaseId,
    );
  }

  private persistLeaseInputCursorInTxn(l: LeaseMem): void {
    const iSplit = splitU64(l.inputSeq);
    this.store.exec(
      `UPDATE native_active_leases
       SET input_stage = ?, input_item_idx = ?, input_byte_off = ?,
           input_seq_hi = ?, input_seq_lo = ?
       WHERE lease_id = ?`,
      l.inputStage,
      l.inputItemIdx,
      l.inputByteOff,
      iSplit.hi,
      iSplit.lo,
      l.leaseId,
    );
  }

  private persistLeaseResultStateInTxn(l: LeaseMem): void {
    this.store.exec(
      `UPDATE native_active_leases SET
        cur_obj_index = ?, cur_obj_meta_json = ?, cur_obj_uri = ?, cur_obj_bytes = ?,
        next_obj_index = ?, tail_bytes = ?, tail_next_chunk_seq = ?, next_seg_index = ?,
        total_result_bytes = ?, acked_result_bytes = ?, result_too_large = ?,
        completed_objs_json = ?, inline_result_blob = ?
       WHERE lease_id = ?`,
      l.curObjIndex,
      l.curObjIndex !== null ? JSON.stringify(l.curObjMeta) : null,
      l.curObjIndex !== null ? l.curObjUri : null,
      l.curObjBytes,
      l.nextObjIndex,
      l.tailBytes,
      l.tailNextChunkSeq,
      l.nextSegIndex,
      l.totalResultBytes,
      l.ackedResultBytes,
      l.resultTooLarge ? 1 : 0,
      JSON.stringify(l.completedObjs),
      l.inlineResultBlob.byteLength > 0 ? l.inlineResultBlob : null,
      l.leaseId,
    );
  }

  // ---------------------------------------------------------------------------
  // Event Log, Primary Invoke & Watchers Delivery + Pre-sized Single-Buffer Finished
  // ---------------------------------------------------------------------------

  private appendJobEventInTxn(
    job: JobMem,
    attemptId: string | null,
    kind: "accepted" | "assigned" | "output" | "progress" | "attempt_restarted" | "finished",
    meta: Record<string, unknown> | null,
    payload: Uint8Array | null,
    sizeBytes: number,
    nowMs: number,
  ): bigint {
    // Evict oldest evictable non-primary events when over maxEvents or perJobEventCapBytes!
    while (
      job.eventLogCount >= this.config.eventLogMaxEvents ||
      (kind !== "accepted" &&
        kind !== "assigned" &&
        kind !== "attempt_restarted" &&
        kind !== "finished" &&
        job.eventLogBytes + sizeBytes > this.config.perJobEventCapBytes)
    ) {
      const front = this.store.exec<{
        seq_hi: number;
        seq_lo: number;
        size_bytes: number;
      }>(
        `SELECT seq_hi, seq_lo, size_bytes FROM native_job_events
         WHERE scoped_job_id = ?
         ORDER BY seq_hi ASC, seq_lo ASC LIMIT 1`,
        job.scopedJobId,
      )[0];
      if (!front) break;
      const frontSeq = joinU64(front.seq_hi, front.seq_lo);
      if (job.primaryConsumed !== null && frontSeq > job.primaryConsumed) {
        // Primary Invoke consumer has NOT read this event yet; never drop unread primary output!
        break;
      }
      this.store.exec(
        `DELETE FROM native_job_events
         WHERE scoped_job_id = ? AND seq_hi = ? AND seq_lo = ?`,
        job.scopedJobId,
        front.seq_hi,
        front.seq_lo,
      );
      job.evictedThrough = frontSeq;
      job.eventLogCount = Math.max(0, job.eventLogCount - 1);
      job.eventLogBytes = Math.max(0, job.eventLogBytes - Number(front.size_bytes));
      this.totalEventBytesHeld = Math.max(
        0,
        this.totalEventBytesHeld - Number(front.size_bytes),
      );
      const evSplit = splitU64(job.evictedThrough);
      this.store.exec(
        `UPDATE native_jobs SET evicted_seq_hi = ?, evicted_seq_lo = ? WHERE scoped_job_id = ?`,
        evSplit.hi,
        evSplit.lo,
        job.scopedJobId,
      );
    }

    job.lastSequence += 1n;
    const seq = job.lastSequence;
    const sSplit = splitU64(seq);
    this.store.exec(
      `INSERT INTO native_job_events (
        scoped_job_id, seq_hi, seq_lo, attempt_id, at_ms, kind, meta_json, payload, size_bytes
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      job.scopedJobId,
      sSplit.hi,
      sSplit.lo,
      attemptId,
      nowMs,
      kind,
      meta ? JSON.stringify(meta) : null,
      payload && payload.byteLength > 0 ? payload : null,
      sizeBytes,
    );
    job.eventLogCount += 1;
    job.eventLogBytes += sizeBytes;
    this.totalEventBytesHeld += sizeBytes;
    return seq;
  }

  private async notifyJobFollowers(scopedJobId: string): Promise<void> {
    const job = this.jobs.get(scopedJobId);
    if (!job) return;

    const tasks: Array<Promise<void>> = [];
    if (job.callerConnId !== null) {
      tasks.push(this.deliverPrimaryInvokeEvents(scopedJobId));
    }
    const wSet = this.watchersByJob.get(scopedJobId);
    if (wSet && wSet.size > 0) {
      for (const wid of [...wSet]) {
        tasks.push(this.deliverWatcherEvents(wid));
      }
    }
    if (tasks.length > 0) {
      await Promise.all(tasks);
    }
  }

  private async deliverPrimaryInvokeEvents(scopedJobId: string): Promise<void> {
    const job = this.jobs.get(scopedJobId);
    if (!job || job.callerConnId === null) return;
    const ws = this.socketsByConnId.get(job.callerConnId);
    if (!ws || ws.readyState !== WS_READY_STATE_OPEN) return;

    const afterSeq = job.primaryConsumed ?? 0n;
    const rows = this.readJobEventsAfterSync(scopedJobId, afterSeq);
    if (rows.length === 0) return;

    for (const ev of rows) {
      if (ws.readyState !== WS_READY_STATE_OPEN) return;
      const seq = joinU64(ev.seq_hi, ev.seq_lo);
      if (ev.kind === "finished") {
        await this.sendFinishedEventToSocket(
          ws,
          job,
          seq,
          ev.attempt_id,
          Number(ev.at_ms),
        );
        this.store.transactionSync(() => {
          job.primaryConsumed = null;
          job.callerConnId = null;
          this.store.exec(
            `UPDATE native_jobs
             SET caller_conn_id = NULL, primary_seq_hi = NULL, primary_seq_lo = NULL
             WHERE scoped_job_id = ?`,
            job.scopedJobId,
          );
        });
        return;
      }

      const encoded = this.encodeNonTerminalEventWsFrame(job.rawJobId, ev);
      const permit = await this.isolateBudget.acquireOutboundSend(
        ws,
        encoded.byteLength,
        0,
      );
      if (this.disposed || ws.readyState !== WS_READY_STATE_OPEN) {
        permit.abort();
        return;
      }
      try {
        ws.send(encoded);
        permit.completeSend();
      } catch {
        permit.abort();
        return;
      }

      this.store.transactionSync(() => {
        job.primaryConsumed = seq;
        const pSplit = splitU64(seq);
        this.store.exec(
          `UPDATE native_jobs SET primary_seq_hi = ?, primary_seq_lo = ? WHERE scoped_job_id = ?`,
          pSplit.hi,
          pSplit.lo,
          job.scopedJobId,
        );
      });
    }
  }

  private async deliverWatcherEvents(watchConnId: number): Promise<void> {
    const watcher = this.watchers.get(watchConnId);
    if (!watcher || watcher.finished) return;
    const ws = this.socketsByConnId.get(watchConnId);
    if (!ws || ws.readyState !== WS_READY_STATE_OPEN) {
      this.store.transactionSync(() => this.removeWatcherInTxn(watchConnId));
      return;
    }

    const job = this.jobs.get(watcher.scopedJobId);
    if (!job) {
      ws.close(1000, "");
      this.store.transactionSync(() => this.removeWatcherInTxn(watchConnId));
      return;
    }

    // Slow Watch subscriber check (section 5.2: slow Watch is dropped without stalling workers!)
    if (ws.bufferedAmount > this.config.perJobEventCapBytes) {
      this.sendWsErrorAndClose(
        ws,
        GrpcCode.ResourceExhausted,
        "watch subscriber fell too far behind",
      );
      this.store.transactionSync(() => this.removeWatcherInTxn(watchConnId));
      return;
    }

    // Cursor check (`after == 0` or `after >= oldest - 1` is valid; older fails with OutOfRange(11))
    const cursor = watcher.deliveredSeq;
    if (cursor !== 0n && cursor < job.evictedThrough) {
      const oldest = job.evictedThrough + 1n;
      this.sendWsErrorAndClose(
        ws,
        GrpcCode.OutOfRange,
        `cursor expired; oldest available sequence is ${oldest}`,
      );
      this.store.transactionSync(() => this.removeWatcherInTxn(watchConnId));
      return;
    }

    const rows = this.readJobEventsAfterSync(job.scopedJobId, cursor);
    if (rows.length === 0) {
      // Synthesise Accepted + Finished if archived/compacted terminal job has empty event table!
      if (
        (job.state === "succeeded" ||
          job.state === "failed" ||
          job.state === "cancelled") &&
        job.eventLogCount === 0
      ) {
        const base = this.generation << 32n;
        const accSeq = base + 1n;
        const finSeq = base + 0xffff_ffffn;
        if (accSeq > cursor) {
          const accFrame = pb.encodeExecutionEventWsFrame({
            jobId: job.rawJobId,
            sequence: accSeq,
            attemptId: job.lastAttemptId ?? "",
            atMs: BigInt(job.updatedAtMs),
            event: { case: "accepted", value: {} },
          });
          this.sendWsRawSync(ws, accFrame);
        }
        if (finSeq > cursor) {
          await this.sendFinishedEventToSocket(
            ws,
            job,
            finSeq,
            job.lastAttemptId,
            job.updatedAtMs,
          );
        } else {
          ws.close(1000, "");
        }
        this.store.transactionSync(() => this.removeWatcherInTxn(watchConnId));
      }
      return;
    }

    for (const ev of rows) {
      if (ws.readyState !== WS_READY_STATE_OPEN) return;
      const seq = joinU64(ev.seq_hi, ev.seq_lo);
      if (ev.kind === "finished") {
        await this.sendFinishedEventToSocket(
          ws,
          job,
          seq,
          ev.attempt_id,
          Number(ev.at_ms),
        );
        this.store.transactionSync(() => this.removeWatcherInTxn(watchConnId));
        return;
      }

      const encoded = this.encodeNonTerminalEventWsFrame(job.rawJobId, ev);
      const permit = await this.isolateBudget.acquireOutboundSend(
        ws,
        encoded.byteLength,
        0,
      );
      if (this.disposed || ws.readyState !== WS_READY_STATE_OPEN) {
        permit.abort();
        return;
      }
      try {
        ws.send(encoded);
        permit.completeSend();
      } catch {
        permit.abort();
        this.store.transactionSync(() => this.removeWatcherInTxn(watchConnId));
        return;
      }

      // Persist watcher delivered_seq so each watcher resumes independently across hibernation!
      this.store.transactionSync(() => {
        watcher.deliveredSeq = seq;
        const dSplit = splitU64(seq);
        this.store.exec(
          `UPDATE native_watchers SET delivered_seq_hi = ?, delivered_seq_lo = ?
           WHERE watch_conn_id = ?`,
          dSplit.hi,
          dSplit.lo,
          watchConnId,
        );
      });
    }
  }

  private removeWatcherInTxn(watchConnId: number): void {
    const w = this.watchers.get(watchConnId);
    if (!w) return;
    this.watchers.delete(watchConnId);
    const set = this.watchersByJob.get(w.scopedJobId);
    if (set) {
      set.delete(watchConnId);
      if (set.size === 0) this.watchersByJob.delete(w.scopedJobId);
    }
    this.store.exec(
      `DELETE FROM native_watchers WHERE watch_conn_id = ?`,
      watchConnId,
    );
  }

  private readJobEventsAfterSync(scopedJobId: string, afterSeq: bigint) {
    const s = splitU64(afterSeq);
    return this.store.exec<{
      seq_hi: number;
      seq_lo: number;
      attempt_id: string | null;
      at_ms: number;
      kind: string;
      meta_json: string | null;
      payload: Uint8Array | null;
      size_bytes: number;
    }>(
      `SELECT seq_hi, seq_lo, attempt_id, at_ms, kind, meta_json, payload, size_bytes
       FROM native_job_events
       WHERE scoped_job_id = ?
         AND (seq_hi > ? OR (seq_hi = ? AND seq_lo > ?))
       ORDER BY seq_hi ASC, seq_lo ASC`,
      scopedJobId,
      s.hi,
      s.hi,
      s.lo,
    );
  }

  private encodeNonTerminalEventWsFrame(
    rawJobId: string,
    ev: {
      seq_hi: number;
      seq_lo: number;
      attempt_id: string | null;
      at_ms: number;
      kind: string;
      meta_json: string | null;
      payload: Uint8Array | null;
    },
  ): Uint8Array {
    const seq = joinU64(ev.seq_hi, ev.seq_lo);
    const meta = ev.meta_json ? (JSON.parse(ev.meta_json) as Record<string, any>) : {};
    let variant: pb.ExecutionEventVariantInput;
    switch (ev.kind) {
      case "accepted":
        variant = { case: "accepted", value: {} };
        break;
      case "assigned":
        variant = {
          case: "assigned",
          value: {
            workerId: String(meta.workerId ?? ""),
            attemptNumber: Number(meta.attemptNumber ?? 1),
          },
        };
        break;
      case "output":
      case "progress":
        variant = {
          case: "output",
          value: {
            data: ev.payload ? normalizeBlob(ev.payload) : new Uint8Array(0),
            metadata: (meta.metadata as Record<string, string>) ?? {},
            progress: ev.kind === "progress" || Boolean(meta.progress),
          },
        };
        break;
      case "attempt_restarted":
        variant = {
          case: "attemptRestarted",
          value: {
            previousAttemptId: String(meta.previousAttemptId ?? ""),
            reason: meta.reason as StoredErrorInfo | undefined,
          },
        };
        break;
      default:
        variant = { case: "accepted", value: {} };
    }
    return pb.encodeExecutionEventWsFrame({
      jobId: rawJobId,
      sequence: seq,
      attemptId: ev.attempt_id ?? "",
      atMs: BigInt(ev.at_ms),
      event: variant,
    });
  }

  /**
   * Pre-sized single-buffer encoder & sender for `ExecutionEvent{Finished}` (sections 5.1 & 5.2):
   * - Computes exact encoded frame length `L` purely from output manifests (ZERO payload reads yet!)
   * - Reserves `2 * L + r2ReadBuffer` in `IsolateMemoryBudget` (and `2 * L` in `56 MiB` large-send
   *   semaphore when `L > 1 MiB`, so two concurrent `3 x 7 MiB` sends serialize cleanly!)
   * - Allocates ONE `Uint8Array(L)` and streams `<= 2 MiB` slices directly into `buf.set(slice, pos)`!
   * - Calls `ws.send(buf)`, releases JS half (`L` + R2 read buffer) keeping `max(bufferedAmount, L)`
   *   until `bufferedAmount` drains, then sends `ws.close(1000)`!
   */
  private async sendFinishedEventToSocket(
    ws: NativeWebSocket,
    job: JobMem,
    sequence: bigint,
    attemptId: string | null,
    atMs: number,
  ): Promise<void> {
    if (ws.readyState !== WS_READY_STATE_OPEN) return;

    const protoState = nameToProtoState(job.state);
    const outputRefs: OutputManifestItemRef[] = [];
    let needsR2 = false;
    let maxObjLen = 0;

    if (job.state === "succeeded") {
      const manifests = this.store.readBundleManifestsSync(job.scopedJobId, "output");
      for (const m of manifests) {
        for (const e of m.entries) {
          if (e.t === "p" || e.t === "pr2") continue;
          outputRefs.push({ seq: m.seq, entry: e });
          if (e.t !== "u" && e.l > maxObjLen) {
            maxObjLen = e.l;
          }
          if (
            e.t === "r2" ||
            (e.t === "segs" && e.s.some((s) => "k" in s))
          ) {
            needsR2 = true;
          }
        }
      }
    }

    const errorProto: pb.ErrorInfoInput | undefined = job.error
      ? {
          code: job.error.code,
          message: job.error.message,
          retryable: job.error.retryable,
        }
      : undefined;

    // Compute exact encoded length L without reading any output bytes!
    const jobIdBytes = utf8Encoder.encode(job.rawJobId);
    const attIdStr = attemptId ?? "";
    const attIdBytes = utf8Encoder.encode(attIdStr);

    let finishedBodyLen =
      protoState !== 0 ? 1 + varintLen32(protoState) : 0;
    const perObjEncodedLens: number[] = [];
    for (const ref of outputRefs) {
      const objLen = encodedObjectLenFromManifestEntry(ref.entry);
      perObjEncodedLens.push(objLen);
      finishedBodyLen += lenDelimitedSize(1, objLen);
    }
    const errSubLen = errorProto ? pb.encodedLenErrorInfo(errorProto) : 0;
    if (errorProto !== undefined) {
      finishedBodyLen += lenDelimitedSize(1, errSubLen);
    }

    const eventBodyLen =
      (jobIdBytes.byteLength > 0 ? lenDelimitedSize(1, jobIdBytes.byteLength) : 0) +
      (sequence !== 0n ? 1 + varintLen64(sequence) : 0) +
      (attIdBytes.byteLength > 0 ? lenDelimitedSize(1, attIdBytes.byteLength) : 0) +
      (atMs !== 0 ? 1 + varintLen64(BigInt(atMs)) : 0) +
      lenDelimitedSize(1, finishedBodyLen); // field 14 tag = (14<<3)|2 = 114 (1 byte!)

    const totalFrameLen = 1 + eventBodyLen;
    if (totalFrameLen > WS_MAX_ENCODED_BYTES) {
      this.sendWsErrorAndClose(
        ws,
        GrpcCode.ResourceExhausted,
        `ExecutionFinished frame of ${totalFrameLen} bytes exceeds ${WS_MAX_ENCODED_BYTES} bytes`,
      );
      return;
    }

    const r2ReadReserve = needsR2
      ? Math.min(R2_READ_BUFFER_BYTES, Math.max(65_536, maxObjLen))
      : 0;

    const reservation = await this.isolateBudget.acquireOutboundSend(
      ws,
      totalFrameLen,
      r2ReadReserve,
    );

    if (this.disposed || ws.readyState !== WS_READY_STATE_OPEN) {
      reservation.abort();
      return;
    }

    try {
      const nowMs = this.host.nowMs();
      // Allocate ONE single JS Uint8Array for the entire 0x00 || ExecutionEvent{Finished} frame!
      const frame = new Uint8Array(totalFrameLen);
      let pos = 0;
      frame[pos++] = 0x00;

      // field 1: job_id
      if (jobIdBytes.byteLength > 0) {
        pos = writeVarint32At(frame, pos, (1 << 3) | 2);
        pos = writeVarint32At(frame, pos, jobIdBytes.byteLength);
        frame.set(jobIdBytes, pos);
        pos += jobIdBytes.byteLength;
      }
      // field 2: sequence
      if (sequence !== 0n) {
        pos = writeVarint32At(frame, pos, (2 << 3) | 0);
        pos = writeVarint64At(frame, pos, sequence);
      }
      // field 3: attempt_id
      if (attIdBytes.byteLength > 0) {
        pos = writeVarint32At(frame, pos, (3 << 3) | 2);
        pos = writeVarint32At(frame, pos, attIdBytes.byteLength);
        frame.set(attIdBytes, pos);
        pos += attIdBytes.byteLength;
      }
      // field 4: at_ms
      if (atMs !== 0) {
        pos = writeVarint32At(frame, pos, (4 << 3) | 0);
        pos = writeVarint64At(frame, pos, BigInt(atMs));
      }
      // field 14: finished (ExecutionFinished)
      pos = writeVarint32At(frame, pos, (14 << 3) | 2);
      pos = writeVarint32At(frame, pos, finishedBodyLen);

      // ExecutionFinished.state (field 1)
      if (protoState !== 0) {
        pos = writeVarint32At(frame, pos, (1 << 3) | 0);
        pos = writeVarint32At(frame, pos, protoState);
      }

      // ExecutionFinished.outputs (repeated field 2) — streamed directly into `frame`!
      for (let i = 0; i < outputRefs.length; i++) {
        const ref = outputRefs[i]!;
        const entry = ref.entry;
        const objEncodedLen = perObjEncodedLens[i]!;
        pos = writeVarint32At(frame, pos, (2 << 3) | 2);
        pos = writeVarint32At(frame, pos, objEncodedLen);

        if (entry.t === "u") {
          const uriBytes = utf8Encoder.encode(entry.u);
          pos = writeVarint32At(frame, pos, (2 << 3) | 2);
          pos = writeVarint32At(frame, pos, uriBytes.byteLength);
          frame.set(uriBytes, pos);
          pos += uriBytes.byteLength;
          pos = writeStringMapAt(frame, pos, (3 << 3) | 2, entry.m);
        } else {
          // oneof body.inline (field 1): always present even when 0 bytes!
          pos = writeVarint32At(frame, pos, (1 << 3) | 2);
          pos = writeVarint32At(frame, pos, entry.l);
          let copied = 0;
          while (copied < entry.l) {
            const step = Math.min(R2_READ_BUFFER_BYTES, entry.l - copied);
            const slice = await this.store.readEntrySlice(
              job.scopedJobId,
              "output",
              ref.seq,
              entry,
              copied,
              step,
              nowMs,
            );
            if (slice.byteLength === 0) {
              throw new Error(`Short read streaming output for ${job.scopedJobId}`);
            }
            frame.set(slice, pos);
            pos += slice.byteLength;
            copied += slice.byteLength;
          }
          const meta = "m" in entry ? entry.m : undefined;
          pos = writeStringMapAt(frame, pos, (3 << 3) | 2, meta);
        }
      }

      // ExecutionFinished.error (field 3)
      if (errorProto !== undefined) {
        const errBytes = pb.encodeErrorInfo(errorProto);
        pos = writeVarint32At(frame, pos, (3 << 3) | 2);
        pos = writeVarint32At(frame, pos, errBytes.byteLength);
        frame.set(errBytes, pos);
        pos += errBytes.byteLength;
      }

      if (pos !== totalFrameLen) {
        throw new Error(
          `ExecutionFinished pre-sized frame length mismatch: wrote ${pos}, expected ${totalFrameLen}`,
        );
      }

      if (ws.readyState !== WS_READY_STATE_OPEN) {
        reservation.abort();
        return;
      }

      ws.send(frame);
      reservation.completeSend();
      ws.close(1000, "");
    } catch (err) {
      reservation.abort();
      throw err;
    }
  }

  // ---------------------------------------------------------------------------
  // Retention & Purge (Section 4.5 & 6.4)
  // ---------------------------------------------------------------------------

  private enforceMaxTerminalJobsInTxn(nowMs: number): void {
    const terminals: JobMem[] = [];
    for (const j of this.jobs.values()) {
      if (
        j.state === "succeeded" ||
        j.state === "failed" ||
        j.state === "cancelled"
      ) {
        terminals.push(j);
      }
    }
    if (terminals.length <= this.config.maxTerminalJobs) {
      return;
    }
    terminals.sort((a, b) => a.updatedAtMs - b.updatedAtMs);
    const excess = terminals.length - this.config.maxTerminalJobs;
    for (let i = 0; i < excess; i++) {
      this.purgeSingleJobInTxn(terminals[i]!.scopedJobId, nowMs);
    }
  }

  private purgeExpiredTerminalJobsInTxn(nowMs: number, maxCount: number): number {
    const cutoff = nowMs - this.config.terminalRetentionMs;
    let purged = 0;
    for (const j of [...this.jobs.values()]) {
      if (purged >= maxCount) break;
      if (
        (j.state === "succeeded" ||
          j.state === "failed" ||
          j.state === "cancelled") &&
        j.updatedAtMs <= cutoff
      ) {
        this.purgeSingleJobInTxn(j.scopedJobId, nowMs);
        purged += 1;
      }
    }
    return purged;
  }

  private purgeSingleJobInTxn(scopedJobId: string, nowMs: number): void {
    const job = this.jobs.get(scopedJobId);
    if (!job) return;

    // Collect all R2 keys from bundles and move to `pending_delete` in the SAME transaction!
    const bundleRows = this.store.exec<{ manifest: string }>(
      `SELECT manifest FROM native_object_bundles WHERE scoped_job_id = ?`,
      scopedJobId,
    );
    const r2Keys: string[] = [];
    for (const r of bundleRows) {
      r2Keys.push(...extractR2KeysFromManifest(r.manifest));
    }
    if (r2Keys.length > 0) {
      this.store.queuePendingDeleteInTxn(r2Keys, nowMs);
    }

    this.store.exec(
      `DELETE FROM native_object_bundles WHERE scoped_job_id = ?`,
      scopedJobId,
    );
    this.store.exec(
      `DELETE FROM native_job_events WHERE scoped_job_id = ?`,
      scopedJobId,
    );
    this.store.exec(
      `DELETE FROM native_receipts WHERE scoped_job_id = ?`,
      scopedJobId,
    );
    this.store.exec(
      `DELETE FROM native_jobs WHERE scoped_job_id = ?`,
      scopedJobId,
    );

    this.store.reservedRetainedBytes = Math.max(
      0,
      this.store.reservedRetainedBytes - job.retainedBytesEst,
    );
    this.totalEventBytesHeld = Math.max(
      0,
      this.totalEventBytesHeld - job.eventLogBytes,
    );
    this.jobs.delete(scopedJobId);
  }

  // ---------------------------------------------------------------------------
  // Protobuf View Conversions & Socket Send Helpers
  // ---------------------------------------------------------------------------

  private jobMemToProtoSyncWithoutOutputs(job: JobMem): pb.Job {
    return {
      jobId: job.rawJobId,
      namespace: job.namespace,
      state: nameToProtoState(job.state),
      durability:
        job.durability === "durable"
          ? pb.Durability.DURABLE
          : pb.Durability.EPHEMERAL,
      interface: job.interfaceName,
      attemptCount: job.attemptCount,
      currentAttemptId: job.lastAttemptId ?? "",
      currentWorkerId: job.lastWorkerId ?? "",
      createdAtMs: BigInt(job.createdAtMs),
      updatedAtMs: BigInt(job.updatedAtMs),
      ...(job.error
        ? {
            error: {
              code: job.error.code,
              message: job.error.message,
              retryable: job.error.retryable,
            },
          }
        : {}),
      outputs: [],
      metadata: { ...job.spec.metadata },
      specHash: job.specHash,
    };
  }

  private async jobMemToProto(
    job: JobMem,
    includeOutputs: boolean,
    nowMs: number,
  ): Promise<pb.Job> {
    const base = this.jobMemToProtoSyncWithoutOutputs(job);
    if (includeOutputs && job.state === "succeeded") {
      const loaded = await this.store.loadBundleObjects(
        job.scopedJobId,
        "output",
        nowMs,
      );
      base.outputs = loaded.objects.map((o) =>
        o.kind === "uri"
          ? { body: { case: "uri", value: o.uri ?? "" }, metadata: o.metadata }
          : {
              body: { case: "inline", value: o.data ?? new Uint8Array(0) },
              metadata: o.metadata,
            },
      );
    }
    return base;
  }

  private sendControlMessageToSession(
    sessionId: string,
    msg: pb.ServerMessageInput,
    nowMs: number,
  ): void {
    const session = this.sessions.get(sessionId);
    if (!session || !session.connected) return;
    const ws = this.socketsByConnId.get(session.currentConnId);
    if (!ws || ws.readyState !== WS_READY_STATE_OPEN) {
      this.disconnectSessionInTxn(sessionId, session.currentConnId, nowMs);
      return;
    }
    try {
      this.sendWsProtoSync(ws, pb.encodeServerMessage(msg));
    } catch {
      this.disconnectSessionInTxn(sessionId, session.currentConnId, nowMs);
    }
  }

  private sendWsProtoSync(ws: NativeWebSocket, protoBytes: Uint8Array): void {
    const frame = pb.encodeWsDataFrame(protoBytes);
    this.sendWsRawSync(ws, frame);
  }

  private sendWsRawSync(ws: NativeWebSocket, frameBytes: Uint8Array): void {
    if (ws.readyState !== WS_READY_STATE_OPEN) return;
    const permit = this.isolateBudget.tryAcquireOutboundSync(
      ws,
      frameBytes.byteLength,
    );
    ws.send(frameBytes);
    if (permit) {
      permit.completeSend();
    }
  }

  private sendWsErrorAndClose(
    ws: NativeWebSocket,
    code: GrpcCode | number,
    message: string,
  ): void {
    if (ws.readyState !== WS_READY_STATE_OPEN) return;
    try {
      const errFrame = pb.encodeWsErrorFrame(code, message);
      ws.send(errFrame);
    } catch {
      // ignore
    }
    try {
      ws.close(1000, "");
    } catch {
      // ignore
    }
  }

  private getOrCreateQueue(
    namespace: string,
    interfaceName: string,
  ): FairPriorityQueue {
    let nsMap = this.queues.get(namespace);
    if (!nsMap) {
      nsMap = new Map();
      this.queues.set(namespace, nsMap);
    }
    let q = nsMap.get(interfaceName);
    if (!q) {
      q = new FairPriorityQueue();
      nsMap.set(interfaceName, q);
    }
    return q;
  }

  private dequeueJobInMemory(job: JobMem): void {
    const q = this.queues.get(job.namespace)?.get(job.interfaceName);
    if (q) {
      q.remove(job.priority, job.fairnessKey, job.scopedJobId);
    }
    job.queueSeq = null;
  }

  private getConcurrencyKeyCount(namespace: string, key: string): number {
    return this.inFlightConcurrency.get(namespace)?.get(key) ?? 0;
  }

  private incConcurrencyKey(namespace: string, key: string): void {
    let m = this.inFlightConcurrency.get(namespace);
    if (!m) {
      m = new Map();
      this.inFlightConcurrency.set(namespace, m);
    }
    m.set(key, (m.get(key) ?? 0) + 1);
  }

  private decConcurrencyKey(namespace: string, key: string): void {
    const m = this.inFlightConcurrency.get(namespace);
    if (!m) return;
    const next = (m.get(key) ?? 0) - 1;
    if (next <= 0) {
      m.delete(key);
      if (m.size === 0) this.inFlightConcurrency.delete(namespace);
    } else {
      m.set(key, next);
    }
  }

  // ---------------------------------------------------------------------------
  // Console & Management Backend Implementation (`ConsoleBackend`)
  // ---------------------------------------------------------------------------

  getHealth(): { live: boolean; ready: boolean; message: string | null } {
    const nowMs = this.host.nowMs();
    const hardWriteErr = this.store.checkHardWriteCeiling(nowMs);
    return {
      live: true,
      ready: hardWriteErr === null,
      message: hardWriteErr,
    };
  }

  getOverviewSnapshot(nowMs: number): ConsoleOverviewSnapshot {
    const health = this.getHealth();
    let queued = 0;
    let running = 0;
    let succeeded = 0;
    let failed = 0;
    let cancelled = 0;

    const ifaceStats = new Map<
      string,
      {
        queued: number;
        running: number;
        oldestQueuedSince: number | null;
        sessions: number;
        freeSlots: number;
      }
    >();
    const ensureIface = (name: string) => {
      let s = ifaceStats.get(name);
      if (!s) {
        s = {
          queued: 0,
          running: 0,
          oldestQueuedSince: null,
          sessions: 0,
          freeSlots: 0,
        };
        ifaceStats.set(name, s);
      }
      return s;
    };

    for (const name of Object.keys(this.config.interfaces)) {
      ensureIface(name);
    }

    for (const j of this.jobs.values()) {
      const st = ensureIface(j.interfaceName);
      switch (j.state) {
        case "queued":
          queued += 1;
          st.queued += 1;
          if (
            st.oldestQueuedSince === null ||
            j.queuedSinceMs < st.oldestQueuedSince
          ) {
            st.oldestQueuedSince = j.queuedSinceMs;
          }
          break;
        case "running":
          running += 1;
          st.running += 1;
          break;
        case "succeeded":
          succeeded += 1;
          break;
        case "failed":
          failed += 1;
          break;
        case "cancelled":
          cancelled += 1;
          break;
      }
    }

    // Refresh scalar load from open session socket attachments
    for (const s of this.sessions.values()) {
      if (s.connected) {
        const ws = this.socketsByConnId.get(s.currentConnId);
        const att = ws ? readSocketAttachment(ws) : null;
        if (att) {
          s.lastSeenMs = Math.max(s.lastSeenMs, att.lastSeenAtMs);
          s.queued = att.queued;
          s.running = att.running;
          s.utilization = att.utilization;
        }
      }
    }

    let wHealthy = 0;
    let wDraining = 0;
    let wActiveLeases = 0;
    const wTotalCap: Record<string, number> = {};
    let wLastSeen: number | null = null;

    const poolMap = new Map<
      string,
      {
        namespace: string;
        interface: string;
        pool: string;
        sessions: number;
        free_slots: number;
        active_leases: number;
        desired_additional_slots: number;
      }
    >();

    for (const s of this.sessions.values()) {
      const free = this.sessionFreeSlots(s);
      wActiveLeases += s.activeLeases.size;
      wLastSeen = wLastSeen === null ? s.lastSeenMs : Math.max(wLastSeen, s.lastSeenMs);
      if (s.draining) wDraining += 1;
      if (s.connected && !s.draining) {
        wHealthy += 1;
        for (const [k, v] of Object.entries(s.totalCapacity)) {
          wTotalCap[k] = (wTotalCap[k] ?? 0) + Number(v);
        }
      }
      for (const iface of s.interfaces) {
        const st = ensureIface(iface);
        st.sessions += 1;
        st.freeSlots += free;

        let activeOnIface = 0;
        for (const lid of s.activeLeases) {
          const lease = this.leases.get(lid);
          const job = lease ? this.jobs.get(lease.scopedJobId) : undefined;
          if (job && job.interfaceName === iface) {
            activeOnIface += 1;
          }
        }
        const pKey = `${s.namespace}\0${iface}\0${s.pool}`;
        let pe = poolMap.get(pKey);
        if (!pe) {
          pe = {
            namespace: s.namespace,
            interface: iface,
            pool: s.pool,
            sessions: 0,
            free_slots: 0,
            active_leases: 0,
            desired_additional_slots: 0,
          };
          poolMap.set(pKey, pe);
        }
        pe.sessions += 1;
        pe.free_slots += free;
        pe.active_leases += activeOnIface;
      }
    }

    for (const [ns, ifaceQueues] of this.queues) {
      for (const [iface, q] of ifaceQueues) {
        if (q.totalLen === 0) continue;
        const primaryPool =
          this.config.interfaces[iface]?.poolTiers[0]?.pool ?? "";
        const pKey = `${ns}\0${iface}\0${primaryPool}`;
        let pe = poolMap.get(pKey);
        if (!pe) {
          pe = {
            namespace: ns,
            interface: iface,
            pool: primaryPool,
            sessions: 0,
            free_slots: 0,
            active_leases: 0,
            desired_additional_slots: 0,
          };
          poolMap.set(pKey, pe);
        }
        pe.desired_additional_slots = Math.max(0, q.totalLen - pe.free_slots);
      }
    }

    const interfaces = [...ifaceStats.entries()]
      .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
      .map(([name, st]) => ({
        name,
        queued: st.queued,
        running: st.running,
        oldest_queued_age_ms:
          st.oldestQueuedSince !== null
            ? Math.max(0, nowMs - st.oldestQueuedSince)
            : 0,
        sessions: st.sessions,
        free_slots: st.freeSlots,
        policy: this.config.interfaces[name] ?? null,
      }));

    const pools = [...poolMap.values()].sort((a, b) => {
      if (a.namespace !== b.namespace) return a.namespace < b.namespace ? -1 : 1;
      if (a.interface !== b.interface) return a.interface < b.interface ? -1 : 1;
      return a.pool < b.pool ? -1 : a.pool > b.pool ? 1 : 0;
    });

    const quota = this.store.quotaSnapshot(nowMs);
    const retainedTerminals = succeeded + failed + cancelled;

    return {
      live: health.live,
      ready: health.ready,
      journalMessage: health.message,
      generation: Number(this.generation),
      bootId: this.bootId,
      constructorRuns: this.constructorRuns,
      droppedCustomLoadEntries: this.droppedCustomLoadEntries,
      retainedJobs: retainedTerminals,
      sessions: this.sessions.size,
      maxSessions: this.config.maxSessionSockets,
      priorityLevels: this.config.priorityLevels,
      defaultMaxAttempts: this.config.defaultMaxAttempts,
      heartbeatIntervalMs: this.config.heartbeatIntervalMs,
      heartbeatTimeoutMs: this.config.heartbeatTimeoutMs,
      reconnectGraceMs: this.config.reconnectGraceMs,
      inputBytes: this.totalInputBytesHeld,
      maxPendingBytes: this.config.maxPendingBytes,
      resultBytes: this.totalResultBytesHeld,
      maxResultBytes: this.config.maxResultBytes,
      eventBytes: this.totalEventBytesHeld,
      maxEventBytes: this.config.maxEventBytes,
      committedBatches: this.committedBatches,
      committedOps: this.committedOps,
      queued,
      running,
      succeeded,
      failed,
      cancelled,
      workersRollup: {
        total: this.sessions.size,
        healthy: wHealthy,
        draining: wDraining,
        unavailable: Math.max(0, this.sessions.size - wHealthy),
        active_leases: wActiveLeases,
        total_capacity: wTotalCap,
        last_seen_at_ms: wLastSeen,
      },
      interfaces,
      pools,
      quotaDayUtc: quota.dayUtc,
      quotaMonthUtc: quota.monthUtc,
      quotaRowsWritten: quota.rowsWritten,
      quotaRowsRead: quota.rowsRead,
      quotaDoRequests: quota.doRequests,
      quotaWsMessages: quota.wsMessages,
      quotaR2ClassA: quota.r2ClassA,
      quotaR2ClassB: quota.r2ClassB,
      quotaLargeJobs: quota.largeJobs,
      databaseSizeBytes: quota.databaseSizeBytes,
      reservedRetainedBytes: quota.reservedRetainedBytes,
    };
  }

  private queryJobsInternal(
    query: ConsoleJobsQuery,
    nowMs: number,
  ): {
    matchedPage: JobMem[];
    total: number;
    nextPageToken: string | null;
    histogram: ConsoleHistogram | null;
  } {
    const needle = query.search ? query.search.trim().toLowerCase() : null;
    const matched: JobMem[] = [];
    const samples: Array<{ ts: number; state: JobStateName }> = [];

    for (const job of this.jobs.values()) {
      if (query.states.length > 0 && !query.states.includes(job.state)) {
        continue;
      }
      if (query.interfaceName !== null && job.interfaceName !== query.interfaceName) {
        continue;
      }
      if (query.namespace !== null && job.namespace !== query.namespace) {
        continue;
      }
      if (query.fromMs !== null && job.createdAtMs < query.fromMs) {
        continue;
      }
      if (query.toMs !== null && job.createdAtMs >= query.toMs) {
        continue;
      }
      if (query.workerId !== null && job.lastWorkerId !== query.workerId) {
        continue;
      }
      if (needle !== null && needle.length > 0) {
        const hit =
          job.rawJobId.toLowerCase().includes(needle) ||
          job.interfaceName.toLowerCase().includes(needle) ||
          job.namespace.toLowerCase().includes(needle) ||
          (job.lastWorkerId?.toLowerCase().includes(needle) ?? false) ||
          (job.error?.code.toLowerCase().includes(needle) ?? false) ||
          (job.error?.message.toLowerCase().includes(needle) ?? false) ||
          Object.entries(job.spec.metadata).some(
            ([k, v]) =>
              k.toLowerCase().includes(needle) || v.toLowerCase().includes(needle),
          );
        if (!hit) continue;
      }
      matched.push(job);
      if (query.histogramBuckets > 0) {
        samples.push({ ts: job.createdAtMs, state: job.state });
      }
    }

    // Order matches JobIndex: (updated_at_ms DESC, scoped_job_id ASC)
    matched.sort((a, b) => {
      if (b.updatedAtMs !== a.updatedAtMs) {
        return b.updatedAtMs - a.updatedAtMs;
      }
      return a.scopedJobId < b.scopedJobId
        ? -1
        : a.scopedJobId > b.scopedJobId
          ? 1
          : 0;
    });

    const total = matched.length;
    const matchedPage = matched.slice(query.offset, query.offset + query.limit);
    const nextOffset = query.offset + query.limit;
    const nextPageToken = nextOffset < total ? String(nextOffset) : null;
    const histogram = buildConsoleHistogram(
      query.fromMs,
      query.toMs,
      query.histogramBuckets,
      samples,
      nowMs,
    );

    return { matchedPage, total, nextPageToken, histogram };
  }

  queryJobsForConsole(
    query: ConsoleJobsQuery,
    nowMs: number,
  ): ConsoleJobsQueryResult {
    const res = this.queryJobsInternal(query, nowMs);
    return {
      jobs: res.matchedPage.map((j) => this.jobMemToConsoleSummary(j)),
      total: res.total,
      next_page_token: res.nextPageToken,
      histogram: res.histogram,
    };
  }

  async getJobDetailForConsole(
    namespace: string,
    rawJobId: string,
    nowMs: number,
  ): Promise<ConsoleJobDetailResult | null> {
    const scopedJobId = encodeScopedId(namespace, rawJobId);
    const job = this.jobs.get(scopedJobId);
    if (!job) {
      return null;
    }

    const inputBundle = await this.store.loadBundlePreviews(
      scopedJobId,
      "static_in",
      nowMs,
      PREVIEW_MAX_BYTES,
    );
    const outputBundle =
      job.state === "succeeded"
        ? await this.store.loadBundlePreviews(
            scopedJobId,
            "output",
            nowMs,
            PREVIEW_MAX_BYTES,
          )
        : { previews: [], paramsSizeBytes: 0, paramsHead: null };

    const evRows = this.readJobEventsAfterSync(scopedJobId, 0n);
    const events: ConsoleEventItem[] = evRows.map((ev) => {
      const seq = Number(joinU64(ev.seq_hi, ev.seq_lo));
      const meta = ev.meta_json
        ? (JSON.parse(ev.meta_json) as Record<string, any>)
        : {};
      const payload = ev.payload ? normalizeBlob(ev.payload) : undefined;
      const kind = ev.kind as ConsoleEventItem["kind"];
      return {
        sequence: seq,
        attemptId: ev.attempt_id,
        atMs: Number(ev.at_ms),
        kind,
        payloadSizeBytes:
          kind === "output" || kind === "progress"
            ? (payload?.byteLength ?? 0)
            : 0,
        payloadHead: payload?.subarray(0, Math.min(payload.byteLength, PREVIEW_MAX_BYTES)),
        payloadMeta: (meta.metadata as Record<string, string>) ?? {},
        workerId: typeof meta.workerId === "string" ? meta.workerId : undefined,
        previousAttemptId:
          typeof meta.previousAttemptId === "string"
            ? meta.previousAttemptId
            : undefined,
        reasonMessage: meta.reason?.message,
        terminalState: meta.state as JobStateName | undefined,
        terminalErrorMessage: meta.error?.message ?? null,
      };
    });

    const latestWorkerId =
      job.lastWorkerId ??
      [...events].reverse().find((e) => e.workerId)?.workerId ??
      null;
    let latestWorker: ConsoleWorkerData | null = null;
    if (latestWorkerId) {
      const matchingSessions = [...this.sessions.values()]
        .filter((s) => s.namespace === job.namespace && s.workerId === latestWorkerId)
        .sort((a, b) => (a.sessionId < b.sessionId ? -1 : 1));
      if (matchingSessions[0]) {
        latestWorker = this.sessionToConsoleWorker(matchingSessions[0]);
      }
    }

    return {
      summary: this.jobMemToConsoleSummary(job),
      demand: job.spec.demand,
      policy: job.spec.policy,
      paramsSizeBytes: job.spec.params_len,
      paramsHead: inputBundle.paramsHead,
      inputPreviews: inputBundle.previews,
      outputPreviews: outputBundle.previews,
      events,
      latestWorker,
    };
  }

  async cancelJobForConsole(
    namespace: string,
    rawJobId: string,
    reason: string,
    nowMs: number,
  ): Promise<ConsoleJobSummaryData | null> {
    const scopedJobId = encodeScopedId(namespace, rawJobId);
    const job = this.jobs.get(scopedJobId);
    if (!job) return null;
    if (
      job.state === "queued" ||
      job.state === "running"
    ) {
      const notifyJobs = new Set<string>([scopedJobId]);
      this.store.transactionSync(() => {
        this.cancelJobInTxn(scopedJobId, reason, nowMs);
        this.dispatchInTxn(nowMs, notifyJobs);
      });
      for (const jid of notifyJobs) {
        await this.notifyJobFollowers(jid);
      }
      await this.refreshAlarm(nowMs);
    }
    return this.jobMemToConsoleSummary(job);
  }

  scanAnalyticsJobsForConsole(
    scan: ConsoleAnalyticsScan,
    _nowMs: number,
  ): ConsoleAnalyticsResult {
    // Mirrors analytics.rs::scan_job_metadata:
    // - `archived: true`: completed durable jobs (`durability = 'durable'` and terminal state)
    // - `archived: false`: retained jobs (live and recently completed jobs)
    // Ordered descending by `(created_at_ms, cursor_id)` without reading payloads!
    const items: Array<{ job: JobMem; cursorId: string }> = [];
    for (const job of this.jobs.values()) {
      if (scan.namespace !== null && job.namespace !== scan.namespace) {
        continue;
      }
      if (job.createdAtMs < scan.fromMs || job.createdAtMs >= scan.toMs) {
        continue;
      }
      if (scan.archived) {
        const isTerminal =
          job.state === "succeeded" ||
          job.state === "failed" ||
          job.state === "cancelled";
        if (job.durability !== "durable" || !isTerminal) {
          continue;
        }
      }
      const cursorId =
        scan.namespace !== null ? job.rawJobId : job.scopedJobId;
      if (scan.before !== null) {
        if (
          job.createdAtMs > scan.before.createdAtMs ||
          (job.createdAtMs === scan.before.createdAtMs &&
            cursorId >= scan.before.cursorId)
        ) {
          continue;
        }
      }
      items.push({ job, cursorId });
    }

    items.sort((a, b) => {
      if (b.job.createdAtMs !== a.job.createdAtMs) {
        return b.job.createdAtMs - a.job.createdAtMs;
      }
      return b.cursorId < a.cursorId ? -1 : b.cursorId > a.cursorId ? 1 : 0;
    });

    const hasMore = items.length > scan.limit;
    const sliced = items.slice(0, scan.limit);
    const summaries = sliced.map((x) => this.jobMemToConsoleSummary(x.job));
    return {
      jobs: summaries,
      next_cursor: formatAnalyticsNextCursor(
        summaries[summaries.length - 1],
        hasMore,
        scan.namespace,
      ),
    };
  }

  listWorkersForConsole(_nowMs: number): ConsoleWorkerData[] {
    for (const s of this.sessions.values()) {
      if (s.connected) {
        const ws = this.socketsByConnId.get(s.currentConnId);
        const att = ws ? readSocketAttachment(ws) : null;
        if (att) {
          s.lastSeenMs = Math.max(s.lastSeenMs, att.lastSeenAtMs);
          s.queued = att.queued;
          s.running = att.running;
          s.utilization = att.utilization;
        }
      }
    }
    return [...this.sessions.values()]
      .sort((a, b) => {
        if (a.workerId !== b.workerId) return a.workerId < b.workerId ? -1 : 1;
        return a.sessionId < b.sessionId ? -1 : a.sessionId > b.sessionId ? 1 : 0;
      })
      .map((s) => this.sessionToConsoleWorker(s));
  }

  private jobMemToConsoleSummary(j: JobMem): ConsoleJobSummaryData {
    return buildJobSummary({
      jobId: j.rawJobId,
      namespace: j.namespace,
      state: j.state,
      durability: j.durability,
      interfaceName: j.interfaceName,
      createdAtMs: j.createdAtMs,
      updatedAtMs: j.updatedAtMs,
      currentAttemptId: j.lastAttemptId,
      latestWorkerId: j.lastWorkerId,
      attemptCount: j.attemptCount,
      error: j.error,
      metadata: j.spec.metadata,
    });
  }

  private sessionToConsoleWorker(s: SessionMem): ConsoleWorkerData {
    return {
      session_id: s.sessionId,
      worker_id: s.workerId,
      namespace: s.namespace,
      pool: s.pool,
      healthy: s.connected && !s.draining,
      interfaces: [...s.interfaces],
      attributes: { ...s.attributes },
      total_capacity: Object.fromEntries(
        Object.entries(s.totalCapacity).map(([k, v]) => [k, Number(v)]),
      ),
      max_concurrency: s.maxConcurrency,
      active_leases: s.activeLeases.size,
      free_slots: this.sessionFreeSlots(s),
      utilization: s.utilization,
      load: {
        queued: s.queued,
        running: s.running,
        utilization: s.utilization,
        custom: { ...s.customLoad },
      },
      connected: s.connected,
      draining: s.draining,
      connected_at_ms: s.connectedAtMs,
      last_seen_at_ms: s.lastSeenMs,
    };
  }
}

function protoStateToName(s: pb.JobState): JobStateName | null {
  switch (s) {
    case pb.JobState.QUEUED:
      return "queued";
    case pb.JobState.RUNNING:
      return "running";
    case pb.JobState.SUCCEEDED:
      return "succeeded";
    case pb.JobState.FAILED:
      return "failed";
    case pb.JobState.CANCELLED:
      return "cancelled";
    default:
      return null;
  }
}

function nameToProtoState(s: JobStateName): pb.JobState {
  switch (s) {
    case "queued":
      return pb.JobState.QUEUED;
    case "running":
      return pb.JobState.RUNNING;
    case "succeeded":
      return pb.JobState.SUCCEEDED;
    case "failed":
      return pb.JobState.FAILED;
    case "cancelled":
      return pb.JobState.CANCELLED;
  }
}

function buildConsoleHistogram(
  fromMs: number | null,
  toMs: number | null,
  bucketsRequested: number,
  samples: ReadonlyArray<{ ts: number; state: JobStateName }>,
  nowMs: number,
): ConsoleHistogram | null {
  if (bucketsRequested <= 0) return null;
  let minTs = Number.MAX_SAFE_INTEGER;
  let maxTs = 0;
  for (const s of samples) {
    if (s.ts < minTs) minTs = s.ts;
    if (s.ts > maxTs) maxTs = s.ts;
  }
  let startMs: number;
  let rawEndMs: number;
  if (fromMs !== null && toMs !== null) {
    startMs = fromMs;
    rawEndMs = Math.max(toMs, fromMs + 1);
  } else if (fromMs !== null) {
    const upper = samples.length === 0 ? nowMs : maxTs;
    startMs = fromMs;
    rawEndMs = Math.max(upper, fromMs) + 1;
  } else if (toMs !== null) {
    if (samples.length === 0) return null;
    startMs = Math.min(minTs, Math.max(0, toMs - 1));
    rawEndMs = Math.max(1, toMs);
  } else {
    if (samples.length === 0) return null;
    startMs = minTs;
    rawEndMs = maxTs + 1;
  }

  const numBuckets = Math.max(8, Math.min(96, bucketsRequested));
  const totalSpan = Math.max(numBuckets, rawEndMs - startMs);
  const endMs = startMs + totalSpan;
  const bound = (i: number) =>
    startMs + Math.floor((BigInt(i) * BigInt(totalSpan)) / BigInt(numBuckets) <= BigInt(Number.MAX_SAFE_INTEGER)
      ? Number((BigInt(i) * BigInt(totalSpan)) / BigInt(numBuckets))
      : 0);

  const buckets: ConsoleHistogramBucket[] = [];
  for (let i = 0; i < numBuckets; i++) {
    buckets.push({
      start_ms: bound(i),
      end_ms: bound(i + 1),
      active: 0,
      succeeded: 0,
      failed: 0,
      cancelled: 0,
    });
  }

  const bBig = BigInt(numBuckets);
  const spanBig = BigInt(totalSpan);
  for (const s of samples) {
    if (s.ts < startMs || s.ts >= endMs) continue;
    const rel = BigInt(s.ts - startMs);
    const idx = Math.min(
      numBuckets - 1,
      Number(((rel + 1n) * bBig - 1n) / spanBig),
    );
    const b = buckets[idx]!;
    if (s.state === "queued" || s.state === "running") b.active += 1;
    else if (s.state === "succeeded") b.succeeded += 1;
    else if (s.state === "failed") b.failed += 1;
    else if (s.state === "cancelled") b.cancelled += 1;
  }

  return {
    start_ms: startMs,
    end_ms: endMs,
    bucket_ms: Math.max(1, Math.floor(totalSpan / numBuckets)),
    buckets,
  };
}

// Keep unused imports referenced if needed
void OUTCOME_DIGEST_DOMAIN;
void OBJECT_SHA256_METADATA_KEY;
void OBJECT_SIZE_METADATA_KEY;
