// Shared in-memory structures, resolved configuration, and constants for the native engine.
// Pure types/constants: no engine state, no cloudflare:workers or node:* imports.

import {
  DEFAULT_CANCEL_GRACE_MS,
  DEFAULT_CONSUMER_STALL_TIMEOUT_MS,
  DEFAULT_HEARTBEAT_INTERVAL_MS,
  DEFAULT_HEARTBEAT_TIMEOUT_MS,
  DEFAULT_INITIAL_BACKOFF_MS,
  DEFAULT_MAX_BACKOFF_MS,
  DEFAULT_RECEIPT_RETENTION_MS,
  DEFAULT_RECONNECT_GRACE_MS,
  DEFAULT_TERMINAL_RETENTION_MS,
  MAINTENANCE_BUCKET_MS,
  MAX_ATTEMPT_SOCKETS,
  MAX_INLINE_OBJECT_BYTES,
  MAX_INLINE_TOTAL_BYTES,
  MAX_INVOKE_SOCKETS,
  MAX_PENDING_HANDSHAKE_SOCKETS,
  MAX_SESSION_SOCKETS,
  MAX_WATCH_SOCKETS,
  PER_JOB_EVENT_CAP_BYTES,
  WELCOME_ASSIGN_INLINE_LIMIT,
  WELCOME_ATTEMPT_WINDOW_BYTES,
  WELCOME_MAX_CHUNK_BYTES,
  WELCOME_MAX_CONTROL_BYTES,
  type InterfacePolicyConfig,
  type NativeHost,
} from "./host";
import { IncrementalSha256, specHash } from "./identity";
import {
  type CompletedAttemptObjectRef,
  type DurabilityName,
  type InlineAssignBundle,
  type JobStateName,
  type StoredErrorInfo,
  type StoredSpecMetadata,
} from "./store";
import { utf8ByteLen } from "./utf8";
import * as pb from "./proto";

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

export const PROTOBUF_CONTENT_TYPE = "application/x-protobuf";
export const DEFAULT_FAIRNESS_KEY = "_default";
export const DISPATCH_SCAN = 256;
/** `conn_seq` meta is persisted as a ceiling once per block of ids instead of once per upgrade. */
export const CONN_SEQ_BLOCK = 64;
/** Upper bound on bytes of decoded inline-Assign payloads cached across all queued jobs. */
export const INLINE_ASSIGN_CACHE_MAX_BYTES = 4_194_304;

export function metadataByteLen(m: Record<string, string>): number {
  let sum = 0;
  for (const [k, v] of Object.entries(m)) {
    sum += utf8ByteLen(k) + utf8ByteLen(v);
  }
  return sum;
}

export function toNumberClamped(v: bigint): number {
  if (v <= 0n) return 0;
  if (v > BigInt(Number.MAX_SAFE_INTEGER)) return Number.MAX_SAFE_INTEGER;
  return Number(v);
}

// ---------------------------------------------------------------------------
// Resolved Configuration
// ---------------------------------------------------------------------------

export interface ResolvedEngineConfig {
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

export function resolveEngineConfig(host: NativeHost): ResolvedEngineConfig {
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

export interface SessionMem {
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

export interface LeaseMem {
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

export interface JobMem {
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
  /** Transient decoded small-inline `static_in` payload, kept only while queued (see INLINE_ASSIGN_CACHE_MAX_BYTES). */
  inlineAssign: InlineAssignBundle | null;
}

export interface WatcherMem {
  watchConnId: number;
  scopedJobId: string;
  rawJobId: string;
  namespace: string;
  afterSeq: bigint;
  deliveredSeq: bigint;
  finished: boolean;
}

export interface ReplayUploadState {
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

export function protoStateToName(s: pb.JobState): JobStateName | null {
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

export function nameToProtoState(s: JobStateName): pb.JobState {
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
