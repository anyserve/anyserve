// Native Durable Object Execution Engine (sections 4, 5, 6, 7, 8, 9 of docs/cloudflare-native-do-plan.md v4).
// Mirrors crates/anyserve-core/src/runtime/* and http_ws_service.rs without cloudflare:workers or node:* imports.

import { attemptHandlers, type AttemptHandlers } from "./attempt-handlers";
import { clientHandlers, type ClientHandlers } from "./client-handlers";
import { consoleQueries, type ConsoleQueries } from "./console-queries";
import { eventDelivery, type EventDelivery } from "./event-delivery";
import {
  AuthPolicy,
  GrpcCode,
  GrpcStatusError,
  grpcErrorResponse,
  validateNamespace,
  type Principal,
} from "./auth";
import {
  formatAnalyticsNextCursor,
  handleConsoleOrHealthRequest,
  type AdminResetResult,
  type ConsoleAnalyticsResult,
  type ConsoleAnalyticsScan,
  type ConsoleBackend,
  type ConsoleJobsQuery,
  type ConsoleJobsQueryResult,
} from "./console-api";
import {
  BUNDLE_INLINE_MAX_BYTES,
  EVENT_OVERHEAD_BYTES,
  HTTP_MAX_BODY_BYTES,
  IsolateMemoryBudget,
  MAX_IDENTIFIER_BYTES,
  MAX_INTERFACE_BYTES,
  MAX_METADATA_BYTES,
  MAX_OBJECTS,
  WS_MAX_ENCODED_BYTES,
  WS_READY_STATE_OPEN,
  boundCustomLoad,
  isValidInterfaceName,
  readSocketAttachment,
  writeSocketAttachment,
  type NativeEnvConfig,
  type NativeHost,
  type NativeWebSocket,
  type SocketAttachment,
  type SocketKind,
} from "./host";
import {
  DEFAULT_NAMESPACE,
  decodeScopedId,
  encodeScopedId,
  extractTraceHeaders,
  mergeTraceHeaders,
  specHash,
} from "./identity";
import {
  NativeStore,
  decodeInlineAssignBundle,
  extractR2KeysFromManifest,
  joinU64,
  normalizeBlob,
  parseCapacityJson,
  parseSpecJson,
  serializeCapacityJson,
  splitU64,
  type CompletedAttemptObjectRef,
  type DurabilityName,
  type JobStateName,
  type StoredErrorInfo,
  type StoredObject,
} from "./store";
import { utf8ByteLen } from "./utf8";
import * as pb from "./proto";
import {
  RUNTIME_CODES,
  PROTOBUF_CONTENT_TYPE,
  DISPATCH_SCAN,
  CONN_SEQ_BLOCK,
  metadataByteLen,
  resolveEngineConfig,
  nameToProtoState,
  type ResolvedEngineConfig,
  type SessionMem,
  type LeaseMem,
  type JobMem,
  type WatcherMem,
  type ReplayUploadState,
} from "./engine-types";
import { MASK_64, FairPriorityQueue, pickSessionForJob, sessionFreeSlots } from "./scheduler";

export { CONN_SEQ_BLOCK, INLINE_ASSIGN_CACHE_MAX_BYTES, RUNTIME_CODES } from "./engine-types";
export { rendezvousHash64 } from "./scheduler";

export { DEFAULT_NAMESPACE, decodeScopedId, encodeScopedId };

export function validateNamespaceSafe(ns: string): boolean {
  try {
    validateNamespace(ns);
    return true;
  } catch {
    return false;
  }
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

  generation = 1n;
  disposed = false;
  connSeq = 0;
  connSeqCeiling = 0;
  queueSeq = 0n;
  rng = { state: 0x5dee_ce66_d1ce_4e5bn };
  armedAlarmMs: number | null = null;
  committedBatches = 0;
  committedOps = 0;
  droppedCustomLoadEntries = 0;

  // In-memory state rebuilt on wake from SQLite + getWebSockets()
  readonly sessions = new Map<string, SessionMem>();
  readonly sessionsByInterface = new Map<string, Map<string, Set<string>>>();
  readonly leases = new Map<string, LeaseMem>();
  readonly jobs = new Map<string, JobMem>();
  readonly queues = new Map<string, Map<string, FairPriorityQueue>>();
  readonly inFlightConcurrency = new Map<string, Map<string, number>>();
  readonly watchers = new Map<number, WatcherMem>();
  readonly watchersByJob = new Map<string, Set<number>>();
  readonly replayStates = new Map<number, ReplayUploadState>();

  // Live sockets indexed by connId
  readonly socketsByConnId = new Map<number, NativeWebSocket>();
  readonly socketChains = new Map<number, Promise<void>>();
  readonly pumpingAttempts = new Set<string>();
  readonly pendingInvokeTraceHeaders = new Map<number, Record<string, string>>();

  // Shared runtime byte trackers (for /metrics and /api/overview parity)
  totalInputBytesHeld = 0;
  totalResultBytesHeld = 0;
  totalEventBytesHeld = 0;
  inlineAssignCacheBytes = 0;

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

  rehydrateAfterWake(nowMs: number): void {
    const meta = this.store.initOrLoadMeta(nowMs);
    this.generation = meta.generation;
    // `meta.connSeq` is the persisted ceiling of the last reserved block: every id issued by any
    // previous incarnation is <= it, so resuming strictly above it never reuses an id.
    this.connSeq = meta.connSeq;
    this.connSeqCeiling = meta.connSeq;
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
      this.noteConnId(att.connId);
      this.socketsByConnId.set(att.connId, ws);
    }

    // 2. Load native_sessions
    const sessionRows = this.store.exec<Record<string, any>>(
      `SELECT * FROM native_sessions`,
    );
    for (const r of sessionRows) {
      const connId = Number(r.current_conn_id);
      this.noteConnId(connId);
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
      if (attemptConnId !== null) {
        this.noteConnId(attemptConnId);
      }
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
        inlineAssign: null,
      };
      if (job.callerConnId !== null) {
        this.noteConnId(job.callerConnId);
      }
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
      this.noteConnId(watchConnId);
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

  async syncPlatformAlarmOnWake(): Promise<void> {
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

  hasMatchingFreeWorker(job: JobMem, _nowMs: number): boolean {
    const set = this.sessionsByInterface.get(job.namespace)?.get(job.interfaceName);
    if (!set) return false;
    for (const sid of set) {
      const s = this.sessions.get(sid);
      if (s && sessionFreeSlots(s) > 0) {
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

  countOpenSocketsByKind(): {
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

  async handleWebSocketUpgrade(
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
    if (kind === "invoke") {
      const traceHeaders = extractTraceHeaders(request.headers);
      if (Object.keys(traceHeaders).length > 0) {
        this.pendingInvokeTraceHeaders.set(connId, traceHeaders);
      }
    }

    // Worker session/attempt handshakes expire at heartbeatTimeoutMs if Hello/Open never arrives!
    if (kind === "session" || kind === "attempt") {
      await this.refreshAlarm(nowMs);
    }

    return pair.toUpgradeResponse();
  }

  noteConnId(connId: number): void {
    if (connId > this.connSeq) {
      this.connSeq = connId;
    }
  }

  /**
   * Monotonic, never-reused connection ids. The `conn_seq` meta row holds a ceiling that is raised
   * (one write) only when the in-memory counter crosses it, i.e. once per `CONN_SEQ_BLOCK` ids.
   */
  allocateConnId(_nowMs: number): number {
    this.connSeq += 1;
    const id = this.connSeq;
    if (id > this.connSeqCeiling) {
      const ceiling = id + CONN_SEQ_BLOCK - 1;
      this.store.transactionSync(() => {
        this.store.setMeta("conn_seq", String(ceiling));
      });
      this.connSeqCeiling = ceiling;
    }
    return id;
  }

  async handleUnaryClientRpc(
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
          if (req.spec) {
            mergeTraceHeaders(req.spec.metadata, extractTraceHeaders(request.headers));
          }
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

  protoOkResponse(bytes: Uint8Array): Response {
    // Hand over the array's own buffer when it spans it exactly; otherwise copy once.
    const buf = bytes.buffer;
    const body =
      buf instanceof ArrayBuffer && bytes.byteOffset === 0 && bytes.byteLength === buf.byteLength
        ? buf
        : (bytes.slice().buffer as ArrayBuffer);
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
    let decodedControl: pb.WorkerMessage | null = null;
    if (att.kind === "session" && att.handshakeDone === 1) {
      if (bytes.byteLength > this.config.maxControlMessageBytes) {
        this.sendWsErrorAndClose(
          ws,
          GrpcCode.InvalidArgument,
          `control message of ${bytes.byteLength} bytes exceeds ${this.config.maxControlMessageBytes}`,
        );
        return;
      }
      // Decode ONCE here: Heartbeat is handled inline; any other message hands this decoded value
      // to the session handler (no second decode).
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
      await this.dispatchSocketMessage(ws, freshAtt, bytes, stepNow, decodedControl);
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
      this.pendingInvokeTraceHeaders.delete(att.connId);
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

  computeNextAlarmDueMs(nowMs: number): number | null {
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

  async refreshAlarm(nowMs: number): Promise<void> {
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

  async dispatchSocketMessage(
    ws: NativeWebSocket,
    att: SocketAttachment,
    bytes: Uint8Array,
    nowMs: number,
    decodedControl: pb.WorkerMessage | null = null,
  ): Promise<void> {
    try {
      switch (att.kind) {
        case "session":
          await this.handleSessionSocketMessage(ws, att, bytes, nowMs, decodedControl);
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

  handleSessionHeartbeatSync(
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

  async handleSessionSocketMessage(
    ws: NativeWebSocket,
    att: SocketAttachment,
    bytes: Uint8Array,
    nowMs: number,
    predecoded: pb.WorkerMessage | null = null,
  ): Promise<void> {
    if (bytes.byteLength > this.config.maxControlMessageBytes) {
      throw GrpcStatusError.invalidArgument(
        `control message of ${bytes.byteLength} bytes exceeds ${this.config.maxControlMessageBytes}`,
      );
    }
    const decoded = predecoded ?? pb.decodeWorkerMessage(bytes);
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

  async handleSessionHello(
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
      !hello.interfaces.every((i) => isValidInterfaceName(i))
    ) {
      throw GrpcStatusError.invalidArgument(
        `Hello.interfaces entries must be 1..=${MAX_INTERFACE_BYTES} bytes (no ASCII whitespace or control characters) and at most ${MAX_OBJECTS} interfaces`,
      );
    }
    if (
      utf8ByteLen(hello.workerId) > MAX_IDENTIFIER_BYTES ||
      utf8ByteLen(hello.resumeSessionId) > MAX_IDENTIFIER_BYTES ||
      utf8ByteLen(hello.pool) > MAX_IDENTIFIER_BYTES
    ) {
      throw GrpcStatusError.invalidArgument(
        `Hello identifiers must be at most ${MAX_IDENTIFIER_BYTES} bytes`,
      );
    }
    if (hello.heldLeaseIds.length > MAX_OBJECTS) {
      throw GrpcStatusError.invalidArgument("too many held leases");
    }
    const ifaceBytes = hello.interfaces.reduce((s, i) => s + utf8ByteLen(i), 0);
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

  // ---------------------------------------------------------------------------
  // 3. Client Invoke Stream (`/ws/v2/client/invoke`)
  // ---------------------------------------------------------------------------

  // ---------------------------------------------------------------------------
  // 4. Client Watch Stream (`/ws/v2/client/watch`)
  // ---------------------------------------------------------------------------

  // ---------------------------------------------------------------------------
  // 5. Unary Client RPCs (`Submit`, `GetJob`, `Cancel`, `ListJobs`)
  // ---------------------------------------------------------------------------

  // ---------------------------------------------------------------------------
  // Core State Machine Transitions (All run inside `store.transactionSync`)
  // ---------------------------------------------------------------------------

  specUsesMultiChunkOrR2(
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

  // ---------------------------------------------------------------------------
  // Scheduling & Dispatch (Mirrors machine.rs::dispatch_locked & pick_session)
  // ---------------------------------------------------------------------------

  dispatchInTxn(nowMs: number, notifyJobs?: Set<string>): number {
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
          if (s && sessionFreeSlots(s) > 0) {
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

          const pickedSessionId = pickSessionForJob(this, 
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
          if (!pickedSession || sessionFreeSlots(pickedSession) === 0) {
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

  beginAttemptInTxn(
    job: JobMem,
    sessionId: string,
    nowMs: number,
  ): void {
    const cachedInline = job.inlineAssign;
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
      // Prefer the transient accept-time cache; fall back to SQLite after hibernation / retry.
      const decoded =
        cachedInline ??
        decodeInlineAssignBundle(
          this.store.exec<{ manifest: string; pack_blob: Uint8Array | null }>(
            `SELECT seq, manifest, pack_blob FROM native_object_bundles
             WHERE scoped_job_id = ? AND role = 'static_in'
             ORDER BY seq ASC`,
            job.scopedJobId,
          ),
        );
      if (decoded === null || decoded.bytes > this.config.assignInlineLimit) {
        inputsViaAttempt = true;
      } else {
        inlineInputs = decoded.inputs;
        inlineParams = decoded.params;
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

  endAttemptWithErrorInTxn(
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

  computeRetryBackoffMs(attemptCount: number): number {
    const exp = Math.min(16, Math.max(0, attemptCount - 1));
    return Math.min(
      this.config.defaultMaxBackoffMs,
      this.config.defaultInitialBackoffMs * (1 << exp),
    );
  }

  cancelJobInTxn(scopedJobId: string, reason: string, nowMs: number): void {
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

  failJobInTxn(
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

  applyJobTerminalInTxn(
    job: JobMem,
    terminalState: "succeeded" | "failed" | "cancelled",
    error: StoredErrorInfo | null,
    winningLeaseId: string | null,
    resultDigest: string | null,
    nowMs: number,
  ): void {
    if (job.currentLeaseId) {
      const lease = this.leases.get(job.currentLeaseId);
      if (lease && lease.concurrencyCounted && job.concurrencyKey.length > 0) {
        lease.concurrencyCounted = false;
        this.decConcurrencyKey(job.namespace, job.concurrencyKey);
      }
      job.currentLeaseId = null;
    }
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

  disconnectSessionInTxn(
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

  resolveSessionReconnectGraceMs(session: SessionMem): number {
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

  expireDisconnectedSessionInTxn(
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

  leaseLostInTxn(leaseId: string, nowMs: number): string | null {
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

  cancelLeaseInTxn(leaseId: string, reason: string, nowMs: number): void {
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

  releaseLeaseInTxn(leaseId: string, _nowMs: number): void {
    const lease = this.leases.get(leaseId);
    if (!lease) return;
    const job = this.jobs.get(lease.scopedJobId);
    if (lease.concurrencyCounted) {
      lease.concurrencyCounted = false;
      if (job && job.concurrencyKey.length > 0) {
        this.decConcurrencyKey(job.namespace, job.concurrencyKey);
      }
    }
    if (job && job.currentLeaseId === leaseId) {
      job.currentLeaseId = null;
    }
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

  cleanupLeaseUploadsInTxn(
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

  insertReceiptInTxn(
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

  attachLeaseToSessionMem(
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

  detachLeaseFromSessionMem(
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

  indexSessionInMemory(s: SessionMem): void {
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

  unindexSessionInMemory(s: SessionMem): void {
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

  removeSessionInTxn(sessionId: string): void {
    const s = this.sessions.get(sessionId);
    if (s) {
      this.unindexSessionInMemory(s);
      this.sessions.delete(sessionId);
      this.store.exec(`DELETE FROM native_sessions WHERE session_id = ?`, sessionId);
    }
  }

  persistSessionRowInTxn(s: SessionMem): void {
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

  insertLeaseRowInTxn(l: LeaseMem): void {
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

  persistLeaseRowInTxn(l: LeaseMem): void {
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

  persistLeaseInputCursorInTxn(l: LeaseMem): void {
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

  persistLeaseResultStateInTxn(l: LeaseMem): void {
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

  // ---------------------------------------------------------------------------
  // Retention & Purge (Section 4.5 & 6.4)
  // ---------------------------------------------------------------------------

  enforceMaxTerminalJobsInTxn(nowMs: number): void {
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

  purgeExpiredTerminalJobsInTxn(nowMs: number, maxCount: number): number {
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

  purgeSingleJobInTxn(scopedJobId: string, nowMs: number): void {
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

  jobMemToProtoSyncWithoutOutputs(job: JobMem): pb.Job {
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

  async jobMemToProto(
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

  sendControlMessageToSession(
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

  sendWsProtoSync(ws: NativeWebSocket, protoBytes: Uint8Array): void {
    const frame = pb.encodeWsDataFrame(protoBytes);
    this.sendWsRawSync(ws, frame);
  }

  sendWsRawSync(ws: NativeWebSocket, frameBytes: Uint8Array): void {
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

  sendWsErrorAndClose(
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

  getOrCreateQueue(
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

  dequeueJobInMemory(job: JobMem): void {
    const q = this.queues.get(job.namespace)?.get(job.interfaceName);
    if (q) {
      q.remove(job.priority, job.fairnessKey, job.scopedJobId);
    }
    job.queueSeq = null;
    if (job.inlineAssign !== null) {
      this.inlineAssignCacheBytes = Math.max(0, this.inlineAssignCacheBytes - job.inlineAssign.bytes);
      job.inlineAssign = null;
    }
  }

  getConcurrencyKeyCount(namespace: string, key: string): number {
    return this.inFlightConcurrency.get(namespace)?.get(key) ?? 0;
  }

  incConcurrencyKey(namespace: string, key: string): void {
    let m = this.inFlightConcurrency.get(namespace);
    if (!m) {
      m = new Map();
      this.inFlightConcurrency.set(namespace, m);
    }
    m.set(key, (m.get(key) ?? 0) + 1);
  }

  decConcurrencyKey(namespace: string, key: string): void {
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

}

export interface NativeEngine
  extends AttemptHandlers,
    ClientHandlers,
    ConsoleQueries,
    EventDelivery {}

// Large handler groups live in sibling modules and are mixed into the prototype (`this: NativeEngine`).
Object.assign(NativeEngine.prototype, attemptHandlers, clientHandlers, consoleQueries, eventDelivery);
