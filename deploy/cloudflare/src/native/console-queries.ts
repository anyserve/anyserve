// Console / health read model for NativeEngine (`ConsoleBackend`): overview, job queries, detail,
// workers, and console-initiated cancel. Methods are mixed into `NativeEngine.prototype`.

import {
  buildJobSummary,
  type ConsoleEventItem,
  type ConsoleHistogram,
  type ConsoleHistogramBucket,
  type ConsoleJobDetailResult,
  type ConsoleJobSummaryData,
  type ConsoleJobsQuery,
  type ConsoleOverviewSnapshot,
  type ConsoleWorkerData,
} from "./console-api";
import { PREVIEW_MAX_BYTES, readSocketAttachment } from "./host";
import {
  OBJECT_SHA256_METADATA_KEY,
  OBJECT_SIZE_METADATA_KEY,
  OUTCOME_DIGEST_DOMAIN,
  encodeScopedId,
} from "./identity";
import { joinU64, normalizeBlob, type JobStateName } from "./store";
import { type SessionMem, type JobMem } from "./engine-types";
import { sessionFreeSlots } from "./scheduler";
import type { NativeEngine } from "./engine";

export const consoleQueries = {
  getHealth(this: NativeEngine): { live: boolean; ready: boolean; message: string | null } {
    const nowMs = this.host.nowMs();
    const hardWriteErr = this.store.checkHardWriteCeiling(nowMs);
    return {
      live: true,
      ready: hardWriteErr === null,
      message: hardWriteErr,
    };
  },

  getOverviewSnapshot(
    this: NativeEngine,
    nowMs: number,
    namespace?: string,
  ): ConsoleOverviewSnapshot {
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

    if (namespace === undefined) {
      for (const name of Object.keys(this.config.interfaces)) {
        ensureIface(name);
      }
    }

    for (const j of this.jobs.values()) {
      if (namespace !== undefined && j.namespace !== namespace) continue;
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

    let sessionsCount = 0;
    for (const s of this.sessions.values()) {
      // Refresh scalar load from open session socket attachments in the same pass
      if (s.connected) {
        const ws = this.socketsByConnId.get(s.currentConnId);
        const att = ws ? readSocketAttachment(ws) : null;
        if (att) {
          if (att.lastSeenAtMs > s.lastSeenMs) {
            s.lastSeenMs = att.lastSeenAtMs;
          }
          s.queued = att.queued;
          s.running = att.running;
          s.utilization = att.utilization;
        }
      }
      if (namespace !== undefined && s.namespace !== namespace) continue;
      sessionsCount += 1;
      const free = sessionFreeSlots(s);
      const leaseCount = s.activeLeases.size;
      wActiveLeases += leaseCount;
      wLastSeen = wLastSeen === null ? s.lastSeenMs : Math.max(wLastSeen, s.lastSeenMs);
      if (s.draining) wDraining += 1;
      if (s.connected && !s.draining) {
        wHealthy += 1;
        for (const k in s.totalCapacity) {
          wTotalCap[k] = (wTotalCap[k] ?? 0) + Number(s.totalCapacity[k]);
        }
      }
      const singleIface = s.interfaces.length === 1;
      for (const iface of s.interfaces) {
        const st = ensureIface(iface);
        st.sessions += 1;
        st.freeSlots += free;

        let activeOnIface = 0;
        if (leaseCount > 0) {
          if (singleIface) {
            activeOnIface = leaseCount;
          } else {
            for (const lid of s.activeLeases) {
              const lease = this.leases.get(lid);
              const job = lease ? this.jobs.get(lease.scopedJobId) : undefined;
              if (job && job.interfaceName === iface) {
                activeOnIface += 1;
              }
            }
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
      if (namespace !== undefined && ns !== namespace) continue;
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
      sessions: sessionsCount,
      maxSessions: this.config.maxSessionSockets,
      priorityLevels: this.config.priorityLevels,
      defaultMaxAttempts: this.config.defaultMaxAttempts,
      heartbeatIntervalMs: this.config.heartbeatIntervalMs,
      heartbeatTimeoutMs: this.config.heartbeatTimeoutMs,
      reconnectGraceMs: this.config.reconnectGraceMs,
      inputBytes: namespace !== undefined ? 0 : this.totalInputBytesHeld,
      maxPendingBytes: this.config.maxPendingBytes,
      resultBytes: namespace !== undefined ? 0 : this.totalResultBytesHeld,
      maxResultBytes: this.config.maxResultBytes,
      eventBytes: namespace !== undefined ? 0 : this.totalEventBytesHeld,
      maxEventBytes: this.config.maxEventBytes,
      committedBatches: this.committedBatches,
      committedOps: this.committedOps,
      queued,
      running,
      succeeded,
      failed,
      cancelled,
      workersRollup: {
        total: sessionsCount,
        healthy: wHealthy,
        draining: wDraining,
        unavailable: Math.max(0, sessionsCount - wHealthy),
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
  },

  queryJobsInternal(
    this: NativeEngine,
    query: ConsoleJobsQuery,
    nowMs: number,
  ): {
    matchedPage: JobMem[];
    total: number;
    nextPageToken: string | null;
    histogram: ConsoleHistogram | null;
  } {
    const needle = query.search ? query.search.trim().toLowerCase() : null;
    const hasNeedle = needle !== null && needle.length > 0;
    const filterStates = query.states.length > 0;
    let stateMask = 0;
    if (filterStates) {
      for (let i = 0; i < query.states.length; i++) {
        stateMask |= jobStateBit(query.states[i]!);
      }
    }
    const ifaceFilter = query.interfaceName;
    const nsFilter = query.namespace;
    const fromMs = query.fromMs;
    const toMs = query.toMs;
    const workerFilter = query.workerId;

    const matched: JobMem[] = [];
    for (const job of this.jobs.values()) {
      if (filterStates && (stateMask & jobStateBit(job.state)) === 0) {
        continue;
      }
      if (ifaceFilter !== null && job.interfaceName !== ifaceFilter) {
        continue;
      }
      if (nsFilter !== null && job.namespace !== nsFilter) {
        continue;
      }
      if (fromMs !== null && job.createdAtMs < fromMs) {
        continue;
      }
      if (toMs !== null && job.createdAtMs >= toMs) {
        continue;
      }
      if (workerFilter !== null && job.lastWorkerId !== workerFilter) {
        continue;
      }
      if (hasNeedle) {
        let hit =
          job.rawJobId.toLowerCase().includes(needle) ||
          job.interfaceName.toLowerCase().includes(needle) ||
          job.namespace.toLowerCase().includes(needle) ||
          (job.lastWorkerId !== null && job.lastWorkerId.toLowerCase().includes(needle)) ||
          (job.error !== null &&
            (job.error.code.toLowerCase().includes(needle) ||
              job.error.message.toLowerCase().includes(needle)));
        if (!hit) {
          const meta = job.spec.metadata;
          for (const k in meta) {
            if (k.toLowerCase().includes(needle) || meta[k]!.toLowerCase().includes(needle)) {
              hit = true;
              break;
            }
          }
        }
        if (!hit) continue;
      }
      matched.push(job);
    }

    const total = matched.length;
    const endIdx = query.offset + query.limit;
    let matchedPage: JobMem[];
    if (query.offset >= total || query.limit <= 0) {
      matchedPage = [];
    } else if (total > 64 && endIdx > 0 && endIdx < (total >> 2)) {
      // Partial top-K selection (Order: updated_at_ms DESC, scoped_job_id ASC)
      const topK = selectTopKJobs(matched, endIdx);
      matchedPage = query.offset > 0 ? topK.slice(query.offset) : topK;
    } else {
      matched.sort(compareJobsUpdatedDesc);
      matchedPage = matched.slice(query.offset, endIdx);
    }

    const nextPageToken = endIdx < total ? String(endIdx) : null;
    const histogram =
      query.histogramBuckets > 0
        ? buildConsoleHistogramFromJobs(
            query.fromMs,
            query.toMs,
            query.histogramBuckets,
            matched,
            nowMs,
          )
        : null;

    return { matchedPage, total, nextPageToken, histogram };
  },

  async getJobDetailForConsole(
    this: NativeEngine,
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
  },

  async cancelJobForConsole(
    this: NativeEngine,
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
  },

  listWorkersForConsole(this: NativeEngine, _nowMs: number): ConsoleWorkerData[] {
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
  },

  drainWorkerForConsole(
    this: NativeEngine,
    sessionId: string,
    scopeNamespace: string | null,
    nowMs: number,
  ): boolean {
    const session = this.sessions.get(sessionId);
    if (!session || (scopeNamespace !== null && session.namespace !== scopeNamespace)) {
      return false;
    }
    this.store.transactionSync(() => {
      session.draining = true;
      this.persistSessionRowInTxn(session);
      this.sendControlMessageToSession(
        session.sessionId,
        { msg: { case: "drain", value: {} } },
        nowMs,
      );
    });
    return true;
  },

  jobMemToConsoleSummary(this: NativeEngine, j: JobMem): ConsoleJobSummaryData {
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
  },

  sessionToConsoleWorker(this: NativeEngine, s: SessionMem): ConsoleWorkerData {
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
      free_slots: sessionFreeSlots(s),
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
  },
};

export type ConsoleQueries = typeof consoleQueries;

function jobStateBit(state: JobStateName): number {
  switch (state) {
    case "queued":
      return 1;
    case "running":
      return 2;
    case "succeeded":
      return 4;
    case "failed":
      return 8;
    case "cancelled":
      return 16;
  }
}

function compareJobsUpdatedDesc(a: JobMem, b: JobMem): number {
  if (b.updatedAtMs !== a.updatedAtMs) {
    return b.updatedAtMs - a.updatedAtMs;
  }
  return a.scopedJobId < b.scopedJobId ? -1 : a.scopedJobId > b.scopedJobId ? 1 : 0;
}

/**
 * Selects the top `k` jobs in `(updatedAtMs DESC, scopedJobId ASC)` order using a fixed-size
 * max-heap of the "worst" element among the current top-K (so root is replaced whenever a better
 * job arrives), then sorts the `k` winners.
 */
function selectTopKJobs(items: readonly JobMem[], k: number): JobMem[] {
  const heap: JobMem[] = new Array(k);
  for (let i = 0; i < k; i++) {
    heap[i] = items[i]!;
  }
  // Heapify so heap[0] is the WORST (largest compareJobsUpdatedDesc) element in the top-k set.
  const siftDown = (idx: number) => {
    let i = idx;
    const half = k >>> 1;
    const item = heap[i]!;
    while (i < half) {
      let child = (i << 1) + 1;
      const right = child + 1;
      if (right < k && compareJobsUpdatedDesc(heap[right]!, heap[child]!) > 0) {
        child = right;
      }
      if (compareJobsUpdatedDesc(heap[child]!, item) <= 0) {
        break;
      }
      heap[i] = heap[child]!;
      i = child;
    }
    heap[i] = item;
  };
  for (let i = (k >>> 1) - 1; i >= 0; i--) {
    siftDown(i);
  }
  for (let i = k; i < items.length; i++) {
    const cand = items[i]!;
    if (compareJobsUpdatedDesc(cand, heap[0]!) < 0) {
      heap[0] = cand;
      siftDown(0);
    }
  }
  heap.sort(compareJobsUpdatedDesc);
  return heap;
}

function buildConsoleHistogramCore(
  fromMs: number | null,
  toMs: number | null,
  bucketsRequested: number,
  count: number,
  minTs: number,
  maxTs: number,
  nowMs: number,
): {
  startMs: number;
  endMs: number;
  totalSpan: number;
  numBuckets: number;
  buckets: ConsoleHistogramBucket[];
  useFastFloat: boolean;
} | null {
  let startMs: number;
  let rawEndMs: number;
  if (fromMs !== null && toMs !== null) {
    startMs = fromMs;
    rawEndMs = Math.max(toMs, fromMs + 1);
  } else if (fromMs !== null) {
    const upper = count === 0 ? nowMs : maxTs;
    startMs = fromMs;
    rawEndMs = Math.max(upper, fromMs) + 1;
  } else if (toMs !== null) {
    if (count === 0) return null;
    startMs = Math.min(minTs, Math.max(0, toMs - 1));
    rawEndMs = Math.max(1, toMs);
  } else {
    if (count === 0) return null;
    startMs = minTs;
    rawEndMs = maxTs + 1;
  }

  const numBuckets = Math.max(8, Math.min(96, bucketsRequested));
  const totalSpan = Math.max(numBuckets, rawEndMs - startMs);
  const endMs = startMs + totalSpan;
  const useFastFloat = totalSpan * numBuckets <= Number.MAX_SAFE_INTEGER;
  const buckets: ConsoleHistogramBucket[] = new Array(numBuckets);

  if (useFastFloat) {
    let prevBound = startMs;
    for (let i = 0; i < numBuckets; i++) {
      const nextBound = startMs + Math.floor(((i + 1) * totalSpan) / numBuckets);
      buckets[i] = {
        start_ms: prevBound,
        end_ms: nextBound,
        active: 0,
        succeeded: 0,
        failed: 0,
        cancelled: 0,
      };
      prevBound = nextBound;
    }
  } else {
    const spanBig = BigInt(totalSpan);
    const bBig = BigInt(numBuckets);
    const maxSafeBig = BigInt(Number.MAX_SAFE_INTEGER);
    const bound = (i: number) => {
      const q = (BigInt(i) * spanBig) / bBig;
      return startMs + (q <= maxSafeBig ? Number(q) : 0);
    };
    let prevBound = bound(0);
    for (let i = 0; i < numBuckets; i++) {
      const nextBound = bound(i + 1);
      buckets[i] = {
        start_ms: prevBound,
        end_ms: nextBound,
        active: 0,
        succeeded: 0,
        failed: 0,
        cancelled: 0,
      };
      prevBound = nextBound;
    }
  }

  return { startMs, endMs, totalSpan, numBuckets, buckets, useFastFloat };
}

function addSampleToBucket(b: ConsoleHistogramBucket, state: JobStateName): void {
  if (state === "queued" || state === "running") b.active += 1;
  else if (state === "succeeded") b.succeeded += 1;
  else if (state === "failed") b.failed += 1;
  else if (state === "cancelled") b.cancelled += 1;
}

function buildConsoleHistogramFromJobs(
  fromMs: number | null,
  toMs: number | null,
  bucketsRequested: number,
  jobs: readonly JobMem[],
  nowMs: number,
): ConsoleHistogram | null {
  if (bucketsRequested <= 0) return null;
  let minTs = Number.MAX_SAFE_INTEGER;
  let maxTs = 0;
  for (let i = 0; i < jobs.length; i++) {
    const ts = jobs[i]!.createdAtMs;
    if (ts < minTs) minTs = ts;
    if (ts > maxTs) maxTs = ts;
  }
  const core = buildConsoleHistogramCore(
    fromMs,
    toMs,
    bucketsRequested,
    jobs.length,
    minTs,
    maxTs,
    nowMs,
  );
  if (!core) return null;

  const { startMs, endMs, totalSpan, numBuckets, buckets, useFastFloat } = core;
  if (useFastFloat) {
    const lastIdx = numBuckets - 1;
    for (let i = 0; i < jobs.length; i++) {
      const j = jobs[i]!;
      const ts = j.createdAtMs;
      if (ts < startMs || ts >= endMs) continue;
      let idx = Math.floor(((ts - startMs + 1) * numBuckets - 1) / totalSpan);
      if (idx > lastIdx) idx = lastIdx;
      addSampleToBucket(buckets[idx]!, j.state);
    }
  } else {
    const bBig = BigInt(numBuckets);
    const spanBig = BigInt(totalSpan);
    for (let i = 0; i < jobs.length; i++) {
      const j = jobs[i]!;
      const ts = j.createdAtMs;
      if (ts < startMs || ts >= endMs) continue;
      const rel = BigInt(ts - startMs);
      const idx = Math.min(numBuckets - 1, Number(((rel + 1n) * bBig - 1n) / spanBig));
      addSampleToBucket(buckets[idx]!, j.state);
    }
  }

  return {
    start_ms: startMs,
    end_ms: endMs,
    bucket_ms: Math.max(1, Math.floor(totalSpan / numBuckets)),
    buckets,
  };
}

export function buildConsoleHistogram(
  fromMs: number | null,
  toMs: number | null,
  bucketsRequested: number,
  samples: ReadonlyArray<{ ts: number; state: JobStateName }>,
  nowMs: number,
): ConsoleHistogram | null {
  if (bucketsRequested <= 0) return null;
  let minTs = Number.MAX_SAFE_INTEGER;
  let maxTs = 0;
  for (let i = 0; i < samples.length; i++) {
    const ts = samples[i]!.ts;
    if (ts < minTs) minTs = ts;
    if (ts > maxTs) maxTs = ts;
  }
  const core = buildConsoleHistogramCore(
    fromMs,
    toMs,
    bucketsRequested,
    samples.length,
    minTs,
    maxTs,
    nowMs,
  );
  if (!core) return null;

  const { startMs, endMs, totalSpan, numBuckets, buckets, useFastFloat } = core;
  if (useFastFloat) {
    const lastIdx = numBuckets - 1;
    for (let i = 0; i < samples.length; i++) {
      const s = samples[i]!;
      if (s.ts < startMs || s.ts >= endMs) continue;
      let idx = Math.floor(((s.ts - startMs + 1) * numBuckets - 1) / totalSpan);
      if (idx > lastIdx) idx = lastIdx;
      addSampleToBucket(buckets[idx]!, s.state);
    }
  } else {
    const bBig = BigInt(numBuckets);
    const spanBig = BigInt(totalSpan);
    for (let i = 0; i < samples.length; i++) {
      const s = samples[i]!;
      if (s.ts < startMs || s.ts >= endMs) continue;
      const rel = BigInt(s.ts - startMs);
      const idx = Math.min(numBuckets - 1, Number(((rel + 1n) * bBig - 1n) / spanBig));
      addSampleToBucket(buckets[idx]!, s.state);
    }
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

