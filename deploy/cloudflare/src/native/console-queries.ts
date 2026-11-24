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

    let sessionsCount = 0;
    for (const s of this.sessions.values()) {
      if (namespace !== undefined && s.namespace !== namespace) continue;
      sessionsCount += 1;
      const free = sessionFreeSlots(s);
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

