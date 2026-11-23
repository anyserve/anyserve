// Pure scheduling primitives (mirrors machine.rs): fair priority queue, 64-bit xorshift/rendezvous
// hashing, and session selection. No engine state: callers pass a narrow `SchedulerView`.

import { type InterfacePolicyConfig, type PoolTierConfig } from "./host";
import { DEFAULT_FAIRNESS_KEY, type SessionMem, type JobMem } from "./engine-types";

const utf8Encoder = new TextEncoder();

// ---------------------------------------------------------------------------
// Exact 64-bit xorshift64* & Rendezvous Hash (mirrors machine.rs lines 1638-1668)
// ---------------------------------------------------------------------------

export const MASK_64 = 0xffff_ffff_ffff_ffffn;

export function nextRandom64(rng: { state: bigint }): bigint {
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

export interface QueueEntry {
  seq: bigint;
  scopedJobId: string;
}

export class FairPriorityQueue {
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

export interface SchedulerView {
  readonly sessions: ReadonlyMap<string, SessionMem>;
  readonly sessionsByInterface: ReadonlyMap<string, ReadonlyMap<string, ReadonlySet<string>>>;
  readonly rng: { state: bigint };
}

export function pickSessionForJob(
  view: SchedulerView,
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
    const s = view.sessions.get(sid);
    if (!s || sessionFreeSlots(s) === 0) continue;

    const tIdx = tierIndexForSession(tiers, s.pool, elapsedMs);
    if (tIdx === null) continue;

    if (!matchesRequiredAttributes(demand.required_attributes, s.attributes)) {
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
    if (!sessionFitsCapacity(s, demand.required_capacity)) {
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
      view.sessionsByInterface.get(job.namespace)?.get(job.interfaceName) ??
      new Set<string>();
    for (const peerSid of allSids) {
      const peer = view.sessions.get(peerSid);
      if (!peer || !peer.connected || peer.draining) continue;
      if (tierIndexForSession(tiers, peer.pool, elapsedMs) === null) continue;
      if (!matchesRequiredAttributes(demand.required_attributes, peer.attributes)) {
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
  const first = Number(nextRandom64(view.rng) % n);
  let second = Number(nextRandom64(view.rng) % (n - 1n));
  if (second >= first) {
    second += 1;
  }
  const a = best[first]!;
  const b = best[second]!;
  return sessionLoadScore(b) < sessionLoadScore(a)
    ? b.sessionId
    : a.sessionId;
}

export function tierIndexForSession(
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

export function matchesRequiredAttributes(
  required: Record<string, string>,
  actual: Record<string, string>,
): boolean {
  for (const [k, v] of Object.entries(required)) {
    if (actual[k] !== v) return false;
  }
  return true;
}

export function sessionLoadScore(s: SessionMem): number {
  const conc = Math.max(1, s.maxConcurrency);
  return (
    s.activeLeases.size / conc +
    s.utilization * 0.5 +
    (s.queued / conc) * 0.25
  );
}

export function sessionFreeSlots(s: SessionMem): number {
  if (!s.connected || s.draining) return 0;
  const byServer = Math.max(0, s.maxConcurrency - s.activeLeases.size);
  const byWorker = Math.max(0, s.reportedFree - s.assignedSinceReport);
  return Math.min(byServer, byWorker);
}

export function sessionFitsCapacity(
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
