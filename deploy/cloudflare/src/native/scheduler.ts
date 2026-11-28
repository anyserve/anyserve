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

const INV_2_32 = 2.3283064365386963e-10; // 1 / 2^32

// Mutable 2-word scratch for zero-allocation 64-bit FNV-1a + SplitMix64 mixing.
let hashOutHi = 0;
let hashOutLo = 0;

function fnv1aFeedString(hi: number, lo: number, str: string): void {
  let h = hi;
  let l = lo;
  let ascii = true;
  for (let i = 0; i < str.length; i++) {
    const c = str.charCodeAt(i);
    if (c >= 0x80) {
      ascii = false;
      break;
    }
    l = (l ^ c) >>> 0;
    const prodLo = l * 0x1b3;
    const nextLo = prodLo >>> 0;
    const carry = (prodLo - nextLo) * INV_2_32;
    h = (Math.imul(h, 0x1b3) + ((l << 8) >>> 0) + carry) >>> 0;
    l = nextLo;
  }
  if (!ascii) {
    h = hi;
    l = lo;
    const bytes = utf8Encoder.encode(str);
    for (let i = 0; i < bytes.byteLength; i++) {
      l = (l ^ bytes[i]!) >>> 0;
      const prodLo = l * 0x1b3;
      const nextLo = prodLo >>> 0;
      const carry = (prodLo - nextLo) * INV_2_32;
      h = (Math.imul(h, 0x1b3) + ((l << 8) >>> 0) + carry) >>> 0;
      l = nextLo;
    }
  }
  hashOutHi = h;
  hashOutLo = l;
}

function fnv1aFeedSepAndFinalize(
  prefixHi: number,
  prefixLo: number,
  workerId: string,
): void {
  fnv1aFeedString(prefixHi, prefixLo, workerId);
  let hi = hashOutHi;
  let lo = hashOutLo;

  // hash = ((hash ^ (hash >> 30)) * 0xbf58_476d_1ce4_e5b9) & MASK_64
  lo = (lo ^ ((lo >>> 30) | (hi << 2))) >>> 0;
  hi = (hi ^ (hi >>> 30)) >>> 0;
  let a00 = lo & 0xffff;
  let a16 = lo >>> 16;
  let c00 = a00 * 0xe5b9;
  let c16 = (c00 >>> 16) + a16 * 0xe5b9;
  let c32 = c16 >>> 16;
  c16 = (c16 & 0xffff) + a00 * 0x1ce4;
  c32 += c16 >>> 16;
  hi = (c32 + Math.imul(a16, 0x1ce4) + Math.imul(hi, 0x1ce4e5b9) + Math.imul(lo, 0xbf58476d)) >>> 0;
  lo = ((c16 << 16) | (c00 & 0xffff)) >>> 0;

  // hash = ((hash ^ (hash >> 27)) * 0x94d0_49bb_1331_11eb) & MASK_64
  lo = (lo ^ ((lo >>> 27) | (hi << 5))) >>> 0;
  hi = (hi ^ (hi >>> 27)) >>> 0;
  a00 = lo & 0xffff;
  a16 = lo >>> 16;
  c00 = a00 * 0x11eb;
  c16 = (c00 >>> 16) + a16 * 0x11eb;
  c32 = c16 >>> 16;
  c16 = (c16 & 0xffff) + a00 * 0x1331;
  c32 += c16 >>> 16;
  hi = (c32 + Math.imul(a16, 0x1331) + Math.imul(hi, 0x133111eb) + Math.imul(lo, 0x94d049bb)) >>> 0;
  lo = ((c16 << 16) | (c00 & 0xffff)) >>> 0;

  // hash = (hash ^ (hash >> 31)) & MASK_64
  hashOutLo = (lo ^ ((lo >>> 31) | (hi << 1))) >>> 0;
  hashOutHi = (hi ^ (hi >>> 31)) >>> 0;
}

function rendezvousPrefix(affinityKey: string): void {
  fnv1aFeedString(0xcbf29ce4, 0x84222325, affinityKey);
  const loXor = (hashOutLo ^ 0xff) >>> 0;
  const prodLo = loXor * 0x1b3;
  const nextLo = prodLo >>> 0;
  const carry = (prodLo - nextLo) * INV_2_32;
  hashOutHi = (Math.imul(hashOutHi, 0x1b3) + ((loXor << 8) >>> 0) + carry) >>> 0;
  hashOutLo = nextLo;
}

export function rendezvousHash64(affinityKey: string, workerId: string): bigint {
  rendezvousPrefix(affinityKey);
  fnv1aFeedSepAndFinalize(hashOutHi, hashOutLo, workerId);
  return (BigInt(hashOutHi) << 32n) | BigInt(hashOutLo);
}

export interface QueueEntry {
  seq: bigint;
  scopedJobId: string;
}

interface FairSubqueue {
  items: Array<QueueEntry | undefined>;
  head: number;
  liveCount: number;
  posById: Map<string, number>;
  tailSeq: bigint;
  tailId: string;
}

function compactSubqueue(sub: FairSubqueue): void {
  const next: Array<QueueEntry | undefined> = new Array(sub.liveCount);
  const posById = sub.posById;
  posById.clear();
  let write = 0;
  for (let i = sub.head; i < sub.items.length; i++) {
    const item = sub.items[i];
    if (item !== undefined) {
      next[write] = item;
      posById.set(item.scopedJobId, write);
      write += 1;
    }
  }
  sub.items = next;
  sub.head = 0;
  if (write > 0) {
    const tail = next[write - 1]!;
    sub.tailSeq = tail.seq;
    sub.tailId = tail.scopedJobId;
  }
}

export class FairPriorityQueue {
  private readonly levels: Array<{
    ring: string[];
    subqueues: Map<string, FairSubqueue>;
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
      sub = {
        items: [],
        head: 0,
        liveCount: 0,
        posById: new Map(),
        tailSeq: 0n,
        tailId: "",
      };
      level.subqueues.set(fkey, sub);
      level.ring.push(fkey);
    }
    if (sub.posById.has(entry.scopedJobId)) {
      return;
    }
    if (
      sub.liveCount === 0 ||
      entry.seq > sub.tailSeq ||
      (entry.seq === sub.tailSeq && entry.scopedJobId > sub.tailId)
    ) {
      // O(1) monotonic append (the common case for queueSeq)
      const pos = sub.items.length;
      sub.items.push(entry);
      sub.posById.set(entry.scopedJobId, pos);
      sub.liveCount += 1;
      sub.tailSeq = entry.seq;
      sub.tailId = entry.scopedJobId;
    } else {
      // Out-of-order insert: compact tombstones if needed, then splice in sorted order.
      if (sub.head > 0 || sub.liveCount !== sub.items.length) {
        compactSubqueue(sub);
      }
      let pos = sub.items.length;
      while (pos > 0) {
        const prev = sub.items[pos - 1]!;
        if (
          prev.seq < entry.seq ||
          (prev.seq === entry.seq && prev.scopedJobId < entry.scopedJobId)
        ) {
          break;
        }
        pos -= 1;
      }
      sub.items.splice(pos, 0, entry);
      for (let i = pos; i < sub.items.length; i++) {
        sub.posById.set(sub.items[i]!.scopedJobId, i);
      }
      sub.liveCount += 1;
      const tail = sub.items[sub.items.length - 1]!;
      sub.tailSeq = tail.seq;
      sub.tailId = tail.scopedJobId;
    }
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
    const pos = sub.posById.get(scopedJobId);
    if (pos === undefined) {
      return false;
    }
    sub.posById.delete(scopedJobId);
    sub.items[pos] = undefined;
    sub.liveCount -= 1;
    level.count -= 1;
    this.totalLen -= 1;

    if (sub.liveCount === 0) {
      level.subqueues.delete(fkey);
      const rIdx = level.ring.indexOf(fkey);
      if (rIdx >= 0) {
        level.ring.splice(rIdx, 1);
      }
      return true;
    }

    if (pos === sub.head) {
      while (sub.head < sub.items.length && sub.items[sub.head] === undefined) {
        sub.head += 1;
      }
    }
    if (pos === sub.items.length - 1) {
      while (sub.items.length > sub.head && sub.items[sub.items.length - 1] === undefined) {
        sub.items.pop();
      }
      const tail = sub.items[sub.items.length - 1]!;
      sub.tailSeq = tail.seq;
      sub.tailId = tail.scopedJobId;
    }
    if (sub.items.length >= 64 && sub.liveCount * 2 <= sub.items.length) {
      compactSubqueue(sub);
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
        const sub = level.subqueues.get(level.ring[0]!);
        if (sub) {
          const items = sub.items;
          for (let i = sub.head; i < items.length && out.length < limit; i++) {
            const item = items[i];
            if (item !== undefined) {
              out.push(item);
            }
          }
        }
        continue;
      }
      const cursors: Array<{ items: Array<QueueEntry | undefined>; pos: number }> = [];
      for (let r = 0; r < level.ring.length; r++) {
        const sub = level.subqueues.get(level.ring[r]!);
        if (sub && sub.liveCount > 0) {
          cursors.push({ items: sub.items, pos: sub.head });
        }
      }
      let added = 0;
      while (added < need && cursors.length > 0) {
        let progressed = false;
        for (let i = 0; i < cursors.length && added < need; i++) {
          const c = cursors[i]!;
          const items = c.items;
          while (c.pos < items.length && items[c.pos] === undefined) {
            c.pos += 1;
          }
          if (c.pos < items.length) {
            out.push(items[c.pos]!);
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

function hasAnyKey(obj: Record<string, unknown>): boolean {
  for (const _ in obj) {
    return true;
  }
  return false;
}

export function pickSessionForJob(
  view: SchedulerView,
  job: JobMem,
  policy: InterfacePolicyConfig | undefined,
  candidates: string[],
  nowMs: number,
): string | null {
  const demand = job.spec.demand;
  const reqAttrs = demand.required_attributes;
  const hasReqAttrs = hasAnyKey(reqAttrs);
  const prefAttrs = demand.preferred_attributes;
  let prefCount = 0;
  for (const _ in prefAttrs) {
    prefCount += 1;
  }
  const hasPreferred = prefCount > 0;
  const hasReqCap = hasAnyKey(demand.required_capacity);
  const spillOpen =
    !hasPreferred ||
    job.spilloverAtMs === null ||
    nowMs >= job.spilloverAtMs;
  const tiers: readonly PoolTierConfig[] = policy?.poolTiers ?? EMPTY_TIERS;
  const hasTiers = tiers.length > 0;
  const elapsedMs = Math.max(0, nowMs - job.queuedSinceMs);
  const hasFailedWorkers = job.failedWorkers.size > 0;
  const hasRejectedSessions = job.rejectedSessions.size > 0;

  let best: SessionMem[] = [];
  let bestHealthy = false;
  let bestNotCooled = false;
  let bestTierIdx = Number.MAX_SAFE_INTEGER; // smaller tier index is higher rank!
  let bestMatches = -1;

  for (let i = 0; i < candidates.length; i++) {
    const s = view.sessions.get(candidates[i]!);
    if (!s || sessionFreeSlots(s) === 0) continue;

    const tIdx = hasTiers ? tierIndexForSession(tiers, s.pool, elapsedMs) : 0;
    if (tIdx === null) continue;

    if (hasReqAttrs && !matchesRequiredAttributes(reqAttrs, s.attributes)) {
      continue;
    }
    let matches = 0;
    if (hasPreferred) {
      for (const k in prefAttrs) {
        if (s.attributes[k] === prefAttrs[k]) matches += 1;
      }
      if (!spillOpen && matches < prefCount) {
        continue;
      }
    }
    if (hasReqCap && !sessionFitsCapacity(s, demand.required_capacity)) {
      continue;
    }

    const healthy =
      (!hasFailedWorkers || !job.failedWorkers.has(s.workerId)) &&
      (!hasRejectedSessions || !job.rejectedSessions.has(s.sessionId));
    const notCooled = s.cooldownUntilMs === 0 || nowMs >= s.cooldownUntilMs;

    if (best.length === 0) {
      best = [s];
      bestHealthy = healthy;
      bestNotCooled = notCooled;
      bestTierIdx = tIdx;
      bestMatches = matches;
    } else {
      let cmp = 0;
      if (healthy !== bestHealthy) cmp = healthy ? 1 : -1;
      else if (notCooled !== bestNotCooled) cmp = notCooled ? 1 : -1;
      else if (tIdx !== bestTierIdx) cmp = tIdx < bestTierIdx ? 1 : -1;
      else if (matches !== bestMatches) cmp = matches > bestMatches ? 1 : -1;

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
    const allSids = view.sessionsByInterface
      .get(job.namespace)
      ?.get(job.interfaceName);
    if (allSids) {
      for (const peerSid of allSids) {
        const peer = view.sessions.get(peerSid);
        if (!peer || !peer.connected || peer.draining) continue;
        if (hasTiers && tierIndexForSession(tiers, peer.pool, elapsedMs) === null) continue;
        if (hasReqAttrs && !matchesRequiredAttributes(reqAttrs, peer.attributes)) {
          continue;
        }
        if (hasReqCap) {
          let totalCapOk = true;
          for (const name in demand.required_capacity) {
            const needed = demand.required_capacity[name]!;
            if (needed > 0n && (peer.totalCapacity[name] ?? 0n) < needed) {
              totalCapOk = false;
              break;
            }
          }
          if (!totalCapOk) continue;
        }
        const peerBetter = !bestHealthy
          ? (!hasFailedWorkers || !job.failedWorkers.has(peer.workerId)) &&
            (!hasRejectedSessions || !job.rejectedSessions.has(peer.sessionId))
          : nowMs >= peer.cooldownUntilMs;
        if (peerBetter) {
          return null;
        }
      }
    }
  }

  if (best.length === 1) {
    return best[0]!.sessionId;
  }

  // Bounded-load Rendezvous Hashing for `affinity_key` (< 0.85 utilization)
  if (demand.affinity_key.length > 0) {
    rendezvousPrefix(demand.affinity_key);
    const prefixHi = hashOutHi;
    const prefixLo = hashOutLo;
    let winner: SessionMem | null = null;
    let winHi = 0;
    let winLo = 0;
    for (let i = 0; i < best.length; i++) {
      const cand = best[i]!;
      const conc = cand.maxConcurrency > 1 ? cand.maxConcurrency : 1;
      if (cand.activeLeases.size / conc >= 0.85) {
        continue;
      }
      fnv1aFeedSepAndFinalize(prefixHi, prefixLo, cand.workerId);
      const hi = hashOutHi;
      const lo = hashOutLo;
      if (
        winner === null ||
        hi > winHi ||
        (hi === winHi && (lo > winLo || (lo === winLo && cand.workerId > winner.workerId)))
      ) {
        winner = cand;
        winHi = hi;
        winLo = lo;
      }
    }
    if (winner !== null) {
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

const EMPTY_TIERS: readonly PoolTierConfig[] = [];

export function tierIndexForSession(
  tiers: readonly PoolTierConfig[],
  pool: string,
  elapsedMs: number,
): number | null {
  if (tiers.length === 0) {
    return 0;
  }
  for (let i = 0; i < tiers.length; i++) {
    if (tiers[i]!.pool === pool) {
      return elapsedMs >= tiers[i]!.spilloverAfterMs ? i : null;
    }
  }
  return null;
}

export function matchesRequiredAttributes(
  required: Record<string, string>,
  actual: Record<string, string>,
): boolean {
  for (const k in required) {
    if (actual[k] !== required[k]) return false;
  }
  return true;
}

export function sessionLoadScore(s: SessionMem): number {
  const conc = s.maxConcurrency > 1 ? s.maxConcurrency : 1;
  return (
    s.activeLeases.size / conc +
    s.utilization * 0.5 +
    (s.queued / conc) * 0.25
  );
}

export function sessionFreeSlots(s: SessionMem): number {
  if (!s.connected || s.draining) return 0;
  const byServer = s.maxConcurrency - s.activeLeases.size;
  if (byServer <= 0) return 0;
  const byWorker = s.reportedFree - s.assignedSinceReport;
  if (byWorker <= 0) return 0;
  return byServer < byWorker ? byServer : byWorker;
}

export function sessionFitsCapacity(
  s: SessionMem,
  demand: Record<string, bigint>,
): boolean {
  for (const name in demand) {
    const needed = demand[name]!;
    if (needed <= 0n) continue;
    const total = s.totalCapacity[name];
    if (total === undefined) return false;
    const reserved = s.reservedCapacity[name] ?? 0n;
    if (total - reserved < needed) return false;
  }
  return true;
}
