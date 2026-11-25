// Job event log append + delivery to primary Invoke callers and Watchers (incl. large Finished streaming).
// Methods are mixed into `NativeEngine.prototype` (`this: NativeEngine`).

import { GrpcCode } from "./auth";
import {
  IsolateMemoryBudget,
  R2_READ_BUFFER_BYTES,
  WS_MAX_ENCODED_BYTES,
  WS_READY_STATE_OPEN,
  type NativeWebSocket,
} from "./host";
import { joinU64, normalizeBlob, splitU64, type StoredErrorInfo } from "./store";
import * as pb from "./proto";
import { nameToProtoState, type JobMem } from "./engine-types";
import {
  varintLen32,
  varintLen64,
  writeVarint32At,
  writeVarint64At,
  lenDelimitedSize,
  writeStringMapAt,
  encodedObjectLenFromManifestEntry,
  type OutputManifestItemRef,
} from "./event-wire";
import type { NativeEngine } from "./engine";

const utf8Encoder = new TextEncoder();

export const eventDelivery = {
  appendJobEventInTxn(
    this: NativeEngine,
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
  },

  async notifyJobFollowers(this: NativeEngine, scopedJobId: string): Promise<void> {
    const job = this.jobs.get(scopedJobId);
    if (!job) return;

    const tasks: Array<Promise<void>> = [];
    if (job.callerConnId !== null) {
      tasks.push(this.deliverPrimaryInvokeEvents(scopedJobId));
    }
    const execWaiter = this.executeWaiters.get(scopedJobId);
    if (
      execWaiter &&
      (job.state === "succeeded" ||
        job.state === "failed" ||
        job.state === "cancelled")
    ) {
      this.executeWaiters.delete(scopedJobId);
      tasks.push(
        (async () => {
          try {
            let outputs: pb.ProtoObjectInput[] = [];
            if (job.state === "succeeded") {
              const loaded = await this.store.loadBundleObjects(
                job.scopedJobId,
                "output",
                this.host.nowMs(),
              );
              outputs = loaded.objects.map((o) =>
                o.kind === "uri"
                  ? {
                      body: { case: "uri" as const, value: o.uri ?? "" },
                      metadata: o.metadata,
                    }
                  : {
                      body: {
                        case: "inline" as const,
                        value: o.data ?? new Uint8Array(0),
                      },
                      metadata: o.metadata,
                    },
              );
            }
            execWaiter.resolve({
              jobId: job.rawJobId,
              sequence: job.lastSequence,
              attemptId: job.lastAttemptId ?? "",
              atMs: BigInt(job.updatedAtMs),
              event: {
                case: "finished",
                value: {
                  state: nameToProtoState(job.state),
                  outputs,
                  ...(job.error
                    ? {
                        error: {
                          code: job.error.code,
                          message: job.error.message,
                          retryable: job.error.retryable,
                        },
                      }
                    : {}),
                },
              },
            });
          } catch (err) {
            execWaiter.reject(
              err instanceof Error ? err : new Error(String(err)),
            );
          }
        })(),
      );
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
  },

  async deliverPrimaryInvokeEvents(this: NativeEngine, scopedJobId: string): Promise<void> {
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
  },

  async deliverWatcherEvents(this: NativeEngine, watchConnId: number): Promise<void> {
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
  },

  removeWatcherInTxn(this: NativeEngine, watchConnId: number): void {
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
  },

  readJobEventsAfterSync(this: NativeEngine, scopedJobId: string, afterSeq: bigint) {
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
  },

  encodeNonTerminalEventWsFrame(
    this: NativeEngine,
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
  },

  /**
   * Pre-sized single-buffer encoder & sender for `ExecutionEvent{Finished}` (sections 5.1 & 5.2):
   * - Computes exact encoded frame length `L` purely from output manifests (ZERO payload reads yet!)
   * - Reserves `2 * L + r2ReadBuffer` in `IsolateMemoryBudget` (and `2 * L` in `56 MiB` large-send
   *   semaphore when `L > 1 MiB`, so two concurrent `3 x 7 MiB` sends serialize cleanly!)
   * - Allocates ONE `Uint8Array(L)` and streams `<= 2 MiB` slices directly into `buf.set(slice, pos)`!
   * - Calls `ws.send(buf)`, releases JS half (`L` + R2 read buffer) keeping `max(bufferedAmount, L)`
   *   until `bufferedAmount` drains, then sends `ws.close(1000)`!
   */
  async sendFinishedEventToSocket(
    this: NativeEngine,
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
  },
};

export type EventDelivery = typeof eventDelivery;
