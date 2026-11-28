// Worker attempt-socket protocol (AttemptOpen, input pump, live Frame/ResultChunk/Finish, terminal replay).
// Methods are mixed into `NativeEngine.prototype` (`this: NativeEngine`).

import { GrpcStatusError } from "./auth";
import {
  BUNDLE_INLINE_MAX_BYTES,
  EVENT_OVERHEAD_BYTES,
  INPUT_BATCH_MAX_BYTES,
  INPUT_BATCH_MAX_CHUNKS,
  MAX_METADATA_BYTES,
  MAX_OBJECTS,
  UPLOAD_SEGMENT_BYTES,
  UPLOAD_WINDOW_SLACK_THRESHOLD,
  WS_READY_STATE_OPEN,
  writeSocketAttachment,
  type NativeWebSocket,
  type SocketAttachment,
} from "./host";
import {
  IncrementalSha256,
  OBJECT_OVERHEAD,
  outcomeDigest,
  outcomeDigestStream,
  resultObjectAccountedBytes,
  type CanonicalObjectInput,
  type StreamingCanonicalObjectInput,
} from "./identity";
import {
  concatBytes,
  normalizeBlob,
  splitU64,
  type BundleManifestEntry,
  type SegmentSliceRef,
  type StoredErrorInfo,
} from "./store";
import { utf8ByteLen } from "./utf8";
import * as pb from "./proto";
import { RUNTIME_CODES, metadataByteLen, type LeaseMem } from "./engine-types";
import type { NativeEngine } from "./engine";

export const attemptHandlers = {
  async handleAttemptSocketMessage(
    this: NativeEngine,
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
  },

  lookupRecordedReceiptSync(this: NativeEngine, leaseId: string, namespace: string): string | null {
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
  },

  async handleAttemptOpen(
    this: NativeEngine,
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
      const isCleanInitialOpen =
        prevAttemptConnId === null &&
        lease.tailNextChunkSeq === 0 &&
        lease.nextSegIndex === 0 &&
        lease.nextObjIndex === 0 &&
        lease.curObjIndex === null &&
        lease.totalResultBytes === 0 &&
        lease.inputSeq === 0n &&
        lease.inputItemIdx === 0 &&
        lease.inputByteOff === 0;

      // Section 4.4: A new AttemptOpen (new attempt_conn_id) discards uncommitted tail & segments!
      this.store.transactionSync(() => {
        if (isCleanInitialOpen) {
          lease.attemptConnId = att.connId;
          lease.accepted = true;
          this.store.exec(
            `UPDATE native_active_leases SET attempt_conn_id = ?, accepted = 1 WHERE lease_id = ?`,
            att.connId,
            lease.leaseId,
          );
          return;
        }
        if (lease.nextSegIndex > 0) {
          const segRows = this.store.exec<{ r2_key: string | null }>(
            `SELECT r2_key FROM native_upload_segments WHERE lease_id = ? AND r2_key IS NOT NULL`,
            lease.leaseId,
          );
          const r2Keys = segRows.map((r) => r.r2_key!).filter(Boolean);
          if (r2Keys.length > 0) {
            this.store.queuePendingDeleteInTxn(r2Keys, nowMs);
          }
        }
        if (lease.tailNextChunkSeq > 0 || lease.nextSegIndex > 0) {
          this.store.exec(`DELETE FROM native_upload_tail WHERE lease_id = ?`, lease.leaseId);
          this.store.exec(`DELETE FROM native_upload_segments WHERE lease_id = ?`, lease.leaseId);
        }

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
      this.sendWsRawSync(
        ws,
        pb.encodeAttemptDownWsFrame({
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
  },

  /**
   * Server-to-worker `InputData` pump (sections 4.4 & 5.3):
   * - Sends synchronous batches (<= 1 MiB / 16 chunks while `bufferedAmount` allows)
   * - Order: static objects (`objectIndex = i`, empty inline & URI included, metadata on first piece,
   *   `objectEnd` on last piece), non-empty params (`objectIndex = undefined`, `parameterChunk = true`,
   *   `objectEnd` on last piece), streaming chunks (`objectIndex = staticCount + s`), trailer (`objectIndex = undefined`, `last = true`).
   * - Writes `(input_stage, input_item_idx, input_byte_off, input_seq)` to `native_active_leases`
   *   in the EXACT SAME synchronous turn as `ws.send()` (ZERO `await` in between)!
   */
  async pumpAttemptInputs(this: NativeEngine, leaseId: string): Promise<void> {
    if (this.pumpingAttempts.has(leaseId)) {
      return;
    }
    this.pumpingAttempts.add(leaseId);
    let staticCache: {
      scopedJobId: string;
      connId: number;
      staticInputs: Array<{ seq: number; entry: BundleManifestEntry }>;
      paramsRef: { seq: number; entry: BundleManifestEntry } | null;
    } | null = null;
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

        // `static_in` manifests are immutable once the job is accepted: read them ONCE per pump run and
        // re-read only if the lease's attempt connection (or job) changed. `stream_in` grows, so it is
        // re-read every batch below.
        if (
          staticCache === null ||
          staticCache.scopedJobId !== job.scopedJobId ||
          staticCache.connId !== snapConnId
        ) {
          const staticInputsRead: Array<{ seq: number; entry: BundleManifestEntry }> = [];
          let paramsRefRead: { seq: number; entry: BundleManifestEntry } | null = null;
          for (const row of this.store.readBundleManifestsSync(job.scopedJobId, "static_in")) {
            for (const e of row.entries) {
              if (e.t === "p" || e.t === "pr2") {
                paramsRefRead = { seq: row.seq, entry: e };
              } else {
                staticInputsRead.push({ seq: row.seq, entry: e });
              }
            }
          }
          staticCache = {
            scopedJobId: job.scopedJobId,
            connId: snapConnId,
            staticInputs: staticInputsRead,
            paramsRef: paramsRefRead,
          };
        }
        const { staticInputs, paramsRef } = staticCache;

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
            const frame = pb.encodeAttemptDownWsFrame({
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
            });
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
            const frame = pb.encodeAttemptDownWsFrame({
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
            });
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
            const frame = pb.encodeAttemptDownWsFrame({
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
            });
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
            const frame = pb.encodeAttemptDownWsFrame({
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
            });
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
  },

  async handleLiveAttemptFrame(
    this: NativeEngine,
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
    const frameMetaBytes = metadataByteLen(frame.metadata);
    if (frameMetaBytes > MAX_METADATA_BYTES) {
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

    const eventSize = EVENT_OVERHEAD_BYTES + frame.data.byteLength + frameMetaBytes;

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
          this.syncJobTimerTracking(job);
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
      let hasFrameMeta = false;
      for (const _ in frame.metadata) {
        hasFrameMeta = true;
        break;
      }
      this.appendJobEventInTxn(
        job,
        lease.attemptId,
        frame.progress ? "progress" : "output",
        hasFrameMeta || frame.progress
          ? { metadata: frame.metadata, progress: frame.progress }
          : null,
        frame.data,
        eventSize,
        nowMs,
      );
      const lastSplit = splitU64(job.lastSequence);
      this.store.exec(
        `UPDATE native_jobs
         SET last_seq_hi = ?, last_seq_lo = ?, event_log_count = ?, event_log_bytes = ?
         WHERE scoped_job_id = ?`,
        lastSplit.hi,
        lastSplit.lo,
        job.eventLogCount,
        job.eventLogBytes,
        job.scopedJobId,
      );
    });

    this.sendWsRawSync(
      ws,
      pb.encodeAttemptDownWsFrame({
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
  },

  /**
   * Worker `ResultChunk` upload (section 4.4):
   * - Single-chunk object (`chunk.last` on first chunk, <= 512 KiB): stored inline on `native_active_leases`
   *   before `Ack` is sent (zero `native_upload_tail` / `native_upload_segments` rows!).
   * - Multi-chunk object: EVERY chunk persisted to `native_upload_tail` BEFORE `webSocketMessage` returns!
   * - Seals at `UPLOAD_SEGMENT_BYTES` (2 MiB), `chunk.last`, or `< 1 MiB` window slack into R2 segment
   *   (or inline BLOB for short final piece), deletes sealed tail rows, updates `acked_result_bytes`, then sends `Ack`!
   */
  async handleLiveAttemptResultChunk(
    this: NativeEngine,
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

      this.sendWsRawSync(
        ws,
        pb.encodeAttemptDownWsFrame({
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
          OBJECT_OVERHEAD + metaBytes + utf8ByteLen(chunk.uri);
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
  },

  async handleLiveAttemptFinish(
    this: NativeEngine,
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
      this.sendWsRawSync(
        ws,
        pb.encodeAttemptDownWsFrame({
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
  },

  finishOutcomeError(
    this: NativeEngine,
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
  },

  async computeLiveLeaseOutcomeDigest(
    this: NativeEngine,
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
  },

  writeCompletedLeaseOutputsToBundleInTxn(
    this: NativeEngine,
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
  },

  async handleReplayAttemptMessage(
    this: NativeEngine,
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
          OBJECT_OVERHEAD + metaBytes + utf8ByteLen(chunk.uri),
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
  },
};

export type AttemptHandlers = typeof attemptHandlers;
