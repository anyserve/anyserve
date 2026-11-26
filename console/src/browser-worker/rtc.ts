import {
  buildWsSubprotocols,
  decodeUtf8,
  decodeWsPayload,
  encodeUtf8,
  pb,
  type StreamingInvokeResult,
  type StreamingInvokeTimelineEvent,
} from "./protocol";

export const DEFAULT_RTC_ICE_SERVERS: RTCIceServer[] = [
  { urls: ["stun:stun.l.google.com:19302", "stun:stun.cloudflare.com:3478"] },
];

export const RTC_SIGNAL_META_KEY = "anyserve.rtc";
export const RTC_SIGNAL_META_VALUE = "signal";
export const DEFAULT_ICE_TIMEOUT_MS = 3_500;
export const DEFAULT_RTC_MAX_ATTEMPTS = 2;
const DC_HIGH_WATER_BYTES = 256 * 1024;
const DC_LOW_WATER_BYTES = 64 * 1024;
const DC_DRAIN_POLL_MS = 20;

export type ClientRtcSignal =
  | { t: "offer"; kind?: "webrtc"; sdp: string }
  | { t: "ice"; candidate: RTCIceCandidateInit }
  | { t: "ice_end" }
  | { t: "p2p_ready" }
  | {
      t: "fallback";
      reason: string;
      prompt: string;
      benchChunks?: number;
      benchChunkBytes?: number;
    };

export type WorkerRtcSignal =
  | { t: "answer"; kind?: "webrtc"; sdp: string }
  | { t: "ice"; candidate: RTCIceCandidateInit }
  | { t: "ice_end" }
  | { t: "rtc_error"; reason: string };

export type DataChannelClientRequest = {
  t: "req";
  prompt: string;
  benchChunks?: number;
  benchChunkBytes?: number;
};

export type DataChannelClientMsg = DataChannelClientRequest | { t: "ack" };

export type DataChannelWorkerMsg =
  | { t: "progress"; text: string }
  | { t: "tok"; text: string }
  | { t: "done"; handlerMs: number; p2pBytes: number; frames: number }
  | { t: "err"; message: string };

export interface RtcInvokeOptions {
  wsBaseUrl: string;
  token?: string | null;
  interfaceName: string;
  prompt: string;
  benchChunks?: number;
  benchChunkBytes?: number;
  preferredAttributes?: Record<string, string>;
  metadata?: Record<string, string>;
  /**
   * Optional WebRTC data-plane bypass is strictly limited to EPHEMERAL Invoke jobs.
   * Passing `"durable"` is rejected explicitly so callers never assume server-side
   * output persistence or replay on a P2P-bypassed stream.
   */
  durability?: "ephemeral" | "durable";
  /**
   * Maximum client-side scheduling rounds when a P2P peer disconnects BEFORE any
   * output has been exposed to the caller (default: 2). Once any output chunk has
   * been exposed, disconnects fail fast with `output_interrupted` without retrying.
   */
  maxAttempts?: number;
  iceServers?: RTCIceServer[];
  iceTimeoutMs?: number;
  signal?: AbortSignal;
  WebSocketCtor?: typeof WebSocket;
  PeerConnectionCtor?: typeof RTCPeerConnection | null;
  onJobAccepted?: (jobId: string) => void;
  onChunk?: (text: string) => void;
  onBinaryChunk?: (bytes: Uint8Array) => void;
  onProgress?: (message: string) => void;
  onTimeline?: (event: StreamingInvokeTimelineEvent) => void;
}

export interface WorkerRtcPeerCallbacks {
  sendSignal: (signal: WorkerRtcSignal) => void;
  onRequest: (req: DataChannelClientRequest) => void;
  onAck?: () => void;
  onDisconnected?: () => void;
}

/**
 * Main-thread (`Window`) WebRTC responder attached to a single Browser Worker lease.
 * Runs in the main thread because `RTCPeerConnection` is `[Exposed=Window]`.
 */
export class WorkerRtcPeer {
  private readonly pc: RTCPeerConnection | null;
  private dc: RTCDataChannel | null = null;
  private readonly pendingIce: RTCIceCandidateInit[] = [];
  private remoteSet = false;
  private closed = false;
  private readonly createdAt = performance.now();
  private iceMs: number | null = null;
  private sendChain: Promise<void> = Promise.resolve();
  private readonly callbacks: WorkerRtcPeerCallbacks;

  constructor(
    callbacks: WorkerRtcPeerCallbacks,
    iceServers: RTCIceServer[] = DEFAULT_RTC_ICE_SERVERS,
    PeerConnectionCtor: typeof RTCPeerConnection | null = typeof RTCPeerConnection !== "undefined"
      ? RTCPeerConnection
      : null,
  ) {
    this.callbacks = callbacks;
    if (!PeerConnectionCtor) {
      this.pc = null;
      callbacks.sendSignal({ t: "rtc_error", reason: "RTCPeerConnection unavailable in worker host" });
      return;
    }
    const pc = new PeerConnectionCtor({ iceServers });
    this.pc = pc;

    pc.onicecandidate = (ev) => {
      if (this.closed) return;
      if (ev.candidate) {
        callbacks.sendSignal({
          t: "ice",
          candidate: typeof ev.candidate.toJSON === "function" ? ev.candidate.toJSON() : ev.candidate,
        });
      } else {
        callbacks.sendSignal({ t: "ice_end" });
      }
    };

    pc.ondatachannel = (ev) => {
      if (this.closed) return;
      const dc = ev.channel;
      dc.binaryType = "arraybuffer";
      dc.bufferedAmountLowThreshold = DC_LOW_WATER_BYTES;
      this.dc = dc;
      dc.onopen = () => {
        if (this.iceMs === null) {
          this.iceMs = Math.max(0, Math.round(performance.now() - this.createdAt));
        }
      };
      dc.onmessage = (msgEv) => {
        if (this.closed || typeof msgEv.data !== "string") return;
        try {
          const parsed = JSON.parse(msgEv.data) as DataChannelClientMsg;
          if (parsed && parsed.t === "req") {
            callbacks.onRequest(parsed);
          } else if (parsed && parsed.t === "ack") {
            callbacks.onAck?.();
          }
        } catch {
          // ignore malformed peer message
        }
      };
      dc.onclose = () => {
        if (!this.closed) callbacks.onDisconnected?.();
      };
    };
  }

  getIceMs(): number {
    return this.iceMs ?? Math.max(0, Math.round(performance.now() - this.createdAt));
  }

  async handleSignal(sig: ClientRtcSignal): Promise<void> {
    if (this.closed || !this.pc) return;
    try {
      if (sig.t === "offer") {
        await this.pc.setRemoteDescription({ type: "offer", sdp: sig.sdp });
        this.remoteSet = true;
        for (const cand of this.pendingIce.splice(0)) {
          await this.pc.addIceCandidate(cand);
        }
        const answer = await this.pc.createAnswer();
        await this.pc.setLocalDescription(answer);
        if (answer.sdp) {
          this.callbacks.sendSignal({ t: "answer", kind: "webrtc", sdp: answer.sdp });
        }
      } else if (sig.t === "ice") {
        if (this.remoteSet) {
          await this.pc.addIceCandidate(sig.candidate);
        } else {
          this.pendingIce.push(sig.candidate);
        }
      }
    } catch (err) {
      this.callbacks.sendSignal({
        t: "rtc_error",
        reason: err instanceof Error ? err.message : String(err),
      });
    }
  }

  sendData(payload: string | Uint8Array): Promise<void> {
    const queued = this.sendChain.then(() => this.writeFrame(payload));
    this.sendChain = queued.catch(() => {});
    return queued;
  }

  private async writeFrame(payload: string | Uint8Array): Promise<void> {
    const dc = this.dc;
    if (this.closed || !dc || dc.readyState !== "open") return;
    if (dc.bufferedAmount > DC_HIGH_WATER_BYTES) {
      await new Promise<void>((resolve) => {
        let settled = false;
        const finish = () => {
          if (settled) return;
          settled = true;
          dc.removeEventListener?.("bufferedamountlow", finish);
          clearInterval(poll);
          resolve();
        };
        if (typeof dc.addEventListener === "function") {
          dc.addEventListener("bufferedamountlow", finish, { once: true });
        } else {
          dc.onbufferedamountlow = finish;
        }
        const poll = setInterval(() => {
          if (this.closed || dc.readyState !== "open" || dc.bufferedAmount <= DC_LOW_WATER_BYTES) {
            finish();
          }
        }, DC_DRAIN_POLL_MS);
        if (dc.bufferedAmount <= DC_LOW_WATER_BYTES) {
          finish();
        }
      });
    }
    if (!this.closed && dc.readyState === "open") {
      if (typeof payload === "string") {
        dc.send(payload);
      } else if (payload.buffer instanceof ArrayBuffer) {
        dc.send(payload as Uint8Array<ArrayBuffer>);
      } else {
        const copy = new Uint8Array(payload.byteLength);
        copy.set(payload);
        dc.send(copy);
      }
    }
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    try {
      this.dc?.close();
    } catch {
      // ignore
    }
    this.dc = null;
    try {
      this.pc?.close();
    } catch {
      // ignore
    }
  }
}

type RoundOutcome =
  | { kind: "done"; result: StreamingInvokeResult }
  | { kind: "retry"; reason: string; jobId: string | null; workerId: string | null };

/**
 * Optional EPHEMERAL-only WebRTC P2P data-plane bypass over AnyServe's existing
 * full-duplex `/ws/v2/client/invoke` (zero server/proto changes).
 *
 * Semantics match AnyServe's relay guarantees:
 * - Only triggered explicitly via `metadata["anyserve.transport"] = "webrtc"` on
 *   EPHEMERAL Invoke requests (durable requests are rejected).
 * - Public STUN only (no TURN): symmetric NAT / blocked UDP falls back to WS Relay
 *   automatically after `iceTimeoutMs` (default 3.5 s).
 * - Failover consistency: if the P2P peer drops BEFORE any output chunk has been
 *   exposed to the caller, the client automatically re-schedules up to `maxAttempts`
 *   rounds. Once output has been exposed, disconnects fail fast with
 *   `errorCode: "output_interrupted"` instead of splicing partial streams.
 */
export async function invokeWebRtcOrFallback(options: RtcInvokeOptions): Promise<StreamingInvokeResult> {
  if (options.durability === "durable") {
    throw new Error(
      "WebRTC P2P data-plane bypass is only supported for EPHEMERAL Invoke jobs (durable jobs must use the control-plane relay so outputs can be persisted and replayed).",
    );
  }

  const WsImpl = options.WebSocketCtor ?? WebSocket;
  const PcImpl =
    options.PeerConnectionCtor !== undefined
      ? options.PeerConnectionCtor
      : typeof RTCPeerConnection !== "undefined"
        ? RTCPeerConnection
        : null;
  const iceTimeoutMs = options.iceTimeoutMs ?? DEFAULT_ICE_TIMEOUT_MS;
  const maxAttempts = Math.max(1, options.maxAttempts ?? DEFAULT_RTC_MAX_ATTEMPTS);
  const overallStartedAt = performance.now();
  const timeline: StreamingInvokeTimelineEvent[] = [];
  let totalAttempts = 0;
  let restarted = false;
  let firstAttemptAt: number | null = null;
  let failoverMs: number | null = null;

  const pushTimeline = (kind: StreamingInvokeTimelineEvent["kind"], detail: string) => {
    const ev: StreamingInvokeTimelineEvent = {
      atMs: Math.max(0, Math.round(performance.now() - overallStartedAt)),
      kind,
      detail,
    };
    timeline.push(ev);
    options.onTimeline?.(ev);
  };

  const runRound = (roundNum: number): Promise<RoundOutcome> =>
    new Promise<RoundOutcome>((resolve, reject) => {
      let settled = false;
      let jobId: string | null = null;
      let lastWorkerId: string | null = null;
      let ttftMs: number | null = null;
      let firstDataAt: number | null = null;
      let lastDataAt: number | null = null;
      let combinedText = "";
      let transportActual: "p2p" | "relay" = "relay";
      let fallbackReason: string | null = null;
      let iceMs: number | null = null;
      let p2pBytes = 0;
      let relayedPayloadBytes = 0;
      let framesReceived = 0;
      let inputEnded = false;
      let modeResolved = false;
      let hasExposedOutput = false;
      let p2pCompleted = false;

      let pc: RTCPeerConnection | null = null;
      let dc: RTCDataChannel | null = null;
      let iceTimer: ReturnType<typeof setTimeout> | null = null;
      const pendingRemoteIce: RTCIceCandidateInit[] = [];
      let remoteAnswerSet = false;

      const wsUrl = `${options.wsBaseUrl.replace(/\/+$/, "")}/ws/v2/client/invoke`;
      const ws = new WsImpl(wsUrl, buildWsSubprotocols(options.token));
      ws.binaryType = "arraybuffer";

      const clearIceTimer = () => {
        if (iceTimer !== null) {
          clearTimeout(iceTimer);
          iceTimer = null;
        }
      };

      const closeRtc = () => {
        clearIceTimer();
        try {
          if (dc) {
            dc.onclose = null;
            dc.onmessage = null;
            dc.close();
          }
        } catch {
          // ignore
        }
        dc = null;
        try {
          if (pc) {
            pc.oniceconnectionstatechange = null;
            pc.onicecandidate = null;
            pc.close();
          }
        } catch {
          // ignore
        }
        pc = null;
      };

      const closeWsQuiet = (code = 1000, reason = "done") => {
        try {
          ws.onclose = null;
          ws.onerror = null;
          ws.close(code, reason);
        } catch {
          // ignore
        }
      };

      const sendInputSignal = (sig: ClientRtcSignal, endInput = false) => {
        if (ws.readyState !== 1 || inputEnded) return;
        ws.send(
          pb.encodeInvokeRequest({
            msg: {
              case: "input",
              value: {
                data: encodeUtf8(JSON.stringify(sig)),
                metadata: { [RTC_SIGNAL_META_KEY]: RTC_SIGNAL_META_VALUE },
              },
            },
          }),
        );
        if (endInput) {
          inputEnded = true;
          ws.send(pb.encodeInvokeRequest({ msg: { case: "inputEnd", value: {} } }));
        }
      };

      const triggerFallback = (reason: string) => {
        if (modeResolved || settled) return;
        modeResolved = true;
        transportActual = "relay";
        fallbackReason = reason;
        closeRtc();
        pushTimeline("progress", `WebRTC fallback to WS relay (${reason})`);
        options.onProgress?.(`WebRTC unavailable (${reason}) — falling back to WS relay`);
        sendInputSignal(
          {
            t: "fallback",
            reason,
            prompt: options.prompt,
            ...(options.benchChunks != null ? { benchChunks: options.benchChunks } : {}),
            ...(options.benchChunkBytes != null ? { benchChunkBytes: options.benchChunkBytes } : {}),
          },
          true,
        );
      };

      const computeStreamDurationMs = (): number | null =>
        firstDataAt !== null && lastDataAt !== null ? Math.max(1, Math.round(lastDataAt - firstDataAt)) : null;

      const finishOutcome = (outcome: RoundOutcome) => {
        if (settled) return;
        settled = true;
        closeRtc();
        options.signal?.removeEventListener("abort", onAbort);
        resolve(outcome);
      };

      const finishReject = (err: Error) => {
        if (settled) return;
        settled = true;
        closeRtc();
        options.signal?.removeEventListener("abort", onAbort);
        reject(err);
      };

      const handleP2pDisconnect = (disconnectReason: string) => {
        if (settled || p2pCompleted || transportActual !== "p2p") return;
        closeWsQuiet(1000, disconnectReason);
        if (!hasExposedOutput && roundNum < maxAttempts) {
          pushTimeline(
            "attempt_restarted",
            `WebRTC P2P peer dropped before first output (${disconnectReason}) — re-scheduling attempt #${roundNum + 1}`,
          );
          options.onProgress?.(`WebRTC peer dropped before first output — re-scheduling attempt #${roundNum + 1}…`);
          finishOutcome({ kind: "retry", reason: disconnectReason, jobId, workerId: lastWorkerId });
          return;
        }
        const code = hasExposedOutput ? "output_interrupted" : "worker_lost";
        const message = hasExposedOutput
          ? `output_interrupted: WebRTC P2P peer disconnected after streaming started (${disconnectReason})`
          : `worker_lost: WebRTC P2P peer disconnected before first output (${disconnectReason})`;
        pushTimeline("finished", `failed: ${message}`);
        finishOutcome({
          kind: "done",
          result: {
            jobId,
            state: "failed",
            text: combinedText,
            error: message,
            errorCode: code,
            workerId: lastWorkerId,
            attempts: totalAttempts,
            restarted,
            failoverMs,
            ttftMs,
            totalMs: Math.max(0, Math.round(performance.now() - overallStartedAt)),
            transportActual,
            fallbackReason,
            iceMs,
            p2pBytes,
            relayedPayloadBytes,
            streamDurationMs: computeStreamDurationMs(),
            framesReceived,
            timeline,
          },
        });
      };

      const onAbort = () => {
        closeWsQuiet(1000, "cancelled by caller");
        pushTimeline("finished", "cancelled by caller");
        finishOutcome({
          kind: "done",
          result: {
            jobId,
            state: "cancelled",
            text: combinedText,
            error: "cancelled by caller",
            errorCode: "cancelled",
            workerId: lastWorkerId,
            attempts: totalAttempts,
            restarted,
            failoverMs,
            ttftMs,
            totalMs: Math.max(0, Math.round(performance.now() - overallStartedAt)),
            transportActual,
            fallbackReason,
            iceMs,
            p2pBytes,
            relayedPayloadBytes,
            streamDurationMs: computeStreamDurationMs(),
            framesReceived,
            timeline,
          },
        });
      };

      if (options.signal) {
        if (options.signal.aborted) {
          onAbort();
          return;
        }
        options.signal.addEventListener("abort", onAbort, { once: true });
      }

      const startRtcNegotiation = async () => {
        if (!PcImpl) {
          triggerFallback("webrtc_unsupported");
          return;
        }
        const iceStart = performance.now();
        try {
          pc = new PcImpl({ iceServers: options.iceServers ?? DEFAULT_RTC_ICE_SERVERS });
          dc = pc.createDataChannel("anyserve-p2p", { ordered: true });
          dc.binaryType = "arraybuffer";

          iceTimer = setTimeout(() => {
            triggerFallback("ice_timeout");
          }, iceTimeoutMs);

          pc.onicecandidate = (ev) => {
            if (settled || modeResolved) return;
            if (ev.candidate) {
              sendInputSignal({
                t: "ice",
                candidate: typeof ev.candidate.toJSON === "function" ? ev.candidate.toJSON() : ev.candidate,
              });
            } else {
              sendInputSignal({ t: "ice_end" });
            }
          };

          pc.oniceconnectionstatechange = () => {
            const st = pc?.iceConnectionState;
            if (st === "failed" || st === "disconnected" || st === "closed") {
              if (!modeResolved) {
                triggerFallback(`ice_${st}`);
              } else if (transportActual === "p2p" && !p2pCompleted) {
                handleP2pDisconnect(`ice_${st}`);
              }
            }
          };

          dc.onopen = () => {
            if (modeResolved || settled) return;
            modeResolved = true;
            clearIceTimer();
            transportActual = "p2p";
            iceMs = Math.max(1, Math.round(performance.now() - iceStart));
            pushTimeline("progress", `WebRTC P2P DataChannel open (${iceMs} ms) — 0 B payload relayed`);
            options.onProgress?.(`WebRTC P2P connected (${iceMs} ms) — streaming directly peer-to-peer`);
            sendInputSignal({ t: "p2p_ready" }, true);
            const req: DataChannelClientRequest = {
              t: "req",
              prompt: options.prompt,
              ...(options.benchChunks != null ? { benchChunks: options.benchChunks } : {}),
              ...(options.benchChunkBytes != null ? { benchChunkBytes: options.benchChunkBytes } : {}),
            };
            dc?.send(JSON.stringify(req));
          };

          dc.onclose = () => {
            if (transportActual === "p2p" && !p2pCompleted && !settled) {
              handleP2pDisconnect("datachannel_closed");
            }
          };

          dc.onmessage = (msgEv) => {
            if (settled) return;
            const now = performance.now();
            if (msgEv.data instanceof ArrayBuffer || msgEv.data instanceof Uint8Array) {
              const bytes = msgEv.data instanceof Uint8Array ? msgEv.data : new Uint8Array(msgEv.data);
              hasExposedOutput = true;
              if (ttftMs === null) {
                ttftMs = Math.max(0, Math.round(now - overallStartedAt));
                firstDataAt = now;
                pushTimeline("output", `first P2P chunk at +${ttftMs} ms`);
              }
              lastDataAt = now;
              p2pBytes += bytes.byteLength;
              framesReceived += 1;
              options.onBinaryChunk?.(bytes);
              return;
            }
            if (typeof msgEv.data !== "string") return;
            try {
              const msg = JSON.parse(msgEv.data) as DataChannelWorkerMsg;
              if (msg.t === "progress") {
                pushTimeline("progress", msg.text);
                options.onProgress?.(msg.text);
              } else if (msg.t === "tok") {
                hasExposedOutput = true;
                if (ttftMs === null) {
                  ttftMs = Math.max(0, Math.round(now - overallStartedAt));
                  firstDataAt = now;
                  pushTimeline("output", `first P2P token at +${ttftMs} ms`);
                }
                lastDataAt = now;
                p2pBytes += encodeUtf8(msg.text).byteLength;
                framesReceived += 1;
                combinedText += msg.text;
                options.onChunk?.(msg.text);
              } else if (msg.t === "done") {
                p2pCompleted = true;
                if (msg.p2pBytes > p2pBytes) p2pBytes = msg.p2pBytes;
                pushTimeline("progress", `P2P stream complete (${p2pBytes} B direct, 0 B relayed)`);
                if (dc && dc.readyState === "open") {
                  try {
                    dc.send(JSON.stringify({ t: "ack" } satisfies DataChannelClientMsg));
                  } catch {
                    // ignore ack send failure if channel already closing
                  }
                }
              } else if (msg.t === "err") {
                pushTimeline("error", msg.message);
              }
            } catch {
              // ignore malformed JSON
            }
          };

          const offer = await pc.createOffer();
          await pc.setLocalDescription(offer);
          if (!offer.sdp) {
            triggerFallback("empty_sdp_offer");
            return;
          }
          sendInputSignal({ t: "offer", kind: "webrtc", sdp: offer.sdp });
        } catch (err) {
          triggerFallback(err instanceof Error ? err.message : "rtc_init_failed");
        }
      };

      ws.onopen = () => {
        const startFrame = pb.encodeInvokeRequest({
          msg: {
            case: "start",
            value: {
              spec: {
                demand: {
                  interface: options.interfaceName,
                  preferredAttributes: options.preferredAttributes ?? {},
                },
                inputs: [],
                metadata: {
                  source: "console.playground",
                  "anyserve.transport": "webrtc",
                  ...(options.metadata ?? {}),
                },
                policy: {
                  durability: pb.Durability.EPHEMERAL,
                  maxAttempts: 1,
                  queueTimeoutMs: 15_000n,
                  executionTimeoutMs: 120_000n,
                },
              },
              streamingInput: true,
            },
          },
        });
        ws.send(startFrame);
        void startRtcNegotiation();
      };

      ws.onmessage = (ev: MessageEvent) => {
        if (settled) return;
        if (!(ev.data instanceof ArrayBuffer) && !(ev.data instanceof Uint8Array)) return;
        let decoded;
        try {
          decoded = decodeWsPayload(ev.data, pb.decodeExecutionEvent);
        } catch (err) {
          finishReject(err instanceof Error ? err : new Error(String(err)));
          return;
        }
        if (decoded.kind === "error") {
          pushTimeline("error", `gRPC ${decoded.code}: ${decoded.message}`);
          finishReject(new Error(decoded.message || `invoke failed (code ${decoded.code})`));
          return;
        }
        const event = decoded.value;
        if (!jobId && event.jobId) {
          jobId = event.jobId;
          options.onJobAccepted?.(jobId);
        }
        const kind = event.event;
        if (!kind) return;

        switch (kind.case) {
          case "accepted":
            pushTimeline("accepted", `job ${event.jobId}`);
            break;
          case "assigned":
            totalAttempts += 1;
            lastWorkerId = kind.value.workerId;
            if (totalAttempts === 1) {
              firstAttemptAt = performance.now();
            } else if (totalAttempts === 2 && firstAttemptAt !== null && failoverMs === null) {
              failoverMs = Math.max(0, Math.round(performance.now() - firstAttemptAt));
            }
            pushTimeline("assigned", `attempt #${totalAttempts} -> ${kind.value.workerId}`);
            break;
          case "attemptRestarted":
            restarted = true;
            pushTimeline("attempt_restarted", kind.value.reason?.message || "attempt restarted");
            break;
          case "output": {
            if (kind.value.progress && kind.value.metadata[RTC_SIGNAL_META_KEY] === RTC_SIGNAL_META_VALUE) {
              try {
                const sig = JSON.parse(decodeUtf8(kind.value.data)) as WorkerRtcSignal;
                if (sig.t === "answer" && pc && !modeResolved) {
                  void pc
                    .setRemoteDescription({ type: "answer", sdp: sig.sdp })
                    .then(async () => {
                      remoteAnswerSet = true;
                      for (const cand of pendingRemoteIce.splice(0)) {
                        await pc?.addIceCandidate(cand);
                      }
                    })
                    .catch(() => triggerFallback("invalid_answer"));
                } else if (sig.t === "ice" && pc && !modeResolved) {
                  if (remoteAnswerSet) {
                    void pc.addIceCandidate(sig.candidate).catch(() => {});
                  } else {
                    pendingRemoteIce.push(sig.candidate);
                  }
                } else if (sig.t === "rtc_error") {
                  triggerFallback(sig.reason || "worker_rtc_error");
                }
              } catch {
                // ignore malformed signaling frame
              }
              break;
            }
            const now = performance.now();
            if (kind.value.progress) {
              const msg = decodeUtf8(kind.value.data);
              pushTimeline("progress", msg);
              options.onProgress?.(msg);
            } else {
              hasExposedOutput = true;
              if (ttftMs === null) {
                ttftMs = Math.max(0, Math.round(now - overallStartedAt));
                firstDataAt = now;
                pushTimeline("output", `first relayed chunk at +${ttftMs} ms`);
              }
              lastDataAt = now;
              relayedPayloadBytes += kind.value.data.byteLength;
              framesReceived += 1;
              if (kind.value.metadata["content_type"] === "application/octet-stream") {
                options.onBinaryChunk?.(kind.value.data);
              } else {
                const text = decodeUtf8(kind.value.data);
                combinedText += text;
                options.onChunk?.(text);
              }
            }
            break;
          }
          case "finished": {
            const st = kind.value.state;
            const state: StreamingInvokeResult["state"] =
              st === pb.JobState.SUCCEEDED
                ? "succeeded"
                : st === pb.JobState.CANCELLED
                  ? "cancelled"
                  : "failed";
            const rawErrMsg = kind.value.error?.message || null;
            const rawErrCode = kind.value.error?.code || null;
            closeWsQuiet(1000, "done");

            if (state === "failed" && !hasExposedOutput && roundNum < maxAttempts) {
              pushTimeline(
                "attempt_restarted",
                `${rawErrMsg || rawErrCode || "attempt failed"} before first output — re-scheduling attempt #${roundNum + 1}`,
              );
              finishOutcome({
                kind: "retry",
                reason: rawErrCode || "attempt_failed",
                jobId,
                workerId: lastWorkerId,
              });
              return;
            }

            const effectiveCode =
              state === "failed" && hasExposedOutput && !p2pCompleted ? "output_interrupted" : rawErrCode;
            const effectiveMsg =
              state === "failed" && hasExposedOutput && !p2pCompleted
                ? rawErrMsg || "output_interrupted: stream ended before completion"
                : rawErrMsg;

            pushTimeline("finished", effectiveMsg ? `${state}: ${effectiveMsg}` : `${state} (${transportActual})`);
            finishOutcome({
              kind: "done",
              result: {
                jobId,
                state,
                text: combinedText,
                error: effectiveMsg,
                errorCode: effectiveCode,
                workerId: lastWorkerId,
                attempts: totalAttempts,
                restarted,
                failoverMs,
                ttftMs,
                totalMs: Math.max(0, Math.round(performance.now() - overallStartedAt)),
                transportActual,
                fallbackReason,
                iceMs,
                p2pBytes,
                relayedPayloadBytes,
                streamDurationMs: computeStreamDurationMs(),
                framesReceived,
                timeline,
              },
            });
            break;
          }
        }
      };

      ws.onerror = () => {
        if (!settled) {
          finishReject(new Error("WebSocket connection to /ws/v2/client/invoke failed"));
        }
      };

      ws.onclose = (ev: CloseEvent) => {
        if (!settled) {
          if (transportActual === "p2p" && !p2pCompleted) {
            handleP2pDisconnect(`ws_closed_${ev.code}`);
            return;
          }
          finishReject(
            new Error(
              ev.reason
                ? `Invoke stream closed (${ev.code}): ${ev.reason}`
                : `Invoke stream closed before completion (${ev.code})`,
            ),
          );
        }
      };
    });

  for (let round = 1; round <= maxAttempts; round += 1) {
    const outcome = await runRound(round);
    if (outcome.kind === "done") {
      return outcome.result;
    }
    restarted = true;
  }

  throw new Error("WebRTC invoke exhausted retry attempts");
}
