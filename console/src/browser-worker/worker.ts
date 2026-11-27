/// <reference lib="webworker" />

import {
  ECHO_INTERFACE,
  LLM_INTERFACE,
  SMOLLM_MODEL_ID,
  buildWsSubprotocols,
  concatBytes,
  decodeUtf8,
  decodeWsPayload,
  encodeUtf8,
  pb,
} from "./protocol";
import {
  RTC_SIGNAL_META_KEY,
  RTC_SIGNAL_META_VALUE,
  type ClientRtcSignal,
  type DataChannelClientRequest,
  type DataChannelWorkerMsg,
  type WorkerRtcSignal,
} from "./rtc";

const TRANSFORMERS_CDN_URL = "https://cdn.jsdelivr.net/npm/@huggingface/transformers@3/+esm";

export type WorkerSlotRole = "primary" | "standby";
export type ComputeBackend = "echo-only" | "webgpu" | "wasm";

export type HostToWorkerCmd =
  | {
      type: "start";
      wsBaseUrl: string;
      workerId: string;
      role: WorkerSlotRole;
      token: string | null;
    }
  | { type: "load_model" }
  | { type: "stop" }
  | { type: "rtc_signal_out"; leaseId: string; signal: WorkerRtcSignal }
  | { type: "rtc_req"; leaseId: string; req: DataChannelClientRequest; iceMs: number }
  | { type: "rtc_ack"; leaseId: string }
  | { type: "rtc_disconnected"; leaseId: string };

export type WorkerToHostEvent =
  | {
      type: "state";
      status: "connecting" | "online" | "busy" | "stopped" | "error";
      sessionId: string | null;
      interfaces: string[];
      activeJobId: string | null;
      jobsCompleted: number;
      backend: ComputeBackend;
      error: string | null;
    }
  | {
      type: "model";
      status: "idle" | "loading" | "ready" | "error";
      backend: ComputeBackend;
      progress: number;
      file: string | null;
      error: string | null;
    }
  | { type: "rtc_signal_in"; leaseId: string; signal: ClientRtcSignal }
  | { type: "rtc_send"; leaseId: string; payload: DataChannelWorkerMsg | Uint8Array }
  | { type: "rtc_close"; leaseId: string }
  | {
      type: "log";
      message: string;
    };

interface LoadedPipeline {
  backend: "webgpu" | "wasm";
  generator: (
    messages: Array<{ role: string; content: string }>,
    options: Record<string, unknown>,
  ) => Promise<unknown>;
  TextStreamerCtor: new (
    tokenizer: unknown,
    options: { skip_prompt?: boolean; skip_special_tokens?: boolean; callback_function?: (text: string) => void },
  ) => unknown;
  tokenizer: unknown;
}

const ctx: DedicatedWorkerGlobalScope = self as unknown as DedicatedWorkerGlobalScope;

let wsBaseUrl = "";
let workerId = "browser-worker-1";
let workerRole: WorkerSlotRole = "primary";
let workerToken: string | null = null;

let sessionWs: WebSocket | null = null;
let sessionId: string | null = null;
let heartbeatTimer: ReturnType<typeof setInterval> | null = null;
let stopping = false;
let jobsCompleted = 0;
let activeJobId: string | null = null;
let loadedModel: LoadedPipeline | null = null;
let modelLoading = false;
let currentBackend: ComputeBackend = "echo-only";
const activeAborts = new Map<string, AbortController>();
const rtcHandlers = new Map<
  string,
  {
    onSignalOut: (sig: WorkerRtcSignal) => void;
    onReq: (req: DataChannelClientRequest, iceMs: number) => void;
    onAck: () => void;
    onDisconnected: () => void;
  }
>();

function emit(ev: WorkerToHostEvent): void {
  ctx.postMessage(ev);
}

function activeInterfaces(): string[] {
  return loadedModel ? [ECHO_INTERFACE, LLM_INTERFACE] : [ECHO_INTERFACE];
}

function emitState(
  status: "connecting" | "online" | "busy" | "stopped" | "error",
  error: string | null = null,
): void {
  emit({
    type: "state",
    status,
    sessionId,
    interfaces: activeInterfaces(),
    activeJobId,
    jobsCompleted,
    backend: currentBackend,
    error,
  });
}

function clearHeartbeat(): void {
  if (heartbeatTimer !== null) {
    clearInterval(heartbeatTimer);
    heartbeatTimer = null;
  }
}

function sendControl(msg: pb.WorkerMessageInput): void {
  if (sessionWs && sessionWs.readyState === WebSocket.OPEN) {
    sessionWs.send(pb.encodeWorkerMessage(msg));
  }
}

function closeSession(nextStatus: "stopped" | "error", reason: string | null = null): void {
  clearHeartbeat();
  for (const ctrl of activeAborts.values()) {
    ctrl.abort();
  }
  activeAborts.clear();
  activeJobId = null;
  const ws = sessionWs;
  sessionWs = null;
  sessionId = null;
  if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) {
    try {
      ws.close(1000, reason ?? "stopped");
    } catch {
      // ignore
    }
  }
  emitState(nextStatus, nextStatus === "error" ? reason : null);
}

function connectSession(): void {
  clearHeartbeat();
  if (sessionWs) {
    const old = sessionWs;
    sessionWs = null;
    try {
      old.close(1000, "reconnecting");
    } catch {
      // ignore
    }
  }
  stopping = false;
  emitState("connecting");

  const url = `${wsBaseUrl.replace(/\/+$/, "")}/ws/v2/worker/session`;
  const ws = new WebSocket(url, buildWsSubprotocols(workerToken));
  ws.binaryType = "arraybuffer";
  sessionWs = ws;

  ws.onopen = () => {
    if (sessionWs !== ws) return;
    const ifaces = activeInterfaces();
    sendControl({
      msg: {
        case: "hello",
        value: {
          workerId,
          pool: "browser",
          interfaces: ifaces,
          maxConcurrency: 1,
          attributes: {
            runtime: "browser",
            role: workerRole,
            backend: currentBackend,
          },
        },
      },
    });
  };

  ws.onmessage = (ev: MessageEvent) => {
    if (sessionWs !== ws) return;
    if (!(ev.data instanceof ArrayBuffer) && !(ev.data instanceof Uint8Array)) return;
    let decoded;
    try {
      decoded = decodeWsPayload(ev.data, pb.decodeServerMessage);
    } catch (err) {
      emit({ type: "log", message: `Failed to decode ServerMessage: ${String(err)}` });
      return;
    }
    if (decoded.kind === "error") {
      emit({ type: "log", message: `Session error (${decoded.code}): ${decoded.message}` });
      closeSession("error", decoded.message || `Session error (${decoded.code})`);
      return;
    }
    const msg = decoded.value.msg;
    if (!msg) return;

    switch (msg.case) {
      case "welcome": {
        sessionId = msg.value.sessionId;
        const intervalMs = Math.max(1_000, Math.min(msg.value.heartbeatIntervalMs || 5_000, 30_000));
        clearHeartbeat();
        heartbeatTimer = setInterval(() => {
          sendControl({
            msg: {
              case: "heartbeat",
              value: {
                load: {
                  queued: 0,
                  running: activeJobId ? 1 : 0,
                  utilization: activeJobId ? 1 : 0,
                  custom: {},
                },
              },
            },
          });
        }, intervalMs);
        sendControl({ msg: { case: "credit", value: { freeSlots: 1 } } });
        emitState("online");
        emit({ type: "log", message: `Registered ${workerId} (${sessionId}) for [${activeInterfaces().join(", ")}]` });
        break;
      }
      case "assign": {
        void handleAssign(msg.value);
        break;
      }
      case "cancel": {
        const ctrl = activeAborts.get(msg.value.leaseId);
        ctrl?.abort();
        sendControl({ msg: { case: "cancelAck", value: { leaseId: msg.value.leaseId } } });
        break;
      }
      case "fence": {
        const ctrl = activeAborts.get(msg.value.leaseId);
        ctrl?.abort();
        emit({ type: "log", message: `Lease ${msg.value.leaseId} fenced: ${msg.value.reason}` });
        break;
      }
      case "drain": {
        emit({ type: "log", message: "Received drain signal from control plane" });
        break;
      }
    }
  };

  ws.onerror = () => {
    if (sessionWs !== ws || stopping) return;
    closeSession("error", "WebSocket session connection failed (check worker token & server)");
  };

  ws.onclose = (ev: CloseEvent) => {
    if (sessionWs !== ws) return;
    clearHeartbeat();
    sessionWs = null;
    sessionId = null;
    if (!stopping) {
      emitState("error", ev.reason || `Session closed (${ev.code})`);
    }
  };
}

function sleepMs(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal.aborted) {
      reject(new Error("aborted"));
      return;
    }
    const id = setTimeout(() => {
      signal.removeEventListener("abort", onAbort);
      resolve();
    }, ms);
    const onAbort = () => {
      clearTimeout(id);
      reject(new Error("aborted"));
    };
    signal.addEventListener("abort", onAbort, { once: true });
  });
}

async function handleAssign(assign: pb.Assign): Promise<void> {
  sendControl({
    msg: {
      case: "assignReply",
      value: {
        leaseId: assign.leaseId,
        accepted: true,
        rejectReason: "",
      },
    },
  });

  const abortCtrl = new AbortController();
  activeAborts.set(assign.leaseId, abortCtrl);
  activeJobId = assign.jobId;
  emitState("busy");

  const attemptUrl = `${wsBaseUrl.replace(/\/+$/, "")}/ws/v2/worker/attempt`;
  const attemptWs = new WebSocket(attemptUrl, buildWsSubprotocols(workerToken));
  attemptWs.binaryType = "arraybuffer";

  try {
    const wantsWebRtc = assign.metadata["anyserve.transport"] === "webrtc";
    if (wantsWebRtc && assign.streamingInput && assign.metadata["anyserve.durability"] !== "durable") {
      await handleWebRtcAttempt(attemptWs, assign, abortCtrl.signal);
      return;
    }

    const inputBytes = await openAttemptAndReadInputs(attemptWs, assign, abortCtrl.signal);
    const prompt = decodeUtf8(inputBytes).trim() || "Hello from AnyServe browser worker!";
    const handlerStart = performance.now();

    let nextSeq = 1n;
    const sendFrame = (data: Uint8Array, progress: boolean, contentType = "text/plain; charset=utf-8") => {
      if (attemptWs.readyState !== WebSocket.OPEN) return;
      attemptWs.send(
        pb.encodeAttemptUp({
          msg: {
            case: "frame",
            value: {
              sequence: nextSeq,
              data,
              metadata: { content_type: contentType },
              progress,
            },
          },
        }),
      );
      nextSeq += 1n;
    };

    if (wantsWebRtc) {
      // Optional WebRTC bypass is strictly EPHEMERAL streaming Invoke only;
      // non-streaming Submit or durable requests transparently fall back to relay.
      sendFrame(
        encodeUtf8("WebRTC P2P requires EPHEMERAL streaming Invoke; falling back to WS relay"),
        true,
      );
    }

    const prefillMs = Math.min(5_000, Math.max(0, Number(assign.metadata["demo.prefill_ms"] ?? "0") || 0));
    if (prefillMs > 0) {
      // Progress frames do not set attempt.exposed=true, mirroring real prefill work
      // before the first output token is emitted.
      sendFrame(encodeUtf8(`prefilling on ${workerId} (${workerRole})...`), true);
      await sleepMs(prefillMs, abortCtrl.signal);
    }

    const benchChunks = Math.min(2_048, Math.max(0, Number(assign.metadata["demo.bench_chunks"] ?? "0") || 0));
    const benchChunkBytes = Math.min(
      65_536,
      Math.max(1_024, Number(assign.metadata["demo.bench_chunk_bytes"] ?? "16384") || 16_384),
    );
    await executePayloadHandler({
      interfaceName: assign.interface,
      prompt,
      benchChunks,
      benchChunkBytes,
      tokenDelayMs: Math.min(200, Math.max(0, Number(assign.metadata["demo.token_delay_ms"] ?? "28") || 28)),
      signal: abortCtrl.signal,
      onProgress: (msg) => sendFrame(encodeUtf8(msg), true),
      onTextToken: (tok) => sendFrame(encodeUtf8(tok), false),
      onBinaryChunk: (chunk) => sendFrame(chunk, false, "application/octet-stream"),
    });

    const handlerDurationMs = BigInt(Math.max(0, Math.round(performance.now() - handlerStart)));
    if (abortCtrl.signal.aborted) {
      await sendFinishAndWait(attemptWs, {
        outcome: { case: "cancelled", value: {} },
        handlerDurationMs,
      });
    } else {
      await sendFinishAndWait(attemptWs, {
        outcome: { case: "succeeded", value: {} },
        handlerDurationMs,
      });
      jobsCompleted += 1;
    }
  } catch (err) {
    if (attemptWs.readyState === WebSocket.OPEN) {
      try {
        await sendFinishAndWait(attemptWs, {
          outcome: abortCtrl.signal.aborted
            ? { case: "cancelled", value: {} }
            : {
                case: "failed",
                value: {
                  code: "worker_failed",
                  message: err instanceof Error ? err.message : String(err),
                  retryable: false,
                },
              },
        });
      } catch {
        // ignore finish error on failed attempt
      }
    }
  } finally {
    activeAborts.delete(assign.leaseId);
    try {
      if (attemptWs.readyState === WebSocket.OPEN || attemptWs.readyState === WebSocket.CONNECTING) {
        attemptWs.close(1000, "attempt done");
      }
    } catch {
      // ignore
    }
    activeJobId = null;
    if (sessionWs && sessionWs.readyState === WebSocket.OPEN) {
      sendControl({ msg: { case: "credit", value: { freeSlots: 1 } } });
      emitState("online");
    }
  }
}

async function executePayloadHandler(opts: {
  interfaceName: string;
  prompt: string;
  benchChunks: number;
  benchChunkBytes: number;
  tokenDelayMs: number;
  signal: AbortSignal;
  onProgress: (msg: string) => void;
  onTextToken: (tok: string) => void;
  onBinaryChunk: (chunk: Uint8Array) => void;
}): Promise<{ bytes: number; frames: number }> {
  let bytes = 0;
  let frames = 0;
  if (opts.benchChunks > 0) {
    const chunk = new Uint8Array(opts.benchChunkBytes);
    for (let i = 0; i < chunk.byteLength; i++) chunk[i] = i & 0xff;
    for (let c = 0; c < opts.benchChunks; c++) {
      if (opts.signal.aborted) break;
      opts.onBinaryChunk(chunk);
      bytes += chunk.byteLength;
      frames += 1;
    }
    return { bytes, frames };
  }
  if (opts.interfaceName === LLM_INTERFACE) {
    if (!loadedModel) {
      throw new Error("SmolLM2-135M model is not loaded on this browser worker");
    }
    opts.onProgress(`running ${SMOLLM_MODEL_ID.split("/").pop()} on ${loadedModel.backend.toUpperCase()} (${workerId})...`);
    const streamer = new loadedModel.TextStreamerCtor(loadedModel.tokenizer, {
      skip_prompt: true,
      skip_special_tokens: true,
      callback_function: (tokenText: string) => {
        if (!opts.signal.aborted && tokenText) {
          bytes += encodeUtf8(tokenText).byteLength;
          frames += 1;
          opts.onTextToken(tokenText);
        }
      },
    });
    await loadedModel.generator(
      [
        {
          role: "system",
          content: "You are a concise assistant running inside a browser Web Worker scheduled by AnyServe.",
        },
        { role: "user", content: opts.prompt },
      ],
      {
        max_new_tokens: 96,
        do_sample: false,
        streamer,
      },
    );
    return { bytes, frames };
  }
  const reply = `[${workerId} · ${workerRole}] ${opts.prompt}`;
  const tokens = reply.split(/(\s+)/).filter((t) => t.length > 0);
  for (const tok of tokens) {
    if (opts.signal.aborted) break;
    bytes += encodeUtf8(tok).byteLength;
    frames += 1;
    opts.onTextToken(tok);
    if (opts.tokenDelayMs > 0) {
      await sleepMs(opts.tokenDelayMs, opts.signal);
    }
  }
  return { bytes, frames };
}

async function handleWebRtcAttempt(
  attemptWs: WebSocket,
  assign: pb.Assign,
  signal: AbortSignal,
): Promise<void> {
  const leaseId = assign.leaseId;
  let nextSeq = 1n;
  const sendAttemptFrame = (
    data: Uint8Array,
    progress: boolean,
    metadata: Record<string, string>,
  ) => {
    if (attemptWs.readyState !== WebSocket.OPEN) return;
    attemptWs.send(
      pb.encodeAttemptUp({
        msg: {
          case: "frame",
          value: { sequence: nextSeq, data, metadata, progress },
        },
      }),
    );
    nextSeq += 1n;
  };

  type ModeDecision =
    | { mode: "p2p"; req: DataChannelClientRequest; iceMs: number }
    | { mode: "fallback"; reason: string; prompt: string; benchChunks: number; benchChunkBytes: number };

  let resolveDecision!: (d: ModeDecision) => void;
  let rejectDecision!: (err: Error) => void;
  let decided = false;
  const decisionPromise = new Promise<ModeDecision>((res, rej) => {
    resolveDecision = (d) => {
      if (decided) return;
      decided = true;
      res(d);
    };
    rejectDecision = (e) => {
      if (decided) return;
      decided = true;
      rej(e);
    };
  });

  let resolveAck!: () => void;
  let acked = false;
  const ackPromise = new Promise<void>((res) => {
    resolveAck = () => {
      if (acked) return;
      acked = true;
      res();
    };
  });

  const onAbort = () => {
    resolveAck();
    rejectDecision(new Error("cancelled"));
  };
  if (signal.aborted) {
    throw new Error("cancelled");
  }
  signal.addEventListener("abort", onAbort, { once: true });

  rtcHandlers.set(leaseId, {
    onSignalOut: (sig) => {
      sendAttemptFrame(encodeUtf8(JSON.stringify(sig)), true, {
        [RTC_SIGNAL_META_KEY]: RTC_SIGNAL_META_VALUE,
      });
    },
    onReq: (req, iceMs) => {
      resolveDecision({ mode: "p2p", req, iceMs });
    },
    onAck: () => {
      resolveAck();
    },
    onDisconnected: () => {
      resolveAck();
    },
  });

  // Incremental signaling reader over the live /ws/v2/worker/attempt stream.
  const objBuffers = new Map<number, Uint8Array[]>();
  attemptWs.onopen = () => {
    attemptWs.send(
      pb.encodeAttemptUp({
        msg: {
          case: "open",
          value: {
            leaseId,
            generation: assign.generation,
            resumeAfterSequence: 0n,
          },
        },
      }),
    );
  };
  attemptWs.onmessage = (ev: MessageEvent) => {
    if (!(ev.data instanceof ArrayBuffer) && !(ev.data instanceof Uint8Array)) return;
    let decoded;
    try {
      decoded = decodeWsPayload(ev.data, pb.decodeAttemptDown);
    } catch (err) {
      rejectDecision(err instanceof Error ? err : new Error(String(err)));
      return;
    }
    if (decoded.kind === "error") {
      rejectDecision(new Error(decoded.message || `attempt error ${decoded.code}`));
      return;
    }
    const msg = decoded.value.msg;
    if (!msg) return;
    if (msg.case === "input") {
      if (!msg.value.parameterChunk && msg.value.objectIndex !== undefined) {
        const idx = msg.value.objectIndex;
        const list = objBuffers.get(idx) ?? [];
        if (msg.value.data.byteLength > 0) list.push(msg.value.data);
        objBuffers.set(idx, list);
        if (msg.value.objectEnd) {
          objBuffers.delete(idx);
          const full = concatBytes(list);
          try {
            const sig = JSON.parse(decodeUtf8(full)) as ClientRtcSignal;
            if (sig.t === "fallback") {
              resolveDecision({
                mode: "fallback",
                reason: sig.reason,
                prompt: sig.prompt,
                benchChunks: Math.min(2_048, Math.max(0, sig.benchChunks ?? 0)),
                benchChunkBytes: Math.min(65_536, Math.max(1_024, sig.benchChunkBytes ?? 16_384)),
              });
            } else if (sig.t === "offer" || sig.t === "ice" || sig.t === "ice_end") {
              emit({ type: "rtc_signal_in", leaseId, signal: sig });
            }
          } catch {
            // ignore non-JSON chunk
          }
        }
      }
    } else if (msg.case === "finished") {
      rejectDecision(new Error(msg.value.reason || "attempt finished early"));
    }
  };
  attemptWs.onerror = () => rejectDecision(new Error("attempt WebSocket failed"));
  attemptWs.onclose = () => rejectDecision(new Error("attempt WebSocket closed before signaling finished"));

  try {
    const decision = await decisionPromise;
    const handlerStart = performance.now();

    if (decision.mode === "p2p") {
      const benchChunks = Math.min(2_048, Math.max(0, decision.req.benchChunks ?? 0));
      const benchChunkBytes = Math.min(65_536, Math.max(1_024, decision.req.benchChunkBytes ?? 16_384));
      const stats = await executePayloadHandler({
        interfaceName: assign.interface,
        prompt: decision.req.prompt.trim() || "Hello over WebRTC P2P!",
        benchChunks,
        benchChunkBytes,
        tokenDelayMs: benchChunks > 0 ? 0 : 28,
        signal,
        onProgress: (text) => emit({ type: "rtc_send", leaseId, payload: { t: "progress", text } }),
        onTextToken: (text) => emit({ type: "rtc_send", leaseId, payload: { t: "tok", text } }),
        onBinaryChunk: (chunk) => emit({ type: "rtc_send", leaseId, payload: chunk }),
      });
      const handlerMs = Math.max(0, Math.round(performance.now() - handlerStart));
      emit({
        type: "rtc_send",
        leaseId,
        payload: { t: "done", handlerMs, p2pBytes: stats.bytes, frames: stats.frames },
      });

      // Wait for peer DataChannel ack so every P2P frame is guaranteed
      // delivered before ExecutionEvent::finished tears down the channel.
      await Promise.race([
        ackPromise,
        new Promise<void>((res) => setTimeout(res, 30_000)),
      ]);

      // Report lightweight transport & byte observability metadata to control plane (zero proto change).
      const summary = JSON.stringify({
        transport: "p2p",
        p2p_bytes: stats.bytes,
        relayed_payload_bytes: 0,
        ice_ms: decision.iceMs,
        frames: stats.frames,
      });
      if (attemptWs.readyState === WebSocket.OPEN) {
        attemptWs.send(
          pb.encodeAttemptUp({
            msg: {
              case: "result",
              value: {
                index: 0,
                data: encodeUtf8(summary),
                last: true,
                metadata: {
                  content_type: "application/json",
                  "anyserve.transport_actual": "p2p",
                  "anyserve.p2p_bytes": String(stats.bytes),
                  "anyserve.relayed_payload_bytes": "0",
                  "anyserve.ice_ms": String(decision.iceMs),
                },
              },
            },
          }),
        );
      }
      await sendFinishAndWait(attemptWs, {
        outcome: signal.aborted ? { case: "cancelled", value: {} } : { case: "succeeded", value: {} },
        handlerDurationMs: BigInt(handlerMs),
      });
      if (!signal.aborted) jobsCompleted += 1;
    } else {
      emit({ type: "rtc_close", leaseId });
      const stats = await executePayloadHandler({
        interfaceName: assign.interface,
        prompt: decision.prompt.trim() || "Hello over AnyServe relay fallback!",
        benchChunks: decision.benchChunks,
        benchChunkBytes: decision.benchChunkBytes,
        tokenDelayMs: decision.benchChunks > 0 ? 0 : 28,
        signal,
        onProgress: (text) => sendAttemptFrame(encodeUtf8(text), true, { content_type: "text/plain; charset=utf-8" }),
        onTextToken: (tok) => sendAttemptFrame(encodeUtf8(tok), false, { content_type: "text/plain; charset=utf-8" }),
        onBinaryChunk: (chunk) => sendAttemptFrame(chunk, false, { content_type: "application/octet-stream" }),
      });
      const handlerMs = Math.max(0, Math.round(performance.now() - handlerStart));
      const summary = JSON.stringify({
        transport: "relay",
        fallback_reason: decision.reason,
        relayed_payload_bytes: stats.bytes,
        frames: stats.frames,
      });
      if (attemptWs.readyState === WebSocket.OPEN) {
        attemptWs.send(
          pb.encodeAttemptUp({
            msg: {
              case: "result",
              value: {
                index: 0,
                data: encodeUtf8(summary),
                last: true,
                metadata: {
                  content_type: "application/json",
                  "anyserve.transport_actual": "relay",
                  "anyserve.fallback_reason": decision.reason,
                  "anyserve.relayed_payload_bytes": String(stats.bytes),
                },
              },
            },
          }),
        );
      }
      await sendFinishAndWait(attemptWs, {
        outcome: signal.aborted ? { case: "cancelled", value: {} } : { case: "succeeded", value: {} },
        handlerDurationMs: BigInt(handlerMs),
      });
      if (!signal.aborted) jobsCompleted += 1;
    }
  } finally {
    signal.removeEventListener("abort", onAbort);
    rtcHandlers.delete(leaseId);
    emit({ type: "rtc_close", leaseId });
  }
}

function openAttemptAndReadInputs(
  ws: WebSocket,
  assign: pb.Assign,
  signal: AbortSignal,
): Promise<Uint8Array> {
  return new Promise((resolve, reject) => {
    const chunks: Uint8Array[] = [];
    let opened = false;
    let settled = false;

    const done = (fn: () => void) => {
      if (settled) return;
      settled = true;
      signal.removeEventListener("abort", onAbort);
      ws.onmessage = null;
      ws.onerror = null;
      ws.onclose = null;
      fn();
    };

    const onAbort = () => done(() => reject(new Error("cancelled")));
    if (signal.aborted) {
      reject(new Error("cancelled"));
      return;
    }
    signal.addEventListener("abort", onAbort, { once: true });

    ws.onopen = () => {
      ws.send(
        pb.encodeAttemptUp({
          msg: {
            case: "open",
            value: {
              leaseId: assign.leaseId,
              generation: assign.generation,
              resumeAfterSequence: 0n,
            },
          },
        }),
      );
    };

    ws.onmessage = (ev: MessageEvent) => {
      if (!(ev.data instanceof ArrayBuffer) && !(ev.data instanceof Uint8Array)) return;
      let decoded;
      try {
        decoded = decodeWsPayload(ev.data, pb.decodeAttemptDown);
      } catch (err) {
        done(() => reject(err instanceof Error ? err : new Error(String(err))));
        return;
      }
      if (decoded.kind === "error") {
        done(() => reject(new Error(decoded.message || `attempt error ${decoded.code}`)));
        return;
      }
      const msg = decoded.value.msg;
      if (!msg) return;
      if (msg.case === "opened") {
        opened = true;
        if (!assign.inputsViaAttempt) {
          const inlinePieces = assign.inlineInputs
            .map((obj) => (obj.body?.case === "inline" ? obj.body.value : new Uint8Array(0)))
            .filter((b) => b.byteLength > 0);
          done(() => resolve(concatBytes(inlinePieces)));
        }
      } else if (msg.case === "input") {
        if (!msg.value.parameterChunk && msg.value.data.byteLength > 0) {
          chunks.push(msg.value.data);
        }
        if (msg.value.last) {
          done(() => resolve(concatBytes(chunks)));
        }
      } else if (msg.case === "finished") {
        done(() => reject(new Error(msg.value.reason || "attempt finished early")));
      }
    };

    ws.onerror = () => done(() => reject(new Error("attempt WebSocket failed")));
    ws.onclose = () => {
      if (!opened) {
        done(() => reject(new Error("attempt WebSocket closed before Opened")));
      } else {
        done(() => resolve(concatBytes(chunks)));
      }
    };
  });
}

function sendFinishAndWait(ws: WebSocket, finish: pb.FinishInput): Promise<void> {
  return new Promise((resolve) => {
    if (ws.readyState !== WebSocket.OPEN) {
      resolve();
      return;
    }
    const timer = setTimeout(() => {
      ws.onmessage = null;
      resolve();
    }, 5_000);
    ws.onmessage = (ev: MessageEvent) => {
      if (!(ev.data instanceof ArrayBuffer) && !(ev.data instanceof Uint8Array)) return;
      try {
        const decoded = decodeWsPayload(ev.data, pb.decodeAttemptDown);
        if (decoded.kind === "error" || decoded.value.msg?.case === "finished") {
          clearTimeout(timer);
          ws.onmessage = null;
          resolve();
        }
      } catch {
        // ignore decode errors while waiting for AttemptFinished
      }
    };
    ws.send(pb.encodeAttemptUp({ msg: { case: "finish", value: finish } }));
  });
}

async function detectWebGpu(): Promise<boolean> {
  try {
    const nav = navigator as Navigator & { gpu?: { requestAdapter: () => Promise<unknown> } };
    if (!nav.gpu) return false;
    const adapter = await nav.gpu.requestAdapter();
    return Boolean(adapter);
  } catch {
    return false;
  }
}

async function loadSmolLmModel(): Promise<void> {
  if (loadedModel) {
    emit({ type: "model", status: "ready", backend: loadedModel.backend, progress: 100, file: null, error: null });
    return;
  }
  if (modelLoading) return;
  modelLoading = true;

  const hasWebGpu = await detectWebGpu();
  const targetBackend: "webgpu" | "wasm" = hasWebGpu ? "webgpu" : "wasm";
  const targetDtype = hasWebGpu ? "q4f16" : "q8";
  emit({
    type: "model",
    status: "loading",
    backend: targetBackend,
    progress: 0,
    file: "Downloading @huggingface/transformers@3 runtime...",
    error: null,
  });

  try {
    const transformers = (await import(/* @vite-ignore */ TRANSFORMERS_CDN_URL)) as {
      pipeline: (
        task: string,
        model: string,
        opts: Record<string, unknown>,
      ) => Promise<{
        (messages: Array<{ role: string; content: string }>, opts: Record<string, unknown>): Promise<unknown>;
        tokenizer: unknown;
      }>;
      TextStreamer: LoadedPipeline["TextStreamerCtor"];
    };

    const generator = await transformers.pipeline("text-generation", SMOLLM_MODEL_ID, {
      device: targetBackend,
      dtype: targetDtype,
      progress_callback: (info: { status?: string; file?: string; progress?: number }) => {
        if (typeof info.progress === "number" && Number.isFinite(info.progress)) {
          emit({
            type: "model",
            status: "loading",
            backend: targetBackend,
            progress: Math.min(100, Math.max(0, Math.round(info.progress))),
            file: info.file ?? info.status ?? null,
            error: null,
          });
        }
      },
    });

    loadedModel = {
      backend: targetBackend,
      generator,
      TextStreamerCtor: transformers.TextStreamer,
      tokenizer: generator.tokenizer,
    };
    currentBackend = targetBackend;
    emit({ type: "model", status: "ready", backend: targetBackend, progress: 100, file: null, error: null });
    emit({
      type: "log",
      message: `Loaded ${SMOLLM_MODEL_ID} on ${targetBackend.toUpperCase()} (${targetDtype})`,
    });

    // Re-register with the control plane so `browser.llm.v1` is advertised immediately.
    if (sessionWs && sessionWs.readyState === WebSocket.OPEN) {
      connectSession();
    }
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    emit({ type: "model", status: "error", backend: targetBackend, progress: 0, file: null, error: message });
    emit({ type: "log", message: `Model load failed: ${message}` });
  } finally {
    modelLoading = false;
  }
}

ctx.onmessage = (ev: MessageEvent<HostToWorkerCmd>) => {
  const cmd = ev.data;
  if (!cmd) return;
  if (cmd.type === "start") {
    wsBaseUrl = cmd.wsBaseUrl;
    workerId = cmd.workerId;
    workerRole = cmd.role;
    workerToken = cmd.token;
    connectSession();
  } else if (cmd.type === "load_model") {
    void loadSmolLmModel();
  } else if (cmd.type === "stop") {
    stopping = true;
    closeSession("stopped");
  } else if (cmd.type === "rtc_signal_out") {
    rtcHandlers.get(cmd.leaseId)?.onSignalOut(cmd.signal);
  } else if (cmd.type === "rtc_req") {
    rtcHandlers.get(cmd.leaseId)?.onReq(cmd.req, cmd.iceMs);
  } else if (cmd.type === "rtc_ack") {
    rtcHandlers.get(cmd.leaseId)?.onAck();
  } else if (cmd.type === "rtc_disconnected") {
    rtcHandlers.get(cmd.leaseId)?.onDisconnected();
  }
};
