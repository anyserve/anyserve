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
  | { type: "stop" };

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
    const inputBytes = await openAttemptAndReadInputs(attemptWs, assign, abortCtrl.signal);
    const prompt = decodeUtf8(inputBytes).trim() || "Hello from AnyServe browser worker!";
    const handlerStart = performance.now();

    let nextSeq = 1n;
    const sendFrame = (text: string, progress: boolean) => {
      if (attemptWs.readyState !== WebSocket.OPEN) return;
      attemptWs.send(
        pb.encodeAttemptUp({
          msg: {
            case: "frame",
            value: {
              sequence: nextSeq,
              data: encodeUtf8(text),
              metadata: { content_type: "text/plain; charset=utf-8" },
              progress,
            },
          },
        }),
      );
      nextSeq += 1n;
    };

    const prefillMs = Math.min(5_000, Math.max(0, Number(assign.metadata["demo.prefill_ms"] ?? "0") || 0));
    if (prefillMs > 0) {
      // Progress frames do not set attempt.exposed=true, mirroring real prefill work
      // before the first output token is emitted.
      sendFrame(`prefilling on ${workerId} (${workerRole})...`, true);
      await sleepMs(prefillMs, abortCtrl.signal);
    }

    if (assign.interface === LLM_INTERFACE) {
      if (!loadedModel) {
        throw new Error("SmolLM2-135M model is not loaded on this browser worker");
      }
      sendFrame(`running ${SMOLLM_MODEL_ID.split("/").pop()} on ${loadedModel.backend.toUpperCase()} (${workerId})...`, true);
      const streamer = new loadedModel.TextStreamerCtor(loadedModel.tokenizer, {
        skip_prompt: true,
        skip_special_tokens: true,
        callback_function: (tokenText: string) => {
          if (!abortCtrl.signal.aborted && tokenText) {
            sendFrame(tokenText, false);
          }
        },
      });
      await loadedModel.generator(
        [
          {
            role: "system",
            content: "You are a concise assistant running inside a browser Web Worker scheduled by AnyServe.",
          },
          { role: "user", content: prompt },
        ],
        {
          max_new_tokens: 96,
          do_sample: false,
          streamer,
        },
      );
    } else {
      const delayMs = Math.min(200, Math.max(0, Number(assign.metadata["demo.token_delay_ms"] ?? "28") || 28));
      const reply = `[${workerId} · ${workerRole}] ${prompt}`;
      const tokens = reply.split(/(\s+)/).filter((t) => t.length > 0);
      for (const tok of tokens) {
        if (abortCtrl.signal.aborted) break;
        sendFrame(tok, false);
        if (delayMs > 0) {
          await sleepMs(delayMs, abortCtrl.signal);
        }
      }
    }

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
  }
};
