import * as pb from "@anyserve/proto";

export { pb };

export const WS_SUBPROTOCOL_V2 = "anyserve.v2";
export const WS_BEARER_SUBPROTOCOL_PREFIX = "anyserve.bearer.";
export const ECHO_INTERFACE = "browser.echo.v1";
export const LLM_INTERFACE = "browser.llm.v1";
export const SMOLLM_MODEL_ID = "onnx-community/SmolLM2-135M-Instruct-ONNX-MHA";

const utf8Encoder = new TextEncoder();
const utf8Decoder = new TextDecoder();

export function encodeUtf8(text: string): Uint8Array {
  const bytes = utf8Encoder.encode(text);
  return bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes);
}

export function decodeUtf8(bytes: Uint8Array): string {
  return utf8Decoder.decode(bytes);
}

export function encodeBase64UrlNoPad(input: string): string {
  const bytes = utf8Encoder.encode(input);
  let binary = "";
  for (let i = 0; i < bytes.length; i++) {
    binary += String.fromCharCode(bytes[i]!);
  }
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
}

/**
 * Builds the `Sec-WebSocket-Protocol` list for `/ws/v2/*`.
 * Never echoes credentials in URL query strings.
 */
export function buildWsSubprotocols(token?: string | null): string[] {
  const trimmed = token?.trim() ?? "";
  if (!trimmed) {
    return [WS_SUBPROTOCOL_V2];
  }
  return [WS_SUBPROTOCOL_V2, `${WS_BEARER_SUBPROTOCOL_PREFIX}${encodeBase64UrlNoPad(trimmed)}`];
}

export function deriveWsBaseUrl(apiBase?: string | null): string {
  const explicit = apiBase?.trim();
  const base =
    explicit && explicit.length > 0
      ? explicit
      : typeof window !== "undefined" && window.location.origin && window.location.origin !== "null"
        ? window.location.origin
        : "http://127.0.0.1:3001";
  const clean = base.replace(/\/+$/, "");
  if (/^https:\/\//i.test(clean)) return `wss://${clean.slice(8)}`;
  if (/^http:\/\//i.test(clean)) return `ws://${clean.slice(7)}`;
  return clean;
}

export function decodeWsPayload<T>(
  raw: ArrayBuffer | Uint8Array,
  decoder: (bytes: Uint8Array) => T,
): { kind: "data"; value: T } | { kind: "error"; code: number; message: string } {
  const bytes = raw instanceof Uint8Array ? raw : new Uint8Array(raw);
  const frame = pb.decodeWsFrame(bytes);
  if (frame.kind === "error") {
    return frame;
  }
  return { kind: "data", value: decoder(frame.payload) };
}

export function concatBytes(chunks: readonly Uint8Array[]): Uint8Array {
  if (chunks.length === 0) return new Uint8Array(0);
  if (chunks.length === 1) return chunks[0]!;
  let total = 0;
  for (const c of chunks) total += c.byteLength;
  const out = new Uint8Array(total);
  let offset = 0;
  for (const c of chunks) {
    out.set(c, offset);
    offset += c.byteLength;
  }
  return out;
}

export interface StreamingInvokeTimelineEvent {
  atMs: number;
  kind: "accepted" | "assigned" | "progress" | "output" | "attempt_restarted" | "finished" | "error";
  detail: string;
}

export interface StreamingInvokeResult {
  jobId: string | null;
  state: "succeeded" | "failed" | "cancelled";
  text: string;
  error: string | null;
  workerId: string | null;
  attempts: number;
  restarted: boolean;
  failoverMs: number | null;
  ttftMs: number | null;
  totalMs: number;
  timeline: StreamingInvokeTimelineEvent[];
}

export interface StreamingInvokeOptions {
  wsBaseUrl: string;
  token?: string | null;
  interfaceName: string;
  prompt: string;
  preferredAttributes?: Record<string, string>;
  metadata?: Record<string, string>;
  maxAttempts?: number;
  signal?: AbortSignal;
  WebSocketCtor?: typeof WebSocket;
  onJobAccepted?: (jobId: string) => void;
  onAssigned?: (workerId: string, attemptNumber: number) => void;
  onChunk?: (text: string) => void;
  onProgress?: (message: string) => void;
  onTimeline?: (event: StreamingInvokeTimelineEvent) => void;
}

export function invokeStreamingJob(options: StreamingInvokeOptions): Promise<StreamingInvokeResult> {
  const WsImpl = options.WebSocketCtor ?? WebSocket;
  const startedAt = performance.now();
  const timeline: StreamingInvokeTimelineEvent[] = [];
  const pushTimeline = (kind: StreamingInvokeTimelineEvent["kind"], detail: string) => {
    const ev: StreamingInvokeTimelineEvent = {
      atMs: Math.max(0, Math.round(performance.now() - startedAt)),
      kind,
      detail,
    };
    timeline.push(ev);
    options.onTimeline?.(ev);
  };

  return new Promise<StreamingInvokeResult>((resolve, reject) => {
    let settled = false;
    let jobId: string | null = null;
    let lastWorkerId: string | null = null;
    let attempts = 0;
    let restarted = false;
    let firstAssignedAt: number | null = null;
    let failoverMs: number | null = null;
    let ttftMs: number | null = null;
    let combinedText = "";

    const wsUrl = `${options.wsBaseUrl.replace(/\/+$/, "")}/ws/v2/client/invoke`;
    const ws = new WsImpl(wsUrl, buildWsSubprotocols(options.token));
    ws.binaryType = "arraybuffer";

    const finishResolve = (res: StreamingInvokeResult) => {
      if (settled) return;
      settled = true;
      options.signal?.removeEventListener("abort", onAbort);
      resolve(res);
    };

    const finishReject = (err: Error) => {
      if (settled) return;
      settled = true;
      options.signal?.removeEventListener("abort", onAbort);
      reject(err);
    };

    const onAbort = () => {
      try {
        ws.close(1000, "cancelled by caller");
      } catch {
        // ignore close errors
      }
      pushTimeline("finished", "cancelled by caller");
      finishResolve({
        jobId,
        state: "cancelled",
        text: combinedText,
        error: "cancelled by caller",
        workerId: lastWorkerId,
        attempts,
        restarted,
        failoverMs,
        ttftMs,
        totalMs: Math.max(0, Math.round(performance.now() - startedAt)),
        timeline,
      });
    };

    if (options.signal) {
      if (options.signal.aborted) {
        onAbort();
        return;
      }
      options.signal.addEventListener("abort", onAbort, { once: true });
    }

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
              inputs: [
                {
                  body: { case: "inline", value: encodeUtf8(options.prompt) },
                  metadata: { content_type: "text/plain; charset=utf-8" },
                },
              ],
              metadata: {
                source: "console.playground",
                ...(options.metadata ?? {}),
              },
              policy: {
                durability: pb.Durability.EPHEMERAL,
                maxAttempts: options.maxAttempts ?? 2,
                queueTimeoutMs: 15_000n,
                executionTimeoutMs: 120_000n,
              },
            },
            streamingInput: false,
          },
        },
      });
      ws.send(startFrame);
    };

    ws.onmessage = (ev: MessageEvent) => {
      if (settled) return;
      if (!(ev.data instanceof ArrayBuffer) && !(ev.data instanceof Uint8Array)) {
        return;
      }
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
          attempts += 1;
          lastWorkerId = kind.value.workerId;
          if (attempts === 1) {
            firstAssignedAt = performance.now();
          } else if (attempts === 2 && firstAssignedAt !== null && failoverMs === null) {
            failoverMs = Math.max(0, Math.round(performance.now() - firstAssignedAt));
          }
          pushTimeline("assigned", `attempt #${attempts} -> ${kind.value.workerId}`);
          options.onAssigned?.(kind.value.workerId, attempts);
          break;
        case "attemptRestarted":
          restarted = true;
          pushTimeline(
            "attempt_restarted",
            kind.value.reason?.message || "previous attempt failed; retrying on another worker",
          );
          break;
        case "output": {
          const text = decodeUtf8(kind.value.data);
          if (kind.value.progress) {
            pushTimeline("progress", text);
            options.onProgress?.(text);
          } else {
            if (ttftMs === null) {
              ttftMs = Math.max(0, Math.round(performance.now() - startedAt));
              pushTimeline("output", `first token at +${ttftMs} ms`);
            }
            combinedText += text;
            options.onChunk?.(text);
          }
          break;
        }
        case "finished": {
          const st = kind.value.state;
          if (!combinedText && kind.value.outputs.length > 0) {
            const pieces = kind.value.outputs
              .map((o) => (o.body?.case === "inline" ? decodeUtf8(o.body.value) : ""))
              .join("");
            if (pieces) {
              combinedText = pieces;
              options.onChunk?.(pieces);
            }
          }
          const state: StreamingInvokeResult["state"] =
            st === pb.JobState.SUCCEEDED
              ? "succeeded"
              : st === pb.JobState.CANCELLED
                ? "cancelled"
                : "failed";
          const errMsg = kind.value.error?.message || null;
          pushTimeline("finished", errMsg ? `${state}: ${errMsg}` : state);
          try {
            ws.close(1000, "done");
          } catch {
            // ignore
          }
          finishResolve({
            jobId,
            state,
            text: combinedText,
            error: errMsg,
            workerId: lastWorkerId,
            attempts,
            restarted,
            failoverMs,
            ttftMs,
            totalMs: Math.max(0, Math.round(performance.now() - startedAt)),
            timeline,
          });
          break;
        }
      }
    };

    ws.onerror = () => {
      if (!settled) {
        finishReject(new Error("WebSocket connection to /ws/v2/client/invoke failed (check auth token / server status)"));
      }
    };

    ws.onclose = (ev: CloseEvent) => {
      if (!settled) {
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
}
