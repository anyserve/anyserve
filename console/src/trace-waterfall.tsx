import { memo, useMemo } from "react";

import { formatDuration, formatNumber, formatTimestamp } from "./format";
import { CopyButton } from "./pane-tables";
import type { JobDetailResponse, JobEventSummary, MetadataMap } from "./types";

export interface ParsedW3cTrace {
  traceparent: string;
  traceparentValid: boolean;
  version: string | null;
  traceId: string | null;
  parentSpanId: string | null;
  traceFlags: string | null;
  sampled: boolean | null;
  tracestate: string | null;
  baggage: string | null;
  requestId: string | null;
}

const LOWER_HEX_RE = /^[0-9a-f]+$/;
const ALL_ZERO_RE = /^0+$/;

export function parseW3cTrace(metadata: MetadataMap): ParsedW3cTrace | null {
  const rawTraceparent = metadata.traceparent?.trim() || "";
  const tracestate = metadata.tracestate?.trim() || null;
  const baggage = metadata.baggage?.trim() || null;
  const requestId = metadata["x-request-id"]?.trim() || null;

  let traceparentValid = false;
  let version: string | null = null;
  let traceId: string | null = null;
  let parentSpanId: string | null = null;
  let traceFlags: string | null = null;
  let sampled: boolean | null = null;

  if (rawTraceparent.length > 0) {
    const parts = rawTraceparent.split("-");
    if (
      parts.length === 4 &&
      parts[0]!.length === 2 &&
      parts[1]!.length === 32 &&
      parts[2]!.length === 16 &&
      parts[3]!.length === 2 &&
      LOWER_HEX_RE.test(parts[0]!) &&
      LOWER_HEX_RE.test(parts[1]!) &&
      LOWER_HEX_RE.test(parts[2]!) &&
      LOWER_HEX_RE.test(parts[3]!) &&
      parts[0] !== "ff" &&
      !ALL_ZERO_RE.test(parts[1]!) &&
      !ALL_ZERO_RE.test(parts[2]!)
    ) {
      traceparentValid = true;
      version = parts[0]!;
      traceId = parts[1]!;
      parentSpanId = parts[2]!;
      traceFlags = parts[3]!;
      sampled = (parseInt(parts[3]!, 16) & 0x01) !== 0;
    }
  }

  if (!traceparentValid && !tracestate && !requestId && !baggage) {
    return null;
  }

  return {
    traceparent: rawTraceparent,
    traceparentValid,
    version,
    traceId,
    parentSpanId,
    traceFlags,
    sampled,
    tracestate,
    baggage,
    requestId,
  };
}

export interface WaterfallMarker {
  key: string;
  kind: "output" | "progress";
  atMs: number;
  bytes: number;
  isTtfo: boolean;
  leftPct: number;
}

export interface WaterfallSpan {
  id: string;
  kind: "job" | "admit" | "queue_wait" | "requeue_wait" | "attempt";
  label: string;
  workerId: string | null;
  tone: "job" | "queue" | "overhead" | "running" | "succeeded" | "failed" | "cancelled" | "restarted";
  startMs: number;
  endMs: number;
  durationMs: number;
  handlerMs?: number | null;
  relayMs?: number | null;
  leftPct: number;
  widthPct: number;
  markers: WaterfallMarker[];
}

export interface ExecutionWaterfallModel {
  originMs: number;
  endMs: number;
  totalMs: number;
  admitMs: number;
  queueMs: number;
  execMs: number;
  handlerMs: number | null;
  relayMs: number | null;
  anyserveOverheadMs: number;
  overheadExact: boolean;
  ttfoMs: number | null;
  retries: number;
  spans: WaterfallSpan[];
}

function clampTrackRange(startMs: number, endMs: number, originMs: number, trackSpanMs: number): { leftPct: number; widthPct: number } {
  const rawLeft = ((startMs - originMs) / trackSpanMs) * 100;
  const leftPct = Math.min(98.5, Math.max(0, rawLeft));
  const rawWidth = ((Math.max(startMs, endMs) - startMs) / trackSpanMs) * 100;
  const widthPct = Math.min(100 - leftPct, Math.max(1.5, rawWidth));
  return { leftPct, widthPct };
}

function attemptTone(state: string): WaterfallSpan["tone"] {
  switch (state) {
    case "succeeded":
      return "succeeded";
    case "failed":
      return "failed";
    case "cancelled":
      return "cancelled";
    case "restarted":
      return "restarted";
    default:
      return "running";
  }
}

export function buildExecutionWaterfall(detail: JobDetailResponse): ExecutionWaterfallModel | null {
  const { job, attempts, events } = detail;
  const allStarts = [job.created_at_ms, ...events.map((e) => e.created_at_ms), ...attempts.map((a) => a.created_at_ms)].filter((v) =>
    Number.isFinite(v),
  );
  if (allStarts.length === 0) return null;

  const originMs = Math.min(...allStarts);
  const rawEndMs = Math.max(
    originMs,
    job.updated_at_ms,
    ...events.map((e) => e.created_at_ms),
    ...attempts.map((a) => a.finished_at_ms ?? a.created_at_ms),
  );
  const totalMs = Math.max(0, rawEndMs - originMs);
  const trackSpanMs = Math.max(1, rawEndMs - originMs);

  const firstOutput = events.find((e) => e.kind === "output");
  const ttfoMs = firstOutput ? Math.max(0, firstOutput.created_at_ms - originMs) : null;
  const retries = Math.max(0, attempts.length - 1);

  const spans: WaterfallSpan[] = [];
  const rootRange = clampTrackRange(originMs, rawEndMs, originMs, trackSpanMs);
  spans.push({
    id: "job",
    kind: "job",
    label: `job · ${job.interface_name}`,
    workerId: null,
    tone: job.state === "queued" ? "queue" : attemptTone(job.state),
    startMs: originMs,
    endMs: rawEndMs,
    durationMs: totalMs,
    leftPct: rootRange.leftPct,
    widthPct: rootRange.widthPct,
    markers: [],
  });

  const acceptedEvent = events.find((e) => e.kind === "accepted");
  const acceptedAtMs = acceptedEvent ? Math.max(originMs, acceptedEvent.created_at_ms) : originMs;
  const admitMs = Math.max(0, acceptedAtMs - originMs);
  let queueMs = 0;
  let execMs = 0;
  let handlerSumMs = 0;
  let relaySumMs = 0;
  let measuredAttempts = 0;

  if (admitMs > 0) {
    const admitRange = clampTrackRange(originMs, acceptedAtMs, originMs, trackSpanMs);
    spans.push({
      id: "admit",
      kind: "admit",
      label: "anyserve · admit",
      workerId: null,
      tone: "overhead",
      startMs: originMs,
      endMs: acceptedAtMs,
      durationMs: admitMs,
      leftPct: admitRange.leftPct,
      widthPct: admitRange.widthPct,
      markers: [],
    });
  }

  if (attempts.length === 0) {
    queueMs = Math.max(0, rawEndMs - acceptedAtMs);
    const qRange = clampTrackRange(acceptedAtMs, rawEndMs, originMs, trackSpanMs);
    spans.push({
      id: "queue-0",
      kind: "queue_wait",
      label: "queue wait",
      workerId: null,
      tone: "queue",
      startMs: acceptedAtMs,
      endMs: rawEndMs,
      durationMs: queueMs,
      leftPct: qRange.leftPct,
      widthPct: qRange.widthPct,
      markers: [],
    });
  } else {
    const firstStart = Math.max(acceptedAtMs, attempts[0]!.created_at_ms);
    const initialQueueMs = Math.max(0, firstStart - acceptedAtMs);
    queueMs += initialQueueMs;
    const q0Range = clampTrackRange(acceptedAtMs, firstStart, originMs, trackSpanMs);
    spans.push({
      id: "queue-0",
      kind: "queue_wait",
      label: "queue wait",
      workerId: null,
      tone: "queue",
      startMs: acceptedAtMs,
      endMs: firstStart,
      durationMs: initialQueueMs,
      leftPct: q0Range.leftPct,
      widthPct: q0Range.widthPct,
      markers: [],
    });

    const streamEvents: Array<{ event: JobEventSummary; assignedAttemptIdx: number }> = [];
    for (const ev of events) {
      if (ev.kind !== "output" && ev.kind !== "progress") continue;
      let idx = -1;
      if (ev.attempt_id) {
        idx = attempts.findIndex((a) => a.attempt_id === ev.attempt_id);
      }
      if (idx === -1) {
        for (let i = attempts.length - 1; i >= 0; i--) {
          const a = attempts[i]!;
          const aEnd = a.finished_at_ms ?? rawEndMs;
          if (ev.created_at_ms >= a.created_at_ms && ev.created_at_ms <= aEnd) {
            idx = i;
            break;
          }
        }
      }
      if (idx !== -1) {
        streamEvents.push({ event: ev, assignedAttemptIdx: idx });
      }
    }

    for (let i = 0; i < attempts.length; i++) {
      const attempt = attempts[i]!;
      if (i > 0) {
        const prevEnd = attempts[i - 1]!.finished_at_ms ?? attempt.created_at_ms;
        if (attempt.created_at_ms > prevEnd) {
          const gapMs = attempt.created_at_ms - prevEnd;
          queueMs += gapMs;
          const gapRange = clampTrackRange(prevEnd, attempt.created_at_ms, originMs, trackSpanMs);
          spans.push({
            id: `requeue-${i}`,
            kind: "requeue_wait",
            label: `retry #${i} queue`,
            workerId: null,
            tone: "queue",
            startMs: prevEnd,
            endMs: attempt.created_at_ms,
            durationMs: gapMs,
            leftPct: gapRange.leftPct,
            widthPct: gapRange.widthPct,
            markers: [],
          });
        }
      }

      const aStart = attempt.created_at_ms;
      const aEnd = Math.max(aStart, attempt.finished_at_ms ?? rawEndMs);
      const aDur = attempt.duration_ms ?? Math.max(0, aEnd - aStart);
      execMs += aDur;
      const reportedHandler = attempt.handler_duration_ms;
      let spanHandlerMs: number | null = null;
      let spanRelayMs: number | null = null;
      if (reportedHandler != null && Number.isFinite(reportedHandler)) {
        spanHandlerMs = Math.min(aDur, Math.max(0, reportedHandler));
        spanRelayMs = Math.max(0, aDur - spanHandlerMs);
        handlerSumMs += spanHandlerMs;
        relaySumMs += spanRelayMs;
        measuredAttempts += 1;
      }
      const aRange = clampTrackRange(aStart, aEnd, originMs, trackSpanMs);
      const markers: WaterfallMarker[] = streamEvents
        .filter((entry) => entry.assignedAttemptIdx === i)
        .map(({ event }) => ({
          key: String(event.sequence),
          kind: event.kind as "output" | "progress",
          atMs: event.created_at_ms,
          bytes: event.payload_size_bytes,
          isTtfo: firstOutput != null && event.sequence === firstOutput.sequence,
          leftPct: Math.min(99.5, Math.max(0.5, ((event.created_at_ms - originMs) / trackSpanMs) * 100)),
        }));

      spans.push({
        id: `attempt-${attempt.attempt_id}`,
        kind: "attempt",
        label: `attempt #${i + 1}`,
        workerId: attempt.worker_id || null,
        tone: attemptTone(attempt.state),
        startMs: aStart,
        endMs: aEnd,
        durationMs: aDur,
        handlerMs: spanHandlerMs,
        relayMs: spanRelayMs,
        leftPct: aRange.leftPct,
        widthPct: aRange.widthPct,
        markers,
      });
    }
  }

  const lastAttemptEnd = attempts.length > 0 ? (attempts[attempts.length - 1]!.finished_at_ms ?? rawEndMs) : rawEndMs;
  const tailMs = Math.max(0, rawEndMs - lastAttemptEnd);
  const overheadExact = attempts.length > 0 && measuredAttempts === attempts.length;
  const handlerMs = overheadExact ? handlerSumMs : null;
  const relayMs = overheadExact ? relaySumMs : null;
  const anyserveOverheadMs = overheadExact ? Math.max(0, totalMs - handlerSumMs) : admitMs + queueMs + tailMs;

  return {
    originMs,
    endMs: rawEndMs,
    totalMs,
    admitMs,
    queueMs,
    execMs,
    handlerMs,
    relayMs,
    anyserveOverheadMs,
    overheadExact,
    ttfoMs,
    retries,
    spans,
  };
}

export interface ParsedTransportInfo {
  requested: "webrtc" | "relay";
  actual: "p2p" | "relay" | "pending";
  p2pBytes: number | null;
  relayedPayloadBytes: number | null;
  iceMs: number | null;
  fallbackReason: string | null;
}

function parseNonNegativeInt(raw?: string): number | null {
  if (!raw) return null;
  const n = Number(raw);
  return Number.isFinite(n) && n >= 0 ? Math.round(n) : null;
}

function formatByteCount(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(2)} MB`;
}

export function parseJobTransport(detail: JobDetailResponse): ParsedTransportInfo | null {
  for (const out of detail.job.outputs) {
    const actualRaw = out.metadata["anyserve.transport_actual"]?.trim();
    if (actualRaw === "p2p" || actualRaw === "relay") {
      return {
        requested: detail.job.metadata["anyserve.transport"] === "webrtc" ? "webrtc" : "relay",
        actual: actualRaw,
        p2pBytes: parseNonNegativeInt(out.metadata["anyserve.p2p_bytes"]),
        relayedPayloadBytes: parseNonNegativeInt(out.metadata["anyserve.relayed_payload_bytes"]),
        iceMs: parseNonNegativeInt(out.metadata["anyserve.ice_ms"]),
        fallbackReason: out.metadata["anyserve.fallback_reason"]?.trim() || null,
      };
    }
  }
  if (detail.job.metadata["anyserve.transport"] === "webrtc") {
    return {
      requested: "webrtc",
      actual: "pending",
      p2pBytes: null,
      relayedPayloadBytes: null,
      iceMs: null,
      fallbackReason: null,
    };
  }
  return null;
}

export interface ParsedDirectTransfer {
  path: "direct" | "relay";
  bytes: number | null;
}

export function parseDirectTransfer(detail: JobDetailResponse): ParsedDirectTransfer | null {
  for (const out of detail.job.outputs) {
    const pathRaw = out.metadata["anyserve.direct_path"]?.trim();
    if (pathRaw === "direct" || pathRaw === "relay") {
      return {
        path: pathRaw,
        bytes: parseNonNegativeInt(out.metadata["anyserve.direct_bytes"]),
      };
    }
  }
  return null;
}

export const ExecutionWaterfall = memo(function ExecutionWaterfall({
  detail,
  onOpenWorker,
  onFilterTrace,
}: {
  detail: JobDetailResponse;
  onOpenWorker?: (workerId: string) => void;
  onFilterTrace?: (traceId: string) => void;
}) {
  const trace = useMemo(() => parseW3cTrace(detail.job.metadata), [detail.job.metadata]);
  const transport = useMemo(() => parseJobTransport(detail), [detail]);
  const directTransfer = useMemo(() => parseDirectTransfer(detail), [detail]);
  const model = useMemo(() => buildExecutionWaterfall(detail), [detail]);

  if (!trace && !model) return null;

  return (
    <div className="waterfall-block">
      {trace ? (
        <div className="trace-banner">
          <div className="trace-banner__row">
            {trace.traceparentValid && trace.traceId ? (
              <>
                <span className="trace-banner__label">Trace</span>
                {onFilterTrace ? (
                  <button
                    type="button"
                    className="entity-link mono trace-banner__id"
                    title={`Filter jobs with trace ${trace.traceId}`}
                    onClick={() => onFilterTrace(trace.traceId!)}
                  >
                    {trace.traceId}
                  </button>
                ) : (
                  <span className="mono trace-banner__id" title={trace.traceId}>
                    {trace.traceId}
                  </span>
                )}
                {trace.parentSpanId ? (
                  <span className="tag mono" title={`Parent span ${trace.parentSpanId}`}>
                    span {trace.parentSpanId}
                  </span>
                ) : null}
                <span className={`tag ${trace.sampled ? "tag--ok" : ""}`}>
                  {trace.sampled ? "sampled" : "unsampled"}
                </span>
                <CopyButton text={trace.traceparent} label="traceparent" resetKey={`${detail.job.job_id}:tp`} />
              </>
            ) : null}
            {trace.requestId ? (
              <span className="tag mono" title={`x-request-id: ${trace.requestId}`}>
                req {trace.requestId}
              </span>
            ) : null}
            {trace.tracestate ? (
              <span className="tag mono" title={`tracestate: ${trace.tracestate}`}>
                tracestate
              </span>
            ) : null}
          </div>
        </div>
      ) : null}

      {model ? (
        <div className="waterfall">
          <div className="waterfall__summary">
            <span className="waterfall__metric">
              Total <strong>{formatDuration(model.totalMs)}</strong>
            </span>
            <span
              className="waterfall__metric waterfall__metric--overhead"
              title={
                model.overheadExact
                  ? `AnyServe added latency: admit ${formatDuration(model.admitMs)} · queue ${formatDuration(model.queueMs)} · relay & RTT ${formatDuration(model.relayMs ?? 0)}`
                  : `AnyServe control-plane overhead: admit ${formatDuration(model.admitMs)} · queue ${formatDuration(model.queueMs)} (upgrade worker SDK to separate relay RTT from handler exec)`
              }
            >
              AnyServe <strong>+{formatDuration(model.anyserveOverheadMs)}</strong>
            </span>
            <span className="waterfall__metric">
              Queue <strong>{formatDuration(model.queueMs)}</strong>
            </span>
            {model.handlerMs != null ? (
              <span className="waterfall__metric" title={`Worker handler execution across ${model.spans.filter((s) => s.kind === "attempt").length} attempt(s)`}>
                Handler <strong>{formatDuration(model.handlerMs)}</strong>
              </span>
            ) : (
              <span className="waterfall__metric">
                Exec <strong>{formatDuration(model.execMs)}</strong>
              </span>
            )}
            {model.ttfoMs != null ? (
              <span className="waterfall__metric">
                TTFO <strong>{formatDuration(model.ttfoMs)}</strong>
              </span>
            ) : null}
            {model.retries > 0 ? (
              <span className="waterfall__metric">
                Retries <strong>{formatNumber(model.retries)}</strong>
              </span>
            ) : null}
            {transport?.actual === "p2p" ? (
              <span
                className="waterfall__metric waterfall__metric--p2p"
                title={`Worker-reported WebRTC P2P DataChannel transfer (${transport.p2pBytes != null ? formatByteCount(transport.p2pBytes) : "direct"}, ${transport.relayedPayloadBytes ?? 0} B payload relayed; worker-reported metadata, not server-audited${transport.iceMs != null ? `, ICE ${formatDuration(transport.iceMs)}` : ""})`}
              >
                Data plane <strong>WebRTC P2P</strong>
                {transport.p2pBytes != null ? ` · ${formatByteCount(transport.p2pBytes)} (worker-reported)` : ""}
                {transport.iceMs != null ? ` · ICE ${formatDuration(transport.iceMs)}` : ""}
              </span>
            ) : null}
            {transport?.actual === "relay" && transport.fallbackReason ? (
              <span
                className="waterfall__metric waterfall__metric--warn"
                title={`WebRTC fell back to WebSocket relay (${transport.fallbackReason})`}
              >
                Data plane <strong>WS Relay</strong> (fallback: {transport.fallbackReason})
              </span>
            ) : null}
            {transport?.actual === "pending" ? (
              <span className="waterfall__metric">
                Data plane <strong>WebRTC signaling…</strong>
              </span>
            ) : null}
            {directTransfer ? (
              <span
                className={`waterfall__metric ${directTransfer.path === "direct" ? "waterfall__metric--p2p" : "waterfall__metric--warn"}`}
                title={`SDK-reported iroh direct data-plane transfer (${directTransfer.path}${directTransfer.bytes != null ? `, ${formatByteCount(directTransfer.bytes)}` : ""}; SDK-reported metadata, not server-audited)`}
              >
                Data plane <strong>{directTransfer.path === "direct" ? "iroh direct" : "iroh relay"}</strong>
                {directTransfer.bytes != null ? ` · ${formatByteCount(directTransfer.bytes)} (SDK-reported)` : " (SDK-reported)"}
              </span>
            ) : null}
          </div>

          <div className="waterfall__rows" role="list" aria-label="Execution waterfall">
            {model.spans.map((span) => {
              const splitAttempt = span.kind === "attempt" && span.handlerMs != null && span.relayMs != null && span.durationMs > 0;
              const relaySharePct = splitAttempt ? (span.relayMs! / span.durationMs) * 100 : 0;
              const handlerSharePct = splitAttempt ? 100 - relaySharePct : 100;
              const trackTitle = splitAttempt
                ? `${span.label} · ${formatDuration(span.durationMs)} (handler ${formatDuration(span.handlerMs!)} · anyserve relay ${formatDuration(span.relayMs!)})`
                : `${span.label} · ${formatDuration(span.durationMs)} (${formatTimestamp(span.startMs)} → ${formatTimestamp(span.endMs)})`;
              return (
                <div key={span.id} className="waterfall__row" role="listitem">
                  <div className="waterfall__label">
                    <span className="waterfall__span-name" title={span.label}>
                      {span.label}
                    </span>
                    {span.workerId ? (
                      onOpenWorker ? (
                        <button
                          type="button"
                          className="entity-link mono waterfall__worker"
                          title={`Open worker ${span.workerId}`}
                          onClick={() => onOpenWorker(span.workerId!)}
                        >
                          {span.workerId}
                        </button>
                      ) : (
                        <span className="mono waterfall__worker">{span.workerId}</span>
                      )
                    ) : null}
                    {span.kind === "attempt" && span.handlerMs != null && span.relayMs != null ? (
                      <span className="mono waterfall__submeta" title={trackTitle}>
                        handler {formatDuration(span.handlerMs)} · relay {formatDuration(span.relayMs)}
                      </span>
                    ) : null}
                  </div>
                  <div className="waterfall__track" title={trackTitle}>
                    {splitAttempt ? (
                      <div
                        className="waterfall__bar waterfall__bar--split"
                        style={{ left: `${span.leftPct.toFixed(2)}%`, width: `${span.widthPct.toFixed(2)}%` }}
                      >
                        {span.relayMs! > 0 ? (
                          <div className="waterfall__bar-seg waterfall__bar-seg--overhead" style={{ width: `${relaySharePct.toFixed(2)}%` }} />
                        ) : null}
                        {span.handlerMs! > 0 ? (
                          <div className={`waterfall__bar-seg waterfall__bar-seg--${span.tone}`} style={{ width: `${handlerSharePct.toFixed(2)}%` }} />
                        ) : null}
                      </div>
                    ) : (
                      <div
                        className={`waterfall__bar waterfall__bar--${span.tone}`}
                        style={{ left: `${span.leftPct.toFixed(2)}%`, width: `${span.widthPct.toFixed(2)}%` }}
                      />
                    )}
                    {span.markers.map((marker) => (
                      <span
                        key={marker.key}
                        className={`waterfall__marker waterfall__marker--${marker.kind}${marker.isTtfo ? " waterfall__marker--ttfo" : ""}`}
                        style={{ left: `${marker.leftPct.toFixed(2)}%` }}
                        title={`${marker.isTtfo ? "TTFO · " : ""}${marker.kind} at +${formatDuration(Math.max(0, marker.atMs - model.originMs))} (${formatNumber(marker.bytes)} B)`}
                      />
                    ))}
                  </div>
                  <div className="waterfall__dur num">{formatDuration(span.durationMs)}</div>
                </div>
              );
            })}
          </div>
        </div>
      ) : null}
    </div>
  );
});
