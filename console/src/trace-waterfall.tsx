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
  kind: "job" | "queue_wait" | "requeue_wait" | "attempt";
  label: string;
  workerId: string | null;
  tone: "job" | "queue" | "running" | "succeeded" | "failed" | "cancelled" | "restarted";
  startMs: number;
  endMs: number;
  durationMs: number;
  leftPct: number;
  widthPct: number;
  markers: WaterfallMarker[];
}

export interface ExecutionWaterfallModel {
  originMs: number;
  endMs: number;
  totalMs: number;
  queueMs: number;
  execMs: number;
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

  let queueMs = 0;
  let execMs = 0;

  if (attempts.length === 0) {
    queueMs = totalMs;
    const qRange = clampTrackRange(originMs, rawEndMs, originMs, trackSpanMs);
    spans.push({
      id: "queue-0",
      kind: "queue_wait",
      label: "queue wait",
      workerId: null,
      tone: "queue",
      startMs: originMs,
      endMs: rawEndMs,
      durationMs: queueMs,
      leftPct: qRange.leftPct,
      widthPct: qRange.widthPct,
      markers: [],
    });
  } else {
    const firstStart = Math.max(originMs, attempts[0]!.created_at_ms);
    const initialQueueMs = Math.max(0, firstStart - originMs);
    queueMs += initialQueueMs;
    const q0Range = clampTrackRange(originMs, firstStart, originMs, trackSpanMs);
    spans.push({
      id: "queue-0",
      kind: "queue_wait",
      label: "queue wait",
      workerId: null,
      tone: "queue",
      startMs: originMs,
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
        leftPct: aRange.leftPct,
        widthPct: aRange.widthPct,
        markers,
      });
    }
  }

  return {
    originMs,
    endMs: rawEndMs,
    totalMs,
    queueMs,
    execMs,
    ttfoMs,
    retries,
    spans,
  };
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
            <span className="waterfall__metric">
              Queue <strong>{formatDuration(model.queueMs)}</strong>
            </span>
            <span className="waterfall__metric">
              Exec <strong>{formatDuration(model.execMs)}</strong>
            </span>
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
          </div>

          <div className="waterfall__rows" role="list" aria-label="Execution waterfall">
            {model.spans.map((span) => (
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
                </div>
                <div
                  className="waterfall__track"
                  title={`${span.label} · ${formatDuration(span.durationMs)} (${formatTimestamp(span.startMs)} → ${formatTimestamp(span.endMs)})`}
                >
                  <div
                    className={`waterfall__bar waterfall__bar--${span.tone}`}
                    style={{ left: `${span.leftPct.toFixed(2)}%`, width: `${span.widthPct.toFixed(2)}%` }}
                  />
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
            ))}
          </div>
        </div>
      ) : null}
    </div>
  );
});
