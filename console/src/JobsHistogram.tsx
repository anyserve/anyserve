import { useEffect, useMemo, useRef, useState } from "react";

import type { TimeFilterState, TimePreset } from "./app-constants";
import { formatAxisTime, formatDuration, formatNumber } from "./format";
import type { JobsHistogram } from "./types";

export type { TimeFilterState, TimePreset };

export const TIME_PRESETS: Array<{ value: TimePreset; label: string; windowMs: number | null }> = [
  { value: "all", label: "All", windowMs: null },
  { value: "5m", label: "5m", windowMs: 5 * 60_000 },
  { value: "15m", label: "15m", windowMs: 15 * 60_000 },
  { value: "1h", label: "1h", windowMs: 60 * 60_000 },
  { value: "6h", label: "6h", windowMs: 6 * 3_600_000 },
  { value: "24h", label: "24h", windowMs: 24 * 3_600_000 },
  { value: "custom", label: "Custom", windowMs: null },
];

export const TIME_PRESET_MS: Record<Exclude<TimePreset, "all" | "custom">, number> = {
  "5m": 5 * 60_000,
  "15m": 15 * 60_000,
  "1h": 60 * 60_000,
  "6h": 6 * 3_600_000,
  "24h": 24 * 3_600_000,
};

const PLACEHOLDER_COUNT = 32;

interface JobsHistogramProps {
  histogram: JobsHistogram | null;
  timeFilter: TimeFilterState;
  refreshing?: boolean;
  onZoomRange: (fromMs: number, toMs: number) => void;
  onResetTime: () => void;
}

function summarizeBucketCounts(succeeded: number, errors: number, active: number): string {
  const parts: string[] = [];
  if (succeeded > 0) parts.push(`${formatNumber(succeeded)} ok`);
  if (errors > 0) parts.push(`${formatNumber(errors)} err`);
  if (active > 0) parts.push(`${formatNumber(active)} active`);
  return parts.length > 0 ? parts.join(" · ") : "0 jobs";
}

function activeTimeSummary(filter: TimeFilterState): string {
  if (filter.preset === "custom" && filter.fromMs != null && filter.toMs != null) {
    const span = filter.toMs - filter.fromMs;
    return `${formatAxisTime(filter.fromMs, span)} — ${formatAxisTime(filter.toMs, span)}`;
  }
  return filter.preset === "all" ? "All time" : `Last ${filter.preset}`;
}

export function JobsHistogram({ histogram, timeFilter, refreshing, onZoomRange, onResetTime }: JobsHistogramProps) {
  const [hoveredIdx, setHoveredIdx] = useState<number | null>(null);
  const [brush, setBrush] = useState<{ anchor: number; current: number } | null>(null);
  const barsRef = useRef<HTMLDivElement | null>(null);
  const brushRef = useRef<{ anchor: number; current: number } | null>(null);
  const draggedRef = useRef(false);
  const suppressClickRef = useRef(false);
  brushRef.current = brush;

  const totals = useMemo(() => {
    if (!histogram) return { maxBucket: 0, succeeded: 0, errors: 0, active: 0, sum: 0 };
    let maxBucket = 0;
    let succeeded = 0;
    let errors = 0;
    let active = 0;
    for (const b of histogram.buckets) {
      const err = b.failed + b.cancelled;
      const total = b.succeeded + err + b.active;
      if (total > maxBucket) maxBucket = total;
      succeeded += b.succeeded;
      errors += err;
      active += b.active;
    }
    return { maxBucket, succeeded, errors, active, sum: succeeded + errors + active };
  }, [histogram]);

  useEffect(() => {
    if (!brush || !histogram) return undefined;
    const onPointerUp = () => {
      const activeBrush = brushRef.current;
      const wasDragged = draggedRef.current;
      setBrush(null);
      draggedRef.current = false;
      if (!activeBrush || !wasDragged) return;
      const lo = Math.min(activeBrush.anchor, activeBrush.current);
      const hi = Math.max(activeBrush.anchor, activeBrush.current);
      if (lo !== hi && histogram.buckets[lo] && histogram.buckets[hi]) {
        suppressClickRef.current = true;
        onZoomRange(histogram.buckets[lo].start_ms, histogram.buckets[hi].end_ms);
      }
    };
    window.addEventListener("pointerup", onPointerUp);
    window.addEventListener("pointercancel", onPointerUp);
    return () => {
      window.removeEventListener("pointerup", onPointerUp);
      window.removeEventListener("pointercancel", onPointerUp);
    };
  }, [brush != null, histogram, onZoomRange]);

  if (!histogram || totals.sum === 0) {
    return (
      <div className={`histogram histogram--empty${refreshing ? " histogram--refreshing" : ""}`} aria-label="Jobs time distribution">
        <div className="histogram__head">
          <div className="histogram__span">
            <span className="mono">{activeTimeSummary(timeFilter)}</span>
            {timeFilter.preset !== "all" ? (
              <button type="button" className="link-inline" onClick={onResetTime}>Reset time</button>
            ) : null}
          </div>
          <span className="muted">{histogram === null ? "Loading timeline…" : "No matching jobs in window"}</span>
        </div>
        <div className="histogram__bars histogram__bars--idle" style={{ gridTemplateColumns: `repeat(${PLACEHOLDER_COUNT}, minmax(0, 1fr))` }} aria-hidden="true">
          {Array.from({ length: PLACEHOLDER_COUNT }, (_, idx) => (
            <span key={idx} className="histogram__idle-track" />
          ))}
        </div>
        <div className="histogram__axis mono"><span>—</span><span>—</span></div>
      </div>
    );
  }

  const buckets = histogram.buckets;
  const spanMs = Math.max(1, histogram.end_ms - histogram.start_ms);
  const brushLo = brush ? Math.min(brush.anchor, brush.current) : null;
  const brushHi = brush ? Math.max(brush.anchor, brush.current) : null;

  const hoverBucket = hoveredIdx != null ? buckets[hoveredIdx] : undefined;
  const brushSummary =
    brushLo != null && brushHi != null && buckets[brushLo] && buckets[brushHi]
      ? (() => {
          let count = 0;
          for (let i = brushLo; i <= brushHi; i += 1) {
            const b = buckets[i];
            count += b.succeeded + b.failed + b.cancelled + b.active;
          }
          return `${formatAxisTime(buckets[brushLo].start_ms, spanMs)} – ${formatAxisTime(buckets[brushHi].end_ms, spanMs)} · ${formatNumber(count)} jobs`;
        })()
      : null;

  const resolveIndexFromEvent = (target: EventTarget | null, clientX: number): number | null => {
    const col = target instanceof Element ? target.closest<HTMLElement>("[data-bucket-index]") : null;
    if (col?.dataset.bucketIndex != null) {
      const idx = Number(col.dataset.bucketIndex);
      if (Number.isInteger(idx) && idx >= 0 && idx < buckets.length) return idx;
    }
    const rect = barsRef.current?.getBoundingClientRect();
    if (!rect || rect.width <= 0) return null;
    const rel = Math.max(0, Math.min(0.999999, (clientX - rect.left) / rect.width));
    return Math.floor(rel * buckets.length);
  };

  const axisIndices = buckets.length >= 4
    ? [0, Math.floor((buckets.length - 1) / 3), Math.floor(((buckets.length - 1) * 2) / 3), buckets.length - 1]
    : [0];

  return (
    <div className={`histogram${refreshing ? " histogram--refreshing" : ""}`} aria-label="Jobs time distribution">
      <div className="histogram__head">
        <div className="histogram__span">
          <span className="mono">
            {formatAxisTime(histogram.start_ms, spanMs)} — {formatAxisTime(histogram.end_ms, spanMs)}
          </span>
          <span className="muted">· {formatDuration(histogram.bucket_ms)}/bar</span>
          {timeFilter.preset !== "all" ? (
            <button type="button" className="link-inline" onClick={onResetTime}>Reset time</button>
          ) : null}
        </div>
        <div className="histogram__readout">
          {brushSummary ? (
            <span className="mono histogram__readout-active">Zoom {brushSummary}</span>
          ) : hoverBucket ? (
            <span className="mono histogram__readout-active">
              {formatAxisTime(hoverBucket.start_ms, spanMs)}–{formatAxisTime(hoverBucket.end_ms, spanMs)} ·{" "}
              {summarizeBucketCounts(hoverBucket.succeeded, hoverBucket.failed + hoverBucket.cancelled, hoverBucket.active)}
            </span>
          ) : (
            <span className="histogram__legend">
              {totals.succeeded > 0 ? <span><i className="histogram__dot histogram__dot--ok" />{formatNumber(totals.succeeded)} ok</span> : null}
              {totals.errors > 0 ? <span><i className="histogram__dot histogram__dot--err" />{formatNumber(totals.errors)} err</span> : null}
              {totals.active > 0 ? <span><i className="histogram__dot histogram__dot--act" />{formatNumber(totals.active)} active</span> : null}
              <span className="muted">Click or drag bar to zoom</span>
            </span>
          )}
        </div>
      </div>

      <div
        ref={barsRef}
        className="histogram__bars"
        style={{ gridTemplateColumns: `repeat(${buckets.length}, minmax(0, 1fr))` }}
        onPointerMove={(e) => {
          const idx = resolveIndexFromEvent(e.target, e.clientX);
          if (idx == null) return;
          setHoveredIdx(idx);
          if (brushRef.current && idx !== brushRef.current.current) {
            if (idx !== brushRef.current.anchor) draggedRef.current = true;
            setBrush({ anchor: brushRef.current.anchor, current: idx });
          }
        }}
        onPointerLeave={() => setHoveredIdx(null)}
      >
        {buckets.map((bucket, idx) => {
          const errors = bucket.failed + bucket.cancelled;
          const total = bucket.succeeded + errors + bucket.active;
          const heightPct = total === 0 || totals.maxBucket === 0 ? 0 : Math.max(8, Math.round((total / totals.maxBucket) * 100));
          const inBrush = brushLo != null && brushHi != null && idx >= brushLo && idx <= brushHi;
          const label = `${formatAxisTime(bucket.start_ms, spanMs)}–${formatAxisTime(bucket.end_ms, spanMs)}: ${summarizeBucketCounts(bucket.succeeded, errors, bucket.active)}`;

          return (
            <button
              key={idx}
              type="button"
              data-bucket-index={idx}
              className={`histogram__col${inBrush ? " is-brushed" : ""}${hoveredIdx === idx ? " is-hovered" : ""}${total === 0 ? " is-empty" : ""}`}
              aria-label={label}
              title={label}
              onPointerDown={(e) => {
                if (e.button !== 0) return;
                draggedRef.current = false;
                suppressClickRef.current = false;
                setBrush({ anchor: idx, current: idx });
              }}
              onClick={() => {
                if (suppressClickRef.current) {
                  suppressClickRef.current = false;
                  return;
                }
                onZoomRange(bucket.start_ms, bucket.end_ms);
              }}
            >
              {total > 0 ? (
                <span className="histogram__stack" style={{ height: `${heightPct}%` }}>
                  {bucket.succeeded > 0 ? <i className="histogram__seg histogram__seg--ok" style={{ flexGrow: bucket.succeeded }} /> : null}
                  {bucket.active > 0 ? <i className="histogram__seg histogram__seg--act" style={{ flexGrow: bucket.active }} /> : null}
                  {errors > 0 ? <i className="histogram__seg histogram__seg--err" style={{ flexGrow: errors }} /> : null}
                </span>
              ) : null}
            </button>
          );
        })}
      </div>

      <div className="histogram__axis mono">
        {axisIndices.map((bucketIdx, pos) => (
          <span key={`${bucketIdx}-${pos}`}>
            {formatAxisTime(pos === axisIndices.length - 1 ? histogram.end_ms : buckets[bucketIdx].start_ms, spanMs)}
          </span>
        ))}
      </div>
    </div>
  );
}
