import type { JobDetailResponse, JobSummary, RustDuration, WorkerSummary } from "./types";

const timeFormatter = new Intl.DateTimeFormat(undefined, {
  month: "short",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
});

const shortTimeFormatter = new Intl.DateTimeFormat(undefined, {
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
  hour12: false,
});

const dateTimeAxisFormatter = new Intl.DateTimeFormat(undefined, {
  month: "short",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
  hour12: false,
});

const numberFormatter = new Intl.NumberFormat();

export function sortJobs(jobs: JobSummary[]): JobSummary[] {
  return [...jobs].sort((a, b) => b.updated_at_ms - a.updated_at_ms);
}

export function sortWorkers(workers: WorkerSummary[]): WorkerSummary[] {
  return [...workers].sort((a, b) => {
    if (a.healthy !== b.healthy) return Number(b.healthy) - Number(a.healthy);
    if (a.connected !== b.connected) return Number(b.connected) - Number(a.connected);
    return b.last_seen_at_ms - a.last_seen_at_ms;
  });
}

export function formatTimestamp(value: number | null | undefined): string {
  if (!value) return "N/A";
  const date = new Date(value);
  const ms = String(date.getMilliseconds()).padStart(3, "0");
  return timeFormatter
    .formatToParts(date)
    .flatMap((part) => (part.type === "second" ? [part.value, ".", ms] : [part.value]))
    .join("");
}

export function formatShortTime(value: number | null | undefined): string {
  return value ? shortTimeFormatter.format(new Date(value)) : "—";
}

export function formatAxisTime(value: number, spanMs: number): string {
  if (!Number.isFinite(value)) return "—";
  return spanMs <= 6 * 3_600_000 ? shortTimeFormatter.format(new Date(value)) : dateTimeAxisFormatter.format(new Date(value));
}

export function toDatetimeLocal(ms: number): string {
  const d = new Date(ms);
  const pad = (n: number) => String(n).padStart(2, "0");
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
}

export function fromDatetimeLocal(raw: string): number | null {
  const trimmed = raw.trim();
  if (!trimmed) return null;
  const parsed = Date.parse(trimmed);
  return Number.isFinite(parsed) ? parsed : null;
}

export function formatDuration(value: number | null | undefined): string {
  if (value == null) return "N/A";
  if (value < 1_000) return `${value} ms`;
  const seconds = value / 1_000;
  if (seconds < 60) return `${seconds.toFixed(1)} s`;
  return `${Math.floor(seconds / 60)}m ${(seconds % 60).toFixed(0)}s`;
}

export function formatRustDuration(value: RustDuration | null | undefined): string {
  if (!value) return "Default";
  return formatDuration(Math.round(value.secs * 1_000 + value.nanos / 1_000_000));
}

export function formatMicros(value: number | null | undefined): string {
  if (value == null) return "N/A";
  return value < 1_000 ? `${formatNumber(value)} µs` : `${(value / 1_000).toFixed(2)} ms`;
}

export function formatBytes(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return "N/A";
  if (value < 1024) return `${value} B`;
  const kib = value / 1024;
  if (kib < 1024) return `${kib.toFixed(1)} KiB`;
  const mib = kib / 1024;
  return mib < 1024 ? `${mib.toFixed(1)} MiB` : `${(mib / 1024).toFixed(2)} GiB`;
}

export function formatPercent(ratio: number | null | undefined): string {
  if (ratio == null || !Number.isFinite(ratio)) return "0%";
  const pct = Math.max(0, Math.min(100, ratio * 100));
  return pct >= 10 || pct === 0 ? `${Math.round(pct)}%` : `${pct.toFixed(1)}%`;
}

export function budgetPercent(used: number, max: number): number {
  if (!Number.isFinite(used) || !Number.isFinite(max) || max <= 0) return 0;
  return Math.max(0, Math.min(100, (used / max) * 100));
}

export function formatNumber(value: number): string {
  return numberFormatter.format(value);
}

export function formatCapacityMap(value: Record<string, string | number>): string {
  const entries = Object.entries(value);
  return entries.length === 0 ? "N/A" : entries.map(([k, v]) => `${k}:${v}`).join(" ");
}

export function formatPool(pool: string | null | undefined): string {
  return pool?.trim() || "default";
}

export function formatObjectMeta(value: { size_bytes: number | null; metadata: Record<string, string> }): string {
  const parts: string[] = [];
  if (value.size_bytes != null) parts.push(`${formatNumber(value.size_bytes)} B`);
  const meta = Object.entries(value.metadata)
    .filter(([k]) => k !== "content_type")
    .map(([k, v]) => `${k}:${v}`);
  if (meta.length > 0) parts.push(meta.join(" "));
  return parts.join(" · ") || "No metadata";
}

export function latestAttemptWorker(detail: JobDetailResponse): string | null {
  for (let i = detail.attempts.length - 1; i >= 0; i -= 1) {
    if (detail.attempts[i]?.worker_id) return detail.attempts[i].worker_id;
  }
  return null;
}

export function asErrorMessage(error: unknown): string {
  return error instanceof Error ? error.message : "Unknown error";
}

/** Compact primitive JSON arrays inline with spaces so long vectors wrap cleanly at commas. */
function formatJsonValue(value: unknown, indent = 0): string {
  if (Array.isArray(value)) {
    const isPrimitiveArray = value.every((v) => v === null || typeof v !== "object");
    if (isPrimitiveArray && value.length <= 16) return `[${value.map((v) => JSON.stringify(v)).join(", ")}]`;
    if (value.length === 0) return "[]";
    const pad = "  ".repeat(indent + 1);
    if (isPrimitiveArray) {
      const sample = value.slice(0, 12).map((v) => JSON.stringify(v)).join(", ");
      return `[${sample}, … (${value.length} items)]`;
    }
    return `[\n${value.map((v) => `${pad}${formatJsonValue(v, indent + 1)}`).join(",\n")}\n${"  ".repeat(indent)}]`;
  }
  if (value && typeof value === "object") {
    const entries = Object.entries(value as Record<string, unknown>);
    if (entries.length === 0) return "{}";
    const pad = "  ".repeat(indent + 1);
    return `{\n${entries
      .map(([k, v]) => `${pad}${JSON.stringify(k)}: ${formatJsonValue(v, indent + 1)}`)
      .join(",\n")}\n${"  ".repeat(indent)}}`;
  }
  return JSON.stringify(value);
}

export interface FormattedPreview {
  text: string;
  streamText?: string;
  frameCount?: number;
  kind?: "json" | "sse";
}

export function formatPayloadPreview(raw: string, contentType?: string | null): FormattedPreview {
  const trimmed = raw.trim();
  const lines = trimmed.split(/\r?\n/).map((line) => line.trim()).filter(Boolean);
  const sseLines = lines.filter((line) => line.startsWith("data:"));
  if (sseLines.length > 0 && sseLines.length >= lines.length / 2) {
    let tokens = "";
    for (const line of sseLines) {
      const payload = line.slice(5).trim();
      if (!payload || payload === "[DONE]") continue;
      try {
        const chunk = JSON.parse(payload) as {
          choices?: Array<{ delta?: { content?: string }; text?: string }>;
        };
        for (const c of chunk.choices ?? []) {
          tokens += c.delta?.content ?? c.text ?? "";
        }
      } catch {
        // Trailing partial frame after preview truncation.
      }
    }
    return {
      text: sseLines.join("\n"),
      streamText: tokens || undefined,
      frameCount: sseLines.length,
      kind: "sse",
    };
  }
  if (contentType?.includes("json") || trimmed.startsWith("{") || trimmed.startsWith("[")) {
    try {
      return { text: formatJsonValue(JSON.parse(trimmed)), kind: "json" };
    } catch {
      // Truncated or non-JSON body; preserve raw string.
    }
  }
  return { text: raw };
}
