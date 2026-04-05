import { TAB_PATHS, type ConsoleTab } from "./app-constants";
import type { JobDetailResponse, JobSummary, WorkerSummary } from "./types";

const timeFormatter = new Intl.DateTimeFormat(undefined, {
  month: "short",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
});

export function sortJobs(jobs: JobSummary[]): JobSummary[] {
  return [...jobs].sort((left, right) => right.updated_at_ms - left.updated_at_ms);
}

export function sortWorkers(workers: WorkerSummary[]): WorkerSummary[] {
  return [...workers].sort((left, right) => {
    if (left.healthy !== right.healthy) {
      return Number(right.healthy) - Number(left.healthy);
    }
    return right.last_seen_at_ms - left.last_seen_at_ms;
  });
}

export function formatTimestamp(value: number | null): string {
  if (!value) {
    return "N/A";
  }
  const date = new Date(value);
  const milliseconds = String(date.getMilliseconds()).padStart(3, "0");
  const parts = timeFormatter.formatToParts(date);

  return parts
    .flatMap((part) =>
      part.type === "second" ? [part.value, ".", milliseconds] : [part.value],
    )
    .join("");
}

export function formatDuration(value: number | null): string {
  if (value == null) {
    return "N/A";
  }
  if (value < 1_000) {
    return `${value} ms`;
  }
  const seconds = value / 1_000;
  if (seconds < 60) {
    return `${seconds.toFixed(1)} s`;
  }
  const minutes = Math.floor(seconds / 60);
  return `${minutes}m ${(seconds % 60).toFixed(0)}s`;
}

export function formatNumber(value: number): string {
  return new Intl.NumberFormat().format(value);
}

export function formatCapacityMap(value: Record<string, string | number>): string {
  const entries = Object.entries(value);
  if (entries.length === 0) {
    return "N/A";
  }
  return entries.map(([key, item]) => `${key}:${item}`).join(" ");
}

export function formatObjectMeta(value: {
  size_bytes: number | null;
  metadata: Record<string, string>;
}): string {
  const parts: string[] = [];
  if (value.size_bytes != null) {
    parts.push(`${formatNumber(value.size_bytes)} B`);
  }
  const metadataEntries = Object.entries(value.metadata)
    .filter(([key]) => key !== "content_type")
    .map(([key, item]) => `${key}:${item}`);
  if (metadataEntries.length > 0) {
    parts.push(metadataEntries.join(" "));
  }
  return parts.join(" · ") || "No metadata";
}

export function truncateMiddle(value: string, maxLength: number): string {
  if (value.length <= maxLength) {
    return value;
  }
  const half = Math.max(4, Math.floor((maxLength - 1) / 2));
  return `${value.slice(0, half)}…${value.slice(-half)}`;
}

export function latestAttemptWorker(detail: JobDetailResponse): string | null {
  for (let index = detail.attempts.length - 1; index >= 0; index -= 1) {
    const workerId = detail.attempts[index]?.worker_id;
    if (workerId) {
      return workerId;
    }
  }
  return null;
}

export function asErrorMessage(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }
  return "Unknown error";
}

export function readTabFromLocation(): ConsoleTab {
  const pathname = window.location.pathname.replace(/\/+$/, "") || "/";
  const match = (Object.entries(TAB_PATHS) as Array<[ConsoleTab, string]>).find(
    ([, path]) => pathname === path,
  );
  return match?.[0] ?? "queue";
}
