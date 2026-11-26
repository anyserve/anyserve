import type { JobState, OverviewResponse } from "./types";

export type ConsoleTab = "overview" | "jobs" | "interfaces" | "pools" | "workers" | "explore" | "playground";
export type JobScope = "all" | "queue" | "history";
export type JobStatusFilter = "all" | JobState;
export type TimePreset = "all" | "5m" | "15m" | "1h" | "6h" | "24h" | "custom";
export type Theme = "light" | "dark";

export interface TimeFilterState {
  preset: TimePreset;
  fromMs: number | null;
  toMs: number | null;
}

export const DEFAULT_TIME_FILTER: TimeFilterState = { preset: "all", fromMs: null, toMs: null };

export interface PaneStat {
  label: string;
  value: number | string;
  state?: JobStatusFilter;
  active?: boolean;
  tone?: "ok" | "warn" | "danger";
}

export const VALID_JOB_STATES: readonly JobState[] = ["queued", "running", "succeeded", "failed", "cancelled"];
export const QUEUE_STATES: JobState[] = ["queued", "running"];
export const HISTORY_STATES: JobState[] = ["succeeded", "failed", "cancelled"];
export const ALL_JOB_STATES: JobState[] = [];

export const TAB_PATHS: Record<ConsoleTab, string> = {
  overview: "/",
  jobs: "/jobs",
  interfaces: "/interfaces",
  pools: "/pools",
  workers: "/workers",
  explore: "/explore",
  playground: "/playground",
};

export const JOB_SCOPE_PATHS: Record<JobScope, string> = {
  all: "/jobs",
  queue: "/queue",
  history: "/history",
};

export const OVERVIEW_POLL_MS = 1_000;
export const JOBS_ACTIVE_POLL_MS = 800;
export const JOBS_HISTORY_POLL_MS = 2_000;
export const DETAIL_POLL_MS = 1_000;
export const WORKERS_POLL_MS = 2_000;

export const routePath = (tab: ConsoleTab, scope: JobScope): string =>
  tab === "jobs" ? JOB_SCOPE_PATHS[scope] : TAB_PATHS[tab];

export function routeFromPathname(rawPathname: string): { tab: ConsoleTab; jobScope?: JobScope } {
  const pathname = rawPathname.replace(/\/+$/, "") || "/";
  if (pathname === "/queue") return { tab: "jobs", jobScope: "queue" };
  if (pathname === "/history") return { tab: "jobs", jobScope: "history" };
  if (pathname === "/jobs") return { tab: "jobs", jobScope: "all" };
  const match = (Object.entries(TAB_PATHS) as Array<[ConsoleTab, string]>).find(([, path]) => pathname === path);
  return { tab: match?.[0] ?? "overview" };
}

export interface ParsedConsoleUrl {
  tab: ConsoleTab;
  jobScope: JobScope;
  status: JobStatusFilter;
  iface: string;
  ns: string;
  worker: string;
  q: string;
  time: TimeFilterState;
  pinnedJobId: string | null;
  selectedInterface: string | null;
  selectedPoolKey: string | null;
  selectedWorkerId: string | null;
}

const VALID_TIME_PRESETS: readonly TimePreset[] = ["all", "5m", "15m", "1h", "6h", "24h", "custom"];

export function parseUrlState(pathname: string, search: string): ParsedConsoleUrl {
  const route = routeFromPathname(pathname);
  let jobScope: JobScope = route.jobScope ?? "all";
  const params = new URLSearchParams(search);

  const rawState = params.get("state");
  let status: JobStatusFilter =
    rawState && (VALID_JOB_STATES as readonly string[]).includes(rawState) ? (rawState as JobState) : "all";
  if (status !== "all") {
    if (jobScope === "queue" && !QUEUE_STATES.includes(status)) jobScope = "all";
    if (jobScope === "history" && !HISTORY_STATES.includes(status)) jobScope = "all";
  }

  const rawTime = params.get("time");
  const fromRaw = params.get("from");
  const toRaw = params.get("to");
  const fromMs = fromRaw && /^\d+$/.test(fromRaw) ? Number(fromRaw) : null;
  const toMs = toRaw && /^\d+$/.test(toRaw) ? Number(toRaw) : null;

  let time: TimeFilterState = DEFAULT_TIME_FILTER;
  if (rawTime === "custom" && fromMs != null && toMs != null && fromMs < toMs) {
    time = { preset: "custom", fromMs, toMs };
  } else if (rawTime && rawTime !== "custom" && (VALID_TIME_PRESETS as readonly string[]).includes(rawTime)) {
    time = { preset: rawTime as TimePreset, fromMs: null, toMs: null };
  } else if (fromMs != null && toMs != null && fromMs < toMs) {
    time = { preset: "custom", fromMs, toMs };
  }

  return {
    tab: route.tab,
    jobScope,
    status,
    iface: params.get("iface")?.trim() || "all",
    ns: params.get("ns")?.trim() || "all",
    worker: params.get("worker")?.trim() || "all",
    q: params.get("q") ?? "",
    time,
    pinnedJobId: params.get("job")?.trim() || null,
    selectedInterface: route.tab === "interfaces" ? params.get("iface")?.trim() || null : null,
    selectedPoolKey: route.tab === "pools" ? params.get("pool")?.trim() || null : null,
    selectedWorkerId: route.tab === "workers" ? params.get("worker")?.trim() || null : null,
  };
}

export function buildUrlSearch(input: {
  tab: ConsoleTab;
  status: JobStatusFilter;
  iface: string;
  ns: string;
  worker: string;
  q: string;
  time: TimeFilterState;
  pinnedJobId: string | null;
  selectedInterface: string | null;
  selectedPoolKey: string | null;
  selectedWorkerId: string | null;
}): string {
  const params = new URLSearchParams();
  if (input.tab === "jobs") {
    if (input.status !== "all") params.set("state", input.status);
    if (input.iface !== "all") params.set("iface", input.iface);
    if (input.ns !== "all") params.set("ns", input.ns);
    if (input.worker !== "all") params.set("worker", input.worker);
    if (input.q.trim()) params.set("q", input.q.trim());
    if (input.time.preset === "custom" && input.time.fromMs != null && input.time.toMs != null) {
      params.set("time", "custom");
      params.set("from", String(input.time.fromMs));
      params.set("to", String(input.time.toMs));
    } else if (input.time.preset !== "all") {
      params.set("time", input.time.preset);
    }
    if (input.pinnedJobId) params.set("job", input.pinnedJobId);
  } else if (input.tab === "interfaces" && input.selectedInterface) {
    params.set("iface", input.selectedInterface);
  } else if (input.tab === "pools" && input.selectedPoolKey) {
    params.set("pool", input.selectedPoolKey);
  } else if (input.tab === "workers" && input.selectedWorkerId) {
    params.set("worker", input.selectedWorkerId);
  }
  const qs = params.toString();
  return qs ? `?${qs}` : "";
}

export function scopeMap(scope: JobScope, id: string | null): Record<JobScope, string | null> {
  return {
    all: scope === "all" ? id : null,
    queue: scope === "queue" ? id : null,
    history: scope === "history" ? id : null,
  };
}

export function resolveEffectiveStates(scope: JobScope, status: JobStatusFilter): JobState[] {
  if (status !== "all") return [status];
  if (scope === "queue") return QUEUE_STATES;
  if (scope === "history") return HISTORY_STATES;
  return ALL_JOB_STATES;
}

export function buildJobStats(scope: JobScope, status: JobStatusFilter, overview: OverviewResponse | null): PaneStat[] {
  const q = overview?.queue;
  const h = overview?.history;
  const stat = (label: string, value: number, state: JobStatusFilter, tone?: PaneStat["tone"]): PaneStat => ({
    label,
    value,
    state,
    active: status === state,
    ...(tone ? { tone } : {}),
  });
  if (scope === "queue") {
    return [stat("Active", q?.active ?? 0, "all"), stat("Queued", q?.queued ?? 0, "queued", "warn"), stat("Running", q?.running ?? 0, "running", "warn")];
  }
  const outcomes = [
    stat("Succeeded", h?.succeeded ?? 0, "succeeded", "ok"),
    stat("Failed", h?.failed ?? 0, "failed", "danger"),
    stat("Cancelled", h?.cancelled ?? 0, "cancelled", "danger"),
  ];
  return scope === "history"
    ? [...outcomes, stat("Total", h?.total ?? 0, "all")]
    : [stat("Queued", q?.queued ?? 0, "queued", "warn"), stat("Running", q?.running ?? 0, "running", "warn"), ...outcomes];
}
