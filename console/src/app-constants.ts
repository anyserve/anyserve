import type { JobState } from "./types";

export type ConsoleTab = "overview" | "jobs" | "interfaces" | "pools" | "workers" | "explore";
export type JobScope = "all" | "queue" | "history";
export type Theme = "light" | "dark";
export type PaneStat = { label: string; value: number | string };

export const TAB_PATHS: Record<ConsoleTab, string> = {
  overview: "/",
  jobs: "/jobs",
  interfaces: "/interfaces",
  pools: "/pools",
  workers: "/workers",
  explore: "/explore",
};

export const JOB_SCOPE_PATHS: Record<JobScope, string> = {
  all: "/jobs",
  queue: "/queue",
  history: "/history",
};

export const QUEUE_STATES: JobState[] = ["queued", "running"];
export const HISTORY_STATES: JobState[] = ["succeeded", "failed", "cancelled"];
export const ALL_JOB_STATES: JobState[] = [];

export const OVERVIEW_POLL_MS = 1_000;
export const JOBS_ACTIVE_POLL_MS = 800;
export const JOBS_HISTORY_POLL_MS = 2_000;
export const DETAIL_POLL_MS = 1_000;
export const WORKERS_POLL_MS = 2_000;

export function routeFromPathname(rawPathname: string): { tab: ConsoleTab; jobScope?: JobScope } {
  const pathname = rawPathname.replace(/\/+$/, "") || "/";
  if (pathname === "/queue") return { tab: "jobs", jobScope: "queue" };
  if (pathname === "/history") return { tab: "jobs", jobScope: "history" };
  if (pathname === "/jobs") return { tab: "jobs", jobScope: "all" };
  const match = (Object.entries(TAB_PATHS) as Array<[ConsoleTab, string]>).find(([, path]) => pathname === path);
  return { tab: match?.[0] ?? "overview" };
}
