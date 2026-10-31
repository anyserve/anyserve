import type { JobState } from "./types";

export type ConsoleTab = "queue" | "workers" | "history" | "explore";

export const CONSOLE_TABS: ConsoleTab[] = ["queue", "workers", "history", "explore"];

export const TAB_LABELS: Record<ConsoleTab, string> = {
  queue: "Queue",
  workers: "Workers",
  history: "History",
  explore: "Explore",
};

export const TAB_PATHS: Record<ConsoleTab, string> = {
  queue: "/queue",
  workers: "/workers",
  history: "/history",
  explore: "/explore",
};

export const QUEUE_STATES: JobState[] = ["queued", "running"];
export const HISTORY_STATES: JobState[] = ["succeeded", "failed", "cancelled"];

export const QUEUE_POLL_MS = 800;
export const OVERVIEW_POLL_MS = 1_000;
export const DETAIL_POLL_MS = 1_000;
export const WORKERS_POLL_MS = 2_000;
export const HISTORY_POLL_MS = 2_000;

export type PaneStat = {
  label: string;
  value: number;
};
