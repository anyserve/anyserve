// @vitest-environment jsdom
import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import App from "./App";
import { cancelJob, getJob, getJobs, getOverview, getWorkers } from "./api";
import type { JobDetailResponse, JobSummary } from "./types";

vi.mock("./api", () => ({
  AuthenticationError: class extends Error {},
  cancelJob: vi.fn(), getJob: vi.fn(), getJobs: vi.fn(), getOverview: vi.fn(), getWorkers: vi.fn(), setConsoleToken: vi.fn(),
}));

const summary = (id: string): JobSummary => ({
  job_id: id, state: "queued", interface_name: "echo", source: null,
  created_at_ms: 1, updated_at_ms: id === "A" ? 2 : 1, duration_ms: null,
  current_attempt_id: null, latest_worker_id: null, attempt_count: 0, last_error: null, metadata: {},
});
const detail = (id: string): JobDetailResponse => ({
  job: { ...summary(id), demand: null, policy: null, params_size_bytes: 0, params_preview: null,
    params_truncated: false, output_preview: null, inputs: [], outputs: [] },
  attempts: [], events: [], latest_worker: null, history_incomplete: false,
});

let replies: Map<string, (value: JobDetailResponse) => void>;
beforeEach(() => {
  vi.resetAllMocks();
  window.history.replaceState(null, "", "/queue");
  replies = new Map();
  vi.mocked(getJobs).mockImplementation(async (states) => ({ jobs: states.includes("queued") ? [summary("A"), summary("B")] : [], next_page_token: null }));
  vi.mocked(getJob).mockImplementation((id) => new Promise(resolve => replies.set(id, resolve)));
  vi.mocked(getWorkers).mockResolvedValue({ workers: [], total: 0, healthy: 0 });
  vi.mocked(getOverview).mockResolvedValue({
    runtime: { code: "memory", label: "Memory", journal: "disabled" },
    queue: { queued: 2, running: 0, active: 2 },
    history: { succeeded: 0, failed: 0, cancelled: 0, total: 0 },
    workers: { total: 0, healthy: 0, unavailable: 0, active_leases: 0, total_capacity: {}, last_seen_at_ms: null },
    interfaces: [], refreshed_at_ms: 1,
  });
});
afterEach(() => { cleanup(); vi.useRealTimers(); });

it("keeps the selected job's detail and cancel target when an older response arrives late", async () => {
  render(<App />);
  await waitFor(() => expect(replies.has("A")).toBe(true));
  const oldSignal = vi.mocked(getJob).mock.calls[0][1];
  fireEvent.click(screen.getByText("B"));
  await waitFor(() => expect(replies.has("B")).toBe(true));
  expect(oldSignal?.aborted).toBe(true);
  await act(async () => replies.get("B")!(detail("B")));
  await act(async () => replies.get("A")!(detail("A")));
  expect(screen.getByRole("heading", { name: "B" })).toBeTruthy();
  expect(screen.queryByRole("heading", { name: "A" })).toBeNull();
  vi.mocked(cancelJob).mockResolvedValue({ job: { ...summary("B"), state: "cancelled" } });
  fireEvent.click(screen.getByRole("button", { name: "Cancel job" }));
  await waitFor(() => expect(cancelJob).toHaveBeenCalledWith("B"));
});

it("hides the previous detail and cancel action while loading a new selection", async () => {
  render(<App />);
  await waitFor(() => expect(replies.has("A")).toBe(true));
  await act(async () => replies.get("A")!(detail("A")));
  expect(screen.getByRole("button", { name: "Cancel job" })).toBeTruthy();
  fireEvent.click(screen.getByText("B"));
  expect(screen.queryByRole("heading", { name: "A" })).toBeNull();
  expect(screen.queryByRole("button", { name: "Cancel job" })).toBeNull();
});

it("polls the active pane and fetches completed details only once per selection", async () => {
  vi.useFakeTimers();
  vi.mocked(getJobs).mockImplementation(async (states) => ({
    jobs: states.includes("queued") ? [summary("A")] : [{ ...summary("B"), state: "succeeded" }],
    next_page_token: null,
  }));
  vi.mocked(getJob).mockImplementation(async (id) => ({
    ...detail(id), job: { ...detail(id).job, state: id === "B" ? "succeeded" : "queued" },
  }));
  render(<App />);
  await act(async () => {});
  expect(vi.mocked(getJobs).mock.calls.every(([states]) => states.includes("queued"))).toBe(true);
  expect(getWorkers).not.toHaveBeenCalled();
  fireEvent.click(screen.getByRole("button", { name: /History/ }));
  await act(async () => {});
  vi.mocked(getJobs).mockClear();
  vi.mocked(getJob).mockClear();
  await act(() => vi.advanceTimersByTimeAsync(60_000));
  expect(getJobs).toHaveBeenCalledTimes(30);
  expect(vi.mocked(getJobs).mock.calls.every(([states]) => states.includes("succeeded"))).toBe(true);
  expect(getWorkers).not.toHaveBeenCalled();
  expect(getJob).not.toHaveBeenCalled();
  fireEvent.click(screen.getByRole("button", { name: /Workers/ }));
  await act(async () => {});
  vi.mocked(getJobs).mockClear();
  await act(() => vi.advanceTimersByTimeAsync(4_000));
  expect(getWorkers).toHaveBeenCalledTimes(3);
  expect(getJobs).not.toHaveBeenCalled();
  fireEvent.click(screen.getByRole("button", { name: /History/ }));
  await act(async () => {});
  expect(getJob).toHaveBeenCalledTimes(1);
  expect(getJob).toHaveBeenCalledWith("B", expect.any(AbortSignal));
});
