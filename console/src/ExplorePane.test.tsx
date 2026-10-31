// @vitest-environment jsdom
import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import ExplorePane from "./ExplorePane";
import { getAnalyticsJobs, getJob } from "./api";
import { AnalyticsEngine } from "./analytics-engine";
import type { AnalyticsJob, AnalyticsResponse } from "./types";

vi.mock("./api", () => ({ AuthenticationError: class extends Error {}, getAnalyticsJobs: vi.fn(), getJob: vi.fn() }));
vi.mock("./analytics-engine", () => ({ AnalyticsEngine: { create: vi.fn() } }));
const job: AnalyticsJob = {
  job_id: "A", state: "succeeded", interface_name: "echo", source: null,
  created_at_ms: 1, updated_at_ms: 2, duration_ms: 1, current_attempt_id: null,
  latest_worker_id: null, attempt_count: 1, last_error: null, metadata: {}, durability: "durable",
};
let db: { append: ReturnType<typeof vi.fn>; query: ReturnType<typeof vi.fn>; dispose: ReturnType<typeof vi.fn>; closed: boolean };
beforeEach(() => {
  vi.resetAllMocks();
  db = { append: vi.fn().mockResolvedValue(undefined), query: vi.fn().mockResolvedValue({
    columns: ["job_id", "state"], rows: [["A", "succeeded"], [null, null]], elapsedMs: 2, truncated: false, clippedCells: 0,
  }), dispose: vi.fn(), closed: false };
  vi.mocked(AnalyticsEngine.create).mockResolvedValue(db as unknown as AnalyticsEngine);
  vi.mocked(getAnalyticsJobs).mockResolvedValue({ jobs: [job], next_cursor: null });
});
afterEach(cleanup);

it("loads pages on demand, queries the whole dataset, and retains null result rows", async () => {
  vi.mocked(getAnalyticsJobs).mockResolvedValueOnce({ jobs: [job], next_cursor: "cursor" })
    .mockResolvedValueOnce({ jobs: [{ ...job, job_id: "B" }], next_cursor: null });
  render(<ExplorePane journalEnabled onAuthenticationError={vi.fn()} />);
  expect(AnalyticsEngine.create).not.toHaveBeenCalled();
  fireEvent.click(screen.getByRole("button", { name: "Load data" }));
  await waitFor(() => expect(screen.getAllByText("NULL")).toHaveLength(2));
  expect(getAnalyticsJobs).toHaveBeenCalledTimes(2);
  const calls = vi.mocked(getAnalyticsJobs).mock.calls;
  expect(calls[0].slice(0, 3)).toEqual(calls[1].slice(0, 3));
  expect(calls[1][4]).toBe("cursor"); expect(db.append).toHaveBeenCalledTimes(2);
  expect(screen.getByRole("status").textContent).toContain("Loaded 2 jobs");
  fireEvent.change(screen.getByLabelText("Query template"), { target: { value: "Interface performance" } });
  fireEvent.click(screen.getByRole("button", { name: "Run query" }));
  await waitFor(() => expect(db.query).toHaveBeenCalledTimes(2));
  expect(db.query.mock.calls[1][0]).toContain("quantile_cont");
  expect(getAnalyticsJobs).toHaveBeenCalledTimes(2);
  fireEvent.change(screen.getByRole("searchbox"), { target: { value: "missing" } });
  expect(screen.getByText("No matching rows.")).toBeTruthy();
  expect(getJob).not.toHaveBeenCalled();
});

it("cancels a pending load immediately and ignores late responses", async () => {
  let resolve!: (value: AnalyticsResponse) => void;
  vi.mocked(getAnalyticsJobs).mockImplementationOnce(() => new Promise(done => { resolve = done; }));
  const view = render(<ExplorePane journalEnabled onAuthenticationError={vi.fn()} />);
  fireEvent.click(screen.getByRole("button", { name: "Load data" }));
  await waitFor(() => expect(getAnalyticsJobs).toHaveBeenCalled());
  const signal = vi.mocked(getAnalyticsJobs).mock.calls[0][5];
  fireEvent.click(screen.getByRole("button", { name: "Cancel" }));
  expect(signal.aborted).toBe(true); expect(db.dispose).toHaveBeenCalled();
  await act(async () => resolve({ jobs: [job], next_cursor: null }));
  expect(db.append).not.toHaveBeenCalled(); expect(db.query).not.toHaveBeenCalled();
  expect((screen.getByRole("button", { name: "Load data" }) as HTMLButtonElement).disabled).toBe(false);
  fireEvent.click(screen.getByRole("button", { name: "Load data" }));
  await waitFor(() => expect(db.query).toHaveBeenCalledTimes(1));
  const disposals = db.dispose.mock.calls.length;
  view.unmount(); expect(db.dispose).toHaveBeenCalledTimes(disposals + 1);
});

it("marks a row-limited dataset as partial", async () => {
  let page = 0;
  vi.mocked(getAnalyticsJobs).mockImplementation(async () => ({ jobs: Array.from({ length: 250 }, (_, id) => ({ ...job, job_id: `${page}-${id}` })), next_cursor: String(++page) }));
  render(<ExplorePane journalEnabled onAuthenticationError={vi.fn()} />);
  fireEvent.change(screen.getByLabelText("Maximum jobs"), { target: { value: "1000" } });
  fireEvent.click(screen.getByRole("button", { name: "Load data" }));
  await waitFor(() => expect(screen.getByText(/This dataset is partial/)).toBeTruthy());
  expect(getAnalyticsJobs).toHaveBeenCalledTimes(4);
  expect(screen.getByRole("status").textContent).toContain("1,000 jobs");
});

it("releases the database and shows a retryable state when metadata loading fails", async () => {
  vi.mocked(getAnalyticsJobs).mockRejectedValue(new Error("Server unavailable"));
  render(<ExplorePane journalEnabled onAuthenticationError={vi.fn()} />);
  fireEvent.click(screen.getByRole("button", { name: "Load data" }));
  await waitFor(() => expect(screen.getByRole("alert").textContent).toContain("Server unavailable"));
  expect(screen.getByRole("status").textContent).toContain("Data load failed");
  expect(db.dispose).toHaveBeenCalledTimes(1);
  expect((screen.getByRole("button", { name: "Run query" }) as HTMLButtonElement).disabled).toBe(true);
});
