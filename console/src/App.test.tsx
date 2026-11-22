// @vitest-environment jsdom
import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import App from "./App";
import {
  cancelJob,
  clearConsoleToken,
  getJob,
  getJobs,
  getOverview,
  getWorkers,
  hasConsoleToken,
  setConsoleToken,
} from "./api";
import type { JobDetailResponse, JobSummary, OverviewResponse, WorkersResponse } from "./types";

vi.mock("./api", () => {
  let token: string | null = null;
  return {
    AuthenticationError: class extends Error {},
    cancelJob: vi.fn(),
    getJob: vi.fn(),
    getJobs: vi.fn(),
    getOverview: vi.fn(),
    getWorkers: vi.fn(),
    setConsoleToken: vi.fn((next: string) => {
      token = next.trim();
    }),
    getConsoleToken: vi.fn(() => token),
    hasConsoleToken: vi.fn(() => Boolean(token && token.length > 0)),
    clearConsoleToken: vi.fn(() => {
      token = null;
    }),
  };
});

const summary = (id: string, overrides: Partial<JobSummary> = {}): JobSummary => ({
  job_id: id,
  state: "queued",
  interface_name: "echo",
  source: null,
  created_at_ms: 1,
  updated_at_ms: id === "A" ? 2 : 1,
  duration_ms: null,
  current_attempt_id: null,
  latest_worker_id: null,
  attempt_count: 0,
  last_error: null,
  metadata: {},
  ...overrides,
});

const detail = (id: string, overrides: Partial<JobDetailResponse["job"]> = {}): JobDetailResponse => ({
  job: {
    ...summary(id),
    demand: null,
    policy: null,
    params_size_bytes: 0,
    params_preview: null,
    params_truncated: false,
    output_preview: null,
    inputs: [],
    outputs: [],
    ...overrides,
  },
  attempts: [],
  events: [],
  latest_worker: null,
  history_incomplete: false,
});

const minimalOverview = (): OverviewResponse => ({
  runtime: { code: "memory", label: "Memory", journal: "disabled" },
  queue: { queued: 2, running: 0, active: 2 },
  history: { succeeded: 0, failed: 0, cancelled: 0, total: 0 },
  workers: {
    total: 0,
    healthy: 0,
    unavailable: 0,
    active_leases: 0,
    total_capacity: {},
    last_seen_at_ms: null,
  },
  interfaces: [],
  refreshed_at_ms: 1,
});

const richOverview = (): OverviewResponse => ({
  runtime: { code: "memory_plus_sqlite", label: "Memory + SQLite journal", journal: "sqlite" },
  queue: { queued: 3, running: 2, active: 5 },
  history: { succeeded: 12, failed: 1, cancelled: 0, total: 13 },
  workers: {
    total: 3,
    healthy: 2,
    unavailable: 1,
    draining: 1,
    active_leases: 4,
    total_capacity: { gpu: 4, cpu: 16 },
    last_seen_at_ms: 1_700_000_000_000,
  },
  interfaces: [
    {
      name: "llm.generate",
      queued: 3,
      running: 2,
      oldest_queued_age_ms: 420,
      sessions: 2,
      free_slots: 6,
      policy: {
        max_queue: 200,
        queue_timeout_ms: 30_000,
        max_in_flight_per_key: 4,
        pool_tiers: [
          { pool: "", spillover_after_ms: 0, reconnect_grace_ms: 2_000 },
          { pool: "gpu-burst", spillover_after_ms: 500, reconnect_grace_ms: 5_000 },
        ],
      },
    },
  ],
  refreshed_at_ms: 1_700_000_005_000,
  health: {
    live: true,
    ready: true,
    journal: { status: "healthy", message: null },
  },
  control_plane: {
    grpc_listen: "127.0.0.1:50052",
    grpc_tls_enabled: true,
    console_listen: "127.0.0.1:3001",
    auth_enabled: true,
    generation: 7,
    retained_jobs: 18,
    sessions: 2,
    max_sessions: 4096,
    priority_levels: 4,
    default_max_attempts: 3,
    heartbeat_interval_ms: 5000,
    heartbeat_timeout_ms: 15000,
    reconnect_grace_ms: 3000,
  },
  budgets: {
    input_bytes: 2048,
    max_pending_bytes: 16384,
    result_bytes: 1024,
    max_result_bytes: 8192,
    event_bytes: 512,
    max_event_bytes: 4096,
  },
  journal_stats: {
    pending_ops: 1,
    committed_batches: 42,
    committed_ops: 310,
    last_batch_size: 8,
    last_commit_micros: 650,
  },
  pools: [
    {
      namespace: "tenant-a",
      interface: "llm.generate",
      pool: "gpu-primary",
      sessions: 2,
      free_slots: 6,
      active_leases: 2,
      desired_additional_slots: 3,
    },
    {
      namespace: "default",
      interface: "echo",
      pool: "",
      sessions: 1,
      free_slots: 2,
      active_leases: 0,
      desired_additional_slots: 0,
    },
  ],
});

const richWorkers = (): WorkersResponse => ({
  total: 3,
  healthy: 1,
  draining: 1,
  disconnected: 1,
  workers: [
    {
      worker_id: "worker-gpu-01",
      session_id: "sess-111",
      namespace: "tenant-a",
      pool: "gpu-primary",
      healthy: true,
      interfaces: ["llm.generate"],
      attributes: { arch: "cuda", zone: "us-east" },
      total_capacity: { gpu: 2 },
      max_concurrency: 3,
      free_slots: 2,
      utilization: 0,
      connected: true,
      draining: false,
      active_leases: 1,
      connected_at_ms: 1_700_000_000_000,
      last_seen_at_ms: 1_700_000_004_000,
      load: { queued: 1, running: 2, utilization: 0, custom: { vram_gb: 24 } },
    },
    {
      worker_id: "worker-drain-02",
      session_id: "sess-222",
      namespace: "tenant-a",
      pool: "gpu-burst",
      healthy: false,
      interfaces: ["llm.generate"],
      attributes: {},
      total_capacity: { gpu: 1 },
      max_concurrency: 2,
      free_slots: 2,
      utilization: 0,
      connected: true,
      draining: true,
      active_leases: 0,
      connected_at_ms: 1_700_000_000_000,
      last_seen_at_ms: 1_700_000_003_000,
      load: { queued: 0, running: 0, utilization: 0, custom: {} },
    },
    {
      worker_id: "worker-lost-03",
      session_id: "sess-333",
      namespace: "default",
      pool: "",
      healthy: false,
      interfaces: ["echo"],
      attributes: {},
      total_capacity: { cpu: 4 },
      max_concurrency: 2,
      free_slots: 1,
      utilization: 0.5,
      connected: false,
      draining: false,
      active_leases: 1,
      connected_at_ms: 1_700_000_000_000,
      last_seen_at_ms: 1_700_000_001_000,
      load: { queued: 0, running: 1, utilization: 0.5, custom: {} },
    },
  ],
});

let replies: Map<string, (value: JobDetailResponse) => void>;

beforeEach(() => {
  vi.clearAllMocks();
  clearConsoleToken();
  localStorage.clear();
  document.documentElement.removeAttribute("data-theme");
  window.history.replaceState(null, "", "/queue");
  replies = new Map();
  vi.mocked(getJobs).mockImplementation(async (states) => ({
    jobs: states.length === 0 || states.includes("queued") ? [summary("A"), summary("B")] : [],
    next_page_token: null,
  }));
  vi.mocked(getJob).mockImplementation((id) => new Promise((resolve) => replies.set(id, resolve)));
  vi.mocked(getWorkers).mockResolvedValue({ workers: [], total: 0, healthy: 0 });
  vi.mocked(getOverview).mockResolvedValue(minimalOverview());
});

afterEach(() => {
  cleanup();
  vi.useRealTimers();
});

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

it("keeps each scope's selection and follows the worker filter", async () => {
  vi.mocked(getJobs).mockImplementation(async (states) => ({
    jobs: states.includes("queued")
      ? [summary("A"), summary("B")]
      : ["C", "D"].map((id) => ({
          ...summary(id),
          state: "succeeded" as const,
          latest_worker_id: `worker-${id}`,
        })),
    next_page_token: null,
  }));
  vi.mocked(getJob).mockImplementation(async (id) => ({
    ...detail(id),
    job: { ...detail(id).job, state: ["C", "D"].includes(id) ? "succeeded" : "queued" },
  }));
  render(<App />);
  fireEvent.click(await screen.findByText("B"));
  await screen.findByRole("heading", { name: "B" });
  fireEvent.click(screen.getByRole("button", { name: "History" }));
  fireEvent.click(await screen.findByText("D"));
  await screen.findByRole("heading", { name: "D" });
  fireEvent.click(screen.getByRole("button", { name: "Queue" }));
  await screen.findByRole("heading", { name: "B" });
  fireEvent.click(screen.getByRole("button", { name: "History" }));
  await screen.findByRole("heading", { name: "D" });
  fireEvent.change(screen.getByLabelText("Worker"), { target: { value: "worker-C" } });
  await screen.findByRole("heading", { name: "C" });
  expect(screen.queryByRole("heading", { name: "D" })).toBeNull();
  expect(screen.queryByRole("button", { name: "Cancel job" })).toBeNull();
});

it("polls only active panes and fetches completed details only once per selection", async () => {
  vi.useFakeTimers();
  vi.mocked(getJobs).mockImplementation(async (states) => ({
    jobs: states.includes("queued") ? [summary("A")] : [{ ...summary("B"), state: "succeeded" }],
    next_page_token: null,
  }));
  vi.mocked(getJob).mockImplementation(async (id) => ({
    ...detail(id),
    job: { ...detail(id).job, state: id === "B" ? "succeeded" : "queued" },
  }));
  render(<App />);
  await act(async () => {});
  expect(vi.mocked(getJobs).mock.calls.every(([states]) => states.includes("queued"))).toBe(true);
  expect(getWorkers).not.toHaveBeenCalled();
  fireEvent.click(screen.getByRole("button", { name: "History" }));
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
  fireEvent.click(screen.getByRole("button", { name: /Jobs/ }));
  await act(async () => {});
  expect(getJob).toHaveBeenCalledTimes(1);
  expect(getJob).toHaveBeenCalledWith("B", expect.any(AbortSignal), undefined);
});

it("polls only /api/overview on Overview, renders draining count, and supports click-throughs", async () => {
  window.history.replaceState(null, "", "/");
  vi.mocked(getOverview).mockResolvedValue(richOverview());
  vi.mocked(getWorkers).mockResolvedValue(richWorkers());

  render(<App />);
  expect(await screen.findByText("Memory Byte Budgets")).toBeTruthy();
  expect(getWorkers).not.toHaveBeenCalled();
  expect(screen.getByText("127.0.0.1:50052")).toBeTruthy();
  expect(screen.getByText("127.0.0.1:3001")).toBeTruthy();
  expect(screen.getByText(/42 \/ 310/)).toBeTruthy();
  expect(screen.getByText("8 ops · 650 µs")).toBeTruthy();
  expect(screen.getByText("2.0 KiB / 16.0 KiB (12.5%)")).toBeTruthy();
  expect(screen.getByText("Draining")).toBeTruthy();
  expect(screen.getAllByText("Memory + SQLite journal")).toHaveLength(1);

  fireEvent.click(screen.getAllByText("llm.generate")[0]);
  expect(await screen.findByText("Pool Tiers Spillover Chain")).toBeTruthy();
  expect(screen.getAllByText("default").length).toBeGreaterThan(0);
  expect(screen.getAllByText("gpu-burst").length).toBeGreaterThan(0);

  fireEvent.click(screen.getByRole("button", { name: "View Jobs" }));
  await waitFor(() =>
    expect(getJobs).toHaveBeenCalledWith([], 100, 0, expect.any(AbortSignal), {
      interface: "llm.generate",
      namespace: "all",
    }),
  );
});

it("renders Pools and Workers with slot occupancy meters, reported load, and navigation", async () => {
  window.history.replaceState(null, "", "/pools");
  vi.mocked(getOverview).mockResolvedValue(richOverview());
  vi.mocked(getWorkers).mockResolvedValue(richWorkers());

  render(<App />);
  expect(await screen.findByText("Pool Inspector")).toBeTruthy();
  expect(screen.getAllByText("Desired additional").length).toBeGreaterThan(0);
  expect(screen.getByText("+3")).toBeTruthy();
  expect(screen.getByText("Spillover Tier Position")).toBeTruthy();
  expect(screen.getAllByText("default").length).toBeGreaterThan(0);

  fireEvent.click(screen.getByRole("button", { name: /Workers/ }));
  expect(await screen.findByRole("heading", { name: "worker-gpu-01" })).toBeTruthy();
  expect(screen.getByText("1 / 3")).toBeTruthy();
  expect(screen.getByText("33%")).toBeTruthy();
  expect(screen.getAllByText("sess-111").length).toBeGreaterThan(0);
  expect(screen.getByText("vram_gb")).toBeTruthy();

  fireEvent.click(screen.getByRole("button", { name: "Disconnected" }));
  expect(screen.queryByRole("heading", { name: "worker-gpu-01" })).toBeNull();
  expect(screen.getByRole("heading", { name: "worker-lost-03" })).toBeTruthy();
  expect(screen.getByText((_, node) => node?.className === "inspector__subtitle-row" && node.textContent === "default·default")).toBeTruthy();
  expect(screen.getByText("Session disconnected; active leases survive until reconnect grace expires.")).toBeTruthy();

  fireEvent.click(screen.getByRole("button", { name: "View Worker Jobs" }));
  await waitFor(() => expect(window.location.pathname).toBe("/jobs"));
});

it("renders resiliently when optional overview and worker fields are absent", async () => {
  window.history.replaceState(null, "", "/");
  vi.mocked(getOverview).mockResolvedValue(minimalOverview());

  render(<App />);
  expect(await screen.findByText("Memory")).toBeTruthy();
  expect(screen.getByText("Budget metrics unavailable on this server payload.")).toBeTruthy();
  expect(screen.getByText("Control plane metadata unavailable on this server payload.")).toBeTruthy();
});

it("formats OpenAI SSE / JSON payloads, shows retryable attempts, and collapses empty payload boxes", async () => {
  window.history.replaceState(null, "", "/jobs");
  vi.mocked(getOverview).mockResolvedValue(richOverview());
  vi.mocked(getJobs)
    .mockResolvedValueOnce({
      jobs: [
        summary("job-1", {
          namespace: "tenant-a",
          durability: "durable",
          interface_name: "openai/chat/qwen2.5-7b",
          source: "sdk-py",
          error_code: "worker_failed",
          error_retryable: true,
          last_error: "HTTP 503: upstream busy",
          state: "failed",
        }),
      ],
      next_page_token: "cursor-2",
    })
    .mockResolvedValueOnce({
      jobs: [summary("job-2", { namespace: "tenant-a", interface_name: "triton/minilm" })],
      next_page_token: null,
    });
  vi.mocked(getJob).mockImplementation(async (id) =>
    id === "job-1"
      ? {
          ...detail(id, {
            namespace: "tenant-a",
            durability: "durable",
            interface_name: "openai/chat/qwen2.5-7b",
            state: "failed",
            error_code: "worker_failed",
            error_retryable: true,
            last_error: "HTTP 503: upstream busy",
            params_size_bytes: 42,
            params_preview: '{"messages":[{"role":"user","content":"hi"}],"stream":true}',
            output_preview: {
              size_bytes: 95,
              content_type: "text/event-stream",
              preview: 'data: {"choices":[{"delta":{"content":"Hello"}}]}\ndata: {"choices":[{"delta":{"content":"!"}}]}\ndata: [DONE]',
              truncated: false,
            },
            demand: {
              interface: "openai/chat/qwen2.5-7b",
              required_attributes: { arch: "cuda" },
              preferred_attributes: {},
              required_capacity: { gpu: 1 },
              affinity_key: "model-llama3",
            },
            policy: {
              priority: 3,
              max_attempts: 5,
              durability: "Durable",
              queue_timeout: { secs: 10, nanos: 0 },
              execution_timeout: { secs: 60, nanos: 0 },
              total_timeout: null,
              spillover_after: { secs: 0, nanos: 500_000_000 },
              fairness_key: "org-42",
              concurrency_key: "slot-A",
            },
          }),
          attempts: [
            {
              attempt_id: "att-1",
              state: "restarted",
              worker_id: "w-1",
              created_at_ms: 10,
              finished_at_ms: 20,
              duration_ms: 10,
              last_error: "HTTP 503: upstream busy",
            },
            {
              attempt_id: "att-2",
              state: "failed",
              worker_id: "w-2",
              created_at_ms: 30,
              finished_at_ms: 40,
              duration_ms: 10,
              last_error: "HTTP 503: upstream busy",
            },
          ],
        }
      : detail(id, {
          namespace: "tenant-a",
          interface_name: "triton/minilm",
          output_preview: {
            size_bytes: 28,
            content_type: "application/json",
            preview: '{"model_name":"minilm"}',
            truncated: false,
          },
          outputs: [
            {
              kind: "inline",
              size_bytes: 28,
              uri: null,
              content_type: "application/json",
              preview: '{"model_name":"minilm"}',
              truncated: false,
              metadata: {},
            },
          ],
        }),
  );

  render(<App />);
  expect(await screen.findByText("job-1")).toBeTruthy();
  await waitFor(() => expect(getJob).toHaveBeenCalledWith("job-1", expect.any(AbortSignal), "tenant-a"));
  expect(await screen.findByText(/Affinity key: model-llama3/)).toBeTruthy();
  expect(screen.getByText(/Fairness key: org-42/)).toBeTruthy();
  expect(screen.getByText(/Concurrency key: slot-A/)).toBeTruthy();
  expect(screen.getByText("Retryable")).toBeTruthy();
  expect(screen.getByText("retryable · retried")).toBeTruthy();
  expect(screen.getByText("Streamed text · 3 SSE frames")).toBeTruthy();
  expect(screen.getByText("Hello!")).toBeTruthy();

  fireEvent.click(screen.getByRole("button", { name: "Load more" }));
  expect(await screen.findByText("job-2")).toBeTruthy();

  fireEvent.change(screen.getByPlaceholderText("Search job ID, source, error…"), { target: { value: "job-2" } });
  expect(screen.queryByText("job-1")).toBeNull();
  expect(screen.getByText("job-2")).toBeTruthy();

  fireEvent.click(screen.getByText("job-2"));
  expect(await screen.findByText(/model_name/)).toBeTruthy();
  expect(screen.queryByText("Outputs")).toBeNull();
  expect(screen.getAllByText(/minilm/).length).toBeGreaterThan(0);
});

it("toggles Light/Dark theme and disconnects token cleanly", async () => {
  window.history.replaceState(null, "", "/");
  setConsoleToken("secret-token");
  vi.mocked(getOverview).mockResolvedValue(richOverview());

  render(<App />);
  expect(await screen.findByText("Memory Byte Budgets")).toBeTruthy();
  expect(document.documentElement.getAttribute("data-theme")).toBe("light");

  fireEvent.click(screen.getByRole("button", { name: "Switch theme" }));
  expect(document.documentElement.getAttribute("data-theme")).toBe("dark");

  fireEvent.click(screen.getByRole("button", { name: "Disconnect" }));
  expect(hasConsoleToken()).toBe(false);
  expect(screen.getByRole("button", { name: "Connect" })).toBeTruthy();
});

it("uses AnyServe branding and navigates directly from JobInspector links to Worker, Interface, and Pools", async () => {
  window.history.replaceState(null, "", "/jobs");
  vi.mocked(getOverview).mockResolvedValue(richOverview());
  vi.mocked(getWorkers).mockResolvedValue(richWorkers());
  vi.mocked(getJobs).mockResolvedValue({
    jobs: [
      summary("job-jump-1", {
        state: "succeeded",
        namespace: "tenant-a",
        interface_name: "llm.generate",
        latest_worker_id: "worker-gpu-01",
      }),
    ],
    next_page_token: null,
  });
  vi.mocked(getJob).mockResolvedValue({
    ...detail("job-jump-1", {
      state: "succeeded",
      namespace: "tenant-a",
      interface_name: "llm.generate",
    }),
    attempts: [
      {
        attempt_id: "att-1",
        state: "succeeded",
        worker_id: "worker-gpu-01",
        created_at_ms: 10,
        finished_at_ms: 25,
        duration_ms: 15,
        last_error: null,
      },
    ],
  });

  render(<App />);
  expect(screen.getByText("AnyServe")).toBeTruthy();
  await screen.findByRole("button", { name: "worker-gpu-01", hidden: false });

  // Clicking Namespace filters Jobs to tenant-a; clicking it again jumps to Pools filtered by tenant-a.
  fireEvent.click(screen.getAllByRole("button", { name: "tenant-a" })[0]);
  await waitFor(() =>
    expect(getJobs).toHaveBeenLastCalledWith([], 100, 0, expect.any(AbortSignal), {
      interface: "all",
      namespace: "tenant-a",
    }),
  );

  // Clicking Interface in JobInspector subtitle jumps directly to /interfaces and selects llm.generate.
  fireEvent.click(screen.getAllByRole("button", { name: "llm.generate" })[0]);
  expect(await screen.findByText("Pool Tiers Spillover Chain")).toBeTruthy();
  expect(window.location.pathname).toBe("/interfaces");

  // Jump back to Jobs and click Worker in JobInspector to jump to /workers with worker-gpu-01 selected.
  fireEvent.click(screen.getByRole("button", { name: "View Jobs" }));
  await screen.findByText("Scheduling & Policy");
  const workerButtons = await screen.findAllByRole("button", { name: "worker-gpu-01" });
  fireEvent.click(workerButtons[workerButtons.length - 1]);
  expect(await screen.findByText("Outbound Session")).toBeTruthy();
  expect(screen.getByRole("heading", { name: "worker-gpu-01" })).toBeTruthy();
  expect(window.location.pathname).toBe("/workers");
});

it("collapses large payloads by default and opens Connect Worker onboarding modal with live endpoint", async () => {
  window.history.replaceState(null, "", "/jobs");
  vi.mocked(getOverview).mockResolvedValue(richOverview());
  vi.mocked(getWorkers).mockResolvedValue(richWorkers());
  vi.mocked(getJobs).mockResolvedValue({
    jobs: [summary("job-big", { state: "succeeded", interface_name: "llm.generate", latest_worker_id: "worker-gpu-01" })],
    next_page_token: null,
  });
  vi.mocked(getJob).mockResolvedValue(
    detail("job-big", {
      state: "succeeded",
      interface_name: "llm.generate",
      output_preview: {
        size_bytes: 16_384,
        content_type: "application/json",
        preview: JSON.stringify({ model: "bge-m3", vector_marker: "UNIQUE_VECTOR_MARKER", vals: Array.from({ length: 60 }, (_, i) => i) }),
        truncated: true,
      },
    }),
  );

  render(<App />);
  expect(await screen.findByText(/Large payload \(16\.0 KiB · truncated preview\) hidden by default/)).toBeTruthy();
  expect(screen.queryByText(/UNIQUE_VECTOR_MARKER/)).toBeNull();

  fireEvent.click(screen.getByRole("button", { name: "Show preview" }));
  expect(await screen.findByText(/UNIQUE_VECTOR_MARKER/)).toBeTruthy();

  fireEvent.click(screen.getByRole("button", { name: "Hide preview" }));
  expect(screen.queryByText(/UNIQUE_VECTOR_MARKER/)).toBeNull();

  // Navigate to Workers and open Connect Worker onboarding modal.
  fireEvent.click(screen.getByRole("button", { name: /Workers/ }));
  fireEvent.click(await screen.findByRole("button", { name: "Connect Worker" }));
  expect(screen.getByRole("dialog")).toBeTruthy();
  expect(screen.getAllByText(/https:\/\/127\.0\.0\.1:50052/).length).toBeGreaterThan(0);
  expect(screen.getByText(/Worker token required/)).toBeTruthy();

  fireEvent.click(screen.getByRole("tab", { name: "AI Connectors" }));
  expect(screen.getByText(/anyserve\.universe\.git/)).toBeTruthy();

  fireEvent.click(screen.getByRole("tab", { name: "Linux Install" }));
  expect(screen.getByText(/\/usr\/local\/bin\/anyserve/)).toBeTruthy();

  fireEvent.keyDown(window, { key: "Escape" });
  expect(screen.queryByRole("dialog")).toBeNull();
});
