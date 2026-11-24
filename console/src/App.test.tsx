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
  vi.unstubAllEnvs();
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
    expect(getJobs).toHaveBeenCalledWith(
      [],
      100,
      0,
      expect.any(AbortSignal),
      expect.objectContaining({
        interface: "llm.generate",
        namespace: "all",
      }),
    ),
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
    expect(getJobs).toHaveBeenLastCalledWith(
      [],
      100,
      0,
      expect.any(AbortSignal),
      expect.objectContaining({
        interface: "all",
        namespace: "tenant-a",
      }),
    ),
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

it("supports time presets, interactive histogram bar click & brush zoom, server search, and bounded total progress", async () => {
  window.history.replaceState(null, "", "/jobs");
  vi.mocked(getOverview).mockResolvedValue(richOverview());
  const baseTs = 1_700_000_000_000;
  const sampleHistogram = {
    start_ms: baseTs,
    end_ms: baseTs + 60_000,
    bucket_ms: 15_000,
    buckets: [
      { start_ms: baseTs, end_ms: baseTs + 15_000, active: 0, succeeded: 12, failed: 0, cancelled: 0 },
      { start_ms: baseTs + 15_000, end_ms: baseTs + 30_000, active: 0, succeeded: 8, failed: 2, cancelled: 0 },
      { start_ms: baseTs + 30_000, end_ms: baseTs + 45_000, active: 1, succeeded: 2, failed: 0, cancelled: 0 },
      { start_ms: baseTs + 45_000, end_ms: baseTs + 60_000, active: 0, succeeded: 0, failed: 0, cancelled: 0 },
    ],
  };

  vi.mocked(getJobs).mockImplementation(async (_states, _limit, offset, _signal, filters) => {
    if (filters?.q === "needle-older") {
      return {
        jobs: [summary("job-older-needle", { state: "succeeded", source: "needle-older", latest_worker_id: "worker-gpu-01" })],
        total: 1,
        next_page_token: null,
        histogram: {
          ...sampleHistogram,
          buckets: [{ ...sampleHistogram.buckets[0], succeeded: 1 }, { ...sampleHistogram.buckets[1], succeeded: 0, failed: 0 }, { ...sampleHistogram.buckets[2], active: 0, succeeded: 0 }, sampleHistogram.buckets[3]],
        },
      };
    }
    if (offset === "page-2") {
      return {
        jobs: [summary("job-page-2", { state: "succeeded", latest_worker_id: "worker-gpu-01" })],
        total: 25,
        next_page_token: null,
      };
    }
    if (filters?.fromMs != null && filters?.toMs != null) {
      return {
        jobs: [summary("job-zoomed", { state: "failed", error_code: "timeout", latest_worker_id: "worker-gpu-01" })],
        total: 10,
        next_page_token: null,
        histogram: sampleHistogram,
      };
    }
    return {
      jobs: [summary("job-page-1", { state: "succeeded", latest_worker_id: "worker-gpu-01" })],
      total: 25,
      next_page_token: "page-2",
      histogram: sampleHistogram,
    };
  });
  vi.mocked(getJob).mockImplementation(async (id) => detail(id));

  render(<App />);
  expect(await screen.findByText("1 of 25 jobs")).toBeTruthy();
  expect(screen.getByText("Showing 1 of 25")).toBeTruthy();
  expect(screen.getByRole("button", { name: "Load more (24 remaining)" })).toBeTruthy();

  // Loading the last page clears Load more and displays the bounded end-of-list caption.
  fireEvent.click(screen.getByRole("button", { name: "Load more (24 remaining)" }));
  expect(await screen.findByText("End of list · all 25 jobs loaded")).toBeTruthy();

  // Selecting a preset time window passes windowMs to /api/jobs.
  fireEvent.click(screen.getByRole("button", { name: "15m" }));
  await waitFor(() =>
    expect(getJobs).toHaveBeenLastCalledWith(
      [],
      100,
      0,
      expect.any(AbortSignal),
      expect.objectContaining({ windowMs: 15 * 60_000, buckets: 32 }),
    ),
  );

  // Custom time editor applies explicit [fromMs, toMs] bounds.
  fireEvent.click(screen.getByRole("button", { name: "Custom" }));
  fireEvent.change(screen.getByLabelText("From time"), { target: { value: "2026-01-01T10:00:00" } });
  fireEvent.change(screen.getByLabelText("To time"), { target: { value: "2026-01-01T10:30:00" } });
  fireEvent.click(screen.getByRole("button", { name: "Apply" }));
  await waitFor(() =>
    expect(getJobs).toHaveBeenLastCalledWith(
      [],
      100,
      0,
      expect.any(AbortSignal),
      expect.objectContaining({
        fromMs: Date.parse("2026-01-01T10:00:00"),
        toMs: Date.parse("2026-01-01T10:30:00"),
        buckets: 32,
      }),
    ),
  );

  // Worker filter forwards worker_id to /api/jobs.
  fireEvent.change(screen.getByLabelText("Worker"), { target: { value: "worker-gpu-01" } });
  await waitFor(() =>
    expect(getJobs).toHaveBeenLastCalledWith(
      [],
      100,
      0,
      expect.any(AbortSignal),
      expect.objectContaining({ worker: "worker-gpu-01" }),
    ),
  );
  fireEvent.change(screen.getByLabelText("Worker"), { target: { value: "all" } });

  // Clicking a single histogram bar zooms to that exact bucket's [start_ms, end_ms).
  await waitFor(() => expect(document.querySelectorAll(".histogram__col").length).toBe(4));
  const bars = document.querySelectorAll<HTMLButtonElement>(".histogram__col");
  fireEvent.click(bars[1]);
  await waitFor(() =>
    expect(getJobs).toHaveBeenLastCalledWith(
      [],
      100,
      0,
      expect.any(AbortSignal),
      expect.objectContaining({
        fromMs: baseTs + 15_000,
        toMs: baseTs + 30_000,
        buckets: 32,
      }),
    ),
  );
  expect(await screen.findByRole("heading", { name: "job-zoomed" })).toBeTruthy();

  // Drag-brushing across multiple histogram bars selects the combined [start_ms, end_ms] range.
  const zoomedBars = document.querySelectorAll<HTMLButtonElement>(".histogram__col");
  fireEvent.pointerDown(zoomedBars[0], { button: 0, clientX: 10 });
  fireEvent.pointerMove(zoomedBars[2], { clientX: 90 });
  fireEvent.pointerUp(window);
  await waitFor(() =>
    expect(getJobs).toHaveBeenLastCalledWith(
      [],
      100,
      0,
      expect.any(AbortSignal),
      expect.objectContaining({
        fromMs: baseTs,
        toMs: baseTs + 45_000,
        buckets: 32,
      }),
    ),
  );

  // Reset time returns to All time.
  fireEvent.click(screen.getByRole("button", { name: "Reset time" }));
  await waitFor(() =>
    expect(getJobs).toHaveBeenLastCalledWith(
      [],
      100,
      0,
      expect.any(AbortSignal),
      expect.not.objectContaining({ fromMs: expect.any(Number) }),
    ),
  );

  // Server-backed search debounces 150ms, fetches deeper matches, and shows exact end-of-results total.
  fireEvent.change(screen.getByPlaceholderText("Search job ID, source, error…"), { target: { value: "needle-older" } });
  expect(await screen.findByRole("heading", { name: "job-older-needle" }, { timeout: 2_000 })).toBeTruthy();
  expect(screen.getByText("End of results · all 1 matching jobs loaded")).toBeTruthy();
});

it("filters by job status via select or KPI cards and preserves filters & selection across reload", async () => {
  vi.mocked(getOverview).mockResolvedValue(richOverview());
  const jobs = [
    summary("job-fail-1", { state: "failed", error_code: "timeout", last_error: "timeout waiting" }),
    summary("job-fail-2", { state: "failed", error_code: "timeout", last_error: "timeout reading" }),
    summary("job-ok-1", { state: "succeeded", updated_at_ms: 5 }),
  ];
  vi.mocked(getJobs).mockImplementation(async (states, _limit, _offset, _signal, filters) => {
    const matched = jobs.filter(
      (j) =>
        (states.length === 0 || states.includes(j.state)) &&
        (!filters?.q || (j.error_code ?? "").includes(filters.q) || j.job_id.includes(filters.q)),
    );
    return {
      jobs: matched,
      total: matched.length,
      next_page_token: null,
      histogram: {
        start_ms: 1_700_000_000_000,
        end_ms: 1_700_000_060_000,
        bucket_ms: 30_000,
        buckets: [
          { start_ms: 1_700_000_000_000, end_ms: 1_700_000_030_000, active: 0, succeeded: states.includes("succeeded") || states.length === 0 ? 1 : 0, failed: states.includes("failed") || states.length === 0 ? 2 : 0, cancelled: 0 },
          { start_ms: 1_700_000_030_000, end_ms: 1_700_000_060_000, active: 0, succeeded: 0, failed: 0, cancelled: 0 },
        ],
      },
    };
  });
  vi.mocked(getJob).mockImplementation(async (id) => detail(id, { state: id.startsWith("job-fail") ? "failed" : "succeeded" }));

  const view = render(<App />);
  // Choosing a history status while on /queue promotes scope cleanly to /jobs and updates the URL.
  fireEvent.change(screen.getByLabelText("Status"), { target: { value: "failed" } });
  await waitFor(() =>
    expect(getJobs).toHaveBeenLastCalledWith(
      ["failed"],
      100,
      0,
      expect.any(AbortSignal),
      expect.objectContaining({ buckets: 32 }),
    ),
  );
  expect(window.location.pathname).toBe("/jobs");

  fireEvent.click(screen.getByRole("button", { name: "15m" }));
  fireEvent.change(screen.getByPlaceholderText("Search job ID, source, error…"), { target: { value: "timeout" } });
  await waitFor(() =>
    expect(getJobs).toHaveBeenLastCalledWith(
      ["failed"],
      100,
      0,
      expect.any(AbortSignal),
      expect.objectContaining({ q: "timeout", windowMs: 15 * 60_000 }),
    ),
  );

  fireEvent.click(await screen.findByText("job-fail-2"));
  await screen.findByRole("heading", { name: "job-fail-2" });
  expect(window.location.search).toContain("state=failed");
  expect(window.location.search).toContain("q=timeout");
  expect(window.location.search).toContain("time=15m");
  expect(window.location.search).toContain("job=job-fail-2");

  // Simulate full browser reload with the current URL intact.
  view.unmount();
  vi.mocked(getJobs).mockClear();
  render(<App />);

  await waitFor(() => expect(getJobs).toHaveBeenCalled());
  expect(vi.mocked(getJobs).mock.calls[0][0]).toEqual(["failed"]);
  expect(vi.mocked(getJobs).mock.calls[0][4]).toEqual(
    expect.objectContaining({ q: "timeout", windowMs: 15 * 60_000 }),
  );
  expect(await screen.findByRole("heading", { name: "job-fail-2" })).toBeTruthy();
  expect((screen.getByLabelText("Status") as HTMLSelectElement).value).toBe("failed");
  expect((screen.getByPlaceholderText("Search job ID, source, error…") as HTMLInputElement).value).toBe("timeout");

  // Clicking the active Failed KPI card toggles back to All statuses; clearing search and clicking Succeeded switches to succeeded.
  fireEvent.click(screen.getByRole("button", { name: /^Failed\b/i }));
  await waitFor(() => expect((screen.getByLabelText("Status") as HTMLSelectElement).value).toBe("all"));

  fireEvent.click(screen.getByRole("button", { name: "Clear search" }));
  fireEvent.click(screen.getByRole("button", { name: /^Succeeded\b/i }));
  await waitFor(() => expect((screen.getByLabelText("Status") as HTMLSelectElement).value).toBe("succeeded"));
  expect(await screen.findByRole("heading", { name: "job-ok-1" })).toBeTruthy();
});

it("renders memory_plus_cloudflare mode badge, wss:// public_endpoint, and WebSocket modal subtitle", async () => {
  window.history.replaceState(null, "", "/workers");
  const cloudflareOverview: OverviewResponse = {
    ...richOverview(),
    runtime: { code: "memory_plus_cloudflare", label: "Memory + Cloudflare journal", journal: "cloudflare" },
    control_plane: {
      ...richOverview().control_plane!,
      public_endpoint: "  wss://anyserve.example.com  ",
    },
  };
  vi.mocked(getOverview).mockResolvedValue(cloudflareOverview);
  vi.mocked(getWorkers).mockResolvedValue(richWorkers());

  const view = render(<App />);
  expect(await screen.findByText("Memory + Cloudflare journal")).toBeTruthy();

  fireEvent.click(await screen.findByRole("button", { name: "Connect Worker" }));
  expect(screen.getByRole("dialog")).toBeTruthy();
  expect(document.querySelector(".modal__sub")?.textContent).toBe(
    "Workers open outbound WebSocket streams to wss://anyserve.example.com (no inbound port required).",
  );
  expect(screen.getByText("Endpoint: wss://anyserve.example.com")).toBeTruthy();
  expect(screen.getByText(/serve\(echo, endpoint="wss:\/\/anyserve\.example\.com"\)/)).toBeTruthy();

  // Falls back to VITE_PUBLIC_ENDPOINT (including ws://) when control_plane.public_endpoint is empty/unset.
  view.unmount();
  vi.stubEnv("VITE_PUBLIC_ENDPOINT", "  ws://127.0.0.1:3001  ");
  vi.mocked(getOverview).mockResolvedValue({
    ...cloudflareOverview,
    control_plane: {
      ...cloudflareOverview.control_plane!,
      public_endpoint: "   ",
    },
  });

  render(<App />);
  fireEvent.click(await screen.findByRole("button", { name: "Connect Worker" }));
  expect(document.querySelector(".modal__sub")?.textContent).toBe(
    "Workers open outbound WebSocket streams to ws://127.0.0.1:3001 (no inbound port required).",
  );
  expect(screen.getByText("Endpoint: ws://127.0.0.1:3001")).toBeTruthy();
});

it("uses fast default poll intervals when recommended_poll_ms is undefined or null", async () => {
  vi.useFakeTimers();
  window.history.replaceState(null, "", "/queue");
  vi.mocked(getOverview).mockResolvedValue(richOverview());
  vi.mocked(getJobs).mockResolvedValue({
    jobs: [summary("job-fast")],
    next_page_token: null,
  });
  vi.mocked(getJob).mockResolvedValue(detail("job-fast"));
  vi.mocked(getWorkers).mockResolvedValue(richWorkers());

  render(<App />);
  await act(async () => {});
  expect(getOverview).toHaveBeenCalledTimes(1);
  expect(getJobs).toHaveBeenCalledTimes(1);
  expect(getJob).toHaveBeenCalledTimes(1);

  await act(() => vi.advanceTimersByTimeAsync(799));
  expect(getJobs).toHaveBeenCalledTimes(1);
  await act(() => vi.advanceTimersByTimeAsync(1));
  expect(getJobs).toHaveBeenCalledTimes(2);
  expect(getOverview).toHaveBeenCalledTimes(1);
  expect(getJob).toHaveBeenCalledTimes(1);

  await act(() => vi.advanceTimersByTimeAsync(200));
  expect(getOverview).toHaveBeenCalledTimes(2);
  expect(getJob).toHaveBeenCalledTimes(2);

  fireEvent.click(screen.getByRole("button", { name: /Workers/ }));
  await act(async () => {});
  vi.mocked(getWorkers).mockClear();
  await act(() => vi.advanceTimersByTimeAsync(1_999));
  expect(getWorkers).not.toHaveBeenCalled();
  await act(() => vi.advanceTimersByTimeAsync(1));
  expect(getWorkers).toHaveBeenCalledTimes(1);
});

it("honours recommended_poll_ms=10000 across overview, jobs, workers, and job-detail polls while pausing when hidden", async () => {
  vi.useFakeTimers();
  window.history.replaceState(null, "", "/queue");
  const slowOverview: OverviewResponse = {
    ...richOverview(),
    control_plane: {
      ...richOverview().control_plane!,
      recommended_poll_ms: 10_000,
    },
  };
  vi.mocked(getOverview).mockResolvedValue(slowOverview);
  vi.mocked(getJobs).mockResolvedValue({
    jobs: [summary("job-10s")],
    next_page_token: null,
  });
  vi.mocked(getJob).mockResolvedValue(detail("job-10s"));
  vi.mocked(getWorkers).mockResolvedValue(richWorkers());

  render(<App />);
  await act(async () => {});
  // Initial load performs exactly one fetch per active resource (no duplicate fetch when pollMs updates to 10_000).
  expect(getOverview).toHaveBeenCalledTimes(1);
  expect(getJobs).toHaveBeenCalledTimes(1);
  expect(getJob).toHaveBeenCalledTimes(1);

  await act(() => vi.advanceTimersByTimeAsync(9_999));
  expect(getOverview).toHaveBeenCalledTimes(1);
  expect(getJobs).toHaveBeenCalledTimes(1);
  expect(getJob).toHaveBeenCalledTimes(1);

  await act(() => vi.advanceTimersByTimeAsync(1));
  expect(getOverview).toHaveBeenCalledTimes(2);
  expect(getJobs).toHaveBeenCalledTimes(2);
  expect(getJob).toHaveBeenCalledTimes(2);

  // Workers tab also honours the 10_000 ms interval.
  fireEvent.click(screen.getByRole("button", { name: /Workers/ }));
  await act(async () => {});
  expect(getWorkers).toHaveBeenCalledTimes(1);
  await act(() => vi.advanceTimersByTimeAsync(9_999));
  expect(getWorkers).toHaveBeenCalledTimes(1);
  await act(() => vi.advanceTimersByTimeAsync(1));
  expect(getWorkers).toHaveBeenCalledTimes(2);

  // Pausing when tab is hidden still stops timers and refreshes on return.
  const visibility = vi.spyOn(document, "visibilityState", "get");
  visibility.mockReturnValue("hidden");
  act(() => document.dispatchEvent(new Event("visibilitychange")));
  vi.mocked(getOverview).mockClear();
  vi.mocked(getWorkers).mockClear();
  await act(() => vi.advanceTimersByTimeAsync(30_000));
  expect(getOverview).not.toHaveBeenCalled();
  expect(getWorkers).not.toHaveBeenCalled();

  visibility.mockReturnValue("visible");
  await act(async () => {
    document.dispatchEvent(new Event("visibilitychange"));
  });
  expect(getOverview).toHaveBeenCalledTimes(1);
  expect(getWorkers).toHaveBeenCalledTimes(1);
  visibility.mockRestore();
});

it("disables timer polling when recommended_poll_ms=0 and refreshes on manual Refresh button click", async () => {
  vi.useFakeTimers();
  window.history.replaceState(null, "", "/queue");
  const manualOverview: OverviewResponse = {
    ...richOverview(),
    control_plane: {
      ...richOverview().control_plane!,
      recommended_poll_ms: 0,
    },
  };
  vi.mocked(getOverview).mockResolvedValue(manualOverview);
  vi.mocked(getJobs).mockResolvedValue({
    jobs: [summary("job-manual")],
    next_page_token: null,
  });
  vi.mocked(getJob).mockResolvedValue(detail("job-manual"));
  vi.mocked(getWorkers).mockResolvedValue(richWorkers());

  render(<App />);
  await act(async () => {});
  expect(getOverview).toHaveBeenCalledTimes(1);
  expect(getJobs).toHaveBeenCalledTimes(1);
  expect(getJob).toHaveBeenCalledTimes(1);

  // No timer polls fire after initial load even after a full minute.
  await act(() => vi.advanceTimersByTimeAsync(60_000));
  expect(getOverview).toHaveBeenCalledTimes(1);
  expect(getJobs).toHaveBeenCalledTimes(1);
  expect(getJob).toHaveBeenCalledTimes(1);

  // Clicking Sidebar Refresh button fetches overview, jobs, and active job detail once.
  const refreshBtn = screen.getByRole("button", { name: "Refresh" });
  expect(refreshBtn.classList.contains("ctrl-pill")).toBe(true);
  expect(refreshBtn.classList.contains("ctrl-pill--icon")).toBe(true);
  expect(refreshBtn.getAttribute("title")).toBe("Refresh");
  expect(refreshBtn.querySelector("svg")).toBeTruthy();
  expect(refreshBtn.textContent).toBe("");
  expect(refreshBtn.closest(".sidebar__controls")).toBeTruthy();

  await act(async () => {
    fireEvent.click(refreshBtn);
  });
  expect(getOverview).toHaveBeenCalledTimes(2);
  expect(getJobs).toHaveBeenCalledTimes(2);
  expect(getJob).toHaveBeenCalledTimes(2);

  // Still no timer polls after manual refresh.
  await act(() => vi.advanceTimersByTimeAsync(60_000));
  expect(getOverview).toHaveBeenCalledTimes(2);
  expect(getJobs).toHaveBeenCalledTimes(2);
  expect(getJob).toHaveBeenCalledTimes(2);

  // Workers tab also fetches once on navigation, never polls on timer, and refreshes on click.
  fireEvent.click(screen.getByRole("button", { name: /Workers/ }));
  await act(async () => {});
  expect(getWorkers).toHaveBeenCalledTimes(1);
  await act(() => vi.advanceTimersByTimeAsync(60_000));
  expect(getWorkers).toHaveBeenCalledTimes(1);

  await act(async () => {
    fireEvent.click(screen.getByRole("button", { name: "Refresh" }));
  });
  expect(getWorkers).toHaveBeenCalledTimes(2);
});

it("renders multi-interface worker rows with primary interface button + +N badge, WebSocket session labels, and 24-hour timestamps", async () => {
  window.history.replaceState(null, "", "/workers");
  const multiIfaces = [
    "policy.evaluate.v1",
    "stream.transform.v1",
    "chaos.flaky.v1",
    "slow.cancel.v1",
  ];
  const wsOverview: OverviewResponse = {
    ...richOverview(),
    runtime: { code: "memory_plus_cloudflare", label: "Memory + Cloudflare DO", journal: "cloudflare" },
    control_plane: {
      ...richOverview().control_plane!,
      public_endpoint: "wss://anyserve-cloudflare-native.example.workers.dev",
    },
    interfaces: [
      ...richOverview().interfaces,
      {
        name: "policy.evaluate.v1",
        queued: 0,
        running: 1,
        oldest_queued_age_ms: 0,
        sessions: 1,
        free_slots: 3,
        policy: null,
      },
    ],
  };
  const multiWorkers: WorkersResponse = {
    total: 2,
    healthy: 1,
    draining: 0,
    disconnected: 1,
    workers: [
      {
        worker_id: "bot-py-robot-1",
        session_id: "sess-multi-01",
        namespace: "default",
        pool: "default",
        healthy: true,
        interfaces: multiIfaces,
        attributes: { role: "robot" },
        total_capacity: { slot: 4, vram_mb: 8192 },
        max_concurrency: 4,
        free_slots: 3,
        utilization: 0.25,
        connected: true,
        draining: false,
        active_leases: 1,
        connected_at_ms: Date.UTC(2026, 10, 23, 22, 31, 1, 293),
        last_seen_at_ms: Date.UTC(2026, 10, 23, 22, 31, 1, 293),
        load: { queued: 0, running: 1, utilization: 0.25, custom: {} },
      },
      {
        worker_id: "bot-none-2",
        session_id: "sess-none-02",
        namespace: "default",
        pool: "default",
        healthy: false,
        interfaces: [],
        attributes: {},
        total_capacity: {},
        max_concurrency: 1,
        free_slots: 0,
        utilization: 0,
        connected: false,
        draining: false,
        active_leases: 0,
        connected_at_ms: Date.UTC(2026, 10, 23, 22, 30, 0, 0),
        last_seen_at_ms: Date.UTC(2026, 10, 23, 22, 30, 5, 0),
        load: { queued: 0, running: 0, utilization: 0, custom: {} },
      },
    ],
  };

  vi.mocked(getOverview).mockResolvedValue(wsOverview);
  vi.mocked(getWorkers).mockResolvedValue(multiWorkers);

  render(<App />);
  expect(await screen.findByRole("heading", { name: "bot-py-robot-1" })).toBeTruthy();

  // Refresh button remains accessible via role + name in Sidebar.
  expect(screen.getByRole("button", { name: "Refresh" })).toBeTruthy();

  // Multi-interface row renders primary clickable button + "+3" count badge with full list in title.
  const countBadge = screen.getByText("+3");
  expect(countBadge.getAttribute("title")).toBe(multiIfaces.join(", "));
  const rowPrimaryBtn = countBadge.closest(".cell-inline")?.querySelector("button");
  expect(rowPrimaryBtn?.textContent).toBe("policy.evaluate.v1");
  expect(rowPrimaryBtn?.getAttribute("title")).toBe("Open interface policy.evaluate.v1");

  // Outbound Session shows WebSocket session active / closed when public_endpoint uses wss://.
  expect(screen.getByText("WebSocket session active")).toBeTruthy();
  expect(screen.queryByText("gRPC stream active")).toBeNull();

  // Inspector timestamps use 24-hour format without orphan "AM"/"PM" suffixes.
  for (const el of document.querySelectorAll(".meta-item__value")) {
    expect(el.textContent ?? "").not.toMatch(/\b(AM|PM)\b/i);
  }

  // Selecting the disconnected zero-interface worker renders "None" in table and "WebSocket session closed".
  fireEvent.click(screen.getByText("bot-none-2"));
  expect(await screen.findByText("WebSocket session closed")).toBeTruthy();

  // Clicking the primary interface button navigates directly to /interfaces for that interface.
  fireEvent.click(rowPrimaryBtn!);
  expect(await screen.findByRole("heading", { name: "policy.evaluate.v1" })).toBeTruthy();
  expect(window.location.pathname).toBe("/interfaces");
});

it("captures #token= from URL hash, strips the hash, and locks tenant scope UI", async () => {
  window.history.replaceState(null, "", "/jobs#token=asv1.hash-token.sig");
  vi.mocked(getOverview).mockResolvedValue({
    ...minimalOverview(),
    scope: { is_operator: false, namespace: "gh-9001", subject: "gh:octocat" },
  });
  vi.mocked(getJobs).mockResolvedValue({ jobs: [], next_page_token: null, total: 0 });

  render(<App />);
  expect(hasConsoleToken()).toBe(true);
  expect(setConsoleToken).toHaveBeenCalledWith("asv1.hash-token.sig");
  await waitFor(() => expect(window.location.hash).toBe(""));
  expect(await screen.findByText("gh:octocat")).toBeTruthy();
  expect(document.querySelector(".sidebar__scope")?.textContent).toContain("gh-9001");
  await waitFor(() =>
    expect(getJobs).toHaveBeenCalledWith(
      expect.any(Array),
      100,
      0,
      expect.any(AbortSignal),
      expect.objectContaining({ namespace: "gh-9001" }),
    ),
  );
});
