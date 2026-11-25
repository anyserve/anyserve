// @vitest-environment jsdom
import { afterEach, expect, it, vi } from "vitest";
import {
  AuthenticationError,
  AuthorizationError,
  cancelJob,
  clearConsoleToken,
  drainWorker,
  fetchInfo,
  getAnalyticsJobs,
  getConsoleToken,
  getJob,
  getJobs,
  getOverview,
  getWorkers,
  hasConsoleToken,
  safeHttpUrl,
  setConsoleToken,
} from "./api";
import { deriveEndpoint } from "./ConnectWorkerModal";
import type { ControlPlaneSummary } from "./types";

afterEach(() => {
  vi.unstubAllGlobals();
  sessionStorage.clear();
});

it("forwards cancellation and authentication to every polling endpoint", async () => {
  const fetch = vi.fn().mockImplementation(async () => new Response("{}"));
  vi.stubGlobal("fetch", fetch);
  setConsoleToken(" client-token ");
  expect(hasConsoleToken()).toBe(true);
  expect(getConsoleToken()).toBe("client-token");
  const signal = new AbortController().signal;
  await Promise.all([
    getOverview(signal),
    getJobs(["queued"], 100, 5, signal),
    getJob("job", signal),
    getWorkers(signal),
  ]);
  expect(fetch.mock.calls.map(([url]) => url)).toEqual([
    "/api/overview",
    "/api/jobs?states=queued&limit=100&offset=5",
    "/api/jobs/job",
    "/api/workers",
  ]);
  for (const [, init] of fetch.mock.calls) {
    expect(init.signal).toBe(signal);
    expect(init.headers.Authorization).toBe("Bearer client-token");
  }
  clearConsoleToken();
  expect(hasConsoleToken()).toBe(false);
});

it("encodes custom job IDs, metadata cursors, and optional filters while preserving authorization", async () => {
  const fetch = vi.fn().mockImplementation(async () => new Response("{}"));
  vi.stubGlobal("fetch", fetch);
  setConsoleToken("client-token");
  const signal = new AbortController().signal;
  await getJob("trace/job?x#y", signal);
  await cancelJob("trace/job?x#y");
  await getAnalyticsJobs("archive", 10, 20, 250, '[15,"trace/job"]', signal);
  await getJobs(["queued"], 100, '[15,"trace/job"]', signal, {
    interface: " gpu.infer ",
    namespace: "tenant-a",
  });
  await getJobs([], 50, 0, signal, { interface: "all", namespace: "  " });
  await getJob("job-ns", signal, "tenant-a");
  await cancelJob("job-ns", "tenant-a");
  await getJob("job-def", signal, "default");

  expect(fetch.mock.calls[0][0]).toBe("/api/jobs/trace%2Fjob%3Fx%23y");
  expect(fetch.mock.calls[1][0]).toBe("/api/jobs/trace%2Fjob%3Fx%23y/cancel");

  const analyticsUrl = new URL(fetch.mock.calls[2][0], "http://localhost");
  expect(analyticsUrl.searchParams.get("cursor")).toBe('[15,"trace/job"]');
  expect(fetch.mock.calls[2][1].signal).toBe(signal);
  expect(fetch.mock.calls[2][1].headers.Authorization).toBe("Bearer client-token");

  const filteredUrl = new URL(fetch.mock.calls[3][0], "http://localhost");
  expect(filteredUrl.searchParams.get("page_token")).toBe('[15,"trace/job"]');
  expect(filteredUrl.searchParams.get("interface")).toBe("gpu.infer");
  expect(filteredUrl.searchParams.get("namespace")).toBe("tenant-a");

  const allStatesUrl = new URL(fetch.mock.calls[4][0], "http://localhost");
  expect(allStatesUrl.searchParams.has("states")).toBe(false);
  expect(allStatesUrl.searchParams.has("interface")).toBe(false);
  expect(allStatesUrl.searchParams.has("namespace")).toBe(false);

  expect(fetch.mock.calls[5][0]).toBe("/api/jobs/job-ns?namespace=tenant-a");
  expect(fetch.mock.calls[6][0]).toBe("/api/jobs/job-ns/cancel?namespace=tenant-a");
  expect(fetch.mock.calls[7][0]).toBe("/api/jobs/job-def");
});

it("preserves authentication vs authorization errors and info/drain helpers", async () => {
  const fetchMock = vi
    .fn()
    .mockResolvedValueOnce(new Response('{"error":"expired"}', { status: 401 }))
    .mockResolvedValueOnce(new Response('{"error":"forbidden"}', { status: 403 }))
    .mockResolvedValueOnce(new Response('{"auth_enabled":true,"grpc_enabled":false}', { status: 200 }))
    .mockResolvedValueOnce(new Response('{"session_id":"s1","draining":true}', { status: 200 }));
  vi.stubGlobal("fetch", fetchMock);
  await expect(getOverview()).rejects.toBeInstanceOf(AuthenticationError);
  await expect(getOverview()).rejects.toBeInstanceOf(AuthorizationError);
  const info = await fetchInfo();
  expect(info.grpc_enabled).toBe(false);
  const drain = await drainWorker("s/1");
  expect(drain.draining).toBe(true);
  expect(fetchMock.mock.calls[3][0]).toBe("/api/workers/s%2F1/drain");
});

it("safeHttpUrl allows http/https and blocks javascript:/data:/unsafe schemes", () => {
  expect(safeHttpUrl("https://relay.example.com/tokens")).toBe("https://relay.example.com/tokens");
  expect(safeHttpUrl("  http://127.0.0.1:8080/tokens  ")).toBe("http://127.0.0.1:8080/tokens");
  expect(safeHttpUrl("/tokens")).toBe(`${window.location.origin}/tokens`);
  expect(safeHttpUrl("javascript:alert(1)")).toBeNull();
  expect(safeHttpUrl("  JaVaScRiPt:alert(1)")).toBeNull();
  expect(safeHttpUrl("data:text/html,<script>alert(1)</script>")).toBeNull();
  expect(safeHttpUrl("vbscript:msgbox(1)")).toBeNull();
  expect(safeHttpUrl("ftp://example.com/file")).toBeNull();
  expect(safeHttpUrl("")).toBeNull();
  expect(safeHttpUrl("   ")).toBeNull();
  expect(safeHttpUrl(null)).toBeNull();
  expect(safeHttpUrl(undefined)).toBeNull();
});

it("deriveEndpoint converts http(s) to ws(s) when grpc_enabled is false and handles redacted grpc_listen", () => {
  const baseCp: ControlPlaneSummary = {
    grpc_enabled: true,
    grpc_listen: "127.0.0.1:50052",
    grpc_tls_enabled: false,
    console_listen: "0.0.0.0:3001",
    auth_enabled: true,
    generation: 1,
    retained_jobs: 0,
    sessions: 0,
    max_sessions: 100,
    priority_levels: 4,
    default_max_attempts: 3,
    heartbeat_interval_ms: 5000,
    heartbeat_timeout_ms: 15000,
    reconnect_grace_ms: 3000,
  };

  // grpc_enabled: false converts https:// public_endpoint -> wss:// and http:// -> ws://
  expect(
    deriveEndpoint({ ...baseCp, grpc_enabled: false, public_endpoint: "https://relay.example.com" }),
  ).toBe("wss://relay.example.com");
  expect(
    deriveEndpoint({ ...baseCp, grpc_enabled: false, public_endpoint: "http://relay.local:3001" }),
  ).toBe("ws://relay.local:3001");
  expect(
    deriveEndpoint({ ...baseCp, grpc_enabled: false, public_endpoint: "wss://already.ws.example" }),
  ).toBe("wss://already.ws.example");

  // grpc_enabled: false without public_endpoint converts window.location.origin to ws(s)://
  expect(
    deriveEndpoint({ ...baseCp, grpc_enabled: false, public_endpoint: null }),
  ).toBe(window.location.origin.replace(/^http/, "ws"));

  // grpc_enabled: true with redacted empty grpc_listen falls back cleanly instead of "http://"
  expect(
    deriveEndpoint({ ...baseCp, grpc_enabled: true, grpc_listen: "" }),
  ).toBe("http://127.0.0.1:50052");
  expect(
    deriveEndpoint({ ...baseCp, grpc_enabled: true, grpc_listen: "   ", grpc_tls_enabled: true }),
  ).toBe("https://127.0.0.1:50052");

  // Wildcard host resolution for 0.0.0.0: and [::]:
  const host = window.location.hostname || "127.0.0.1";
  expect(
    deriveEndpoint({ ...baseCp, grpc_enabled: true, grpc_listen: "0.0.0.0:50052" }),
  ).toBe(`http://${host}:50052`);
  expect(
    deriveEndpoint({ ...baseCp, grpc_enabled: true, grpc_listen: "[::]:50052", grpc_tls_enabled: true }),
  ).toBe(`https://${host}:50052`);
});
