// @vitest-environment jsdom
import { afterEach, expect, it, vi } from "vitest";
import {
  AuthenticationError,
  cancelJob,
  clearConsoleToken,
  getAnalyticsJobs,
  getConsoleToken,
  getJob,
  getJobs,
  getOverview,
  getWorkers,
  hasConsoleToken,
  setConsoleToken,
} from "./api";

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

it("preserves authentication errors for active requests", async () => {
  vi.stubGlobal(
    "fetch",
    vi.fn().mockResolvedValue(new Response('{"error":"expired"}', { status: 401 })),
  );
  await expect(getOverview()).rejects.toBeInstanceOf(AuthenticationError);
});
