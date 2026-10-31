// @vitest-environment jsdom
import { afterEach, expect, it, vi } from "vitest";
import { AuthenticationError, cancelJob, getAnalyticsJobs, getJob, getJobs, getOverview, getWorkers, setConsoleToken } from "./api";

afterEach(() => { vi.unstubAllGlobals(); sessionStorage.clear(); });

it("forwards cancellation and authentication to every polling endpoint", async () => {
  const fetch = vi.fn().mockImplementation(async () => new Response("{}"));
  vi.stubGlobal("fetch", fetch);
  setConsoleToken(" client-token ");
  const signal = new AbortController().signal;
  await Promise.all([getOverview(signal), getJobs(["queued"], 100, 5, signal), getJob("job", signal), getWorkers(signal)]);
  expect(fetch.mock.calls.map(([url]) => url)).toEqual([
    "/api/overview", "/api/jobs?states=queued&limit=100&offset=5", "/api/jobs/job", "/api/workers",
  ]);
  for (const [, init] of fetch.mock.calls) {
    expect(init.signal).toBe(signal);
    expect(init.headers.Authorization).toBe("Bearer client-token");
  }
});

it("encodes custom job IDs and metadata cursors while preserving authorization", async () => {
  const fetch = vi.fn().mockImplementation(async () => new Response("{}"));
  vi.stubGlobal("fetch", fetch); setConsoleToken("client-token");
  const signal = new AbortController().signal;
  await getJob("trace/job?x#y", signal);
  await cancelJob("trace/job?x#y");
  await getAnalyticsJobs("archive", 10, 20, 250, '[15,"trace/job"]', signal);
  expect(fetch.mock.calls[0][0]).toBe("/api/jobs/trace%2Fjob%3Fx%23y");
  expect(fetch.mock.calls[1][0]).toBe("/api/jobs/trace%2Fjob%3Fx%23y/cancel");
  const url = new URL(fetch.mock.calls[2][0], "http://localhost");
  expect(url.searchParams.get("cursor")).toBe('[15,"trace/job"]');
  expect(fetch.mock.calls[2][1].signal).toBe(signal);
  expect(fetch.mock.calls[2][1].headers.Authorization).toBe("Bearer client-token");
});

it("preserves authentication errors for active requests", async () => {
  vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response('{"error":"expired"}', { status: 401 })));
  await expect(getOverview()).rejects.toBeInstanceOf(AuthenticationError);
});
