// @vitest-environment jsdom
import { afterEach, expect, it, vi } from "vitest";
import { AuthenticationError, getJob, getJobs, getOverview, getWorkers, setConsoleToken } from "./api";

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

it("preserves authentication errors for active requests", async () => {
  vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response('{"error":"expired"}', { status: 401 })));
  await expect(getOverview()).rejects.toBeInstanceOf(AuthenticationError);
});
