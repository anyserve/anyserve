// @vitest-environment jsdom
import { StrictMode, type PropsWithChildren } from "react";
import { act, cleanup, renderHook } from "@testing-library/react";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { usePollingEffect } from "./usePollingEffect";

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((ok, fail) => { resolve = ok; reject = fail; });
  return { promise, resolve, reject };
}

beforeEach(() => vi.useFakeTimers());
afterEach(() => { cleanup(); vi.useRealTimers(); });

it("waits for a slow request to finish before scheduling the next poll", async () => {
  const pending = deferred<string>();
  const task = vi.fn().mockReturnValue(pending.promise);
  const success = vi.fn();
  renderHook(() => usePollingEffect(task, success, vi.fn(), 1000, []));
  await act(() => vi.advanceTimersByTimeAsync(10_000));
  expect(task).toHaveBeenCalledTimes(1);
  await act(async () => pending.resolve("done"));
  expect(success).toHaveBeenCalledWith("done");
  await act(() => vi.advanceTimersByTimeAsync(999));
  expect(task).toHaveBeenCalledTimes(1);
  await act(() => vi.advanceTimersByTimeAsync(1));
  expect(task).toHaveBeenCalledTimes(2);
});

it.each(["resolve", "reject"] as const)("ignores an old request that %ss after selection changes", async (outcome) => {
  const old = deferred<string>();
  const next = deferred<string>();
  const task = vi.fn().mockReturnValueOnce(old.promise).mockReturnValueOnce(next.promise);
  const success = vi.fn();
  const error = vi.fn();
  const { rerender } = renderHook(({ id }) => usePollingEffect(task, success, error, 1000, [id]), {
    initialProps: { id: "A" },
  });
  const signal = task.mock.calls[0][0] as AbortSignal;
  rerender({ id: "B" });
  expect(signal.aborted).toBe(true);
  await act(async () => next.resolve("B"));
  await act(async () => old[outcome]("A"));
  expect(success.mock.calls).toEqual([["B"]]);
  expect(error).not.toHaveBeenCalled();
  expect(vi.getTimerCount()).toBe(1);
});

it("stops pending requests and timers when disabled or unmounted", async () => {
  const pending = deferred<string>();
  const task = vi.fn().mockReturnValue(pending.promise);
  const success = vi.fn();
  const { rerender, unmount } = renderHook(({ enabled }) => usePollingEffect(task, success, vi.fn(), 1000, [], enabled), {
    initialProps: { enabled: false },
  });
  expect(task).not.toHaveBeenCalled();
  rerender({ enabled: true });
  const signal = task.mock.calls[0][0] as AbortSignal;
  rerender({ enabled: false });
  await act(async () => pending.resolve("late"));
  expect(signal.aborted).toBe(true);
  expect(success).not.toHaveBeenCalled();
  expect(vi.getTimerCount()).toBe(0);
  rerender({ enabled: true });
  unmount();
  await act(async () => {});
  expect((task.mock.calls[1][0] as AbortSignal).aborted).toBe(true);
  expect(success).not.toHaveBeenCalled();
  expect(vi.getTimerCount()).toBe(0);
});

it("reports failures and retries without restarting on callback-only renders", async () => {
  const failure = new Error("offline");
  const first = vi.fn().mockRejectedValue(failure);
  const next = vi.fn().mockResolvedValue("new callback");
  const success = vi.fn();
  const error = vi.fn();
  const { rerender } = renderHook(({ task }) => usePollingEffect(task, success, error, 1000, []), {
    initialProps: { task: first },
  });
  await act(async () => {});
  expect(error).toHaveBeenCalledWith(failure);
  rerender({ task: next });
  expect(next).not.toHaveBeenCalled();
  await act(() => vi.advanceTimersByTimeAsync(1000));
  expect(next).toHaveBeenCalledTimes(1);
  expect(success).toHaveBeenCalledWith("new callback");
});

it("discards the first StrictMode effect's result", async () => {
  const pending = deferred<string>();
  const task = vi.fn().mockReturnValue(pending.promise);
  const success = vi.fn();
  renderHook(() => usePollingEffect(task, success, vi.fn(), 1000, []), {
    wrapper: ({ children }: PropsWithChildren) => <StrictMode>{children}</StrictMode>,
  });
  expect(task).toHaveBeenCalledTimes(2);
  expect((task.mock.calls[0][0] as AbortSignal).aborted).toBe(true);
  await act(async () => pending.resolve("current"));
  expect(success.mock.calls).toEqual([["current"]]);
  expect(vi.getTimerCount()).toBe(1);
});

it("aborts hidden-page requests and immediately refreshes on return without stale callbacks", async () => {
  const visibility = vi.spyOn(document, "visibilityState", "get");
  visibility.mockReturnValue("hidden");
  const old = deferred<string>();
  const task = vi.fn().mockReturnValueOnce(old.promise).mockResolvedValue("fresh");
  const success = vi.fn();
  const { unmount } = renderHook(() => usePollingEffect(task, success, vi.fn(), 1000, []));
  await act(() => vi.advanceTimersByTimeAsync(60_000));
  expect(task).not.toHaveBeenCalled();
  visibility.mockReturnValue("visible");
  act(() => document.dispatchEvent(new Event("visibilitychange")));
  expect(task).toHaveBeenCalledTimes(1);
  visibility.mockReturnValue("hidden");
  act(() => document.dispatchEvent(new Event("visibilitychange")));
  expect((task.mock.calls[0][0] as AbortSignal).aborted).toBe(true);
  await act(async () => old.resolve("stale"));
  await act(() => vi.advanceTimersByTimeAsync(60_000));
  expect(success).not.toHaveBeenCalled();
  expect(task).toHaveBeenCalledTimes(1);
  visibility.mockReturnValue("visible");
  await act(async () => document.dispatchEvent(new Event("visibilitychange")));
  expect(success.mock.calls).toEqual([["fresh"]]);
  expect(vi.getTimerCount()).toBe(1);
  unmount();
  act(() => document.dispatchEvent(new Event("visibilitychange")));
  expect(task).toHaveBeenCalledTimes(2);
  visibility.mockRestore();
});
