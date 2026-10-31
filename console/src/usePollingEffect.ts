import { useEffect, useRef } from "react";

export function usePollingEffect<T>(
  task: (signal: AbortSignal) => Promise<T>,
  onSuccess: (value: T) => void,
  onError: (error: unknown) => void,
  pollMs: number,
  deps: readonly unknown[],
  enabled = true,
) {
  const callbacks = useRef({ task, onSuccess, onError });

  useEffect(() => {
    callbacks.current = { task, onSuccess, onError };
  }, [task, onSuccess, onError]);

  useEffect(() => {
    if (!enabled) {
      return;
    }

    let controller: AbortController | undefined;
    let timeoutId: number | undefined;

    async function refresh(current: AbortController) {
      try {
        const value = await callbacks.current.task(current.signal);
        if (!current.signal.aborted) {
          callbacks.current.onSuccess(value);
        }
      } catch (error) {
        if (!current.signal.aborted) {
          callbacks.current.onError(error);
        }
      } finally {
        // Schedule from completion so a slow request cannot overlap the next.
        if (!current.signal.aborted) {
          timeoutId = window.setTimeout(() => void refresh(current), pollMs);
        }
      }
    }

    function stop() {
      controller?.abort();
      window.clearTimeout(timeoutId);
    }

    function resumeIfVisible() {
      stop();
      if (document.visibilityState !== "hidden") {
        controller = new AbortController();
        void refresh(controller);
      }
    }

    document.addEventListener("visibilitychange", resumeIfVisible);
    resumeIfVisible();
    return () => {
      document.removeEventListener("visibilitychange", resumeIfVisible);
      stop();
    };
  }, [enabled, pollMs, ...deps]);
}
