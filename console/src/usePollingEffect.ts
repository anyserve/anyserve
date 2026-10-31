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

    const controller = new AbortController();
    let timeoutId: number | undefined;

    async function refresh() {
      try {
        const value = await callbacks.current.task(controller.signal);
        if (!controller.signal.aborted) {
          callbacks.current.onSuccess(value);
        }
      } catch (error) {
        if (!controller.signal.aborted) {
          callbacks.current.onError(error);
        }
      } finally {
        // Schedule from completion so a slow request cannot overlap the next.
        if (!controller.signal.aborted) {
          timeoutId = window.setTimeout(() => void refresh(), pollMs);
        }
      }
    }

    void refresh();
    return () => {
      controller.abort();
      window.clearTimeout(timeoutId);
    };
  }, [enabled, pollMs, ...deps]);
}
