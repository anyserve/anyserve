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

  const pollMsRef = useRef(pollMs);
  pollMsRef.current = pollMs;

  const schedulerRef = useRef<{ reschedule: (nextPollMs: number) => void } | null>(null);

  useEffect(() => {
    if (!enabled) {
      schedulerRef.current = null;
      return;
    }
    let controller: AbortController | undefined;
    let timeoutId: number | undefined;
    let inFlight = false;
    let settled = false;
    let disposed = false;

    function armTimer(activeController: AbortController, delayMs: number) {
      window.clearTimeout(timeoutId);
      timeoutId = undefined;
      if (!disposed && !activeController.signal.aborted && delayMs > 0 && document.visibilityState !== "hidden") {
        timeoutId = window.setTimeout(() => {
          timeoutId = undefined;
          void refresh(activeController);
        }, delayMs);
      }
    }

    async function refresh(current: AbortController) {
      inFlight = true;
      try {
        const value = await callbacks.current.task(current.signal);
        if (!current.signal.aborted) callbacks.current.onSuccess(value);
      } catch (error) {
        if (!current.signal.aborted) callbacks.current.onError(error);
      } finally {
        if (controller === current) {
          inFlight = false;
        }
        if (!current.signal.aborted && !disposed) {
          settled = true;
          armTimer(current, pollMsRef.current);
        }
      }
    }

    function stop() {
      controller?.abort();
      controller = undefined;
      inFlight = false;
      window.clearTimeout(timeoutId);
      timeoutId = undefined;
    }

    function resumeIfVisible() {
      if (document.visibilityState === "hidden") {
        stop();
        return;
      }
      if (pollMsRef.current <= 0 && settled) {
        return;
      }
      stop();
      controller = new AbortController();
      void refresh(controller);
    }

    schedulerRef.current = {
      reschedule(nextPollMs: number) {
        if (disposed) return;
        window.clearTimeout(timeoutId);
        timeoutId = undefined;
        if (nextPollMs <= 0 || document.visibilityState === "hidden" || inFlight) {
          return;
        }
        if (!controller || controller.signal.aborted) {
          controller = new AbortController();
        }
        armTimer(controller, nextPollMs);
      },
    };

    document.addEventListener("visibilitychange", resumeIfVisible);
    resumeIfVisible();
    return () => {
      disposed = true;
      schedulerRef.current = null;
      document.removeEventListener("visibilitychange", resumeIfVisible);
      stop();
    };
  }, [enabled, ...deps]);

  const prevPollMsRef = useRef(pollMs);
  useEffect(() => {
    if (prevPollMsRef.current === pollMs) return;
    prevPollMsRef.current = pollMs;
    pollMsRef.current = pollMs;
    schedulerRef.current?.reschedule(pollMs);
  }, [pollMs]);
}
