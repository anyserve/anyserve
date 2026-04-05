import { useEffect, useRef } from "react";

export function usePollingEffect(
  task: () => Promise<void>,
  pollMs: number,
  deps: readonly unknown[],
  enabled = true,
) {
  const taskRef = useRef(task);

  useEffect(() => {
    taskRef.current = task;
  }, [task]);

  useEffect(() => {
    if (!enabled) {
      return;
    }

    let cancelled = false;

    async function refresh() {
      if (cancelled) {
        return;
      }
      await taskRef.current();
    }

    void refresh();
    const intervalId = window.setInterval(() => {
      void refresh();
    }, pollMs);

    return () => {
      cancelled = true;
      window.clearInterval(intervalId);
    };
  }, [enabled, pollMs, ...deps]);
}
