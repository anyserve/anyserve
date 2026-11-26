import { useCallback, useEffect, useRef, useState, useSyncExternalStore } from "react";

import { getConsoleToken, safeHttpUrl } from "./api";
import { browserWorkerManager, type BrowserWorkerManager, type BrowserWorkerSlotState } from "./browser-worker/manager";
import {
  ECHO_INTERFACE,
  LLM_INTERFACE,
  SMOLLM_MODEL_ID,
  deriveWsBaseUrl,
  invokeStreamingJob,
  type StreamingInvokeResult,
  type StreamingInvokeTimelineEvent,
} from "./browser-worker/protocol";
import type { WorkerSlotRole } from "./browser-worker/worker";
import { formatDuration } from "./format";
import { IconArrowRight, IconKey, IconPlayground } from "./icons";
import type { ControlPlaneSummary } from "./types";

interface PlaygroundPaneProps {
  controlPlane?: ControlPlaneSummary;
  authPortalUrl?: string | null;
  lockedNamespace?: string | null;
  manager?: BrowserWorkerManager;
  onOpenJobInJobs: (jobId: string, namespace?: string) => void;
  onOpenWorkers: () => void;
  onRefresh?: () => void;
}

const DEFAULT_ECHO_PROMPT = "Schedule this prompt from the browser and stream every word back with waterfall tracing.";
const DEFAULT_LLM_PROMPT = "Explain in two short sentences why outbound workers help when a GPU sits behind NAT.";

function statusTone(status: BrowserWorkerSlotState["status"]): "ok" | "warn" | "bad" | "offline" {
  switch (status) {
    case "online":
    case "busy":
      return "ok";
    case "connecting":
      return "warn";
    case "killed":
    case "error":
      return "bad";
    default:
      return "offline";
  }
}

function statusLabel(slot: BrowserWorkerSlotState): string {
  switch (slot.status) {
    case "online":
      return "Online";
    case "busy":
      return slot.activeJobId ? `Running ${slot.activeJobId.slice(0, 8)}` : "Busy";
    case "connecting":
      return "Connecting…";
    case "killed":
      return "Killed (worker.terminate())";
    case "error":
      return "Error";
    default:
      return "Stopped";
  }
}

export function PlaygroundPane({
  controlPlane,
  authPortalUrl,
  lockedNamespace,
  manager = browserWorkerManager,
  onOpenJobInJobs,
  onOpenWorkers,
  onRefresh,
}: PlaygroundPaneProps) {
  const snap = useSyncExternalStore(manager.subscribe, manager.getSnapshot, manager.getSnapshot);
  const authEnabled = Boolean(controlPlane?.auth_enabled);
  const safePortalUrl = safeHttpUrl(authPortalUrl);
  const wsBaseUrl = deriveWsBaseUrl(import.meta.env.VITE_CONSOLE_API_BASE_URL ?? "");

  // Worker token lives strictly in component/worker memory — never overwrites Console's client token.
  const [workerToken, setWorkerToken] = useState("");
  const [selectedInterface, setSelectedInterface] = useState<string>(ECHO_INTERFACE);
  const [prompt, setPrompt] = useState(DEFAULT_ECHO_PROMPT);
  const [running, setRunning] = useState(false);
  const [liveOutput, setLiveOutput] = useState("");
  const [liveProgress, setLiveProgress] = useState<string | null>(null);
  const [liveTimeline, setLiveTimeline] = useState<StreamingInvokeTimelineEvent[]>([]);
  const [lastResult, setLastResult] = useState<StreamingInvokeResult | null>(null);
  const [activeJobId, setActiveJobId] = useState<string | null>(null);
  const [invokeError, setInvokeError] = useState<string | null>(null);
  const abortRef = useRef<AbortController | null>(null);

  useEffect(() => () => abortRef.current?.abort(), []);

  const primary = snap.slots.primary;
  const standby = snap.slots.standby;
  const primaryOnline = primary.status === "online" || primary.status === "busy";
  const standbyOnline = standby.status === "online" || standby.status === "busy";
  const anyOnline = primaryOnline || standbyOnline;
  const anyModelReady =
    (primaryOnline && primary.modelStatus === "ready") || (standbyOnline && standby.modelStatus === "ready");
  const effectiveWorkerToken = authEnabled ? workerToken.trim() : "";
  const needsWorkerToken = authEnabled && effectiveWorkerToken.length === 0;
  const reconnectGraceMs = controlPlane?.reconnect_grace_ms ?? controlPlane?.heartbeat_timeout_ms ?? 15_000;
  const failoverKillTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(
    () => () => {
      if (failoverKillTimerRef.current !== null) clearTimeout(failoverKillTimerRef.current);
    },
    [],
  );

  const ensureSlotOnline = useCallback(
    async (slot: WorkerSlotRole) => {
      const current = manager.getSnapshot().slots[slot];
      if (current.status === "online" || current.status === "busy") return;
      await manager.startWorker(slot, {
        wsBaseUrl,
        token: authEnabled ? workerToken.trim() || null : null,
      });
    },
    [manager, wsBaseUrl, authEnabled, workerToken],
  );

  const handleStartSlot = useCallback(
    async (slot: WorkerSlotRole) => {
      setInvokeError(null);
      if (needsWorkerToken) {
        setInvokeError("Paste a worker token above before starting a browser worker (client tokens cannot open worker sessions).");
        return;
      }
      try {
        await manager.startWorker(slot, {
          wsBaseUrl,
          token: authEnabled ? effectiveWorkerToken : null,
        });
        onRefresh?.();
      } catch (err) {
        setInvokeError(err instanceof Error ? err.message : String(err));
      }
    },
    [manager, wsBaseUrl, authEnabled, effectiveWorkerToken, needsWorkerToken, onRefresh],
  );

  const handleLoadModel = useCallback(
    async (slot: WorkerSlotRole) => {
      setInvokeError(null);
      try {
        if (manager.getSnapshot().slots[slot].status === "stopped" || manager.getSnapshot().slots[slot].status === "killed") {
          if (needsWorkerToken) {
            setInvokeError("Paste a worker token above first.");
            return;
          }
          await ensureSlotOnline(slot);
        }
        manager.loadModel(slot);
      } catch (err) {
        setInvokeError(err instanceof Error ? err.message : String(err));
      }
    },
    [manager, needsWorkerToken, ensureSlotOnline],
  );

  const runInvoke = useCallback(
    async (failoverDemo: boolean) => {
      setInvokeError(null);
      setLastResult(null);
      setLiveOutput("");
      setLiveProgress(null);
      setLiveTimeline([]);
      setActiveJobId(null);

      if (failoverDemo && needsWorkerToken && (!primaryOnline || !standbyOnline)) {
        setInvokeError("Paste a worker token above to start both primary and standby browser workers for the failover demo.");
        return;
      }
      if (!failoverDemo && !anyOnline) {
        if (needsWorkerToken) {
          setInvokeError("Paste a worker token above and start a browser worker first.");
          return;
        }
        try {
          await ensureSlotOnline("primary");
        } catch (err) {
          setInvokeError(err instanceof Error ? err.message : String(err));
          return;
        }
      }

      const targetInterface = failoverDemo ? ECHO_INTERFACE : selectedInterface;
      if (!failoverDemo && targetInterface === LLM_INTERFACE && !anyModelReady) {
        setInvokeError("Load SmolLM2-135M on an online browser worker first, or switch to browser.echo.v1.");
        return;
      }

      if (failoverDemo) {
        try {
          await Promise.all([ensureSlotOnline("primary"), ensureSlotOnline("standby")]);
          onRefresh?.();
        } catch (err) {
          setInvokeError(err instanceof Error ? err.message : String(err));
          return;
        }
      }

      const controller = new AbortController();
      abortRef.current = controller;
      setRunning(true);
      let killedPrimary = false;

      try {
        const res = await invokeStreamingJob({
          wsBaseUrl,
          token: getConsoleToken(),
          interfaceName: targetInterface,
          prompt: prompt.trim() || (targetInterface === LLM_INTERFACE ? DEFAULT_LLM_PROMPT : DEFAULT_ECHO_PROMPT),
          preferredAttributes: failoverDemo ? { role: "primary" } : undefined,
          metadata: failoverDemo
            ? {
                "demo.prefill_ms": "2500",
                "demo.scenario": "real-failover-terminate",
              }
            : { "demo.scenario": targetInterface === LLM_INTERFACE ? "smollm2-135m" : "echo" },
          maxAttempts: failoverDemo ? 3 : 2,
          signal: controller.signal,
          onJobAccepted: (jid) => setActiveJobId(jid),
          onAssigned: (assignedWorkerId, attemptNum) => {
            if (failoverDemo && attemptNum === 1 && !killedPrimary && assignedWorkerId === "browser-worker-1") {
              killedPrimary = true;
              failoverKillTimerRef.current = setTimeout(() => {
                failoverKillTimerRef.current = null;
                if (controller.signal.aborted) return;
                manager.killWorker("primary");
                setLiveProgress(
                  `browser-worker-1 killed via worker.terminate() — waiting reconnect_grace_ms (${formatDuration(reconnectGraceMs)}) for scheduler failover to browser-worker-2…`,
                );
              }, 250);
            }
          },
          onChunk: (chunk) => setLiveOutput((cur) => cur + chunk),
          onProgress: (msg) => setLiveProgress(msg),
          onTimeline: (ev) => setLiveTimeline((cur) => [...cur, ev]),
        });
        setLastResult(res);
        setActiveJobId(res.jobId);
        onRefresh?.();
      } catch (err) {
        setInvokeError(err instanceof Error ? err.message : String(err));
      } finally {
        if (failoverKillTimerRef.current !== null) {
          clearTimeout(failoverKillTimerRef.current);
          failoverKillTimerRef.current = null;
        }
        if (abortRef.current === controller) {
          abortRef.current = null;
        }
        setRunning(false);
      }
    },
    [
      needsWorkerToken,
      primaryOnline,
      standbyOnline,
      anyOnline,
      anyModelReady,
      selectedInterface,
      wsBaseUrl,
      prompt,
      ensureSlotOnline,
      manager,
      reconnectGraceMs,
      onRefresh,
    ],
  );

  const handleSelectInterface = (iface: string) => {
    setSelectedInterface(iface);
    setPrompt((cur) => {
      if (iface === LLM_INTERFACE && cur === DEFAULT_ECHO_PROMPT) return DEFAULT_LLM_PROMPT;
      if (iface === ECHO_INTERFACE && cur === DEFAULT_LLM_PROMPT) return DEFAULT_ECHO_PROMPT;
      return cur;
    });
  };

  const renderWorkerCard = (slot: BrowserWorkerSlotState) => {
    const isLive = slot.status === "online" || slot.status === "busy";
    const isConnecting = slot.status === "connecting";
    return (
      <div key={slot.slot} className="pg-worker-card">
        <div className="pg-worker-card__head">
          <div className="pg-worker-card__id">
            <span className={`status-dot status-dot--${statusTone(slot.status)}`} aria-hidden="true" />
            <span className="mono">{slot.workerId}</span>
            <span className="tag">{slot.slot}</span>
            {slot.backend !== "echo-only" ? <span className="tag tag--ok">{slot.backend.toUpperCase()}</span> : null}
          </div>
          <div className="pg-worker-card__actions">
            {isLive || isConnecting ? (
              <>
                <button
                  type="button"
                  className="btn btn--danger"
                  title="Abruptly call worker.terminate() to drop WebSockets and test real scheduler failover"
                  onClick={() => manager.killWorker(slot.slot)}
                >
                  Kill worker
                </button>
                <button type="button" className="btn btn--ghost" onClick={() => manager.stopWorker(slot.slot)}>
                  Stop
                </button>
              </>
            ) : (
              <button type="button" className="btn btn--primary" onClick={() => void handleStartSlot(slot.slot)}>
                {slot.status === "killed" ? "Restart" : "Start Worker"}
              </button>
            )}
          </div>
        </div>

        <div className="pg-worker-card__meta">
          <span>{statusLabel(slot)}</span>
          {slot.sessionId ? <span className="mono">session {slot.sessionId.slice(0, 12)}</span> : null}
          <span className="num">Completed: {slot.jobsCompleted}</span>
        </div>

        {slot.interfaces.length > 0 ? (
          <div className="pg-worker-card__tags">
            {slot.interfaces.map((iface) => (
              <span key={iface} className="tag mono">
                {iface}
              </span>
            ))}
          </div>
        ) : null}

        <div className="pg-worker-card__model">
          <div className="pg-worker-card__model-row">
            <button
              type="button"
              className="btn"
              disabled={slot.modelStatus === "loading" || slot.modelStatus === "ready"}
              onClick={() => void handleLoadModel(slot.slot)}
            >
              {slot.modelStatus === "ready"
                ? `SmolLM2-135M Ready (${slot.backend.toUpperCase()})`
                : slot.modelStatus === "loading"
                  ? `Loading SmolLM2-135M (${slot.modelProgress}%)…`
                  : "Load SmolLM2-135M (WebGPU / WASM)"}
            </button>
          </div>
          {slot.modelStatus === "loading" ? (
            <div className="pg-progress">
              <div className="pg-progress__track">
                <div className="pg-progress__fill" style={{ width: `${slot.modelProgress}%` }} />
              </div>
              <span className="muted mono ellipsis">{slot.modelFile ?? "Downloading weights…"}</span>
            </div>
          ) : null}
          {slot.modelError ? <div className="pg-inline-err">{slot.modelError}</div> : null}
          {slot.error ? <div className="pg-inline-err">{slot.error}</div> : null}
        </div>
      </div>
    );
  };

  return (
    <section className="panel panel--full pg-pane">
      <header className="panel__header">
        <div className="panel__header-title">
          <h1>
            <IconPlayground style={{ verticalAlign: "-2px", marginRight: 6 }} />
            Browser Worker Playground
          </h1>
          <span>Outbound Dedicated Web Workers over /ws/v2/* — stays alive while inspecting Jobs &amp; Workers</span>
        </div>
        <div className="panel__header-actions">
          <button type="button" className="btn" onClick={onOpenWorkers}>
            View in Workers <IconArrowRight />
          </button>
        </div>
      </header>

      <div className="pg-body">
        <div className="pg-auth-strip">
          {authEnabled ? (
            <>
              <label className="pg-auth-strip__field">
                <span>Worker token (kept in memory only; never replaces your Console client token)</span>
                <input
                  type="password"
                  autoComplete="off"
                  placeholder="Paste worker token (as_w_… or asv1.…)"
                  value={workerToken}
                  onChange={(e) => setWorkerToken(e.target.value)}
                />
              </label>
              {safePortalUrl ? (
                <a className="btn" href={safePortalUrl} target="_blank" rel="noopener noreferrer">
                  <IconKey /> Get Worker Token
                </a>
              ) : null}
            </>
          ) : (
            <span className="tag tag--ok">Auth disabled (local anonymous mode) — ready to connect</span>
          )}
          <span className="tag mono">WS: {wsBaseUrl}</span>
        </div>

        <div className="pg-grid">
          <div className="pg-col">
            <div className="pg-section-head">
              <h2>1. Browser Workers (Dedicated Web Workers)</h2>
              <p className="muted">
                Heartbeat &amp; execution run in a Dedicated Web Worker (unaffected by main-thread timer throttling).{" "}
                <strong>SmolLM2-135M</strong> (~118 MB cached from CDN, Apache 2.0) is a{" "}
                <strong>135M small model — demos scheduling &amp; streaming, not IQ</strong>; falls back to WASM CPU when WebGPU is unavailable.
              </p>
            </div>
            <div className="pg-workers-list">
              {renderWorkerCard(primary)}
              {renderWorkerCard(standby)}
            </div>
            {snap.logs.length > 0 ? (
              <div className="pg-logs">
                <div className="pg-logs__title">Worker events</div>
                <pre className="code-block">
                  {snap.logs.map((l) => `[${l.at}] ${l.message}`).join("\n")}
                </pre>
              </div>
            ) : null}
          </div>

          <div className="pg-col">
            <div className="pg-section-head">
              <h2>2. Live Streaming Invoke &amp; Failover</h2>
              <p className="muted">
                Streams over <code>/ws/v2/client/invoke</code> with <code>Sec-WebSocket-Protocol</code> auth and reports exact{" "}
                <code>handler_duration_ms</code> into the Execution Waterfall.
              </p>
            </div>

            <div className="seg-control" role="tablist" aria-label="Target interface">
              <button
                type="button"
                role="tab"
                aria-selected={selectedInterface === ECHO_INTERFACE}
                className={`seg-control__btn${selectedInterface === ECHO_INTERFACE ? " is-active" : ""}`}
                onClick={() => handleSelectInterface(ECHO_INTERFACE)}
              >
                {ECHO_INTERFACE} (0 MB · Instant)
              </button>
              <button
                type="button"
                role="tab"
                aria-selected={selectedInterface === LLM_INTERFACE}
                className={`seg-control__btn${selectedInterface === LLM_INTERFACE ? " is-active" : ""}`}
                onClick={() => handleSelectInterface(LLM_INTERFACE)}
              >
                {LLM_INTERFACE} (SmolLM2-135M)
              </button>
            </div>

            <label className="pg-prompt-label">
              <span>Prompt</span>
              <textarea
                rows={3}
                value={prompt}
                onChange={(e) => setPrompt(e.target.value)}
                placeholder="Type a prompt to stream through AnyServe…"
              />
            </label>

            <div className="pg-actions">
              {running ? (
                <button type="button" className="btn btn--danger" onClick={() => abortRef.current?.abort()}>
                  Cancel In-Flight Job
                </button>
              ) : (
                <>
                  <button type="button" className="btn btn--primary" onClick={() => void runInvoke(false)}>
                    Run Streaming Invoke
                  </button>
                  <button type="button" className="btn" onClick={() => void runInvoke(true)}>
                    Kill worker &amp; test failover
                  </button>
                </>
              )}
            </div>

            <p className="pg-note muted">
              <strong>Real failover (no faked errors):</strong> starts primary + standby workers, routes attempt #1 to{" "}
              <code>browser-worker-1</code> via <code>preferred_attributes</code>, and calls real{" "}
              <code>worker.terminate()</code> during prefill (before the first output token). The control plane detects
              the dropped WebSocket, holds the lease for <code>reconnect_grace_ms</code> (currently{" "}
              <strong className="mono">{formatDuration(reconnectGraceMs)}</strong>; configurable via{" "}
              <code>[runtime].reconnect_grace_ms</code> or per-pool <code>pool_tiers.reconnect_grace_ms</code>), then
              automatically retries attempt #2 on <code>browser-worker-2</code>. Once tokens have streamed to the
              caller, AnyServe intentionally fails fast with <code>output_interrupted</code> instead of splicing two
              streams.
            </p>

            {invokeError ? (
              <div className="pg-inline-err" role="alert">
                {invokeError}
              </div>
            ) : null}

            <div className="pg-output-card">
              <div className="pg-output-card__head">
                <span className="mono">
                  {activeJobId ? `job ${activeJobId}` : "Output stream"}
                </span>
                <div className="pg-output-card__badges">
                  {lastResult ? (
                    <>
                      <span className={`tag ${lastResult.state === "succeeded" ? "tag--ok" : ""}`}>
                        {lastResult.state}
                      </span>
                      {lastResult.restarted ? (
                        <span className="tag tag--warn">
                          failover · {lastResult.attempts} attempts
                          {lastResult.failoverMs != null ? ` (${formatDuration(lastResult.failoverMs)})` : ""}
                        </span>
                      ) : null}
                      {lastResult.ttftMs != null ? (
                        <span className="tag mono">TTFT {formatDuration(lastResult.ttftMs)}</span>
                      ) : null}
                      <span className="tag mono">Total {formatDuration(lastResult.totalMs)}</span>
                    </>
                  ) : null}
                  {activeJobId ? (
                    <button
                      type="button"
                      className="btn btn--primary"
                      onClick={() => onOpenJobInJobs(activeJobId, lockedNamespace ?? undefined)}
                    >
                      Inspect Waterfall <IconArrowRight />
                    </button>
                  ) : null}
                </div>
              </div>
              {liveProgress ? <div className="pg-progress-note mono">{liveProgress}</div> : null}
              <pre className="code-block pg-output-pre">
                {liveOutput || (running ? "Waiting for tokens…" : "Run an invoke or failover demo to see live streamed tokens.")}
              </pre>
            </div>

            {liveTimeline.length > 0 ? (
              <div className="pg-timeline">
                <div className="pg-logs__title">Live scheduler events ({SMOLLM_MODEL_ID.split("/").pop()} / echo)</div>
                <ul className="pg-timeline__list">
                  {liveTimeline.map((ev, idx) => (
                    <li key={`${ev.kind}-${idx}`} className={`pg-timeline__item pg-timeline__item--${ev.kind}`}>
                      <span className="mono muted">+{formatDuration(ev.atMs)}</span>
                      <span className="tag mono">{ev.kind}</span>
                      <span>{ev.detail}</span>
                    </li>
                  ))}
                </ul>
              </div>
            ) : null}
          </div>
        </div>
      </div>
    </section>
  );
}
