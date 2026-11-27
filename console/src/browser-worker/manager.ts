import { WorkerRtcPeer } from "./rtc";
import type { ComputeBackend, HostToWorkerCmd, WorkerSlotRole, WorkerToHostEvent } from "./worker";

export type BrowserWorkerStatus = "stopped" | "connecting" | "online" | "busy" | "killed" | "error";

export interface BrowserWorkerSlotState {
  slot: WorkerSlotRole;
  workerId: string;
  status: BrowserWorkerStatus;
  sessionId: string | null;
  interfaces: string[];
  activeJobId: string | null;
  jobsCompleted: number;
  backend: ComputeBackend;
  error: string | null;
  modelStatus: "idle" | "loading" | "ready" | "error";
  modelProgress: number;
  modelFile: string | null;
  modelError: string | null;
}

export interface BrowserWorkerSnapshot {
  slots: Record<WorkerSlotRole, BrowserWorkerSlotState>;
  logs: Array<{ at: string; message: string }>;
}

export type WorkerFactory = () => Pick<Worker, "postMessage" | "terminate" | "onmessage" | "onerror">;

function defaultWorkerFactory(): Worker {
  return new Worker(new URL("./worker.ts", import.meta.url), { type: "module" });
}

function makeInitialSlot(slot: WorkerSlotRole): BrowserWorkerSlotState {
  return {
    slot,
    workerId: slot === "primary" ? "browser-worker-1" : "browser-worker-2",
    status: "stopped",
    sessionId: null,
    interfaces: [],
    activeJobId: null,
    jobsCompleted: 0,
    backend: "echo-only",
    error: null,
    modelStatus: "idle",
    modelProgress: 0,
    modelFile: null,
    modelError: null,
  };
}

export class BrowserWorkerManager {
  private readonly factory: WorkerFactory;
  private readonly workers: Partial<Record<WorkerSlotRole, ReturnType<WorkerFactory>>> = {};
  private readonly rtcPeers: Record<WorkerSlotRole, Map<string, WorkerRtcPeer>> = {
    primary: new Map(),
    standby: new Map(),
  };
  private readonly listeners = new Set<() => void>();
  private readonly onlineWaiters: Partial<
    Record<WorkerSlotRole, Array<{ resolve: () => void; reject: (err: Error) => void }>>
  > = {};
  private snapshot: BrowserWorkerSnapshot = {
    slots: {
      primary: makeInitialSlot("primary"),
      standby: makeInitialSlot("standby"),
    },
    logs: [],
  };

  constructor(factory: WorkerFactory = defaultWorkerFactory) {
    this.factory = factory;
  }

  subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  };

  getSnapshot = (): BrowserWorkerSnapshot => this.snapshot;

  onlineCount(): number {
    let n = 0;
    for (const s of Object.values(this.snapshot.slots)) {
      if (s.status === "online" || s.status === "busy") n += 1;
    }
    return n;
  }

  private emitChange(): void {
    for (const fn of this.listeners) fn();
  }

  private pushLog(message: string): void {
    const now = new Date();
    const at = now.toTimeString().slice(0, 8);
    this.snapshot = {
      ...this.snapshot,
      logs: [{ at, message }, ...this.snapshot.logs].slice(0, 40),
    };
  }

  private updateSlot(slot: WorkerSlotRole, patch: Partial<BrowserWorkerSlotState>): void {
    this.snapshot = {
      ...this.snapshot,
      slots: {
        ...this.snapshot.slots,
        [slot]: { ...this.snapshot.slots[slot], ...patch },
      },
    };
  }

  private settleOnlineWaiters(slot: WorkerSlotRole, err?: Error): void {
    const list = this.onlineWaiters[slot];
    if (!list || list.length === 0) return;
    delete this.onlineWaiters[slot];
    for (const w of list) {
      if (err) w.reject(err);
      else w.resolve();
    }
  }

  private ensureWorker(slot: WorkerSlotRole): ReturnType<WorkerFactory> {
    let w = this.workers[slot];
    if (w) return w;
    w = this.factory();
    w.onmessage = (ev: MessageEvent<WorkerToHostEvent>) => {
      const msg = ev.data;
      if (!msg) return;
      if (msg.type === "state") {
        this.updateSlot(slot, {
          status: msg.status,
          sessionId: msg.sessionId,
          interfaces: msg.interfaces,
          activeJobId: msg.activeJobId,
          jobsCompleted: msg.jobsCompleted,
          backend: msg.backend,
          error: msg.error,
        });
        if (msg.status === "online" || msg.status === "busy") {
          this.settleOnlineWaiters(slot);
        } else if (msg.status === "error" || msg.status === "stopped") {
          this.settleOnlineWaiters(slot, new Error(msg.error || `Worker ${slot} ${msg.status}`));
        }
      } else if (msg.type === "model") {
        this.updateSlot(slot, {
          modelStatus: msg.status,
          backend: msg.status === "ready" ? msg.backend : this.snapshot.slots[slot].backend,
          modelProgress: msg.progress,
          modelFile: msg.file,
          modelError: msg.error,
        });
      } else if (msg.type === "rtc_signal_in") {
        let peer = this.rtcPeers[slot].get(msg.leaseId);
        if (!peer) {
          peer = new WorkerRtcPeer({
            sendSignal: (signal) => {
              const curW = this.workers[slot];
              if (curW) {
                curW.postMessage({ type: "rtc_signal_out", leaseId: msg.leaseId, signal } satisfies HostToWorkerCmd);
              }
            },
            onRequest: (req) => {
              const curW = this.workers[slot];
              if (curW) {
                curW.postMessage({
                  type: "rtc_req",
                  leaseId: msg.leaseId,
                  req,
                  iceMs: peer!.getIceMs(),
                } satisfies HostToWorkerCmd);
              }
            },
            onAck: () => {
              const curW = this.workers[slot];
              if (curW) {
                curW.postMessage({ type: "rtc_ack", leaseId: msg.leaseId } satisfies HostToWorkerCmd);
              }
            },
            onDisconnected: () => {
              const curW = this.workers[slot];
              if (curW) {
                curW.postMessage({ type: "rtc_disconnected", leaseId: msg.leaseId } satisfies HostToWorkerCmd);
              }
            },
          });
          this.rtcPeers[slot].set(msg.leaseId, peer);
        }
        void peer.handleSignal(msg.signal);
        return;
      } else if (msg.type === "rtc_send") {
        const peer = this.rtcPeers[slot].get(msg.leaseId);
        if (peer) {
          void peer.sendData(
            msg.payload instanceof Uint8Array ? msg.payload : JSON.stringify(msg.payload),
          );
        }
        return;
      } else if (msg.type === "rtc_close") {
        const peer = this.rtcPeers[slot].get(msg.leaseId);
        if (peer) {
          peer.close();
          this.rtcPeers[slot].delete(msg.leaseId);
        }
        return;
      } else if (msg.type === "log") {
        this.pushLog(msg.message);
      }
      this.emitChange();
    };
    w.onerror = (ev: ErrorEvent) => {
      const message = ev.message || "Dedicated Web Worker crashed";
      this.updateSlot(slot, { status: "error", error: message });
      this.pushLog(`[${slot}] ${message}`);
      this.settleOnlineWaiters(slot, new Error(message));
      this.emitChange();
    };
    this.workers[slot] = w;
    return w;
  }

  startWorker(slot: WorkerSlotRole, opts: { wsBaseUrl: string; token: string | null }): Promise<void> {
    const cur = this.snapshot.slots[slot];
    const w = this.ensureWorker(slot);
    this.updateSlot(slot, { status: "connecting", error: null });
    this.emitChange();
    const cmd: HostToWorkerCmd = {
      type: "start",
      wsBaseUrl: opts.wsBaseUrl,
      workerId: cur.workerId,
      role: slot,
      token: opts.token,
    };
    w.postMessage(cmd);

    return new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.settleOnlineWaiters(slot, new Error(`Timed out waiting for ${cur.workerId} to connect`));
      }, 10_000);
      const list = this.onlineWaiters[slot] ?? [];
      list.push({
        resolve: () => {
          clearTimeout(timer);
          resolve();
        },
        reject: (err) => {
          clearTimeout(timer);
          reject(err);
        },
      });
      this.onlineWaiters[slot] = list;
    });
  }

  loadModel(slot: WorkerSlotRole): void {
    const w = this.ensureWorker(slot);
    const cmd: HostToWorkerCmd = { type: "load_model" };
    w.postMessage(cmd);
  }

  private closeSlotRtcPeers(slot: WorkerSlotRole): void {
    for (const peer of this.rtcPeers[slot].values()) {
      peer.close();
    }
    this.rtcPeers[slot].clear();
  }

  /**
   * Abruptly terminates the Dedicated Web Worker via `worker.terminate()` without
   * sending any stop/Finish frame so the control plane detects the real dropped
   * WebSocket and executes its genuine reconnect_grace + retry failover path.
   */
  killWorker(slot: WorkerSlotRole): void {
    this.closeSlotRtcPeers(slot);
    const cur = this.snapshot.slots[slot];
    const w = this.workers[slot];
    if (w) {
      w.terminate();
      delete this.workers[slot];
    }
    this.updateSlot(slot, {
      status: "killed",
      sessionId: null,
      interfaces: [],
      activeJobId: null,
      error: null,
      modelStatus: "idle",
      modelProgress: 0,
      modelFile: null,
      modelError: null,
      backend: "echo-only",
    });
    this.pushLog(`${cur.workerId} killed via worker.terminate() — WebSockets dropped abruptly`);
    this.settleOnlineWaiters(slot, new Error("worker killed"));
    this.emitChange();
  }

  stopWorker(slot: WorkerSlotRole): void {
    this.closeSlotRtcPeers(slot);
    const w = this.workers[slot];
    if (w) {
      try {
        const cmd: HostToWorkerCmd = { type: "stop" };
        w.postMessage(cmd);
      } catch {
        // ignore
      }
      w.terminate();
      delete this.workers[slot];
    }
    this.updateSlot(slot, {
      status: "stopped",
      sessionId: null,
      interfaces: [],
      activeJobId: null,
      error: null,
      modelStatus: "idle",
      modelProgress: 0,
      modelFile: null,
      modelError: null,
      backend: "echo-only",
    });
    this.settleOnlineWaiters(slot, new Error("worker stopped"));
    this.emitChange();
  }

  stopAll(): void {
    this.stopWorker("primary");
    this.stopWorker("standby");
  }
}

export const browserWorkerManager = new BrowserWorkerManager();
