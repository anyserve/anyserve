// @vitest-environment jsdom
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { BrowserWorkerManager } from "./manager";
import {
  ECHO_INTERFACE,
  buildWsSubprotocols,
  deriveWsBaseUrl,
  encodeBase64UrlNoPad,
  encodeUtf8,
  invokeStreamingJob,
  pb,
} from "./protocol";
import type { HostToWorkerCmd, WorkerToHostEvent } from "./worker";
import { PlaygroundPane, buildPeerTabUrl } from "../PlaygroundPane";

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

class FakeInvokeWebSocket {
  static lastUrl = "";
  static lastProtocols: string[] = [];
  static sentFrames: Uint8Array[] = [];
  static instance: FakeInvokeWebSocket | null = null;

  binaryType = "blob";
  readyState = 0;
  onopen: (() => void) | null = null;
  onmessage: ((ev: MessageEvent) => void) | null = null;
  onerror: (() => void) | null = null;
  onclose: ((ev: CloseEvent) => void) | null = null;

  constructor(url: string, protocols?: string | string[]) {
    FakeInvokeWebSocket.lastUrl = url;
    FakeInvokeWebSocket.lastProtocols = Array.isArray(protocols)
      ? protocols
      : protocols
        ? [protocols]
        : [];
    FakeInvokeWebSocket.sentFrames = [];
    FakeInvokeWebSocket.instance = this;
    queueMicrotask(() => {
      this.readyState = 1;
      this.onopen?.();
    });
  }

  send(data: Uint8Array): void {
    FakeInvokeWebSocket.sentFrames.push(new Uint8Array(data));
  }

  close(_code?: number, _reason?: string): void {
    this.readyState = 3;
  }

  emitEvent(event: pb.ExecutionEventInput): void {
    const frame = pb.encodeWsDataFrame(pb.encodeExecutionEvent(event));
    this.onmessage?.({ data: frame.buffer.slice(frame.byteOffset, frame.byteOffset + frame.byteLength) } as MessageEvent);
  }
}

function makeFakeWorker() {
  const sent: HostToWorkerCmd[] = [];
  const worker = {
    sent,
    terminated: false,
    onmessage: null as ((ev: MessageEvent<WorkerToHostEvent>) => void) | null,
    onerror: null as ((ev: ErrorEvent) => void) | null,
    postMessage(cmd: HostToWorkerCmd) {
      sent.push(cmd);
    },
    terminate() {
      this.terminated = true;
    },
    emit(ev: WorkerToHostEvent) {
      this.onmessage?.({ data: ev } as MessageEvent<WorkerToHostEvent>);
    },
  };
  return worker;
}

describe("browser-worker protocol", () => {
  it("builds Sec-WebSocket-Protocol lists without leaking raw tokens in URLs", () => {
    expect(buildWsSubprotocols(null)).toEqual(["anyserve.v2"]);
    expect(buildWsSubprotocols("   ")).toEqual(["anyserve.v2"]);
    expect(buildWsSubprotocols("as_w_a+b/c=d")).toEqual([
      "anyserve.v2",
      `anyserve.bearer.${encodeBase64UrlNoPad("as_w_a+b/c=d")}`,
    ]);
    expect(encodeBase64UrlNoPad("as_w_a+b/c=d")).not.toMatch(/[+/=]/);
    expect(deriveWsBaseUrl("https://anyserve.arcosx.org/")).toBe("wss://anyserve.arcosx.org");
    expect(deriveWsBaseUrl("http://127.0.0.1:3001")).toBe("ws://127.0.0.1:3001");
  });

  it("streams failover attempt_restarted + output tokens + finished over /ws/v2/client/invoke", async () => {
    const chunks: string[] = [];
    const promise = invokeStreamingJob({
      wsBaseUrl: "wss://relay.example.com",
      token: "cli-secret",
      interfaceName: ECHO_INTERFACE,
      prompt: "hello failover",
      preferredAttributes: { role: "primary" },
      metadata: { "demo.fail_before_first_token": "1" },
      WebSocketCtor: FakeInvokeWebSocket as unknown as typeof WebSocket,
      onChunk: (c) => chunks.push(c),
    });

    await waitFor(() => expect(FakeInvokeWebSocket.sentFrames.length).toBe(1));
    expect(FakeInvokeWebSocket.lastUrl).toBe("wss://relay.example.com/ws/v2/client/invoke");
    expect(FakeInvokeWebSocket.lastProtocols).toEqual([
      "anyserve.v2",
      `anyserve.bearer.${encodeBase64UrlNoPad("cli-secret")}`,
    ]);

    const decodedStart = pb.decodeInvokeRequest(FakeInvokeWebSocket.sentFrames[0]!);
    expect(decodedStart.msg?.case).toBe("start");
    if (decodedStart.msg?.case === "start") {
      expect(decodedStart.msg.value.spec?.demand?.interface).toBe(ECHO_INTERFACE);
      expect(decodedStart.msg.value.spec?.demand?.preferredAttributes).toEqual({ role: "primary" });
    }

    const ws = FakeInvokeWebSocket.instance!;
    ws.emitEvent({ jobId: "job-fo-1", sequence: 1n, event: { case: "accepted", value: {} } });
    ws.emitEvent({
      jobId: "job-fo-1",
      sequence: 2n,
      attemptId: "att-1",
      event: { case: "assigned", value: { workerId: "browser-worker-1", attemptNumber: 1 } },
    });
    ws.emitEvent({
      jobId: "job-fo-1",
      sequence: 3n,
      attemptId: "att-1",
      event: { case: "output", value: { data: encodeUtf8("prefilling..."), progress: true } },
    });
    ws.emitEvent({
      jobId: "job-fo-1",
      sequence: 4n,
      event: {
        case: "attemptRestarted",
        value: {
          previousAttemptId: "att-1",
          reason: { code: "worker_lost", message: "simulated crash", retryable: true },
        },
      },
    });
    ws.emitEvent({
      jobId: "job-fo-1",
      sequence: 5n,
      attemptId: "att-2",
      event: { case: "assigned", value: { workerId: "browser-worker-2", attemptNumber: 2 } },
    });
    ws.emitEvent({
      jobId: "job-fo-1",
      sequence: 6n,
      attemptId: "att-2",
      event: { case: "output", value: { data: encodeUtf8("recovered!"), progress: false } },
    });
    ws.emitEvent({
      jobId: "job-fo-1",
      sequence: 7n,
      attemptId: "att-2",
      event: { case: "finished", value: { state: pb.JobState.SUCCEEDED } },
    });

    const result = await promise;
    expect(result.jobId).toBe("job-fo-1");
    expect(result.state).toBe("succeeded");
    expect(result.restarted).toBe(true);
    expect(result.attempts).toBe(2);
    expect(result.workerId).toBe("browser-worker-2");
    expect(result.text).toBe("recovered!");
    expect(chunks).toEqual(["recovered!"]);
  });
});

describe("BrowserWorkerManager & PlaygroundPane", () => {
  it("tracks worker lifecycle, model loading, and enforces worker token separation in auth mode", async () => {
    const workers: ReturnType<typeof makeFakeWorker>[] = [];
    const manager = new BrowserWorkerManager(() => {
      const w = makeFakeWorker();
      workers.push(w);
      return w;
    });

    const onOpenJobInJobs = vi.fn();
    const onOpenWorkers = vi.fn();
    render(
      <PlaygroundPane
        controlPlane={{
          grpc_enabled: false,
          grpc_listen: "127.0.0.1:50052",
          grpc_tls_enabled: false,
          console_listen: "127.0.0.1:3001",
          auth_enabled: true,
          generation: 1,
          retained_jobs: 0,
          sessions: 0,
          max_sessions: 100,
          priority_levels: 4,
          default_max_attempts: 3,
          heartbeat_interval_ms: 5000,
          heartbeat_timeout_ms: 15000,
          reconnect_grace_ms: 15000,
        }}
        authPortalUrl="https://anyserve.arcosx.org/tokens"
        manager={manager}
        onOpenJobInJobs={onOpenJobInJobs}
        onOpenWorkers={onOpenWorkers}
      />,
    );

    // Honest 135M disclaimer is visible.
    expect(screen.getByText(/135M small model — demos scheduling & streaming, not IQ/i)).toBeTruthy();

    // Clicking Start without worker token shows a clear error and does not spawn a worker.
    fireEvent.click(screen.getAllByRole("button", { name: /Start Worker/i })[0]!);
    expect(screen.getByRole("alert").textContent).toMatch(/worker token/i);
    expect(workers.length).toBe(0);

    // Supply worker token and start primary worker.
    fireEvent.change(screen.getByPlaceholderText(/Paste worker token/i), {
      target: { value: "as_w_test_token" },
    });
    fireEvent.click(screen.getAllByRole("button", { name: /Start Worker/i })[0]!);
    expect(workers.length).toBe(1);
    expect(workers[0]!.sent[0]).toMatchObject({
      type: "start",
      workerId: "browser-worker-1",
      role: "primary",
      token: "as_w_test_token",
    });

    // Worker reports Welcome -> online.
    workers[0]!.emit({
      type: "state",
      status: "online",
      sessionId: "sess-abc123",
      interfaces: [ECHO_INTERFACE],
      activeJobId: null,
      jobsCompleted: 0,
      backend: "echo-only",
      error: null,
    });
    await waitFor(() => expect(screen.getByText(/session sess-abc123/i)).toBeTruthy());
    expect(manager.onlineCount()).toBe(1);

    // Trigger model load command on primary slot.
    fireEvent.click(screen.getAllByRole("button", { name: /Load SmolLM2-135M/i })[0]!);
    expect(workers[0]!.sent[1]).toEqual({ type: "load_model" });
    workers[0]!.emit({
      type: "model",
      status: "ready",
      backend: "wasm",
      progress: 100,
      file: null,
      error: null,
    });
    await waitFor(() => expect(screen.getByText(/SmolLM2-135M Ready \(WASM\)/i)).toBeTruthy());

    // Clicking "Kill worker" calls worker.terminate() directly WITHOUT sending a graceful stop frame.
    const sentBeforeKill = workers[0]!.sent.length;
    fireEvent.click(screen.getByRole("button", { name: /^Kill worker$/i }));
    expect(workers[0]!.terminated).toBe(true);
    expect(workers[0]!.sent.length).toBe(sentBeforeKill);
    expect(manager.getSnapshot().slots.primary.status).toBe("killed");
    expect(manager.onlineCount()).toBe(0);
    expect(screen.getAllByText(/reconnect_grace_ms/i).length).toBeGreaterThanOrEqual(1);
  });

  it("never leaks worker tokens into Peer Tab links and renders Relay vs WebRTC P2P benchmark", async () => {
    expect(buildPeerTabUrl("https://anyserve.arcosx.org/", false, "as_c_client")).toBe(
      "https://anyserve.arcosx.org/playground",
    );
    expect(buildPeerTabUrl("https://anyserve.arcosx.org/", true, "as_c_client+1")).toBe(
      "https://anyserve.arcosx.org/playground#token=as_c_client%2B1",
    );

    const workers: ReturnType<typeof makeFakeWorker>[] = [];
    const manager = new BrowserWorkerManager(() => {
      const w = makeFakeWorker();
      workers.push(w);
      return w;
    });
    const startP = manager.startWorker("primary", { wsBaseUrl: "ws://127.0.0.1:3001", token: null });
    workers[0]!.emit({
      type: "state",
      status: "online",
      sessionId: "sess-bench",
      interfaces: [ECHO_INTERFACE],
      activeJobId: null,
      jobsCompleted: 0,
      backend: "echo-only",
      error: null,
    });
    await startP;

    const invokeRelay = vi.fn().mockResolvedValue({
      jobId: "job-bench-relay",
      state: "succeeded",
      text: "",
      error: null,
      workerId: "browser-worker-1",
      attempts: 1,
      restarted: false,
      failoverMs: null,
      ttftMs: 14,
      totalMs: 42,
      transportActual: "relay",
      fallbackReason: null,
      iceMs: null,
      p2pBytes: 0,
      relayedPayloadBytes: 524_288,
      streamDurationMs: 28,
      framesReceived: 32,
      timeline: [],
    });
    const invokeWebRtc = vi.fn().mockResolvedValue({
      jobId: "job-bench-p2p",
      state: "succeeded",
      text: "",
      error: null,
      workerId: "browser-worker-1",
      attempts: 1,
      restarted: false,
      failoverMs: null,
      ttftMs: 75,
      totalMs: 92,
      transportActual: "p2p",
      fallbackReason: null,
      iceMs: 68,
      p2pBytes: 524_288,
      relayedPayloadBytes: 0,
      streamDurationMs: 12,
      framesReceived: 32,
      timeline: [],
    });

    const onOpenJobInJobs = vi.fn();
    render(
      <PlaygroundPane
        manager={manager}
        invokeRelay={invokeRelay}
        invokeWebRtc={invokeWebRtc}
        onOpenJobInJobs={onOpenJobInJobs}
        onOpenWorkers={vi.fn()}
      />,
    );

    fireEvent.click(screen.getByRole("button", { name: /Benchmark: Relay vs P2P/i }));
    await waitFor(() => expect(screen.getByLabelText(/Relay vs WebRTC P2P benchmark/i)).toBeTruthy());

    expect(invokeRelay).toHaveBeenCalledWith(expect.objectContaining({ benchChunks: 32, benchChunkBytes: 16_384 }));
    expect(invokeWebRtc).toHaveBeenCalledWith(expect.objectContaining({ benchChunks: 32, benchChunkBytes: 16_384 }));
    expect(screen.getByText("512.0 KB")).toBeTruthy();
    expect(screen.getAllByText("0 B").length).toBeGreaterThanOrEqual(1);
    expect(screen.getByText("68 ms")).toBeTruthy();

    const waterfallBtns = screen.getAllByRole("button", { name: /^Waterfall$/i });
    fireEvent.click(waterfallBtns[1]!);
    expect(onOpenJobInJobs).toHaveBeenCalledWith("job-bench-p2p", undefined);
  });

  it("allows a peer tab without a worker token to invoke & benchmark remote online workers", async () => {
    const workers: ReturnType<typeof makeFakeWorker>[] = [];
    const manager = new BrowserWorkerManager(() => {
      const w = makeFakeWorker();
      workers.push(w);
      return w;
    });
    const invokeRelay = vi.fn().mockResolvedValue({
      jobId: "job-peer-relay",
      state: "succeeded",
      text: "",
      error: null,
      workerId: "browser-worker-1",
      attempts: 1,
      restarted: false,
      failoverMs: null,
      ttftMs: 10,
      totalMs: 30,
      transportActual: "relay",
      fallbackReason: null,
      iceMs: null,
      p2pBytes: 0,
      relayedPayloadBytes: 524_288,
      streamDurationMs: 20,
      framesReceived: 32,
      timeline: [],
    });
    const invokeWebRtc = vi.fn().mockResolvedValue({
      jobId: "job-peer-p2p",
      state: "succeeded",
      text: "",
      error: null,
      workerId: "browser-worker-1",
      attempts: 1,
      restarted: false,
      failoverMs: null,
      ttftMs: 45,
      totalMs: 60,
      transportActual: "p2p",
      fallbackReason: null,
      iceMs: 35,
      p2pBytes: 524_288,
      relayedPayloadBytes: 0,
      streamDurationMs: 10,
      framesReceived: 32,
      timeline: [],
    });

    render(
      <PlaygroundPane
        controlPlane={{
          grpc_enabled: false,
          grpc_listen: "",
          grpc_tls_enabled: false,
          console_listen: "127.0.0.1:3001",
          auth_enabled: true,
          generation: 1,
          retained_jobs: 0,
          sessions: 1,
          max_sessions: 100,
          priority_levels: 4,
          default_max_attempts: 3,
          heartbeat_interval_ms: 5000,
          heartbeat_timeout_ms: 15000,
          reconnect_grace_ms: 15000,
        }}
        remoteWorkersOnline={1}
        manager={manager}
        invokeRelay={invokeRelay}
        invokeWebRtc={invokeWebRtc}
        onOpenJobInJobs={vi.fn()}
        onOpenWorkers={vi.fn()}
      />,
    );

    expect(screen.getByText(/Peer mode · 1 remote worker online/i)).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: /Benchmark: Relay vs P2P/i }));
    await waitFor(() => expect(screen.getByLabelText(/Relay vs WebRTC P2P benchmark/i)).toBeTruthy());
    // Must NOT spawn any local worker on Tab 2.
    expect(workers.length).toBe(0);
    expect(invokeRelay).toHaveBeenCalledTimes(1);
    expect(invokeWebRtc).toHaveBeenCalledTimes(1);
  });
});
