// @vitest-environment jsdom
import { describe, expect, it } from "vitest";

import { encodeUtf8, pb } from "./protocol";
import {
  RTC_SIGNAL_META_KEY,
  RTC_SIGNAL_META_VALUE,
  WorkerRtcPeer,
  invokeWebRtcOrFallback,
  type ClientRtcSignal,
  type DataChannelClientMsg,
  type DataChannelWorkerMsg,
  type WorkerRtcSignal,
} from "./rtc";

class FakeDataChannel {
  binaryType = "arraybuffer";
  readyState: RTCDataChannelState = "connecting";
  bufferedAmount = 0;
  bufferedAmountLowThreshold = 0;
  onopen: (() => void) | null = null;
  onmessage: ((ev: MessageEvent) => void) | null = null;
  onclose: (() => void) | null = null;
  onbufferedamountlow: (() => void) | null = null;
  peer: FakeDataChannel | null = null;
  sent: Array<string | Uint8Array> = [];
  private listeners = new Map<string, Set<() => void>>();

  addEventListener(type: string, cb: () => void): void {
    const set = this.listeners.get(type) ?? new Set();
    set.add(cb);
    this.listeners.set(type, set);
  }

  removeEventListener(type: string, cb: () => void): void {
    this.listeners.get(type)?.delete(cb);
  }

  emitBufferedAmountLow(): void {
    this.bufferedAmount = 0;
    this.onbufferedamountlow?.();
    for (const cb of [...(this.listeners.get("bufferedamountlow") ?? [])]) {
      this.listeners.get("bufferedamountlow")?.delete(cb);
      cb();
    }
  }

  send(data: string | Uint8Array): void {
    this.sent.push(data);
    const peer = this.peer;
    if (!peer || peer.readyState !== "open") return;
    const delivered =
      data instanceof Uint8Array
        ? data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength)
        : data;
    queueMicrotask(() => {
      if (peer.readyState === "open") {
        peer.onmessage?.({ data: delivered } as MessageEvent);
      }
    });
  }

  close(): void {
    if (this.readyState === "closed") return;
    this.readyState = "closed";
    this.onclose?.();
    if (this.peer && this.peer.readyState !== "closed") {
      this.peer.readyState = "closed";
      this.peer.onclose?.();
    }
  }
}

class FakeRTCPeerConnection {
  static pairs: FakeRTCPeerConnection[] = [];
  static stallIce = false;

  iceConnectionState: RTCIceConnectionState = "new";
  onicecandidate: ((ev: { candidate: RTCIceCandidate | null }) => void) | null = null;
  oniceconnectionstatechange: (() => void) | null = null;
  ondatachannel: ((ev: { channel: FakeDataChannel }) => void) | null = null;
  localDc: FakeDataChannel | null = null;
  remoteCandidates: RTCIceCandidateInit[] = [];
  closed = false;

  constructor(_config?: RTCConfiguration) {
    FakeRTCPeerConnection.pairs.push(this);
  }

  createDataChannel(_label: string, _opts?: RTCDataChannelInit): FakeDataChannel {
    this.localDc = new FakeDataChannel();
    return this.localDc;
  }

  async createOffer(): Promise<RTCSessionDescriptionInit> {
    return { type: "offer", sdp: "v=0\r\no=- 1 1 IN IP4 127.0.0.1\r\n" };
  }

  async createAnswer(): Promise<RTCSessionDescriptionInit> {
    return { type: "answer", sdp: "v=0\r\no=- 2 2 IN IP4 127.0.0.1\r\n" };
  }

  async setLocalDescription(_desc: RTCSessionDescriptionInit): Promise<void> {
    queueMicrotask(() => {
      if (this.closed) return;
      const candStr = "candidate:1 1 udp 2122260223 127.0.0.1 54321 typ host";
      this.onicecandidate?.({
        candidate: {
          candidate: candStr,
          sdpMid: "0",
          sdpMLineIndex: 0,
          toJSON() {
            return { candidate: candStr, sdpMid: "0", sdpMLineIndex: 0 };
          },
        } as unknown as RTCIceCandidate,
      });
      this.onicecandidate?.({ candidate: null });
    });
  }

  async setRemoteDescription(desc: RTCSessionDescriptionInit): Promise<void> {
    if (desc.type === "answer" && !FakeRTCPeerConnection.stallIce) {
      // Simulate ICE completion connecting caller PC (the one with localDc) and responder PC.
      const caller = FakeRTCPeerConnection.pairs.find((p) => p.localDc !== null);
      const responder = FakeRTCPeerConnection.pairs.find((p) => p !== caller);
      if (caller?.localDc && responder) {
        const responderDc = new FakeDataChannel();
        caller.localDc.peer = responderDc;
        responderDc.peer = caller.localDc;
        queueMicrotask(() => {
          if (caller.closed || responder.closed) return;
          responder.ondatachannel?.({ channel: responderDc });
          caller.localDc!.readyState = "open";
          responderDc.readyState = "open";
          caller.iceConnectionState = "connected";
          responder.iceConnectionState = "connected";
          responderDc.onopen?.();
          caller.localDc!.onopen?.();
        });
      }
    }
  }

  async addIceCandidate(cand: RTCIceCandidateInit): Promise<void> {
    this.remoteCandidates.push(cand);
  }

  close(): void {
    this.closed = true;
    this.iceConnectionState = "closed";
    this.localDc?.close();
  }
}

class FakeSignalingWebSocket {
  static onClientFrame: ((ws: FakeSignalingWebSocket, req: pb.InvokeRequest) => void) | null = null;
  binaryType = "arraybuffer";
  readyState = 0;
  onopen: (() => void) | null = null;
  onmessage: ((ev: MessageEvent) => void) | null = null;
  onerror: (() => void) | null = null;
  onclose: ((ev: CloseEvent) => void) | null = null;
  receivedFrames: pb.InvokeRequest[] = [];

  constructor(_url: string, _protocols?: string | string[]) {
    queueMicrotask(() => {
      this.readyState = 1;
      this.onopen?.();
    });
  }

  send(data: Uint8Array): void {
    const req = pb.decodeInvokeRequest(new Uint8Array(data));
    this.receivedFrames.push(req);
    FakeSignalingWebSocket.onClientFrame?.(this, req);
  }

  emitEvent(event: pb.ExecutionEventInput): void {
    const frame = pb.encodeWsDataFrame(pb.encodeExecutionEvent(event));
    this.onmessage?.({ data: frame.buffer.slice(frame.byteOffset, frame.byteOffset + frame.byteLength) } as MessageEvent);
  }

  close(): void {
    this.readyState = 3;
  }
}

describe("WebRTC P2P data plane (rtc.ts)", () => {
  it("completes full P2P handshake & 512 KB binary stream with 0 B relayed via control plane", async () => {
    FakeRTCPeerConnection.pairs = [];
    FakeRTCPeerConnection.stallIce = false;

    let workerPeer: WorkerRtcPeer | null = null;
    let clientSignals: ClientRtcSignal[] = [];
    let inputEnded = false;
    let ackReceived = false;

    FakeSignalingWebSocket.onClientFrame = (ws, req) => {
      if (req.msg?.case === "start") {
        expect(req.msg.value.streamingInput).toBe(true);
        expect(req.msg.value.spec?.metadata["anyserve.transport"]).toBe("webrtc");
        ws.emitEvent({ jobId: "job-p2p-1", sequence: 1n, event: { case: "accepted", value: {} } });
        ws.emitEvent({
          jobId: "job-p2p-1",
          sequence: 2n,
          attemptId: "att-1",
          event: { case: "assigned", value: { workerId: "browser-worker-1", attemptNumber: 1 } },
        });
        workerPeer = new WorkerRtcPeer(
          {
            sendSignal: (sig: WorkerRtcSignal) => {
              ws.emitEvent({
                jobId: "job-p2p-1",
                sequence: 3n,
                attemptId: "att-1",
                event: {
                  case: "output",
                  value: {
                    data: encodeUtf8(JSON.stringify(sig)),
                    progress: true,
                    metadata: { [RTC_SIGNAL_META_KEY]: RTC_SIGNAL_META_VALUE },
                  },
                },
              });
            },
            onRequest: async (clientReq) => {
              const chunks = clientReq.benchChunks ?? 0;
              const chunkSize = clientReq.benchChunkBytes ?? 16_384;
              const buf = new Uint8Array(chunkSize);
              for (let i = 0; i < chunks; i++) {
                await workerPeer!.sendData(buf);
              }
              await workerPeer!.sendData(
                JSON.stringify({
                  t: "done",
                  handlerMs: 5,
                  p2pBytes: chunks * chunkSize,
                  frames: chunks,
                } satisfies DataChannelWorkerMsg),
              );
            },
            onAck: () => {
              ackReceived = true;
              ws.emitEvent({
                jobId: "job-p2p-1",
                sequence: 4n,
                attemptId: "att-1",
                event: { case: "finished", value: { state: pb.JobState.SUCCEEDED } },
              });
            },
          },
          [],
          FakeRTCPeerConnection as unknown as typeof RTCPeerConnection,
        );
      } else if (req.msg?.case === "input") {
        expect(req.msg.value.metadata[RTC_SIGNAL_META_KEY]).toBe(RTC_SIGNAL_META_VALUE);
        const sig = JSON.parse(new TextDecoder().decode(req.msg.value.data)) as ClientRtcSignal;
        clientSignals.push(sig);
        if (sig.t === "offer" || sig.t === "ice" || sig.t === "ice_end") {
          void workerPeer?.handleSignal(sig);
        }
      } else if (req.msg?.case === "inputEnd") {
        inputEnded = true;
      }
    };

    let binaryFrames = 0;
    const res = await invokeWebRtcOrFallback({
      wsBaseUrl: "wss://relay.example.com",
      token: "cli-tok",
      interfaceName: "browser.echo.v1",
      prompt: "bench",
      benchChunks: 32,
      benchChunkBytes: 16_384,
      WebSocketCtor: FakeSignalingWebSocket as unknown as typeof WebSocket,
      PeerConnectionCtor: FakeRTCPeerConnection as unknown as typeof RTCPeerConnection,
      onBinaryChunk: () => {
        binaryFrames += 1;
      },
    });

    expect(res.state).toBe("succeeded");
    expect(res.transportActual).toBe("p2p");
    expect(res.fallbackReason).toBeNull();
    expect(res.p2pBytes).toBe(32 * 16_384);
    expect(res.relayedPayloadBytes).toBe(0);
    expect(res.framesReceived).toBe(32);
    expect(binaryFrames).toBe(32);
    expect(res.iceMs).toBeGreaterThanOrEqual(1);
    expect(inputEnded).toBe(true);
    expect(ackReceived).toBe(true);
    expect(clientSignals.some((s) => s.t === "offer")).toBe(true);
    expect(clientSignals.some((s) => s.t === "p2p_ready")).toBe(true);
    (workerPeer as WorkerRtcPeer | null)?.close();
  });

  it("automatically falls back to WS relay on ICE timeout", async () => {
    FakeRTCPeerConnection.pairs = [];
    FakeRTCPeerConnection.stallIce = true;

    let fallbackPrompt = "";
    let inputEnded = false;

    FakeSignalingWebSocket.onClientFrame = (ws, req) => {
      if (req.msg?.case === "start") {
        ws.emitEvent({ jobId: "job-fb-1", sequence: 1n, event: { case: "accepted", value: {} } });
        ws.emitEvent({
          jobId: "job-fb-1",
          sequence: 2n,
          attemptId: "att-1",
          event: { case: "assigned", value: { workerId: "browser-worker-1", attemptNumber: 1 } },
        });
      } else if (req.msg?.case === "input") {
        const sig = JSON.parse(new TextDecoder().decode(req.msg.value.data)) as ClientRtcSignal;
        if (sig.t === "fallback") {
          fallbackPrompt = sig.prompt;
          ws.emitEvent({
            jobId: "job-fb-1",
            sequence: 3n,
            attemptId: "att-1",
            event: { case: "output", value: { data: encodeUtf8("relayed reply"), progress: false } },
          });
          ws.emitEvent({
            jobId: "job-fb-1",
            sequence: 4n,
            attemptId: "att-1",
            event: { case: "finished", value: { state: pb.JobState.SUCCEEDED } },
          });
        }
      } else if (req.msg?.case === "inputEnd") {
        inputEnded = true;
      }
    };

    const res = await invokeWebRtcOrFallback({
      wsBaseUrl: "wss://relay.example.com",
      interfaceName: "browser.echo.v1",
      prompt: "strict nat prompt",
      iceTimeoutMs: 30,
      WebSocketCtor: FakeSignalingWebSocket as unknown as typeof WebSocket,
      PeerConnectionCtor: FakeRTCPeerConnection as unknown as typeof RTCPeerConnection,
    });

    expect(res.state).toBe("succeeded");
    expect(res.transportActual).toBe("relay");
    expect(res.fallbackReason).toBe("ice_timeout");
    expect(res.text).toBe("relayed reply");
    expect(res.relayedPayloadBytes).toBeGreaterThan(0);
    expect(res.p2pBytes).toBe(0);
    expect(fallbackPrompt).toBe("strict nat prompt");
    expect(inputEnded).toBe(true);
  });

  it("falls back immediately when RTCPeerConnection is unavailable", async () => {
    FakeSignalingWebSocket.onClientFrame = (ws, req) => {
      if (req.msg?.case === "input") {
        const sig = JSON.parse(new TextDecoder().decode(req.msg.value.data)) as ClientRtcSignal;
        if (sig.t === "fallback") {
          expect(sig.reason).toBe("webrtc_unsupported");
          ws.emitEvent({
            jobId: "job-nowebrtc-1",
            sequence: 1n,
            attemptId: "att-1",
            event: { case: "output", value: { data: encodeUtf8("ok"), progress: false } },
          });
          ws.emitEvent({
            jobId: "job-nowebrtc-1",
            sequence: 2n,
            attemptId: "att-1",
            event: { case: "finished", value: { state: pb.JobState.SUCCEEDED } },
          });
        }
      }
    };

    const res = await invokeWebRtcOrFallback({
      wsBaseUrl: "wss://relay.example.com",
      interfaceName: "browser.echo.v1",
      prompt: "no rtc",
      WebSocketCtor: FakeSignalingWebSocket as unknown as typeof WebSocket,
      PeerConnectionCtor: null,
    });

    expect(res.transportActual).toBe("relay");
    expect(res.fallbackReason).toBe("webrtc_unsupported");
    expect(res.text).toBe("ok");
  });

  it("preserves FIFO send order under bufferedAmount backpressure and handles DataChannel ack", async () => {
    FakeRTCPeerConnection.pairs = [];
    const signals: WorkerRtcSignal[] = [];
    let acks = 0;

    const peer = new WorkerRtcPeer(
      {
        sendSignal: (s) => signals.push(s),
        onRequest: () => {},
        onAck: () => {
          acks += 1;
        },
      },
      [],
      FakeRTCPeerConnection as unknown as typeof RTCPeerConnection,
    );

    const responderPc = FakeRTCPeerConnection.pairs[0]!;
    const dc = new FakeDataChannel();
    dc.readyState = "open";
    responderPc.ondatachannel?.({ channel: dc });

    // Trigger high-water mark before first send; second send must queue strictly behind first.
    dc.bufferedAmount = 300 * 1024;
    const p1 = peer.sendData("first");
    const p2 = peer.sendData("second");
    await Promise.resolve();
    expect(dc.sent).toEqual([]);

    dc.emitBufferedAmountLow();
    await Promise.all([p1, p2]);
    expect(dc.sent).toEqual(["first", "second"]);

    dc.onmessage?.({ data: JSON.stringify({ t: "ack" } satisfies DataChannelClientMsg) } as MessageEvent);
    expect(acks).toBe(1);

    peer.close();
    expect(responderPc.closed).toBe(true);
    expect(signals.length).toBe(0);
  });

  it("rejects durable requests explicitly because WebRTC bypass is EPHEMERAL Invoke only", async () => {
    await expect(
      invokeWebRtcOrFallback({
        wsBaseUrl: "wss://relay.example.com",
        interfaceName: "browser.echo.v1",
        prompt: "should fail",
        durability: "durable",
        WebSocketCtor: FakeSignalingWebSocket as unknown as typeof WebSocket,
        PeerConnectionCtor: FakeRTCPeerConnection as unknown as typeof RTCPeerConnection,
      }),
    ).rejects.toThrow(/EPHEMERAL Invoke/i);
  });

  it("retries automatically when a P2P peer drops BEFORE any output is exposed", async () => {
    FakeRTCPeerConnection.pairs = [];
    FakeRTCPeerConnection.stallIce = false;

    let round = 0;
    const peers: WorkerRtcPeer[] = [];

    FakeSignalingWebSocket.onClientFrame = (ws, req) => {
      if (req.msg?.case === "start") {
        round += 1;
        const curRound = round;
        // Isolate pairs per round so FakeRTCPeerConnection connects the right caller+responder.
        FakeRTCPeerConnection.pairs = [];
        const assignedWorker = curRound === 1 ? "browser-worker-1" : "browser-worker-2";
        const jid = `job-p2p-fo-${curRound}`;
        ws.emitEvent({ jobId: jid, sequence: 1n, event: { case: "accepted", value: {} } });
        ws.emitEvent({
          jobId: jid,
          sequence: 2n,
          attemptId: `att-${curRound}`,
          event: { case: "assigned", value: { workerId: assignedWorker, attemptNumber: 1 } },
        });
        const peer = new WorkerRtcPeer(
          {
            sendSignal: (sig: WorkerRtcSignal) => {
              ws.emitEvent({
                jobId: jid,
                sequence: 3n,
                attemptId: `att-${curRound}`,
                event: {
                  case: "output",
                  value: {
                    data: encodeUtf8(JSON.stringify(sig)),
                    progress: true,
                    metadata: { [RTC_SIGNAL_META_KEY]: RTC_SIGNAL_META_VALUE },
                  },
                },
              });
            },
            onRequest: async () => {
              if (curRound === 1) {
                // Progress frames do NOT count as exposed output; worker then crashes before token #1.
                await peer.sendData(JSON.stringify({ t: "progress", text: "prefilling..." } satisfies DataChannelWorkerMsg));
                peer.close();
                return;
              }
              await peer.sendData(JSON.stringify({ t: "tok", text: "recovered p2p" } satisfies DataChannelWorkerMsg));
              await peer.sendData(
                JSON.stringify({ t: "done", handlerMs: 3, p2pBytes: 13, frames: 1 } satisfies DataChannelWorkerMsg),
              );
            },
            onAck: () => {
              ws.emitEvent({
                jobId: jid,
                sequence: 4n,
                attemptId: `att-${curRound}`,
                event: { case: "finished", value: { state: pb.JobState.SUCCEEDED } },
              });
            },
          },
          [],
          FakeRTCPeerConnection as unknown as typeof RTCPeerConnection,
        );
        peers.push(peer);
      } else if (req.msg?.case === "input") {
        const sig = JSON.parse(new TextDecoder().decode(req.msg.value.data)) as ClientRtcSignal;
        const activePeer = peers[peers.length - 1];
        if (sig.t === "offer" || sig.t === "ice" || sig.t === "ice_end") {
          void activePeer?.handleSignal(sig);
        }
      }
    };

    const res = await invokeWebRtcOrFallback({
      wsBaseUrl: "wss://relay.example.com",
      interfaceName: "browser.echo.v1",
      prompt: "failover over p2p",
      maxAttempts: 2,
      WebSocketCtor: FakeSignalingWebSocket as unknown as typeof WebSocket,
      PeerConnectionCtor: FakeRTCPeerConnection as unknown as typeof RTCPeerConnection,
    });

    expect(round).toBe(2);
    expect(res.state).toBe("succeeded");
    expect(res.restarted).toBe(true);
    expect(res.attempts).toBe(2);
    expect(res.workerId).toBe("browser-worker-2");
    expect(res.transportActual).toBe("p2p");
    expect(res.text).toBe("recovered p2p");
    expect(res.relayedPayloadBytes).toBe(0);
    expect(res.timeline.some((e) => e.kind === "attempt_restarted")).toBe(true);
    for (const p of peers) p.close();
  });

  it("fails fast with output_interrupted (no retry) when a P2P peer drops AFTER exposing output", async () => {
    FakeRTCPeerConnection.pairs = [];
    FakeRTCPeerConnection.stallIce = false;

    let round = 0;
    let activePeer: WorkerRtcPeer | null = null;

    FakeSignalingWebSocket.onClientFrame = (ws, req) => {
      if (req.msg?.case === "start") {
        round += 1;
        FakeRTCPeerConnection.pairs = [];
        ws.emitEvent({ jobId: "job-p2p-int-1", sequence: 1n, event: { case: "accepted", value: {} } });
        ws.emitEvent({
          jobId: "job-p2p-int-1",
          sequence: 2n,
          attemptId: "att-1",
          event: { case: "assigned", value: { workerId: "browser-worker-1", attemptNumber: 1 } },
        });
        activePeer = new WorkerRtcPeer(
          {
            sendSignal: (sig: WorkerRtcSignal) => {
              ws.emitEvent({
                jobId: "job-p2p-int-1",
                sequence: 3n,
                attemptId: "att-1",
                event: {
                  case: "output",
                  value: {
                    data: encodeUtf8(JSON.stringify(sig)),
                    progress: true,
                    metadata: { [RTC_SIGNAL_META_KEY]: RTC_SIGNAL_META_VALUE },
                  },
                },
              });
            },
            onRequest: async () => {
              await activePeer!.sendData(JSON.stringify({ t: "tok", text: "partial " } satisfies DataChannelWorkerMsg));
              // Allow caller to receive the exposed token before abrupt disconnect.
              await new Promise((r) => setTimeout(r, 10));
              activePeer!.close();
            },
          },
          [],
          FakeRTCPeerConnection as unknown as typeof RTCPeerConnection,
        );
      } else if (req.msg?.case === "input") {
        const sig = JSON.parse(new TextDecoder().decode(req.msg.value.data)) as ClientRtcSignal;
        if (sig.t === "offer" || sig.t === "ice" || sig.t === "ice_end") {
          void activePeer?.handleSignal(sig);
        }
      }
    };

    const chunks: string[] = [];
    const res = await invokeWebRtcOrFallback({
      wsBaseUrl: "wss://relay.example.com",
      interfaceName: "browser.echo.v1",
      prompt: "mid-stream crash",
      maxAttempts: 3,
      WebSocketCtor: FakeSignalingWebSocket as unknown as typeof WebSocket,
      PeerConnectionCtor: FakeRTCPeerConnection as unknown as typeof RTCPeerConnection,
      onChunk: (c) => chunks.push(c),
    });

    expect(round).toBe(1);
    expect(res.state).toBe("failed");
    expect(res.errorCode).toBe("output_interrupted");
    expect(res.restarted).toBe(false);
    expect(res.attempts).toBe(1);
    expect(res.text).toBe("partial ");
    expect(chunks).toEqual(["partial "]);
    (activePeer as WorkerRtcPeer | null)?.close();
  });
});
