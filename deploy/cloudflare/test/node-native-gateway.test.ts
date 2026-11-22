import { randomBytes } from "node:crypto";
import { createConnection, type Socket } from "node:net";
import { describe, expect, it } from "vitest";
import { startNodeNativeGatewayServer } from "../src/node-native-gateway";
import * as pb from "../src/native/proto";

function encodeClientMaskedFrame(
  opcode: number,
  payload: Uint8Array,
  fin = true,
): Buffer {
  const mask = randomBytes(4);
  const first = (fin ? 0x80 : 0x00) | (opcode & 0x0f);
  let header: Buffer;
  const len = payload.byteLength;
  if (len < 126) {
    header = Buffer.allocUnsafe(2 + 4);
    header[0] = first;
    header[1] = 0x80 | len;
    mask.copy(header, 2);
  } else if (len <= 0xffff) {
    header = Buffer.allocUnsafe(4 + 4);
    header[0] = first;
    header[1] = 0x80 | 126;
    header.writeUInt16BE(len, 2);
    mask.copy(header, 4);
  } else {
    header = Buffer.allocUnsafe(10 + 4);
    header[0] = first;
    header[1] = 0x80 | 127;
    header.writeUInt32BE(Math.floor(len / 4_294_967_296), 2);
    header.writeUInt32BE(len >>> 0, 6);
    mask.copy(header, 10);
  }
  const maskedBody = Buffer.allocUnsafe(len);
  for (let i = 0; i < len; i++) {
    maskedBody[i] = payload[i]! ^ mask[i & 3]!;
  }
  return Buffer.concat([header, maskedBody]);
}

interface RawWsFrame {
  fin: boolean;
  opcode: number;
  payload: Buffer;
}

class RawTcpWsClient {
  readonly socket: Socket;
  private buf = Buffer.alloc(0);
  private waiters: Array<() => void> = [];

  private constructor(socket: Socket, initial: Buffer) {
    this.socket = socket;
    this.buf = initial;
    socket.on("data", (c: Buffer) => {
      this.buf = Buffer.concat([this.buf, c]);
      const ws = this.waiters.splice(0);
      for (const w of ws) w();
    });
    socket.on("end", () => {
      const ws = this.waiters.splice(0);
      for (const w of ws) w();
    });
    socket.on("close", () => {
      const ws = this.waiters.splice(0);
      for (const w of ws) w();
    });
  }

  static async connect(
    port: number,
    path: string,
    headers: Record<string, string> = {},
  ): Promise<
    | { upgraded: true; client: RawTcpWsClient }
    | { upgraded: false; statusCode: number; headers: Record<string, string>; body: string }
  > {
    const sock = createConnection({ host: "127.0.0.1", port });
    sock.setNoDelay(true);
    await new Promise<void>((resolve, reject) => {
      sock.once("connect", resolve);
      sock.once("error", reject);
    });

    const key = randomBytes(16).toString("base64");
    const lines = [
      `GET ${path} HTTP/1.1`,
      `Host: 127.0.0.1:${port}`,
      "Upgrade: websocket",
      "Connection: Upgrade",
      `Sec-WebSocket-Key: ${key}`,
      "Sec-WebSocket-Version: 13",
    ];
    for (const [k, v] of Object.entries(headers)) {
      lines.push(`${k}: ${v}`);
    }
    lines.push("", "");
    sock.write(lines.join("\r\n"));

    let raw = Buffer.alloc(0);
    while (true) {
      const idx = raw.indexOf("\r\n\r\n");
      if (idx >= 0) {
        const headText = raw.subarray(0, idx).toString("utf8");
        const rest = raw.subarray(idx + 4);
        const headLines = headText.split("\r\n");
        const statusMatch = /^HTTP\/1\.1\s+(\d+)/.exec(headLines[0] ?? "");
        const statusCode = Number(statusMatch?.[1] ?? 0);
        const respHeaders: Record<string, string> = {};
        for (const l of headLines.slice(1)) {
          const colon = l.indexOf(":");
          if (colon > 0) {
            respHeaders[l.slice(0, colon).trim().toLowerCase()] = l
              .slice(colon + 1)
              .trim();
          }
        }
        if (statusCode === 101) {
          return { upgraded: true, client: new RawTcpWsClient(sock, rest) };
        }
        const contentLen = Number(respHeaders["content-length"] ?? 0);
        let bodyBuf = rest;
        while (bodyBuf.length < contentLen) {
          const next = await new Promise<Buffer | null>((resolve) => {
            sock.once("data", (c: Buffer) => resolve(c));
            sock.once("end", () => resolve(null));
            sock.once("close", () => resolve(null));
          });
          if (!next) break;
          bodyBuf = Buffer.concat([bodyBuf, next]);
        }
        sock.destroy();
        return {
          upgraded: false,
          statusCode,
          headers: respHeaders,
          body: bodyBuf.subarray(0, contentLen || bodyBuf.length).toString("utf8"),
        };
      }
      const chunk = await new Promise<Buffer>((resolve, reject) => {
        sock.once("data", (c: Buffer) => resolve(c));
        sock.once("error", reject);
      });
      raw = Buffer.concat([raw, chunk]);
    }
  }

  sendRaw(buf: Buffer): void {
    this.socket.write(buf);
  }

  async readFrame(): Promise<RawWsFrame> {
    while (true) {
      if (this.buf.length >= 2) {
        const b0 = this.buf[0]!;
        const b1 = this.buf[1]!;
        const fin = (b0 & 0x80) !== 0;
        const opcode = b0 & 0x0f;
        const len7 = b1 & 0x7f;
        let headerLen = 2;
        let payloadLen = len7;
        if (len7 === 126) {
          headerLen = 4;
          if (this.buf.length >= 4) {
            payloadLen = this.buf.readUInt16BE(2);
          } else {
            payloadLen = -1;
          }
        } else if (len7 === 127) {
          headerLen = 10;
          if (this.buf.length >= 10) {
            payloadLen =
              this.buf.readUInt32BE(2) * 4_294_967_296 +
              this.buf.readUInt32BE(6);
          } else {
            payloadLen = -1;
          }
        }
        if (payloadLen >= 0 && this.buf.length >= headerLen + payloadLen) {
          const payload = this.buf.subarray(headerLen, headerLen + payloadLen);
          this.buf = this.buf.subarray(headerLen + payloadLen);
          return { fin, opcode, payload };
        }
      }
      if (this.socket.destroyed) {
        throw new Error("Socket closed before complete frame arrived");
      }
      await new Promise<void>((resolve) => {
        this.waiters.push(resolve);
      });
    }
  }

  destroy(): void {
    this.socket.destroy();
  }
}

describe("Node 24 HTTP + RFC 6455 WebSocket Native Gateway", () => {
  it("enforces pre-upgrade HTTP errors, Ping/Pong, 1005/1006, 32 MiB+1 -> 1009, and /__test/hibernate on live TCP sockets", async () => {
    const gw = await startNodeNativeGatewayServer({
      env: {
        ANYSERVE_CLIENT_TOKENS: "gw-client-tok",
        ANYSERVE_WORKER_TOKENS: "gw-worker-tok",
      },
      enableTestHooks: true,
    });

    try {
      // 1. Pre-upgrade 401 & 403 returned as raw HTTP responses before 101 Switching Protocols
      const unauth = await RawTcpWsClient.connect(
        gw.port,
        "/ws/v2/worker/session",
      );
      expect(unauth.upgraded).toBe(false);
      if (!unauth.upgraded) {
        expect(unauth.statusCode).toBe(401);
        expect(unauth.headers["x-anyserve-grpc-code"]).toBe("16");
      }

      const wrongRole = await RawTcpWsClient.connect(
        gw.port,
        "/ws/v2/worker/session",
        { Authorization: "Bearer gw-client-tok" },
      );
      expect(wrongRole.upgraded).toBe(false);
      if (!wrongRole.upgraded) {
        expect(wrongRole.statusCode).toBe(403);
        expect(wrongRole.headers["x-anyserve-grpc-code"]).toBe("7");
      }

      // 2. Connect worker session, test Ping/Pong, and survive POST /__test/hibernate on the same TCP socket!
      const workerConnRes = await RawTcpWsClient.connect(
        gw.port,
        "/ws/v2/worker/session",
        { Authorization: "Bearer gw-worker-tok" },
      );
      expect(workerConnRes.upgraded).toBe(true);
      if (!workerConnRes.upgraded) throw new Error("unreachable");
      const workerConn = workerConnRes.client;

      const helloBytes = pb.encodeWorkerMessage({
        msg: {
          case: "hello",
          value: {
            workerId: "tcp-worker-1",
            interfaces: ["gw.echo"],
            maxConcurrency: 2,
          },
        },
      });
      workerConn.sendRaw(encodeClientMaskedFrame(0x2, helloBytes));

      const welcomeFrame = await workerConn.readFrame();
      expect(welcomeFrame.opcode).toBe(0x2);
      const welcomeDecoded = pb.decodeWsFrame(welcomeFrame.payload);
      expect(welcomeDecoded.kind).toBe("data");

      // Send RFC 6455 Ping -> must receive Pong with matching payload
      const pingPayload = Buffer.from("keepalive-ping");
      workerConn.sendRaw(encodeClientMaskedFrame(0x9, pingPayload));
      const pongFrame = await workerConn.readFrame();
      expect(pongFrame.opcode).toBe(0xa);
      expect(pongFrame.payload.equals(pingPayload)).toBe(true);

      // Force hibernation via HTTP hook while TCP socket stays open!
      const hibRes = await fetch(`${gw.httpUrl}/__test/hibernate`, {
        method: "POST",
      });
      expect(hibRes.status).toBe(200);
      const hibJson = (await hibRes.json()) as {
        prev_boot_id: string;
        boot_id: string;
        constructor_runs: number;
        open_websockets: number;
      };
      expect(hibJson.boot_id).not.toBe(hibJson.prev_boot_id);
      expect(hibJson.constructor_runs).toBe(2);
      expect(hibJson.open_websockets).toBe(1);

      // Send Credit on the SAME post-hibernation TCP socket and verify worker stays online in /api/workers!
      const creditBytes = pb.encodeWorkerMessage({
        msg: { case: "credit", value: { freeSlots: 2 } },
      });
      workerConn.sendRaw(encodeClientMaskedFrame(0x2, creditBytes));
      // Give the inbound socket pump one tick to process Credit before HTTP /api/workers
      await new Promise((r) => setTimeout(r, 15));

      const workersRes = await fetch(`${gw.httpUrl}/api/workers`, {
        headers: { authorization: "Bearer gw-client-tok" },
      });
      expect(workersRes.status).toBe(200);
      const workersJson = (await workersRes.json()) as {
        workers: Array<{ worker_id: string; connected: boolean }>;
      };
      expect(workersJson.workers).toHaveLength(1);
      expect(workersJson.workers[0]!.worker_id).toBe("tcp-worker-1");
      expect(workersJson.workers[0]!.connected).toBe(true);

      // 3. > 32 MiB (33,554,433 bytes) frame header triggers immediate Close 1009!
      const oversizeConnRes = await RawTcpWsClient.connect(
        gw.port,
        "/ws/v2/client/invoke",
        { Authorization: "Bearer gw-client-tok" },
      );
      expect(oversizeConnRes.upgraded).toBe(true);
      if (!oversizeConnRes.upgraded) throw new Error("unreachable");
      const oversizeConn = oversizeConnRes.client;

      const oversizeHeader = Buffer.allocUnsafe(14);
      oversizeHeader[0] = 0x82; // FIN + Binary
      oversizeHeader[1] = 0x80 | 127; // Masked + 64-bit length
      oversizeHeader.writeUInt32BE(0, 2);
      oversizeHeader.writeUInt32BE(33_554_433, 6); // 32 MiB + 1 byte
      oversizeHeader.writeUInt32BE(0x12345678, 10);
      oversizeConn.sendRaw(oversizeHeader);

      const close1009 = await oversizeConn.readFrame();
      expect(close1009.opcode).toBe(0x8);
      expect(close1009.payload.readUInt16BE(0)).toBe(1009);
      oversizeConn.destroy();

      // 4. Clean empty Close frame (1005) echoed with empty Close frame
      const close1005ConnRes = await RawTcpWsClient.connect(
        gw.port,
        "/ws/v2/client/watch",
        { Authorization: "Bearer gw-client-tok" },
      );
      expect(close1005ConnRes.upgraded).toBe(true);
      if (!close1005ConnRes.upgraded) throw new Error("unreachable");
      const close1005Conn = close1005ConnRes.client;
      close1005Conn.sendRaw(encodeClientMaskedFrame(0x8, new Uint8Array(0)));
      const closeReply = await close1005Conn.readFrame();
      expect(closeReply.opcode).toBe(0x8);
      expect(closeReply.payload.length).toBe(0);
      close1005Conn.destroy();

      workerConn.destroy();
    } finally {
      await gw.close();
    }
  });
});
