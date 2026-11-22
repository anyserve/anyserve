import { DurableObject } from "cloudflare:workers";
import type { Env } from "./env";
import { NativeEngine } from "./native/engine";
import type {
  NativeEnvConfig,
  NativeHost,
  NativeR2Bucket,
  NativeRuntimeOverrides,
  NativeWebSocket,
  NativeWebSocketPairResult,
} from "./native/host";

export interface NativeWorkerEnv extends Env, NativeEnvConfig {}

const constructorRunsByDoId = new Map<string, number>();

function parseRuntimeOverrides(raw?: string): NativeRuntimeOverrides | undefined {
  const trimmed = raw?.trim();
  if (!trimmed) {
    return undefined;
  }
  const parsed = JSON.parse(trimmed) as unknown;
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new Error("ANYSERVE_RUNTIME_OVERRIDES_JSON must be a JSON object");
  }
  return parsed as NativeRuntimeOverrides;
}

/**
 * Cloudflare Workers Free native Durable Object control plane (no Containers).
 * Uses Hibernatable WebSockets (`acceptWebSocket`, `webSocketMessage`, `webSocketClose`, `alarm`)
 * and delegates all control-plane logic to the runtime-agnostic `NativeEngine`.
 */
export class AnyserveNativeDO extends DurableObject<NativeWorkerEnv> {
  private readonly engine: NativeEngine;

  constructor(ctx: DurableObjectState, env: NativeWorkerEnv) {
    super(ctx, env);
    const doId = ctx.id.toString();
    const overrides = parseRuntimeOverrides(env.ANYSERVE_RUNTIME_OVERRIDES_JSON);
    const host: NativeHost = {
      doId,
      storage: {
        sql: ctx.storage.sql,
        transactionSync: <T>(closure: () => T): T =>
          ctx.storage.transactionSync(closure),
      },
      r2: (env.ANYSERVE_R2 as unknown as NativeR2Bucket | undefined) ?? null,
      env,
      overrides,
      acceptWebSocket: (ws: NativeWebSocket, tags?: string[]) => {
        ctx.acceptWebSocket(ws as unknown as WebSocket, tags);
      },
      getWebSockets: (tag?: string): NativeWebSocket[] => {
        return ctx.getWebSockets(tag) as unknown as NativeWebSocket[];
      },
      createWebSocketPair: (): NativeWebSocketPairResult => {
        const [client, server] = Object.values(new WebSocketPair()) as [
          WebSocket,
          WebSocket,
        ];
        return {
          client,
          server: server as unknown as NativeWebSocket,
          toUpgradeResponse: (headers?: HeadersInit) =>
            new Response(null, {
              status: 101,
              headers,
              webSocket: client,
            }),
        };
      },
      nowMs: () => Date.now(),
      setAlarm: async (scheduledTimeMs: number) => {
        await ctx.storage.setAlarm(scheduledTimeMs);
      },
      getAlarm: async () => {
        return await ctx.storage.getAlarm();
      },
      deleteAlarm: async () => {
        await ctx.storage.deleteAlarm();
      },
      nextConstructorRun: () => {
        const next = (constructorRunsByDoId.get(doId) ?? 0) + 1;
        constructorRunsByDoId.set(doId, next);
        return next;
      },
    };
    this.engine = new NativeEngine(host);
  }

  override async fetch(request: Request): Promise<Response> {
    return await this.engine.fetch(request);
  }

  override async webSocketMessage(
    ws: WebSocket,
    message: string | ArrayBuffer,
  ): Promise<void> {
    await this.engine.webSocketMessage(
      ws as unknown as NativeWebSocket,
      message,
    );
  }

  override async webSocketClose(
    ws: WebSocket,
    code: number,
    reason: string,
    wasClean: boolean,
  ): Promise<void> {
    await this.engine.webSocketClose(
      ws as unknown as NativeWebSocket,
      code,
      reason,
      wasClean,
    );
  }

  override async webSocketError(ws: WebSocket, _error: unknown): Promise<void> {
    await this.engine.webSocketClose(
      ws as unknown as NativeWebSocket,
      1006,
      "websocket error",
      false,
    );
  }

  override async alarm(): Promise<void> {
    await this.engine.alarm();
  }
}
