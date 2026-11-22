import { Container } from "@cloudflare/containers";
import type { Env } from "./env";
import { JournalStorage } from "./storage";

const DEFAULT_INACTIVITY_TIMEOUT_MS = 10 * 60 * 1000;
const ACTIVE_WS_INACTIVITY_TIMEOUT_MS = 60 * 60 * 1000;
const WS_RENEW_THROTTLE_MS = 30 * 1000;

function parseDurationMs(raw: string | undefined, fallback: number): number {
  if (!raw) {
    return fallback;
  }
  const parsed = Number(raw);
  return Number.isFinite(parsed) && parsed > 0 ? Math.trunc(parsed) : fallback;
}

function buildContainerEnvVars(env: Env): Record<string, string> {
  const vars: Record<string, string> = {
    ANYSERVE_CLIENT_TOKENS: env.ANYSERVE_CLIENT_TOKENS ?? "",
    ANYSERVE_WORKER_TOKENS: env.ANYSERVE_WORKER_TOKENS ?? "",
  };
  if (env.ANYSERVE_PUBLIC_ENDPOINT) {
    vars.ANYSERVE_PUBLIC_ENDPOINT = env.ANYSERVE_PUBLIC_ENDPOINT;
  }
  const journalToken =
    env.JOURNAL_INTERNAL_SECRET ?? env.ANYSERVE_CLOUDFLARE_JOURNAL_TOKEN;
  if (journalToken) {
    vars.ANYSERVE_CLOUDFLARE_JOURNAL_TOKEN = journalToken;
  }
  if (env.RUST_LOG) {
    vars.RUST_LOG = env.RUST_LOG;
  }
  return vars;
}

function isWebSocketUpgrade(request: Request): boolean {
  return request.headers.get("upgrade")?.toLowerCase() === "websocket";
}

function normalizeWebSocketCloseCode(code: number): number {
  return code === 1005 || code === 1006 ? 1000 : code;
}

export class AnyserveControlPlaneDO extends Container<Env> {
  override defaultPort = 3001;
  override requiredPorts = [3001];
  override sleepAfter = "10m";
  override enableInternet = true;
  override pingEndpoint = "localhost/healthz";

  private readonly journalStorage: JournalStorage;
  private readonly inactivityTimeoutMs: number;
  private activeWsCount = 0;
  private lastWsKeepaliveRefreshMs = 0;

  constructor(ctx: DurableObjectState<{}>, env: Env) {
    super(ctx, env);
    this.inactivityTimeoutMs = parseDurationMs(
      env.CONTAINER_INACTIVITY_TIMEOUT_MS,
      DEFAULT_INACTIVITY_TIMEOUT_MS,
    );
    this.envVars = buildContainerEnvVars(env);
    this.journalStorage = new JournalStorage(this.ctx.storage, env.ANYSERVE_R2, {
      authSecret:
        env.JOURNAL_INTERNAL_SECRET ?? env.ANYSERVE_CLOUDFLARE_JOURNAL_TOKEN,
    });
    this.ctx.blockConcurrencyWhile(async () => {
      await this.restoreContainerBindings();
    });
  }

  get activeWebSocketCount(): number {
    return this.activeWsCount;
  }

  async restoreContainerBindings(): Promise<void> {
    const container = this.ctx.container;
    if (!container) {
      return;
    }
    const timeoutMs =
      this.activeWsCount > 0
        ? ACTIVE_WS_INACTIVITY_TIMEOUT_MS
        : this.inactivityTimeoutMs;
    await container.setInactivityTimeout(timeoutMs);
    const gatewayFactory = this.ctx.exports?.JournalGateway;
    if (typeof gatewayFactory === "function") {
      const gateway = gatewayFactory({
        props: { doId: this.ctx.id.toString() },
      });
      await container.interceptOutboundHttp("journal.internal", gateway);
    }
  }

  async handleJournalRequest(request: Request): Promise<Response> {
    return await this.journalStorage.handleRequest(request);
  }

  override async onActivityExpired(): Promise<void> {
    if (this.activeWsCount > 0) {
      this.renewActivityTimeout();
      await this.refreshContainerInactivityTimeout(true);
      return;
    }
    await super.onActivityExpired();
  }

  override async fetch(request: Request): Promise<Response> {
    if (!this.ctx.container?.running) {
      await this.restoreContainerBindings();
    }
    if (isWebSocketUpgrade(request)) {
      return await this.proxyWebSocket(request);
    }
    return await super.fetch(request);
  }

  private async refreshContainerInactivityTimeout(force: boolean): Promise<void> {
    const container = this.ctx.container;
    if (!container) {
      return;
    }
    const now = Date.now();
    if (!force && now - this.lastWsKeepaliveRefreshMs < WS_RENEW_THROTTLE_MS) {
      return;
    }
    this.lastWsKeepaliveRefreshMs = now;
    const timeoutMs =
      this.activeWsCount > 0
        ? ACTIVE_WS_INACTIVITY_TIMEOUT_MS
        : this.inactivityTimeoutMs;
    await container.setInactivityTimeout(timeoutMs);
  }

  private async proxyWebSocket(request: Request): Promise<Response> {
    const state = await this.getState();
    if (!this.ctx.container?.running || state.status !== "healthy") {
      try {
        await this.startAndWaitForPorts(this.defaultPort, {
          abort: request.signal,
        });
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        const lower = message.toLowerCase();
        if (lower.includes("no container instance")) {
          return new Response(
            "Control-plane container instance is temporarily unavailable.",
            { status: 503 },
          );
        }
        if (lower.includes("too many containers")) {
          return new Response(message, { status: 429 });
        }
        return new Response(`Failed to start container: ${message}`, {
          status: 500,
        });
      }
    }

    const container = this.ctx.container;
    if (!container) {
      return new Response("Container binding unavailable", { status: 500 });
    }
    const tcpPort = container.getTcpPort(this.defaultPort);
    const containerUrl = request.url.replace(/^https?:/i, "http:");
    this.renewActivityTimeout();

    const upstream = await tcpPort.fetch(containerUrl, request);
    if (!upstream.webSocket) {
      return upstream;
    }

    const containerWs = upstream.webSocket;
    const [client, server] = Object.values(new WebSocketPair()) as [
      WebSocket,
      WebSocket,
    ];

    this.activeWsCount += 1;
    this.renewActivityTimeout();
    void this.refreshContainerInactivityTimeout(true);

    let settled = false;
    const onDisconnect = () => {
      if (settled) {
        return;
      }
      settled = true;
      this.activeWsCount = Math.max(0, this.activeWsCount - 1);
      this.renewActivityTimeout();
      if (this.activeWsCount === 0) {
        void this.refreshContainerInactivityTimeout(true);
      }
    };

    containerWs.accept();
    server.accept();

    server.addEventListener("message", async (event) => {
      this.renewActivityTimeout();
      void this.refreshContainerInactivityTimeout(false);
      try {
        const data =
          event.data instanceof Blob
            ? await event.data.arrayBuffer()
            : event.data;
        containerWs.send(data);
      } catch {
        onDisconnect();
        server.close(1011, "Failed to forward message to container");
      }
    });

    containerWs.addEventListener("message", async (event) => {
      this.renewActivityTimeout();
      void this.refreshContainerInactivityTimeout(false);
      try {
        const data =
          event.data instanceof Blob
            ? await event.data.arrayBuffer()
            : event.data;
        server.send(data);
      } catch {
        onDisconnect();
        containerWs.close(1011, "Failed to forward message to client");
      }
    });

    server.addEventListener("close", (event) => {
      onDisconnect();
      containerWs.close(
        normalizeWebSocketCloseCode(event.code),
        event.reason,
      );
    });

    containerWs.addEventListener("close", (event) => {
      onDisconnect();
      server.close(normalizeWebSocketCloseCode(event.code), event.reason);
    });

    server.addEventListener("error", () => {
      onDisconnect();
      containerWs.close(1011, "Client WebSocket error");
    });

    containerWs.addEventListener("error", () => {
      onDisconnect();
      server.close(1011, "Container WebSocket error");
    });

    return new Response(null, {
      status: upstream.status,
      headers: upstream.headers,
      webSocket: client,
    });
  }
}
