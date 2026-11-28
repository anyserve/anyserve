import { readFileSync } from "node:fs";
import { DatabaseSync } from "node:sqlite";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  createNodeSqliteStorage,
  InMemoryR2Bucket,
} from "../src/node-dev-gateway";

// Minimal cloudflare:workers + WebSocketPair mocks so we can exercise Worker routing,
// JournalGateway RPC forwarding, and AnyserveControlPlaneDO lifecycle in Node 24 Vitest.
vi.mock("cloudflare:workers", () => {
  class DurableObject<Env = unknown, Props = {}> {
    protected ctx: unknown;
    protected env: Env;
    constructor(ctx: unknown, env: Env) {
      this.ctx = ctx;
      this.env = env;
    }
  }
  class WorkerEntrypoint<Env = unknown, Props = {}> {
    protected ctx: { props?: Props } & Record<string, unknown>;
    protected env: Env;
    constructor(ctx: { props?: Props } & Record<string, unknown>, env: Env) {
      this.ctx = ctx;
      this.env = env;
    }
  }
  return { DurableObject, WorkerEntrypoint };
});

class MockWebSocket {
  accepted = false;
  closed: { code?: number; reason?: string } | null = null;
  sent: Array<string | ArrayBuffer | ArrayBufferView> = [];
  private listeners = new Map<string, Array<(ev: any) => void>>();

  accept() {
    this.accepted = true;
  }

  send(data: string | ArrayBuffer | ArrayBufferView) {
    this.sent.push(data);
  }

  close(code?: number, reason?: string) {
    this.closed = { code, reason };
  }

  addEventListener(type: string, cb: (ev: any) => void) {
    const arr = this.listeners.get(type) ?? [];
    arr.push(cb);
    this.listeners.set(type, arr);
  }

  emit(type: string, event: any) {
    for (const cb of this.listeners.get(type) ?? []) {
      cb(event);
    }
  }
}

class MockWebSocketPair {
  0: MockWebSocket;
  1: MockWebSocket;
  constructor() {
    this[0] = new MockWebSocket();
    this[1] = new MockWebSocket();
  }
}

const originalWebSocketPair = (globalThis as Record<string, unknown>)
  .WebSocketPair;
const OriginalResponse = globalThis.Response;

beforeEach(() => {
  (globalThis as Record<string, unknown>).WebSocketPair = MockWebSocketPair;
  // Cloudflare workerd allows status 101 on Response when webSocket is attached; Node's undici Response throws RangeError.
  class CloudflareResponse extends OriginalResponse {
    readonly webSocket?: unknown;
    private readonly customStatus?: number;
    constructor(
      body?: BodyInit | null,
      init?: ResponseInit & { webSocket?: unknown },
    ) {
      if (init?.status === 101 && init.webSocket) {
        const { status: _s, webSocket, ...rest } = init;
        super(body, { ...rest, status: 200 });
        this.customStatus = 101;
        this.webSocket = webSocket;
      } else {
        super(body, init);
      }
    }
    override get status(): number {
      return this.customStatus ?? super.status;
    }
  }
  globalThis.Response = CloudflareResponse as typeof Response;
});

afterEach(() => {
  if (originalWebSocketPair === undefined) {
    delete (globalThis as Record<string, unknown>).WebSocketPair;
  } else {
    (globalThis as Record<string, unknown>).WebSocketPair =
      originalWebSocketPair;
  }
  globalThis.Response = OriginalResponse;
});

describe("Worker routing, JournalGateway, and AnyserveControlPlaneDO", () => {
  it("routes control-plane HTTP/WS paths to singleton DO and static routes to ASSETS", async () => {
    const workerModule = await import("../src/index");
    const worker = workerModule.default;

    const doFetches: string[] = [];
    const assetFetches: string[] = [];
    const idFromNameCalls: string[] = [];
    const fakeId = { toString: () => "singleton-do-id" };

    const env = {
      ASSETS: {
        fetch: async (req: Request) => {
          assetFetches.push(new URL(req.url).pathname);
          return new Response("asset-ok", { status: 200 });
        },
      },
      ANYSERVE_DO: {
        idFromName: (name: string) => {
          idFromNameCalls.push(name);
          return fakeId;
        },
        get: () => ({
          fetch: async (req: Request) => {
            doFetches.push(new URL(req.url).pathname);
            return new Response("do-ok", { status: 200 });
          },
        }),
      },
    } as unknown as Parameters<typeof worker.fetch>[1];

    for (const path of [
      "/api/overview",
      "/rpc/v2/client/submit",
      "/ws/v2/worker/session",
      "/healthz",
      "/readyz",
      "/metrics",
    ]) {
      const res = await worker.fetch(
        new Request(`https://anyserve.example.com${path}`),
        env,
      );
      expect(res.status).toBe(200);
      expect(await res.text()).toBe("do-ok");
    }

    for (const path of ["/", "/explore", "/assets/duckdb-eh-123.wasm.bin"]) {
      const res = await worker.fetch(
        new Request(`https://anyserve.example.com${path}`),
        env,
      );
      expect(res.status).toBe(200);
      expect(await res.text()).toBe("asset-ok");
    }

    expect(idFromNameCalls).toEqual(Array(6).fill("singleton-leader"));
    expect(doFetches).toEqual([
      "/api/overview",
      "/rpc/v2/client/submit",
      "/ws/v2/worker/session",
      "/healthz",
      "/readyz",
      "/metrics",
    ]);
    expect(assetFetches).toEqual([
      "/",
      "/explore",
      "/assets/duckdb-eh-123.wasm.bin",
    ]);
  });

  it("restores inactivity timeout & outbound interception, forwards JournalGateway RPC, and tracks active WebSockets", async () => {
    const {
      AnyserveControlPlaneDO,
      AnyserveNativeDO,
      ContainerProxy,
      JournalGateway,
    } = await import("../src/index");
    expect(ContainerProxy).toBeDefined();
    expect(AnyserveNativeDO).toBeDefined();

    const db = new DatabaseSync(":memory:");
    const { storage: sqlStorage } = createNodeSqliteStorage(db, {
      enforceCloudflareLimits: true,
    });
    const r2 = new InMemoryR2Bucket();
    const kv = new Map<string, unknown>();

    const inactivityTimeouts: Array<number | bigint> = [];
    const interceptedHosts: Array<{ addr: string; binding: unknown }> = [];
    const containerSignals: number[] = [];
    const containerWsMock = new MockWebSocket();
    let rejectNextUpgrade = false;

    const tcpPort = {
      fetch: async (url: string, req: Request) => {
        if (rejectNextUpgrade) {
          return new Response("missing bearer token", {
            status: 401,
            headers: { "x-anyserve-grpc-code": "16" },
          });
        }
        if (req.headers.get("upgrade")?.toLowerCase() === "websocket") {
          return {
            status: 101,
            headers: new Headers(),
            webSocket: containerWsMock,
          } as unknown as Response;
        }
        return new Response(`proxied:${new URL(url).pathname}`, {
          status: 200,
        });
      },
    };

    const blockPromises: Array<Promise<unknown>> = [];
    const fakeContainer = {
      running: true,
      start: () => {},
      monitor: () => new Promise<void>(() => {}),
      destroy: async () => {},
      signal: (signo: number) => {
        containerSignals.push(signo);
      },
      getTcpPort: () => tcpPort,
      setInactivityTimeout: async (ms: number | bigint) => {
        inactivityTimeouts.push(ms);
      },
      interceptOutboundHttp: async (addr: string, binding: unknown) => {
        interceptedHosts.push({ addr, binding });
      },
    };

    let gatewayPropsSeen: { doId: string } | undefined;
    const fakeGatewayBinding = {
      fetch: async (req: Request) => gateway.fetch(req),
    };

    const doCtx = {
      id: { toString: () => "do-instance-123" },
      container: fakeContainer,
      exports: {
        ContainerProxy: () => ({
          fetch: async () => new Response("ok"),
        }),
        JournalGateway: ({ props }: { props: { doId: string } }) => {
          gatewayPropsSeen = props;
          return fakeGatewayBinding;
        },
      },
      blockConcurrencyWhile: <T>(fn: () => Promise<T>) => {
        const p = fn();
        blockPromises.push(p);
        return p;
      },
      storage: {
        sql: sqlStorage.sql,
        transactionSync: sqlStorage.transactionSync,
        kv: {
          get: (k: string) => kv.get(k),
          put: (k: string, v: unknown) => kv.set(k, v),
        },
        get: async (k: string) => kv.get(k),
        put: async (k: string, v: unknown) => {
          kv.set(k, v);
        },
        setAlarm: async () => {},
        deleteAlarm: async () => {},
        sync: async () => {},
      },
    };

    const env = {
      ANYSERVE_R2: r2,
      ANYSERVE_CLIENT_TOKENS: "client-tok",
      ANYSERVE_WORKER_TOKENS: "worker-tok",
      JOURNAL_INTERNAL_SECRET: "internal-secret",
      CONTAINER_INACTIVITY_TIMEOUT_MS: "120000",
      ANYSERVE_DO: {
        idFromString: (id: string) => ({ toString: () => id }),
        get: () => doInstance,
      },
    };

    const doInstance = new AnyserveControlPlaneDO(
      doCtx as unknown as ConstructorParameters<typeof AnyserveControlPlaneDO>[0],
      env as unknown as ConstructorParameters<typeof AnyserveControlPlaneDO>[1],
    );
    await Promise.all(blockPromises);

    const gateway = new JournalGateway(
      { props: gatewayPropsSeen } as unknown as ConstructorParameters<
        typeof JournalGateway
      >[0],
      env as unknown as ConstructorParameters<typeof JournalGateway>[1],
    );

    // 1. Constructor restored inactivity timeout, bound outbound journal.internal, and passed container envVars.
    expect(inactivityTimeouts[0]).toBe(120_000);
    expect(gatewayPropsSeen).toEqual({ doId: "do-instance-123" });
    expect(interceptedHosts).toEqual([
      { addr: "journal.internal", binding: fakeGatewayBinding },
    ]);
    expect(doInstance.envVars).toMatchObject({
      ANYSERVE_CLIENT_TOKENS: "client-tok",
      ANYSERVE_WORKER_TOKENS: "worker-tok",
      ANYSERVE_CLOUDFLARE_JOURNAL_TOKEN: "internal-secret",
    });

    // 2. JournalGateway forwards http://journal.internal/v1/* to DO handleJournalRequest RPC.
    const unauthGwRes = await gateway.fetch(
      new Request("http://journal.internal/v1/acquire", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ holder_id: "c1", lease_duration_ms: 5000 }),
      }),
    );
    expect(unauthGwRes.status).toBe(401);

    const authGwRes = await gateway.fetch(
      new Request("http://journal.internal/v1/acquire", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: "Bearer internal-secret",
        },
        body: JSON.stringify({ holder_id: "c1", lease_duration_ms: 5000 }),
      }),
    );
    expect(authGwRes.status).toBe(200);
    expect(await authGwRes.json()).toEqual({
      acquired: true,
      term: 1,
      active: [],
      workers: [],
    });

    // 3. Pre-upgrade WebSocket rejection (401/403/503) returns directly without incrementing activeWsCount.
    kv.set("__CF_CONTAINER_STATE", {
      status: "healthy",
      lastChange: Date.now(),
    });
    rejectNextUpgrade = true;
    const rejectedWs = await doInstance.fetch(
      new Request("https://anyserve.example.com/ws/v2/worker/session", {
        headers: { upgrade: "websocket" },
      }),
    );
    expect(rejectedWs.status).toBe(401);
    expect(rejectedWs.headers.get("x-anyserve-grpc-code")).toBe("16");
    expect(doInstance.activeWebSocketCount).toBe(0);

    // 4. Active WebSocket keeps container awake across onActivityExpired until closed.
    rejectNextUpgrade = false;
    const wsRes = await doInstance.fetch(
      new Request("https://anyserve.example.com/ws/v2/worker/session", {
        headers: { upgrade: "websocket" },
      }),
    );
    expect(wsRes.status).toBe(101);
    expect(doInstance.activeWebSocketCount).toBe(1);
    expect(containerWsMock.accepted).toBe(true);

    await doInstance.onActivityExpired();
    expect(containerSignals).toEqual([]); // Must NOT stop while worker WS is active!

    // Close the container WebSocket and confirm onActivityExpired now signals SIGTERM (15).
    containerWsMock.emit("close", { code: 1000, reason: "worker disconnected" });
    expect(doInstance.activeWebSocketCount).toBe(0);

    await doInstance.onActivityExpired();
    expect(containerSignals).toEqual([15]);

    db.close();
  });

  it("falls back to the request host for ANYSERVE_PUBLIC_ENDPOINT only when unset and the container is stopped", async () => {
    const { AnyserveControlPlaneDO } = await import("../src/index");
    const db = new DatabaseSync(":memory:");
    const { storage: sqlStorage } = createNodeSqliteStorage(db, {
      enforceCloudflareLimits: true,
    });
    const kv = new Map<string, unknown>();
    const build = (running: boolean, extraEnv: Record<string, string>) => {
      const ctx = {
        id: { toString: () => "do-host-fallback" },
        container: {
          running,
          start: () => {},
          monitor: () => new Promise<void>(() => {}),
          destroy: async () => {},
          signal: () => {},
          getTcpPort: () => ({}),
          setInactivityTimeout: async () => {},
          interceptOutboundHttp: async () => {},
        },
        exports: { ContainerProxy: () => ({ fetch: async () => new Response("ok") }) },
        blockConcurrencyWhile: <T>(fn: () => Promise<T>) => fn(),
        storage: {
          sql: sqlStorage.sql,
          transactionSync: sqlStorage.transactionSync,
          kv: {
            get: (k: string) => kv.get(k),
            put: (k: string, v: unknown) => kv.set(k, v),
          },
          get: async (k: string) => kv.get(k),
          put: async (k: string, v: unknown) => {
            kv.set(k, v);
          },
          setAlarm: async () => {},
          deleteAlarm: async () => {},
          sync: async () => {},
        },
      };
      const env = {
        ANYSERVE_R2: new InMemoryR2Bucket(),
        ANYSERVE_CLIENT_TOKENS: "c",
        ANYSERVE_WORKER_TOKENS: "w",
        ...extraEnv,
      };
      const instance = new AnyserveControlPlaneDO(
        ctx as unknown as ConstructorParameters<typeof AnyserveControlPlaneDO>[0],
        env as unknown as ConstructorParameters<typeof AnyserveControlPlaneDO>[1],
      );
      // Stop before actually starting a container: we only inspect envVars.
      (instance as unknown as { proxyWebSocket: () => Promise<Response> }).proxyWebSocket =
        async () => new Response("ws-stub");
      return instance;
    };
    const wsRequest = () =>
      new Request("https://edge.example.workers.dev/ws/v2/worker/session", {
        headers: { upgrade: "websocket" },
      });

    const unset = build(false, {});
    expect(unset.envVars.ANYSERVE_PUBLIC_ENDPOINT).toBeUndefined();
    await unset.fetch(wsRequest());
    expect(unset.envVars.ANYSERVE_PUBLIC_ENDPOINT).toBe(
      "wss://edge.example.workers.dev",
    );
    expect(unset.envVars.ANYSERVE_CLIENT_TOKENS).toBe("c");

    const blank = build(false, { ANYSERVE_PUBLIC_ENDPOINT: "   " });
    await blank.fetch(wsRequest());
    expect(blank.envVars.ANYSERVE_PUBLIC_ENDPOINT).toBe(
      "wss://edge.example.workers.dev",
    );

    const configured = build(false, {
      ANYSERVE_PUBLIC_ENDPOINT: "wss://configured.example.com",
    });
    await configured.fetch(wsRequest());
    expect(configured.envVars.ANYSERVE_PUBLIC_ENDPOINT).toBe(
      "wss://configured.example.com",
    );

    const running = build(true, {});
    await running.fetch(wsRequest());
    expect(running.envVars.ANYSERVE_PUBLIC_ENDPOINT).toBeUndefined();
    db.close();
  });

  it("keeps Dockerfile embedded /etc/anyserve/server.toml in sync with server.toml", () => {
    const testDir = dirname(fileURLToPath(import.meta.url));
    const cfDir = resolve(testDir, "..");
    const serverToml = readFileSync(resolve(cfDir, "server.toml"), "utf8");
    const dockerfile = readFileSync(resolve(cfDir, "Dockerfile"), "utf8");
    const match =
      /COPY <<'ANYSERVE_SERVER_TOML' \/etc\/anyserve\/server\.toml\n([\s\S]*?)\nANYSERVE_SERVER_TOML\n/.exec(
        dockerfile,
      );
    expect(match).not.toBeNull();
    expect(`${match![1]!}\n`).toBe(serverToml);
  });

  it("validates dual wrangler.jsonc (Native Free default) and wrangler.containers.jsonc (Paid Containers) configs", () => {
    const testDir = dirname(fileURLToPath(import.meta.url));
    const cfDir = resolve(testDir, "..");

    const nativeCfg = JSON.parse(
      readFileSync(resolve(cfDir, "wrangler.jsonc"), "utf8"),
    );
    expect(nativeCfg.name).toBe("anyserve-cloudflare-native");
    expect(nativeCfg.main).toBe("src/native-worker.ts");
    expect(nativeCfg.compatibility_date).toBe("2025-11-17");
    expect(nativeCfg.build?.command).toContain("npm --prefix");
    expect(nativeCfg.build?.command).toContain("../../console run build");
    expect(nativeCfg.containers).toBeUndefined();
    expect(nativeCfg.durable_objects).toEqual({
      bindings: [{ name: "ANYSERVE_DO", class_name: "AnyserveNativeDO" }],
    });
    expect(nativeCfg.migrations).toEqual([
      { tag: "native-v1", new_sqlite_classes: ["AnyserveNativeDO"] },
    ]);
    expect(nativeCfg.assets).toMatchObject({
      directory: "../../console/dist",
      binding: "ASSETS",
      not_found_handling: "single-page-application",
      run_worker_first: [
        "/api/*",
        "/rpc/*",
        "/ws/*",
        "/healthz",
        "/readyz",
        "/metrics",
      ],
    });
    expect(nativeCfg.r2_buckets).toEqual([
      { binding: "ANYSERVE_R2", bucket_name: "anyserve-journal" },
    ]);

    const containersCfg = JSON.parse(
      readFileSync(resolve(cfDir, "wrangler.containers.jsonc"), "utf8"),
    );
    expect(containersCfg.name).toBe("anyserve-cloudflare");
    expect(containersCfg.main).toBe("src/index.ts");
    expect(containersCfg.containers).toEqual([
      {
        class_name: "AnyserveControlPlaneDO",
        image: "./Dockerfile",
        image_build_context: "../..",
        instance_type: "lite",
        max_instances: 1,
      },
    ]);
    expect(containersCfg.durable_objects).toEqual({
      bindings: [{ name: "ANYSERVE_DO", class_name: "AnyserveControlPlaneDO" }],
    });
    expect(containersCfg.migrations).toEqual([
      { tag: "v1", new_sqlite_classes: ["AnyserveControlPlaneDO"] },
    ]);
    expect(containersCfg.r2_buckets).toEqual([
      { binding: "ANYSERVE_R2", bucket_name: "anyserve-journal" },
    ]);
  });

  it("routes Worker fetch requests into AnyserveNativeDO singleton-leader and ASSETS", async () => {
    const workerModule = await import("../src/index");
    const { AnyserveNativeDO } = workerModule;
    const worker = workerModule.default;

    const db = new DatabaseSync(":memory:");
    const { storage: sqlStorage } = createNodeSqliteStorage(db, {
      enforceCloudflareLimits: true,
    });
    const acceptedSockets: MockWebSocket[] = [];
    const doCtx = {
      id: { toString: () => "native-singleton-id" },
      acceptWebSocket: (ws: MockWebSocket) => {
        ws.accept();
        acceptedSockets.push(ws);
      },
      getWebSockets: () => acceptedSockets,
      storage: {
        sql: sqlStorage.sql,
        transactionSync: sqlStorage.transactionSync,
        setAlarm: async () => {},
        getAlarm: async () => null,
        deleteAlarm: async () => {},
      },
    };

    const r2 = new InMemoryR2Bucket();
    const idFromNameCalls: string[] = [];
    let nativeDoInstance: InstanceType<typeof AnyserveNativeDO> | null = null;

    const env = {
      ANYSERVE_R2: r2,
      ANYSERVE_CLIENT_TOKENS: "native-client-tok",
      ANYSERVE_WORKER_TOKENS: "native-worker-tok",
      ASSETS: {
        fetch: async (req: Request) =>
          new Response(`asset:${new URL(req.url).pathname}`, { status: 200 }),
      },
      ANYSERVE_DO: {
        idFromName: (name: string) => {
          idFromNameCalls.push(name);
          return { toString: () => `id:${name}` };
        },
        get: () => {
          if (!nativeDoInstance) {
            nativeDoInstance = new AnyserveNativeDO(
              doCtx as unknown as ConstructorParameters<
                typeof AnyserveNativeDO
              >[0],
              env as unknown as ConstructorParameters<
                typeof AnyserveNativeDO
              >[1],
            );
          }
          return nativeDoInstance;
        },
      },
    } as unknown as Parameters<typeof worker.fetch>[1];

    const readyRes = await worker.fetch(
      new Request("https://native.example.workers.dev/readyz"),
      env,
    );
    expect(readyRes.status).toBe(204);

    const overviewRes = await worker.fetch(
      new Request("https://native.example.workers.dev/api/overview", {
        headers: { authorization: "Bearer native-client-tok" },
      }),
      env,
    );
    expect(overviewRes.status).toBe(200);
    const overview = (await overviewRes.json()) as Record<string, any>;
    expect(overview.runtime.code).toBe("memory_plus_cloudflare");
    expect(overview.control_plane.recommended_poll_ms).toBe(10_000);
    expect(overview.control_plane.public_endpoint).toBe(
      "wss://native.example.workers.dev",
    );

    const unauthWs = await worker.fetch(
      new Request("https://native.example.workers.dev/ws/v2/worker/session", {
        headers: { upgrade: "websocket" },
      }),
      env,
    );
    expect(unauthWs.status).toBe(401);
    expect(unauthWs.headers.get("x-anyserve-grpc-code")).toBe("16");

    const assetRes = await worker.fetch(
      new Request("https://native.example.workers.dev/explore"),
      env,
    );
    expect(assetRes.status).toBe(200);
    expect(await assetRes.text()).toBe("asset:/explore");
    expect(idFromNameCalls).toEqual([
      "singleton-leader",
      "singleton-leader",
      "singleton-leader",
    ]);

    db.close();
  });

  it("buildContainerEnvVars forwards portal URL, memory limit, signing key, and revocation settings", async () => {
    const { buildContainerEnvVars } = await import("../src/journal-do");
    const vars = buildContainerEnvVars({
      ANYSERVE_CLIENT_TOKENS: "c1",
      ANYSERVE_WORKER_TOKENS: "w1",
      ANYSERVE_TOKEN_SIGNING_KEY: "sign-key",
      ANYSERVE_AUTH_REVOCATION_URL: "https://rev.internal/list",
      ANYSERVE_AUTH_REVOCATION_POLL_INTERVAL_MS: "1500",
      ANYSERVE_CONSOLE_AUTH_PORTAL_URL: "https://relay.example.com/tokens",
      ANYSERVE_MEMORY_LIMIT_MB: "192",
    } as Parameters<typeof buildContainerEnvVars>[0]);
    expect(vars).toMatchObject({
      ANYSERVE_CLIENT_TOKENS: "c1",
      ANYSERVE_WORKER_TOKENS: "w1",
      ANYSERVE_TOKEN_SIGNING_KEY: "sign-key",
      ANYSERVE_AUTH_REVOCATION_URL: "https://rev.internal/list",
      ANYSERVE_AUTH_REVOCATION_POLL_INTERVAL_MS: "1500",
      ANYSERVE_CONSOLE_AUTH_PORTAL_URL: "https://relay.example.com/tokens",
      ANYSERVE_MEMORY_LIMIT_MB: "192",
    });
  });
});
