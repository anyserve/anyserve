import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it, vi } from "vitest";

vi.mock("cloudflare:workers", () => {
  class DurableObject<Env = unknown> {
    protected ctx: unknown;
    protected env: Env;
    constructor(ctx: unknown, env: Env) {
      this.ctx = ctx;
      this.env = env;
    }
  }
  return { DurableObject };
});

import * as nativeWorkerModule from "../src/native-worker";
import worker, { SINGLETON_LEADER_NAME, isControlPlanePath } from "../src/native-worker";

const srcDir = resolve(dirname(fileURLToPath(import.meta.url)), "../src");

function relativeImports(source: string): string[] {
  const specs: string[] = [];
  for (const m of source.matchAll(/(?:import|export)\s[^;]*?from\s+["']([^"']+)["']/g)) {
    specs.push(m[1]!);
  }
  return specs;
}

describe("native-worker (Workers Free entrypoint)", () => {
  it("exports exactly AnyserveNativeDO, SINGLETON_LEADER_NAME, isControlPlanePath and a default fetch handler", () => {
    expect(Object.keys(nativeWorkerModule).sort()).toEqual([
      "AnyserveNativeDO",
      "SINGLETON_LEADER_NAME",
      "default",
      "isControlPlanePath",
    ]);
    expect(typeof worker.fetch).toBe("function");
    expect(SINGLETON_LEADER_NAME).toBe("singleton-leader");
  });

  it("classifies control-plane paths", () => {
    for (const p of ["/api", "/api/overview", "/rpc/v2/client/submit", "/ws/v2/worker/session", "/healthz", "/readyz", "/metrics"]) {
      expect(isControlPlanePath(p), p).toBe(true);
    }
    for (const p of ["/", "/index.html", "/assets/app.js", "/apix", "/wsx", "/metrics/extra", "/healthz/"]) {
      expect(isControlPlanePath(p), p).toBe(false);
    }
  });

  it("routes control-plane paths to the singleton-leader DO stub and everything else to ASSETS", async () => {
    const doCalls: string[] = [];
    const assetCalls: string[] = [];
    const env = {
      ANYSERVE_DO: {
        idFromName: (name: string) => {
          doCalls.push(`id:${name}`);
          return { name } as unknown as DurableObjectId;
        },
        get: (id: DurableObjectId) => ({
          fetch: async (req: Request) => {
            doCalls.push(`fetch:${(id as unknown as { name: string }).name}:${new URL(req.url).pathname}`);
            return new Response("do", { status: 201 });
          },
        }),
      },
      ASSETS: {
        fetch: async (req: Request) => {
          assetCalls.push(new URL(req.url).pathname);
          return new Response("asset", { status: 200 });
        },
      },
    } as unknown as Parameters<typeof worker.fetch>[1];

    const apiRes = await worker.fetch(new Request("https://x.example/api/overview"), env);
    expect(apiRes.status).toBe(201);
    const assetRes = await worker.fetch(new Request("https://x.example/index.html"), env);
    expect(assetRes.status).toBe(200);
    expect(doCalls).toEqual(["id:singleton-leader", "fetch:singleton-leader:/api/overview"]);
    expect(assetCalls).toEqual(["/index.html"]);
  });

  it("never reaches @cloudflare/containers, journal-do, storage, or index in its transitive import graph", () => {
    const visited = new Set<string>();
    const queue = [resolve(srcDir, "native-worker.ts")];
    const external = new Set<string>();
    while (queue.length > 0) {
      const file = queue.pop()!;
      if (visited.has(file)) continue;
      visited.add(file);
      const source = readFileSync(file, "utf8");
      for (const spec of relativeImports(source)) {
        if (!spec.startsWith(".")) {
          external.add(spec);
          continue;
        }
        // type-only imports are erased from the bundle
        const stmt = new RegExp(`import\\s+type\\s[^;]*from\\s+["']${spec.replace(/[./]/g, "\\$&")}["']`);
        if (stmt.test(source)) continue;
        queue.push(resolve(dirname(file), `${spec}.ts`));
      }
    }
    const names = [...visited].map((f) => f.slice(srcDir.length + 1));
    expect(names).toContain("native-do.ts");
    expect(names).toContain("native/engine.ts");
    for (const forbidden of ["journal-do.ts", "storage.ts", "index.ts", "env.ts"]) {
      expect(names, forbidden).not.toContain(forbidden);
    }
    expect([...external].sort()).toEqual(["cloudflare:workers"]);
  });
});
