// Dedicated Workers Free entrypoint for the native Durable Object control plane.
// Deliberately imports NOTHING from @cloudflare/containers, ./journal-do or ./storage so the Free
// bundle excludes the Container/journal stack (see test/native-worker.test.ts for the import-graph guard).

import type { Env } from "./env";
import { AnyserveNativeDO } from "./native-do";

export { AnyserveNativeDO };

export const SINGLETON_LEADER_NAME = "singleton-leader";

const CONTROL_PLANE_EXACT_PATHS = new Set(["/healthz", "/readyz", "/metrics"]);
const CONTROL_PLANE_PREFIXES = ["/api", "/rpc", "/ws"];

export function isControlPlanePath(pathname: string): boolean {
  if (CONTROL_PLANE_EXACT_PATHS.has(pathname)) {
    return true;
  }
  return CONTROL_PLANE_PREFIXES.some(
    (prefix) => pathname === prefix || pathname.startsWith(`${prefix}/`),
  );
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const pathname = new URL(request.url).pathname;
    if (isControlPlanePath(pathname)) {
      const id = env.ANYSERVE_DO.idFromName(SINGLETON_LEADER_NAME);
      return await env.ANYSERVE_DO.get(id).fetch(request);
    }
    return await env.ASSETS.fetch(request);
  },
} satisfies ExportedHandler<Env>;
