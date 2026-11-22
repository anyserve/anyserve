import { ContainerProxy } from "@cloudflare/containers";
import { WorkerEntrypoint } from "cloudflare:workers";
import type { Env } from "./env";
import { AnyserveControlPlaneDO } from "./journal-do";
import { AnyserveNativeDO } from "./native-do";

export { AnyserveControlPlaneDO, AnyserveNativeDO, ContainerProxy };

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

export interface JournalGatewayProps {
  doId: string;
}

export class JournalGateway extends WorkerEntrypoint<Env, JournalGatewayProps> {
  override async fetch(request: Request): Promise<Response> {
    const doId = this.ctx.props?.doId;
    if (!doId) {
      return Response.json(
        { error: "missing JournalGateway doId prop" },
        { status: 500 },
      );
    }
    const containerDo = this.env
      .ANYSERVE_DO as unknown as DurableObjectNamespace<AnyserveControlPlaneDO>;
    const stub = containerDo.get(containerDo.idFromString(doId));
    return await stub.handleJournalRequest(request);
  }
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const pathname = new URL(request.url).pathname;
    if (isControlPlanePath(pathname)) {
      const id = env.ANYSERVE_DO.idFromName(SINGLETON_LEADER_NAME);
      const stub = env.ANYSERVE_DO.get(id);
      return await stub.fetch(request);
    }
    return await env.ASSETS.fetch(request);
  },
} satisfies ExportedHandler<Env>;
