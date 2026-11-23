import { ContainerProxy } from "@cloudflare/containers";
import { WorkerEntrypoint } from "cloudflare:workers";
import type { Env } from "./env";
import { AnyserveControlPlaneDO } from "./journal-do";
import { AnyserveNativeDO } from "./native-do";
import { SINGLETON_LEADER_NAME, isControlPlanePath } from "./native-worker";

export { AnyserveControlPlaneDO, AnyserveNativeDO, ContainerProxy };

export { SINGLETON_LEADER_NAME, isControlPlanePath };

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
