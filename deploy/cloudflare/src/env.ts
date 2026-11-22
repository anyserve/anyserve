import type { AnyserveControlPlaneDO } from "./journal-do";

declare global {
  namespace Cloudflare {
    interface Env {
      ASSETS: Fetcher;
      ANYSERVE_DO: DurableObjectNamespace<AnyserveControlPlaneDO>;
      ANYSERVE_R2: R2Bucket;
      ANYSERVE_CLIENT_TOKENS?: string;
      ANYSERVE_WORKER_TOKENS?: string;
      ANYSERVE_PUBLIC_ENDPOINT?: string;
      JOURNAL_INTERNAL_SECRET?: string;
      ANYSERVE_CLOUDFLARE_JOURNAL_TOKEN?: string;
      CONTAINER_INACTIVITY_TIMEOUT_MS?: string;
      RUST_LOG?: string;
    }
    interface GlobalProps {
      mainModule: typeof import("./index");
      durableNamespaces: "AnyserveControlPlaneDO";
    }
  }
}

export type Env = Cloudflare.Env;
