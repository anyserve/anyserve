import type { AnyserveControlPlaneDO } from "./journal-do";
import type { AnyserveNativeDO } from "./native-do";

declare global {
  namespace Cloudflare {
    interface Env {
      ASSETS: Fetcher;
      ANYSERVE_DO: DurableObjectNamespace<
        AnyserveNativeDO | AnyserveControlPlaneDO
      >;
      ANYSERVE_R2: R2Bucket;
      ANYSERVE_CLIENT_TOKENS?: string;
      ANYSERVE_WORKER_TOKENS?: string;
      ANYSERVE_SCOPED_TOKENS?: string;
      ANYSERVE_TOKEN_SIGNING_KEY?: string;
      ANYSERVE_AUTH_REVOCATION_URL?: string;
      ANYSERVE_AUTH_REVOCATION_POLL_INTERVAL_MS?: string;
      ANYSERVE_CONSOLE_AUTH_PORTAL_URL?: string;
      ANYSERVE_MEMORY_LIMIT_MB?: string;
      ANYSERVE_PUBLIC_ENDPOINT?: string;
      ANYSERVE_CONSOLE_POLL_MS?: string;
      ANYSERVE_ALLOWED_ORIGINS?: string;
      ANYSERVE_RUNTIME_OVERRIDES_JSON?: string;
      JOURNAL_INTERNAL_SECRET?: string;
      ANYSERVE_CLOUDFLARE_JOURNAL_TOKEN?: string;
      CONTAINER_INACTIVITY_TIMEOUT_MS?: string;
      RUST_LOG?: string;
    }
    interface GlobalProps {
      mainModule: typeof import("./index");
      durableNamespaces: "AnyserveNativeDO" | "AnyserveControlPlaneDO";
    }
  }
}

export type Env = Cloudflare.Env;
