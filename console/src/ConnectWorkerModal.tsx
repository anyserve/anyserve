import { useEffect, useState } from "react";

import { PROJECT_INSTALL_PS1_URL, PROJECT_INSTALL_SH_URL } from "./app-constants";
import { IconClose } from "./icons";
import { CopyButton } from "./pane-tables";
import type { ControlPlaneSummary } from "./types";

type OnboardingTab = "python" | "connectors" | "rust" | "container" | "linux";

interface ConnectWorkerModalProps {
  open: boolean;
  controlPlane?: ControlPlaneSummary;
  onClose: () => void;
}

const TABS: Array<{ id: OnboardingTab; label: string }> = [
  { id: "python", label: "Python SDK" },
  { id: "connectors", label: "AI Connectors" },
  { id: "rust", label: "Rust SDK" },
  { id: "container", label: "Docker & Helm" },
  { id: "linux", label: "CLI Install" },
];

function resolveWildcardHost(listen: string, host: string): string {
  if (listen.startsWith("0.0.0.0:")) return `${host}:${listen.slice("0.0.0.0:".length)}`;
  if (listen.startsWith("[::]:")) return `${host}:${listen.slice("[::]:".length)}`;
  return listen;
}

function toWebSocketUrl(url: string): string {
  if (/^https:\/\//i.test(url)) return `wss://${url.slice(8)}`;
  if (/^http:\/\//i.test(url)) return `ws://${url.slice(7)}`;
  return url;
}

export function deriveEndpoint(controlPlane?: ControlPlaneSummary): string {
  const explicit = controlPlane?.public_endpoint?.trim() || import.meta.env.VITE_PUBLIC_ENDPOINT?.trim();
  const host = typeof window !== "undefined" && window.location.hostname ? window.location.hostname : "127.0.0.1";
  if (controlPlane?.grpc_enabled === false) {
    if (explicit) return toWebSocketUrl(explicit);
    const origin = typeof window !== "undefined" ? window.location.origin : "";
    if (origin && origin !== "null") return toWebSocketUrl(origin);
    const listen = controlPlane.console_listen?.trim() || `${host}:3001`;
    return `ws://${resolveWildcardHost(listen, host)}`;
  }
  if (explicit) return explicit;
  const listen = controlPlane?.grpc_listen?.trim() || "127.0.0.1:50052";
  return `${controlPlane?.grpc_tls_enabled ? "https" : "http"}://${resolveWildcardHost(listen, host)}`;
}

function CopyableBlock({ title, code }: { title: string; code: string }) {
  return (
    <div className="snippet-card">
      <div className="snippet-card__head">
        <span>{title}</span>
        <CopyButton text={code} />
      </div>
      <pre className="code-block code-block--snippet">{code}</pre>
    </div>
  );
}

export function ConnectWorkerModal({ open, controlPlane, onClose }: ConnectWorkerModalProps) {
  const [tab, setTab] = useState<OnboardingTab>("python");

  useEffect(() => {
    if (!open) return;
    function onKeyDown(event: KeyboardEvent) {
      if (event.key === "Escape") onClose();
    }
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [open, onClose]);

  if (!open) return null;

  const endpoint = deriveEndpoint(controlPlane);
  const grpcDisabled = controlPlane?.grpc_enabled === false;
  const isWebSocket = grpcDisabled || endpoint.startsWith("wss://") || endpoint.startsWith("ws://");
  const authEnabled = Boolean(controlPlane?.auth_enabled);
  const tokenLine = authEnabled ? 'export ANYSERVE_TOKEN="<worker-token>"\n' : "";
  const clientTokenLine = authEnabled ? 'export ANYSERVE_TOKEN="<client-token>"\n' : "";

  return (
    <div
      className="modal-backdrop"
      role="presentation"
      onClick={(e) => {
        if (e.target === e.currentTarget) onClose();
      }}
    >
      <section className="modal" role="dialog" aria-modal="true" aria-labelledby="connect-worker-title">
        <div className="modal__head">
          <div>
            <h2 id="connect-worker-title">Connect Worker</h2>
            <p className="modal__sub">
              Workers open outbound {isWebSocket ? "WebSocket" : "gRPC"} streams to <code className="mono">{endpoint}</code> (no inbound port required{grpcDisabled ? "; gRPC listener disabled" : ""}).
            </p>
          </div>
          <button type="button" className="icon-btn" aria-label="Close modal" onClick={onClose}>
            <IconClose />
          </button>
        </div>

        <div className="modal__meta">
          <span className="tag mono">Endpoint: {endpoint}</span>
          {grpcDisabled ? <span className="tag">HTTP/WS transport (gRPC disabled)</span> : null}
          <span className={`tag ${authEnabled ? "tag--warn" : ""}`}>
            {authEnabled ? "Worker token required (ANYSERVE_TOKEN)" : "Auth disabled (local mode)"}
          </span>
        </div>

        <div className="seg-control modal__tabs" role="tablist" aria-label="Worker integration methods">
          {TABS.map((item) => (
            <button
              key={item.id}
              type="button"
              role="tab"
              aria-selected={tab === item.id}
              className={`seg-control__btn${tab === item.id ? " is-active" : ""}`}
              onClick={() => setTab(item.id)}
            >
              {item.label}
            </button>
          ))}
        </div>

        <div className="modal__body">
          {tab === "python" ? (
            <>
              <CopyableBlock
                title="1. Install Python SDK (Python 3.10+)"
                code={`python3 -m venv .venv && . .venv/bin/activate\npip install anyserve-runtime`}
              />
              <CopyableBlock
                title="2. Run worker.py"
                code={`cat <<'PY' > worker.py\nfrom anyserve import serve, worker\n\n@worker(interface="demo.echo.v1", pool="default", max_active_leases=4)\ndef echo(payload: bytes) -> bytes:\n    return payload\n\nif __name__ == "__main__":\n    serve(echo, endpoint="${endpoint}")\nPY\n${tokenLine}ANYSERVE_ENDPOINT="${endpoint}" python worker.py`}
              />
            </>
          ) : null}

          {tab === "connectors" ? (
            <>
              <CopyableBlock
                title="1. OpenAI-compatible inference (vLLM, Ollama, llama.cpp, SGLang)"
                code={`git clone https://github.com/anyserve/anyserve.universe.git\ncd anyserve.universe\npip install anyserve-runtime\n${tokenLine}python openai/worker.py \\\n  --endpoint "${endpoint}" \\\n  --base-url "http://127.0.0.1:8000/v1" \\\n  --concurrency 16 \\\n  --attr site=edge`}
              />
              <CopyableBlock
                title="2. Triton Inference Server (or yolo/ and vla/)"
                code={`${tokenLine}python triton-inference-server/worker.py \\\n  --endpoint "${endpoint}" \\\n  --triton "http://127.0.0.1:8000" \\\n  --model minilm \\\n  --concurrency 16`}
              />
            </>
          ) : null}

          {tab === "rust" ? (
            <>
              <CopyableBlock
                title="1. Run the repository Rust worker example"
                code={`${tokenLine}cargo run --locked -p anyserve-demo -- \\\n  --mode worker \\\n  --endpoint "${endpoint}" \\\n  --worker-id rust-worker-01`}
              />
              <CopyableBlock
                title="2. Embed anyserve-client in Rust"
                code={`use anyserve_client::execution::WorkerSession;\n// Connect outbound session to "${endpoint}" and register\n// interfaces, attributes, pool, and capacity via WorkerSession::connect.`}
              />
            </>
          ) : null}

          {tab === "container" ? (
            <>
              <CopyableBlock
                title="1. Build & run Python worker container"
                code={`docker build -f examples/python/Dockerfile -t anyserve-worker:local .\ndocker run -d --name anyserve-worker --restart unless-stopped \\\n  --add-host host.docker.internal:host-gateway \\\n  -e ANYSERVE_ENDPOINT="${endpoint.replace("127.0.0.1", "host.docker.internal").replace("localhost", "host.docker.internal")}" \\\n  ${authEnabled ? '-e ANYSERVE_TOKEN="<worker-token>" \\\n  ' : ""}anyserve-worker:local`}
              />
              <CopyableBlock
                title="2. Helm worker fleet deployment"
                code={`helm install anyserve deploy/helm/anyserve \\\n  --set worker.enabled=true \\\n  --set worker.image=anyserve-worker:local${authEnabled ? " \\\n  --set auth.existingSecret=anyserve-tokens" : ""}`}
              />
            </>
          ) : null}

          {tab === "linux" ? (
            <>
              <CopyableBlock
                title="1. One-line install (macOS & Linux — x86_64 / arm64)"
                code={`curl -fsSL ${PROJECT_INSTALL_SH_URL} | sh\nanyserve --version`}
              />
              <CopyableBlock
                title="2. One-line install (Windows PowerShell — x86_64)"
                code={`irm ${PROJECT_INSTALL_PS1_URL} | iex\nanyserve --version`}
              />
              <CopyableBlock
                title="3. Pinned Linux x86_64 archive (direct download)"
                code={`curl -fsSL https://github.com/anyserve/anyserve/releases/download/v0.6.0/anyserve-v0.6.0-x86_64-unknown-linux-gnu.tar.gz \\\n  | tar -xz\nsudo install -m 0755 anyserve-v0.6.0-x86_64-unknown-linux-gnu/anyserve /usr/local/bin/anyserve\n${clientTokenLine}ANYSERVE_ENDPOINT="${endpoint}" anyserve job ls`}
              />
            </>
          ) : null}
        </div>
      </section>
    </div>
  );
}
