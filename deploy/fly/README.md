# Fly.io Relay (Native gRPC + Persistent Volume + Auto-TLS)

Deploy a single always-on AnyServe control plane on Fly.io with an NVMe volume for SQLite durability, automatic TLS, **native HTTP/2 gRPC** on port `50052`, and **WebSocket fallback (`wss://`) + embedded Browser Console** on port `443`.

## Files

- [fly.toml](fly.toml): builds the root [Dockerfile](../../Dockerfile) (which embeds the Console UI), mounts the `anyserve_data` volume at `/data`, exposes `:443` (`http_service` -> `:3001`) and `:50052` (TLS + `h2_backend` -> `:50052`), and keeps 1 Machine running (`512mb`, shared CPU).
- [server.toml](server.toml): optional reference config tuned for a `512mb` VM if you bake a custom image with explicit runtime budgets.

> Run all `fly` commands below from the **repository root** so the Docker build context includes `crates/` and `console/`.

## Launch in 3 steps

### 1. Register the Fly app

```bash
fly launch --no-deploy -c deploy/fly/fly.toml
```

Keep 1 Machine (`min_machines_running = 1`, `auto_stop_machines = "off"`) when using the local SQLite journal.

### 2. Create the volume and set secrets

Replace `<app>` with your Fly app name (and `sin` with your `primary_region` if changed):

```bash
fly volumes create anyserve_data --region sin --size 1 -c deploy/fly/fly.toml

fly secrets set -c deploy/fly/fly.toml \
  ANYSERVE_CLIENT_TOKENS="$(openssl rand -hex 24)" \
  ANYSERVE_WORKER_TOKENS="$(openssl rand -hex 24)" \
  ANYSERVE_PUBLIC_ENDPOINT="https://<app>.fly.dev:50052"
```

`anyserve serve` automatically reads `ANYSERVE_CLIENT_TOKENS`, `ANYSERVE_WORKER_TOKENS`, and `ANYSERVE_PUBLIC_ENDPOINT` from the environment with no TOML file required, protecting gRPC, WebSocket, `/api/*`, and `/metrics`.

### 3. Deploy

```bash
fly deploy -c deploy/fly/fly.toml
```

## Connect

- **Native HTTP/2 gRPC (recommended):**
  ```bash
  export ANYSERVE_ENDPOINT="https://<app>.fly.dev:50052"
  export ANYSERVE_TOKEN="<worker-or-client-token>"
  ```
- **WebSocket fallback:**
  ```bash
  export ANYSERVE_ENDPOINT="wss://<app>.fly.dev"
  export ANYSERVE_TOKEN="<worker-or-client-token>"
  ```
- **Browser Console:** open `https://<app>.fly.dev` and sign in with a token from `ANYSERVE_CLIENT_TOKENS`.

## Optional: custom runtime budgets (`server.toml`)

If you want explicit 512 MiB runtime memory limits or per-interface queue policies from [server.toml](server.toml), add a 2-line Dockerfile alongside it in `deploy/fly/` (`FROM ghcr.io/anyserve/anyserve:v0.5.0` and `COPY server.toml /etc/anyserve/server.toml`), deploy from `deploy/fly/` (so the root `.dockerignore` does not exclude `deploy/`), and change `[processes].app` in [fly.toml](fly.toml) to:

```toml
[processes]
  app = "serve --config /etc/anyserve/server.toml"
```

For multi-replica failover with PostgreSQL, see [Deployment & Recovery](../../docs/deployment.md).
