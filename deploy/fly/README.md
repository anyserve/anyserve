# Fly.io Relay

Single persistent Fly Machine with an NVMe volume (`/data/journal.sqlite`), automatic TLS, native HTTP/2 gRPC on `:50052`, and WebSocket (`wss://`) + embedded Console on `:443`.

Run all `fly` commands from the **repository root** so `Dockerfile` includes `crates/` and `console/`.

## Launch in 3 steps

```bash
# 1. Register app (keep min_machines_running = 1, auto_stop_machines = "off")
fly launch --no-deploy -c deploy/fly/fly.toml

# 2. Create volume and configure tokens
fly volumes create anyserve_data --region sin --size 1 -c deploy/fly/fly.toml
fly secrets set -c deploy/fly/fly.toml \
  ANYSERVE_CLIENT_TOKENS="$(openssl rand -hex 24)" \
  ANYSERVE_WORKER_TOKENS="$(openssl rand -hex 24)" \
  ANYSERVE_PUBLIC_ENDPOINT="https://<app>.fly.dev:50052"

# 3. Deploy
fly deploy -c deploy/fly/fly.toml
```

> **Non-root volume ownership:** The container runs as UID/GID `10001` (`anyserve`). If Fly mounts a newly created volume owned by `root`, fix `/data` ownership once (or via a one-off root console): `chown -R 10001:10001 /data`.

## Connect

- **Native gRPC (recommended)**: `ANYSERVE_ENDPOINT="https://<app>.fly.dev:50052"`
- **WebSocket fallback**: `ANYSERVE_ENDPOINT="wss://<app>.fly.dev"`
- **Browser Console**: open `https://<app>.fly.dev` with a token from `ANYSERVE_CLIENT_TOKENS`.

See [Deployment & Recovery](../../docs/deployment.md) for backup and PostgreSQL HA notes.
