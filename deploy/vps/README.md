# Single-Domain VPS Relay (Docker Compose + Caddy)

One domain (`https://relay.example.com`) with automatic Let's Encrypt HTTPS serving **native HTTP/2 gRPC**, **WebSocket (`wss://`)**, and the **embedded Browser Console**.

- [docker-compose.yml](docker-compose.yml): `anyserve` (official image with embedded Console + SQLite volume) + `caddy:2-alpine` on `80` and `443` (`tcp`/`udp`).
- [Caddyfile](Caddyfile): proxies `/anyserve.v2.*`, `/grpc.health.*`, `/grpc.reflection.*` to `h2c://anyserve:50052`, everything else to `anyserve:3001`, and returns `403` for `/metrics`.
- [server.toml](server.toml): SQLite `/data/journal.sqlite` (`24h` retention), env bearer auth, and 2 GiB VPS memory budgets.

**Prerequisites:** Docker Compose + DNS `A`/`AAAA` pointing to the VPS (ports `80`/`443` open).

## Launch in 3 steps

```bash
# 1. Configure .env inside deploy/vps/
cd deploy/vps
cat > .env <<EOF
ANYSERVE_DOMAIN=relay.example.com
ANYSERVE_PUBLIC_ENDPOINT=https://relay.example.com
ANYSERVE_CLIENT_TOKENS=$(openssl rand -hex 24)
ANYSERVE_WORKER_TOKENS=$(openssl rand -hex 24)
EOF
chmod 600 .env

# 2. Start and verify readiness (uncomment `build: ../..` in docker-compose.yml to build from source)
docker compose up -d
curl -i https://relay.example.com/readyz
```

## Connect

- **Native gRPC (recommended)**: `ANYSERVE_ENDPOINT="https://relay.example.com"`
- **WebSocket fallback**: `ANYSERVE_ENDPOINT="wss://relay.example.com"`
- **Browser Console**: open `https://relay.example.com` with a token from `ANYSERVE_CLIENT_TOKENS`.

See [Deployment & Recovery](../../docs/deployment.md) for backup and PostgreSQL HA notes.
