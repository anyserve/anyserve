# Single-Domain VPS Relay (Docker Compose + Caddy)

Deploy an AnyServe control plane on any Linux VPS with automatic Let's Encrypt HTTPS, serving **native HTTP/2 gRPC**, **WebSocket fallback (`wss://`)**, and the **embedded Browser Console** on one domain (`https://relay.example.com`).

## What is included

- [docker-compose.yml](docker-compose.yml): `anyserve` (official image with embedded Console UI + persistent SQLite volume) behind `caddy:2-alpine` on ports `80` and `443` (`tcp` + `udp`).
- [Caddyfile](Caddyfile): terminates TLS automatically, proxies gRPC (`/anyserve.v2.*`, `/grpc.health.*`, `/grpc.reflection.*`) to `h2c://anyserve:50052`, proxies the Console UI + `/api/*` + `/rpc/v2/*` + `/ws/v2/*` + `/healthz` + `/readyz` to `anyserve:3001`, and blocks `/metrics` with `403`.
- [server.toml](server.toml): SQLite journal at `/data/journal.sqlite` (`24 h` retention), env-backed bearer auth, and bounded memory budgets for a 2 GiB VPS.

## Prerequisites

- Docker with Docker Compose (`docker compose`).
- A DNS `A` / `AAAA` record pointing your domain to the VPS, with inbound TCP `80`, TCP `443`, and optional UDP `443` open.

## Launch in 3 steps

### 1. Create `.env`

From `deploy/vps/`:

```bash
cp .env.example .env
```

Edit `.env` to set `ANYSERVE_DOMAIN`, `ANYSERVE_PUBLIC_ENDPOINT`, and separate client/worker tokens generated with `openssl rand -hex 24`:

```bash
cat > .env <<EOF
ANYSERVE_DOMAIN=relay.example.com
ANYSERVE_PUBLIC_ENDPOINT=https://relay.example.com
ANYSERVE_CLIENT_TOKENS=$(openssl rand -hex 24)
ANYSERVE_WORKER_TOKENS=$(openssl rand -hex 24)
EOF
chmod 600 .env
```

### 2. Start the relay

```bash
docker compose up -d
```

To build locally from this repository instead of pulling GHCR, uncomment `build: ../..` in [docker-compose.yml](docker-compose.yml) and run `docker compose up -d --build`.

Verify readiness:

```bash
curl -i https://relay.example.com/readyz
```

### 3. Connect workers, clients, and Console

- **Native HTTP/2 gRPC (recommended):**
  ```bash
  export ANYSERVE_ENDPOINT="https://relay.example.com"
  export ANYSERVE_TOKEN="<worker-or-client-token>"
  ```
- **WebSocket fallback (behind HTTP/1.1-only proxies):**
  ```bash
  export ANYSERVE_ENDPOINT="wss://relay.example.com"
  export ANYSERVE_TOKEN="<worker-or-client-token>"
  ```
- **Browser Console:** open `https://relay.example.com` and sign in with a token from `ANYSERVE_CLIENT_TOKENS`.

For backup, PostgreSQL HA, and operational notes, see [Deployment & Recovery](../../docs/deployment.md).
