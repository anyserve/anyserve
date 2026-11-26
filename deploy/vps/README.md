# Single-Domain VPS Relay (Docker Compose + Caddy)

One domain (`https://relay.example.com`) with automatic Let's Encrypt HTTPS serving **native HTTP/2 gRPC**, **WebSocket (`wss://`)**, and the **embedded Browser Console**.

- [docker-compose.yml](docker-compose.yml): `anyserve` (official image with embedded Console + SQLite volume) + `caddy:2-alpine` on `80` and `443` (`tcp`/`udp`) + optional `auth-github` profile.
- [Caddyfile](Caddyfile): proxies `/anyserve.v2.*`, `/grpc.health.*`, `/grpc.reflection.*` to `h2c://anyserve:50052`, everything else to `anyserve:3001`, and returns `403` for `/metrics`.
- [Caddyfile.auth-github](Caddyfile.auth-github): additionally routes `/tokens` and `/auth/*` to `auth-github:3002` and blocks `/internal/*`.
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

## Optional: GitHub Login Self-Service Tokens (`anyserve-auth-github`)

Create a GitHub OAuth App with callback URL `https://relay.example.com/auth/github/callback`, add these to `deploy/vps/.env`, and start with `--profile auth-github`:

```bash
cat >> .env <<EOF
ANYSERVE_CADDYFILE=./Caddyfile.auth-github
ANYSERVE_CONSOLE_AUTH_PORTAL_URL=https://relay.example.com/tokens
ANYSERVE_TOKEN_SIGNING_KEY=$(openssl rand -hex 32)
ANYSERVE_AUTH_REVOCATION_URL=http://auth-github:3002/internal/revocations
GITHUB_CLIENT_ID=<your-client-id>
GITHUB_CLIENT_SECRET=<your-client-secret>
ANYSERVE_AUTH_MIN_ACCOUNT_AGE_DAYS=30
EOF
docker compose --profile auth-github up -d --build
```

Users visit `https://relay.example.com/tokens` to sign in with GitHub, mint/revoke `client` and `worker` tokens scoped strictly to their isolated `gh-<id>` namespace, and open the Console directly via **Open in Console**. Operator tokens in `ANYSERVE_CLIENT_TOKENS` retain full cluster access and `/metrics`.

A live reference deployment of this stack (with automated `main`-branch GHCR builds and SSH rollouts via [deploy-vps.yml](../../.github/workflows/deploy-vps.yml)) is running at [https://anyserve.arcosx.org](https://anyserve.arcosx.org) ([GitHub Login token portal](https://anyserve.arcosx.org/tokens)).

## Upgrading from older root-owned `/data` volumes

The `anyserve` container runs as non-root `anyserve` (`10001:10001`). Fresh volumes inherit `/data` ownership automatically; when upgrading an existing root-owned `anyserve_data` volume (or bind mount), update ownership once before starting:

```bash
docker compose run --rm --user 0 --entrypoint chown anyserve -R 10001:10001 /data
```

## Connect

- **Native gRPC (recommended)**: `ANYSERVE_ENDPOINT="https://relay.example.com"`
- **WebSocket fallback**: `ANYSERVE_ENDPOINT="wss://relay.example.com"`
- **Browser Console**: open `https://relay.example.com` with an operator or minted `client` token.

See [Deployment & Recovery](../../docs/deployment.md) for backup and PostgreSQL HA notes.
