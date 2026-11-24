# anyserve-auth-github

Optional standalone GitHub OAuth login and self-service token portal plugin for public AnyServe VPS deployments.

## How it works

1. Users sign in with GitHub on `/tokens` (OAuth callback: `<ANYSERVE_AUTH_PUBLIC_URL>/auth/github/callback`).
2. Each GitHub user is mapped strictly to isolated namespace `gh-<github_id>` and can mint, inspect, and immediately revoke `client` and `worker` signed tokens (`asv1.*`, never operator-privileged).
3. `anyserve` validates signed tokens locally with `ANYSERVE_TOKEN_SIGNING_KEY` and polls `/internal/revocations` via `ANYSERVE_AUTH_REVOCATION_URL` for near-instant revocation without restarting or dropping active worker WebSockets.

## Docker Compose / CLI usage

See [deploy/vps/README.md](../../deploy/vps/README.md) for the opt-in `docker compose --profile auth-github` setup, or run standalone:

```bash
export ANYSERVE_TOKEN_SIGNING_KEY="$(openssl rand -hex 32)"
export GITHUB_CLIENT_ID="..."
export GITHUB_CLIENT_SECRET="..."
export ANYSERVE_AUTH_PUBLIC_URL="https://relay.example.com"

cargo run -p anyserve-auth-github -- \
  --grpc-endpoint https://relay.example.com \
  --ws-endpoint wss://relay.example.com \
  --console-url https://relay.example.com
```
