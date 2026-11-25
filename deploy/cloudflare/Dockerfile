# syntax=docker/dockerfile:1.7
FROM rust:1.99.1-bookworm AS build
ARG DEBIAN_MIRROR=http://deb.debian.org
RUN sed -i "s|http://deb.debian.org|${DEBIAN_MIRROR}|g" /etc/apt/sources.list.d/debian.sources && \
    apt-get update && \
    apt-get install -y --no-install-recommends protobuf-compiler libprotobuf-dev && \
    rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked -p anyserve --bin anyserve && \
    cp target/release/anyserve /usr/local/bin/anyserve

FROM debian:bookworm-slim
ARG DEBIAN_MIRROR=http://deb.debian.org
RUN sed -i "s|http://deb.debian.org|${DEBIAN_MIRROR}|g" /etc/apt/sources.list.d/debian.sources && \
    apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*
COPY --from=build /usr/local/bin/anyserve /usr/local/bin/anyserve
# Root .dockerignore excludes `deploy/`, so embed the exact server.toml via BuildKit heredoc.
COPY <<'ANYSERVE_SERVER_TOML' /etc/anyserve/server.toml
[server]
grpc_enabled = false

[console]
enabled = true
listen = "0.0.0.0:3001"
public_endpoint = "wss://anyserve.example.com"

[auth]
client_tokens_env = "ANYSERVE_CLIENT_TOKENS"
worker_tokens_env = "ANYSERVE_WORKER_TOKENS"

[journal]
enabled = true
backend = "cloudflare"
endpoint = "http://journal.internal"
queue_capacity = 1024
max_batch = 64
retention_secs = 604800

[runtime]
max_pending_jobs = 2000
max_terminal_jobs = 1000
max_pending_bytes = 67108864
max_result_bytes = 67108864
max_event_bytes = 33554432
max_inline_object_bytes = 8388608
max_inline_total_bytes = 25165824
ANYSERVE_SERVER_TOML
EXPOSE 3001
CMD ["anyserve", "serve", "--config", "/etc/anyserve/server.toml"]
