# syntax=docker/dockerfile:1.7
FROM node:24-bookworm-slim AS console-build
WORKDIR /src/console
COPY console/package.json console/package-lock.json ./
RUN --mount=type=cache,target=/root/.npm \
    npm ci
COPY console/ ./
RUN npm run build

FROM rust:1.99.1-bookworm AS build
ARG DEBIAN_MIRROR=http://deb.debian.org
RUN sed -i "s|http://deb.debian.org|${DEBIAN_MIRROR}|g" /etc/apt/sources.list.d/debian.sources && \
    apt-get update && apt-get install -y --no-install-recommends protobuf-compiler libprotobuf-dev && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
COPY --from=console-build /src/console/dist /src/console/dist
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --locked --release -p anyserve --features embed-console && cp target/release/anyserve /usr/local/bin/anyserve

FROM debian:bookworm-slim
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /usr/local/bin/anyserve /usr/local/bin/anyserve
ENTRYPOINT ["anyserve"]
