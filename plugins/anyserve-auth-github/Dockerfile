# syntax=docker/dockerfile:1.7
FROM rust:1.99.1-bookworm AS build
ARG DEBIAN_MIRROR=http://deb.debian.org
RUN sed -i "s|http://deb.debian.org|${DEBIAN_MIRROR}|g" /etc/apt/sources.list.d/debian.sources && \
    apt-get update && apt-get install -y --no-install-recommends protobuf-compiler libprotobuf-dev && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --locked --release -p anyserve-auth-github && cp target/release/anyserve-auth-github /usr/local/bin/anyserve-auth-github

FROM debian:bookworm-slim
RUN useradd --system --uid 10001 --create-home --shell /usr/sbin/nologin anyserve && \
    mkdir -p /data && chown 10001:10001 /data
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /usr/local/bin/anyserve-auth-github /usr/local/bin/anyserve-auth-github
USER anyserve
ENTRYPOINT ["anyserve-auth-github"]
