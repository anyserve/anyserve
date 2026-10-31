# syntax=docker/dockerfile:1.7
# Build from the repository root with -f examples/python/Dockerfile.
FROM rust:1.99.1-bookworm AS build
ARG DEBIAN_MIRROR=http://deb.debian.org
RUN sed -i "s|http://deb.debian.org|${DEBIAN_MIRROR}|g" /etc/apt/sources.list.d/debian.sources && \
    apt-get update && apt-get install -y --no-install-recommends protobuf-compiler libprotobuf-dev && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    PYO3_NO_PYTHON=1 cargo build --locked --release -p anyserve-python && \
    cp target/release/lib_native.so /_native.abi3.so

FROM python:3.12-slim-bookworm
WORKDIR /app
COPY clients/python/python/anyserve/*.py /app/anyserve/
COPY --from=build /_native.abi3.so /app/anyserve/_native.abi3.so
COPY examples/python/worker.py /app/worker.py
ENTRYPOINT ["python", "/app/worker.py"]
