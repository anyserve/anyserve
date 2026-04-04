# Anyserve

Anyserve is a Rust-first inference relay.

It accepts inference requests over gRPC, stores request state in Redis, lets workers pull queued jobs, and streams worker responses back to the caller.

The repository is managed with `mise`, built as a Cargo workspace, and exports its Python SDK from Rust through `PyO3` and `maturin`.
