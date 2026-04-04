# Anyserve

Anyserve is a Rust-first control plane for generic distributed execution.

The core model is intentionally workload-neutral:

- jobs declare an `interface_name`
- jobs express `Demand` through attributes and capacity keys
- workers expose `Supply` through interfaces, attributes, and available capacity
- the kernel issues leases, collects events, and requeues expired work

The default runtime is zero-dependency:

- in-memory state store
- basic scheduler
- inline object payloads

Higher-level workload semantics can live above this core instead of being hard-coded into it.

The runtime surface is pure gRPC:

- control plane APIs
- worker APIs
- gRPC health service

Examples may still use endpoint strings such as `http://127.0.0.1:50052`. In this codebase that is a gRPC channel URI for the client transport, not a REST API base URL.
