# Anyserve

Serve Models Anywhere, Anytime, on Any Platform.

Anyserve is a workload-neutral control plane for serving models across your own
workers without rebuilding infrastructure for each workload.

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

## Production Surface

The kernel stays generic, but the production surface should feel concrete:

- create a production-grade inference endpoint in front of your own workers
- start with a single-region quickstart
- hide the scheduling internals behind an endpoint, API key, worker connect flow, and dashboard

See [Create Gateway](create-gateway.md) for the concrete production surface and dashboard shape.
