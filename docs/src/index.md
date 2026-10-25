# Anyserve

Anyserve is a backend-independent execution and scheduling system. Clients submit
opaque inputs through the native protocol; one Runtime owns admission, placement,
attempts, flow control, cancellation and optional recovery. Application workers
connect outbound and own their backends.

The control plane is a single process. Invoke handles ephemeral live requests;
Submit defaults to recoverable execution with complete input in an optional
SQLite journal. A short restart recovers durable jobs within their attempt
budget. Live output history and ephemeral jobs do not survive a restart.

## Reading guide

| Goal | Read |
|---|---|
| Start a server and worker | [Usage](usage.md) |
| Understand jobs, placement, streams and persistence | [Execution architecture](architecture.md) |
| Choose a topology, restart or migrate | [Deployment and recovery](deployment.md), [workers across clusters](edge.md) |
| Set admission, memory and journal limits | [Configuration](configuration.md) |
| Validate a code change | [Testing](testing.md), [benchmark method](benchmark.md) |
