# Anyserve

Anyserve runs inference through workers on your own hosts and clusters. One
control-plane Runtime owns routing, admission, attempts, cancellation and bounded
output events; workers own their inference backends and connect outbound.

The control plane is a single process. Invoke handles ephemeral live requests;
Submit defaults to recoverable execution with complete input in an optional
SQLite journal. A short restart can recover durable jobs within their attempt
budget. Live output history and ephemeral jobs do not survive restart.

## Reading guide

| Goal | Read | Status |
|---|---|---|
| Start a server and worker | [Usage](usage.md), [Ollama](ollama.md) | Current walkthroughs |
| Understand jobs, placement, streams and persistence | [Execution architecture](architecture.md) | Current 0.5.0 implementation; source baseline `aa94ad9` |
| Choose a topology, restart or migrate | [Deployment and recovery](deployment.md), [edge inference](edge.md) | Current operating boundaries |
| Set admission, memory and journal limits | [Configuration](configuration.md) | Current defaults and overflow behavior |
| Validate a code change | [Testing](testing.md), [benchmark method](benchmark.md) | Commands to run against the checkout being changed |
| Read measured control-plane costs | [Load benchmark results](benchmark-results.md) | Recorded on 2026-10-19; loopback and mock backends |
| Trace the redesign and its evidence | [0.5 acceptance record](redesign.md), [runtime references](reference-architectures.md), [routing references](inference-routing-research.md) | Historical records from 2026-10-18 |
| Explore a hosted product | [Hosted gateway proposal](create-gateway.md) | Product proposal; workspace provisioning and managed pools are not implemented here |

The architecture and configuration pages describe the running system. Historical
acceptance results apply to their recorded revision; rerun the testing commands
to establish the status of another checkout. Reference reviews explain design
inputs and do not expand the supported API or deployment topology.
