# Anyserve

Anyserve runs inference through workers on your own hosts and clusters. One
control-plane Runtime owns routing, admission, attempts, cancellation and bounded
output events; workers own their inference backends and connect outbound.

The control plane is a single process. Invoke handles ephemeral live requests;
Submit defaults to recoverable execution with complete input in an optional
SQLite journal. A short restart can recover durable jobs within their attempt
budget. Live output history and ephemeral jobs do not survive restart.

Start with [usage](usage.md), the [architecture](architecture.md), and
[configuration](configuration.md). Existing [Ollama](ollama.md) and
[edge](edge.md) walkthroughs use the same execution runtime. The
[acceptance records](redesign.md) describe what has actually been verified.
