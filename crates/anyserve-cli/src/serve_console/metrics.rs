//! Prometheus snapshots of the shared Runtime, independent of client ingress.

use std::fmt::Write;

use anyserve_core::runtime::Runtime;

pub(super) fn render(runtime: &Runtime) -> String {
    let stats = runtime.stats();
    let health = runtime.health();
    let config = runtime.config();
    let workers = runtime.list_workers();
    let mut out = String::new();
    out.push_str("# HELP anyserve_runtime_ready Whether the runtime accepts new work.\n# TYPE anyserve_runtime_ready gauge\n");
    let _ = writeln!(out, "anyserve_runtime_ready {}", u8::from(health.ready));
    out.push_str("# HELP anyserve_jobs Jobs currently retained in memory, by state.\n# TYPE anyserve_jobs gauge\n");
    for (state, count) in [
        ("queued", stats.queued),
        ("running", stats.running),
        ("succeeded", stats.succeeded),
        ("failed", stats.failed),
        ("cancelled", stats.cancelled),
    ] {
        let _ = writeln!(out, "anyserve_jobs{{state=\"{state}\"}} {count}");
    }
    out.push_str("# HELP anyserve_live_workers Connected, non-draining workers.\n# TYPE anyserve_live_workers gauge\n");
    let _ = writeln!(
        out,
        "anyserve_live_workers {}",
        workers
            .iter()
            .filter(|worker| worker.connected && !worker.draining)
            .count()
    );
    out.push_str("# HELP anyserve_sessions Retained worker sessions, including reconnect grace.\n# TYPE anyserve_sessions gauge\n");
    let _ = writeln!(out, "anyserve_sessions {}", stats.sessions);
    out.push_str("# HELP anyserve_active_leases Active worker leases, including reconnect grace.\n# TYPE anyserve_active_leases gauge\n");
    let _ = writeln!(
        out,
        "anyserve_active_leases {}",
        workers
            .iter()
            .map(|worker| u64::from(worker.active_leases))
            .sum::<u64>()
    );
    out.push_str("# HELP anyserve_budget_bytes Bytes currently reserved in each shared Runtime budget.\n# TYPE anyserve_budget_bytes gauge\n# HELP anyserve_budget_limit_bytes Configured byte limit for each shared Runtime budget.\n# TYPE anyserve_budget_limit_bytes gauge\n");
    for (budget, used, limit) in [
        ("input", stats.input_bytes, config.max_pending_bytes),
        ("result", stats.result_bytes, config.max_result_bytes),
        ("event", stats.event_bytes, config.max_event_bytes),
    ] {
        let _ = writeln!(out, "anyserve_budget_bytes{{budget=\"{budget}\"}} {used}");
        let _ = writeln!(
            out,
            "anyserve_budget_limit_bytes{{budget=\"{budget}\"}} {limit}"
        );
    }
    out.push_str("# HELP anyserve_interface_jobs Queued or running jobs retained per interface.\n# TYPE anyserve_interface_jobs gauge\n# HELP anyserve_interface_oldest_queue_age_seconds Age in seconds of the oldest queued job per interface.\n# TYPE anyserve_interface_oldest_queue_age_seconds gauge\n# HELP anyserve_interface_sessions Retained sessions, including reconnect grace.\n# TYPE anyserve_interface_sessions gauge\n# HELP anyserve_interface_free_slots Available execution slots per interface after credits and concurrency limits.\n# TYPE anyserve_interface_free_slots gauge\n");
    for (interface, current) in &stats.interfaces {
        let interface = escape(interface);
        for (state, count) in [("queued", current.queued), ("running", current.running)] {
            let _ = writeln!(
                out,
                "anyserve_interface_jobs{{interface=\"{interface}\",state=\"{state}\"}} {count}"
            );
        }
        let _ = writeln!(
            out,
            "anyserve_interface_oldest_queue_age_seconds{{interface=\"{interface}\"}} {}",
            current.oldest_queued_age_ms as f64 / 1000.0
        );
        let _ = writeln!(
            out,
            "anyserve_interface_sessions{{interface=\"{interface}\"}} {}",
            current.sessions
        );
        let _ = writeln!(
            out,
            "anyserve_interface_free_slots{{interface=\"{interface}\"}} {}",
            current.free_slots
        );
    }
    if let Some(journal) = stats.journal {
        out.push_str("# HELP anyserve_journal_pending_ops Outstanding journal operations.\n# TYPE anyserve_journal_pending_ops gauge\n# HELP anyserve_journal_committed_ops_total Journal operations committed since this Runtime started.\n# TYPE anyserve_journal_committed_ops_total counter\n# HELP anyserve_journal_committed_batches_total Journal transaction batches committed since this Runtime started.\n# TYPE anyserve_journal_committed_batches_total counter\n# HELP anyserve_journal_last_batch_size Number of operations in the last committed journal batch.\n# TYPE anyserve_journal_last_batch_size gauge\n# HELP anyserve_journal_last_commit_duration_seconds Duration in seconds of the last committed journal batch.\n# TYPE anyserve_journal_last_commit_duration_seconds gauge\n");
        let _ = writeln!(out, "anyserve_journal_pending_ops {}", journal.pending_ops);
        let _ = writeln!(
            out,
            "anyserve_journal_committed_ops_total {}",
            journal.committed_ops
        );
        let _ = writeln!(
            out,
            "anyserve_journal_committed_batches_total {}",
            journal.committed_batches
        );
        let _ = writeln!(
            out,
            "anyserve_journal_last_batch_size {}",
            journal.last_batch_size
        );
        let _ = writeln!(
            out,
            "anyserve_journal_last_commit_duration_seconds {}",
            journal.last_commit_micros as f64 / 1_000_000.0
        );
    }
    out
}

fn escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\n', "\\n")
        .replace('"', "\\\"")
}
