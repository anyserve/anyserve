//! Prometheus snapshots of the shared Runtime, independent of client ingress.

use std::collections::BTreeMap;
use std::fmt::Write;

use anyserve_core::auth::AuthPolicy;
use anyserve_core::now_ms;
use anyserve_core::runtime::Runtime;
use anyserve_core::runtime::api::{InterfaceStats, PoolStats};

const MAX_METRIC_SERIES: usize = 100;
const OTHER_LABEL: &str = "_other";

type PoolKey<'a> = (&'a str, &'a str, &'a str);

pub(super) fn render(
    runtime: &Runtime,
    auth: &AuthPolicy,
    revocation_sync_ok: Option<bool>,
) -> String {
    let snapshot = runtime.snapshot();
    let health = snapshot.health;
    let stats = snapshot.stats;
    let config = runtime.config();
    let live_workers = snapshot.workers.healthy;
    let active_leases = snapshot.workers.active_leases;

    let mut out = String::with_capacity(2048);

    write_header(
        &mut out,
        "anyserve_runtime_ready",
        "gauge",
        "Whether the runtime accepts new work.",
    );
    let _ = writeln!(out, "anyserve_runtime_ready {}", u8::from(health.ready));

    write_header(
        &mut out,
        "anyserve_jobs",
        "gauge",
        "Jobs currently retained in memory, by state.",
    );
    for (state, count) in [
        ("queued", stats.queued),
        ("running", stats.running),
        ("succeeded", stats.succeeded),
        ("failed", stats.failed),
        ("cancelled", stats.cancelled),
    ] {
        let _ = writeln!(out, "anyserve_jobs{{state=\"{state}\"}} {count}");
    }

    write_header(
        &mut out,
        "anyserve_jobs_submitted_total",
        "counter",
        "Total jobs admitted since this Runtime started.",
    );
    let _ = writeln!(
        out,
        "anyserve_jobs_submitted_total {}",
        stats.jobs_submitted_total
    );

    write_header(
        &mut out,
        "anyserve_jobs_completed_total",
        "counter",
        "Total jobs completed since this Runtime started, by terminal state.",
    );
    for (state, count) in [
        ("succeeded", stats.jobs_succeeded_total),
        ("failed", stats.jobs_failed_total),
        ("cancelled", stats.jobs_cancelled_total),
    ] {
        let _ = writeln!(
            out,
            "anyserve_jobs_completed_total{{state=\"{state}\"}} {count}"
        );
    }

    write_header(
        &mut out,
        "anyserve_attempts_started_total",
        "counter",
        "Total execution attempts assigned since this Runtime started.",
    );
    let _ = writeln!(
        out,
        "anyserve_attempts_started_total {}",
        stats.attempts_started_total
    );

    write_header(
        &mut out,
        "anyserve_auth_rejections_total",
        "counter",
        "Total authentication/authorization rejections.",
    );
    let _ = writeln!(
        out,
        "anyserve_auth_rejections_total {}",
        auth.auth_rejections_total()
    );

    let sync_ok = revocation_sync_ok.or_else(|| auth.revocation_last_sync_at_ms().map(|_| true));
    if let Some(ok) = sync_ok {
        write_header(
            &mut out,
            "anyserve_auth_revocation_sync_ok",
            "gauge",
            "Whether the last auth revocation sync succeeded (1 = ok, 0 = failing or unsynced).",
        );
        let _ = writeln!(out, "anyserve_auth_revocation_sync_ok {}", u8::from(ok));
    }
    if let Some(last_sync_ms) = auth.revocation_last_sync_at_ms() {
        write_header(
            &mut out,
            "anyserve_auth_revocation_last_sync_age_seconds",
            "gauge",
            "Seconds since the last successful revocation sync.",
        );
        let age_secs = now_ms().saturating_sub(last_sync_ms) as f64 / 1000.0;
        let _ = writeln!(
            out,
            "anyserve_auth_revocation_last_sync_age_seconds {age_secs}"
        );
    }

    write_header(
        &mut out,
        "anyserve_live_workers",
        "gauge",
        "Connected, non-draining workers.",
    );
    let _ = writeln!(out, "anyserve_live_workers {live_workers}");

    write_header(
        &mut out,
        "anyserve_sessions",
        "gauge",
        "Retained worker sessions, including reconnect grace.",
    );
    let _ = writeln!(out, "anyserve_sessions {}", stats.sessions);

    write_header(
        &mut out,
        "anyserve_active_leases",
        "gauge",
        "Active worker leases, including reconnect grace.",
    );
    let _ = writeln!(out, "anyserve_active_leases {active_leases}");

    let budgets = [
        ("input", stats.input_bytes, config.max_pending_bytes),
        ("result", stats.result_bytes, config.max_result_bytes),
        ("event", stats.event_bytes, config.max_event_bytes),
    ];
    write_header(
        &mut out,
        "anyserve_budget_bytes",
        "gauge",
        "Bytes currently reserved in each shared Runtime budget.",
    );
    for (budget, used, _) in budgets {
        let _ = writeln!(out, "anyserve_budget_bytes{{budget=\"{budget}\"}} {used}");
    }
    write_header(
        &mut out,
        "anyserve_budget_limit_bytes",
        "gauge",
        "Configured byte limit for each shared Runtime budget.",
    );
    for (budget, _, limit) in budgets {
        let _ = writeln!(
            out,
            "anyserve_budget_limit_bytes{{budget=\"{budget}\"}} {limit}"
        );
    }

    if !stats.interfaces.is_empty() {
        write_interface_families(&mut out, &stats.interfaces);
    }
    if !stats.pools.is_empty() {
        write_pool_families(&mut out, &stats.pools);
    }
    if let Some(journal) = stats.journal {
        write_header(
            &mut out,
            "anyserve_journal_pending_ops",
            "gauge",
            "Outstanding journal operations.",
        );
        let _ = writeln!(out, "anyserve_journal_pending_ops {}", journal.pending_ops);

        write_header(
            &mut out,
            "anyserve_journal_committed_ops_total",
            "counter",
            "Journal operations committed since this Runtime started.",
        );
        let _ = writeln!(
            out,
            "anyserve_journal_committed_ops_total {}",
            journal.committed_ops
        );

        write_header(
            &mut out,
            "anyserve_journal_committed_batches_total",
            "counter",
            "Journal transaction batches committed since this Runtime started.",
        );
        let _ = writeln!(
            out,
            "anyserve_journal_committed_batches_total {}",
            journal.committed_batches
        );

        write_header(
            &mut out,
            "anyserve_journal_last_batch_size",
            "gauge",
            "Number of operations in the last committed journal batch.",
        );
        let _ = writeln!(
            out,
            "anyserve_journal_last_batch_size {}",
            journal.last_batch_size
        );

        write_header(
            &mut out,
            "anyserve_journal_last_commit_duration_seconds",
            "gauge",
            "Duration in seconds of the last committed journal batch.",
        );
        let _ = writeln!(
            out,
            "anyserve_journal_last_commit_duration_seconds {}",
            journal.last_commit_micros as f64 / 1_000_000.0
        );
    }
    out
}

fn write_header(out: &mut String, name: &str, kind: &str, help: &str) {
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} {kind}");
}

fn cap_interfaces(
    interfaces: &BTreeMap<String, InterfaceStats>,
) -> (Vec<(&str, &InterfaceStats)>, Option<InterfaceStats>) {
    if interfaces.len() <= MAX_METRIC_SERIES {
        return (
            interfaces.iter().map(|(k, v)| (k.as_str(), v)).collect(),
            None,
        );
    }
    let mut explicit = Vec::with_capacity(MAX_METRIC_SERIES);
    let mut other: Option<InterfaceStats> = None;
    for (name, current) in interfaces {
        if name != OTHER_LABEL && explicit.len() < MAX_METRIC_SERIES {
            explicit.push((name.as_str(), current));
        } else {
            let acc = other.get_or_insert_with(InterfaceStats::default);
            acc.queued += current.queued;
            acc.running += current.running;
            acc.sessions += current.sessions;
            acc.free_slots += current.free_slots;
            acc.oldest_queued_age_ms = acc.oldest_queued_age_ms.max(current.oldest_queued_age_ms);
        }
    }
    (explicit, other)
}

fn cap_pools(
    pools: &BTreeMap<(String, String, String), PoolStats>,
) -> (Vec<(PoolKey<'_>, &PoolStats)>, Option<PoolStats>) {
    if pools.len() <= MAX_METRIC_SERIES {
        return (
            pools
                .iter()
                .map(|((ns, iface, pool), v)| ((ns.as_str(), iface.as_str(), pool.as_str()), v))
                .collect(),
            None,
        );
    }
    let mut explicit = Vec::with_capacity(MAX_METRIC_SERIES);
    let mut other: Option<PoolStats> = None;
    for ((ns, iface, pool), current) in pools {
        let is_other = ns == OTHER_LABEL && iface == OTHER_LABEL && pool == OTHER_LABEL;
        if !is_other && explicit.len() < MAX_METRIC_SERIES {
            explicit.push(((ns.as_str(), iface.as_str(), pool.as_str()), current));
        } else {
            let acc = other.get_or_insert_with(PoolStats::default);
            acc.sessions += current.sessions;
            acc.free_slots += current.free_slots;
            acc.active_leases += current.active_leases;
            acc.desired_additional_slots += current.desired_additional_slots;
        }
    }
    (explicit, other)
}

fn write_interface_families(out: &mut String, interfaces: &BTreeMap<String, InterfaceStats>) {
    let (explicit, other) = cap_interfaces(interfaces);
    let mut entries: Vec<(String, &InterfaceStats)> =
        Vec::with_capacity(explicit.len() + usize::from(other.is_some()));
    for (name, current) in explicit {
        let mut escaped = String::with_capacity(name.len());
        write_escaped(&mut escaped, name);
        entries.push((escaped, current));
    }
    if let Some(ref other_stats) = other {
        entries.push((OTHER_LABEL.to_owned(), other_stats));
    }

    write_header(
        out,
        "anyserve_interface_jobs",
        "gauge",
        "Queued or running jobs retained per interface.",
    );
    for (iface, current) in &entries {
        for (state, count) in [("queued", current.queued), ("running", current.running)] {
            let _ = writeln!(
                out,
                "anyserve_interface_jobs{{interface=\"{iface}\",state=\"{state}\"}} {count}"
            );
        }
    }

    write_header(
        out,
        "anyserve_interface_oldest_queue_age_seconds",
        "gauge",
        "Age in seconds of the oldest queued job per interface.",
    );
    for (iface, current) in &entries {
        let _ = writeln!(
            out,
            "anyserve_interface_oldest_queue_age_seconds{{interface=\"{iface}\"}} {}",
            current.oldest_queued_age_ms as f64 / 1000.0
        );
    }

    write_header(
        out,
        "anyserve_interface_sessions",
        "gauge",
        "Retained sessions, including reconnect grace.",
    );
    for (iface, current) in &entries {
        let _ = writeln!(
            out,
            "anyserve_interface_sessions{{interface=\"{iface}\"}} {}",
            current.sessions
        );
    }

    write_header(
        out,
        "anyserve_interface_free_slots",
        "gauge",
        "Available execution slots per interface after credits and concurrency limits.",
    );
    for (iface, current) in &entries {
        let _ = writeln!(
            out,
            "anyserve_interface_free_slots{{interface=\"{iface}\"}} {}",
            current.free_slots
        );
    }
}

fn format_pool_labels(namespace: &str, interface: &str, pool: &str) -> String {
    let mut labels = String::with_capacity(namespace.len() + interface.len() + pool.len() + 36);
    labels.push_str("namespace=\"");
    write_escaped(&mut labels, namespace);
    labels.push_str("\",interface=\"");
    write_escaped(&mut labels, interface);
    labels.push_str("\",pool=\"");
    write_escaped(&mut labels, pool);
    labels.push('"');
    labels
}

fn write_pool_families(out: &mut String, pools: &BTreeMap<(String, String, String), PoolStats>) {
    let (explicit, other) = cap_pools(pools);
    let mut entries: Vec<(String, &PoolStats)> =
        Vec::with_capacity(explicit.len() + usize::from(other.is_some()));
    for ((ns, iface, pool), current) in explicit {
        entries.push((format_pool_labels(ns, iface, pool), current));
    }
    if let Some(ref other_stats) = other {
        entries.push((
            format_pool_labels(OTHER_LABEL, OTHER_LABEL, OTHER_LABEL),
            other_stats,
        ));
    }

    write_header(
        out,
        "anyserve_pool_sessions",
        "gauge",
        "Retained worker sessions per namespace, interface, and pool.",
    );
    for (labels, current) in &entries {
        let _ = writeln!(
            out,
            "anyserve_pool_sessions{{{labels}}} {}",
            current.sessions
        );
    }

    write_header(
        out,
        "anyserve_pool_free_slots",
        "gauge",
        "Available execution slots per namespace, interface, and pool.",
    );
    for (labels, current) in &entries {
        let _ = writeln!(
            out,
            "anyserve_pool_free_slots{{{labels}}} {}",
            current.free_slots
        );
    }

    write_header(
        out,
        "anyserve_pool_active_leases",
        "gauge",
        "Active execution leases per namespace, interface, and pool.",
    );
    for (labels, current) in &entries {
        let _ = writeln!(
            out,
            "anyserve_pool_active_leases{{{labels}}} {}",
            current.active_leases
        );
    }

    write_header(
        out,
        "anyserve_pool_desired_additional_slots",
        "gauge",
        "Additional worker slots needed for queued demand per pool.",
    );
    for (labels, current) in &entries {
        let _ = writeln!(
            out,
            "anyserve_pool_desired_additional_slots{{{labels}}} {}",
            current.desired_additional_slots
        );
    }
}

fn write_escaped(out: &mut String, value: &str) {
    let mut last = 0;
    for (idx, ch) in value.char_indices() {
        let replacement = match ch {
            '\\' => "\\\\",
            '\n' => "\\n",
            '"' => "\\\"",
            _ => continue,
        };
        out.push_str(&value[last..idx]);
        out.push_str(replacement);
        last = idx + ch.len_utf8();
    }
    out.push_str(&value[last..]);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cap_interfaces_avoids_other_collision_and_folds_overflow() {
        let mut map = BTreeMap::new();
        map.insert(
            OTHER_LABEL.to_owned(),
            InterfaceStats {
                queued: 7,
                running: 1,
                oldest_queued_age_ms: 500,
                sessions: 2,
                free_slots: 3,
            },
        );
        for i in 0..105 {
            map.insert(
                format!("iface.{i:03}"),
                InterfaceStats {
                    queued: 1,
                    running: 0,
                    oldest_queued_age_ms: i as u64,
                    sessions: 0,
                    free_slots: 0,
                },
            );
        }
        let mut out = String::new();
        write_interface_families(&mut out, &map);

        let other_queued_lines: Vec<&str> = out
            .lines()
            .filter(|l| {
                l.starts_with("anyserve_interface_jobs{interface=\"_other\",state=\"queued\"} ")
            })
            .collect();
        assert_eq!(
            other_queued_lines,
            vec!["anyserve_interface_jobs{interface=\"_other\",state=\"queued\"} 12"]
        );
        let series_count = out
            .lines()
            .filter(|l| l.starts_with("anyserve_interface_free_slots{"))
            .count();
        assert_eq!(series_count, MAX_METRIC_SERIES + 1);
    }

    #[test]
    fn cap_pools_avoids_other_collision_and_folds_overflow() {
        let mut map = BTreeMap::new();
        map.insert(
            (
                OTHER_LABEL.to_owned(),
                OTHER_LABEL.to_owned(),
                OTHER_LABEL.to_owned(),
            ),
            PoolStats {
                sessions: 4,
                free_slots: 5,
                active_leases: 2,
                desired_additional_slots: 1,
            },
        );
        for i in 0..103 {
            map.insert(
                ("default".into(), format!("iface.{i:03}"), "pool".into()),
                PoolStats {
                    sessions: 1,
                    free_slots: 1,
                    active_leases: 0,
                    desired_additional_slots: 1,
                },
            );
        }
        let mut out = String::new();
        write_pool_families(&mut out, &map);

        let other_lines: Vec<&str> = out
            .lines()
            .filter(|l| {
                l.starts_with(
                    "anyserve_pool_sessions{namespace=\"_other\",interface=\"_other\",pool=\"_other\"} ",
                )
            })
            .collect();
        assert_eq!(
            other_lines,
            vec![
                "anyserve_pool_sessions{namespace=\"_other\",interface=\"_other\",pool=\"_other\"} 7"
            ]
        );
        let series_count = out
            .lines()
            .filter(|l| l.starts_with("anyserve_pool_sessions{"))
            .count();
        assert_eq!(series_count, MAX_METRIC_SERIES + 1);
    }
}
