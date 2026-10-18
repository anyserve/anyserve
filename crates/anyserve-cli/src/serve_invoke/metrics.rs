//! Minimal Prometheus exposition for the invoke gateway (no extra deps).

use std::collections::BTreeMap;
use std::fmt::Write;
use std::sync::Mutex;
use std::time::Duration;

use anyserve_core::runtime::Runtime;

const BUCKETS_MS: &[f64] = &[
    5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 10000.0,
];

#[derive(Default)]
struct Histogram {
    buckets: Vec<u64>,
    count: u64,
    sum_ms: f64,
}

#[derive(Default)]
pub(super) struct InvokeMetrics {
    latency: Mutex<BTreeMap<(String, u16), Histogram>>,
}

impl InvokeMetrics {
    pub(super) fn observe(&self, interface: &str, status: u16, elapsed: Duration) {
        let ms = elapsed.as_secs_f64() * 1000.0;
        let mut latency = self.latency.lock().expect("metrics poisoned");
        let interface =
            if latency.len() >= 1024 && !latency.contains_key(&(interface.to_owned(), status)) {
                "__other__"
            } else {
                interface
            };
        let histogram = latency
            .entry((interface.to_string(), status))
            .or_insert_with(|| Histogram {
                buckets: vec![0; BUCKETS_MS.len()],
                ..Histogram::default()
            });
        for (bucket, bound) in histogram.buckets.iter_mut().zip(BUCKETS_MS) {
            if ms <= *bound {
                *bucket += 1;
            }
        }
        histogram.count += 1;
        histogram.sum_ms += ms;
    }

    pub(super) fn render(&self, runtime: &Runtime) -> String {
        let mut out = String::new();
        let stats = runtime.stats();
        let _ = writeln!(
            out,
            "# TYPE anyserve_jobs gauge\nanyserve_jobs{{state=\"queued\"}} {}\nanyserve_jobs{{state=\"running\"}} {}",
            stats.queued, stats.running
        );
        let _ = writeln!(
            out,
            "# TYPE anyserve_live_workers gauge\nanyserve_live_workers {}",
            runtime
                .list_workers()
                .iter()
                .filter(|worker| worker.connected && !worker.draining)
                .count()
        );
        if let Some(journal) = stats.journal {
            let _ = writeln!(
                out,
                "# TYPE anyserve_journal_pending_ops gauge\nanyserve_journal_pending_ops {}",
                journal.pending_ops
            );
            let _ = writeln!(
                out,
                "# TYPE anyserve_journal_committed_ops counter\nanyserve_journal_committed_ops {}",
                journal.committed_ops
            );
        }
        out.push_str("# TYPE anyserve_invoke_duration_ms histogram\n");
        let latency = self.latency.lock().expect("metrics poisoned");
        for ((interface, status), histogram) in latency.iter() {
            let labels = format!("interface=\"{}\",code=\"{status}\"", escape(interface));
            for (bound, count) in BUCKETS_MS.iter().zip(&histogram.buckets) {
                let _ = writeln!(
                    out,
                    "anyserve_invoke_duration_ms_bucket{{{labels},le=\"{bound}\"}} {count}"
                );
            }
            let _ = writeln!(
                out,
                "anyserve_invoke_duration_ms_bucket{{{labels},le=\"+Inf\"}} {}",
                histogram.count
            );
            let _ = writeln!(
                out,
                "anyserve_invoke_duration_ms_sum{{{labels}}} {}",
                histogram.sum_ms
            );
            let _ = writeln!(
                out,
                "anyserve_invoke_duration_ms_count{{{labels}}} {}",
                histogram.count
            );
        }
        out
    }
}

fn escape(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}
