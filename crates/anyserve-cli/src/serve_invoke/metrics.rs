//! Minimal Prometheus exposition for the invoke gateway (no extra deps).

use std::collections::BTreeMap;
use std::fmt::Write;
use std::sync::Mutex;
use std::time::Duration;

use anyserve_core::kernel::Kernel;
use anyserve_core::now_ms;

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

    pub(super) async fn render(&self, kernel: &Kernel) -> String {
        let mut out = String::new();
        if let Ok(counts) = kernel.job_state_counts().await {
            out.push_str("# TYPE anyserve_jobs gauge\n");
            for (state, value) in [
                ("pending", counts.pending),
                ("leased", counts.leased),
                ("running", counts.running),
                ("succeeded", counts.succeeded),
                ("failed", counts.failed),
                ("cancelled", counts.cancelled),
            ] {
                let _ = writeln!(out, "anyserve_jobs{{state=\"{state}\"}} {value}");
            }
        }
        if let Ok(workers) = kernel.list_workers().await {
            let now = now_ms();
            let mut by_region: BTreeMap<String, (u64, u64)> = BTreeMap::new();
            for worker in workers.iter().filter(|worker| worker.expires_at_ms > now) {
                let region = worker
                    .spec
                    .attributes
                    .get("region")
                    .cloned()
                    .unwrap_or_default();
                let entry = by_region.entry(region).or_default();
                entry.0 += 1;
                entry.1 += u64::from(worker.status.active_leases);
            }
            out.push_str(
                "# TYPE anyserve_live_workers gauge\n# TYPE anyserve_active_leases gauge\n",
            );
            for (region, (live, leases)) in by_region {
                let region = escape(&region);
                let _ = writeln!(out, "anyserve_live_workers{{region=\"{region}\"}} {live}");
                let _ = writeln!(
                    out,
                    "anyserve_active_leases{{region=\"{region}\"}} {leases}"
                );
            }
        }
        let _ = writeln!(
            out,
            "# TYPE anyserve_waiting_poll_loops gauge\nanyserve_waiting_poll_loops {}",
            kernel.waiting_poll_loops()
        );

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
