use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use anyserve_client::{
    AnyserveClient, EventKind, FrameKind, FrameWrite, JobState, JobSubmission, StreamDirection,
    StreamOpen, StreamScope, WorkerRegistration,
};
use clap::{Parser, Subcommand, ValueEnum};
use hdrhistogram::Histogram;
use tokio::sync::Mutex;
use tokio::time::{Instant, sleep};
use uuid::Uuid;

#[derive(Parser, Debug)]
#[command(
    name = "anyserve-bench",
    about = "End-to-end gRPC benchmarks for anyserve"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    Run(RunArgs),
    Compare(CompareArgs),
}

#[derive(Clone, Debug, Parser)]
struct RunArgs {
    #[arg(long)]
    endpoint: String,
    #[arg(long)]
    label: String,
    #[arg(long, value_enum)]
    scenario: Scenario,
    #[arg(long, default_value_t = 15)]
    duration_secs: u64,
    #[arg(long, default_value_t = 3)]
    warmup_secs: u64,
    #[arg(long, default_value_t = 1)]
    concurrency: usize,
    #[arg(long, default_value_t = 1)]
    workers: usize,
    #[arg(long, default_value_t = 1024)]
    payload_bytes: usize,
    #[arg(long, default_value_t = 16)]
    frames_per_stream: usize,
    #[arg(long)]
    json_out: Option<PathBuf>,
}

#[derive(Debug, Parser)]
struct CompareArgs {
    #[arg(long)]
    baseline: PathBuf,
    #[arg(long)]
    candidate: PathBuf,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum Scenario {
    LeaseCycle,
    WatchLatency,
    StreamFollow,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct BenchmarkResult {
    label: String,
    scenario: String,
    endpoint: String,
    duration_secs: u64,
    total_ops: u64,
    success_ops: u64,
    error_ops: u64,
    throughput_ops_per_sec: f64,
    throughput_bytes_per_sec: f64,
    latency_ms: LatencySummary,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct LatencySummary {
    p50: f64,
    p95: f64,
    p99: f64,
    max: f64,
}

struct MetricsRecorder {
    success_ops: AtomicU64,
    error_ops: AtomicU64,
    bytes: AtomicU64,
    histogram: Mutex<Histogram<u64>>,
}

impl MetricsRecorder {
    fn new() -> Result<Self> {
        Ok(Self {
            success_ops: AtomicU64::new(0),
            error_ops: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            histogram: Mutex::new(
                Histogram::new_with_bounds(1, 60_000_000, 3).context("create latency histogram")?,
            ),
        })
    }

    async fn record_success(&self, latency: Duration, bytes: u64) -> Result<()> {
        self.success_ops.fetch_add(1, Ordering::Relaxed);
        self.bytes.fetch_add(bytes, Ordering::Relaxed);
        self.histogram
            .lock()
            .await
            .record(duration_to_micros(latency))
            .context("record latency")
    }

    fn record_error(&self) {
        self.error_ops.fetch_add(1, Ordering::Relaxed);
    }

    async fn finish(
        &self,
        label: String,
        scenario: Scenario,
        endpoint: String,
        duration_secs: u64,
    ) -> BenchmarkResult {
        let histogram = self.histogram.lock().await;
        let success_ops = self.success_ops.load(Ordering::Relaxed);
        let error_ops = self.error_ops.load(Ordering::Relaxed);
        let total_ops = success_ops + error_ops;
        let bytes = self.bytes.load(Ordering::Relaxed);
        let duration = duration_secs as f64;
        BenchmarkResult {
            label,
            scenario: scenario.as_str().to_string(),
            endpoint,
            duration_secs,
            total_ops,
            success_ops,
            error_ops,
            throughput_ops_per_sec: if duration > 0.0 {
                success_ops as f64 / duration
            } else {
                0.0
            },
            throughput_bytes_per_sec: if duration > 0.0 {
                bytes as f64 / duration
            } else {
                0.0
            },
            latency_ms: LatencySummary::from_histogram(&histogram),
        }
    }
}

impl LatencySummary {
    fn from_histogram(histogram: &Histogram<u64>) -> Self {
        if histogram.len() == 0 {
            return Self {
                p50: 0.0,
                p95: 0.0,
                p99: 0.0,
                max: 0.0,
            };
        }
        Self {
            p50: micros_to_ms(histogram.value_at_quantile(0.50)),
            p95: micros_to_ms(histogram.value_at_quantile(0.95)),
            p99: micros_to_ms(histogram.value_at_quantile(0.99)),
            max: micros_to_ms(histogram.max()),
        }
    }
}

struct BenchmarkWindow {
    warmup_deadline: Instant,
    stop_deadline: Instant,
}

impl BenchmarkWindow {
    fn new(warmup: Duration, duration: Duration) -> Self {
        let warmup_deadline = Instant::now() + warmup;
        Self {
            warmup_deadline,
            stop_deadline: warmup_deadline + duration,
        }
    }

    fn should_start_op(&self) -> bool {
        Instant::now() < self.stop_deadline
    }

    fn should_record(&self, started_at: Instant) -> bool {
        started_at >= self.warmup_deadline && started_at < self.stop_deadline
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Run(args) => run_command(args).await,
        Command::Compare(args) => compare_command(args),
    }
}

async fn run_command(args: RunArgs) -> Result<()> {
    let result = run_benchmark(args.clone()).await?;
    print_result(&result);
    if let Some(path) = args.json_out {
        fs::write(&path, serde_json::to_vec_pretty(&result)?)
            .with_context(|| format!("write benchmark json {}", path.display()))?;
    }
    Ok(())
}

fn compare_command(args: CompareArgs) -> Result<()> {
    let baseline: BenchmarkResult = serde_json::from_slice(
        &fs::read(&args.baseline).with_context(|| format!("read {}", args.baseline.display()))?,
    )
    .with_context(|| format!("parse {}", args.baseline.display()))?;
    let candidate: BenchmarkResult = serde_json::from_slice(
        &fs::read(&args.candidate).with_context(|| format!("read {}", args.candidate.display()))?,
    )
    .with_context(|| format!("parse {}", args.candidate.display()))?;

    println!("Scenario: {}", candidate.scenario);
    println!(
        "Throughput ops/s: {:.2} -> {:.2} ({:+.2}%)",
        baseline.throughput_ops_per_sec,
        candidate.throughput_ops_per_sec,
        percent_change(
            baseline.throughput_ops_per_sec,
            candidate.throughput_ops_per_sec
        )
    );
    println!(
        "Throughput bytes/s: {:.2} -> {:.2} ({:+.2}%)",
        baseline.throughput_bytes_per_sec,
        candidate.throughput_bytes_per_sec,
        percent_change(
            baseline.throughput_bytes_per_sec,
            candidate.throughput_bytes_per_sec
        )
    );
    println!(
        "Latency p50 ms: {:.2} -> {:.2} ({:+.2}%)",
        baseline.latency_ms.p50,
        candidate.latency_ms.p50,
        percent_change(baseline.latency_ms.p50, candidate.latency_ms.p50)
    );
    println!(
        "Latency p95 ms: {:.2} -> {:.2} ({:+.2}%)",
        baseline.latency_ms.p95,
        candidate.latency_ms.p95,
        percent_change(baseline.latency_ms.p95, candidate.latency_ms.p95)
    );
    println!(
        "Latency p99 ms: {:.2} -> {:.2} ({:+.2}%)",
        baseline.latency_ms.p99,
        candidate.latency_ms.p99,
        percent_change(baseline.latency_ms.p99, candidate.latency_ms.p99)
    );
    println!(
        "Errors: {} -> {} ({:+.2}%)",
        baseline.error_ops,
        candidate.error_ops,
        percent_change(baseline.error_ops as f64, candidate.error_ops as f64)
    );
    Ok(())
}

async fn run_benchmark(args: RunArgs) -> Result<BenchmarkResult> {
    let recorder = Arc::new(MetricsRecorder::new()?);
    let window = Arc::new(BenchmarkWindow::new(
        Duration::from_secs(args.warmup_secs),
        Duration::from_secs(args.duration_secs),
    ));

    match args.scenario {
        Scenario::LeaseCycle => run_lease_cycle(args, window, recorder).await,
        Scenario::WatchLatency => run_watch_latency(args, window, recorder).await,
        Scenario::StreamFollow => run_stream_follow(args, window, recorder).await,
    }
}

async fn run_lease_cycle(
    args: RunArgs,
    window: Arc<BenchmarkWindow>,
    recorder: Arc<MetricsRecorder>,
) -> Result<BenchmarkResult> {
    let interface_name = format!("bench.lease.{}", Uuid::new_v4().simple());
    let payload = vec![b'x'; args.payload_bytes];
    let stop_workers = Arc::new(AtomicBool::new(false));
    let worker_count = args.workers.max(1);
    let producer_count = args.concurrency.max(1);
    let mut worker_handles = Vec::with_capacity(worker_count);
    for index in 0..worker_count {
        let endpoint = args.endpoint.clone();
        let interface_name = interface_name.clone();
        let stop_workers = Arc::clone(&stop_workers);
        worker_handles.push(tokio::spawn(async move {
            lease_cycle_worker_loop(endpoint, interface_name, index, stop_workers).await
        }));
    }

    let mut producer_handles = Vec::with_capacity(producer_count);
    for index in 0..producer_count {
        let endpoint = args.endpoint.clone();
        let interface_name = interface_name.clone();
        let payload = payload.clone();
        let recorder = Arc::clone(&recorder);
        let window = Arc::clone(&window);
        producer_handles.push(tokio::spawn(async move {
            let mut client = AnyserveClient::connect(endpoint).await?;
            loop {
                if !window.should_start_op() {
                    break;
                }
                let started_at = Instant::now();
                let job_id = format!("bench-lease-{index}-{}", Uuid::new_v4().simple());
                let mut request = JobSubmission::new(interface_name.clone());
                request.job_id = Some(job_id);
                request.params = payload.clone();
                request.required_capacity.insert("slot".to_string(), 1);

                let outcome = async {
                    let job = client.submit_job(request).await?;
                    wait_for_terminal_job(&mut client, &job.job_id).await
                }
                .await;

                if window.should_record(started_at) {
                    match outcome {
                        Ok(()) => {
                            recorder
                                .record_success(started_at.elapsed(), args.payload_bytes as u64)
                                .await?;
                        }
                        Err(_) => recorder.record_error(),
                    }
                }
            }
            Ok::<(), anyhow::Error>(())
        }));
    }

    for handle in producer_handles {
        handle.await.context("join lease benchmark producer")??;
    }

    stop_workers.store(true, Ordering::Relaxed);
    for handle in worker_handles {
        handle.await.context("join lease benchmark worker")??;
    }

    Ok(recorder
        .finish(args.label, args.scenario, args.endpoint, args.duration_secs)
        .await)
}

async fn lease_cycle_worker_loop(
    endpoint: String,
    interface_name: String,
    worker_index: usize,
    stop: Arc<AtomicBool>,
) -> Result<()> {
    let mut client = AnyserveClient::connect(endpoint).await?;
    let mut registration = WorkerRegistration::new(vec![interface_name]);
    registration.worker_id = Some(format!(
        "bench-worker-{worker_index}-{}",
        Uuid::new_v4().simple()
    ));
    registration.total_capacity.insert("slot".to_string(), 1);
    let worker = client.register_worker(registration).await?;

    while !stop.load(Ordering::Relaxed) {
        match client.poll_lease(worker.worker_id.clone()).await {
            Ok(Some(lease)) => {
                client
                    .complete_lease(
                        worker.worker_id.clone(),
                        lease.lease.lease_id,
                        Vec::new(),
                        HashMap::new(),
                    )
                    .await?;
            }
            Ok(None) => sleep(Duration::from_millis(10)).await,
            Err(_) => sleep(Duration::from_millis(25)).await,
        }
    }

    Ok(())
}

async fn run_watch_latency(
    args: RunArgs,
    window: Arc<BenchmarkWindow>,
    recorder: Arc<MetricsRecorder>,
) -> Result<BenchmarkResult> {
    let task_count = args.concurrency.max(1);
    let payload = vec![b'x'; args.payload_bytes];
    let mut handles = Vec::with_capacity(task_count);

    for index in 0..task_count {
        let endpoint = args.endpoint.clone();
        let payload = payload.clone();
        let recorder = Arc::clone(&recorder);
        let window = Arc::clone(&window);
        handles.push(tokio::spawn(async move {
            let interface_name = format!("bench.watch.{index}.{}", Uuid::new_v4().simple());
            let mut submitter = AnyserveClient::connect(endpoint.clone()).await?;
            let mut worker_client = AnyserveClient::connect(endpoint.clone()).await?;
            let mut watcher = AnyserveClient::connect(endpoint).await?;

            let mut worker_registration = WorkerRegistration::new(vec![interface_name.clone()]);
            worker_registration.worker_id = Some(format!(
                "bench-watch-worker-{index}-{}",
                Uuid::new_v4().simple()
            ));
            worker_registration
                .total_capacity
                .insert("slot".to_string(), 1);
            let worker = worker_client.register_worker(worker_registration).await?;

            let mut request = JobSubmission::new(interface_name);
            request.job_id = Some(format!(
                "bench-watch-job-{index}-{}",
                Uuid::new_v4().simple()
            ));
            request.required_capacity.insert("slot".to_string(), 1);
            let job = submitter.submit_job(request).await?;
            let lease = poll_until_lease(&mut worker_client, worker.worker_id.clone()).await?;
            let mut cursor = 0u64;
            let mut stream = watcher.watch_job(job.job_id.clone(), 0).await?;

            while let Ok(Ok(Some(event))) =
                tokio::time::timeout(Duration::from_millis(25), stream.message()).await
            {
                cursor = event.sequence;
            }

            loop {
                if !window.should_start_op() {
                    break;
                }
                let started_at = Instant::now();
                let outcome = async {
                    worker_client
                        .report_event(
                            worker.worker_id.clone(),
                            lease.lease.lease_id.clone(),
                            EventKind::Progress,
                            payload.clone(),
                            HashMap::new(),
                        )
                        .await?;
                    loop {
                        let event = stream
                            .message()
                            .await
                            .context("receive watched job event")?
                            .ok_or_else(|| {
                                anyhow!("watch job stream ended before progress event")
                            })?;
                        if event.sequence > cursor {
                            cursor = event.sequence;
                            break;
                        }
                    }
                    Ok::<(), anyhow::Error>(())
                }
                .await;

                if window.should_record(started_at) {
                    match outcome {
                        Ok(()) => {
                            recorder
                                .record_success(started_at.elapsed(), args.payload_bytes as u64)
                                .await?;
                        }
                        Err(_) => recorder.record_error(),
                    }
                }
            }

            worker_client
                .complete_lease(
                    worker.worker_id,
                    lease.lease.lease_id,
                    Vec::new(),
                    HashMap::new(),
                )
                .await?;
            Ok::<(), anyhow::Error>(())
        }));
    }

    for handle in handles {
        handle.await.context("join watch benchmark worker")??;
    }

    Ok(recorder
        .finish(args.label, args.scenario, args.endpoint, args.duration_secs)
        .await)
}

async fn run_stream_follow(
    args: RunArgs,
    window: Arc<BenchmarkWindow>,
    recorder: Arc<MetricsRecorder>,
) -> Result<BenchmarkResult> {
    let task_count = args.concurrency.max(1);
    let payload = vec![b'x'; args.payload_bytes];
    let expected_bytes = (args.frames_per_stream * args.payload_bytes) as u64;
    let mut handles = Vec::with_capacity(task_count);

    for index in 0..task_count {
        let endpoint = args.endpoint.clone();
        let payload = payload.clone();
        let recorder = Arc::clone(&recorder);
        let window = Arc::clone(&window);
        handles.push(tokio::spawn(async move {
            let interface_name = format!("bench.stream.{index}.{}", Uuid::new_v4().simple());
            let mut setup = AnyserveClient::connect(endpoint.clone()).await?;
            let mut producer = AnyserveClient::connect(endpoint.clone()).await?;
            let mut consumer = AnyserveClient::connect(endpoint).await?;
            let job = setup
                .submit_job({
                    let mut request = JobSubmission::new(interface_name);
                    request.job_id = Some(format!(
                        "bench-stream-job-{index}-{}",
                        Uuid::new_v4().simple()
                    ));
                    request
                })
                .await?;

            loop {
                if !window.should_start_op() {
                    break;
                }
                let started_at = Instant::now();
                let stream = setup
                    .open_stream(
                        StreamOpen::job(
                            job.job_id.clone(),
                            format!("bench.output.{index}.{}", Uuid::new_v4().simple()),
                            StreamDirection::Bidirectional,
                        )
                        .with_scope(StreamScope::Job),
                    )
                    .await?;
                let outcome = async {
                    let mut follow = consumer
                        .pull_frames(stream.stream_id.clone(), 0, true)
                        .await?;
                    let mut writes = Vec::with_capacity(args.frames_per_stream + 1);
                    for _ in 0..args.frames_per_stream {
                        writes.push(FrameWrite::data(payload.clone()));
                    }
                    writes.push(FrameWrite::new(FrameKind::Close, Vec::new()));
                    producer
                        .push_frames(stream.stream_id.clone(), writes, None, None)
                        .await?;

                    let mut received_bytes = 0u64;
                    loop {
                        let frame = follow
                            .message()
                            .await
                            .context("receive followed frame")?
                            .ok_or_else(|| anyhow!("follow stream ended before close frame"))?;
                        match frame.kind() {
                            FrameKind::Data => received_bytes += frame.payload.len() as u64,
                            FrameKind::Close => break,
                            FrameKind::Error => bail!("received error frame during stream benchmark"),
                            FrameKind::Unspecified
                            | FrameKind::Open
                            | FrameKind::Checkpoint
                            | FrameKind::Control => {}
                        }
                    }
                    if received_bytes != expected_bytes {
                        bail!(
                            "expected {expected_bytes} bytes from followed stream, got {received_bytes}"
                        );
                    }
                    Ok::<(), anyhow::Error>(())
                }
                .await;

                if window.should_record(started_at) {
                    match outcome {
                        Ok(()) => {
                            recorder
                                .record_success(started_at.elapsed(), expected_bytes)
                                .await?;
                        }
                        Err(_) => recorder.record_error(),
                    }
                }
            }

            Ok::<(), anyhow::Error>(())
        }));
    }

    for handle in handles {
        handle.await.context("join stream benchmark worker")??;
    }

    Ok(recorder
        .finish(args.label, args.scenario, args.endpoint, args.duration_secs)
        .await)
}

async fn poll_until_lease(
    client: &mut AnyserveClient,
    worker_id: String,
) -> Result<anyserve_client::LeaseGrant> {
    loop {
        if let Some(lease) = client.poll_lease(worker_id.clone()).await? {
            return Ok(lease);
        }
        sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_for_terminal_job(client: &mut AnyserveClient, job_id: &str) -> Result<()> {
    let mut stream = client.watch_job(job_id.to_string(), 0).await?;
    while let Some(event) = stream
        .message()
        .await
        .context("receive watched job event")?
    {
        match event.kind() {
            EventKind::Succeeded | EventKind::Failed | EventKind::Cancelled => return Ok(()),
            _ => {}
        }
    }

    let job = client.get_job(job_id.to_string()).await?;
    if matches!(
        job.state(),
        JobState::Succeeded | JobState::Failed | JobState::Cancelled
    ) {
        Ok(())
    } else {
        bail!("job {job_id} did not reach a terminal state")
    }
}

fn print_result(result: &BenchmarkResult) {
    println!("Label: {}", result.label);
    println!("Scenario: {}", result.scenario);
    println!("Endpoint: {}", result.endpoint);
    println!("Duration: {}s", result.duration_secs);
    println!("Success ops: {}", result.success_ops);
    println!("Error ops: {}", result.error_ops);
    println!("Throughput ops/s: {:.2}", result.throughput_ops_per_sec);
    println!("Throughput bytes/s: {:.2}", result.throughput_bytes_per_sec);
    println!(
        "Latency ms: p50={:.2} p95={:.2} p99={:.2} max={:.2}",
        result.latency_ms.p50, result.latency_ms.p95, result.latency_ms.p99, result.latency_ms.max
    );
}

fn duration_to_micros(duration: Duration) -> u64 {
    duration.as_micros().try_into().unwrap_or(u64::MAX).max(1)
}

fn micros_to_ms(value: u64) -> f64 {
    value as f64 / 1_000.0
}

fn percent_change(baseline: f64, candidate: f64) -> f64 {
    if baseline == 0.0 {
        return if candidate == 0.0 { 0.0 } else { 100.0 };
    }
    ((candidate - baseline) / baseline) * 100.0
}

impl Scenario {
    fn as_str(self) -> &'static str {
        match self {
            Self::LeaseCycle => "lease-cycle",
            Self::WatchLatency => "watch-latency",
            Self::StreamFollow => "stream-follow",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{LatencySummary, percent_change};
    use hdrhistogram::Histogram;

    #[test]
    fn percent_change_handles_zero_baseline() {
        assert_eq!(percent_change(0.0, 0.0), 0.0);
        assert_eq!(percent_change(0.0, 42.0), 100.0);
    }

    #[test]
    fn latency_summary_reads_histogram_quantiles() {
        let mut histogram =
            Histogram::new_with_bounds(1, 10_000_000, 3).expect("histogram should build");
        histogram.record(1_000).unwrap();
        histogram.record(2_000).unwrap();
        histogram.record(3_000).unwrap();
        let summary = LatencySummary::from_histogram(&histogram);
        assert!(summary.p50 > 0.0);
        assert!(summary.p95 >= summary.p50);
    }
}
