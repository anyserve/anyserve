use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyserve_client::execution::{ExecutionClient, v2};
use serde_json::json;
use tokio::sync::Barrier;
use tokio::task::JoinSet;
use tokio::time::Instant;

use crate::{ClientArgs, Result, client};

const SUBMIT_TIMEOUT: Duration = Duration::from_secs(30);
const TICK: Duration = Duration::from_millis(10);

#[derive(Default)]
struct Inflight {
    active: AtomicU32,
    peak: AtomicU32,
}

#[derive(Default)]
struct RateCounts {
    scheduled: u64,
    offered: u64,
    sent: u64,
    skipped_busy: u64,
    skipped_expired: u64,
    skipped_max_jobs: u64,
}

impl RateCounts {
    fn json(&self) -> serde_json::Value {
        json!({"scheduled": self.scheduled, "offered": self.offered, "sent": self.sent,
            "skipped": self.scheduled - self.sent, "skipped_busy": self.skipped_busy,
            "skipped_expired": self.skipped_expired, "skipped_max_jobs": self.skipped_max_jobs})
    }
}

type Group = (ExecutionClient, Vec<Attempt>);

enum Skipped {
    Expired,
    MaxJobs,
}

fn quota(rate: u32, elapsed: Duration) -> u64 {
    ((u128::from(rate) * elapsed.as_nanos()) / 1_000_000_000).min(u64::MAX as u128) as u64
}

fn paced_tick(
    rate: u32,
    begin: Instant,
    finish: Instant,
    now: Instant,
    counts: &mut RateCounts,
) -> (Instant, u64) {
    let elapsed = now.duration_since(begin);
    let offset = elapsed - Duration::from_nanos((elapsed.as_nanos() % TICK.as_nanos()) as u64);
    let next_tick = (begin + offset + TICK).min(finish);
    let expired = quota(rate, offset).saturating_sub(counts.scheduled);
    counts.scheduled += expired;
    counts.skipped_expired += expired;
    let allowance = quota(rate, next_tick.duration_since(begin)) - counts.scheduled;
    counts.scheduled += allowance;
    (next_tick, allowance)
}

fn collect_paced(
    (group, phase, skipped): (Group, usize, Option<Skipped>),
    idle: &mut Vec<Group>,
    counts: &mut [RateCounts; 2],
) {
    match skipped {
        Some(Skipped::Expired) => counts[phase].skipped_expired += 1,
        Some(Skipped::MaxJobs) => counts[phase].skipped_max_jobs += 1,
        None => counts[phase].sent += 1,
    }
    idle.push(group);
}

async fn submit_once(
    connection: &mut ExecutionClient,
    job_id: String,
    spec: v2::JobSpec,
    sent: Instant,
    measured: bool,
    inflight: &Inflight,
) -> Attempt {
    let active = inflight.active.fetch_add(1, Ordering::Relaxed) + 1;
    inflight.peak.fetch_max(active, Ordering::Relaxed);
    let response = tokio::time::timeout(
        SUBMIT_TIMEOUT,
        connection.submit(Some(job_id.clone()), spec),
    )
    .await;
    inflight.active.fetch_sub(1, Ordering::Relaxed);
    let (submit_status, error, code) = match response {
        Ok(Ok(response)) => match check_ack(&job_id, &response) {
            Ok(()) => ("accepted", None, None),
            Err(error) => ("invalid_ack", Some(error), Some("InvalidAck".into())),
        },
        Ok(Err(error)) => (
            "rpc_error",
            Some(error.to_string()),
            Some(format!("{:?}", error.code())),
        ),
        Err(error) => (
            "timeout",
            Some(error.to_string()),
            Some("DeadlineExceeded".into()),
        ),
    };
    Attempt {
        job_id,
        sent,
        measured,
        latency_ms: sent.elapsed().as_secs_f64() * 1000.0,
        submit_status,
        error,
        code,
        drain_status: "timeout",
        drain_error: None,
        committed_without_ack: false,
    }
}

struct Pace {
    rate: u32,
    max_jobs: u32,
    job_timeout_ms: u64,
    prefix: String,
    windows: [(Instant, Instant); 2],
}

impl Pace {
    async fn run(
        self,
        clients: Vec<ExecutionClient>,
        payload: Arc<Vec<u8>>,
        inflight: Arc<Inflight>,
    ) -> Result<(Vec<Group>, [RateCounts; 2])> {
        let mut idle: Vec<Group> = clients
            .into_iter()
            .map(|connection| (connection, Vec::new()))
            .collect();
        let mut tasks = JoinSet::new();
        let next = Arc::new(AtomicU32::new(0));
        let prefix = Arc::new(self.prefix);
        let mut counts = [RateCounts::default(), RateCounts::default()];
        for (phase, (begin, finish)) in self.windows.into_iter().enumerate() {
            let mut next_tick = begin;
            while Instant::now() < finish && next.load(Ordering::Relaxed) < self.max_jobs {
                while let Some(completed) = tasks.try_join_next() {
                    collect_paced(completed?, &mut idle, &mut counts);
                }
                let now = Instant::now();
                if now >= finish {
                    break;
                }
                if now >= next_tick {
                    let (until, allowance) =
                        paced_tick(self.rate, begin, finish, now, &mut counts[phase]);
                    next_tick = until;
                    let offered = allowance.min(idle.len() as u64);
                    counts[phase].offered += offered;
                    counts[phase].skipped_busy += allowance - offered;
                    for _ in 0..offered {
                        let (mut connection, mut attempts) = idle.pop().expect("idle slot");
                        let payload = payload.clone();
                        let prefix = prefix.clone();
                        let next = next.clone();
                        let inflight = inflight.clone();
                        tasks.spawn(async move {
                            let mut spec = client::job_spec(3, &payload);
                            spec.policy
                                .as_mut()
                                .expect("benchmark policy")
                                .total_timeout_ms = self.job_timeout_ms;
                            let sent = Instant::now();
                            if sent >= next_tick {
                                return ((connection, attempts), phase, Some(Skipped::Expired));
                            }
                            #[allow(deprecated, reason = "AtomicU32::try_update is still unstable")]
                            let Ok(index) =
                                next.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |index| {
                                    (index < self.max_jobs).then(|| index + 1)
                                })
                            else {
                                return ((connection, attempts), phase, Some(Skipped::MaxJobs));
                            };
                            let id = format!("{prefix}-{index:010}");
                            attempts.push(
                                submit_once(&mut connection, id, spec, sent, phase == 1, &inflight)
                                    .await,
                            );
                            ((connection, attempts), phase, None)
                        });
                    }
                }
                tokio::select! {
                    completed = tasks.join_next(), if !tasks.is_empty() => {
                        collect_paced(completed.expect("pending task")?, &mut idle, &mut counts);
                    }
                    _ = tokio::time::sleep_until(next_tick) => {}
                }
            }
            if next.load(Ordering::Relaxed) >= self.max_jobs {
                break;
            }
            let expired = quota(self.rate, finish.duration_since(begin)) - counts[phase].scheduled;
            counts[phase].scheduled += expired;
            counts[phase].skipped_expired += expired;
        }
        while let Some(completed) = tasks.join_next().await {
            collect_paced(completed?, &mut idle, &mut counts);
        }
        Ok((idle, counts))
    }
}

struct Attempt {
    job_id: String,
    sent: Instant,
    measured: bool,
    latency_ms: f64,
    submit_status: &'static str,
    error: Option<String>,
    code: Option<String>,
    drain_status: &'static str,
    drain_error: Option<String>,
    committed_without_ack: bool,
}

fn check_ack(id: &str, response: &v2::SubmitResponse) -> std::result::Result<(), String> {
    let job = response.job.as_ref().ok_or("Submit omitted job")?;
    if job.job_id != id {
        return Err("Submit returned a different job_id".into());
    }
    if job.durability != v2::Durability::Durable as i32 {
        return Err("Submit did not acknowledge durable acceptance".into());
    }
    if !response.created {
        return Err("Submit returned an existing job for a unique job_id".into());
    }
    Ok(())
}

pub(super) async fn run(
    args: ClientArgs,
    clients: Vec<ExecutionClient>,
    payload: Arc<Vec<u8>>,
) -> Result {
    if args.max_jobs == 0 || args.drain_timeout.is_zero() || args.submit_rate == Some(0) {
        return Err("S6 requires positive max_jobs and drain_timeout".into());
    }
    let job_timeout_ms = args
        .warmup
        .saturating_add(args.duration)
        .saturating_add(SUBMIT_TIMEOUT)
        .saturating_add(args.drain_timeout)
        .saturating_add(Duration::from_secs(5))
        .as_millis()
        .min(u64::MAX as u128) as u64;
    let wall_start = SystemTime::now().duration_since(UNIX_EPOCH)?;
    let prefix = format!(
        "bench-s6-{:x}-{:x}",
        wall_start.as_nanos(),
        std::process::id()
    );
    let start = Instant::now();
    let measure_start = start + args.warmup;
    let end = measure_start + args.duration;
    let inflight = Arc::new(Inflight::default());
    let mut tasks = JoinSet::new();
    let (groups, rate_counts) = if let Some(rate) = args.submit_rate {
        let pace = Pace {
            rate,
            max_jobs: args.max_jobs,
            job_timeout_ms,
            prefix: prefix.clone(),
            windows: [(start, measure_start), (measure_start, end)],
        };
        let (groups, counts) = pace.run(clients, payload.clone(), inflight.clone()).await?;
        (groups, Some(counts))
    } else {
        let next = Arc::new(AtomicU32::new(0));
        let barrier = Arc::new(Barrier::new(clients.len() + 1));
        for mut connection in clients {
            let payload = payload.clone();
            let next = next.clone();
            let barrier = barrier.clone();
            let prefix = prefix.clone();
            let inflight = inflight.clone();
            tasks.spawn(async move {
                let mut attempts = Vec::new();
                barrier.wait().await;
                loop {
                    if Instant::now() >= end || next.load(Ordering::Relaxed) >= args.max_jobs {
                        break;
                    }
                    let mut spec = client::job_spec(3, &payload);
                    spec.policy
                        .as_mut()
                        .expect("benchmark policy")
                        .total_timeout_ms = job_timeout_ms;
                    #[allow(deprecated, reason = "AtomicU32::try_update is still unstable")]
                    let Ok(index) =
                        next.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |index| {
                            (index < args.max_jobs).then(|| index + 1)
                        })
                    else {
                        break;
                    };
                    let job_id = format!("{prefix}-{index:010}");
                    let sent = Instant::now();
                    if sent >= end {
                        break;
                    }
                    attempts.push(
                        submit_once(
                            &mut connection,
                            job_id,
                            spec,
                            sent,
                            sent >= measure_start,
                            &inflight,
                        )
                        .await,
                    );
                }
                (connection, attempts)
            });
        }
        barrier.wait().await;
        let mut groups = Vec::new();
        while let Some(completed) = tasks.join_next().await {
            groups.push(completed?);
        }
        (groups, None)
    };
    // Freeze the Submit denominator before any result reads or verification.
    let submitted = Instant::now();
    let measured_start = measure_start.min(submitted);
    let measured_seconds = submitted.duration_since(measured_start).as_secs_f64();
    let capped = groups.iter().map(|(_, rows)| rows.len()).sum::<usize>() == args.max_jobs as usize;
    let stopped = if capped {
        groups
            .iter()
            .flat_map(|(_, rows)| rows)
            .map(|row| row.sent)
            .max()
            .unwrap_or(start)
    } else {
        end.min(submitted)
    };
    let deadline = submitted + args.drain_timeout;
    for (mut connection, mut attempts) in groups {
        let payload = payload.clone();
        tasks.spawn(async move {
            // Verify acknowledged work before spending time on ambiguous errors.
            attempts.sort_by_key(|row| row.submit_status != "accepted");
            for attempt in &mut attempts {
                if Instant::now() >= deadline {
                    attempt.drain_error = Some("shared drain deadline exceeded".into());
                    continue;
                }
                let verify = async {
                    if attempt.submit_status != "accepted" {
                        let job = connection.get(&attempt.job_id, false).await?;
                        if job.job_id != attempt.job_id
                            || job.durability != v2::Durability::Durable as i32
                        {
                            return Err("Get returned a different or non-durable job".into());
                        }
                        attempt.committed_without_ack = true;
                    }
                    let events = connection.watch(&attempt.job_id, 0).await?;
                    let id = client::verify(events, false, &payload).await?;
                    if id != attempt.job_id {
                        return Err("Watch returned a different job_id".into());
                    }
                    Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
                };
                match tokio::time::timeout_at(deadline, verify).await {
                    Ok(Ok(())) => attempt.drain_status = "verified",
                    Ok(Err(error)) => {
                        attempt.drain_status = if error
                            .downcast_ref::<tonic::Status>()
                            .is_some_and(|status| status.code() == tonic::Code::NotFound)
                        {
                            "not_found"
                        } else {
                            "error"
                        };
                        attempt.drain_error = Some(error.to_string());
                    }
                    Err(_) => attempt.drain_error = Some("shared drain deadline exceeded".into()),
                }
            }
            (connection, attempts)
        });
    }
    let mut attempts = Vec::new();
    while let Some(completed) = tasks.join_next().await {
        attempts.extend(completed?.1);
    }
    let finished = Instant::now();
    attempts.sort_by(|a, b| a.job_id.cmp(&b.job_id));
    let mut latencies: Vec<_> = attempts
        .iter()
        .filter(|row| row.measured && row.submit_status == "accepted")
        .map(|row| row.latency_ms)
        .collect();
    latencies.sort_by(f64::total_cmp);
    let percentile = |q: f64| {
        latencies
            .get(((q * latencies.len() as f64).ceil() as usize).saturating_sub(1))
            .copied()
    };
    let errors = attempts
        .iter()
        .filter(|row| row.measured && row.error.is_some())
        .count();
    let warmup_errors = attempts
        .iter()
        .filter(|row| !row.measured && row.error.is_some())
        .count();
    let submit_acks = attempts
        .iter()
        .filter(|row| row.submit_status == "accepted")
        .count();
    let mut errors_by_code = BTreeMap::<&str, usize>::new();
    for code in attempts.iter().filter_map(|row| row.code.as_deref()) {
        *errors_by_code.entry(code).or_default() += 1;
    }
    let unresolved: Vec<_> = attempts
        .iter()
        .filter(|row| row.drain_status != "verified")
        .map(|row| row.job_id.as_str())
        .collect();
    let success =
        errors == 0 && warmup_errors == 0 && !latencies.is_empty() && unresolved.is_empty();
    let first_error = attempts
        .iter()
        .find_map(|row| row.error.as_deref().or(row.drain_error.as_deref()));
    let unix = |at: Instant| wall_start.as_secs_f64() + at.duration_since(start).as_secs_f64();
    let mut report = json!({
        "scenario": "S6", "name": "submit-acceptance", "success": success,
        "concurrency": args.concurrency, "drain_concurrency": args.concurrency,
        "observed_peak_inflight": inflight.peak.load(Ordering::Relaxed),
        "connections": args.connections.unwrap_or(args.concurrency.min(8)),
        "grpc_channels": args.connections.unwrap_or(args.concurrency.min(8)),
        "payload_bytes": args.payload, "payload_pattern": args.payload_pattern.name(),
        "payload_seed": args.payload_seed, "payload_generator": args.payload_pattern.algorithm(),
        "warmup_seconds": args.warmup.as_secs_f64(), "target_duration_seconds": args.duration.as_secs_f64(),
        "actual_duration_seconds": measured_seconds, "max_jobs": args.max_jobs,
        "job_total_timeout_ms": job_timeout_ms,
        "stop_reason": if capped { "max_jobs" } else { "duration" },
        "submission_started_unix_s": unix(start), "submission_stopped_unix_s": unix(stopped),
        "submission_finished_unix_s": unix(submitted),
        "measurement_started_unix_s": unix(measured_start), "measurement_finished_unix_s": unix(submitted),
    });
    let serde_json::Value::Object(results) = json!({
        "requests": latencies.len(), "errors": errors, "warmup_errors": warmup_errors, "first_error": first_error,
        "submit_ack_rps": if measured_seconds > 0.0 { latencies.len() as f64 / measured_seconds } else { 0.0 },
        "p50_ms": percentile(0.5), "p95_ms": percentile(0.95), "p99_ms": percentile(0.99), "max_ms": latencies.last(),
        "latency_samples_ms": latencies, "submit_attempts": attempts.len(), "submit_acks": submit_acks,
        "warmup_requests": submit_acks - latencies.len(), "submit_errors_by_code": errors_by_code,
        "drain_started_unix_s": unix(submitted), "drain_finished_unix_s": unix(finished),
        "drain_seconds": finished.duration_since(submitted).as_secs_f64(),
        "drain_errors": unresolved.len(), "unresolved_ids": unresolved,
        "verified_jobs": attempts.len() - unresolved.len(),
        "committed_without_ack": attempts.iter().filter(|row| row.committed_without_ack).count(),
        "run_finished_unix_s": unix(finished),
        "attempts": attempts.iter().map(|row| json!({
            "job_id": row.job_id, "phase": if row.measured { "measurement" } else { "warmup" },
            "sent_unix_s": unix(row.sent), "submit_latency_ms": row.latency_ms,
            "submit_status": row.submit_status, "error": row.error, "code": row.code,
            "drain_status": row.drain_status, "drain_error": row.drain_error,
        })).collect::<Vec<_>>(),
    }) else {
        unreachable!("JSON object")
    };
    report.as_object_mut().expect("JSON object").extend(results);
    if let Some(counts) = rate_counts {
        let sending_start = measure_start.min(stopped);
        let sending_seconds = stopped.duration_since(sending_start).as_secs_f64();
        report["rate_control"] = json!({
            "target_submit_rps": args.submit_rate, "tick_ms": TICK.as_millis(),
            "measurement_window_complete": !capped && stopped == end,
            "measurement_sending_started_unix_s": unix(sending_start),
            "measurement_sending_finished_unix_s": unix(stopped),
            "measurement_sending_seconds": sending_seconds,
            "actual_sent_rps": if sending_seconds > 0.0 { counts[1].sent as f64 / sending_seconds } else { 0.0 },
            "warmup": counts[0].json(), "measured": counts[1].json(),
        });
    }

    if let Some(parent) = args
        .out
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        tokio::fs::create_dir_all(parent).await?;
    }
    tokio::fs::write(&args.out, serde_json::to_vec(&report)?).await?;
    println!(
        "{}",
        json!({"out": args.out, "requests": report["requests"], "submit_ack_rps": report["submit_ack_rps"], "success": success})
    );
    if !success {
        return Err(
            "S6 had Submit errors, unverified results or no measured ACKs; see output JSON".into(),
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use serde_json::Value;
    use tonic::codegen::tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
    use tonic::{Request, Response, Status, Streaming};

    use super::*;

    type RpcResult<T> = std::result::Result<T, Status>;

    #[derive(Clone, Default)]
    struct Service {
        jobs: Arc<Mutex<HashMap<String, v2::JobSpec>>>,
        active: Arc<AtomicU32>,
        peak: Arc<AtomicU32>,
        calls: Arc<AtomicU32>,
        lose_ack: bool,
        submit_delay: Duration,
        first_submit_delay: Duration,
        watch_delay: Duration,
        stall_watch: bool,
        trailer_error: bool,
    }

    #[tonic::async_trait]
    impl v2::client_server::Client for Service {
        type InvokeStream = ReceiverStream<RpcResult<v2::ExecutionEvent>>;
        type WatchStream = Self::InvokeStream;

        async fn submit(
            &self,
            request: Request<v2::SubmitRequest>,
        ) -> RpcResult<Response<v2::SubmitResponse>> {
            let active = self.active.fetch_add(1, Ordering::Relaxed) + 1;
            self.peak.fetch_max(active, Ordering::Relaxed);
            let first = self.calls.fetch_add(1, Ordering::Relaxed) == 0;
            let delay = if first && !self.first_submit_delay.is_zero() {
                self.first_submit_delay
            } else {
                self.submit_delay
            };
            tokio::time::sleep(delay).await;
            let request = request.into_inner();
            assert!(!request.job_id.is_empty());
            assert!(
                self.jobs
                    .lock()
                    .unwrap()
                    .insert(request.job_id.clone(), request.spec.unwrap())
                    .is_none()
            );
            self.active.fetch_sub(1, Ordering::Relaxed);
            if self.lose_ack {
                return Err(Status::unavailable("ACK lost after commit"));
            }
            Ok(Response::new(v2::SubmitResponse {
                created: true,
                job: Some(v2::Job {
                    job_id: request.job_id,
                    durability: v2::Durability::Durable as i32,
                    state: v2::JobState::Succeeded as i32,
                    ..Default::default()
                }),
            }))
        }

        async fn get_job(
            &self,
            request: Request<v2::GetJobRequest>,
        ) -> RpcResult<Response<v2::Job>> {
            let id = request.into_inner().job_id;
            if !self.jobs.lock().unwrap().contains_key(&id) {
                return Err(Status::not_found("unknown job"));
            }
            Ok(Response::new(v2::Job {
                job_id: id,
                durability: v2::Durability::Durable as i32,
                state: v2::JobState::Succeeded as i32,
                ..Default::default()
            }))
        }

        async fn watch(
            &self,
            request: Request<v2::WatchRequest>,
        ) -> RpcResult<Response<Self::WatchStream>> {
            tokio::time::sleep(self.watch_delay).await;
            let id = request.into_inner().job_id;
            let spec = self
                .jobs
                .lock()
                .unwrap()
                .get(&id)
                .cloned()
                .ok_or_else(|| Status::not_found("unknown job"))?;
            let (send, receive) = tokio::sync::mpsc::channel(2);
            if self.stall_watch {
                tokio::spawn(async move {
                    send.closed().await;
                });
            } else {
                send.try_send(Ok(v2::ExecutionEvent {
                    job_id: id,
                    event: Some(v2::execution_event::Event::Finished(
                        v2::ExecutionFinished {
                            state: v2::JobState::Succeeded as i32,
                            outputs: spec.inputs,
                            ..Default::default()
                        },
                    )),
                    ..Default::default()
                }))
                .unwrap();
                if self.trailer_error {
                    send.try_send(Err(Status::unavailable("broken trailer")))
                        .unwrap();
                }
            }
            Ok(Response::new(ReceiverStream::new(receive)))
        }

        async fn invoke(
            &self,
            _: Request<Streaming<v2::InvokeRequest>>,
        ) -> RpcResult<Response<Self::InvokeStream>> {
            Err(Status::unimplemented("unused"))
        }
        async fn cancel(&self, _: Request<v2::CancelRequest>) -> RpcResult<Response<v2::Job>> {
            Err(Status::unimplemented("unused"))
        }
        async fn list_jobs(
            &self,
            _: Request<v2::ListJobsRequest>,
        ) -> RpcResult<Response<v2::ListJobsResponse>> {
            Err(Status::unimplemented("unused"))
        }
    }

    struct Server(tokio::task::JoinHandle<()>);
    impl Drop for Server {
        fn drop(&mut self) {
            self.0.abort();
        }
    }

    async fn setup(service: Service) -> (ClientArgs, Server) {
        static NEXT: AtomicU32 = AtomicU32::new(0);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(v2::client_server::ClientServer::new(service))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        (
            ClientArgs {
                endpoint,
                scenario: 6,
                concurrency: 2,
                connections: Some(2),
                duration: Duration::from_secs(10),
                warmup: Duration::ZERO,
                max_jobs: 3,
                submit_rate: None,
                drain_timeout: Duration::from_secs(2),
                payload: 32,
                payload_pattern: crate::payload::Pattern::Random,
                payload_seed: 42,
                out: std::env::temp_dir().join(format!(
                    "anyserve-s6-{}-{}.json",
                    std::process::id(),
                    NEXT.fetch_add(1, Ordering::Relaxed)
                )),
            },
            Server(task),
        )
    }

    async fn result(args: ClientArgs) -> (bool, Value) {
        let path = args.out.clone();
        let ok = client::run(args).await.is_ok();
        let value = serde_json::from_slice(&tokio::fs::read(&path).await.unwrap()).unwrap();
        tokio::fs::remove_file(path).await.unwrap();
        (ok, value)
    }

    fn assert_rate_counts(report: &Value) {
        let control = &report["rate_control"];
        for phase in ["warmup", "measured"] {
            let counts = &control[phase];
            let count = |key: &str| counts[key].as_u64().unwrap();
            assert_eq!(count("scheduled"), count("sent") + count("skipped"));
            assert_eq!(
                count("skipped"),
                count("skipped_busy") + count("skipped_expired") + count("skipped_max_jobs")
            );
            assert!(count("sent") <= count("offered") && count("offered") <= count("scheduled"));
            assert_eq!(
                count("sent"),
                report["attempts"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .filter(|row| row["phase"]
                        == if phase == "measured" {
                            "measurement"
                        } else {
                            "warmup"
                        })
                    .count() as u64
            );
        }
        let attempts = report["attempts"].as_array().unwrap();
        for (index, attempt) in attempts.iter().enumerate() {
            assert!(
                attempt["job_id"]
                    .as_str()
                    .unwrap()
                    .ends_with(&format!("-{index:010}"))
            );
        }
    }

    #[test]
    fn overdue_ticks_expire_and_partial_ticks_keep_integer_quotas() {
        let begin = Instant::now();
        let finish = begin + Duration::from_millis(237);
        let mut counts = RateCounts::default();
        assert_eq!(
            paced_tick(
                1000,
                begin,
                finish,
                begin + Duration::from_millis(35),
                &mut counts
            )
            .1,
            10
        );
        assert_eq!(counts.skipped_expired, 30);
        assert_eq!(
            paced_tick(
                1000,
                begin,
                finish,
                begin + Duration::from_millis(205),
                &mut counts
            )
            .1,
            10
        );
        assert_eq!(counts.skipped_expired, 190);
        assert_eq!(
            paced_tick(
                1000,
                begin,
                finish,
                begin + Duration::from_millis(235),
                &mut counts
            )
            .1,
            7
        );
        assert_eq!(counts.scheduled, 237);
        for rate in [7, 13] {
            let mut counts = RateCounts::default();
            let mut now = begin;
            let mut offered = 0;
            while now < finish {
                let (next, allowance) = paced_tick(rate, begin, finish, now, &mut counts);
                offered += allowance;
                now = next;
            }
            assert_eq!(offered, quota(rate, Duration::from_millis(237)));
            assert_eq!(counts.skipped_expired, 0);
        }
    }

    #[tokio::test]
    async fn arbitrary_rates_split_phase_and_partial_tick_without_inventing_sends() {
        for rate in [7, 13] {
            let (mut args, _server) = setup(Service::default()).await;
            args.submit_rate = Some(rate);
            args.max_jobs = 100;
            args.warmup = Duration::from_millis(137);
            args.duration = Duration::from_millis(237);
            let (ok, report) = result(args).await;
            assert!(ok);
            assert_rate_counts(&report);
            let control = &report["rate_control"];
            assert_eq!(
                control["warmup"]["scheduled"],
                quota(rate, Duration::from_millis(137))
            );
            assert_eq!(
                control["measured"]["scheduled"],
                quota(rate, Duration::from_millis(237))
            );
            assert_eq!(control["measurement_window_complete"], true);
            assert_eq!(control["measurement_sending_seconds"], 0.237);
            assert_eq!(report["stop_reason"], "duration");
        }
    }

    #[tokio::test]
    async fn slow_rpc_skips_busy_slots_instead_of_queueing_target_load() {
        let service = Service {
            submit_delay: Duration::from_millis(40),
            ..Default::default()
        };
        let (mut args, _server) = setup(service.clone()).await;
        args.submit_rate = Some(1000);
        args.max_jobs = 1000;
        args.warmup = Duration::ZERO;
        args.duration = Duration::from_millis(137);
        let (ok, report) = result(args).await;
        assert!(ok);
        assert_rate_counts(&report);
        assert_eq!(report["observed_peak_inflight"], 2);
        assert_eq!(service.peak.load(Ordering::Relaxed), 2);
        assert_eq!(report["rate_control"]["measured"]["scheduled"], 137);
        assert!(
            report["rate_control"]["measured"]["skipped_busy"]
                .as_u64()
                .unwrap()
                > 100
        );
        assert!(report["rate_control"]["actual_sent_rps"].as_f64().unwrap() < 100.0);
        assert!(report["submit_attempts"].as_u64().unwrap() <= 8);
        assert!(report["actual_duration_seconds"].as_f64().unwrap() >= 0.137);
    }

    #[tokio::test]
    async fn recovering_rpc_never_catches_up_skipped_ticks_with_a_burst() {
        let (mut args, _server) = setup(Service {
            first_submit_delay: Duration::from_millis(65),
            ..Default::default()
        })
        .await;
        args.concurrency = 1;
        args.connections = Some(1);
        args.submit_rate = Some(100);
        args.max_jobs = 100;
        args.duration = Duration::from_millis(157);
        let (ok, report) = result(args).await;
        assert!(ok);
        assert_rate_counts(&report);
        assert!(
            report["rate_control"]["measured"]["skipped_busy"]
                .as_u64()
                .unwrap()
                >= 5
        );
        let begin = report["rate_control"]["measurement_sending_started_unix_s"]
            .as_f64()
            .unwrap();
        let mut ticks = BTreeMap::new();
        for attempt in report["attempts"].as_array().unwrap() {
            let tick = ((attempt["sent_unix_s"].as_f64().unwrap() - begin) * 100.0).floor() as u64;
            *ticks.entry(tick).or_insert(0) += 1;
        }
        assert!(ticks.values().all(|&sent| sent <= 1));
    }

    #[tokio::test]
    async fn paced_cap_during_warmup_consumes_only_sent_ids_and_has_no_measurement_window() {
        let (mut args, _server) = setup(Service::default()).await;
        args.submit_rate = Some(1000);
        args.warmup = Duration::from_secs(1);
        args.max_jobs = 1;
        let (ok, report) = result(args).await;
        assert!(!ok);
        assert_rate_counts(&report);
        assert_eq!(report["submit_attempts"], 1);
        assert_eq!(report["stop_reason"], "max_jobs");
        assert_eq!(report["rate_control"]["measurement_window_complete"], false);
        assert_eq!(report["rate_control"]["measurement_sending_seconds"], 0.0);
        assert_eq!(report["rate_control"]["actual_sent_rps"], 0.0);
        assert_eq!(report["rate_control"]["measured"]["sent"], 0);
    }

    #[tokio::test]
    async fn submit_rate_rejects_other_scenarios_before_connecting() {
        let (mut args, _server) = setup(Service::default()).await;
        args.submit_rate = Some(1);
        args.scenario = 3;
        assert!(
            client::run(args)
                .await
                .unwrap_err()
                .to_string()
                .contains("only supported by S6")
        );
    }

    #[tokio::test]
    async fn cap_bounds_attempts_and_submit_window_excludes_drain() {
        let service = Service {
            submit_delay: Duration::from_millis(20),
            watch_delay: Duration::from_millis(100),
            ..Default::default()
        };
        let (args, _server) = setup(service.clone()).await;
        let (ok, report) = result(args).await;
        assert!(ok);
        assert_eq!(report["requests"], 3);
        assert_eq!(report["submit_attempts"], 3);
        assert_eq!(service.jobs.lock().unwrap().len(), 3);
        assert_eq!(service.peak.load(Ordering::Relaxed), 2);
        assert_eq!(report["stop_reason"], "max_jobs");
        assert!(report["actual_duration_seconds"].as_f64().unwrap() < 1.0);
        assert!(report["drain_seconds"].as_f64().unwrap() >= 0.1);
        assert_eq!(
            report["measurement_finished_unix_s"],
            report["drain_started_unix_s"]
        );
        assert!(
            report["drain_finished_unix_s"].as_f64().unwrap()
                > report["measurement_finished_unix_s"].as_f64().unwrap()
        );
        assert_eq!(
            report["submit_ack_rps"].as_f64().unwrap(),
            3.0 / report["actual_duration_seconds"].as_f64().unwrap()
        );
        assert_eq!(report["verified_jobs"], 3);
        assert!(report.get("throughput_rps").is_none());
    }

    #[tokio::test]
    async fn lost_ack_is_reconciled_without_counting_an_ack_or_retrying_submit() {
        let service = Service {
            lose_ack: true,
            ..Default::default()
        };
        let (args, _server) = setup(service.clone()).await;
        let (ok, report) = result(args).await;
        assert!(!ok);
        assert_eq!(report["requests"], 0);
        assert_eq!(report["errors"], 3);
        assert_eq!(report["committed_without_ack"], 3);
        assert_eq!(report["verified_jobs"], 3);
        assert_eq!(report["drain_errors"], 0);
        assert_eq!(service.jobs.lock().unwrap().len(), 3);
        assert!(report["attempts"].as_array().unwrap().iter().all(|row| row["submit_status"] == "rpc_error" && row["drain_status"] == "verified"));
    }

    #[tokio::test]
    async fn exhausting_cap_during_warmup_has_no_measurement_window() {
        let (mut args, _server) = setup(Service::default()).await;
        args.warmup = Duration::from_secs(10);
        args.max_jobs = 1;
        let (ok, report) = result(args).await;
        assert!(!ok);
        assert_eq!(report["requests"], 0);
        assert_eq!(report["warmup_requests"], 1);
        assert_eq!(report["actual_duration_seconds"], 0.0);
        assert_eq!(report["submit_ack_rps"], 0.0);
        assert_eq!(
            report["measurement_started_unix_s"],
            report["measurement_finished_unix_s"]
        );
        assert_eq!(report["attempts"][0]["phase"], "warmup");
    }

    #[tokio::test]
    async fn duration_stop_drains_submit_rpcs_and_classifies_by_send_time() {
        let (mut args, _server) = setup(Service {
            submit_delay: Duration::from_millis(100),
            ..Default::default()
        })
        .await;
        args.concurrency = 1;
        args.connections = Some(1);
        args.max_jobs = 100;
        args.warmup = Duration::from_millis(50);
        args.duration = Duration::from_millis(100);
        let (ok, report) = result(args).await;
        assert!(ok);
        assert_eq!(report["stop_reason"], "duration");
        assert_eq!(report["warmup_requests"], 1);
        assert_eq!(report["requests"], 1);
        assert_eq!(report["submit_attempts"], 2);
        assert!(report["actual_duration_seconds"].as_f64().unwrap() > 0.1);
        assert!(
            report["submission_finished_unix_s"].as_f64().unwrap()
                > report["submission_stopped_unix_s"].as_f64().unwrap()
        );
    }

    #[tokio::test]
    async fn drain_timeout_and_bad_trailer_preserve_unverified_ids() {
        for stalled in [true, false] {
            let (mut args, _server) = setup(Service {
                stall_watch: stalled,
                trailer_error: !stalled,
                ..Default::default()
            })
            .await;
            args.max_jobs = 4;
            args.drain_timeout = Duration::from_millis(50);
            let (ok, report) = result(args).await;
            assert!(!ok);
            assert_eq!(report["requests"], 4);
            assert_eq!(report["errors"], 0);
            assert_eq!(report["verified_jobs"], 0);
            assert_eq!(report["drain_errors"], 4);
            assert_eq!(report["unresolved_ids"].as_array().unwrap().len(), 4);
            assert_eq!(report["attempts"].as_array().unwrap().len(), 4);
            assert!(report["drain_seconds"].as_f64().unwrap() < 0.5);
        }
    }

    #[test]
    fn ack_requires_matching_new_durable_job() {
        let mut response = v2::SubmitResponse {
            created: true,
            job: Some(v2::Job {
                job_id: "expected".into(),
                durability: v2::Durability::Durable as i32,
                ..Default::default()
            }),
        };
        assert!(check_ack("expected", &response).is_ok());
        assert!(check_ack("other", &response).is_err());
        response.created = false;
        assert!(check_ack("expected", &response).is_err());
        response.created = true;
        response.job.as_mut().unwrap().durability = v2::Durability::Ephemeral as i32;
        assert!(check_ack("expected", &response).is_err());
    }
}
