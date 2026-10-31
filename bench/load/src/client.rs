use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyserve_client::execution::{ExecutionClient, v2};
use serde_json::json;
use tokio::sync::Barrier;
use tokio::task::JoinSet;
use tokio::time::Instant;

use crate::{ClientArgs, Result};

pub(super) const SCENARIOS: [&str; 5] = [
    "invoke-buffered",
    "invoke-stream",
    "submit-durable",
    "submit-ephemeral",
    "mixed",
];

pub(super) fn parse_scenario(value: &str) -> std::result::Result<u8, String> {
    if let Some(index) = SCENARIOS.iter().position(|name| *name == value) {
        return Ok(index as u8 + 1);
    }
    value
        .trim_start_matches(['S', 's'])
        .parse::<u8>()
        .ok()
        .filter(|scenario| (1..=5).contains(scenario))
        .ok_or_else(|| "expected S1..S5 or a scenario name".into())
}

#[derive(Default)]
struct Samples {
    latencies: Vec<f64>,
    mixed_paths: [Vec<f64>; 2],
    errors: usize,
    warmup_errors: usize,
    first_error: Option<String>,
}

pub(super) async fn run(args: ClientArgs) -> Result {
    if args.duration.is_zero() {
        return Err("measurement duration must be positive".into());
    }
    let connection_count = args.connections.unwrap_or(args.concurrency.min(8));
    if connection_count > args.concurrency {
        return Err("connections must not exceed concurrency".into());
    }
    // Each connect creates a distinct channel. Clones share that channel, so
    // tasks round-robin over this explicit pool before the warm-up clock.
    let mut channels = Vec::new();
    for _ in 0..connection_count {
        channels.push(ExecutionClient::connect_with_token(&args.endpoint, None).await?);
    }
    let clients: Vec<_> = (0..args.concurrency)
        .map(|index| channels[(index % connection_count) as usize].clone())
        .collect();
    let payload = Arc::new(vec![b'x'; args.payload]);
    let barrier = Arc::new(Barrier::new(clients.len() + 1));
    let start = Instant::now();
    let unix_start = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs_f64();
    let measure_start = start + args.warmup;
    let end = measure_start + args.duration;
    let mut tasks = JoinSet::new();
    for mut connection in clients {
        let barrier = barrier.clone();
        let payload = payload.clone();
        tasks.spawn(async move {
            let mut samples = Samples::default();
            let mut sequence = 0;
            barrier.wait().await;
            loop {
                let sent = Instant::now();
                if sent >= end {
                    break;
                }
                let scenario = if args.scenario == 5 {
                    if sequence % 2 == 0 { 1 } else { 3 }
                } else {
                    args.scenario
                };
                sequence += 1;
                let result = tokio::time::timeout(
                    Duration::from_secs(30),
                    request(&mut connection, scenario, &payload),
                )
                .await;
                let elapsed = sent.elapsed().as_secs_f64() * 1000.0;
                match result {
                    Ok(Ok(_)) if sent >= measure_start => {
                        samples.latencies.push(elapsed);
                        if args.scenario == 5 {
                            samples.mixed_paths[usize::from(scenario == 3)].push(elapsed);
                        }
                    }
                    Ok(Ok(_)) => {}
                    error => {
                        if sent >= measure_start {
                            samples.errors += 1;
                        } else {
                            samples.warmup_errors += 1;
                        }
                        samples.first_error.get_or_insert_with(|| match error {
                            Ok(Err(error)) => error.to_string(),
                            Err(error) => error.to_string(),
                            Ok(Ok(_)) => unreachable!(),
                        });
                    }
                }
            }
            samples
        });
    }
    barrier.wait().await;
    let mut combined = Samples::default();
    while let Some(completed) = tasks.join_next().await {
        let samples = completed?;
        combined.latencies.extend(samples.latencies);
        for (combined, samples) in combined.mixed_paths.iter_mut().zip(samples.mixed_paths) {
            combined.extend(samples);
        }
        combined.errors += samples.errors;
        combined.warmup_errors += samples.warmup_errors;
        if combined.first_error.is_none() {
            combined.first_error = samples.first_error;
        }
    }
    let actual = Instant::now().duration_since(measure_start).as_secs_f64();
    combined.latencies.sort_by(f64::total_cmp);
    let percentile = |quantile: f64| -> Option<f64> {
        if combined.latencies.is_empty() {
            None
        } else {
            let index = (quantile * combined.latencies.len() as f64).ceil() as usize;
            Some(combined.latencies[index.saturating_sub(1)])
        }
    };
    let by_path = if args.scenario == 5 {
        let mut by_path = serde_json::Map::new();
        for (name, samples) in ["S1", "S3"].into_iter().zip(&mut combined.mixed_paths) {
            samples.sort_by(f64::total_cmp);
            let percentile = |quantile: f64| {
                samples
                    .get(((quantile * samples.len() as f64).ceil() as usize).saturating_sub(1))
                    .copied()
            };
            by_path.insert(
                name.into(),
                json!({
                    "requests": samples.len(), "p50_ms": percentile(0.5),
                    "p95_ms": percentile(0.95), "p99_ms": percentile(0.99),
                    "max_ms": samples.last(),
                }),
            );
        }
        serde_json::Value::Object(by_path)
    } else {
        serde_json::Value::Null
    };
    let result = json!({
        "scenario": format!("S{}", args.scenario),
        "name": SCENARIOS[args.scenario as usize - 1],
        "concurrency": args.concurrency,
        "connections": connection_count,
        "grpc_channels": connection_count,
        "by_path": by_path,
        "payload_bytes": args.payload,
        "warmup_seconds": args.warmup.as_secs_f64(),
        "target_duration_seconds": args.duration.as_secs_f64(),
        "actual_duration_seconds": actual,
        "measurement_started_unix_s": unix_start + args.warmup.as_secs_f64(),
        "measurement_finished_unix_s": unix_start + args.warmup.as_secs_f64() + actual,
        "requests": combined.latencies.len(),
        "errors": combined.errors,
        "warmup_errors": combined.warmup_errors,
        "first_error": combined.first_error,
        "throughput_rps": combined.latencies.len() as f64 / actual,
        "p50_ms": percentile(0.50),
        "p95_ms": percentile(0.95),
        "p99_ms": percentile(0.99),
        "max_ms": combined.latencies.last(),
        "latency_samples_ms": combined.latencies,
    });
    if let Some(parent) = args
        .out
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        tokio::fs::create_dir_all(parent).await?;
    }
    tokio::fs::write(&args.out, serde_json::to_vec(&result)?).await?;
    println!(
        "{}",
        json!({"out": args.out, "requests": result["requests"], "errors": result["errors"], "p50_ms": result["p50_ms"], "throughput_rps": result["throughput_rps"]})
    );
    if combined.errors != 0 || combined.warmup_errors != 0 || combined.latencies.is_empty() {
        return Err(
            "benchmark had errors or no successful measured requests; see output JSON".into(),
        );
    }
    Ok(())
}

pub(super) async fn seed(
    endpoint: &str,
    count: usize,
    size: usize,
    out: &std::path::Path,
) -> Result {
    let mut client = ExecutionClient::connect_with_token(endpoint, None).await?;
    let payload = vec![b'x'; size];
    let mut ids = Vec::with_capacity(count);
    for _ in 0..count {
        ids.push(
            tokio::time::timeout(Duration::from_secs(30), request(&mut client, 3, &payload))
                .await??,
        );
    }
    tokio::fs::write(
        out,
        serde_json::to_vec(&json!({"job_ids": ids, "payload_bytes": size}))?,
    )
    .await?;
    Ok(())
}

async fn request(client: &mut ExecutionClient, scenario: u8, payload: &[u8]) -> Result<String> {
    let streamed = scenario == 2;
    let spec = v2::JobSpec {
        demand: Some(v2::Demand {
            interface: if streamed {
                "bench.stream.v1"
            } else {
                "bench.echo.v1"
            }
            .into(),
            required_capacity: [("slot".into(), 1)].into(),
            ..Default::default()
        }),
        inputs: vec![v2::Object {
            body: Some(v2::object::Body::Inline(payload.to_vec().into())),
            ..Default::default()
        }],
        policy: Some(v2::ExecutionPolicy {
            durability: if scenario == 3 {
                v2::Durability::Durable
            } else {
                v2::Durability::Ephemeral
            } as i32,
            total_timeout_ms: 30_000,
            ..Default::default()
        }),
        ..Default::default()
    };
    let mut events = if matches!(scenario, 3 | 4) {
        let accepted = client.submit(None, spec).await?;
        let job = accepted.job.ok_or("Submit omitted job")?;
        client.watch(job.job_id, 0).await?
    } else {
        client.invoke(spec).await?
    };
    let mut frames = 0;
    while let Some(event) = events.message().await? {
        match event.event {
            Some(v2::execution_event::Event::Output(frame)) => {
                if !streamed || frame.progress || frame.data.as_ref() != [b'x'; 1024] {
                    return Err("unexpected output frame".into());
                }
                frames += 1;
            }
            Some(v2::execution_event::Event::Finished(finish)) => {
                if finish.state != v2::JobState::Succeeded as i32 {
                    return Err(format!("execution failed: {:?}", finish.error).into());
                }
                if streamed {
                    if frames != 64 || !finish.outputs.is_empty() {
                        return Err(format!(
                            "expected 64 frames and no terminal objects, got {frames} frames"
                        )
                        .into());
                    }
                } else if finish.outputs.len() != 1
                    || !matches!(&finish.outputs[0].body, Some(v2::object::Body::Inline(data)) if data.as_ref() == payload)
                {
                    return Err("echo result differs from input".into());
                }
                // Finished is the application result. Read the gRPC trailer as
                // well so completed calls do not generate HTTP/2 resets.
                if events.message().await?.is_some() {
                    return Err("event received after Finished".into());
                }
                return Ok(event.job_id);
            }
            _ => {}
        }
    }
    Err("event stream ended before Finished".into())
}
