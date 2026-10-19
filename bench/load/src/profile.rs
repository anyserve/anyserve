use std::fs::File;
use std::io::Write;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use anyserve_client::execution::v2::{client_server::ClientServer, worker_server::WorkerServer};
use anyserve_core::auth::{AuthLayer, AuthPolicy};
use anyserve_core::client_service::ClientGrpcService;
use anyserve_core::runtime::{
    Runtime,
    api::{JournalConfig, RuntimeConfig},
};
use anyserve_core::worker_service::WorkerGrpcService;
use serde_json::json;

use crate::Result;

pub(super) async fn serve(listen: SocketAddr, journal: PathBuf, profile: PathBuf) -> Result {
    // Match CLI init_tracing: without a subscriber, tracing's log fallback
    // adds work absent from the production server and distorts CPU profiles.
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .or_else(|_| tracing_subscriber::EnvFilter::try_new("info"))?;
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .compact()
        .try_init()?;
    std::fs::create_dir_all(&profile)?;
    let guard = pprof::ProfilerGuardBuilder::default()
        .frequency(999)
        .blocklist(&["libc", "libgcc", "pthread"])
        .build()?;
    let runtime = Runtime::start(RuntimeConfig {
        journal: Some(JournalConfig::new(journal)),
        ..Default::default()
    })
    .await?;
    const MESSAGE_BYTES: usize = 64 * 1024 * 1024;
    let client = ClientServer::new(ClientGrpcService::new(runtime.clone()))
        .max_decoding_message_size(MESSAGE_BYTES)
        .max_encoding_message_size(MESSAGE_BYTES);
    let worker = WorkerServer::new(WorkerGrpcService::new(runtime.clone()))
        .max_decoding_message_size(MESSAGE_BYTES)
        .max_encoding_message_size(MESSAGE_BYTES);
    let (mut health, service) = tonic_health::server::health_reporter();
    health
        .set_serving::<ClientServer<ClientGrpcService>>()
        .await;
    health
        .set_serving::<WorkerServer<WorkerGrpcService>>()
        .await;
    let (stop, stopped) = tokio::sync::oneshot::channel();
    // Keep these transport settings aligned with CLI serve_runtime::run_grpc_server.
    let mut server = tokio::spawn(
        tonic::transport::Server::builder()
            .tcp_nodelay(true)
            .initial_stream_window_size(Some(4 * 1024 * 1024))
            .initial_connection_window_size(Some(32 * 1024 * 1024))
            .http2_keepalive_interval(Some(Duration::from_secs(20)))
            .http2_max_pending_accept_reset_streams(Some(1024))
            .layer(AuthLayer::new(AuthPolicy::default()))
            .add_service(service)
            .add_service(client)
            .add_service(worker)
            .serve_with_shutdown(listen, async {
                let _ = stopped.await;
            }),
    );
    println!(
        "ready profile server pid={} listen={listen}",
        std::process::id()
    );
    tokio::select! {
        signal = crate::shutdown_signal() => signal?,
        result = &mut server => { result??; return Err("profile server ended before shutdown".into()); },
    }
    let stats = runtime.stats();
    let journal = stats.journal.map(|journal| json!({
        "pending_ops": journal.pending_ops,
        "committed_batches": journal.committed_batches,
        "committed_ops": journal.committed_ops,
        "mean_batch_size": journal.committed_ops as f64 / journal.committed_batches.max(1) as f64,
        "last_batch_size": journal.last_batch_size,
        "last_commit_micros": journal.last_commit_micros,
    }));
    let report = guard.report().build()?;
    drop(guard);
    let samples: isize = report.data.values().sum();
    if samples == 0 {
        return Err("pprof collected no CPU samples".into());
    }
    let mut folded = Vec::new();
    for (frames, count) in &report.data {
        let mut stack = vec![frames.thread_name_or_id()];
        for frame in frames.frames.iter().rev() {
            for symbol in frame.iter().rev() {
                stack.push(symbol.to_string().replace([';', '\n', '\r'], ":"));
            }
        }
        folded.push(format!("{} {count}\n", stack.join(";")));
    }
    folded.sort_unstable();
    let mut file = File::create(profile.join("profile.folded"))?;
    for line in folded {
        file.write_all(line.as_bytes())?;
    }
    report.flamegraph(File::create(profile.join("flamegraph.svg"))?)?;
    std::fs::write(
        profile.join("stats.json"),
        serde_json::to_vec_pretty(&json!({
            "frequency_hz": 999, "samples": samples, "journal": journal,
            "retained_jobs": stats.retained_jobs,
            "event_bytes": stats.event_bytes, "result_bytes": stats.result_bytes,
        }))?,
    )?;
    runtime.shutdown().await;
    let _ = stop.send(());
    if tokio::time::timeout(Duration::from_secs(5), &mut server)
        .await
        .is_err()
    {
        server.abort();
    }
    println!("profile written to {}", profile.display());
    Ok(())
}
