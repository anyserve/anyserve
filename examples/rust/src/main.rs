//! Minimal native client and echo worker; the CLI worker adds production
//! reconnect, health checks and graceful shutdown for HTTP inference engines.
use std::collections::HashMap;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use anyserve_client::execution::{Attempt, ExecutionClient, WorkerSession, v2};
use clap::{Parser, ValueEnum};
use tokio::sync::watch;
use tokio::task::JoinSet;

#[derive(Clone, Debug, ValueEnum)]
enum Mode {
    Submit,
    Invoke,
    Worker,
}
#[derive(Parser, Debug)]
struct Args {
    #[arg(long, value_enum, default_value = "worker")]
    mode: Mode,
    #[arg(long, default_value = "http://127.0.0.1:50052")]
    endpoint: String,
    #[arg(long)]
    job_id: Option<String>,
    #[arg(long, default_value = "rust-echo")]
    worker_id: String,
}
#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    match args.mode {
        Mode::Worker => worker(&args.endpoint, args.worker_id).await,
        mode => request(&args.endpoint, args.job_id, matches!(mode, Mode::Submit)).await,
    }
}
async fn request(endpoint: &str, job_id: Option<String>, durable: bool) -> Result<()> {
    let mut client = ExecutionClient::connect(endpoint).await?;
    let spec = v2::JobSpec {
        demand: Some(v2::Demand {
            interface: "demo.echo.v1".into(),
            ..Default::default()
        }),
        inputs: vec![v2::Object {
            body: Some(v2::object::Body::Inline(b"hello from Rust".to_vec().into())),
            metadata: HashMap::new(),
        }],
        ..Default::default()
    };
    let mut events = if durable {
        let job = client
            .submit(job_id, spec)
            .await?
            .job
            .context("missing job")?;
        println!("durably accepted {}", job.job_id);
        client.watch(job.job_id, 0).await?
    } else {
        client.invoke(spec).await?
    };
    while let Some(event) = events.message().await? {
        println!(
            "{} seq={} attempt={}",
            event.job_id, event.sequence, event.attempt_id
        );
        if let Some(v2::execution_event::Event::Finished(finish)) = event.event {
            if finish.state != v2::JobState::Succeeded as i32 {
                bail!("execution failed: {:?}", finish.error);
            }
            for output in finish.outputs {
                if let Some(v2::object::Body::Inline(bytes)) = output.body {
                    println!("{}", String::from_utf8_lossy(&bytes));
                }
            }
            return Ok(());
        }
    }
    bail!("execution ended without a terminal event")
}
async fn worker(endpoint: &str, worker_id: String) -> Result<()> {
    let token = std::env::var(anyserve_client::TOKEN_ENV).ok();
    let mut session = WorkerSession::connect(
        endpoint,
        token.as_deref(),
        v2::Hello {
            worker_id,
            interfaces: vec!["demo.echo.v1".into()],
            max_concurrency: 1,
            total_capacity: HashMap::from([("slot".into(), 1)]),
            ..Default::default()
        },
    )
    .await?;
    let Some(v2::server_message::Msg::Welcome(welcome)) = session
        .messages
        .message()
        .await?
        .and_then(|message| message.msg)
    else {
        bail!("missing Welcome");
    };
    let limits = welcome.limits.context("missing limits")?;
    let heartbeat_timeout = Duration::from_millis(u64::from(welcome.heartbeat_timeout_ms));
    send(
        &session.control,
        v2::worker_message::Msg::Credit(v2::Credit { free_slots: 1 }),
    )
    .await?;
    let mut heartbeat = tokio::time::interval(Duration::from_millis(u64::from(
        welcome.heartbeat_interval_ms.max(1),
    )));
    let mut running = JoinSet::new();
    let mut cancellation = None;
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => return Ok(()),
            _ = heartbeat.tick() => send(&session.control, v2::worker_message::Msg::Heartbeat(v2::Heartbeat { load: None })).await?,
            result = running.join_next(), if !running.is_empty() => {
                let (lease, result, cancelled) = result.context("worker task set closed")??;
                cancellation = None;
                if cancelled { send(&session.control, v2::worker_message::Msg::CancelAck(v2::CancelAck { lease_id: lease })).await?; }
                result?;
                send(&session.control, v2::worker_message::Msg::Credit(v2::Credit { free_slots: 1 })).await?;
            }
            message = session.messages.message() => match message?.and_then(|message| message.msg) {
                Some(v2::server_message::Msg::Assign(assignment)) => {
                    send(&session.control, v2::worker_message::Msg::AssignReply(v2::AssignReply { lease_id: assignment.lease_id.clone(), accepted: true, reject_reason: String::new() })).await?;
                    let (cancel, mut cancelled) = watch::channel(false);
                    cancellation = Some(cancel);
                    let mut data = session.data.clone();
                    running.spawn(async move {
                        let result: Result<()> = tokio::select! {
                            _ = cancelled.changed() => Ok(()),
                            result = async {
                                let mut attempt = Attempt::open(&mut data, &assignment, &limits, heartbeat_timeout).await?;
                                if !attempt.terminal_replay {
                                    let outputs = attempt.inputs.clone();
                                    attempt.commit(&mut data, &assignment, &limits, &outputs, v2::finish::Outcome::Succeeded(v2::Succeeded {})).await?;
                                }
                                Ok(())
                            } => result,
                        };
                        (assignment.lease_id, result, *cancelled.borrow())
                    });
                }
                Some(v2::server_message::Msg::Cancel(_) | v2::server_message::Msg::Fence(_)) => {
                    if let Some(cancel) = &cancellation { let _ = cancel.send(true); }
                }
                Some(v2::server_message::Msg::Drain(_)) | None => return Ok(()),
                _ => {},
            }
        }
    }
}
async fn send(
    sender: &tokio::sync::mpsc::Sender<v2::WorkerMessage>,
    msg: v2::worker_message::Msg,
) -> Result<()> {
    sender.send(v2::WorkerMessage { msg: Some(msg) }).await?;
    Ok(())
}
