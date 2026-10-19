use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyserve_client::execution::{Attempt, AttemptClient, WorkerSession, v2};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;

use crate::{Result, WorkerArgs};

#[derive(Clone)]
pub(super) enum Mode {
    Echo,
    Stream(u32, v2::Output),
}

pub(super) fn parse_mode(value: &str) -> std::result::Result<Mode, String> {
    if value == "echo" {
        return Ok(Mode::Echo);
    }
    let parts: Vec<_> = value.split(':').collect();
    if let ["stream", frames, bytes] = parts.as_slice() {
        let frames = frames.parse::<u32>().map_err(|error| error.to_string())?;
        let bytes = bytes.parse::<usize>().map_err(|error| error.to_string())?;
        if frames > 0 && (1..=64 * 1024).contains(&bytes) {
            return Ok(Mode::Stream(
                frames,
                v2::Output {
                    data: vec![b'x'; bytes].into(),
                    ..Default::default()
                },
            ));
        }
    }
    Err("expected echo or stream:F:B with F > 0 and 1 <= B <= 65536".into())
}

pub(super) async fn run(args: WorkerArgs) -> Result {
    let mode = Arc::new(args.mode);
    let worker_id = format!("bench-{}", std::process::id());
    let mut active: HashMap<String, watch::Sender<bool>> = HashMap::new();
    let mut tasks = JoinSet::new();
    let mut task_leases = HashMap::new();
    let mut resume = String::new();
    let stop = crate::shutdown_signal();
    tokio::pin!(stop);
    loop {
        let hello = v2::Hello {
            worker_id: worker_id.clone(),
            resume_session_id: resume.clone(),
            interfaces: vec![args.interface.clone()],
            total_capacity: [("slot".into(), i64::from(args.concurrency))].into(),
            max_concurrency: args.concurrency,
            held_lease_ids: active.keys().cloned().collect(),
            sdk_version: env!("CARGO_PKG_VERSION").into(),
            ..Default::default()
        };
        let connection = tokio::select! {
            signal = &mut stop => return signal,
            result = WorkerSession::connect(&args.endpoint, None, hello) => result,
        };
        let mut session = match connection {
            Ok(session) => session,
            Err(error) => {
                eprintln!("worker connect: {error}");
                tokio::time::sleep(Duration::from_millis(250)).await;
                continue;
            }
        };
        let connected: Result = async {
            let welcome = match session.messages.message().await?.and_then(|message| message.msg) {
                Some(v2::server_message::Msg::Welcome(welcome)) => welcome,
                _ => return Err("worker session did not start with Welcome".into()),
            };
            let limits = welcome.limits.ok_or("Welcome omitted limits")?;
            let heartbeat_timeout = Duration::from_millis(u64::from(welcome.heartbeat_timeout_ms));
            resume = welcome.session_id;
            for (lease, cancel) in &active {
                if !welcome.resumed_lease_ids.contains(lease) { let _ = cancel.send(true); }
            }
            let mut draining = false;
            credit(&session.control, args.concurrency, active.len(), draining).await?;
            println!("ready worker {worker_id} interface={}", args.interface);
            let mut heartbeat = tokio::time::interval(Duration::from_millis(u64::from(welcome.heartbeat_interval_ms.max(1))));
            heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    signal = &mut stop => { signal?; return Ok(()); },
                    _ = heartbeat.tick() => send(&session.control, v2::worker_message::Msg::Heartbeat(v2::Heartbeat {
                        load: Some(v2::Load { running: active.len() as u32, ..Default::default() }),
                    })).await?,
                    completed = tasks.join_next_with_id(), if !tasks.is_empty() => {
                        let (id, result) = match completed.ok_or("worker task set closed")? {
                            Ok((id, result)) => (id, result),
                            Err(error) => (error.id(), Err(error.into())),
                        };
                        let lease = task_leases.remove(&id).ok_or("task has no lease")?;
                        let cancelled = active.remove(&lease).is_some_and(|cancel| *cancel.borrow());
                        if cancelled { send(&session.control, v2::worker_message::Msg::CancelAck(v2::CancelAck { lease_id: lease })).await?; }
                        result?;
                        credit(&session.control, args.concurrency, active.len(), draining).await?;
                    },
                    message = session.messages.message() => match message?.and_then(|message| message.msg) {
                        Some(v2::server_message::Msg::Assign(assignment)) => {
                            let accepted = !draining && active.len() < args.concurrency as usize;
                            send(&session.control, v2::worker_message::Msg::AssignReply(v2::AssignReply {
                                lease_id: assignment.lease_id.clone(), accepted,
                                reject_reason: if accepted { String::new() } else { "no local slot".into() },
                            })).await?;
                            if accepted {
                                let (cancel, mut cancelled) = watch::channel(false);
                                active.insert(assignment.lease_id.clone(), cancel);
                                let mut data = session.data.clone();
                                let mode = mode.clone();
                                let lease = assignment.lease_id.clone();
                                let task = tasks.spawn(async move {
                                    tokio::select! {
                                        _ = cancelled.changed() => Ok(()),
                                        result = execute(&mut data, &assignment, &limits, heartbeat_timeout, &mode) => result,
                                    }
                                });
                                task_leases.insert(task.id(), lease);
                            } else {
                                credit(&session.control, args.concurrency, active.len(), draining).await?;
                            }
                        },
                        Some(v2::server_message::Msg::Cancel(cancel)) => {
                            if let Some(signal) = active.get(&cancel.lease_id) { let _ = signal.send(true); }
                            else { send(&session.control, v2::worker_message::Msg::CancelAck(v2::CancelAck { lease_id: cancel.lease_id })).await?; }
                        },
                        Some(v2::server_message::Msg::Fence(fence)) => {
                            if let Some(signal) = active.get(&fence.lease_id) { let _ = signal.send(true); }
                        },
                        Some(v2::server_message::Msg::Drain(_)) => {
                            draining = true;
                            credit(&session.control, args.concurrency, active.len(), true).await?;
                        },
                        None => return Err("control session closed".into()),
                        _ => {},
                    },
                }
            }
        }.await;
        if connected.is_ok() {
            return Ok(());
        }
        eprintln!("worker reconnect: {}", connected.unwrap_err());
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn execute(
    data: &mut AttemptClient,
    assignment: &v2::Assign,
    limits: &v2::Limits,
    heartbeat_timeout: Duration,
    mode: &Mode,
) -> Result {
    let mut attempt = Attempt::open(data, assignment, limits, heartbeat_timeout).await?;
    let outputs = match mode {
        Mode::Echo => attempt.inputs.clone(),
        Mode::Stream(frames, payload) => {
            for _ in 0..*frames {
                if let Err(error) = attempt
                    .frame(payload.data.clone(), Default::default(), false)
                    .await
                {
                    attempt
                        .commit(
                            data,
                            assignment,
                            limits,
                            &[],
                            v2::finish::Outcome::Failed(v2::ErrorInfo {
                                code: "output_interrupted".into(),
                                message: error.to_string(),
                                retryable: true,
                            }),
                        )
                        .await?;
                    return Ok(());
                }
            }
            Vec::new()
        }
    };
    attempt
        .commit(
            data,
            assignment,
            limits,
            &outputs,
            v2::finish::Outcome::Succeeded(v2::Succeeded {}),
        )
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests;

async fn send(control: &mpsc::Sender<v2::WorkerMessage>, msg: v2::worker_message::Msg) -> Result {
    control.send(v2::WorkerMessage { msg: Some(msg) }).await?;
    Ok(())
}

async fn credit(
    control: &mpsc::Sender<v2::WorkerMessage>,
    concurrency: u32,
    active: usize,
    draining: bool,
) -> Result {
    send(
        control,
        v2::worker_message::Msg::Credit(v2::Credit {
            free_slots: if draining {
                0
            } else {
                concurrency.saturating_sub(active as u32)
            },
        }),
    )
    .await
}
