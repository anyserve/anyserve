use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use anyserve_client::execution::{WorkerSession, v2};
use tokio::sync::watch;
use tokio::task::JoinSet;

use super::config::{WorkerArgs, WorkerConfig, load_config};
use super::helpers::build_http_client;

mod execution;

#[derive(Clone)]
pub(super) struct WorkerContext {
    pub config: Arc<WorkerConfig>,
    pub http: reqwest::Client,
}

struct ActiveAttempt {
    cancel: watch::Sender<bool>,
    resources: HashMap<String, i64>,
}
struct BackgroundTasks(Vec<tokio::task::JoinHandle<()>>);
impl Drop for BackgroundTasks {
    fn drop(&mut self) {
        for task in &self.0 {
            task.abort();
        }
    }
}

pub(crate) async fn run(endpoint: &str, args: WorkerArgs) -> Result<()> {
    let config = Arc::new(load_config(&args)?);
    let context = WorkerContext {
        http: build_http_client(&config)?,
        config,
    };
    let worker_id = context.config.worker_id.clone();
    let (health_tx, mut healthy) = watch::channel(context.config.health_url.is_none());
    let health_task = tokio::spawn(health_loop(context.clone(), health_tx));
    let (stop_tx, mut stop) = watch::channel(false);
    let signal_task = tokio::spawn(async move {
        shutdown_signal().await;
        let _ = stop_tx.send(true);
    });
    let _background = BackgroundTasks(vec![health_task, signal_task]);
    session_loop(endpoint, &worker_id, context, &mut healthy, &mut stop).await
}

async fn session_loop(
    endpoint: &str,
    worker_id: &str,
    context: WorkerContext,
    healthy: &mut watch::Receiver<bool>,
    stop: &mut watch::Receiver<bool>,
) -> Result<()> {
    let config = &context.config;
    let mut active: HashMap<String, ActiveAttempt> = HashMap::new();
    let mut executions = JoinSet::new();
    let mut task_leases = HashMap::new();
    let mut resume_session_id = String::new();
    let mut draining_since = None;
    loop {
        if *stop.borrow() && draining_since.is_none() {
            draining_since = Some(tokio::time::Instant::now());
        }
        if draining_since.is_some_and(|started| {
            active.is_empty() || started.elapsed() >= Duration::from_secs(30)
        }) {
            executions.abort_all();
            return Ok(());
        }
        let hello = v2::Hello {
            worker_id: worker_id.to_owned(),
            resume_session_id: resume_session_id.clone(),
            interfaces: config.interfaces.clone(),
            attributes: config.attributes.clone(),
            total_capacity: config.capacity.clone(),
            max_concurrency: config.max_active_leases,
            sdk_version: env!("CARGO_PKG_VERSION").to_owned(),
            held_lease_ids: active.keys().cloned().collect(),
            metadata: config.metadata.clone(),
        };
        let mut session =
            match WorkerSession::connect(endpoint, config.token.as_deref(), hello).await {
                Ok(session) => session,
                Err(error) => {
                    if permanent_error(&error) {
                        return Err(error);
                    }
                    tracing::warn!(%error, "worker session reconnect failed");
                    reconnect_backoff(config.reconnect_interval, stop).await;
                    continue;
                }
            };
        let connected: Result<()> = async {
        let mut session_draining = false;
        let welcome = match session.messages.message().await? {
            Some(v2::ServerMessage {
                msg: Some(v2::server_message::Msg::Welcome(welcome)),
            }) => welcome,
            _ => return Err(tonic::Status::invalid_argument("worker session did not start with Welcome").into()),
        };
        let limits = welcome.limits.context("Welcome has no data limits")?;
        let heartbeat_timeout = Duration::from_millis(u64::from(welcome.heartbeat_timeout_ms));
        resume_session_id = welcome.session_id;
        for (lease, held) in &active {
            if !welcome.resumed_lease_ids.contains(lease) {
                let _ = held.cancel.send(true);
            }
        }
        tracing::info!(%worker_id, provider = %config.provider, generation = welcome.generation, "worker session connected");
        send_credit(
            &session.control,
            config.max_active_leases,
            active.len(),
            *healthy.borrow() && draining_since.is_none() && !session_draining && !active.values().any(|held| *held.cancel.borrow()),
        )
        .await?;
        let mut heartbeat = tokio::time::interval(Duration::from_millis(u64::from(
            welcome.heartbeat_interval_ms.max(1),
        )));
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            if draining_since.is_some_and(|started| {
                active.is_empty() || started.elapsed() >= Duration::from_secs(30)
            }) {
                executions.abort_all();
                return Ok(());
            }
            tokio::select! {
                message = session.messages.message() => {
                    match message {
                        Ok(Some(v2::ServerMessage { msg: Some(v2::server_message::Msg::Assign(assignment)) })) => {
                            let accepted = *healthy.borrow() && draining_since.is_none() && !session_draining && active.len() < config.max_active_leases as usize && local_capacity(config, &active, &assignment.reserved_capacity);
                            send_control(&session.control, v2::worker_message::Msg::AssignReply(v2::AssignReply {
                                lease_id: assignment.lease_id.clone(), accepted,
                                reject_reason: if accepted { String::new() } else { "worker is not accepting work".to_owned() },
                            })).await?;
                            if accepted {
                                let (cancel, cancelled) = watch::channel(false);
                                active.insert(assignment.lease_id.clone(), ActiveAttempt { cancel, resources: assignment.reserved_capacity.clone() });
                                let context = context.clone();
                                let data = session.data.clone();
                                let lease_id = assignment.lease_id.clone();
                                let task = executions.spawn(execution::run_attempt(context, data, assignment, limits, heartbeat_timeout, cancelled));
                                task_leases.insert(task.id(), lease_id);
                            } else {
                                send_credit(&session.control, config.max_active_leases, active.len(), *healthy.borrow() && draining_since.is_none() && !session_draining && !active.values().any(|held| *held.cancel.borrow())).await?;
                            }
                        }
                        Ok(Some(v2::ServerMessage { msg: Some(v2::server_message::Msg::Cancel(cancel)) })) => {
                            if let Some(signal) = active.get(&cancel.lease_id) { let _ = signal.cancel.send(true); send_credit(&session.control, config.max_active_leases, active.len(), false).await?; }
                            else { send_control(&session.control, v2::worker_message::Msg::CancelAck(v2::CancelAck { lease_id: cancel.lease_id })).await?; }
                        }
                        Ok(Some(v2::ServerMessage { msg: Some(v2::server_message::Msg::Fence(fence)) })) => {
                            if let Some(signal) = active.get(&fence.lease_id) { let _ = signal.cancel.send(true); send_credit(&session.control, config.max_active_leases, active.len(), false).await?; }
                        }
                        Ok(Some(v2::ServerMessage { msg: Some(v2::server_message::Msg::Drain(_)) })) => {
                            session_draining = true;
                            send_credit(&session.control, config.max_active_leases, active.len(), false).await?;
                        }
                        Ok(Some(_)) => {}
                        Ok(None) => break,
                        Err(error) => { tracing::warn!(%error, "worker control connection lost"); break; }
                    }
                }
                completed = executions.join_next_with_id(), if !executions.is_empty() => {
                    let completed = finish_execution(completed.context("worker execution set closed")?, &mut task_leases, &mut active)?;
                    let lease_id = completed.lease_id;
                    if completed.cancelled {
                        send_control(&session.control, v2::worker_message::Msg::CancelAck(v2::CancelAck { lease_id: lease_id.clone() })).await?;
                    }
                    if let Err(error) = completed.result {
                        tracing::warn!(%lease_id, %error, "attempt ended");
                        if !retired_attempt(&error) {
                            // Reconnect without this lease, including when its
                            // task panicked. Other held attempts resume.
                            break;
                        }
                    }
                    send_credit(&session.control, config.max_active_leases, active.len(), *healthy.borrow() && draining_since.is_none() && !session_draining && !active.values().any(|held| *held.cancel.borrow())).await?;
                }
                _ = heartbeat.tick() => {
                    if send_control(&session.control, v2::worker_message::Msg::Heartbeat(v2::Heartbeat {
                        load: Some(v2::Load { running: active.len() as u32, ..Default::default() }),
                    })).await.is_err() { break; }
                }
                _ = healthy.changed() => {
                    send_credit(&session.control, config.max_active_leases, active.len(), *healthy.borrow() && draining_since.is_none() && !session_draining && !active.values().any(|held| *held.cancel.borrow())).await?;
                }
                _ = stop.changed(), if draining_since.is_none() => {
                    draining_since = Some(tokio::time::Instant::now());
                    send_control(&session.control, v2::worker_message::Msg::Draining(v2::Draining {})).await?;
                }
            }
        }
        Ok(())
        }.await;
        if let Err(error) = connected {
            if permanent_error(&error) {
                return Err(error);
            }
            tracing::warn!(%error, "worker session interrupted; reconnecting");
        }
        reconnect_backoff(config.reconnect_interval, stop).await;
    }
}

struct CompletedAttempt {
    lease_id: String,
    cancelled: bool,
    result: Result<()>,
}

fn finish_execution(
    completed: std::result::Result<(tokio::task::Id, Result<()>), tokio::task::JoinError>,
    task_leases: &mut HashMap<tokio::task::Id, String>,
    active: &mut HashMap<String, ActiveAttempt>,
) -> Result<CompletedAttempt> {
    let (id, result) = match completed {
        Ok(result) => result,
        Err(error) => (error.id(), Err(error.into())),
    };
    let lease_id = task_leases
        .remove(&id)
        .context("execution has no registered lease")?;
    let cancelled = active
        .remove(&lease_id)
        .is_some_and(|held| *held.cancel.borrow());
    Ok(CompletedAttempt {
        lease_id,
        cancelled,
        result,
    })
}

fn retired_attempt(error: &anyhow::Error) -> bool {
    error.downcast_ref::<tonic::Status>().is_some_and(|status| {
        matches!(
            status.code(),
            tonic::Code::FailedPrecondition | tonic::Code::Aborted
        )
    })
}

async fn send_control(
    control: &tokio::sync::mpsc::Sender<v2::WorkerMessage>,
    msg: v2::worker_message::Msg,
) -> Result<()> {
    control
        .send(v2::WorkerMessage { msg: Some(msg) })
        .await
        .context("worker control connection closed")
}

async fn send_credit(
    control: &tokio::sync::mpsc::Sender<v2::WorkerMessage>,
    slots: u32,
    active: usize,
    accepting: bool,
) -> Result<()> {
    send_control(
        control,
        v2::worker_message::Msg::Credit(v2::Credit {
            free_slots: if accepting {
                slots.saturating_sub(active as u32)
            } else {
                0
            },
        }),
    )
    .await
}

async fn health_loop(context: WorkerContext, healthy: watch::Sender<bool>) {
    let Some(url) = &context.config.health_url else {
        std::future::pending::<()>().await;
        return;
    };
    let mut interval = tokio::time::interval(context.config.health_interval);
    loop {
        interval.tick().await;
        let ok = context
            .http
            .get(url)
            .timeout(context.config.health_interval)
            .send()
            .await
            .is_ok_and(|response| response.status().is_success());
        healthy.send_if_modified(|value| std::mem::replace(value, ok) != ok);
    }
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("install SIGTERM handler");
        tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = terminate.recv() => {} }
    }
    #[cfg(not(unix))]
    let _ = tokio::signal::ctrl_c().await;
}

fn permanent_error(error: &anyhow::Error) -> bool {
    error.downcast_ref::<tonic::Status>().is_some_and(|status| {
        matches!(
            status.code(),
            tonic::Code::Unauthenticated
                | tonic::Code::PermissionDenied
                | tonic::Code::InvalidArgument
                | tonic::Code::AlreadyExists
                | tonic::Code::Unimplemented
        )
    })
}
async fn reconnect_backoff(delay: Duration, stop: &mut watch::Receiver<bool>) {
    if *stop.borrow() {
        tokio::time::sleep(delay).await;
    } else {
        tokio::select! { _ = tokio::time::sleep(delay) => {}, _ = stop.changed() => {} }
    }
}
fn local_capacity(
    config: &WorkerConfig,
    active: &HashMap<String, ActiveAttempt>,
    needed: &HashMap<String, i64>,
) -> bool {
    needed.iter().all(|(name, needed)| {
        let used = active
            .values()
            .map(|held| held.resources.get(name).copied().unwrap_or(0))
            .fold(0i64, i64::saturating_add);
        config
            .capacity
            .get(name)
            .copied()
            .unwrap_or(0)
            .saturating_sub(used)
            >= *needed
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_panicked_task_releases_its_local_lease_and_capacity() {
        let mut tasks: JoinSet<Result<()>> = JoinSet::new();
        let task = tasks.spawn(async { panic!("controlled worker panic") });
        let mut leases = HashMap::from([(task.id(), "lease".into())]);
        let mut active = HashMap::from([(
            "lease".into(),
            ActiveAttempt {
                cancel: watch::channel(false).0,
                resources: HashMap::from([("slot".into(), 1)]),
            },
        )]);
        let completed = finish_execution(
            tasks.join_next_with_id().await.unwrap(),
            &mut leases,
            &mut active,
        )
        .unwrap();
        assert_eq!(completed.lease_id, "lease");
        assert!(completed.result.is_err());
        assert!(!retired_attempt(completed.result.as_ref().unwrap_err()));
        assert!(leases.is_empty());
        assert!(active.is_empty());
    }
    struct FaultWorker {
        inner: anyserve_core::worker_service::WorkerGrpcService,
        fail: std::sync::atomic::AtomicBool,
        reject_assignment: Arc<std::sync::atomic::AtomicBool>,
    }

    #[tonic::async_trait]
    impl v2::worker_server::Worker for FaultWorker {
        type SessionStream = <anyserve_core::worker_service::WorkerGrpcService as v2::worker_server::Worker>::SessionStream;
        type AttemptStream = <anyserve_core::worker_service::WorkerGrpcService as v2::worker_server::Worker>::AttemptStream;
        async fn session(
            &self,
            request: tonic::Request<tonic::Streaming<v2::WorkerMessage>>,
        ) -> Result<tonic::Response<Self::SessionStream>, tonic::Status> {
            use futures::StreamExt;
            let response = v2::worker_server::Worker::session(&self.inner, request).await?;
            let reject = Arc::clone(&self.reject_assignment);
            let stream = response.into_inner().map(move |mut message| {
                if let Ok(envelope) = &mut message
                    && let Some(v2::server_message::Msg::Assign(assignment)) = &mut envelope.msg
                    && reject.swap(false, std::sync::atomic::Ordering::SeqCst)
                {
                    assignment.reserved_capacity.insert("unavailable".into(), 1);
                }
                message
            });
            Ok(tonic::Response::new(Box::pin(stream)))
        }
        async fn attempt(
            &self,
            request: tonic::Request<tonic::Streaming<v2::AttemptUp>>,
        ) -> Result<tonic::Response<Self::AttemptStream>, tonic::Status> {
            if self.fail.swap(false, std::sync::atomic::Ordering::SeqCst) {
                return Err(tonic::Status::invalid_argument(
                    "injected non-retryable data-link error",
                ));
            }
            v2::worker_server::Worker::attempt(&self.inner, request).await
        }
    }

    #[tokio::test]
    async fn a_failed_data_link_is_forgotten_on_reconnect_and_the_job_can_retry() {
        run_fault(false).await;
    }

    #[tokio::test]
    async fn a_rejected_assignment_refreshes_credit_for_the_next_job() {
        run_fault(true).await;
    }

    async fn run_fault(reject: bool) {
        use crate::test_support::{Backend, temporary_path};
        use anyserve_core::runtime::{
            Runtime,
            api::{Delivery, Demand, InputMode, InvokeRequest, JobSpec, Object, RuntimeConfig},
        };
        use tokio_stream::wrappers::TcpListenerStream;
        let runtime = Runtime::start(RuntimeConfig {
            default_initial_backoff: Duration::from_millis(5),
            ..Default::default()
        })
        .await
        .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let service = FaultWorker {
            inner: anyserve_core::worker_service::WorkerGrpcService::new(runtime.clone()),
            fail: std::sync::atomic::AtomicBool::new(!reject),
            reject_assignment: Arc::new(std::sync::atomic::AtomicBool::new(reject)),
        };
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(v2::worker_server::WorkerServer::new(service))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        let backend = Backend::start(axum::Router::new().route(
            "/act",
            axum::routing::post(|body: axum::body::Bytes| async move { body }),
        ))
        .await;
        let path = temporary_path("worker-reconnect.toml");
        std::fs::write(
            &path,
            format!(
                "kind='http'\nbase_url={:?}\n[routes]\n'policy.v1'='/act'\n",
                backend.url
            ),
        )
        .unwrap();
        let mut config = load_config(&WorkerArgs {
            config: path.clone(),
            worker_id: None,
        })
        .unwrap();
        std::fs::remove_file(path).unwrap();
        config.reconnect_interval = Duration::from_millis(5);
        let context = WorkerContext {
            http: build_http_client(&config).unwrap(),
            config: Arc::new(config),
        };
        let (_health_tx, mut healthy) = watch::channel(true);
        let (stop_tx, mut stop) = watch::channel(false);
        let worker = tokio::spawn(async move {
            let worker_id = context.config.worker_id.clone();
            session_loop(&endpoint, &worker_id, context, &mut healthy, &mut stop).await
        });
        use anyserve_core::runtime::api::{ExecutionPolicy, JobState};
        let request = |max_attempts| InvokeRequest {
            spec: JobSpec {
                demand: Demand {
                    interface: "policy.v1".into(),
                    ..Default::default()
                },
                inputs: vec![Object::inline(b"ok".as_slice())],
                policy: ExecutionPolicy {
                    max_attempts,
                    ..Default::default()
                },
                ..Default::default()
            },
            input: InputMode::Complete,
            delivery: Delivery::Buffered,
        };
        let execution = runtime.invoke(request(reject.then_some(1))).await.unwrap();
        let mut result = tokio::time::timeout(
            Duration::from_secs(4),
            crate::serve_execution::collect(execution),
        )
        .await
        .expect("failed lease was not released")
        .unwrap();
        if reject {
            assert_eq!(result.state, JobState::Failed);
            let execution = runtime.invoke(request(None)).await.unwrap();
            result = tokio::time::timeout(
                Duration::from_secs(4),
                crate::serve_execution::collect(execution),
            )
            .await
            .expect("reject did not restore advertised credit")
            .unwrap();
        }
        assert_eq!(result.attempts, if reject { 1 } else { 2 });
        assert!(
            matches!(result.outputs.first(), Some(Object::Inline { data, .. }) if data.as_ref() == b"ok")
        );
        stop_tx.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(2), worker)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        server.abort();
        runtime.shutdown().await;
    }
}
