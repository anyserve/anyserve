//! Outbound edge/sidecar worker bridge connecting local HTTP/SSE inference
//! servers (vLLM, Ollama, TRT-LLM, Jetson services) to an Anyserve control plane.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use anyserve_client::TOKEN_ENV;
use anyserve_client::execution::{Attempt, AttemptClient, WorkerSession, v2};
use bytes::Bytes;
use clap::Args;
use tokio::sync::{oneshot, watch};
use tracing::{debug, info, warn};

const PERMANENT_CODES: &[tonic::Code] = &[
    tonic::Code::InvalidArgument,
    tonic::Code::PermissionDenied,
    tonic::Code::Unauthenticated,
];

#[derive(Args, Debug, Clone)]
pub struct ConnectArgs {
    /// Interfaces served by this worker (repeatable).
    #[arg(long = "interface", required = true)]
    pub interfaces: Vec<String>,
    /// Local HTTP endpoint to POST inference requests to.
    #[arg(long)]
    pub upstream: String,
    /// Optional HTTP health URL probed before registering with the control plane.
    #[arg(long)]
    pub health_url: Option<String>,
    /// Stable worker identifier (defaults to <hostname>-<pid>).
    #[arg(long)]
    pub worker_id: Option<String>,
    /// Logical scheduling pool (e.g. jetson-local, site-rack, cloud-burst).
    #[arg(long, default_value = "")]
    pub pool: String,
    /// Bearer token for control-plane authentication (falls back to ANYSERVE_TOKEN).
    #[arg(long)]
    pub token: Option<String>,
    /// Tenant namespace header (falls back to ANYSERVE_NAMESPACE).
    #[arg(long)]
    pub namespace: Option<String>,
    /// Maximum concurrent attempts executed by this bridge.
    #[arg(long, default_value_t = 4)]
    pub max_concurrency: u32,
    /// Worker attribute key=value pairs (repeatable).
    #[arg(long = "attr", value_parser = parse_kv)]
    pub attributes: Vec<(String, String)>,
    /// Worker capacity key=integer pairs (repeatable).
    #[arg(long = "capacity", value_parser = parse_cap)]
    pub capacity: Vec<(String, i64)>,
    /// Stream HTTP response chunks / SSE `data:` payloads as output frames.
    #[arg(long)]
    pub stream_sse: bool,
    /// Seconds to wait before reconnecting after transient control-plane failures.
    #[arg(long, default_value_t = 1.0)]
    pub reconnect_interval_secs: f64,
    /// Timeout for HTTP health checks and upstream requests in seconds.
    #[arg(long, default_value_t = 300.0)]
    pub request_timeout_secs: f64,
    /// Interval in seconds for continuous background --health-url probing.
    #[arg(long, default_value_t = 2.0)]
    pub health_interval_secs: f64,
}

fn parse_kv(raw: &str) -> Result<(String, String), String> {
    let (k, v) = raw
        .split_once('=')
        .ok_or_else(|| format!("expected KEY=VALUE, got '{raw}'"))?;
    if k.trim().is_empty() {
        return Err("attribute key cannot be empty".into());
    }
    Ok((k.trim().to_string(), v.to_string()))
}

fn parse_cap(raw: &str) -> Result<(String, i64), String> {
    let (k, v) = raw
        .split_once('=')
        .ok_or_else(|| format!("expected KEY=INTEGER, got '{raw}'"))?;
    let val = v
        .trim()
        .parse::<i64>()
        .map_err(|e| format!("invalid integer capacity '{v}': {e}"))?;
    Ok((k.trim().to_string(), val))
}

fn default_worker_id() -> String {
    let host = std::env::var("HOSTNAME")
        .or_else(|_| std::fs::read_to_string("/etc/hostname").map(|s| s.trim().to_string()))
        .unwrap_or_else(|_| "worker".to_string());
    format!("{host}-{}", std::process::id())
}

async fn wait_for_upstream_health(client: &reqwest::Client, url: &str) -> Result<()> {
    for attempt in 1..=30 {
        match client.get(url).timeout(Duration::from_secs(3)).send().await {
            Ok(resp) if resp.status().is_success() => {
                info!(url, "upstream health probe ready");
                return Ok(());
            }
            Ok(resp) => debug!(url, status = %resp.status(), attempt, "waiting for health probe"),
            Err(err) => debug!(url, %err, attempt, "waiting for health probe"),
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    bail!("upstream health probe at {url} did not become ready in time")
}

async fn wait_shutdown_signal() {
    #[cfg(unix)]
    {
        if let Ok(mut terminate) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = terminate.recv() => {}
            }
            return;
        }
    }
    let _ = tokio::signal::ctrl_c().await;
}

pub async fn run_connect(endpoint: &str, args: ConnectArgs) -> Result<()> {
    let (sig_tx, sig_rx) = watch::channel(false);
    tokio::spawn(async move {
        wait_shutdown_signal().await;
        let _ = sig_tx.send(true);
    });
    run_connect_with_shutdown(endpoint, args, sig_rx).await
}

pub async fn run_connect_with_shutdown(
    endpoint: &str,
    args: ConnectArgs,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    if args.max_concurrency == 0 {
        bail!("--max-concurrency must be at least 1");
    }
    let token = args.token.clone().or_else(|| std::env::var(TOKEN_ENV).ok());
    let namespace = args
        .namespace
        .clone()
        .or_else(|| std::env::var(anyserve_client::NAMESPACE_ENV).ok());
    let worker_id = args.worker_id.clone().unwrap_or_else(default_worker_id);

    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs_f64(args.request_timeout_secs.max(1.0)))
        .build()
        .context("build HTTP client")?;

    if let Some(health_url) = &args.health_url {
        tokio::select! {
            _ = shutdown.changed(), if !*shutdown.borrow() => {
                if *shutdown.borrow() {
                    return Ok(());
                }
            }
            res = wait_for_upstream_health(&http, health_url) => res?,
        }
    }

    let mut attributes: HashMap<String, String> = args.attributes.iter().cloned().collect();
    attributes
        .entry("worker_id".to_string())
        .or_insert_with(|| worker_id.clone());
    let mut capacity: HashMap<String, i64> = args.capacity.iter().cloned().collect();
    capacity
        .entry("slot".to_string())
        .or_insert(i64::from(args.max_concurrency));

    let active_aborts: Arc<Mutex<HashMap<String, oneshot::Sender<()>>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let active_jobs_count: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
    let upstream_healthy = Arc::new(AtomicBool::new(true));
    let mut resume_session_id = String::new();

    info!(
        endpoint,
        upstream = %args.upstream,
        worker_id = %worker_id,
        pool = %args.pool,
        interfaces = ?args.interfaces,
        "starting anyserve connect HTTP bridge"
    );

    loop {
        if *shutdown.borrow() {
            return Ok(());
        }
        let held_lease_ids: Vec<String> = {
            active_jobs_count
                .lock()
                .expect("active leases poisoned")
                .iter()
                .cloned()
                .collect()
        };
        let hello = v2::Hello {
            worker_id: worker_id.clone(),
            interfaces: args.interfaces.clone(),
            attributes: attributes.clone(),
            total_capacity: capacity.clone(),
            max_concurrency: args.max_concurrency,
            sdk_version: env!("CARGO_PKG_VERSION").to_string(),
            resume_session_id: resume_session_id.clone(),
            held_lease_ids,
            metadata: Default::default(),
            pool: args.pool.clone(),
        };

        let mut session = match WorkerSession::connect_with_namespace(
            endpoint,
            token.as_deref(),
            namespace.as_deref(),
            hello,
        )
        .await
        {
            Ok(session) => session,
            Err(err) => {
                if let Some(status) = err.downcast_ref::<tonic::Status>()
                    && PERMANENT_CODES.contains(&status.code())
                {
                    return Err(anyhow!(
                        "control plane rejected worker: {}",
                        status.message()
                    ));
                }
                warn!(%err, "control-plane connection failed; retrying");
                tokio::select! {
                    _ = shutdown.changed() => {
                        if *shutdown.borrow() {
                            return Ok(());
                        }
                    }
                    _ = tokio::time::sleep(Duration::from_secs_f64(args.reconnect_interval_secs.max(0.1))) => continue,
                }
                continue;
            }
        };

        let welcome = match session.messages.message().await? {
            Some(v2::ServerMessage {
                msg: Some(v2::server_message::Msg::Welcome(welcome)),
            }) => welcome,
            _ => {
                warn!("worker session did not start with Welcome; reconnecting");
                continue;
            }
        };
        resume_session_id = welcome.session_id.clone();
        let limits = welcome
            .limits
            .ok_or_else(|| anyhow!("Welcome missingLimits"))?;
        let heartbeat_timeout =
            Duration::from_millis(u64::from(welcome.heartbeat_timeout_ms.max(1000)));
        let hb_interval = Duration::from_millis(u64::from(welcome.heartbeat_interval_ms.max(100)));

        let draining = Arc::new(AtomicBool::new(false));
        let send_credit = |control: &tokio::sync::mpsc::Sender<v2::WorkerMessage>,
                           active: &Arc<Mutex<HashSet<String>>>,
                           draining_flag: &AtomicBool,
                           healthy_flag: &AtomicBool| {
            let busy = active.lock().expect("active leases poisoned").len() as u32;
            let free =
                if draining_flag.load(Ordering::Acquire) || !healthy_flag.load(Ordering::Acquire) {
                    0
                } else {
                    args.max_concurrency.saturating_sub(busy)
                };
            let _ = control.try_send(v2::WorkerMessage {
                msg: Some(v2::worker_message::Msg::Credit(v2::Credit {
                    free_slots: free,
                })),
            });
        };

        send_credit(
            &session.control,
            &active_jobs_count,
            &draining,
            &upstream_healthy,
        );

        let hb_control = session.control.clone();
        let hb_task = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(hb_interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                ticker.tick().await;
                if hb_control
                    .send(v2::WorkerMessage {
                        msg: Some(v2::worker_message::Msg::Heartbeat(v2::Heartbeat {
                            load: None,
                        })),
                    })
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });

        let probe_task = args.health_url.clone().map(|health_url| {
            let http = http.clone();
            let interval = Duration::from_secs_f64(args.health_interval_secs.max(0.05));
            let healthy_flag = Arc::clone(&upstream_healthy);
            let draining_flag = Arc::clone(&draining);
            let control = session.control.clone();
            let active = Arc::clone(&active_jobs_count);
            let max_conc = args.max_concurrency;
            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(interval);
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                ticker.tick().await;
                loop {
                    ticker.tick().await;
                    let ok = match http
                        .get(&health_url)
                        .timeout(Duration::from_secs(3))
                        .send()
                        .await
                    {
                        Ok(resp) => resp.status().is_success(),
                        Err(_) => false,
                    };
                    let prev = healthy_flag.swap(ok, Ordering::SeqCst);
                    if prev != ok && !draining_flag.load(Ordering::Acquire) {
                        let free = if ok {
                            let busy = active.lock().expect("active leases poisoned").len() as u32;
                            max_conc.saturating_sub(busy)
                        } else {
                            0
                        };
                        if control
                            .send(v2::WorkerMessage {
                                msg: Some(v2::worker_message::Msg::Credit(v2::Credit {
                                    free_slots: free,
                                })),
                            })
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                }
            })
        });

        let session_result: Result<bool> = loop {
            if *shutdown.borrow() {
                preemptive_drain(&session.control, &draining, &active_aborts).await;
                break Ok(true);
            }
            tokio::select! {
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        preemptive_drain(&session.control, &draining, &active_aborts).await;
                        break Ok(true);
                    }
                }
                msg = session.messages.message() => {
                    let msg = match msg {
                        Ok(Some(m)) => m,
                        Ok(None) => break Err(anyhow!("worker session closed by server")),
                        Err(status) => {
                            if PERMANENT_CODES.contains(&status.code()) {
                                hb_task.abort();
                                if let Some(t) = &probe_task { t.abort(); }
                                return Err(anyhow!("permanent session error: {}", status.message()));
                            }
                            break Err(anyhow!("worker session error: {status}"));
                        }
                    };
                    match msg.msg {
                        Some(v2::server_message::Msg::Assign(assign)) => {
                            let lease_id = assign.lease_id.clone();
                            let busy = active_jobs_count.lock().expect("active poisoned").len() as u32;
                            let accepted = !draining.load(Ordering::Acquire)
                                && upstream_healthy.load(Ordering::Acquire)
                                && busy < args.max_concurrency;
                            let _ = session
                                .control
                                .send(v2::WorkerMessage {
                                    msg: Some(v2::worker_message::Msg::AssignReply(
                                        v2::AssignReply {
                                            lease_id: lease_id.clone(),
                                            accepted,
                                            reject_reason: if accepted {
                                                String::new()
                                            } else {
                                                "bridge concurrency full, unhealthy, or draining".to_string()
                                            },
                                        },
                                    )),
                                })
                                .await;
                            if !accepted {
                                send_credit(&session.control, &active_jobs_count, &draining, &upstream_healthy);
                                continue;
                            }
                            active_jobs_count
                                .lock()
                                .expect("active poisoned")
                                .insert(lease_id.clone());
                            let (abort_tx, abort_rx) = oneshot::channel();
                            active_aborts
                                .lock()
                                .expect("abort map poisoned")
                                .insert(lease_id.clone(), abort_tx);

                            let mut attempt_client = session.data.clone();
                            let http_client = http.clone();
                            let upstream = args.upstream.clone();
                            let stream_sse = args.stream_sse;
                            let control = session.control.clone();
                            let active_set = Arc::clone(&active_jobs_count);
                            let abort_map = Arc::clone(&active_aborts);
                            let draining_flag = Arc::clone(&draining);
                            let healthy_flag = Arc::clone(&upstream_healthy);
                            let max_conc = args.max_concurrency;

                            tokio::spawn(async move {
                                let _cancelled = execute_upstream_attempt(
                                    &mut attempt_client,
                                    assign,
                                    &limits,
                                    heartbeat_timeout,
                                    &http_client,
                                    &upstream,
                                    stream_sse,
                                    abort_rx,
                                )
                                .await;
                                abort_map.lock().expect("abort map poisoned").remove(&lease_id);
                                let remaining = {
                                    let mut guard = active_set.lock().expect("active poisoned");
                                    guard.remove(&lease_id);
                                    guard.len() as u32
                                };
                                let free = if draining_flag.load(Ordering::Acquire)
                                    || !healthy_flag.load(Ordering::Acquire)
                                {
                                    0
                                } else {
                                    max_conc.saturating_sub(remaining)
                                };
                                let _ = control
                                    .send(v2::WorkerMessage {
                                        msg: Some(v2::worker_message::Msg::Credit(v2::Credit {
                                            free_slots: free,
                                        })),
                                    })
                                    .await;
                            });
                        }
                        Some(v2::server_message::Msg::Cancel(cancel)) => {
                            if let Some(tx) = active_aborts
                                .lock()
                                .expect("abort map poisoned")
                                .remove(&cancel.lease_id)
                            {
                                let _ = tx.send(());
                            }
                            let _ = session
                                .control
                                .send(v2::WorkerMessage {
                                    msg: Some(v2::worker_message::Msg::CancelAck(v2::CancelAck {
                                        lease_id: cancel.lease_id,
                                    })),
                                })
                                .await;
                        }
                        Some(v2::server_message::Msg::Fence(fence)) => {
                            if let Some(tx) = active_aborts
                                .lock()
                                .expect("abort map poisoned")
                                .remove(&fence.lease_id)
                            {
                                let _ = tx.send(());
                            }
                        }
                        Some(v2::server_message::Msg::Drain(_)) => {
                            draining.store(true, Ordering::Release);
                            send_credit(&session.control, &active_jobs_count, &draining, &upstream_healthy);
                        }
                        _ => {}
                    }
                }
            }
        };

        hb_task.abort();
        if let Some(t) = probe_task {
            t.abort();
        }
        match session_result {
            Ok(true) => return Ok(()),
            Ok(false) => continue,
            Err(err) => {
                warn!(%err, "reconnecting outbound worker bridge");
                tokio::select! {
                    _ = shutdown.changed() => {
                        if *shutdown.borrow() {
                            return Ok(());
                        }
                    }
                    _ = tokio::time::sleep(Duration::from_secs_f64(
                        args.reconnect_interval_secs.max(0.1),
                    )) => {}
                }
            }
        }
    }
}

async fn preemptive_drain(
    control: &tokio::sync::mpsc::Sender<v2::WorkerMessage>,
    draining: &AtomicBool,
    active_aborts: &Mutex<HashMap<String, oneshot::Sender<()>>>,
) {
    info!("received shutdown signal; sending preemptive draining");
    draining.store(true, Ordering::Release);
    let _ = control
        .send(v2::WorkerMessage {
            msg: Some(v2::worker_message::Msg::Draining(v2::Draining {
                preempting: true,
            })),
        })
        .await;
    let aborts: Vec<_> = active_aborts
        .lock()
        .expect("abort map poisoned")
        .drain()
        .map(|(_, tx)| tx)
        .collect();
    for tx in aborts {
        let _ = tx.send(());
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
}

#[allow(clippy::too_many_arguments)]
async fn execute_upstream_attempt(
    data_client: &mut AttemptClient,
    assign: v2::Assign,
    limits: &v2::Limits,
    heartbeat_timeout: Duration,
    http: &reqwest::Client,
    upstream: &str,
    force_sse: bool,
    mut abort_rx: oneshot::Receiver<()>,
) -> bool {
    let mut attempt = match Attempt::open(data_client, &assign, limits, heartbeat_timeout).await {
        Ok(attempt) => attempt,
        Err(err) => {
            warn!(lease_id = %assign.lease_id, %err, "failed to open Attempt stream");
            return false;
        }
    };
    if attempt.terminal_replay {
        return false;
    }

    let mut payload = Vec::new();
    let mut content_type = None;
    for obj in &attempt.inputs {
        if let Some(ct) = obj.metadata.get("content_type") {
            content_type = Some(ct.clone());
        }
        match &obj.body {
            Some(v2::object::Body::Inline(bytes)) => payload.extend_from_slice(bytes),
            Some(v2::object::Body::Uri(_)) => {
                let _ = attempt
                    .finish(
                        &[],
                        v2::finish::Outcome::Failed(v2::ErrorInfo {
                            code: "unsupported_uri_input".to_string(),
                            message: "anyserve connect forwards inline byte payloads only"
                                .to_string(),
                            retryable: false,
                        }),
                    )
                    .await;
                return false;
            }
            None => {}
        }
    }
    if payload.is_empty() && !attempt.params.is_empty() {
        payload.extend_from_slice(&attempt.params);
    }

    let mut req = http
        .post(upstream)
        .header("x-anyserve-job-id", &assign.job_id)
        .header("x-anyserve-attempt-id", &assign.attempt_id)
        .header("x-anyserve-interface", &assign.interface)
        .body(payload);
    if let Some(ct) = content_type {
        req = req.header(reqwest::header::CONTENT_TYPE, ct);
    } else if force_sse {
        req = req.header(reqwest::header::CONTENT_TYPE, "application/json");
    }

    let resp = tokio::select! {
        _ = &mut abort_rx => return true,
        res = req.send() => match res {
            Ok(resp) => resp,
            Err(err) => {
                let _ = attempt.finish(
                    &[],
                    v2::finish::Outcome::Failed(v2::ErrorInfo {
                        code: "upstream_connect_error".to_string(),
                        message: err.to_string(),
                        retryable: true,
                    }),
                ).await;
                return false;
            }
        }
    };

    let status = resp.status();
    if !status.is_success() {
        let retryable =
            status.is_server_error() || status == reqwest::StatusCode::TOO_MANY_REQUESTS;
        let body_text = resp.text().await.unwrap_or_default();
        let _ = attempt
            .finish(
                &[],
                v2::finish::Outcome::Failed(v2::ErrorInfo {
                    code: format!("http_{}", status.as_u16()),
                    message: if body_text.is_empty() {
                        status.to_string()
                    } else {
                        body_text
                    },
                    retryable,
                }),
            )
            .await;
        return false;
    }

    let is_sse = force_sse
        || resp
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|ct| ct.contains("text/event-stream"));

    if is_sse {
        let mut resp = resp;
        let mut buffer = Vec::new();
        loop {
            let chunk = tokio::select! {
                _ = &mut abort_rx => return true,
                next = resp.chunk() => match next {
                    Ok(Some(bytes)) => bytes,
                    Ok(None) => break,
                    Err(err) => {
                        let _ = attempt.finish(
                            &[],
                            v2::finish::Outcome::Failed(v2::ErrorInfo {
                                code: "upstream_stream_error".to_string(),
                                message: err.to_string(),
                                retryable: true,
                            }),
                        ).await;
                        return false;
                    }
                }
            };
            buffer.extend_from_slice(&chunk);
            while let Some(pos) = buffer.iter().position(|&b| b == b'\n') {
                let line: Vec<u8> = buffer.drain(..=pos).collect();
                let trimmed = line.strip_suffix(b"\n").unwrap_or(&line);
                let trimmed = trimmed.strip_suffix(b"\r").unwrap_or(trimmed);
                if trimmed.is_empty() {
                    continue;
                }
                let frame_payload = trimmed
                    .strip_prefix(b"data:")
                    .unwrap_or(trimmed)
                    .trim_ascii();
                if frame_payload == b"[DONE]" {
                    continue;
                }
                if !frame_payload.is_empty() {
                    let mut bytes = Bytes::copy_from_slice(frame_payload);
                    if !bytes.ends_with(b"\n") {
                        let mut v = bytes.to_vec();
                        v.push(b'\n');
                        bytes = Bytes::from(v);
                    }
                    if attempt
                        .frame(bytes, Default::default(), false)
                        .await
                        .is_err()
                    {
                        return false;
                    }
                }
            }
        }
        if !buffer.is_empty() {
            let _ = attempt
                .frame(Bytes::from(buffer), Default::default(), false)
                .await;
        }
        let _ = attempt
            .finish(&[], v2::finish::Outcome::Succeeded(v2::Succeeded {}))
            .await;
    } else {
        let bytes = tokio::select! {
            _ = &mut abort_rx => return true,
            res = resp.bytes() => match res {
                Ok(b) => b,
                Err(err) => {
                    let _ = attempt.finish(
                        &[],
                        v2::finish::Outcome::Failed(v2::ErrorInfo {
                            code: "upstream_read_error".to_string(),
                            message: err.to_string(),
                            retryable: true,
                        }),
                    ).await;
                    return false;
                }
            }
        };
        let outputs = vec![v2::Object {
            body: Some(v2::object::Body::Inline(bytes)),
            metadata: Default::default(),
        }];
        let _ = attempt
            .finish(&outputs, v2::finish::Outcome::Succeeded(v2::Succeeded {}))
            .await;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::Infallible;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

    use anyserve_client::execution::{ExecutionClient, v2};
    use anyserve_core::auth::AuthPolicy;
    use anyserve_core::runtime::api::RuntimeConfig;
    use axum::body::Body;
    use axum::http::{HeaderMap, StatusCode};
    use axum::response::Response;
    use axum::routing::{get, post};
    use tokio_stream::wrappers::UnboundedReceiverStream;

    use crate::test_support::{Server, Worker};

    fn test_args(upstream: String, health_url: Option<String>, stream_sse: bool) -> ConnectArgs {
        ConnectArgs {
            interfaces: vec!["llm.chat".to_string()],
            upstream,
            health_url,
            worker_id: Some("bridge-test".to_string()),
            pool: "edge-local".to_string(),
            token: None,
            namespace: None,
            max_concurrency: 2,
            attributes: Vec::new(),
            capacity: Vec::new(),
            stream_sse,
            reconnect_interval_secs: 0.1,
            request_timeout_secs: 5.0,
            health_interval_secs: 0.1,
        }
    }

    fn simple_spec(payload: &[u8]) -> v2::JobSpec {
        v2::JobSpec {
            demand: Some(v2::Demand {
                interface: "llm.chat".to_string(),
                ..Default::default()
            }),
            inputs: vec![v2::Object {
                body: Some(v2::object::Body::Inline(Bytes::copy_from_slice(payload))),
                metadata: HashMap::new(),
            }],
            policy: Some(v2::ExecutionPolicy {
                durability: v2::Durability::Ephemeral as i32,
                max_attempts: 2,
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    async fn spawn_mock_http(router: axum::Router) -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        (format!("http://{addr}"), task)
    }

    #[tokio::test]
    async fn connect_http_post_roundtrip_and_headers() {
        let router = axum::Router::new().route(
            "/infer",
            post(|headers: HeaderMap, body: Bytes| async move {
                let job_id = headers
                    .get("x-anyserve-job-id")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("")
                    .to_string();
                let iface = headers
                    .get("x-anyserve-interface")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("")
                    .to_string();
                format!("{iface}:{job_id}:{}", String::from_utf8_lossy(&body))
            }),
        );
        let (http_url, http_task) = spawn_mock_http(router).await;
        let server = Server::start(RuntimeConfig::default(), AuthPolicy::default()).await;

        let (stop_tx, stop_rx) = watch::channel(false);
        let endpoint = server.endpoint.clone();
        let args = test_args(format!("{http_url}/infer"), None, false);
        let bridge_task = tokio::spawn(async move {
            run_connect_with_shutdown(&endpoint, args, stop_rx)
                .await
                .unwrap();
        });

        let mut client = ExecutionClient::connect_with_token(&server.endpoint, None)
            .await
            .unwrap();
        let mut events = client.invoke(simple_spec(b"ping")).await.unwrap();
        let mut got = None;
        let mut job_id = String::new();
        while let Some(ev) = events.message().await.unwrap() {
            if job_id.is_empty() {
                job_id = ev.job_id.clone();
            }
            if let Some(v2::execution_event::Event::Finished(done)) = ev.event
                && let Some(v2::object::Body::Inline(data)) =
                    done.outputs.first().and_then(|o| o.body.as_ref())
            {
                got = Some(data.clone());
                break;
            }
        }
        assert_eq!(
            got.unwrap().as_ref(),
            format!("llm.chat:{job_id}:ping").as_bytes()
        );

        let _ = stop_tx.send(true);
        let _ = bridge_task.await;
        server.stop().await;
        http_task.abort();
    }

    #[tokio::test]
    async fn connect_sse_streaming_and_cancel_aborts_upstream() {
        let aborted = Arc::new(AtomicBool::new(false));
        let aborted_clone = Arc::clone(&aborted);
        let router = axum::Router::new()
            .route(
                "/sse",
                post(|| async move {
                    let body_stream = tokio_stream::iter(vec![
                        Ok::<_, Infallible>(Bytes::from_static(b"data: {\"tok\":\"a\"}\n\n")),
                        Ok::<_, Infallible>(Bytes::from_static(
                            b"data: {\"tok\":\"b\"}\n\ndata: [DONE]\n\n",
                        )),
                    ]);
                    Response::builder()
                        .header("content-type", "text/event-stream")
                        .body(Body::from_stream(body_stream))
                        .unwrap()
                }),
            )
            .route(
                "/slow",
                post(move || {
                    let aborted_flag = Arc::clone(&aborted_clone);
                    struct DropGuard(Arc<AtomicBool>);
                    impl Drop for DropGuard {
                        fn drop(&mut self) {
                            self.0.store(true, Ordering::SeqCst);
                        }
                    }
                    async move {
                        let (tx, rx) =
                            tokio::sync::mpsc::unbounded_channel::<Result<Bytes, Infallible>>();
                        tokio::spawn(async move {
                            let _guard = DropGuard(aborted_flag);
                            let _ = tx.send(Ok(Bytes::from_static(b"data: start\n\n")));
                            tokio::select! {
                                _ = tx.closed() => {}
                                _ = tokio::time::sleep(Duration::from_secs(10)) => {}
                            }
                        });
                        Response::builder()
                            .header("content-type", "text/event-stream")
                            .body(Body::from_stream(UnboundedReceiverStream::new(rx)))
                            .unwrap()
                    }
                }),
            );
        let (http_url, http_task) = spawn_mock_http(router).await;
        let server = Server::start(RuntimeConfig::default(), AuthPolicy::default()).await;

        let (stop_tx, stop_rx) = watch::channel(false);
        let ep = server.endpoint.clone();
        let sse_url = format!("{http_url}/sse");
        let b1 = tokio::spawn(async move {
            let _ = run_connect_with_shutdown(&ep, test_args(sse_url, None, true), stop_rx.clone())
                .await;
        });

        let mut client = ExecutionClient::connect_with_token(&server.endpoint, None)
            .await
            .unwrap();
        let mut events = client.invoke(simple_spec(b"go")).await.unwrap();
        let mut frames = Vec::new();
        while let Some(ev) = events.message().await.unwrap() {
            match ev.event {
                Some(v2::execution_event::Event::Output(out)) => frames.push(out.data),
                Some(v2::execution_event::Event::Finished(_)) => break,
                _ => {}
            }
        }
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].as_ref(), b"{\"tok\":\"a\"}\n");
        assert_eq!(frames[1].as_ref(), b"{\"tok\":\"b\"}\n");
        let _ = stop_tx.send(true);
        let _ = b1.await;

        let (stop2_tx, stop2_rx) = watch::channel(false);
        let ep2 = server.endpoint.clone();
        let slow_url = format!("{http_url}/slow");
        let b2 = tokio::spawn(async move {
            let _ =
                run_connect_with_shutdown(&ep2, test_args(slow_url, None, true), stop2_rx).await;
        });
        let mut slow_events = client.invoke(simple_spec(b"hang")).await.unwrap();
        while let Some(ev) = slow_events.message().await.unwrap() {
            if matches!(ev.event, Some(v2::execution_event::Event::Output(_))) {
                break;
            }
        }
        drop(slow_events);
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            aborted.load(Ordering::SeqCst),
            "dropping Invoke must abort upstream HTTP stream"
        );

        let _ = stop2_tx.send(true);
        let _ = b2.await;
        server.stop().await;
        http_task.abort();
    }

    #[tokio::test]
    async fn connect_http_503_retries_and_400_fails_without_retry() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let router = axum::Router::new()
            .route(
                "/flaky",
                post({
                    let attempts = Arc::clone(&attempts);
                    move || {
                        let attempts = Arc::clone(&attempts);
                        async move {
                            if attempts.fetch_add(1, AtomicOrdering::SeqCst) == 0 {
                                (StatusCode::SERVICE_UNAVAILABLE, "overloaded".to_string())
                            } else {
                                (StatusCode::OK, "recovered".to_string())
                            }
                        }
                    }
                }),
            )
            .route(
                "/bad",
                post(|| async { (StatusCode::BAD_REQUEST, "bad payload".to_string()) }),
            );
        let (http_url, http_task) = spawn_mock_http(router).await;
        let server = Server::start(
            RuntimeConfig {
                default_initial_backoff: Duration::from_millis(10),
                ..Default::default()
            },
            AuthPolicy::default(),
        )
        .await;

        let (stop_tx, stop_rx) = watch::channel(false);
        let ep = server.endpoint.clone();
        let flaky_url = format!("{http_url}/flaky");
        let b1 = tokio::spawn(async move {
            let _ =
                run_connect_with_shutdown(&ep, test_args(flaky_url, None, false), stop_rx).await;
        });

        let mut client = ExecutionClient::connect_with_token(&server.endpoint, None)
            .await
            .unwrap();
        let mut evs = client.invoke(simple_spec(b"x")).await.unwrap();
        let mut final_out = None;
        while let Some(ev) = evs.message().await.unwrap() {
            if let Some(v2::execution_event::Event::Finished(done)) = ev.event {
                final_out = done.outputs.into_iter().next();
                break;
            }
        }
        assert_eq!(attempts.load(AtomicOrdering::SeqCst), 2);
        assert!(matches!(
            final_out.and_then(|o| o.body),
            Some(v2::object::Body::Inline(b)) if b.as_ref() == b"recovered"
        ));
        let _ = stop_tx.send(true);
        let _ = b1.await;

        let (stop2_tx, stop2_rx) = watch::channel(false);
        let ep2 = server.endpoint.clone();
        let bad_url = format!("{http_url}/bad");
        let b2 = tokio::spawn(async move {
            let _ =
                run_connect_with_shutdown(&ep2, test_args(bad_url, None, false), stop2_rx).await;
        });
        let mut bad_evs = client.invoke(simple_spec(b"x")).await.unwrap();
        let mut err_code = String::new();
        while let Some(ev) = bad_evs.message().await.unwrap() {
            if let Some(v2::execution_event::Event::Finished(done)) = ev.event {
                err_code = done.error.map(|e| e.code).unwrap_or_default();
                assert_eq!(done.state, v2::JobState::Failed as i32);
                break;
            }
        }
        assert_eq!(err_code, "http_400");
        let _ = stop2_tx.send(true);
        let _ = b2.await;
        server.stop().await;
        http_task.abort();
    }

    #[tokio::test]
    async fn connect_health_probe_and_preemptive_shutdown_failover() {
        let healthy = Arc::new(AtomicBool::new(true));
        let started_slow = Arc::new(tokio::sync::Notify::new());
        let router = axum::Router::new()
            .route(
                "/health",
                get({
                    let healthy = Arc::clone(&healthy);
                    move || {
                        let healthy = Arc::clone(&healthy);
                        async move {
                            if healthy.load(Ordering::Acquire) {
                                StatusCode::OK
                            } else {
                                StatusCode::SERVICE_UNAVAILABLE
                            }
                        }
                    }
                }),
            )
            .route(
                "/slow_post",
                post({
                    let started_slow = Arc::clone(&started_slow);
                    move || {
                        let started_slow = Arc::clone(&started_slow);
                        async move {
                            started_slow.notify_one();
                            tokio::time::sleep(Duration::from_secs(10)).await;
                            "late"
                        }
                    }
                }),
            );
        let (http_url, http_task) = spawn_mock_http(router).await;
        let server = Server::start(RuntimeConfig::default(), AuthPolicy::default()).await;

        let (stop_tx, stop_rx) = watch::channel(false);
        let ep = server.endpoint.clone();
        let mut args = test_args(
            format!("{http_url}/slow_post"),
            Some(format!("{http_url}/health")),
            false,
        );
        args.health_interval_secs = 0.05;
        let bridge = tokio::spawn(async move {
            let _ = run_connect_with_shutdown(&ep, args, stop_rx).await;
        });

        // Wait for bridge registration + initial credit.
        for _ in 0..50 {
            if server
                .runtime
                .list_workers()
                .iter()
                .any(|w| w.worker_id == "bridge-test" && w.free_slots == 2)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        // Flip health to 503 -> free_slots drops to 0 cleanly; flip back to 200 -> recovers.
        healthy.store(false, Ordering::Release);
        for _ in 0..40 {
            if server
                .runtime
                .list_workers()
                .iter()
                .any(|w| w.worker_id == "bridge-test" && w.free_slots == 0)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert_eq!(
            server
                .runtime
                .list_workers()
                .into_iter()
                .find(|w| w.worker_id == "bridge-test")
                .unwrap()
                .free_slots,
            0
        );
        healthy.store(true, Ordering::Release);
        for _ in 0..40 {
            if server
                .runtime
                .list_workers()
                .iter()
                .any(|w| w.worker_id == "bridge-test" && w.free_slots == 2)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert_eq!(
            server
                .runtime
                .list_workers()
                .into_iter()
                .find(|w| w.worker_id == "bridge-test")
                .unwrap()
                .free_slots,
            2
        );

        // Start an unexposed invocation on bridge, then signal shutdown (Draining{preempting=true});
        // backup worker attached after preemption must receive the rescheduled attempt immediately.
        let mut client = ExecutionClient::connect_with_token(&server.endpoint, None)
            .await
            .unwrap();
        let mut events = client.invoke(simple_spec(b"failover")).await.unwrap();
        started_slow.notified().await;

        let mut backup = Worker::connect(&server, "backup", vec!["llm.chat".into()], 1).await;
        let _ = stop_tx.send(true);
        let _ = bridge.await;

        let backup_ass = backup.assignment().await;
        let mut attempt = Attempt::open(
            &mut backup.session.data,
            &backup_ass,
            &backup.limits,
            backup.heartbeat_timeout,
        )
        .await
        .unwrap();
        let output = v2::Object {
            body: Some(v2::object::Body::Inline(Bytes::from_static(b"from-backup"))),
            metadata: HashMap::new(),
        };
        attempt
            .commit(
                &mut backup.session.data,
                &backup_ass,
                &backup.limits,
                std::slice::from_ref(&output),
                v2::finish::Outcome::Succeeded(v2::Succeeded {}),
            )
            .await
            .unwrap();
        let mut got_backup = false;
        while let Some(ev) = events.message().await.unwrap() {
            if let Some(v2::execution_event::Event::Finished(done)) = ev.event
                && matches!(
                    done.outputs.first().and_then(|o| o.body.as_ref()),
                    Some(v2::object::Body::Inline(b)) if b.as_ref() == b"from-backup"
                )
            {
                got_backup = true;
                break;
            }
        }
        assert!(got_backup);
        server.stop().await;
        http_task.abort();
    }
}
