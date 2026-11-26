use std::collections::{HashMap, VecDeque};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use futures::Stream;
use tokio::sync::{Notify, mpsc};
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::{Code, Request, Response, Status, Streaming};

use super::*;
use crate::execution::{
    Attempt, DirectAttemptError, DirectExecutionEvents, DirectWorkerSession, ExecutionClient,
    WorkerSession, v2,
};
use crate::{
    DIRECT_BYTES_METADATA_KEY, DIRECT_PATH_METADATA_KEY, ERROR_CODE_DIRECT_PUBLIC_RELAY_THROTTLED,
    ERROR_CODE_DIRECT_UNSUPPORTED, OBJECT_SHA256_METADATA_KEY, OBJECT_SIZE_METADATA_KEY,
    is_direct_uri, redact_direct_uri,
};

#[derive(Debug)]
struct AlwaysPublicRelayClassifier;

impl PathClassifier for AlwaysPublicRelayClassifier {
    fn classify(
        &self,
        _conn: &iroh::endpoint::Connection,
        _has_custom_relay: bool,
    ) -> ObservedPath {
        ObservedPath::PublicRelayOnly
    }
}

fn local_direct_config() -> DirectConfig {
    DirectConfig {
        disable_relays: true,
        bind_addr: Some(std::net::SocketAddr::from(([127, 0, 0, 1], 0))),
        relay_url: None,
        secret_key_path: None,
        threshold_bytes: 1024,
        inline_fallback_limit_bytes: 64 * 1024,
        upgrade_window: Duration::from_millis(60),
        idle_timeout: Duration::from_secs(5),
        input_token_ttl: Duration::from_secs(30),
        output_cache_max_bytes: 16 * 1024 * 1024,
        output_cache_ttl: Duration::from_secs(30),
        ..Default::default()
    }
}

/// Minimal in-process tonic gRPC relay server for hermetic end-to-end client+worker
/// tests without depending on `anyserve-core` (avoiding cyclic dev-deps).
#[derive(Clone)]
struct MiniRelay {
    state: Arc<Mutex<RelayState>>,
    notify: Arc<Notify>,
}

struct RelayState {
    max_inline_object_bytes: usize,
    workers: Vec<MiniWorker>,
    pending_jobs: VecDeque<MiniJob>,
    active_attempts: HashMap<String, ActiveAttempt>,
    next_id: u64,
    observed_inline_payload_bytes: usize,
    observed_uri_inputs: Vec<String>,
    observed_uri_outputs: Vec<String>,
}

struct MiniWorker {
    worker_id: String,
    attributes: HashMap<String, String>,
    tx: mpsc::Sender<Result<v2::ServerMessage, Status>>,
    busy: bool,
}

struct MiniJob {
    job_id: String,
    spec: v2::JobSpec,
    max_attempts: u32,
    attempt_number: u32,
    client_tx: mpsc::Sender<Result<v2::ExecutionEvent, Status>>,
}

struct ActiveAttempt {
    job: MiniJob,
    worker_idx: usize,
    outputs: Vec<v2::Object>,
}

impl MiniRelay {
    async fn start(max_inline_object_bytes: usize) -> (String, Self, tokio::task::JoinHandle<()>) {
        let relay = Self {
            state: Arc::new(Mutex::new(RelayState {
                max_inline_object_bytes,
                workers: Vec::new(),
                pending_jobs: VecDeque::new(),
                active_attempts: HashMap::new(),
                next_id: 1,
                observed_inline_payload_bytes: 0,
                observed_uri_inputs: Vec::new(),
                observed_uri_outputs: Vec::new(),
            })),
            notify: Arc::new(Notify::new()),
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let r1 = relay.clone();
        let r2 = relay.clone();
        let handle = tokio::spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(
                    v2::client_server::ClientServer::new(r1)
                        .max_decoding_message_size(anyserve_proto::MAX_MESSAGE_SIZE)
                        .max_encoding_message_size(anyserve_proto::MAX_MESSAGE_SIZE),
                )
                .add_service(
                    v2::worker_server::WorkerServer::new(r2)
                        .max_decoding_message_size(anyserve_proto::MAX_MESSAGE_SIZE)
                        .max_encoding_message_size(anyserve_proto::MAX_MESSAGE_SIZE),
                )
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await;
        });
        (format!("http://{addr}"), relay, handle)
    }

    fn dispatch(&self) {
        let mut st = self.state.lock().unwrap();
        let mut requeued = VecDeque::new();
        while let Some(mut job) = st.pending_jobs.pop_front() {
            let req_attrs = job
                .spec
                .demand
                .as_ref()
                .map(|d| &d.required_attributes)
                .cloned()
                .unwrap_or_default();
            let chosen = st.workers.iter().position(|w| {
                !w.busy
                    && !w.tx.is_closed()
                    && req_attrs
                        .iter()
                        .all(|(k, v)| w.attributes.get(k).is_some_and(|wv| wv == v))
            });
            if let Some(idx) = chosen {
                job.attempt_number += 1;
                let lease_id = format!("lease-{}", st.next_id);
                st.next_id += 1;
                let assign = v2::Assign {
                    lease_id: lease_id.clone(),
                    attempt_id: format!("att-{}", job.attempt_number),
                    job_id: job.job_id.clone(),
                    attempt_number: job.attempt_number,
                    generation: 1,
                    interface: job
                        .spec
                        .demand
                        .as_ref()
                        .map(|d| d.interface.clone())
                        .unwrap_or_default(),
                    params: job.spec.params.clone(),
                    metadata: job.spec.metadata.clone(),
                    inline_inputs: job.spec.inputs.clone(),
                    inputs_via_attempt: false,
                    streaming_input: false,
                    ..Default::default()
                };
                st.workers[idx].busy = true;
                let worker_id = st.workers[idx].worker_id.clone();
                let worker_tx = st.workers[idx].tx.clone();
                let client_tx = job.client_tx.clone();
                let job_id = job.job_id.clone();
                let attempt_number = job.attempt_number;
                st.active_attempts.insert(
                    lease_id,
                    ActiveAttempt {
                        job,
                        worker_idx: idx,
                        outputs: Vec::new(),
                    },
                );
                let _ = worker_tx.try_send(Ok(v2::ServerMessage {
                    msg: Some(v2::server_message::Msg::Assign(assign)),
                }));
                let _ = client_tx.try_send(Ok(v2::ExecutionEvent {
                    job_id,
                    sequence: 2,
                    attempt_id: format!("att-{attempt_number}"),
                    event: Some(v2::execution_event::Event::Assigned(v2::Assigned {
                        worker_id,
                        attempt_number,
                    })),
                    ..Default::default()
                }));
            } else {
                requeued.push_back(job);
            }
        }
        st.pending_jobs = requeued;
    }
}

#[tonic::async_trait]
impl v2::client_server::Client for MiniRelay {
    type InvokeStream =
        Pin<Box<dyn Stream<Item = Result<v2::ExecutionEvent, Status>> + Send + 'static>>;
    type WatchStream =
        Pin<Box<dyn Stream<Item = Result<v2::ExecutionEvent, Status>> + Send + 'static>>;

    async fn submit(
        &self,
        _: Request<v2::SubmitRequest>,
    ) -> Result<Response<v2::SubmitResponse>, Status> {
        Err(Status::unimplemented("not used"))
    }

    async fn invoke(
        &self,
        request: Request<Streaming<v2::InvokeRequest>>,
    ) -> Result<Response<Self::InvokeStream>, Status> {
        let mut inbound = request.into_inner();
        let Some(Ok(v2::InvokeRequest {
            msg: Some(v2::invoke_request::Msg::Start(start)),
        })) = inbound.message().await.transpose()
        else {
            return Err(Status::invalid_argument("missing InvokeStart"));
        };
        let spec = start
            .spec
            .ok_or_else(|| Status::invalid_argument("missing JobSpec"))?;

        {
            let mut st = self.state.lock().unwrap();
            for obj in &spec.inputs {
                match &obj.body {
                    Some(v2::object::Body::Inline(data)) => {
                        if data.len() > st.max_inline_object_bytes {
                            return Err(Status::invalid_argument(format!(
                                "inline object {} bytes exceeds max_inline_object_bytes ({})",
                                data.len(),
                                st.max_inline_object_bytes
                            )));
                        }
                        st.observed_inline_payload_bytes += data.len();
                    }
                    Some(v2::object::Body::Uri(u)) => {
                        st.observed_uri_inputs.push(u.clone());
                    }
                    None => {}
                }
            }
        }

        let (tx, rx) = mpsc::channel(16);
        let max_attempts = spec
            .policy
            .as_ref()
            .map(|p| p.max_attempts)
            .filter(|&n| n > 0)
            .unwrap_or(3);
        {
            let mut st = self.state.lock().unwrap();
            let job_id = format!("job-{}", st.next_id);
            st.next_id += 1;
            let _ = tx.try_send(Ok(v2::ExecutionEvent {
                job_id: job_id.clone(),
                sequence: 1,
                event: Some(v2::execution_event::Event::Accepted(v2::Accepted {})),
                ..Default::default()
            }));
            st.pending_jobs.push_back(MiniJob {
                job_id,
                spec,
                max_attempts,
                attempt_number: 0,
                client_tx: tx,
            });
        }
        self.dispatch();
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn watch(
        &self,
        _: Request<v2::WatchRequest>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        Err(Status::unimplemented("not used"))
    }

    async fn get_job(&self, _: Request<v2::GetJobRequest>) -> Result<Response<v2::Job>, Status> {
        Err(Status::unimplemented("not used"))
    }

    async fn cancel(&self, _: Request<v2::CancelRequest>) -> Result<Response<v2::Job>, Status> {
        Err(Status::unimplemented("not used"))
    }

    async fn list_jobs(
        &self,
        _: Request<v2::ListJobsRequest>,
    ) -> Result<Response<v2::ListJobsResponse>, Status> {
        Err(Status::unimplemented("not used"))
    }
}

#[tonic::async_trait]
impl v2::worker_server::Worker for MiniRelay {
    type SessionStream =
        Pin<Box<dyn Stream<Item = Result<v2::ServerMessage, Status>> + Send + 'static>>;
    type AttemptStream =
        Pin<Box<dyn Stream<Item = Result<v2::AttemptDown, Status>> + Send + 'static>>;

    async fn session(
        &self,
        request: Request<Streaming<v2::WorkerMessage>>,
    ) -> Result<Response<Self::SessionStream>, Status> {
        let mut inbound = request.into_inner();
        let Some(Ok(v2::WorkerMessage {
            msg: Some(v2::worker_message::Msg::Hello(hello)),
        })) = inbound.message().await.transpose()
        else {
            return Err(Status::invalid_argument("missing Hello"));
        };
        let (tx, rx) = mpsc::channel(16);
        let session_id = {
            let mut st = self.state.lock().unwrap();
            let id = format!("sess-{}", st.next_id);
            st.next_id += 1;
            st.workers.push(MiniWorker {
                worker_id: hello.worker_id,
                attributes: hello.attributes,
                tx: tx.clone(),
                busy: false,
            });
            id
        };
        let _ = tx
            .send(Ok(v2::ServerMessage {
                msg: Some(v2::server_message::Msg::Welcome(v2::Welcome {
                    session_id,
                    generation: 1,
                    heartbeat_interval_ms: 5_000,
                    heartbeat_timeout_ms: 15_000,
                    limits: Some(v2::Limits {
                        max_control_message_bytes: 256 * 1024,
                        max_chunk_bytes: 64 * 1024,
                        attempt_window_bytes: 4 * 1024 * 1024,
                        assign_inline_limit: 16 * 1024 * 1024,
                    }),
                    resumed_lease_ids: Vec::new(),
                })),
            }))
            .await;
        self.dispatch();
        let relay = self.clone();
        tokio::spawn(async move {
            while let Ok(Some(_)) = inbound.message().await {}
            relay.notify.notify_waiters();
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn attempt(
        &self,
        request: Request<Streaming<v2::AttemptUp>>,
    ) -> Result<Response<Self::AttemptStream>, Status> {
        let mut inbound = request.into_inner();
        let (tx, rx) = mpsc::channel(16);
        let relay = self.clone();
        tokio::spawn(async move {
            let Some(Ok(v2::AttemptUp {
                msg: Some(v2::attempt_up::Msg::Open(open)),
            })) = inbound.message().await.transpose()
            else {
                return;
            };
            let lease_id = open.lease_id;
            let _ = tx
                .send(Ok(v2::AttemptDown {
                    msg: Some(v2::attempt_down::Msg::Opened(v2::AttemptOpened {
                        resume_from_sequence: 0,
                        terminal_replay: false,
                    })),
                }))
                .await;

            let mut acc_outputs: HashMap<u32, (Vec<u8>, String, HashMap<String, String>)> =
                HashMap::new();
            while let Ok(Some(msg)) = inbound.message().await {
                match msg.msg {
                    Some(v2::attempt_up::Msg::Result(chunk)) => {
                        let entry = acc_outputs
                            .entry(chunk.index)
                            .or_insert_with(|| (Vec::new(), String::new(), chunk.metadata.clone()));
                        if !chunk.uri.is_empty() {
                            entry.1 = chunk.uri;
                        } else {
                            entry.0.extend_from_slice(&chunk.data);
                        }
                    }
                    Some(v2::attempt_up::Msg::Finish(fin)) => {
                        let mut ordered: Vec<(u32, v2::Object)> = acc_outputs
                            .drain()
                            .map(|(idx, (bytes, uri, meta))| {
                                let body = if !uri.is_empty() {
                                    v2::object::Body::Uri(uri)
                                } else {
                                    v2::object::Body::Inline(Bytes::from(bytes))
                                };
                                (
                                    idx,
                                    v2::Object {
                                        body: Some(body),
                                        metadata: meta,
                                    },
                                )
                            })
                            .collect();
                        ordered.sort_by_key(|(idx, _)| *idx);
                        let outputs: Vec<v2::Object> =
                            ordered.into_iter().map(|(_, o)| o).collect();

                        let mut should_dispatch = false;
                        {
                            let mut st = relay.state.lock().unwrap();
                            for o in &outputs {
                                match &o.body {
                                    Some(v2::object::Body::Inline(d)) => {
                                        st.observed_inline_payload_bytes += d.len();
                                    }
                                    Some(v2::object::Body::Uri(u)) => {
                                        st.observed_uri_outputs.push(u.clone());
                                    }
                                    None => {}
                                }
                            }
                            if let Some(mut active) = st.active_attempts.remove(&lease_id) {
                                active.outputs = outputs;
                                st.workers[active.worker_idx].busy = false;
                                match fin.outcome {
                                    Some(v2::finish::Outcome::Succeeded(_)) => {
                                        let _ =
                                            active.job.client_tx.try_send(Ok(v2::ExecutionEvent {
                                                job_id: active.job.job_id.clone(),
                                                sequence: 10,
                                                event: Some(v2::execution_event::Event::Finished(
                                                    v2::ExecutionFinished {
                                                        state: v2::JobState::Succeeded as i32,
                                                        outputs: active.outputs,
                                                        error: None,
                                                    },
                                                )),
                                                ..Default::default()
                                            }));
                                    }
                                    Some(v2::finish::Outcome::Failed(err)) => {
                                        if err.retryable
                                            && active.job.attempt_number < active.job.max_attempts
                                        {
                                            // Mark failed worker unavailable so retry hits worker #2.
                                            st.workers[active.worker_idx].busy = true;
                                            let _ = active
                                                .job
                                                .client_tx
                                                .try_send(Ok(v2::ExecutionEvent {
                                                job_id: active.job.job_id.clone(),
                                                sequence: 5,
                                                event: Some(
                                                    v2::execution_event::Event::AttemptRestarted(
                                                        v2::AttemptRestarted {
                                                            previous_attempt_id: format!(
                                                                "att-{}",
                                                                active.job.attempt_number
                                                            ),
                                                            reason: Some(err),
                                                        },
                                                    ),
                                                ),
                                                ..Default::default()
                                            }));
                                            st.pending_jobs.push_back(active.job);
                                            should_dispatch = true;
                                        } else {
                                            let _ = active.job.client_tx.try_send(Ok(
                                                v2::ExecutionEvent {
                                                    job_id: active.job.job_id.clone(),
                                                    sequence: 10,
                                                    event: Some(
                                                        v2::execution_event::Event::Finished(
                                                            v2::ExecutionFinished {
                                                                state: v2::JobState::Failed as i32,
                                                                outputs: Vec::new(),
                                                                error: Some(err),
                                                            },
                                                        ),
                                                    ),
                                                    ..Default::default()
                                                },
                                            ));
                                        }
                                    }
                                    _ => {}
                                }
                            }
                        }
                        let _ = tx
                            .send(Ok(v2::AttemptDown {
                                msg: Some(v2::attempt_down::Msg::Finished(v2::AttemptFinished {
                                    status: v2::attempt_finished::Status::Committed as i32,
                                    reason: String::new(),
                                })),
                            }))
                            .await;
                        if should_dispatch {
                            relay.dispatch();
                        }
                        break;
                    }
                    _ => {}
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

async fn consume_terminal(
    events: &mut DirectExecutionEvents,
) -> Result<v2::ExecutionFinished, Box<Status>> {
    while let Some(ev) = events.message().await.map_err(Box::new)? {
        if let Some(v2::execution_event::Event::Finished(fin)) = ev.event {
            return Ok(fin);
        }
    }
    Err(Box::new(Status::unavailable(
        "stream ended without Finished",
    )))
}

#[tokio::test]
async fn direct_uri_redaction_and_advertise_direct_addrs_option() {
    let ep = DirectEndpoint::bind(DirectConfig {
        advertise_direct_addrs: false,
        ..local_direct_config()
    })
    .await
    .unwrap();
    let (uri, _, _, _guard) = ep.stage_input(Bytes::from_static(b"hello"), 3).unwrap();
    assert!(is_direct_uri(&uri));
    assert!(
        !uri.contains("addr="),
        "URI must omit addr= when disabled: {uri}"
    );

    let sample = "anyserve+iroh://abc123endpoint/secretToken999?relay=https://r.example/&addr=1.2.3.4:5&addr=[::1]:9";
    assert_eq!(
        redact_direct_uri(sample),
        "anyserve+iroh://abc123endpoint/[redacted]?relay=https://r.example/&addr=[redacted]&addr=[redacted]"
    );
    assert_eq!(
        redact_direct_uri("anyserve+iroh://abc123endpoint/secretToken999"),
        "anyserve+iroh://abc123endpoint/[redacted]"
    );
    assert_eq!(
        redact_direct_uri("s3://bucket/key?X-Amz-Sig=1"),
        "s3://bucket/key?X-Amz-Sig=1"
    );

    // ParsedDirectUri Debug also redacts both token and IP addrs.
    let ep_ip = DirectEndpoint::bind(local_direct_config()).await.unwrap();
    let (with_ip_uri, _, _, _g2) = ep_ip.stage_input(Bytes::from_static(b"hi"), 3).unwrap();
    let parsed = decode_direct_uri(&with_ip_uri).unwrap();
    assert!(
        parsed.addrs.iter().any(|a| matches!(
            a,
            iroh::TransportAddr::Ip(sa) if sa.ip() == std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)
        )),
        "expected 127.0.0.1 in addrs: {:?}",
        parsed.addrs
    );
    let dbg = format!("{parsed:?}");
    assert!(dbg.contains("[redacted]"), "{dbg}");
    assert!(!dbg.contains(&parsed.token), "token leaked in Debug: {dbg}");
    assert!(!dbg.contains("127.0.0.1"), "IP addr leaked in Debug: {dbg}");
    ep_ip.close().await;
    ep.close().await;
}

#[tokio::test]
async fn persistent_secret_key_0600_and_allowed_peers_enforce_identity() {
    let tmp_dir =
        std::env::temp_dir().join(format!("anyserve-direct-key-{}", uuid::Uuid::new_v4()));
    let key_path = tmp_dir.join("node.key");

    let cfg1 = DirectConfig {
        secret_key_path: Some(key_path.clone()),
        ..local_direct_config()
    };
    let debug_str = format!("{cfg1:?}");
    assert!(
        debug_str.contains("[configured]"),
        "DirectConfig debug must redact key path details: {debug_str}"
    );

    let ep1 = DirectEndpoint::bind(cfg1.clone()).await.unwrap();
    let stable_id = ep1.id();

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let mode = std::fs::metadata(&key_path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "secret key file must have 0600 permissions");
    }
    ep1.close().await;

    // Rebinding with the same key_path preserves the exact EndpointId.
    let allowed_client = DirectEndpoint::bind(local_direct_config()).await.unwrap();
    let unauthorized_peer = DirectEndpoint::bind(local_direct_config()).await.unwrap();

    let ep2 = DirectEndpoint::bind(DirectConfig {
        secret_key_path: Some(key_path.clone()),
        allowed_peers: Some(vec![allowed_client.id()]),
        ..local_direct_config()
    })
    .await
    .unwrap();
    assert_eq!(ep2.id(), stable_id);

    let payload = Bytes::from(vec![0x42u8; 4096]);
    let (uri, sha, sz, _guard) = ep2.stage_input(payload.clone(), 4).unwrap();
    let mut meta = HashMap::new();
    meta.insert(OBJECT_SHA256_METADATA_KEY.to_owned(), sha);
    meta.insert(OBJECT_SIZE_METADATA_KEY.to_owned(), sz.to_string());

    // Whitelisted peer succeeds.
    let (got, report) = allowed_client
        .fetch_object(&uri, &meta, &DirectCancel::new())
        .await
        .unwrap();
    assert_eq!(got, payload);
    assert_eq!(report.path, DirectPath::Direct);

    // Unauthorized peer holding the EXACT same valid URI+token is cryptographically rejected.
    let err = unauthorized_peer
        .fetch_object(&uri, &meta, &DirectCancel::new())
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            DirectError::PeerRejected(_) | DirectError::Protocol(_) | DirectError::Connect(_)
        ),
        "expected peer rejection, got {err:?}"
    );

    ep2.close().await;
    allowed_client.close().await;
    unauthorized_peer.close().await;
    let _ = std::fs::remove_dir_all(&tmp_dir);
}

#[tokio::test]
async fn sha256_mismatch_expiry_revocation_fetch_limit_and_lru_ack_eviction() {
    let provider = DirectEndpoint::bind(DirectConfig {
        input_token_ttl: Duration::from_millis(80),
        output_cache_max_bytes: 8 * 1024,
        output_cache_ttl: Duration::from_millis(120),
        ..local_direct_config()
    })
    .await
    .unwrap();
    let consumer = DirectEndpoint::bind(local_direct_config()).await.unwrap();

    // 1. SHA-256 mismatch rejected cleanly.
    let data = Bytes::from(vec![0xABu8; 2048]);
    let (uri, _real_sha, sz, mut guard) = provider.stage_input(data.clone(), 3).unwrap();
    let mut bad_meta = HashMap::new();
    bad_meta.insert(OBJECT_SHA256_METADATA_KEY.to_owned(), "0".repeat(64));
    bad_meta.insert(OBJECT_SIZE_METADATA_KEY.to_owned(), sz.to_string());
    let err = consumer
        .fetch_object(&uri, &bad_meta, &DirectCancel::new())
        .await
        .unwrap_err();
    assert!(matches!(err, DirectError::Sha256Mismatch { .. }), "{err:?}");

    // 2. Revocation via guard immediately rejects subsequent fetches.
    guard.revoke_now();
    let err = consumer
        .fetch_object(&uri, &HashMap::new(), &DirectCancel::new())
        .await
        .unwrap_err();
    assert!(matches!(err, DirectError::NotFoundOrExpired), "{err:?}");

    // 3. TTL expiry rejects fetch.
    let (exp_uri, _, _, _exp_guard) = provider.stage_input(data.clone(), 3).unwrap();
    tokio::time::sleep(Duration::from_millis(110)).await;
    let err = consumer
        .fetch_object(&exp_uri, &HashMap::new(), &DirectCancel::new())
        .await
        .unwrap_err();
    assert!(matches!(err, DirectError::NotFoundOrExpired), "{err:?}");

    // 4. max_fetches derived from max_attempts (not hardcoded 8, clamped 3..=20).
    assert_eq!(derive_input_max_fetches(None), 3);
    assert_eq!(derive_input_max_fetches(Some(1)), 3);
    assert_eq!(derive_input_max_fetches(Some(12)), 12);
    assert_eq!(derive_input_max_fetches(Some(99)), 20);

    let p_long = DirectEndpoint::bind(local_direct_config()).await.unwrap();
    let (lim_uri, sha, sz, _lim_guard) = p_long.stage_input(data.clone(), 2).unwrap();
    let mut ok_meta = HashMap::new();
    ok_meta.insert(OBJECT_SHA256_METADATA_KEY.to_owned(), sha);
    ok_meta.insert(OBJECT_SIZE_METADATA_KEY.to_owned(), sz.to_string());
    for _ in 0..2 {
        let (got, _) = consumer
            .fetch_object(&lim_uri, &ok_meta, &DirectCancel::new())
            .await
            .unwrap();
        assert_eq!(got, data);
    }
    let err = consumer
        .fetch_object(&lim_uri, &ok_meta, &DirectCancel::new())
        .await
        .unwrap_err();
    assert!(matches!(err, DirectError::FetchLimitExceeded), "{err:?}");

    // 5. Post-finish output LRU capacity eviction + TTL eviction + early ACK eviction.
    let (out1_uri, _, _) = provider
        .stage_output(Bytes::from(vec![1u8; 5 * 1024]))
        .unwrap();
    let (out2_uri, sha2, sz2) = provider
        .stage_output(Bytes::from(vec![2u8; 5 * 1024]))
        .unwrap();
    // 5K + 5K > 8K cache cap -> out1 must be evicted while out2 stays cached.
    let (_, out_count, out_bytes) = provider.store().stats();
    assert_eq!(out_count, 1);
    assert_eq!(out_bytes, 5 * 1024);
    assert!(matches!(
        consumer
            .fetch_object(&out1_uri, &HashMap::new(), &DirectCancel::new())
            .await,
        Err(DirectError::NotFoundOrExpired)
    ));

    // Fetching out2 sends 1B ACK -> provider evicts out2 immediately (early release before TTL!).
    let mut out2_meta = HashMap::new();
    out2_meta.insert(OBJECT_SHA256_METADATA_KEY.to_owned(), sha2);
    out2_meta.insert(OBJECT_SIZE_METADATA_KEY.to_owned(), sz2.to_string());
    let (got2, _) = consumer
        .fetch_object(&out2_uri, &out2_meta, &DirectCancel::new())
        .await
        .unwrap();
    assert_eq!(got2.len(), 5 * 1024);
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if provider.store().stats().1 == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("ACK must evict worker output from post-finish LRU immediately");

    // Output TTL eviction.
    let (out3_uri, _, _) = provider.stage_output(Bytes::from(vec![3u8; 1024])).unwrap();
    tokio::time::sleep(Duration::from_millis(160)).await;
    assert_eq!(provider.store().stats().1, 0);
    assert!(matches!(
        consumer
            .fetch_object(&out3_uri, &HashMap::new(), &DirectCancel::new())
            .await,
        Err(DirectError::NotFoundOrExpired)
    ));

    provider.close().await;
    p_long.close().await;
    consumer.close().await;
}

#[tokio::test]
async fn cancellation_aborts_in_flight_direct_transfer() {
    let provider = DirectEndpoint::bind(local_direct_config()).await.unwrap();
    let consumer = DirectEndpoint::bind(local_direct_config()).await.unwrap();

    // Pre-cancelled token aborts immediately before dialing.
    let big = Bytes::from(vec![0x77u8; 16 * 1024 * 1024]);
    let (uri, sha, sz, _guard) = provider.stage_input(big, 3).unwrap();
    let mut meta = HashMap::new();
    meta.insert(OBJECT_SHA256_METADATA_KEY.to_owned(), sha);
    meta.insert(OBJECT_SIZE_METADATA_KEY.to_owned(), sz.to_string());

    let pre_cancel = DirectCancel::new();
    pre_cancel.cancel();
    assert!(matches!(
        consumer.fetch_object(&uri, &meta, &pre_cancel).await,
        Err(DirectError::Cancelled)
    ));

    // Deterministic in-flight cancellation during path upgrade window aborts cleanly in <500ms.
    let slow_consumer = DirectEndpoint::bind(DirectConfig {
        path_classifier: Arc::new(AlwaysPublicRelayClassifier),
        upgrade_window: Duration::from_secs(5),
        ..local_direct_config()
    })
    .await
    .unwrap();
    let in_flight_cancel = DirectCancel::new();
    let trigger = in_flight_cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(30)).await;
        trigger.cancel();
    });
    let t0 = std::time::Instant::now();
    let err = slow_consumer
        .fetch_object(&uri, &meta, &in_flight_cancel)
        .await
        .unwrap_err();
    assert!(matches!(err, DirectError::Cancelled), "{err:?}");
    assert!(
        t0.elapsed() < Duration::from_secs(1),
        "in-flight cancel took too long: {:?}",
        t0.elapsed()
    );

    slow_consumer.close().await;
    provider.close().await;
    consumer.close().await;
}

#[tokio::test]
async fn end_to_end_direct_roundtrip_failover_retry_and_capability_negotiation() {
    let (endpoint_url, relay, srv_handle) = MiniRelay::start(16 * 1024 * 1024).await;

    // Register a NON-direct worker FIRST to prove required_attributes["anyserve.direct"]="1"
    // routes direct-input jobs ONLY to direct-capable workers (skipping non-direct workers).
    let mut s_non_direct = WorkerSession::connect(
        &endpoint_url,
        None,
        v2::Hello {
            worker_id: "w-non-direct".into(),
            interfaces: vec!["direct.echo".into()],
            max_concurrency: 2,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let non_direct_limits = match s_non_direct.messages.message().await.unwrap().unwrap().msg {
        Some(v2::server_message::Msg::Welcome(w)) => w.limits.unwrap(),
        other => panic!("expected welcome, got {other:?}"),
    };
    let non_direct_got_direct_job = Arc::new(AtomicBool::new(false));
    let nd_flag = non_direct_got_direct_job.clone();
    let non_direct_handler_calls = Arc::new(AtomicUsize::new(0));
    let nd_handler_calls = non_direct_handler_calls.clone();
    let nd_task = tokio::spawn(async move {
        while let Ok(Some(v2::ServerMessage {
            msg: Some(v2::server_message::Msg::Assign(assign)),
        })) = s_non_direct.messages.message().await
        {
            // Plain Attempt::open automatically detects anyserve+iroh:// inputs,
            // commits non-retryable direct_unsupported pre-handler, and returns Err
            // so the user handler is NEVER reachable!
            let mut att = match Attempt::open(
                &mut s_non_direct.data,
                &assign,
                &non_direct_limits,
                Duration::from_secs(5),
            )
            .await
            {
                Ok(att) => att,
                Err(_) => {
                    nd_flag.store(true, Ordering::SeqCst);
                    continue;
                }
            };
            nd_handler_calls.fetch_add(1, Ordering::SeqCst);
            let data = match &att.inputs[0].body {
                Some(v2::object::Body::Inline(b)) => b.clone(),
                _ => Bytes::new(),
            };
            att.finish(
                &[v2::Object {
                    body: Some(v2::object::Body::Inline(data)),
                    metadata: HashMap::new(),
                }],
                v2::finish::Outcome::Succeeded(v2::Succeeded {}),
            )
            .await
            .unwrap();
        }
    });

    // Worker 1 (direct-capable): connects via DirectWorkerSession (auto-advertises
    // anyserve.direct=1), opens safely via open_attempt, then fails retryably BEFORE output!
    let w1_ep = DirectEndpoint::bind(local_direct_config()).await.unwrap();
    let mut s1 = DirectWorkerSession::connect(
        &endpoint_url,
        None,
        v2::Hello {
            worker_id: "w-direct-1".into(),
            interfaces: vec!["direct.echo".into()],
            max_concurrency: 2,
            ..Default::default()
        },
        Arc::clone(&w1_ep),
    )
    .await
    .unwrap();
    let welcome1 = match s1.session.messages.message().await.unwrap().unwrap().msg {
        Some(v2::server_message::Msg::Welcome(w)) => w,
        other => panic!("expected welcome, got {other:?}"),
    };
    let limits1 = welcome1.limits.unwrap();

    // Worker 2 (direct-capable): re-fetches same input URI on Attempt #2 and returns direct output!
    let w2_ep = DirectEndpoint::bind(local_direct_config()).await.unwrap();
    let mut s2 = DirectWorkerSession::connect(
        &endpoint_url,
        None,
        v2::Hello {
            worker_id: "w-direct-2".into(),
            interfaces: vec!["direct.echo".into()],
            max_concurrency: 2,
            ..Default::default()
        },
        Arc::clone(&w2_ep),
    )
    .await
    .unwrap();
    let welcome2 = match s2.session.messages.message().await.unwrap().unwrap().msg {
        Some(v2::server_message::Msg::Welcome(w)) => w,
        other => panic!("expected welcome, got {other:?}"),
    };
    let limits2 = welcome2.limits.unwrap();

    let w1_fetched_bytes = Arc::new(AtomicUsize::new(0));
    let w1_fetched_clone = w1_fetched_bytes.clone();
    let w1_task = tokio::spawn(async move {
        if let Ok(Some(v2::ServerMessage {
            msg: Some(v2::server_message::Msg::Assign(assign)),
        })) = s1.session.messages.message().await
        {
            let mut att = s1
                .open_attempt(
                    &assign,
                    &limits1,
                    Duration::from_secs(5),
                    &DirectCancel::new(),
                )
                .await
                .unwrap();
            w1_fetched_clone.store(att.input_reports()[0].bytes as usize, Ordering::SeqCst);
            // Simulate worker crash before emitting any output -> retryable failure.
            att.finish_failed(v2::ErrorInfo {
                code: "worker_lost".into(),
                message: "simulated crash on attempt 1".into(),
                retryable: true,
            })
            .await
            .unwrap();
        }
    });

    let w2_task = tokio::spawn(async move {
        // Handle 2 jobs: (1) retry of direct client job, (2) legacy non-direct client job.
        for _ in 0..2 {
            let Some(Ok(v2::ServerMessage {
                msg: Some(v2::server_message::Msg::Assign(assign)),
            })) = s2.session.messages.message().await.transpose()
            else {
                break;
            };
            let mut att = s2
                .open_attempt(
                    &assign,
                    &limits2,
                    Duration::from_secs(5),
                    &DirectCancel::new(),
                )
                .await
                .unwrap();
            let input_bytes = match &att.inputs()[0].body {
                Some(v2::object::Body::Inline(b)) => b.clone(),
                other => panic!("expected resolved inline input, got {other:?}"),
            };
            let mut out_bytes = input_bytes.to_vec();
            out_bytes.reverse();
            att.finish_succeeded(vec![(Bytes::from(out_bytes), HashMap::new())])
                .await
                .unwrap();
        }
    });

    // 1. Direct client invokes 128 KiB payload -> durable rejected first.
    let mut direct_client = ExecutionClient::connect(&endpoint_url)
        .await
        .unwrap()
        .with_direct(local_direct_config());
    let durable_err = direct_client
        .invoke_direct(v2::JobSpec {
            demand: Some(v2::Demand {
                interface: "direct.echo".into(),
                ..Default::default()
            }),
            policy: Some(v2::ExecutionPolicy {
                durability: v2::Durability::Durable as i32,
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .err()
        .expect("durable invoke_direct must be rejected");
    assert_eq!(durable_err.code(), Code::InvalidArgument);

    // Real ephemeral direct invocation with failover retry!
    let payload: Vec<u8> = (0..(128 * 1024)).map(|i| (i % 251) as u8).collect();
    let expected_out: Vec<u8> = payload.iter().rev().copied().collect();
    let mut events = direct_client
        .invoke_direct(v2::JobSpec {
            demand: Some(v2::Demand {
                interface: "direct.echo".into(),
                ..Default::default()
            }),
            inputs: vec![v2::Object {
                body: Some(v2::object::Body::Inline(Bytes::from(payload.clone()))),
                ..Default::default()
            }],
            policy: Some(v2::ExecutionPolicy {
                max_attempts: 3,
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .unwrap();

    let fin = consume_terminal(&mut events).await.unwrap();
    assert_eq!(fin.state, v2::JobState::Succeeded as i32);
    assert_eq!(w1_fetched_bytes.load(Ordering::SeqCst), payload.len());
    assert_eq!(events.output_reports().len(), 1);
    assert_eq!(events.output_reports()[0].path, DirectPath::Direct);
    assert_eq!(
        events.output_reports()[0].bytes as usize,
        expected_out.len()
    );
    match &fin.outputs[0].body {
        Some(v2::object::Body::Inline(b)) => assert_eq!(b.as_ref(), expected_out.as_slice()),
        other => panic!("expected resolved inline output, got {other:?}"),
    }
    assert_eq!(
        fin.outputs[0]
            .metadata
            .get(DIRECT_PATH_METADATA_KEY)
            .map(String::as_str),
        Some("direct")
    );
    assert!(
        fin.outputs[0]
            .metadata
            .contains_key(DIRECT_BYTES_METADATA_KEY)
    );
    // Verify the relay observed ZERO raw inline payload bytes — only tiny URI strings,
    // and that the non-direct worker registered first was skipped by attribute routing!
    assert!(
        !non_direct_got_direct_job.load(Ordering::SeqCst),
        "non-direct worker must not be routed a direct-input job"
    );
    {
        let st = relay.state.lock().unwrap();
        assert_eq!(st.observed_inline_payload_bytes, 0);
        assert_eq!(st.observed_uri_inputs.len(), 1);
        assert!(is_direct_uri(&st.observed_uri_inputs[0]));
        assert_eq!(st.observed_uri_outputs.len(), 1);
        assert!(is_direct_uri(&st.observed_uri_outputs[0]));
    }
    // Verify client input token was revoked upon terminal Finished.
    let client_ep = direct_client.direct_endpoint().await.unwrap().unwrap();
    assert_eq!(client_ep.store().stats().0, 0);

    // 1b. Defense-in-depth: if a manually crafted request without required_attributes
    //     delivers an anyserve+iroh:// URI to the non-direct worker, it must fail
    //     pre-handler with code="direct_unsupported" (retryable=false) and 0 handler calls!
    let mut manual_client = ExecutionClient::connect(&endpoint_url).await.unwrap();
    let mut manual_stream = manual_client
        .invoke(v2::JobSpec {
            demand: Some(v2::Demand {
                interface: "direct.echo".into(),
                ..Default::default()
            }),
            inputs: vec![v2::Object {
                body: Some(v2::object::Body::Uri(format!(
                    "anyserve+iroh://{}/fake0123456789abcdef0123456789ab",
                    client_ep.id()
                ))),
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .unwrap();
    let mut manual_fin = None;
    while let Some(ev) = manual_stream.message().await.unwrap() {
        if let Some(v2::execution_event::Event::Finished(f)) = ev.event {
            manual_fin = Some(f);
        }
    }
    let manual_fin = manual_fin.unwrap();
    assert_eq!(manual_fin.state, v2::JobState::Failed as i32);
    let manual_err = manual_fin.error.unwrap();
    assert_eq!(manual_err.code, ERROR_CODE_DIRECT_UNSUPPORTED);
    assert!(!manual_err.retryable);
    assert!(non_direct_got_direct_job.load(Ordering::SeqCst));
    assert_eq!(non_direct_handler_calls.load(Ordering::SeqCst), 0);

    // 2. Capability negotiation: non-direct client calling direct-capable Worker #2
    //    must receive an inline output (NOT an unresolvable anyserve+iroh:// URI).
    //    (Target Worker #2 explicitly via anyserve.direct=1 required_attributes so
    //     we exercise Worker #2's stage_direct_output fallback to inline.)
    let mut legacy_client = ExecutionClient::connect(&endpoint_url).await.unwrap();
    let legacy_payload = Bytes::from(vec![0x33u8; 4096]);
    let mut legacy_stream = legacy_client
        .invoke(v2::JobSpec {
            demand: Some(v2::Demand {
                interface: "direct.echo".into(),
                required_attributes: [(
                    crate::DIRECT_CAPABILITY_KEY.to_owned(),
                    crate::DIRECT_CAPABILITY_VALUE.to_owned(),
                )]
                .into_iter()
                .collect(),
                ..Default::default()
            }),
            inputs: vec![v2::Object {
                body: Some(v2::object::Body::Inline(legacy_payload.clone())),
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .unwrap();
    let mut legacy_fin = None;
    while let Some(ev) = legacy_stream.message().await.unwrap() {
        if let Some(v2::execution_event::Event::Finished(f)) = ev.event {
            legacy_fin = Some(f);
        }
    }
    let legacy_fin = legacy_fin.unwrap();
    assert_eq!(legacy_fin.state, v2::JobState::Succeeded as i32);
    assert!(
        matches!(&legacy_fin.outputs[0].body, Some(v2::object::Body::Inline(b)) if b.len() == 4096),
        "legacy client without anyserve.direct=1 must receive inline output"
    );

    nd_task.abort();
    w1_task.await.unwrap();
    w2_task.await.unwrap();
    w1_ep.close().await;
    w2_ep.close().await;
    client_ep.close().await;
    srv_handle.abort();
}

#[tokio::test]
async fn public_relay_only_fallback_fail_fast_server_limit_and_direct_unsupported() {
    // Relay caps inline objects at 16 KiB so we can verify verbatim propagation on fallback rejection.
    let (endpoint_url, _relay, srv_handle) = MiniRelay::start(16 * 1024).await;

    // Worker with injected AlwaysPublicRelayClassifier (hermetic, zero internet calls!).
    let worker_cfg = DirectConfig {
        path_classifier: Arc::new(AlwaysPublicRelayClassifier),
        ..local_direct_config()
    };
    let w_ep = DirectEndpoint::bind(worker_cfg).await.unwrap();
    let mut session = DirectWorkerSession::connect(
        &endpoint_url,
        None,
        v2::Hello {
            worker_id: "w-throttled".into(),
            interfaces: vec!["throttled.echo".into()],
            max_concurrency: 2,
            ..Default::default()
        },
        Arc::clone(&w_ep),
    )
    .await
    .unwrap();
    let limits = match session
        .session
        .messages
        .message()
        .await
        .unwrap()
        .unwrap()
        .msg
    {
        Some(v2::server_message::Msg::Welcome(w)) => w.limits.unwrap(),
        other => panic!("expected welcome, got {other:?}"),
    };

    let handler_calls = Arc::new(AtomicUsize::new(0));
    let handler_calls_w = handler_calls.clone();
    let total_attempts_seen = Arc::new(AtomicUsize::new(0));
    let total_attempts_w = total_attempts_seen.clone();
    let saw_direct_cap_on_fallback = Arc::new(AtomicBool::new(false));
    let saw_cap_w = saw_direct_cap_on_fallback.clone();

    let worker_task = tokio::spawn(async move {
        // Expect exactly 4 attempts across the 3 client invokes (proving non-retryable
        // direct_public_relay_throttled NEVER consumes max_attempts retries!):
        // 1) 4 KiB direct -> throttled pre-handler inside open_attempt (handler NOT called!)
        // 2) 4 KiB inline fallback -> succeeds (handler called ONCE, anyserve.direct stripped!)
        // 3) 20 KiB direct -> throttled pre-handler inside open_attempt (handler NOT called!; client fallback rejected by 16 KiB server cap)
        // 4) 80 KiB direct (with max_attempts=3) -> throttled pre-handler inside open_attempt (1 attempt only, handler NOT called!; client fails fast)
        for _ in 0..4 {
            let Some(Ok(v2::ServerMessage {
                msg: Some(v2::server_message::Msg::Assign(assign)),
            })) = session.session.messages.message().await.transpose()
            else {
                break;
            };
            total_attempts_w.fetch_add(1, Ordering::SeqCst);
            let mut att = match session
                .open_attempt(
                    &assign,
                    &limits,
                    Duration::from_secs(5),
                    &DirectCancel::new(),
                )
                .await
            {
                Ok(att) => att,
                Err(DirectAttemptError::Direct(DirectError::PublicRelayThrottled { .. })) => {
                    // open_attempt ALREADY finished the attempt non-retryably before returning Err!
                    continue;
                }
                Err(other) => panic!("unexpected open_attempt error: {other:?}"),
            };
            // Handler executes ONLY when open_attempt succeeded.
            handler_calls_w.fetch_add(1, Ordering::SeqCst);
            if att.client_supports_direct() {
                saw_cap_w.store(true, Ordering::SeqCst);
            }
            let data = match &att.inputs()[0].body {
                Some(v2::object::Body::Inline(b)) => b.clone(),
                _ => Bytes::new(),
            };
            att.finish_succeeded(vec![(data, HashMap::new())])
                .await
                .unwrap();
        }
    });

    let mut client = ExecutionClient::connect(&endpoint_url)
        .await
        .unwrap()
        .with_direct(DirectConfig {
            threshold_bytes: 1024,
            inline_fallback_limit_bytes: 64 * 1024,
            ..local_direct_config()
        });

    // Case 1: 4 KiB <= inline_fallback_limit_bytes (64 KiB) & <= server cap (16 KiB)
    // -> worker rejects pre-handler with direct_public_relay_throttled, client falls back
    //    once inline (stripping anyserve.direct), handler runs exactly ONCE!
    let small = Bytes::from(vec![0x11u8; 4 * 1024]);
    let mut ev1 = client
        .invoke_direct(v2::JobSpec {
            demand: Some(v2::Demand {
                interface: "throttled.echo".into(),
                ..Default::default()
            }),
            inputs: vec![v2::Object {
                body: Some(v2::object::Body::Inline(small.clone())),
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .unwrap();
    let fin1 = consume_terminal(&mut ev1).await.unwrap();
    assert_eq!(fin1.state, v2::JobState::Succeeded as i32);
    assert!(ev1.fell_back_inline());
    assert!(
        ev1.fallback_from_job_id().is_some(),
        "fallback_from_job_id must record the initial throttled job id"
    );
    assert_eq!(handler_calls.load(Ordering::SeqCst), 1);
    assert!(
        !saw_direct_cap_on_fallback.load(Ordering::SeqCst),
        "inline fallback must strip anyserve.direct so worker returns inline output"
    );
    assert!(matches!(&fin1.outputs[0].body, Some(v2::object::Body::Inline(b)) if b == &small));

    // Case 2: 20 KiB <= inline_fallback_limit_bytes (64 KiB) BUT > server max_inline_object_bytes (16 KiB)
    // -> server rejection on inline fallback must be propagated verbatim, and handler MUST NOT run!
    let medium = Bytes::from(vec![0x22u8; 20 * 1024]);
    let mut ev2 = client
        .invoke_direct(v2::JobSpec {
            demand: Some(v2::Demand {
                interface: "throttled.echo".into(),
                ..Default::default()
            }),
            inputs: vec![v2::Object {
                body: Some(v2::object::Body::Inline(medium)),
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .unwrap();
    let err2 = consume_terminal(&mut ev2).await.unwrap_err();
    assert_eq!(err2.code(), Code::InvalidArgument);
    assert!(
        err2.message().contains("max_inline_object_bytes"),
        "expected verbatim server inline limit error, got: {}",
        err2.message()
    );
    assert_eq!(handler_calls.load(Ordering::SeqCst), 1);

    // Case 3: 80 KiB > inline_fallback_limit_bytes (64 KiB) with max_attempts = 3
    // -> worker fails with retryable=false (1 attempt only, never burns max_attempts!),
    //    client fails fast with PublicRelayThrottled, no inline resend, handler MUST NOT run!
    let large = Bytes::from(vec![0x33u8; 80 * 1024]);
    let mut ev3 = client
        .invoke_direct(v2::JobSpec {
            demand: Some(v2::Demand {
                interface: "throttled.echo".into(),
                ..Default::default()
            }),
            inputs: vec![v2::Object {
                body: Some(v2::object::Body::Inline(large)),
                ..Default::default()
            }],
            policy: Some(v2::ExecutionPolicy {
                max_attempts: 3,
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .unwrap();
    let err3 = consume_terminal(&mut ev3).await.unwrap_err();
    assert_eq!(err3.code(), Code::FailedPrecondition);
    assert!(
        err3.message().contains("rate-limited public n0 relay"),
        "{}",
        err3.message()
    );
    assert_eq!(handler_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        total_attempts_seen.load(Ordering::SeqCst),
        4,
        "non-retryable direct_public_relay_throttled must not consume max_attempts retries"
    );

    // Case 4: Output side PublicRelayOnly -> fails fast (no retroactive fallback).
    let out_provider = DirectEndpoint::bind(local_direct_config()).await.unwrap();
    let out_consumer = DirectEndpoint::bind(DirectConfig {
        path_classifier: Arc::new(AlwaysPublicRelayClassifier),
        ..local_direct_config()
    })
    .await
    .unwrap();
    let (out_uri, out_sha, out_sz) = out_provider
        .stage_output(Bytes::from(vec![0x44u8; 4096]))
        .unwrap();
    let mut out_meta = HashMap::new();
    out_meta.insert(OBJECT_SHA256_METADATA_KEY.to_owned(), out_sha);
    out_meta.insert(OBJECT_SIZE_METADATA_KEY.to_owned(), out_sz.to_string());
    let out_err = out_consumer
        .fetch_object(&out_uri, &out_meta, &DirectCancel::new())
        .await
        .unwrap_err();
    assert!(
        matches!(out_err, DirectError::PublicRelayThrottled { .. }),
        "{out_err:?}"
    );

    // Case 5: Non-direct worker defense-in-depth outcome check (`direct_unsupported`, non-retryable).
    match Attempt::direct_unsupported_outcome() {
        v2::finish::Outcome::Failed(info) => {
            assert_eq!(info.code, ERROR_CODE_DIRECT_UNSUPPORTED);
            assert!(!info.retryable);
        }
        other => panic!("expected Failed outcome, got {other:?}"),
    }
    match (DirectError::PublicRelayThrottled {
        detail: "test".into(),
    })
    .to_attempt_failure_outcome()
    {
        v2::finish::Outcome::Failed(info) => {
            assert_eq!(info.code, ERROR_CODE_DIRECT_PUBLIC_RELAY_THROTTLED);
            assert!(!info.retryable);
        }
        other => panic!("expected Failed outcome, got {other:?}"),
    }

    worker_task.await.unwrap();
    out_provider.close().await;
    out_consumer.close().await;
    w_ep.close().await;
    client
        .direct_endpoint()
        .await
        .unwrap()
        .unwrap()
        .close()
        .await;
    srv_handle.abort();
}
