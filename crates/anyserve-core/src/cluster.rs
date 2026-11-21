#![allow(
    clippy::result_large_err,
    reason = "tonic::Status is the transport error type"
)]
//! Internal active-active control-plane PeerMesh over gRPC.
//!
//! Each replica running against PostgreSQL maintains persistent bidirectional
//! `SyncDirectory` streams with live peers discovered through the `nodes` table:
//! - Exchanges `PoolSlotSummary` snapshots to power single-hop `ForwardInvoke`
//!   routing for zero-queue ephemeral requests when local workers are absent/busy.
//! - Pushes `<1ms` `ClaimWakeup` notifications on durable submission so remote
//!   replicas claim immediately instead of waiting for `poll_interval`.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use anyserve_proto::cluster::peer_mesh_client::PeerMeshClient;
use anyserve_proto::cluster::peer_mesh_server::{PeerMesh, PeerMeshServer};
use anyserve_proto::cluster::{
    self as cp, ClaimWakeup, DirectorySnapshot, ForwardInvokeRequest, ForwardInvokeStart,
    NodeDirectoryEvent, PoolSlotSummary, WatchRemoteRequest,
};
use anyserve_proto::v2 as pb;
use futures::{Stream, StreamExt};
use tokio::sync::{Notify, mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;
use tonic::metadata::MetadataValue;
use tonic::service::Interceptor;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tonic::{Request, Response, Status, Streaming};

use crate::client_service::{event_from_proto, event_to_proto, spec_from_proto, spec_to_proto};
use crate::error::RuntimeError;
use crate::runtime::api::{
    Demand, EventStream, Execution, ExecutionEvent, InputMode, InputSink, InterfacePolicy,
    InvokeRequest, JobSpec,
};
use crate::runtime::events::Permit;
use crate::runtime::input::{InputLog, InputStatus};
use crate::runtime::{Inner, Runtime};

type ResponseStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

#[derive(Clone, Debug)]
struct PeerInfo {
    addr: String,
    summaries: Vec<PoolSlotSummary>,
    outbound: Option<mpsc::Sender<NodeDirectoryEvent>>,
    cancel: Option<watch::Sender<bool>>,
}

#[derive(Clone)]
struct PeerAuthInterceptor {
    header: MetadataValue<tonic::metadata::Ascii>,
}

impl Interceptor for PeerAuthInterceptor {
    fn call(&mut self, mut req: Request<()>) -> Result<Request<()>, Status> {
        req.metadata_mut()
            .insert("authorization", self.header.clone());
        Ok(req)
    }
}

#[derive(Default)]
pub(crate) struct PeerMeshState {
    peers: Mutex<HashMap<String, PeerInfo>>,
    directory_changed: Arc<Notify>,
    seq: AtomicU64,
}

impl PeerMeshState {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn notify_directory_changed(&self) {
        self.directory_changed.notify_waiters();
    }

    pub(crate) fn next_seq(&self) -> u64 {
        self.seq.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// Synchronizes peer addresses exclusively from the trusted PG `nodes` table
    /// and signals `maintain_peer_link` tasks for removed or rotated peers to exit.
    pub(crate) fn sync_live_peers(
        &self,
        live: &HashMap<String, String>,
    ) -> Vec<(String, String, watch::Receiver<bool>)> {
        let mut peers = self.peers.lock().expect("peer directory poisoned");
        let mut stale = Vec::new();
        for (id, info) in peers.iter() {
            if live.get(id) != Some(&info.addr) {
                stale.push(id.clone());
            }
        }
        for id in stale {
            if let Some(info) = peers.remove(&id)
                && let Some(cancel) = info.cancel
            {
                let _ = cancel.send(true);
            }
        }
        let mut to_spawn = Vec::new();
        for (node_id, addr) in live {
            if !peers.contains_key(node_id) {
                let (cancel_tx, cancel_rx) = watch::channel(false);
                peers.insert(
                    node_id.clone(),
                    PeerInfo {
                        addr: addr.clone(),
                        summaries: Vec::new(),
                        outbound: None,
                        cancel: Some(cancel_tx),
                    },
                );
                to_spawn.push((node_id.clone(), addr.clone(), cancel_rx));
            }
        }
        to_spawn
    }

    pub(crate) fn shutdown_all(&self) {
        let mut peers = self.peers.lock().expect("peer directory poisoned");
        for (_, info) in peers.drain() {
            if let Some(cancel) = info.cancel {
                let _ = cancel.send(true);
            }
        }
    }

    /// Updates cached capacity summaries only for peers already present in the
    /// trusted `nodes` table; never accepts address overrides from inbound RPCs.
    pub(crate) fn upsert_directory(&self, node_id: &str, summaries: Vec<PoolSlotSummary>) {
        let mut peers = self.peers.lock().expect("peer directory poisoned");
        if let Some(entry) = peers.get_mut(node_id) {
            entry.summaries = summaries;
        }
    }

    pub(crate) fn register_outbound(&self, node_id: &str, tx: mpsc::Sender<NodeDirectoryEvent>) {
        let mut peers = self.peers.lock().expect("peer directory poisoned");
        if let Some(entry) = peers.get_mut(node_id) {
            entry.outbound = Some(tx);
        }
    }

    #[cfg(test)]
    pub(crate) fn live_peer_count(&self) -> usize {
        self.peers.lock().expect("peer directory poisoned").len()
    }

    pub(crate) fn find_peer_for_demand(
        &self,
        namespace: &str,
        demand: &Demand,
        policy: Option<&InterfacePolicy>,
    ) -> Option<(String, String)> {
        let tiers = policy.map(|p| p.pool_tiers.as_slice()).unwrap_or(&[]);
        let peers = self.peers.lock().expect("peer directory poisoned");
        let mut best: Option<(f64, String, String)> = None;
        for (node_id, info) in peers.iter() {
            if info.addr.is_empty() {
                continue;
            }
            for summary in &info.summaries {
                if summary.namespace != namespace
                    || summary.interface != demand.interface
                    || summary.free_slots == 0
                {
                    continue;
                }
                if !tiers.is_empty()
                    && !tiers
                        .iter()
                        .any(|t| t.pool == summary.pool && t.spillover_after.is_zero())
                {
                    continue;
                }
                if !demand
                    .required_attributes
                    .iter()
                    .all(|(k, v)| summary.shared_attributes.get(k) == Some(v))
                {
                    continue;
                }
                if !demand.required_capacity.iter().all(|(k, needed)| {
                    *needed <= 0
                        || summary.max_free_capacity.get(k).copied().unwrap_or(0) >= *needed
                }) {
                    continue;
                }
                let replace = match &best {
                    None => true,
                    Some((score, _, _)) => summary.min_load_score < *score,
                };
                if replace {
                    best = Some((summary.min_load_score, node_id.clone(), info.addr.clone()));
                }
            }
        }
        best.map(|(_, node_id, addr)| (node_id, addr))
    }

    pub(crate) fn broadcast_wakeup(
        &self,
        source_node_id: &str,
        self_addr: &str,
        namespace: &str,
        interface: &str,
    ) {
        let seq = self.next_seq();
        let event = NodeDirectoryEvent {
            source_node_id: source_node_id.to_string(),
            internal_grpc_addr: self_addr.to_string(),
            seq,
            payload: Some(cp::node_directory_event::Payload::Wakeup(ClaimWakeup {
                namespace: namespace.to_string(),
                interface: interface.to_string(),
            })),
        };
        let senders: Vec<_> = {
            let peers = self.peers.lock().expect("peer directory poisoned");
            peers.values().filter_map(|p| p.outbound.clone()).collect()
        };
        for tx in senders {
            let _ = tx.try_send(event.clone());
        }
    }
}

#[derive(Clone)]
pub struct PeerMeshGrpcService {
    runtime: Runtime,
}

impl PeerMeshGrpcService {
    pub fn new(runtime: Runtime) -> Self {
        Self { runtime }
    }

    pub fn server(runtime: Runtime) -> PeerMeshServer<Self> {
        PeerMeshServer::new(Self::new(runtime))
            .max_decoding_message_size(anyserve_proto::MAX_MESSAGE_SIZE)
            .max_encoding_message_size(anyserve_proto::MAX_MESSAGE_SIZE)
    }
}

fn build_snapshot(runtime: &Runtime) -> NodeDirectoryEvent {
    let (node_id, internal_addr, seq) = {
        let pg = runtime.inner.postgres.as_ref();
        let mesh = runtime.inner.peer_mesh.as_ref();
        (
            pg.map(|p| p.node_id.clone()).unwrap_or_default(),
            pg.and_then(|p| p.config.internal_grpc_addr.clone())
                .unwrap_or_default(),
            mesh.map(|m| m.next_seq()).unwrap_or(0),
        )
    };
    let summaries = runtime.cluster_directory();
    NodeDirectoryEvent {
        source_node_id: node_id,
        internal_grpc_addr: internal_addr,
        seq,
        payload: Some(cp::node_directory_event::Payload::Snapshot(
            DirectorySnapshot { summaries },
        )),
    }
}

fn handle_inbound_event(runtime: &Runtime, event: NodeDirectoryEvent) {
    if let Some(payload) = event.payload {
        match payload {
            cp::node_directory_event::Payload::Snapshot(snap) => {
                if let Some(mesh) = &runtime.inner.peer_mesh
                    && !event.source_node_id.is_empty()
                {
                    mesh.upsert_directory(&event.source_node_id, snap.summaries);
                }
            }
            cp::node_directory_event::Payload::SlotDelta(_) => {}
            cp::node_directory_event::Payload::Wakeup(_) => {
                runtime.inner.cluster_wakeup.notify_one();
            }
        }
    }
}

#[tonic::async_trait]
impl PeerMesh for PeerMeshGrpcService {
    type SyncDirectoryStream = ResponseStream<NodeDirectoryEvent>;
    type ForwardInvokeStream = ResponseStream<pb::ExecutionEvent>;
    type WatchRemoteStream = ResponseStream<pb::ExecutionEvent>;

    async fn sync_directory(
        &self,
        request: Request<Streaming<NodeDirectoryEvent>>,
    ) -> Result<Response<Self::SyncDirectoryStream>, Status> {
        let mut inbound = request.into_inner();
        let runtime = self.runtime.clone();
        tokio::spawn(async move {
            while let Ok(Some(event)) = inbound.message().await {
                handle_inbound_event(&runtime, event);
            }
        });

        let (tx, rx) = mpsc::channel(64);
        let runtime = self.runtime.clone();
        tokio::spawn(async move {
            let notify = runtime
                .inner
                .peer_mesh
                .as_ref()
                .map(|m| Arc::clone(&m.directory_changed))
                .unwrap_or_else(|| Arc::new(Notify::new()));
            let _ = tx.send(Ok(build_snapshot(&runtime))).await;
            loop {
                tokio::select! {
                    _ = notify.notified() => {}
                    _ = tokio::time::sleep(Duration::from_millis(500)) => {}
                }
                if tx.send(Ok(build_snapshot(&runtime))).await.is_err() {
                    break;
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn forward_invoke(
        &self,
        request: Request<Streaming<ForwardInvokeRequest>>,
    ) -> Result<Response<Self::ForwardInvokeStream>, Status> {
        let mut inbound = request.into_inner();
        let start = match inbound.message().await?.and_then(|m| m.msg) {
            Some(cp::forward_invoke_request::Msg::Start(start)) => start,
            _ => {
                return Err(Status::invalid_argument(
                    "first ForwardInvoke message must be Start",
                ));
            }
        };
        if start.hop_count > 1 {
            return Err(Status::failed_precondition(
                "forward_invoke hop_count exceeded single-hop limit",
            ));
        }
        let invoke = start
            .invoke
            .ok_or_else(|| Status::invalid_argument("ForwardInvokeStart.invoke is required"))?;
        let mut execution = self
            .runtime
            .invoke_local(InvokeRequest {
                namespace: (!start.namespace.is_empty()).then_some(start.namespace),
                spec: spec_from_proto(
                    invoke
                        .spec
                        .ok_or_else(|| Status::invalid_argument("spec is required"))?,
                )?,
                input: if invoke.streaming_input {
                    InputMode::Streaming
                } else {
                    InputMode::Complete
                },
            })
            .await?;
        let input = execution.take_input();
        let stream = async_stream::try_stream! {
            let pump = pump_forward_input(inbound, input);
            tokio::pin!(pump);
            let mut input_done = false;
            loop {
                enum Next {
                    Input(Result<(), Status>),
                    Event(Option<Result<ExecutionEvent, RuntimeError>>),
                }
                let next = tokio::select! {
                    res = &mut pump, if !input_done => Next::Input(res),
                    ev = execution.next() => Next::Event(ev),
                };
                match next {
                    Next::Input(res) => { res?; input_done = true; }
                    Next::Event(ev) => {
                        let event = ev.ok_or_else(|| Status::unavailable("forwarded execution ended unexpectedly"))??;
                        let terminal = matches!(event.kind, crate::runtime::api::EventKind::Finished { .. });
                        yield event_to_proto(event);
                        if terminal { break; }
                    }
                }
            }
        };
        Ok(Response::new(Box::pin(stream)))
    }

    async fn watch_remote(
        &self,
        request: Request<WatchRemoteRequest>,
    ) -> Result<Response<Self::WatchRemoteStream>, Status> {
        let req = request.into_inner();
        let events = self
            .runtime
            .watch_in(&req.namespace, &req.job_id, req.after_sequence)?;
        Ok(Response::new(Box::pin(
            events.map(|ev| ev.map(event_to_proto).map_err(Status::from)),
        )))
    }
}

async fn pump_forward_input(
    mut inbound: Streaming<ForwardInvokeRequest>,
    input: Option<InputSink>,
) -> Result<(), Status> {
    let mut ended = input.is_none();
    while let Some(msg) = inbound.message().await? {
        match (msg.msg, &input, ended) {
            (Some(cp::forward_invoke_request::Msg::Input(chunk)), Some(sink), false) => {
                sink.send(chunk.data, chunk.metadata.into_iter().collect())
                    .await?;
            }
            (Some(cp::forward_invoke_request::Msg::InputEnd(_)), Some(sink), false) => {
                sink.finish().await?;
                ended = true;
            }
            _ => {
                return Err(Status::invalid_argument(
                    "unexpected ForwardInvoke input frame",
                ));
            }
        }
    }
    if !ended {
        return Err(Status::invalid_argument(
            "forwarded streaming input ended without InputEnd",
        ));
    }
    Ok(())
}

async fn connect_peer(addr: &str) -> Result<Channel, RuntimeError> {
    let mut endpoint = Endpoint::from_shared(addr.to_string())
        .map_err(|e| RuntimeError::invalid(format!("invalid peer address '{addr}': {e}")))?
        .tcp_nodelay(true)
        .connect_timeout(Duration::from_secs(2));
    if addr.starts_with("https://") {
        endpoint = endpoint
            .tls_config(ClientTlsConfig::new().with_webpki_roots())
            .map_err(|e| RuntimeError::invalid(format!("configure TLS for peer '{addr}': {e}")))?;
    }
    endpoint
        .connect()
        .await
        .map_err(|e| RuntimeError::unavailable(format!("connect peer '{addr}': {e}")))
}

async fn peer_client(
    addr: &str,
    peer_token: &str,
) -> Result<PeerMeshClient<InterceptedService<Channel, PeerAuthInterceptor>>, RuntimeError> {
    let header: MetadataValue<tonic::metadata::Ascii> = format!("Bearer {}", peer_token.trim())
        .parse()
        .map_err(|_| RuntimeError::invalid("invalid peer_token characters"))?;
    let channel = connect_peer(addr).await?;
    Ok(
        PeerMeshClient::with_interceptor(channel, PeerAuthInterceptor { header })
            .max_decoding_message_size(anyserve_proto::MAX_MESSAGE_SIZE)
            .max_encoding_message_size(anyserve_proto::MAX_MESSAGE_SIZE),
    )
}

pub(crate) async fn peer_sync_loop(me: Weak<Inner>) {
    loop {
        let Some(inner) = me.upgrade() else {
            return;
        };
        if inner.lock().shutting_down {
            if let Some(mesh) = &inner.peer_mesh {
                mesh.shutdown_all();
            }
            return;
        }
        let Some(pg) = inner.postgres.as_ref().map(Arc::clone) else {
            return;
        };
        if !pg.healthy() {
            if let Some(mesh) = &inner.peer_mesh {
                mesh.shutdown_all();
            }
            return;
        }
        if let Ok(peers) = pg.list_live_peers().await
            && let Some(mesh) = &inner.peer_mesh
        {
            let live_map: HashMap<String, String> = peers.into_iter().collect();
            for (node_id, addr, cancel_rx) in mesh.sync_live_peers(&live_map) {
                tokio::spawn(maintain_peer_link(me.clone(), node_id, addr, cancel_rx));
            }
        }
        drop(inner);
        drop(pg);
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn maintain_peer_link(
    me: Weak<Inner>,
    peer_node_id: String,
    peer_addr: String,
    mut cancel_rx: watch::Receiver<bool>,
) {
    loop {
        if *cancel_rx.borrow() {
            return;
        }
        let Some(inner) = me.upgrade() else {
            return;
        };
        if inner.lock().shutting_down {
            return;
        }
        let Some(mesh) = inner.peer_mesh.as_ref().map(Arc::clone) else {
            return;
        };
        let peer_token = inner
            .postgres
            .as_ref()
            .and_then(|pg| pg.config.peer_token.as_deref())
            .unwrap_or("")
            .trim()
            .to_string();
        if peer_token.is_empty() {
            return;
        }
        let mut client = match peer_client(&peer_addr, &peer_token).await {
            Ok(ch) => ch,
            Err(_) => {
                drop(inner);
                tokio::select! {
                    _ = cancel_rx.changed() => return,
                    _ = tokio::time::sleep(Duration::from_millis(500)) => continue,
                }
            }
        };
        let (tx, rx) = mpsc::channel(64);
        mesh.register_outbound(&peer_node_id, tx.clone());
        let rt = Runtime {
            inner: Arc::clone(&inner),
        };
        let _ = tx.send(build_snapshot(&rt)).await;
        let Ok(response) = client.sync_directory(ReceiverStream::new(rx)).await else {
            drop(inner);
            drop(rt);
            tokio::select! {
                _ = cancel_rx.changed() => return,
                _ = tokio::time::sleep(Duration::from_millis(500)) => continue,
            }
        };
        let mut inbound = response.into_inner();
        let rt_read = rt.clone();
        let notify = Arc::clone(&mesh.directory_changed);
        loop {
            tokio::select! {
                _ = cancel_rx.changed() => {
                    if *cancel_rx.borrow() {
                        return;
                    }
                }
                msg = inbound.message() => match msg {
                    Ok(Some(event)) => handle_inbound_event(&rt_read, event),
                    _ => break,
                },
                _ = notify.notified() => {
                    if tx.send(build_snapshot(&rt_read)).await.is_err() {
                        break;
                    }
                }
                _ = tokio::time::sleep(Duration::from_millis(500)) => {
                    if tx.send(build_snapshot(&rt_read)).await.is_err() {
                        break;
                    }
                }
            }
        }
        drop(rt_read);
        drop(rt);
        drop(inner);
        tokio::select! {
            _ = cancel_rx.changed() => return,
            _ = tokio::time::sleep(Duration::from_millis(250)) => {}
        }
    }
}

pub(crate) async fn forward_invoke(
    peer_addr: String,
    peer_token: String,
    namespace: String,
    job_id: String,
    spec: JobSpec,
    input_mode: InputMode,
    inner: &Arc<Inner>,
) -> Result<Execution, RuntimeError> {
    let _permit = inner.admit(&inner.lock(), &namespace, &spec)?;
    let streaming = input_mode == InputMode::Streaming;
    let limits = &inner.config.limits;
    let local_input = streaming.then(|| {
        let used = spec
            .inputs
            .iter()
            .map(|o| o.inline_len() as u64)
            .sum::<u64>()
            + spec.params.len() as u64;
        Arc::new(InputLog::new(
            limits.max_inline_total_bytes.saturating_sub(used),
            Arc::clone(&inner.input_budget),
        ))
    });

    let mut client = peer_client(&peer_addr, &peer_token).await?;

    let (req_tx, req_rx) = mpsc::channel(32);
    let start_req = ForwardInvokeRequest {
        msg: Some(cp::forward_invoke_request::Msg::Start(ForwardInvokeStart {
            namespace: namespace.clone(),
            invoke: Some(pb::InvokeStart {
                spec: Some(spec_to_proto(&spec)),
                streaming_input: streaming,
            }),
            hop_count: 1,
        })),
    };
    req_tx
        .send(start_req)
        .await
        .map_err(|_| RuntimeError::unavailable("failed to open PeerMesh ForwardInvoke stream"))?;

    if let Some(log) = &local_input {
        let log_clone = Arc::clone(log);
        let tx_clone = req_tx.clone();
        tokio::spawn(async move {
            let mut changes = log_clone.subscribe();
            let mut idx = 0usize;
            loop {
                let (chunks, status) = log_clone.read_from(idx);
                for (data, meta) in chunks {
                    idx += 1;
                    if tx_clone
                        .send(ForwardInvokeRequest {
                            msg: Some(cp::forward_invoke_request::Msg::Input(pb::InputChunk {
                                data,
                                metadata: meta.into_iter().collect(),
                            })),
                        })
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                match status {
                    InputStatus::Finished => {
                        let _ = tx_clone
                            .send(ForwardInvokeRequest {
                                msg: Some(cp::forward_invoke_request::Msg::InputEnd(
                                    pb::InputEnd {},
                                )),
                            })
                            .await;
                        return;
                    }
                    InputStatus::Aborted => return,
                    InputStatus::Open => {
                        if changes.changed().await.is_err() {
                            return;
                        }
                    }
                }
            }
        });
    }

    let mut inbound = client
        .forward_invoke(ReceiverStream::new(req_rx))
        .await
        .map_err(|s| RuntimeError::unavailable(format!("PeerMesh ForwardInvoke: {}", s.message())))?
        .into_inner();

    let (cancel_tx, mut cancel_rx) = tokio::sync::oneshot::channel::<()>();
    struct CancelGuard {
        _tx: Option<tokio::sync::oneshot::Sender<()>>,
        _permit: Permit,
    }
    let guard = CancelGuard {
        _tx: Some(cancel_tx),
        _permit,
    };

    let events = EventStream::new(async_stream::stream! {
        loop {
            tokio::select! {
                _ = &mut cancel_rx => break,
                next = inbound.message() => match next {
                    Ok(Some(proto_ev)) => {
                        match event_from_proto(proto_ev) {
                            Ok(ev) => {
                                let done = matches!(ev.kind, crate::runtime::api::EventKind::Finished { .. });
                                yield Ok(ev);
                                if done { break; }
                            }
                            Err(status) => {
                                yield Err(RuntimeError::unavailable(status.message().to_string()));
                                break;
                            }
                        }
                    }
                    Ok(None) => break,
                    Err(status) => {
                        yield Err(RuntimeError::unavailable(status.message().to_string()));
                        break;
                    }
                }
            }
        }
    });

    Ok(Execution {
        job_id,
        events,
        input: local_input.map(|log| InputSink { log }),
        _guard: Box::new(guard),
    })
}
