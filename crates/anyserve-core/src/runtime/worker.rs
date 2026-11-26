//! Worker-facing operations: control sessions and attempt data streams. Only
//! the worker gRPC service calls these.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use anyserve_proto::v2 as pb;
use bytes::Bytes;
use tokio::sync::{mpsc, oneshot, watch};

use super::Runtime;
use super::api::{
    ErrorInfo, JobState, MAX_INTERFACE_BYTES, Metadata, Object, WorkerLoad, codes,
    is_valid_interface_name,
};
use super::events::LogFull;
use super::identity::outcome_digest;
use super::input::InputLog;
use super::machine::{LeaseEnd, new_id};
use super::state::{FinishStatus, Phase, Session, TerminalPlan};
use crate::error::RuntimeError;
use crate::now_ms;

/// How often a frame waiting for room in the shared event budget retries.
const BUDGET_RETRY: Duration = Duration::from_millis(250);
/// Bounds on worker-reported `Load.custom` entries kept per session.
const MAX_LOAD_CUSTOM_ENTRIES: usize = 64;
const MAX_LOAD_CUSTOM_KEY_BYTES: usize = 128;
const MAX_LOAD_CUSTOM_TOTAL_KEY_BYTES: usize = 8192;

/// The only place worker-reported load is sanitized: `load_score` trusts the result.
fn worker_load(load: pb::Load) -> WorkerLoad {
    let utilization = if load.utilization.is_finite() {
        load.utilization.clamp(0.0, 1.0)
    } else {
        0.0
    };
    let mut custom = std::collections::BTreeMap::new();
    let mut key_bytes = 0usize;
    for (k, v) in load.custom {
        if !v.is_finite() || k.is_empty() || k.len() > MAX_LOAD_CUSTOM_KEY_BYTES {
            continue;
        }
        if custom.len() >= MAX_LOAD_CUSTOM_ENTRIES
            || key_bytes + k.len() > MAX_LOAD_CUSTOM_TOTAL_KEY_BYTES
        {
            break;
        }
        key_bytes += k.len();
        custom.insert(k, v);
    }
    WorkerLoad {
        queued: load.queued,
        running: load.running,
        utilization,
        custom,
    }
}

pub(crate) struct SessionHandle {
    pub session_id: String,
    pub connection: u64,
    pub messages: mpsc::Receiver<pb::ServerMessage>,
}

/// Failure opening a worker session or attempt stream: either the signed
/// credential was revoked/expired under the runtime lock, or a domain error.
#[derive(Debug)]
pub(crate) enum WorkerOpenError {
    Unauthenticated(&'static str),
    Runtime(RuntimeError),
}

impl From<RuntimeError> for WorkerOpenError {
    fn from(err: RuntimeError) -> Self {
        Self::Runtime(err)
    }
}

impl From<WorkerOpenError> for tonic::Status {
    fn from(err: WorkerOpenError) -> Self {
        match err {
            WorkerOpenError::Unauthenticated(msg) => tonic::Status::unauthenticated(msg),
            WorkerOpenError::Runtime(err) => err.into(),
        }
    }
}

/// What an `Attempt` stream may do.
pub(crate) enum AttemptLink {
    /// The lease is current: inputs, frames, results and Finish.
    Live {
        lease_id: String,
        resume_from: u64,
        inputs: Option<InputPlan>,
    },
    /// The lease already completed (possibly in an earlier generation): the
    /// worker may only resend its result, which is compared with the receipt.
    Replay { lease_id: String },
}

impl AttemptLink {
    pub(crate) fn lease_id(&self) -> &str {
        match self {
            Self::Live { lease_id, .. } | Self::Replay { lease_id } => lease_id,
        }
    }
}

pub(crate) struct InputPlan {
    pub objects: Vec<Object>,
    pub params: Bytes,
    pub streaming: Option<Arc<InputLog>>,
}

pub(crate) enum AttemptOutcome {
    Succeeded,
    Failed(ErrorInfo),
    Cancelled,
}

fn metadata_size(metadata: &Metadata) -> usize {
    metadata.iter().map(|(k, v)| k.len() + v.len()).sum()
}

fn to_metadata(map: std::collections::HashMap<String, String>) -> Metadata {
    map.into_iter().collect()
}

impl Runtime {
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn open_session(&self, hello: pb::Hello) -> Result<SessionHandle, RuntimeError> {
        self.open_session_for(
            hello,
            crate::auth::Principal {
                role: crate::auth::Role::Worker,
                ..Default::default()
            },
            &crate::auth::AuthPolicy::default(),
        )
        .map_err(|err| match err {
            WorkerOpenError::Runtime(err) => err,
            WorkerOpenError::Unauthenticated(msg) => RuntimeError::invalid(msg),
        })
    }

    pub(crate) fn open_session_for(
        &self,
        hello: pb::Hello,
        principal: crate::auth::Principal,
        auth: &crate::auth::AuthPolicy,
    ) -> Result<SessionHandle, WorkerOpenError> {
        self.ensure_active_leader()?;
        let limits = &self.inner.config.limits;
        let identifier = limits.max_identifier_bytes as usize;
        if hello.interfaces.is_empty() {
            return Err(RuntimeError::invalid("Hello.interfaces is required").into());
        }
        if hello.interfaces.len() > limits.max_objects as usize
            || !hello.interfaces.iter().all(|i| is_valid_interface_name(i))
        {
            return Err(RuntimeError::invalid(format!(
                "Hello.interfaces entries must be 1..={MAX_INTERFACE_BYTES} bytes (no ASCII whitespace or control characters) and at most {} interfaces",
                limits.max_objects
            ))
            .into());
        }
        if hello.worker_id.len() > identifier
            || hello.resume_session_id.len() > identifier
            || hello.pool.len() > identifier
        {
            return Err(RuntimeError::invalid(format!(
                "Hello identifiers must be at most {identifier} bytes"
            ))
            .into());
        }
        if hello.held_lease_ids.len() > limits.max_objects as usize {
            return Err(RuntimeError::invalid("too many held leases").into());
        }
        if hello.max_concurrency > 65_536 {
            return Err(
                RuntimeError::invalid("Hello.max_concurrency must be at most 65536").into(),
            );
        }
        if hello.total_capacity.len() > limits.max_objects as usize {
            return Err(RuntimeError::invalid(format!(
                "Hello.total_capacity may contain at most {} entries",
                limits.max_objects
            ))
            .into());
        }
        let mut capacity_key_bytes = 0usize;
        for (key, value) in &hello.total_capacity {
            if key.is_empty() || key.len() > identifier {
                return Err(RuntimeError::invalid(format!(
                    "Hello.total_capacity keys must be 1..={identifier} bytes"
                ))
                .into());
            }
            if *value < 0 {
                return Err(RuntimeError::invalid(
                    "Hello.total_capacity values cannot be negative",
                )
                .into());
            }
            capacity_key_bytes += key.len();
        }
        let attributes = to_metadata(hello.attributes);
        let metadata = to_metadata(hello.metadata);
        let limit = limits.max_metadata_bytes as usize;
        let interface_bytes: usize = hello.interfaces.iter().map(String::len).sum();
        if metadata_size(&attributes)
            + metadata_size(&metadata)
            + interface_bytes
            + capacity_key_bytes
            > limit
        {
            return Err(RuntimeError::invalid(format!(
                "worker attributes and metadata exceed {limit} bytes"
            ))
            .into());
        }
        let worker_id = if hello.worker_id.trim().is_empty() {
            new_id()
        } else {
            hello.worker_id
        };
        let interfaces: BTreeSet<String> = hello.interfaces.into_iter().collect();
        let total_capacity = hello.total_capacity.into_iter().collect();
        let max_concurrency = hello.max_concurrency.max(1);
        let pool = principal
            .forced_pool
            .clone()
            .unwrap_or_else(|| hello.pool.clone());

        let inner = &self.inner;
        let mut st = inner.lock();
        let now = now_ms();
        if auth.is_principal_invalidated(&principal, now / 1000) {
            auth.record_rejection();
            return Err(WorkerOpenError::Unauthenticated(
                "bearer token has been revoked or expired",
            ));
        }
        if st.shutting_down {
            return Err(RuntimeError::unavailable("runtime is shutting down").into());
        }
        let namespace = principal.namespace;
        let token_jti = principal.jti;
        let token_exp = principal.exp;
        let resume = !hello.resume_session_id.is_empty()
            && st
                .sessions
                .get(&hello.resume_session_id)
                .is_some_and(|session| {
                    session.worker_id == worker_id && session.namespace == namespace
                });
        if !resume {
            if st.sessions.len() >= inner.config.max_sessions {
                return Err(RuntimeError::exhausted("worker session limit reached").into());
            }
            if let Some(ns_limit) = inner.config.tenant_quotas.max_sessions_per_namespace {
                let ns_sessions = st
                    .sessions
                    .values()
                    .filter(|session| session.namespace == namespace)
                    .count();
                if ns_sessions >= ns_limit {
                    return Err(RuntimeError::exhausted(format!(
                        "worker session limit reached for namespace '{namespace}'"
                    ))
                    .into());
                }
            }
        }
        if let Some(ns_iface_limit) = inner.config.tenant_quotas.max_interfaces_per_namespace {
            let resuming_id = resume.then_some(hello.resume_session_id.as_str());
            let kept_other = st.sessions_by_interface.get(&namespace).map_or(0, |m| {
                m.iter()
                    .filter(|(iface, ids)| {
                        !interfaces.contains(iface.as_str())
                            && match resuming_id {
                                Some(sid) => ids.len() > 1 || !ids.contains(sid),
                                None => !ids.is_empty(),
                            }
                    })
                    .count()
            });
            if kept_other + interfaces.len() > ns_iface_limit {
                return Err(RuntimeError::exhausted(format!(
                    "registered interface limit reached for namespace '{namespace}'"
                ))
                .into());
            }
        }
        st.connection_seq += 1;
        let connection = st.connection_seq;
        let capacity = (max_concurrency as usize * 4 + hello.held_lease_ids.len() + 8).max(64);
        let (tx, messages) = mpsc::channel(capacity);
        let session_id = if resume {
            hello.resume_session_id.clone()
        } else {
            new_id()
        };
        let mut was_draining = false;
        if resume {
            st.unindex_session(&session_id);
            let session = st.sessions.get_mut(&session_id).expect("resumed session");
            was_draining = session.draining;
            session.pool = pool;
            session.forced_pool = principal.forced_pool;
            session.token_jti = token_jti;
            session.token_exp = token_exp;
            session.interfaces = interfaces;
            session.attributes = attributes;
            session.metadata = metadata;
            session.total_capacity = total_capacity;
            session.max_concurrency = max_concurrency;
            session.reported_free = max_concurrency;
            session.assigned_since_report = 0;
            session.tx = Some(tx.clone());
            session.connection = connection;
            session.disconnected_at_ms = None;
            session.last_seen_ms = now;
        } else {
            st.sessions.insert(
                session_id.clone(),
                Session {
                    id: session_id.clone(),
                    worker_id: worker_id.clone(),
                    namespace: namespace.clone(),
                    pool,
                    forced_pool: principal.forced_pool,
                    token_jti,
                    token_exp,
                    interfaces,
                    attributes,
                    metadata,
                    total_capacity,
                    max_concurrency,
                    reported_free: max_concurrency,
                    assigned_since_report: 0,
                    active: BTreeSet::new(),
                    reserved: Default::default(),
                    load: WorkerLoad::default(),
                    draining: false,
                    tx: Some(tx.clone()),
                    connection,
                    connected_at_ms: now,
                    last_seen_ms: now,
                    disconnected_at_ms: None,
                    consecutive_failures: 0,
                    cooldown_until_ms: 0,
                },
            );
        }
        st.index_session(&session_id);

        // Leases the worker still holds move to this session when they belong
        // to the same worker and namespace in this generation; every other one is fenced.
        let held: BTreeSet<String> = hello.held_lease_ids.into_iter().collect();
        let mut resumed = Vec::new();
        let mut fenced = Vec::new();
        for lease_id in &held {
            let movable = st.leases.get(lease_id).is_some_and(|lease| {
                !lease.cancelling
                    && st.sessions.get(&lease.session_id).is_some_and(|owner| {
                        owner.worker_id == worker_id && owner.namespace == namespace
                    })
            });
            if !movable {
                fenced.push(lease_id.clone());
                continue;
            }
            let lease = st.leases.get_mut(lease_id).expect("lease");
            let previous = std::mem::replace(&mut lease.session_id, session_id.clone());
            let job_id = lease.job_id.clone();
            let reserved = lease.reserved.clone();
            if let Some(old) = st.sessions.get_mut(&previous) {
                old.detach(lease_id, &reserved);
            }
            st.sessions
                .get_mut(&session_id)
                .expect("session")
                .attach(lease_id.clone(), &reserved);
            if let Some(attempt) = st
                .jobs
                .get_mut(&job_id)
                .and_then(|job| job.attempt.as_mut())
            {
                attempt.session_id = session_id.clone();
            }
            resumed.push(lease_id.clone());
        }
        // Leases this session had but the worker no longer reports are lost.
        let forgotten: Vec<String> = st.sessions[&session_id]
            .active
            .iter()
            .filter(|lease| !held.contains(*lease))
            .cloned()
            .collect();
        for lease_id in forgotten {
            self.lease_lost_locked(&mut st, &lease_id, now);
        }

        let limits = &inner.config.limits;
        let welcome = pb::ServerMessage {
            msg: Some(pb::server_message::Msg::Welcome(pb::Welcome {
                session_id: session_id.clone(),
                generation: st.generation,
                heartbeat_interval_ms: u32::try_from(inner.config.heartbeat_interval.as_millis())
                    .unwrap_or(u32::MAX),
                heartbeat_timeout_ms: u32::try_from(inner.config.heartbeat_timeout.as_millis())
                    .unwrap_or(u32::MAX),
                limits: Some(pb::Limits {
                    max_control_message_bytes: limits.max_control_message_bytes,
                    max_chunk_bytes: limits.max_chunk_bytes,
                    attempt_window_bytes: limits.attempt_window_bytes,
                    assign_inline_limit: limits.assign_inline_limit,
                }),
                resumed_lease_ids: resumed,
            })),
        };
        // The queue holds the welcome, optional drain, and every fence: capacity covers them.
        let _ = tx.try_send(welcome);
        if was_draining {
            let _ = tx.try_send(pb::ServerMessage {
                msg: Some(pb::server_message::Msg::Drain(pb::Drain {})),
            });
        }
        for lease_id in fenced {
            let _ = tx.try_send(pb::ServerMessage {
                msg: Some(pb::server_message::Msg::Fence(pb::Fence {
                    lease_id,
                    reason: "lease is not held by this worker in the current generation"
                        .to_string(),
                })),
            });
        }
        inner.dispatch_locked(&mut st, now);
        Ok(SessionHandle {
            session_id,
            connection,
            messages,
        })
    }

    /// Ends the attempt of `lease_id` as lost by its worker.
    fn lease_lost_locked(&self, st: &mut super::state::State, lease_id: &str, now: u64) {
        let Some(lease) = st.leases.get(lease_id) else {
            return;
        };
        if lease.cancelling {
            self.inner.release_lease_locked(st, lease_id);
            return;
        }
        let job_id = lease.job_id.clone();
        let Some(job) = st.jobs.get(&job_id) else {
            self.inner.release_lease_locked(st, lease_id);
            return;
        };
        let current = job.attempt.as_ref().is_some_and(|a| a.lease_id == lease_id);
        // Its attempt start is still being committed and the Assign not yet
        // sent, so the worker cannot have reported it.
        if current && matches!(job.phase, Phase::Starting) {
            return;
        }
        if current && matches!(job.phase, Phase::Assigned | Phase::Running) {
            self.inner.attempt_ended_locked(
                st,
                &job_id,
                ErrorInfo::new(codes::WORKER_LOST, "worker no longer holds the lease", true),
                LeaseEnd::Release,
                None,
                now,
            );
        } else {
            self.inner.release_lease_locked(st, lease_id);
        }
    }

    pub(crate) fn session_message(
        &self,
        session_id: &str,
        connection: u64,
        message: pb::worker_message::Msg,
    ) -> Result<(), RuntimeError> {
        let inner = &self.inner;
        let mut st = inner.lock();
        let now = now_ms();
        let session = st
            .sessions
            .get_mut(session_id)
            .filter(|session| session.connection == connection && session.tx.is_some())
            .ok_or_else(|| RuntimeError::stale("session is no longer current"))?;
        session.last_seen_ms = now;
        // Only messages that can open a slot are worth a dispatch pass.
        let mut may_free_slot = false;
        match message {
            pb::worker_message::Msg::Hello(_) => {
                return Err(RuntimeError::invalid("Hello may only open a session"));
            }
            pb::worker_message::Msg::Credit(credit) => {
                session.reported_free = credit.free_slots;
                session.assigned_since_report = 0;
                may_free_slot = true;
            }
            pb::worker_message::Msg::Heartbeat(heartbeat) => {
                if let Some(load) = heartbeat.load {
                    session.load = worker_load(load);
                }
            }
            pb::worker_message::Msg::Draining(draining) => {
                session.draining = true;
                if draining.preempting {
                    let leases: Vec<String> = session.active.iter().cloned().collect();
                    for lease_id in leases {
                        let Some(lease) = st.leases.get(&lease_id) else {
                            continue;
                        };
                        if lease.cancelling {
                            continue;
                        }
                        let job_id = lease.job_id.clone();
                        let preemptable = st.jobs.get(&job_id).is_some_and(|job| {
                            matches!(job.phase, Phase::Assigned | Phase::Running)
                                && job
                                    .attempt
                                    .as_ref()
                                    .is_some_and(|a| a.lease_id == lease_id && !a.exposed)
                        });
                        if preemptable {
                            inner.attempt_ended_locked(
                                &mut st,
                                &job_id,
                                ErrorInfo::new(
                                    codes::WORKER_PREEMPTED,
                                    "worker preempted while draining",
                                    true,
                                ),
                                LeaseEnd::Cancel(codes::WORKER_PREEMPTED.to_string()),
                                None,
                                now,
                            );
                            may_free_slot = true;
                        }
                    }
                }
            }
            pb::worker_message::Msg::CancelAck(ack) => {
                if st
                    .leases
                    .get(&ack.lease_id)
                    .is_some_and(|lease| lease.session_id == session_id && lease.cancelling)
                {
                    inner.release_lease_locked(&mut st, &ack.lease_id);
                    may_free_slot = true;
                }
            }
            pb::worker_message::Msg::AssignReply(reply) => {
                let job_id = st
                    .leases
                    .get(&reply.lease_id)
                    .filter(|lease| lease.session_id == session_id && !lease.cancelling)
                    .map(|lease| lease.job_id.clone())
                    .ok_or_else(|| RuntimeError::stale("lease is not active on this session"))?;
                let job = st.jobs.get_mut(&job_id).expect("leased job");
                let current = job
                    .attempt
                    .as_ref()
                    .is_some_and(|attempt| attempt.lease_id == reply.lease_id);
                if current && reply.accepted {
                    if matches!(job.phase, Phase::Assigned) {
                        st.update_job(&job_id, |job| {
                            job.phase = Phase::Running;
                            job.updated_at_ms = now;
                        });
                    }
                } else if current && matches!(job.phase, Phase::Assigned | Phase::Running) {
                    job.rejected_sessions.insert(session_id.to_string());
                    let reason = if reply.reject_reason.is_empty() {
                        "worker rejected the assignment".to_string()
                    } else {
                        super::api::truncate_utf8(
                            reply.reject_reason,
                            super::api::MAX_ERROR_MESSAGE_BYTES,
                        )
                    };
                    inner.attempt_ended_locked(
                        &mut st,
                        &job_id,
                        ErrorInfo::new(codes::WORKER_REJECTED, reason, true),
                        LeaseEnd::Release,
                        None,
                        now,
                    );
                    may_free_slot = true;
                }
            }
        }
        if may_free_slot {
            inner.dispatch_locked(&mut st, now);
        }
        Ok(())
    }

    /// The control stream of `connection` ended. Its leases survive for one
    /// heartbeat timeout so the worker can reconnect.
    pub(crate) fn session_closed(&self, session_id: &str, connection: u64) {
        let inner = &self.inner;
        let mut st = inner.lock();
        if st
            .sessions
            .get(session_id)
            .is_some_and(|session| session.connection == connection)
        {
            inner.disconnect_locked(&mut st, session_id, now_ms());
        }
    }

    /// Requests an active worker session to drain cleanly. A session of another tenant
    /// (when `namespace` is given) is reported as not found.
    pub fn drain_session(
        &self,
        namespace: Option<&str>,
        session_id: &str,
    ) -> Result<(), RuntimeError> {
        self.ensure_active_leader()?;
        let inner = &self.inner;
        let mut st = inner.lock();
        let session = st
            .sessions
            .get_mut(session_id)
            .filter(|session| namespace.is_none_or(|ns| session.namespace == ns))
            .ok_or_else(|| RuntimeError::not_found("worker session", session_id))?;
        session.draining = true;
        inner.send_locked(
            &mut st,
            session_id,
            pb::server_message::Msg::Drain(pb::Drain {}),
            now_ms(),
        );
        Ok(())
    }

    #[cfg(test)]
    pub(crate) async fn open_attempt(
        &self,
        open: &pb::AttemptOpen,
    ) -> Result<(AttemptLink, u64), RuntimeError> {
        let principal = crate::auth::Principal {
            role: crate::auth::Role::Worker,
            ..Default::default()
        };
        self.open_attempt_for(&principal, &crate::auth::AuthPolicy::default(), open)
            .await
            .map_err(|err| match err {
                WorkerOpenError::Runtime(err) => err,
                WorkerOpenError::Unauthenticated(msg) => RuntimeError::invalid(msg),
            })
    }

    pub(crate) async fn open_attempt_for(
        &self,
        principal: &crate::auth::Principal,
        auth: &crate::auth::AuthPolicy,
        open: &pb::AttemptOpen,
    ) -> Result<(AttemptLink, u64), WorkerOpenError> {
        self.ensure_active_leader()?;
        if let Some(opened) = self.open_attempt_in_memory(principal, auth, open)? {
            return Ok(opened);
        }
        if let Some((job_id, _)) = self.receipt_on_disk(&open.lease_id).await?
            && super::identity::decode_scoped_id(&job_id).0 == principal.namespace
        {
            if auth.is_principal_invalidated(principal, now_ms() / 1000) {
                auth.record_rejection();
                return Err(WorkerOpenError::Unauthenticated(
                    "bearer token has been revoked or expired",
                ));
            }
            return Ok((
                AttemptLink::Replay {
                    lease_id: open.lease_id.clone(),
                },
                0,
            ));
        }
        Err(RuntimeError::stale(
            "lease is not active in this generation and has no recorded outcome",
        )
        .into())
    }

    /// Evicts the oldest terminal jobs to free `needed` result bytes; used
    /// once before an upload is refused for the result budget.
    pub(crate) fn reclaim_results(&self, needed: usize) {
        let mut st = self.inner.lock();
        self.inner.reclaim_locked(&mut st, needed as u64, true);
    }

    /// Completion receipt of an evicted durable job.
    async fn receipt_on_disk(
        &self,
        lease_id: &str,
    ) -> Result<Option<(String, String)>, RuntimeError> {
        match &self.inner.journal {
            None => Ok(None),
            Some(journal) => journal
                .receipt(lease_id)
                .await
                .map_err(|error| RuntimeError::unavailable(format!("read journal: {error:#}"))),
        }
    }

    fn open_attempt_in_memory(
        &self,
        principal: &crate::auth::Principal,
        auth: &crate::auth::AuthPolicy,
        open: &pb::AttemptOpen,
    ) -> Result<Option<(AttemptLink, u64)>, WorkerOpenError> {
        let inner = &self.inner;
        let mut st = inner.lock();
        let now = now_ms();
        if auth.is_principal_invalidated(principal, now / 1000) {
            auth.record_rejection();
            return Err(WorkerOpenError::Unauthenticated(
                "bearer token has been revoked or expired",
            ));
        }
        let namespace = principal.namespace.as_str();
        let live_job_id = (open.generation == st.generation)
            .then(|| {
                st.leases
                    .get(&open.lease_id)
                    .filter(|lease| !lease.cancelling)
                    .map(|lease| lease.job_id.clone())
            })
            .flatten();
        if let Some(job_id) = live_job_id {
            let job = st.jobs.get_mut(&job_id).expect("leased job");
            if job.namespace != namespace {
                return Err(RuntimeError::stale(
                    "lease is not active in this generation and has no recorded outcome",
                )
                .into());
            }
            let Some(attempt) = job
                .attempt
                .as_ref()
                .filter(|attempt| attempt.lease_id == open.lease_id)
            else {
                return Err(RuntimeError::stale("lease is not the job's current attempt").into());
            };
            let resume_from = attempt.last_frame_seq;
            let inputs = attempt.inputs_via_attempt.then(|| InputPlan {
                objects: job.spec.inputs.clone(),
                params: job.spec.params.clone(),
                streaming: job.input.clone(),
            });
            if matches!(job.phase, Phase::Assigned) {
                st.update_job(&job_id, |job| {
                    job.phase = Phase::Running;
                    job.updated_at_ms = now;
                });
            }
            return Ok(Some((
                AttemptLink::Live {
                    lease_id: open.lease_id.clone(),
                    resume_from,
                    inputs,
                },
                resume_from,
            )));
        }
        if let Some((job_id, _)) = st.finished_leases.get(&open.lease_id)
            && super::identity::decode_scoped_id(job_id).0 == namespace
        {
            return Ok(Some((
                AttemptLink::Replay {
                    lease_id: open.lease_id.clone(),
                },
                0,
            )));
        }
        Ok(None)
    }

    /// Appends one output frame. Waits while a streamed consumer is behind,
    /// and fails the job if it stays behind for `consumer_stall_timeout`.
    pub(crate) async fn push_frame(
        &self,
        lease_id: &str,
        mut frame: pb::Frame,
    ) -> Result<u64, RuntimeError> {
        let limits = &self.inner.config.limits;
        if frame.data.len() > limits.max_chunk_bytes as usize {
            return Err(RuntimeError::invalid(format!(
                "frame of {} bytes exceeds max_chunk_bytes {}",
                frame.data.len(),
                limits.max_chunk_bytes
            )));
        }
        let metadata = to_metadata(std::mem::take(&mut frame.metadata));
        if metadata_size(&metadata) > limits.max_metadata_bytes as usize {
            return Err(RuntimeError::invalid("frame metadata is too large"));
        }
        let stall = self.inner.config.consumer_stall_timeout;
        let deadline = tokio::time::Instant::now() + stall;
        // Terminal jobs are evicted for room at most once per frame.
        let mut reclaimed = false;
        // Taken only once an append has failed, and followed by one more try:
        // consumer progress after that try always wakes this writer.
        let mut consumed: Option<watch::Receiver<u64>> = None;
        loop {
            let now = now_ms();
            let (job_id, cause) = {
                let mut st = self.inner.lock();
                let lease = st
                    .leases
                    .get(lease_id)
                    .filter(|lease| !lease.cancelling)
                    .ok_or_else(|| RuntimeError::stale("lease is no longer active"))?;
                let job_id = lease.job_id.clone();
                let mut job = st.jobs.get_mut(&job_id).expect("leased job");
                let attempt = job
                    .attempt
                    .as_ref()
                    .filter(|attempt| attempt.lease_id == lease_id)
                    .ok_or_else(|| RuntimeError::stale("lease is not the job's current attempt"))?;
                if frame.sequence <= attempt.last_frame_seq {
                    return Ok(attempt.last_frame_seq);
                }
                if frame.sequence != attempt.last_frame_seq + 1 {
                    return Err(RuntimeError::invalid(format!(
                        "frame sequence {} does not follow {}",
                        frame.sequence, attempt.last_frame_seq
                    )));
                }
                loop {
                    let append_res = {
                        let attempt = job.attempt.as_ref().expect("attempt");
                        job.log.append_output(
                            &attempt.attempt_id,
                            &frame.data,
                            &metadata,
                            frame.progress,
                            now,
                            reclaimed,
                        )
                    };
                    match append_res {
                        Ok(sequence) => {
                            let streamed = job.streamed;
                            let attempt = job.attempt.as_mut().expect("attempt");
                            attempt.last_frame_seq = frame.sequence;
                            // A live caller may read it any moment: never
                            // retry this attempt behind its back.
                            if streamed && !frame.progress {
                                attempt.exposed = true;
                            }
                            if matches!(job.phase, Phase::Assigned) {
                                job.phase = Phase::Running;
                            }
                            let log = Arc::clone(&job.log);
                            drop(st);
                            // Followers wake once the lock is free.
                            log.notify_appended(sequence);
                            return Ok(frame.sequence);
                        }
                        Err(LogFull::BudgetFull) if !reclaimed => {
                            let needed = (frame.data.len() + metadata_size(&metadata) + 128) as u64;
                            self.inner.reclaim_locked(&mut st, needed, false);
                            reclaimed = true;
                            job = st.jobs.get_mut(&job_id).expect("leased job");
                        }
                        Err(_) if consumed.is_none() => {
                            consumed = Some(job.log.subscribe_consumed());
                        }
                        Err(cause) => break (job_id, cause),
                    }
                }
            };
            let consumed = consumed.as_mut().expect("subscribed before waiting");
            // The frame stays unacknowledged meanwhile, so the worker's window
            // stops it from sending more.
            let woke = tokio::time::timeout_at(deadline, async {
                match cause {
                    LogFull::ConsumerBehind => consumed.changed().await.is_ok(),
                    // Room comes from other jobs: retry periodically, and on
                    // this job's consumer progress.
                    LogFull::BudgetFull => tokio::select! {
                        changed = consumed.changed() => changed.is_ok(),
                        _ = tokio::time::sleep(BUDGET_RETRY) => true,
                    },
                }
            })
            .await;
            match woke {
                Ok(true) => continue,
                Ok(false) => return Err(RuntimeError::stale("job ended")),
                Err(_) if cause == LogFull::ConsumerBehind => {
                    self.consumer_stalled(&job_id, lease_id, stall);
                    return Err(RuntimeError::stale("the job's consumer stopped reading"));
                }
                // Other jobs held the budget throughout. The job is not failed
                // here; the worker's Finish decides what happens next.
                Err(_) => {
                    return Err(RuntimeError::exhausted("event buffers stayed at capacity"));
                }
            }
        }
    }

    fn consumer_stalled(&self, job_id: &str, lease_id: &str, stall: Duration) {
        let inner = &self.inner;
        let mut st = inner.lock();
        let now = now_ms();
        let current = st.jobs.get(job_id).is_some_and(|job| {
            !job.is_terminal() && job.attempt.as_ref().is_some_and(|a| a.lease_id == lease_id)
        });
        if current {
            let error = ErrorInfo::new(
                codes::CONSUMER_STALLED,
                format!("consumer did not read output for {}s", stall.as_secs()),
                false,
            );
            inner.finish_job_locked(
                &mut st,
                job_id,
                TerminalPlan {
                    cancel_lease: Some((lease_id.to_string(), codes::CONSUMER_STALLED.to_string())),
                    ..TerminalPlan::new(JobState::Failed, Some(error))
                },
                now,
            );
        }
    }

    /// Records a worker's outcome for its lease. Repeated completions of the
    /// same lease succeed only when the server-computed digest matches.
    #[cfg(test)]
    pub(crate) async fn finish_attempt(
        &self,
        link: &AttemptLink,
        outcome: AttemptOutcome,
        outputs: Vec<Object>,
    ) -> Result<FinishStatus, RuntimeError> {
        self.finish_attempt_with_duration(link, outcome, outputs, None)
            .await
    }

    /// Records a worker's outcome for its lease along with optional
    /// worker-measured monotonic handler execution time.
    pub(crate) async fn finish_attempt_with_duration(
        &self,
        link: &AttemptLink,
        outcome: AttemptOutcome,
        outputs: Vec<Object>,
        handler_duration_ms: Option<u64>,
    ) -> Result<FinishStatus, RuntimeError> {
        let (error, outputs) = match outcome {
            AttemptOutcome::Succeeded => (None, outputs),
            AttemptOutcome::Failed(error) => (Some(error), Vec::new()),
            AttemptOutcome::Cancelled => (
                Some(ErrorInfo::new(
                    codes::CANCELLED,
                    "worker cancelled the attempt",
                    false,
                )),
                Vec::new(),
            ),
        };
        let digest = outcome_digest(&outputs, error.as_ref());
        let lease_id = link.lease_id().to_string();
        let compare = |recorded: &str| {
            if recorded == digest {
                FinishStatus::Committed
            } else {
                FinishStatus::Rejected(
                    "a different outcome was already recorded for this lease".to_string(),
                )
            }
        };
        let receiver: Option<oneshot::Receiver<Result<FinishStatus, RuntimeError>>> = 'lock: {
            let inner = &self.inner;
            let mut st = inner.lock();
            let now = now_ms();
            if let Some((_, recorded)) = st.finished_leases.get(&lease_id) {
                return Ok(compare(recorded));
            }
            // No live lease and no receipt in memory: ask the journal below.
            let Some(lease) = st.leases.get(&lease_id) else {
                break 'lock None;
            };
            if matches!(link, AttemptLink::Replay { .. }) {
                return Ok(FinishStatus::Fenced("lease is not finished".to_string()));
            }
            if st
                .jobs
                .get(&lease.job_id)
                .is_some_and(|job| job.indeterminate)
            {
                return Err(RuntimeError::unavailable(
                    "the job's commit outcome is unknown until the control plane restarts",
                ));
            }
            if lease.cancelling {
                inner.release_lease_locked(&mut st, &lease_id);
                inner.dispatch_locked(&mut st, now);
                return Ok(FinishStatus::Fenced("the job was cancelled".to_string()));
            }
            let job_id = lease.job_id.clone();
            let job = st.jobs.get_mut(&job_id).expect("leased job");
            match &job.phase {
                Phase::Terminating(plan)
                    if plan
                        .finished
                        .as_ref()
                        .is_some_and(|(lease, _)| *lease == lease_id) =>
                {
                    let same = plan.finished.as_ref().is_some_and(|(_, d)| *d == digest);
                    if !same {
                        return Ok(FinishStatus::Rejected(
                            "a different outcome is being recorded for this lease".to_string(),
                        ));
                    }
                    let (tx, rx) = oneshot::channel();
                    job.finish_waiters.push(tx);
                    Some(rx)
                }
                Phase::Assigned | Phase::Running
                    if job.attempt.as_ref().is_some_and(|a| a.lease_id == lease_id) =>
                {
                    let receipt = Some((lease_id.clone(), digest.clone()));
                    match error {
                        None => {
                            if let Some(sid) = job.attempt.as_ref().map(|a| a.session_id.clone())
                                && let Some(session) = st.sessions.get_mut(&sid)
                            {
                                session.consecutive_failures = 0;
                                session.cooldown_until_ms = 0;
                            }
                            inner.finish_job_locked(
                                &mut st,
                                &job_id,
                                TerminalPlan {
                                    outputs,
                                    release_lease: Some(lease_id.clone()),
                                    finished: receipt,
                                    handler_duration_ms,
                                    ..TerminalPlan::new(JobState::Succeeded, None)
                                },
                                now,
                            );
                            let Some(job) = st.jobs.get_mut(&job_id) else {
                                return Ok(FinishStatus::Committed);
                            };
                            if job.is_terminal() {
                                return Ok(FinishStatus::Committed);
                            }
                            let (tx, rx) = oneshot::channel();
                            job.finish_waiters.push(tx);
                            Some(rx)
                        }
                        Some(error) => {
                            let error = if error.code.is_empty() {
                                ErrorInfo::new(codes::WORKER_FAILED, error.message, error.retryable)
                            } else {
                                error
                            };
                            let retried = inner.attempt_ended_locked(
                                &mut st,
                                &job_id,
                                error,
                                LeaseEnd::Release,
                                receipt,
                                now,
                            );
                            if retried {
                                inner.dispatch_locked(&mut st, now);
                                return Ok(FinishStatus::Committed);
                            }
                            let Some(job) = st.jobs.get_mut(&job_id) else {
                                return Ok(FinishStatus::Committed);
                            };
                            if job.is_terminal() {
                                return Ok(FinishStatus::Committed);
                            }
                            let (tx, rx) = oneshot::channel();
                            job.finish_waiters.push(tx);
                            Some(rx)
                        }
                    }
                }
                _ => {
                    return Ok(FinishStatus::Fenced(
                        "lease is not the job's current attempt".to_string(),
                    ));
                }
            }
        };
        match receiver {
            Some(receiver) => receiver
                .await
                .map_err(|_| RuntimeError::unavailable("runtime stopped"))?,
            None => Ok(match self.receipt_on_disk(&lease_id).await? {
                Some((_, recorded)) => compare(&recorded),
                None => FinishStatus::Fenced("lease is no longer active".to_string()),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorKind;
    use crate::runtime::api::{Durability, JobSpec, RuntimeConfig, SubmitRequest};

    async fn next_message(handle: &mut SessionHandle) -> pb::server_message::Msg {
        tokio::time::timeout(Duration::from_secs(5), handle.messages.recv())
            .await
            .expect("control message in time")
            .expect("session open")
            .msg
            .expect("message body")
    }

    fn worker_principal(namespace: &str) -> crate::auth::Principal {
        crate::auth::Principal {
            namespace: namespace.into(),
            role: crate::auth::Role::Worker,
            subject: format!("{namespace}-w"),
            is_operator: false,
            ..Default::default()
        }
    }

    fn hello(worker_id: &str, resume: &str, held: Vec<String>) -> pb::Hello {
        pb::Hello {
            worker_id: worker_id.to_string(),
            resume_session_id: resume.to_string(),
            interfaces: vec!["echo".to_string()],
            total_capacity: [("slot".to_string(), 2)].into(),
            max_concurrency: 4,
            held_lease_ids: held,
            ..Default::default()
        }
    }

    async fn submit_slot_job(runtime: &Runtime) -> String {
        let mut spec = JobSpec::default();
        spec.demand.interface = "echo".to_string();
        spec.demand.required_capacity.insert("slot".to_string(), 1);
        spec.policy.durability = Some(Durability::Ephemeral);
        runtime
            .submit(SubmitRequest {
                spec,
                ..SubmitRequest::default()
            })
            .await
            .unwrap()
            .job
            .job_id
    }

    async fn next_assign(handle: &mut SessionHandle) -> pb::Assign {
        loop {
            if let pb::server_message::Msg::Assign(assign) = next_message(handle).await {
                return assign;
            }
        }
    }

    fn check(runtime: &Runtime) -> Vec<(u32, i64)> {
        let st = runtime.inner.lock();
        super::super::state::assert_reservations(&st);
        let mut held: Vec<(u32, i64)> = st
            .sessions
            .values()
            .map(|s| {
                (
                    s.active.len() as u32,
                    s.reserved.get("slot").copied().unwrap_or(0),
                )
            })
            .collect();
        held.sort();
        held
    }

    #[tokio::test]
    async fn reservations_track_every_lease_change() {
        let runtime = Runtime::start(RuntimeConfig::default()).await.unwrap();
        let mut first = runtime.open_session(hello("w", "", Vec::new())).unwrap();
        let a = submit_slot_job(&runtime).await;
        let b = submit_slot_job(&runtime).await;
        let lease_a = next_assign(&mut first).await.lease_id;
        let lease_b = next_assign(&mut first).await.lease_id;
        assert_eq!(check(&runtime), vec![(2, 2)]);
        // The session's two slots are reserved: a third job has to wait.
        let c = submit_slot_job(&runtime).await;
        assert_eq!(
            runtime.get_job(&c, false).await.unwrap().state,
            crate::runtime::api::JobState::Queued
        );

        // Finishing one releases its slot, which the queued job takes.
        let (link, _) = runtime
            .open_attempt(&pb::AttemptOpen {
                lease_id: lease_a.clone(),
                generation: runtime.generation(),
                resume_after_sequence: 0,
            })
            .await
            .unwrap();
        runtime
            .finish_attempt(&link, AttemptOutcome::Succeeded, Vec::new())
            .await
            .unwrap();
        let lease_c = next_assign(&mut first).await.lease_id;
        assert_eq!(check(&runtime), vec![(2, 2)]);
        let _ = a;

        // A new connection of the same worker takes the lease it reports, and
        // its capacity, from the old session; the other stays there.
        let session = first.session_id.clone();
        drop(first);
        runtime.session_closed(&session, 1);
        let _second = runtime
            .open_session(hello("w", "", vec![lease_b.clone()]))
            .unwrap();
        assert_eq!(check(&runtime), vec![(1, 1), (1, 1)]);
        let _ = lease_c;

        // Freezing returns the job's capacity.
        {
            let mut st = runtime.inner.lock();
            runtime.inner.freeze_locked(&mut st, &b, "test");
        }
        assert_eq!(check(&runtime), vec![(0, 0), (1, 1)]);
    }

    #[tokio::test]
    async fn worker_indexes_follow_changed_interfaces_drain_and_expiry() {
        let runtime = Runtime::start(RuntimeConfig::default()).await.unwrap();
        let mut handle = runtime.open_session(hello("w", "", Vec::new())).unwrap();
        let first = submit_slot_job(&runtime).await;
        let lease = next_assign(&mut handle).await.lease_id;
        let session_id = handle.session_id.clone();
        runtime.session_closed(&session_id, handle.connection);
        check(&runtime);
        let mut resumed = hello("w", &session_id, vec![lease]);
        resumed.interfaces = vec!["other".into()];
        let mut handle = runtime.open_session(resumed).unwrap();
        check(&runtime);
        let queued = submit_slot_job(&runtime).await;
        assert_eq!(
            runtime.get_job(&queued, false).await.unwrap().state,
            JobState::Queued
        );
        let mut spec = JobSpec::default();
        spec.demand.interface = "other".into();
        spec.policy.durability = Some(Durability::Ephemeral);
        let assigned = runtime
            .submit(SubmitRequest {
                spec: spec.clone(),
                ..Default::default()
            })
            .await
            .unwrap()
            .job;
        assert_eq!(next_assign(&mut handle).await.job_id, assigned.job_id);
        runtime
            .session_message(
                &session_id,
                handle.connection,
                pb::worker_message::Msg::Draining(pb::Draining::default()),
            )
            .unwrap();
        let waiting = runtime
            .submit(SubmitRequest {
                spec,
                ..Default::default()
            })
            .await
            .unwrap()
            .job;
        assert_eq!(waiting.state, JobState::Queued);
        check(&runtime);
        runtime.session_closed(&session_id, handle.connection);
        runtime.inner.fire(super::super::TimerKey::SessionGrace {
            session_id,
            connection: handle.connection,
        });
        check(&runtime);
        assert!(runtime.list_workers().is_empty());
        assert_eq!(
            runtime.get_job(&first, false).await.unwrap().state,
            JobState::Queued
        );
    }

    #[tokio::test]
    async fn durable_terminal_jobs_leave_the_running_index_and_can_be_evicted() {
        use crate::runtime::api::{JobFilter, JournalConfig};
        use crate::runtime::index::assert_matches_jobs;

        let path = std::env::temp_dir()
            .join(format!("anyserve-index-{}", uuid::Uuid::new_v4()))
            .join("journal.sqlite");
        let runtime = Runtime::start(RuntimeConfig {
            journal: Some(JournalConfig::new(path)),
            ..Default::default()
        })
        .await
        .unwrap();
        let mut session = runtime.open_session(hello("w", "", Vec::new())).unwrap();

        for terminal in [JobState::Succeeded, JobState::Failed, JobState::Cancelled] {
            let mut spec = JobSpec::default();
            spec.demand.interface = "echo".to_string();
            spec.policy.durability = Some(Durability::Durable);
            let job = runtime
                .submit(SubmitRequest {
                    spec,
                    ..Default::default()
                })
                .await
                .unwrap()
                .job;
            let assign = next_assign(&mut session).await;
            let (link, _) = runtime
                .open_attempt(&pb::AttemptOpen {
                    lease_id: assign.lease_id,
                    generation: runtime.generation(),
                    resume_after_sequence: 0,
                })
                .await
                .unwrap();
            assert_matches_jobs(&runtime.inner.lock());
            assert_eq!(runtime.stats().running, 1);

            let outcome = match terminal {
                JobState::Succeeded => AttemptOutcome::Succeeded,
                JobState::Failed => {
                    AttemptOutcome::Failed(ErrorInfo::new("test_failure", "failed", false))
                }
                JobState::Cancelled => {
                    runtime.cancel(&job.job_id, "test".into()).await.unwrap();
                    AttemptOutcome::Cancelled
                }
                _ => unreachable!(),
            };
            runtime
                .finish_attempt(&link, outcome, Vec::new())
                .await
                .unwrap();

            assert_matches_jobs(&runtime.inner.lock());
            assert_eq!(runtime.stats().running, 0);
            let listed = runtime.list_jobs(JobFilter::default()).await.unwrap();
            assert_eq!(listed.jobs.len(), 1);
            assert_eq!(listed.jobs[0].state, terminal);

            // Simulate retention expiry without waiting for the ten-minute default.
            {
                let mut st = runtime.inner.lock();
                runtime.inner.evict_terminal_locked(&mut st, u64::MAX);
                assert_matches_jobs(&st);
            }
            let listed = runtime.list_jobs(JobFilter::default()).await.unwrap();
            assert!(listed.jobs.is_empty());
            assert!(listed.next_page_token.is_none());
            assert!(runtime.health().ready);
            assert_eq!(
                runtime.get_job(&job.job_id, false).await.unwrap().state,
                terminal
            );
        }
        runtime.shutdown().await;
    }

    #[tokio::test]
    async fn finished_jobs_leave_no_timers_behind() {
        let runtime = Runtime::start(RuntimeConfig::default()).await.unwrap();
        let mut session = runtime.open_session(hello("w", "", Vec::new())).unwrap();
        for _ in 0..3 {
            let mut spec = JobSpec::default();
            spec.demand.interface = "echo".to_string();
            spec.policy.durability = Some(Durability::Ephemeral);
            spec.policy.queue_timeout = Some(Duration::from_secs(60));
            spec.policy.execution_timeout = Some(Duration::from_secs(60));
            spec.policy.total_timeout = Some(Duration::from_secs(60));
            runtime
                .submit(SubmitRequest {
                    spec,
                    ..SubmitRequest::default()
                })
                .await
                .unwrap();
            let lease_id = next_assign(&mut session).await.lease_id;
            let (link, _) = runtime
                .open_attempt(&pb::AttemptOpen {
                    lease_id,
                    generation: runtime.generation(),
                    resume_after_sequence: 0,
                })
                .await
                .unwrap();
            runtime
                .finish_attempt(&link, AttemptOutcome::Succeeded, Vec::new())
                .await
                .unwrap();
        }
        // Every deadline of a finished job is cancelled, whichever kind it is.
        let timers = runtime.inner.timers.lock();
        assert!(timers.keys.is_empty(), "{:?}", timers.keys.values());
        assert_eq!(timers.cancelled.len(), 9);
    }

    #[tokio::test]
    async fn a_frozen_job_returns_capacity_and_refuses_finish() {
        let runtime = Runtime::start(RuntimeConfig::default()).await.unwrap();
        let mut handle = runtime
            .open_session(pb::Hello {
                worker_id: "w".to_string(),
                interfaces: vec!["echo".to_string()],
                max_concurrency: 1,
                ..Default::default()
            })
            .unwrap();
        let mut spec = JobSpec::default();
        spec.demand.interface = "echo".to_string();
        spec.policy.durability = Some(Durability::Ephemeral);
        let job = runtime
            .submit(SubmitRequest {
                spec,
                ..SubmitRequest::default()
            })
            .await
            .unwrap()
            .job;
        let assign = loop {
            if let pb::server_message::Msg::Assign(assign) = next_message(&mut handle).await {
                break assign;
            }
        };
        let (link, _) = runtime
            .open_attempt(&pb::AttemptOpen {
                lease_id: assign.lease_id.clone(),
                generation: assign.generation,
                resume_after_sequence: 0,
            })
            .await
            .unwrap();
        {
            let mut st = runtime.inner.lock();
            runtime.inner.freeze_locked(&mut st, &job.job_id, "test");
        }
        assert_eq!(runtime.list_workers()[0].active_leases, 0);
        let fence = loop {
            if let pb::server_message::Msg::Fence(fence) = next_message(&mut handle).await {
                break fence;
            }
        };
        assert_eq!(fence.lease_id, assign.lease_id);
        let refused = tokio::time::timeout(
            Duration::from_secs(1),
            runtime.finish_attempt(&link, AttemptOutcome::Succeeded, Vec::new()),
        )
        .await
        .expect("a frozen job's Finish does not wait")
        .unwrap_err();
        assert_eq!(refused.kind, ErrorKind::PersistenceUnavailable);
    }

    /// Bytes whose last drop records whether the runtime lock was free then.
    struct DropProbe {
        inner: std::sync::Weak<super::super::Inner>,
        lock_free: Arc<std::sync::Mutex<Option<bool>>>,
    }

    impl AsRef<[u8]> for DropProbe {
        fn as_ref(&self) -> &[u8] {
            b"probe"
        }
    }

    impl Drop for DropProbe {
        fn drop(&mut self) {
            let free = self
                .inner
                .upgrade()
                .is_some_and(|inner| inner.state.try_lock().is_ok());
            *self.lock_free.lock().unwrap() = Some(free);
        }
    }

    #[tokio::test]
    async fn released_jobs_and_events_are_freed_after_the_lock() {
        let runtime = Runtime::start(RuntimeConfig {
            max_terminal_jobs: 1,
            ..RuntimeConfig::default()
        })
        .await
        .unwrap();
        let mut session = runtime.open_session(hello("w", "", Vec::new())).unwrap();
        let probe = |lock_free: &Arc<std::sync::Mutex<Option<bool>>>| {
            Bytes::from_owner(DropProbe {
                inner: Arc::downgrade(&runtime.inner),
                lock_free: Arc::clone(lock_free),
            })
        };
        let frame_freed = Arc::new(std::sync::Mutex::new(None));
        let result_freed = Arc::new(std::sync::Mutex::new(None));
        for round in 0..2 {
            submit_slot_job(&runtime).await;
            let lease_id = next_assign(&mut session).await.lease_id;
            let (link, _) = runtime
                .open_attempt(&pb::AttemptOpen {
                    lease_id: lease_id.clone(),
                    generation: runtime.generation(),
                    resume_after_sequence: 0,
                })
                .await
                .unwrap();
            let mut outputs = Vec::new();
            if round == 0 {
                runtime
                    .push_frame(
                        &lease_id,
                        pb::Frame {
                            sequence: 1,
                            data: probe(&frame_freed),
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap();
                outputs.push(Object::Inline {
                    data: probe(&result_freed),
                    metadata: Metadata::new(),
                });
            }
            runtime
                .finish_attempt(&link, AttemptOutcome::Succeeded, outputs)
                .await
                .unwrap();
            if round == 0 {
                // Nobody follows the first job: finishing it compacted its
                // log, frame included.
                assert_eq!(*frame_freed.lock().unwrap(), Some(true));
                assert_eq!(*result_freed.lock().unwrap(), None);
            }
        }
        // Finishing the second job evicted the first, result included.
        assert_eq!(*result_freed.lock().unwrap(), Some(true));
        assert!(runtime.inner.lock().released.jobs.is_empty());
    }

    #[tokio::test]
    async fn cross_namespace_held_leases_are_fenced_even_for_matching_worker_id() {
        let runtime = Runtime::start(RuntimeConfig::default()).await.unwrap();
        let auth = crate::auth::AuthPolicy::default();
        let alpha_principal = worker_principal("alpha");
        let beta_principal = worker_principal("beta");
        let mut alpha_session = runtime
            .open_session_for(
                hello("shared-worker", "", Vec::new()),
                alpha_principal,
                &auth,
            )
            .unwrap();
        let mut spec = JobSpec::default();
        spec.demand.interface = "echo".into();
        spec.policy.durability = Some(Durability::Ephemeral);
        runtime
            .submit(SubmitRequest {
                namespace: Some("alpha".into()),
                job_id: Some("alpha-job".into()),
                spec,
                ..Default::default()
            })
            .await
            .unwrap();
        let lease_id = next_assign(&mut alpha_session).await.lease_id;

        // A worker in namespace "beta" with the exact same worker_id tries to claim alpha's lease.
        let mut beta_session = runtime
            .open_session_for(
                hello("shared-worker", "", vec![lease_id.clone()]),
                beta_principal,
                &auth,
            )
            .unwrap();
        match next_message(&mut beta_session).await {
            pb::server_message::Msg::Welcome(welcome) => {
                assert!(
                    welcome.resumed_lease_ids.is_empty(),
                    "cross-namespace lease must not resume: {:?}",
                    welcome.resumed_lease_ids
                );
            }
            other => panic!("expected Welcome, got {other:?}"),
        }
        match next_message(&mut beta_session).await {
            pb::server_message::Msg::Fence(fence) => assert_eq!(fence.lease_id, lease_id),
            other => panic!("expected Fence for stolen lease, got {other:?}"),
        }
        // Alpha's session still holds its lease.
        assert_eq!(
            runtime.inner.lock().leases[&lease_id].session_id,
            alpha_session.session_id
        );
    }

    #[tokio::test]
    async fn retrying_job_cancels_stale_phase_timers_and_dispatches_queued_work() {
        let runtime = Runtime::start(RuntimeConfig {
            default_initial_backoff: Duration::from_secs(60),
            default_max_backoff: Duration::from_secs(60),
            default_max_attempts: 3,
            default_max_in_flight_per_key: Some(1),
            ..Default::default()
        })
        .await
        .unwrap();
        let mut w1 = runtime.open_session(hello("w1", "", Vec::new())).unwrap();
        let mut w2 = runtime.open_session(hello("w2", "", Vec::new())).unwrap();

        let make_spec = || {
            let mut spec = JobSpec::default();
            spec.demand.interface = "echo".into();
            spec.policy.durability = Some(Durability::Ephemeral);
            spec.policy.concurrency_key = "arm-1".into();
            spec.policy.queue_timeout = Some(Duration::from_secs(60));
            spec.policy.execution_timeout = Some(Duration::from_secs(60));
            spec.policy.total_timeout = Some(Duration::from_secs(60));
            spec
        };
        runtime
            .submit(SubmitRequest {
                job_id: Some("job-1".into()),
                spec: make_spec(),
                ..Default::default()
            })
            .await
            .unwrap();
        runtime
            .submit(SubmitRequest {
                job_id: Some("job-2".into()),
                spec: make_spec(),
                ..Default::default()
            })
            .await
            .unwrap();

        let assign1 = tokio::select! {
            a = next_assign(&mut w1) => a,
            a = next_assign(&mut w2) => a,
        };
        let (link1, _) = runtime
            .open_attempt(&pb::AttemptOpen {
                lease_id: assign1.lease_id.clone(),
                generation: runtime.generation(),
                resume_after_sequence: 0,
            })
            .await
            .unwrap();
        // Retryable failure on job-1 releases the concurrency key and immediately dispatches job-2.
        runtime
            .finish_attempt(
                &link1,
                AttemptOutcome::Failed(ErrorInfo::new("flaky", "retry", true)),
                Vec::new(),
            )
            .await
            .unwrap();

        let job2_state = runtime.get_job("job-2", false).await.unwrap().state;
        assert_eq!(job2_state, JobState::Running);

        // Stale phase timers from job-1's previous queue/attempt must already be cancelled
        // while job-1 waits for retry backoff; only its TotalDeadline and the new QueueDeadline + Dispatch remain.
        {
            let st = runtime.inner.lock();
            let scoped1 =
                super::super::identity::encode_scoped_id(crate::auth::DEFAULT_NAMESPACE, "job-1");
            let job1 = &st.jobs[&scoped1];
            assert!(job1.total_timer.is_some());
            assert_eq!(job1.timers.len(), 2);
            let timers = runtime.inner.timers.lock();
            assert!(!timers.keys.values().any(|k| matches!(
                k,
                super::super::TimerKey::ExecutionDeadline { job_id, .. } if job_id == &scoped1
            )));
        }

        // Complete job-2 cleanly and cancel waiting job-1: zero live timers remain.
        let assign2 = tokio::select! {
            a = next_assign(&mut w1) => a,
            a = next_assign(&mut w2) => a,
        };
        let (link2, _) = runtime
            .open_attempt(&pb::AttemptOpen {
                lease_id: assign2.lease_id.clone(),
                generation: runtime.generation(),
                resume_after_sequence: 0,
            })
            .await
            .unwrap();
        runtime
            .finish_attempt(&link2, AttemptOutcome::Succeeded, Vec::new())
            .await
            .unwrap();
        runtime.cancel("job-1", "done".into()).await.unwrap();
        let timers = runtime.inner.timers.lock();
        assert!(timers.keys.is_empty(), "{:?}", timers.keys);
    }

    #[tokio::test]
    async fn cross_namespace_attempt_open_is_fenced_live_and_replay() {
        let runtime = Runtime::start(RuntimeConfig::default()).await.unwrap();
        let auth = crate::auth::AuthPolicy::default();
        let alpha = worker_principal("alpha");
        let beta = worker_principal("beta");
        let mut w_alpha = runtime
            .open_session_for(hello("w-alpha", "", Vec::new()), alpha.clone(), &auth)
            .unwrap();
        let mut spec = JobSpec::default();
        spec.demand.interface = "echo".into();
        spec.policy.durability = Some(Durability::Ephemeral);
        runtime
            .submit(SubmitRequest {
                namespace: Some("alpha".into()),
                job_id: Some("j-alpha".into()),
                spec,
                ..Default::default()
            })
            .await
            .unwrap();
        let assign = next_assign(&mut w_alpha).await;

        // Tenant "beta" must not open tenant "alpha"'s live lease.
        let err = match runtime
            .open_attempt_for(
                &beta,
                &auth,
                &pb::AttemptOpen {
                    lease_id: assign.lease_id.clone(),
                    generation: assign.generation,
                    resume_after_sequence: 0,
                },
            )
            .await
        {
            Ok(_) => panic!("beta worker must not open alpha live lease"),
            Err(WorkerOpenError::Runtime(err)) => err,
            Err(other) => panic!("expected StaleLease, got {other:?}"),
        };
        assert_eq!(err.kind, ErrorKind::StaleLease);

        // Tenant "alpha" opens and completes the attempt.
        let (link, _) = runtime
            .open_attempt_for(
                &alpha,
                &auth,
                &pb::AttemptOpen {
                    lease_id: assign.lease_id.clone(),
                    generation: assign.generation,
                    resume_after_sequence: 0,
                },
            )
            .await
            .unwrap();
        runtime
            .finish_attempt(&link, AttemptOutcome::Succeeded, Vec::new())
            .await
            .unwrap();

        // Tenant "beta" must also be fenced when attempting to replay alpha's finished lease.
        let err = match runtime
            .open_attempt_for(
                &beta,
                &auth,
                &pb::AttemptOpen {
                    lease_id: assign.lease_id.clone(),
                    generation: assign.generation,
                    resume_after_sequence: 0,
                },
            )
            .await
        {
            Ok(_) => panic!("beta worker must not replay alpha finished lease"),
            Err(WorkerOpenError::Runtime(err)) => err,
            Err(other) => panic!("expected StaleLease, got {other:?}"),
        };
        assert_eq!(err.kind, ErrorKind::StaleLease);
    }

    #[tokio::test]
    async fn empty_queues_and_namespaces_are_cleaned_up_on_dequeue() {
        let runtime = Runtime::start(RuntimeConfig::default()).await.unwrap();
        let mut spec = JobSpec::default();
        spec.demand.interface = "ephemeral.iface".into();
        spec.policy.durability = Some(Durability::Ephemeral);
        let outcome = runtime
            .submit(SubmitRequest {
                namespace: Some("tenant-temp".into()),
                job_id: Some("temp-1".into()),
                spec,
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(!runtime.inner.lock().queues.is_empty());
        runtime
            .cancel_in("tenant-temp", &outcome.job.job_id, "done".into())
            .await
            .unwrap();
        let st = runtime.inner.lock();
        assert!(
            st.queues.is_empty(),
            "empty namespace/interface queues leaked: {:?}",
            st.queues.keys().collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn ephemeral_guest_namespaces_reclaim_all_maps_on_drain_and_eviction() {
        let runtime = Runtime::start(RuntimeConfig {
            max_terminal_jobs: 0,
            heartbeat_timeout: Duration::from_millis(50),
            reconnect_grace: Some(Duration::from_millis(10)),
            ..Default::default()
        })
        .await
        .unwrap();
        let auth = crate::auth::AuthPolicy::default();

        for i in 0..200u32 {
            let ns = format!("guest-{i:012x}");
            let mut sess = runtime
                .open_session_for(
                    hello(&format!("w-{i}"), "", Vec::new()),
                    worker_principal(&ns),
                    &auth,
                )
                .unwrap();
            let mut spec = JobSpec::default();
            spec.demand.interface = "echo".into();
            spec.policy.durability = Some(Durability::Ephemeral);
            runtime
                .submit(SubmitRequest {
                    namespace: Some(ns.clone()),
                    job_id: Some(format!("j-{i}")),
                    spec,
                    ..Default::default()
                })
                .await
                .unwrap();
            let assign = next_assign(&mut sess).await;
            let (link, _) = runtime
                .open_attempt_for(
                    &worker_principal(&ns),
                    &auth,
                    &pb::AttemptOpen {
                        lease_id: assign.lease_id.clone(),
                        generation: assign.generation,
                        resume_after_sequence: 0,
                    },
                )
                .await
                .unwrap();
            runtime
                .finish_attempt(&link, AttemptOutcome::Succeeded, Vec::new())
                .await
                .unwrap();
            runtime.session_closed(&sess.session_id, sess.connection);
        }

        // Wait past reconnect_grace so timer sweep removes all 200 disconnected guest sessions.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            {
                let st = runtime.inner.lock();
                if st.sessions.is_empty() {
                    break;
                }
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for disconnected guest sessions to expire"
            );
            tokio::time::sleep(Duration::from_millis(15)).await;
        }

        let st = runtime.inner.lock();
        assert!(st.jobs.is_empty(), "jobs leaked: {}", st.jobs.len());
        assert_eq!(
            st.job_index.namespace_count(),
            0,
            "job_index namespaces leaked"
        );
        assert!(st.queues.is_empty(), "queues leaked: {}", st.queues.len());
        assert!(
            st.scan_cursor.is_empty(),
            "scan_cursor leaked: {}",
            st.scan_cursor.len()
        );
        assert!(
            st.accepting.is_empty(),
            "accepting leaked: {}",
            st.accepting.len()
        );
        assert!(
            st.in_flight_concurrency.is_empty(),
            "in_flight_concurrency leaked: {}",
            st.in_flight_concurrency.len()
        );
        assert!(
            st.sessions_by_interface.is_empty(),
            "sessions_by_interface leaked: {}",
            st.sessions_by_interface.len()
        );
    }

    #[tokio::test]
    async fn hello_heartbeat_and_routing_key_bounds_enforced() {
        let runtime = Runtime::start(RuntimeConfig::default()).await.unwrap();
        // max_concurrency > 65536 rejected.
        let mut bad_hello = hello("w1", "", Vec::new());
        bad_hello.max_concurrency = 100_000;
        assert!(runtime.open_session(bad_hello).is_err());

        // Negative or oversized total_capacity rejected.
        let mut neg_cap = hello("w1", "", Vec::new());
        neg_cap.total_capacity.insert("gpu".into(), -1);
        assert!(runtime.open_session(neg_cap).is_err());

        // Routing key length > max_identifier_bytes rejected.
        let mut big_key = JobSpec::default();
        big_key.demand.interface = "echo".into();
        big_key.demand.affinity_key = "k".repeat(300);
        assert!(
            runtime
                .submit(SubmitRequest {
                    spec: big_key,
                    ..Default::default()
                })
                .await
                .is_err()
        );

        // Heartbeat utilization NaN/Inf sanitized and custom capped; saturating total_capacity sum.
        let mut big_cap = hello("w-cap-1", "", Vec::new());
        big_cap.total_capacity.insert("mem".into(), i64::MAX);
        let h1 = runtime.open_session(big_cap).unwrap();
        let mut big_cap2 = hello("w-cap-2", "", Vec::new());
        big_cap2.total_capacity.insert("mem".into(), i64::MAX);
        let _h2 = runtime.open_session(big_cap2).unwrap();

        let mut custom = std::collections::HashMap::new();
        custom.insert("nan".into(), f64::NAN);
        for i in 0..80 {
            custom.insert(format!("k{i}"), i as f64);
        }
        runtime
            .session_message(
                &h1.session_id,
                h1.connection,
                pb::worker_message::Msg::Heartbeat(pb::Heartbeat {
                    load: Some(pb::Load {
                        queued: 1,
                        running: 1,
                        utilization: f64::NAN,
                        custom,
                    }),
                }),
            )
            .unwrap();
        let snap = runtime.snapshot();
        assert_eq!(snap.workers.total_capacity["mem"], i64::MAX);
        let w = runtime
            .list_workers()
            .into_iter()
            .find(|w| w.session_id == h1.session_id)
            .unwrap();
        assert_eq!(w.load.utilization, 0.0);
        assert!(w.load.custom.len() <= 64);
        assert!(!w.load.custom.contains_key("nan"));
    }

    #[tokio::test]
    async fn disconnect_revoked_and_expired_sessions_isolates_victims() {
        let runtime = Runtime::start(RuntimeConfig::default()).await.unwrap();
        let policy = crate::auth::AuthPolicy::default();
        let now = crate::now_ms() / 1000;
        let p_revoked = crate::auth::Principal {
            jti: Some("jti-bad".into()),
            exp: Some(now + 3600),
            ..worker_principal("ns-a")
        };
        let p_keep = crate::auth::Principal {
            jti: Some("jti-ok".into()),
            exp: Some(now + 3600),
            ..worker_principal("ns-b")
        };
        let s_bad = runtime
            .open_session_for(hello("w-bad", "", Vec::new()), p_revoked, &policy)
            .unwrap();
        let s_ok = runtime
            .open_session_for(hello("w-ok", "", Vec::new()), p_keep, &policy)
            .unwrap();

        policy.apply_revocations(crate::auth::RevocationSnapshot {
            version: 1,
            revoked_jtis: vec!["jti-bad".into()],
        });
        assert_eq!(runtime.disconnect_revoked_and_expired_sessions(&policy), 1);
        let workers = runtime.list_workers();
        assert!(!workers.iter().any(|w| w.session_id == s_bad.session_id));
        assert!(
            workers
                .iter()
                .any(|w| w.session_id == s_ok.session_id && w.connected)
        );
    }

    #[tokio::test]
    async fn tenant_quotas_isolate_noisy_namespace() {
        let runtime = Runtime::start(RuntimeConfig {
            tenant_quotas: crate::runtime::api::TenantQuotas {
                max_sessions_per_namespace: Some(1),
                max_pending_jobs_per_namespace: Some(1),
                max_interfaces_per_namespace: Some(1),
            },
            ..Default::default()
        })
        .await
        .unwrap();
        let auth = crate::auth::AuthPolicy::default();

        let _s1 = runtime
            .open_session_for(
                hello("w1", "", Vec::new()),
                worker_principal("noisy"),
                &auth,
            )
            .unwrap();
        // Second session in "noisy" is blocked, while "clean" succeeds.
        assert!(
            runtime
                .open_session_for(
                    hello("w2", "", Vec::new()),
                    worker_principal("noisy"),
                    &auth
                )
                .is_err()
        );
        let _clean = runtime
            .open_session_for(
                hello("w1", "", Vec::new()),
                worker_principal("clean"),
                &auth,
            )
            .unwrap();

        let mut spec1 = JobSpec::default();
        spec1.demand.interface = "unserved.a".into();
        spec1.policy.durability = Some(Durability::Ephemeral);
        runtime
            .submit(SubmitRequest {
                namespace: Some("noisy".into()),
                spec: spec1,
                ..Default::default()
            })
            .await
            .unwrap();
        // Second pending job / interface in "noisy" rejected; "clean" succeeds.
        let mut spec2 = JobSpec::default();
        spec2.demand.interface = "unserved.b".into();
        spec2.policy.durability = Some(Durability::Ephemeral);
        assert!(
            runtime
                .submit(SubmitRequest {
                    namespace: Some("noisy".into()),
                    spec: spec2.clone(),
                    ..Default::default()
                })
                .await
                .is_err()
        );
        runtime
            .submit(SubmitRequest {
                namespace: Some("clean".into()),
                spec: spec2,
                ..Default::default()
            })
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn lifecycle_counters_and_drain_session_behave_accurately() {
        let runtime = Runtime::start(RuntimeConfig::default()).await.unwrap();
        let auth = crate::auth::AuthPolicy::default();
        let alpha = worker_principal("alpha");
        let mut h = runtime
            .open_session_for(hello("w-drain", "", Vec::new()), alpha.clone(), &auth)
            .unwrap();
        let _welcome = next_message(&mut h).await;

        // Cross-namespace drain reports NotFound without affecting the session.
        let err = runtime
            .drain_session(Some("beta"), &h.session_id)
            .unwrap_err();
        assert_eq!(err.kind, ErrorKind::NotFound);
        assert!(!runtime.list_workers()[0].draining);

        // Submit + dispatch + succeed 1 job, fail 1 job, cancel 1 queued job.
        let mut spec = JobSpec::default();
        spec.demand.interface = "echo".into();
        spec.policy.durability = Some(Durability::Ephemeral);
        spec.policy.max_attempts = Some(1);

        runtime
            .submit(SubmitRequest {
                namespace: Some("alpha".into()),
                spec: spec.clone(),
                ..Default::default()
            })
            .await
            .unwrap();
        let a1 = next_assign(&mut h).await;
        let (link1, _) = runtime
            .open_attempt_for(
                &alpha,
                &auth,
                &pb::AttemptOpen {
                    lease_id: a1.lease_id.clone(),
                    generation: a1.generation,
                    resume_after_sequence: 0,
                },
            )
            .await
            .unwrap();
        runtime
            .finish_attempt(&link1, AttemptOutcome::Succeeded, Vec::new())
            .await
            .unwrap();

        // Fail 1 job cleanly.
        runtime
            .submit(SubmitRequest {
                namespace: Some("alpha".into()),
                spec: spec.clone(),
                ..Default::default()
            })
            .await
            .unwrap();
        let a2 = next_assign(&mut h).await;
        let (link2, _) = runtime
            .open_attempt_for(
                &alpha,
                &auth,
                &pb::AttemptOpen {
                    lease_id: a2.lease_id,
                    generation: a2.generation,
                    resume_after_sequence: 0,
                },
            )
            .await
            .unwrap();
        runtime
            .finish_attempt(
                &link2,
                AttemptOutcome::Failed(ErrorInfo::new("boom", "boom", false)),
                Vec::new(),
            )
            .await
            .unwrap();

        // Drain session in matching namespace.
        runtime.drain_session(Some("alpha"), &h.session_id).unwrap();
        assert!(runtime.list_workers()[0].draining);
        assert!(matches!(
            next_message(&mut h).await,
            pb::server_message::Msg::Drain(_)
        ));

        // Submit & cancel a queued job while worker is draining.
        let j3 = runtime
            .submit(SubmitRequest {
                namespace: Some("alpha".into()),
                spec,
                ..Default::default()
            })
            .await
            .unwrap()
            .job
            .job_id;
        runtime
            .cancel_in("alpha", &j3, "test cancel".into())
            .await
            .unwrap();

        let stats = runtime.snapshot().stats;
        assert_eq!(stats.jobs_submitted_total, 3);
        assert_eq!(stats.attempts_started_total, 2);
        assert_eq!(stats.jobs_succeeded_total, 1);
        assert_eq!(stats.jobs_failed_total, 1);
        assert_eq!(stats.jobs_cancelled_total, 1);
    }

    #[tokio::test]
    async fn max_interfaces_per_namespace_covers_ephemeral_accepting_and_workers() {
        use crate::journal::{
            BoxFuture, CommitOutcome, JournalBackend, JournalOp, Leadership, LeaseHandle, Recovery,
        };

        struct HoldAcceptBackend {
            gate: Arc<tokio::sync::Notify>,
            held_tx: Arc<std::sync::Mutex<Vec<oneshot::Sender<CommitOutcome>>>>,
        }

        impl JournalBackend for HoldAcceptBackend {
            fn try_acquire_leadership(&self) -> BoxFuture<'_, anyhow::Result<Option<Leadership>>> {
                Box::pin(async {
                    Ok(Some(Leadership {
                        term: 1,
                        recovery: Recovery {
                            generation: 1,
                            active: Vec::new(),
                        },
                        lease: LeaseHandle::always_held(),
                    }))
                })
            }
            fn enqueue(
                &self,
                _term: u64,
                op: JournalOp,
            ) -> Result<oneshot::Receiver<CommitOutcome>, RuntimeError> {
                let (tx, rx) = oneshot::channel();
                if matches!(op, JournalOp::Accept(_)) {
                    self.held_tx.lock().unwrap().push(tx);
                    self.gate.notify_waiters();
                } else {
                    let _ = tx.send(CommitOutcome::Committed);
                }
                Ok(rx)
            }
            fn spec_hash<'a>(
                &'a self,
                _: &'a str,
            ) -> BoxFuture<'a, anyhow::Result<Option<String>>> {
                Box::pin(async { Ok(None) })
            }
            fn receipt<'a>(
                &'a self,
                _: &'a str,
            ) -> BoxFuture<'a, anyhow::Result<Option<(String, String)>>> {
                Box::pin(async { Ok(None) })
            }
            fn load_job<'a>(
                &'a self,
                _: &'a str,
                _: bool,
                _: Arc<crate::runtime::events::ByteBudget>,
            ) -> BoxFuture<'a, anyhow::Result<Option<crate::journal::StoredJob>>> {
                Box::pin(async { Ok(None) })
            }
            fn load_job_preview<'a>(
                &'a self,
                _: &'a str,
            ) -> BoxFuture<'a, anyhow::Result<Option<crate::journal::StoredJobPreview>>>
            {
                Box::pin(async { Ok(None) })
            }
            fn scan_jobs<'a>(
                &'a self,
                _: u64,
                _: u64,
                _: Option<(u64, String)>,
                _: Option<&'a str>,
                _: usize,
            ) -> BoxFuture<'a, anyhow::Result<Vec<crate::journal::StoredJob>>> {
                Box::pin(async { Ok(Vec::new()) })
            }
            fn purge<'a>(&'a self, _: u64, _: u64) -> BoxFuture<'a, anyhow::Result<usize>> {
                Box::pin(async { Ok(0) })
            }
            fn stats(&self) -> crate::runtime::api::JournalStats {
                Default::default()
            }
            fn close(&self) -> BoxFuture<'_, ()> {
                Box::pin(async {})
            }
        }

        let gate = Arc::new(tokio::sync::Notify::new());
        let held_tx = Arc::new(std::sync::Mutex::new(Vec::new()));
        let backend = Arc::new(HoldAcceptBackend {
            gate: Arc::clone(&gate),
            held_tx: Arc::clone(&held_tx),
        });
        let runtime = Runtime::start_with_backend(
            RuntimeConfig {
                tenant_quotas: crate::runtime::api::TenantQuotas {
                    max_sessions_per_namespace: None,
                    max_pending_jobs_per_namespace: None,
                    max_interfaces_per_namespace: Some(2),
                },
                ..Default::default()
            },
            Some(backend),
        )
        .await
        .unwrap();
        let auth = crate::auth::AuthPolicy::default();

        // 1. Ephemeral distinct interface cap (without pending-job cap collision).
        let make_spec = |iface: &str, dur: Durability| {
            let mut spec = JobSpec::default();
            spec.demand.interface = iface.into();
            spec.policy.durability = Some(dur);
            spec
        };
        for iface in ["eph.a", "eph.b", "eph.a"] {
            runtime
                .submit(SubmitRequest {
                    namespace: Some("ns-eph".into()),
                    spec: make_spec(iface, Durability::Ephemeral),
                    ..Default::default()
                })
                .await
                .unwrap();
        }
        assert_eq!(
            runtime
                .submit(SubmitRequest {
                    namespace: Some("ns-eph".into()),
                    spec: make_spec("eph.c", Durability::Ephemeral),
                    ..Default::default()
                })
                .await
                .unwrap_err()
                .kind,
            ErrorKind::ResourceExhausted
        );

        // 2. Concurrent durable Accepting jobs count distinct interfaces before commit.
        let rt1 = runtime.clone();
        let s1 = tokio::spawn(async move {
            rt1.submit(SubmitRequest {
                namespace: Some("ns-dur".into()),
                spec: make_spec("dur.a", Durability::Durable),
                ..Default::default()
            })
            .await
        });
        let rt2 = runtime.clone();
        let s2 = tokio::spawn(async move {
            rt2.submit(SubmitRequest {
                namespace: Some("ns-dur".into()),
                spec: make_spec("dur.b", Durability::Durable),
                ..Default::default()
            })
            .await
        });
        while held_tx.lock().unwrap().len() < 2 {
            gate.notified().await;
        }
        assert_eq!(
            runtime
                .submit(SubmitRequest {
                    namespace: Some("ns-dur".into()),
                    spec: make_spec("dur.c", Durability::Durable),
                    ..Default::default()
                })
                .await
                .unwrap_err()
                .kind,
            ErrorKind::ResourceExhausted
        );
        for tx in held_tx.lock().unwrap().drain(..) {
            let _ = tx.send(CommitOutcome::Committed);
        }
        s1.await.unwrap().unwrap();
        s2.await.unwrap().unwrap();

        // 3. Worker Hello.interfaces cap (fresh, cross-session, and resume replacement).
        let mut too_many = hello("w-over", "", Vec::new());
        too_many.interfaces = vec!["i.1".into(), "i.2".into(), "i.3".into()];
        assert!(
            runtime
                .open_session_for(too_many, worker_principal("ns-w"), &auth)
                .is_err()
        );

        let mut h1_hello = hello("w-1", "", Vec::new());
        h1_hello.interfaces = vec!["i.1".into(), "i.2".into()];
        let h1 = runtime
            .open_session_for(h1_hello, worker_principal("ns-w"), &auth)
            .unwrap();
        let mut h2_hello = hello("w-2", "", Vec::new());
        h2_hello.interfaces = vec!["i.2".into(), "i.3".into()];
        assert!(
            runtime
                .open_session_for(h2_hello, worker_principal("ns-w"), &auth)
                .is_err()
        );
        // Resuming w-1 replaces its own interface set, so ["i.3", "i.4"] stays within cap 2.
        let mut resume_h1 = hello("w-1", &h1.session_id, Vec::new());
        resume_h1.interfaces = vec!["i.3".into(), "i.4".into()];
        let _resumed = runtime
            .open_session_for(resume_h1, worker_principal("ns-w"), &auth)
            .unwrap();
        runtime.shutdown().await;
    }

    #[tokio::test]
    async fn active_lease_revocation_fences_immediately_expires_via_maintenance_and_preserves_drain()
     {
        let runtime = Runtime::start(RuntimeConfig {
            reconnect_grace: Some(Duration::from_secs(60)),
            default_max_attempts: 2,
            ..Default::default()
        })
        .await
        .unwrap();
        let policy = crate::auth::AuthPolicy::default();
        let now_s = crate::now_ms() / 1000;
        let p_rev = crate::auth::Principal {
            jti: Some("jti-active".into()),
            exp: Some(now_s + 3600),
            ..worker_principal("ns-r")
        };
        let mut handle = runtime
            .open_session_for(hello("w-rev", "", Vec::new()), p_rev.clone(), &policy)
            .unwrap();
        let mut spec = JobSpec::default();
        spec.demand.interface = "echo".into();
        spec.policy.durability = Some(Durability::Ephemeral);
        let job = runtime
            .submit(SubmitRequest {
                namespace: Some("ns-r".into()),
                spec: spec.clone(),
                ..Default::default()
            })
            .await
            .unwrap()
            .job;
        let assign = next_assign(&mut handle).await;
        let (link, _) = runtime
            .open_attempt_for(
                &p_rev,
                &policy,
                &pb::AttemptOpen {
                    lease_id: assign.lease_id.clone(),
                    generation: assign.generation,
                    resume_after_sequence: 0,
                },
            )
            .await
            .unwrap();
        runtime
            .push_frame(
                &assign.lease_id,
                pb::Frame {
                    sequence: 1,
                    data: Bytes::from_static(b"ok"),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        // Revoke JTI: session + lease fenced IMMEDIATELY without waiting 60s reconnect_grace.
        policy.apply_revocations(crate::auth::RevocationSnapshot {
            version: 1,
            revoked_jtis: vec!["jti-active".into()],
        });
        assert_eq!(runtime.disconnect_revoked_and_expired_sessions(&policy), 1);
        assert!(runtime.list_workers().is_empty());
        assert!(handle.messages.recv().await.is_none());
        assert_eq!(
            runtime
                .get_job_in("ns-r", &job.job_id, false)
                .await
                .unwrap()
                .state,
            JobState::Queued
        );
        assert_eq!(
            runtime
                .push_frame(
                    &assign.lease_id,
                    pb::Frame {
                        sequence: 2,
                        data: Bytes::from_static(b"stale"),
                        ..Default::default()
                    },
                )
                .await
                .unwrap_err()
                .kind,
            ErrorKind::StaleLease
        );
        assert!(matches!(
            runtime
                .finish_attempt(&link, AttemptOutcome::Succeeded, Vec::new())
                .await
                .unwrap(),
            FinishStatus::Fenced(_)
        ));

        // TOCTOU check under runtime lock rejects revoked credentials on open_session_for / open_attempt_for.
        let before_rej = policy.auth_rejections_total();
        assert!(matches!(
            runtime.open_session_for(hello("w-rev", "", Vec::new()), p_rev.clone(), &policy),
            Err(WorkerOpenError::Unauthenticated(_))
        ));
        assert!(matches!(
            runtime
                .open_attempt_for(
                    &p_rev,
                    &policy,
                    &pb::AttemptOpen {
                        lease_id: assign.lease_id,
                        generation: assign.generation,
                        resume_after_sequence: 0,
                    },
                )
                .await,
            Err(WorkerOpenError::Unauthenticated(_))
        ));
        assert_eq!(policy.auth_rejections_total(), before_rej + 2);

        // Built-in maintenance loop sweeps expired session even without revocation poller.
        let p_exp = crate::auth::Principal {
            jti: Some("jti-exp".into()),
            exp: Some(crate::now_ms() / 1000 + 1),
            ..worker_principal("ns-exp")
        };
        let mut h_exp = runtime
            .open_session_for(hello("w-exp", "", Vec::new()), p_exp, &policy)
            .unwrap();
        let _welcome = next_message(&mut h_exp).await;
        tokio::time::timeout(Duration::from_secs(4), async {
            while h_exp.messages.recv().await.is_some() {}
        })
        .await
        .expect("maintenance loop must evict expired worker session");
        assert!(
            !runtime
                .list_workers()
                .iter()
                .any(|w| w.session_id == h_exp.session_id)
        );

        // Draining set while disconnected in reconnect grace survives reconnect and sends Drain.
        let p_drain = worker_principal("ns-d");
        let mut h_drain = runtime
            .open_session_for(hello("w-d", "", Vec::new()), p_drain.clone(), &policy)
            .unwrap();
        runtime
            .submit(SubmitRequest {
                namespace: Some("ns-d".into()),
                spec,
                ..Default::default()
            })
            .await
            .unwrap();
        let d_assign = next_assign(&mut h_drain).await;
        let sid = h_drain.session_id.clone();
        runtime.session_closed(&sid, h_drain.connection);
        runtime.drain_session(Some("ns-d"), &sid).unwrap();
        let mut h_resumed = runtime
            .open_session_for(
                hello("w-d", &sid, vec![d_assign.lease_id]),
                p_drain,
                &policy,
            )
            .unwrap();
        assert!(matches!(
            next_message(&mut h_resumed).await,
            pb::server_message::Msg::Welcome(_)
        ));
        assert!(matches!(
            next_message(&mut h_resumed).await,
            pb::server_message::Msg::Drain(_)
        ));
        assert!(
            runtime
                .list_workers()
                .iter()
                .any(|w| w.session_id == sid && w.draining)
        );
        runtime.shutdown().await;
    }

    #[tokio::test]
    async fn unbounded_error_and_reason_strings_are_truncated_at_utf8_boundaries() {
        use crate::runtime::api::{MAX_ERROR_CODE_BYTES, MAX_ERROR_MESSAGE_BYTES};

        let runtime = Runtime::start(RuntimeConfig::default()).await.unwrap();
        let mut w = runtime
            .open_session(hello("w-err", "", Vec::new()))
            .unwrap();
        let long_msg = "é".repeat(3_000); // 6000 bytes, 2-byte UTF-8 chars
        let long_code = "🦀".repeat(50); // 200 bytes, 4-byte UTF-8 chars

        // 1. ErrorInfo::bounded caps both code and message cleanly on UTF-8 char boundaries.
        let bounded = ErrorInfo::bounded(long_code, long_msg.clone(), false);
        assert!(bounded.code.len() <= MAX_ERROR_CODE_BYTES);
        assert_eq!(bounded.code.len(), MAX_ERROR_CODE_BYTES);
        assert!(bounded.message.len() <= MAX_ERROR_MESSAGE_BYTES);
        assert_eq!(bounded.message.len(), MAX_ERROR_MESSAGE_BYTES);

        // 2. AssignReply.reject_reason capped cleanly.
        let mut spec = JobSpec::default();
        spec.demand.interface = "echo".into();
        spec.policy.durability = Some(Durability::Ephemeral);
        spec.policy.max_attempts = Some(1);
        let j1 = runtime
            .submit(SubmitRequest {
                spec: spec.clone(),
                ..Default::default()
            })
            .await
            .unwrap()
            .job;
        let a1 = next_assign(&mut w).await;
        runtime
            .session_message(
                &w.session_id,
                w.connection,
                pb::worker_message::Msg::AssignReply(pb::AssignReply {
                    lease_id: a1.lease_id,
                    accepted: false,
                    reject_reason: long_msg.clone(),
                }),
            )
            .unwrap();
        let v1 = runtime.get_job(&j1.job_id, false).await.unwrap();
        assert!(v1.error.unwrap().message.len() <= MAX_ERROR_MESSAGE_BYTES);

        // 3. CancelRequest.reason capped cleanly.
        spec.demand.interface = "unserved".into();
        let j2 = runtime
            .submit(SubmitRequest {
                spec,
                ..Default::default()
            })
            .await
            .unwrap()
            .job;
        let cancelled = runtime.cancel(&j2.job_id, long_msg).await.unwrap();
        assert!(cancelled.error.unwrap().message.len() <= MAX_ERROR_MESSAGE_BYTES);
        runtime.shutdown().await;
    }

    #[tokio::test]
    async fn lifecycle_counters_ignore_recovery_and_rolled_back_commits() {
        use crate::journal::{
            BoxFuture, CommitOutcome, JournalBackend, JournalOp, Leadership, LeaseHandle, Recovery,
            StoredJob,
        };
        use std::sync::atomic::{AtomicBool, Ordering};

        struct RollbackBackend {
            rollback_accept: AtomicBool,
            rollback_start: AtomicBool,
        }

        impl JournalBackend for RollbackBackend {
            fn try_acquire_leadership(&self) -> BoxFuture<'_, anyhow::Result<Option<Leadership>>> {
                let mut spec = JobSpec::default();
                spec.demand.interface = "echo".into();
                spec.policy.durability = Some(Durability::Durable);
                let recovered = StoredJob {
                    job_id: "recovered-0".into(),
                    state: JobState::Queued,
                    spec,
                    spec_hash: "hr".into(),
                    attempt_count: 0,
                    max_attempts: 3,
                    created_at_ms: 100,
                    updated_at_ms: 100,
                    error: None,
                    outputs: Vec::new(),
                    last_attempt_id: None,
                    last_worker_id: None,
                    failed_workers: Default::default(),
                };
                Box::pin(async move {
                    Ok(Some(Leadership {
                        term: 1,
                        recovery: Recovery {
                            generation: 1,
                            active: vec![recovered],
                        },
                        lease: LeaseHandle::always_held(),
                    }))
                })
            }
            fn enqueue(
                &self,
                _term: u64,
                op: JournalOp,
            ) -> Result<oneshot::Receiver<CommitOutcome>, RuntimeError> {
                let (tx, rx) = oneshot::channel();
                let outcome = match op {
                    JournalOp::Accept(_) if self.rollback_accept.load(Ordering::Acquire) => {
                        CommitOutcome::RolledBack("disk full".into())
                    }
                    JournalOp::AttemptStart(_) if self.rollback_start.load(Ordering::Acquire) => {
                        CommitOutcome::RolledBack("disk full".into())
                    }
                    _ => CommitOutcome::Committed,
                };
                let _ = tx.send(outcome);
                Ok(rx)
            }
            fn spec_hash<'a>(
                &'a self,
                _: &'a str,
            ) -> BoxFuture<'a, anyhow::Result<Option<String>>> {
                Box::pin(async { Ok(None) })
            }
            fn receipt<'a>(
                &'a self,
                _: &'a str,
            ) -> BoxFuture<'a, anyhow::Result<Option<(String, String)>>> {
                Box::pin(async { Ok(None) })
            }
            fn load_job<'a>(
                &'a self,
                _: &'a str,
                _: bool,
                _: Arc<crate::runtime::events::ByteBudget>,
            ) -> BoxFuture<'a, anyhow::Result<Option<StoredJob>>> {
                Box::pin(async { Ok(None) })
            }
            fn load_job_preview<'a>(
                &'a self,
                _: &'a str,
            ) -> BoxFuture<'a, anyhow::Result<Option<crate::journal::StoredJobPreview>>>
            {
                Box::pin(async { Ok(None) })
            }
            fn scan_jobs<'a>(
                &'a self,
                _: u64,
                _: u64,
                _: Option<(u64, String)>,
                _: Option<&'a str>,
                _: usize,
            ) -> BoxFuture<'a, anyhow::Result<Vec<StoredJob>>> {
                Box::pin(async { Ok(Vec::new()) })
            }
            fn purge<'a>(&'a self, _: u64, _: u64) -> BoxFuture<'a, anyhow::Result<usize>> {
                Box::pin(async { Ok(0) })
            }
            fn stats(&self) -> crate::runtime::api::JournalStats {
                Default::default()
            }
            fn close(&self) -> BoxFuture<'_, ()> {
                Box::pin(async {})
            }
        }

        let backend = Arc::new(RollbackBackend {
            rollback_accept: AtomicBool::new(true),
            rollback_start: AtomicBool::new(true),
        });
        let runtime = Runtime::start_with_backend(
            RuntimeConfig::default(),
            Some(Arc::clone(&backend) as Arc<dyn JournalBackend>),
        )
        .await
        .unwrap();
        // Recovered job did NOT increment jobs_submitted_total.
        assert_eq!(runtime.stats().jobs_submitted_total, 0);

        // Rolled-back durable accept does NOT increment jobs_submitted_total.
        let mut spec = JobSpec::default();
        spec.demand.interface = "echo".into();
        spec.policy.durability = Some(Durability::Durable);
        assert!(
            runtime
                .submit(SubmitRequest {
                    spec: spec.clone(),
                    ..Default::default()
                })
                .await
                .is_err()
        );
        assert_eq!(runtime.stats().jobs_submitted_total, 0);

        // Restore journal writes via Probe, then trigger rolled-back AttemptStart on recovered job.
        backend.rollback_accept.store(false, Ordering::Release);
        {
            let _ = runtime.inner.commit(JournalOp::Probe, |_, _| {});
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
        let _w = runtime.open_session(hello("w-rb", "", Vec::new())).unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(runtime.stats().attempts_started_total, 0);

        // Once AttemptStart commits, attempts_started_total increments cleanly.
        backend.rollback_start.store(false, Ordering::Release);
        {
            let _ = runtime.inner.commit(JournalOp::Probe, |_, _| {});
        }
        tokio::time::timeout(Duration::from_secs(3), async {
            while runtime.stats().attempts_started_total == 0 {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("committed attempt start must increment counter");
        assert_eq!(runtime.stats().attempts_started_total, 1);
        runtime.shutdown().await;
    }
}
