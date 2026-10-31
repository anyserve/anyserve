//! Worker-facing operations: control sessions and attempt data streams. Only
//! the worker gRPC service calls these.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use anyserve_proto::v2 as pb;
use bytes::Bytes;
use tokio::sync::{mpsc, oneshot, watch};

use super::Runtime;
use super::api::{ErrorInfo, JobState, Metadata, Object, WorkerLoad, codes};
use super::events::LogFull;
use super::identity::outcome_digest;
use super::input::InputLog;
use super::machine::{LeaseEnd, new_id};
use super::state::{FinishStatus, Phase, Session, TerminalPlan};
use crate::error::RuntimeError;
use crate::now_ms;

/// How often a frame waiting for room in the shared event budget retries.
const BUDGET_RETRY: Duration = Duration::from_millis(250);

pub(crate) struct SessionHandle {
    pub session_id: String,
    pub connection: u64,
    pub messages: mpsc::Receiver<pb::ServerMessage>,
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
    pub(crate) fn open_session(&self, hello: pb::Hello) -> Result<SessionHandle, RuntimeError> {
        let limits = &self.inner.config.limits;
        let identifier = limits.max_identifier_bytes as usize;
        if hello.interfaces.is_empty() {
            return Err(RuntimeError::invalid("Hello.interfaces is required"));
        }
        if hello.interfaces.len() > limits.max_objects as usize
            || hello
                .interfaces
                .iter()
                .any(|i| i.is_empty() || i.len() > identifier)
            || hello.worker_id.len() > identifier
            || hello.resume_session_id.len() > identifier
        {
            return Err(RuntimeError::invalid(format!(
                "Hello identifiers must be 1..={identifier} bytes and at most {} interfaces",
                limits.max_objects
            )));
        }
        if hello.held_lease_ids.len() > limits.max_objects as usize {
            return Err(RuntimeError::invalid("too many held leases"));
        }
        let attributes = to_metadata(hello.attributes);
        let metadata = to_metadata(hello.metadata);
        let limit = limits.max_metadata_bytes as usize;
        let interface_bytes: usize = hello.interfaces.iter().map(String::len).sum();
        if metadata_size(&attributes) + metadata_size(&metadata) + interface_bytes > limit {
            return Err(RuntimeError::invalid(format!(
                "worker attributes and metadata exceed {limit} bytes"
            )));
        }
        let worker_id = if hello.worker_id.trim().is_empty() {
            new_id()
        } else {
            hello.worker_id
        };
        let interfaces: BTreeSet<String> = hello.interfaces.into_iter().collect();
        let total_capacity = hello.total_capacity.into_iter().collect();
        let max_concurrency = hello.max_concurrency.max(1);

        let inner = &self.inner;
        let mut st = inner.lock();
        let now = now_ms();
        if st.shutting_down {
            return Err(RuntimeError::unavailable("runtime is shutting down"));
        }
        let resume = !hello.resume_session_id.is_empty()
            && st
                .sessions
                .get(&hello.resume_session_id)
                .is_some_and(|session| session.worker_id == worker_id);
        if !resume && st.sessions.len() >= inner.config.max_sessions {
            return Err(RuntimeError::exhausted("worker session limit reached"));
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
        if resume {
            st.unindex_session(&session_id);
            let session = st.sessions.get_mut(&session_id).expect("resumed session");
            session.interfaces = interfaces;
            session.attributes = attributes;
            session.metadata = metadata;
            session.total_capacity = total_capacity;
            session.max_concurrency = max_concurrency;
            session.reported_free = max_concurrency;
            session.assigned_since_report = 0;
            session.draining = false;
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
                },
            );
        }
        st.index_session(&session_id);

        // Leases the worker still holds move to this session when they belong
        // to the same worker in this generation; every other one is fenced.
        let held: BTreeSet<String> = hello.held_lease_ids.into_iter().collect();
        let mut resumed = Vec::new();
        let mut fenced = Vec::new();
        for lease_id in &held {
            let movable = st.leases.get(lease_id).is_some_and(|lease| {
                !lease.cancelling
                    && st
                        .sessions
                        .get(&lease.session_id)
                        .is_some_and(|owner| owner.worker_id == worker_id)
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
                heartbeat_interval_ms: inner.config.heartbeat_interval.as_millis() as u32,
                heartbeat_timeout_ms: inner.config.heartbeat_timeout.as_millis() as u32,
                limits: Some(pb::Limits {
                    max_control_message_bytes: limits.max_control_message_bytes,
                    max_chunk_bytes: limits.max_chunk_bytes,
                    attempt_window_bytes: limits.attempt_window_bytes,
                    assign_inline_limit: limits.assign_inline_limit,
                }),
                resumed_lease_ids: resumed,
            })),
        };
        // The queue holds the welcome and every fence: capacity covers them.
        let _ = tx.try_send(welcome);
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
                    session.load = WorkerLoad {
                        queued: load.queued,
                        running: load.running,
                        utilization: load.utilization,
                        custom: load.custom.into_iter().collect(),
                    };
                }
            }
            pb::worker_message::Msg::Draining(_) => {
                session.draining = true;
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
                    job.rejected_by.insert(session_id.to_string());
                    let reason = if reply.reject_reason.is_empty() {
                        "worker rejected the assignment".to_string()
                    } else {
                        reply.reject_reason
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

    pub(crate) async fn open_attempt(
        &self,
        open: &pb::AttemptOpen,
    ) -> Result<(AttemptLink, u64), RuntimeError> {
        if let Some(opened) = self.open_attempt_in_memory(open)? {
            return Ok(opened);
        }
        if self.receipt_on_disk(&open.lease_id).await?.is_some() {
            return Ok((
                AttemptLink::Replay {
                    lease_id: open.lease_id.clone(),
                },
                0,
            ));
        }
        Err(RuntimeError::stale(
            "lease is not active in this generation and has no recorded outcome",
        ))
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
        open: &pb::AttemptOpen,
    ) -> Result<Option<(AttemptLink, u64)>, RuntimeError> {
        let inner = &self.inner;
        let mut st = inner.lock();
        let now = now_ms();
        let live = open.generation == st.generation
            && st
                .leases
                .get(&open.lease_id)
                .is_some_and(|lease| !lease.cancelling);
        if live {
            let job_id = st.leases[&open.lease_id].job_id.clone();
            let job = st.jobs.get_mut(&job_id).expect("leased job");
            let Some(attempt) = job
                .attempt
                .as_ref()
                .filter(|attempt| attempt.lease_id == open.lease_id)
            else {
                return Err(RuntimeError::stale(
                    "lease is not the job's current attempt",
                ));
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
        if st.finished_leases.contains_key(&open.lease_id) {
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
        frame: pb::Frame,
    ) -> Result<u64, RuntimeError> {
        let limits = &self.inner.config.limits;
        if frame.data.len() > limits.max_chunk_bytes as usize {
            return Err(RuntimeError::invalid(format!(
                "frame of {} bytes exceeds max_chunk_bytes {}",
                frame.data.len(),
                limits.max_chunk_bytes
            )));
        }
        let metadata = to_metadata(frame.metadata);
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
                let attempt_id = attempt.attempt_id.clone();
                loop {
                    match job.log.append_output(
                        attempt_id.clone(),
                        frame.data.clone(),
                        metadata.clone(),
                        frame.progress,
                        now,
                        reclaimed,
                    ) {
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
    pub(crate) async fn finish_attempt(
        &self,
        link: &AttemptLink,
        outcome: AttemptOutcome,
        outputs: Vec<Object>,
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
            let (tx, rx) = oneshot::channel();
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
                    job.finish_waiters.push(tx);
                }
                Phase::Assigned | Phase::Running
                    if job.attempt.as_ref().is_some_and(|a| a.lease_id == lease_id) =>
                {
                    let receipt = Some((lease_id.clone(), digest.clone()));
                    match error {
                        None => {
                            job.finish_waiters.push(tx);
                            inner.finish_job_locked(
                                &mut st,
                                &job_id,
                                TerminalPlan {
                                    outputs,
                                    release_lease: Some(lease_id.clone()),
                                    finished: receipt,
                                    ..TerminalPlan::new(JobState::Succeeded, None)
                                },
                                now,
                            );
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
                            let job = st.jobs.get_mut(&job_id).expect("job");
                            if retried || job.is_terminal() {
                                return Ok(FinishStatus::Committed);
                            }
                            job.finish_waiters.push(tx);
                        }
                    }
                }
                _ => {
                    return Ok(FinishStatus::Fenced(
                        "lease is not the job's current attempt".to_string(),
                    ));
                }
            }
            Some(rx)
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
                pb::worker_message::Msg::Draining(pb::Draining {}),
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
}
