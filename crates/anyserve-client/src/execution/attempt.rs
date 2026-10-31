use std::collections::{BTreeMap, HashMap, VecDeque};

use std::hash::BuildHasher;
use std::time::Duration;

use bytes::Bytes;
use tokio::time::Instant;
use tonic::Status;

use super::{AttemptClient, AttemptLink, v2};

/// One attempt's ordered data path with a byte window shared by frames/results.
/// The control session runs independently while this object waits on a slow reader.
pub struct Attempt {
    link: AttemptLink,
    heartbeat_timeout: Duration,
    chunk_bytes: usize,
    window_bytes: usize,
    next_sequence: u64,
    unacked: VecDeque<(u64, usize)>,
    frame_bytes: usize,
    result_sent: u64,
    result_acked: u64,
    input_done: bool,
    discard_input: bool,
    pending_input: VecDeque<v2::InputData>,
    pending_input_bytes: usize,
    /// A committed lease reopened solely to validate a repeated Finish.
    pub terminal_replay: bool,
    pub inputs: Vec<v2::Object>,
    pub params: Bytes,
}

struct ReceivedObject {
    data: Vec<u8>,
    uri: Option<String>,
    metadata: HashMap<String, String>,
}

impl Attempt {
    pub async fn open(
        client: &mut AttemptClient,
        assignment: &v2::Assign,
        limits: &v2::Limits,
        heartbeat_timeout: Duration,
    ) -> Result<Self, Status> {
        Self::open_mode(client, assignment, limits, heartbeat_timeout, true).await
    }

    /// Open without waiting for InputEnd, for duplex handlers such as video.
    pub async fn open_stream(
        client: &mut AttemptClient,
        assignment: &v2::Assign,
        limits: &v2::Limits,
        heartbeat_timeout: Duration,
    ) -> Result<Self, Status> {
        Self::open_mode(client, assignment, limits, heartbeat_timeout, false).await
    }

    async fn open_mode(
        client: &mut AttemptClient,
        assignment: &v2::Assign,
        limits: &v2::Limits,
        heartbeat_timeout: Duration,
        buffered: bool,
    ) -> Result<Self, Status> {
        let deadline = retry_deadline(heartbeat_timeout)?;
        let mut backoff = RetryBackoff::default();
        loop {
            let result = tokio::time::timeout_at(
                deadline,
                Self::open_once(client, assignment, limits, buffered, heartbeat_timeout),
            )
            .await
            .map_err(|_| retry_expired())?;
            match result {
                Err(error) if retry_transport(&error) => backoff.wait(deadline).await?,
                result => return result,
            }
        }
    }

    async fn open_once(
        client: &mut AttemptClient,
        assignment: &v2::Assign,
        limits: &v2::Limits,
        buffered: bool,
        heartbeat_timeout: Duration,
    ) -> Result<Self, Status> {
        if limits.max_chunk_bytes == 0 || limits.attempt_window_bytes < limits.max_chunk_bytes {
            return Err(Status::invalid_argument("invalid attempt byte window"));
        }
        let link = client
            .open(assignment.lease_id.clone(), assignment.generation, 0)
            .await?;
        let mut attempt = Self {
            link,
            heartbeat_timeout,
            chunk_bytes: limits.max_chunk_bytes as usize,
            window_bytes: limits.attempt_window_bytes as usize,
            next_sequence: 1,
            unacked: VecDeque::new(),
            frame_bytes: 0,
            result_sent: 0,
            result_acked: 0,
            input_done: !assignment.inputs_via_attempt && !assignment.streaming_input,
            discard_input: false,
            pending_input: VecDeque::new(),
            pending_input_bytes: 0,
            terminal_replay: false,
            inputs: assignment.inline_inputs.clone(),
            params: assignment.params.clone(),
        };
        let mut inputs: BTreeMap<u32, ReceivedObject> = BTreeMap::new();
        let mut parameters = Vec::new();
        let mut opened = false;
        loop {
            match attempt.receive().await? {
                v2::attempt_down::Msg::Opened(message) => {
                    attempt.next_sequence = message
                        .resume_from_sequence
                        .checked_add(1)
                        .ok_or_else(|| Status::out_of_range("frame sequence exhausted"))?;
                    opened = true;
                    attempt.terminal_replay = message.terminal_replay;
                    if message.terminal_replay {
                        attempt.input_done = true;
                    }
                    if message.terminal_replay || !buffered || attempt.input_done {
                        break;
                    }
                }
                v2::attempt_down::Msg::Input(input) => {
                    if input.last {
                        attempt.input_done = true;
                        if assignment.inputs_via_attempt || assignment.streaming_input {
                            attempt.inputs = inputs
                                .into_values()
                                .map(|object| v2::Object {
                                    body: Some(match object.uri {
                                        Some(uri) => v2::object::Body::Uri(uri),
                                        None => v2::object::Body::Inline(object.data.into()),
                                    }),
                                    metadata: object.metadata,
                                })
                                .collect();
                            if assignment.inputs_via_attempt {
                                attempt.params = parameters.into();
                            }
                        }
                        break;
                    }
                    if input.parameter_chunk {
                        parameters.extend_from_slice(&input.data);
                        continue;
                    }
                    if let Some(index) = input.object_index {
                        let object = inputs.entry(index).or_insert_with(|| ReceivedObject {
                            data: Vec::new(),
                            uri: (!input.uri.is_empty()).then(|| input.uri.clone()),
                            metadata: input.metadata.clone(),
                        });
                        object.data.extend_from_slice(&input.data);
                    }
                }
                v2::attempt_down::Msg::Ack(_) => {}
                v2::attempt_down::Msg::Finished(message) => return Err(finish_status(message)),
            }
        }
        if !opened {
            return Err(Status::failed_precondition(
                "attempt input arrived before Opened",
            ));
        }
        Ok(attempt)
    }

    pub async fn frame(
        &mut self,
        data: Bytes,
        metadata: HashMap<String, String>,
        progress: bool,
    ) -> Result<(), Status> {
        if self.terminal_replay {
            return Err(Status::failed_precondition(
                "terminal replay only accepts a repeated result",
            ));
        }
        let overhead = 64
            + metadata
                .iter()
                .map(|(key, value)| key.len() + value.len())
                .sum::<usize>();
        let payload = self
            .window_bytes
            .checked_sub(overhead)
            .filter(|size| *size > 0)
            .ok_or_else(|| Status::resource_exhausted("frame metadata exceeds the attempt window"))?
            .min(self.chunk_bytes);
        for index in 0..data.len().div_ceil(payload).max(1) {
            let start = index * payload;
            let chunk = data.slice(start..(start + payload).min(data.len()));
            let charged = chunk.len() + overhead;
            self.reserve(charged).await?;
            let sequence = self.next_sequence;
            self.send(v2::attempt_up::Msg::Frame(v2::Frame {
                sequence,
                data: chunk,
                metadata: metadata.clone(),
                progress,
            }))
            .await?;
            self.unacked.push_back((sequence, charged));
            self.frame_bytes += charged;
            self.next_sequence += 1;
        }
        Ok(())
    }

    /// Read one input fragment. `object_end` preserves logical input boundaries.
    pub async fn input_chunk(&mut self) -> Result<Option<v2::InputData>, Status> {
        loop {
            let message = if let Some(input) = self.pending_input.pop_front() {
                self.pending_input_bytes -= input_size(&input);
                v2::attempt_down::Msg::Input(input)
            } else {
                if self.input_done {
                    return Ok(None);
                }
                self.receive().await?
            };
            match message {
                v2::attempt_down::Msg::Input(input) => {
                    if input.last {
                        self.input_done = true;
                        return Ok(None);
                    }
                    return Ok(Some(input));
                }
                message => self.ack(message)?,
            }
        }
    }

    pub async fn finish(
        &mut self,
        outputs: &[v2::Object],
        outcome: v2::finish::Outcome,
    ) -> Result<(), Status> {
        // ResultChunk uses an empty URI field for inline data.
        if outputs.iter().any(
            |object| matches!(&object.body, Some(v2::object::Body::Uri(uri)) if uri.is_empty()),
        ) {
            return Err(Status::invalid_argument("object URI must not be empty"));
        }
        for (index, object) in outputs.iter().enumerate() {
            let (data, uri) = match &object.body {
                Some(v2::object::Body::Inline(data)) => (data.clone(), String::new()),
                Some(v2::object::Body::Uri(uri)) => (Bytes::new(), uri.clone()),
                None => return Err(Status::invalid_argument("result object has no body")),
            };
            if data.is_empty() {
                self.send(v2::attempt_up::Msg::Result(v2::ResultChunk {
                    index: index as u32,
                    data,
                    last: true,
                    metadata: object.metadata.clone(),
                    uri,
                }))
                .await?;
            } else {
                let chunks = data.chunks(self.chunk_bytes);
                let count = chunks.len();
                for (part, chunk) in chunks.enumerate() {
                    self.reserve(chunk.len()).await?;
                    self.send(v2::attempt_up::Msg::Result(v2::ResultChunk {
                        index: index as u32,
                        data: Bytes::copy_from_slice(chunk),
                        last: part + 1 == count,
                        metadata: if part == 0 {
                            object.metadata.clone()
                        } else {
                            HashMap::new()
                        },
                        uri: String::new(),
                    }))
                    .await?;
                    self.result_sent += chunk.len() as u64;
                }
            }
        }
        self.send(v2::attempt_up::Msg::Finish(v2::Finish {
            outcome: Some(outcome),
        }))
        .await?;
        loop {
            match self.receive().await? {
                v2::attempt_down::Msg::Finished(message) => {
                    return match v2::attempt_finished::Status::try_from(message.status) {
                        Ok(v2::attempt_finished::Status::Committed) => Ok(()),
                        _ => Err(finish_status(message)),
                    };
                }
                message => self.ack(message)?,
            }
        }
    }

    /// Re-deliver a terminal result after an interrupted acknowledgement.
    /// This method never invokes the engine; dropping it cancels the retry loop.
    pub async fn commit(
        &mut self,
        data: &mut AttemptClient,
        assignment: &v2::Assign,
        limits: &v2::Limits,
        outputs: &[v2::Object],
        outcome: v2::finish::Outcome,
    ) -> Result<(), Status> {
        let deadline = retry_deadline(self.heartbeat_timeout)?;
        let last_frame = self.next_sequence - 1;
        let mut outputs = outputs;
        let mut outcome = outcome;
        self.discard_input = true;
        self.pending_input.clear();
        self.pending_input_bytes = 0;
        let mut backoff = RetryBackoff::default();
        loop {
            let result = tokio::time::timeout_at(deadline, self.finish(outputs, outcome.clone()))
                .await
                .map_err(|_| retry_expired())?;
            match result {
                Ok(()) => return Ok(()),
                Err(error) if retry_transport(&error) => loop {
                    backoff.wait(deadline).await?;
                    let result = tokio::time::timeout_at(
                        deadline,
                        Self::open_once(data, assignment, limits, false, self.heartbeat_timeout),
                    )
                    .await
                    .map_err(|_| retry_expired())?;
                    match result {
                        Ok(mut reopened) => {
                            let resume_from = reopened.next_sequence - 1;
                            // Frames are not buffered for replay. A live lease
                            // missing a sent frame must not commit a truncated
                            // success. A terminal replay already has a receipt
                            // and must repeat the original outcome for its digest.
                            if !reopened.terminal_replay
                                && resume_from < last_frame
                                && matches!(outcome, v2::finish::Outcome::Succeeded(_))
                            {
                                outcome = v2::finish::Outcome::Failed(v2::ErrorInfo {
                                    code: "output_interrupted".into(),
                                    message: format!(
                                        "attempt reopened after frame {resume_from}, but sent through {last_frame}"
                                    ),
                                    retryable: true,
                                });
                                outputs = &[];
                            }
                            reopened.discard_input = true;
                            *self = reopened;
                            break;
                        }
                        Err(error) if retry_transport(&error) => continue,
                        Err(error) => return Err(error),
                    }
                },
                Err(error) => return Err(error),
            }
        }
    }

    async fn reserve(&mut self, bytes: usize) -> Result<(), Status> {
        while self.frame_bytes
            + (self.result_sent.saturating_sub(self.result_acked) as usize)
            + bytes
            > self.window_bytes
        {
            let message = self.receive().await?;
            self.ack(message)?;
        }
        Ok(())
    }

    fn ack(&mut self, message: v2::attempt_down::Msg) -> Result<(), Status> {
        match message {
            v2::attempt_down::Msg::Ack(ack) => {
                while self
                    .unacked
                    .front()
                    .is_some_and(|(sequence, _)| *sequence <= ack.up_to_sequence)
                {
                    let (_, bytes) = self.unacked.pop_front().unwrap();
                    self.frame_bytes -= bytes;
                }
                self.result_acked = self
                    .result_acked
                    .max(ack.result_bytes.min(self.result_sent));
                Ok(())
            }
            v2::attempt_down::Msg::Finished(message) => Err(finish_status(message)),
            v2::attempt_down::Msg::Input(_) if self.discard_input => Ok(()),
            v2::attempt_down::Msg::Input(input) if !self.input_done => {
                let size = input_size(&input);
                if self.pending_input_bytes.saturating_add(size) > anyserve_proto::MAX_MESSAGE_SIZE
                {
                    return Err(Status::resource_exhausted(
                        "pending input exceeds the receive budget",
                    ));
                }
                self.pending_input_bytes += size;
                self.pending_input.push_back(input);
                Ok(())
            }
            v2::attempt_down::Msg::Input(input) if input.last => Ok(()),
            _ => Err(Status::failed_precondition(
                "unexpected attempt input after execution began",
            )),
        }
    }

    async fn send(&mut self, message: v2::attempt_up::Msg) -> Result<(), Status> {
        self.link
            .send
            .send(v2::AttemptUp { msg: Some(message) })
            .await
            .map_err(|_| Status::unavailable("attempt upload closed"))
    }

    async fn receive(&mut self) -> Result<v2::attempt_down::Msg, Status> {
        self.link
            .events
            .message()
            .await?
            .and_then(|message| message.msg)
            .ok_or_else(|| Status::unavailable("attempt ended without a terminal acknowledgement"))
    }
}

#[cfg(test)]
mod recovery_tests;

fn finish_status(message: v2::AttemptFinished) -> Status {
    match v2::attempt_finished::Status::try_from(message.status) {
        Ok(v2::attempt_finished::Status::Fenced) => Status::failed_precondition(message.reason),
        Ok(v2::attempt_finished::Status::Committed) => {
            Status::already_exists("attempt already committed")
        }
        _ => Status::aborted(message.reason),
    }
}

fn retry_transport(error: &Status) -> bool {
    matches!(
        error.code(),
        tonic::Code::Unavailable
            | tonic::Code::Unknown
            | tonic::Code::Cancelled
            | tonic::Code::DeadlineExceeded
            | tonic::Code::Internal
            | tonic::Code::ResourceExhausted
    )
}

fn input_size(input: &v2::InputData) -> usize {
    128 + input.data.len()
        + input.uri.len()
        + input
            .metadata
            .iter()
            .map(|(key, value)| key.len() + value.len())
            .sum::<usize>()
}

fn retry_expired() -> Status {
    Status::deadline_exceeded("attempt delivery deadline elapsed")
}

fn retry_deadline(heartbeat_timeout: Duration) -> Result<Instant, Status> {
    // Job deadlines belong to the server. A monotonic local window avoids
    // treating clock skew as an already-expired assignment.
    Instant::now()
        .checked_add(heartbeat_timeout.saturating_mul(4))
        .ok_or_else(|| Status::invalid_argument("attempt retry window is out of range"))
}

struct RetryBackoff {
    next_ms: u64,
}
impl Default for RetryBackoff {
    fn default() -> Self {
        Self { next_ms: 100 }
    }
}
impl RetryBackoff {
    fn delay(&mut self) -> Duration {
        let base = self.next_ms;
        self.next_ms = base.saturating_mul(2).min(5_000);
        // Equal jitter keeps retries spread out even at the five-second cap.
        let random = std::collections::hash_map::RandomState::new().hash_one(());
        Duration::from_millis(base / 2 + random % (base / 2 + 1))
    }
    async fn wait(&mut self, deadline: Instant) -> Result<(), Status> {
        tokio::time::timeout_at(deadline, tokio::time::sleep(self.delay()))
            .await
            .map_err(|_| retry_expired())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_window_uses_only_the_negotiated_heartbeat() {
        let remaining = retry_deadline(Duration::from_secs(1)).unwrap() - Instant::now();
        assert!(remaining <= Duration::from_secs(4) && remaining > Duration::from_secs(3));
        assert!(retry_transport(&Status::unknown("hyper reset")));
        assert!(retry_transport(&Status::cancelled("h2 CANCEL")));
        assert!(!retry_transport(&Status::failed_precondition("fenced")));
    }

    #[tokio::test]
    async fn retry_wait_stops_at_deadline() {
        let mut backoff = RetryBackoff::default();
        let error = backoff.wait(Instant::now()).await.unwrap_err();
        assert_eq!(error.code(), tonic::Code::DeadlineExceeded);
    }

    #[test]
    fn exponential_backoff_is_jittered_and_capped() {
        let mut backoff = RetryBackoff::default();
        for base in [100, 200, 400, 800, 1600, 3200, 5000, 5000, 5000] {
            let delay = backoff.delay();
            assert!(delay >= Duration::from_millis(base / 2));
            assert!(delay <= Duration::from_millis(base));
        }
    }
    struct ExhaustedWorker {
        calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        allow_first_open: bool,
        replay_inputs: bool,
    }

    #[tonic::async_trait]
    impl v2::worker_server::Worker for ExhaustedWorker {
        type SessionStream =
            tokio_stream::wrappers::ReceiverStream<Result<v2::ServerMessage, Status>>;
        type AttemptStream =
            tokio_stream::wrappers::ReceiverStream<Result<v2::AttemptDown, Status>>;

        async fn session(
            &self,
            _: tonic::Request<tonic::Streaming<v2::WorkerMessage>>,
        ) -> Result<tonic::Response<Self::SessionStream>, Status> {
            Err(Status::unimplemented("unused test session"))
        }
        async fn attempt(
            &self,
            request: tonic::Request<tonic::Streaming<v2::AttemptUp>>,
        ) -> Result<tonic::Response<Self::AttemptStream>, Status> {
            let call = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if !self.allow_first_open || (call > 0 && !self.replay_inputs) {
                return Err(Status::resource_exhausted("result budget full"));
            }
            let replay_inputs = self.replay_inputs;
            let mut requests = request.into_inner();
            let (send, receive) = tokio::sync::mpsc::channel(2);
            tokio::spawn(async move {
                let _ = send
                    .send(Ok(v2::AttemptDown {
                        msg: Some(v2::attempt_down::Msg::Opened(v2::AttemptOpened {
                            resume_from_sequence: 0,
                            terminal_replay: false,
                        })),
                    }))
                    .await;
                if replay_inputs {
                    let _ = send
                        .send(Ok(v2::AttemptDown {
                            msg: Some(v2::attempt_down::Msg::Input(v2::InputData {
                                data: vec![0; 1024].into(),
                                object_index: Some(0),
                                ..Default::default()
                            })),
                        }))
                        .await;
                }
                while let Ok(Some(message)) = requests.message().await {
                    if matches!(message.msg, Some(v2::attempt_up::Msg::Finish(_))) {
                        let response = if replay_inputs && call >= 2 {
                            Ok(v2::AttemptDown {
                                msg: Some(v2::attempt_down::Msg::Finished(v2::AttemptFinished {
                                    status: v2::attempt_finished::Status::Committed as i32,
                                    reason: String::new(),
                                })),
                            })
                        } else if replay_inputs && call == 0 {
                            Err(Status::unknown("connection reset before acknowledgement"))
                        } else if replay_inputs {
                            Err(Status::cancelled("h2 CANCEL before acknowledgement"))
                        } else {
                            Err(Status::resource_exhausted("result budget full"))
                        };
                        let _ = send.send(response).await;
                        break;
                    }
                }
            });
            Ok(tonic::Response::new(
                tokio_stream::wrappers::ReceiverStream::new(receive),
            ))
        }
    }

    #[tokio::test]
    async fn open_and_commit_stop_retrying_when_a_server_stays_exhausted() {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        use tokio_stream::wrappers::TcpListenerStream;
        for allow_first_open in [false, true] {
            let calls = Arc::new(AtomicUsize::new(0));
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!("http://{}", listener.local_addr().unwrap());
            let service = ExhaustedWorker {
                calls: calls.clone(),
                allow_first_open,
                replay_inputs: false,
            };
            let task = tokio::spawn(async move {
                tonic::transport::Server::builder()
                    .add_service(v2::worker_server::WorkerServer::new(service))
                    .serve_with_incoming(TcpListenerStream::new(listener))
                    .await
                    .unwrap();
            });
            let mut client = AttemptClient::connect(&endpoint, None).await.unwrap();
            let assignment = v2::Assign {
                total_deadline_ms: 1,
                execution_deadline_ms: 1,
                ..Default::default()
            };
            let limits = v2::Limits {
                max_chunk_bytes: 1024,
                attempt_window_bytes: 4096,
                ..Default::default()
            };
            let timeout = Duration::from_millis(40);
            let result = tokio::time::timeout(Duration::from_secs(2), async {
                match Attempt::open(&mut client, &assignment, &limits, timeout).await {
                    Ok(mut attempt) => {
                        attempt
                            .commit(
                                &mut client,
                                &assignment,
                                &limits,
                                &[],
                                v2::finish::Outcome::Succeeded(v2::Succeeded {}),
                            )
                            .await
                    }
                    Err(error) => Err(error),
                }
            })
            .await;
            task.abort();
            assert_eq!(
                result
                    .expect("retry loop exceeded its deadline")
                    .unwrap_err()
                    .code(),
                tonic::Code::DeadlineExceeded
            );
            let total = calls.load(Ordering::SeqCst);
            assert!(total > 0 && total <= 5, "unexpected retry load: {total}");
        }
    }
    #[tokio::test]
    async fn result_retry_does_not_wait_for_or_buffer_unfinished_input() {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        use tokio_stream::wrappers::TcpListenerStream;
        let calls = Arc::new(AtomicUsize::new(0));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let service = ExhaustedWorker {
            calls: calls.clone(),
            allow_first_open: true,
            replay_inputs: true,
        };
        let task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(v2::worker_server::WorkerServer::new(service))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        let mut client = AttemptClient::connect(&endpoint, None).await.unwrap();
        let assignment = v2::Assign {
            inputs_via_attempt: true,
            streaming_input: true,
            total_deadline_ms: 1,
            ..Default::default()
        };
        let limits = v2::Limits {
            max_chunk_bytes: 1024,
            attempt_window_bytes: 4096,
            ..Default::default()
        };
        let mut attempt =
            Attempt::open_stream(&mut client, &assignment, &limits, Duration::from_secs(1))
                .await
                .unwrap();
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            attempt.commit(
                &mut client,
                &assignment,
                &limits,
                &[],
                v2::finish::Outcome::Succeeded(v2::Succeeded {}),
            ),
        )
        .await;
        task.abort();
        result.expect("commit waited for InputEnd").unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        assert!(attempt.pending_input.is_empty());
        assert_eq!(attempt.pending_input_bytes, 0);
    }
}
