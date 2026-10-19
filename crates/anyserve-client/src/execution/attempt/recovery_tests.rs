use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use tokio::sync::{Notify, mpsc};
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::{Request, Response, Streaming};

use super::*;

struct RecordedFinish {
    finish: v2::Finish,
    results: Vec<v2::ResultChunk>,
}

struct InterruptedWorker {
    resume_from: u64,
    terminal_replay: bool,
    fail_ack_again: bool,
    calls: Arc<AtomicUsize>,
    cut: Arc<Notify>,
    finishes: mpsc::UnboundedSender<RecordedFinish>,
}

fn down(msg: v2::attempt_down::Msg) -> Result<v2::AttemptDown, Status> {
    Ok(v2::AttemptDown { msg: Some(msg) })
}

#[tonic::async_trait]
impl v2::worker_server::Worker for InterruptedWorker {
    type SessionStream = ReceiverStream<Result<v2::ServerMessage, Status>>;
    type AttemptStream = ReceiverStream<Result<v2::AttemptDown, Status>>;

    async fn session(
        &self,
        _: Request<Streaming<v2::WorkerMessage>>,
    ) -> Result<Response<Self::SessionStream>, Status> {
        Err(Status::unimplemented("unused"))
    }

    async fn attempt(
        &self,
        request: Request<Streaming<v2::AttemptUp>>,
    ) -> Result<Response<Self::AttemptStream>, Status> {
        let mut input = request.into_inner();
        assert!(matches!(
            input.message().await?.unwrap().msg,
            Some(v2::attempt_up::Msg::Open(_))
        ));
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        let (send, receive) = mpsc::channel(4);
        let resume_from = if call == 0 {
            0
        } else if call > 1 {
            2
        } else {
            self.resume_from
        };
        send.send(down(v2::attempt_down::Msg::Opened(v2::AttemptOpened {
            resume_from_sequence: resume_from,
            terminal_replay: call > 0 && self.terminal_replay,
        })))
        .await
        .unwrap();
        let cut = self.cut.clone();
        let finishes = self.finishes.clone();
        let fail_ack_again = self.fail_ack_again;
        tokio::spawn(async move {
            let mut results = Vec::new();
            while let Ok(Some(message)) = input.message().await {
                match message.msg {
                    Some(v2::attempt_up::Msg::Frame(frame)) if call == 0 => {
                        if frame.sequence == 2 {
                            // Both frame() calls returned successfully, but the
                            // upload fails with only a prefix retained remotely.
                            cut.notified().await;
                            let _ = send
                                .send(Err(Status::unavailable("upload interrupted")))
                                .await;
                            break;
                        }
                        send.send(down(v2::attempt_down::Msg::Ack(v2::Ack {
                            up_to_sequence: frame.sequence,
                            result_bytes: 0,
                        })))
                        .await
                        .unwrap();
                    }
                    Some(v2::attempt_up::Msg::Result(result)) => results.push(result),
                    Some(v2::attempt_up::Msg::Finish(finish)) => {
                        finishes.send(RecordedFinish { finish, results }).unwrap();
                        let reply = if fail_ack_again && call == 1 {
                            Err(Status::unknown("terminal acknowledgement interrupted"))
                        } else {
                            down(v2::attempt_down::Msg::Finished(v2::AttemptFinished {
                                status: v2::attempt_finished::Status::Committed as i32,
                                reason: String::new(),
                            }))
                        };
                        let _ = send.send(reply).await;
                        break;
                    }
                    _ => {}
                }
            }
            // Keep this fixture's request shutdown from causing another reset.
            drop(send);
            while let Ok(Some(_)) = input.message().await {}
        });
        Ok(Response::new(ReceiverStream::new(receive)))
    }
}

struct Server(tokio::task::JoinHandle<()>);

impl Drop for Server {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn recover(
    resume_from: u64,
    terminal_replay: bool,
    fail_ack_again: bool,
) -> Vec<RecordedFinish> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let cut = Arc::new(Notify::new());
    let calls = Arc::new(AtomicUsize::new(0));
    let (finishes, mut observed) = mpsc::unbounded_channel();
    let worker = InterruptedWorker {
        resume_from,
        terminal_replay,
        fail_ack_again,
        calls: calls.clone(),
        cut: cut.clone(),
        finishes,
    };
    let _server = Server(tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(v2::worker_server::WorkerServer::new(worker))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    }));
    let mut client = AttemptClient::connect(&endpoint, None).await.unwrap();
    let assignment = v2::Assign {
        lease_id: "lease".into(),
        ..Default::default()
    };
    let limits = v2::Limits {
        max_chunk_bytes: 1024,
        attempt_window_bytes: 4096,
        ..Default::default()
    };
    let mut attempt = Attempt::open(&mut client, &assignment, &limits, Duration::from_secs(1))
        .await
        .unwrap();
    for data in [b"first".as_slice(), b"second".as_slice()] {
        attempt
            .frame(Bytes::copy_from_slice(data), HashMap::new(), false)
            .await
            .unwrap();
    }
    cut.notify_one();
    let outputs = [v2::Object {
        body: Some(v2::object::Body::Inline(Bytes::from_static(b"result"))),
        ..Default::default()
    }];
    tokio::time::timeout(
        Duration::from_secs(2),
        attempt.commit(
            &mut client,
            &assignment,
            &limits,
            &outputs,
            v2::finish::Outcome::Succeeded(v2::Succeeded {}),
        ),
    )
    .await
    .expect("commit did not recover")
    .unwrap();
    let retries = if fail_ack_again { 2 } else { 1 };
    assert_eq!(calls.load(Ordering::SeqCst), retries + 1);
    (0..retries).map(|_| observed.try_recv().unwrap()).collect()
}

#[tokio::test]
async fn a_live_reopen_with_missing_frames_commits_a_retryable_failure() {
    for record in recover(1, false, true).await {
        let Some(v2::finish::Outcome::Failed(error)) = record.finish.outcome else {
            panic!("a truncated upload committed success");
        };
        assert_eq!(error.code, "output_interrupted");
        assert!(error.retryable);
        assert!(
            record.results.is_empty(),
            "truncated success outputs were resent"
        );
    }
}

#[tokio::test]
async fn a_live_reopen_after_every_frame_keeps_the_successful_result() {
    let records = recover(2, false, false).await;
    assert!(matches!(
        records[0].finish.outcome,
        Some(v2::finish::Outcome::Succeeded(_))
    ));
    assert_eq!(records[0].results.len(), 1);
    assert_eq!(records[0].results[0].data.as_ref(), b"result");
}

#[tokio::test]
async fn terminal_replay_preserves_the_original_result_for_receipt_verification() {
    let records = recover(0, true, false).await;
    assert!(matches!(
        records[0].finish.outcome,
        Some(v2::finish::Outcome::Succeeded(_))
    ));
    assert_eq!(records[0].results.len(), 1);
    assert_eq!(records[0].results[0].data.as_ref(), b"result");
}
