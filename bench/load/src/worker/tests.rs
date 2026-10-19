use tonic::codegen::tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::{Request, Response, Status, Streaming};

use super::*;

struct Worker(mpsc::UnboundedSender<v2::Finish>);

#[tonic::async_trait]
impl v2::worker_server::Worker for Worker {
    type SessionStream = ReceiverStream<std::result::Result<v2::ServerMessage, Status>>;
    type AttemptStream = ReceiverStream<std::result::Result<v2::AttemptDown, Status>>;

    async fn session(
        &self,
        _: Request<Streaming<v2::WorkerMessage>>,
    ) -> std::result::Result<Response<Self::SessionStream>, Status> {
        Err(Status::unimplemented("unused"))
    }

    async fn attempt(
        &self,
        request: Request<Streaming<v2::AttemptUp>>,
    ) -> std::result::Result<Response<Self::AttemptStream>, Status> {
        let mut input = request.into_inner();
        assert!(matches!(
            input.message().await?.unwrap().msg,
            Some(v2::attempt_up::Msg::Open(_))
        ));
        let (send, receive) = mpsc::channel(2);
        send.send(Ok(v2::AttemptDown {
            msg: Some(v2::attempt_down::Msg::Opened(v2::AttemptOpened::default())),
        }))
        .await
        .unwrap();
        let outcomes = self.0.clone();
        tokio::spawn(async move {
            while let Ok(Some(message)) = input.message().await {
                if let Some(v2::attempt_up::Msg::Finish(finish)) = message.msg {
                    outcomes.send(finish).unwrap();
                    let _ = send
                        .send(Ok(v2::AttemptDown {
                            msg: Some(v2::attempt_down::Msg::Finished(v2::AttemptFinished {
                                status: v2::attempt_finished::Status::Committed as i32,
                                reason: String::new(),
                            })),
                        }))
                        .await;
                    break;
                }
            }
            drop(send);
            while let Ok(Some(_)) = input.message().await {}
        });
        Ok(Response::new(ReceiverStream::new(receive)))
    }
}

#[tokio::test]
async fn a_frame_error_finishes_the_mock_attempt_as_failed() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let (outcomes, mut received) = mpsc::unbounded_channel();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(v2::worker_server::WorkerServer::new(Worker(outcomes)))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    let mut client = AttemptClient::connect(&endpoint, None).await.unwrap();
    // Open is valid, but the frame's 64-byte overhead leaves no payload room.
    let limits = v2::Limits {
        max_chunk_bytes: 64,
        attempt_window_bytes: 64,
        ..Default::default()
    };
    let mode = Mode::Stream(
        64,
        v2::Output {
            data: vec![b'x'; 1024].into(),
            ..Default::default()
        },
    );
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        execute(
            &mut client,
            &v2::Assign::default(),
            &limits,
            Duration::from_secs(1),
            &mode,
        ),
    )
    .await;
    server.abort();
    result.expect("failed attempt was left unfinished").unwrap();
    let finish = received.try_recv().expect("no Finish reached the server");
    let Some(v2::finish::Outcome::Failed(error)) = finish.outcome else {
        panic!("frame error reported success");
    };
    assert_eq!(error.code, "output_interrupted");
    assert!(error.retryable);
}
