use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{Notify, mpsc};
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::{Request, Response};

use super::*;
use crate::execution::{ExecutionClient, Invocation};

#[derive(Clone, Default)]
struct Service {
    live: bool,
    gate: Option<Arc<Notify>>,
    trailer_error: bool,
    dropped: Arc<Notify>,
}

fn event(kind: v2::execution_event::Event) -> v2::ExecutionEvent {
    v2::ExecutionEvent {
        job_id: "test".into(),
        event: Some(kind),
        ..Default::default()
    }
}

impl Service {
    fn response(&self) -> ReceiverStream<Result<v2::ExecutionEvent, Status>> {
        let (send, receive) = mpsc::channel(3);
        send.try_send(Ok(event(v2::execution_event::Event::Accepted(
            v2::Accepted {},
        ))))
        .unwrap();
        if self.live {
            let dropped = self.dropped.clone();
            tokio::spawn(async move {
                while send
                    .send(Ok(event(v2::execution_event::Event::Output(
                        v2::Output::default(),
                    ))))
                    .await
                    .is_ok()
                {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                dropped.notify_one();
            });
        } else {
            send.try_send(Ok(event(v2::execution_event::Event::Finished(
                v2::ExecutionFinished {
                    state: v2::JobState::Succeeded as i32,
                    ..Default::default()
                },
            ))))
            .unwrap();
            if let Some(gate) = self.gate.clone() {
                let trailer_error = self.trailer_error;
                tokio::spawn(async move {
                    gate.notified().await;
                    if trailer_error {
                        let _ = send.send(Err(Status::unavailable("trailer failure"))).await;
                    }
                });
            }
        }
        ReceiverStream::new(receive)
    }
}

#[tonic::async_trait]
impl v2::client_server::Client for Service {
    type InvokeStream = ReceiverStream<Result<v2::ExecutionEvent, Status>>;
    type WatchStream = Self::InvokeStream;

    async fn invoke(
        &self,
        request: Request<Streaming<v2::InvokeRequest>>,
    ) -> Result<Response<Self::InvokeStream>, Status> {
        let mut input = request.into_inner();
        assert!(input.message().await?.is_some());
        tokio::spawn(async move { while let Ok(Some(_)) = input.message().await {} });
        Ok(Response::new(self.response()))
    }

    async fn watch(
        &self,
        _: Request<v2::WatchRequest>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        Ok(Response::new(self.response()))
    }

    async fn submit(
        &self,
        _: Request<v2::SubmitRequest>,
    ) -> Result<Response<v2::SubmitResponse>, Status> {
        Err(Status::unimplemented("unused"))
    }

    async fn get_job(&self, _: Request<v2::GetJobRequest>) -> Result<Response<v2::Job>, Status> {
        Err(Status::unimplemented("unused"))
    }

    async fn cancel(&self, _: Request<v2::CancelRequest>) -> Result<Response<v2::Job>, Status> {
        Err(Status::unimplemented("unused"))
    }

    async fn list_jobs(
        &self,
        _: Request<v2::ListJobsRequest>,
    ) -> Result<Response<v2::ListJobsResponse>, Status> {
        Err(Status::unimplemented("unused"))
    }
}

struct Server(tokio::task::JoinHandle<()>);

impl Drop for Server {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn connect(service: Service) -> (ExecutionClient, Server) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(v2::client_server::ClientServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    (
        ExecutionClient::connect_with_token(&endpoint, None)
            .await
            .unwrap(),
        Server(task),
    )
}

#[derive(Clone, Copy, Debug)]
enum Route {
    Invoke,
    Duplex,
    Watch,
}

async fn open(
    client: &mut ExecutionClient,
    route: Route,
) -> (ExecutionEvents, Option<mpsc::Sender<v2::InvokeRequest>>) {
    match route {
        Route::Invoke => (client.invoke(Default::default()).await.unwrap(), None),
        Route::Duplex => {
            let Invocation { input, events } =
                client.invoke_stream(Default::default()).await.unwrap();
            (events, Some(input))
        }
        Route::Watch => (client.watch("test", 0).await.unwrap(), None),
    }
}

#[tokio::test]
async fn finished_waits_for_response_eof_for_message_and_stream_readers() {
    for route in [Route::Invoke, Route::Duplex, Route::Watch] {
        for stream_reader in [false, true] {
            let gate = Arc::new(Notify::new());
            let (mut client, _server) = connect(Service {
                gate: Some(gate.clone()),
                ..Default::default()
            })
            .await;
            let (mut events, _input) = open(&mut client, route).await;
            assert!(matches!(
                events.message().await.unwrap().unwrap().event,
                Some(v2::execution_event::Event::Accepted(_))
            ));
            let terminal = {
                let next = async {
                    if stream_reader {
                        events.next().await.transpose()
                    } else {
                        events.message().await
                    }
                };
                tokio::pin!(next);
                assert!(
                    tokio::time::timeout(Duration::from_millis(20), &mut next)
                        .await
                        .is_err()
                );
                gate.notify_one();
                next.await.unwrap().unwrap()
            };
            assert!(matches!(
                terminal.event,
                Some(v2::execution_event::Event::Finished(_))
            ));
            // Ordinary application behavior: break immediately on Finished.
            drop(events);
        }
    }
}

#[tokio::test]
async fn trailer_errors_are_returned_instead_of_a_successful_finished_event() {
    for route in [Route::Invoke, Route::Duplex, Route::Watch] {
        let gate = Arc::new(Notify::new());
        let (mut client, _server) = connect(Service {
            gate: Some(gate.clone()),
            trailer_error: true,
            ..Default::default()
        })
        .await;
        let (mut events, _input) = open(&mut client, route).await;
        events.message().await.unwrap().unwrap();
        gate.notify_one();
        let status = events.message().await.unwrap_err();
        assert_eq!(status.code(), tonic::Code::Unavailable);
        assert_eq!(status.message(), "trailer failure");
        assert!(events.message().await.unwrap().is_none());
    }
}

#[tokio::test]
async fn dropping_a_live_response_still_releases_the_server_stream() {
    for route in [Route::Invoke, Route::Duplex, Route::Watch] {
        let dropped = Arc::new(Notify::new());
        let (mut client, _server) = connect(Service {
            live: true,
            dropped: dropped.clone(),
            ..Default::default()
        })
        .await;
        let (mut events, input) = open(&mut client, route).await;
        events.message().await.unwrap().unwrap();
        drop(input);
        drop(events);
        tokio::time::timeout(Duration::from_secs(2), dropped.notified())
            .await
            .unwrap_or_else(|_| panic!("live response not closed for {route:?}"));
    }
}

#[tokio::test]
async fn repeated_finished_then_direct_drop_keeps_the_same_channel_usable() {
    let (mut client, _server) = connect(Service::default()).await;
    // More than h2's 1024 local-reset allowance, with Invoke and Watch sharing
    // one connection. Neither read style explicitly requests EOF here.
    for index in 0..4096 {
        let route = if index % 2 == 0 {
            Route::Invoke
        } else {
            Route::Watch
        };
        let (mut events, _input) = open(&mut client, route).await;
        loop {
            let event = if index % 4 < 2 {
                events.message().await.unwrap().unwrap()
            } else {
                events.next().await.unwrap().unwrap()
            };
            if matches!(event.event, Some(v2::execution_event::Event::Finished(_))) {
                break;
            }
        }
        drop(events);
    }
}
