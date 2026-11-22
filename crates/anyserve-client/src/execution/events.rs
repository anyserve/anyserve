use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::sync::mpsc;
use tokio_stream::{Stream, StreamExt};
use tonic::{Status, Streaming};

use super::v2;

/// Unified streaming handle supporting both HTTP/2 gRPC streams and WebSocket
/// framed streams.
#[derive(Debug)]
#[allow(
    clippy::large_enum_variant,
    reason = "RpcStream wraps tonic::Streaming directly without extra allocation"
)]
pub enum RpcStream<T> {
    Grpc(Streaming<T>),
    Ws(mpsc::Receiver<Result<T, Status>>),
}

impl<T> RpcStream<T> {
    pub async fn message(&mut self) -> Result<Option<T>, Status> {
        match self {
            Self::Grpc(stream) => stream.message().await,
            Self::Ws(rx) => match rx.recv().await {
                Some(item) => item.map(Some),
                None => Ok(None),
            },
        }
    }
}

impl<T> Stream for RpcStream<T> {
    type Item = Result<T, Status>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.get_mut() {
            Self::Grpc(stream) => Pin::new(stream).poll_next(cx),
            Self::Ws(rx) => rx.poll_recv(cx),
        }
    }
}

/// Execution events whose terminal event is returned after the response stream
/// ends. Breaking on Finished can then release a completed HTTP/2 or WebSocket
/// stream safely.
#[derive(Debug)]
pub struct ExecutionEvents {
    stream: RpcStream<v2::ExecutionEvent>,
    terminal: Option<v2::ExecutionEvent>,
    ended: bool,
}

impl ExecutionEvents {
    pub(crate) fn new(stream: RpcStream<v2::ExecutionEvent>) -> Self {
        Self {
            stream,
            terminal: None,
            ended: false,
        }
    }

    pub async fn message(&mut self) -> Result<Option<v2::ExecutionEvent>, Status> {
        self.next().await.transpose()
    }
}

impl Stream for ExecutionEvents {
    type Item = Result<v2::ExecutionEvent, Status>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.ended {
            return Poll::Ready(None);
        }
        loop {
            match Pin::new(&mut this.stream).poll_next(cx) {
                Poll::Ready(Some(Ok(event))) => {
                    if this.terminal.is_some() {
                        this.ended = true;
                        this.terminal = None;
                        return Poll::Ready(Some(Err(Status::internal(
                            "event received after Finished",
                        ))));
                    }
                    if matches!(event.event, Some(v2::execution_event::Event::Finished(_))) {
                        // Dropping a body with unread trailers sends CANCEL.
                        // Late trailers can exhaust h2's local reset allowance
                        // and close a channel used by unrelated calls.
                        this.terminal = Some(event);
                    } else {
                        return Poll::Ready(Some(Ok(event)));
                    }
                }
                Poll::Ready(Some(Err(status))) => {
                    this.ended = true;
                    this.terminal = None;
                    return Poll::Ready(Some(Err(status)));
                }
                Poll::Ready(None) => {
                    this.ended = true;
                    return Poll::Ready(this.terminal.take().map(Ok));
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

#[cfg(test)]
mod tests;
