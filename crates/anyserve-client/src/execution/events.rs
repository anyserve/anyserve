use std::pin::Pin;
use std::task::{Context, Poll};

use tokio_stream::{Stream, StreamExt};
use tonic::{Status, Streaming};

use super::v2;

/// Execution events whose terminal event is returned after the gRPC response
/// ends. Breaking on Finished can then release a completed HTTP/2 stream safely.
#[derive(Debug)]
pub struct ExecutionEvents {
    stream: Streaming<v2::ExecutionEvent>,
    terminal: Option<v2::ExecutionEvent>,
    ended: bool,
}

impl ExecutionEvents {
    pub(super) fn new(stream: Streaming<v2::ExecutionEvent>) -> Self {
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
