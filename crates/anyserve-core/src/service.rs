use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Error;
use anyserve_proto::inference::grpc_inference_service_server::GrpcInferenceService;
use anyserve_proto::inference::{
    FetchInferRequest, FetchInferResponse, InferCore, InferRequest, InferResponse,
    SendResponseRequest,
};
use async_stream::try_stream;
use futures::{Stream, StreamExt};
use tonic::{Request, Response, Status};
use tracing::{debug, error};
use uuid::Uuid;

use crate::consts::{
    INFER_METADATA_STATUS, INFER_METADATA_STATUS_VALUE_COMPLETED,
    INFER_METADATA_STATUS_VALUE_FAILED, INFER_METADATA_STATUS_VALUE_PROCESSING,
    INFER_METADATA_STATUS_VALUE_QUEUED, INFER_METADATA_STATUS_VALUE_SCHEDULED, METADATA_TIMESTAMP,
    RESPONSE_METADATA_TYPE, RESPONSE_METADATA_TYPE_VALUE_ACK, RESPONSE_METADATA_TYPE_VALUE_FAILED,
    RESPONSE_METADATA_TYPE_VALUE_FINISH, RESPONSE_METADATA_TYPE_VALUE_PROCESSING,
};
use crate::meta::{MetaStore, normalize_queue, queue_from_request};

type ResponseStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

#[derive(Clone)]
pub struct InferenceService {
    meta: Arc<dyn MetaStore>,
}

impl InferenceService {
    pub fn new(meta: Arc<dyn MetaStore>) -> Self {
        Self { meta }
    }
}

#[tonic::async_trait]
impl GrpcInferenceService for InferenceService {
    type InferStream = ResponseStream<InferResponse>;
    type InferStreamStream = ResponseStream<InferResponse>;
    type FetchInferStream = ResponseStream<FetchInferResponse>;

    async fn infer(
        &self,
        request: Request<InferRequest>,
    ) -> Result<Response<Self::InferStream>, Status> {
        let request = request.into_inner();
        let meta = Arc::clone(&self.meta);
        let queue = queue_from_request(&request);
        let stream = try_stream! {
            let request_id = if let Some(request_id) = request.request_id.clone() {
                if !meta.exists_infer_request(&request_id).await.map_err(to_status)? {
                    Err(Status::not_found("infer request not found"))?;
                }
                request_id
            } else {
                let mut infer = request
                    .infer
                    .clone()
                    .ok_or_else(|| Status::invalid_argument("infer is required"))?;
                infer
                    .metadata
                    .entry(METADATA_TIMESTAMP.to_string())
                    .or_insert_with(current_timestamp_nanos);
                infer.metadata.insert(
                    INFER_METADATA_STATUS.to_string(),
                    INFER_METADATA_STATUS_VALUE_QUEUED.to_string(),
                );
                let request_id = Uuid::new_v4().to_string();
                meta.queue_infer_request(&queue, &infer, &request_id)
                    .await
                    .map_err(to_status)?;
                debug!(request_id, queue, "queued infer request");
                request_id
            };

            loop {
                if let Some(response) = meta
                    .pop_infer_response(&request_id)
                    .await
                    .map_err(to_status)?
                {
                    let finished = response
                        .metadata
                        .get(RESPONSE_METADATA_TYPE)
                        .map(String::as_str)
                        == Some(RESPONSE_METADATA_TYPE_VALUE_FINISH);

                    yield InferResponse {
                        request_id: request_id.clone(),
                        response: Some(response),
                    };

                    if finished {
                        break;
                    }
                } else {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        };

        Ok(Response::new(Box::pin(stream)))
    }

    async fn infer_stream(
        &self,
        request: Request<tonic::Streaming<InferRequest>>,
    ) -> Result<Response<Self::InferStreamStream>, Status> {
        let mut stream = request.into_inner();
        let request_id = Uuid::new_v4().to_string();

        let outbound = try_stream! {
            while let Some(item) = stream.next().await {
                let request = item.map_err(to_status)?;
                let infer = request.infer.unwrap_or_default();
                let mut metadata = infer.metadata.clone();
                metadata.insert(
                    RESPONSE_METADATA_TYPE.to_string(),
                    RESPONSE_METADATA_TYPE_VALUE_PROCESSING.to_string(),
                );

                yield InferResponse {
                    request_id: request_id.clone(),
                    response: Some(InferCore {
                        content: infer.content,
                        metadata,
                    }),
                };
            }

            yield InferResponse {
                request_id,
                response: Some(InferCore {
                    content: Vec::new(),
                    metadata: HashMap::from([(
                        RESPONSE_METADATA_TYPE.to_string(),
                        RESPONSE_METADATA_TYPE_VALUE_FINISH.to_string(),
                    )]),
                }),
            };
        };

        Ok(Response::new(Box::pin(outbound)))
    }

    async fn fetch_infer(
        &self,
        request: Request<FetchInferRequest>,
    ) -> Result<Response<Self::FetchInferStream>, Status> {
        let request = request.into_inner();
        let queue = normalize_queue(request.queue);
        let meta = Arc::clone(&self.meta);
        let filters = request.metadata;

        let stream = try_stream! {
            if let Some((request_id, infer)) = meta
                .pop_infer_request(&queue, &filters)
                .await
                .map_err(to_status)?
            {
                debug!(request_id, queue, "worker fetched infer request");
                yield FetchInferResponse {
                    request_id,
                    infer: Some(infer),
                };
            }
        };

        Ok(Response::new(Box::pin(stream)))
    }

    async fn send_response(
        &self,
        request: Request<tonic::Streaming<SendResponseRequest>>,
    ) -> Result<Response<()>, Status> {
        let mut stream = request.into_inner();

        while let Some(item) = stream.next().await {
            let request = item.map_err(to_status)?;
            let request_id = request.request_id.clone();
            let response = request
                .response
                .ok_or_else(|| Status::invalid_argument("response is required"))?;
            let response_type = response
                .metadata
                .get(RESPONSE_METADATA_TYPE)
                .cloned()
                .ok_or_else(|| Status::invalid_argument("response metadata '@type' is required"))?;

            match response_type.as_str() {
                RESPONSE_METADATA_TYPE_VALUE_ACK => {
                    self.set_status(&request_id, INFER_METADATA_STATUS_VALUE_SCHEDULED)
                        .await
                        .map_err(to_status)?;
                    self.meta
                        .queue_send_response(&request_id, &response)
                        .await
                        .map_err(to_status)?;
                }
                RESPONSE_METADATA_TYPE_VALUE_PROCESSING => {
                    self.set_status(&request_id, INFER_METADATA_STATUS_VALUE_PROCESSING)
                        .await
                        .map_err(to_status)?;
                    self.meta
                        .queue_send_response(&request_id, &response)
                        .await
                        .map_err(to_status)?;
                }
                RESPONSE_METADATA_TYPE_VALUE_FINISH => {
                    self.set_status(&request_id, INFER_METADATA_STATUS_VALUE_COMPLETED)
                        .await
                        .map_err(to_status)?;
                    self.meta
                        .queue_send_response(&request_id, &response)
                        .await
                        .map_err(to_status)?;
                    return Ok(Response::new(()));
                }
                RESPONSE_METADATA_TYPE_VALUE_FAILED => {
                    self.set_status(&request_id, INFER_METADATA_STATUS_VALUE_FAILED)
                        .await
                        .map_err(to_status)?;
                    self.meta
                        .queue_send_response(&request_id, &response)
                        .await
                        .map_err(to_status)?;
                    error!(request_id, "worker reported failed inference");
                    return Err(Status::internal("inference response error"));
                }
                other => {
                    return Err(Status::invalid_argument(format!(
                        "unknown response type: {other}"
                    )));
                }
            }
        }

        Ok(Response::new(()))
    }
}

impl InferenceService {
    async fn set_status(&self, request_id: &str, status: &str) -> Result<(), Error> {
        self.meta
            .set_infer_request_metadata(
                request_id,
                &HashMap::from([(INFER_METADATA_STATUS.to_string(), status.to_string())]),
            )
            .await
    }
}

fn to_status(error: impl Into<Error>) -> Status {
    let error = error.into();
    Status::internal(error.to_string())
}

fn current_timestamp_nanos() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos().to_string())
        .unwrap_or_else(|_| "0".to_string())
}
