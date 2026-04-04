use std::collections::HashMap;
use std::time::Duration;

use anyhow::Result;
use anyserve_proto::inference::grpc_inference_service_client::GrpcInferenceServiceClient;
use anyserve_proto::inference::{FetchInferRequest, InferCore, InferRequest, SendResponseRequest};
use async_stream::stream;
use clap::{Parser, ValueEnum};
use tokio_stream::StreamExt;
use tonic::Request;
use tracing::info;

#[derive(Clone, Debug, ValueEnum)]
enum Mode {
    Produce,
    Consume,
}

#[derive(Parser, Debug)]
struct Cli {
    #[arg(long, value_enum, default_value = "consume")]
    mode: Mode,
    #[arg(long, default_value = "http://127.0.0.1:50052")]
    endpoint: String,
    #[arg(long)]
    request_id: Option<String>,
    #[arg(long, default_value = "default")]
    queue: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_target(false)
        .compact()
        .init();
    let cli = Cli::parse();

    match cli.mode {
        Mode::Produce => produce(&cli.endpoint, cli.queue, cli.request_id).await,
        Mode::Consume => consume(&cli.endpoint, cli.queue).await,
    }
}

async fn produce(endpoint: &str, queue: String, request_id: Option<String>) -> Result<()> {
    let mut client = GrpcInferenceServiceClient::connect(endpoint.to_string()).await?;

    let request = InferRequest {
        queue: Some(queue),
        infer: Some(InferCore {
            content: b"1 2 3".to_vec(),
            metadata: HashMap::from([("model_name".to_string(), "test".to_string())]),
        }),
        request_id,
    };

    let mut stream = client.infer(Request::new(request)).await?.into_inner();
    while let Some(response) = stream.next().await {
        info!(?response, "received infer response");
    }

    Ok(())
}

async fn consume(endpoint: &str, queue: String) -> Result<()> {
    loop {
        let mut client = GrpcInferenceServiceClient::connect(endpoint.to_string()).await?;
        let mut stream = client
            .fetch_infer(Request::new(FetchInferRequest {
                queue: Some(queue.clone()),
                metadata: HashMap::from([("model_name".to_string(), "test".to_string())]),
            }))
            .await?
            .into_inner();

        if let Some(message) = stream.next().await {
            let message = message?;
            let request_id = message.request_id.clone();
            info!(request_id, ?message, "received infer request");

            let response_stream = stream! {
                yield SendResponseRequest {
                    request_id: request_id.clone(),
                    response: Some(InferCore {
                        content: Vec::new(),
                        metadata: HashMap::from([("@type".to_string(), "response.created".to_string())]),
                    }),
                    metrics: HashMap::new(),
                };

                yield SendResponseRequest {
                    request_id: request_id.clone(),
                    response: Some(InferCore {
                        content: b"first response".to_vec(),
                        metadata: HashMap::from([("@type".to_string(), "response.processing".to_string())]),
                    }),
                    metrics: HashMap::new(),
                };
                tokio::time::sleep(Duration::from_secs(1)).await;

                yield SendResponseRequest {
                    request_id: request_id.clone(),
                    response: Some(InferCore {
                        content: b"second response".to_vec(),
                        metadata: HashMap::from([("@type".to_string(), "response.processing".to_string())]),
                    }),
                    metrics: HashMap::new(),
                };
                tokio::time::sleep(Duration::from_secs(1)).await;

                yield SendResponseRequest {
                    request_id,
                    response: Some(InferCore {
                        content: Vec::new(),
                        metadata: HashMap::from([("@type".to_string(), "response.finished".to_string())]),
                    }),
                    metrics: HashMap::new(),
                };
            };

            client.send_response(Request::new(response_stream)).await?;
        } else {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}
