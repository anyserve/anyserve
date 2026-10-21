//! Real TCP services shared by integration tests; no external databases or GPU.
use std::time::Duration;

use anyserve_client::execution::{WorkerSession, v2};
use anyserve_core::auth::{AuthLayer, AuthPolicy};
use anyserve_core::client_service::ClientGrpcService;
use anyserve_core::runtime::{Runtime, api::RuntimeConfig};
use anyserve_core::worker_service::WorkerGrpcService;
use anyserve_proto::v2::{client_server::ClientServer, worker_server::WorkerServer};
use tokio_stream::wrappers::TcpListenerStream;

pub(crate) struct Server {
    pub runtime: Runtime,
    pub endpoint: String,
    task: tokio::task::JoinHandle<()>,
}
impl Server {
    pub async fn start(config: RuntimeConfig, auth: AuthPolicy) -> Self {
        let runtime = Runtime::start(config).await.unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let client = ClientGrpcService::new(runtime.clone());
        let worker = WorkerGrpcService::new(runtime.clone());
        let task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .layer(AuthLayer::new(auth))
                .add_service(
                    ClientServer::new(client)
                        .max_decoding_message_size(anyserve_proto::MAX_MESSAGE_SIZE)
                        .max_encoding_message_size(anyserve_proto::MAX_MESSAGE_SIZE),
                )
                .add_service(
                    WorkerServer::new(worker)
                        .max_decoding_message_size(anyserve_proto::MAX_MESSAGE_SIZE)
                        .max_encoding_message_size(anyserve_proto::MAX_MESSAGE_SIZE),
                )
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        Self {
            runtime,
            endpoint,
            task,
        }
    }
    pub async fn stop(self) {
        self.task.abort();
        self.runtime.shutdown().await;
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub(crate) struct Worker {
    pub session: WorkerSession,
    pub limits: v2::Limits,
    pub heartbeat_timeout: Duration,
}
impl Worker {
    pub async fn connect(
        server: &Server,
        worker_id: &str,
        interfaces: Vec<String>,
        concurrency: u32,
    ) -> Self {
        let mut session = WorkerSession::connect(
            &server.endpoint,
            None,
            v2::Hello {
                worker_id: worker_id.to_owned(),
                interfaces,
                max_concurrency: concurrency,
                total_capacity: std::collections::HashMap::from([(
                    "slot".to_owned(),
                    i64::from(concurrency),
                )]),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let Some(v2::server_message::Msg::Welcome(welcome)) =
            session.messages.message().await.unwrap().unwrap().msg
        else {
            panic!("missing Welcome");
        };
        let worker = Self {
            session,
            limits: welcome.limits.unwrap(),
            heartbeat_timeout: Duration::from_millis(u64::from(welcome.heartbeat_timeout_ms)),
        };
        worker.credit(concurrency).await;
        worker
    }
    pub async fn credit(&self, free_slots: u32) {
        self.session
            .control
            .send(v2::WorkerMessage {
                msg: Some(v2::worker_message::Msg::Credit(v2::Credit { free_slots })),
            })
            .await
            .unwrap();
    }
    pub async fn assignment(&mut self) -> v2::Assign {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(v2::server_message::Msg::Assign(assignment)) =
                    self.session.messages.message().await.unwrap().unwrap().msg
                {
                    self.session
                        .control
                        .send(v2::WorkerMessage {
                            msg: Some(v2::worker_message::Msg::AssignReply(v2::AssignReply {
                                lease_id: assignment.lease_id.clone(),
                                accepted: true,
                                reject_reason: String::new(),
                            })),
                        })
                        .await
                        .unwrap();
                    return assignment;
                }
            }
        })
        .await
        .expect("assignment timed out")
    }
}

pub(crate) fn temporary_path(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "anyserve-{label}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}
