use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyserve_client::{AnyserveClient, JobSubmission, ObjectRef, WorkerRegistration, object_ref};
use anyserve_core::auth::AuthPolicy;
use anyserve_core::frame::MemoryFramePlane;
use anyserve_core::kernel::Kernel;
use anyserve_core::notify::NoopClusterNotifier;
use anyserve_core::scheduler::BasicScheduler;
use anyserve_core::store::MemoryStateStore;

use super::run_grpc_server;
use crate::serve_config::GrpcConfig;

#[tokio::test]
async fn grpc_accepts_large_inline_inputs_and_outputs() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let kernel = Arc::new(Kernel::new(
        Arc::new(MemoryStateStore::new()),
        Arc::new(MemoryFramePlane::new()),
        Arc::new(NoopClusterNotifier),
        Arc::new(BasicScheduler),
        30,
        30,
        25,
    ));
    let (shutdown, receiver) = tokio::sync::watch::channel(false);
    let server = tokio::spawn(run_grpc_server(
        addr,
        kernel,
        GrpcConfig::default(),
        AuthPolicy::default(),
        receiver,
    ));
    let endpoint = format!("http://{addr}");
    let mut client = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(client) = AnyserveClient::connect_with_token(&endpoint, None).await {
                break client;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let worker = client
        .register_worker(WorkerRegistration::new(vec!["test.large".into()]))
        .await
        .unwrap();
    let payload = vec![42; 5 * 1024 * 1024];
    let object = ObjectRef {
        reference: Some(object_ref::Reference::Inline(payload.clone())),
        metadata: HashMap::new(),
    };
    let mut submission = JobSubmission::new("test.large");
    submission.inputs = vec![object.clone()];
    let job = client.submit_job(submission).await.unwrap();
    let grant = client
        .poll_lease(worker.worker_id.clone())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(grant.job.spec.unwrap().inputs[0], object);
    client
        .complete_lease(
            worker.worker_id,
            grant.lease.lease_id,
            vec![object.clone()],
            HashMap::new(),
        )
        .await
        .unwrap();
    assert_eq!(
        client.get_job(job.job_id).await.unwrap().outputs,
        vec![object]
    );
    shutdown.send(true).unwrap();
    server.await.unwrap().unwrap();
}
