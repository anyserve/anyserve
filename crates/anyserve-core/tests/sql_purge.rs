use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use anyserve_core::frame::MemoryFramePlane;
use anyserve_core::kernel::{Kernel, OpenStreamCommand};
use anyserve_core::model::{
    Attributes, JobSpec, JobState, StreamDirection, StreamScope, WorkerSpec,
};
use anyserve_core::notify::NoopClusterNotifier;
use anyserve_core::scheduler::BasicScheduler;
use anyserve_core::sql_store::SqlStateStore;
use uuid::Uuid;

async fn purge_round_trip(dsn: &str) {
    let suffix = Uuid::new_v4().simple().to_string();
    let interface_name = format!("demo.purge.v1.{suffix}");
    let worker_id = format!("worker-purge-{suffix}");
    let kernel = Kernel::new(
        Arc::new(SqlStateStore::connect(dsn).await.unwrap()),
        Arc::new(MemoryFramePlane::new()),
        Arc::new(NoopClusterNotifier),
        Arc::new(BasicScheduler),
        30,
        30,
        25,
    );
    kernel
        .register_worker(
            Some(worker_id.clone()),
            WorkerSpec {
                interfaces: BTreeSet::from([interface_name.clone()]),
                total_capacity: BTreeMap::from([("slot".to_string(), 1)]),
                max_active_leases: 1,
                ..WorkerSpec::default()
            },
        )
        .await
        .unwrap();
    let done = format!("job-done-{suffix}");
    let waiting = format!("job-waiting-{suffix}");
    for job_id in [&done, &waiting] {
        kernel
            .submit_job(
                Some(job_id.clone()),
                JobSpec {
                    interface_name: interface_name.clone(),
                    ..JobSpec::default()
                },
            )
            .await
            .unwrap();
    }
    let lease = kernel.poll_lease(&worker_id).await.unwrap().unwrap();
    kernel
        .open_stream(OpenStreamCommand {
            job_id: lease.job.job_id.clone(),
            attempt_id: None,
            worker_id: Some(worker_id.clone()),
            lease_id: Some(lease.lease.lease_id.clone()),
            stream_name: "output.default".to_string(),
            scope: StreamScope::Lease,
            direction: StreamDirection::WorkerToClient,
            metadata: Attributes::new(),
        })
        .await
        .unwrap();
    kernel
        .complete_lease(
            &worker_id,
            &lease.lease.lease_id,
            Vec::new(),
            Attributes::new(),
        )
        .await
        .unwrap();

    kernel.set_job_retention(Duration::from_millis(1));
    tokio::time::sleep(Duration::from_millis(5)).await;
    // Shared databases may hold other expired jobs; ours had one stream.
    assert!(kernel.purge_expired_jobs().await.unwrap() >= 1);

    assert!(kernel.get_job(&lease.job.job_id).await.is_err());
    assert!(kernel.get_attempt(&lease.attempt.attempt_id).await.is_err());
    let other = if lease.job.job_id == done {
        &waiting
    } else {
        &done
    };
    assert_eq!(
        kernel.get_job(other).await.unwrap().state,
        JobState::Pending
    );
}

#[tokio::test]
async fn sqlite_purges_expired_terminal_jobs() {
    let path = std::env::temp_dir().join(format!("anyserve-purge-{}.db", Uuid::new_v4()));
    purge_round_trip(&format!("sqlite://{}?mode=rwc", path.display())).await;
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
#[ignore = "requires docker-backed postgres via ANYSERVE_TEST_POSTGRES_DSN"]
async fn postgres_purges_expired_terminal_jobs() {
    let Ok(dsn) = std::env::var("ANYSERVE_TEST_POSTGRES_DSN") else {
        return;
    };
    purge_round_trip(&dsn).await;
}
