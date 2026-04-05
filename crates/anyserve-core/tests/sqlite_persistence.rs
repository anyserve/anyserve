use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;

use anyserve_core::frame::MemoryFramePlane;
use anyserve_core::kernel::{Kernel, OpenStreamCommand};
use anyserve_core::model::{
    Attributes, ExecutionPolicy, FrameKind, JobSpec, StreamDirection, StreamScope, WorkerSpec,
};
use anyserve_core::notify::NoopClusterNotifier;
use anyserve_core::scheduler::BasicScheduler;
use anyserve_core::sql_store::SqlStateStore;
use uuid::Uuid;

fn sqlite_dsn(test_name: &str) -> (String, PathBuf) {
    let path = std::env::temp_dir().join(format!("anyserve-{test_name}-{}.db", Uuid::new_v4()));
    (format!("sqlite://{}", path.display()), path)
}

#[tokio::test]
async fn sqlite_persists_control_plane_but_not_frames() {
    let (dsn, db_path) = sqlite_dsn("persistence");

    {
        let state_store = Arc::new(SqlStateStore::connect(&dsn).await.unwrap());
        let kernel = Kernel::new(
            state_store,
            Arc::new(MemoryFramePlane::new()),
            Arc::new(NoopClusterNotifier),
            Arc::new(BasicScheduler),
            30,
            30,
            25,
        );

        let job = kernel
            .submit_job(
                Some("job-sqlite-1".to_string()),
                JobSpec {
                    interface_name: "demo.execute.v1".to_string(),
                    policy: ExecutionPolicy::default(),
                    ..JobSpec::default()
                },
            )
            .await
            .unwrap();
        let worker = kernel
            .register_worker(
                Some("worker-sqlite-1".to_string()),
                WorkerSpec {
                    interfaces: BTreeSet::from(["demo.execute.v1".to_string()]),
                    total_capacity: BTreeMap::from([("slot".to_string(), 1)]),
                    max_active_leases: 1,
                    ..WorkerSpec::default()
                },
            )
            .await
            .unwrap();
        let assignment = kernel.poll_lease(&worker.worker_id).await.unwrap().unwrap();

        let stream = kernel
            .open_stream(OpenStreamCommand {
                job_id: job.job_id.clone(),
                attempt_id: None,
                worker_id: None,
                lease_id: None,
                stream_name: "stdout".to_string(),
                scope: StreamScope::Job,
                direction: StreamDirection::Bidirectional,
                metadata: Attributes::new(),
            })
            .await
            .unwrap();

        kernel
            .push_frame(
                &stream.stream_id,
                None,
                None,
                FrameKind::Data,
                b"hello".to_vec(),
                Attributes::new(),
            )
            .await
            .unwrap();
        kernel
            .push_frame(
                &stream.stream_id,
                None,
                None,
                FrameKind::Close,
                Vec::new(),
                Attributes::new(),
            )
            .await
            .unwrap();

        let persisted_stream = kernel.get_stream(&stream.stream_id).await.unwrap();
        assert_eq!(persisted_stream.last_sequence, 2);
        assert!(assignment.job.current_attempt_id.is_some());
    }

    {
        let state_store = Arc::new(SqlStateStore::connect(&dsn).await.unwrap());
        let kernel = Kernel::new(
            state_store,
            Arc::new(MemoryFramePlane::new()),
            Arc::new(NoopClusterNotifier),
            Arc::new(BasicScheduler),
            30,
            30,
            25,
        );

        let job = kernel.get_job("job-sqlite-1").await.unwrap();
        assert_eq!(job.job_id, "job-sqlite-1");
        assert!(job.current_attempt_id.is_some());

        let events = kernel.watch_job(&job.job_id, 0).await.unwrap();
        assert_eq!(events.len(), 2);

        let streams = kernel.list_streams(&job.job_id).await.unwrap();
        assert_eq!(streams.len(), 1);
        assert_eq!(streams[0].stream_name, "stdout");
        assert_eq!(streams[0].last_sequence, 2);

        let frames = kernel.pull_frames(&streams[0].stream_id, 0).await.unwrap();
        assert!(frames.is_empty());
    }

    let _ = std::fs::remove_file(db_path);
}

#[tokio::test]
async fn sqlite_rejects_second_control_plane_instance() {
    let (dsn, db_path) = sqlite_dsn("single-instance-lock");
    let lock_path = db_path.with_file_name(format!(
        "{}.control-plane.lock",
        db_path
            .file_name()
            .expect("sqlite db file name should exist")
            .to_string_lossy()
    ));

    let first = SqlStateStore::connect(&dsn).await.unwrap();
    let error = match SqlStateStore::connect(&dsn).await {
        Ok(_) => panic!("second sqlite store should fail while the first lock is held"),
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains("SQLite backend only supports a single control-plane instance")
    );

    drop(first);

    let reopened = SqlStateStore::connect(&dsn).await.unwrap();
    drop(reopened);

    let _ = std::fs::remove_file(db_path);
    let _ = std::fs::remove_file(lock_path);
}
