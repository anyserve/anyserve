use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyserve_core::frame::MemoryFramePlane;
use anyserve_core::kernel::{Kernel, OpenStreamCommand};
use anyserve_core::model::{
    Attributes, ExecutionPolicy, FrameKind, JobSpec, JobState, ObjectRef, StreamDirection,
    StreamScope, WorkerSpec,
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

        let input_stream = kernel
            .open_stream(OpenStreamCommand {
                job_id: job.job_id.clone(),
                attempt_id: None,
                worker_id: None,
                lease_id: None,
                stream_name: "input.default".to_string(),
                scope: StreamScope::Job,
                direction: StreamDirection::ClientToWorker,
                metadata: Attributes::new(),
            })
            .await
            .unwrap();

        kernel
            .push_frame(
                &input_stream.stream_id,
                None,
                None,
                FrameKind::Data,
                br#"{"prompt":"sqlite persisted input"}"#.to_vec(),
                Attributes::new(),
            )
            .await
            .unwrap();
        kernel
            .close_stream(&input_stream.stream_id, None, None, Attributes::new())
            .await
            .unwrap();

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
        assert_eq!(
            job.spec.inputs,
            vec![ObjectRef::inline(
                br#"{"prompt":"sqlite persisted input"}"#.to_vec()
            )]
        );

        let events = kernel.watch_job(&job.job_id, 0).await.unwrap();
        assert_eq!(events.len(), 2);

        let streams = kernel.list_streams(&job.job_id).await.unwrap();
        assert_eq!(streams.len(), 2);
        let input_stream = streams
            .iter()
            .find(|stream| stream.stream_name == "input.default")
            .unwrap();
        assert_eq!(input_stream.last_sequence, 1);
        let output_stream = streams
            .iter()
            .find(|stream| stream.stream_name == "stdout")
            .unwrap();
        assert_eq!(output_stream.last_sequence, 2);

        let frames = kernel
            .pull_frames(&output_stream.stream_id, 0)
            .await
            .unwrap();
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

#[tokio::test]
async fn sqlite_summary_queries_return_counts_and_attempt_rollups() {
    let (dsn, db_path) = sqlite_dsn("summary-queries");

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

    let worker = kernel
        .register_worker(
            Some("worker-sqlite-summary".to_string()),
            WorkerSpec {
                interfaces: BTreeSet::from(["demo.summary.v1".to_string()]),
                total_capacity: BTreeMap::from([("slot".to_string(), 1)]),
                max_active_leases: 1,
                ..WorkerSpec::default()
            },
        )
        .await
        .unwrap();
    let succeeded = kernel
        .submit_job(
            Some("job-succeeded".to_string()),
            JobSpec {
                interface_name: "demo.summary.v1".to_string(),
                metadata: Attributes::from([("source".to_string(), "summary".to_string())]),
                ..JobSpec::default()
            },
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(2)).await;
    let cancelled = kernel
        .submit_job(
            Some("job-cancelled".to_string()),
            JobSpec {
                interface_name: "demo.summary.v1".to_string(),
                ..JobSpec::default()
            },
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(2)).await;
    let pending = kernel
        .submit_job(
            Some("job-pending".to_string()),
            JobSpec {
                interface_name: "demo.summary.v1".to_string(),
                ..JobSpec::default()
            },
        )
        .await
        .unwrap();

    let assignment = kernel.poll_lease(&worker.worker_id).await.unwrap().unwrap();
    assert_eq!(assignment.job.job_id, succeeded.job_id);
    kernel
        .complete_lease(
            &worker.worker_id,
            &assignment.lease.lease_id,
            vec![ObjectRef::inline(b"done".to_vec())],
            Attributes::new(),
        )
        .await
        .unwrap();
    kernel.cancel_job(&cancelled.job_id).await.unwrap();

    let counts = kernel.job_state_counts().await.unwrap();
    assert_eq!(counts.pending, 1);
    assert_eq!(counts.succeeded, 1);
    assert_eq!(counts.cancelled, 1);

    let page = kernel
        .list_job_summary_page(&[JobState::Succeeded, JobState::Cancelled], 10, 0)
        .await
        .unwrap();
    assert_eq!(page.total, 2);
    assert_eq!(page.jobs.len(), 2);
    let succeeded_summary = page
        .jobs
        .iter()
        .find(|job| job.job_id == succeeded.job_id)
        .unwrap();
    assert_eq!(succeeded_summary.attempt_count, 1);
    assert_eq!(
        succeeded_summary.latest_worker_id.as_deref(),
        Some("worker-sqlite-summary")
    );
    assert_eq!(succeeded_summary.source.as_deref(), Some("summary"));
    assert!(
        page.jobs
            .iter()
            .all(|job| { matches!(job.state, JobState::Succeeded | JobState::Cancelled) })
    );
    assert!(page.jobs.iter().all(|job| job.job_id != pending.job_id));

    let _ = std::fs::remove_file(db_path);
}
