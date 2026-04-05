use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

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

fn required_env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
}

#[tokio::test]
#[ignore = "requires docker-backed postgres via ANYSERVE_TEST_POSTGRES_DSN"]
async fn postgres_persists_control_plane_and_outputs_but_not_memory_frames() {
    let Some(postgres_dsn) = required_env("ANYSERVE_TEST_POSTGRES_DSN") else {
        eprintln!("skipping: ANYSERVE_TEST_POSTGRES_DSN is not set");
        return;
    };

    let suffix = Uuid::new_v4().simple().to_string();
    let job_id = format!("job-pg-memory-{suffix}");
    let worker_id = format!("worker-pg-memory-{suffix}");
    let interface_name = format!("demo.execute.v1.{suffix}");

    {
        let kernel = Kernel::new(
            Arc::new(SqlStateStore::connect(&postgres_dsn).await.unwrap()),
            Arc::new(MemoryFramePlane::new()),
            Arc::new(NoopClusterNotifier),
            Arc::new(BasicScheduler),
            30,
            30,
            25,
        );

        let job = kernel
            .submit_job(
                Some(job_id.clone()),
                JobSpec {
                    interface_name: interface_name.clone(),
                    policy: ExecutionPolicy::default(),
                    ..JobSpec::default()
                },
            )
            .await
            .unwrap();
        let worker = kernel
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
                br#"{"prompt":"persist me"}"#.to_vec(),
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
                worker_id: Some(worker.worker_id.clone()),
                lease_id: Some(assignment.lease.lease_id.clone()),
                stream_name: "output.default".to_string(),
                scope: StreamScope::Job,
                direction: StreamDirection::WorkerToClient,
                metadata: Attributes::new(),
            })
            .await
            .unwrap();

        kernel
            .push_frame(
                &stream.stream_id,
                Some(worker.worker_id.clone()),
                Some(assignment.lease.lease_id.clone()),
                FrameKind::Data,
                b"stream payload".to_vec(),
                Attributes::new(),
            )
            .await
            .unwrap();
        kernel
            .push_frame(
                &stream.stream_id,
                Some(worker.worker_id.clone()),
                Some(assignment.lease.lease_id.clone()),
                FrameKind::Close,
                Vec::new(),
                Attributes::new(),
            )
            .await
            .unwrap();
        kernel
            .complete_lease(
                &worker.worker_id,
                &assignment.lease.lease_id,
                vec![ObjectRef::inline(br#"{"result":"persisted"}"#.to_vec())],
                Attributes::new(),
            )
            .await
            .unwrap();
    }

    {
        let kernel = Kernel::new(
            Arc::new(SqlStateStore::connect(&postgres_dsn).await.unwrap()),
            Arc::new(MemoryFramePlane::new()),
            Arc::new(NoopClusterNotifier),
            Arc::new(BasicScheduler),
            30,
            30,
            25,
        );

        let job = kernel.get_job(&job_id).await.unwrap();
        assert_eq!(job.state, JobState::Succeeded);
        assert_eq!(job.spec.inputs.len(), 1);
        assert_eq!(
            job.spec.inputs[0],
            ObjectRef::inline(br#"{"prompt":"persist me"}"#.to_vec())
        );
        assert_eq!(job.outputs.len(), 1);
        assert_eq!(
            job.outputs[0],
            ObjectRef::inline(br#"{"result":"persisted"}"#.to_vec())
        );

        let events = kernel.watch_job(&job.job_id, 0).await.unwrap();
        assert_eq!(events.len(), 3);

        let attempts = kernel.list_attempts(&job.job_id).await.unwrap();
        assert_eq!(attempts.len(), 1);

        let streams = kernel.list_streams(&job.job_id).await.unwrap();
        assert_eq!(streams.len(), 2);
        let input_stream = streams
            .iter()
            .find(|stream| stream.stream_name == "input.default")
            .unwrap();
        assert_eq!(input_stream.last_sequence, 1);
        let output_stream = streams
            .iter()
            .find(|stream| stream.stream_name == "output.default")
            .unwrap();
        assert_eq!(output_stream.last_sequence, 2);

        let frames = kernel
            .pull_frames(&output_stream.stream_id, 0)
            .await
            .unwrap();
        assert!(frames.is_empty());
    }
}
