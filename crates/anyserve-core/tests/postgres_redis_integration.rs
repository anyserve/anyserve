use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use anyserve_core::frame::RedisFramePlane;
use anyserve_core::kernel::{Kernel, OpenStreamCommand};
use anyserve_core::model::{
    Attributes, ExecutionPolicy, FrameKind, JobSpec, JobState, StreamDirection, StreamScope,
    WorkerSpec,
};
use anyserve_core::notify::RedisClusterNotifier;
use anyserve_core::scheduler::BasicScheduler;
use anyserve_core::sql_store::SqlStateStore;
use uuid::Uuid;

fn required_env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
}

#[tokio::test]
#[ignore = "requires docker-backed postgres + redis via ANYSERVE_TEST_POSTGRES_DSN and ANYSERVE_TEST_REDIS_URL"]
async fn postgres_and_redis_share_state_and_frames_across_kernels() {
    let Some(postgres_dsn) = required_env("ANYSERVE_TEST_POSTGRES_DSN") else {
        eprintln!("skipping: ANYSERVE_TEST_POSTGRES_DSN is not set");
        return;
    };
    let Some(redis_url) = required_env("ANYSERVE_TEST_REDIS_URL") else {
        eprintln!("skipping: ANYSERVE_TEST_REDIS_URL is not set");
        return;
    };

    let suffix = Uuid::new_v4().simple().to_string();
    let job_id = format!("job-pg-redis-{suffix}");
    let worker_id = format!("worker-pg-redis-{suffix}");
    let interface_name = format!("demo.execute.v1.{suffix}");

    let kernel_a = Kernel::new(
        Arc::new(SqlStateStore::connect(&postgres_dsn).await.unwrap()),
        Arc::new(RedisFramePlane::new(&redis_url, 300).unwrap()),
        Arc::new(RedisClusterNotifier::new(&redis_url).unwrap()),
        Arc::new(BasicScheduler),
        30,
        30,
        25,
    );
    let kernel_b = Kernel::new(
        Arc::new(SqlStateStore::connect(&postgres_dsn).await.unwrap()),
        Arc::new(RedisFramePlane::new(&redis_url, 300).unwrap()),
        Arc::new(RedisClusterNotifier::new(&redis_url).unwrap()),
        Arc::new(BasicScheduler),
        30,
        30,
        25,
    );

    let job = kernel_a
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
    let worker = kernel_a
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
    let assignment = kernel_a
        .poll_lease(&worker.worker_id)
        .await
        .unwrap()
        .unwrap();

    let stream = kernel_a
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

    kernel_a
        .push_frame(
            &stream.stream_id,
            None,
            None,
            FrameKind::Data,
            b"hello from kernel a".to_vec(),
            Attributes::new(),
        )
        .await
        .unwrap();
    kernel_a
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

    let job_from_b = kernel_b.get_job(&job_id).await.unwrap();
    assert_eq!(job_from_b.job_id, job_id);
    assert_eq!(job_from_b.state, JobState::Leased);
    assert_eq!(
        job_from_b.lease_id.as_deref(),
        Some(assignment.lease.lease_id.as_str())
    );

    let events_from_b = kernel_b.watch_job(&job_id, 0).await.unwrap();
    assert_eq!(events_from_b.len(), 2);

    let stream_from_b = kernel_b.get_stream(&stream.stream_id).await.unwrap();
    assert_eq!(stream_from_b.last_sequence, 2);

    let frames_from_b = kernel_b.pull_frames(&stream.stream_id, 0).await.unwrap();
    assert_eq!(frames_from_b.len(), 2);
    assert_eq!(frames_from_b[0].payload, b"hello from kernel a".to_vec());
    assert_eq!(frames_from_b[0].sequence, 1);
    assert_eq!(frames_from_b[1].kind, FrameKind::Close);
    assert_eq!(frames_from_b[1].sequence, 2);

    kernel_b
        .complete_lease(
            &worker_id,
            &assignment.lease.lease_id,
            Vec::new(),
            Attributes::new(),
        )
        .await
        .unwrap();

    let completed_job = kernel_a.get_job(&job_id).await.unwrap();
    assert_eq!(completed_job.state, JobState::Succeeded);

    let final_events = kernel_a.watch_job(&job_id, 0).await.unwrap();
    assert_eq!(final_events.len(), 3);
}

#[tokio::test]
#[ignore = "requires docker-backed postgres + redis via ANYSERVE_TEST_POSTGRES_DSN and ANYSERVE_TEST_REDIS_URL"]
async fn postgres_only_assigns_one_concurrent_lease() {
    let Some(postgres_dsn) = required_env("ANYSERVE_TEST_POSTGRES_DSN") else {
        eprintln!("skipping: ANYSERVE_TEST_POSTGRES_DSN is not set");
        return;
    };
    let Some(redis_url) = required_env("ANYSERVE_TEST_REDIS_URL") else {
        eprintln!("skipping: ANYSERVE_TEST_REDIS_URL is not set");
        return;
    };

    let suffix = Uuid::new_v4().simple().to_string();
    let job_id = format!("job-pg-race-{suffix}");
    let worker_a_id = format!("worker-pg-race-a-{suffix}");
    let worker_b_id = format!("worker-pg-race-b-{suffix}");
    let interface_name = format!("demo.execute.v1.{suffix}");

    let kernel_a = Kernel::new(
        Arc::new(SqlStateStore::connect(&postgres_dsn).await.unwrap()),
        Arc::new(RedisFramePlane::new(&redis_url, 300).unwrap()),
        Arc::new(RedisClusterNotifier::new(&redis_url).unwrap()),
        Arc::new(BasicScheduler),
        30,
        30,
        25,
    );
    let kernel_b = Kernel::new(
        Arc::new(SqlStateStore::connect(&postgres_dsn).await.unwrap()),
        Arc::new(RedisFramePlane::new(&redis_url, 300).unwrap()),
        Arc::new(RedisClusterNotifier::new(&redis_url).unwrap()),
        Arc::new(BasicScheduler),
        30,
        30,
        25,
    );

    kernel_a
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

    let worker_a = kernel_a
        .register_worker(
            Some(worker_a_id.clone()),
            WorkerSpec {
                interfaces: BTreeSet::from([interface_name.clone()]),
                total_capacity: BTreeMap::from([("slot".to_string(), 1)]),
                max_active_leases: 1,
                ..WorkerSpec::default()
            },
        )
        .await
        .unwrap();
    let worker_b = kernel_b
        .register_worker(
            Some(worker_b_id.clone()),
            WorkerSpec {
                interfaces: BTreeSet::from([interface_name]),
                total_capacity: BTreeMap::from([("slot".to_string(), 1)]),
                max_active_leases: 1,
                ..WorkerSpec::default()
            },
        )
        .await
        .unwrap();

    let (left, right) = tokio::join!(
        kernel_a.poll_lease(&worker_a.worker_id),
        kernel_b.poll_lease(&worker_b.worker_id)
    );

    let outcomes = [left.unwrap(), right.unwrap()];
    let assignments: Vec<_> = outcomes.into_iter().flatten().collect();
    assert_eq!(assignments.len(), 1);

    let leased_job = kernel_a.get_job(&job_id).await.unwrap();
    assert_eq!(leased_job.state, JobState::Leased);
    assert_eq!(
        leased_job.lease_id.as_deref(),
        Some(assignments[0].lease.lease_id.as_str())
    );

    let events = kernel_b.watch_job(&job_id, 0).await.unwrap();
    assert_eq!(events.len(), 2);
}

#[tokio::test]
#[ignore = "requires docker-backed postgres + redis via ANYSERVE_TEST_POSTGRES_DSN and ANYSERVE_TEST_REDIS_URL"]
async fn postgres_redis_notifies_cross_kernel_watchers_and_unblocks_frames() {
    let Some(postgres_dsn) = required_env("ANYSERVE_TEST_POSTGRES_DSN") else {
        eprintln!("skipping: ANYSERVE_TEST_POSTGRES_DSN is not set");
        return;
    };
    let Some(redis_url) = required_env("ANYSERVE_TEST_REDIS_URL") else {
        eprintln!("skipping: ANYSERVE_TEST_REDIS_URL is not set");
        return;
    };

    let suffix = Uuid::new_v4().simple().to_string();
    let job_id = format!("job-pg-notify-{suffix}");
    let worker_id = format!("worker-pg-notify-{suffix}");
    let interface_name = format!("demo.execute.v1.{suffix}");

    let kernel_a = Kernel::new(
        Arc::new(SqlStateStore::connect(&postgres_dsn).await.unwrap()),
        Arc::new(RedisFramePlane::new(&redis_url, 300).unwrap()),
        Arc::new(RedisClusterNotifier::new(&redis_url).unwrap()),
        Arc::new(BasicScheduler),
        30,
        30,
        25,
    );
    let kernel_b = Kernel::new(
        Arc::new(SqlStateStore::connect(&postgres_dsn).await.unwrap()),
        Arc::new(RedisFramePlane::new(&redis_url, 300).unwrap()),
        Arc::new(RedisClusterNotifier::new(&redis_url).unwrap()),
        Arc::new(BasicScheduler),
        30,
        30,
        25,
    );

    let job = kernel_a
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
    let worker = kernel_a
        .register_worker(
            Some(worker_id.clone()),
            WorkerSpec {
                interfaces: BTreeSet::from([interface_name]),
                total_capacity: BTreeMap::from([("slot".to_string(), 1)]),
                max_active_leases: 1,
                ..WorkerSpec::default()
            },
        )
        .await
        .unwrap();
    let assignment = kernel_a
        .poll_lease(&worker.worker_id)
        .await
        .unwrap()
        .unwrap();

    let mut cluster_job_events = kernel_b
        .subscribe_cluster_job_events(&job.job_id)
        .await
        .unwrap();
    kernel_a
        .report_event(
            &worker.worker_id,
            &assignment.lease.lease_id,
            anyserve_core::model::EventKind::Progress,
            b"step".to_vec(),
            Attributes::new(),
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), cluster_job_events.changed())
        .await
        .expect("cluster job event notification should arrive")
        .unwrap();

    let events = kernel_b.watch_job(&job.job_id, 0).await.unwrap();
    assert!(
        events
            .iter()
            .any(|event| event.kind == anyserve_core::model::EventKind::Progress)
    );

    let mut cluster_job_streams = kernel_b
        .subscribe_cluster_job_streams(&job.job_id)
        .await
        .unwrap();
    let stream = kernel_a
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
    tokio::time::timeout(Duration::from_secs(2), cluster_job_streams.changed())
        .await
        .expect("cluster job stream notification should arrive")
        .unwrap();

    let kernel_b = Arc::new(kernel_b);
    let wait_handle = {
        let kernel_b = Arc::clone(&kernel_b);
        let stream_id = stream.stream_id.clone();
        tokio::spawn(async move {
            kernel_b
                .wait_for_frames(&stream_id, 0, Duration::from_secs(2))
                .await
                .unwrap()
        })
    };

    tokio::time::sleep(Duration::from_millis(50)).await;
    kernel_a
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

    let frames = wait_handle.await.unwrap();
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].payload, b"hello".to_vec());
}

#[tokio::test]
#[ignore = "requires docker-backed postgres + redis via ANYSERVE_TEST_POSTGRES_DSN and ANYSERVE_TEST_REDIS_URL"]
async fn postgres_claim_matches_priority_attributes_and_capacity() {
    let Some(postgres_dsn) = required_env("ANYSERVE_TEST_POSTGRES_DSN") else {
        eprintln!("skipping: ANYSERVE_TEST_POSTGRES_DSN is not set");
        return;
    };
    let Some(redis_url) = required_env("ANYSERVE_TEST_REDIS_URL") else {
        eprintln!("skipping: ANYSERVE_TEST_REDIS_URL is not set");
        return;
    };

    let suffix = Uuid::new_v4().simple().to_string();
    let interface_name = format!("demo.execute.v1.{suffix}");
    let worker_id = format!("worker-pg-dispatch-{suffix}");

    let kernel = Kernel::new(
        Arc::new(SqlStateStore::connect(&postgres_dsn).await.unwrap()),
        Arc::new(RedisFramePlane::new(&redis_url, 300).unwrap()),
        Arc::new(RedisClusterNotifier::new(&redis_url).unwrap()),
        Arc::new(BasicScheduler),
        30,
        30,
        25,
    );

    let attr_miss_job = kernel
        .submit_job(
            Some(format!("job-pg-attr-miss-{suffix}")),
            JobSpec {
                interface_name: interface_name.clone(),
                demand: anyserve_core::model::Demand {
                    required_attributes: BTreeMap::from([(
                        "accelerator.vendor".to_string(),
                        "amd".to_string(),
                    )]),
                    ..Default::default()
                },
                policy: ExecutionPolicy {
                    priority: 99,
                    ..ExecutionPolicy::default()
                },
                ..JobSpec::default()
            },
        )
        .await
        .unwrap();

    let capacity_miss_job = kernel
        .submit_job(
            Some(format!("job-pg-capacity-miss-{suffix}")),
            JobSpec {
                interface_name: interface_name.clone(),
                demand: anyserve_core::model::Demand {
                    required_capacity: BTreeMap::from([("slot".to_string(), 2)]),
                    ..Default::default()
                },
                policy: ExecutionPolicy {
                    priority: 98,
                    ..ExecutionPolicy::default()
                },
                ..JobSpec::default()
            },
        )
        .await
        .unwrap();

    let preferred_zero_job = kernel
        .submit_job(
            Some(format!("job-pg-preferred-zero-{suffix}")),
            JobSpec {
                interface_name: interface_name.clone(),
                demand: anyserve_core::model::Demand {
                    preferred_attributes: BTreeMap::from([(
                        "runtime".to_string(),
                        "python".to_string(),
                    )]),
                    required_capacity: BTreeMap::from([("slot".to_string(), 1)]),
                    ..Default::default()
                },
                policy: ExecutionPolicy {
                    priority: 5,
                    ..ExecutionPolicy::default()
                },
                ..JobSpec::default()
            },
        )
        .await
        .unwrap();

    let preferred_one_job = kernel
        .submit_job(
            Some(format!("job-pg-preferred-one-{suffix}")),
            JobSpec {
                interface_name: interface_name.clone(),
                demand: anyserve_core::model::Demand {
                    preferred_attributes: BTreeMap::from([(
                        "runtime".to_string(),
                        "demo".to_string(),
                    )]),
                    required_capacity: BTreeMap::from([("slot".to_string(), 1)]),
                    ..Default::default()
                },
                policy: ExecutionPolicy {
                    priority: 5,
                    ..ExecutionPolicy::default()
                },
                ..JobSpec::default()
            },
        )
        .await
        .unwrap();

    let high_priority_job = kernel
        .submit_job(
            Some(format!("job-pg-priority-{suffix}")),
            JobSpec {
                interface_name: interface_name.clone(),
                demand: anyserve_core::model::Demand {
                    required_capacity: BTreeMap::from([("slot".to_string(), 1)]),
                    ..Default::default()
                },
                policy: ExecutionPolicy {
                    priority: 10,
                    ..ExecutionPolicy::default()
                },
                ..JobSpec::default()
            },
        )
        .await
        .unwrap();

    let worker = kernel
        .register_worker(
            Some(worker_id.clone()),
            WorkerSpec {
                interfaces: BTreeSet::from([interface_name]),
                attributes: BTreeMap::from([
                    ("runtime".to_string(), "demo".to_string()),
                    ("accelerator.vendor".to_string(), "nvidia".to_string()),
                ]),
                total_capacity: BTreeMap::from([("slot".to_string(), 1)]),
                max_active_leases: 1,
                ..WorkerSpec::default()
            },
        )
        .await
        .unwrap();

    let first = kernel.poll_lease(&worker.worker_id).await.unwrap().unwrap();
    assert_eq!(first.job.job_id, high_priority_job.job_id);
    kernel
        .complete_lease(
            &worker_id,
            &first.lease.lease_id,
            Vec::new(),
            Attributes::new(),
        )
        .await
        .unwrap();

    let second = kernel.poll_lease(&worker.worker_id).await.unwrap().unwrap();
    assert_eq!(second.job.job_id, preferred_one_job.job_id);
    kernel
        .complete_lease(
            &worker_id,
            &second.lease.lease_id,
            Vec::new(),
            Attributes::new(),
        )
        .await
        .unwrap();

    let third = kernel.poll_lease(&worker.worker_id).await.unwrap().unwrap();
    assert_eq!(third.job.job_id, preferred_zero_job.job_id);

    let attr_miss = kernel.get_job(&attr_miss_job.job_id).await.unwrap();
    let capacity_miss = kernel.get_job(&capacity_miss_job.job_id).await.unwrap();
    assert_eq!(attr_miss.state, JobState::Pending);
    assert_eq!(capacity_miss.state, JobState::Pending);
}
