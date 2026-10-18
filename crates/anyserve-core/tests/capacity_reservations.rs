use std::collections::BTreeMap;
use std::sync::Arc;

use anyserve_core::frame::MemoryFramePlane;
use anyserve_core::kernel::Kernel;
use anyserve_core::model::{Attributes, Demand, JobSpec, WorkerSpec};
use anyserve_core::notify::NoopClusterNotifier;
use anyserve_core::scheduler::BasicScheduler;
use anyserve_core::sql_store::SqlStateStore;
use anyserve_core::store::{MemoryStateStore, StateStore};
use uuid::Uuid;

fn kernel(store: Arc<dyn StateStore>) -> Kernel {
    Kernel::new(
        store,
        Arc::new(MemoryFramePlane::new()),
        Arc::new(NoopClusterNotifier),
        Arc::new(BasicScheduler),
        30,
        30,
        25,
    )
}

async fn check_reservations(store: Arc<dyn StateStore>) {
    let a = kernel(store.clone());
    let b = kernel(store.clone());
    for release in ["complete", "fail", "cancel", "expire"] {
        let interface = format!("capacity.{}", Uuid::new_v4());
        let worker = a
            .register_worker(
                None,
                WorkerSpec {
                    interfaces: [interface.clone()].into(),
                    total_capacity: BTreeMap::from([("slot".into(), 2)]),
                    max_active_leases: 2,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        for _ in 0..2 {
            a.submit_job(
                None,
                JobSpec {
                    interface_name: interface.clone(),
                    demand: Demand {
                        required_capacity: BTreeMap::from([("slot".into(), 2)]),
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        }

        let (first, second) = tokio::join!(
            a.poll_lease(&worker.worker_id),
            b.poll_lease(&worker.worker_id)
        );
        let grants: Vec<_> = [first.unwrap(), second.unwrap()]
            .into_iter()
            .flatten()
            .collect();
        assert_eq!(grants.len(), 1, "capacity must be reserved atomically");
        let grant = &grants[0];

        // A stale heartbeat advertising the full capacity cannot erase leases.
        a.heartbeat_worker(
            &worker.worker_id,
            worker.spec.total_capacity.clone(),
            0,
            Attributes::new(),
        )
        .await
        .unwrap();
        assert!(b.poll_lease(&worker.worker_id).await.unwrap().is_none());
        match release {
            "complete" => a
                .complete_lease(
                    &worker.worker_id,
                    &grant.lease.lease_id,
                    Vec::new(),
                    Attributes::new(),
                )
                .await
                .unwrap(),
            "fail" => a
                .fail_lease(
                    &worker.worker_id,
                    &grant.lease.lease_id,
                    "test failure".into(),
                    false,
                    Attributes::new(),
                )
                .await
                .unwrap(),
            "cancel" => {
                a.cancel_job(&grant.job.job_id).await.unwrap();
            }
            "expire" => {
                store
                    .reap_expired_leases_transition(grant.lease.expires_at_ms + 1)
                    .await
                    .unwrap();
            }
            _ => unreachable!(),
        }
        assert!(
            b.poll_lease(&worker.worker_id).await.unwrap().is_some(),
            "{release} must release capacity"
        );
    }

    let interface = format!("capacity.{}", Uuid::new_v4());
    let worker = a
        .register_worker(
            None,
            WorkerSpec {
                interfaces: [interface.clone()].into(),
                total_capacity: BTreeMap::from([("slot".into(), 4)]),
                max_active_leases: 2,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    for _ in 0..2 {
        a.submit_job(
            None,
            JobSpec {
                interface_name: interface.clone(),
                demand: Demand {
                    required_capacity: BTreeMap::from([("slot".into(), 2)]),
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await
        .unwrap();
    }
    assert!(a.poll_lease(&worker.worker_id).await.unwrap().is_some());
    a.heartbeat_worker(
        &worker.worker_id,
        BTreeMap::from([("slot".into(), 2)]),
        1,
        Attributes::new(),
    )
    .await
    .unwrap();
    assert!(
        b.poll_lease(&worker.worker_id).await.unwrap().is_some(),
        "reported usage must not be counted twice"
    );
}

#[tokio::test]
async fn memory_reserves_capacity_until_leases_are_released() {
    check_reservations(Arc::new(MemoryStateStore::new())).await;
}

#[tokio::test]
async fn sqlite_reserves_capacity_until_leases_are_released() {
    check_reservations(Arc::new(
        SqlStateStore::connect("sqlite::memory:").await.unwrap(),
    ))
    .await;
}

#[tokio::test]
#[ignore = "requires ANYSERVE_TEST_POSTGRES_DSN"]
async fn postgres_reserves_capacity_until_leases_are_released() {
    let dsn = std::env::var("ANYSERVE_TEST_POSTGRES_DSN")
        .expect("ANYSERVE_TEST_POSTGRES_DSN is required");
    check_reservations(Arc::new(SqlStateStore::connect(&dsn).await.unwrap())).await;
}
