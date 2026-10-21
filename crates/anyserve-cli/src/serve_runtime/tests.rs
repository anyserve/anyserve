use std::collections::HashMap;
use std::time::Duration;

use anyserve_client::execution::{Attempt, AttemptClient, ExecutionClient, v2};
use anyserve_core::auth::AuthPolicy;
use anyserve_core::runtime::api::{JournalConfig, RuntimeConfig};

use crate::test_support::{Server, Worker, temporary_path};

fn spec(interface: &str, body: Vec<u8>, durable: bool) -> v2::JobSpec {
    v2::JobSpec {
        demand: Some(v2::Demand {
            interface: interface.to_owned(),
            ..Default::default()
        }),
        inputs: vec![v2::Object {
            body: Some(v2::object::Body::Inline(body.into())),
            metadata: HashMap::new(),
        }],
        policy: Some(v2::ExecutionPolicy {
            durability: (if durable {
                v2::Durability::Durable
            } else {
                v2::Durability::Ephemeral
            }) as i32,
            ..Default::default()
        }),
        ..Default::default()
    }
}
async fn client(server: &Server) -> ExecutionClient {
    ExecutionClient::connect_with_token(&server.endpoint, None)
        .await
        .unwrap()
}

#[tokio::test]
async fn large_input_and_result_use_the_attempt_data_connection() {
    let server = Server::start(RuntimeConfig::default(), AuthPolicy::default()).await;
    let mut client = client(&server).await;
    let mut worker = Worker::connect(&server, "large", vec!["test.large".into()], 1).await;
    let payload = vec![42; 5 * 1024 * 1024];
    let job = client
        .submit(None, spec("test.large", payload.clone(), false))
        .await
        .unwrap()
        .job
        .unwrap();
    let assignment = worker.assignment().await;
    assert!(assignment.inputs_via_attempt);
    assert!(assignment.inline_inputs.is_empty());
    let mut attempt = Attempt::open(
        &mut worker.session.data,
        &assignment,
        &worker.limits,
        worker.heartbeat_timeout,
    )
    .await
    .unwrap();
    assert_eq!(
        attempt.inputs[0].body,
        Some(v2::object::Body::Inline(payload.clone().into()))
    );
    let output = v2::Object {
        body: Some(v2::object::Body::Inline(payload.into())),
        metadata: HashMap::new(),
    };
    attempt
        .commit(
            &mut worker.session.data,
            &assignment,
            &worker.limits,
            std::slice::from_ref(&output),
            v2::finish::Outcome::Succeeded(v2::Succeeded {}),
        )
        .await
        .unwrap();
    assert_eq!(
        client.get(job.job_id, true).await.unwrap().outputs,
        vec![output]
    );
    server.stop().await;
}

#[tokio::test]
async fn durable_acceptance_and_completion_receipt_survive_restart() {
    let path = temporary_path("journal.sqlite");
    let config = RuntimeConfig {
        journal: Some(JournalConfig::new(&path)),
        ..Default::default()
    };
    let server = Server::start(config.clone(), AuthPolicy::default()).await;
    let mut first = client(&server).await;
    let request = spec("test.durable", b"input".to_vec(), true);
    let accepted = first
        .submit(Some("stable-id".into()), request.clone())
        .await
        .unwrap();
    assert!(accepted.created);
    let duplicate = first
        .submit(Some("stable-id".into()), request.clone())
        .await
        .unwrap();
    assert!(!duplicate.created);
    assert_eq!(
        first
            .submit(
                Some("stable-id".into()),
                spec("test.durable", b"different".to_vec(), true)
            )
            .await
            .unwrap_err()
            .code(),
        tonic::Code::AlreadyExists
    );
    server.stop().await;

    let server = Server::start(config.clone(), AuthPolicy::default()).await;
    let mut current = client(&server).await;
    assert_eq!(
        current.get("stable-id", true).await.unwrap().state(),
        v2::JobState::Queued
    );
    let mut worker = Worker::connect(&server, "durable", vec!["test.durable".into()], 1).await;
    let assignment = worker.assignment().await;
    let output = v2::Object {
        body: Some(v2::object::Body::Inline(b"result".to_vec().into())),
        metadata: HashMap::new(),
    };
    let mut attempt = Attempt::open(
        &mut worker.session.data,
        &assignment,
        &worker.limits,
        worker.heartbeat_timeout,
    )
    .await
    .unwrap();
    attempt
        .finish(
            std::slice::from_ref(&output),
            v2::finish::Outcome::Succeeded(v2::Succeeded {}),
        )
        .await
        .unwrap();
    assert_eq!(
        current.get("stable-id", true).await.unwrap().state(),
        v2::JobState::Succeeded
    );
    drop(worker);
    server.stop().await;

    let server = Server::start(config, AuthPolicy::default()).await;
    let mut data = AttemptClient::connect(&server.endpoint, None)
        .await
        .unwrap();
    // Simulate the worker missing the original completion acknowledgement, then
    // reconnecting after the control plane has changed generation.
    let limits = v2::Limits {
        max_control_message_bytes: 256 * 1024,
        max_chunk_bytes: 64 * 1024,
        attempt_window_bytes: 4 * 1024 * 1024,
        assign_inline_limit: 64 * 1024,
    };
    let mut replay = Attempt::open(&mut data, &assignment, &limits, Duration::from_secs(15))
        .await
        .unwrap();
    replay
        .finish(
            std::slice::from_ref(&output),
            v2::finish::Outcome::Succeeded(v2::Succeeded {}),
        )
        .await
        .unwrap();
    let mut conflicting = Attempt::open(&mut data, &assignment, &limits, Duration::from_secs(15))
        .await
        .unwrap();
    let changed = v2::Object {
        body: Some(v2::object::Body::Inline(
            b"conflicting result".to_vec().into(),
        )),
        metadata: HashMap::new(),
    };
    let error = conflicting
        .finish(&[changed], v2::finish::Outcome::Succeeded(v2::Succeeded {}))
        .await
        .unwrap_err();
    assert!(
        matches!(
            error.code(),
            tonic::Code::AlreadyExists | tonic::Code::Aborted
        ),
        "unexpected conflict status: {error}"
    );
    let mut current = client(&server).await;
    assert!(
        !current
            .submit(Some("stable-id".into()), request)
            .await
            .unwrap()
            .created
    );
    server.stop().await;
    for suffix in ["", "-wal", "-shm", ".lock"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
    }
}

#[tokio::test]
async fn invoke_stream_input_has_an_explicit_input_end_and_terminal_event() {
    let server = Server::start(RuntimeConfig::default(), AuthPolicy::default()).await;
    let mut client = client(&server).await;
    let mut worker = Worker::connect(&server, "stream", vec!["test.stream".into()], 1).await;
    let mut request = spec("test.stream", Vec::new(), false);
    request.inputs.clear();
    let invocation = client.invoke_stream(request).await.unwrap();
    let assignment = worker.assignment().await;
    invocation.send(b"first".to_vec()).await.unwrap();
    invocation.send(b"second".to_vec()).await.unwrap();
    invocation.finish_input().await.unwrap();
    let mut attempt = Attempt::open(
        &mut worker.session.data,
        &assignment,
        &worker.limits,
        worker.heartbeat_timeout,
    )
    .await
    .unwrap();
    let bytes = attempt
        .inputs
        .iter()
        .flat_map(|object| match &object.body {
            Some(v2::object::Body::Inline(bytes)) => bytes.to_vec(),
            _ => Vec::new(),
        })
        .collect::<Vec<_>>();
    assert_eq!(bytes, b"firstsecond");
    attempt
        .frame(b"answer".to_vec().into(), HashMap::new(), false)
        .await
        .unwrap();
    attempt
        .finish(&[], v2::finish::Outcome::Succeeded(v2::Succeeded {}))
        .await
        .unwrap();
    let mut events = invocation.events;
    let mut output = Vec::new();
    let mut terminal = false;
    while let Some(event) = events.message().await.unwrap() {
        match event.event.unwrap() {
            v2::execution_event::Event::Output(frame) => output.extend_from_slice(&frame.data),
            v2::execution_event::Event::Finished(finish) => {
                terminal = true;
                assert_eq!(finish.state, v2::JobState::Succeeded as i32);
            }
            _ => {}
        }
    }
    assert!(terminal);
    assert_eq!(output, b"answer");
    server.stop().await;
}

#[tokio::test]
async fn new_services_enforce_token_roles_over_real_connections() {
    let auth = AuthPolicy::new(["client".to_owned()], ["worker".to_owned()]);
    let server = Server::start(RuntimeConfig::default(), auth).await;
    let mut unauthenticated = ExecutionClient::connect_with_token(&server.endpoint, None)
        .await
        .unwrap();
    assert_eq!(
        unauthenticated
            .get("absent", false)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unauthenticated
    );
    let mut worker_client = ExecutionClient::connect_with_token(&server.endpoint, Some("worker"))
        .await
        .unwrap();
    assert_eq!(
        worker_client
            .submit(None, spec("test.auth", Vec::new(), false))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
    let denied = anyserve_client::execution::WorkerSession::connect(
        &server.endpoint,
        Some("client"),
        v2::Hello {
            worker_id: "forged".into(),
            max_concurrency: 1,
            interfaces: vec!["test.auth".into()],
            ..Default::default()
        },
    )
    .await;
    let error = denied.err().unwrap();
    assert_eq!(
        error.downcast_ref::<tonic::Status>().unwrap().code(),
        tonic::Code::PermissionDenied
    );
    let mut allowed = ExecutionClient::connect_with_token(&server.endpoint, Some("client"))
        .await
        .unwrap();
    assert_eq!(
        allowed.get("absent", false).await.unwrap_err().code(),
        tonic::Code::NotFound
    );
    server.stop().await;
}

#[tokio::test]
async fn queued_constraints_cannot_starve_a_compatible_job_beyond_the_scan_batch() {
    let server = Server::start(RuntimeConfig::default(), AuthPolicy::default()).await;
    let mut client = client(&server).await;
    for _ in 0..300 {
        let mut request = spec("test.fair", Vec::new(), false);
        request
            .demand
            .as_mut()
            .unwrap()
            .required_attributes
            .insert("accelerator".into(), "unavailable".into());
        request.policy.as_mut().unwrap().priority = 3;
        client.submit(None, request).await.unwrap();
    }
    let expected = client
        .submit(None, spec("test.fair", b"eligible".to_vec(), false))
        .await
        .unwrap()
        .job
        .unwrap();
    let mut worker = Worker::connect(&server, "fair", vec!["test.fair".into()], 1).await;
    assert_eq!(worker.assignment().await.job_id, expected.job_id);
    server.stop().await;
}

#[tokio::test]
async fn durable_acceptance_in_flight_consumes_queue_capacity() {
    let path = temporary_path("admission.sqlite");
    let server = Server::start(
        RuntimeConfig {
            default_max_queue: 1,
            journal: Some(JournalConfig::new(&path)),
            ..Default::default()
        },
        AuthPolicy::default(),
    )
    .await;
    let client = client(&server).await;
    let futures = (0..20).map(|_| {
        let mut client = client.clone();
        async move {
            client
                .submit(None, spec("test.admission", Vec::new(), true))
                .await
        }
    });
    let outcomes = futures::future::join_all(futures).await;
    assert_eq!(outcomes.iter().filter(|result| result.is_ok()).count(), 1);
    for error in outcomes.into_iter().filter_map(Result::err) {
        assert_eq!(error.code(), tonic::Code::ResourceExhausted);
    }
    server.stop().await;
    for suffix in ["", "-wal", "-shm", ".lock"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
    }
}

#[tokio::test]
async fn a_retired_attempts_buffered_output_cannot_be_spliced_into_a_retry() {
    use anyserve_core::runtime::api::{EventKind, InvokeRequest, JobState};
    use futures::StreamExt;
    let server = Server::start(RuntimeConfig::default(), AuthPolicy::default()).await;
    let mut worker = Worker::connect(&server, "exposure", vec!["test.exposure".into()], 1).await;
    // Intentionally leave the Execution unpolled until after Frame and Fail.
    // This reproduces the exposure/publication race without relying on timing.
    let mut execution = server
        .runtime
        .invoke(InvokeRequest {
            spec: anyserve_core::client_service::spec_from_proto(spec(
                "test.exposure",
                Vec::new(),
                false,
            ))
            .unwrap(),
            ..Default::default()
        })
        .await
        .unwrap();
    let assignment = worker.assignment().await;
    let mut attempt = Attempt::open(
        &mut worker.session.data,
        &assignment,
        &worker.limits,
        worker.heartbeat_timeout,
    )
    .await
    .unwrap();
    attempt
        .frame(b"partial".to_vec().into(), HashMap::new(), false)
        .await
        .unwrap();
    attempt
        .finish(
            &[],
            v2::finish::Outcome::Failed(v2::ErrorInfo {
                code: "worker_lost".into(),
                message: "lost after output".into(),
                retryable: true,
            }),
        )
        .await
        .unwrap();
    let mut output = Vec::new();
    let state = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while let Some(event) = execution.next().await {
            match event.unwrap().kind {
                EventKind::Output {
                    data,
                    progress: false,
                    ..
                } => output.extend_from_slice(&data),
                EventKind::AttemptRestarted { .. } => {
                    assert!(
                        output.is_empty(),
                        "output from a retired attempt was exposed before retry"
                    );
                    worker.credit(1).await;
                    let assignment = worker.assignment().await;
                    let mut attempt = Attempt::open(
                        &mut worker.session.data,
                        &assignment,
                        &worker.limits,
                        worker.heartbeat_timeout,
                    )
                    .await
                    .unwrap();
                    attempt
                        .frame(b"answer".to_vec().into(), HashMap::new(), false)
                        .await
                        .unwrap();
                    attempt
                        .finish(&[], v2::finish::Outcome::Succeeded(v2::Succeeded {}))
                        .await
                        .unwrap();
                }
                EventKind::Finished { state, .. } => return state,
                _ => {}
            }
        }
        panic!("missing terminal event")
    })
    .await
    .unwrap();
    match state {
        JobState::Succeeded => assert_eq!(output, b"answer"),
        JobState::Failed => assert!(output.is_empty() || output == b"partial"),
        _ => panic!("unexpected terminal state"),
    }
    server.stop().await;
}

#[tokio::test]
async fn reopening_an_attempt_continues_after_the_last_acknowledged_frame() {
    use anyserve_core::runtime::api::EventKind;
    use futures::StreamExt;
    let server = Server::start(RuntimeConfig::default(), AuthPolicy::default()).await;
    let mut worker = Worker::connect(&server, "resume-frames", vec!["test.resume".into()], 1).await;
    let mut client = client(&server).await;
    let job = client
        .submit(None, spec("test.resume", b"input".to_vec(), false))
        .await
        .unwrap()
        .job
        .unwrap();
    let assignment = worker.assignment().await;
    let mut attempt = Attempt::open(
        &mut worker.session.data,
        &assignment,
        &worker.limits,
        worker.heartbeat_timeout,
    )
    .await
    .unwrap();
    let mut events = server.runtime.watch(&job.job_id, 0).unwrap();
    attempt
        .frame(b"first".as_slice().into(), HashMap::new(), false)
        .await
        .unwrap();
    let first = tokio::time::timeout(Duration::from_secs(2), async {
        while let Some(event) = events.next().await {
            if let EventKind::Output { data, .. } = event.unwrap().kind {
                return data;
            }
        }
        panic!("missing first frame")
    })
    .await
    .unwrap();
    drop(attempt);
    let mut reopened = Attempt::open(
        &mut worker.session.data,
        &assignment,
        &worker.limits,
        worker.heartbeat_timeout,
    )
    .await
    .unwrap();
    reopened
        .frame(b"second".as_slice().into(), HashMap::new(), false)
        .await
        .unwrap();
    reopened
        .finish(&[], v2::finish::Outcome::Succeeded(v2::Succeeded {}))
        .await
        .unwrap();
    let mut frames = vec![first];
    tokio::time::timeout(Duration::from_secs(2), async {
        while let Some(event) = events.next().await {
            match event.unwrap().kind {
                EventKind::Output { data, .. } => frames.push(data),
                EventKind::Finished { state, .. } => {
                    assert_eq!(state, anyserve_core::runtime::api::JobState::Succeeded);
                    return;
                }
                _ => {}
            }
        }
        panic!("missing terminal event")
    })
    .await
    .unwrap();
    assert_eq!(frames, [b"first".as_slice(), b"second".as_slice()]);
    server.stop().await;
}
