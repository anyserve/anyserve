use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use anyserve_core::auth::AuthPolicy;
use anyserve_core::runtime::api::{Demand, RuntimeConfig};
use axum::body::Body;
use axum::response::IntoResponse;
use futures::StreamExt;

use super::router;
use crate::serve_config::ServeOpenAIConfig;
use crate::test_support::{Backend, BuiltinWorker, Server};

fn config() -> ServeOpenAIConfig {
    ServeOpenAIConfig {
        listen: "127.0.0.1:0".into(),
        models: vec!["demo".into()],
        chat_interface: "llm.chat.v1".into(),
        embeddings_interface: "llm.embed.v1".into(),
        request_timeout_secs: 5,
        demand: Demand::default(),
        policy: Default::default(),
    }
}
const WORKER: &str = "kind='llm'\nmax_active_leases=2\n[routes]\n'llm.chat.v1'='/chat'\n";

async fn upstream(done: bool, calls: Arc<AtomicUsize>) -> Backend {
    Backend::start(axum::Router::new().route("/chat", axum::routing::post(move |body: axum::Json<serde_json::Value>| {
        calls.fetch_add(1, Ordering::SeqCst);
        async move {
            if body.get("stream").and_then(|value| value.as_bool()) != Some(true) {
                return axum::Json(serde_json::json!({"choices":[{"message":{"content":"Hello"}}]})).into_response();
            }
            let stream = async_stream::stream! {
                yield Ok::<_, std::convert::Infallible>("data: {\"choices\":[{\"delta\":{\"content\":\"Hel\"}}]}\n\n");
                tokio::time::sleep(Duration::from_millis(40)).await;
                yield Ok("data: {\"choices\":[{\"delta\":{\"content\":\"lo\"}}]}\n\n");
                if done { yield Ok("data: [DONE]\n\n"); }
            };
            ([("content-type", "text/event-stream")], Body::from_stream(stream)).into_response()
        }
    }))).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn openai_streams_before_completion_and_non_streaming_returns_json() {
    let server = Server::start(RuntimeConfig::default(), AuthPolicy::default()).await;
    let calls = Arc::new(AtomicUsize::new(0));
    let backend = upstream(true, calls.clone()).await;
    let worker = BuiltinWorker::start(&server, &backend, WORKER).await;
    let gateway = Backend::start(router(
        server.runtime.clone(),
        config(),
        Arc::new(AuthPolicy::default()),
    ))
    .await;
    let client = reqwest::Client::new();
    let response = client
        .post(format!("{}/v1/chat/completions", gateway.url))
        .json(&serde_json::json!({"model":"demo", "stream":true, "messages":[]}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.headers()["content-type"], "text/event-stream");
    let mut stream = response.bytes_stream();
    let first = stream.next().await.unwrap().unwrap();
    assert!(String::from_utf8_lossy(&first).contains("Hel"));
    assert!(!String::from_utf8_lossy(&first).contains("[DONE]"));
    let mut bytes = first.to_vec();
    while let Some(chunk) = stream.next().await {
        bytes.extend_from_slice(&chunk.unwrap());
    }
    assert!(String::from_utf8_lossy(&bytes).contains("[DONE]"));
    let response: serde_json::Value = client
        .post(format!("{}/v1/chat/completions", gateway.url))
        .json(&serde_json::json!({"model":"demo", "messages":[]}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(response["choices"][0]["message"]["content"], "Hello");
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    drop(worker);
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn truncated_sse_is_an_abnormal_response_and_never_switches_attempts() {
    let server = Server::start(RuntimeConfig::default(), AuthPolicy::default()).await;
    let calls = Arc::new(AtomicUsize::new(0));
    let backend = upstream(false, calls.clone()).await;
    let worker = BuiltinWorker::start(&server, &backend, WORKER).await;
    let gateway = Backend::start(router(
        server.runtime.clone(),
        config(),
        Arc::new(AuthPolicy::default()),
    ))
    .await;
    let response = reqwest::Client::new()
        .post(format!("{}/v1/chat/completions", gateway.url))
        .json(&serde_json::json!({"model":"demo", "stream":true, "messages":[]}))
        .send()
        .await;
    if let Ok(response) = response {
        let mut stream = response.bytes_stream();
        let mut abnormal = false;
        while let Some(chunk) = stream.next().await {
            if chunk.is_err() {
                abnormal = true;
                break;
            }
        }
        assert!(
            abnormal,
            "truncated output was reported as a normal HTTP EOF"
        );
    }
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "an exposed stream must not be retried"
    );
    drop(worker);
    server.stop().await;
}

#[tokio::test]
async fn failures_before_the_first_output_use_json_status_for_both_response_modes() {
    use axum::http::StatusCode;
    for exhausted in [false, true] {
        let server = Server::start(
            RuntimeConfig {
                max_pending_jobs: if exhausted { 0 } else { 100 },
                ..Default::default()
            },
            AuthPolicy::default(),
        )
        .await;
        let gateway = Backend::start(router(
            server.runtime.clone(),
            config(),
            Arc::new(AuthPolicy::default()),
        ))
        .await;
        for stream in [false, true] {
            let response = reqwest::Client::new()
                .post(format!("{}/v1/chat/completions", gateway.url))
                .header("x-anyserve-queue-timeout-ms", "30")
                .json(&serde_json::json!({"model":"demo", "stream":stream, "messages":[]}))
                .send()
                .await
                .unwrap();
            assert_eq!(
                response.status(),
                if exhausted {
                    StatusCode::TOO_MANY_REQUESTS
                } else {
                    StatusCode::SERVICE_UNAVAILABLE
                }
            );
            assert!(
                response.headers()["content-type"]
                    .to_str()
                    .unwrap()
                    .starts_with("application/json")
            );
            let body: serde_json::Value = response.json().await.unwrap();
            assert!(body["error"]["message"].is_string());
        }
        server.stop().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_deadline_before_the_first_output_returns_json_504() {
    let server = Server::start(RuntimeConfig::default(), AuthPolicy::default()).await;
    let backend = Backend::start(axum::Router::new().route(
        "/chat",
        axum::routing::post(|| async {
            tokio::time::sleep(Duration::from_secs(1)).await;
            axum::Json(serde_json::json!({"choices":[]}))
        }),
    ))
    .await;
    let worker = BuiltinWorker::start(&server, &backend, WORKER).await;
    let gateway = Backend::start(router(
        server.runtime.clone(),
        config(),
        Arc::new(AuthPolicy::default()),
    ))
    .await;
    for stream in [false, true] {
        let response = reqwest::Client::new()
            .post(format!("{}/v1/chat/completions", gateway.url))
            .header("x-anyserve-timeout-ms", "80")
            .json(&serde_json::json!({"model":"demo", "stream":stream, "messages":[]}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::GATEWAY_TIMEOUT);
        let body: serde_json::Value = response.json().await.unwrap();
        assert!(body["error"]["message"].is_string());
    }
    drop(worker);
    server.stop().await;
}

#[tokio::test]
async fn readiness_requires_a_ready_runtime_and_chat_worker_but_not_embeddings_or_idle_credit() {
    use crate::test_support::Worker;
    use anyserve_client::execution::v2;
    let server = Server::start(RuntimeConfig::default(), AuthPolicy::default()).await;
    let worker = Worker::connect(&server, "chat-only", vec!["llm.chat.v1".into()], 1).await;
    worker.credit(0).await;
    tokio::time::timeout(Duration::from_secs(2), async {
        while server.runtime.list_workers()[0].free_slots != 0 {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    let gateway = Backend::start(router(
        server.runtime.clone(),
        config(),
        Arc::new(AuthPolicy::default()),
    ))
    .await;
    let response = reqwest::get(format!("{}/readyz", gateway.url))
        .await
        .unwrap();
    assert_eq!(response.status(), axum::http::StatusCode::OK);
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["chat_ready"], true);
    assert_eq!(body["embeddings_ready"], false);
    worker
        .session
        .control
        .send(v2::WorkerMessage {
            msg: Some(v2::worker_message::Msg::Draining(v2::Draining {})),
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while !server.runtime.list_workers()[0].draining {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        reqwest::get(format!("{}/readyz", gateway.url))
            .await
            .unwrap()
            .status(),
        axum::http::StatusCode::SERVICE_UNAVAILABLE
    );
    let _second = Worker::connect(&server, "chat-again", vec!["llm.chat.v1".into()], 1).await;
    server.runtime.shutdown().await;
    assert_eq!(
        reqwest::get(format!("{}/readyz", gateway.url))
            .await
            .unwrap()
            .status(),
        axum::http::StatusCode::SERVICE_UNAVAILABLE
    );
}
