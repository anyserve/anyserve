use anyhow::{Context, Result};
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue};

use super::config::WorkerConfig;

pub(crate) fn build_http_client(config: &WorkerConfig) -> Result<reqwest::Client> {
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    if let Some(api_key) = config.api_key.as_ref() {
        let header = HeaderValue::from_str(&format!("Bearer {api_key}"))
            .context("build authorization header")?;
        headers.insert(AUTHORIZATION, header);
    }

    reqwest::Client::builder()
        .default_headers(headers)
        .tcp_nodelay(true)
        .pool_max_idle_per_host(config.max_active_leases as usize)
        .connect_timeout(config.upstream_timeout)
        .read_timeout(config.upstream_timeout)
        .build()
        .context("build HTTP client")
}

pub(crate) fn truncate(value: &str, limit: usize) -> String {
    let chars = value.chars().collect::<Vec<_>>();
    if chars.len() <= limit {
        return value.to_string();
    }
    chars.into_iter().take(limit).collect::<String>()
}

#[cfg(test)]
mod tests {
    #[test]
    fn truncate_keeps_short_values() {
        assert_eq!(super::truncate("hello", 10), "hello");
    }
    #[tokio::test]
    async fn upstream_timeout_limits_inactivity_instead_of_stream_lifetime() {
        use crate::test_support::{Backend, temporary_path};
        use crate::worker::config::{WorkerArgs, load_config};
        use axum::body::Body;
        use std::time::Duration;

        let backend = Backend::start(
            axum::Router::new()
                .route(
                    "/active",
                    axum::routing::get(|| async {
                        Body::from_stream(async_stream::stream! {
                            for _ in 0..5 {
                                yield Ok::<_, std::convert::Infallible>("x");
                                tokio::time::sleep(Duration::from_millis(100)).await;
                            }
                        })
                    }),
                )
                .route(
                    "/idle",
                    axum::routing::get(|| async {
                        Body::from_stream(async_stream::stream! {
                            yield Ok::<_, std::convert::Infallible>("x");
                            std::future::pending::<()>().await;
                        })
                    }),
                ),
        )
        .await;
        let path = temporary_path("worker-read-timeout.toml");
        std::fs::write(&path, format!("base_url={:?}", backend.url)).unwrap();
        let mut config = load_config(&WorkerArgs {
            config: path.clone(),
            worker_id: None,
        })
        .unwrap();
        std::fs::remove_file(path).unwrap();
        config.upstream_timeout = Duration::from_millis(300);
        let client = super::build_http_client(&config).unwrap();
        let started = tokio::time::Instant::now();
        let bytes = client
            .get(format!("{}/active", backend.url))
            .send()
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(bytes.as_ref(), b"xxxxx");
        assert!(started.elapsed() > config.upstream_timeout);
        assert!(
            client
                .get(format!("{}/idle", backend.url))
                .send()
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap_err()
                .is_timeout()
        );
    }
}
