use std::fs;

use super::{
    DEFAULT_HEARTBEAT_TTL_SECS, DEFAULT_LEASE_TTL_SECS, DEFAULT_WATCH_POLL_INTERVAL_MS,
    FrameBackend, ResolvedServeConfig, ServeOverrides, StorageBackend,
};

fn temp_file_path(name: &str) -> std::path::PathBuf {
    let unique = format!(
        "anyserve-cli-{name}-{}-{}.toml",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time should be valid")
            .as_nanos()
    );
    std::env::temp_dir().join(unique)
}

#[test]
fn serve_config_uses_defaults_without_file() {
    let resolved = ResolvedServeConfig::load(ServeOverrides::default()).unwrap();
    assert_eq!(resolved.grpc.host, "0.0.0.0");
    assert_eq!(resolved.grpc.port, 50_052);
    assert!(!resolved.grpc.tls_enabled);
    assert!(resolved.openai.is_none());
    assert_eq!(resolved.heartbeat_ttl_secs, DEFAULT_HEARTBEAT_TTL_SECS);
    assert_eq!(resolved.default_lease_ttl_secs, DEFAULT_LEASE_TTL_SECS);
    assert_eq!(resolved.storage.backend, StorageBackend::Memory);
    assert_eq!(resolved.frames.backend, FrameBackend::Memory);
    assert_eq!(
        resolved.frames.watch_poll_interval_ms,
        DEFAULT_WATCH_POLL_INTERVAL_MS
    );
}

#[test]
fn serve_config_reads_values_from_toml() {
    let path = temp_file_path("server");
    fs::write(
        &path,
        r#"
[server]
grpc_host = "127.0.0.1"
grpc_port = 50062
heartbeat_ttl_secs = 45
default_lease_ttl_secs = 90

[server.tls]
enabled = true
cert_file = "/tmp/server.pem"
key_file = "/tmp/server.key"
"#,
    )
    .unwrap();

    let resolved = ResolvedServeConfig::load(ServeOverrides {
        config: Some(path.clone()),
        ..ServeOverrides::default()
    })
    .unwrap();

    fs::remove_file(path).unwrap();

    assert_eq!(resolved.grpc.host, "127.0.0.1");
    assert_eq!(resolved.grpc.port, 50062);
    assert!(resolved.grpc.tls_enabled);
    assert_eq!(resolved.grpc.cert_file.as_deref(), Some("/tmp/server.pem"));
    assert_eq!(resolved.grpc.key_file.as_deref(), Some("/tmp/server.key"));
    assert_eq!(resolved.heartbeat_ttl_secs, 45);
    assert_eq!(resolved.default_lease_ttl_secs, 90);
}

#[test]
fn serve_config_prefers_cli_over_file() {
    let path = temp_file_path("override");
    fs::write(
        &path,
        r#"
[server]
grpc_host = "127.0.0.1"
grpc_port = 50062
heartbeat_ttl_secs = 45
default_lease_ttl_secs = 90
"#,
    )
    .unwrap();

    let resolved = ResolvedServeConfig::load(ServeOverrides {
        config: Some(path.clone()),
        grpc_host: Some("0.0.0.0".to_string()),
        grpc_port: Some(50072),
        heartbeat_ttl_secs: Some(15),
        default_lease_ttl_secs: Some(25),
        ..ServeOverrides::default()
    })
    .unwrap();

    fs::remove_file(path).unwrap();

    assert_eq!(resolved.grpc.host, "0.0.0.0");
    assert_eq!(resolved.grpc.port, 50072);
    assert_eq!(resolved.heartbeat_ttl_secs, 15);
    assert_eq!(resolved.default_lease_ttl_secs, 25);
}

#[test]
fn serve_config_enables_openai_when_requested() {
    let path = temp_file_path("openai");
    fs::write(
        &path,
        r#"
[openai]
enabled = true
listen = "127.0.0.1:8081"
models = ["chat-fast", "embed-default"]
chat_interface = "llm.chat.v1"
embeddings_interface = "llm.embed.v1"
request_timeout_secs = 15

[openai.demand.required_attributes]
provider = "ollama"
"#,
    )
    .unwrap();

    let resolved = ResolvedServeConfig::load(ServeOverrides {
        config: Some(path.clone()),
        ..ServeOverrides::default()
    })
    .unwrap();

    fs::remove_file(path).unwrap();

    let openai = resolved.openai.expect("openai config should be enabled");
    assert_eq!(openai.listen, "127.0.0.1:8081");
    assert_eq!(openai.models.len(), 2);
    assert_eq!(openai.request_timeout_secs, 15);
    assert_eq!(openai.inline_request_limit_bytes, 64 * 1024);
    assert_eq!(
        openai
            .demand
            .required_attributes
            .get("provider")
            .map(String::as_str),
        Some("ollama")
    );
}

#[test]
fn postgres_storage_defaults_to_redis_frames() {
    let path = temp_file_path("postgres-redis");
    fs::write(
        &path,
        r#"
[server.storage]
backend = "postgres"
dsn = "postgres://example:anyserve@localhost/anyserve"

[server.frames]
redis_url = "redis://127.0.0.1:6379"
"#,
    )
    .unwrap();

    let resolved = ResolvedServeConfig::load(ServeOverrides {
        config: Some(path.clone()),
        ..ServeOverrides::default()
    })
    .unwrap();

    fs::remove_file(path).unwrap();

    assert_eq!(resolved.storage.backend, StorageBackend::Postgres);
    assert_eq!(resolved.frames.backend, FrameBackend::Redis);
    assert_eq!(
        resolved.frames.redis_url.as_deref(),
        Some("redis://127.0.0.1:6379")
    );
}

#[test]
fn postgres_storage_allows_explicit_memory_frames() {
    let path = temp_file_path("postgres-memory");
    fs::write(
        &path,
        r#"
[server.storage]
backend = "postgres"
dsn = "postgres://example:anyserve@localhost/anyserve"

[server.frames]
backend = "memory"
"#,
    )
    .unwrap();

    let resolved = ResolvedServeConfig::load(ServeOverrides {
        config: Some(path.clone()),
        ..ServeOverrides::default()
    })
    .unwrap();

    fs::remove_file(path).unwrap();

    assert_eq!(resolved.storage.backend, StorageBackend::Postgres);
    assert_eq!(resolved.frames.backend, FrameBackend::Memory);
    assert!(resolved.frames.redis_url.is_none());
}

#[test]
fn memory_storage_rejects_redis_frames() {
    let path = temp_file_path("memory-redis-invalid");
    fs::write(
        &path,
        r#"
[server.frames]
backend = "redis"
redis_url = "redis://127.0.0.1:6379"
"#,
    )
    .unwrap();

    let error = ResolvedServeConfig::load(ServeOverrides {
        config: Some(path.clone()),
        ..ServeOverrides::default()
    })
    .unwrap_err();

    fs::remove_file(path).unwrap();

    assert!(
        error
            .to_string()
            .contains("memory storage only supports frames.backend = 'memory'")
    );
}

#[test]
fn sqlite_storage_rejects_redis_frames() {
    let path = temp_file_path("sqlite-redis-invalid");
    fs::write(
        &path,
        r#"
[server.storage]
backend = "sqlite"
dsn = "sqlite:///tmp/anyserve.db"

[server.frames]
backend = "redis"
redis_url = "redis://127.0.0.1:6379"
"#,
    )
    .unwrap();

    let error = ResolvedServeConfig::load(ServeOverrides {
        config: Some(path.clone()),
        ..ServeOverrides::default()
    })
    .unwrap_err();

    fs::remove_file(path).unwrap();

    assert!(
        error
            .to_string()
            .contains("sqlite storage only supports frames.backend = 'memory'")
    );
}
