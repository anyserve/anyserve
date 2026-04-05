use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use anyserve_core::config::GrpcConfig;
use anyserve_core::model::{Attributes, Capacity, Demand};
use serde::Deserialize;

pub const DEFAULT_HEARTBEAT_TTL_SECS: u64 = 30;
pub const DEFAULT_LEASE_TTL_SECS: u64 = 30;
pub const DEFAULT_WATCH_POLL_INTERVAL_MS: u64 = 250;
pub const DEFAULT_CLOSED_RETENTION_SECS: u64 = 300;
const DEFAULT_OPENAI_LISTEN: &str = "0.0.0.0:8080";
const DEFAULT_CHAT_INTERFACE: &str = "llm.chat.v1";
const DEFAULT_EMBEDDINGS_INTERFACE: &str = "llm.embed.v1";
const DEFAULT_OPENAI_REQUEST_TIMEOUT_SECS: u64 = 60;
const DEFAULT_OPENAI_INLINE_REQUEST_LIMIT_BYTES: usize = 64 * 1024;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ServeOverrides {
    pub config: Option<PathBuf>,
    pub grpc_host: Option<String>,
    pub grpc_port: Option<u16>,
    pub grpc_tls_enabled: Option<bool>,
    pub grpc_cert_file: Option<String>,
    pub grpc_key_file: Option<String>,
    pub heartbeat_ttl_secs: Option<u64>,
    pub default_lease_ttl_secs: Option<u64>,
}

#[derive(Clone, Debug)]
pub struct ResolvedServeConfig {
    pub config_path: Option<PathBuf>,
    pub grpc: GrpcConfig,
    pub heartbeat_ttl_secs: u64,
    pub default_lease_ttl_secs: u64,
    pub storage: StorageConfig,
    pub frames: FrameConfig,
    pub openai: Option<ServeOpenAIConfig>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StorageBackend {
    Memory,
    Sqlite,
    Postgres,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StorageConfig {
    pub backend: StorageBackend,
    pub dsn: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FrameBackend {
    Memory,
    Redis,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FrameConfig {
    pub backend: FrameBackend,
    pub redis_url: Option<String>,
    pub closed_retention_secs: u64,
    pub watch_poll_interval_ms: u64,
}

#[derive(Clone, Debug)]
pub struct ServeOpenAIConfig {
    pub listen: String,
    pub models: Vec<String>,
    pub chat_interface: String,
    pub embeddings_interface: String,
    pub request_timeout_secs: u64,
    pub inline_request_limit_bytes: usize,
    pub demand: Demand,
}

impl ResolvedServeConfig {
    pub fn load(overrides: ServeOverrides) -> Result<Self> {
        let file_path = overrides.config.clone();
        let file_config = if let Some(path) = file_path.as_ref() {
            Some(AnyserveConfigFile::load(path)?)
        } else {
            None
        };

        let server = file_config
            .as_ref()
            .and_then(|config| config.server.as_ref());
        let tls = server.and_then(|server| server.tls.as_ref());
        let storage = server
            .and_then(|server| server.storage.as_ref())
            .cloned()
            .unwrap_or_default()
            .resolve()?;
        let frames = server
            .and_then(|server| server.frames.as_ref())
            .cloned()
            .unwrap_or_default()
            .resolve(storage.backend)?;
        let grpc = GrpcConfig {
            host: overrides
                .grpc_host
                .or_else(|| server.and_then(|server| server.grpc_host.clone()))
                .unwrap_or_else(|| GrpcConfig::default().host),
            port: overrides
                .grpc_port
                .or_else(|| server.and_then(|server| server.grpc_port))
                .unwrap_or(GrpcConfig::default().port),
            tls_enabled: overrides
                .grpc_tls_enabled
                .or_else(|| tls.and_then(|tls| tls.enabled))
                .unwrap_or(GrpcConfig::default().tls_enabled),
            cert_file: overrides
                .grpc_cert_file
                .or_else(|| tls.and_then(|tls| tls.cert_file.clone())),
            key_file: overrides
                .grpc_key_file
                .or_else(|| tls.and_then(|tls| tls.key_file.clone())),
        };

        Ok(Self {
            config_path: file_path,
            grpc,
            heartbeat_ttl_secs: overrides
                .heartbeat_ttl_secs
                .or_else(|| server.and_then(|server| server.heartbeat_ttl_secs))
                .unwrap_or(DEFAULT_HEARTBEAT_TTL_SECS),
            default_lease_ttl_secs: overrides
                .default_lease_ttl_secs
                .or_else(|| server.and_then(|server| server.default_lease_ttl_secs))
                .unwrap_or(DEFAULT_LEASE_TTL_SECS),
            storage,
            frames,
            openai: file_config
                .as_ref()
                .and_then(|config| config.openai.as_ref())
                .and_then(ServeOpenAIConfigFile::resolve),
        })
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct AnyserveConfigFile {
    #[serde(default)]
    server: Option<ServerConfigFile>,
    #[serde(default)]
    openai: Option<ServeOpenAIConfigFile>,
}

impl AnyserveConfigFile {
    fn load(path: &Path) -> Result<Self> {
        let raw = fs::read_to_string(path)
            .with_context(|| format!("read config file {}", path.display()))?;
        toml::from_str(&raw).with_context(|| format!("parse config file {}", path.display()))
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct ServerConfigFile {
    #[serde(default)]
    grpc_host: Option<String>,
    #[serde(default)]
    grpc_port: Option<u16>,
    #[serde(default)]
    heartbeat_ttl_secs: Option<u64>,
    #[serde(default)]
    default_lease_ttl_secs: Option<u64>,
    #[serde(default)]
    storage: Option<StorageConfigFile>,
    #[serde(default)]
    frames: Option<FrameConfigFile>,
    #[serde(default)]
    tls: Option<TlsConfigFile>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct StorageConfigFile {
    #[serde(default)]
    backend: Option<String>,
    #[serde(default)]
    dsn: Option<String>,
}

impl StorageConfigFile {
    fn resolve(self) -> Result<StorageConfig> {
        let backend = match self.backend.as_deref().unwrap_or("memory") {
            "memory" => StorageBackend::Memory,
            "sqlite" => StorageBackend::Sqlite,
            "postgres" => StorageBackend::Postgres,
            other => anyhow::bail!("unsupported storage backend '{other}'"),
        };
        if matches!(backend, StorageBackend::Sqlite | StorageBackend::Postgres)
            && self.dsn.is_none()
        {
            anyhow::bail!("storage.dsn is required for non-memory backends");
        }
        Ok(StorageConfig {
            backend,
            dsn: self.dsn,
        })
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct FrameConfigFile {
    #[serde(default)]
    backend: Option<String>,
    #[serde(default)]
    redis_url: Option<String>,
    #[serde(default)]
    closed_retention_secs: Option<u64>,
    #[serde(default)]
    watch_poll_interval_ms: Option<u64>,
}

impl FrameConfigFile {
    fn resolve(self, storage_backend: StorageBackend) -> Result<FrameConfig> {
        let backend = match self.backend.as_deref() {
            Some("memory") => FrameBackend::Memory,
            Some("redis") => FrameBackend::Redis,
            Some(other) => anyhow::bail!("unsupported frames backend '{other}'"),
            None => {
                if storage_backend == StorageBackend::Postgres {
                    FrameBackend::Redis
                } else {
                    FrameBackend::Memory
                }
            }
        };
        if backend == FrameBackend::Redis && self.redis_url.is_none() {
            anyhow::bail!("frames.redis_url is required when frames.backend = 'redis'");
        }
        match (storage_backend, backend) {
            (StorageBackend::Memory, FrameBackend::Memory)
            | (StorageBackend::Sqlite, FrameBackend::Memory)
            | (StorageBackend::Postgres, FrameBackend::Memory)
            | (StorageBackend::Postgres, FrameBackend::Redis) => {}
            (StorageBackend::Memory, FrameBackend::Redis) => {
                anyhow::bail!("memory storage only supports frames.backend = 'memory'")
            }
            (StorageBackend::Sqlite, FrameBackend::Redis) => {
                anyhow::bail!("sqlite storage only supports frames.backend = 'memory'")
            }
        }
        Ok(FrameConfig {
            backend,
            redis_url: self.redis_url,
            closed_retention_secs: self
                .closed_retention_secs
                .unwrap_or(DEFAULT_CLOSED_RETENTION_SECS),
            watch_poll_interval_ms: self
                .watch_poll_interval_ms
                .unwrap_or(DEFAULT_WATCH_POLL_INTERVAL_MS),
        })
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct TlsConfigFile {
    #[serde(default)]
    enabled: Option<bool>,
    #[serde(default)]
    cert_file: Option<String>,
    #[serde(default)]
    key_file: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct ServeOpenAIConfigFile {
    #[serde(default)]
    enabled: Option<bool>,
    #[serde(default)]
    listen: Option<String>,
    #[serde(default)]
    models: Vec<String>,
    #[serde(default)]
    chat_interface: Option<String>,
    #[serde(default)]
    embeddings_interface: Option<String>,
    #[serde(default)]
    request_timeout_secs: Option<u64>,
    #[serde(default)]
    inline_request_limit_bytes: Option<usize>,
    #[serde(default)]
    demand: Option<DemandConfigFile>,
}

impl ServeOpenAIConfigFile {
    fn resolve(&self) -> Option<ServeOpenAIConfig> {
        if !self.enabled.unwrap_or(false) {
            return None;
        }

        Some(ServeOpenAIConfig {
            listen: self
                .listen
                .clone()
                .unwrap_or_else(|| DEFAULT_OPENAI_LISTEN.to_string()),
            models: if self.models.is_empty() {
                vec!["llm.chat.v1".to_string(), "llm.embed.v1".to_string()]
            } else {
                self.models.clone()
            },
            chat_interface: self
                .chat_interface
                .clone()
                .unwrap_or_else(|| DEFAULT_CHAT_INTERFACE.to_string()),
            embeddings_interface: self
                .embeddings_interface
                .clone()
                .unwrap_or_else(|| DEFAULT_EMBEDDINGS_INTERFACE.to_string()),
            request_timeout_secs: self
                .request_timeout_secs
                .unwrap_or(DEFAULT_OPENAI_REQUEST_TIMEOUT_SECS),
            inline_request_limit_bytes: self
                .inline_request_limit_bytes
                .unwrap_or(DEFAULT_OPENAI_INLINE_REQUEST_LIMIT_BYTES),
            demand: self
                .demand
                .clone()
                .map(DemandConfigFile::resolve)
                .unwrap_or_default(),
        })
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct DemandConfigFile {
    #[serde(default)]
    required_attributes: Attributes,
    #[serde(default)]
    preferred_attributes: Attributes,
    #[serde(default)]
    required_capacity: Capacity,
}

impl DemandConfigFile {
    fn resolve(self) -> Demand {
        Demand {
            required_attributes: self.required_attributes,
            preferred_attributes: self.preferred_attributes,
            required_capacity: self.required_capacity,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DEFAULT_HEARTBEAT_TTL_SECS, DEFAULT_LEASE_TTL_SECS, DEFAULT_WATCH_POLL_INTERVAL_MS,
        FrameBackend, ResolvedServeConfig, ServeOverrides, StorageBackend,
    };
    use std::fs;

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
}
