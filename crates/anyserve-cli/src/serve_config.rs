mod file;
#[cfg(test)]
mod tests;

use std::path::PathBuf;

use anyhow::Result;
use anyserve_core::model::Demand;

use file::{AnyserveConfigFile, ServeOpenAIConfigFile};

pub const DEFAULT_HEARTBEAT_TTL_SECS: u64 = 30;
pub const DEFAULT_LEASE_TTL_SECS: u64 = 30;
pub const DEFAULT_WATCH_POLL_INTERVAL_MS: u64 = 250;
pub const DEFAULT_CLOSED_RETENTION_SECS: u64 = 300;
const DEFAULT_OPENAI_LISTEN: &str = "0.0.0.0:8080";
const DEFAULT_CONSOLE_LISTEN: &str = "127.0.0.1:3001";
const DEFAULT_CHAT_INTERFACE: &str = "llm.chat.v1";
const DEFAULT_EMBEDDINGS_INTERFACE: &str = "llm.embed.v1";
const DEFAULT_OPENAI_REQUEST_TIMEOUT_SECS: u64 = 60;
const DEFAULT_OPENAI_INLINE_REQUEST_LIMIT_BYTES: usize = 64 * 1024;

#[derive(Clone, Debug)]
pub struct GrpcConfig {
    pub host: String,
    pub port: u16,
    pub tls_enabled: bool,
    pub cert_file: Option<String>,
    pub key_file: Option<String>,
}

impl Default for GrpcConfig {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".to_string(),
            port: 50_052,
            tls_enabled: false,
            cert_file: None,
            key_file: None,
        }
    }
}

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
    pub console: Option<ServeConsoleConfig>,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeMode {
    Memory,
    MemoryPlusPostgres,
    MemoryPlusSqlite,
    PostgresAndRedis,
}

impl RuntimeMode {
    pub fn code(self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::MemoryPlusPostgres => "memory_plus_postgres",
            Self::MemoryPlusSqlite => "memory_plus_sqlite",
            Self::PostgresAndRedis => "postgres_and_redis",
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Memory => "Memory",
            Self::MemoryPlusPostgres => "Memory + PostgreSQL",
            Self::MemoryPlusSqlite => "Memory + SQLite",
            Self::PostgresAndRedis => "PostgreSQL + Redis",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServeConsoleConfig {
    pub listen: String,
    pub allowed_origins: Vec<String>,
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
        let console = file_config
            .as_ref()
            .and_then(|config| config.console.as_ref())
            .and_then(|config| config.clone().resolve());
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
            console,
            openai: file_config
                .as_ref()
                .and_then(|config| config.openai.as_ref())
                .and_then(ServeOpenAIConfigFile::resolve),
        })
    }

    pub fn runtime_mode(&self) -> RuntimeMode {
        match (self.storage.backend, self.frames.backend) {
            (StorageBackend::Memory, FrameBackend::Memory) => RuntimeMode::Memory,
            (StorageBackend::Sqlite, FrameBackend::Memory) => RuntimeMode::MemoryPlusSqlite,
            (StorageBackend::Postgres, FrameBackend::Memory) => RuntimeMode::MemoryPlusPostgres,
            (StorageBackend::Postgres, FrameBackend::Redis) => RuntimeMode::PostgresAndRedis,
            _ => unreachable!("validated storage and frame backends should map to a runtime mode"),
        }
    }
}
