use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use anyserve_core::auth::AuthPolicy;
use anyserve_core::model::{Attributes, Capacity, Demand};
use serde::Deserialize;

use super::{
    DEFAULT_CHAT_INTERFACE, DEFAULT_CLOSED_RETENTION_SECS, DEFAULT_CONSOLE_LISTEN,
    DEFAULT_EMBEDDINGS_INTERFACE, DEFAULT_INVOKE_LISTEN, DEFAULT_INVOKE_MAX_BODY_BYTES,
    DEFAULT_INVOKE_TIMEOUT_MS, DEFAULT_OPENAI_LISTEN, DEFAULT_OPENAI_REQUEST_TIMEOUT_SECS,
    DEFAULT_WATCH_POLL_INTERVAL_MS, FrameBackend, FrameConfig, ServeConsoleConfig,
    ServeInvokeConfig, ServeOpenAIConfig, StorageBackend, StorageConfig,
};
use crate::edge_routing::RoutingDefaults;

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub(super) struct AnyserveConfigFile {
    #[serde(default)]
    pub(super) server: Option<ServerConfigFile>,
    #[serde(default)]
    pub(super) console: Option<ServeConsoleConfigFile>,
    #[serde(default)]
    pub(super) openai: Option<ServeOpenAIConfigFile>,
    #[serde(default)]
    pub(super) invoke: Option<ServeInvokeConfigFile>,
    #[serde(default)]
    pub(super) auth: Option<AuthConfigFile>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub(super) struct AuthConfigFile {
    #[serde(default)]
    client_tokens: Vec<String>,
    /// Env var holding comma-separated client tokens.
    #[serde(default)]
    client_tokens_env: Option<String>,
    #[serde(default)]
    worker_tokens: Vec<String>,
    /// Env var holding comma-separated worker tokens.
    #[serde(default)]
    worker_tokens_env: Option<String>,
}

impl AuthConfigFile {
    pub(super) fn resolve(self) -> Result<AuthPolicy> {
        let from_env = |name: Option<&str>| -> Result<Vec<String>> {
            let Some(name) = name else {
                return Ok(Vec::new());
            };
            let raw = std::env::var(name)
                .with_context(|| format!("read auth tokens from env var {name}"))?;
            Ok(raw.split(',').map(str::to_string).collect())
        };
        let mut client_tokens = self.client_tokens;
        client_tokens.extend(from_env(self.client_tokens_env.as_deref())?);
        let mut worker_tokens = self.worker_tokens;
        worker_tokens.extend(from_env(self.worker_tokens_env.as_deref())?);
        Ok(AuthPolicy::new(client_tokens, worker_tokens))
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub(super) struct RoutingPolicyConfigFile {
    #[serde(default)]
    queue_timeout_ms: Option<u64>,
    #[serde(default)]
    spillover_after_ms: Option<u64>,
    #[serde(default)]
    lease_ttl_secs: Option<u64>,
    #[serde(default)]
    priority: Option<i32>,
}

impl RoutingPolicyConfigFile {
    fn resolve(self) -> RoutingDefaults {
        RoutingDefaults {
            queue_timeout_ms: self.queue_timeout_ms.unwrap_or_default(),
            spillover_after_ms: self.spillover_after_ms.unwrap_or_default(),
            lease_ttl_secs: self.lease_ttl_secs.unwrap_or_default(),
            priority: self.priority.unwrap_or_default(),
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub(super) struct ServeInvokeConfigFile {
    #[serde(default)]
    enabled: Option<bool>,
    #[serde(default)]
    listen: Option<String>,
    #[serde(default)]
    default_timeout_ms: Option<u64>,
    #[serde(default)]
    max_body_bytes: Option<usize>,
    #[serde(default)]
    forward_headers: Vec<String>,
    #[serde(default)]
    demand: Option<DemandConfigFile>,
    #[serde(default)]
    policy: Option<RoutingPolicyConfigFile>,
}

impl ServeInvokeConfigFile {
    pub(super) fn resolve(self) -> Option<ServeInvokeConfig> {
        if !self.enabled.unwrap_or(true) {
            return None;
        }
        Some(ServeInvokeConfig {
            listen: self
                .listen
                .unwrap_or_else(|| DEFAULT_INVOKE_LISTEN.to_string()),
            default_timeout_ms: self.default_timeout_ms.unwrap_or(DEFAULT_INVOKE_TIMEOUT_MS),
            max_body_bytes: self.max_body_bytes.unwrap_or(DEFAULT_INVOKE_MAX_BODY_BYTES),
            forward_headers: self
                .forward_headers
                .into_iter()
                .map(|name| name.to_ascii_lowercase())
                .collect(),
            demand: self
                .demand
                .map(DemandConfigFile::resolve)
                .unwrap_or_default(),
            defaults: self.policy.unwrap_or_default().resolve(),
        })
    }
}

impl AnyserveConfigFile {
    pub(super) fn load(path: &Path) -> Result<Self> {
        let raw = fs::read_to_string(path)
            .with_context(|| format!("read config file {}", path.display()))?;
        toml::from_str(&raw).with_context(|| format!("parse config file {}", path.display()))
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub(super) struct ServerConfigFile {
    #[serde(default)]
    pub(super) grpc_host: Option<String>,
    #[serde(default)]
    pub(super) grpc_port: Option<u16>,
    #[serde(default)]
    pub(super) heartbeat_ttl_secs: Option<u64>,
    #[serde(default)]
    pub(super) default_lease_ttl_secs: Option<u64>,
    #[serde(default)]
    pub(super) job_retention_secs: Option<u64>,
    #[serde(default)]
    pub(super) storage: Option<StorageConfigFile>,
    #[serde(default)]
    pub(super) frames: Option<FrameConfigFile>,
    #[serde(default)]
    pub(super) tls: Option<TlsConfigFile>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub(super) struct StorageConfigFile {
    #[serde(default)]
    backend: Option<String>,
    #[serde(default)]
    dsn: Option<String>,
}

impl StorageConfigFile {
    pub(super) fn resolve(self) -> Result<StorageConfig> {
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
pub(super) struct FrameConfigFile {
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
    pub(super) fn resolve(self, storage_backend: StorageBackend) -> Result<FrameConfig> {
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
pub(super) struct ServeConsoleConfigFile {
    #[serde(default)]
    enabled: Option<bool>,
    #[serde(default)]
    listen: Option<String>,
    #[serde(default)]
    allowed_origins: Vec<String>,
}

impl ServeConsoleConfigFile {
    pub(super) fn resolve(self) -> Option<ServeConsoleConfig> {
        if !self.enabled.unwrap_or(true) {
            return None;
        }

        Some(ServeConsoleConfig {
            listen: self
                .listen
                .unwrap_or_else(|| DEFAULT_CONSOLE_LISTEN.to_string()),
            allowed_origins: self.allowed_origins,
        })
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub(super) struct TlsConfigFile {
    #[serde(default)]
    pub(super) enabled: Option<bool>,
    #[serde(default)]
    pub(super) cert_file: Option<String>,
    #[serde(default)]
    pub(super) key_file: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub(super) struct ServeOpenAIConfigFile {
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
    demand: Option<DemandConfigFile>,
    #[serde(default)]
    policy: Option<RoutingPolicyConfigFile>,
}

impl ServeOpenAIConfigFile {
    pub(super) fn resolve(&self) -> Option<ServeOpenAIConfig> {
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
            demand: self
                .demand
                .clone()
                .map(DemandConfigFile::resolve)
                .unwrap_or_default(),
            policy: self.policy.clone().unwrap_or_default().resolve(),
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
