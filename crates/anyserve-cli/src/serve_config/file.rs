use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use anyserve_core::auth::AuthPolicy;
use anyserve_core::runtime::api::{Capacity, Demand, Metadata as Attributes};
use serde::Deserialize;

use super::{
    DEFAULT_CHAT_INTERFACE, DEFAULT_CONSOLE_LISTEN, DEFAULT_EMBEDDINGS_INTERFACE,
    DEFAULT_INVOKE_LISTEN, DEFAULT_INVOKE_MAX_BODY_BYTES, DEFAULT_INVOKE_TIMEOUT_MS,
    DEFAULT_OPENAI_LISTEN, DEFAULT_OPENAI_REQUEST_TIMEOUT_SECS, ServeConsoleConfig,
    ServeInvokeConfig, ServeOpenAIConfig,
};
use anyserve_core::runtime::api::{InterfacePolicy, JournalConfig, RuntimeConfig};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

use crate::edge_routing::RoutingDefaults;

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub(super) struct AnyserveConfigFile {
    #[serde(default)]
    pub(super) server: Option<ServerConfigFile>,
    #[serde(default)]
    pub(super) runtime: Option<RuntimeConfigFile>,
    #[serde(default)]
    pub(super) journal: Option<JournalConfigFile>,
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
    execution_timeout_ms: Option<u64>,
    #[serde(default)]
    priority: Option<u32>,
}

impl RoutingPolicyConfigFile {
    fn resolve(self) -> RoutingDefaults {
        RoutingDefaults {
            queue_timeout_ms: self.queue_timeout_ms.unwrap_or_default(),
            spillover_after_ms: self.spillover_after_ms.unwrap_or_default(),
            execution_timeout_ms: self.execution_timeout_ms.unwrap_or_default(),
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
        let value: toml::Value = toml::from_str(&raw).context("parse TOML")?;
        if value
            .get("server")
            .is_some_and(|server| server.get("storage").is_some() || server.get("frames").is_some())
        {
            anyhow::bail!(
                "server.storage and server.frames were replaced by [journal]; see docs/src/architecture.md#migration-from-04"
            );
        }
        toml::from_str(&raw).with_context(|| format!("parse config file {}; for 0.4 configuration migration see docs/src/architecture.md#migration-from-04", path.display()))
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
    pub(super) tls: Option<TlsConfigFile>,
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
            interface: String::new(),
            required_attributes: self.required_attributes,
            preferred_attributes: self.preferred_attributes,
            required_capacity: self.required_capacity,
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub(super) struct JournalConfigFile {
    enabled: Option<bool>,
    path: Option<PathBuf>,
    full_sync: Option<bool>,
    queue_capacity: Option<usize>,
    max_batch: Option<usize>,
    retention_secs: Option<u64>,
}
impl JournalConfigFile {
    pub(super) fn resolve(self, path_override: Option<PathBuf>) -> Result<Option<JournalConfig>> {
        if !self.enabled.unwrap_or(true) && path_override.is_none() {
            return Ok(None);
        }
        let mut config = JournalConfig::new(
            path_override
                .or(self.path)
                .unwrap_or_else(|| PathBuf::from(".anyserve/journal.sqlite")),
        );
        if let Some(value) = self.full_sync {
            config.full_sync = value;
        }
        if let Some(value) = self.queue_capacity {
            config.queue_capacity = value;
        }
        if let Some(value) = self.max_batch {
            config.max_batch = value;
        }
        if let Some(value) = self.retention_secs {
            config.retention = Duration::from_secs(value);
        }
        if config.path.as_os_str().is_empty() || config.queue_capacity == 0 || config.max_batch == 0
        {
            anyhow::bail!("journal path, queue_capacity and max_batch must be non-empty/positive");
        }
        Ok(Some(config))
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub(super) struct RuntimeConfigFile {
    heartbeat_interval_ms: Option<u64>,
    heartbeat_timeout_ms: Option<u64>,
    max_attempts: Option<u32>,
    max_queue: Option<usize>,
    max_pending_bytes: Option<u64>,
    max_pending_jobs: Option<usize>,
    max_event_bytes: Option<u64>,
    max_result_bytes: Option<u64>,
    max_sessions: Option<usize>,
    consumer_stall_timeout_ms: Option<u64>,
    cancel_grace_ms: Option<u64>,
    terminal_retention_secs: Option<u64>,
    max_terminal_jobs: Option<usize>,
    event_log_max_events: Option<usize>,
    event_log_max_bytes: Option<usize>,
    #[serde(default)]
    interfaces: BTreeMap<String, InterfaceConfigFile>,
}
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct InterfaceConfigFile {
    max_queue: Option<usize>,
    queue_timeout_ms: Option<u64>,
}
impl RuntimeConfigFile {
    pub(super) fn resolve(self) -> RuntimeConfig {
        let mut config = RuntimeConfig::default();
        if let Some(value) = self.heartbeat_interval_ms {
            config.heartbeat_interval = Duration::from_millis(value);
        }
        if let Some(value) = self.heartbeat_timeout_ms {
            config.heartbeat_timeout = Duration::from_millis(value);
        }
        if let Some(value) = self.max_attempts {
            config.default_max_attempts = value;
        }
        if let Some(value) = self.max_queue {
            config.default_max_queue = value;
        }
        if let Some(value) = self.max_pending_bytes {
            config.max_pending_bytes = value;
        }
        if let Some(value) = self.max_pending_jobs {
            config.max_pending_jobs = value;
        }
        if let Some(value) = self.max_event_bytes {
            config.max_event_bytes = value;
        }
        if let Some(value) = self.max_result_bytes {
            config.max_result_bytes = value;
        }
        if let Some(value) = self.max_sessions {
            config.max_sessions = value;
        }
        if let Some(value) = self.consumer_stall_timeout_ms {
            config.consumer_stall_timeout = Duration::from_millis(value);
        }
        if let Some(value) = self.cancel_grace_ms {
            config.cancel_grace = Duration::from_millis(value);
        }
        if let Some(value) = self.terminal_retention_secs {
            config.terminal_retention = Duration::from_secs(value);
        }
        if let Some(value) = self.max_terminal_jobs {
            config.max_terminal_jobs = value;
        }
        if let Some(value) = self.event_log_max_events {
            config.event_log_max_events = value;
        }
        if let Some(value) = self.event_log_max_bytes {
            config.event_log_max_bytes = value;
        }
        config.interfaces = self
            .interfaces
            .into_iter()
            .map(|(name, policy)| {
                (
                    name,
                    InterfacePolicy {
                        max_queue: policy.max_queue,
                        queue_timeout: policy.queue_timeout_ms.map(Duration::from_millis),
                    },
                )
            })
            .collect();
        config
    }
}
