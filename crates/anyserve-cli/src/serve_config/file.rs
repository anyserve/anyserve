use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use anyserve_core::auth::AuthPolicy;
use serde::Deserialize;

use super::{DEFAULT_CONSOLE_LISTEN, ServeConsoleConfig};
use anyserve_core::runtime::api::{InterfacePolicy, JournalConfig, RuntimeConfig};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

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
pub(super) struct JournalConfigFile {
    enabled: Option<bool>,
    backend: Option<JournalBackend>,
    url_env: Option<String>,
    schema: Option<String>,
    lease_duration_ms: Option<u64>,
    poll_interval_ms: Option<u64>,
    max_connections: Option<u32>,
    path: Option<PathBuf>,
    full_sync: Option<bool>,
    queue_capacity: Option<usize>,
    max_batch: Option<usize>,
    retention_secs: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
enum JournalBackend {
    Sqlite,
    Postgres,
}

#[cfg(test)]
mod postgres_tests {
    use super::*;

    #[test]
    fn postgres_config_uses_environment_and_redacts_the_url() {
        let file: JournalConfigFile =
            toml::from_str("backend='postgres'\nschema='cluster_a'\nlease_duration_ms=9000")
                .unwrap();
        let config = file
            .resolve_with_env(None, |name| {
                assert_eq!(name, "ANYSERVE_DATABASE_URL");
                Some("postgresql://user:secret@localhost/anyserve".into())
            })
            .unwrap()
            .unwrap();
        assert_eq!(config.postgres.as_ref().unwrap().schema, "cluster_a");
        assert_eq!(
            config.postgres.as_ref().unwrap().lease_duration,
            Duration::from_secs(9)
        );
        assert!(!format!("{config:?}").contains("secret"));
    }

    #[test]
    fn postgres_needs_an_explicit_url_but_overrides_do_not() {
        let file: JournalConfigFile = toml::from_str("backend='postgres'").unwrap();
        assert!(
            file.clone()
                .resolve_with_env(None, |_| None)
                .unwrap_err()
                .to_string()
                .contains("ANYSERVE_DATABASE_URL")
        );
        assert!(
            file.resolve_with_env(Some("local.sqlite".into()), |_| panic!(
                "must not read PG env"
            ))
            .unwrap()
            .unwrap()
            .postgres
            .is_none()
        );
        let file: JournalConfigFile = toml::from_str("backend='postgres'\nenabled=false").unwrap();
        assert!(
            file.resolve_with_env(None, |_| panic!("disabled"))
                .unwrap()
                .is_none()
        );
        assert!(toml::from_str::<JournalConfigFile>("backend='unknown'").is_err());
    }
}

impl JournalConfigFile {
    pub(super) fn resolve(self, path_override: Option<PathBuf>) -> Result<Option<JournalConfig>> {
        self.resolve_with_env(path_override, |name| std::env::var(name).ok())
    }

    fn resolve_with_env(
        self,
        path_override: Option<PathBuf>,
        env: impl FnOnce(&str) -> Option<String>,
    ) -> Result<Option<JournalConfig>> {
        if !self.enabled.unwrap_or(true) && path_override.is_none() {
            return Ok(None);
        }
        let postgres = path_override.is_none() && self.backend == Some(JournalBackend::Postgres);
        let mut config = if postgres {
            if self.path.is_some() || self.full_sync == Some(false) {
                anyhow::bail!("journal.path and full_sync=false cannot be used with PostgreSQL");
            }
            let name = self.url_env.as_deref().unwrap_or("ANYSERVE_DATABASE_URL");
            let url = env(name)
                .filter(|url| !url.trim().is_empty())
                .with_context(|| format!("PostgreSQL requires connection URL in env var {name}"))?;
            let mut config = JournalConfig::postgres(url);
            let pg = config.postgres.as_mut().expect("PostgreSQL config");
            if let Some(schema) = self.schema {
                pg.schema = schema;
            }
            if let Some(value) = self.lease_duration_ms {
                pg.lease_duration = Duration::from_millis(value);
            }
            if let Some(value) = self.poll_interval_ms {
                pg.poll_interval = Duration::from_millis(value);
            }
            if let Some(value) = self.max_connections {
                pg.max_connections = value;
            }
            config
        } else {
            JournalConfig::new(
                path_override
                    .or(self.path)
                    .unwrap_or_else(|| PathBuf::from(".anyserve/journal.sqlite")),
            )
        };
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
        if (!postgres && config.path.as_os_str().is_empty())
            || config.queue_capacity == 0
            || config.max_batch == 0
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
