use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};
use anyserve_core::runtime::api::{InterfacePolicy, LiveRuntimeSettings, PoolTier};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PoolTierDto {
    pub(crate) pool: String,
    #[serde(default)]
    pub(crate) spillover_after_ms: u64,
    #[serde(default)]
    pub(crate) reconnect_grace_ms: Option<u64>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct InterfacePolicyDto {
    #[serde(default)]
    pub(crate) max_queue: Option<usize>,
    #[serde(default)]
    pub(crate) queue_timeout_ms: Option<u64>,
    #[serde(default)]
    pub(crate) max_in_flight_per_key: Option<usize>,
    #[serde(default)]
    pub(crate) pool_tiers: Vec<PoolTierDto>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RuntimeSettingsDto {
    pub(crate) default_max_attempts: u32,
    pub(crate) default_max_queue: usize,
    #[serde(default)]
    pub(crate) default_max_in_flight_per_key: Option<usize>,
    pub(crate) max_pending_jobs: usize,
    pub(crate) max_terminal_jobs: usize,
    pub(crate) terminal_retention_secs: u64,
    pub(crate) max_sessions: usize,
    pub(crate) heartbeat_interval_ms: u64,
    pub(crate) heartbeat_timeout_ms: u64,
    #[serde(default)]
    pub(crate) reconnect_grace_ms: Option<u64>,
    pub(crate) default_initial_backoff_ms: u64,
    pub(crate) default_max_backoff_ms: u64,
    pub(crate) consumer_stall_timeout_ms: u64,
    pub(crate) cancel_grace_ms: u64,
    #[serde(default)]
    pub(crate) interfaces: BTreeMap<String, InterfacePolicyDto>,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct SettingsLimitsMeta {
    pub(crate) max_attempts_limit: u32,
    pub(crate) priority_levels: u32,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct SettingsResponse {
    pub(crate) editable: bool,
    pub(crate) persisted: bool,
    pub(crate) storage_path: Option<String>,
    pub(crate) modified: bool,
    pub(crate) settings: RuntimeSettingsDto,
    pub(crate) defaults: RuntimeSettingsDto,
    pub(crate) limits: SettingsLimitsMeta,
    pub(crate) server_toml: String,
}

fn duration_ms(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

impl From<&LiveRuntimeSettings> for RuntimeSettingsDto {
    fn from(s: &LiveRuntimeSettings) -> Self {
        let interfaces = s
            .interfaces
            .iter()
            .map(|(name, policy)| {
                let tiers = policy
                    .pool_tiers
                    .iter()
                    .map(|tier| PoolTierDto {
                        pool: tier.pool.clone(),
                        spillover_after_ms: duration_ms(tier.spillover_after),
                        reconnect_grace_ms: tier.reconnect_grace.map(duration_ms),
                    })
                    .collect();
                (
                    name.clone(),
                    InterfacePolicyDto {
                        max_queue: policy.max_queue,
                        queue_timeout_ms: policy.queue_timeout.map(duration_ms),
                        max_in_flight_per_key: policy.max_in_flight_per_key,
                        pool_tiers: tiers,
                    },
                )
            })
            .collect();
        Self {
            default_max_attempts: s.default_max_attempts,
            default_max_queue: s.default_max_queue,
            default_max_in_flight_per_key: s.default_max_in_flight_per_key,
            max_pending_jobs: s.max_pending_jobs,
            max_terminal_jobs: s.max_terminal_jobs,
            terminal_retention_secs: s.terminal_retention.as_secs(),
            max_sessions: s.max_sessions,
            heartbeat_interval_ms: duration_ms(s.heartbeat_interval),
            heartbeat_timeout_ms: duration_ms(s.heartbeat_timeout),
            reconnect_grace_ms: s.reconnect_grace.map(duration_ms),
            default_initial_backoff_ms: duration_ms(s.default_initial_backoff),
            default_max_backoff_ms: duration_ms(s.default_max_backoff),
            consumer_stall_timeout_ms: duration_ms(s.consumer_stall_timeout),
            cancel_grace_ms: duration_ms(s.cancel_grace),
            interfaces,
        }
    }
}

impl RuntimeSettingsDto {
    pub(crate) fn into_live(self) -> LiveRuntimeSettings {
        let interfaces = self
            .interfaces
            .into_iter()
            .map(|(name, policy)| {
                let pool_tiers = policy
                    .pool_tiers
                    .into_iter()
                    .map(|tier| PoolTier {
                        pool: tier.pool,
                        spillover_after: Duration::from_millis(tier.spillover_after_ms),
                        reconnect_grace: tier.reconnect_grace_ms.map(Duration::from_millis),
                    })
                    .collect();
                (
                    name,
                    InterfacePolicy {
                        max_queue: policy.max_queue,
                        queue_timeout: policy.queue_timeout_ms.map(Duration::from_millis),
                        max_in_flight_per_key: policy.max_in_flight_per_key,
                        pool_tiers,
                    },
                )
            })
            .collect();
        LiveRuntimeSettings {
            heartbeat_interval: Duration::from_millis(self.heartbeat_interval_ms),
            heartbeat_timeout: Duration::from_millis(self.heartbeat_timeout_ms),
            reconnect_grace: self.reconnect_grace_ms.map(Duration::from_millis),
            default_max_attempts: self.default_max_attempts,
            default_initial_backoff: Duration::from_millis(self.default_initial_backoff_ms),
            default_max_backoff: Duration::from_millis(self.default_max_backoff_ms),
            default_max_queue: self.default_max_queue,
            default_max_in_flight_per_key: self.default_max_in_flight_per_key,
            max_pending_jobs: self.max_pending_jobs,
            consumer_stall_timeout: Duration::from_millis(self.consumer_stall_timeout_ms),
            cancel_grace: Duration::from_millis(self.cancel_grace_ms),
            terminal_retention: Duration::from_secs(self.terminal_retention_secs),
            max_terminal_jobs: self.max_terminal_jobs,
            max_sessions: self.max_sessions,
            interfaces,
        }
    }
}

/// Renders the effective live settings as a valid `[runtime]` TOML block that
/// can be pasted directly into `server.toml`.
pub(crate) fn render_server_toml_snippet(s: &LiveRuntimeSettings) -> String {
    use std::fmt::Write;

    let mut out = String::from("[runtime]\n");
    let _ = writeln!(out, "default_max_attempts = {}", s.default_max_attempts);
    let _ = writeln!(out, "max_queue = {}", s.default_max_queue);
    if let Some(cap) = s.default_max_in_flight_per_key {
        let _ = writeln!(out, "max_in_flight_per_key = {cap}");
    }
    let _ = writeln!(out, "max_pending_jobs = {}", s.max_pending_jobs);
    let _ = writeln!(out, "max_terminal_jobs = {}", s.max_terminal_jobs);
    let _ = writeln!(
        out,
        "terminal_retention_secs = {}",
        s.terminal_retention.as_secs()
    );
    let _ = writeln!(out, "max_sessions = {}", s.max_sessions);
    let _ = writeln!(
        out,
        "heartbeat_interval_ms = {}",
        duration_ms(s.heartbeat_interval)
    );
    let _ = writeln!(
        out,
        "heartbeat_timeout_ms = {}",
        duration_ms(s.heartbeat_timeout)
    );
    if let Some(grace) = s.reconnect_grace {
        let _ = writeln!(out, "reconnect_grace_ms = {}", duration_ms(grace));
    }
    let _ = writeln!(
        out,
        "default_initial_backoff_ms = {}",
        duration_ms(s.default_initial_backoff)
    );
    let _ = writeln!(
        out,
        "default_max_backoff_ms = {}",
        duration_ms(s.default_max_backoff)
    );
    let _ = writeln!(
        out,
        "consumer_stall_timeout_ms = {}",
        duration_ms(s.consumer_stall_timeout)
    );
    let _ = writeln!(out, "cancel_grace_ms = {}", duration_ms(s.cancel_grace));

    for (iface, policy) in &s.interfaces {
        let key = toml_quote(iface);
        let has_scalars = policy.max_queue.is_some()
            || policy.queue_timeout.is_some()
            || policy.max_in_flight_per_key.is_some();
        if has_scalars {
            let _ = writeln!(out, "\n[runtime.interfaces.{key}]");
            if let Some(q) = policy.max_queue {
                let _ = writeln!(out, "max_queue = {q}");
            }
            if let Some(qt) = policy.queue_timeout {
                let _ = writeln!(out, "queue_timeout_ms = {}", duration_ms(qt));
            }
            if let Some(cap) = policy.max_in_flight_per_key {
                let _ = writeln!(out, "max_in_flight_per_key = {cap}");
            }
        }
        for tier in &policy.pool_tiers {
            let _ = writeln!(out, "\n[[runtime.interfaces.{key}.pool_tiers]]");
            let _ = writeln!(out, "pool = {}", toml_quote(&tier.pool));
            if !tier.spillover_after.is_zero() {
                let _ = writeln!(
                    out,
                    "spillover_after_ms = {}",
                    duration_ms(tier.spillover_after)
                );
            }
            if let Some(grace) = tier.reconnect_grace {
                let _ = writeln!(out, "reconnect_grace_ms = {}", duration_ms(grace));
            }
        }
    }
    out
}

fn toml_quote(raw: &str) -> String {
    serde_json::to_string(raw).unwrap_or_else(|_| format!("\"{raw}\""))
}

#[derive(Clone, Debug, Default)]
pub(crate) struct SettingsStore {
    path: Option<PathBuf>,
}

impl SettingsStore {
    pub(crate) fn new(path: Option<PathBuf>) -> Self {
        Self { path }
    }

    pub(crate) fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    pub(crate) fn load_file(path: &Path) -> Result<Option<LiveRuntimeSettings>> {
        match std::fs::read_to_string(path) {
            Ok(raw) => {
                let dto: RuntimeSettingsDto = serde_json::from_str(&raw)
                    .with_context(|| format!("parse runtime settings {}", path.display()))?;
                Ok(Some(dto.into_live()))
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => {
                Err(err).with_context(|| format!("read runtime settings {}", path.display()))
            }
        }
    }

    pub(crate) fn save(&self, settings: &LiveRuntimeSettings) -> Result<bool> {
        let Some(path) = &self.path else {
            return Ok(false);
        };
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create settings directory {}", parent.display()))?;
        }
        let dto = RuntimeSettingsDto::from(settings);
        let encoded = serde_json::to_string_pretty(&dto).context("serialize runtime settings")?;
        let tmp = path.with_extension(format!("json.tmp.{}", std::process::id()));
        write_private_file(&tmp, encoded.as_bytes())
            .with_context(|| format!("write temp settings {}", tmp.display()))?;
        std::fs::rename(&tmp, path)
            .with_context(|| format!("replace runtime settings {}", path.display()))?;
        Ok(true)
    }

    pub(crate) fn remove(&self) -> Result<()> {
        let Some(path) = &self.path else {
            return Ok(());
        };
        match std::fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => {
                Err(err).with_context(|| format!("remove runtime settings {}", path.display()))
            }
        }
    }
}

fn write_private_file(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::io::Write;
        use std::os::unix::fs::OpenOptionsExt;

        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(path)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        Ok(())
    }
    #[cfg(not(unix))]
    {
        std::fs::write(path, bytes)
    }
}
