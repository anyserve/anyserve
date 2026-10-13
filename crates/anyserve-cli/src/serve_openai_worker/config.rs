use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use clap::Args;
use serde::Deserialize;

const DEFAULT_POLL_INTERVAL_SECS: u64 = 1;
const DEFAULT_LONG_POLL_MS: u64 = 20_000;
const DEFAULT_HEARTBEAT_INTERVAL_SECS: u64 = 5;
const DEFAULT_HEALTH_INTERVAL_SECS: u64 = 5;
const DEFAULT_INPUT_STREAM_WAIT_SECS: u64 = 5;
const DEFAULT_UPSTREAM_TIMEOUT_SECS: u64 = 60;
const DEFAULT_RETRYABLE_STATUSES: &[u16] = &[429, 502, 503, 504];
pub(crate) const KIND_LLM: &str = "llm";
pub(crate) const KIND_HTTP: &str = "http";

#[derive(Args, Debug)]
pub struct ServeOpenAIWorkerArgs {
    #[arg(long = "config", value_name = "PATH")]
    pub config: PathBuf,
    #[arg(long = "worker-id")]
    pub worker_id: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct LeaseActivity {
    pub(crate) active_lease_ids: BTreeSet<String>,
}

#[derive(Clone, Debug)]
pub(crate) struct ResolvedServeOpenAIWorkerConfig {
    pub(crate) kind: String,
    pub(crate) worker_id: Option<String>,
    pub(crate) provider: String,
    pub(crate) base_url: String,
    pub(crate) api_key: Option<String>,
    pub(crate) token: Option<String>,
    pub(crate) interfaces: Vec<String>,
    /// Number of leases processed concurrently; one long-poll loop per slot.
    pub(crate) max_active_leases: u32,
    /// Back-off after a failed poll.
    pub(crate) poll_interval: Duration,
    /// How long each PollLease call may wait server-side for work. Zero falls
    /// back to fixed-interval polling.
    pub(crate) long_poll: Duration,
    pub(crate) heartbeat_interval: Duration,
    pub(crate) health_url: Option<String>,
    pub(crate) health_interval: Duration,
    pub(crate) input_stream_wait: Duration,
    pub(crate) upstream_timeout: Duration,
    /// `http` kind: interface name -> path appended to `base_url`.
    pub(crate) routes: HashMap<String, String>,
    /// `http` kind: upstream statuses that are retried on another worker.
    pub(crate) retryable_statuses: BTreeSet<u16>,
    pub(crate) attributes: HashMap<String, String>,
    pub(crate) capacity: HashMap<String, i64>,
    pub(crate) metadata: HashMap<String, String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ServeOpenAIWorkerConfigFile {
    #[serde(default)]
    kind: Option<String>,
    base_url: String,
    #[serde(default)]
    provider: Option<String>,
    #[serde(default)]
    api_key: Option<String>,
    #[serde(default)]
    api_key_env: Option<String>,
    #[serde(default)]
    token: Option<String>,
    #[serde(default)]
    token_env: Option<String>,
    #[serde(default)]
    interfaces: Vec<String>,
    #[serde(default)]
    worker_id: Option<String>,
    #[serde(default)]
    max_active_leases: Option<u32>,
    #[serde(default)]
    poll_interval_secs: Option<u64>,
    #[serde(default)]
    long_poll_ms: Option<u64>,
    #[serde(default)]
    heartbeat_interval_secs: Option<u64>,
    #[serde(default)]
    health_url: Option<String>,
    #[serde(default)]
    health_interval_secs: Option<u64>,
    #[serde(default)]
    input_stream_wait_secs: Option<u64>,
    #[serde(default)]
    upstream_timeout_secs: Option<u64>,
    #[serde(default)]
    routes: HashMap<String, String>,
    #[serde(default)]
    retryable_statuses: Option<Vec<u16>>,
    #[serde(default)]
    attributes: HashMap<String, String>,
    #[serde(default)]
    capacity: HashMap<String, i64>,
    #[serde(default)]
    metadata: HashMap<String, String>,
}

pub(crate) fn load_config(args: &ServeOpenAIWorkerArgs) -> Result<ResolvedServeOpenAIWorkerConfig> {
    let raw = fs::read_to_string(&args.config)
        .with_context(|| format!("read worker config {}", args.config.display()))?;
    let file: ServeOpenAIWorkerConfigFile = toml::from_str(&raw)
        .with_context(|| format!("parse worker config {}", args.config.display()))?;
    resolve_config(file, args.worker_id.clone())
        .with_context(|| format!("resolve worker config {}", args.config.display()))
}

fn resolve_config(
    file: ServeOpenAIWorkerConfigFile,
    worker_id_override: Option<String>,
) -> Result<ResolvedServeOpenAIWorkerConfig> {
    let kind = file.kind.unwrap_or_else(|| KIND_LLM.to_string());
    if kind != KIND_LLM && kind != KIND_HTTP {
        bail!("unsupported worker kind '{kind}'; expected '{KIND_LLM}' or '{KIND_HTTP}'");
    }

    let api_key = secret_from(file.api_key, file.api_key_env.as_deref())?;
    let token = secret_from(file.token, file.token_env.as_deref())?
        .or_else(|| std::env::var(anyserve_client::TOKEN_ENV).ok());

    let mut attributes = file.attributes;
    let provider = file.provider.unwrap_or_else(|| {
        if kind == KIND_LLM {
            "openai-compatible".to_string()
        } else {
            "http".to_string()
        }
    });
    if kind == KIND_LLM {
        attributes
            .entry("family".to_string())
            .or_insert_with(|| "llm".to_string());
        attributes
            .entry("protocol".to_string())
            .or_insert_with(|| "openai-compatible".to_string());
    } else {
        attributes
            .entry("protocol".to_string())
            .or_insert_with(|| "http".to_string());
    }
    attributes
        .entry("provider".to_string())
        .or_insert_with(|| provider.clone());

    // A stable, client-chosen id doubles as a placement attribute, so callers can
    // pin a session to one worker with `prefer: worker_id=<id>` (+ spillover).
    let worker_id = worker_id_override
        .or(file.worker_id)
        .unwrap_or_else(default_worker_id);
    attributes
        .entry("worker_id".to_string())
        .or_insert_with(|| worker_id.clone());

    let max_active_leases = file.max_active_leases.unwrap_or(1).max(1);
    let mut capacity = file.capacity;
    capacity
        .entry("slot".to_string())
        .or_insert(i64::from(max_active_leases));

    let interfaces = if !file.interfaces.is_empty() {
        file.interfaces
    } else if kind == KIND_LLM {
        vec!["llm.chat.v1".to_string(), "llm.embed.v1".to_string()]
    } else {
        let mut interfaces = file.routes.keys().cloned().collect::<Vec<_>>();
        interfaces.sort();
        interfaces
    };
    if interfaces.is_empty() {
        bail!("worker must declare at least one interface (or route for kind = 'http')");
    }
    if kind == KIND_HTTP
        && let Some(missing) = interfaces
            .iter()
            .find(|interface| !file.routes.contains_key(*interface))
    {
        bail!("kind = 'http' requires a route for interface '{missing}'");
    }

    Ok(ResolvedServeOpenAIWorkerConfig {
        kind,
        worker_id: Some(worker_id),
        provider,
        base_url: file.base_url.trim_end_matches('/').to_string(),
        api_key,
        token,
        interfaces,
        max_active_leases,
        poll_interval: Duration::from_secs(
            file.poll_interval_secs
                .unwrap_or(DEFAULT_POLL_INTERVAL_SECS),
        ),
        long_poll: Duration::from_millis(file.long_poll_ms.unwrap_or(DEFAULT_LONG_POLL_MS)),
        heartbeat_interval: Duration::from_secs(
            file.heartbeat_interval_secs
                .unwrap_or(DEFAULT_HEARTBEAT_INTERVAL_SECS),
        ),
        health_url: file.health_url,
        health_interval: Duration::from_secs(
            file.health_interval_secs
                .unwrap_or(DEFAULT_HEALTH_INTERVAL_SECS),
        ),
        input_stream_wait: Duration::from_secs(
            file.input_stream_wait_secs
                .unwrap_or(DEFAULT_INPUT_STREAM_WAIT_SECS),
        ),
        upstream_timeout: Duration::from_secs(
            file.upstream_timeout_secs
                .unwrap_or(DEFAULT_UPSTREAM_TIMEOUT_SECS),
        ),
        routes: file.routes,
        retryable_statuses: file
            .retryable_statuses
            .unwrap_or_else(|| DEFAULT_RETRYABLE_STATUSES.to_vec())
            .into_iter()
            .collect(),
        attributes,
        capacity,
        metadata: file.metadata,
    })
}

fn default_worker_id() -> String {
    let host = std::env::var("HOSTNAME").unwrap_or_else(|_| "worker".to_string());
    format!("{host}-{}", std::process::id())
}

fn secret_from(value: Option<String>, env_name: Option<&str>) -> Result<Option<String>> {
    if let Some(value) = value {
        return Ok(Some(value));
    }
    env_name
        .map(|name| std::env::var(name).with_context(|| format!("read secret from env var {name}")))
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(raw: &str) -> Result<ResolvedServeOpenAIWorkerConfig> {
        resolve_config(toml::from_str(raw)?, None)
    }

    #[test]
    fn llm_defaults_stay_backward_compatible() {
        let config = parse(r#"base_url = "http://127.0.0.1:11434/v1""#).unwrap();
        assert_eq!(config.kind, KIND_LLM);
        assert_eq!(config.max_active_leases, 1);
        assert_eq!(config.capacity.get("slot"), Some(&1));
        assert_eq!(
            config.attributes.get("family").map(String::as_str),
            Some("llm")
        );
        assert_eq!(
            config.long_poll,
            Duration::from_millis(DEFAULT_LONG_POLL_MS)
        );
    }

    #[test]
    fn http_kind_derives_interfaces_from_routes() {
        let config = parse(
            r#"
            kind = "http"
            base_url = "http://holdem-svc:8080"
            max_active_leases = 8
            [routes]
            "gameplay.holdem.act.v1" = "/strategy_act"
            "#,
        )
        .unwrap();
        assert_eq!(
            config.interfaces,
            vec!["gameplay.holdem.act.v1".to_string()]
        );
        assert_eq!(config.capacity.get("slot"), Some(&8));
        assert!(config.retryable_statuses.contains(&503));
        assert_eq!(
            config.attributes.get("protocol").map(String::as_str),
            Some("http")
        );
    }

    #[test]
    fn http_kind_requires_route_per_interface() {
        let error = parse(
            r#"
            kind = "http"
            base_url = "http://svc"
            interfaces = ["a.v1"]
            "#,
        )
        .unwrap_err();
        assert!(error.to_string().contains("requires a route"));
    }
}
