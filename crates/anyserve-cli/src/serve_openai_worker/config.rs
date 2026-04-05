use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use clap::Args;
use serde::Deserialize;

const DEFAULT_POLL_INTERVAL_SECS: u64 = 1;
const DEFAULT_HEARTBEAT_INTERVAL_SECS: u64 = 5;
const DEFAULT_INPUT_STREAM_WAIT_SECS: u64 = 5;
const DEFAULT_UPSTREAM_TIMEOUT_SECS: u64 = 60;
const DEFAULT_WORKER_KIND: &str = "llm";

#[derive(Args, Debug)]
pub struct ServeOpenAIWorkerArgs {
    #[arg(long = "config", value_name = "PATH")]
    pub config: PathBuf,
    #[arg(long = "worker-id")]
    pub worker_id: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct LeaseActivity {
    pub(crate) active_lease_id: Option<String>,
    pub(crate) active_leases: u32,
}

#[derive(Clone, Debug)]
pub(crate) struct ResolvedServeOpenAIWorkerConfig {
    pub(crate) kind: String,
    pub(crate) worker_id: Option<String>,
    pub(crate) provider: String,
    pub(crate) base_url: String,
    pub(crate) api_key: Option<String>,
    pub(crate) interfaces: Vec<String>,
    pub(crate) max_active_leases: u32,
    pub(crate) poll_interval: Duration,
    pub(crate) heartbeat_interval: Duration,
    pub(crate) input_stream_wait: Duration,
    pub(crate) upstream_timeout: Duration,
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
    interfaces: Vec<String>,
    #[serde(default)]
    worker_id: Option<String>,
    #[serde(default)]
    max_active_leases: Option<u32>,
    #[serde(default)]
    poll_interval_secs: Option<u64>,
    #[serde(default)]
    heartbeat_interval_secs: Option<u64>,
    #[serde(default)]
    input_stream_wait_secs: Option<u64>,
    #[serde(default)]
    upstream_timeout_secs: Option<u64>,
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

    let kind = file.kind.unwrap_or_else(|| DEFAULT_WORKER_KIND.to_string());
    if kind != DEFAULT_WORKER_KIND {
        bail!(
            "unsupported worker kind '{kind}' in {}; expected '{DEFAULT_WORKER_KIND}'",
            args.config.display()
        );
    }

    let api_key = if let Some(value) = file.api_key {
        Some(value)
    } else if let Some(name) = file.api_key_env {
        Some(std::env::var(&name).with_context(|| format!("read api key from env var {name}"))?)
    } else {
        None
    };

    let mut attributes = file.attributes;
    let provider = file
        .provider
        .unwrap_or_else(|| "openai-compatible".to_string());
    attributes
        .entry("family".to_string())
        .or_insert_with(|| "llm".to_string());
    attributes
        .entry("protocol".to_string())
        .or_insert_with(|| "openai-compatible".to_string());
    attributes
        .entry("provider".to_string())
        .or_insert_with(|| provider.clone());

    let mut capacity = file.capacity;
    capacity.entry("slot".to_string()).or_insert(1);

    let max_active_leases = file.max_active_leases.unwrap_or(1);
    if max_active_leases != 1 {
        bail!(
            "worker config {} only supports max_active_leases = 1 right now",
            args.config.display()
        );
    }

    let interfaces = if file.interfaces.is_empty() {
        vec!["llm.chat.v1".to_string(), "llm.embed.v1".to_string()]
    } else {
        file.interfaces
    };

    Ok(ResolvedServeOpenAIWorkerConfig {
        kind,
        worker_id: args.worker_id.clone().or(file.worker_id),
        provider,
        base_url: file.base_url.trim_end_matches('/').to_string(),
        api_key,
        interfaces,
        max_active_leases,
        poll_interval: Duration::from_secs(
            file.poll_interval_secs
                .unwrap_or(DEFAULT_POLL_INTERVAL_SECS),
        ),
        heartbeat_interval: Duration::from_secs(
            file.heartbeat_interval_secs
                .unwrap_or(DEFAULT_HEARTBEAT_INTERVAL_SECS),
        ),
        input_stream_wait: Duration::from_secs(
            file.input_stream_wait_secs
                .unwrap_or(DEFAULT_INPUT_STREAM_WAIT_SECS),
        ),
        upstream_timeout: Duration::from_secs(
            file.upstream_timeout_secs
                .unwrap_or(DEFAULT_UPSTREAM_TIMEOUT_SECS),
        ),
        attributes,
        capacity,
        metadata: file.metadata,
    })
}
