use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use anyserve_client::{
    AnyserveClient, EventKind, FrameKind, FrameWrite, JobRecord, ObjectRef, StreamDirection,
    StreamOpen, WorkerRegistration, object_ref,
};
use clap::Args;
use futures::StreamExt;
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue};
use serde::Deserialize;
use tokio::sync::Mutex;
use tokio::time::Instant;

const DEFAULT_POLL_INTERVAL_SECS: u64 = 1;
const DEFAULT_HEARTBEAT_INTERVAL_SECS: u64 = 5;
const DEFAULT_INPUT_STREAM_WAIT_SECS: u64 = 5;
const DEFAULT_UPSTREAM_TIMEOUT_SECS: u64 = 60;
const DEFAULT_OUTPUT_STREAM: &str = "output.default";
const DEFAULT_INPUT_STREAM: &str = "input.default";
const DEFAULT_WORKER_KIND: &str = "llm";

#[derive(Args, Debug)]
pub struct ServeOpenAIWorkerArgs {
    #[arg(long = "config", value_name = "PATH")]
    pub config: PathBuf,
    #[arg(long = "worker-id")]
    pub worker_id: Option<String>,
}

#[derive(Clone, Debug, Default)]
struct LeaseActivity {
    active_lease_id: Option<String>,
    active_leases: u32,
}

#[derive(Clone, Debug)]
struct ResolvedServeOpenAIWorkerConfig {
    kind: String,
    worker_id: Option<String>,
    provider: String,
    base_url: String,
    api_key: Option<String>,
    interfaces: Vec<String>,
    max_active_leases: u32,
    poll_interval: Duration,
    heartbeat_interval: Duration,
    input_stream_wait: Duration,
    upstream_timeout: Duration,
    attributes: HashMap<String, String>,
    capacity: HashMap<String, i64>,
    metadata: HashMap<String, String>,
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

pub async fn run(control_plane_endpoint: &str, args: ServeOpenAIWorkerArgs) -> Result<()> {
    let config = load_config(&args)?;
    let http_client = build_http_client(&config)?;
    let mut client = connect_client(control_plane_endpoint).await?;

    let mut registration = WorkerRegistration::new(config.interfaces.clone());
    registration.worker_id = config.worker_id.clone();
    registration.attributes = config.attributes.clone();
    registration.total_capacity = config.capacity.clone();
    registration.max_active_leases = config.max_active_leases;
    registration.metadata = config.metadata.clone();

    let worker = client.register_worker(registration).await?;
    let worker_id = worker.worker_id.clone();

    let activity = Arc::new(Mutex::new(LeaseActivity::default()));
    tokio::spawn(heartbeat_loop(
        control_plane_endpoint.to_string(),
        worker_id.clone(),
        config.clone(),
        activity.clone(),
    ));

    loop {
        let grant = match client.poll_lease(worker_id.clone()).await {
            Ok(grant) => grant,
            Err(error) => {
                tracing::warn!(worker_id = %worker_id, error = %error, "llm worker poll lease failed");
                tokio::time::sleep(config.poll_interval).await;
                client = match connect_client(control_plane_endpoint).await {
                    Ok(client) => client,
                    Err(error) => {
                        tracing::warn!(worker_id = %worker_id, error = %error, "llm worker reconnect failed");
                        continue;
                    }
                };
                continue;
            }
        };
        let Some(grant) = grant else {
            tokio::time::sleep(config.poll_interval).await;
            continue;
        };

        {
            let mut state = activity.lock().await;
            state.active_lease_id = Some(grant.lease.lease_id.clone());
            state.active_leases = 1;
        }

        let lease_id = grant.lease.lease_id.clone();
        let job_id = grant.job.job_id.clone();
        let result = process_grant(&mut client, &http_client, &config, &worker_id, grant.job).await;

        {
            let mut state = activity.lock().await;
            state.active_lease_id = None;
            state.active_leases = 0;
        }

        if let Err(error) = result {
            tracing::error!(worker_id = %worker_id, error = %error, "llm worker failed processing lease");
            if !lease_id.trim().is_empty() {
                let _ = client
                    .fail_lease(
                        worker_id.clone(),
                        lease_id,
                        format!("worker execution failed for job {job_id}: {error}"),
                        false,
                        HashMap::from([
                            ("provider".to_string(), config.provider.clone()),
                            ("worker.kind".to_string(), config.kind.clone()),
                        ]),
                    )
                    .await;
            }
        }
    }
}

fn load_config(args: &ServeOpenAIWorkerArgs) -> Result<ResolvedServeOpenAIWorkerConfig> {
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

fn build_http_client(config: &ResolvedServeOpenAIWorkerConfig) -> Result<reqwest::Client> {
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    if let Some(api_key) = config.api_key.as_ref() {
        let header = HeaderValue::from_str(&format!("Bearer {api_key}"))
            .context("build authorization header")?;
        headers.insert(AUTHORIZATION, header);
    }

    reqwest::Client::builder()
        .default_headers(headers)
        .timeout(config.upstream_timeout)
        .build()
        .context("build HTTP client")
}

async fn connect_client(control_plane_endpoint: &str) -> Result<AnyserveClient> {
    AnyserveClient::connect(control_plane_endpoint.to_string()).await
}

async fn heartbeat_loop(
    control_plane_endpoint: String,
    worker_id: String,
    config: ResolvedServeOpenAIWorkerConfig,
    activity: Arc<Mutex<LeaseActivity>>,
) {
    let mut interval = tokio::time::interval(config.heartbeat_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut client: Option<AnyserveClient> = None;

    loop {
        interval.tick().await;

        let snapshot = { activity.lock().await.clone() };
        if client.is_none() {
            client = match connect_client(&control_plane_endpoint).await {
                Ok(client) => Some(client),
                Err(error) => {
                    tracing::warn!(worker_id = %worker_id, error = %error, "llm worker heartbeat reconnect failed");
                    continue;
                }
            };
        }
        let Some(client_ref) = client.as_mut() else {
            continue;
        };

        if let Err(error) = client_ref
            .heartbeat_worker(
                worker_id.clone(),
                available_capacity(&config.capacity, snapshot.active_leases),
                snapshot.active_leases,
                config.metadata.clone(),
            )
            .await
        {
            tracing::warn!(worker_id = %worker_id, error = %error, "llm worker heartbeat failed");
            client = None;
            continue;
        }

        if let Some(lease_id) = snapshot.active_lease_id {
            if let Err(error) = client_ref
                .renew_lease(worker_id.clone(), lease_id.clone())
                .await
            {
                tracing::warn!(worker_id = %worker_id, lease_id = %lease_id, error = %error, "llm worker lease renew failed");
                client = None;
            }
        }
    }
}

async fn process_grant(
    client: &mut AnyserveClient,
    http_client: &reqwest::Client,
    config: &ResolvedServeOpenAIWorkerConfig,
    worker_id: &str,
    job: JobRecord,
) -> Result<()> {
    let lease_id = job.lease_id.clone();
    if lease_id.trim().is_empty() {
        bail!("job '{}' is missing lease_id", job.job_id);
    }

    let attempt_id = job.current_attempt_id.clone().trim().to_string();
    let spec = job
        .spec
        .clone()
        .context("job is missing spec for llm worker")?;
    let upstream_path = upstream_path(&spec.interface_name)?;
    let request_body = read_request_body(client, &job, config.input_stream_wait).await?;
    if request_body.is_empty() {
        bail!("job '{}' request body was empty", job.job_id);
    }

    client
        .report_event(
            worker_id.to_string(),
            lease_id.clone(),
            EventKind::Started,
            Vec::new(),
            HashMap::from([
                ("provider".to_string(), config.provider.clone()),
                ("worker.kind".to_string(), config.kind.clone()),
            ]),
        )
        .await?;

    let response = http_client
        .post(format!("{}/{}", config.base_url, upstream_path))
        .body(request_body)
        .send()
        .await
        .with_context(|| format!("proxy request to {}", config.base_url))?;

    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        client
            .fail_lease(
                worker_id.to_string(),
                lease_id,
                format!("upstream returned {}: {}", status, truncate(&body, 512)),
                false,
                HashMap::from([
                    ("provider".to_string(), config.provider.clone()),
                    ("worker.kind".to_string(), config.kind.clone()),
                ]),
            )
            .await?;
        return Ok(());
    }

    let content_type = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("application/json")
        .to_string();

    let output_stream = client
        .open_stream(
            StreamOpen::job(
                job.job_id.clone(),
                DEFAULT_OUTPUT_STREAM,
                StreamDirection::WorkerToClient,
            )
            .with_worker_id(worker_id.to_string())
            .with_lease_id(lease_id.clone())
            .with_attempt_id(attempt_id),
        )
        .await?;

    let mut persisted_output = Vec::new();
    if content_type.contains("text/event-stream") {
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.context("read upstream event stream chunk")?;
            if chunk.is_empty() {
                continue;
            }
            persisted_output.extend_from_slice(&chunk);
            client
                .push_frames(
                    output_stream.stream_id.clone(),
                    vec![
                        FrameWrite::data(chunk.to_vec()).with_metadata(HashMap::from([(
                            "content_type".to_string(),
                            content_type.clone(),
                        )])),
                    ],
                    Some(worker_id.to_string()),
                    Some(lease_id.clone()),
                )
                .await?;
        }
    } else {
        let body = response
            .bytes()
            .await
            .context("read upstream response body")?;
        persisted_output = body.to_vec();
        client
            .push_frames(
                output_stream.stream_id.clone(),
                vec![
                    FrameWrite::data(body.to_vec()).with_metadata(HashMap::from([(
                        "content_type".to_string(),
                        content_type.clone(),
                    )])),
                ],
                Some(worker_id.to_string()),
                Some(lease_id.clone()),
            )
            .await?;
    }

    let outputs = persisted_outputs(&content_type, persisted_output);

    client
        .close_stream(
            output_stream.stream_id.clone(),
            Some(worker_id.to_string()),
            Some(lease_id.clone()),
            HashMap::new(),
        )
        .await?;
    client
        .report_event(
            worker_id.to_string(),
            lease_id.clone(),
            EventKind::OutputReady,
            Vec::new(),
            HashMap::from([
                ("stream_name".to_string(), DEFAULT_OUTPUT_STREAM.to_string()),
                ("content_type".to_string(), content_type),
            ]),
        )
        .await?;
    client
        .complete_lease(worker_id.to_string(), lease_id, outputs, HashMap::new())
        .await?;

    Ok(())
}

async fn read_request_body(
    client: &mut AnyserveClient,
    job: &JobRecord,
    input_stream_wait: Duration,
) -> Result<Vec<u8>> {
    if let Some(spec) = job.spec.as_ref() {
        if !spec.params.is_empty() {
            return Ok(spec.params.clone());
        }
    }

    let deadline = Instant::now() + input_stream_wait;
    loop {
        let streams = client.list_streams(job.job_id.clone()).await?;
        if let Some(input_stream) = streams
            .into_iter()
            .find(|stream| stream.stream_name == DEFAULT_INPUT_STREAM)
        {
            let mut frames = client.pull_frames(input_stream.stream_id, 0, false).await?;
            let mut body = Vec::new();
            while let Some(frame) = frames.next().await {
                let frame = frame.context("read input frame")?;
                if frame.kind() == FrameKind::Data {
                    body.extend(frame.payload);
                }
            }
            if !body.is_empty() {
                return Ok(body);
            }
        }

        if Instant::now() >= deadline {
            break;
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    Ok(Vec::new())
}

fn upstream_path(interface_name: &str) -> Result<&'static str> {
    match interface_name {
        "llm.chat.v1" => Ok("chat/completions"),
        "llm.embed.v1" | "llm.embeddings.v1" => Ok("embeddings"),
        other => bail!("unsupported llm worker interface '{other}'"),
    }
}

fn available_capacity(total: &HashMap<String, i64>, active_leases: u32) -> HashMap<String, i64> {
    let mut available = total.clone();
    if let Some(slots) = available.get_mut("slot") {
        *slots = (*slots - i64::from(active_leases)).max(0);
    }
    available
}

fn truncate(value: &str, limit: usize) -> String {
    let chars = value.chars().collect::<Vec<_>>();
    if chars.len() <= limit {
        return value.to_string();
    }
    chars.into_iter().take(limit).collect::<String>()
}

fn persisted_outputs(content_type: &str, payload: Vec<u8>) -> Vec<ObjectRef> {
    if payload.is_empty() {
        return Vec::new();
    }

    vec![ObjectRef {
        reference: Some(object_ref::Reference::Inline(payload)),
        metadata: HashMap::from([("content_type".to_string(), content_type.to_string())]),
    }]
}

#[cfg(test)]
mod tests {
    use super::{available_capacity, persisted_outputs, truncate, upstream_path};
    use anyserve_client::object_ref;
    use std::collections::HashMap;

    #[test]
    fn llm_worker_maps_supported_interfaces() {
        assert_eq!(upstream_path("llm.chat.v1").unwrap(), "chat/completions");
        assert_eq!(upstream_path("llm.embed.v1").unwrap(), "embeddings");
        assert_eq!(upstream_path("llm.embeddings.v1").unwrap(), "embeddings");
    }

    #[test]
    fn llm_worker_reduces_slot_capacity_for_active_lease() {
        let available = available_capacity(&HashMap::from([("slot".to_string(), 4)]), 2);
        assert_eq!(available.get("slot").copied(), Some(2));
    }

    #[test]
    fn truncate_keeps_short_values() {
        assert_eq!(truncate("hello", 10), "hello");
    }

    #[test]
    fn persisted_outputs_store_inline_payload_and_content_type() {
        let outputs = persisted_outputs("application/json", br#"{"ok":true}"#.to_vec());
        assert_eq!(outputs.len(), 1);
        assert_eq!(
            outputs[0].reference,
            Some(object_ref::Reference::Inline(br#"{"ok":true}"#.to_vec()))
        );
        assert_eq!(
            outputs[0].metadata.get("content_type").map(String::as_str),
            Some("application/json")
        );
    }
}
