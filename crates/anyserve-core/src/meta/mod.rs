use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use anyserve_proto::inference::{InferCore, InferRequest};
use async_trait::async_trait;
use prost::Message;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};

use crate::consts::{
    DEFAULT_QUEUE, INFER_METADATA_STATUS, INFER_METADATA_STATUS_VALUE_QUEUED,
    INFER_METADATA_STATUS_VALUE_SCHEDULED, METADATA_TIMESTAMP,
};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Format {
    pub name: String,
    pub uuid: String,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct Queue {
    pub name: String,
    pub index: String,
    pub streaming: String,
    pub storage: String,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct QueueStats {
    pub queue: String,
    pub total: usize,
    pub queued: usize,
    pub scheduled: usize,
    pub processing: usize,
    pub completed: usize,
    pub failed: usize,
    pub other: HashMap<String, usize>,
}

#[async_trait]
pub trait MetaStore: Send + Sync {
    async fn init(&self, format: &Format, force: bool) -> Result<()>;
    async fn load(&self) -> Result<Format>;
    async fn queue_infer_request(
        &self,
        queue: &str,
        infer: &InferCore,
        request_id: &str,
    ) -> Result<()>;
    async fn pop_infer_request(
        &self,
        queue: &str,
        filters: &HashMap<String, String>,
    ) -> Result<Option<(String, InferCore)>>;
    async fn queue_send_response(&self, request_id: &str, response: &InferCore) -> Result<()>;
    async fn pop_infer_response(&self, request_id: &str) -> Result<Option<InferCore>>;
    async fn delete_infer_request(&self, request_id: &str) -> Result<()>;
    async fn set_infer_request_metadata(
        &self,
        request_id: &str,
        metadata: &HashMap<String, String>,
    ) -> Result<()>;
    async fn exists_infer_request(&self, request_id: &str) -> Result<bool>;
    async fn list_queues(&self) -> Result<Vec<Queue>>;
    async fn create_queue(&self, queue: Queue) -> Result<()>;
    async fn delete_queue(&self, queue_name: &str) -> Result<()>;
    async fn queue_stats(&self, queue_name: &str) -> Result<QueueStats>;
}

pub async fn connect(meta_uri: &str) -> Result<Arc<dyn MetaStore>> {
    let scheme = meta_uri
        .split_once("://")
        .map(|(scheme, _)| scheme)
        .ok_or_else(|| anyhow!("invalid meta uri: {meta_uri}"))?;

    match scheme {
        "redis" | "unix" => Ok(Arc::new(RedisMetaStore::from_uri(meta_uri)?)),
        _ => bail!("unsupported meta backend: {scheme}"),
    }
}

#[derive(Clone)]
pub struct RedisMetaStore {
    client: redis::Client,
}

impl RedisMetaStore {
    pub fn from_uri(meta_uri: &str) -> Result<Self> {
        let client = redis::Client::open(meta_uri)
            .with_context(|| format!("invalid redis uri: {meta_uri}"))?;
        Ok(Self { client })
    }

    async fn connection(&self) -> Result<redis::aio::MultiplexedConnection> {
        self.client
            .get_multiplexed_async_connection()
            .await
            .context("connect redis")
    }

    fn setting_key(&self) -> &'static str {
        "setting"
    }

    fn queues_key(&self) -> &'static str {
        "queues"
    }

    fn queue_definition_key(&self, queue: &str) -> String {
        format!("queue:{queue}")
    }

    fn request_queue_key(&self, request_id: &str) -> String {
        format!("request_queue:{request_id}")
    }

    fn request_payload_key(&self, queue: &str, request_id: &str) -> String {
        format!("queue:{queue}:request:{request_id}")
    }

    fn request_meta_key(&self, queue: &str, request_id: &str) -> String {
        format!("queue:{queue}:meta:{request_id}")
    }

    fn response_key(&self, queue: &str, request_id: &str) -> String {
        format!("queue:{queue}:response:{request_id}")
    }

    fn queue_meta_pattern(&self, queue: &str) -> String {
        format!("queue:{queue}:meta:*")
    }

    fn queue_pattern(&self, queue: &str) -> String {
        format!("queue:{queue}:*")
    }

    async fn queue_exists(&self, queue: &str) -> Result<bool> {
        let mut conn = self.connection().await?;
        conn.sismember(self.queues_key(), queue)
            .await
            .context("check queue exists")
    }

    async fn resolve_queue_for_request(&self, request_id: &str) -> Result<String> {
        let mut conn = self.connection().await?;
        let queue: Option<String> = conn
            .get(self.request_queue_key(request_id))
            .await
            .context("resolve request queue")?;
        queue.ok_or_else(|| anyhow!("infer request not found: {request_id}"))
    }

    async fn scan_keys(&self, pattern: &str) -> Result<Vec<String>> {
        let mut conn = self.connection().await?;
        let mut cursor = 0_u64;
        let mut keys = Vec::new();

        loop {
            let (next, batch): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(pattern)
                .arg("COUNT")
                .arg(200)
                .query_async(&mut conn)
                .await
                .with_context(|| format!("scan keys with pattern {pattern}"))?;
            keys.extend(batch);
            if next == 0 {
                break;
            }
            cursor = next;
        }

        Ok(keys)
    }

    async fn set_hash_fields(&self, key: &str, values: &HashMap<String, String>) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        let mut conn = self.connection().await?;
        let mut cmd = redis::cmd("HSET");
        cmd.arg(key);
        for (field, value) in values {
            cmd.arg(field).arg(value);
        }
        let _: usize = cmd.query_async(&mut conn).await.context("hset fields")?;
        Ok(())
    }

    async fn delete_keys(&self, keys: &[String]) -> Result<()> {
        if keys.is_empty() {
            return Ok(());
        }

        let mut conn = self.connection().await?;
        let _: usize = conn.del(keys.to_vec()).await.context("delete keys")?;
        Ok(())
    }
}

#[async_trait]
impl MetaStore for RedisMetaStore {
    async fn init(&self, format: &Format, force: bool) -> Result<()> {
        let mut conn = self.connection().await?;
        let existing: Option<String> =
            conn.get(self.setting_key()).await.context("load setting")?;

        if existing.is_some() && !force {
            bail!("backend already initialized; pass --force to overwrite");
        }

        let body = serde_json::to_string_pretty(format).context("serialize setting")?;
        let _: () = conn
            .set(self.setting_key(), body)
            .await
            .context("save setting")?;
        Ok(())
    }

    async fn load(&self) -> Result<Format> {
        let mut conn = self.connection().await?;
        let body: Option<String> = conn.get(self.setting_key()).await.context("load setting")?;
        let body = body.ok_or_else(|| {
            anyhow!("backend is not formatted, please run `anyserve init ...` first")
        })?;
        serde_json::from_str(&body).context("parse setting")
    }

    async fn queue_infer_request(
        &self,
        queue: &str,
        infer: &InferCore,
        request_id: &str,
    ) -> Result<()> {
        let queue = if queue.is_empty() {
            DEFAULT_QUEUE
        } else {
            queue
        };
        if !self.queue_exists(queue).await? {
            bail!("queue '{queue}' does not exist");
        }

        let mut metadata = infer.metadata.clone();
        metadata
            .entry(METADATA_TIMESTAMP.to_string())
            .or_insert_with(|| now_nanos().to_string());
        metadata
            .entry(INFER_METADATA_STATUS.to_string())
            .or_insert_with(|| INFER_METADATA_STATUS_VALUE_QUEUED.to_string());

        let mut conn = self.connection().await?;
        let _: () = conn
            .set(self.request_queue_key(request_id), queue)
            .await
            .context("save request queue mapping")?;
        let _: () = conn
            .set(
                self.request_payload_key(queue, request_id),
                infer.content.clone(),
            )
            .await
            .context("save request payload")?;
        drop(conn);

        self.set_hash_fields(&self.request_meta_key(queue, request_id), &metadata)
            .await
    }

    async fn pop_infer_request(
        &self,
        queue: &str,
        filters: &HashMap<String, String>,
    ) -> Result<Option<(String, InferCore)>> {
        let queue = if queue.is_empty() {
            DEFAULT_QUEUE
        } else {
            queue
        };
        let meta_keys = self.scan_keys(&self.queue_meta_pattern(queue)).await?;
        let mut best: Option<(String, String, HashMap<String, String>, u128)> = None;
        let mut conn = self.connection().await?;

        for meta_key in meta_keys {
            let metadata: HashMap<String, String> = conn
                .hgetall(&meta_key)
                .await
                .context("load request metadata")?;
            if metadata.get(INFER_METADATA_STATUS).map(String::as_str)
                != Some(INFER_METADATA_STATUS_VALUE_QUEUED)
            {
                continue;
            }
            if !matches_filters(&metadata, filters) {
                continue;
            }

            let timestamp = metadata
                .get(METADATA_TIMESTAMP)
                .and_then(|value| value.parse::<u128>().ok())
                .unwrap_or(u128::MAX);
            let request_id = meta_key
                .rsplit(':')
                .next()
                .ok_or_else(|| anyhow!("invalid meta key: {meta_key}"))?
                .to_string();

            let should_replace = best
                .as_ref()
                .map(|(_, _, _, best_ts)| timestamp < *best_ts)
                .unwrap_or(true);
            if should_replace {
                best = Some((request_id, meta_key, metadata, timestamp));
            }
        }

        let Some((request_id, meta_key, mut metadata, _)) = best else {
            return Ok(None);
        };

        let payload: Vec<u8> = conn
            .get(self.request_payload_key(queue, &request_id))
            .await
            .with_context(|| format!("load request payload for {request_id}"))?;
        drop(conn);

        metadata.insert(
            INFER_METADATA_STATUS.to_string(),
            INFER_METADATA_STATUS_VALUE_SCHEDULED.to_string(),
        );
        self.set_hash_fields(
            &meta_key,
            &HashMap::from([(
                INFER_METADATA_STATUS.to_string(),
                INFER_METADATA_STATUS_VALUE_SCHEDULED.to_string(),
            )]),
        )
        .await?;

        Ok(Some((
            request_id,
            InferCore {
                content: payload,
                metadata,
            },
        )))
    }

    async fn queue_send_response(&self, request_id: &str, response: &InferCore) -> Result<()> {
        let queue = self.resolve_queue_for_request(request_id).await?;
        let mut conn = self.connection().await?;
        let payload = response.encode_to_vec();
        let _: usize = conn
            .rpush(self.response_key(&queue, request_id), payload)
            .await
            .context("queue response")?;
        Ok(())
    }

    async fn pop_infer_response(&self, request_id: &str) -> Result<Option<InferCore>> {
        let queue = self.resolve_queue_for_request(request_id).await?;
        let mut conn = self.connection().await?;
        let payload: Option<Vec<u8>> = redis::cmd("LPOP")
            .arg(self.response_key(&queue, request_id))
            .query_async(&mut conn)
            .await
            .context("pop response payload")?;

        payload
            .map(|payload| InferCore::decode(payload.as_slice()).context("decode response payload"))
            .transpose()
    }

    async fn delete_infer_request(&self, request_id: &str) -> Result<()> {
        let queue = self.resolve_queue_for_request(request_id).await?;
        self.delete_keys(&[
            self.request_queue_key(request_id),
            self.request_payload_key(&queue, request_id),
            self.request_meta_key(&queue, request_id),
            self.response_key(&queue, request_id),
        ])
        .await
    }

    async fn set_infer_request_metadata(
        &self,
        request_id: &str,
        metadata: &HashMap<String, String>,
    ) -> Result<()> {
        let queue = self.resolve_queue_for_request(request_id).await?;
        self.set_hash_fields(&self.request_meta_key(&queue, request_id), metadata)
            .await
    }

    async fn exists_infer_request(&self, request_id: &str) -> Result<bool> {
        let queue = match self.resolve_queue_for_request(request_id).await {
            Ok(queue) => queue,
            Err(_) => return Ok(false),
        };

        let mut conn = self.connection().await?;
        let request_exists: bool = conn
            .exists(self.request_payload_key(&queue, request_id))
            .await
            .context("check request payload")?;
        let meta_exists: bool = conn
            .exists(self.request_meta_key(&queue, request_id))
            .await
            .context("check request metadata")?;
        Ok(request_exists && meta_exists)
    }

    async fn list_queues(&self) -> Result<Vec<Queue>> {
        let mut conn = self.connection().await?;
        let queue_names: Vec<String> = conn
            .smembers(self.queues_key())
            .await
            .context("list queues")?;
        let mut queues = Vec::with_capacity(queue_names.len());

        for name in queue_names {
            let details: HashMap<String, String> = conn
                .hgetall(self.queue_definition_key(&name))
                .await
                .with_context(|| format!("load queue definition for {name}"))?;
            queues.push(Queue {
                name,
                index: details.get("index").cloned().unwrap_or_default(),
                streaming: details.get("streaming").cloned().unwrap_or_default(),
                storage: details.get("storage").cloned().unwrap_or_default(),
            });
        }

        queues.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(queues)
    }

    async fn create_queue(&self, queue: Queue) -> Result<()> {
        if queue.name.is_empty() {
            bail!("queue name cannot be empty");
        }
        if self.queue_exists(&queue.name).await? {
            bail!("queue '{}' already exists", queue.name);
        }

        let mut conn = self.connection().await?;
        let _: usize = conn
            .sadd(self.queues_key(), &queue.name)
            .await
            .with_context(|| format!("register queue {}", queue.name))?;
        let _: usize = redis::cmd("HSET")
            .arg(self.queue_definition_key(&queue.name))
            .arg("index")
            .arg(&queue.index)
            .arg("streaming")
            .arg(&queue.streaming)
            .arg("storage")
            .arg(&queue.storage)
            .query_async(&mut conn)
            .await
            .with_context(|| format!("save queue definition for {}", queue.name))?;
        Ok(())
    }

    async fn delete_queue(&self, queue_name: &str) -> Result<()> {
        if !self.queue_exists(queue_name).await? {
            bail!("queue '{queue_name}' does not exist");
        }

        let meta_keys = self.scan_keys(&self.queue_meta_pattern(queue_name)).await?;
        let mut delete_keys = self.scan_keys(&self.queue_pattern(queue_name)).await?;
        for meta_key in &meta_keys {
            if let Some(request_id) = meta_key.rsplit(':').next() {
                delete_keys.push(self.request_queue_key(request_id));
            }
        }
        delete_keys.push(self.queue_definition_key(queue_name));

        self.delete_keys(&delete_keys).await?;

        let mut conn = self.connection().await?;
        let _: usize = conn
            .srem(self.queues_key(), queue_name)
            .await
            .with_context(|| format!("unregister queue {queue_name}"))?;
        Ok(())
    }

    async fn queue_stats(&self, queue_name: &str) -> Result<QueueStats> {
        if !self.queue_exists(queue_name).await? {
            bail!("queue '{queue_name}' does not exist");
        }

        let meta_keys = self.scan_keys(&self.queue_meta_pattern(queue_name)).await?;
        let mut conn = self.connection().await?;
        let mut stats = QueueStats {
            queue: queue_name.to_string(),
            total: meta_keys.len(),
            ..QueueStats::default()
        };

        for key in meta_keys {
            let status: Option<String> = conn
                .hget(&key, INFER_METADATA_STATUS)
                .await
                .with_context(|| format!("load status for {key}"))?;
            match status.as_deref() {
                Some("queued") => stats.queued += 1,
                Some("scheduled") => stats.scheduled += 1,
                Some("processing") => stats.processing += 1,
                Some("completed") => stats.completed += 1,
                Some("failed") => stats.failed += 1,
                Some(other) => {
                    *stats.other.entry(other.to_string()).or_insert(0) += 1;
                }
                None => {
                    *stats.other.entry("missing".to_string()).or_insert(0) += 1;
                }
            }
        }

        Ok(stats)
    }
}

fn matches_filters(metadata: &HashMap<String, String>, filters: &HashMap<String, String>) -> bool {
    filters.iter().all(|(key, value)| {
        metadata_value(metadata, key)
            .map(|candidate| candidate == value)
            .unwrap_or(false)
    })
}

fn metadata_value<'a>(metadata: &'a HashMap<String, String>, key: &str) -> Option<&'a str> {
    metadata
        .get(key)
        .or_else(|| {
            if key.starts_with('@') {
                metadata.get(key.trim_start_matches('@'))
            } else {
                metadata.get(&format!("@{key}"))
            }
        })
        .map(String::as_str)
}

fn now_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default()
}

pub fn normalize_queue(queue: Option<String>) -> String {
    queue.unwrap_or_else(|| DEFAULT_QUEUE.to_string())
}

pub fn queue_from_request(request: &InferRequest) -> String {
    normalize_queue(request.queue.clone())
}
