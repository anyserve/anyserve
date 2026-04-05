use std::collections::BTreeMap;

use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use redis::Script;
use redis::streams::{StreamRangeReply, StreamReadOptions, StreamReadReply};
use redis::{AsyncCommands, RedisResult};
use tokio::sync::{Mutex, OnceCell, watch};
use tokio::time::{Duration, timeout};

use crate::model::{Attributes, Frame, FrameKind, StreamRecord};

#[async_trait]
pub trait FramePlane: Send + Sync {
    async fn stream_created(&self, stream: &StreamRecord) -> Result<()>;
    async fn stream_updated(&self, stream: &StreamRecord) -> Result<()>;
    async fn append_frame(
        &self,
        stream: &StreamRecord,
        kind: FrameKind,
        payload: Vec<u8>,
        metadata: Attributes,
        now_ms: u64,
    ) -> Result<Frame>;
    async fn frames_after(&self, stream_id: &str, after_sequence: u64) -> Result<Vec<Frame>>;
    async fn wait_for_frames(
        &self,
        stream_id: &str,
        after_sequence: u64,
        timeout: Duration,
    ) -> Result<Vec<Frame>>;
    async fn latest_sequence(&self, stream_id: &str) -> Result<Option<u64>>;
    async fn subscribe_stream_updates(&self, stream_id: &str) -> watch::Receiver<u64>;
}

#[derive(Default)]
struct MemoryFrames {
    frames: BTreeMap<String, Vec<Frame>>,
    last_sequences: BTreeMap<String, u64>,
}

#[derive(Default)]
struct MemoryFrameWatchers {
    stream_updates: BTreeMap<String, watch::Sender<u64>>,
}

pub struct MemoryFramePlane {
    inner: Mutex<MemoryFrames>,
    watchers: Mutex<MemoryFrameWatchers>,
}

impl MemoryFramePlane {
    pub fn new() -> Self {
        Self::default()
    }

    async fn notify_stream_updates(&self, stream_id: &str) {
        let mut watchers = self.watchers.lock().await;
        bump_version(
            watchers
                .stream_updates
                .entry(stream_id.to_string())
                .or_insert_with(version_sender),
        );
    }
}

impl Default for MemoryFramePlane {
    fn default() -> Self {
        Self {
            inner: Mutex::new(MemoryFrames::default()),
            watchers: Mutex::new(MemoryFrameWatchers::default()),
        }
    }
}

#[async_trait]
impl FramePlane for MemoryFramePlane {
    async fn stream_created(&self, stream: &StreamRecord) -> Result<()> {
        let mut inner = self.inner.lock().await;
        inner
            .last_sequences
            .entry(stream.stream_id.clone())
            .or_insert(stream.last_sequence);
        Ok(())
    }

    async fn stream_updated(&self, stream: &StreamRecord) -> Result<()> {
        let mut inner = self.inner.lock().await;
        inner
            .last_sequences
            .insert(stream.stream_id.clone(), stream.last_sequence);
        drop(inner);
        self.notify_stream_updates(&stream.stream_id).await;
        Ok(())
    }

    async fn append_frame(
        &self,
        stream: &StreamRecord,
        kind: FrameKind,
        payload: Vec<u8>,
        metadata: Attributes,
        now_ms: u64,
    ) -> Result<Frame> {
        let frame = {
            let mut inner = self.inner.lock().await;
            let sequence = inner
                .last_sequences
                .get(&stream.stream_id)
                .copied()
                .unwrap_or(stream.last_sequence)
                + 1;
            inner
                .last_sequences
                .insert(stream.stream_id.clone(), sequence);
            let frame = Frame {
                stream_id: stream.stream_id.clone(),
                sequence,
                kind,
                payload,
                metadata,
                created_at_ms: now_ms,
            };
            inner
                .frames
                .entry(stream.stream_id.clone())
                .or_default()
                .push(frame.clone());
            frame
        };
        self.notify_stream_updates(&stream.stream_id).await;
        Ok(frame)
    }

    async fn frames_after(&self, stream_id: &str, after_sequence: u64) -> Result<Vec<Frame>> {
        let inner = self.inner.lock().await;
        Ok(inner
            .frames
            .get(stream_id)
            .into_iter()
            .flatten()
            .filter(|frame| frame.sequence > after_sequence)
            .cloned()
            .collect())
    }

    async fn wait_for_frames(
        &self,
        stream_id: &str,
        after_sequence: u64,
        wait_timeout: Duration,
    ) -> Result<Vec<Frame>> {
        let mut updates = self.subscribe_stream_updates(stream_id).await;
        let existing = self.frames_after(stream_id, after_sequence).await?;
        if !existing.is_empty() {
            return Ok(existing);
        }

        match timeout(wait_timeout, updates.changed()).await {
            Ok(Ok(())) => self.frames_after(stream_id, after_sequence).await,
            Ok(Err(_)) | Err(_) => Ok(Vec::new()),
        }
    }

    async fn latest_sequence(&self, stream_id: &str) -> Result<Option<u64>> {
        let inner = self.inner.lock().await;
        Ok(inner.last_sequences.get(stream_id).copied())
    }

    async fn subscribe_stream_updates(&self, stream_id: &str) -> watch::Receiver<u64> {
        let mut watchers = self.watchers.lock().await;
        watchers
            .stream_updates
            .entry(stream_id.to_string())
            .or_insert_with(version_sender)
            .subscribe()
    }
}

fn version_sender() -> watch::Sender<u64> {
    let (sender, _) = watch::channel(0);
    sender
}

fn bump_version(sender: &watch::Sender<u64>) {
    let next = sender.borrow().saturating_add(1);
    sender.send_replace(next);
}

pub struct RedisFramePlane {
    client: redis::Client,
    closed_retention_secs: u64,
    shared_connection: OnceCell<redis::aio::MultiplexedConnection>,
    watchers: Mutex<MemoryFrameWatchers>,
}

impl RedisFramePlane {
    pub fn new(redis_url: &str, closed_retention_secs: u64) -> Result<Self> {
        Ok(Self {
            client: redis::Client::open(redis_url)
                .with_context(|| format!("open redis frame plane client {redis_url}"))?,
            closed_retention_secs,
            shared_connection: OnceCell::new(),
            watchers: Mutex::new(MemoryFrameWatchers::default()),
        })
    }

    fn frames_key(stream_id: &str) -> String {
        format!("anyserve:stream:{stream_id}:frames")
    }

    fn meta_key(stream_id: &str) -> String {
        format!("anyserve:stream:{stream_id}:meta")
    }

    fn seq_key(stream_id: &str) -> String {
        format!("anyserve:stream:{stream_id}:seq")
    }

    async fn notify_stream_updates(&self, stream_id: &str) {
        let mut watchers = self.watchers.lock().await;
        bump_version(
            watchers
                .stream_updates
                .entry(stream_id.to_string())
                .or_insert_with(version_sender),
        );
    }

    async fn shared_connection(&self) -> Result<redis::aio::MultiplexedConnection> {
        let connection = self
            .shared_connection
            .get_or_try_init(|| async {
                self.client
                    .get_multiplexed_async_connection()
                    .await
                    .context("connect shared redis frame plane connection")
            })
            .await?;
        Ok(connection.clone())
    }
}

#[async_trait]
impl FramePlane for RedisFramePlane {
    async fn stream_created(&self, stream: &StreamRecord) -> Result<()> {
        let mut conn = self.shared_connection().await?;
        let meta_key = Self::meta_key(&stream.stream_id);
        let seq_key = Self::seq_key(&stream.stream_id);

        redis::pipe()
            .cmd("HSETNX")
            .arg(&meta_key)
            .arg("last_sequence")
            .arg(stream.last_sequence)
            .ignore()
            .cmd("SETNX")
            .arg(&seq_key)
            .arg(stream.last_sequence)
            .ignore()
            .query_async::<()>(&mut conn)
            .await
            .with_context(|| format!("initialize redis stream '{}'", stream.stream_id))?;
        Ok(())
    }

    async fn stream_updated(&self, stream: &StreamRecord) -> Result<()> {
        let mut conn = self.shared_connection().await?;
        let meta_key = Self::meta_key(&stream.stream_id);
        let frames_key = Self::frames_key(&stream.stream_id);
        let seq_key = Self::seq_key(&stream.stream_id);

        let mut pipe = redis::pipe();
        pipe.cmd("HSET")
            .arg(&meta_key)
            .arg("last_sequence")
            .arg(stream.last_sequence)
            .ignore();
        if stream.state.is_terminal() && self.closed_retention_secs > 0 {
            pipe.cmd("EXPIRE")
                .arg(&meta_key)
                .arg(self.closed_retention_secs)
                .ignore()
                .cmd("EXPIRE")
                .arg(&frames_key)
                .arg(self.closed_retention_secs)
                .ignore()
                .cmd("EXPIRE")
                .arg(&seq_key)
                .arg(self.closed_retention_secs)
                .ignore();
        }
        pipe.query_async::<()>(&mut conn)
            .await
            .with_context(|| format!("update redis stream metadata '{}'", stream.stream_id))?;
        self.notify_stream_updates(&stream.stream_id).await;
        Ok(())
    }

    async fn append_frame(
        &self,
        stream: &StreamRecord,
        kind: FrameKind,
        payload: Vec<u8>,
        metadata: Attributes,
        now_ms: u64,
    ) -> Result<Frame> {
        let mut conn = self.shared_connection().await?;
        let seq_key = Self::seq_key(&stream.stream_id);
        let meta_key = Self::meta_key(&stream.stream_id);
        let frames_key = Self::frames_key(&stream.stream_id);
        let script = Script::new(
            r#"
local seq = redis.call("INCR", KEYS[1])
redis.call("HSET", KEYS[2], "last_sequence", seq)
redis.call("XADD", KEYS[3], seq .. "-0",
  "kind", ARGV[1],
  "payload", ARGV[2],
  "metadata", ARGV[3],
  "created_at_ms", ARGV[4])
return seq
"#,
        );
        let metadata_json = serde_json::to_string(&metadata).context("serialize frame metadata")?;
        let kind_json = serde_json::to_value(kind).context("serialize frame kind")?;
        let kind_text = kind_json
            .as_str()
            .ok_or_else(|| anyhow!("frame kind did not serialize as string"))?
            .to_string();
        let sequence: i64 = script
            .key(seq_key)
            .key(meta_key)
            .key(frames_key)
            .arg(kind_text)
            .arg(payload.clone())
            .arg(metadata_json)
            .arg(now_ms)
            .invoke_async(&mut conn)
            .await
            .with_context(|| format!("append redis frame for stream '{}'", stream.stream_id))?;
        let appended = Frame {
            stream_id: stream.stream_id.clone(),
            sequence: u64::try_from(sequence).context("frame sequence did not fit in u64")?,
            kind,
            payload,
            metadata,
            created_at_ms: now_ms,
        };
        self.notify_stream_updates(&stream.stream_id).await;
        Ok(appended)
    }

    async fn frames_after(&self, stream_id: &str, after_sequence: u64) -> Result<Vec<Frame>> {
        let mut conn = self.shared_connection().await?;
        let reply = read_frames_after(&mut conn, stream_id, after_sequence)
            .await
            .with_context(|| format!("read redis frames for stream '{stream_id}'"))?;

        reply_to_frames(stream_id, reply)
    }

    async fn wait_for_frames(
        &self,
        stream_id: &str,
        after_sequence: u64,
        wait_timeout: Duration,
    ) -> Result<Vec<Frame>> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .with_context(|| format!("connect redis for stream '{stream_id}'"))?;

        let reply: StreamReadReply = conn
            .xread_options(
                &[Self::frames_key(stream_id)],
                &[format!("{after_sequence}-0")],
                &StreamReadOptions::default()
                    .block(wait_timeout.as_millis() as usize)
                    .count(128),
            )
            .await
            .with_context(|| format!("block waiting for redis frames '{stream_id}'"))?;
        if reply.keys.is_empty() {
            return Ok(Vec::new());
        }

        let key = reply
            .keys
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("redis xread reply missing stream key"))?;
        let range = StreamRangeReply { ids: key.ids };
        reply_to_frames(stream_id, range)
    }

    async fn latest_sequence(&self, stream_id: &str) -> Result<Option<u64>> {
        let mut conn = self.shared_connection().await?;
        let value: RedisResult<Option<u64>> =
            conn.hget(Self::meta_key(stream_id), "last_sequence").await;
        value.with_context(|| format!("read last_sequence for redis stream '{stream_id}'"))
    }

    async fn subscribe_stream_updates(&self, stream_id: &str) -> watch::Receiver<u64> {
        let mut watchers = self.watchers.lock().await;
        watchers
            .stream_updates
            .entry(stream_id.to_string())
            .or_insert_with(version_sender)
            .subscribe()
    }
}

async fn read_frames_after(
    conn: &mut redis::aio::MultiplexedConnection,
    stream_id: &str,
    after_sequence: u64,
) -> Result<StreamRangeReply> {
    let start = format!("({after_sequence}-0");
    redis::cmd("XRANGE")
        .arg(RedisFramePlane::frames_key(stream_id))
        .arg(start)
        .arg("+")
        .query_async(conn)
        .await
        .context("query redis stream range")
}

fn reply_to_frames(stream_id: &str, reply: StreamRangeReply) -> Result<Vec<Frame>> {
    let mut frames = Vec::with_capacity(reply.ids.len());
    for entry in reply.ids {
        let sequence = entry
            .id
            .split_once('-')
            .and_then(|(left, _)| left.parse::<u64>().ok())
            .ok_or_else(|| anyhow!("invalid redis stream id '{}'", entry.id))?;
        let kind_text: String = entry
            .get("kind")
            .ok_or_else(|| anyhow!("redis frame '{}' missing kind", entry.id))?;
        let kind = serde_json::from_value(serde_json::Value::String(kind_text))
            .context("decode frame kind")?;
        let payload: Vec<u8> = entry
            .get("payload")
            .ok_or_else(|| anyhow!("redis frame '{}' missing payload", entry.id))?;
        let metadata_text: String = entry
            .get("metadata")
            .ok_or_else(|| anyhow!("redis frame '{}' missing metadata", entry.id))?;
        let metadata: Attributes =
            serde_json::from_str(&metadata_text).context("decode frame metadata")?;
        let created_at_ms: u64 = entry
            .get("created_at_ms")
            .ok_or_else(|| anyhow!("redis frame '{}' missing created_at_ms", entry.id))?;
        frames.push(Frame {
            stream_id: stream_id.to_string(),
            sequence,
            kind,
            payload,
            metadata,
            created_at_ms,
        });
    }
    Ok(frames)
}
