use std::future::pending;

use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use futures::StreamExt;
use redis::AsyncCommands;
use tokio::sync::OnceCell;

pub enum ClusterSubscription {
    Noop,
    Redis(redis::aio::PubSubStream),
}

impl ClusterSubscription {
    pub fn noop() -> Self {
        Self::Noop
    }

    pub async fn changed(&mut self) -> Result<()> {
        match self {
            Self::Noop => pending::<Result<()>>().await,
            Self::Redis(stream) => {
                stream
                    .next()
                    .await
                    .ok_or_else(|| anyhow!("cluster notifier stream closed"))?;
                Ok(())
            }
        }
    }
}

#[async_trait]
pub trait ClusterNotifier: Send + Sync {
    async fn publish_job_events(&self, job_id: &str) -> Result<()>;
    async fn publish_job_streams(&self, job_id: &str) -> Result<()>;
    async fn subscribe_job_events(&self, job_id: &str) -> Result<ClusterSubscription>;
    async fn subscribe_job_streams(&self, job_id: &str) -> Result<ClusterSubscription>;
}

pub struct NoopClusterNotifier;

#[async_trait]
impl ClusterNotifier for NoopClusterNotifier {
    async fn publish_job_events(&self, _job_id: &str) -> Result<()> {
        Ok(())
    }

    async fn publish_job_streams(&self, _job_id: &str) -> Result<()> {
        Ok(())
    }

    async fn subscribe_job_events(&self, _job_id: &str) -> Result<ClusterSubscription> {
        Ok(ClusterSubscription::Noop)
    }

    async fn subscribe_job_streams(&self, _job_id: &str) -> Result<ClusterSubscription> {
        Ok(ClusterSubscription::Noop)
    }
}

pub struct RedisClusterNotifier {
    client: redis::Client,
    publisher_connection: OnceCell<redis::aio::MultiplexedConnection>,
}

impl RedisClusterNotifier {
    pub fn new(redis_url: &str) -> Result<Self> {
        Ok(Self {
            client: redis::Client::open(redis_url)
                .with_context(|| format!("open redis cluster notifier client {redis_url}"))?,
            publisher_connection: OnceCell::new(),
        })
    }

    fn job_events_channel(job_id: &str) -> String {
        format!("anyserve:notify:job-events:{job_id}")
    }

    fn job_streams_channel(job_id: &str) -> String {
        format!("anyserve:notify:job-streams:{job_id}")
    }

    async fn publisher_connection(&self) -> Result<redis::aio::MultiplexedConnection> {
        let connection = self
            .publisher_connection
            .get_or_try_init(|| async {
                self.client
                    .get_multiplexed_async_connection()
                    .await
                    .context("connect shared redis notifier publisher")
            })
            .await?;
        Ok(connection.clone())
    }

    async fn publish(&self, channel: String) -> Result<()> {
        let mut conn = self.publisher_connection().await?;
        let _: i64 = conn
            .publish(channel.as_str(), "update")
            .await
            .with_context(|| format!("publish redis notifier channel {channel}"))?;
        Ok(())
    }

    async fn subscribe(&self, channel: String) -> Result<ClusterSubscription> {
        let mut pubsub = self
            .client
            .get_async_pubsub()
            .await
            .with_context(|| format!("connect redis notifier for subscribe {channel}"))?;
        let _: () = pubsub
            .subscribe(channel.as_str())
            .await
            .with_context(|| format!("subscribe redis notifier channel {channel}"))?;
        Ok(ClusterSubscription::Redis(pubsub.into_on_message()))
    }
}

#[async_trait]
impl ClusterNotifier for RedisClusterNotifier {
    async fn publish_job_events(&self, job_id: &str) -> Result<()> {
        self.publish(Self::job_events_channel(job_id)).await
    }

    async fn publish_job_streams(&self, job_id: &str) -> Result<()> {
        self.publish(Self::job_streams_channel(job_id)).await
    }

    async fn subscribe_job_events(&self, job_id: &str) -> Result<ClusterSubscription> {
        self.subscribe(Self::job_events_channel(job_id)).await
    }

    async fn subscribe_job_streams(&self, job_id: &str) -> Result<ClusterSubscription> {
        self.subscribe(Self::job_streams_channel(job_id)).await
    }
}
