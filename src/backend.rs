//! Bridge backend interface and implementations.

use bytes::Bytes;
use iggy::prelude::*;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::config::TopicRegistry;

pub const DEFAULT_IGGY_POOL_SIZE: usize = 4;

#[derive(Debug, Clone)]
pub struct BridgeMessage {
    pub offset: u64,
    pub timestamp_ms: i64,
    pub key: Option<Bytes>,
    pub payload: Bytes,
    pub headers: HashMap<String, Bytes>,
}

#[derive(Debug, thiserror::Error)]
pub enum BridgeError {
    #[error("Topic not found: {0}")]
    TopicNotFound(String),
    #[error("Partition not found: {0}:{1}")]
    PartitionNotFound(String, u32),
    #[error("Backend communication error: {0}")]
    CommunicationError(String),
    #[error("Unauthorized: {0}")]
    Unauthorized(String),
}

pub trait IggyBackend: Send + Sync {
    fn get_partition_offsets(
        &self,
        stream: &str,
        topic: &str,
        partition_id: u32,
    ) -> impl std::future::Future<Output = Result<(u64, u64), BridgeError>> + Send;

    fn poll_messages(
        &self,
        stream: &str,
        topic: &str,
        partition_id: u32,
        offset: u64,
        count: u32,
    ) -> impl std::future::Future<Output = Result<Vec<BridgeMessage>, BridgeError>> + Send;

    fn store_consumer_offset(
        &self,
        group: &str,
        stream: &str,
        topic: &str,
        partition_id: u32,
        offset: u64,
    ) -> impl std::future::Future<Output = Result<(), BridgeError>> + Send;

    fn get_consumer_offset(
        &self,
        group: &str,
        stream: &str,
        topic: &str,
        partition_id: u32,
    ) -> impl std::future::Future<Output = Result<Option<u64>, BridgeError>> + Send;

    fn send_messages(
        &self,
        stream: &str,
        topic: &str,
        partition_id: u32,
        messages: Vec<BridgeMessage>,
    ) -> impl std::future::Future<Output = Result<u64, BridgeError>> + Send;
}

/// In-memory mock backend for deterministic unit, multi-replica, and wire contract testing.
#[derive(Clone)]
pub struct MockIggyBackend {
    inner: Arc<RwLock<MockState>>,
    pool_slots: Arc<Vec<AtomicUsize>>,
    next_slot: Arc<AtomicUsize>,
}

impl Default for MockIggyBackend {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Default)]
struct MockState {
    // (stream, topic) -> partitions -> messages
    topics: HashMap<(String, String), Vec<Vec<BridgeMessage>>>,
    // (group, stream, topic, partition) -> offset
    consumer_offsets: HashMap<(String, String, String, u32), u64>,
}

impl MockIggyBackend {
    pub fn new() -> Self {
        Self::new_with_pool_size(DEFAULT_IGGY_POOL_SIZE)
    }

    pub fn new_with_pool_size(pool_size: usize) -> Self {
        let size = pool_size.max(1);
        let slots = (0..size).map(|_| AtomicUsize::new(0)).collect();
        Self {
            inner: Arc::new(RwLock::new(MockState::default())),
            pool_slots: Arc::new(slots),
            next_slot: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub async fn with_registry(registry: &TopicRegistry) -> Self {
        Self::with_registry_and_pool_size(registry, DEFAULT_IGGY_POOL_SIZE).await
    }

    pub async fn with_registry_and_pool_size(registry: &TopicRegistry, pool_size: usize) -> Self {
        let backend = Self::new_with_pool_size(pool_size);
        for t in &registry.topics {
            backend.add_topic(&t.stream, &t.topic, t.partitions).await;
        }
        backend
    }

    pub fn pool_size(&self) -> usize {
        self.pool_slots.len()
    }

    pub fn pool_dispatch_counts(&self) -> Vec<usize> {
        self.pool_slots
            .iter()
            .map(|c| c.load(Ordering::Relaxed))
            .collect()
    }

    fn record_pool_dispatch(&self) {
        let idx = self.next_slot.fetch_add(1, Ordering::Relaxed) % self.pool_slots.len();
        self.pool_slots[idx].fetch_add(1, Ordering::Relaxed);
    }

    pub async fn add_topic(&self, stream: &str, topic: &str, partitions: u32) {
        let mut state = self.inner.write().await;
        let p_vec = (0..partitions).map(|_| Vec::new()).collect();
        state.topics.insert((stream.to_string(), topic.to_string()), p_vec);
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn send_message(
        &self,
        stream: &str,
        topic: &str,
        partition_id: u32,
        key: Option<Bytes>,
        payload: Bytes,
        headers: HashMap<String, Bytes>,
        timestamp_ms: i64,
    ) -> Result<u64, BridgeError> {
        let mut state = self.inner.write().await;
        let partitions = state
            .topics
            .get_mut(&(stream.to_string(), topic.to_string()))
            .ok_or_else(|| BridgeError::TopicNotFound(format!("{stream}/{topic}")))?;
        
        let p = partitions
            .get_mut(partition_id as usize)
            .ok_or_else(|| BridgeError::PartitionNotFound(format!("{stream}/{topic}"), partition_id))?;
        
        let offset = p.len() as u64;
        p.push(BridgeMessage {
            offset,
            timestamp_ms,
            key,
            payload,
            headers,
        });
        Ok(offset)
    }
}

impl IggyBackend for MockIggyBackend {
    async fn get_partition_offsets(
        &self,
        stream: &str,
        topic: &str,
        partition_id: u32,
    ) -> Result<(u64, u64), BridgeError> {
        self.record_pool_dispatch();
        let state = self.inner.read().await;
        let partitions = state
            .topics
            .get(&(stream.to_string(), topic.to_string()))
            .ok_or_else(|| BridgeError::TopicNotFound(format!("{stream}/{topic}")))?;
        
        let p = partitions
            .get(partition_id as usize)
            .ok_or_else(|| BridgeError::PartitionNotFound(format!("{stream}/{topic}"), partition_id))?;
        
        let high_watermark = p.len() as u64;
        // Earliest offset is always 0 per contract sheet
        Ok((0, high_watermark))
    }

    async fn poll_messages(
        &self,
        stream: &str,
        topic: &str,
        partition_id: u32,
        offset: u64,
        count: u32,
    ) -> Result<Vec<BridgeMessage>, BridgeError> {
        self.record_pool_dispatch();
        let state = self.inner.read().await;
        let partitions = state
            .topics
            .get(&(stream.to_string(), topic.to_string()))
            .ok_or_else(|| BridgeError::TopicNotFound(format!("{stream}/{topic}")))?;
        
        let p = partitions
            .get(partition_id as usize)
            .ok_or_else(|| BridgeError::PartitionNotFound(format!("{stream}/{topic}"), partition_id))?;
        
        let start = offset as usize;
        if start >= p.len() {
            return Ok(Vec::new());
        }
        let end = (start + count as usize).min(p.len());
        Ok(p[start..end].to_vec())
    }

    async fn store_consumer_offset(
        &self,
        group: &str,
        stream: &str,
        topic: &str,
        partition_id: u32,
        offset: u64,
    ) -> Result<(), BridgeError> {
        self.record_pool_dispatch();
        let mut state = self.inner.write().await;
        state.consumer_offsets.insert(
            (
                group.to_string(),
                stream.to_string(),
                topic.to_string(),
                partition_id,
            ),
            offset,
        );
        Ok(())
    }

    async fn get_consumer_offset(
        &self,
        group: &str,
        stream: &str,
        topic: &str,
        partition_id: u32,
    ) -> Result<Option<u64>, BridgeError> {
        self.record_pool_dispatch();
        let state = self.inner.read().await;
        Ok(state.consumer_offsets.get(&(
            group.to_string(),
            stream.to_string(),
            topic.to_string(),
            partition_id,
        )).copied())
    }

    async fn send_messages(
        &self,
        stream: &str,
        topic: &str,
        partition_id: u32,
        messages: Vec<BridgeMessage>,
    ) -> Result<u64, BridgeError> {
        self.record_pool_dispatch();
        let mut state = self.inner.write().await;
        let partitions = state
            .topics
            .get_mut(&(stream.to_string(), topic.to_string()))
            .ok_or_else(|| BridgeError::TopicNotFound(format!("{stream}/{topic}")))?;

        let p = partitions
            .get_mut(partition_id as usize)
            .ok_or_else(|| BridgeError::PartitionNotFound(format!("{stream}/{topic}"), partition_id))?;

        let base_offset = p.len() as u64;
        for mut m in messages {
            m.offset = p.len() as u64;
            p.push(m);
        }
        Ok(base_offset)
    }
}

/// Pooled native Apache Iggy TCP client backend for horizontally scalable bridge replicas (Story 16.1, FR-34).
pub struct LiveIggyBackend {
    clients: Vec<Arc<TcpClient>>,
    next_idx: AtomicUsize,
}

impl LiveIggyBackend {
    pub async fn connect(
        server_address: &str,
        username: &str,
        password: &str,
    ) -> Result<Self, BridgeError> {
        Self::connect_pool(server_address, username, password, DEFAULT_IGGY_POOL_SIZE).await
    }

    pub async fn connect_pool(
        server_address: &str,
        username: &str,
        password: &str,
        pool_size: usize,
    ) -> Result<Self, BridgeError> {
        let size = pool_size.max(1);
        let mut clients = Vec::with_capacity(size);
        for _ in 0..size {
            let config = TcpClientConfig {
                server_address: server_address.to_string(),
                ..Default::default()
            };
            let client = TcpClient::create(Arc::new(config))
                .map_err(|e| BridgeError::CommunicationError(e.to_string()))?;
            client
                .connect()
                .await
                .map_err(|e| BridgeError::CommunicationError(e.to_string()))?;
            client
                .login_user(username, password)
                .await
                .map_err(|e| BridgeError::Unauthorized(e.to_string()))?;
            clients.push(Arc::new(client));
        }
        Ok(Self {
            clients,
            next_idx: AtomicUsize::new(0),
        })
    }

    pub fn pool_size(&self) -> usize {
        self.clients.len()
    }

    fn next_client(&self) -> &Arc<TcpClient> {
        let idx = self.next_idx.fetch_add(1, Ordering::Relaxed) % self.clients.len();
        &self.clients[idx]
    }
}

impl IggyBackend for LiveIggyBackend {
    async fn get_partition_offsets(
        &self,
        stream: &str,
        topic: &str,
        partition_id: u32,
    ) -> Result<(u64, u64), BridgeError> {
        let stream_id = Identifier::named(stream)
            .map_err(|e| BridgeError::TopicNotFound(format!("{stream}: {e}")))?;
        let topic_id = Identifier::named(topic)
            .map_err(|e| BridgeError::TopicNotFound(format!("{topic}: {e}")))?;
        let topic_details = self
            .next_client()
            .get_topic(&stream_id, &topic_id)
            .await
            .map_err(|e| BridgeError::CommunicationError(e.to_string()))?
            .ok_or_else(|| BridgeError::TopicNotFound(format!("{stream}/{topic}")))?;
        for p in &topic_details.partitions {
            if p.id == partition_id {
                return Ok((0, p.current_offset));
            }
        }
        Err(BridgeError::PartitionNotFound(
            format!("{stream}/{topic}"),
            partition_id,
        ))
    }

    async fn poll_messages(
        &self,
        stream: &str,
        topic: &str,
        partition_id: u32,
        offset: u64,
        count: u32,
    ) -> Result<Vec<BridgeMessage>, BridgeError> {
        let stream_id = Identifier::named(stream)
            .map_err(|e| BridgeError::TopicNotFound(format!("{stream}: {e}")))?;
        let topic_id = Identifier::named(topic)
            .map_err(|e| BridgeError::TopicNotFound(format!("{topic}: {e}")))?;
        let consumer = Consumer::default();
        let strategy = PollingStrategy::offset(offset);
        let polled = self
            .next_client()
            .poll_messages(
                &stream_id,
                &topic_id,
                Some(partition_id),
                &consumer,
                &strategy,
                count,
                false,
            )
            .await
            .map_err(|e| BridgeError::CommunicationError(e.to_string()))?;

        let messages = polled
            .messages
            .into_iter()
            .map(|m| {
                let mut headers = HashMap::new();
                if let Ok(Some(user_headers)) = m.user_headers_map() {
                    for (k, v) in user_headers {
                        if let Ok(key_str) = k.as_str() {
                            headers.insert(key_str.to_string(), v.value());
                        }
                    }
                }
                let key = headers.remove("kafka.key");
                let timestamp_ms = (m.header.origin_timestamp / 1000) as i64;
                BridgeMessage {
                    offset: m.header.offset,
                    timestamp_ms,
                    key,
                    payload: m.payload,
                    headers,
                }
            })
            .collect();

        Ok(messages)
    }

    async fn store_consumer_offset(
        &self,
        group: &str,
        stream: &str,
        topic: &str,
        partition_id: u32,
        offset: u64,
    ) -> Result<(), BridgeError> {
        let stream_id = Identifier::named(stream)
            .map_err(|e| BridgeError::TopicNotFound(format!("{stream}: {e}")))?;
        let topic_id = Identifier::named(topic)
            .map_err(|e| BridgeError::TopicNotFound(format!("{topic}: {e}")))?;
        let consumer = Consumer::group(
            Identifier::named(group)
                .map_err(|e| BridgeError::CommunicationError(e.to_string()))?,
        );
        self.next_client()
            .store_consumer_offset(
                &consumer,
                &stream_id,
                &topic_id,
                Some(partition_id),
                offset,
            )
            .await
            .map_err(|e| BridgeError::CommunicationError(e.to_string()))?;
        Ok(())
    }

    async fn get_consumer_offset(
        &self,
        group: &str,
        stream: &str,
        topic: &str,
        partition_id: u32,
    ) -> Result<Option<u64>, BridgeError> {
        let stream_id = Identifier::named(stream)
            .map_err(|e| BridgeError::TopicNotFound(format!("{stream}: {e}")))?;
        let topic_id = Identifier::named(topic)
            .map_err(|e| BridgeError::TopicNotFound(format!("{topic}: {e}")))?;
        let consumer = Consumer::group(
            Identifier::named(group)
                .map_err(|e| BridgeError::CommunicationError(e.to_string()))?,
        );
        match self
            .next_client()
            .get_consumer_offset(&consumer, &stream_id, &topic_id, Some(partition_id))
            .await
        {
            Ok(Some(info)) => Ok(Some(info.stored_offset)),
            Ok(None) => Ok(None),
            Err(e) => Err(BridgeError::CommunicationError(e.to_string())),
        }
    }

    async fn send_messages(
        &self,
        stream: &str,
        topic: &str,
        partition_id: u32,
        messages: Vec<BridgeMessage>,
    ) -> Result<u64, BridgeError> {
        use std::collections::BTreeMap;
        let stream_id = Identifier::named(stream)
            .map_err(|e| BridgeError::TopicNotFound(format!("{stream}: {e}")))?;
        let topic_id = Identifier::named(topic)
            .map_err(|e| BridgeError::TopicNotFound(format!("{topic}: {e}")))?;

        let mut iggy_messages = Vec::with_capacity(messages.len());
        for m in messages {
            let mut headers_map = BTreeMap::new();
            for (k, v) in m.headers {
                if let (Ok(key), Ok(val)) = (
                    HeaderKey::try_from(k.as_str()),
                    HeaderValue::try_from(v.as_ref()),
                ) {
                    headers_map.insert(key, val);
                }
            }
            let mut iggy_msg = if headers_map.is_empty() {
                IggyMessage::builder().payload(m.payload).build()
            } else {
                IggyMessage::builder()
                    .payload(m.payload)
                    .user_headers(headers_map)
                    .build()
            }
            .map_err(|e| BridgeError::CommunicationError(e.to_string()))?;
            iggy_msg.header.origin_timestamp = (m.timestamp_ms * 1000) as u64;
            iggy_messages.push(iggy_msg);
        }

        let client = self.next_client();
        let partitioning = Partitioning::partition_id(partition_id);
        client
            .send_messages(&stream_id, &topic_id, &partitioning, &mut iggy_messages)
            .await
            .map_err(|e| BridgeError::CommunicationError(e.to_string()))?;

        let topic_details = client
            .get_topic(&stream_id, &topic_id)
            .await
            .map_err(|e| BridgeError::CommunicationError(e.to_string()))?
            .ok_or_else(|| BridgeError::TopicNotFound(format!("{stream}/{topic}")))?;
        let current_offset = topic_details
            .partitions
            .iter()
            .find(|p| p.id == partition_id)
            .map(|p| p.current_offset)
            .unwrap_or(0);
        let base_offset = current_offset.saturating_sub(iggy_messages.len() as u64);
        Ok(base_offset)
    }
}
