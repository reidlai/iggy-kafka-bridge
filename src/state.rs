//! Bridge state and Kafka request handlers.

use bytes::BytesMut;
use kafka_protocol::messages::api_versions_response::ApiVersion;
use kafka_protocol::messages::fetch_response::{FetchableTopicResponse, PartitionData};
use kafka_protocol::messages::find_coordinator_response::Coordinator;
use kafka_protocol::messages::list_offsets_response::{ListOffsetsPartitionResponse, ListOffsetsTopicResponse};
use kafka_protocol::messages::metadata_response::{MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic};
use kafka_protocol::messages::offset_commit_response::{OffsetCommitResponsePartition, OffsetCommitResponseTopic};
use kafka_protocol::messages::offset_fetch_response::{OffsetFetchResponsePartition, OffsetFetchResponseTopic};
use kafka_protocol::messages::*;
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType};
use std::sync::Arc;
use tracing::warn;

use crate::backend::IggyBackend;
use crate::config::TopicRegistry;

pub const ERROR_NONE: i16 = 0;
pub const ERROR_OFFSET_OUT_OF_RANGE: i16 = 1;
pub const ERROR_UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
pub const ERROR_NOT_LEADER_OR_FOLLOWER: i16 = 6;

#[derive(Clone)]
pub struct BridgeState<B: IggyBackend> {
    pub backend: Arc<B>,
    pub registry: TopicRegistry,
    pub node_id: i32,
    pub advertised_host: String,
    pub advertised_port: i32,
}

impl<B: IggyBackend> BridgeState<B> {
    pub fn new(
        backend: Arc<B>,
        registry: TopicRegistry,
        node_id: i32,
        advertised_host: String,
        advertised_port: i32,
    ) -> Self {
        Self {
            backend,
            registry,
            node_id,
            advertised_host,
            advertised_port,
        }
    }

    pub fn handle_api_versions(&self, _req: &ApiVersionsRequest, version: i16) -> ApiVersionsResponse {
        let mut resp = ApiVersionsResponse::default();
        resp.error_code = ERROR_NONE;

        let supported = [
            (ApiKey::Produce, 3, 9),
            (ApiKey::Fetch, 4, 11),
            (ApiKey::ListOffsets, 1, 7),
            (ApiKey::Metadata, 0, 9),
            (ApiKey::OffsetCommit, 2, 9),
            (ApiKey::OffsetFetch, 1, 9),
            (ApiKey::FindCoordinator, 0, 4),
            (ApiKey::ApiVersions, 0, 3),
        ];

        let mut apis = Vec::with_capacity(supported.len());
        for (key, min_v, max_v) in supported {
            let mut api = ApiVersion::default();
            api.api_key = key as i16;
            api.min_version = min_v;
            api.max_version = max_v;
            apis.push(api);
        }
        resp.api_keys = apis.into();
        resp.throttle_time_ms = 0;
        if version >= 3 {
            resp.finalized_features_epoch = -1;
        }
        resp
    }

    pub fn handle_metadata(&self, req: &MetadataRequest, version: i16) -> MetadataResponse {
        let mut resp = MetadataResponse::default();

        let mut broker = MetadataResponseBroker::default();
        broker.node_id = BrokerId(self.node_id);
        broker.host = StrBytes::from_string(self.advertised_host.clone());
        broker.port = self.advertised_port;
        resp.brokers.push(broker);
        resp.controller_id = BrokerId(self.node_id);

        if version >= 2 {
            resp.cluster_id = Some(StrBytes::from_static_str("iggy-bridge-cluster"));
        }

        let topic_map = self.registry.topic_map();

        let filter_topics: Option<Vec<String>> = req.topics.as_ref().map(|topics| {
            topics
                .iter()
                .filter_map(|t| t.name.as_ref().map(|n| n.0.to_string()))
                .collect()
        });

        match filter_topics {
            Some(topics) if !topics.is_empty() => {
                for requested_name in topics {
                    let mut topic_resp = MetadataResponseTopic::default();
                    topic_resp.name = Some(TopicName(StrBytes::from_string(requested_name.clone())));

                    if let Some(entry) = topic_map.get(&requested_name) {
                        topic_resp.error_code = ERROR_NONE;
                        topic_resp.is_internal = false;
                        // Ascending partition order is required per Contract Sheet section 3 & L6
                        for p in 0..entry.partitions {
                            let mut part = MetadataResponsePartition::default();
                            part.error_code = ERROR_NONE;
                            part.partition_index = p as i32;
                            part.leader_id = BrokerId(self.node_id);
                            part.leader_epoch = 0;
                            part.replica_nodes = vec![BrokerId(self.node_id)];
                            part.isr_nodes = vec![BrokerId(self.node_id)];
                            topic_resp.partitions.push(part);
                        }
                    } else {
                        // Unmapped topic
                        topic_resp.error_code = ERROR_UNKNOWN_TOPIC_OR_PARTITION;
                    }
                    resp.topics.push(topic_resp);
                }
            }
            _ => {
                // All topics in registry
                for entry in &self.registry.topics {
                    let mut topic_resp = MetadataResponseTopic::default();
                    topic_resp.name = Some(TopicName(StrBytes::from_string(entry.kafka_name.clone())));
                    topic_resp.error_code = ERROR_NONE;
                    topic_resp.is_internal = false;
                    for p in 0..entry.partitions {
                        let mut part = MetadataResponsePartition::default();
                        part.error_code = ERROR_NONE;
                        part.partition_index = p as i32;
                        part.leader_id = BrokerId(self.node_id);
                        part.leader_epoch = 0;
                        part.replica_nodes = vec![BrokerId(self.node_id)];
                        part.isr_nodes = vec![BrokerId(self.node_id)];
                        topic_resp.partitions.push(part);
                    }
                    resp.topics.push(topic_resp);
                }
            }
        }

        resp
    }

    pub async fn handle_list_offsets(&self, req: &ListOffsetsRequest, _version: i16) -> ListOffsetsResponse {
        let mut resp = ListOffsetsResponse::default();
        let topic_map = self.registry.topic_map();

        for req_topic in &req.topics {
            let topic_name_str = req_topic.name.0.as_str();
            let mut topic_resp = ListOffsetsTopicResponse::default();
            topic_resp.name = req_topic.name.clone();

            if let Some(entry) = topic_map.get(topic_name_str) {
                for req_part in &req_topic.partitions {
                    let mut part_resp = ListOffsetsPartitionResponse::default();
                    part_resp.partition_index = req_part.partition_index;

                    if req_part.partition_index < 0 || req_part.partition_index >= entry.partitions as i32 {
                        part_resp.error_code = ERROR_UNKNOWN_TOPIC_OR_PARTITION;
                        part_resp.offset = -1;
                    } else {
                        match self
                            .backend
                            .get_partition_offsets(&entry.stream, &entry.topic, req_part.partition_index as u32)
                            .await
                        {
                            Ok((earliest, latest)) => {
                                part_resp.error_code = ERROR_NONE;
                                if req_part.timestamp == -2 {
                                    // EARLIEST: always 0 per contract sheet section 2
                                    part_resp.offset = earliest as i64;
                                } else {
                                    // LATEST: high watermark per contract sheet section 2
                                    part_resp.offset = latest as i64;
                                }
                            }
                            Err(e) => {
                                warn!("Error getting offsets for {topic_name_str}:{}: {e}", req_part.partition_index);
                                part_resp.error_code = ERROR_UNKNOWN_TOPIC_OR_PARTITION;
                                part_resp.offset = -1;
                            }
                        }
                    }
                    topic_resp.partitions.push(part_resp);
                }
            } else {
                for req_part in &req_topic.partitions {
                    let mut part_resp = ListOffsetsPartitionResponse::default();
                    part_resp.partition_index = req_part.partition_index;
                    part_resp.error_code = ERROR_UNKNOWN_TOPIC_OR_PARTITION;
                    part_resp.offset = -1;
                    topic_resp.partitions.push(part_resp);
                }
            }
            resp.topics.push(topic_resp);
        }

        resp
    }

    pub async fn handle_fetch(&self, req: &FetchRequest, _version: i16) -> FetchResponse {
        let mut resp = FetchResponse::default();
        resp.error_code = ERROR_NONE;
        resp.session_id = 0;
        let topic_map = self.registry.topic_map();

        for req_topic in &req.topics {
            let topic_name_str = req_topic.topic.0.as_str();
            let mut topic_resp = FetchableTopicResponse::default();
            topic_resp.topic = req_topic.topic.clone();

            if let Some(entry) = topic_map.get(topic_name_str) {
                for req_part in &req_topic.partitions {
                    let mut part_resp = PartitionData::default();
                    part_resp.partition_index = req_part.partition;

                    if req_part.partition < 0 || req_part.partition >= entry.partitions as i32 {
                        part_resp.error_code = ERROR_UNKNOWN_TOPIC_OR_PARTITION;
                        part_resp.high_watermark = 0;
                        part_resp.last_stable_offset = 0;
                        part_resp.log_start_offset = -1;
                        part_resp.records = None;
                    } else {
                        let partition_id = req_part.partition as u32;
                        match self
                            .backend
                            .get_partition_offsets(&entry.stream, &entry.topic, partition_id)
                            .await
                        {
                            Ok((_earliest, latest_hw)) => {
                                let fetch_offset = req_part.fetch_offset;
                                if fetch_offset < 0 {
                                    part_resp.error_code = ERROR_OFFSET_OUT_OF_RANGE;
                                    part_resp.high_watermark = latest_hw as i64;
                                    part_resp.last_stable_offset = latest_hw as i64;
                                    part_resp.log_start_offset = -1;
                                    part_resp.records = None;
                                } else if fetch_offset > latest_hw as i64 {
                                    // Out of range per Contract Sheet section 7
                                    part_resp.error_code = ERROR_OFFSET_OUT_OF_RANGE;
                                    part_resp.high_watermark = latest_hw as i64;
                                    part_resp.last_stable_offset = latest_hw as i64;
                                    part_resp.log_start_offset = -1;
                                    part_resp.records = None;
                                } else if fetch_offset == latest_hw as i64 {
                                    // At high watermark: empty success
                                    part_resp.error_code = ERROR_NONE;
                                    part_resp.high_watermark = latest_hw as i64;
                                    part_resp.last_stable_offset = latest_hw as i64;
                                    part_resp.log_start_offset = -1;
                                    part_resp.records = None;
                                } else {
                                    // Fetch records starting from fetch_offset
                                    let max_records = 100u32;
                                    match self
                                        .backend
                                        .poll_messages(
                                            &entry.stream,
                                            &entry.topic,
                                            partition_id,
                                            fetch_offset as u64,
                                            max_records,
                                        )
                                        .await
                                    {
                                        Ok(messages) => {
                                            part_resp.error_code = ERROR_NONE;
                                            part_resp.high_watermark = latest_hw as i64;
                                            part_resp.last_stable_offset = latest_hw as i64;
                                            part_resp.log_start_offset = -1;

                                            if messages.is_empty() {
                                                part_resp.records = None;
                                            } else {
                                                let records: Vec<Record> = messages
                                                    .into_iter()
                                                    .map(|m| {
                                                        let mut headers = indexmap::IndexMap::new();
                                                        for (k, v) in m.headers {
                                                            headers.insert(
                                                                StrBytes::from_string(k),
                                                                Some(v),
                                                            );
                                                        }
                                                        Record {
                                                            transactional: false,
                                                            control: false,
                                                            delete_horizon: false,
                                                            partition_leader_epoch: -1,
                                                            producer_id: -1,
                                                            producer_epoch: -1,
                                                            timestamp_type: TimestampType::Creation,
                                                            offset: m.offset as i64,
                                                            sequence: m.offset as i32,
                                                            timestamp: m.timestamp_ms,
                                                            key: m.key,
                                                            value: Some(m.payload),
                                                            headers,
                                                        }
                                                    })
                                                    .collect();

                                                let mut buf = BytesMut::new();
                                                let options = RecordEncodeOptions {
                                                    version: 2,
                                                    compression: Compression::None,
                                                };
                                                if let Err(e) =
                                                    RecordBatchEncoder::encode(&mut buf, &records, &options)
                                                {
                                                    warn!("Failed to encode record batch: {e}");
                                                    part_resp.records = None;
                                                } else {
                                                    part_resp.records = Some(buf.freeze());
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            warn!("Error polling messages: {e}");
                                            part_resp.error_code = ERROR_UNKNOWN_TOPIC_OR_PARTITION;
                                            part_resp.high_watermark = 0;
                                            part_resp.last_stable_offset = 0;
                                            part_resp.log_start_offset = -1;
                                            part_resp.records = None;
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                warn!("Error getting partition offsets: {e}");
                                part_resp.error_code = ERROR_UNKNOWN_TOPIC_OR_PARTITION;
                                part_resp.high_watermark = 0;
                                part_resp.last_stable_offset = 0;
                                part_resp.log_start_offset = -1;
                                part_resp.records = None;
                            }
                        }
                    }
                    topic_resp.partitions.push(part_resp);
                }
            } else {
                for req_part in &req_topic.partitions {
                    let mut part_resp = PartitionData::default();
                    part_resp.partition_index = req_part.partition;
                    part_resp.error_code = ERROR_UNKNOWN_TOPIC_OR_PARTITION;
                    part_resp.high_watermark = 0;
                    part_resp.last_stable_offset = 0;
                    part_resp.log_start_offset = -1;
                    part_resp.records = None;
                    topic_resp.partitions.push(part_resp);
                }
            }
            resp.responses.push(topic_resp);
        }

        resp
    }

    pub fn handle_find_coordinator(&self, req: &FindCoordinatorRequest, version: i16) -> FindCoordinatorResponse {
        let mut resp = FindCoordinatorResponse::default();
        resp.error_code = ERROR_NONE;
        resp.node_id = BrokerId(self.node_id);
        resp.host = StrBytes::from_string(self.advertised_host.clone());
        resp.port = self.advertised_port;

        if version >= 4 {
            let mut coord = Coordinator::default();
            coord.key = req.key.clone();
            coord.node_id = BrokerId(self.node_id);
            coord.host = StrBytes::from_string(self.advertised_host.clone());
            coord.port = self.advertised_port;
            coord.error_code = ERROR_NONE;
            resp.coordinators.push(coord);
        }

        resp
    }

    pub async fn handle_offset_commit(&self, req: &OffsetCommitRequest, _version: i16) -> OffsetCommitResponse {
        let mut resp = OffsetCommitResponse::default();
        let topic_map = self.registry.topic_map();
        let group = req.group_id.0.as_str();

        for req_topic in &req.topics {
            let topic_name_str = req_topic.name.0.as_str();
            let mut topic_resp = OffsetCommitResponseTopic::default();
            topic_resp.name = req_topic.name.clone();

            if let Some(entry) = topic_map.get(topic_name_str) {
                for req_part in &req_topic.partitions {
                    let mut part_resp = OffsetCommitResponsePartition::default();
                    part_resp.partition_index = req_part.partition_index;

                    if req_part.partition_index < 0 || req_part.partition_index >= entry.partitions as i32 {
                        part_resp.error_code = ERROR_UNKNOWN_TOPIC_OR_PARTITION;
                    } else {
                        let partition_id = req_part.partition_index as u32;
                        let offset = req_part.committed_offset as u64;
                        // Store offset as kafka.cg.<group> per contract sheet Section 8
                        let iggy_group = format!("kafka.cg.{group}");
                        if let Err(e) = self
                            .backend
                            .store_consumer_offset(
                                &iggy_group,
                                &entry.stream,
                                &entry.topic,
                                partition_id,
                                offset,
                            )
                            .await
                        {
                            warn!("Failed to store consumer offset: {e}");
                            part_resp.error_code = ERROR_UNKNOWN_TOPIC_OR_PARTITION;
                        } else {
                            part_resp.error_code = ERROR_NONE;
                        }
                    }
                    topic_resp.partitions.push(part_resp);
                }
            } else {
                for req_part in &req_topic.partitions {
                    let mut part_resp = OffsetCommitResponsePartition::default();
                    part_resp.partition_index = req_part.partition_index;
                    part_resp.error_code = ERROR_UNKNOWN_TOPIC_OR_PARTITION;
                    topic_resp.partitions.push(part_resp);
                }
            }
            resp.topics.push(topic_resp);
        }

        resp
    }

    pub async fn handle_offset_fetch(&self, req: &OffsetFetchRequest, _version: i16) -> OffsetFetchResponse {
        let mut resp = OffsetFetchResponse::default();
        resp.error_code = ERROR_NONE;
        let topic_map = self.registry.topic_map();
        let group = req.group_id.0.as_str();
        let iggy_group = format!("kafka.cg.{group}");

        if let Some(req_topics) = &req.topics {
            for req_topic in req_topics {
                let topic_name_str = req_topic.name.0.as_str();
                let mut topic_resp = OffsetFetchResponseTopic::default();
                topic_resp.name = req_topic.name.clone();

                if let Some(entry) = topic_map.get(topic_name_str) {
                    for partition_index in &req_topic.partition_indexes {
                        let mut part_resp = OffsetFetchResponsePartition::default();
                        part_resp.partition_index = *partition_index;

                        if *partition_index < 0 || *partition_index >= entry.partitions as i32 {
                            part_resp.error_code = ERROR_UNKNOWN_TOPIC_OR_PARTITION;
                            part_resp.committed_offset = -1;
                        } else {
                            match self
                                .backend
                                .get_consumer_offset(
                                    &iggy_group,
                                    &entry.stream,
                                    &entry.topic,
                                    *partition_index as u32,
                                )
                                .await
                            {
                                Ok(Some(offset)) => {
                                    part_resp.error_code = ERROR_NONE;
                                    part_resp.committed_offset = offset as i64;
                                }
                                Ok(None) => {
                                    part_resp.error_code = ERROR_NONE;
                                    part_resp.committed_offset = -1;
                                }
                                Err(e) => {
                                    warn!("Failed to fetch consumer offset: {e}");
                                    part_resp.error_code = ERROR_UNKNOWN_TOPIC_OR_PARTITION;
                                    part_resp.committed_offset = -1;
                                }
                            }
                        }
                        topic_resp.partitions.push(part_resp);
                    }
                } else {
                    for partition_index in &req_topic.partition_indexes {
                        let mut part_resp = OffsetFetchResponsePartition::default();
                        part_resp.partition_index = *partition_index;
                        part_resp.error_code = ERROR_UNKNOWN_TOPIC_OR_PARTITION;
                        part_resp.committed_offset = -1;
                        topic_resp.partitions.push(part_resp);
                    }
                }
                resp.topics.push(topic_resp);
            }
        }

        resp
    }
}
