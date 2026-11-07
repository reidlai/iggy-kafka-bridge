//! Kafka wire protocol frame processor and request dispatcher.

use bytes::{BufMut, Bytes, BytesMut};
use kafka_protocol::messages::*;
use kafka_protocol::protocol::{Decodable, Encodable};
use std::convert::TryFrom;
use tracing::{debug, warn};

use crate::backend::IggyBackend;
use crate::state::BridgeState;

/// Processes an incoming raw Kafka request frame (excluding the 4-byte length prefix)
/// and produces a complete response frame (including the 4-byte length prefix).
pub async fn process_frame<B: IggyBackend>(
    mut raw_frame: Bytes,
    state: &BridgeState<B>,
) -> Result<Bytes, Box<dyn std::error::Error + Send + Sync>> {
    if raw_frame.len() < 4 {
        return Err("Frame too short for Kafka header".into());
    }

    let api_key_raw = i16::from_be_bytes([raw_frame[0], raw_frame[1]]);
    let api_version = i16::from_be_bytes([raw_frame[2], raw_frame[3]]);

    let api_key = ApiKey::try_from(api_key_raw)
        .map_err(|_| format!("Unsupported ApiKey: {api_key_raw}"))?;

    let req_header_v = api_key.request_header_version(api_version);
    let res_header_v = api_key.response_header_version(api_version);

    let req_header = RequestHeader::decode(&mut raw_frame, req_header_v)?;
    debug!(
        "Received Kafka request: key={:?}, version={}, correlation_id={}",
        api_key, api_version, req_header.correlation_id
    );

    let req_kind = RequestKind::decode(api_key, &mut raw_frame, api_version)?;

    let res_kind: ResponseKind = match req_kind {
        RequestKind::ApiVersions(ref req) => {
            let resp = state.handle_api_versions(req, api_version);
            ResponseKind::ApiVersions(resp)
        }
        RequestKind::Metadata(ref req) => {
            let resp = state.handle_metadata(req, api_version);
            ResponseKind::Metadata(resp)
        }
        RequestKind::ListOffsets(ref req) => {
            let resp = state.handle_list_offsets(req, api_version).await;
            ResponseKind::ListOffsets(resp)
        }
        RequestKind::Fetch(ref req) => {
            let resp = state.handle_fetch(req, api_version).await;
            ResponseKind::Fetch(resp)
        }
        RequestKind::FindCoordinator(ref req) => {
            let resp = state.handle_find_coordinator(req, api_version);
            ResponseKind::FindCoordinator(resp)
        }
        RequestKind::OffsetCommit(ref req) => {
            let resp = state.handle_offset_commit(req, api_version).await;
            ResponseKind::OffsetCommit(resp)
        }
        RequestKind::OffsetFetch(ref req) => {
            let resp = state.handle_offset_fetch(req, api_version).await;
            ResponseKind::OffsetFetch(resp)
        }
        other => {
            warn!("Unhandled RequestKind: {:?}", other);
            return Err(format!("Unhandled RequestKind: {:?}", api_key).into());
        }
    };

    let mut body_buf = BytesMut::new();
    let mut res_header = ResponseHeader::default();
    res_header.correlation_id = req_header.correlation_id;
    res_header.encode(&mut body_buf, res_header_v)?;
    res_kind.encode(&mut body_buf, api_version)?;

    let mut frame = BytesMut::with_capacity(4 + body_buf.len());
    frame.put_i32(body_buf.len() as i32);
    frame.put(body_buf);

    Ok(frame.freeze())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::MockIggyBackend;
    use crate::config::TopicRegistry;
    use std::sync::Arc;

    fn sample_registry() -> TopicRegistry {
        let yaml = r#"
version: 1
topics:
  - stream: orders
    topic: intake
    kafka_name: orders.intake
    partitions: 4
    kafka:
      consume: [cg-arroyo-raw]
      produce: false
  - stream: orders
    topic: consolidated
    kafka_name: orders.consolidated
    partitions: 4
    kafka:
      consume: [cg-arroyo-state]
      produce: false
  - stream: olist
    topic: orders_stream
    kafka_name: olist.orders_stream
    partitions: 4
    kafka:
      consume: [cg-arroyo-olist]
      produce: true
"#;
        TopicRegistry::from_yaml(yaml).unwrap()
    }

    #[tokio::test]
    async fn test_process_api_versions() {
        let reg = sample_registry();
        let backend = Arc::new(MockIggyBackend::with_registry(&reg).await);
        let state = BridgeState::new(backend, reg, 1, "127.0.0.1".into(), 9092);

        // Build ApiVersionsRequest frame
        let mut req_header = RequestHeader::default();
        req_header.request_api_key = ApiKey::ApiVersions as i16;
        req_header.request_api_version = 3;
        req_header.correlation_id = 999;
        req_header.client_id = Some("test-client".into());

        let mut buf = BytesMut::new();
        req_header.encode(&mut buf, 2).unwrap();
        let req = ApiVersionsRequest::default();
        req.encode(&mut buf, 3).unwrap();

        let resp_frame = process_frame(buf.freeze(), &state).await.unwrap();
        assert!(resp_frame.len() > 4);

        let mut read_resp = resp_frame;
        let len = read_resp.split_to(4);
        let payload_len = i32::from_be_bytes([len[0], len[1], len[2], len[3]]) as usize;
        assert_eq!(payload_len, read_resp.len());

        let res_header = ResponseHeader::decode(&mut read_resp, 0).unwrap();
        assert_eq!(res_header.correlation_id, 999);

        let res_body = ApiVersionsResponse::decode(&mut read_resp, 3).unwrap();
        assert_eq!(res_body.error_code, 0);
        assert!(!res_body.api_keys.is_empty());
    }

    #[tokio::test]
    async fn test_process_metadata_known_and_unknown() {
        let reg = sample_registry();
        let backend = Arc::new(MockIggyBackend::with_registry(&reg).await);
        let state = BridgeState::new(backend, reg, 1, "127.0.0.1".into(), 9092);

        // Build MetadataRequest frame for "orders.intake" and "unknown.topic"
        let mut req_header = RequestHeader::default();
        req_header.request_api_key = ApiKey::Metadata as i16;
        req_header.request_api_version = 9;
        req_header.correlation_id = 1001;

        let mut req_body = MetadataRequest::default();
        let mut t1 = metadata_request::MetadataRequestTopic::default();
        t1.name = Some(TopicName("orders.intake".into()));
        let mut t2 = metadata_request::MetadataRequestTopic::default();
        t2.name = Some(TopicName("unknown.topic".into()));
        req_body.topics = Some(vec![t1, t2]);

        let mut buf = BytesMut::new();
        req_header.encode(&mut buf, 2).unwrap();
        req_body.encode(&mut buf, 9).unwrap();

        let resp_frame = process_frame(buf.freeze(), &state).await.unwrap();
        let mut read_resp = resp_frame;
        let _ = read_resp.split_to(4);
        let res_header = ResponseHeader::decode(&mut read_resp, 1).unwrap();
        assert_eq!(res_header.correlation_id, 1001);

        let res_body = MetadataResponse::decode(&mut read_resp, 9).unwrap();
        assert_eq!(res_body.topics.len(), 2);

        let intake = res_body.topics.iter().find(|t| t.name.as_ref().unwrap().0.as_str() == "orders.intake").unwrap();
        assert_eq!(intake.error_code, 0);
        assert_eq!(intake.partitions.len(), 4);
        // Verify partition ordering is strictly ascending 0, 1, 2, 3
        for (i, p) in intake.partitions.iter().enumerate() {
            assert_eq!(p.partition_index, i as i32);
        }

        let unknown = res_body.topics.iter().find(|t| t.name.as_ref().unwrap().0.as_str() == "unknown.topic").unwrap();
        assert_eq!(unknown.error_code, 3); // UNKNOWN_TOPIC_OR_PARTITION
        assert_eq!(unknown.partitions.len(), 0);
    }

    #[tokio::test]
    async fn test_process_list_offsets_and_fetch() {
        let reg = sample_registry();
        let backend = Arc::new(MockIggyBackend::with_registry(&reg).await);

        // Preload some messages into orders.intake partition 0
        for i in 0..5 {
            backend
                .send_message(
                    "orders",
                    "intake",
                    0,
                    Some(Bytes::from(format!("key-{i}"))),
                    Bytes::from(format!("{{\"payload\":\"data-{i}\"}}")),
                    Default::default(),
                    1620000000000 + i * 1000,
                )
                .await
                .unwrap();
        }

        let state = BridgeState::new(backend, reg, 1, "127.0.0.1".into(), 9092);

        // Test ListOffsets LATEST (-1)
        let mut req_header = RequestHeader::default();
        req_header.request_api_key = ApiKey::ListOffsets as i16;
        req_header.request_api_version = 7;
        req_header.correlation_id = 1002;

        let mut req_body = ListOffsetsRequest::default();
        let mut t = list_offsets_request::ListOffsetsTopic::default();
        t.name = TopicName("orders.intake".into());
        let mut p = list_offsets_request::ListOffsetsPartition::default();
        p.partition_index = 0;
        p.timestamp = -1; // LATEST
        t.partitions.push(p);
        req_body.topics.push(t);

        let mut buf = BytesMut::new();
        let req_header_v = ApiKey::ListOffsets.request_header_version(7);
        req_header.encode(&mut buf, req_header_v).unwrap();
        req_body.encode(&mut buf, 7).unwrap();

        let resp_frame = process_frame(buf.freeze(), &state).await.unwrap();
        let mut read_resp = resp_frame;
        let _ = read_resp.split_to(4);
        let res_header_v = ApiKey::ListOffsets.response_header_version(7);
        let res_header = ResponseHeader::decode(&mut read_resp, res_header_v).unwrap();
        assert_eq!(res_header.correlation_id, 1002);

        let res_body = ListOffsetsResponse::decode(&mut read_resp, 7).unwrap();
        assert_eq!(res_body.topics[0].partitions[0].offset, 5); // 5 messages -> high watermark 5

        // Test Fetch from offset 0
        let mut fetch_header = RequestHeader::default();
        fetch_header.request_api_key = ApiKey::Fetch as i16;
        fetch_header.request_api_version = 11;
        fetch_header.correlation_id = 1003;

        let mut fetch_body = FetchRequest::default();
        let mut ft = fetch_request::FetchTopic::default();
        ft.topic = TopicName("orders.intake".into());
        let mut fp = fetch_request::FetchPartition::default();
        fp.partition = 0;
        fp.fetch_offset = 0;
        fp.partition_max_bytes = 1024 * 1024;
        ft.partitions.push(fp);
        fetch_body.topics.push(ft);

        let mut f_buf = BytesMut::new();
        let f_req_header_v = ApiKey::Fetch.request_header_version(11);
        fetch_header.encode(&mut f_buf, f_req_header_v).unwrap();
        fetch_body.encode(&mut f_buf, 11).unwrap();

        let fetch_frame = process_frame(f_buf.freeze(), &state).await.unwrap();
        let mut read_fetch = fetch_frame;
        let _ = read_fetch.split_to(4);
        let f_res_header_v = ApiKey::Fetch.response_header_version(11);
        let _ = ResponseHeader::decode(&mut read_fetch, f_res_header_v).unwrap();
        let fetch_res = FetchResponse::decode(&mut read_fetch, 11).unwrap();

        assert_eq!(fetch_res.responses[0].partitions[0].error_code, 0);
        assert_eq!(fetch_res.responses[0].partitions[0].high_watermark, 5);
        assert_eq!(fetch_res.responses[0].partitions[0].last_stable_offset, 5);

        let records_bytes = fetch_res.responses[0].partitions[0].records.as_ref().unwrap();
        let mut rec_slice = records_bytes.clone();
        let record_set = kafka_protocol::records::RecordBatchDecoder::decode(&mut rec_slice).unwrap();
        println!("record_set records: {:?}", record_set.records);
        println!("remaining in rec_slice: {}", rec_slice.len());
        assert_eq!(record_set.records.len(), 5);
        assert_eq!(record_set.records[0].key.as_ref().unwrap().as_ref(), b"key-0");
        assert_eq!(record_set.records[4].key.as_ref().unwrap().as_ref(), b"key-4");
    }

    #[tokio::test]
    async fn test_offset_commit_and_fetch_roundtrip() {
        let reg = sample_registry();
        let backend = Arc::new(MockIggyBackend::with_registry(&reg).await);
        let state = BridgeState::new(backend, reg, 1, "127.0.0.1".into(), 9092);

        // Commit offset 42 for consumer group cg-arroyo-raw on orders.intake partition 0
        let mut commit_hdr = RequestHeader::default();
        commit_hdr.request_api_key = ApiKey::OffsetCommit as i16;
        commit_hdr.request_api_version = 7;
        commit_hdr.correlation_id = 2001;

        let mut commit_body = OffsetCommitRequest::default();
        commit_body.group_id = GroupId("cg-arroyo-raw".into());
        let mut ct = offset_commit_request::OffsetCommitRequestTopic::default();
        ct.name = TopicName("orders.intake".into());
        let mut cp = offset_commit_request::OffsetCommitRequestPartition::default();
        cp.partition_index = 0;
        cp.committed_offset = 42;
        ct.partitions.push(cp);
        commit_body.topics.push(ct);

        let mut buf = BytesMut::new();
        let commit_req_header_v = ApiKey::OffsetCommit.request_header_version(7);
        commit_hdr.encode(&mut buf, commit_req_header_v).unwrap();
        commit_body.encode(&mut buf, 7).unwrap();

        let resp_frame = process_frame(buf.freeze(), &state).await.unwrap();
        let mut read_resp = resp_frame;
        let _ = read_resp.split_to(4);
        let commit_res_header_v = ApiKey::OffsetCommit.response_header_version(7);
        let _ = ResponseHeader::decode(&mut read_resp, commit_res_header_v).unwrap();
        let commit_res = OffsetCommitResponse::decode(&mut read_resp, 7).unwrap();
        assert_eq!(commit_res.topics[0].partitions[0].error_code, 0);

        // Fetch committed offset
        let mut fetch_hdr = RequestHeader::default();
        fetch_hdr.request_api_key = ApiKey::OffsetFetch as i16;
        fetch_hdr.request_api_version = 7;
        fetch_hdr.correlation_id = 2002;

        let mut fetch_body = OffsetFetchRequest::default();
        fetch_body.group_id = GroupId("cg-arroyo-raw".into());
        let mut oft = offset_fetch_request::OffsetFetchRequestTopic::default();
        oft.name = TopicName("orders.intake".into());
        oft.partition_indexes.push(0);
        fetch_body.topics = Some(vec![oft]);

        let mut f_buf = BytesMut::new();
        let fetch_req_header_v = ApiKey::OffsetFetch.request_header_version(7);
        fetch_hdr.encode(&mut f_buf, fetch_req_header_v).unwrap();
        fetch_body.encode(&mut f_buf, 7).unwrap();

        let fetch_frame = process_frame(f_buf.freeze(), &state).await.unwrap();
        let mut read_fetch = fetch_frame;
        let _ = read_fetch.split_to(4);
        let fetch_res_header_v = ApiKey::OffsetFetch.response_header_version(7);
        let _ = ResponseHeader::decode(&mut read_fetch, fetch_res_header_v).unwrap();
        let fetch_res = OffsetFetchResponse::decode(&mut read_fetch, 7).unwrap();

        assert_eq!(fetch_res.topics[0].partitions[0].error_code, 0);
        assert_eq!(fetch_res.topics[0].partitions[0].committed_offset, 42);
    }
}
