//! End-to-end wire protocol contract tests over live TCP loopback.

use bytes::{BufMut, Bytes, BytesMut};
use iggy_kafka_bridge::backend::MockIggyBackend;
use iggy_kafka_bridge::config::TopicRegistry;
use iggy_kafka_bridge::state::BridgeState;
use kafka_protocol::messages::*;
use kafka_protocol::protocol::{Decodable, Encodable};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

fn test_registry() -> TopicRegistry {
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

async fn send_kafka_request(
    stream: &mut TcpStream,
    api_key: ApiKey,
    api_version: i16,
    correlation_id: i32,
    body_encoder: impl FnOnce(&mut BytesMut),
) -> Bytes {
    let mut payload = BytesMut::new();
    let req_header_v = api_key.request_header_version(api_version);

    let mut req_header = RequestHeader::default();
    req_header.request_api_key = api_key as i16;
    req_header.request_api_version = api_version;
    req_header.correlation_id = correlation_id;
    req_header.client_id = Some("test-client".into());
    req_header.encode(&mut payload, req_header_v).unwrap();

    body_encoder(&mut payload);

    let mut frame = BytesMut::with_capacity(4 + payload.len());
    frame.put_i32(payload.len() as i32);
    frame.put(payload);

    stream.write_all(&frame).await.unwrap();
    stream.flush().await.unwrap();

    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await.unwrap();
    let res_len = i32::from_be_bytes(len_buf) as usize;

    let mut res_payload = vec![0u8; res_len];
    stream.read_exact(&mut res_payload).await.unwrap();
    Bytes::from(res_payload)
}

#[tokio::test]
async fn test_tcp_wire_handshake_and_consume() {
    let registry = test_registry();
    let backend = Arc::new(MockIggyBackend::with_registry(&registry).await);

    // Prepopulate 10 messages in orders.intake partition 0
    for i in 0..10 {
        let mut headers = std::collections::HashMap::new();
        headers.insert("replay.seq".to_string(), Bytes::from(format!("{i}")));
        backend
            .send_message(
                "orders",
                "intake",
                0,
                Some(Bytes::from(format!("order-{i}"))),
                Bytes::from(format!("{{\"order_id\":\"order-{i}\",\"amount\":100}}")),
                headers,
                1620000000000 + i * 500,
            )
            .await
            .unwrap();
    }

    // Bind to ephemeral port
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let state = Arc::new(BridgeState::new(
        backend,
        registry,
        1,
        "127.0.0.1".into(),
        addr.port() as i32,
    ));

    // Spawn server
    let server_state = Arc::clone(&state);
    tokio::spawn(async move {
        loop {
            if let Ok((socket, _)) = listener.accept().await {
                let st = Arc::clone(&server_state);
                tokio::spawn(async move {
                    let mut s = socket;
                    let mut len_buf = [0u8; 4];
                    while s.read_exact(&mut len_buf).await.is_ok() {
                        let len = i32::from_be_bytes(len_buf) as usize;
                        let mut body = vec![0u8; len];
                        if s.read_exact(&mut body).await.is_err() {
                            break;
                        }
                        if let Ok(resp) = iggy_kafka_bridge::protocol::process_frame(Bytes::from(body), &st).await {
                            if s.write_all(&resp).await.is_err() {
                                break;
                            }
                            let _ = s.flush().await;
                        }
                    }
                });
            }
        }
    });

    let mut client = TcpStream::connect(addr).await.unwrap();

    // 1. ApiVersions v3
    let mut resp = send_kafka_request(&mut client, ApiKey::ApiVersions, 3, 101, |buf| {
        ApiVersionsRequest::default().encode(buf, 3).unwrap();
    }).await;
    let _ = ResponseHeader::decode(&mut resp, ApiKey::ApiVersions.response_header_version(3)).unwrap();
    let api_versions = ApiVersionsResponse::decode(&mut resp, 3).unwrap();
    assert_eq!(api_versions.error_code, 0);
    assert!(api_versions.api_keys.iter().any(|k| k.api_key == ApiKey::Fetch as i16));

    // 2. Metadata v9 for all topics
    let mut meta_resp = send_kafka_request(&mut client, ApiKey::Metadata, 9, 102, |buf| {
        let mut req = MetadataRequest::default();
        req.topics = None; // all topics
        req.encode(buf, 9).unwrap();
    }).await;
    let _ = ResponseHeader::decode(&mut meta_resp, ApiKey::Metadata.response_header_version(9)).unwrap();
    let metadata = MetadataResponse::decode(&mut meta_resp, 9).unwrap();
    assert_eq!(metadata.brokers.len(), 1);
    assert_eq!(metadata.brokers[0].port, addr.port() as i32);
    assert_eq!(metadata.topics.len(), 3); // orders.intake, orders.consolidated, olist.orders_stream

    let intake = metadata.topics.iter().find(|t| t.name.as_ref().unwrap().0.as_str() == "orders.intake").unwrap();
    assert_eq!(intake.error_code, 0);
    assert_eq!(intake.partitions.len(), 4);
    for (i, p) in intake.partitions.iter().enumerate() {
        assert_eq!(p.partition_index, i as i32);
    }

    // 3. FindCoordinator v2
    let mut coord_resp = send_kafka_request(&mut client, ApiKey::FindCoordinator, 2, 103, |buf| {
        let mut req = FindCoordinatorRequest::default();
        req.key = "cg-arroyo-raw".into();
        req.encode(buf, 2).unwrap();
    }).await;
    let _ = ResponseHeader::decode(&mut coord_resp, ApiKey::FindCoordinator.response_header_version(2)).unwrap();
    let coord = FindCoordinatorResponse::decode(&mut coord_resp, 2).unwrap();
    assert_eq!(coord.error_code, 0);
    assert_eq!(coord.port, addr.port() as i32);

    // 4. ListOffsets v7 (earliest and latest)
    let mut offset_resp = send_kafka_request(&mut client, ApiKey::ListOffsets, 7, 104, |buf| {
        let mut req = ListOffsetsRequest::default();
        let mut t = list_offsets_request::ListOffsetsTopic::default();
        t.name = TopicName("orders.intake".into());

        let mut p_earliest = list_offsets_request::ListOffsetsPartition::default();
        p_earliest.partition_index = 0;
        p_earliest.timestamp = -2; // EARLIEST

        t.partitions.push(p_earliest);
        req.topics.push(t);
        req.encode(buf, 7).unwrap();
    }).await;
    let _ = ResponseHeader::decode(&mut offset_resp, ApiKey::ListOffsets.response_header_version(7)).unwrap();
    let list_offsets = ListOffsetsResponse::decode(&mut offset_resp, 7).unwrap();
    assert_eq!(list_offsets.topics[0].partitions[0].offset, 0); // Earliest is always 0 per AD-28

    // 5. Fetch v11 (reading from offset 0)
    let mut fetch_resp = send_kafka_request(&mut client, ApiKey::Fetch, 11, 105, |buf| {
        let mut req = FetchRequest::default();
        let mut t = fetch_request::FetchTopic::default();
        t.topic = TopicName("orders.intake".into());
        let mut p = fetch_request::FetchPartition::default();
        p.partition = 0;
        p.fetch_offset = 0;
        p.partition_max_bytes = 1024 * 1024;
        t.partitions.push(p);
        req.topics.push(t);
        req.encode(buf, 11).unwrap();
    }).await;
    let _ = ResponseHeader::decode(&mut fetch_resp, ApiKey::Fetch.response_header_version(11)).unwrap();
    let fetch = FetchResponse::decode(&mut fetch_resp, 11).unwrap();
    assert_eq!(fetch.responses[0].partitions[0].error_code, 0);
    assert_eq!(fetch.responses[0].partitions[0].high_watermark, 10);
    assert_eq!(fetch.responses[0].partitions[0].last_stable_offset, 10);
    assert_eq!(fetch.responses[0].partitions[0].log_start_offset, -1);

    let rec_bytes = fetch.responses[0].partitions[0].records.as_ref().unwrap();
    let mut rec_slice = rec_bytes.clone();
    let record_set = kafka_protocol::records::RecordBatchDecoder::decode(&mut rec_slice).unwrap();
    assert_eq!(record_set.records.len(), 10);
    assert_eq!(record_set.records[0].offset, 0);
    assert_eq!(record_set.records[9].offset, 9);
    assert_eq!(record_set.records[0].key.as_ref().unwrap().as_ref(), b"order-0");

    // 6. OffsetCommit v7 and OffsetFetch v7
    let mut commit_resp = send_kafka_request(&mut client, ApiKey::OffsetCommit, 7, 106, |buf| {
        let mut req = OffsetCommitRequest::default();
        req.group_id = GroupId("cg-arroyo-raw".into());
        let mut t = offset_commit_request::OffsetCommitRequestTopic::default();
        t.name = TopicName("orders.intake".into());
        let mut p = offset_commit_request::OffsetCommitRequestPartition::default();
        p.partition_index = 0;
        p.committed_offset = 9; // committed offset
        t.partitions.push(p);
        req.topics.push(t);
        req.encode(buf, 7).unwrap();
    }).await;
    let _ = ResponseHeader::decode(&mut commit_resp, ApiKey::OffsetCommit.response_header_version(7)).unwrap();
    let commit = OffsetCommitResponse::decode(&mut commit_resp, 7).unwrap();
    assert_eq!(commit.topics[0].partitions[0].error_code, 0);

    let mut fetch_offset_resp = send_kafka_request(&mut client, ApiKey::OffsetFetch, 7, 107, |buf| {
        let mut req = OffsetFetchRequest::default();
        req.group_id = GroupId("cg-arroyo-raw".into());
        let mut t = offset_fetch_request::OffsetFetchRequestTopic::default();
        t.name = TopicName("orders.intake".into());
        t.partition_indexes.push(0);
        req.topics = Some(vec![t]);
        req.encode(buf, 7).unwrap();
    }).await;
    let _ = ResponseHeader::decode(&mut fetch_offset_resp, ApiKey::OffsetFetch.response_header_version(7)).unwrap();
    let fetch_offset = OffsetFetchResponse::decode(&mut fetch_offset_resp, 7).unwrap();
    assert_eq!(fetch_offset.topics[0].partitions[0].error_code, 0);
    assert_eq!(fetch_offset.topics[0].partitions[0].committed_offset, 9);

    // 7. Unknown topic rejection with UNKNOWN_TOPIC_OR_PARTITION (3)
    let mut unknown_resp = send_kafka_request(&mut client, ApiKey::Metadata, 9, 108, |buf| {
        let mut req = MetadataRequest::default();
        let mut t = metadata_request::MetadataRequestTopic::default();
        t.name = Some(TopicName("nonexistent.topic".into()));
        req.topics = Some(vec![t]);
        req.encode(buf, 9).unwrap();
    }).await;
    let _ = ResponseHeader::decode(&mut unknown_resp, ApiKey::Metadata.response_header_version(9)).unwrap();
    let unknown_meta = MetadataResponse::decode(&mut unknown_resp, 9).unwrap();
    assert_eq!(unknown_meta.topics[0].error_code, 3);
}
