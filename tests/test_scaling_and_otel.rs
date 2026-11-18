//! Integration tests for Stateless Multi-Replica Scaling, Connection Pooling, Pod Identity,
//! and OpenTelemetry Observability (Stories 16.1 & 16.2, FR-34, SM-9).

use bytes::{Bytes, BytesMut};
use iggy_kafka_proxy::backend::{IggyBackend, MockIggyBackend};
use iggy_kafka_proxy::config::{resolve_replica_identity, TopicRegistry};
use iggy_kafka_proxy::protocol::process_frame;
use iggy_kafka_proxy::state::BridgeState;
use iggy_kafka_proxy::telemetry::{
    init_telemetry, run_metrics_server_with_listener, BridgeMetrics, LogFormat, OtelConfig,
    OtelTier,
};
use kafka_protocol::messages::*;
use kafka_protocol::protocol::{Decodable, Encodable};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

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
  - stream: benchmark
    topic: hpc_mqbench
    kafka_name: benchmark.hpc_mqbench
    partitions: 4
    kafka:
      consume: [cg-arroyo-benchmark]
      produce: true
"#;
    TopicRegistry::from_yaml(yaml).unwrap()
}

#[tokio::test]
async fn test_connection_pool_round_robin_distribution() {
    let reg = sample_registry();
    let backend = MockIggyBackend::with_registry_and_pool_size(&reg, 4).await;
    assert_eq!(backend.pool_size(), 4);

    // Execute 12 backend operations across the 4-connection pool
    for p in 0..12 {
        let part = (p % 4) as u32;
        let _ = backend
            .get_partition_offsets("benchmark", "hpc_mqbench", part)
            .await
            .unwrap();
    }

    let counts = backend.pool_dispatch_counts();
    assert_eq!(counts, vec![3, 3, 3, 3]);

    // Zero pool size clamps safely to 1 without panic
    let zero_clamped = MockIggyBackend::new_with_pool_size(0);
    assert_eq!(zero_clamped.pool_size(), 1);
}

#[test]
fn test_kubernetes_pod_identity_resolution() {
    // 1. StatefulSet ordinal pod name + POD_IP fallback
    let id1 = resolve_replica_identity(
        None,
        Some("iggy-kafka-bridge-3"),
        "bridge",
        Some("10.244.1.42"),
        9092,
    );
    assert_eq!(id1.node_id, 3);
    assert_eq!(id1.advertised_host, "10.244.1.42");
    assert_eq!(id1.advertised_port, 9092);

    // 2. StatefulSet ordinal 0 maps to positive node_id = 1
    let id0 = resolve_replica_identity(
        None,
        Some("iggy-kafka-bridge-0"),
        "bridge",
        Some("10.244.1.10"),
        9092,
    );
    assert_eq!(id0.node_id, 1);
    assert_eq!(id0.advertised_host, "10.244.1.10");

    // 3. Deployment ReplicaSet pod hash derives deterministic positive node_id
    let id_hash_a = resolve_replica_identity(
        None,
        Some("iggy-kafka-bridge-7d8f9c6b4-k8x9q"),
        "iggy-kafka-bridge.ferrous-streaming.svc.cluster.local",
        Some("10.244.2.15"),
        9092,
    );
    let id_hash_b = resolve_replica_identity(
        None,
        Some("iggy-kafka-bridge-7d8f9c6b4-k8x9q"),
        "iggy-kafka-bridge.ferrous-streaming.svc.cluster.local",
        Some("10.244.2.15"),
        9092,
    );
    assert!(id_hash_a.node_id > 0);
    assert_eq!(id_hash_a.node_id, id_hash_b.node_id);
    // Explicit service DNS hostname is preserved when not "bridge" or "0.0.0.0"
    assert_eq!(
        id_hash_a.advertised_host,
        "iggy-kafka-bridge.ferrous-streaming.svc.cluster.local"
    );

    // 4. Explicit node_id overrides POD_NAME
    let id_explicit = resolve_replica_identity(
        Some(7),
        Some("iggy-kafka-bridge-2"),
        "127.0.0.1",
        None,
        9092,
    );
    assert_eq!(id_explicit.node_id, 7);
    assert_eq!(id_explicit.advertised_host, "127.0.0.1");
}

#[tokio::test]
async fn test_multi_replica_stateless_interoperability() {
    let reg = sample_registry();
    let shared_backend = Arc::new(MockIggyBackend::with_registry_and_pool_size(&reg, 4).await);

    // Replica 1 (Pod 1) and Replica 2 (Pod 2) share the stateless backend
    let replica_1 = BridgeState::new(
        Arc::clone(&shared_backend),
        reg.clone(),
        1,
        "10.244.0.11".into(),
        9092,
    );
    let replica_2 = BridgeState::new(
        Arc::clone(&shared_backend),
        reg,
        2,
        "10.244.0.12".into(),
        9092,
    );

    // 1. Client produces a record through Replica 1
    let mut rec_buf = BytesMut::new();
    let rec = kafka_protocol::records::Record {
        transactional: false,
        control: false,
        delete_horizon: false,
        partition_leader_epoch: -1,
        producer_id: -1,
        producer_epoch: -1,
        timestamp_type: kafka_protocol::records::TimestampType::Creation,
        offset: 0,
        sequence: 0,
        timestamp: 1_731_640_000_000,
        key: Some(Bytes::from("bench-key-1")),
        value: Some(Bytes::from("{\"record_id\":\"run-1:00000001\"}")),
        headers: Default::default(),
    };
    let options = kafka_protocol::records::RecordEncodeOptions {
        version: 2,
        compression: kafka_protocol::records::Compression::None,
    };
    kafka_protocol::records::RecordBatchEncoder::encode(&mut rec_buf, &[rec], &options).unwrap();

    let mut prod_hdr = RequestHeader::default();
    prod_hdr.request_api_key = ApiKey::Produce as i16;
    prod_hdr.request_api_version = 7;
    prod_hdr.correlation_id = 10;

    let mut prod_req = ProduceRequest::default();
    prod_req.acks = -1;
    let mut td = produce_request::TopicProduceData::default();
    td.name = TopicName("benchmark.hpc_mqbench".into());
    let mut pd = produce_request::PartitionProduceData::default();
    pd.index = 0;
    pd.records = Some(rec_buf.freeze());
    td.partition_data.push(pd);
    prod_req.topic_data.push(td);

    let mut p_buf = BytesMut::new();
    prod_hdr
        .encode(&mut p_buf, ApiKey::Produce.request_header_version(7))
        .unwrap();
    prod_req.encode(&mut p_buf, 7).unwrap();

    let prod_resp_frame = process_frame(p_buf.freeze(), &replica_1).await.unwrap();
    assert!(prod_resp_frame.len() > 4);
    assert_eq!(replica_1.metrics.records_produced_total(), 1);

    // 2. Replica 1 commits offset 1 for cg-arroyo-benchmark
    let mut commit_hdr = RequestHeader::default();
    commit_hdr.request_api_key = ApiKey::OffsetCommit as i16;
    commit_hdr.request_api_version = 7;
    commit_hdr.correlation_id = 11;

    let mut commit_req = OffsetCommitRequest::default();
    commit_req.group_id = GroupId("cg-arroyo-benchmark".into());
    let mut ct = offset_commit_request::OffsetCommitRequestTopic::default();
    ct.name = TopicName("benchmark.hpc_mqbench".into());
    let mut cp = offset_commit_request::OffsetCommitRequestPartition::default();
    cp.partition_index = 0;
    cp.committed_offset = 1;
    ct.partitions.push(cp);
    commit_req.topics.push(ct);

    let mut c_buf = BytesMut::new();
    commit_hdr
        .encode(&mut c_buf, ApiKey::OffsetCommit.request_header_version(7))
        .unwrap();
    commit_req.encode(&mut c_buf, 7).unwrap();
    let _ = process_frame(c_buf.freeze(), &replica_1).await.unwrap();

    // 3. Another Arroyo worker connects to Replica 2 and fetches both the committed offset and the record!
    let mut of_hdr = RequestHeader::default();
    of_hdr.request_api_key = ApiKey::OffsetFetch as i16;
    of_hdr.request_api_version = 7;
    of_hdr.correlation_id = 20;

    let mut of_req = OffsetFetchRequest::default();
    of_req.group_id = GroupId("cg-arroyo-benchmark".into());
    let mut oft = offset_fetch_request::OffsetFetchRequestTopic::default();
    oft.name = TopicName("benchmark.hpc_mqbench".into());
    oft.partition_indexes.push(0);
    of_req.topics = Some(vec![oft]);

    let mut of_buf = BytesMut::new();
    of_hdr
        .encode(&mut of_buf, ApiKey::OffsetFetch.request_header_version(7))
        .unwrap();
    of_req.encode(&mut of_buf, 7).unwrap();

    let mut of_resp_frame = process_frame(of_buf.freeze(), &replica_2).await.unwrap();
    let _ = of_resp_frame.split_to(4);
    let _ = ResponseHeader::decode(
        &mut of_resp_frame,
        ApiKey::OffsetFetch.response_header_version(7),
    )
    .unwrap();
    let of_resp = OffsetFetchResponse::decode(&mut of_resp_frame, 7).unwrap();
    assert_eq!(of_resp.topics[0].partitions[0].committed_offset, 1);

    // Fetch record on Replica 2
    let mut f_hdr = RequestHeader::default();
    f_hdr.request_api_key = ApiKey::Fetch as i16;
    f_hdr.request_api_version = 11;
    f_hdr.correlation_id = 21;

    let mut f_req = FetchRequest::default();
    let mut ft = fetch_request::FetchTopic::default();
    ft.topic = TopicName("benchmark.hpc_mqbench".into());
    let mut fp = fetch_request::FetchPartition::default();
    fp.partition = 0;
    fp.fetch_offset = 0;
    fp.partition_max_bytes = 1024 * 1024;
    ft.partitions.push(fp);
    f_req.topics.push(ft);

    let mut f_buf = BytesMut::new();
    f_hdr
        .encode(&mut f_buf, ApiKey::Fetch.request_header_version(11))
        .unwrap();
    f_req.encode(&mut f_buf, 11).unwrap();

    let mut f_resp_frame = process_frame(f_buf.freeze(), &replica_2).await.unwrap();
    let _ = f_resp_frame.split_to(4);
    let _ =
        ResponseHeader::decode(&mut f_resp_frame, ApiKey::Fetch.response_header_version(11))
            .unwrap();
    let f_resp = FetchResponse::decode(&mut f_resp_frame, 11).unwrap();
    assert_eq!(f_resp.responses[0].partitions[0].high_watermark, 1);
    assert_eq!(replica_2.metrics.records_fetched_total(), 1);
}

#[test]
fn test_opentelemetry_three_tier_configuration() {
    // Tier 1: OTEL_SDK_DISABLED=true
    let mut env1 = HashMap::new();
    env1.insert("OTEL_SDK_DISABLED".to_string(), "true".to_string());
    env1.insert(
        "OTEL_EXPORTER_OTLP_ENDPOINT".to_string(),
        "http://localhost:4317".to_string(),
    );
    let cfg1 = OtelConfig::from_map(&env1);
    assert_eq!(cfg1.tier(), OtelTier::Disabled);
    init_telemetry(&cfg1);

    // Tier 2: Console exporter
    let mut env2 = HashMap::new();
    env2.insert("OTEL_TRACES_EXPORTER".to_string(), "console".to_string());
    env2.insert("BRIDGE_LOG_FORMAT".to_string(), "json".to_string());
    let cfg2 = OtelConfig::from_map(&env2);
    assert_eq!(cfg2.tier(), OtelTier::Console);
    assert_eq!(cfg2.log_format, LogFormat::Json);

    // Tier 3: OTLP endpoint configured
    let mut env3 = HashMap::new();
    env3.insert(
        "OTEL_SERVICE_NAME".to_string(),
        "iggy-kafka-bridge-k8s".to_string(),
    );
    env3.insert(
        "OTEL_EXPORTER_OTLP_ENDPOINT".to_string(),
        "http://otel-collector:4317".to_string(),
    );
    let cfg3 = OtelConfig::from_map(&env3);
    assert_eq!(cfg3.tier(), OtelTier::Otlp);
    assert_eq!(cfg3.service_name, "iggy-kafka-bridge-k8s");
}

async fn http_get(addr: std::net::SocketAddr, path: &str) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let req = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
    stream.write_all(req.as_bytes()).await.unwrap();
    stream.flush().await.unwrap();

    let mut resp_bytes = Vec::new();
    stream.read_to_end(&mut resp_bytes).await.unwrap();
    let resp_str = String::from_utf8_lossy(&resp_bytes).to_string();
    let status_line = resp_str.lines().next().unwrap_or("");
    let status_code = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(0);
    let body = resp_str
        .split("\r\n\r\n")
        .nth(1)
        .unwrap_or("")
        .to_string();
    (status_code, body)
}

#[tokio::test]
async fn test_observability_http_server_metrics_healthz_and_readyz() {
    let metrics = Arc::new(BridgeMetrics::new(2, 4));
    metrics.record_request("Produce", 420, false);
    metrics.record_request("Fetch", 680, false);
    metrics.record_produced(25);
    metrics.record_fetched(25);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_metrics = Arc::clone(&metrics);
    tokio::spawn(async move {
        let _ = run_metrics_server_with_listener(listener, server_metrics).await;
    });

    // 1. GET /healthz
    let (code_h, body_h) = http_get(addr, "/healthz").await;
    assert_eq!(code_h, 200);
    assert!(body_h.contains("\"status\":\"healthy\""));
    assert!(body_h.contains("\"node_id\":2"));

    // 2. GET /readyz (ready = true)
    let (code_r1, body_r1) = http_get(addr, "/readyz").await;
    assert_eq!(code_r1, 200);
    assert!(body_r1.contains("\"ready\":true"));
    assert!(body_r1.contains("\"iggy_pool_size\":4"));

    // 3. GET /readyz when marked unready -> 503 Service Unavailable
    metrics.set_ready(false);
    let (code_r2, body_r2) = http_get(addr, "/readyz").await;
    assert_eq!(code_r2, 503);
    assert!(body_r2.contains("\"ready\":false"));
    metrics.set_ready(true);

    // 4. GET /metrics (Prometheus text format)
    let (code_m, body_m) = http_get(addr, "/metrics").await;
    assert_eq!(code_m, 200);
    assert!(body_m.contains("bridge_iggy_pool_size{node_id=\"2\"} 4"));
    assert!(body_m.contains("bridge_requests_total{node_id=\"2\",api_key=\"Produce\"} 1"));
    assert!(body_m.contains("bridge_requests_total{node_id=\"2\",api_key=\"Fetch\"} 1"));
    assert!(body_m.contains("bridge_records_produced_total{node_id=\"2\"} 25"));
    assert!(body_m.contains("bridge_records_fetched_total{node_id=\"2\"} 25"));
    assert!(body_m.contains("bridge_inflight_requests{node_id=\"2\"} 0"));
    assert!(body_m.contains("bridge_request_duration_us_count{node_id=\"2\"} 2"));
}
