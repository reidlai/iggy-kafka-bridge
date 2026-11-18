//! Tests for topic registry loading and validation.

use iggy_kafka_bridge::config::TopicRegistry;
use std::path::Path;

#[test]
fn test_load_real_contracts_topics_yml() {
    let path = Path::new("tests/fixtures/topics.yml");
    let registry = TopicRegistry::load_from_file(path).expect("Failed to load topics.yml fixture");
    assert_eq!(registry.version, 1);
    assert!(!registry.topics.is_empty());

    let map = registry.topic_map();
    assert!(map.contains_key("orders.intake"));
    assert!(map.contains_key("orders.consolidated"));
    assert!(map.contains_key("olist.orders_stream"));

    assert!(registry.is_consume_allowed("orders.intake", "cg-arroyo-raw"));
    assert!(!registry.is_consume_allowed("orders.intake", "unauthorized-group"));
    assert!(registry.is_produce_allowed("olist.orders_stream"));
    assert!(!registry.is_produce_allowed("orders.intake"));
}

#[test]
fn test_load_toml_topic_map() {
    let toml_content = r#"
default_stream = "kafka"

[topics."benchmark.hpc_mqbench"]
stream = "benchmark"
topic  = "hpc_mqbench"

[topics."orders.intake"]
stream = "orders"
topic  = "intake"
"#;
    let temp_dir = tempfile::tempdir().unwrap();
    let toml_path = temp_dir.path().join("topic-map.toml");
    std::fs::write(&toml_path, toml_content).unwrap();

    let registry = TopicRegistry::load_from_file(&toml_path).expect("Failed to load topic-map.toml");
    assert_eq!(registry.version, 1);
    assert_eq!(registry.topics.len(), 2);
    assert!(registry.is_consume_allowed("orders.intake", "cg-arroyo-raw"));
    assert!(registry.is_produce_allowed("benchmark.hpc_mqbench"));
}
