//! Tests for topic registry loading and validation.

use iggy_kafka_bridge::config::TopicRegistry;
use std::path::Path;

#[test]
fn test_load_real_contracts_topics_yml() {
    let path = Path::new("../ferrous-cx/contracts/topics.yml");
    if path.exists() {
        let registry = TopicRegistry::load_from_file(path).expect("Failed to load topics.yml");
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
}
