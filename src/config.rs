//! Configuration and topic registry parsing for iggy-kafka-bridge (AD-27, AD-28).

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TopicRegistry {
    pub version: u32,
    pub topics: Vec<TopicEntry>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TopicEntry {
    pub stream: String,
    pub topic: String,
    pub kafka_name: String,
    pub partitions: u32,
    #[serde(default)]
    pub retention: Option<String>,
    #[serde(default)]
    pub durability: Option<String>,
    pub kafka: KafkaTopicPermissions,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct KafkaTopicPermissions {
    #[serde(default)]
    pub consume: Vec<String>,
    #[serde(default)]
    pub produce: bool,
}

impl TopicRegistry {
    /// Loads topic registry from YAML file (e.g. ferrous-cx/contracts/topics.yml).
    pub fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let content = std::fs::read_to_string(path)?;
        let registry: TopicRegistry = serde_yaml::from_str(&content)?;
        Ok(registry)
    }

    /// Loads topic registry from YAML string slice.
    pub fn from_yaml(content: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let registry: TopicRegistry = serde_yaml::from_str(content)?;
        Ok(registry)
    }

    /// Builds a lookup map from kafka_name to TopicEntry.
    pub fn topic_map(&self) -> HashMap<String, TopicEntry> {
        let mut map = HashMap::new();
        for t in &self.topics {
            map.insert(t.kafka_name.clone(), t.clone());
        }
        map
    }

    /// Verifies if a given kafka topic name is allowed for consume by the given consumer group.
    pub fn is_consume_allowed(&self, kafka_name: &str, group: &str) -> bool {
        if let Some(entry) = self.topics.iter().find(|t| t.kafka_name == kafka_name) {
            entry.kafka.consume.iter().any(|g| g == group)
        } else {
            false
        }
    }

    /// Verifies if a given kafka topic name is allowed for produce.
    pub fn is_produce_allowed(&self, kafka_name: &str) -> bool {
        if let Some(entry) = self.topics.iter().find(|t| t.kafka_name == kafka_name) {
            entry.kafka.produce
        } else {
            false
        }
    }
}
