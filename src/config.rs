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

/// Resolved replica identity and advertised broker endpoint for stateless multi-replica deployments (Story 16.1, FR-34).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicaIdentity {
    pub node_id: i32,
    pub advertised_host: String,
    pub advertised_port: i32,
    pub pod_name: Option<String>,
    pub pod_ip: Option<String>,
}

/// Derive a deterministic positive Kafka broker `node_id` and `advertised_host` from explicit configuration
/// or Kubernetes downward API environment variables (`POD_NAME`, `POD_IP`).
pub fn resolve_replica_identity(
    explicit_node_id: Option<i32>,
    pod_name: Option<&str>,
    default_advertised_host: &str,
    pod_ip: Option<&str>,
    advertised_port: i32,
) -> ReplicaIdentity {
    let clean_pod_name = pod_name
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(ToOwned::to_owned);
    let clean_pod_ip = pod_ip
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(ToOwned::to_owned);

    let node_id = match explicit_node_id {
        Some(id) if id > 0 => id,
        _ => match clean_pod_name.as_deref() {
            Some(name) => derive_node_id_from_pod_name(name),
            None => 1,
        },
    };

    let advertised_host = match clean_pod_ip.as_deref() {
        Some(ip)
            if default_advertised_host.trim().is_empty()
                || default_advertised_host == "bridge"
                || default_advertised_host == "0.0.0.0" =>
        {
            ip.to_string()
        }
        _ => default_advertised_host.to_string(),
    };

    ReplicaIdentity {
        node_id,
        advertised_host,
        advertised_port,
        pod_name: clean_pod_name,
        pod_ip: clean_pod_ip,
    }
}

fn derive_node_id_from_pod_name(pod_name: &str) -> i32 {
    if let Some(suffix) = pod_name.rsplit('-').next()
        && let Ok(ordinal) = suffix.parse::<i32>()
    {
        return if ordinal <= 0 { 1 } else { ordinal };
    }
    // Deterministic FNV-1a 32-bit hash mapped to 1..=99_999 for Deployment pod hashes
    let mut hash: u32 = 2_166_136_261;
    for b in pod_name.as_bytes() {
        hash ^= u32::from(*b);
        hash = hash.wrapping_mul(16_777_619);
    }
    ((hash % 99_999) as i32) + 1
}
