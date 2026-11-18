//! iggy-kafka-bridge CLI binary with multi-replica connection pooling, Kubernetes pod identity resolution,
//! and OpenTelemetry observability (Stories 16.1 & 16.2, FR-33, FR-34).

use clap::Parser;
use iggy_kafka_bridge::backend::{LiveIggyBackend, DEFAULT_IGGY_POOL_SIZE};
use iggy_kafka_bridge::config::{resolve_replica_identity, TopicRegistry};
use iggy_kafka_bridge::server::BridgeServer;
use iggy_kafka_bridge::state::BridgeState;
use iggy_kafka_bridge::telemetry::{
    init_telemetry, run_metrics_server, BridgeMetrics, OtelConfig, DEFAULT_METRICS_BIND_ADDR,
};
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{error, info};

#[derive(Parser, Debug)]
#[command(
    name = "iggy-kafka-bridge",
    about = "Horizontally scalable pure-Rust Kafka wire protocol bridge to Apache Iggy streaming broker",
    version = "0.1.0"
)]
struct Args {
    /// Path to topics.yml topic registry
    #[arg(short, long, env = "IGGY_KAFKA_TOPICS_PATH", default_value = "topics.yml")]
    config: PathBuf,

    /// Iggy server TCP address
    #[arg(long, env = "IGGY_SERVER_ADDRESS", default_value = "127.0.0.1:8090")]
    iggy_address: String,

    /// Iggy bridge username
    #[arg(long, env = "IGGY_USERNAME", default_value = "bridge")]
    iggy_username: String,

    /// Iggy bridge password
    #[arg(long, env = "IGGY_KAFKA_IGGY_PASSWORD", default_value = "bridge")]
    iggy_password: String,

    /// Number of pooled Apache Iggy TCP connections per replica
    #[arg(long, env = "IGGY_CONNECTION_POOL_SIZE", default_value_t = DEFAULT_IGGY_POOL_SIZE)]
    pool_size: usize,

    /// Bind address for compose / cluster network listener
    #[arg(long, env = "BRIDGE_BIND_COMPOSE", default_value = "0.0.0.0:9092")]
    listen_bridge: String,

    /// Advertised host for compose / cluster network listener
    #[arg(long, env = "BRIDGE_ADVERTISED_COMPOSE_HOST", default_value = "bridge")]
    advertised_bridge_host: String,

    /// Advertised port for compose / cluster network listener
    #[arg(long, env = "BRIDGE_ADVERTISED_COMPOSE_PORT", default_value_t = 9092)]
    advertised_bridge_port: i32,

    /// Optional bind address for host loopback listener
    #[arg(long, env = "BRIDGE_BIND_HOST")]
    listen_host: Option<String>,

    /// Advertised host for loopback listener
    #[arg(long, env = "BRIDGE_ADVERTISED_HOST_HOST", default_value = "127.0.0.1")]
    advertised_host_host: String,

    /// Advertised port for loopback listener
    #[arg(long, env = "BRIDGE_ADVERTISED_HOST_PORT", default_value_t = 9092)]
    advertised_host_port: i32,

    /// Explicit Broker Node ID (if omitted, derived from POD_NAME or defaults to 1)
    #[arg(long, env = "BRIDGE_NODE_ID")]
    node_id: Option<i32>,

    /// Kubernetes Pod Name injected via downward API
    #[arg(long, env = "POD_NAME")]
    pod_name: Option<String>,

    /// Kubernetes Pod IP injected via downward API
    #[arg(long, env = "POD_IP")]
    pod_ip: Option<String>,

    /// Bind address for HTTP observability server (/metrics, /healthz, /readyz)
    #[arg(long, env = "BRIDGE_METRICS_ADDR", default_value = DEFAULT_METRICS_BIND_ADDR)]
    metrics_listen: String,

    /// Run healthcheck and exit (0 = healthy, 1 = unhealthy)
    #[arg(long)]
    healthcheck: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let otel_cfg = OtelConfig::from_env();
    init_telemetry(&otel_cfg);

    let args = Args::parse();

    let config_path = if (args.config == PathBuf::from("topics.yml") || !args.config.exists())
        && let Ok(legacy_map) = std::env::var("IGGY_KAFKA_TOPIC_MAP_PATH")
    {
        PathBuf::from(legacy_map)
    } else {
        args.config.clone()
    };

    let iggy_addr = if args.iggy_address == "127.0.0.1:8090" {
        std::env::var("IGGY_KAFKA_IGGY_ADDR").unwrap_or(args.iggy_address)
    } else {
        args.iggy_address
    };

    let iggy_user = if args.iggy_username == "bridge" {
        std::env::var("BRIDGE_IGGY_USERNAME")
            .or_else(|_| std::env::var("IGGY_KAFKA_IGGY_USERNAME"))
            .unwrap_or(args.iggy_username)
    } else {
        args.iggy_username
    };

    let iggy_pw = if args.iggy_password == "bridge" {
        std::env::var("BRIDGE_IGGY_PASSWORD")
            .or_else(|_| std::env::var("IGGY_KAFKA_IGGY_PASSWORD"))
            .unwrap_or(args.iggy_password)
    } else {
        args.iggy_password
    };

    let explicit_node_id = args.node_id.or_else(|| {
        std::env::var("IGGY_KAFKA_NODE_ID").ok().and_then(|v| v.parse().ok())
    });

    let adv_host = if args.advertised_bridge_host == "bridge" {
        std::env::var("IGGY_KAFKA_ADVERTISED_HOST")
            .unwrap_or(args.advertised_bridge_host)
    } else {
        args.advertised_bridge_host
    };

    let adv_port = if args.advertised_bridge_port == 9092 {
        std::env::var("IGGY_KAFKA_ADVERTISED_PORT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(args.advertised_bridge_port)
    } else {
        args.advertised_bridge_port
    };

    if args.healthcheck {
        info!("Running healthcheck against Iggy broker at {}...", iggy_addr);
        match LiveIggyBackend::connect(&iggy_addr, &iggy_user, &iggy_pw).await {
            Ok(_) => {
                info!("Healthcheck passed: successfully connected and authenticated to Iggy broker.");
                std::process::exit(0);
            }
            Err(e) => {
                error!("Healthcheck failed: {}", e);
                std::process::exit(1);
            }
        }
    }

    let identity = resolve_replica_identity(
        explicit_node_id,
        args.pod_name.as_deref(),
        &adv_host,
        args.pod_ip.as_deref(),
        adv_port,
    );

    info!(
        service = %otel_cfg.service_name,
        otel_tier = ?otel_cfg.tier(),
        node_id = identity.node_id,
        advertised_host = %identity.advertised_host,
        advertised_port = identity.advertised_port,
        pool_size = args.pool_size.max(1),
        "Initializing iggy-kafka-bridge replica..."
    );

    info!("Loading topic registry from {:?}...", config_path);
    let registry = TopicRegistry::load_from_file(&config_path)?;
    info!("Loaded {} topic definitions.", registry.topics.len());

    info!(
        "Connecting pool of {} client(s) to Iggy broker at {}...",
        args.pool_size.max(1),
        iggy_addr
    );
    let backend = Arc::new(
        LiveIggyBackend::connect_pool(
            &iggy_addr,
            &iggy_user,
            &iggy_pw,
            args.pool_size,
        )
        .await?,
    );
    info!(
        "Successfully connected pool ({} connections) to Iggy broker.",
        backend.pool_size()
    );

    let metrics = Arc::new(BridgeMetrics::new(identity.node_id, backend.pool_size()));
    let mut handles = Vec::new();

    // Observability HTTP listener (/metrics, /healthz, /readyz)
    if !args.metrics_listen.trim().is_empty() {
        let m_addr = args.metrics_listen.clone();
        let m_ref = Arc::clone(&metrics);
        handles.push(tokio::spawn(async move {
            if let Err(e) = run_metrics_server(m_addr, m_ref).await {
                error!("Observability HTTP server error: {e}");
            }
        }));
    }

    // Primary compose / cluster listener
    let state_bridge = Arc::new(BridgeState::with_metrics(
        Arc::clone(&backend),
        registry.clone(),
        identity.node_id,
        identity.advertised_host,
        identity.advertised_port,
        Arc::clone(&metrics),
    ));
    let server_bridge = BridgeServer::new(state_bridge, args.listen_bridge);
    handles.push(tokio::spawn(async move {
        if let Err(e) = server_bridge.run().await {
            error!("Bridge compose listener error: {e}");
        }
    }));

    // Optional host loopback listener
    if let Some(bind_host) = args.listen_host {
        let state_host = Arc::new(BridgeState::with_metrics(
            Arc::clone(&backend),
            registry.clone(),
            identity.node_id,
            args.advertised_host_host,
            args.advertised_host_port,
            Arc::clone(&metrics),
        ));
        let server_host = BridgeServer::new(state_host, bind_host);
        handles.push(tokio::spawn(async move {
            if let Err(e) = server_host.run().await {
                error!("Bridge host listener error: {e}");
            }
        }));
    }

    for h in handles {
        let _ = h.await;
    }
    Ok(())
}
