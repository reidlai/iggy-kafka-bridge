//! iggy-kafka-bridge CLI binary.

use clap::Parser;
use iggy_kafka_bridge::backend::LiveIggyBackend;
use iggy_kafka_bridge::config::TopicRegistry;
use iggy_kafka_bridge::server::BridgeServer;
use iggy_kafka_bridge::state::BridgeState;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{error, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Parser, Debug)]
#[command(
    name = "iggy-kafka-bridge",
    about = "Kafka wire protocol bridge to Apache Iggy streaming broker",
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

    /// Bind address for compose network listener
    #[arg(long, env = "BRIDGE_BIND_COMPOSE", default_value = "0.0.0.0:9092")]
    listen_bridge: String,

    /// Advertised host for compose network listener
    #[arg(long, env = "BRIDGE_ADVERTISED_COMPOSE_HOST", default_value = "bridge")]
    advertised_bridge_host: String,

    /// Advertised port for compose network listener
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

    /// Broker Node ID
    #[arg(long, default_value_t = 1)]
    node_id: i32,

    /// Run healthcheck and exit (0 = healthy, 1 = unhealthy)
    #[arg(long)]
    healthcheck: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "iggy_kafka_bridge=info,info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let args = Args::parse();

    if args.healthcheck {
        info!("Running healthcheck against Iggy broker at {}...", args.iggy_address);
        match LiveIggyBackend::connect(&args.iggy_address, &args.iggy_username, &args.iggy_password).await {
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

    info!("Loading topic registry from {:?}...", args.config);
    let registry = TopicRegistry::load_from_file(&args.config)?;
    info!("Loaded {} topic definitions.", registry.topics.len());

    info!("Connecting to Iggy broker at {}...", args.iggy_address);
    let backend = Arc::new(
        LiveIggyBackend::connect(&args.iggy_address, &args.iggy_username, &args.iggy_password).await?,
    );
    info!("Successfully connected to Iggy broker.");

    let mut handles = Vec::new();

    // Primary compose listener
    let state_bridge = Arc::new(BridgeState::new(
        Arc::clone(&backend),
        registry.clone(),
        args.node_id,
        args.advertised_bridge_host,
        args.advertised_bridge_port,
    ));
    let server_bridge = BridgeServer::new(state_bridge, args.listen_bridge);
    handles.push(tokio::spawn(async move {
        if let Err(e) = server_bridge.run().await {
            error!("Bridge compose listener error: {e}");
        }
    }));

    // Optional host loopback listener
    if let Some(bind_host) = args.listen_host {
        let state_host = Arc::new(BridgeState::new(
            Arc::clone(&backend),
            registry.clone(),
            args.node_id,
            args.advertised_host_host,
            args.advertised_host_port,
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
