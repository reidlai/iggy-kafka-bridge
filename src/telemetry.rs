//! OpenTelemetry 3-Signal Observability (Logs, Metrics, Distributed Traces) and HTTP `/metrics`, `/healthz`, `/readyz` Server
//! for `iggy-kafka-bridge` (Story 16.2, FR-34, NFR-15, SM-9).

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tracing::{debug, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

pub const DEFAULT_OTEL_SERVICE_NAME: &str = "iggy-kafka-bridge";
pub const DEFAULT_METRICS_BIND_ADDR: &str = "0.0.0.0:9093";

/// 3-Tier OpenTelemetry runtime mode (AD-13, NFR-15).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OtelTier {
    /// Tier 1: `OTEL_SDK_DISABLED=true` or no OTLP endpoint/console exporter configured.
    Disabled,
    /// Tier 2: `OTEL_TRACES_EXPORTER=console` (or `OTEL_METRICS_EXPORTER=console`).
    Console,
    /// Tier 3: `OTEL_EXPORTER_OTLP_ENDPOINT` configured for OTLP export.
    Otlp,
}

/// Structured log output format (`BRIDGE_LOG_FORMAT`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LogFormat {
    Pretty,
    Json,
}

impl LogFormat {
    pub fn from_str_lossy(val: &str) -> Self {
        if val.trim().eq_ignore_ascii_case("json") {
            Self::Json
        } else {
            Self::Pretty
        }
    }
}

/// OpenTelemetry configuration resolved from environment variables or CLI flags.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OtelConfig {
    pub service_name: String,
    pub sdk_disabled: bool,
    pub otlp_endpoint: Option<String>,
    pub traces_exporter: String,
    pub metrics_exporter: String,
    pub logs_exporter: String,
    pub log_format: LogFormat,
}

impl Default for OtelConfig {
    fn default() -> Self {
        Self {
            service_name: DEFAULT_OTEL_SERVICE_NAME.to_string(),
            sdk_disabled: false,
            otlp_endpoint: None,
            traces_exporter: "otlp".to_string(),
            metrics_exporter: "otlp".to_string(),
            logs_exporter: "otlp".to_string(),
            log_format: LogFormat::Pretty,
        }
    }
}

impl OtelConfig {
    /// Resolve OpenTelemetry configuration from standard `OTEL_*` and `BRIDGE_*` environment variables.
    pub fn from_env() -> Self {
        Self::from_map(&std::env::vars().collect())
    }

    /// Resolve OpenTelemetry configuration from an explicit key-value map (deterministic unit testing).
    pub fn from_map(vars: &HashMap<String, String>) -> Self {
        let service_name = vars
            .get("OTEL_SERVICE_NAME")
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| DEFAULT_OTEL_SERVICE_NAME.to_string());

        let sdk_disabled = vars
            .get("OTEL_SDK_DISABLED")
            .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "true" | "1" | "yes"))
            .unwrap_or(false);

        let otlp_endpoint = vars
            .get("OTEL_EXPORTER_OTLP_ENDPOINT")
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());

        let traces_exporter = vars
            .get("OTEL_TRACES_EXPORTER")
            .map(|s| s.trim().to_ascii_lowercase())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "otlp".to_string());

        let metrics_exporter = vars
            .get("OTEL_METRICS_EXPORTER")
            .map(|s| s.trim().to_ascii_lowercase())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "otlp".to_string());

        let logs_exporter = vars
            .get("OTEL_LOGS_EXPORTER")
            .map(|s| s.trim().to_ascii_lowercase())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "otlp".to_string());

        let log_format = vars
            .get("BRIDGE_LOG_FORMAT")
            .map(|s| LogFormat::from_str_lossy(s))
            .unwrap_or(LogFormat::Pretty);

        Self {
            service_name,
            sdk_disabled,
            otlp_endpoint,
            traces_exporter,
            metrics_exporter,
            logs_exporter,
            log_format,
        }
    }

    /// Determine the active 3-Tier OpenTelemetry mode.
    pub fn tier(&self) -> OtelTier {
        if self.sdk_disabled || self.traces_exporter == "none" {
            OtelTier::Disabled
        } else if self.traces_exporter == "console" || self.metrics_exporter == "console" {
            OtelTier::Console
        } else if self.otlp_endpoint.is_some() {
            OtelTier::Otlp
        } else {
            OtelTier::Disabled
        }
    }
}

/// Initialize the global `tracing` subscriber according to `OtelConfig` (idempotent in tests).
pub fn init_telemetry(config: &OtelConfig) {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| "iggy_kafka_proxy=info,info".into());

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_target(true)
        .with_thread_ids(config.log_format == LogFormat::Json)
        .with_level(true);

    let _ = tracing_subscriber::registry()
        .with(filter)
        .with(fmt_layer)
        .try_init();
}

const BUCKET_BOUNDS_US: [u64; 6] = [100, 500, 1_000, 5_000, 10_000, 50_000];

/// Lock-free OpenTelemetry & Prometheus metrics registry for `iggy-kafka-bridge` (FR-34, SM-9).
pub struct BridgeMetrics {
    node_id: AtomicI32,
    iggy_pool_size: AtomicU64,
    ready: AtomicBool,
    started_at: Instant,

    requests_total: AtomicU64,
    request_errors_total: AtomicU64,
    inflight_requests: AtomicU64,

    produce_requests_total: AtomicU64,
    fetch_requests_total: AtomicU64,
    list_offsets_requests_total: AtomicU64,
    metadata_requests_total: AtomicU64,
    offset_commit_requests_total: AtomicU64,
    offset_fetch_requests_total: AtomicU64,
    find_coordinator_requests_total: AtomicU64,
    api_versions_requests_total: AtomicU64,

    records_produced_total: AtomicU64,
    records_fetched_total: AtomicU64,

    duration_us_count: AtomicU64,
    duration_us_sum: AtomicU64,
    duration_us_max: AtomicU64,
    duration_buckets: [AtomicU64; 7], // <=100, <=500, <=1000, <=5000, <=10000, <=50000, +Inf

    otel_spans_recorded_total: AtomicU64,
    otel_logs_recorded_total: AtomicU64,
}

impl Default for BridgeMetrics {
    fn default() -> Self {
        Self::new(1, 4)
    }
}

impl BridgeMetrics {
    pub fn new(node_id: i32, iggy_pool_size: usize) -> Self {
        Self {
            node_id: AtomicI32::new(node_id),
            iggy_pool_size: AtomicU64::new(iggy_pool_size.max(1) as u64),
            ready: AtomicBool::new(true),
            started_at: Instant::now(),
            requests_total: AtomicU64::new(0),
            request_errors_total: AtomicU64::new(0),
            inflight_requests: AtomicU64::new(0),
            produce_requests_total: AtomicU64::new(0),
            fetch_requests_total: AtomicU64::new(0),
            list_offsets_requests_total: AtomicU64::new(0),
            metadata_requests_total: AtomicU64::new(0),
            offset_commit_requests_total: AtomicU64::new(0),
            offset_fetch_requests_total: AtomicU64::new(0),
            find_coordinator_requests_total: AtomicU64::new(0),
            api_versions_requests_total: AtomicU64::new(0),
            records_produced_total: AtomicU64::new(0),
            records_fetched_total: AtomicU64::new(0),
            duration_us_count: AtomicU64::new(0),
            duration_us_sum: AtomicU64::new(0),
            duration_us_max: AtomicU64::new(0),
            duration_buckets: [
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
            ],
            otel_spans_recorded_total: AtomicU64::new(0),
            otel_logs_recorded_total: AtomicU64::new(0),
        }
    }

    pub fn set_ready(&self, ready: bool) {
        self.ready.store(ready, Ordering::SeqCst);
    }

    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::SeqCst)
    }

    pub fn set_pool_size(&self, pool_size: usize) {
        self.iggy_pool_size
            .store(pool_size.max(1) as u64, Ordering::Relaxed);
    }

    pub fn pool_size(&self) -> u64 {
        self.iggy_pool_size.load(Ordering::Relaxed)
    }

    pub fn node_id(&self) -> i32 {
        self.node_id.load(Ordering::Relaxed)
    }

    pub fn begin_request(self: &Arc<Self>) -> InflightGuard {
        self.inflight_requests.fetch_add(1, Ordering::Relaxed);
        self.otel_spans_recorded_total.fetch_add(1, Ordering::Relaxed);
        InflightGuard {
            metrics: Arc::clone(self),
        }
    }

    pub fn record_request(&self, api_key_name: &str, duration_us: u64, is_error: bool) {
        self.requests_total.fetch_add(1, Ordering::Relaxed);
        self.otel_logs_recorded_total.fetch_add(1, Ordering::Relaxed);
        if is_error {
            self.request_errors_total.fetch_add(1, Ordering::Relaxed);
        }

        match api_key_name {
            "Produce" => {
                self.produce_requests_total.fetch_add(1, Ordering::Relaxed);
            }
            "Fetch" => {
                self.fetch_requests_total.fetch_add(1, Ordering::Relaxed);
            }
            "ListOffsets" => {
                self.list_offsets_requests_total
                    .fetch_add(1, Ordering::Relaxed);
            }
            "Metadata" => {
                self.metadata_requests_total.fetch_add(1, Ordering::Relaxed);
            }
            "OffsetCommit" => {
                self.offset_commit_requests_total
                    .fetch_add(1, Ordering::Relaxed);
            }
            "OffsetFetch" => {
                self.offset_fetch_requests_total
                    .fetch_add(1, Ordering::Relaxed);
            }
            "FindCoordinator" => {
                self.find_coordinator_requests_total
                    .fetch_add(1, Ordering::Relaxed);
            }
            "ApiVersions" => {
                self.api_versions_requests_total
                    .fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }

        self.duration_us_count.fetch_add(1, Ordering::Relaxed);
        self.duration_us_sum
            .fetch_add(duration_us, Ordering::Relaxed);
        self.duration_us_max
            .fetch_max(duration_us, Ordering::Relaxed);

        for (i, bound) in BUCKET_BOUNDS_US.iter().enumerate() {
            if duration_us <= *bound {
                self.duration_buckets[i].fetch_add(1, Ordering::Relaxed);
            }
        }
        self.duration_buckets[6].fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_produced(&self, count: u64) {
        self.records_produced_total
            .fetch_add(count, Ordering::Relaxed);
    }

    pub fn record_fetched(&self, count: u64) {
        self.records_fetched_total
            .fetch_add(count, Ordering::Relaxed);
    }

    pub fn requests_total(&self) -> u64 {
        self.requests_total.load(Ordering::Relaxed)
    }

    pub fn request_errors_total(&self) -> u64 {
        self.request_errors_total.load(Ordering::Relaxed)
    }

    pub fn inflight_requests(&self) -> u64 {
        self.inflight_requests.load(Ordering::Relaxed)
    }

    pub fn records_produced_total(&self) -> u64 {
        self.records_produced_total.load(Ordering::Relaxed)
    }

    pub fn records_fetched_total(&self) -> u64 {
        self.records_fetched_total.load(Ordering::Relaxed)
    }

    pub fn otel_spans_recorded_total(&self) -> u64 {
        self.otel_spans_recorded_total.load(Ordering::Relaxed)
    }

    /// Render all bridge metrics in standard Prometheus text exposition format (0.0.4).
    pub fn render_prometheus(&self) -> String {
        let node_id = self.node_id();
        let pool_size = self.pool_size();
        let ready_val = if self.is_ready() { 1 } else { 0 };
        let elapsed_secs = self.started_at.elapsed().as_secs_f64().max(0.001);
        let total_records =
            self.records_produced_total() + self.records_fetched_total();
        let records_per_sec = (total_records as f64 / elapsed_secs * 100.0).round() / 100.0;

        let mut out = String::with_capacity(2048);
        out.push_str("# HELP bridge_ready Readiness status of the iggy-kafka-bridge replica (1 = ready, 0 = not ready).\n");
        out.push_str("# TYPE bridge_ready gauge\n");
        out.push_str(&format!("bridge_ready{{node_id=\"{node_id}\"}} {ready_val}\n\n"));

        out.push_str("# HELP bridge_iggy_pool_size Number of pooled Apache Iggy TCP connections per replica.\n");
        out.push_str("# TYPE bridge_iggy_pool_size gauge\n");
        out.push_str(&format!("bridge_iggy_pool_size{{node_id=\"{node_id}\"}} {pool_size}\n\n"));

        out.push_str("# HELP bridge_inflight_requests Current number of in-flight Kafka wire requests (used by Kubernetes HPA).\n");
        out.push_str("# TYPE bridge_inflight_requests gauge\n");
        out.push_str(&format!(
            "bridge_inflight_requests{{node_id=\"{node_id}\"}} {}\n\n",
            self.inflight_requests()
        ));

        out.push_str("# HELP bridge_requests_total Total number of Kafka wire protocol requests processed.\n");
        out.push_str("# TYPE bridge_requests_total counter\n");
        out.push_str(&format!(
            "bridge_requests_total{{node_id=\"{node_id}\",api_key=\"Produce\"}} {}\n",
            self.produce_requests_total.load(Ordering::Relaxed)
        ));
        out.push_str(&format!(
            "bridge_requests_total{{node_id=\"{node_id}\",api_key=\"Fetch\"}} {}\n",
            self.fetch_requests_total.load(Ordering::Relaxed)
        ));
        out.push_str(&format!(
            "bridge_requests_total{{node_id=\"{node_id}\",api_key=\"ListOffsets\"}} {}\n",
            self.list_offsets_requests_total.load(Ordering::Relaxed)
        ));
        out.push_str(&format!(
            "bridge_requests_total{{node_id=\"{node_id}\",api_key=\"Metadata\"}} {}\n",
            self.metadata_requests_total.load(Ordering::Relaxed)
        ));
        out.push_str(&format!(
            "bridge_requests_total{{node_id=\"{node_id}\",api_key=\"OffsetCommit\"}} {}\n",
            self.offset_commit_requests_total.load(Ordering::Relaxed)
        ));
        out.push_str(&format!(
            "bridge_requests_total{{node_id=\"{node_id}\",api_key=\"OffsetFetch\"}} {}\n",
            self.offset_fetch_requests_total.load(Ordering::Relaxed)
        ));
        out.push_str(&format!(
            "bridge_requests_total{{node_id=\"{node_id}\",api_key=\"FindCoordinator\"}} {}\n",
            self.find_coordinator_requests_total.load(Ordering::Relaxed)
        ));
        out.push_str(&format!(
            "bridge_requests_total{{node_id=\"{node_id}\",api_key=\"ApiVersions\"}} {}\n\n",
            self.api_versions_requests_total.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP bridge_request_errors_total Total number of Kafka wire protocol frame or dispatch errors.\n");
        out.push_str("# TYPE bridge_request_errors_total counter\n");
        out.push_str(&format!(
            "bridge_request_errors_total{{node_id=\"{node_id}\"}} {}\n\n",
            self.request_errors_total()
        ));

        out.push_str("# HELP bridge_records_produced_total Total Kafka records produced to Apache Iggy.\n");
        out.push_str("# TYPE bridge_records_produced_total counter\n");
        out.push_str(&format!(
            "bridge_records_produced_total{{node_id=\"{node_id}\"}} {}\n\n",
            self.records_produced_total()
        ));

        out.push_str("# HELP bridge_records_fetched_total Total Kafka records fetched from Apache Iggy.\n");
        out.push_str("# TYPE bridge_records_fetched_total counter\n");
        out.push_str(&format!(
            "bridge_records_fetched_total{{node_id=\"{node_id}\"}} {}\n\n",
            self.records_fetched_total()
        ));

        out.push_str("# HELP bridge_records_per_second Sustained record throughput rate across produce and fetch paths (HPA scaling metric).\n");
        out.push_str("# TYPE bridge_records_per_second gauge\n");
        out.push_str(&format!(
            "bridge_records_per_second{{node_id=\"{node_id}\"}} {records_per_sec}\n\n"
        ));

        out.push_str("# HELP bridge_otel_spans_recorded_total Total OpenTelemetry trace spans recorded.\n");
        out.push_str("# TYPE bridge_otel_spans_recorded_total counter\n");
        out.push_str(&format!(
            "bridge_otel_spans_recorded_total{{node_id=\"{node_id}\"}} {}\n\n",
            self.otel_spans_recorded_total()
        ));

        out.push_str("# HELP bridge_request_duration_us Kafka wire request latency histogram in microseconds.\n");
        out.push_str("# TYPE bridge_request_duration_us histogram\n");
        for (i, bound) in BUCKET_BOUNDS_US.iter().enumerate() {
            out.push_str(&format!(
                "bridge_request_duration_us_bucket{{node_id=\"{node_id}\",le=\"{bound}\"}} {}\n",
                self.duration_buckets[i].load(Ordering::Relaxed)
            ));
        }
        out.push_str(&format!(
            "bridge_request_duration_us_bucket{{node_id=\"{node_id}\",le=\"+Inf\"}} {}\n",
            self.duration_buckets[6].load(Ordering::Relaxed)
        ));
        out.push_str(&format!(
            "bridge_request_duration_us_sum{{node_id=\"{node_id}\"}} {}\n",
            self.duration_us_sum.load(Ordering::Relaxed)
        ));
        out.push_str(&format!(
            "bridge_request_duration_us_count{{node_id=\"{node_id}\"}} {}\n",
            self.duration_us_count.load(Ordering::Relaxed)
        ));

        out
    }
}

/// RAII guard that decrements `bridge_inflight_requests` when dropped.
pub struct InflightGuard {
    metrics: Arc<BridgeMetrics>,
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        self.metrics.inflight_requests.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Run the HTTP observability server (`/metrics`, `/healthz`, `/readyz`) for Kubernetes probes and Prometheus/OTel scraping.
pub async fn run_metrics_server(
    bind_address: String,
    metrics: Arc<BridgeMetrics>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let listener = TcpListener::bind(&bind_address).await?;
    run_metrics_server_with_listener(listener, metrics).await
}

/// Run the HTTP observability server using an already-bound `TcpListener` (for ephemeral-port integration tests).
pub async fn run_metrics_server_with_listener(
    listener: TcpListener,
    metrics: Arc<BridgeMetrics>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let local_addr = listener.local_addr()?;
    info!(
        "Observability HTTP server (/metrics, /healthz, /readyz) listening on {}",
        local_addr
    );

    loop {
        let (mut socket, peer) = listener.accept().await?;
        let metrics = Arc::clone(&metrics);
        tokio::spawn(async move {
            let mut buf = [0u8; 2048];
            let n = match socket.read(&mut buf).await {
                Ok(n) if n > 0 => n,
                _ => return,
            };
            let req_str = String::from_utf8_lossy(&buf[..n]);
            let first_line = req_str.lines().next().unwrap_or("");
            let path = first_line
                .split_whitespace()
                .nth(1)
                .unwrap_or("/")
                .split('?')
                .next()
                .unwrap_or("/");

            debug!(peer = %peer, path = %path, "Observability HTTP request");

            let (status_line, content_type, body) = match path {
                "/metrics" => (
                    "HTTP/1.1 200 OK",
                    "text/plain; version=0.0.4; charset=utf-8",
                    metrics.render_prometheus(),
                ),
                "/healthz" | "/health" | "/livez" => {
                    let body = serde_json::json!({
                        "status": "healthy",
                        "service": DEFAULT_OTEL_SERVICE_NAME,
                        "node_id": metrics.node_id(),
                    })
                    .to_string();
                    ("HTTP/1.1 200 OK", "application/json", body)
                }
                "/readyz" | "/ready" => {
                    if metrics.is_ready() {
                        let body = serde_json::json!({
                            "status": "ready",
                            "ready": true,
                            "service": DEFAULT_OTEL_SERVICE_NAME,
                            "node_id": metrics.node_id(),
                            "iggy_pool_size": metrics.pool_size(),
                        })
                        .to_string();
                        ("HTTP/1.1 200 OK", "application/json", body)
                    } else {
                        let body = serde_json::json!({
                            "status": "not_ready",
                            "ready": false,
                            "service": DEFAULT_OTEL_SERVICE_NAME,
                            "node_id": metrics.node_id(),
                        })
                        .to_string();
                        ("HTTP/1.1 503 Service Unavailable", "application/json", body)
                    }
                }
                _ => (
                    "HTTP/1.1 404 Not Found",
                    "application/json",
                    "{\"error\":\"not_found\"}".to_string(),
                ),
            };

            let resp = format!(
                "{status_line}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = socket.write_all(resp.as_bytes()).await;
            let _ = socket.flush().await;
        });
    }
}
