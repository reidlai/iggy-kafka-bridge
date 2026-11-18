//! Automated tests for Kubernetes Horizontal Auto-Scaling manifests (`k8s/`) and `README.md`
//! documentation completeness in `iggy-kafka-bridge` (Stories 16.3, 16.4, 16.5).

use std::fs;
use std::path::PathBuf;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

#[test]
fn test_k8s_deployment_manifest_contract() {
    let path = repo_root().join("k8s").join("deployment.yaml");
    assert!(path.is_file(), "k8s/deployment.yaml must exist");

    let content = fs::read_to_string(&path).expect("read k8s/deployment.yaml");
    let doc: serde_yaml::Value =
        serde_yaml::from_str(&content).expect("parse k8s/deployment.yaml as YAML");

    assert_eq!(doc["apiVersion"].as_str(), Some("apps/v1"));
    assert_eq!(doc["kind"].as_str(), Some("Deployment"));
    assert!(
        doc["metadata"]["name"].as_str() == Some("iggy-kafka-proxy")
            || doc["metadata"]["name"].as_str() == Some("iggy-kafka-bridge")
    );

    let pod_spec = &doc["spec"]["template"]["spec"];
    assert_eq!(pod_spec["securityContext"]["runAsNonRoot"].as_bool(), Some(true));
    assert_eq!(pod_spec["securityContext"]["runAsUser"].as_u64(), Some(10001));

    let container = &pod_spec["containers"][0];
    assert!(
        container["name"].as_str() == Some("iggy-kafka-proxy")
            || container["name"].as_str() == Some("iggy-kafka-bridge")
    );
    assert_eq!(
        container["securityContext"]["readOnlyRootFilesystem"].as_bool(),
        Some(true)
    );
    assert_eq!(
        container["securityContext"]["allowPrivilegeEscalation"].as_bool(),
        Some(false)
    );

    // Check ports 9092 (kafka) and 9093 (metrics)
    let ports = container["ports"].as_sequence().expect("container ports");
    assert!(ports.iter().any(|p| p["containerPort"].as_u64() == Some(9092)));
    assert!(ports.iter().any(|p| p["containerPort"].as_u64() == Some(9093)));

    // Check liveness & readiness probes on /healthz and /readyz
    assert_eq!(
        container["livenessProbe"]["httpGet"]["path"].as_str(),
        Some("/healthz")
    );
    assert_eq!(
        container["readinessProbe"]["httpGet"]["path"].as_str(),
        Some("/readyz")
    );

    // Check required scaling and OTel environment variables
    let env_list = container["env"].as_sequence().expect("container env");
    let env_names: Vec<&str> = env_list
        .iter()
        .filter_map(|e| e["name"].as_str())
        .collect();
    for required_env in [
        "POD_NAME",
        "POD_IP",
        "IGGY_SERVER_ADDRESS",
        "IGGY_CONNECTION_POOL_SIZE",
        "BRIDGE_METRICS_ADDR",
        "BRIDGE_LOG_FORMAT",
        "OTEL_SERVICE_NAME",
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "OTEL_TRACES_EXPORTER",
        "OTEL_METRICS_EXPORTER",
        "OTEL_LOGS_EXPORTER",
    ] {
        assert!(
            env_names.contains(&required_env),
            "k8s/deployment.yaml must configure env var {required_env}"
        );
    }
}

#[test]
fn test_k8s_service_and_hpa_manifests_contract() {
    let svc_path = repo_root().join("k8s").join("service.yaml");
    assert!(svc_path.is_file(), "k8s/service.yaml must exist");
    let svc: serde_yaml::Value =
        serde_yaml::from_str(&fs::read_to_string(&svc_path).unwrap()).expect("parse service.yaml");
    assert_eq!(svc["apiVersion"].as_str(), Some("v1"));
    assert_eq!(svc["kind"].as_str(), Some("Service"));
    assert_eq!(
        svc["metadata"]["annotations"]["prometheus.io/scrape"].as_str(),
        Some("true")
    );

    let hpa_path = repo_root().join("k8s").join("hpa.yaml");
    assert!(hpa_path.is_file(), "k8s/hpa.yaml must exist");
    let hpa_text = fs::read_to_string(&hpa_path).unwrap();
    let hpa: serde_yaml::Value = serde_yaml::from_str(&hpa_text).expect("parse hpa.yaml");

    assert_eq!(hpa["apiVersion"].as_str(), Some("autoscaling/v2"));
    assert_eq!(hpa["kind"].as_str(), Some("HorizontalPodAutoscaler"));
    assert!(
        hpa["spec"]["scaleTargetRef"]["name"].as_str() == Some("iggy-kafka-proxy")
            || hpa["spec"]["scaleTargetRef"]["name"].as_str() == Some("iggy-kafka-bridge")
    );
    assert_eq!(hpa["spec"]["minReplicas"].as_u64(), Some(2));
    assert_eq!(hpa["spec"]["maxReplicas"].as_u64(), Some(10));

    assert!(hpa_text.contains("bridge_inflight_requests"));
    assert!(hpa_text.contains("bridge_records_per_second"));
}

#[test]
fn test_readme_and_mit_license_completeness() {
    let root = repo_root();
    let license_path = root.join("LICENSE");
    assert!(license_path.is_file(), "LICENSE file must exist");
    let license_text = fs::read_to_string(&license_path).unwrap();
    assert!(license_text.contains("MIT License"));

    let cargo_toml = fs::read_to_string(root.join("Cargo.toml")).unwrap();
    assert!(cargo_toml.contains("license = \"MIT\""));

    let readme_path = root.join("README.md");
    assert!(readme_path.is_file(), "README.md must exist");
    let readme = fs::read_to_string(&readme_path).unwrap();

    for required in [
        "## 🎯 Objectives",
        "## 🏛️ Technical Architecture",
        "```mermaid",
        "## ☸️ Horizontal Auto-Scaling in Kubernetes (`k8s/`)",
        "## 🔭 OpenTelemetry Observability (Logging, Metrics, Traces)",
        "## 🚀 Quickstart",
        "## ⚙️ CLI Flags & Environment Variables Reference",
        "k8s/deployment.yaml",
        "k8s/service.yaml",
        "k8s/hpa.yaml",
        "IGGY_CONNECTION_POOL_SIZE",
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "bridge_inflight_requests",
        "bridge_records_per_second",
        "/metrics",
        "/healthz",
        "/readyz",
    ] {
        assert!(
            readme.contains(required),
            "iggy-kafka-bridge/README.md missing required section or key: {required}"
        );
    }
}
