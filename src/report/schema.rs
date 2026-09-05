use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::chaos::ChaosMetrics;
use crate::metrics::{QuicMetrics, SseMetrics};

/// Top-level report artifact persisted to disk after each load test.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ReportArtifact {
    pub metadata: ReportMetadata,
    pub summary: ReportSummary,
    pub latency_percentiles: LatencyPercentiles,
    pub error_breakdown: HashMap<String, u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub avg_connection_latency_us: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quic: Option<QuicMetrics>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sse: Option<SseMetrics>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chaos: Option<ChaosMetrics>,
}

/// Test run metadata including configuration and system information.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ReportMetadata {
    pub timestamp: String,
    pub duration_secs: f64,
    pub target_url: String,
    pub cli_options: CliOptions,
    pub system_info: SystemInfo,
}

/// CLI options that produced this test run.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CliOptions {
    pub method: String,
    pub concurrency: usize,
    pub timeout_secs: u64,
    pub chaos: bool,
    pub chaos_rate: f32,
    pub body: Option<String>,
    pub headers: Option<Vec<(String, String)>>,
}

/// System information about the machine that ran the test.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SystemInfo {
    pub hostname: String,
    pub platform: String,
    pub version: String,
}

/// Aggregate request statistics.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ReportSummary {
    pub total_requests: u64,
    pub successful_requests: u64,
    pub failed_requests: u64,
    pub rps: f64,
    pub bytes_transferred: u64,
}

/// Latency percentiles in microseconds.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LatencyPercentiles {
    pub p50_us: f64,
    pub p90_us: f64,
    pub p95_us: f64,
    pub p99_us: f64,
    pub min_us: f64,
    pub max_us: f64,
    pub mean_us: f64,
}

fn get_system_info() -> SystemInfo {
    SystemInfo {
        hostname: std::env::var("HOSTNAME")
            .or_else(|_| std::env::var("COMPUTERNAME"))
            .unwrap_or_else(|_| "unknown".to_string()),
        platform: std::env::consts::OS.to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
    }
}

impl ReportArtifact {
    /// Build a ReportArtifact from an enriched TestSummary and TestConfig.
    ///
    /// Latency values are converted from milliseconds to microseconds (× 1000.0).
    /// Status codes are mapped from `HashMap<u16, u64>` to `HashMap<String, u64>`.
    pub fn from_summary_and_config(
        summary: &crate::metrics::TestSummary,
        config: &crate::config::TestConfig,
    ) -> Self {
        let successful = (summary.total_requests).saturating_sub(summary.total_errors) as u64;
        let rps = if summary.duration_secs > 0.0 {
            summary.total_requests as f64 / summary.duration_secs
        } else {
            0.0
        };

        let error_breakdown: HashMap<String, u64> = summary
            .status_codes
            .iter()
            .map(|(&k, &v)| (k.to_string(), v))
            .collect();

        ReportArtifact {
            metadata: ReportMetadata {
                timestamp: summary.timestamp.clone(),
                duration_secs: summary.duration_secs,
                target_url: config.url.clone(),
                cli_options: CliOptions {
                    method: config.method.clone(),
                    concurrency: config.concurrency,
                    timeout_secs: config.timeout_secs,
                    chaos: config.chaos,
                    chaos_rate: config.chaos_rate,
                    body: config.body.clone(),
                    headers: config.headers.clone(),
                },
                system_info: get_system_info(),
            },
            summary: ReportSummary {
                total_requests: summary.total_requests as u64,
                successful_requests: successful,
                failed_requests: summary.total_errors as u64,
                rps,
                bytes_transferred: summary.total_bytes_received,
            },
            latency_percentiles: LatencyPercentiles {
                p50_us: summary.p50_latency_ms * 1000.0,
                p90_us: summary.p90_latency_ms * 1000.0,
                p95_us: summary.p95_latency_ms * 1000.0,
                p99_us: summary.p99_latency_ms * 1000.0,
                min_us: summary.min_latency_ms * 1000.0,
                max_us: summary.max_latency_ms * 1000.0,
                mean_us: summary.average_latency_ms * 1000.0,
            },
            error_breakdown,
            avg_connection_latency_us: if summary.avg_connection_latency_us > 0.0 {
                Some(summary.avg_connection_latency_us)
            } else {
                None
            },
            quic: summary.quic.clone(),
            sse: summary.sse.clone(),
            chaos: if summary.chaos_injected_total > 0 {
                Some(ChaosMetrics {
                    total_injected: summary.chaos_injected_total,
                    faults_by_type: summary.chaos_faults_by_type.clone(),
                })
            } else {
                None
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_artifact() -> ReportArtifact {
        let mut error_breakdown = HashMap::new();
        error_breakdown.insert("200".to_string(), 95);
        error_breakdown.insert("500".to_string(), 5);

        ReportArtifact {
            metadata: ReportMetadata {
                timestamp: "2026-08-28T10:30:00Z".to_string(),
                duration_secs: 30.0,
                target_url: "http://localhost:8080/api".to_string(),
                cli_options: CliOptions {
                    method: "GET".to_string(),
                    concurrency: 50,
                    timeout_secs: 10,
                    chaos: false,
                    chaos_rate: 0.1,
                    body: None,
                    headers: Some(vec![(
                        "Authorization".to_string(),
                        "Bearer tok".to_string(),
                    )]),
                },
                system_info: SystemInfo {
                    hostname: "test-host".to_string(),
                    platform: "linux".to_string(),
                    version: "0.5.0".to_string(),
                },
            },
            summary: ReportSummary {
                total_requests: 1000,
                successful_requests: 950,
                failed_requests: 50,
                rps: 33.33,
                bytes_transferred: 524288,
            },
            latency_percentiles: LatencyPercentiles {
                p50_us: 1500.0,
                p90_us: 3000.0,
                p95_us: 4500.0,
                p99_us: 9000.0,
                min_us: 100.0,
                max_us: 15000.0,
                mean_us: 2200.0,
            },
            error_breakdown,
            avg_connection_latency_us: None,
            quic: None,
            sse: None,
            chaos: None,
        }
    }

    #[test]
    fn test_report_artifact_serialization_roundtrip() {
        let artifact = sample_artifact();
        let json = serde_json::to_string(&artifact).unwrap();
        let deserialized: ReportArtifact = serde_json::from_str(&json).unwrap();

        assert_eq!(
            artifact.metadata.target_url,
            deserialized.metadata.target_url
        );
        assert_eq!(
            artifact.summary.total_requests,
            deserialized.summary.total_requests
        );
        assert_eq!(
            artifact.latency_percentiles.p50_us,
            deserialized.latency_percentiles.p50_us
        );
        assert_eq!(
            artifact.error_breakdown.get("200"),
            deserialized.error_breakdown.get("200")
        );
    }

    #[test]
    fn test_report_artifact_json_structure() {
        let artifact = sample_artifact();
        let json = serde_json::to_value(&artifact).unwrap();

        assert!(json.get("metadata").is_some());
        assert!(json.get("summary").is_some());
        assert!(json.get("latency_percentiles").is_some());
        assert!(json.get("error_breakdown").is_some());

        // Metadata fields
        assert!(json["metadata"]["timestamp"].is_string());
        assert!(json["metadata"]["duration_secs"].is_f64());
        assert!(json["metadata"]["target_url"].is_string());
        assert!(json["metadata"]["cli_options"].is_object());
        assert!(json["metadata"]["system_info"].is_object());

        // Summary fields
        assert!(json["summary"]["total_requests"].is_u64());
        assert!(json["summary"]["successful_requests"].is_u64());
        assert!(json["summary"]["failed_requests"].is_u64());
        assert!(json["summary"]["rps"].is_f64());
        assert!(json["summary"]["bytes_transferred"].is_u64());

        // Latency fields
        assert!(json["latency_percentiles"]["p50_us"].is_f64());
        assert!(json["latency_percentiles"]["p99_us"].is_f64());
    }

    #[test]
    fn test_latency_percentiles_values() {
        let lp = LatencyPercentiles {
            p50_us: 1500.0,
            p90_us: 3000.0,
            p95_us: 4500.0,
            p99_us: 9000.0,
            min_us: 100.0,
            max_us: 15000.0,
            mean_us: 2200.0,
        };
        let json = serde_json::to_value(&lp).unwrap();
        assert_eq!(json["p50_us"], 1500.0);
        assert_eq!(json["p90_us"], 3000.0);
        assert_eq!(json["p95_us"], 4500.0);
        assert_eq!(json["p99_us"], 9000.0);
        assert_eq!(json["min_us"], 100.0);
        assert_eq!(json["max_us"], 15000.0);
        assert_eq!(json["mean_us"], 2200.0);
    }

    #[test]
    fn test_error_breakdown_from_status_codes() {
        let mut breakdown = HashMap::new();
        breakdown.insert("200".to_string(), 90);
        breakdown.insert("404".to_string(), 7);
        breakdown.insert("500".to_string(), 3);

        let json = serde_json::to_value(&breakdown).unwrap();
        assert_eq!(json["200"], 90);
        assert_eq!(json["404"], 7);
        assert_eq!(json["500"], 3);
    }

    #[test]
    fn test_report_artifact_pretty_json() {
        let artifact = sample_artifact();
        let json = serde_json::to_string_pretty(&artifact).unwrap();

        assert!(json.contains("\"metadata\""));
        assert!(json.contains("\"latency_percentiles\""));
        assert!(json.contains("1500"));
        // Pretty-printed should have newlines
        assert!(json.contains('\n'));
    }

    #[test]
    fn test_empty_error_breakdown() {
        let mut artifact = sample_artifact();
        artifact.error_breakdown = HashMap::new();

        let json = serde_json::to_value(&artifact).unwrap();
        assert!(json["error_breakdown"].as_object().unwrap().is_empty());
    }

    #[test]
    fn test_optional_cli_options() {
        let mut artifact = sample_artifact();
        artifact.metadata.cli_options.body = Some(r#"{"key":"val"}"#.to_string());
        artifact.metadata.cli_options.headers = None;

        let json_str = serde_json::to_string(&artifact).unwrap();
        let deserialized: ReportArtifact = serde_json::from_str(&json_str).unwrap();

        assert_eq!(
            deserialized.metadata.cli_options.body,
            Some(r#"{"key":"val"}"#.to_string())
        );
        assert!(deserialized.metadata.cli_options.headers.is_none());
    }

    #[test]
    fn test_from_summary_and_config() {
        use crate::config::{TestConfig, WsMode};
        use crate::metrics::TestSummary;

        let mut status_codes = std::collections::HashMap::new();
        status_codes.insert(200, 95u64);
        status_codes.insert(500, 5u64);

        let summary = TestSummary {
            url: "http://localhost:8080".to_string(),
            total_requests: 100,
            total_errors: 5,
            average_latency_ms: 12.5,
            p95_latency_ms: 25.0,
            p99_latency_ms: 50.0,
            min_latency_ms: 1.0,
            p50_latency_ms: 10.0,
            p90_latency_ms: 20.0,
            max_latency_ms: 100.0,
            total_bytes_received: 1024,
            duration_secs: 10.0,
            workers: 5,
            timestamp: "2026-08-28T10:00:00Z".to_string(),
            raw_command: None,
            status_codes,
            avg_e2e_latency_us: 0.0,
            avg_connection_latency_us: 0.0,
            quic: None,
            sse: None,
            chaos_injected_total: 0,
            chaos_faults_by_type: std::collections::HashMap::new(),
        };

        let config = TestConfig::new(
            "http://localhost:8080".into(),
            10,
            10,
            10,
            false,
            0.1,
            false,
            "GET",
            None,
            None,
            None,
            WsMode::Handshake,
            None,
            None,
            None,
            None,
            None,
            None,
            false,
            false,
            None,
            None,
            None,
            None,
            None,
            false,
            false,
            None,
            false,
            None,
            None,
            false,
        );

        let artifact = ReportArtifact::from_summary_and_config(&summary, &config);

        assert_eq!(artifact.summary.total_requests, 100);
        assert_eq!(artifact.summary.successful_requests, 95);
        assert_eq!(artifact.summary.failed_requests, 5);
        assert!((artifact.summary.rps - 10.0).abs() < 0.01);
        assert_eq!(artifact.latency_percentiles.p50_us, 10000.0);
        assert_eq!(artifact.latency_percentiles.p99_us, 50000.0);
        assert_eq!(artifact.latency_percentiles.min_us, 1000.0);
        assert_eq!(artifact.latency_percentiles.max_us, 100000.0);
        assert_eq!(artifact.error_breakdown.get("200"), Some(&95));
        assert_eq!(artifact.error_breakdown.get("500"), Some(&5));
        assert_eq!(artifact.metadata.target_url, "http://localhost:8080");
        assert_eq!(artifact.metadata.cli_options.method, "GET");
        assert_eq!(artifact.metadata.cli_options.concurrency, 10);
        assert!(!artifact.metadata.cli_options.chaos);
    }
}
