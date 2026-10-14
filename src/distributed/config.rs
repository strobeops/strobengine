//! Worker daemon configuration, parsed from CLI flags and environment
//! variables (`STROBENGINE_*`).

use std::collections::HashMap;

use clap::Parser;
use sysinfo::System;

use super::error::WorkerError;
use super::proto::ResourceCapacity;

/// Default heartbeat cadence in milliseconds.
pub const DEFAULT_HEARTBEAT_INTERVAL_MS: u64 = 2_000;
/// Default telemetry aggregation window in milliseconds.
pub const DEFAULT_METRIC_WINDOW_MS: u64 = 1_000;
/// Default per-request timeout in milliseconds.
pub const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 10_000;
/// Virtual users advertised per logical CPU core when `--max-vus` is unset.
pub const VUS_PER_CORE: u32 = 100;

/// Configuration for the worker daemon.
///
/// Every field is settable from the command line and from an environment
/// variable, which makes the daemon easy to configure in containers.
#[derive(Debug, Clone, Parser)]
#[command(
    name = "strobengine-worker",
    about = "strobengine distributed load-testing worker daemon",
    version
)]
pub struct WorkerConfig {
    /// Master controller gRPC endpoint (e.g. `http://127.0.0.1:50051`).
    #[arg(long, env = "STROBENGINE_MASTER_ENDPOINT", value_name = "URL")]
    pub master_endpoint: String,

    /// Stable worker identifier; defaults to the hostname.
    #[arg(long, env = "STROBENGINE_WORKER_ID", value_name = "ID")]
    pub worker_id: Option<String>,

    /// Reported host name; defaults to the system host name.
    #[arg(long, env = "STROBENGINE_HOSTNAME", value_name = "NAME")]
    pub hostname: Option<String>,

    /// Reachable IP address advertised to the master; defaults to 127.0.0.1.
    #[arg(long, env = "STROBENGINE_IP_ADDRESS", value_name = "IP")]
    pub ip_address: Option<String>,

    /// Maximum virtual users this worker will run (0 = derive from CPU cores).
    #[arg(long, env = "STROBENGINE_MAX_VUS", default_value_t = 0)]
    pub max_vus: u32,

    /// Operator tags as `key=value` (repeatable or comma-separated).
    #[arg(
        long = "label",
        env = "STROBENGINE_LABELS",
        value_delimiter = ',',
        value_name = "KEY=VALUE"
    )]
    pub labels: Vec<String>,

    /// Heartbeat interval in milliseconds.
    #[arg(
        long,
        env = "STROBENGINE_HEARTBEAT_INTERVAL_MS",
        default_value_t = DEFAULT_HEARTBEAT_INTERVAL_MS
    )]
    pub heartbeat_interval_ms: u64,

    /// Telemetry aggregation window in milliseconds.
    #[arg(
        long,
        env = "STROBENGINE_METRIC_WINDOW_MS",
        default_value_t = DEFAULT_METRIC_WINDOW_MS
    )]
    pub metric_window_ms: u64,

    /// Per-request timeout in milliseconds.
    #[arg(
        long,
        env = "STROBENGINE_REQUEST_TIMEOUT_MS",
        default_value_t = DEFAULT_REQUEST_TIMEOUT_MS
    )]
    pub request_timeout_ms: u64,

    /// Whether the worker must connect to the master over TLS.
    #[arg(long, env = "STROBENGINE_TLS", default_value_t = false)]
    pub tls: bool,
}

impl WorkerConfig {
    /// Parse labels (`key=value`) into a map, rejecting malformed entries.
    pub fn labels_map(&self) -> Result<HashMap<String, String>, WorkerError> {
        let mut map = HashMap::new();
        for entry in &self.labels {
            let (key, value) = entry.split_once('=').ok_or_else(|| {
                WorkerError::Config(format!("invalid label '{entry}' (want key=value)"))
            })?;
            if key.is_empty() {
                return Err(WorkerError::Config(format!(
                    "label '{entry}' has an empty key"
                )));
            }
            map.insert(key.to_string(), value.to_string());
        }
        Ok(map)
    }

    /// Normalised endpoint URI (`host:port` is prefixed with `http://`).
    pub fn endpoint_uri(&self) -> String {
        if self.master_endpoint.contains("://") {
            self.master_endpoint.clone()
        } else {
            format!("http://{}", self.master_endpoint)
        }
    }

    /// The worker id, falling back to the resolved hostname.
    pub fn resolved_worker_id(&self) -> String {
        self.worker_id
            .clone()
            .unwrap_or_else(|| self.resolved_hostname())
    }

    /// The reported hostname, falling back to the system host name.
    pub fn resolved_hostname(&self) -> String {
        self.hostname
            .clone()
            .or_else(System::host_name)
            .unwrap_or_else(|| "unknown-host".to_string())
    }

    /// The advertised IP address, falling back to loopback.
    pub fn resolved_ip_address(&self) -> String {
        self.ip_address
            .clone()
            .unwrap_or_else(|| "127.0.0.1".to_string())
    }

    /// The effective VU ceiling (`max_vus`, or `cores * VUS_PER_CORE`).
    pub fn resolved_max_vus(&self) -> u32 {
        if self.max_vus > 0 {
            self.max_vus
        } else {
            let cores = std::thread::available_parallelism()
                .map(|n| n.get() as u32)
                .unwrap_or(1);
            cores.saturating_mul(VUS_PER_CORE).max(VUS_PER_CORE)
        }
    }

    /// Build the resource-capacity snapshot advertised at registration.
    pub fn capacity(&self) -> ResourceCapacity {
        let mut system = System::new_all();
        system.refresh_all();
        ResourceCapacity {
            cpu_cores: system.cpus().len() as u32,
            total_memory_bytes: system.total_memory(),
            available_memory_bytes: system.available_memory(),
            available_cpu_percent: 0.0,
        }
    }

    /// Validate the configuration at startup.
    pub fn validate(&self) -> Result<(), WorkerError> {
        if self.master_endpoint.trim().is_empty() {
            return Err(WorkerError::Config(
                "master endpoint must not be empty".into(),
            ));
        }
        if self.heartbeat_interval_ms == 0 {
            return Err(WorkerError::Config(
                "heartbeat interval must be greater than zero".into(),
            ));
        }
        if self.metric_window_ms == 0 {
            return Err(WorkerError::Config(
                "metric window must be greater than zero".into(),
            ));
        }
        self.labels_map()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_args() -> Vec<&'static str> {
        vec!["strobengine-worker", "--master-endpoint", "127.0.0.1:50051"]
    }

    #[test]
    fn parses_endpoint_and_defaults() {
        let config = WorkerConfig::try_parse_from(base_args()).unwrap();
        assert_eq!(config.master_endpoint, "127.0.0.1:50051");
        assert_eq!(config.endpoint_uri(), "http://127.0.0.1:50051");
        assert_eq!(config.heartbeat_interval_ms, DEFAULT_HEARTBEAT_INTERVAL_MS);
        assert_eq!(config.metric_window_ms, DEFAULT_METRIC_WINDOW_MS);
        assert!(!config.tls);
    }

    #[test]
    fn endpoint_uri_is_not_double_prefixed() {
        let config =
            WorkerConfig::try_parse_from(["w", "--master-endpoint", "http://host:50051"]).unwrap();
        assert_eq!(config.endpoint_uri(), "http://host:50051");
    }

    #[test]
    fn labels_parse_and_reject_malformed() {
        let config = WorkerConfig::try_parse_from([
            "w",
            "--master-endpoint",
            "h:1",
            "--label",
            "zone=a",
            "--label",
            "pool=b",
        ])
        .unwrap();
        let labels = config.labels_map().unwrap();
        assert_eq!(labels.get("zone").map(String::as_str), Some("a"));
        assert_eq!(labels.get("pool").map(String::as_str), Some("b"));

        let bad =
            WorkerConfig::try_parse_from(["w", "--master-endpoint", "h:1", "--label", "oops"])
                .unwrap();
        assert!(bad.labels_map().is_err());
    }

    #[test]
    fn max_vus_defaults_to_cores() {
        let config = WorkerConfig::try_parse_from(base_args()).unwrap();
        assert!(config.resolved_max_vus() >= VUS_PER_CORE);

        let explicit =
            WorkerConfig::try_parse_from(["w", "--master-endpoint", "h:1", "--max-vus", "7"])
                .unwrap();
        assert_eq!(explicit.resolved_max_vus(), 7);
    }

    #[test]
    fn validate_rejects_zero_intervals() {
        let config = WorkerConfig::try_parse_from([
            "w",
            "--master-endpoint",
            "h:1",
            "--metric-window-ms",
            "0",
        ])
        .unwrap();
        assert!(config.validate().is_err());
    }
}
