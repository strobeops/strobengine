//! Master controller configuration, parsed from CLI flags and environment
//! variables (`STROBENGINE_*`).

use std::net::SocketAddr;
use std::time::Duration;

use clap::Parser;

use crate::distributed::proto::ClusterConfiguration;

use super::error::{MasterError, MasterResult};

/// Default gRPC listen port.
pub const DEFAULT_PORT: u16 = 50_051;
/// Default heartbeat interval advertised to workers.
pub const DEFAULT_HEARTBEAT_INTERVAL_MS: u32 = 2_000;
/// Default telemetry flush interval advertised to workers.
pub const DEFAULT_METRIC_FLUSH_INTERVAL_MS: u32 = 1_000;
/// Default health-sweep cadence.
pub const DEFAULT_SWEEP_INTERVAL_MS: u64 = 1_000;
/// Default number of latency histogram buckets.
pub const DEFAULT_HISTOGRAM_BUCKET_COUNT: u32 = 10;
/// Heartbeats missed before a worker is considered disconnected.
pub const MISSED_HEARTBEATS_BEFORE_EVICTION: u32 = 3;
/// Default barrier timeout.
pub const DEFAULT_BARRIER_TIMEOUT_MS: u64 = 30_000;
/// Default live sliding-window length, in seconds.
pub const DEFAULT_WINDOW_SECS: u64 = 5;

/// Configuration for the master controller daemon.
#[derive(Debug, Clone, Parser)]
#[command(
    name = "strobengine-master",
    about = "strobengine distributed load-testing master controller",
    version
)]
pub struct MasterConfig {
    /// Cluster identifier handed to every worker.
    #[arg(long, env = "STROBENGINE_CLUSTER_ID", default_value = "strobengine")]
    pub cluster_id: String,

    /// Address to bind the gRPC server to.
    #[arg(long = "bind", env = "STROBENGINE_BIND", default_value = "0.0.0.0")]
    pub bind_address: String,

    /// Port to bind the gRPC server to.
    #[arg(long, env = "STROBENGINE_PORT", default_value_t = DEFAULT_PORT)]
    pub port: u16,

    /// Heartbeat interval recommended to workers (milliseconds).
    #[arg(
        long,
        env = "STROBENGINE_HEARTBEAT_INTERVAL_MS",
        default_value_t = DEFAULT_HEARTBEAT_INTERVAL_MS
    )]
    pub heartbeat_interval_ms: u32,

    /// Telemetry flush interval recommended to workers (milliseconds).
    #[arg(
        long,
        env = "STROBENGINE_METRIC_FLUSH_INTERVAL_MS",
        default_value_t = DEFAULT_METRIC_FLUSH_INTERVAL_MS
    )]
    pub metric_flush_interval_ms: u32,

    /// Worker timeout before eviction (milliseconds); 0 derives it from the
    /// heartbeat interval.
    #[arg(long, env = "STROBENGINE_WORKER_TIMEOUT_MS", default_value_t = 0)]
    pub worker_timeout_ms: u64,

    /// Health-sweep cadence (milliseconds).
    #[arg(
        long,
        env = "STROBENGINE_SWEEP_INTERVAL_MS",
        default_value_t = DEFAULT_SWEEP_INTERVAL_MS
    )]
    pub sweep_interval_ms: u64,

    /// Workers to wait for before broadcasting a run; 0 uses every ready worker.
    #[arg(long, env = "STROBENGINE_EXPECTED_WORKERS", default_value_t = 0)]
    pub expected_workers: usize,

    /// Barrier timeout (milliseconds).
    #[arg(
        long,
        env = "STROBENGINE_BARRIER_TIMEOUT_MS",
        default_value_t = DEFAULT_BARRIER_TIMEOUT_MS
    )]
    pub barrier_timeout_ms: u64,

    /// Latency histogram bucket count (must match the workers).
    #[arg(
        long,
        env = "STROBENGINE_HISTOGRAM_BUCKET_COUNT",
        default_value_t = DEFAULT_HISTOGRAM_BUCKET_COUNT
    )]
    pub histogram_bucket_count: u32,

    /// Live sliding-window length in seconds.
    #[arg(long, env = "STROBENGINE_WINDOW_SECS", default_value_t = DEFAULT_WINDOW_SECS)]
    pub window_secs: u64,

    /// Disable the live terminal dashboard (it is on by default on a TTY).
    #[arg(long = "no-live", env = "STROBENGINE_NO_LIVE", default_value_t = false)]
    pub no_live: bool,

    /// Whether the master requires TLS on the worker channel.
    #[arg(long, env = "STROBENGINE_TLS", default_value_t = false)]
    pub tls: bool,

    /// Optional single-run target URL; when set the daemon runs once and exits.
    #[arg(long, env = "STROBENGINE_RUN_URL", value_name = "URL")]
    pub run_url: Option<String>,

    /// Virtual users to place on each worker for the optional single run.
    #[arg(long = "run-vus", env = "STROBENGINE_RUN_VUS", default_value_t = 0)]
    pub run_virtual_users: u32,

    /// Duration of the optional single run, in seconds.
    #[arg(
        long = "run-duration-secs",
        env = "STROBENGINE_RUN_DURATION_SECS",
        default_value_t = 0
    )]
    pub run_duration_secs: u64,

    /// Path to write the aggregated JSON report of the optional single run.
    #[arg(long = "report", env = "STROBENGINE_REPORT", value_name = "PATH")]
    pub report_path: Option<String>,
}

impl MasterConfig {
    /// The socket address to bind the gRPC server to.
    pub fn listen_addr(&self) -> MasterResult<SocketAddr> {
        format!("{}:{}", self.bind_address, self.port)
            .parse()
            .map_err(|e| MasterError::Config(format!("invalid bind address: {e}")))
    }

    /// Effective worker timeout before eviction.
    pub fn worker_timeout(&self) -> Duration {
        if self.worker_timeout_ms > 0 {
            Duration::from_millis(self.worker_timeout_ms)
        } else {
            Duration::from_millis(
                u64::from(self.heartbeat_interval_ms)
                    * u64::from(MISSED_HEARTBEATS_BEFORE_EVICTION),
            )
        }
    }

    /// Health-sweep cadence.
    pub fn sweep_interval(&self) -> Duration {
        Duration::from_millis(self.sweep_interval_ms.max(50))
    }

    /// Barrier timeout.
    pub fn barrier_timeout(&self) -> Duration {
        Duration::from_millis(self.barrier_timeout_ms.max(100))
    }

    /// Whether an optional single-run specification was provided.
    pub fn has_run_spec(&self) -> bool {
        self.run_url.is_some()
    }

    /// Build the cluster configuration handed to a registering worker.
    pub fn cluster_configuration(&self, master_endpoint: &str) -> ClusterConfiguration {
        ClusterConfiguration {
            cluster_id: self.cluster_id.clone(),
            master_endpoint: master_endpoint.to_string(),
            heartbeat_interval_ms: self.heartbeat_interval_ms,
            metric_flush_interval_ms: self.metric_flush_interval_ms,
            max_batch_size: 0,
            tls_required: self.tls,
            histogram_bucket_count: self.histogram_bucket_count,
            cluster_labels: Default::default(),
        }
    }

    /// Validate the configuration at startup.
    pub fn validate(&self) -> MasterResult<()> {
        self.listen_addr()?;
        if self.port == 0 {
            return Err(MasterError::Config("port must be non-zero".into()));
        }
        if self.heartbeat_interval_ms == 0 {
            return Err(MasterError::Config(
                "heartbeat interval must be greater than zero".into(),
            ));
        }
        if self.histogram_bucket_count == 0 {
            return Err(MasterError::Config(
                "histogram bucket count must be greater than zero".into(),
            ));
        }
        if self.window_secs == 0 {
            return Err(MasterError::Config(
                "window seconds must be greater than zero".into(),
            ));
        }
        if self.has_run_spec() {
            if self.run_virtual_users == 0 {
                return Err(MasterError::Config(
                    "run virtual-users must be greater than zero".into(),
                ));
            }
            if self.run_duration_secs == 0 {
                return Err(MasterError::Config(
                    "run duration must be greater than zero".into(),
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base() -> Vec<&'static str> {
        vec!["strobengine-master"]
    }

    #[test]
    fn parses_defaults() {
        let config = MasterConfig::try_parse_from(base()).unwrap();
        assert_eq!(config.port, DEFAULT_PORT);
        assert_eq!(config.heartbeat_interval_ms, DEFAULT_HEARTBEAT_INTERVAL_MS);
        assert!(!config.has_run_spec());
        assert_eq!(
            config.listen_addr().unwrap().to_string(),
            format!("0.0.0.0:{DEFAULT_PORT}")
        );
    }

    #[test]
    fn derives_worker_timeout_from_heartbeat() {
        let config = MasterConfig::try_parse_from(base()).unwrap();
        assert_eq!(
            config.worker_timeout(),
            Duration::from_millis(u64::from(DEFAULT_HEARTBEAT_INTERVAL_MS) * 3)
        );
    }

    #[test]
    fn explicit_worker_timeout_wins() {
        let config = MasterConfig::try_parse_from(["m", "--worker-timeout-ms", "1234"]).unwrap();
        assert_eq!(config.worker_timeout(), Duration::from_millis(1234));
    }

    #[test]
    fn run_spec_requires_vus_and_duration() {
        let config =
            MasterConfig::try_parse_from(["m", "--run-url", "http://127.0.0.1:9/x"]).unwrap();
        assert!(config.validate().is_err());

        let ok = MasterConfig::try_parse_from([
            "m",
            "--run-url",
            "http://127.0.0.1:9/x",
            "--run-vus",
            "10",
            "--run-duration-secs",
            "5",
        ])
        .unwrap();
        assert!(ok.validate().is_ok());
        assert!(ok.has_run_spec());
    }

    #[test]
    fn cluster_configuration_carries_settings() {
        let config = MasterConfig::try_parse_from(base()).unwrap();
        let cluster = config.cluster_configuration("http://127.0.0.1:50051");
        assert_eq!(cluster.master_endpoint, "http://127.0.0.1:50051");
        assert_eq!(cluster.heartbeat_interval_ms, DEFAULT_HEARTBEAT_INTERVAL_MS);
        assert_eq!(
            cluster.histogram_bucket_count,
            DEFAULT_HISTOGRAM_BUCKET_COUNT
        );
    }
}
