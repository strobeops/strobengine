//! Final report synthesis and output.
//!
//! Holds the aggregated [`GlobalReport`] produced at the end of a distributed
//! run, together with the per-epoch timeline and per-worker share breakdowns,
//! and the helpers used to serialize it to a file, stdout, or any writer.

use std::io::Write;
use std::path::PathBuf;

use serde::Serialize;

/// Latency statistics merged across all workers for a run.
#[derive(Debug, Clone, Serialize)]
pub struct GlobalLatency {
    /// Approximate 50th percentile, milliseconds.
    pub p50_ms: f64,
    /// Approximate 90th percentile, milliseconds.
    pub p90_ms: f64,
    /// Approximate 95th percentile, milliseconds.
    pub p95_ms: f64,
    /// Approximate 99th percentile, milliseconds.
    pub p99_ms: f64,
    /// Minimum latency reported by any worker, milliseconds.
    pub min_ms: f64,
    /// Maximum latency reported by any worker, milliseconds.
    pub max_ms: f64,
    /// Total latency samples across workers.
    pub sample_count: u64,
    /// Merged latency histogram (element-wise sum of worker buckets).
    pub bucket_counts: Vec<u64>,
}

/// Per-error-class count for the global report.
#[derive(Debug, Clone, Serialize)]
pub struct ErrorClassCount {
    /// Human-readable error class name.
    pub error_class: String,
    /// Number of requests in this class.
    pub count: u64,
}

/// One point on the aggregated RPS/latency timeline.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct TimelinePoint {
    /// Elapsed seconds since the run started.
    pub elapsed_secs: f64,
    /// Epoch start, Unix epoch milliseconds.
    pub epoch_ms: i64,
    /// Requests per second observed in this epoch.
    pub rps: f64,
    /// Error rate for this epoch, percent.
    pub error_rate_percent: f64,
    /// Approximate 50th percentile latency, milliseconds.
    pub p50_ms: f64,
    /// Approximate 95th percentile latency, milliseconds.
    pub p95_ms: f64,
    /// Approximate 99th percentile latency, milliseconds.
    pub p99_ms: f64,
}

/// Per-worker contribution to the cluster totals.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct WorkerShare {
    /// Worker id.
    pub worker_id: String,
    /// Requests handled by this worker.
    pub total_requests: u64,
    /// Successful requests handled by this worker.
    pub success_count: u64,
    /// Failed requests handled by this worker.
    pub failure_count: u64,
    /// Error rate for this worker, percent.
    pub error_rate_percent: f64,
    /// Requests per second over the run span.
    pub rps: f64,
    /// This worker's percentage of all requests.
    pub share_percent: f64,
    /// This worker's approximate 95th percentile latency, milliseconds.
    pub p95_ms: f64,
}

/// Aggregated result of a distributed run.
#[derive(Debug, Clone, Serialize)]
pub struct GlobalReport {
    /// Run identifier.
    pub test_run_id: String,
    /// Participating worker ids (sorted).
    pub workers: Vec<String>,
    /// Number of workers that contributed telemetry.
    pub worker_count: usize,
    /// Metric batches ingested.
    pub batches: u64,
    /// Total requests across all workers.
    pub total_requests: u64,
    /// Successful requests.
    pub success_count: u64,
    /// Failed requests.
    pub failure_count: u64,
    /// Error rate as a percentage.
    pub error_rate_percent: f64,
    /// Total response bytes received.
    pub bytes_received: u64,
    /// Aggregate requests per second over the observed window.
    pub rps: f64,
    /// Observed wall-clock span in seconds.
    pub duration_secs: f64,
    /// Merged latency statistics.
    pub latency: GlobalLatency,
    /// Per-error-class breakdown.
    pub error_breakdown: Vec<ErrorClassCount>,
    /// Per-epoch RPS/latency trend.
    pub timeline: Vec<TimelinePoint>,
    /// Per-worker contribution breakdown (sorted by request count, descending).
    pub worker_shares: Vec<WorkerShare>,
}

impl GlobalReport {
    /// Serialize the report to pretty-printed JSON.
    pub fn to_json_pretty(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    /// Write the report as pretty-printed JSON to any writer (file, stdout,
    /// socket, in-memory buffer, ...).
    pub fn write_json<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        let json = self
            .to_json_pretty()
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        writer.write_all(json.as_bytes())
    }
}

/// Where a finished report should be written.
#[derive(Debug, Clone)]
pub enum ReportDestination {
    /// A file path on disk.
    File(PathBuf),
    /// Standard output.
    Stdout,
    /// The report is not persisted.
    None,
}

impl ReportDestination {
    /// Parse a `--report` value: `-` means stdout, empty/absent means none.
    pub fn parse(value: Option<&str>) -> Self {
        match value {
            Some("-") => ReportDestination::Stdout,
            Some(path) if !path.trim().is_empty() => ReportDestination::File(PathBuf::from(path)),
            _ => ReportDestination::None,
        }
    }

    /// Persist `report` according to this destination.
    pub fn write(&self, report: &GlobalReport) -> std::io::Result<()> {
        match self {
            ReportDestination::File(path) => {
                let mut file = std::fs::File::create(path)?;
                report.write_json(&mut file)
            }
            ReportDestination::Stdout => {
                let stdout = std::io::stdout();
                let mut lock = stdout.lock();
                report.write_json(&mut lock)
            }
            ReportDestination::None => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_report() -> GlobalReport {
        GlobalReport {
            test_run_id: "run-1".to_string(),
            workers: vec!["w1".to_string()],
            worker_count: 1,
            batches: 1,
            total_requests: 10,
            success_count: 9,
            failure_count: 1,
            error_rate_percent: 10.0,
            bytes_received: 100,
            rps: 5.0,
            duration_secs: 2.0,
            latency: GlobalLatency {
                p50_ms: 1.0,
                p90_ms: 2.0,
                p95_ms: 3.0,
                p99_ms: 4.0,
                min_ms: 1.0,
                max_ms: 4.0,
                sample_count: 10,
                bucket_counts: vec![0; 10],
            },
            error_breakdown: vec![ErrorClassCount {
                error_class: "Http5xx".to_string(),
                count: 1,
            }],
            timeline: vec![TimelinePoint {
                elapsed_secs: 1.0,
                epoch_ms: 1_000,
                rps: 5.0,
                error_rate_percent: 10.0,
                p50_ms: 1.0,
                p95_ms: 3.0,
                p99_ms: 4.0,
            }],
            worker_shares: vec![WorkerShare {
                worker_id: "w1".to_string(),
                total_requests: 10,
                success_count: 9,
                failure_count: 1,
                error_rate_percent: 10.0,
                rps: 5.0,
                share_percent: 100.0,
                p95_ms: 3.0,
            }],
        }
    }

    #[test]
    fn write_json_includes_new_sections() {
        let report = sample_report();
        let mut buf = Vec::new();
        report.write_json(&mut buf).unwrap();
        let value: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(value["test_run_id"], "run-1");
        assert_eq!(value["timeline"][0]["rps"], 5.0);
        assert_eq!(value["worker_shares"][0]["worker_id"], "w1");
        assert_eq!(value["worker_shares"][0]["share_percent"], 100.0);
    }

    #[test]
    fn destination_parses_dash_and_paths() {
        assert!(matches!(
            ReportDestination::parse(Some("-")),
            ReportDestination::Stdout
        ));
        assert!(matches!(
            ReportDestination::parse(Some("/tmp/r.json")),
            ReportDestination::File(_)
        ));
        assert!(matches!(
            ReportDestination::parse(None),
            ReportDestination::None
        ));
    }

    #[test]
    fn destination_none_writes_nothing() {
        let report = sample_report();
        ReportDestination::None.write(&report).unwrap();
    }
}
