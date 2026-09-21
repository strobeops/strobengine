use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::time::{SystemTime, UNIX_EPOCH};

pub mod system;

use hdrhistogram::Histogram;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use tokio::sync::mpsc;

/// Conversion factor from microseconds to milliseconds (1 ms = 1,000 us).
pub const MICROS_PER_MILLI: f64 = 1_000.0;

/// Highest trackable value for HDR histograms (1 hour in microseconds).
const HISTOGRAM_HIGHEST_TRACKABLE: u64 = 3_600_000_000;
/// Significant figures for HDR histogram precision.
const HISTOGRAM_SIGNIFICANT_FIGURES: u8 = 3;

/// Intermediate aggregated values collected from the metrics channel.
/// Uses fixed-memory HDR Histograms instead of unbounded Vec<u128>.
pub struct AggregatedMetrics {
    pub latency_histogram: Histogram<u64>,
    pub e2e_latency_histogram: Histogram<u64>,
    pub connection_latency_histogram: Histogram<u64>,
    pub status_codes: HashMap<u16, u64>,
    pub total_bytes: u64,
    pub quic_metrics: Option<QuicMetrics>,
    pub sse_metrics: Option<SseMetrics>,
    pub chaos_injected_total: u64,
    pub chaos_faults_by_type: HashMap<String, u64>,
    pub total_sockets_created: u64,
    pub total_connections_reused: u64,
    pub dns_resolution_sum_us: u64,
    pub dns_resolution_count: u64,
}

impl Default for AggregatedMetrics {
    fn default() -> Self {
        Self {
            latency_histogram: Histogram::new_with_max(
                HISTOGRAM_HIGHEST_TRACKABLE,
                HISTOGRAM_SIGNIFICANT_FIGURES,
            )
            .unwrap(),
            e2e_latency_histogram: Histogram::new_with_max(
                HISTOGRAM_HIGHEST_TRACKABLE,
                HISTOGRAM_SIGNIFICANT_FIGURES,
            )
            .unwrap(),
            connection_latency_histogram: Histogram::new_with_max(
                HISTOGRAM_HIGHEST_TRACKABLE,
                HISTOGRAM_SIGNIFICANT_FIGURES,
            )
            .unwrap(),
            status_codes: HashMap::new(),
            total_bytes: 0,
            quic_metrics: None,
            sse_metrics: None,
            chaos_injected_total: 0,
            chaos_faults_by_type: HashMap::new(),
            total_sockets_created: 0,
            total_connections_reused: 0,
            dns_resolution_sum_us: 0,
            dns_resolution_count: 0,
        }
    }
}

/// Get current wall-clock time in nanoseconds since UNIX epoch.
pub fn wallclock_ns() -> u128 {
    ns_since_epoch(SystemTime::now())
}

/// Convert a `SystemTime` to nanoseconds since UNIX epoch.
///
/// Returns `0` if the clock is set before the epoch (e.g. a container/VM
/// clock reset), logging a warning so the silent fallback is observable.
fn ns_since_epoch(when: SystemTime) -> u128 {
    match when.duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_nanos(),
        Err(e) => {
            tracing::warn!(
                error = %e,
                "system clock is before UNIX epoch; returning 0 wall-clock ns"
            );
            0
        }
    }
}

/// Encode a pub/sub payload: 16-byte big-endian nanosecond timestamp prefix + user payload.
pub fn create_pubsub_payload(user_payload: &[u8]) -> Vec<u8> {
    let now_ns = wallclock_ns();
    let mut payload = Vec::with_capacity(16 + user_payload.len());
    payload.extend_from_slice(&now_ns.to_be_bytes());
    payload.extend_from_slice(user_payload);
    payload
}

/// Decode a pub/sub payload, returning (sent_ns, user_payload).
/// Returns None if the data is shorter than 16 bytes.
#[allow(dead_code)]
pub fn parse_pubsub_payload(data: &[u8]) -> Option<(u128, &[u8])> {
    if data.len() < 16 {
        return None;
    }
    let (ts_bytes, rest) = data.split_at(16);
    let ts_array: [u8; 16] = ts_bytes.try_into().ok()?;
    let sent_ns = u128::from_be_bytes(ts_array);
    Some((sent_ns, rest))
}

/// Connection-related metrics for a single request.
#[derive(Debug, Clone, Default)]
pub struct ConnectionMetrics {
    pub connection_latency_us: Option<u128>,
    pub timestamp_sent_ns: Option<u128>,
    pub e2e_latency_us: Option<u128>,
    pub dns_resolution_us: Option<u64>,
    pub is_socket_reused: bool,
}

pub struct RequestMetric {
    pub latency_micros: u128,
    pub status_code: u16,
    pub bytes_received: u64,
    pub is_reconnect: bool,
    pub connection: ConnectionMetrics,
    pub quic_handshake_us: Option<u64>,
    pub quic_0rtt_used: bool,
    pub quic_retransmits: Option<u64>,
    pub sse_events_received: Option<u64>,
    pub sse_first_event_us: Option<u64>,
    pub sse_event_interval_us: Option<u64>,
    pub chaos_fault: Option<crate::chaos::ChaosFault>,
}

impl RequestMetric {
    pub fn error(latency_micros: u128) -> Self {
        Self {
            latency_micros,
            status_code: 0,
            bytes_received: 0,
            is_reconnect: false,
            connection: ConnectionMetrics::default(),
            quic_handshake_us: None,
            quic_0rtt_used: false,
            quic_retransmits: None,
            sse_events_received: None,
            sse_first_event_us: None,
            sse_event_interval_us: None,
            chaos_fault: None,
        }
    }
}

#[pyclass(skip_from_py_object)]
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct QuicMetrics {
    #[pyo3(get)]
    pub zero_rtt_accepted_count: u64,
    #[pyo3(get)]
    pub retransmissions: u64,
    #[pyo3(get)]
    pub avg_handshake_ms: Option<f64>,
}

#[pyclass(skip_from_py_object)]
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct SseMetrics {
    #[pyo3(get)]
    pub total_events_received: u64,
    #[pyo3(get)]
    pub avg_ttfb_ms: Option<f64>,
}

pub struct LiveCounters {
    pub total_requests: AtomicU64,
    pub errors: AtomicU64,
    pub active_workers: AtomicUsize,
    pub completed_requests: AtomicU64,
    pub latency_sum_micros: AtomicU64,
    pub latency_count: AtomicU64,
    pub bytes_received: AtomicU64,
}

impl LiveCounters {
    pub fn new() -> Self {
        Self {
            total_requests: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            active_workers: AtomicUsize::new(0),
            completed_requests: AtomicU64::new(0),
            latency_sum_micros: AtomicU64::new(0),
            latency_count: AtomicU64::new(0),
            bytes_received: AtomicU64::new(0),
        }
    }
}

#[pyclass(skip_from_py_object)]
#[derive(Debug, Clone, serde::Serialize)]
pub struct TestSummary {
    #[pyo3(get)]
    pub url: String,
    #[pyo3(get)]
    pub total_requests: usize,
    #[pyo3(get)]
    pub total_errors: usize,
    #[pyo3(get)]
    pub average_latency_ms: f64,
    #[pyo3(get)]
    pub p95_latency_ms: f64,
    #[pyo3(get)]
    pub p99_latency_ms: f64,
    #[pyo3(get)]
    pub min_latency_ms: f64,
    #[pyo3(get)]
    pub p50_latency_ms: f64,
    #[pyo3(get)]
    pub p90_latency_ms: f64,
    #[pyo3(get)]
    pub max_latency_ms: f64,
    #[pyo3(get)]
    pub total_bytes_received: u64,
    #[pyo3(get)]
    pub duration_secs: f64,
    #[pyo3(get, set)]
    pub workers: usize,
    #[pyo3(get, set)]
    pub timestamp: String,
    #[pyo3(get, set)]
    pub raw_command: Option<String>,
    #[pyo3(get)]
    pub status_codes: HashMap<u16, u64>,
    #[pyo3(get)]
    pub avg_e2e_latency_us: f64,
    #[pyo3(get)]
    pub avg_connection_latency_us: f64,
    #[pyo3(get)]
    pub quic: Option<QuicMetrics>,
    #[pyo3(get)]
    pub sse: Option<SseMetrics>,
    #[pyo3(get)]
    pub chaos_injected_total: u64,
    #[pyo3(get)]
    pub chaos_faults_by_type: HashMap<String, u64>,
    #[pyo3(get)]
    pub std_dev_latency_ms: f64,
    #[pyo3(get)]
    pub p99_99_latency_ms: f64,
    #[pyo3(get)]
    pub latency_histogram: HashMap<String, u64>,
    #[pyo3(get)]
    pub system_metrics: Option<system::SystemMetrics>,
    #[pyo3(get)]
    pub connection_reuse_ratio: f64,
    #[pyo3(get)]
    pub avg_dns_resolution_ms: f64,
}

#[pymethods]
impl TestSummary {
    pub fn clone(&self) -> Self {
        Clone::clone(self)
    }

    pub fn __copy__(&self) -> Self {
        self.clone()
    }

    pub fn __deepcopy__(&self, _py: Python<'_>) -> Self {
        self.clone()
    }

    #[pyo3(signature = (indent=None))]
    pub fn to_json(&self, _py: Python<'_>, indent: Option<usize>) -> PyResult<String> {
        let json_str = if indent.is_some() {
            serde_json::to_string_pretty(self)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?
        } else {
            serde_json::to_string(self)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?
        };
        Ok(json_str)
    }

    pub fn to_dict<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyDict>> {
        let json_mod = py.import("json")?;
        let json_str = self.to_json(py, None)?;
        let obj = json_mod.call_method1("loads", (&json_str,))?;
        let dict = obj.cast_into::<PyDict>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>("Expected a dict from json.loads")
        })?;
        Ok(dict)
    }
}

/// Canonical bucket order for latency histograms.
pub const HISTOGRAM_BUCKET_ORDER: &[&str] = &[
    "<1ms",
    "1-5ms",
    "5-10ms",
    "10-25ms",
    "25-50ms",
    "50-100ms",
    "100-250ms",
    "250-500ms",
    "500-1000ms",
    ">1000ms",
];

/// Convert an HDR histogram into the 10-bucket display format.
fn calculate_histogram_from_hdr(hist: &Histogram<u64>) -> HashMap<String, u64> {
    let mut buckets: HashMap<String, u64> = HashMap::new();
    for key in HISTOGRAM_BUCKET_ORDER {
        buckets.insert(key.to_string(), 0);
    }
    for v in hist.iter_recorded() {
        let ms = v.value_iterated_to() as f64 / MICROS_PER_MILLI;
        let bucket_key = if ms < 1.0 {
            "<1ms"
        } else if ms < 5.0 {
            "1-5ms"
        } else if ms < 10.0 {
            "5-10ms"
        } else if ms < 25.0 {
            "10-25ms"
        } else if ms < 50.0 {
            "25-50ms"
        } else if ms < 100.0 {
            "50-100ms"
        } else if ms < 250.0 {
            "100-250ms"
        } else if ms < 500.0 {
            "250-500ms"
        } else if ms < 1000.0 {
            "500-1000ms"
        } else {
            ">1000ms"
        };
        *buckets.entry(bucket_key.to_string()).or_insert(0) += v.count_at_value();
    }
    buckets
}

/// Input parameters for summary calculation.
pub struct SummaryInput {
    pub url: String,
    pub total_requests: u64,
    pub total_errors: u64,
    pub latency_histogram: Histogram<u64>,
    pub total_bytes: u64,
    pub duration_secs: f64,
    pub workers: usize,
    pub status_codes: HashMap<u16, u64>,
    pub e2e_latency_histogram: Histogram<u64>,
    pub connection_latency_histogram: Histogram<u64>,
    pub quic_metrics: Option<QuicMetrics>,
    pub sse_metrics: Option<SseMetrics>,
    pub chaos_injected_total: u64,
    pub chaos_faults_by_type: HashMap<String, u64>,
    pub resource_samples: Vec<system::ResourceSample>,
    pub total_sockets_created: u64,
    pub total_connections_reused: u64,
    pub dns_resolution_sum_us: u64,
    pub dns_resolution_count: u64,
}

/// Finalize metric aggregation from a receiver channel.
/// Returns aggregated metrics using fixed-memory HDR Histograms.
pub async fn finalize_metrics(mut rx: mpsc::Receiver<RequestMetric>) -> AggregatedMetrics {
    let mut metrics = AggregatedMetrics::default();
    let mut quic_stats = QuicMetrics::default();
    let mut sse_stats = SseMetrics::default();
    let mut has_quic = false;
    let mut quic_handshake_sum_us: u64 = 0;
    let mut quic_handshake_count: u64 = 0;
    let mut sse_first_event_sum_us: u64 = 0;
    let mut sse_first_event_count: u64 = 0;

    while let Some(metric) = rx.recv().await {
        // Record latencies into fixed-memory histograms (safe u128->u64 clamping)
        let lat = u64::try_from(metric.latency_micros).unwrap_or(u64::MAX);
        metrics.latency_histogram.saturating_record(lat);

        if let Some(e2e) = metric.connection.e2e_latency_us {
            let v = u64::try_from(e2e).unwrap_or(u64::MAX);
            metrics.e2e_latency_histogram.saturating_record(v);
        }
        if let Some(conn) = metric.connection.connection_latency_us {
            let v = u64::try_from(conn).unwrap_or(u64::MAX);
            metrics.connection_latency_histogram.saturating_record(v);
        }

        // QUIC aggregation
        if metric.quic_handshake_us.is_some()
            || metric.quic_0rtt_used
            || metric.quic_retransmits.is_some()
        {
            has_quic = true;
            if metric.quic_0rtt_used {
                quic_stats.zero_rtt_accepted_count += 1;
            }
            if let Some(retrans) = metric.quic_retransmits {
                quic_stats.retransmissions += retrans;
            }
            if let Some(handshake) = metric.quic_handshake_us {
                quic_handshake_sum_us += handshake;
                quic_handshake_count += 1;
            }
        }

        // SSE aggregation
        if let Some(events) = metric.sse_events_received {
            sse_stats.total_events_received += events;
        }
        if let Some(first) = metric.sse_first_event_us {
            sse_first_event_sum_us += first;
            sse_first_event_count += 1;
        }

        // Chaos aggregation
        if let Some(ref fault) = metric.chaos_fault {
            metrics.chaos_injected_total += 1;
            *metrics
                .chaos_faults_by_type
                .entry(fault.name().to_string())
                .or_insert(0) += 1;
        }
        // Connection pool & DNS aggregation
        // Note: uninstrumented engines default to is_socket_reused=false,
        // so connection_reuse_ratio will be 0% until per-engine tracking is added.
        if metric.connection.is_socket_reused {
            metrics.total_connections_reused += 1;
        } else {
            metrics.total_sockets_created += 1;
        }
        if let Some(dns_us) = metric.connection.dns_resolution_us {
            metrics.dns_resolution_sum_us += dns_us;
            metrics.dns_resolution_count += 1;
        }

        *metrics.status_codes.entry(metric.status_code).or_insert(0) += 1;
        metrics.total_bytes += metric.bytes_received;
    }

    // Compute QUIC avg handshake via O(1) running counters
    if quic_handshake_count > 0 {
        quic_stats.avg_handshake_ms =
            Some((quic_handshake_sum_us as f64 / quic_handshake_count as f64) / 1000.0);
    }

    // Compute SSE avg TTFB via O(1) running counters
    if sse_first_event_count > 0 {
        sse_stats.avg_ttfb_ms =
            Some((sse_first_event_sum_us as f64 / sse_first_event_count as f64) / 1000.0);
    }

    metrics.quic_metrics = if has_quic { Some(quic_stats) } else { None };
    metrics.sse_metrics = if sse_stats.total_events_received > 0 || sse_first_event_count > 0 {
        Some(sse_stats)
    } else {
        None
    };

    metrics
}

pub fn calculate_summary(input: SummaryInput) -> TestSummary {
    let avg_e2e_latency_us = if input.e2e_latency_histogram.is_empty() {
        0.0
    } else {
        input.e2e_latency_histogram.mean()
    };

    let avg_connection_latency_us = if input.connection_latency_histogram.is_empty() {
        0.0
    } else {
        input.connection_latency_histogram.mean()
    };

    let system_metrics = if input.resource_samples.is_empty() {
        None
    } else {
        Some(system::SystemMetrics::from_samples(&input.resource_samples))
    };

    let total_connections = input.total_sockets_created + input.total_connections_reused;
    let connection_reuse_ratio = if total_connections > 0 {
        input.total_connections_reused as f64 / total_connections as f64
    } else {
        0.0
    };

    let avg_dns_resolution_ms = if input.dns_resolution_count > 0 {
        input.dns_resolution_sum_us as f64 / input.dns_resolution_count as f64 / 1000.0
    } else {
        0.0
    };

    if input.latency_histogram.is_empty() {
        return TestSummary {
            url: input.url,
            total_requests: input.total_requests as usize,
            total_errors: input.total_errors as usize,
            average_latency_ms: 0.0,
            p95_latency_ms: 0.0,
            p99_latency_ms: 0.0,
            min_latency_ms: 0.0,
            p50_latency_ms: 0.0,
            p90_latency_ms: 0.0,
            max_latency_ms: 0.0,
            total_bytes_received: input.total_bytes,
            duration_secs: input.duration_secs,
            workers: input.workers,
            timestamp: String::new(),
            raw_command: None,
            status_codes: input.status_codes,
            avg_e2e_latency_us,
            avg_connection_latency_us,
            quic: input.quic_metrics,
            sse: input.sse_metrics,
            chaos_injected_total: input.chaos_injected_total,
            chaos_faults_by_type: input.chaos_faults_by_type,
            std_dev_latency_ms: 0.0,
            p99_99_latency_ms: 0.0,
            latency_histogram: HashMap::new(),
            system_metrics: system_metrics.clone(),
            connection_reuse_ratio,
            avg_dns_resolution_ms,
        };
    }

    let average_latency_ms = input.latency_histogram.mean() / MICROS_PER_MILLI;
    let min_latency_ms = input.latency_histogram.min() as f64 / MICROS_PER_MILLI;
    let max_latency_ms = input.latency_histogram.max() as f64 / MICROS_PER_MILLI;

    let p50_latency_ms = input.latency_histogram.value_at_quantile(0.50) as f64 / MICROS_PER_MILLI;
    let p90_latency_ms = input.latency_histogram.value_at_quantile(0.90) as f64 / MICROS_PER_MILLI;
    let p95_latency_ms = input.latency_histogram.value_at_quantile(0.95) as f64 / MICROS_PER_MILLI;
    let p99_latency_ms = input.latency_histogram.value_at_quantile(0.99) as f64 / MICROS_PER_MILLI;
    let p99_99_latency_ms =
        input.latency_histogram.value_at_quantile(0.9999) as f64 / MICROS_PER_MILLI;

    let std_dev_latency_ms = input.latency_histogram.stdev() / MICROS_PER_MILLI;

    let latency_histogram = calculate_histogram_from_hdr(&input.latency_histogram);

    TestSummary {
        url: input.url,
        total_requests: input.total_requests as usize,
        total_errors: input.total_errors as usize,
        average_latency_ms,
        p95_latency_ms,
        p99_latency_ms,
        min_latency_ms,
        p50_latency_ms,
        p90_latency_ms,
        max_latency_ms,
        total_bytes_received: input.total_bytes,
        duration_secs: input.duration_secs,
        workers: input.workers,
        timestamp: String::new(),
        raw_command: None,
        status_codes: input.status_codes,
        avg_e2e_latency_us,
        avg_connection_latency_us,
        quic: input.quic_metrics,
        sse: input.sse_metrics,
        chaos_injected_total: input.chaos_injected_total,
        chaos_faults_by_type: input.chaos_faults_by_type,
        std_dev_latency_ms,
        p99_99_latency_ms,
        latency_histogram,
        system_metrics,
        connection_reuse_ratio,
        avg_dns_resolution_ms,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper to create a histogram pre-loaded with values.
    fn create_test_histogram(values: &[u64]) -> Histogram<u64> {
        let mut h =
            Histogram::new_with_max(HISTOGRAM_HIGHEST_TRACKABLE, HISTOGRAM_SIGNIFICANT_FIGURES)
                .unwrap();
        for &v in values {
            h.saturating_record(v);
        }
        h
    }

    #[test]
    fn ns_since_epoch_at_epoch_is_zero() {
        assert_eq!(ns_since_epoch(UNIX_EPOCH), 0);
    }

    #[test]
    fn ns_since_epoch_after_epoch() {
        use std::time::Duration;
        let when = UNIX_EPOCH + Duration::from_secs(5);
        assert_eq!(ns_since_epoch(when), 5_000_000_000);
    }

    #[test]
    fn ns_since_epoch_before_epoch_returns_zero() {
        use std::time::Duration;
        let when = UNIX_EPOCH - Duration::from_secs(5);
        // Clock before the epoch must not panic; it degrades to 0 (with a warn).
        assert_eq!(ns_since_epoch(when), 0);
    }

    #[test]
    fn empty_latencies_returns_zeros() {
        let s = calculate_summary(SummaryInput {
            url: "http://example.com".into(),
            total_requests: 10,
            total_errors: 3,
            latency_histogram: create_test_histogram(&[]),
            total_bytes: 0,
            duration_secs: 1.0,
            workers: 4,
            status_codes: HashMap::new(),
            e2e_latency_histogram: create_test_histogram(&[]),
            connection_latency_histogram: create_test_histogram(&[]),
            quic_metrics: None,
            sse_metrics: None,
            chaos_injected_total: 0,
            chaos_faults_by_type: HashMap::new(),
            resource_samples: Vec::new(),
            total_sockets_created: 0,
            total_connections_reused: 0,
            dns_resolution_sum_us: 0,
            dns_resolution_count: 0,
        });
        assert_eq!(s.url, "http://example.com");
        assert_eq!(s.total_requests, 10);
        assert_eq!(s.total_errors, 3);
        assert_eq!(s.average_latency_ms, 0.0);
        assert_eq!(s.p95_latency_ms, 0.0);
        assert_eq!(s.p99_latency_ms, 0.0);
        assert_eq!(s.min_latency_ms, 0.0);
        assert_eq!(s.p50_latency_ms, 0.0);
        assert_eq!(s.p90_latency_ms, 0.0);
        assert_eq!(s.max_latency_ms, 0.0);
    }

    #[test]
    fn single_request() {
        let s = calculate_summary(SummaryInput {
            url: "http://example.com".into(),
            total_requests: 1,
            total_errors: 0,
            latency_histogram: create_test_histogram(&[5000]),
            total_bytes: 1024,
            duration_secs: 2.0,
            workers: 2,
            status_codes: HashMap::new(),
            e2e_latency_histogram: create_test_histogram(&[]),
            connection_latency_histogram: create_test_histogram(&[]),
            quic_metrics: None,
            sse_metrics: None,
            chaos_injected_total: 0,
            chaos_faults_by_type: HashMap::new(),
            resource_samples: Vec::new(),
            total_sockets_created: 0,
            total_connections_reused: 0,
            dns_resolution_sum_us: 0,
            dns_resolution_count: 0,
        });
        assert_eq!(s.total_requests, 1);
        // HDR quantization may shift values by up to 0.1% of range
        assert!((s.average_latency_ms - 5.0).abs() < 0.01);
        assert!((s.min_latency_ms - 5.0).abs() < 0.01);
        assert!((s.p50_latency_ms - 5.0).abs() < 0.01);
        assert!((s.p90_latency_ms - 5.0).abs() < 0.01);
        assert!((s.p95_latency_ms - 5.0).abs() < 0.01);
        assert!((s.p99_latency_ms - 5.0).abs() < 0.01);
        assert!((s.max_latency_ms - 5.0).abs() < 0.01);
        assert_eq!(s.total_bytes_received, 1024);
        assert_eq!(s.workers, 2);
    }

    #[test]
    fn two_requests() {
        let s = calculate_summary(SummaryInput {
            url: "http://example.com".into(),
            total_requests: 2,
            total_errors: 0,
            latency_histogram: create_test_histogram(&[1000, 2000]),
            total_bytes: 0,
            duration_secs: 1.0,
            workers: 1,
            status_codes: HashMap::new(),
            e2e_latency_histogram: create_test_histogram(&[]),
            connection_latency_histogram: create_test_histogram(&[]),
            quic_metrics: None,
            sse_metrics: None,
            chaos_injected_total: 0,
            chaos_faults_by_type: HashMap::new(),
            resource_samples: Vec::new(),
            total_sockets_created: 0,
            total_connections_reused: 0,
            dns_resolution_sum_us: 0,
            dns_resolution_count: 0,
        });
        assert!((s.average_latency_ms - 1.5).abs() < 0.01);
        assert!((s.min_latency_ms - 1.0).abs() < 0.01);
        assert!((s.p95_latency_ms - 2.0).abs() < 0.01);
        assert!((s.p99_latency_ms - 2.0).abs() < 0.01);
        assert!((s.max_latency_ms - 2.0).abs() < 0.01);
    }

    #[test]
    fn uniform_hundred_values() {
        let latencies: Vec<u64> = (1..=100).collect();
        let s = calculate_summary(SummaryInput {
            url: "http://example.com".into(),
            total_requests: 100,
            total_errors: 0,
            latency_histogram: create_test_histogram(&latencies),
            total_bytes: 0,
            duration_secs: 1.0,
            workers: 1,
            status_codes: HashMap::new(),
            e2e_latency_histogram: create_test_histogram(&[]),
            connection_latency_histogram: create_test_histogram(&[]),
            quic_metrics: None,
            sse_metrics: None,
            chaos_injected_total: 0,
            chaos_faults_by_type: HashMap::new(),
            resource_samples: Vec::new(),
            total_sockets_created: 0,
            total_connections_reused: 0,
            dns_resolution_sum_us: 0,
            dns_resolution_count: 0,
        });
        assert!((s.average_latency_ms - 0.0505).abs() < 0.001);
        assert!((s.min_latency_ms - 0.001).abs() < 0.001);
        assert!((s.p50_latency_ms - 0.051).abs() < 0.001);
        assert!((s.p90_latency_ms - 0.091).abs() < 0.01);
        assert!((s.p95_latency_ms - 0.096).abs() < 0.01);
        assert!((s.p99_latency_ms - 0.1).abs() < 0.01);
        assert!((s.max_latency_ms - 0.1).abs() < 0.01);
    }

    #[test]
    fn all_errors() {
        let s = calculate_summary(SummaryInput {
            url: "http://example.com".into(),
            total_requests: 5,
            total_errors: 5,
            latency_histogram: create_test_histogram(&[100, 200, 300]),
            total_bytes: 0,
            duration_secs: 1.0,
            workers: 1,
            status_codes: HashMap::new(),
            e2e_latency_histogram: create_test_histogram(&[]),
            connection_latency_histogram: create_test_histogram(&[]),
            quic_metrics: None,
            sse_metrics: None,
            chaos_injected_total: 0,
            chaos_faults_by_type: HashMap::new(),
            resource_samples: Vec::new(),
            total_sockets_created: 0,
            total_connections_reused: 0,
            dns_resolution_sum_us: 0,
            dns_resolution_count: 0,
        });
        assert_eq!(s.total_requests, 5);
        assert_eq!(s.total_errors, 5);
    }

    #[test]
    fn microsecond_to_millisecond_conversion() {
        let s = calculate_summary(SummaryInput {
            url: "http://example.com".into(),
            total_requests: 1,
            total_errors: 0,
            latency_histogram: create_test_histogram(&[12345]),
            total_bytes: 0,
            duration_secs: 1.0,
            workers: 1,
            status_codes: HashMap::new(),
            e2e_latency_histogram: create_test_histogram(&[]),
            connection_latency_histogram: create_test_histogram(&[]),
            quic_metrics: None,
            sse_metrics: None,
            chaos_injected_total: 0,
            chaos_faults_by_type: HashMap::new(),
            resource_samples: Vec::new(),
            total_sockets_created: 0,
            total_connections_reused: 0,
            dns_resolution_sum_us: 0,
            dns_resolution_count: 0,
        });
        assert!((s.average_latency_ms - 12.345).abs() < 0.01);
    }

    #[test]
    fn unsorted_latencies_are_handled() {
        let s = calculate_summary(SummaryInput {
            url: "http://example.com".into(),
            total_requests: 3,
            total_errors: 0,
            latency_histogram: create_test_histogram(&[3000, 1000, 2000]),
            total_bytes: 0,
            duration_secs: 1.0,
            workers: 1,
            status_codes: HashMap::new(),
            e2e_latency_histogram: create_test_histogram(&[]),
            connection_latency_histogram: create_test_histogram(&[]),
            quic_metrics: None,
            sse_metrics: None,
            chaos_injected_total: 0,
            chaos_faults_by_type: HashMap::new(),
            resource_samples: Vec::new(),
            total_sockets_created: 0,
            total_connections_reused: 0,
            dns_resolution_sum_us: 0,
            dns_resolution_count: 0,
        });
        assert!((s.p95_latency_ms - 3.0).abs() < 0.01);
        assert!((s.p99_latency_ms - 3.0).abs() < 0.01);
        assert!((s.min_latency_ms - 1.0).abs() < 0.01);
        assert!((s.max_latency_ms - 3.0).abs() < 0.01);
    }

    #[test]
    fn status_codes_preserved() {
        let mut codes = HashMap::new();
        codes.insert(200, 10);
        codes.insert(500, 3);
        let s = calculate_summary(SummaryInput {
            url: "http://example.com".into(),
            total_requests: 13,
            total_errors: 3,
            latency_histogram: create_test_histogram(&[100]),
            total_bytes: 0,
            duration_secs: 1.0,
            workers: 1,
            status_codes: codes,
            e2e_latency_histogram: create_test_histogram(&[]),
            connection_latency_histogram: create_test_histogram(&[]),
            quic_metrics: None,
            sse_metrics: None,
            chaos_injected_total: 0,
            chaos_faults_by_type: HashMap::new(),
            resource_samples: Vec::new(),
            total_sockets_created: 0,
            total_connections_reused: 0,
            dns_resolution_sum_us: 0,
            dns_resolution_count: 0,
        });
        assert_eq!(s.status_codes.get(&200), Some(&10));
        assert_eq!(s.status_codes.get(&500), Some(&3));
    }

    #[test]
    fn test_aggregate_quic_and_sse_metrics() {
        let s = calculate_summary(SummaryInput {
            url: "http://example.com".into(),
            total_requests: 5,
            total_errors: 0,
            latency_histogram: create_test_histogram(&[1000, 2000, 3000, 4000, 5000]),
            total_bytes: 5120,
            duration_secs: 5.0,
            workers: 2,
            status_codes: HashMap::new(),
            e2e_latency_histogram: create_test_histogram(&[]),
            connection_latency_histogram: create_test_histogram(&[100, 200, 300, 400, 500]),
            quic_metrics: Some(QuicMetrics {
                zero_rtt_accepted_count: 3,
                retransmissions: 10,
                avg_handshake_ms: Some(1.5),
            }),
            sse_metrics: Some(SseMetrics {
                total_events_received: 100,
                avg_ttfb_ms: Some(0.5),
            }),
            chaos_injected_total: 0,
            chaos_faults_by_type: HashMap::new(),
            resource_samples: Vec::new(),
            total_sockets_created: 0,
            total_connections_reused: 0,
            dns_resolution_sum_us: 0,
            dns_resolution_count: 0,
        });

        assert!((s.avg_connection_latency_us - 300.0).abs() < 1.0);

        let quic = s.quic.as_ref().unwrap();
        assert_eq!(quic.zero_rtt_accepted_count, 3);
        assert_eq!(quic.retransmissions, 10);
        assert!((quic.avg_handshake_ms.unwrap() - 1.5).abs() < 1e-6);

        let sse = s.sse.as_ref().unwrap();
        assert_eq!(sse.total_events_received, 100);
        assert!((sse.avg_ttfb_ms.unwrap() - 0.5).abs() < 1e-6);
    }

    #[test]
    fn test_summary_optional_protocol_metrics_defaults() {
        let s = calculate_summary(SummaryInput {
            url: "http://example.com".into(),
            total_requests: 1,
            total_errors: 0,
            latency_histogram: create_test_histogram(&[1000]),
            total_bytes: 0,
            duration_secs: 1.0,
            workers: 1,
            status_codes: HashMap::new(),
            e2e_latency_histogram: create_test_histogram(&[]),
            connection_latency_histogram: create_test_histogram(&[]),
            quic_metrics: None,
            sse_metrics: None,
            chaos_injected_total: 0,
            chaos_faults_by_type: HashMap::new(),
            resource_samples: Vec::new(),
            total_sockets_created: 0,
            total_connections_reused: 0,
            dns_resolution_sum_us: 0,
            dns_resolution_count: 0,
        });
        assert!(s.quic.is_none());
        assert!(s.sse.is_none());
        assert_eq!(s.avg_connection_latency_us, 0.0);
    }

    #[test]
    fn test_p99_99_percentile() {
        let latencies: Vec<u64> = (1..=10000).collect();
        let s = calculate_summary(SummaryInput {
            url: "http://example.com".into(),
            total_requests: 10000,
            total_errors: 0,
            latency_histogram: create_test_histogram(&latencies),
            total_bytes: 0,
            duration_secs: 1.0,
            workers: 1,
            status_codes: HashMap::new(),
            e2e_latency_histogram: create_test_histogram(&[]),
            connection_latency_histogram: create_test_histogram(&[]),
            quic_metrics: None,
            sse_metrics: None,
            chaos_injected_total: 0,
            chaos_faults_by_type: HashMap::new(),
            resource_samples: Vec::new(),
            total_sockets_created: 0,
            total_connections_reused: 0,
            dns_resolution_sum_us: 0,
            dns_resolution_count: 0,
        });
        assert!(s.p99_99_latency_ms >= s.p99_latency_ms);
        assert!(s.p99_99_latency_ms <= s.max_latency_ms);
    }

    #[test]
    fn test_histogram_bucket_distribution() {
        let hist = create_test_histogram(&[500, 1500, 7500, 15000, 75000]);
        let buckets = calculate_histogram_from_hdr(&hist);
        assert_eq!(buckets["<1ms"], 1);
        assert_eq!(buckets["1-5ms"], 1);
        assert_eq!(buckets["5-10ms"], 1);
        assert_eq!(buckets["10-25ms"], 1);
        assert_eq!(buckets["50-100ms"], 1);
    }

    #[test]
    fn test_histogram_all_empty() {
        let hist = create_test_histogram(&[]);
        let buckets = calculate_histogram_from_hdr(&hist);
        assert_eq!(buckets["<1ms"], 0);
        assert_eq!(buckets[">1000ms"], 0);
        assert_eq!(buckets.len(), 10);
    }

    #[test]
    fn test_histogram_empty_returns_zeroed_summary() {
        let hist = create_test_histogram(&[]);
        let s = calculate_summary(SummaryInput {
            url: "http://test".into(),
            total_requests: 0,
            total_errors: 0,
            latency_histogram: hist,
            total_bytes: 0,
            duration_secs: 1.0,
            workers: 1,
            status_codes: HashMap::new(),
            e2e_latency_histogram: create_test_histogram(&[]),
            connection_latency_histogram: create_test_histogram(&[]),
            quic_metrics: None,
            sse_metrics: None,
            chaos_injected_total: 0,
            chaos_faults_by_type: HashMap::new(),
            resource_samples: Vec::new(),
            total_sockets_created: 0,
            total_connections_reused: 0,
            dns_resolution_sum_us: 0,
            dns_resolution_count: 0,
        });
        assert_eq!(s.total_requests, 0);
        assert_eq!(s.p50_latency_ms, 0.0);
    }

    #[test]
    fn test_std_dev_single_value() {
        let hist = create_test_histogram(&[5000]);
        let s = calculate_summary(SummaryInput {
            url: "http://example.com".into(),
            total_requests: 1,
            total_errors: 0,
            latency_histogram: hist,
            total_bytes: 0,
            duration_secs: 1.0,
            workers: 1,
            status_codes: HashMap::new(),
            e2e_latency_histogram: create_test_histogram(&[]),
            connection_latency_histogram: create_test_histogram(&[]),
            quic_metrics: None,
            sse_metrics: None,
            chaos_injected_total: 0,
            chaos_faults_by_type: HashMap::new(),
            resource_samples: Vec::new(),
            total_sockets_created: 0,
            total_connections_reused: 0,
            dns_resolution_sum_us: 0,
            dns_resolution_count: 0,
        });
        assert_eq!(s.std_dev_latency_ms, 0.0);
    }

    #[test]
    fn test_std_dev_calculation() {
        // Known values: [1000, 2000, 3000, 4000, 5000] us
        // StdDev = sqrt(2000000) ≈ 1414.21 us
        let hist = create_test_histogram(&[1000, 2000, 3000, 4000, 5000]);
        let s = calculate_summary(SummaryInput {
            url: "http://example.com".into(),
            total_requests: 5,
            total_errors: 0,
            latency_histogram: hist,
            total_bytes: 0,
            duration_secs: 1.0,
            workers: 1,
            status_codes: HashMap::new(),
            e2e_latency_histogram: create_test_histogram(&[]),
            connection_latency_histogram: create_test_histogram(&[]),
            quic_metrics: None,
            sse_metrics: None,
            chaos_injected_total: 0,
            chaos_faults_by_type: HashMap::new(),
            resource_samples: Vec::new(),
            total_sockets_created: 0,
            total_connections_reused: 0,
            dns_resolution_sum_us: 0,
            dns_resolution_count: 0,
        });
        assert!((s.std_dev_latency_ms * MICROS_PER_MILLI - 1414.21).abs() < 15.0);
    }

    #[test]
    fn connection_reuse_all_new() {
        let s = calculate_summary(SummaryInput {
            url: "http://example.com".into(),
            total_requests: 10,
            total_errors: 0,
            latency_histogram: create_test_histogram(&[1000]),
            total_bytes: 0,
            duration_secs: 1.0,
            workers: 1,
            status_codes: HashMap::new(),
            e2e_latency_histogram: create_test_histogram(&[]),
            connection_latency_histogram: create_test_histogram(&[]),
            quic_metrics: None,
            sse_metrics: None,
            chaos_injected_total: 0,
            chaos_faults_by_type: HashMap::new(),
            resource_samples: Vec::new(),
            total_sockets_created: 10,
            total_connections_reused: 0,
            dns_resolution_sum_us: 0,
            dns_resolution_count: 0,
        });
        assert!((s.connection_reuse_ratio - 0.0).abs() < f64::EPSILON);
        assert_eq!(s.avg_dns_resolution_ms, 0.0);
    }

    #[test]
    fn connection_reuse_all_reused() {
        let s = calculate_summary(SummaryInput {
            url: "http://example.com".into(),
            total_requests: 10,
            total_errors: 0,
            latency_histogram: create_test_histogram(&[1000]),
            total_bytes: 0,
            duration_secs: 1.0,
            workers: 1,
            status_codes: HashMap::new(),
            e2e_latency_histogram: create_test_histogram(&[]),
            connection_latency_histogram: create_test_histogram(&[]),
            quic_metrics: None,
            sse_metrics: None,
            chaos_injected_total: 0,
            chaos_faults_by_type: HashMap::new(),
            resource_samples: Vec::new(),
            total_sockets_created: 0,
            total_connections_reused: 10,
            dns_resolution_sum_us: 0,
            dns_resolution_count: 0,
        });
        assert!((s.connection_reuse_ratio - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn connection_reuse_mixed() {
        let s = calculate_summary(SummaryInput {
            url: "http://example.com".into(),
            total_requests: 10,
            total_errors: 0,
            latency_histogram: create_test_histogram(&[1000]),
            total_bytes: 0,
            duration_secs: 1.0,
            workers: 1,
            status_codes: HashMap::new(),
            e2e_latency_histogram: create_test_histogram(&[]),
            connection_latency_histogram: create_test_histogram(&[]),
            quic_metrics: None,
            sse_metrics: None,
            chaos_injected_total: 0,
            chaos_faults_by_type: HashMap::new(),
            resource_samples: Vec::new(),
            total_sockets_created: 5,
            total_connections_reused: 5,
            dns_resolution_sum_us: 0,
            dns_resolution_count: 0,
        });
        assert!((s.connection_reuse_ratio - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn connection_reuse_zero_requests() {
        let s = calculate_summary(SummaryInput {
            url: "http://example.com".into(),
            total_requests: 0,
            total_errors: 0,
            latency_histogram: create_test_histogram(&[]),
            total_bytes: 0,
            duration_secs: 1.0,
            workers: 1,
            status_codes: HashMap::new(),
            e2e_latency_histogram: create_test_histogram(&[]),
            connection_latency_histogram: create_test_histogram(&[]),
            quic_metrics: None,
            sse_metrics: None,
            chaos_injected_total: 0,
            chaos_faults_by_type: HashMap::new(),
            resource_samples: Vec::new(),
            total_sockets_created: 0,
            total_connections_reused: 0,
            dns_resolution_sum_us: 0,
            dns_resolution_count: 0,
        });
        assert_eq!(s.connection_reuse_ratio, 0.0);
        assert_eq!(s.avg_dns_resolution_ms, 0.0);
    }

    #[test]
    fn dns_resolution_average() {
        let s = calculate_summary(SummaryInput {
            url: "http://example.com".into(),
            total_requests: 3,
            total_errors: 0,
            latency_histogram: create_test_histogram(&[1000, 2000, 3000]),
            total_bytes: 0,
            duration_secs: 1.0,
            workers: 1,
            status_codes: HashMap::new(),
            e2e_latency_histogram: create_test_histogram(&[]),
            connection_latency_histogram: create_test_histogram(&[]),
            quic_metrics: None,
            sse_metrics: None,
            chaos_injected_total: 0,
            chaos_faults_by_type: HashMap::new(),
            resource_samples: Vec::new(),
            total_sockets_created: 3,
            total_connections_reused: 0,
            dns_resolution_sum_us: 6000,
            dns_resolution_count: 3,
        });
        assert!((s.avg_dns_resolution_ms - 2.0).abs() < f64::EPSILON);
    }

    #[test]
    fn dns_resolution_no_data() {
        let s = calculate_summary(SummaryInput {
            url: "http://example.com".into(),
            total_requests: 5,
            total_errors: 0,
            latency_histogram: create_test_histogram(&[1000]),
            total_bytes: 0,
            duration_secs: 1.0,
            workers: 1,
            status_codes: HashMap::new(),
            e2e_latency_histogram: create_test_histogram(&[]),
            connection_latency_histogram: create_test_histogram(&[]),
            quic_metrics: None,
            sse_metrics: None,
            chaos_injected_total: 0,
            chaos_faults_by_type: HashMap::new(),
            resource_samples: Vec::new(),
            total_sockets_created: 5,
            total_connections_reused: 0,
            dns_resolution_sum_us: 0,
            dns_resolution_count: 0,
        });
        assert_eq!(s.avg_dns_resolution_ms, 0.0);
    }
}
