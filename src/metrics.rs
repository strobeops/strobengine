use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::time::{SystemTime, UNIX_EPOCH};

use pyo3::prelude::*;
use pyo3::types::PyDict;
use tokio::sync::mpsc;

/// Conversion factor from microseconds to milliseconds (1 ms = 1,000 us).
pub const MICROS_PER_MILLI: f64 = 1_000.0;

/// Get current wall-clock time in nanoseconds since UNIX epoch.
pub fn wallclock_ns() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
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

/// Calculate standard deviation of latencies in microseconds.
/// Returns 0.0 if fewer than 2 values.
fn calculate_std_dev_us(latencies: &[u128]) -> f64 {
    if latencies.len() < 2 {
        return 0.0;
    }
    let mean = latencies.iter().sum::<u128>() as f64 / latencies.len() as f64;
    let variance = latencies
        .iter()
        .map(|&x| {
            let diff = x as f64 - mean;
            diff * diff
        })
        .sum::<f64>()
        / latencies.len() as f64;
    variance.sqrt()
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

/// Compute latency histogram from sorted latencies (in microseconds).
fn calculate_histogram(latencies: &[u128]) -> HashMap<String, u64> {
    let mut buckets: HashMap<String, u64> = HashMap::new();

    // Initialize exact bucket keys matching the output schema
    buckets.insert("<1ms".to_string(), 0);
    buckets.insert("1-5ms".to_string(), 0);
    buckets.insert("5-10ms".to_string(), 0);
    buckets.insert("10-25ms".to_string(), 0);
    buckets.insert("25-50ms".to_string(), 0);
    buckets.insert("50-100ms".to_string(), 0);
    buckets.insert("100-250ms".to_string(), 0);
    buckets.insert("250-500ms".to_string(), 0);
    buckets.insert("500-1000ms".to_string(), 0);
    buckets.insert(">1000ms".to_string(), 0);

    // Classify latencies into buckets
    for &lat in latencies {
        let ms = lat as f64 / MICROS_PER_MILLI;
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

        *buckets.get_mut(bucket_key).unwrap() += 1;
    }

    buckets
}

/// Input parameters for summary calculation.
pub struct SummaryInput {
    pub url: String,
    pub total_requests: u64,
    pub total_errors: u64,
    pub latencies: Vec<u128>,
    pub total_bytes: u64,
    pub duration_secs: f64,
    pub workers: usize,
    pub status_codes: HashMap<u16, u64>,
    pub e2e_latencies: Vec<u128>,
    pub connection_latencies: Vec<u128>,
    pub quic_metrics: Option<QuicMetrics>,
    pub sse_metrics: Option<SseMetrics>,
    pub chaos_injected_total: u64,
    pub chaos_faults_by_type: HashMap<String, u64>,
}

/// Finalize metric aggregation from a receiver channel.
/// Returns aggregated metrics as a tuple for the caller to construct SummaryInput.
pub async fn finalize_metrics(
    mut rx: mpsc::Receiver<RequestMetric>,
) -> (
    Vec<u128>,         // latencies
    Vec<u128>,         // e2e_latencies
    Vec<u128>,         // connection_latencies
    HashMap<u16, u64>, // status_codes
    u64,               // total_bytes
    Option<QuicMetrics>,
    Option<SseMetrics>,
    u64,                  // chaos_injected_total
    HashMap<String, u64>, // chaos_faults_by_type
) {
    let mut latencies = Vec::new();
    let mut e2e_latencies = Vec::new();
    let mut connection_latencies = Vec::new();
    let mut quic_stats = QuicMetrics::default();
    let mut sse_stats = SseMetrics::default();
    let mut has_quic = false;
    let mut quic_handshakes = Vec::new();
    let mut sse_first_events = Vec::new();
    let mut status_codes = HashMap::new();
    let mut total_bytes = 0u64;
    let mut chaos_injected_total = 0u64;
    let mut chaos_faults_by_type = HashMap::new();

    while let Some(metric) = rx.recv().await {
        latencies.push(metric.latency_micros);
        if let Some(e2e) = metric.connection.e2e_latency_us {
            e2e_latencies.push(e2e);
        }
        if let Some(conn) = metric.connection.connection_latency_us {
            connection_latencies.push(conn);
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
                quic_handshakes.push(handshake);
            }
        }
        // SSE aggregation
        if let Some(events) = metric.sse_events_received {
            sse_stats.total_events_received += events;
        }
        if let Some(first) = metric.sse_first_event_us {
            sse_first_events.push(first);
        }
        // Chaos aggregation
        if let Some(ref fault) = metric.chaos_fault {
            chaos_injected_total += 1;
            *chaos_faults_by_type
                .entry(fault.name().to_string())
                .or_insert(0) += 1;
        }
        *status_codes.entry(metric.status_code).or_insert(0) += 1;
        total_bytes += metric.bytes_received;
    }

    // Compute QUIC avg handshake (convert us to ms)
    if !quic_handshakes.is_empty() {
        let sum: u64 = quic_handshakes.iter().sum();
        quic_stats.avg_handshake_ms = Some((sum as f64 / quic_handshakes.len() as f64) / 1000.0);
    }

    // Compute SSE avg TTFB (convert us to ms)
    if !sse_first_events.is_empty() {
        let sum: u64 = sse_first_events.iter().sum();
        sse_stats.avg_ttfb_ms = Some((sum as f64 / sse_first_events.len() as f64) / 1000.0);
    }

    let quic_opt = if has_quic { Some(quic_stats) } else { None };
    let sse_opt = if sse_stats.total_events_received > 0 || !sse_first_events.is_empty() {
        Some(sse_stats)
    } else {
        None
    };

    (
        latencies,
        e2e_latencies,
        connection_latencies,
        status_codes,
        total_bytes,
        quic_opt,
        sse_opt,
        chaos_injected_total,
        chaos_faults_by_type,
    )
}

pub fn calculate_summary(input: SummaryInput) -> TestSummary {
    let avg_e2e_latency_us = if input.e2e_latencies.is_empty() {
        0.0
    } else {
        let sum: u128 = input.e2e_latencies.iter().sum();
        sum as f64 / input.e2e_latencies.len() as f64
    };

    let avg_connection_latency_us = if input.connection_latencies.is_empty() {
        0.0
    } else {
        let sum: u128 = input.connection_latencies.iter().sum();
        sum as f64 / input.connection_latencies.len() as f64
    };

    if input.latencies.is_empty() {
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
        };
    }

    let mut latencies = input.latencies;
    latencies.sort_unstable();

    let len = latencies.len();
    let sum: u128 = latencies.iter().sum();
    let average_latency_ms = sum as f64 / len as f64 / MICROS_PER_MILLI;

    let min_latency_ms = latencies[0] as f64 / MICROS_PER_MILLI;
    let max_latency_ms = latencies[len - 1] as f64 / MICROS_PER_MILLI;

    let p50_idx = (len * 50 / 100).min(len - 1);
    let p90_idx = (len * 90 / 100).min(len - 1);
    let p95_idx = (len * 95 / 100).min(len - 1);
    let p99_idx = (len * 99 / 100).min(len - 1);
    let p99_99_idx = (len * 9999 / 10000).min(len - 1);

    // Compute standard deviation
    let std_dev_us = calculate_std_dev_us(&latencies);
    let std_dev_latency_ms = std_dev_us / MICROS_PER_MILLI;

    // Compute histogram
    let latency_histogram = calculate_histogram(&latencies);

    TestSummary {
        url: input.url,
        total_requests: input.total_requests as usize,
        total_errors: input.total_errors as usize,
        average_latency_ms,
        p95_latency_ms: latencies[p95_idx] as f64 / MICROS_PER_MILLI,
        p99_latency_ms: latencies[p99_idx] as f64 / MICROS_PER_MILLI,
        min_latency_ms,
        p50_latency_ms: latencies[p50_idx] as f64 / MICROS_PER_MILLI,
        p90_latency_ms: latencies[p90_idx] as f64 / MICROS_PER_MILLI,
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
        p99_99_latency_ms: latencies[p99_99_idx] as f64 / MICROS_PER_MILLI,
        latency_histogram,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_latencies_returns_zeros() {
        let s = calculate_summary(SummaryInput {
            url: "http://example.com".into(),
            total_requests: 10,
            total_errors: 3,
            latencies: vec![],
            total_bytes: 0,
            duration_secs: 1.0,
            workers: 4,
            status_codes: HashMap::new(),
            e2e_latencies: vec![],
            connection_latencies: vec![],
            quic_metrics: None,
            sse_metrics: None,
            chaos_injected_total: 0,
            chaos_faults_by_type: HashMap::new(),
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
            latencies: vec![5000],
            total_bytes: 1024,
            duration_secs: 2.0,
            workers: 2,
            status_codes: HashMap::new(),
            e2e_latencies: vec![],
            connection_latencies: vec![],
            quic_metrics: None,
            sse_metrics: None,
            chaos_injected_total: 0,
            chaos_faults_by_type: HashMap::new(),
        });
        assert_eq!(s.total_requests, 1);
        assert_eq!(s.average_latency_ms, 5.0);
        assert_eq!(s.min_latency_ms, 5.0);
        assert_eq!(s.p50_latency_ms, 5.0);
        assert_eq!(s.p90_latency_ms, 5.0);
        assert_eq!(s.p95_latency_ms, 5.0);
        assert_eq!(s.p99_latency_ms, 5.0);
        assert_eq!(s.max_latency_ms, 5.0);
        assert_eq!(s.total_bytes_received, 1024);
        assert_eq!(s.workers, 2);
    }

    #[test]
    fn two_requests() {
        let s = calculate_summary(SummaryInput {
            url: "http://example.com".into(),
            total_requests: 2,
            total_errors: 0,
            latencies: vec![1000, 2000],
            total_bytes: 0,
            duration_secs: 1.0,
            workers: 1,
            status_codes: HashMap::new(),
            e2e_latencies: vec![],
            connection_latencies: vec![],
            quic_metrics: None,
            sse_metrics: None,
            chaos_injected_total: 0,
            chaos_faults_by_type: HashMap::new(),
        });
        assert_eq!(s.average_latency_ms, 1.5);
        assert_eq!(s.min_latency_ms, 1.0);
        assert_eq!(s.p95_latency_ms, 2.0);
        assert_eq!(s.p99_latency_ms, 2.0);
        assert_eq!(s.max_latency_ms, 2.0);
    }

    #[test]
    fn uniform_hundred_values() {
        let latencies: Vec<u128> = (1..=100).collect();
        let s = calculate_summary(SummaryInput {
            url: "http://example.com".into(),
            total_requests: 100,
            total_errors: 0,
            latencies,
            total_bytes: 0,
            duration_secs: 1.0,
            workers: 1,
            status_codes: HashMap::new(),
            e2e_latencies: vec![],
            connection_latencies: vec![],
            quic_metrics: None,
            sse_metrics: None,
            chaos_injected_total: 0,
            chaos_faults_by_type: HashMap::new(),
        });
        assert!((s.average_latency_ms - 0.0505).abs() < 1e-6);
        assert_eq!(s.min_latency_ms, 0.001);
        assert!((s.p50_latency_ms - 0.051).abs() < 1e-6);
        assert!((s.p90_latency_ms - 0.091).abs() < 1e-6);
        assert!((s.p95_latency_ms - 0.096).abs() < 1e-6);
        assert!((s.p99_latency_ms - 0.1).abs() < 1e-6);
        assert_eq!(s.max_latency_ms, 0.1);
    }

    #[test]
    fn all_errors() {
        let s = calculate_summary(SummaryInput {
            url: "http://example.com".into(),
            total_requests: 5,
            total_errors: 5,
            latencies: vec![100, 200, 300],
            total_bytes: 0,
            duration_secs: 1.0,
            workers: 1,
            status_codes: HashMap::new(),
            e2e_latencies: vec![],
            connection_latencies: vec![],
            quic_metrics: None,
            sse_metrics: None,
            chaos_injected_total: 0,
            chaos_faults_by_type: HashMap::new(),
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
            latencies: vec![12345],
            total_bytes: 0,
            duration_secs: 1.0,
            workers: 1,
            status_codes: HashMap::new(),
            e2e_latencies: vec![],
            connection_latencies: vec![],
            quic_metrics: None,
            sse_metrics: None,
            chaos_injected_total: 0,
            chaos_faults_by_type: HashMap::new(),
        });
        assert!((s.average_latency_ms - 12.345).abs() < 1e-6);
    }

    #[test]
    fn unsorted_latencies_are_sorted() {
        let s = calculate_summary(SummaryInput {
            url: "http://example.com".into(),
            total_requests: 3,
            total_errors: 0,
            latencies: vec![3000, 1000, 2000],
            total_bytes: 0,
            duration_secs: 1.0,
            workers: 1,
            status_codes: HashMap::new(),
            e2e_latencies: vec![],
            connection_latencies: vec![],
            quic_metrics: None,
            sse_metrics: None,
            chaos_injected_total: 0,
            chaos_faults_by_type: HashMap::new(),
        });
        assert_eq!(s.p95_latency_ms, 3.0);
        assert_eq!(s.p99_latency_ms, 3.0);
        assert_eq!(s.min_latency_ms, 1.0);
        assert_eq!(s.max_latency_ms, 3.0);
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
            latencies: vec![100],
            total_bytes: 0,
            duration_secs: 1.0,
            workers: 1,
            status_codes: codes,
            e2e_latencies: vec![],
            connection_latencies: vec![],
            quic_metrics: None,
            sse_metrics: None,
            chaos_injected_total: 0,
            chaos_faults_by_type: HashMap::new(),
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
            latencies: vec![1000, 2000, 3000, 4000, 5000],
            total_bytes: 5120,
            duration_secs: 5.0,
            workers: 2,
            status_codes: HashMap::new(),
            e2e_latencies: vec![],
            connection_latencies: vec![100, 200, 300, 400, 500],
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
        });

        assert!((s.avg_connection_latency_us - 300.0).abs() < 1e-6);

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
            latencies: vec![1000],
            total_bytes: 0,
            duration_secs: 1.0,
            workers: 1,
            status_codes: HashMap::new(),
            e2e_latencies: vec![],
            connection_latencies: vec![],
            quic_metrics: None,
            sse_metrics: None,
            chaos_injected_total: 0,
            chaos_faults_by_type: HashMap::new(),
        });
        assert!(s.quic.is_none());
        assert!(s.sse.is_none());
        assert_eq!(s.avg_connection_latency_us, 0.0);
    }

    #[test]
    fn test_std_dev_single_value() {
        let latencies = vec![5000];
        assert_eq!(calculate_std_dev_us(&latencies), 0.0);
    }

    #[test]
    fn test_std_dev_empty() {
        let latencies: Vec<u128> = vec![];
        assert_eq!(calculate_std_dev_us(&latencies), 0.0);
    }

    #[test]
    fn test_std_dev_calculation() {
        // Known values: [1000, 2000, 3000, 4000, 5000] us
        // Mean = 3000, StdDev = sqrt(2000000) ≈ 1414.21 us
        let latencies = vec![1000, 2000, 3000, 4000, 5000];
        let std_dev = calculate_std_dev_us(&latencies);
        assert!((std_dev - 1414.21).abs() < 1.0);
    }

    #[test]
    fn test_p99_99_percentile() {
        let latencies: Vec<u128> = (1..=10000).collect();
        let s = calculate_summary(SummaryInput {
            url: "http://example.com".into(),
            total_requests: 10000,
            total_errors: 0,
            latencies,
            total_bytes: 0,
            duration_secs: 1.0,
            workers: 1,
            status_codes: HashMap::new(),
            e2e_latencies: vec![],
            connection_latencies: vec![],
            quic_metrics: None,
            sse_metrics: None,
            chaos_injected_total: 0,
            chaos_faults_by_type: HashMap::new(),
        });
        // p99.99 should be very close to max
        assert!(s.p99_99_latency_ms >= s.p99_latency_ms);
        assert!(s.p99_99_latency_ms <= s.max_latency_ms);
    }

    #[test]
    fn test_histogram_bucket_distribution() {
        let latencies = vec![500, 1500, 7500, 15000, 75000];
        let hist = calculate_histogram(&latencies);
        assert_eq!(hist["<1ms"], 1); // 500us = 0.5ms
        assert_eq!(hist["1-5ms"], 1); // 1500us = 1.5ms
        assert_eq!(hist["5-10ms"], 1); // 7500us = 7.5ms
        assert_eq!(hist["10-25ms"], 1); // 15000us = 15ms
        assert_eq!(hist["50-100ms"], 1); // 75000us = 75ms
    }

    #[test]
    fn test_histogram_all_empty() {
        let latencies: Vec<u128> = vec![];
        let hist = calculate_histogram(&latencies);
        assert_eq!(hist["<1ms"], 0);
        assert_eq!(hist[">1000ms"], 0);
        assert_eq!(hist.len(), 10);
    }
}
