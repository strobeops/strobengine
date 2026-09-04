use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::time::{SystemTime, UNIX_EPOCH};

use pyo3::prelude::*;
use pyo3::types::PyDict;

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

pub struct RequestMetric {
    pub latency_micros: u128,
    pub status_code: u16,
    pub bytes_received: u64,
    pub is_reconnect: bool,
    pub connection_latency_us: Option<u128>,
    pub timestamp_sent_ns: Option<u128>,
    pub e2e_latency_us: Option<u128>,
    pub quic_handshake_us: Option<u64>,
    pub quic_0rtt_used: bool,
    pub quic_retransmits: Option<u64>,
    pub sse_events_received: Option<u64>,
    pub sse_first_event_us: Option<u64>,
    pub sse_event_interval_us: Option<u64>,
}

impl RequestMetric {
    pub fn error(latency_micros: u128) -> Self {
        Self {
            latency_micros,
            status_code: 0,
            bytes_received: 0,
            is_reconnect: false,
            connection_latency_us: None,
            timestamp_sent_ns: None,
            e2e_latency_us: None,
            quic_handshake_us: None,
            quic_0rtt_used: false,
            quic_retransmits: None,
            sse_events_received: None,
            sse_first_event_us: None,
            sse_event_interval_us: None,
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
#[derive(Debug, Clone)]
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
}

#[pymethods]
impl TestSummary {
    pub fn to_dict<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyDict>> {
        let dict = PyDict::new(py);
        dict.set_item("url", &self.url)?;
        dict.set_item("total_requests", self.total_requests)?;
        dict.set_item("total_errors", self.total_errors)?;
        dict.set_item("average_latency_ms", self.average_latency_ms)?;
        dict.set_item("p95_latency_ms", self.p95_latency_ms)?;
        dict.set_item("p99_latency_ms", self.p99_latency_ms)?;
        dict.set_item("min_latency_ms", self.min_latency_ms)?;
        dict.set_item("p50_latency_ms", self.p50_latency_ms)?;
        dict.set_item("p90_latency_ms", self.p90_latency_ms)?;
        dict.set_item("max_latency_ms", self.max_latency_ms)?;
        dict.set_item("total_bytes_received", self.total_bytes_received)?;
        dict.set_item("duration_secs", self.duration_secs)?;
        dict.set_item("workers", self.workers)?;
        dict.set_item("timestamp", &self.timestamp)?;
        dict.set_item("raw_command", self.raw_command.as_deref())?;
        let status_dict = PyDict::new(py);
        for (&code, &count) in &self.status_codes {
            status_dict.set_item(code, count)?;
        }
        dict.set_item("status_codes", &status_dict)?;
        dict.set_item("avg_e2e_latency_us", self.avg_e2e_latency_us)?;
        dict.set_item("avg_connection_latency_us", self.avg_connection_latency_us)?;
        match &self.quic {
            Some(quic) => dict.set_item("quic", quic.clone())?,
            None => dict.set_item("quic", py.None())?,
        }
        match &self.sse {
            Some(sse) => dict.set_item("sse", sse.clone())?,
            None => dict.set_item("sse", py.None())?,
        }
        Ok(dict)
    }

    #[pyo3(signature = (indent=None))]
    pub fn to_json(&self, py: Python<'_>, indent: Option<usize>) -> PyResult<String> {
        let json_mod = py.import("json")?;
        let dict = self.to_dict(py)?;
        let kwargs = PyDict::new(py);
        if let Some(i) = indent {
            kwargs.set_item("indent", i)?;
        }
        let json_str = json_mod.call_method("dumps", (&dict,), Some(&kwargs))?;
        json_str.extract()
    }
}

#[allow(clippy::too_many_arguments)]
pub fn calculate_summary(
    url: String,
    total_requests: u64,
    total_errors: u64,
    mut latencies: Vec<u128>,
    total_bytes: u64,
    duration_secs: f64,
    workers: usize,
    status_codes: HashMap<u16, u64>,
    e2e_latencies: Vec<u128>,
    connection_latencies: Vec<u128>,
    quic_metrics: Option<QuicMetrics>,
    sse_metrics: Option<SseMetrics>,
) -> TestSummary {
    let avg_e2e_latency_us = if e2e_latencies.is_empty() {
        0.0
    } else {
        let sum: u128 = e2e_latencies.iter().sum();
        sum as f64 / e2e_latencies.len() as f64
    };

    let avg_connection_latency_us = if connection_latencies.is_empty() {
        0.0
    } else {
        let sum: u128 = connection_latencies.iter().sum();
        sum as f64 / connection_latencies.len() as f64
    };

    if latencies.is_empty() {
        return TestSummary {
            url,
            total_requests: total_requests as usize,
            total_errors: total_errors as usize,
            average_latency_ms: 0.0,
            p95_latency_ms: 0.0,
            p99_latency_ms: 0.0,
            min_latency_ms: 0.0,
            p50_latency_ms: 0.0,
            p90_latency_ms: 0.0,
            max_latency_ms: 0.0,
            total_bytes_received: total_bytes,
            duration_secs,
            workers,
            timestamp: String::new(),
            raw_command: None,
            status_codes,
            avg_e2e_latency_us,
            avg_connection_latency_us,
            quic: quic_metrics,
            sse: sse_metrics,
        };
    }

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

    TestSummary {
        url,
        total_requests: total_requests as usize,
        total_errors: total_errors as usize,
        average_latency_ms,
        p95_latency_ms: latencies[p95_idx] as f64 / MICROS_PER_MILLI,
        p99_latency_ms: latencies[p99_idx] as f64 / MICROS_PER_MILLI,
        min_latency_ms,
        p50_latency_ms: latencies[p50_idx] as f64 / MICROS_PER_MILLI,
        p90_latency_ms: latencies[p90_idx] as f64 / MICROS_PER_MILLI,
        max_latency_ms,
        total_bytes_received: total_bytes,
        duration_secs,
        workers,
        timestamp: String::new(),
        raw_command: None,
        status_codes,
        avg_e2e_latency_us,
        avg_connection_latency_us,
        quic: quic_metrics,
        sse: sse_metrics,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_latencies_returns_zeros() {
        let s = calculate_summary(
            "http://example.com".into(),
            10,
            3,
            vec![],
            0,
            1.0,
            4,
            HashMap::new(),
            vec![],
            vec![],
            None,
            None,
        );
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
        let s = calculate_summary(
            "http://example.com".into(),
            1,
            0,
            vec![5000],
            1024,
            2.0,
            2,
            HashMap::new(),
            vec![],
            vec![],
            None,
            None,
        );
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
        let s = calculate_summary(
            "http://example.com".into(),
            2,
            0,
            vec![1000, 2000],
            0,
            1.0,
            1,
            HashMap::new(),
            vec![],
            vec![],
            None,
            None,
        );
        assert_eq!(s.average_latency_ms, 1.5);
        assert_eq!(s.min_latency_ms, 1.0);
        assert_eq!(s.p95_latency_ms, 2.0);
        assert_eq!(s.p99_latency_ms, 2.0);
        assert_eq!(s.max_latency_ms, 2.0);
    }

    #[test]
    fn uniform_hundred_values() {
        let latencies: Vec<u128> = (1..=100).collect();
        let s = calculate_summary(
            "http://example.com".into(),
            100,
            0,
            latencies,
            0,
            1.0,
            1,
            HashMap::new(),
            vec![],
            vec![],
            None,
            None,
        );
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
        let s = calculate_summary(
            "http://example.com".into(),
            5,
            5,
            vec![100, 200, 300],
            0,
            1.0,
            1,
            HashMap::new(),
            vec![],
            vec![],
            None,
            None,
        );
        assert_eq!(s.total_requests, 5);
        assert_eq!(s.total_errors, 5);
    }

    #[test]
    fn microsecond_to_millisecond_conversion() {
        let s = calculate_summary(
            "http://example.com".into(),
            1,
            0,
            vec![12345],
            0,
            1.0,
            1,
            HashMap::new(),
            vec![],
            vec![],
            None,
            None,
        );
        assert!((s.average_latency_ms - 12.345).abs() < 1e-6);
    }

    #[test]
    fn unsorted_latencies_are_sorted() {
        let s = calculate_summary(
            "http://example.com".into(),
            3,
            0,
            vec![3000, 1000, 2000],
            0,
            1.0,
            1,
            HashMap::new(),
            vec![],
            vec![],
            None,
            None,
        );
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
        let s = calculate_summary(
            "http://example.com".into(),
            13,
            3,
            vec![100],
            0,
            1.0,
            1,
            codes,
            vec![],
            vec![],
            None,
            None,
        );
        assert_eq!(s.status_codes.get(&200), Some(&10));
        assert_eq!(s.status_codes.get(&500), Some(&3));
    }
}
