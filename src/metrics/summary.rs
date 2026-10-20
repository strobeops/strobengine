use std::collections::HashMap;

use hdrhistogram::Histogram;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use super::helpers::MICROS_PER_MILLI;
use super::protocols::{
    DatabaseMetrics, GraphqlMetrics, GraphqlWsMetrics, GrpcMetrics, Http3Metrics, QuicMetrics,
    SoapMetrics, SseMetrics, StompMetrics, WebsocketMetrics,
};
use super::system;
use super::timeline::Timeline;

#[pyclass(skip_from_py_object)]
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
#[serde(default)]
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
    pub ws: Option<WebsocketMetrics>,
    #[pyo3(get)]
    pub grpc: Option<GrpcMetrics>,
    #[pyo3(get)]
    pub http3: Option<Http3Metrics>,
    #[pyo3(get)]
    pub graphql: Option<GraphqlMetrics>,
    #[pyo3(get)]
    pub graphql_ws: Option<GraphqlWsMetrics>,
    #[pyo3(get)]
    pub soap: Option<SoapMetrics>,
    #[pyo3(get)]
    pub stomp: Option<StompMetrics>,
    #[pyo3(get)]
    pub database: Option<DatabaseMetrics>,
    #[pyo3(get)]
    pub timeline: Option<Timeline>,
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

    /// Build a summary from its JSON representation.
    ///
    /// Missing fields fall back to their defaults, so partial fixtures are
    /// valid; nested metric blocks may also omit fields.
    #[staticmethod]
    fn from_json(data: &str) -> PyResult<Self> {
        serde_json::from_str(data)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))
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
pub(crate) fn calculate_histogram_from_hdr(hist: &Histogram<u64>) -> HashMap<String, u64> {
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

/// Convert an HDR histogram into a fixed-width vector of bucket counts ordered
/// by [`HISTOGRAM_BUCKET_ORDER`].
///
/// The vector is index-aligned across the fleet so a master can aggregate
/// distributions by element-wise summation without shipping bucket bounds.
pub fn histogram_buckets(hist: &Histogram<u64>) -> Vec<u64> {
    let buckets = calculate_histogram_from_hdr(hist);
    HISTOGRAM_BUCKET_ORDER
        .iter()
        .map(|key| buckets.get(*key).copied().unwrap_or(0))
        .collect()
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
    pub ws_metrics: Option<WebsocketMetrics>,
    pub grpc_metrics: Option<GrpcMetrics>,
    pub http3_metrics: Option<Http3Metrics>,
    pub graphql_metrics: Option<GraphqlMetrics>,
    pub graphql_ws_metrics: Option<GraphqlWsMetrics>,
    pub soap_metrics: Option<SoapMetrics>,
    pub stomp_metrics: Option<StompMetrics>,
    pub db_metrics: Option<DatabaseMetrics>,
    pub timeline: Option<Timeline>,
    pub chaos_injected_total: u64,
    pub chaos_faults_by_type: HashMap<String, u64>,
    pub resource_samples: Vec<system::ResourceSample>,
    pub total_sockets_created: u64,
    pub total_connections_reused: u64,
    pub dns_resolution_sum_us: u64,
    pub dns_resolution_count: u64,
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
            ws: input.ws_metrics,
            grpc: input.grpc_metrics,
            http3: input.http3_metrics,
            graphql: input.graphql_metrics,
            graphql_ws: input.graphql_ws_metrics,
            soap: input.soap_metrics,
            stomp: input.stomp_metrics,
            database: input.db_metrics,
            timeline: input.timeline,
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
        ws: input.ws_metrics,
        grpc: input.grpc_metrics,
        http3: input.http3_metrics,
        graphql: input.graphql_metrics,
        graphql_ws: input.graphql_ws_metrics,
        soap: input.soap_metrics,
        stomp: input.stomp_metrics,
        database: input.db_metrics,
        timeline: input.timeline,
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
