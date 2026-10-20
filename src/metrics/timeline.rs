use hdrhistogram::Histogram;
use pyo3::prelude::*;

/// One timeline sample: throughput and latency over a single interval.
#[pyclass(from_py_object)]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TimelineSample {
    #[pyo3(get)]
    pub elapsed_sec: f64,
    #[pyo3(get)]
    pub rps: f64,
    #[pyo3(get)]
    pub error_rate: f64,
    #[pyo3(get)]
    pub p50_us: f64,
    #[pyo3(get)]
    pub p95_us: f64,
    #[pyo3(get)]
    pub p99_us: f64,
}

/// Time series of throughput/latency samples captured during a run.
#[pyclass(from_py_object)]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Timeline {
    #[pyo3(get)]
    pub interval_ms: u64,
    #[pyo3(get)]
    pub samples: Vec<TimelineSample>,
}

/// Build one timeline sample from an interval's counters and histogram.
pub(crate) fn make_timeline_sample(
    elapsed_sec: f64,
    requests: u64,
    errors: u64,
    interval_secs: f64,
    histogram: &Histogram<u64>,
) -> TimelineSample {
    let rps = if interval_secs > 0.0 {
        requests as f64 / interval_secs
    } else {
        0.0
    };
    let error_rate = if requests > 0 {
        errors as f64 / requests as f64 * 100.0
    } else {
        0.0
    };
    TimelineSample {
        elapsed_sec,
        rps,
        error_rate,
        p50_us: histogram.value_at_quantile(0.50) as f64,
        p95_us: histogram.value_at_quantile(0.95) as f64,
        p99_us: histogram.value_at_quantile(0.99) as f64,
    }
}
