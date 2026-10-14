//! Windowed metric aggregation and telemetry streaming.
//!
//! [`WindowedCollector`] folds per-request [`RequestMetric`]s into fixed time
//! windows and produces a [`MetricBatch`] per window. Batches are pushed into a
//! channel consumed by [`upload_metrics`], which drives the client-streaming
//! `TelemetryService.StreamMetrics` RPC.

use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use hdrhistogram::Histogram;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;

use crate::metrics::{HISTOGRAM_BUCKET_ORDER, MICROS_PER_MILLI, RequestMetric, histogram_buckets};

use super::error::WorkerResult;
use super::proto::telemetry_service_client::TelemetryServiceClient;
use super::proto::{ErrorBreakdown, ErrorClass, LatencySummary, MetricBatch, StreamAcknowledgment};

/// Highest trackable latency (1 hour in microseconds), matching the engine.
const HISTOGRAM_HIGHEST_TRACKABLE: u64 = 3_600_000_000;
/// Significant figures for HDR histogram precision, matching the engine.
const HISTOGRAM_SIGNIFICANT_FIGURES: u8 = 3;

fn now_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn new_histogram() -> Histogram<u64> {
    Histogram::new_with_max(HISTOGRAM_HIGHEST_TRACKABLE, HISTOGRAM_SIGNIFICANT_FIGURES)
        .expect("valid histogram bounds")
}

/// Classify a failed request by HTTP status.
///
/// `RequestMetric` does not yet carry fine-grained transport failure kinds, so
/// status `0` maps to `OTHER`; 4xx/5xx map to their families.
fn classify_error(status_code: u16) -> ErrorClass {
    match status_code {
        400..=499 => ErrorClass::Http4xx,
        500..=599 => ErrorClass::Http5xx,
        _ => ErrorClass::Other,
    }
}

fn latency_summary(hist: &Histogram<u64>) -> LatencySummary {
    if hist.is_empty() {
        return LatencySummary {
            bucket_counts: vec![0; HISTOGRAM_BUCKET_ORDER.len()],
            ..Default::default()
        };
    }
    let as_ms = |us: u64| (us as f64 / MICROS_PER_MILLI) as f32;
    LatencySummary {
        p50_ms: as_ms(hist.value_at_quantile(0.50)),
        p90_ms: as_ms(hist.value_at_quantile(0.90)),
        p95_ms: as_ms(hist.value_at_quantile(0.95)),
        p99_ms: as_ms(hist.value_at_quantile(0.99)),
        min_ms: as_ms(hist.min()),
        max_ms: as_ms(hist.max()),
        mean_ms: (hist.mean() / MICROS_PER_MILLI) as f32,
        std_dev_ms: hist.stdev() / MICROS_PER_MILLI,
        sample_count: hist.len(),
        bucket_counts: histogram_buckets(hist),
    }
}

/// Aggregates request outcomes into fixed time windows for one test run.
pub struct WindowedCollector {
    worker_id: String,
    test_run_id: String,
    window: Duration,
    window_start_ms: i64,
    histogram: Histogram<u64>,
    total: u64,
    success: u64,
    failure: u64,
    bytes_received: u64,
    active_vus: u32,
    errors: HashMap<i32, (u64, Option<String>)>,
    batch_seq: u64,
}

impl WindowedCollector {
    /// Create a collector for `test_run_id` with the given window length.
    pub fn new(
        worker_id: impl Into<String>,
        test_run_id: impl Into<String>,
        window: Duration,
    ) -> Self {
        Self {
            worker_id: worker_id.into(),
            test_run_id: test_run_id.into(),
            window,
            window_start_ms: now_unix_ms(),
            histogram: new_histogram(),
            total: 0,
            success: 0,
            failure: 0,
            bytes_received: 0,
            active_vus: 0,
            errors: HashMap::new(),
            batch_seq: 0,
        }
    }

    /// The configured window length.
    pub fn window(&self) -> Duration {
        self.window
    }

    /// Requests accumulated in the current (open) window.
    pub fn pending_requests(&self) -> u64 {
        self.total
    }

    /// Report the current number of active virtual users for the next batch.
    pub fn set_active_vus(&mut self, active_vus: u32) {
        self.active_vus = active_vus;
    }

    /// Fold a single request outcome into the open window.
    pub fn record(&mut self, metric: &RequestMetric) {
        self.total += 1;
        if metric.is_error() {
            self.failure += 1;
            let class = classify_error(metric.status_code);
            let entry = self.errors.entry(class as i32).or_insert((0, None));
            entry.0 += 1;
        } else {
            self.success += 1;
        }
        let latency = u64::try_from(metric.latency_micros).unwrap_or(u64::MAX);
        self.histogram.saturating_record(latency);
        self.bytes_received += metric.bytes_received;
    }

    /// Close the open window and produce a batch, or `None` if it is empty.
    ///
    /// Also used to flush a partial window on stop.
    pub fn take_batch(&mut self) -> Option<MetricBatch> {
        if self.total == 0 {
            return None;
        }
        let window_secs = self.window.as_secs_f64().max(f64::EPSILON);
        let rps = self.total as f64 / window_secs;
        let errors = self
            .errors
            .iter()
            .map(|(class, (count, detail))| ErrorBreakdown {
                error_class: *class,
                count: *count,
                detail: detail.clone().unwrap_or_default(),
            })
            .collect();

        let batch = MetricBatch {
            worker_id: self.worker_id.clone(),
            test_run_id: self.test_run_id.clone(),
            batch_id: self.batch_seq.to_string(),
            window_start_unix_ms: self.window_start_ms,
            window_duration_ms: self.window.as_millis() as u32,
            total_requests: self.total,
            success_count: self.success,
            failure_count: self.failure,
            bytes_received: self.bytes_received,
            bytes_sent: 0,
            active_vus: self.active_vus,
            latency: Some(latency_summary(&self.histogram)),
            errors,
            requests_per_second: rps,
        };

        // Reset for the next window.
        self.histogram = new_histogram();
        self.total = 0;
        self.success = 0;
        self.failure = 0;
        self.bytes_received = 0;
        self.errors.clear();
        self.window_start_ms = now_unix_ms();
        self.batch_seq += 1;

        Some(batch)
    }
}

/// Drive the client-streaming `StreamMetrics` RPC.
///
/// Sends every batch received from `rx` and returns the master's final
/// acknowledgment once the sender is dropped (half-close).
pub async fn upload_metrics(
    client: &mut TelemetryServiceClient<Channel>,
    rx: mpsc::Receiver<MetricBatch>,
) -> WorkerResult<StreamAcknowledgment> {
    let outbound = ReceiverStream::new(rx);
    let response = client.stream_metrics(outbound).await?;
    Ok(response.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::RequestMetric;

    fn ok(latency_us: u128) -> RequestMetric {
        RequestMetric::builder(latency_us, None).status(200).build()
    }

    fn failed(status: u16) -> RequestMetric {
        RequestMetric::builder(1_000, None).status(status).build()
    }

    #[test]
    fn empty_window_yields_no_batch() {
        let mut collector = WindowedCollector::new("w1", "run1", Duration::from_millis(10));
        assert!(collector.take_batch().is_none());
    }

    #[test]
    fn aggregates_success_and_failure_counts() {
        let mut collector = WindowedCollector::new("w1", "run1", Duration::from_secs(1));
        collector.set_active_vus(4);
        collector.record(&ok(1_000));
        collector.record(&ok(2_000));
        collector.record(&failed(500));
        collector.record(&failed(503));

        let batch = collector.take_batch().unwrap();
        assert_eq!(batch.worker_id, "w1");
        assert_eq!(batch.test_run_id, "run1");
        assert_eq!(batch.batch_id, "0");
        assert_eq!(batch.total_requests, 4);
        assert_eq!(batch.success_count, 2);
        assert_eq!(batch.failure_count, 2);
        assert_eq!(batch.active_vus, 4);
        assert_eq!(batch.window_duration_ms, 1000);
        assert!(batch.requests_per_second > 0.0);

        // Both failures are 5xx.
        assert_eq!(batch.errors.len(), 1);
        assert_eq!(batch.errors[0].error_class, ErrorClass::Http5xx as i32);
        assert_eq!(batch.errors[0].count, 2);
    }

    #[test]
    fn histogram_buckets_are_fixed_width_and_sum_to_total() {
        let mut collector = WindowedCollector::new("w1", "run1", Duration::from_secs(1));
        collector.record(&ok(500)); // <1ms
        collector.record(&ok(3_000)); // 1-5ms
        collector.record(&ok(2_000_000)); // >1000ms

        let batch = collector.take_batch().unwrap();
        let latency = batch.latency.unwrap();
        assert_eq!(latency.bucket_counts.len(), HISTOGRAM_BUCKET_ORDER.len());
        assert_eq!(latency.bucket_counts.iter().sum::<u64>(), 3);
        assert_eq!(latency.sample_count, 3);
        assert!(latency.p99_ms >= latency.p50_ms);
    }

    #[test]
    fn take_batch_resets_the_window_and_increments_seq() {
        let mut collector = WindowedCollector::new("w1", "run1", Duration::from_secs(1));
        collector.record(&ok(1_000));
        let first = collector.take_batch().unwrap();
        assert_eq!(first.batch_id, "0");
        assert_eq!(collector.pending_requests(), 0);
        assert!(collector.take_batch().is_none());

        collector.record(&ok(1_000));
        let second = collector.take_batch().unwrap();
        assert_eq!(second.batch_id, "1");
        assert_eq!(second.total_requests, 1);
    }

    #[test]
    fn classifies_4xx_and_transport_errors() {
        let mut collector = WindowedCollector::new("w1", "run1", Duration::from_secs(1));
        collector.record(&failed(404));
        collector.record(&failed(0));
        let batch = collector.take_batch().unwrap();
        let classes: HashMap<i32, u64> = batch
            .errors
            .iter()
            .map(|e| (e.error_class, e.count))
            .collect();
        assert_eq!(classes.get(&(ErrorClass::Http4xx as i32)), Some(&1));
        assert_eq!(classes.get(&(ErrorClass::Other as i32)), Some(&1));
    }
}
