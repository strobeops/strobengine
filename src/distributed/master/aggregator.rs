//! Real-time telemetry aggregation across workers.
//!
//! Workers stream fixed-width, index-aligned latency histograms (see the
//! `LatencySummary.bucket_counts` contract), so the master merges distributions
//! by element-wise summation and derives cluster-wide percentiles from the
//! merged buckets.

use std::collections::HashMap;

use crate::distributed::proto::{ErrorClass, MetricBatch};

/// Upper bounds (ms) of the engine's fixed 10 latency buckets, matching
/// `metrics::HISTOGRAM_BUCKET_ORDER`:
/// `<1, 1-5, 5-10, 10-25, 25-50, 50-100, 100-250, 250-500, 500-1000, >1000`.
pub const BUCKET_BOUNDS_MS: [f64; 11] = [
    0.0,
    1.0,
    5.0,
    10.0,
    25.0,
    50.0,
    100.0,
    250.0,
    500.0,
    1_000.0,
    f64::INFINITY,
];

use super::reporting::{ErrorClassCount, GlobalLatency, GlobalReport};

/// Merges `MetricBatch` windows from every worker into one run aggregate.
#[derive(Debug)]
pub struct RunAggregator {
    test_run_id: String,
    bucket_count: usize,
    workers: Vec<String>,
    batches: u64,
    total_requests: u64,
    success_count: u64,
    failure_count: u64,
    bytes_received: u64,
    buckets: Vec<u64>,
    sample_count: u64,
    min_ms: Option<f64>,
    max_ms: Option<f64>,
    first_window_start_ms: Option<i64>,
    last_window_end_ms: Option<i64>,
    errors: HashMap<i32, u64>,
}

impl RunAggregator {
    /// Create an aggregator for `test_run_id` expecting `bucket_count` buckets.
    pub fn new(test_run_id: impl Into<String>, bucket_count: usize) -> Self {
        let bucket_count = bucket_count.max(1);
        Self {
            test_run_id: test_run_id.into(),
            bucket_count,
            workers: Vec::new(),
            batches: 0,
            total_requests: 0,
            success_count: 0,
            failure_count: 0,
            bytes_received: 0,
            buckets: vec![0; bucket_count],
            sample_count: 0,
            min_ms: None,
            max_ms: None,
            first_window_start_ms: None,
            last_window_end_ms: None,
            errors: HashMap::new(),
        }
    }

    /// Ingest one telemetry window.
    pub fn ingest(&mut self, batch: &MetricBatch) {
        self.batches += 1;
        if !self.workers.contains(&batch.worker_id) && !batch.worker_id.is_empty() {
            self.workers.push(batch.worker_id.clone());
        }
        self.total_requests += batch.total_requests;
        self.success_count += batch.success_count;
        self.failure_count += batch.failure_count;
        self.bytes_received += batch.bytes_received;

        let start = batch.window_start_unix_ms;
        let end = start + i64::from(batch.window_duration_ms);
        self.first_window_start_ms = Some(
            self.first_window_start_ms
                .map_or(start, |current| current.min(start)),
        );
        self.last_window_end_ms = Some(
            self.last_window_end_ms
                .map_or(end, |current| current.max(end)),
        );

        if let Some(latency) = batch.latency.as_ref() {
            for (index, count) in latency.bucket_counts.iter().enumerate() {
                if index < self.bucket_count {
                    self.buckets[index] += count;
                }
            }
            self.sample_count += latency.sample_count;
            let min = f64::from(latency.min_ms);
            let max = f64::from(latency.max_ms);
            self.min_ms = Some(self.min_ms.map_or(min, |current| current.min(min)));
            self.max_ms = Some(self.max_ms.map_or(max, |current| current.max(max)));
        }

        for error in &batch.errors {
            *self.errors.entry(error.error_class).or_insert(0) += error.count;
        }
    }

    /// Number of workers that have contributed telemetry.
    pub fn worker_count(&self) -> usize {
        self.workers.len()
    }

    /// Total requests ingested so far.
    pub fn total_requests(&self) -> u64 {
        self.total_requests
    }

    /// Produce the final aggregated report.
    pub fn finalize(&mut self) -> GlobalReport {
        let mut workers = self.workers.clone();
        workers.sort();

        let duration_secs = match (self.first_window_start_ms, self.last_window_end_ms) {
            (Some(start), Some(end)) if end > start => (end - start) as f64 / 1_000.0,
            _ => 0.0,
        };
        let rps = if duration_secs > 0.0 {
            self.total_requests as f64 / duration_secs
        } else {
            0.0
        };
        let error_rate_percent = if self.total_requests > 0 {
            self.failure_count as f64 / self.total_requests as f64 * 100.0
        } else {
            0.0
        };

        let fallback_max = self.max_ms.unwrap_or(f64::INFINITY);
        let latency = GlobalLatency {
            p50_ms: approx_percentile(&self.buckets, 0.50, fallback_max),
            p90_ms: approx_percentile(&self.buckets, 0.90, fallback_max),
            p95_ms: approx_percentile(&self.buckets, 0.95, fallback_max),
            p99_ms: approx_percentile(&self.buckets, 0.99, fallback_max),
            min_ms: self.min_ms.unwrap_or(0.0),
            max_ms: self.max_ms.unwrap_or(0.0),
            sample_count: self.sample_count,
            bucket_counts: self.buckets.clone(),
        };

        let mut error_breakdown: Vec<ErrorClassCount> = self
            .errors
            .iter()
            .map(|(class, count)| ErrorClassCount {
                error_class: ErrorClass::try_from(*class)
                    .map(|c| format!("{c:?}"))
                    .unwrap_or_else(|_| format!("UNKNOWN({class})")),
                count: *count,
            })
            .collect();
        error_breakdown.sort_by(|a, b| {
            b.count
                .cmp(&a.count)
                .then(a.error_class.cmp(&b.error_class))
        });

        GlobalReport {
            test_run_id: self.test_run_id.clone(),
            worker_count: workers.len(),
            workers,
            batches: self.batches,
            total_requests: self.total_requests,
            success_count: self.success_count,
            failure_count: self.failure_count,
            error_rate_percent,
            bytes_received: self.bytes_received,
            rps,
            duration_secs,
            latency,
            error_breakdown,
            timeline: Vec::new(),
            worker_shares: Vec::new(),
        }
    }
}

/// Approximate a percentile (0..=1) from fixed buckets by linear interpolation
/// within the containing bucket. `fallback_max` bounds the open-ended last
/// bucket (the observed maximum, when known).
fn approx_percentile(buckets: &[u64], percentile: f64, fallback_max: f64) -> f64 {
    let total: u64 = buckets.iter().sum();
    if total == 0 {
        return 0.0;
    }
    let target = percentile * total as f64;
    let mut cumulative = 0u64;
    for (index, &count) in buckets.iter().enumerate() {
        if count == 0 {
            continue;
        }
        if cumulative as f64 + count as f64 >= target {
            let lo = BUCKET_BOUNDS_MS.get(index).copied().unwrap_or(0.0);
            let hi = BUCKET_BOUNDS_MS
                .get(index + 1)
                .copied()
                .unwrap_or(fallback_max);
            let hi = if hi.is_finite() {
                hi
            } else {
                fallback_max.max(lo)
            };
            let within = (target - cumulative as f64) / count as f64;
            return lo + (hi - lo) * within.clamp(0.0, 1.0);
        }
        cumulative += count;
    }
    fallback_max
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed::proto::{ErrorBreakdown, LatencySummary};

    fn batch(worker: &str, total: u64, success: u64, failure: u64, buckets: &[u64]) -> MetricBatch {
        MetricBatch {
            worker_id: worker.to_string(),
            test_run_id: "run-1".to_string(),
            window_start_unix_ms: 1_000,
            window_duration_ms: 1_000,
            total_requests: total,
            success_count: success,
            failure_count: failure,
            latency: Some(LatencySummary {
                min_ms: 1.0,
                max_ms: 30.0,
                sample_count: total,
                bucket_counts: buckets.to_vec(),
                ..Default::default()
            }),
            errors: vec![ErrorBreakdown {
                error_class: ErrorClass::Http5xx as i32,
                count: failure,
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    #[test]
    fn merges_totals_buckets_and_errors() {
        let mut agg = RunAggregator::new("run-1", 10);
        // Two workers, each with 3 requests in the first bucket (1-5ms keep is
        // bucket index 1) and 1 in the ninth bucket.
        agg.ingest(&batch("w1", 4, 3, 1, &[0, 3, 0, 0, 0, 0, 0, 0, 1, 0]));
        agg.ingest(&batch("w2", 4, 4, 0, &[0, 1, 0, 0, 0, 0, 0, 0, 0, 0]));

        assert_eq!(agg.worker_count(), 2);
        assert_eq!(agg.total_requests(), 8);

        let report = agg.finalize();
        assert_eq!(report.test_run_id, "run-1");
        assert_eq!(report.worker_count, 2);
        assert_eq!(report.workers, vec!["w1".to_string(), "w2".to_string()]);
        assert_eq!(report.total_requests, 8);
        assert_eq!(report.success_count, 7);
        assert_eq!(report.failure_count, 1);
        assert_eq!(report.error_rate_percent, 12.5);
        assert_eq!(report.latency.bucket_counts[1], 4);
        assert_eq!(report.latency.bucket_counts[8], 1);
        assert_eq!(report.latency.bucket_counts.iter().sum::<u64>(), 5);
        assert!(report.latency.p50_ms <= report.latency.p95_ms);
        assert!(report.latency.p95_ms <= report.latency.p99_ms);
        assert_eq!(report.latency.min_ms, 1.0);
        assert_eq!(report.latency.max_ms, 30.0);
        assert_eq!(report.error_breakdown.len(), 1);
        assert_eq!(report.error_breakdown[0].count, 1);
    }

    #[test]
    fn empty_aggregator_reports_zeros() {
        let mut agg = RunAggregator::new("run-empty", 10);
        let report = agg.finalize();
        assert_eq!(report.total_requests, 0);
        assert_eq!(report.rps, 0.0);
        assert_eq!(report.latency.bucket_counts.len(), 10);
    }

    #[test]
    fn percentile_interpolation_is_sane() {
        // Everything in the 50-100ms bucket -> percentiles interpolate inside it.
        let mut buckets = vec![0u64; 10];
        buckets[5] = 100; // 50-100ms
        let p50 = approx_percentile(&buckets, 0.50, f64::INFINITY);
        let p99 = approx_percentile(&buckets, 0.99, f64::INFINITY);
        assert!((50.0..=100.0).contains(&p50), "p50={p50}");
        assert!((50.0..=100.0).contains(&p99), "p99={p99}");
        assert!(p99 >= p50);
    }
}
