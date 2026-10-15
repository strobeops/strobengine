//! Real-time windowed telemetry pipeline.
//!
//! [`TelemetryPipeline`] merges per-second `MetricBatch` windows from every
//! worker into epoch buckets. [`PipelineHandle`] runs the pipeline on a
//! dedicated task: gRPC handlers push batches onto an unbounded channel (no
//! shared lock on the ingest path), the task merges them and publishes a
//! [`LiveSnapshot`] once per epoch, and `finalize` returns the rich
//! [`GlobalReport`] (totals, merged latency, per-epoch timeline, per-worker
//! shares).

use std::collections::{BTreeMap, HashMap};

use tokio::sync::{mpsc, oneshot};

use crate::distributed::proto::{ErrorClass, MetricBatch};

use super::dashboard::{LiveBroadcaster, LiveSnapshot};
use super::reporting::{ErrorClassCount, GlobalLatency, GlobalReport, TimelinePoint, WorkerShare};

/// Length of one telemetry epoch, in milliseconds.
pub const EPOCH_MS: i64 = 1_000;

/// Default sliding-window length used for live snapshots, in epochs (seconds).
pub const DEFAULT_WINDOW_EPOCHS: i64 = 5;

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

/// Mutable metric accumulator; shared by epochs, per-worker tallies, sliding
/// windows and the cumulative total.
#[derive(Debug, Clone, Default)]
struct Metrics {
    total: u64,
    success: u64,
    failure: u64,
    bytes: u64,
    buckets: Vec<u64>,
    sample_count: u64,
    min_ms: Option<f64>,
    max_ms: Option<f64>,
    errors: HashMap<i32, u64>,
}

impl Metrics {
    fn new(bucket_count: usize) -> Self {
        Self {
            buckets: vec![0; bucket_count],
            ..Default::default()
        }
    }

    fn merge_batch(&mut self, batch: &MetricBatch, bucket_count: usize) {
        self.total += batch.total_requests;
        self.success += batch.success_count;
        self.failure += batch.failure_count;
        self.bytes += batch.bytes_received;
        if let Some(latency) = batch.latency.as_ref() {
            for (index, count) in latency.bucket_counts.iter().enumerate() {
                if index < bucket_count {
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

    fn merge(&mut self, other: &Metrics) {
        self.total += other.total;
        self.success += other.success;
        self.failure += other.failure;
        self.bytes += other.bytes;
        for (index, count) in other.buckets.iter().enumerate() {
            if index < self.buckets.len() {
                self.buckets[index] += count;
            }
        }
        self.sample_count += other.sample_count;
        if let Some(min) = other.min_ms {
            self.min_ms = Some(self.min_ms.map_or(min, |current| current.min(min)));
        }
        if let Some(max) = other.max_ms {
            self.max_ms = Some(self.max_ms.map_or(max, |current| current.max(max)));
        }
        for (class, count) in &other.errors {
            *self.errors.entry(*class).or_insert(0) += count;
        }
    }

    fn error_rate_percent(&self) -> f64 {
        if self.total > 0 {
            self.failure as f64 / self.total as f64 * 100.0
        } else {
            0.0
        }
    }

    fn latency(&self) -> GlobalLatency {
        let fallback_max = self.max_ms.unwrap_or(f64::INFINITY);
        GlobalLatency {
            p50_ms: approx_percentile(&self.buckets, 0.50, fallback_max),
            p90_ms: approx_percentile(&self.buckets, 0.90, fallback_max),
            p95_ms: approx_percentile(&self.buckets, 0.95, fallback_max),
            p99_ms: approx_percentile(&self.buckets, 0.99, fallback_max),
            min_ms: self.min_ms.unwrap_or(0.0),
            max_ms: self.max_ms.unwrap_or(0.0),
            sample_count: self.sample_count,
            bucket_counts: self.buckets.clone(),
        }
    }
}

fn epoch_of(unix_ms: i64) -> i64 {
    unix_ms - unix_ms.rem_euclid(EPOCH_MS)
}

/// Aggregates rolling worker telemetry into epochs, windows and a final report.
#[derive(Debug)]
pub struct TelemetryPipeline {
    run_id: String,
    bucket_count: usize,
    window_epochs: i64,
    epochs: BTreeMap<i64, Metrics>,
    workers: HashMap<String, Metrics>,
    started_at_ms: Option<i64>,
    last_published_epoch: Option<i64>,
    cumulative: Metrics,
    batches: u64,
}

impl TelemetryPipeline {
    /// Create a pipeline for `run_id` with `bucket_count` latency buckets and a
    /// sliding window of `window_epochs` seconds.
    pub fn new(run_id: impl Into<String>, bucket_count: usize, window_epochs: i64) -> Self {
        let bucket_count = bucket_count.max(1);
        Self {
            run_id: run_id.into(),
            bucket_count,
            window_epochs: window_epochs.max(1),
            epochs: BTreeMap::new(),
            workers: HashMap::new(),
            started_at_ms: None,
            last_published_epoch: None,
            cumulative: Metrics::new(bucket_count),
            batches: 0,
        }
    }

    /// Ingest one telemetry window. Returns a [`LiveSnapshot`] when the current
    /// epoch advanced since the last publish (throttling the broadcast).
    pub fn ingest(&mut self, batch: &MetricBatch) -> Option<LiveSnapshot> {
        let epoch = epoch_of(batch.window_start_unix_ms);
        if self.started_at_ms.is_none() {
            self.started_at_ms = Some(epoch);
        }
        self.epochs
            .entry(epoch)
            .or_insert_with(|| Metrics::new(self.bucket_count))
            .merge_batch(batch, self.bucket_count);
        if !batch.worker_id.is_empty() {
            self.workers
                .entry(batch.worker_id.clone())
                .or_insert_with(|| Metrics::new(self.bucket_count))
                .merge_batch(batch, self.bucket_count);
        }
        self.cumulative.merge_batch(batch, self.bucket_count);
        self.batches += 1;

        if self.last_published_epoch != Some(epoch) {
            self.last_published_epoch = Some(epoch);
            Some(self.snapshot(epoch))
        } else {
            None
        }
    }

    /// Build a snapshot over the trailing sliding window ending at `epoch`.
    fn snapshot(&self, epoch: i64) -> LiveSnapshot {
        let window = self.window_metrics(epoch);
        let window_secs = self.window_epochs as f64 * (EPOCH_MS as f64 / 1_000.0);
        let rps = if window_secs > 0.0 {
            window.total as f64 / window_secs
        } else {
            0.0
        };
        let latency = window.latency();
        LiveSnapshot {
            run_id: self.run_id.clone(),
            epoch_ms: epoch,
            elapsed_secs: self.elapsed_secs(epoch),
            window_secs,
            rps,
            error_rate_percent: window.error_rate_percent(),
            p50_ms: latency.p50_ms,
            p90_ms: latency.p90_ms,
            p95_ms: latency.p95_ms,
            p99_ms: latency.p99_ms,
            total_requests: self.cumulative.total,
            active_workers: self.workers.len(),
        }
    }

    fn window_metrics(&self, epoch: i64) -> Metrics {
        let mut window = Metrics::new(self.bucket_count);
        let start = epoch - (self.window_epochs - 1) * EPOCH_MS;
        for bucket in self.epochs.range(start..=epoch).map(|(_, m)| m) {
            window.merge(bucket);
        }
        window
    }

    fn elapsed_secs(&self, epoch: i64) -> f64 {
        match self.started_at_ms {
            Some(start) if epoch >= start => (epoch - start) as f64 / 1_000.0,
            _ => 0.0,
        }
    }

    /// Produce the final aggregated report, including the per-epoch timeline and
    /// per-worker shares.
    pub fn finalize(&self) -> GlobalReport {
        let latency = self.cumulative.latency();
        let duration_secs = match (self.started_at_ms, self.epochs.keys().next_back()) {
            (Some(start), Some(&last)) if last >= start => {
                (last - start + EPOCH_MS) as f64 / 1_000.0
            }
            _ => 0.0,
        };
        let rps = if duration_secs > 0.0 {
            self.cumulative.total as f64 / duration_secs
        } else {
            0.0
        };

        let timeline = self.timeline();
        let worker_shares = self.worker_shares(duration_secs);

        let mut workers: Vec<String> = self.workers.keys().cloned().collect();
        workers.sort();

        let mut error_breakdown: Vec<ErrorClassCount> = self
            .cumulative
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
            test_run_id: self.run_id.clone(),
            worker_count: workers.len(),
            workers,
            batches: self.batches,
            total_requests: self.cumulative.total,
            success_count: self.cumulative.success,
            failure_count: self.cumulative.failure,
            error_rate_percent: self.cumulative.error_rate_percent(),
            bytes_received: self.cumulative.bytes,
            rps,
            duration_secs,
            latency,
            error_breakdown,
            timeline,
            worker_shares,
        }
    }

    fn timeline(&self) -> Vec<TimelinePoint> {
        let epoch_secs = EPOCH_MS as f64 / 1_000.0;
        self.epochs
            .iter()
            .map(|(epoch, metrics)| {
                let latency = metrics.latency();
                TimelinePoint {
                    elapsed_secs: self.elapsed_secs(*epoch),
                    epoch_ms: *epoch,
                    rps: if epoch_secs > 0.0 {
                        metrics.total as f64 / epoch_secs
                    } else {
                        0.0
                    },
                    error_rate_percent: metrics.error_rate_percent(),
                    p50_ms: latency.p50_ms,
                    p95_ms: latency.p95_ms,
                    p99_ms: latency.p99_ms,
                }
            })
            .collect()
    }

    fn worker_shares(&self, duration_secs: f64) -> Vec<WorkerShare> {
        let total_all = self.cumulative.total;
        let mut shares: Vec<WorkerShare> = self
            .workers
            .iter()
            .map(|(worker_id, metrics)| {
                let latency = metrics.latency();
                WorkerShare {
                    worker_id: worker_id.clone(),
                    total_requests: metrics.total,
                    success_count: metrics.success,
                    failure_count: metrics.failure,
                    error_rate_percent: metrics.error_rate_percent(),
                    rps: if duration_secs > 0.0 {
                        metrics.total as f64 / duration_secs
                    } else {
                        0.0
                    },
                    share_percent: if total_all > 0 {
                        metrics.total as f64 / total_all as f64 * 100.0
                    } else {
                        0.0
                    },
                    p95_ms: latency.p95_ms,
                }
            })
            .collect();
        shares.sort_by(|a, b| {
            b.total_requests
                .cmp(&a.total_requests)
                .then(a.worker_id.cmp(&b.worker_id))
        });
        shares
    }
}

/// Approximate a percentile (0..=1) from fixed buckets by linear interpolation
/// within the containing bucket. `fallback_max` bounds the open-ended last
/// bucket (the observed maximum, when known).
pub(crate) fn approx_percentile(buckets: &[u64], percentile: f64, fallback_max: f64) -> f64 {
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

enum PipelineCommand {
    Batch(Box<MetricBatch>),
    Finalize(oneshot::Sender<GlobalReport>),
    Shutdown,
}

/// Cheap, cloneable handle to a running [`TelemetryPipeline`] task.
#[derive(Clone)]
pub struct PipelineHandle {
    tx: mpsc::UnboundedSender<PipelineCommand>,
    broadcaster: LiveBroadcaster,
}

impl PipelineHandle {
    /// Spawn the pipeline on its own task, publishing snapshots to `broadcaster`.
    pub fn spawn(
        run_id: impl Into<String>,
        bucket_count: usize,
        window_epochs: i64,
        broadcaster: LiveBroadcaster,
    ) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let pipeline = TelemetryPipeline::new(run_id, bucket_count, window_epochs);
        let publish = broadcaster.clone();
        tokio::spawn(async move {
            run_pipeline(pipeline, rx, publish).await;
        });
        Self { tx, broadcaster }
    }

    /// Enqueue a batch for aggregation (non-blocking). Returns false if the
    /// pipeline has stopped.
    pub fn ingest(&self, batch: MetricBatch) -> bool {
        self.tx
            .send(PipelineCommand::Batch(Box::new(batch)))
            .is_ok()
    }

    /// Subscribe to this run's live snapshots.
    pub fn subscribe(&self) -> tokio::sync::broadcast::Receiver<LiveSnapshot> {
        self.broadcaster.subscribe()
    }

    /// Ask the pipeline to produce the final report. Returns `None` if it has
    /// already stopped.
    pub async fn finalize(&self) -> Option<GlobalReport> {
        let (tx, rx) = oneshot::channel();
        if self.tx.send(PipelineCommand::Finalize(tx)).is_err() {
            return None;
        }
        rx.await.ok()
    }

    /// Stop the pipeline task.
    pub fn shutdown(&self) {
        let _ = self.tx.send(PipelineCommand::Shutdown);
    }
}

async fn run_pipeline(
    mut pipeline: TelemetryPipeline,
    mut rx: mpsc::UnboundedReceiver<PipelineCommand>,
    broadcaster: LiveBroadcaster,
) {
    while let Some(command) = rx.recv().await {
        match command {
            PipelineCommand::Batch(batch) => {
                if let Some(snapshot) = pipeline.ingest(&batch) {
                    broadcaster.publish(snapshot);
                }
            }
            PipelineCommand::Finalize(reply) => {
                let _ = reply.send(pipeline.finalize());
            }
            PipelineCommand::Shutdown => break,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed::proto::{ErrorBreakdown, LatencySummary};

    fn batch_at(
        worker: &str,
        epoch_ms: i64,
        total: u64,
        failure: u64,
        buckets: &[u64],
    ) -> MetricBatch {
        MetricBatch {
            worker_id: worker.to_string(),
            test_run_id: "run-1".to_string(),
            window_start_unix_ms: epoch_ms,
            window_duration_ms: 1_000,
            total_requests: total,
            success_count: total - failure,
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
    fn publishes_once_per_epoch_and_uses_sliding_window() {
        let mut pipeline = TelemetryPipeline::new("run-1", 10, 2);
        // Two epochs back-to-back: publish on each epoch transition only.
        assert!(
            pipeline
                .ingest(&batch_at(
                    "w1",
                    10_000,
                    10,
                    0,
                    &[0, 10, 0, 0, 0, 0, 0, 0, 0, 0]
                ))
                .is_some()
        );
        assert!(
            pipeline
                .ingest(&batch_at(
                    "w1",
                    10_500,
                    10,
                    0,
                    &[0, 10, 0, 0, 0, 0, 0, 0, 0, 0]
                ))
                .is_none()
        );
        let snap = pipeline
            .ingest(&batch_at(
                "w1",
                11_000,
                20,
                0,
                &[0, 20, 0, 0, 0, 0, 0, 0, 0, 0],
            ))
            .expect("new epoch publishes");
        // Epoch 10_000 holds 10 + 10 = 20 requests; the 2-epoch window
        // (10_000 + 11_000) = 40 requests over 2s = 20 rps.
        assert_eq!(snap.epoch_ms, 11_000);
        assert_eq!(snap.window_secs, 2.0);
        assert!((snap.rps - 20.0).abs() < 1e-9, "rps={}", snap.rps);
        assert_eq!(snap.total_requests, 40);
        assert_eq!(snap.active_workers, 1);
    }

    #[test]
    fn sliding_window_ignores_old_epochs() {
        let mut pipeline = TelemetryPipeline::new("run-1", 10, 2);
        pipeline.ingest(&batch_at(
            "w1",
            100_000,
            5,
            0,
            &[5, 0, 0, 0, 0, 0, 0, 0, 0, 0],
        ));
        pipeline.ingest(&batch_at(
            "w1",
            104_000,
            5,
            0,
            &[5, 0, 0, 0, 0, 0, 0, 0, 0, 0],
        ));
        let snap = pipeline
            .ingest(&batch_at(
                "w1",
                110_000,
                5,
                0,
                &[5, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            ))
            .unwrap();
        // Only the 110_000 epoch is within a 2-epoch window ending at 110_000.
        assert!((snap.rps - 2.5).abs() < 1e-9, "rps={}", snap.rps);
    }

    #[test]
    fn finalize_builds_timeline_and_worker_shares() {
        let mut pipeline = TelemetryPipeline::new("run-1", 10, 5);
        pipeline.ingest(&batch_at(
            "w1",
            1_000,
            10,
            1,
            &[0, 10, 0, 0, 0, 0, 0, 0, 0, 0],
        ));
        pipeline.ingest(&batch_at(
            "w2",
            1_000,
            30,
            0,
            &[0, 0, 30, 0, 0, 0, 0, 0, 0, 0],
        ));
        pipeline.ingest(&batch_at(
            "w1",
            2_000,
            10,
            0,
            &[0, 10, 0, 0, 0, 0, 0, 0, 0, 0],
        ));

        let report = pipeline.finalize();
        assert_eq!(report.test_run_id, "run-1");
        assert_eq!(report.total_requests, 50);
        assert_eq!(report.success_count, 49);
        assert_eq!(report.failure_count, 1);
        assert_eq!(report.worker_count, 2);
        assert_eq!(report.workers, vec!["w1".to_string(), "w2".to_string()]);

        // Timeline has one point per epoch, ordered.
        assert_eq!(report.timeline.len(), 2);
        assert_eq!(report.timeline[0].epoch_ms, 1_000);
        assert_eq!(report.timeline[1].epoch_ms, 2_000);
        assert_eq!(report.timeline[0].rps, 40.0);

        // w2 has 30/50 = 60% share, w1 has 20/50 = 40%.
        assert_eq!(report.worker_shares[0].worker_id, "w2");
        assert!((report.worker_shares[0].share_percent - 60.0).abs() < 1e-9);
        assert_eq!(report.worker_shares[1].worker_id, "w1");
        assert!((report.worker_shares[1].share_percent - 40.0).abs() < 1e-9);

        assert_eq!(report.error_breakdown.len(), 1);
        assert_eq!(report.error_breakdown[0].count, 1);
        assert_eq!(report.latency.bucket_counts.iter().sum::<u64>(), 50);
    }

    #[tokio::test]
    async fn handle_forwards_batches_and_finalizes() {
        let broadcaster = LiveBroadcaster::new(8);
        let mut live = broadcaster.subscribe();
        let handle = PipelineHandle::spawn("run-1", 10, 5, broadcaster);

        assert!(handle.ingest(batch_at("w1", 1_000, 7, 0, &[0, 7, 0, 0, 0, 0, 0, 0, 0, 0])));
        let snap = live.recv().await.unwrap();
        assert_eq!(snap.total_requests, 7);

        let report = handle.finalize().await.expect("finalize returns");
        assert_eq!(report.total_requests, 7);
        assert_eq!(report.timeline.len(), 1);
        handle.shutdown();
    }
}
