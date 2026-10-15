//! Live dashboard pub/sub: broadcasts real-time cluster metrics.
//!
//! A [`LiveBroadcaster`] fans out [`LiveSnapshot`]s to any number of
//! subscribers (a CLI progress bar, a TUI, or an external monitoring sink).
//! Lagging subscribers drop old snapshots rather than blocking the pipeline.

use serde::Serialize;
use tokio::sync::broadcast;

/// Default number of snapshots buffered per subscriber.
pub const DEFAULT_LIVE_CAPACITY: usize = 64;

/// Real-time, cluster-wide view of an active run.
#[derive(Debug, Clone, Serialize)]
pub struct LiveSnapshot {
    /// Run identifier.
    pub run_id: String,
    /// Current epoch start, Unix epoch milliseconds.
    pub epoch_ms: i64,
    /// Elapsed seconds since the run started.
    pub elapsed_secs: f64,
    /// Length of the sliding window this snapshot summarises, in seconds.
    pub window_secs: f64,
    /// Requests per second over the sliding window.
    pub rps: f64,
    /// Error rate over the sliding window, percent.
    pub error_rate_percent: f64,
    /// Approximate 50th percentile latency over the window, milliseconds.
    pub p50_ms: f64,
    /// Approximate 90th percentile latency over the window, milliseconds.
    pub p90_ms: f64,
    /// Approximate 95th percentile latency over the window, milliseconds.
    pub p95_ms: f64,
    /// Approximate 99th percentile latency over the window, milliseconds.
    pub p99_ms: f64,
    /// Cumulative requests observed so far.
    pub total_requests: u64,
    /// Workers that have contributed telemetry.
    pub active_workers: usize,
}

/// Fans out live snapshots to subscribers.
#[derive(Clone)]
pub struct LiveBroadcaster {
    tx: broadcast::Sender<LiveSnapshot>,
}

impl Default for LiveBroadcaster {
    fn default() -> Self {
        Self::new(DEFAULT_LIVE_CAPACITY)
    }
}

impl LiveBroadcaster {
    /// Create a broadcaster with the given per-subscriber buffer capacity.
    pub fn new(capacity: usize) -> Self {
        let (tx, _rx) = broadcast::channel(capacity.max(1));
        Self { tx }
    }

    /// Publish a snapshot; ignored when there are no subscribers.
    pub fn publish(&self, snapshot: LiveSnapshot) {
        let _ = self.tx.send(snapshot);
    }

    /// Subscribe to future snapshots.
    pub fn subscribe(&self) -> broadcast::Receiver<LiveSnapshot> {
        self.tx.subscribe()
    }

    /// Number of active subscribers.
    pub fn subscriber_count(&self) -> usize {
        self.tx.receiver_count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot(rps: f64) -> LiveSnapshot {
        LiveSnapshot {
            run_id: "run-1".into(),
            epoch_ms: 1_000,
            elapsed_secs: 1.0,
            window_secs: 5.0,
            rps,
            error_rate_percent: 0.0,
            p50_ms: 1.0,
            p90_ms: 2.0,
            p95_ms: 3.0,
            p99_ms: 4.0,
            total_requests: 10,
            active_workers: 1,
        }
    }

    #[tokio::test]
    async fn subscribers_receive_published_snapshots() {
        let broadcaster = LiveBroadcaster::new(8);
        let mut rx = broadcaster.subscribe();
        assert_eq!(broadcaster.subscriber_count(), 1);

        broadcaster.publish(snapshot(42.0));
        let received = rx.recv().await.unwrap();
        assert_eq!(received.rps, 42.0);
        assert_eq!(received.run_id, "run-1");
    }

    #[tokio::test]
    async fn lagging_subscribers_drop_old_snapshots() {
        let broadcaster = LiveBroadcaster::new(2);
        let mut rx = broadcaster.subscribe();
        for i in 0..5 {
            broadcaster.publish(snapshot(i as f64));
        }
        // The oldest snapshots are dropped rather than blocking the producer.
        match rx.recv().await {
            Err(broadcast::error::RecvError::Lagged(_)) => {}
            other => panic!("expected lagged, got {other:?}"),
        }
    }
}
