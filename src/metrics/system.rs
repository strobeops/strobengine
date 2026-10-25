use std::time::{Duration, Instant};

use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use sysinfo::ProcessesToUpdate;
use tokio::sync::mpsc::{self, error::TrySendError};
use tokio_util::sync::CancellationToken;

/// A single resource measurement snapshot.
#[pyclass(from_py_object)]
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ResourceSample {
    #[pyo3(get)]
    pub timestamp_us: u64,
    #[pyo3(get)]
    pub cpu_usage_percent: f32,
    #[pyo3(get)]
    pub memory_rss_bytes: u64,
    #[pyo3(get)]
    pub thread_count: usize,
    #[pyo3(get)]
    pub open_fds: Option<usize>,
}

/// Aggregated system resource metrics computed from a series of samples.
#[pyclass(skip_from_py_object)]
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct SystemMetrics {
    #[pyo3(get)]
    pub peak_cpu_percent: f32,
    #[pyo3(get)]
    pub avg_cpu_percent: f32,
    #[pyo3(get)]
    pub peak_memory_rss_bytes: u64,
    #[pyo3(get)]
    pub avg_memory_rss_bytes: u64,
    #[pyo3(get)]
    pub peak_thread_count: usize,
    #[pyo3(get)]
    pub time_series: Vec<ResourceSample>,
}

impl SystemMetrics {
    pub fn from_samples(samples: &[ResourceSample]) -> Self {
        if samples.is_empty() {
            return Self::default();
        }

        // Filter out non-finite CPU values to avoid NaN propagation
        let valid_cpu: Vec<f32> = samples
            .iter()
            .map(|s| s.cpu_usage_percent)
            .filter(|c| c.is_finite())
            .collect();

        let peak_cpu = valid_cpu.iter().copied().fold(0.0f32, f32::max);
        let avg_cpu = if valid_cpu.is_empty() {
            0.0
        } else {
            valid_cpu.iter().sum::<f32>() / valid_cpu.len() as f32
        };

        let peak_memory = samples
            .iter()
            .map(|s| s.memory_rss_bytes)
            .max()
            .unwrap_or(0);
        let avg_memory =
            samples.iter().map(|s| s.memory_rss_bytes).sum::<u64>() / samples.len() as u64;
        let peak_threads = samples.iter().map(|s| s.thread_count).max().unwrap_or(0);

        Self {
            peak_cpu_percent: peak_cpu,
            avg_cpu_percent: avg_cpu,
            peak_memory_rss_bytes: peak_memory,
            avg_memory_rss_bytes: avg_memory,
            peak_thread_count: peak_threads,
            time_series: samples.to_vec(),
        }
    }
}

/// Handle to a running resource monitor thread.
pub struct SamplerHandle {
    pub cancel: CancellationToken,
}

impl Drop for SamplerHandle {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

/// Cancellation poll interval while sleeping between samples.
const CANCEL_POLL: Duration = Duration::from_millis(50);

/// Upper bound for the sampling interval (24 hours).
const MAX_SAMPLE_INTERVAL_MS: u64 = 86_400_000;

/// Sleep for `duration`, waking early when `cancel` fires.
///
/// Returns `true` when cancellation was observed.
fn sleep_until_cancelled(cancel: &CancellationToken, duration: Duration) -> bool {
    let Some(deadline) = Instant::now().checked_add(duration) else {
        return cancel.is_cancelled();
    };
    loop {
        if cancel.is_cancelled() {
            return true;
        }
        let now = Instant::now();
        if now >= deadline {
            return false;
        }
        std::thread::sleep(deadline.saturating_duration_since(now).min(CANCEL_POLL));
    }
}

/// Start a background resource sampler.
///
/// The sampler runs on a dedicated OS thread, so the blocking `sysinfo` refresh
/// and `/proc` scans never occupy a Tokio worker. It owns the `System` across
/// ticks (required for CPU-usage diffs) and polls cancellation every
/// [`CANCEL_POLL`]. Samples are dropped (and logged) if the receiver stops
/// draining, rather than blocking cancellation.
///
/// Returns a handle (for shutdown) and a receiver of samples.
/// If `interval_ms` is 0, returns None (monitor disabled).
pub fn start(interval_ms: u64) -> Option<(SamplerHandle, mpsc::Receiver<ResourceSample>)> {
    start_with_capacity(interval_ms, 256)
}

/// Start a sampler with an explicit channel capacity.
///
/// Exposed to tests so backpressure (`try_send` returning `Full`) can be
/// exercised deterministically without producing 256 samples.
fn start_with_capacity(
    interval_ms: u64,
    capacity: usize,
) -> Option<(SamplerHandle, mpsc::Receiver<ResourceSample>)> {
    if interval_ms == 0 {
        return None;
    }

    let requested = interval_ms;
    let interval_ms = requested.min(MAX_SAMPLE_INTERVAL_MS);
    if interval_ms != requested {
        tracing::warn!(
            requested,
            capped = interval_ms,
            "resource sampler interval capped"
        );
    }

    let (tx, rx) = mpsc::channel(capacity.max(1));
    let cancel = CancellationToken::new();
    let thread_cancel = cancel.clone();

    let spawned = std::thread::Builder::new()
        .name("strobengine-sysinfo".into())
        .spawn(move || {
            tracing::debug!(interval_ms, "resource sampler thread started");
            let mut system = sysinfo::System::new();
            let Ok(pid) = sysinfo::get_current_pid() else {
                tracing::warn!("resource monitor unavailable: cannot determine PID");
                return;
            };
            let tick = Duration::from_millis(interval_ms);

            // Seed initial CPU baseline so first sample isn't skewed.
            system.refresh_processes(ProcessesToUpdate::Some(&[pid]), false);

            let start = Instant::now();
            let mut dropped: u64 = 0;

            // Sleep-after-work cadence: the effective period is `tick` plus the
            // refresh duration, matching the previous task-based sampler.
            while !sleep_until_cancelled(&thread_cancel, tick) {
                system.refresh_processes(ProcessesToUpdate::Some(&[pid]), false);
                if thread_cancel.is_cancelled() {
                    break;
                }
                if let Some(process) = system.process(pid) {
                    let sample = ResourceSample {
                        timestamp_us: start.elapsed().as_micros() as u64,
                        cpu_usage_percent: process.cpu_usage(),
                        memory_rss_bytes: process.memory(),
                        thread_count: count_threads(),
                        open_fds: count_open_fds(),
                    };
                    match tx.try_send(sample) {
                        Ok(()) => {}
                        Err(TrySendError::Full(_)) => {
                            dropped += 1;
                            if dropped == 1 {
                                tracing::debug!(
                                    "resource sampler dropping samples: receiver not draining"
                                );
                            } else if dropped.is_multiple_of(256) {
                                tracing::trace!(dropped, "resource samples dropped");
                            }
                        }
                        Err(TrySendError::Closed(_)) => break,
                    }
                }
            }

            tracing::debug!(dropped, "resource sampler thread stopped");
        });

    match spawned {
        Ok(_) => Some((SamplerHandle { cancel }, rx)),
        Err(e) => {
            tracing::warn!(error = %e, "failed to start resource sampler thread");
            None
        }
    }
}

/// Count threads in the current process (Linux only via /proc/self/task).
fn count_threads() -> usize {
    #[cfg(target_os = "linux")]
    {
        std::fs::read_dir("/proc/self/task")
            .map(|dir| dir.count())
            .unwrap_or(1)
    }
    #[cfg(not(target_os = "linux"))]
    {
        1
    }
}

/// Count open file descriptors (Linux only via /proc/self/fd).
/// Subtracts 1 to exclude the directory iterator's own fd.
fn count_open_fds() -> Option<usize> {
    #[cfg(target_os = "linux")]
    {
        std::fs::read_dir("/proc/self/fd")
            .map(|dir| dir.count().saturating_sub(1))
            .ok()
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn count_open_fds_returns_some_on_linux() {
        let fds = count_open_fds();
        #[cfg(target_os = "linux")]
        assert!(fds.is_some());
        #[cfg(not(target_os = "linux"))]
        assert!(fds.is_none());
    }

    #[tokio::test]
    async fn sampler_produces_samples() {
        let (handle, mut rx) = start(50).unwrap();

        // Wait for the first sample deterministically. A fixed sleep races the
        // sampler task: under parallel/loaded test execution the test task can
        // wake and cancel before the sampler task has run its send.
        let sample = tokio::time::timeout(Duration::from_secs(10), rx.recv())
            .await
            .expect("sampler produced no sample within 10s")
            .expect("sampler dropped its channel before producing a sample");

        handle.cancel.cancel();

        assert!(sample.cpu_usage_percent.is_finite() && sample.cpu_usage_percent >= 0.0);
        assert!(sample.memory_rss_bytes > 0);
        assert!(sample.thread_count >= 1);
    }

    #[test]
    fn disabled_returns_none() {
        assert!(start(0).is_none());
    }

    #[test]
    fn system_metrics_from_samples() {
        let samples = vec![
            ResourceSample {
                timestamp_us: 1000,
                cpu_usage_percent: 10.0,
                memory_rss_bytes: 1000,
                thread_count: 4,
                open_fds: Some(10),
            },
            ResourceSample {
                timestamp_us: 2000,
                cpu_usage_percent: 30.0,
                memory_rss_bytes: 2000,
                thread_count: 8,
                open_fds: Some(12),
            },
        ];
        let metrics = SystemMetrics::from_samples(&samples);
        assert!((metrics.peak_cpu_percent - 30.0).abs() < 0.01);
        assert!((metrics.avg_cpu_percent - 20.0).abs() < 0.01);
        assert_eq!(metrics.peak_memory_rss_bytes, 2000);
        assert_eq!(metrics.avg_memory_rss_bytes, 1500);
        assert_eq!(metrics.peak_thread_count, 8);
        assert_eq!(metrics.time_series.len(), 2);
    }

    #[test]
    fn system_metrics_empty() {
        let metrics = SystemMetrics::from_samples(&[]);
        assert_eq!(metrics.peak_cpu_percent, 0.0);
        assert!(metrics.time_series.is_empty());
    }

    #[test]
    fn system_metrics_nan_cpu_filtered() {
        let samples = vec![
            ResourceSample {
                timestamp_us: 1000,
                cpu_usage_percent: f32::NAN,
                memory_rss_bytes: 1000,
                thread_count: 4,
                open_fds: None,
            },
            ResourceSample {
                timestamp_us: 2000,
                cpu_usage_percent: 20.0,
                memory_rss_bytes: 2000,
                thread_count: 8,
                open_fds: None,
            },
        ];
        let metrics = SystemMetrics::from_samples(&samples);
        assert!((metrics.peak_cpu_percent - 20.0).abs() < 0.01);
        assert!((metrics.avg_cpu_percent - 20.0).abs() < 0.01);
        assert_eq!(metrics.time_series.len(), 2);
    }

    #[tokio::test]
    async fn sampler_stops_after_cancel() {
        let (handle, mut rx) = start(20).expect("sampler enabled");
        handle.cancel.cancel();

        let stopped = tokio::time::timeout(Duration::from_secs(2), async {
            while rx.recv().await.is_some() {}
        })
        .await;
        assert!(stopped.is_ok(), "sampler thread did not stop after cancel");
    }

    #[tokio::test]
    async fn sampler_stops_after_cancel_under_backpressure() {
        let (handle, rx) = start_with_capacity(1, 2).expect("sampler enabled");

        // Fill the 256-slot channel without draining it.
        let filled = tokio::time::timeout(Duration::from_secs(2), async {
            while rx.len() < 2 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;
        assert!(filled.is_ok(), "sampler channel never filled");

        handle.cancel.cancel();
        let closed = tokio::time::timeout(Duration::from_secs(2), async {
            while !rx.is_closed() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        assert!(closed.is_ok(), "sampler did not stop under backpressure");
    }
}
