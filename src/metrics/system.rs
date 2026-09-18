use std::time::Duration;

use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use sysinfo::ProcessesToUpdate;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// A single resource measurement snapshot.
#[pyclass(from_py_object)]
#[derive(Debug, Clone, Serialize, Deserialize)]
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

/// Handle to a running resource monitor task.
#[allow(dead_code)]
pub struct SamplerHandle {
    pub cancel: CancellationToken,
}

/// Start a background resource sampler.
///
/// Returns a handle (for shutdown) and a receiver of samples.
/// If `interval_ms` is 0, returns None (monitor disabled).
#[allow(dead_code)]
pub fn start(interval_ms: u64) -> Option<(SamplerHandle, mpsc::Receiver<ResourceSample>)> {
    if interval_ms == 0 {
        return None;
    }

    let (tx, rx) = mpsc::channel(256);
    let cancel = CancellationToken::new();
    let cancel_clone = cancel.clone();

    tokio::spawn(async move {
        let mut system = sysinfo::System::new();
        let Ok(pid) = sysinfo::get_current_pid() else {
            tracing::warn!("resource monitor unavailable: cannot determine PID");
            return;
        };
        let tick = Duration::from_millis(interval_ms);

        // Seed initial CPU baseline so first sample isn't skewed
        system.refresh_processes(ProcessesToUpdate::Some(&[pid]), false);

        let start = std::time::Instant::now();

        loop {
            tokio::select! {
                _ = tokio::time::sleep(tick) => {},
                _ = cancel_clone.cancelled() => break,
            }

            system.refresh_processes(ProcessesToUpdate::Some(&[pid]), false);

            if let Some(process) = system.process(pid) {
                let sample = ResourceSample {
                    timestamp_us: start.elapsed().as_micros() as u64,
                    cpu_usage_percent: process.cpu_usage(),
                    memory_rss_bytes: process.memory(),
                    thread_count: count_threads(),
                    open_fds: count_open_fds(),
                };

                if tx.send(sample).await.is_err() {
                    break;
                }
            }
        }
    });

    Some((SamplerHandle { cancel }, rx))
}

/// Count threads in the current process (Linux only via /proc/self/task).
#[allow(dead_code)]
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
#[allow(dead_code)]
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
        let (handle, mut rx) = start(100).unwrap();
        tokio::time::sleep(Duration::from_millis(250)).await;
        handle.cancel.cancel();
        let mut samples = Vec::new();
        while let Some(s) = rx.recv().await {
            samples.push(s);
        }
        assert!(!samples.is_empty());
        assert!(samples[0].cpu_usage_percent >= 0.0);
        assert!(samples[0].memory_rss_bytes > 0);
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
}
