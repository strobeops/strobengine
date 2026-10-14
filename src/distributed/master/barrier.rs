//! Barrier synchronization: gate a distributed run on workers becoming ready.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{Mutex, Notify};

use super::error::{MasterError, MasterResult};
use super::registry::Registry;

/// Coordinates multi-node test initialization.
///
/// A run waits until the required number of workers report `READY`, then
/// returns the allocated worker ids so the master can broadcast a synchronized
/// `START` (eliminating ramp-up skew).
#[derive(Clone)]
pub struct BarrierCoordinator {
    registry: Arc<Mutex<Registry>>,
    notify: Arc<Notify>,
}

impl BarrierCoordinator {
    /// Create a barrier over the shared registry. `notify` must be signalled by
    /// the server whenever a worker registers or heartbeats.
    pub fn new(registry: Arc<Mutex<Registry>>, notify: Arc<Notify>) -> Self {
        Self { registry, notify }
    }

    /// Wait until at least `expected` workers are `READY` (or any worker when
    /// `expected == 0`), up to `timeout`.
    ///
    /// Returns the allocated worker ids: the first `expected` ready workers, or
    /// every ready worker when `expected == 0`.
    pub async fn wait_for_ready(
        &self,
        expected: usize,
        timeout: Duration,
    ) -> MasterResult<Vec<String>> {
        let required = expected.max(1);
        let deadline = Instant::now() + timeout;

        loop {
            let ready = {
                let registry = self.registry.lock().await;
                let mut ready = registry.ready_workers();
                ready.sort();
                ready
            };

            if ready.len() >= required {
                let allocated = if expected == 0 {
                    ready
                } else {
                    ready.into_iter().take(required).collect()
                };
                tracing::info!(count = allocated.len(), "barrier satisfied");
                return Ok(allocated);
            }

            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(MasterError::BarrierTimeout {
                    expected: required,
                    ready: ready.len(),
                });
            }

            // Wait for a registry change, but also poll so a missed notification
            // can never stall the barrier.
            tokio::select! {
                _ = self.notify.notified() => {}
                _ = tokio::time::sleep(remaining.min(Duration::from_millis(100))) => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed::proto::{RegisterWorkerRequest, WorkerState};

    fn register(registry: &mut Registry, id: &str) {
        registry
            .register(RegisterWorkerRequest {
                worker_id: id.to_string(),
                max_concurrency_vus: 10,
                ..Default::default()
            })
            .unwrap();
    }

    #[tokio::test]
    async fn resolves_once_expected_workers_are_ready() {
        let registry = Arc::new(Mutex::new(Registry::new()));
        let notify = Arc::new(Notify::new());
        let barrier = BarrierCoordinator::new(Arc::clone(&registry), Arc::clone(&notify));

        {
            let mut guard = registry.lock().await;
            register(&mut guard, "w1");
            register(&mut guard, "w2");
            guard
                .record_heartbeat("w1", WorkerState::Ready, 0, String::new())
                .unwrap();
        }

        // Only one ready, but two expected: must wait.
        let barrier_task = {
            let barrier = barrier.clone();
            tokio::spawn(async move { barrier.wait_for_ready(2, Duration::from_secs(5)).await })
        };

        tokio::time::sleep(Duration::from_millis(50)).await;
        {
            let mut guard = registry.lock().await;
            guard
                .record_heartbeat("w2", WorkerState::Ready, 0, String::new())
                .unwrap();
        }
        notify.notify_waiters();

        let allocated = barrier_task.await.unwrap().unwrap();
        assert_eq!(allocated, vec!["w1".to_string(), "w2".to_string()]);
    }

    #[tokio::test]
    async fn times_out_when_workers_never_arrive() {
        let registry = Arc::new(Mutex::new(Registry::new()));
        let notify = Arc::new(Notify::new());
        let barrier = BarrierCoordinator::new(registry, notify);

        let err = barrier
            .wait_for_ready(2, Duration::from_millis(100))
            .await
            .unwrap_err();
        assert!(matches!(err, MasterError::BarrierTimeout { .. }));
    }

    #[tokio::test]
    async fn zero_expected_allocates_every_ready_worker() {
        let registry = Arc::new(Mutex::new(Registry::new()));
        let notify = Arc::new(Notify::new());
        {
            let mut guard = registry.lock().await;
            register(&mut guard, "w1");
            register(&mut guard, "w2");
            guard
                .record_heartbeat("w1", WorkerState::Ready, 0, String::new())
                .unwrap();
            guard
                .record_heartbeat("w2", WorkerState::Ready, 0, String::new())
                .unwrap();
        }
        let barrier = BarrierCoordinator::new(registry, notify);
        let allocated = barrier
            .wait_for_ready(0, Duration::from_millis(100))
            .await
            .unwrap();
        assert_eq!(allocated, vec!["w1".to_string(), "w2".to_string()]);
    }
}
