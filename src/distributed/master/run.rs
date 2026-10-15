//! Master runtime state and run orchestration.
//!
//! [`MasterHandle`] owns the shared [`MasterState`] (registry, barrier,
//! aggregator) and drives a run: barrier → broadcast START → collect telemetry
//! until every allocated worker's stream closes → finalize the global report.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::sync::{Mutex, Notify};

use crate::distributed::proto::{
    ClusterConfiguration, CommandRequest, CommandResponse, CommandType, MetricBatch,
    TestConfiguration,
};

use super::barrier::BarrierCoordinator;
use super::config::MasterConfig;
use super::dashboard::LiveBroadcaster;
use super::error::{MasterError, MasterResult};
use super::pipeline::PipelineHandle;
use super::registry::{Registry, WorkerSnapshot};
use super::reporting::GlobalReport;

/// Extra time beyond the run duration before the master stops waiting for
/// telemetry streams to close.
const COMPLETION_GRACE: Duration = Duration::from_secs(5);

/// Current wall-clock time as a protobuf timestamp.
pub(super) fn now_timestamp() -> prost_types::Timestamp {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    prost_types::Timestamp {
        seconds: now.as_secs() as i64,
        nanos: now.subsec_nanos() as i32,
    }
}

fn now_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// A run currently in progress.
struct ActiveRun {
    run_id: String,
    start_command_id: String,
    allocated: Vec<String>,
    pipeline: PipelineHandle,
    acked: HashSet<String>,
    telemetry_done: HashSet<String>,
}

/// Shared state used by the gRPC services and the run controller.
pub struct MasterState {
    config: MasterConfig,
    registry: Arc<Mutex<Registry>>,
    notify: Arc<Notify>,
    barrier: BarrierCoordinator,
    run: Arc<Mutex<Option<ActiveRun>>>,
    run_notify: Arc<Notify>,
    broadcaster: LiveBroadcaster,
    master_endpoint: String,
}

impl MasterState {
    /// Build the shared state from validated configuration.
    pub fn new(config: MasterConfig) -> MasterResult<Arc<Self>> {
        config.validate()?;
        let listen = config.listen_addr()?;
        let registry = Arc::new(Mutex::new(Registry::new()));
        let notify = Arc::new(Notify::new());
        let barrier = BarrierCoordinator::new(Arc::clone(&registry), Arc::clone(&notify));
        Ok(Arc::new(Self {
            master_endpoint: format!("http://{listen}"),
            config,
            registry,
            notify,
            barrier,
            run: Arc::new(Mutex::new(None)),
            run_notify: Arc::new(Notify::new()),
            broadcaster: LiveBroadcaster::default(),
        }))
    }

    /// The configuration handed to registering workers.
    pub fn cluster_configuration(&self) -> ClusterConfiguration {
        self.config.cluster_configuration(&self.master_endpoint)
    }

    /// The master configuration.
    pub fn config(&self) -> &MasterConfig {
        &self.config
    }

    /// The shared registry.
    pub fn registry(&self) -> &Arc<Mutex<Registry>> {
        &self.registry
    }

    /// Notify the barrier that registry membership/state changed.
    pub fn notify_change(&self) {
        self.notify.notify_waiters();
    }

    /// Forward a telemetry batch to the active run's pipeline (non-blocking).
    pub async fn ingest_batch(&self, batch: &MetricBatch) {
        let pipeline = {
            let guard = self.run.lock().await;
            guard
                .as_ref()
                .filter(|run| batch.test_run_id == run.run_id)
                .map(|run| run.pipeline.clone())
        };
        if let Some(pipeline) = pipeline {
            pipeline.ingest(batch.clone());
        }
    }

    /// Subscribe to live cluster snapshots; the broadcaster persists across runs.
    pub fn subscribe_live(
        &self,
    ) -> tokio::sync::broadcast::Receiver<super::dashboard::LiveSnapshot> {
        self.broadcaster.subscribe()
    }

    /// Record a command acknowledgement for the active run.
    pub async fn record_ack(&self, response: &CommandResponse) {
        let mut guard = self.run.lock().await;
        if let Some(run) = guard.as_mut()
            && response.command_id == run.start_command_id
        {
            run.acked.insert(response.worker_id.clone());
        }
        drop(guard);
        self.run_notify.notify_waiters();
    }

    /// Mark a worker's telemetry stream as closed.
    pub async fn mark_telemetry_done(&self, worker_id: &str) {
        if worker_id.is_empty() {
            return;
        }
        let mut guard = self.run.lock().await;
        if let Some(run) = guard.as_mut() {
            run.telemetry_done.insert(worker_id.to_string());
        }
        drop(guard);
        self.run_notify.notify_waiters();
    }

    /// Snapshot of the registry for reporting.
    pub async fn worker_snapshot(&self) -> Vec<WorkerSnapshot> {
        self.registry.lock().await.snapshot()
    }
}

/// Control handle for the master controller.
#[derive(Clone)]
pub struct MasterHandle {
    state: Arc<MasterState>,
}

impl MasterHandle {
    /// Create a handle from configuration.
    pub fn new(config: MasterConfig) -> MasterResult<Self> {
        Ok(Self {
            state: MasterState::new(config)?,
        })
    }

    /// The shared state (for wiring the gRPC server).
    pub fn state(&self) -> Arc<MasterState> {
        Arc::clone(&self.state)
    }

    /// Number of registered workers.
    pub async fn worker_count(&self) -> usize {
        self.state.registry.lock().await.len()
    }

    /// Snapshot of registered workers.
    pub async fn worker_snapshot(&self) -> Vec<WorkerSnapshot> {
        self.state.worker_snapshot().await
    }

    /// Subscribe to live cluster snapshots.
    pub fn subscribe_live(
        &self,
    ) -> tokio::sync::broadcast::Receiver<super::dashboard::LiveSnapshot> {
        self.state.subscribe_live()
    }

    /// Serve the gRPC control plane until `shutdown` is cancelled.
    pub async fn serve(&self, shutdown: tokio_util::sync::CancellationToken) -> MasterResult<()> {
        let addr = self.state.config.listen_addr()?;
        super::server::serve(Arc::clone(&self.state), addr, shutdown).await
    }

    /// Run a distributed test: wait for `expected` workers, broadcast START,
    /// collect telemetry until every worker finishes, and return the report.
    pub async fn start_run(
        &self,
        mut config: TestConfiguration,
        expected: usize,
    ) -> MasterResult<GlobalReport> {
        // 1. Barrier: wait for the allocated workers to be ready.
        let allocated = self
            .state
            .barrier
            .wait_for_ready(expected, self.state.config.barrier_timeout())
            .await?;
        if allocated.is_empty() {
            return Err(MasterError::Run("no workers available".into()));
        }

        // 2. Resolve the run id and install the active run.
        let run_id = if config.test_run_id.trim().is_empty() {
            format!("run-{}", now_unix_ms())
        } else {
            config.test_run_id.clone()
        };
        config.test_run_id = run_id.clone();
        let start_command_id = format!("cmd-{run_id}-start");
        {
            let bucket_count = self.state.config.histogram_bucket_count as usize;
            let window_epochs = self.state.config.window_secs.max(1) as i64;
            let mut guard = self.state.run.lock().await;
            if guard.is_some() {
                return Err(MasterError::Run("a run is already active".into()));
            }
            let pipeline = PipelineHandle::spawn(
                run_id.clone(),
                bucket_count,
                window_epochs,
                self.state.broadcaster.clone(),
            );
            *guard = Some(ActiveRun {
                run_id: run_id.clone(),
                start_command_id: start_command_id.clone(),
                allocated: allocated.clone(),
                pipeline,
                acked: HashSet::new(),
                telemetry_done: HashSet::new(),
            });
        }

        // 3. Broadcast START to every allocated worker.
        let command = CommandRequest {
            command: CommandType::Start as i32,
            command_id: start_command_id.clone(),
            test_run_id: run_id.clone(),
            config: Some(config.clone()),
            issued_at: Some(now_timestamp()),
            command_timeout_ms: 0,
        };
        let mut delivered = 0usize;
        for worker_id in &allocated {
            let sender = self.state.registry.lock().await.command_sender(worker_id);
            match sender {
                Some(tx) if tx.send(Ok(command.clone())).is_ok() => delivered += 1,
                _ => tracing::warn!(%worker_id, "no command stream to deliver START"),
            }
        }
        if delivered == 0 {
            self.state.run.lock().await.take();
            return Err(MasterError::Run(
                "no worker received the START command".into(),
            ));
        }
        tracing::info!(%run_id, workers = delivered, "broadcast START");

        // 4. Wait until every allocated worker's telemetry stream closes, with a
        //    duration-derived deadline as a safety bound.
        let duration = config
            .duration
            .as_ref()
            .map(|d| Duration::new(d.seconds.max(0) as u64, d.nanos.max(0) as u32))
            .unwrap_or_default();
        let deadline = Instant::now() + duration + COMPLETION_GRACE;
        loop {
            {
                let guard = self.state.run.lock().await;
                match guard.as_ref() {
                    Some(run) if run.telemetry_done.len() >= run.allocated.len() => break,
                    Some(_) => {}
                    None => break,
                }
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                tracing::warn!(%run_id, "run completion deadline reached");
                break;
            }
            tokio::select! {
                _ = self.state.run_notify.notified() => {}
                _ = tokio::time::sleep(remaining.min(Duration::from_millis(50))) => {}
            }
        }

        // 5. Finalize.
        let run = self
            .state
            .run
            .lock()
            .await
            .take()
            .ok_or_else(|| MasterError::Run("active run disappeared".into()))?;
        let report = run
            .pipeline
            .finalize()
            .await
            .ok_or_else(|| MasterError::Run("telemetry pipeline stopped".into()))?;
        run.pipeline.shutdown();
        tracing::info!(
            %run_id,
            workers = report.worker_count,
            requests = report.total_requests,
            "run finalized"
        );
        Ok(report)
    }

    /// Abort the active run (best effort): broadcast ABORT and drop the run.
    pub async fn abort(&self) -> MasterResult<()> {
        let active = self.state.run.lock().await.take();
        if let Some(run) = active {
            run.pipeline.shutdown();
            let command = CommandRequest {
                command: CommandType::Abort as i32,
                command_id: format!("cmd-{}-abort", run.run_id),
                test_run_id: run.run_id.clone(),
                config: None,
                issued_at: Some(now_timestamp()),
                command_timeout_ms: 0,
            };
            for worker_id in &run.allocated {
                if let Some(tx) = self.state.registry.lock().await.command_sender(worker_id) {
                    let _ = tx.send(Ok(command.clone()));
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;
    use crate::distributed::proto::{
        CommandRequest, LatencySummary, RegisterWorkerRequest, WorkerState,
    };

    type CommandRx = tokio::sync::mpsc::UnboundedReceiver<Result<CommandRequest, tonic::Status>>;

    /// Register a worker as READY with a live command stream attached.
    async fn register_ready(handle: &MasterHandle, worker_id: &str) -> CommandRx {
        let state = handle.state();
        state
            .registry()
            .lock()
            .await
            .register(RegisterWorkerRequest {
                worker_id: worker_id.to_string(),
                max_concurrency_vus: 10,
                ..Default::default()
            })
            .unwrap();
        state
            .registry()
            .lock()
            .await
            .record_heartbeat(worker_id, WorkerState::Ready, 0, String::new())
            .unwrap();
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        state
            .registry()
            .lock()
            .await
            .attach_commands(worker_id, tx)
            .unwrap();
        state.notify_change();
        rx
    }

    #[tokio::test]
    async fn start_run_without_workers_times_out() {
        let handle = MasterHandle::new(
            MasterConfig::try_parse_from(["m", "--barrier-timeout-ms", "100"]).unwrap(),
        )
        .unwrap();
        let config = TestConfiguration {
            duration: Some(prost_types::Duration {
                seconds: 1,
                nanos: 0,
            }),
            ..Default::default()
        };
        let err = handle.start_run(config, 1).await.unwrap_err();
        assert!(matches!(err, MasterError::BarrierTimeout { .. }));
    }

    #[tokio::test]
    async fn telemetry_ingestion_completes_the_run() {
        let handle = MasterHandle::new(
            MasterConfig::try_parse_from(["m", "--barrier-timeout-ms", "1000"]).unwrap(),
        )
        .unwrap();
        let _cmd_rx = register_ready(&handle, "w1").await;

        let state = handle.state();
        let run = handle.start_run(
            TestConfiguration {
                test_run_id: "run-x".into(),
                duration: Some(prost_types::Duration {
                    seconds: 1,
                    nanos: 0,
                }),
                ..Default::default()
            },
            1,
        );

        // Deliver telemetry for the run, then signal completion, concurrently.
        let driver = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            state
                .ingest_batch(&MetricBatch {
                    worker_id: "w1".into(),
                    test_run_id: "run-x".into(),
                    total_requests: 7,
                    success_count: 7,
                    latency: Some(LatencySummary {
                        min_ms: 1.0,
                        max_ms: 3.0,
                        sample_count: 7,
                        bucket_counts: vec![0, 7, 0, 0, 0, 0, 0, 0, 0, 0],
                        ..Default::default()
                    }),
                    ..Default::default()
                })
                .await;
            state.mark_telemetry_done("w1").await;
        });

        let report = run.await.expect("run returns");
        driver.await.unwrap();
        assert_eq!(report.total_requests, 7);
        assert_eq!(report.worker_count, 1);
    }
}
