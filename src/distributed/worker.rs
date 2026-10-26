//! Worker daemon runtime: registration, heartbeats, command handling, run
//! orchestration, and telemetry upload.
//!
//! [`Worker`] owns the master connection lifecycle and reconnects with
//! exponential backoff. Call [`Worker::run`] with a [`CancellationToken`] to
//! start it and cancel the token to shut down cleanly.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use sysinfo::System;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tonic::transport::{Channel, Endpoint};

use crate::metrics::RequestMetric;

use super::backoff::Backoff;
use super::config::WorkerConfig;
use super::error::{WorkerError, WorkerResult};
use super::executor::{ExecutionPlan, run_virtual_users};
use super::proto::master_controller_client::MasterControllerClient;
use super::proto::telemetry_service_client::TelemetryServiceClient;
use super::proto::{
    CommandRequest, CommandResponse, CommandStatus, CommandType, HeartbeatDirective,
    HeartbeatRequest, HeartbeatResponse, RegisterWorkerRequest, ResourceCapacity, ResourceUsage,
    WatchCommandsRequest,
};
use super::state::{WorkerState, WorkerStateMachine};
use super::telemetry::{WindowedCollector, upload_metrics};

/// Bounded channel capacity for per-iteration metrics feeding the collector.
const METRIC_CHANNEL_CAPACITY: usize = 8192;
/// Bounded channel capacity for telemetry batches feeding the upload RPC.
const BATCH_CHANNEL_CAPACITY: usize = 64;

fn now_timestamp() -> prost_types::Timestamp {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    prost_types::Timestamp {
        seconds: now.as_secs() as i64,
        nanos: now.subsec_nanos() as i32,
    }
}

// Recovering from poisoning is deliberate: these locks guard small control
// state (a state machine over a Copy enum and an Option<RunControl>), so a
// panic cannot leave a torn value. If stop_run panics after taking the handle,
// the slot is already None; keeping the daemon alive beats propagating it.
fn lock_or_recover<'a, T>(mutex: &'a Mutex<T>, name: &str) -> MutexGuard<'a, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            tracing::warn!(mutex = name, "recovering from poisoned mutex");
            let guard = poisoned.into_inner();
            mutex.clear_poison();
            guard
        }
    }
}

/// A run currently executing on this worker.
struct RunControl {
    cancel: CancellationToken,
    handle: JoinHandle<()>,
}

/// State shared between the heartbeat loop, command loop and run tasks.
struct Shared {
    state: Mutex<WorkerStateMachine>,
    active_vus: AtomicU32,
    run: Mutex<Option<RunControl>>,
}

impl Shared {
    fn new() -> Self {
        Self {
            state: Mutex::new(WorkerStateMachine::new()),
            active_vus: AtomicU32::new(0),
            run: Mutex::new(None),
        }
    }

    fn state(&self) -> WorkerState {
        lock_or_recover(&self.state, "state").state()
    }

    fn set_state(&self, next: WorkerState) {
        let mut guard = lock_or_recover(&self.state, "state");
        if let Err(e) = guard.transition(next) {
            tracing::warn!(error = %e, "ignoring illegal state transition");
        }
    }

    fn active_vus(&self) -> u32 {
        self.active_vus.load(Ordering::Relaxed)
    }

    /// Install a new run, rejecting a START while one is already active.
    fn begin_run(&self, control: RunControl) -> bool {
        let mut guard = lock_or_recover(&self.run, "run");
        if guard.is_some() {
            return false;
        }
        *guard = Some(control);
        true
    }

    /// Cancel the active run (if any) without waiting for it.
    fn cancel_run(&self) {
        if let Some(control) = lock_or_recover(&self.run, "run").as_ref() {
            control.cancel.cancel();
        }
    }

    /// Cancel and await the active run (if any).
    async fn stop_run(&self) {
        let control = {
            let mut guard = lock_or_recover(&self.run, "run");
            guard.take()
        };
        if let Some(control) = control {
            control.cancel.cancel();
            let _ = control.handle.await;
        }
        self.active_vus.store(0, Ordering::Relaxed);
    }
}

/// Immutable parameters cloned into the control tasks.
#[derive(Clone)]
struct RunParams {
    worker_id: String,
    max_vus: u32,
    window: Duration,
    request_timeout: Duration,
}

/// The worker daemon.
pub struct Worker {
    config: WorkerConfig,
    params: RunParams,
    hostname: String,
    ip_address: String,
    labels: HashMap<String, String>,
    capacity: ResourceCapacity,
}

impl Worker {
    /// Create a worker from validated configuration.
    pub fn new(config: WorkerConfig) -> WorkerResult<Self> {
        config.validate()?;
        let labels = config.labels_map()?;
        let hostname = config.resolved_hostname();
        let ip_address = config.resolved_ip_address();
        let capacity = config.capacity();
        let params = RunParams {
            worker_id: config.resolved_worker_id(),
            max_vus: config.resolved_max_vus(),
            window: Duration::from_millis(config.metric_window_ms),
            request_timeout: Duration::from_millis(config.request_timeout_ms),
        };
        Ok(Self {
            config,
            params,
            hostname,
            ip_address,
            labels,
            capacity,
        })
    }

    /// The resolved worker identifier.
    pub fn worker_id(&self) -> &str {
        &self.params.worker_id
    }

    /// Run until `shutdown` is cancelled, reconnecting to the master with
    /// exponential backoff if the connection drops.
    pub async fn run(self, shutdown: CancellationToken) -> WorkerResult<()> {
        let mut backoff = Backoff::new(Duration::from_millis(500), Duration::from_secs(30));
        loop {
            if shutdown.is_cancelled() {
                return Ok(());
            }
            match self.session(&shutdown).await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    if shutdown.is_cancelled() {
                        return Ok(());
                    }
                    tracing::warn!(error = %e, "master session ended; reconnecting");
                }
            }
            let delay = backoff.next_delay();
            tracing::info!(?delay, "backing off before reconnect");
            tokio::select! {
                _ = tokio::time::sleep(delay) => {}
                _ = shutdown.cancelled() => return Ok(()),
            }
        }
    }

    /// Establish a channel, register, and run the control loops until shutdown.
    async fn session(&self, shutdown: &CancellationToken) -> WorkerResult<()> {
        let channel = self.connect().await?;
        let mut master = MasterControllerClient::new(channel.clone());

        let cluster = self.register(&mut master).await?;
        let heartbeat_interval = if cluster.heartbeat_interval_ms > 0 {
            Duration::from_millis(u64::from(cluster.heartbeat_interval_ms))
        } else {
            Duration::from_millis(self.config.heartbeat_interval_ms)
        };

        tracing::info!(
            cluster_id = %cluster.cluster_id,
            heartbeat_interval_ms = heartbeat_interval.as_millis(),
            "registered with master"
        );

        let shared = Arc::new(Shared::new());
        let mut heartbeat = tokio::spawn(heartbeat_loop(
            master.clone(),
            Arc::clone(&shared),
            heartbeat_interval,
            self.params.clone(),
            shutdown.clone(),
        ));
        let mut commands = tokio::spawn(command_loop(
            master.clone(),
            channel,
            Arc::clone(&shared),
            self.params.clone(),
            shutdown.clone(),
        ));

        let outcome: WorkerResult<()> = tokio::select! {
            _ = shutdown.cancelled() => Ok(()),
            r = &mut heartbeat => match r {
                Ok(Ok(())) => Err(WorkerError::Disconnected("heartbeat loop stopped".into())),
                Ok(Err(e)) => Err(e),
                Err(e) => Err(WorkerError::Disconnected(format!("heartbeat task panicked: {e}"))),
            },
            r = &mut commands => match r {
                Ok(Ok(())) => Err(WorkerError::Disconnected("command loop stopped".into())),
                Ok(Err(e)) => Err(e),
                Err(e) => Err(WorkerError::Disconnected(format!("command task panicked: {e}"))),
            },
        };

        shared.stop_run().await;
        heartbeat.abort();
        commands.abort();
        outcome
    }

    async fn connect(&self) -> WorkerResult<Channel> {
        let endpoint = Endpoint::from_shared(self.config.endpoint_uri())
            .map_err(|e| WorkerError::Config(format!("invalid master endpoint: {e}")))?;
        let channel = endpoint.connect().await?;
        Ok(channel)
    }

    async fn register(
        &self,
        master: &mut MasterControllerClient<Channel>,
    ) -> WorkerResult<super::proto::ClusterConfiguration> {
        let request = RegisterWorkerRequest {
            worker_id: self.params.worker_id.clone(),
            hostname: self.hostname.clone(),
            ip_address: self.ip_address.clone(),
            max_concurrency_vus: self.params.max_vus,
            capacity: Some(self.capacity),
            agent_version: env!("CARGO_PKG_VERSION").to_string(),
            labels: self.labels.clone(),
            registered_at: Some(now_timestamp()),
        };
        let response = master.register_worker(request).await?.into_inner();
        if response.status != super::proto::RegistrationStatus::Accepted as i32 {
            return Err(WorkerError::Config(format!(
                "registration rejected: {}",
                response.message
            )));
        }
        Ok(response.cluster_config.unwrap_or_default())
    }
}

async fn heartbeat_loop(
    mut master: MasterControllerClient<Channel>,
    shared: Arc<Shared>,
    interval: Duration,
    params: RunParams,
    shutdown: CancellationToken,
) -> WorkerResult<()> {
    let mut system = System::new();
    let mut ticker = tokio::time::interval(interval.max(Duration::from_millis(100)));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => return Ok(()),
            _ = ticker.tick() => {}
        }

        system.refresh_cpu_usage();
        system.refresh_memory();
        let total = system.total_memory();
        let available = system.available_memory();
        let memory_percent = if total > 0 {
            (total.saturating_sub(available)) as f32 / total as f32 * 100.0
        } else {
            0.0
        };

        let request = HeartbeatRequest {
            worker_id: params.worker_id.clone(),
            state: shared.state().as_proto() as i32,
            active_vus: shared.active_vus(),
            usage: Some(ResourceUsage {
                cpu_utilization_percent: system.global_cpu_usage(),
                memory_utilization_percent: memory_percent,
                memory_used_bytes: total.saturating_sub(available),
                thread_count: 0,
            }),
            sent_at: Some(now_timestamp()),
            test_run_id: String::new(),
        };

        let response: HeartbeatResponse = master.heartbeat(request).await?.into_inner();
        match HeartbeatDirective::try_from(response.directive)
            .unwrap_or(HeartbeatDirective::Unspecified)
        {
            HeartbeatDirective::Abort => {
                tracing::warn!("master directed ABORT via heartbeat");
                shared.cancel_run();
            }
            HeartbeatDirective::Synchronize => {
                tracing::info!("master requested synchronize; reconnecting session");
                return Err(WorkerError::Disconnected("synchronize requested".into()));
            }
            HeartbeatDirective::Continue | HeartbeatDirective::Unspecified => {}
        }
    }
}

async fn command_loop(
    mut master: MasterControllerClient<Channel>,
    channel: Channel,
    shared: Arc<Shared>,
    params: RunParams,
    shutdown: CancellationToken,
) -> WorkerResult<()> {
    let request = WatchCommandsRequest {
        worker_id: params.worker_id.clone(),
        last_command_id: String::new(),
    };
    let mut stream = master.watch_commands(request).await?.into_inner();

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => return Ok(()),
            message = stream.message() => {
                match message? {
                    Some(command) => handle_command(&mut master, &channel, &shared, &params, command).await?,
                    None => {
                        tracing::warn!("command stream closed by master");
                        return Err(WorkerError::Disconnected("command stream closed".into()));
                    }
                }
            }
        }
    }
}

async fn handle_command(
    master: &mut MasterControllerClient<Channel>,
    channel: &Channel,
    shared: &Arc<Shared>,
    params: &RunParams,
    command: CommandRequest,
) -> WorkerResult<()> {
    let kind = CommandType::try_from(command.command).unwrap_or(CommandType::Unspecified);
    tracing::info!(?kind, command_id = %command.command_id, "received command");

    let (status, error_message) = match kind {
        CommandType::Start => match command.config.as_ref() {
            None => (
                CommandStatus::Rejected,
                "START command missing TestConfiguration".to_string(),
            ),
            Some(config) => {
                match ExecutionPlan::from_proto(config, params.max_vus, params.request_timeout) {
                    Err(e) => (CommandStatus::Failed, e.to_string()),
                    Ok(plan) => start_run(
                        channel.clone(),
                        shared.clone(),
                        params.clone(),
                        plan,
                        command.test_run_id.clone(),
                    ),
                }
            }
        },
        CommandType::Stop => {
            shared.stop_run().await;
            shared.set_state(WorkerState::Ready);
            (CommandStatus::Completed, String::new())
        }
        CommandType::Abort => {
            shared.cancel_run();
            shared.set_state(WorkerState::Ready);
            (CommandStatus::Completed, String::new())
        }
        CommandType::Unspecified => (CommandStatus::Rejected, "unspecified command".to_string()),
    };

    let response = CommandResponse {
        command_id: command.command_id.clone(),
        worker_id: params.worker_id.clone(),
        status: status as i32,
        error_message,
        acknowledged_at: Some(now_timestamp()),
    };
    master.acknowledge_command(response).await?;
    Ok(())
}

/// Start a run: wire VUs -> collector -> telemetry, and record the run handle.
fn start_run(
    channel: Channel,
    shared: Arc<Shared>,
    params: RunParams,
    plan: ExecutionPlan,
    test_run_id: String,
) -> (CommandStatus, String) {
    let cancel = CancellationToken::new();
    let test_run_id = if test_run_id.is_empty() {
        format!("{}-{}", params.worker_id, now_timestamp().seconds)
    } else {
        test_run_id
    };
    let virtual_users = plan.virtual_users as u32;

    let run = RunControl {
        cancel: cancel.clone(),
        handle: tokio::spawn(run_orchestrator(
            channel,
            Arc::clone(&shared),
            params.clone(),
            plan,
            test_run_id,
            cancel,
        )),
    };
    if !shared.begin_run(run) {
        return (
            CommandStatus::Rejected,
            "a run is already in progress".to_string(),
        );
    }
    shared.set_state(WorkerState::Running);
    shared.active_vus.store(virtual_users, Ordering::Relaxed);
    (CommandStatus::Acknowledged, String::new())
}

async fn run_orchestrator(
    channel: Channel,
    shared: Arc<Shared>,
    params: RunParams,
    plan: ExecutionPlan,
    test_run_id: String,
    cancel: CancellationToken,
) {
    let (metric_tx, mut metric_rx) = mpsc::channel::<RequestMetric>(METRIC_CHANNEL_CAPACITY);
    let (batch_tx, batch_rx) = mpsc::channel(BATCH_CHANNEL_CAPACITY);

    // Telemetry upload task: drives StreamMetrics until batch_tx is dropped.
    let telemetry = tokio::spawn(async move {
        let mut client = TelemetryServiceClient::new(channel);
        match upload_metrics(&mut client, batch_rx).await {
            Ok(ack) => tracing::info!(
                accepted_batches = ack.accepted_batches,
                accepted_requests = ack.accepted_requests,
                "telemetry stream acknowledged"
            ),
            Err(e) => tracing::warn!(error = %e, "telemetry upload failed"),
        }
    });

    let vus = tokio::spawn(run_virtual_users(plan, cancel.clone(), metric_tx));

    let mut collector = WindowedCollector::new(&params.worker_id, &test_run_id, params.window);
    collector.set_active_vus(shared.active_vus());
    let mut ticker = tokio::time::interval(params.window.max(Duration::from_millis(10)));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            maybe = metric_rx.recv() => match maybe {
                Some(metric) => collector.record(&metric),
                None => break,
            },
            _ = ticker.tick() => {
                if let Some(batch) = collector.take_batch()
                    && batch_tx.send(batch).await.is_err() {
                    break;
                }
            }
        }
    }

    if let Some(batch) = collector.take_batch() {
        let _ = batch_tx.send(batch).await;
    }
    drop(batch_tx); // half-close the telemetry stream
    let _ = vus.await;
    let _ = telemetry.await;

    shared.active_vus.store(0, Ordering::Relaxed);
    shared.set_state(WorkerState::Ready);
    tracing::info!(%test_run_id, "run complete");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn shared_rejects_concurrent_runs() {
        let shared = Shared::new();
        let control = || RunControl {
            cancel: CancellationToken::new(),
            handle: tokio::spawn(async {}),
        };
        assert!(shared.begin_run(control()));
        assert!(!shared.begin_run(control()));
    }

    #[test]
    fn shared_state_transitions() {
        let shared = Shared::new();
        assert_eq!(shared.state(), WorkerState::Ready);
        shared.set_state(WorkerState::Running);
        assert_eq!(shared.state(), WorkerState::Running);
    }

    fn poison<T>(mutex: &Mutex<T>) {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = mutex.lock().unwrap();
            panic!("poison");
        }));
        assert!(mutex.is_poisoned());
    }

    #[test]
    fn lock_recovers_from_poison() {
        let mutex = Mutex::new(0);
        poison(&mutex);
        *lock_or_recover(&mutex, "test") = 1;
        assert!(!mutex.is_poisoned());
        assert_eq!(*mutex.lock().unwrap(), 1);
    }

    #[test]
    fn shared_state_recovers_from_poison() {
        let shared = Shared::new();
        poison(&shared.state);
        assert_eq!(shared.state(), WorkerState::Ready);
        assert!(!shared.state.is_poisoned());
        shared.set_state(WorkerState::Draining);
        assert_eq!(shared.state(), WorkerState::Draining);
    }

    #[tokio::test]
    async fn shared_run_recovers_from_poison_when_empty() {
        let shared = Shared::new();
        poison(&shared.run);
        shared.cancel_run();
        assert!(!shared.run.is_poisoned());

        let cancel = CancellationToken::new();
        assert!(shared.begin_run(RunControl {
            cancel: cancel.clone(),
            handle: tokio::spawn(async {}),
        }));
        shared.cancel_run();
        assert!(cancel.is_cancelled());
        shared.stop_run().await;

        assert!(shared.begin_run(RunControl {
            cancel: CancellationToken::new(),
            handle: tokio::spawn(async {}),
        }));
        shared.stop_run().await;
    }

    #[tokio::test]
    async fn shared_run_recovers_from_poison_with_active_run() {
        let shared = Shared::new();
        let cancel = CancellationToken::new();
        assert!(shared.begin_run(RunControl {
            cancel: cancel.clone(),
            handle: tokio::spawn(async {}),
        }));
        poison(&shared.run);
        assert!(shared.run.is_poisoned());

        assert!(!shared.begin_run(RunControl {
            cancel: CancellationToken::new(),
            handle: tokio::spawn(async {}),
        }));
        assert!(!shared.run.is_poisoned());

        shared.cancel_run();
        assert!(cancel.is_cancelled());
        shared.stop_run().await;

        assert!(shared.begin_run(RunControl {
            cancel: CancellationToken::new(),
            handle: tokio::spawn(async {}),
        }));
        shared.stop_run().await;
    }

    #[test]
    fn now_timestamp_is_plausible() {
        let ts = now_timestamp();
        assert!(ts.seconds > 1_600_000_000);
        assert!((0..1_000_000_000).contains(&ts.nanos));
    }
}
