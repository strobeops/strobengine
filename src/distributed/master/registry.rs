//! Cluster registry: tracks worker nodes, their capabilities and health.
//!
//! The registry is shared behind a mutex and mutated by the gRPC service
//! handlers (registration, heartbeats, command-stream attach/detach) and by a
//! background health sweep that flags unresponsive workers.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::Mutex;
use tokio::sync::mpsc::UnboundedSender;
use tokio_util::sync::CancellationToken;
use tonic::Status;

use crate::distributed::proto::{
    CommandRequest, RegisterWorkerRequest, ResourceCapacity, WorkerState,
};

use super::error::{MasterError, MasterResult};

/// Sender used to push lifecycle commands to a worker's `WatchCommands` stream.
pub type CommandSender = UnboundedSender<Result<CommandRequest, Status>>;

/// Master-side lifecycle/health status of a worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerStatus {
    /// Registered but has not reported a heartbeat yet.
    Registered,
    /// Idle and available for assignment.
    Ready,
    /// Actively executing a run.
    Running,
    /// Missed heartbeats and is considered gone.
    Disconnected,
    /// Reported an error state.
    Failed,
}

impl WorkerStatus {
    /// Stable lower-case label for logs and reports.
    pub fn as_str(self) -> &'static str {
        match self {
            WorkerStatus::Registered => "REGISTERED",
            WorkerStatus::Ready => "READY",
            WorkerStatus::Running => "RUNNING",
            WorkerStatus::Disconnected => "DISCONNECTED",
            WorkerStatus::Failed => "FAILED",
        }
    }

    fn from_wire(state: WorkerState) -> WorkerStatus {
        match state {
            WorkerState::Ready => WorkerStatus::Ready,
            WorkerState::Running | WorkerState::Draining => WorkerStatus::Running,
            WorkerState::Error => WorkerStatus::Failed,
            WorkerState::Unregistered => WorkerStatus::Disconnected,
            WorkerState::Unspecified => WorkerStatus::Registered,
        }
    }
}

/// A full record of a registered worker.
#[derive(Debug, Clone)]
pub struct WorkerRecord {
    /// Cluster-unique id.
    pub worker_id: String,
    /// Reported host name.
    pub hostname: String,
    /// Reported IP address.
    pub ip_address: String,
    /// Reported agent version.
    pub agent_version: String,
    /// Advertised VU ceiling.
    pub max_vus: u32,
    /// Advertised hardware capacity.
    pub capacity: Option<ResourceCapacity>,
    /// Operator labels.
    pub labels: HashMap<String, String>,
    /// Current status.
    pub status: WorkerStatus,
    /// Active virtual users (from the last heartbeat).
    pub active_vus: u32,
    /// Run id reported by the last heartbeat.
    pub test_run_id: String,
    /// When the last heartbeat was received.
    pub last_heartbeat: Instant,
    /// Command channel for the worker's active `WatchCommands` stream.
    command_tx: Option<CommandSender>,
}

/// Read-only view of a worker for reporting.
#[derive(Debug, Clone)]
pub struct WorkerSnapshot {
    /// Cluster-unique id.
    pub worker_id: String,
    /// Reported host name.
    pub hostname: String,
    /// Reported IP address.
    pub ip_address: String,
    /// Advertised VU ceiling.
    pub max_vus: u32,
    /// Active virtual users.
    pub active_vus: u32,
    /// Current status.
    pub status: WorkerStatus,
    /// Operator labels.
    pub labels: HashMap<String, String>,
    /// Milliseconds since the last heartbeat.
    pub last_heartbeat_ms_ago: u64,
}

/// The set of registered workers.
#[derive(Default)]
pub struct Registry {
    workers: HashMap<String, WorkerRecord>,
}

impl Registry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of registered workers.
    pub fn len(&self) -> usize {
        self.workers.len()
    }

    /// Whether the registry is empty.
    pub fn is_empty(&self) -> bool {
        self.workers.is_empty()
    }

    /// Register (or re-register) a worker.
    pub fn register(&mut self, request: RegisterWorkerRequest) -> MasterResult<()> {
        if request.worker_id.trim().is_empty() {
            return Err(MasterError::Registry("worker_id must not be empty".into()));
        }
        if request.max_concurrency_vus == 0 {
            return Err(MasterError::Registry(
                "max_concurrency_vus must be greater than zero".into(),
            ));
        }
        let record = WorkerRecord {
            worker_id: request.worker_id.clone(),
            hostname: request.hostname,
            ip_address: request.ip_address,
            agent_version: request.agent_version,
            max_vus: request.max_concurrency_vus,
            capacity: request.capacity,
            labels: request.labels,
            status: WorkerStatus::Registered,
            active_vus: 0,
            test_run_id: String::new(),
            last_heartbeat: Instant::now(),
            command_tx: None,
        };
        self.workers.insert(request.worker_id, record);
        Ok(())
    }

    /// Apply a heartbeat, updating status/active VUs and liveness.
    pub fn record_heartbeat(
        &mut self,
        worker_id: &str,
        state: WorkerState,
        active_vus: u32,
        test_run_id: String,
    ) -> MasterResult<()> {
        let record = self
            .workers
            .get_mut(worker_id)
            .ok_or_else(|| MasterError::Registry(format!("unknown worker '{worker_id}'")))?;
        record.status = WorkerStatus::from_wire(state);
        record.active_vus = active_vus;
        record.test_run_id = test_run_id;
        record.last_heartbeat = Instant::now();
        Ok(())
    }

    /// Attach the command channel for a worker's `WatchCommands` stream.
    pub fn attach_commands(&mut self, worker_id: &str, tx: CommandSender) -> MasterResult<()> {
        let record = self
            .workers
            .get_mut(worker_id)
            .ok_or_else(|| MasterError::Registry(format!("unknown worker '{worker_id}'")))?;
        record.command_tx = Some(tx);
        Ok(())
    }

    /// Detach the command channel when a worker's stream closes.
    pub fn detach_commands(&mut self, worker_id: &str) {
        if let Some(record) = self.workers.get_mut(worker_id) {
            record.command_tx = None;
        }
    }

    /// Clone the command sender for a worker, if a stream is attached.
    pub fn command_sender(&self, worker_id: &str) -> Option<CommandSender> {
        self.workers
            .get(worker_id)
            .and_then(|record| record.command_tx.clone())
    }

    /// Override a worker's status.
    pub fn set_status(&mut self, worker_id: &str, status: WorkerStatus) {
        if let Some(record) = self.workers.get_mut(worker_id) {
            record.status = status;
        }
    }

    /// Ids of all workers currently reporting `READY`.
    pub fn ready_workers(&self) -> Vec<String> {
        self.workers
            .values()
            .filter(|w| w.status == WorkerStatus::Ready)
            .map(|w| w.worker_id.clone())
            .collect()
    }

    /// Ids of all workers currently reporting `RUNNING`.
    pub fn running_workers(&self) -> Vec<String> {
        self.workers
            .values()
            .filter(|w| w.status == WorkerStatus::Running)
            .map(|w| w.worker_id.clone())
            .collect()
    }

    /// A worker record by id.
    pub fn get(&self, worker_id: &str) -> Option<&WorkerRecord> {
        self.workers.get(worker_id)
    }

    /// Mark workers whose last heartbeat is older than `timeout` as
    /// `Disconnected`, dropping their command channel. Returns newly evicted ids.
    pub fn sweep(&mut self, timeout: Duration) -> Vec<String> {
        let now = Instant::now();
        let mut evicted = Vec::new();
        for record in self.workers.values_mut() {
            if record.status == WorkerStatus::Disconnected {
                continue;
            }
            if now.duration_since(record.last_heartbeat) > timeout {
                record.status = WorkerStatus::Disconnected;
                record.command_tx = None;
                evicted.push(record.worker_id.clone());
            }
        }
        evicted
    }

    /// A read-only snapshot of every registered worker.
    pub fn snapshot(&self) -> Vec<WorkerSnapshot> {
        let now = Instant::now();
        self.workers
            .values()
            .map(|w| WorkerSnapshot {
                worker_id: w.worker_id.clone(),
                hostname: w.hostname.clone(),
                ip_address: w.ip_address.clone(),
                max_vus: w.max_vus,
                active_vus: w.active_vus,
                status: w.status,
                labels: w.labels.clone(),
                last_heartbeat_ms_ago: now.duration_since(w.last_heartbeat).as_millis() as u64,
            })
            .collect()
    }
}

/// Background task that periodically sweeps stale workers from the registry.
pub async fn health_sweep_loop(
    registry: Arc<Mutex<Registry>>,
    interval: Duration,
    timeout: Duration,
    shutdown: CancellationToken,
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = ticker.tick() => {
                let evicted = registry.lock().await.sweep(timeout);
                for worker_id in evicted {
                    tracing::warn!(%worker_id, "worker missed heartbeats; marked disconnected");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn register_request(id: &str, max_vus: u32) -> RegisterWorkerRequest {
        RegisterWorkerRequest {
            worker_id: id.to_string(),
            hostname: "host".to_string(),
            ip_address: "127.0.0.1".to_string(),
            max_concurrency_vus: max_vus,
            agent_version: "0.0.0".to_string(),
            ..Default::default()
        }
    }

    #[test]
    fn register_then_ready_via_heartbeat() {
        let mut registry = Registry::new();
        registry.register(register_request("w1", 100)).unwrap();
        assert_eq!(registry.len(), 1);
        assert!(registry.ready_workers().is_empty());
        assert_eq!(registry.get("w1").unwrap().status, WorkerStatus::Registered);

        registry
            .record_heartbeat("w1", WorkerState::Ready, 0, String::new())
            .unwrap();
        assert_eq!(registry.ready_workers(), vec!["w1".to_string()]);
    }

    #[test]
    fn register_rejects_invalid() {
        let mut registry = Registry::new();
        assert!(registry.register(register_request("", 10)).is_err());
        assert!(registry.register(register_request("w1", 0)).is_err());
    }

    #[test]
    fn heartbeat_for_unknown_worker_fails() {
        let mut registry = Registry::new();
        assert!(
            registry
                .record_heartbeat("ghost", WorkerState::Ready, 0, String::new())
                .is_err()
        );
    }

    #[test]
    fn running_and_failed_states_are_mapped() {
        let mut registry = Registry::new();
        registry.register(register_request("w1", 10)).unwrap();
        registry
            .record_heartbeat("w1", WorkerState::Running, 5, "run-1".into())
            .unwrap();
        assert_eq!(registry.get("w1").unwrap().status, WorkerStatus::Running);
        assert_eq!(registry.get("w1").unwrap().active_vus, 5);
        assert_eq!(registry.running_workers(), vec!["w1".to_string()]);

        registry
            .record_heartbeat("w1", WorkerState::Error, 0, String::new())
            .unwrap();
        assert_eq!(registry.get("w1").unwrap().status, WorkerStatus::Failed);
    }

    #[test]
    fn commands_can_be_attached_and_detached() {
        let mut registry = Registry::new();
        registry.register(register_request("w1", 10)).unwrap();
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        registry.attach_commands("w1", tx).unwrap();
        assert!(registry.command_sender("w1").is_some());
        registry.detach_commands("w1");
        assert!(registry.command_sender("w1").is_none());
    }

    #[test]
    fn sweep_evicts_stale_workers() {
        let mut registry = Registry::new();
        registry.register(register_request("w1", 10)).unwrap();
        // last_heartbeat is "now"; a zero timeout marks it immediately stale.
        let evicted = registry.sweep(Duration::ZERO);
        assert_eq!(evicted, vec!["w1".to_string()]);
        assert_eq!(
            registry.get("w1").unwrap().status,
            WorkerStatus::Disconnected
        );
        // Already disconnected workers are not reported again.
        assert!(registry.sweep(Duration::ZERO).is_empty());
    }
}
