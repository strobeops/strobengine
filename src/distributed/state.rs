//! Worker lifecycle state machine.

use super::error::WorkerError;
use super::proto::WorkerState as ProtoWorkerState;

/// Lifecycle state of a worker as advertised to the master.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerState {
    /// Idle and available for a new run.
    Ready,
    /// Actively executing a test.
    Running,
    /// Finishing in-flight work; not accepting new runs.
    Draining,
    /// Faulted; requires attention before the worker is usable again.
    Error,
}

impl WorkerState {
    /// Map to the wire representation.
    pub fn as_proto(self) -> ProtoWorkerState {
        match self {
            WorkerState::Ready => ProtoWorkerState::Ready,
            WorkerState::Running => ProtoWorkerState::Running,
            WorkerState::Draining => ProtoWorkerState::Draining,
            WorkerState::Error => ProtoWorkerState::Error,
        }
    }

    /// Stable lower-case name used in logs and error messages.
    pub fn as_str(self) -> &'static str {
        match self {
            WorkerState::Ready => "READY",
            WorkerState::Running => "RUNNING",
            WorkerState::Draining => "DRAINING",
            WorkerState::Error => "ERROR",
        }
    }

    /// Whether a transition from `self` to `next` is legal.
    fn can_transition_to(self, next: WorkerState) -> bool {
        use WorkerState::*;
        matches!(
            (self, next),
            // Idle worker may start, drain, or fault.
            (Ready, Running)
                | (Ready, Draining)
                | (Ready, Error)
                // A running worker may finish (drain), fault, or be reset.
                | (Running, Draining)
                | (Running, Ready)
                | (Running, Error)
                // A draining worker returns to ready or faults.
                | (Draining, Ready)
                | (Draining, Error)
                // A faulted worker can be recovered.
                | (Error, Ready)
                | (Error, Draining)
        )
    }
}

/// Guarded state machine enforcing legal lifecycle transitions.
#[derive(Debug)]
pub struct WorkerStateMachine {
    state: WorkerState,
}

impl Default for WorkerStateMachine {
    fn default() -> Self {
        Self::new()
    }
}

impl WorkerStateMachine {
    /// A fresh worker starts idle.
    pub fn new() -> Self {
        Self {
            state: WorkerState::Ready,
        }
    }

    /// The current state.
    pub fn state(&self) -> WorkerState {
        self.state
    }

    /// Attempt a transition; a no-op transition to the current state succeeds.
    pub fn transition(&mut self, next: WorkerState) -> Result<(), WorkerError> {
        if next == self.state || self.state.can_transition_to(next) {
            self.state = next;
            Ok(())
        } else {
            Err(WorkerError::StateTransition(
                self.state.as_str().to_string(),
                next.as_str().to_string(),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn starts_ready() {
        let machine = WorkerStateMachine::new();
        assert_eq!(machine.state(), WorkerState::Ready);
    }

    #[test]
    fn legal_lifecycle_sequence() {
        let mut machine = WorkerStateMachine::new();
        machine.transition(WorkerState::Running).unwrap();
        machine.transition(WorkerState::Draining).unwrap();
        machine.transition(WorkerState::Ready).unwrap();
        assert_eq!(machine.state(), WorkerState::Ready);
    }

    #[test]
    fn same_state_is_a_noop() {
        let mut machine = WorkerStateMachine::new();
        machine.transition(WorkerState::Ready).unwrap();
        assert_eq!(machine.state(), WorkerState::Ready);
    }

    #[test]
    fn illegal_draining_to_running_is_rejected() {
        let mut machine = WorkerStateMachine::new();
        machine.transition(WorkerState::Draining).unwrap();
        let err = machine.transition(WorkerState::Running).unwrap_err();
        assert!(matches!(err, WorkerError::StateTransition(_, _)));
        // State is unchanged after a rejected transition.
        assert_eq!(machine.state(), WorkerState::Draining);
    }

    #[test]
    fn error_recovers_to_ready() {
        let mut machine = WorkerStateMachine::new();
        machine.transition(WorkerState::Error).unwrap();
        machine.transition(WorkerState::Ready).unwrap();
        assert_eq!(machine.state(), WorkerState::Ready);
    }

    #[test]
    fn state_names_and_proto_mapping() {
        assert_eq!(WorkerState::Ready.as_str(), "READY");
        assert_eq!(WorkerState::Running.as_str(), "RUNNING");
        assert_eq!(WorkerState::Running.as_proto(), ProtoWorkerState::Running);
        assert_eq!(WorkerState::Error.as_proto(), ProtoWorkerState::Error);
    }
}
