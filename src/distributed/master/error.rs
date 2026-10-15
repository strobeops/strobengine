//! Error types for the master controller.

use thiserror::Error;

/// Errors surfaced by the master controller.
#[derive(Debug, Error)]
pub enum MasterError {
    /// The configuration is invalid.
    #[error("configuration error: {0}")]
    Config(String),

    /// A registry operation was rejected.
    #[error("registry error: {0}")]
    Registry(String),

    /// The barrier did not reach the required worker count in time.
    #[error("barrier timeout: {ready}/{expected} workers ready")]
    BarrierTimeout {
        /// Workers that were expected.
        expected: usize,
        /// Workers that were ready when the deadline elapsed.
        ready: usize,
    },

    /// A run could not be started or completed.
    #[error("run error: {0}")]
    Run(String),

    /// Establishing or serving the gRPC transport failed.
    #[error("transport error: {0}")]
    Transport(#[from] tonic::transport::Error),

    /// A gRPC call returned a non-OK status.
    #[error("rpc error: {0}")]
    Rpc(#[from] tonic::Status),

    /// Local I/O failed.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

/// Convenience alias used throughout the master runtime.
pub type MasterResult<T> = Result<T, MasterError>;
