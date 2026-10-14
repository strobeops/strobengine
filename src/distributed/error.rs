//! Error types for the distributed worker daemon.

use thiserror::Error;

/// Errors surfaced by the worker runtime.
#[derive(Debug, Error)]
pub enum WorkerError {
    /// The configuration is invalid.
    #[error("configuration error: {0}")]
    Config(String),

    /// An illegal worker state transition was attempted.
    #[error("invalid state transition: {0} -> {1}")]
    StateTransition(String, String),

    /// Establishing or maintaining the master channel failed.
    #[error("transport error: {0}")]
    Transport(#[from] tonic::transport::Error),

    /// A gRPC call returned a non-OK status.
    #[error("rpc error: {0}")]
    Rpc(#[from] tonic::Status),

    /// Local I/O failed.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// The master connection was lost and must be re-established.
    #[error("master connection lost: {0}")]
    Disconnected(String),
}

/// Convenience alias used throughout the worker runtime.
pub type WorkerResult<T> = Result<T, WorkerError>;
