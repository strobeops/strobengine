//! Master controller: cluster registry, barrier coordination, telemetry
//! aggregation and the gRPC control-plane server.

pub mod config;
pub mod error;
pub mod registry;

pub use config::MasterConfig;
pub use error::{MasterError, MasterResult};
pub use registry::{CommandSender, Registry, WorkerSnapshot, WorkerStatus};
