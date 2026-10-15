//! Master controller: cluster registry, barrier coordination, telemetry
//! aggregation and the gRPC control-plane server.

pub mod aggregator;
pub mod barrier;
pub mod config;
pub mod error;
#[cfg(test)]
mod integration;
pub mod registry;
pub mod reporting;
pub mod run;
pub mod server;

pub use aggregator::RunAggregator;
pub use barrier::BarrierCoordinator;
pub use config::MasterConfig;
pub use error::{MasterError, MasterResult};
pub use registry::{CommandSender, Registry, WorkerSnapshot, WorkerStatus};
pub use reporting::{
    ErrorClassCount, GlobalLatency, GlobalReport, ReportDestination, TimelinePoint, WorkerShare,
};
pub use run::{MasterHandle, MasterState};
