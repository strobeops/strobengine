//! Distributed load-testing control plane.
//!
//! This module hosts the wire types generated from
//! `proto/loadtesting/v1/load_test_controller.proto` (see `build.rs`) plus the
//! worker daemon that registers with a master, executes commands, and streams
//! telemetry back.

#[allow(clippy::all, dead_code)]
pub mod proto {
    // Generated from the `loadtesting.v1` package; file name is
    // `<package>.rs` inside `OUT_DIR`.
    tonic::include_proto!("loadtesting.v1");
}

pub mod backoff;
pub mod config;
pub mod error;
pub mod state;

pub use backoff::Backoff;
pub use config::WorkerConfig;
pub use error::{WorkerError, WorkerResult};
pub use state::{WorkerState, WorkerStateMachine};

#[cfg(test)]
mod tests {
    use prost::Message;

    use super::proto;

    #[test]
    fn metric_batch_round_trips() {
        let batch = proto::MetricBatch {
            worker_id: "worker-1".to_string(),
            test_run_id: "run-1".to_string(),
            batch_id: "42".to_string(),
            window_start_unix_ms: 1_700_000_000_000,
            window_duration_ms: 1000,
            total_requests: 100,
            success_count: 98,
            failure_count: 2,
            ..Default::default()
        };

        let mut buf = Vec::new();
        batch.encode(&mut buf).unwrap();
        let decoded = proto::MetricBatch::decode(&buf[..]).unwrap();

        assert_eq!(decoded.worker_id, "worker-1");
        assert_eq!(decoded.total_requests, 100);
        assert_eq!(decoded.failure_count, 2);
        assert_eq!(decoded.window_duration_ms, 1000);
    }

    #[test]
    fn generated_clients_and_servers_exist() {
        // Referencing the generated stubs proves client/server codegen ran; no
        // connection is established.
        let _master = std::any::type_name::<
            proto::master_controller_client::MasterControllerClient<tonic::transport::Channel>,
        >();
        let _telemetry = std::any::type_name::<
            proto::telemetry_service_client::TelemetryServiceClient<tonic::transport::Channel>,
        >();
        let _ = std::any::type_name::<proto::CommandType>();
    }
}
