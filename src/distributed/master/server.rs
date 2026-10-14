//! gRPC service implementations for the master controller.

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;
use tonic::transport::server::TcpIncoming;
use tonic::{Request, Response, Status, Streaming};

use super::error::{MasterError, MasterResult};
use super::run::{MasterState, now_timestamp};
use crate::distributed::proto::master_controller_server::{
    MasterController, MasterControllerServer,
};
use crate::distributed::proto::telemetry_service_server::{
    TelemetryService, TelemetryServiceServer,
};
use crate::distributed::proto::{
    CommandRequest, CommandResponse, HeartbeatDirective, HeartbeatRequest, HeartbeatResponse,
    MetricBatch, RegisterWorkerRequest, RegisterWorkerResponse, RegistrationStatus,
    StreamAcknowledgment, WatchCommandsRequest, WorkerState,
};

/// gRPC service backed by the shared master state.
#[derive(Clone)]
pub struct MasterService {
    state: Arc<MasterState>,
}

impl MasterService {
    /// Wrap the shared master state.
    pub fn new(state: Arc<MasterState>) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl MasterController for MasterService {
    async fn register_worker(
        &self,
        request: Request<RegisterWorkerRequest>,
    ) -> Result<Response<RegisterWorkerResponse>, Status> {
        let request = request.into_inner();
        let worker_id = request.worker_id.clone();
        tracing::info!(%worker_id, hostname = %request.hostname, "worker registration");
        self.state
            .registry()
            .lock()
            .await
            .register(request)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        self.state.notify_change();
        Ok(Response::new(RegisterWorkerResponse {
            status: RegistrationStatus::Accepted as i32,
            acknowledgment_id: format!("ack-{worker_id}"),
            cluster_config: Some(self.state.cluster_configuration()),
            message: String::new(),
        }))
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let request = request.into_inner();
        let state = WorkerState::try_from(request.state).unwrap_or(WorkerState::Unspecified);
        self.state
            .registry()
            .lock()
            .await
            .record_heartbeat(
                &request.worker_id,
                state,
                request.active_vus,
                request.test_run_id.clone(),
            )
            .map_err(|_| {
                Status::not_found(format!("worker '{}' not registered", request.worker_id))
            })?;
        self.state.notify_change();
        Ok(Response::new(HeartbeatResponse {
            directive: HeartbeatDirective::Continue as i32,
            next_heartbeat_interval_ms: self.state.config().heartbeat_interval_ms,
            message: String::new(),
            pending_command_id: String::new(),
            server_time: Some(now_timestamp()),
        }))
    }

    type WatchCommandsStream = UnboundedReceiverStream<Result<CommandRequest, Status>>;

    async fn watch_commands(
        &self,
        request: Request<WatchCommandsRequest>,
    ) -> Result<Response<Self::WatchCommandsStream>, Status> {
        let request = request.into_inner();
        let (tx, rx) = mpsc::unbounded_channel();
        self.state
            .registry()
            .lock()
            .await
            .attach_commands(&request.worker_id, tx)
            .map_err(|_| {
                Status::not_found(format!("worker '{}' not registered", request.worker_id))
            })?;
        tracing::info!(worker_id = %request.worker_id, "command stream attached");
        Ok(Response::new(UnboundedReceiverStream::new(rx)))
    }

    async fn acknowledge_command(
        &self,
        request: Request<CommandResponse>,
    ) -> Result<Response<()>, Status> {
        let response = request.into_inner();
        tracing::debug!(
            worker_id = %response.worker_id,
            command_id = %response.command_id,
            status = response.status,
            "command acknowledged"
        );
        self.state.record_ack(&response).await;
        Ok(Response::new(()))
    }
}

#[tonic::async_trait]
impl TelemetryService for MasterService {
    async fn stream_metrics(
        &self,
        request: Request<Streaming<MetricBatch>>,
    ) -> Result<Response<StreamAcknowledgment>, Status> {
        let mut stream = request.into_inner();
        let mut accepted_batches = 0u32;
        let mut accepted_requests = 0u64;
        let mut last_batch_id = String::new();
        let mut worker_id = String::new();

        while let Some(batch) = stream.message().await? {
            accepted_batches += 1;
            accepted_requests += batch.total_requests;
            last_batch_id = batch.batch_id.clone();
            if !batch.worker_id.is_empty() {
                worker_id = batch.worker_id.clone();
            }
            self.state.ingest_batch(&batch).await;
        }

        // Stream half-close marks this worker's telemetry as complete.
        self.state.mark_telemetry_done(&worker_id).await;
        tracing::info!(%worker_id, accepted_batches, accepted_requests, "telemetry stream closed");

        Ok(Response::new(StreamAcknowledgment {
            worker_id,
            accepted_batches,
            accepted_requests,
            resync_required: false,
            message: String::new(),
            processed_at: Some(now_timestamp()),
            last_batch_id,
        }))
    }
}

/// Serve the master control plane on `addr` until `shutdown` is cancelled.
pub async fn serve(
    state: Arc<MasterState>,
    addr: SocketAddr,
    shutdown: CancellationToken,
) -> MasterResult<()> {
    let incoming = TcpIncoming::bind(addr).map_err(MasterError::Io)?;
    let service = MasterService::new(state);
    Server::builder()
        .add_service(MasterControllerServer::new(service.clone()))
        .add_service(TelemetryServiceServer::new(service))
        .serve_with_incoming_shutdown(incoming, async move { shutdown.cancelled().await })
        .await
        .map_err(MasterError::Transport)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed::master::config::MasterConfig;
    use crate::distributed::proto::master_controller_client::MasterControllerClient;
    use clap::Parser;

    fn free_port() -> u16 {
        std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    }

    #[tokio::test]
    async fn registration_round_trips_over_grpc() {
        let port = free_port();
        let config =
            MasterConfig::try_parse_from(["m", "--bind", "127.0.0.1", "--port", &port.to_string()])
                .unwrap();
        let addr = config.listen_addr().unwrap();
        let state = MasterState::new(config).unwrap();

        let shutdown = CancellationToken::new();
        let server = {
            let state = Arc::clone(&state);
            let shutdown = shutdown.clone();
            tokio::spawn(async move { serve(state, addr, shutdown).await })
        };

        // Wait for the server to accept connections.
        let endpoint = format!("http://{addr}");
        let mut client = None;
        for _ in 0..100 {
            match MasterControllerClient::connect(endpoint.clone()).await {
                Ok(c) => {
                    client = Some(c);
                    break;
                }
                Err(_) => tokio::time::sleep(std::time::Duration::from_millis(20)).await,
            }
        }
        let mut client = client.expect("connect to master");

        let response = client
            .register_worker(RegisterWorkerRequest {
                worker_id: "w1".into(),
                hostname: "h".into(),
                max_concurrency_vus: 10,
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(response.status, RegistrationStatus::Accepted as i32);
        assert!(response.cluster_config.is_some());
        assert_eq!(state.registry().lock().await.len(), 1);

        shutdown.cancel();
        let _ = server.await;
    }
}
