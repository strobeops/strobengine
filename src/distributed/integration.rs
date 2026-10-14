//! End-to-end integration tests for the worker daemon.
//!
//! Each test spins up an in-process fake master (real generated tonic
//! services) and a deterministic local HTTP target, then drives the real
//! [`Worker`] against them. Everything binds to ephemeral loopback ports and
//! synchronization is event-driven (channels + bounded timeouts), never sleeps.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use std::sync::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::{Server, server::TcpIncoming};
use tonic::{Request, Response, Status, Streaming};

use super::config::WorkerConfig;
use super::proto::master_controller_server::{MasterController, MasterControllerServer};
use super::proto::telemetry_service_server::{TelemetryService, TelemetryServiceServer};
use super::proto::{
    ClusterConfiguration, CommandRequest, CommandResponse, CommandStatus, CommandType,
    HeartbeatDirective, HeartbeatRequest, HeartbeatResponse, MetricBatch, RegisterWorkerRequest,
    RegisterWorkerResponse, RegistrationStatus, StreamAcknowledgment, TargetEndpoint,
    TestConfiguration, WatchCommandsRequest,
};
use super::state::WorkerState;
use super::worker::Worker;

const WAIT: Duration = Duration::from_secs(5);

fn timestamp() -> prost_types::Timestamp {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    prost_types::Timestamp {
        seconds: now.as_secs() as i64,
        nanos: now.subsec_nanos() as i32,
    }
}

/// Events recorded by the fake master for test assertions.
#[derive(Debug)]
enum MasterEvent {
    Registered(RegisterWorkerRequest),
    CommandsOpen,
    Heartbeat(HeartbeatRequest),
    CommandAcked(CommandResponse),
    Batch(MetricBatch),
}

struct FakeInner {
    events: mpsc::UnboundedSender<MasterEvent>,
    commands: Mutex<Option<mpsc::UnboundedSender<Result<CommandRequest, Status>>>>,
    heartbeat_ms: u32,
}

/// In-process master controller with injectable command delivery.
#[derive(Clone)]
struct FakeMaster {
    inner: Arc<FakeInner>,
}

impl FakeMaster {
    fn new(heartbeat_ms: u32) -> (Self, mpsc::UnboundedReceiver<MasterEvent>) {
        let (events, rx) = mpsc::unbounded_channel();
        let master = Self {
            inner: Arc::new(FakeInner {
                events,
                commands: Mutex::new(None),
                heartbeat_ms,
            }),
        };
        (master, rx)
    }

    /// Deliver a command to the worker's open command stream.
    fn send_command(&self, command: CommandRequest) {
        if let Some(tx) = self.inner.commands.lock().unwrap().as_ref() {
            let _ = tx.send(Ok(command));
        }
    }
}

#[tonic::async_trait]
impl MasterController for FakeMaster {
    async fn register_worker(
        &self,
        request: Request<RegisterWorkerRequest>,
    ) -> Result<Response<RegisterWorkerResponse>, Status> {
        let request = request.into_inner();
        let _ = self.inner.events.send(MasterEvent::Registered(request));
        Ok(Response::new(RegisterWorkerResponse {
            status: RegistrationStatus::Accepted as i32,
            acknowledgment_id: "ack-1".to_string(),
            cluster_config: Some(ClusterConfiguration {
                cluster_id: "test-cluster".to_string(),
                heartbeat_interval_ms: self.inner.heartbeat_ms,
                metric_flush_interval_ms: 100,
                ..Default::default()
            }),
            message: String::new(),
        }))
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let request = request.into_inner();
        let _ = self.inner.events.send(MasterEvent::Heartbeat(request));
        Ok(Response::new(HeartbeatResponse {
            directive: HeartbeatDirective::Continue as i32,
            next_heartbeat_interval_ms: self.inner.heartbeat_ms,
            ..Default::default()
        }))
    }

    type WatchCommandsStream = UnboundedReceiverStream<Result<CommandRequest, Status>>;

    async fn watch_commands(
        &self,
        _request: Request<WatchCommandsRequest>,
    ) -> Result<Response<Self::WatchCommandsStream>, Status> {
        let (tx, rx) = mpsc::unbounded_channel();
        *self.inner.commands.lock().unwrap() = Some(tx);
        let _ = self.inner.events.send(MasterEvent::CommandsOpen);
        Ok(Response::new(UnboundedReceiverStream::new(rx)))
    }

    async fn acknowledge_command(
        &self,
        request: Request<CommandResponse>,
    ) -> Result<Response<()>, Status> {
        let _ = self
            .inner
            .events
            .send(MasterEvent::CommandAcked(request.into_inner()));
        Ok(Response::new(()))
    }
}

#[tonic::async_trait]
impl TelemetryService for FakeMaster {
    async fn stream_metrics(
        &self,
        request: Request<Streaming<MetricBatch>>,
    ) -> Result<Response<StreamAcknowledgment>, Status> {
        let mut stream = request.into_inner();
        let mut accepted_batches = 0u32;
        let mut accepted_requests = 0u64;
        let mut last_batch_id = String::new();
        while let Some(batch) = stream.message().await? {
            accepted_batches += 1;
            accepted_requests += batch.total_requests;
            last_batch_id = batch.batch_id.clone();
            let _ = self.inner.events.send(MasterEvent::Batch(batch));
        }
        Ok(Response::new(StreamAcknowledgment {
            accepted_batches,
            accepted_requests,
            last_batch_id,
            processed_at: Some(timestamp()),
            ..Default::default()
        }))
    }
}

/// A bundle of the fake master + local HTTP target with owned shutdown handles.
struct TestCluster {
    master_addr: String,
    master: FakeMaster,
    events: mpsc::UnboundedReceiver<MasterEvent>,
    http_url: String,
    http_hits: Arc<AtomicUsize>,
    server_cancel: CancellationToken,
    server_handle: JoinHandle<()>,
    http_cancel: CancellationToken,
    http_handle: JoinHandle<()>,
}

impl TestCluster {
    async fn start(heartbeat_ms: u32) -> Self {
        let (master, events) = FakeMaster::new(heartbeat_ms);

        let incoming = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let master_addr = format!("http://{}", incoming.local_addr().unwrap());
        let server_cancel = CancellationToken::new();
        let server_signal = server_cancel.clone();
        let server_master = master.clone();
        let server_handle = tokio::spawn(async move {
            let _ = Server::builder()
                .add_service(MasterControllerServer::new(server_master.clone()))
                .add_service(TelemetryServiceServer::new(server_master))
                .serve_with_incoming_shutdown(
                    incoming,
                    async move { server_signal.cancelled().await },
                )
                .await;
        });

        let (http_url, http_hits, http_cancel, http_handle) = spawn_http_target().await;

        Self {
            master_addr,
            master,
            events,
            http_url,
            http_hits,
            server_cancel,
            server_handle,
            http_cancel,
            http_handle,
        }
    }

    async fn shutdown(self) {
        self.server_cancel.cancel();
        self.http_cancel.cancel();
        let _ = self.server_handle.await;
        let _ = self.http_handle.await;
    }

    /// Wait for the next event matching `pred`, ignoring unrelated events.
    async fn wait_for<F>(&mut self, pred: F) -> MasterEvent
    where
        F: Fn(&MasterEvent) -> bool,
    {
        let deadline = tokio::time::Instant::now() + WAIT;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            match tokio::time::timeout(remaining, self.events.recv()).await {
                Ok(Some(event)) if pred(&event) => return event,
                Ok(Some(_)) => continue,
                Ok(None) => panic!("master event channel closed"),
                Err(_) => panic!("timed out waiting for master event"),
            }
        }
    }

    async fn wait_for_registration(&mut self) -> RegisterWorkerRequest {
        match self
            .wait_for(|e| matches!(e, MasterEvent::Registered(_)))
            .await
        {
            MasterEvent::Registered(req) => req,
            _ => unreachable!(),
        }
    }

    async fn wait_for_commands_open(&mut self) {
        let _ = self
            .wait_for(|e| matches!(e, MasterEvent::CommandsOpen))
            .await;
    }

    async fn wait_for_heartbeat(&mut self) -> HeartbeatRequest {
        match self
            .wait_for(|e| matches!(e, MasterEvent::Heartbeat(_)))
            .await
        {
            MasterEvent::Heartbeat(req) => req,
            _ => unreachable!(),
        }
    }

    async fn wait_for_batch(&mut self) -> MetricBatch {
        match self.wait_for(|e| matches!(e, MasterEvent::Batch(_))).await {
            MasterEvent::Batch(batch) => batch,
            _ => unreachable!(),
        }
    }

    async fn wait_for_ack(&mut self, command_id: &str) -> CommandResponse {
        let command_id = command_id.to_string();
        match self
            .wait_for(
                move |e| matches!(e, MasterEvent::CommandAcked(a) if a.command_id == command_id),
            )
            .await
        {
            MasterEvent::CommandAcked(ack) => ack,
            _ => unreachable!(),
        }
    }
}

fn worker_config(endpoint: &str, id: &str, max_vus: u32) -> WorkerConfig {
    WorkerConfig {
        master_endpoint: endpoint.to_string(),
        worker_id: Some(id.to_string()),
        hostname: Some("test-host".to_string()),
        ip_address: Some("127.0.0.1".to_string()),
        max_vus,
        labels: vec!["zone=test".to_string()],
        heartbeat_interval_ms: 50,
        metric_window_ms: 100,
        request_timeout_ms: 1_000,
        tls: false,
    }
}

fn start_command(run_id: &str, url: &str, command_id: &str) -> CommandRequest {
    CommandRequest {
        command: CommandType::Start as i32,
        command_id: command_id.to_string(),
        test_run_id: run_id.to_string(),
        config: Some(TestConfiguration {
            test_run_id: run_id.to_string(),
            targets: vec![TargetEndpoint {
                url: url.to_string(),
                ..Default::default()
            }],
            virtual_users: 2,
            request_timeout_ms: 1_000,
            duration: Some(prost_types::Duration {
                seconds: 5,
                nanos: 0,
            }),
            ..Default::default()
        }),
        issued_at: Some(timestamp()),
        command_timeout_ms: 0,
    }
}

fn stop_command(run_id: &str, command_id: &str) -> CommandRequest {
    CommandRequest {
        command: CommandType::Stop as i32,
        command_id: command_id.to_string(),
        test_run_id: run_id.to_string(),
        config: None,
        issued_at: Some(timestamp()),
        command_timeout_ms: 0,
    }
}

/// Spawn a minimal HTTP/1.1 responder on loopback that records each request.
async fn spawn_http_target() -> (String, Arc<AtomicUsize>, CancellationToken, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let hits = Arc::new(AtomicUsize::new(0));
    let cancel = CancellationToken::new();

    let hits_task = Arc::clone(&hits);
    let cancel_task = cancel.clone();
    let handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = cancel_task.cancelled() => break,
                accepted = listener.accept() => {
                    let Ok((mut socket, _)) = accepted else { break };
                    let hits = Arc::clone(&hits_task);
                    tokio::spawn(async move {
                        if serve_one(&mut socket).await.is_ok() {
                            hits.fetch_add(1, Ordering::SeqCst);
                        }
                    });
                }
            }
        }
    });

    (format!("http://{addr}"), hits, cancel, handle)
}

/// Serve exactly one HTTP/1.1 request on `socket` and close.
async fn serve_one(socket: &mut TcpStream) -> std::io::Result<()> {
    let mut buf: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 1024];

    let header_end = loop {
        if let Some(pos) = find_subslice(&buf, b"\r\n\r\n") {
            break pos + 4;
        }
        let n = socket.read(&mut chunk).await?;
        if n == 0 {
            return Ok(());
        }
        buf.extend_from_slice(&chunk[..n]);
    };

    let headers = String::from_utf8_lossy(&buf[..header_end]);
    let content_length = parse_content_length(&headers);
    let mut consumed_body = buf.len() - header_end;
    while consumed_body < content_length {
        let n = socket.read(&mut chunk).await?;
        if n == 0 {
            break;
        }
        consumed_body += n;
    }

    let body = b"{\"ok\":true}";
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n",
        body.len()
    );
    socket.write_all(response.as_bytes()).await?;
    socket.write_all(body).await?;
    socket.flush().await?;
    Ok(())
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn parse_content_length(headers: &str) -> usize {
    headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.trim()
                .eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().ok())
                .flatten()
        })
        .unwrap_or(0)
}

#[tokio::test]
async fn registration_is_received() {
    let mut cluster = TestCluster::start(50).await;
    let config = worker_config(&cluster.master_addr, "worker-a", 8);
    let worker = Worker::new(config).unwrap();

    let token = CancellationToken::new();
    let handle = tokio::spawn(worker.run(token.clone()));

    let registration = cluster.wait_for_registration().await;
    assert_eq!(registration.worker_id, "worker-a");
    assert_eq!(registration.max_concurrency_vus, 8);
    assert_eq!(registration.hostname, "test-host");
    assert_eq!(
        registration.labels.get("zone").map(String::as_str),
        Some("test")
    );
    assert!(registration.capacity.is_some());

    token.cancel();
    let _ = handle.await;
    cluster.shutdown().await;
}

#[tokio::test]
async fn heartbeat_reports_ready_state() {
    let mut cluster = TestCluster::start(50).await;
    let config = worker_config(&cluster.master_addr, "worker-b", 4);
    let worker = Worker::new(config).unwrap();

    let token = CancellationToken::new();
    let handle = tokio::spawn(worker.run(token.clone()));

    // The worker is idle here, so its heartbeats must report READY.
    let heartbeat = cluster.wait_for_heartbeat().await;
    assert_eq!(heartbeat.worker_id, "worker-b");
    assert_eq!(heartbeat.state, WorkerState::Ready.as_proto() as i32);
    assert_eq!(heartbeat.active_vus, 0);
    assert!(heartbeat.usage.is_some());

    token.cancel();
    let _ = handle.await;
    cluster.shutdown().await;
}

#[tokio::test]
async fn full_control_plane_flow() {
    let mut cluster = TestCluster::start(50).await;
    let config = worker_config(&cluster.master_addr, "worker-c", 8);
    let worker = Worker::new(config).unwrap();

    let token = CancellationToken::new();
    let handle = tokio::spawn(worker.run(token.clone()));

    // 1. Register, then the command stream opens.
    let _ = cluster.wait_for_registration().await;
    cluster.wait_for_commands_open().await;
    let _ = cluster.wait_for_heartbeat().await;

    // 2. Deliver START; the worker must acknowledge it.
    cluster
        .master
        .send_command(start_command("run-1", &cluster.http_url, "cmd-start"));
    let ack = cluster.wait_for_ack("cmd-start").await;
    assert_eq!(ack.status, CommandStatus::Acknowledged as i32);

    // 3. The local HTTP target is actually hit by the virtual users.
    let batch = cluster.wait_for_batch().await;
    assert!(
        cluster.http_hits.load(Ordering::SeqCst) > 0,
        "target was not hit"
    );

    // 4. Telemetry carries the run identifiers and a fixed-width histogram.
    assert_eq!(batch.worker_id, "worker-c");
    assert_eq!(batch.test_run_id, "run-1");
    assert!(batch.total_requests > 0);
    assert!(batch.success_count >= 1);
    let latency = batch.latency.expect("latency summary present");
    assert_eq!(latency.bucket_counts.len(), 10);
    assert!(latency.p99_ms >= latency.p50_ms);

    // 5. STOP is acknowledged, and the run winds down.
    cluster
        .master
        .send_command(stop_command("run-1", "cmd-stop"));
    let stop_ack = cluster.wait_for_ack("cmd-stop").await;
    assert_eq!(stop_ack.status, CommandStatus::Completed as i32);

    token.cancel();
    let result = handle.await.expect("worker task joined");
    assert!(result.is_ok(), "worker exited with error: {result:?}");
    cluster.shutdown().await;
}

#[tokio::test]
async fn start_without_configuration_is_rejected() {
    let mut cluster = TestCluster::start(50).await;
    let config = worker_config(&cluster.master_addr, "worker-d", 4);
    let worker = Worker::new(config).unwrap();

    let token = CancellationToken::new();
    let handle = tokio::spawn(worker.run(token.clone()));

    let _ = cluster.wait_for_registration().await;
    cluster.wait_for_commands_open().await;

    let bad_start = CommandRequest {
        command: CommandType::Start as i32,
        command_id: "cmd-bad".to_string(),
        test_run_id: "run-bad".to_string(),
        config: None,
        issued_at: Some(timestamp()),
        command_timeout_ms: 0,
    };
    cluster.master.send_command(bad_start);
    let ack = cluster.wait_for_ack("cmd-bad").await;
    assert_eq!(ack.status, CommandStatus::Rejected as i32);
    assert!(!ack.error_message.is_empty());

    token.cancel();
    let _ = handle.await;
    cluster.shutdown().await;
}

#[test]
fn content_length_parsing_is_case_insensitive() {
    assert_eq!(parse_content_length("Content-Length: 42\r\nHost: x"), 42);
    assert_eq!(parse_content_length("content-length:7\r\n"), 7);
    assert_eq!(parse_content_length("Host: x"), 0);
}

#[test]
fn find_subslice_locates_separator() {
    assert_eq!(find_subslice(b"abc\r\n\r\nxyz", b"\r\n\r\n"), Some(3));
    assert_eq!(find_subslice(b"no separator", b"\r\n\r\n"), None);
}
