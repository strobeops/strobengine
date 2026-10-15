//! Multi-worker integration tests: a real master controller, real worker
//! daemons, and a deterministic local HTTP target — all on ephemeral loopback
//! ports with no external dependencies.

use std::sync::atomic::Ordering;
use std::time::Duration;

use clap::Parser;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::distributed::config::WorkerConfig;
use crate::distributed::proto::{TargetEndpoint, TestConfiguration};
use crate::distributed::test_support::spawn_http_target;
use crate::distributed::worker::Worker;

use super::{MasterConfig, MasterError, MasterHandle};

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn master_config(port: u16) -> MasterConfig {
    MasterConfig::try_parse_from([
        "strobengine-master",
        "--bind",
        "127.0.0.1",
        "--port",
        &port.to_string(),
        "--heartbeat-interval-ms",
        "50",
        "--worker-timeout-ms",
        "2000",
        "--barrier-timeout-ms",
        "15000",
    ])
    .unwrap()
}

async fn spawn_worker(endpoint: &str, id: &str) -> (CancellationToken, JoinHandle<()>) {
    let config = WorkerConfig {
        master_endpoint: endpoint.to_string(),
        worker_id: Some(id.to_string()),
        hostname: Some(id.to_string()),
        ip_address: Some("127.0.0.1".to_string()),
        max_vus: 8,
        labels: Vec::new(),
        heartbeat_interval_ms: 50,
        metric_window_ms: 100,
        request_timeout_ms: 1_000,
        tls: false,
    };
    let worker = Worker::new(config).unwrap();
    let token = CancellationToken::new();
    let run_token = token.clone();
    let handle = tokio::spawn(async move {
        let _ = worker.run(run_token).await;
    });
    (token, handle)
}

async fn wait_for_workers(handle: &MasterHandle, count: usize, timeout: Duration) {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if handle.worker_count().await >= count {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {count} workers"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_worker_cluster_run() {
    let port = free_port();
    let handle = MasterHandle::new(master_config(port)).unwrap();
    let endpoint = format!("http://127.0.0.1:{port}");

    let shutdown = CancellationToken::new();
    let server = {
        let handle = handle.clone();
        let shutdown = shutdown.clone();
        tokio::spawn(async move { handle.serve(shutdown).await })
    };

    let (http_url, hits, http_cancel, http_handle) = spawn_http_target().await;

    let mut workers = Vec::new();
    for index in 0..3 {
        workers.push(spawn_worker(&endpoint, &format!("w{index}")).await);
    }
    wait_for_workers(&handle, 3, Duration::from_secs(10)).await;

    let config = TestConfiguration {
        test_run_id: "cluster-run".to_string(),
        targets: vec![TargetEndpoint {
            url: http_url.clone(),
            ..Default::default()
        }],
        duration: Some(prost_types::Duration {
            seconds: 2,
            nanos: 0,
        }),
        virtual_users: 2,
        request_timeout_ms: 1_000,
        ..Default::default()
    };

    let report = tokio::time::timeout(Duration::from_secs(40), handle.start_run(config, 3))
        .await
        .expect("run completes within timeout")
        .expect("run succeeds");

    assert_eq!(report.test_run_id, "cluster-run");
    assert_eq!(report.worker_count, 3, "all workers should contribute");
    assert!(report.total_requests > 0, "no requests aggregated");
    assert_eq!(
        report.total_requests,
        report.success_count + report.failure_count
    );
    assert!(hits.load(Ordering::SeqCst) > 0, "target was not hit");
    assert_eq!(report.latency.bucket_counts.len(), 10);
    assert!(report.rps > 0.0);

    for (token, task) in workers {
        token.cancel();
        let _ = task.await;
    }
    shutdown.cancel();
    let _ = server.await;
    http_cancel.cancel();
    let _ = http_handle.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn barrier_times_out_with_too_few_workers() {
    let port = free_port();
    let mut config = master_config(port);
    config.barrier_timeout_ms = 200;
    let handle = MasterHandle::new(config).unwrap();

    let test = TestConfiguration {
        duration: Some(prost_types::Duration {
            seconds: 1,
            nanos: 0,
        }),
        ..Default::default()
    };
    let err = handle.start_run(test, 5).await.unwrap_err();
    assert!(matches!(err, MasterError::BarrierTimeout { .. }));
}
