//! strobengine distributed master controller daemon.
//!
//! Serves the worker-facing gRPC control plane and, optionally, drives a single
//! distributed run and writes an aggregated JSON report. Configure via CLI
//! flags or the `STROBENGINE_*` environment variables; see `--help`.

use clap::Parser;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

use _strobengine::distributed::master::{GlobalReport, MasterConfig, MasterHandle};
use _strobengine::distributed::proto::{TargetEndpoint, TestConfiguration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let config = MasterConfig::parse();
    let listen = config.listen_addr()?;
    let handle = MasterHandle::new(config.clone())?;
    tracing::info!(%listen, cluster_id = %config.cluster_id, "starting strobengine master");

    let token = CancellationToken::new();
    let serve = {
        let handle = handle.clone();
        let token = token.clone();
        tokio::spawn(async move { handle.serve(token).await })
    };

    if config.has_run_spec() {
        run_once(&handle, &config).await?;
        token.cancel();
    } else {
        shutdown_signal().await;
        tracing::info!("shutdown signal received; stopping master");
        token.cancel();
    }

    serve.await??;
    tracing::info!("master stopped cleanly");
    Ok(())
}

/// Build the single-run test configuration from the CLI flags.
fn build_test_config(config: &MasterConfig) -> TestConfiguration {
    TestConfiguration {
        targets: vec![TargetEndpoint {
            url: config.run_url.clone().unwrap_or_default(),
            ..Default::default()
        }],
        duration: Some(prost_types::Duration {
            seconds: config.run_duration_secs as i64,
            nanos: 0,
        }),
        virtual_users: config.run_virtual_users,
        ..Default::default()
    }
}

/// Wait for the expected workers, run the test once, and emit the report.
async fn run_once(
    handle: &MasterHandle,
    config: &MasterConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    let test = build_test_config(config);
    tracing::info!(
        url = %config.run_url.clone().unwrap_or_default(),
        expected_workers = config.expected_workers,
        "waiting for workers to start the run"
    );

    let report = handle.start_run(test, config.expected_workers).await?;
    print_summary(&report);

    if let Some(path) = &config.report_path {
        let json = serde_json::to_string_pretty(&report)?;
        std::fs::write(path, json)?;
        println!("report written to {path}");
    }
    Ok(())
}

fn print_summary(report: &GlobalReport) {
    println!(
        "run {}: {} workers, {} requests, {:.2} rps, {:.2}% errors, p95 {:.2} ms",
        report.test_run_id,
        report.worker_count,
        report.total_requests,
        report.rps,
        report.error_rate_percent,
        report.latency.p95_ms,
    );
}

/// Resolve on SIGINT (Ctrl+C) or SIGTERM.
async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
}
