//! strobengine distributed master controller daemon.
//!
//! Serves the worker-facing gRPC control plane and, optionally, drives a single
//! distributed run while rendering a live terminal dashboard and writing an
//! aggregated JSON report. Configure via CLI flags or the `STROBENGINE_*`
//! environment variables; see `--help`.

use std::io::IsTerminal;
use std::time::Duration;

use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

use _strobengine::distributed::master::{
    GlobalReport, LiveSnapshot, MasterConfig, MasterHandle, ReportDestination,
};
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

/// Wait for the expected workers, run the test once while rendering a live
/// dashboard, and emit the aggregated report.
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

    // Live dashboard: subscribe before starting so no snapshot is missed.
    let live = handle.subscribe_live();
    let show_live = !config.no_live && std::io::stdout().is_terminal();
    let bar = if show_live {
        create_live_bar(config.run_duration_secs)
    } else {
        ProgressBar::hidden()
    };
    let render = if show_live {
        Some(tokio::spawn(render_live(bar.clone(), live)))
    } else {
        None
    };

    let result = handle.start_run(test, config.expected_workers).await;
    if let Some(render) = render {
        render.abort();
    }
    bar.finish_and_clear();
    let report = result?;

    print_summary(&report);
    let destination = ReportDestination::parse(config.report_path.as_deref());
    destination.write(&report)?;
    if let ReportDestination::File(path) = &destination {
        println!("report written to {}", path.display());
    }
    Ok(())
}

/// Build the live progress bar (total = run duration in seconds).
fn create_live_bar(duration_secs: u64) -> ProgressBar {
    let bar = ProgressBar::new(duration_secs.max(1));
    bar.set_style(
        ProgressStyle::with_template(
            "{spinner:.green} [{elapsed_precise}] [{bar:32.cyan/dim}] {pos}s/{len}s | {msg}",
        )
        .unwrap()
        .progress_chars("=>-"),
    );
    bar.set_message("waiting for workers...");
    bar.enable_steady_tick(Duration::from_millis(200));
    bar
}

/// Consume live snapshots and update the progress bar until the run ends.
async fn render_live(bar: ProgressBar, mut live: broadcast::Receiver<LiveSnapshot>) {
    loop {
        match live.recv().await {
            Ok(snapshot) => {
                bar.set_position(snapshot.elapsed_secs.round() as u64);
                bar.set_message(format!(
                    "{:.0} rps | {:.2}% err | p50 {:.1}ms p95 {:.1}ms p99 {:.1}ms | {} workers",
                    snapshot.rps,
                    snapshot.error_rate_percent,
                    snapshot.p50_ms,
                    snapshot.p95_ms,
                    snapshot.p99_ms,
                    snapshot.active_workers,
                ));
            }
            // A dashboard may skip frames when it falls behind.
            Err(broadcast::error::RecvError::Lagged(_)) => continue,
            Err(broadcast::error::RecvError::Closed) => break,
        }
    }
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
