//! strobengine distributed worker daemon.
//!
//! Connects to a master controller, registers, serves heartbeats, executes
//! lifecycle commands, and streams telemetry. Configure via CLI flags or the
//! `STROBENGINE_*` environment variables; see `--help`.

use clap::Parser;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

use _strobengine::distributed::{Worker, WorkerConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let config = WorkerConfig::parse();
    let worker = Worker::new(config)?;
    tracing::info!(
        worker_id = worker.worker_id(),
        "starting strobengine worker"
    );

    let token = CancellationToken::new();
    let run_token = token.clone();
    let mut run = tokio::spawn(async move { worker.run(run_token).await });

    tokio::select! {
        result = &mut run => {
            result??;
            return Ok(());
        }
        _ = shutdown_signal() => {
            tracing::info!("shutdown signal received; stopping worker");
            token.cancel();
        }
    }

    run.await??;
    tracing::info!("worker stopped cleanly");
    Ok(())
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
