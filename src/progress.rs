use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use indicatif::{ProgressBar, ProgressStyle};
use tokio_util::sync::CancellationToken;

use crate::metrics::{LiveCounters, MICROS_PER_MILLI};

/// Target update rate for the terminal UI tick loop.
const RENDER_LOOP_SLEEP_MS: u64 = 200;

/// Indicatif progress bar total resolution ticks.
const PROGRESS_BAR_MAX_TICKS: u64 = 100;

pub fn create_progress_bar() -> ProgressBar {
    let pb = ProgressBar::new(PROGRESS_BAR_MAX_TICKS);
    pb.set_style(
        ProgressStyle::with_template(
            "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/dim}] {pos}% | {msg}",
        )
        .expect("valid progress bar template")
        .progress_chars("=>-"),
    );
    pb.set_message("Starting load test...");
    pb.enable_steady_tick(Duration::from_millis(100));
    pb
}

pub async fn render_loop(
    pb: ProgressBar,
    counters: Arc<LiveCounters>,
    start: Instant,
    total_duration: Duration,
    cancel_token: CancellationToken,
) {
    let mut prev_completed = 0u64;
    let mut last_tick = Instant::now();

    loop {
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(RENDER_LOOP_SLEEP_MS)) => {
                let now = Instant::now();
                let delta_sec = (now - last_tick).as_secs_f64();
                last_tick = now;

                let elapsed = start.elapsed();
                let active = counters.active_workers.load(Ordering::Relaxed);
                let completed = counters.completed_requests.load(Ordering::Relaxed);

                // Exit cleanly if test complete or no workers remain
                if elapsed >= total_duration || (active == 0 && completed > 0) {
                    break;
                }

                // Progress percentage based on elapsed time
                let pct = if total_duration.is_zero() {
                    0
                } else {
                    (elapsed.as_secs_f64() / total_duration.as_secs_f64() * 100.0) as u64
                };
                pb.set_position(pct.min(99));

                // Sample metrics
                let errors = counters.errors.load(Ordering::Relaxed);
                let latency_sum = counters.latency_sum_micros.load(Ordering::Relaxed);
                let latency_count = counters.latency_count.load(Ordering::Relaxed);

                // Instantaneous RPS over tick interval
                let delta_completed = completed.saturating_sub(prev_completed);
                let instant_rps = if delta_sec > 0.0 {
                    delta_completed as f64 / delta_sec
                } else {
                    0.0
                };

                // Average latency across all completed requests
                let avg_latency_ms = if latency_count > 0 {
                    latency_sum as f64 / latency_count as f64 / MICROS_PER_MILLI
                } else {
                    0.0
                };

                pb.set_message(format!(
                    "{:.0} req/s | {} VUs | {} err | avg {:.1}ms",
                    instant_rps, active, errors, avg_latency_ms
                ));

                prev_completed = completed;
            }
            _ = cancel_token.cancelled() => {
                pb.abandon_with_message("Interrupted -- benchmark stopped early");
                return;
            }
        }
    }

    let final_completed = counters.completed_requests.load(Ordering::Relaxed);
    let final_errors = counters.errors.load(Ordering::Relaxed);
    pb.set_position(100);
    pb.finish_with_message(format!(
        "Finished: {} reqs | {} errors",
        final_completed, final_errors
    ));
}
