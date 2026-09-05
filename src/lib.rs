use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

mod chaos;
mod config;
mod logging;
mod metrics;
mod progress;
pub mod protocols;
pub mod report;

use std::io::IsTerminal;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use http::Method;
use http::header::{CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue};
use pyo3::prelude::*;
use tokio_util::sync::CancellationToken;

use crate::chaos::ChaosEngine;
use crate::config::{LoadProfile, TestConfig};
use crate::metrics::{LiveCounters, RequestMetric};
use crate::protocols::ProtocolEngine;

/// Buffer capacity for the async MPSC channel streaming metrics from workers to the aggregator.
/// 8,192 (~8k) provides enough head room to prevent worker task backpressure during high RPS bursts
/// without allocating unnecessary heap memory (8,192 * std::mem::size_of::<RequestMetric>()).
const METRIC_CHANNEL_BUFFER: usize = 8192;

/// Client network configuration defaults.
const DEFAULT_CONNECT_TIMEOUT_SECS: u64 = 5;
const DEFAULT_H2_KEEPALIVE_INTERVAL_SECS: u64 = 10;
const DEFAULT_H2_KEEPALIVE_TIMEOUT_SECS: u64 = 5;

/// Supervisor loop poll rate in milliseconds.
const SUPERVISOR_TICK_MS: u64 = 200;

/// Represents the concurency model for the test run.
pub enum ConcurrencyStrategy {
    Constant {
        concurrency: usize,
        duration_secs: u64,
    },
    Dynamic {
        profile: LoadProfile,
    },
}

impl ConcurrencyStrategy {
    pub fn max_concurrency(&self) -> usize {
        match self {
            Self::Constant { concurrency, .. } => *concurrency,
            Self::Dynamic { profile } => profile.max_concurrency(),
        }
    }

    pub fn total_duration_secs(&self) -> u64 {
        match self {
            Self::Constant { duration_secs, .. } => *duration_secs,
            Self::Dynamic { profile } => profile.total_duration(),
        }
    }
}

fn parse_method(method_str: &str) -> PyResult<Method> {
    method_str.to_uppercase().parse().map_err(|_| {
        pyo3::exceptions::PyValueError::new_err(format!("Invalid HTTP method: {method_str}"))
    })
}

fn parse_body(body: Option<&str>) -> Option<bytes::Bytes> {
    body.map(|b| bytes::Bytes::copy_from_slice(b.as_bytes()))
}

fn parse_form(form: Option<&[(String, String)]>) -> Option<bytes::Bytes> {
    form.map(|pairs| {
        let encoded: String = pairs
            .iter()
            .map(|(k, v)| format!("{}={}", urlencoding::encode(k), urlencoding::encode(v)))
            .collect::<Vec<_>>()
            .join("&");
        bytes::Bytes::from(encoded)
    })
}

fn parse_headers(headers: Option<&[(String, String)]>) -> PyResult<HeaderMap> {
    let mut header_map = HeaderMap::new();

    if let Some(h) = headers {
        for (k, v) in h {
            let name = HeaderName::from_bytes(k.as_bytes()).map_err(|e| {
                pyo3::exceptions::PyValueError::new_err(format!("Invalid header name '{k}': {e}"))
            })?;

            let val = HeaderValue::from_str(v).map_err(|e| {
                pyo3::exceptions::PyValueError::new_err(format!(
                    "Invalid header value for '{k}': {e}"
                ))
            })?;

            header_map.append(name, val);
        }
    }
    Ok(header_map)
}

fn build_client(
    concurrency: usize,
    timeout_secs: u64,
    header_map: HeaderMap,
) -> Result<reqwest::Client, reqwest::Error> {
    reqwest::Client::builder()
        .pool_max_idle_per_host(concurrency)
        .timeout(Duration::from_secs(timeout_secs))
        .connect_timeout(Duration::from_secs(DEFAULT_CONNECT_TIMEOUT_SECS))
        .tcp_nodelay(true)
        .http2_keep_alive_interval(Duration::from_secs(DEFAULT_H2_KEEPALIVE_INTERVAL_SECS))
        .http2_keep_alive_timeout(Duration::from_secs(DEFAULT_H2_KEEPALIVE_TIMEOUT_SECS))
        .default_headers(header_map)
        .build()
}

#[pyfunction]
fn init_logging(level: String, log_file: Option<String>) {
    logging::init_tracing(&level, log_file.as_deref());
}

/// RAII Guard that automatically decrements `active_workers` when dropped.
struct WorkerGuard(Arc<LiveCounters>);

impl Drop for WorkerGuard {
    fn drop(&mut self) {
        self.0.active_workers.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Spawn a single worker task that executes iterations for the given duration.
fn spawn_worker(
    engine: Arc<dyn ProtocolEngine>,
    url: String,
    counters: Arc<LiveCounters>,
    tx: tokio::sync::mpsc::Sender<RequestMetric>,
    token: CancellationToken,
    duration: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let _guard = WorkerGuard(Arc::clone(&counters));
        counters.active_workers.fetch_add(1, Ordering::Relaxed);

        // Create worker-local context for persistent connections
        let mut worker_ctx = engine.create_worker_context().await;

        let start = Instant::now();
        while start.elapsed() < duration && !token.is_cancelled() {
            counters.total_requests.fetch_add(1, Ordering::Relaxed);

            let metric = if let Some(ref mut ctx) = worker_ctx {
                tokio::select! {
                    _ = token.cancelled() => {
                        tracing::debug!("worker cancelled");
                        break;
                    }
                    m = engine.execute_iteration_with_context(&url, ctx.as_mut()) => m,
                }
            } else {
                tokio::select! {
                    _ = token.cancelled() => {
                        tracing::debug!("worker cancelled");
                        break;
                    }
                    m = engine.execute_iteration(&url) => m,
                }
            };

            counters.completed_requests.fetch_add(1, Ordering::Relaxed);
            counters
                .bytes_received
                .fetch_add(metric.bytes_received, Ordering::Relaxed);
            counters
                .latency_sum_micros
                .fetch_add(metric.latency_micros as u64, Ordering::Relaxed);
            counters.latency_count.fetch_add(1, Ordering::Relaxed);

            if metric.status_code == 0 || metric.status_code >= 400 {
                counters.errors.fetch_add(1, Ordering::Relaxed);
            }

            let _ = tx.send(metric).await;
        }

        // Clean up persistent session if any
        if let Some(mut ctx) = worker_ctx
            && let Some(session) =
                ctx.downcast_mut::<crate::protocols::websocket::PersistentWsSession>()
        {
            session.close().await;
        }
    })
}

#[allow(clippy::too_many_arguments)]
async fn execute_test(
    engine: Arc<dyn ProtocolEngine>,
    url: String,
    no_progress: bool,
    strategy: ConcurrencyStrategy,
) -> PyResult<metrics::TestSummary> {
    tracing::debug!("protocol engine initialized");

    let counters = Arc::new(LiveCounters::new());
    let total_duration = Duration::from_secs(strategy.total_duration_secs());
    let workers = strategy.max_concurrency();
    let test_start = Instant::now();

    // Track whether cancellation was triggered by user SIGINT
    let interrupted = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let interrupted_check = Arc::clone(&interrupted);

    // Top-level cancellation token for SIGINT handling
    let cancel_token = CancellationToken::new();

    // Spawn SIGINT listener with double Ctrl+C safety hatch
    let token_clone = cancel_token.clone();
    let interrupted_clone = interrupted.clone();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            tracing::info!("received SIGINT, initiating graceful shutdown");
            interrupted_clone.store(true, Ordering::SeqCst);
            token_clone.cancel();

            if tokio::signal::ctrl_c().await.is_ok() {
                tracing::warn!("received second SIGINT, shutdown already in progress");
            }
        }
    });

    // --- Metrics Aggregator Setup ---
    let (tx, rx) = tokio::sync::mpsc::channel::<RequestMetric>(METRIC_CHANNEL_BUFFER);

    let aggregator = tokio::spawn(async move {
        let mut latencies = Vec::new();
        let mut e2e_latencies = Vec::new();
        let mut connection_latencies = Vec::new();
        let mut quic_stats = metrics::QuicMetrics::default();
        let mut sse_stats = metrics::SseMetrics::default();
        let mut has_quic = false;
        let mut quic_handshakes = Vec::new();
        let mut sse_first_events = Vec::new();
        let mut status_codes: std::collections::HashMap<u16, u64> =
            std::collections::HashMap::new();
        let mut total_bytes: u64 = 0;
        let mut chaos_injected_total: u64 = 0;
        let mut chaos_faults_by_type: std::collections::HashMap<String, u64> =
            std::collections::HashMap::new();
        let mut rx = rx;
        while let Some(metric) = rx.recv().await {
            latencies.push(metric.latency_micros);
            if let Some(e2e) = metric.e2e_latency_us {
                e2e_latencies.push(e2e);
            }
            if let Some(conn) = metric.connection_latency_us {
                connection_latencies.push(conn);
            }
            // QUIC aggregation
            if metric.quic_handshake_us.is_some()
                || metric.quic_0rtt_used
                || metric.quic_retransmits.is_some()
            {
                has_quic = true;
                if metric.quic_0rtt_used {
                    quic_stats.zero_rtt_accepted_count += 1;
                }
                if let Some(retrans) = metric.quic_retransmits {
                    quic_stats.retransmissions += retrans;
                }
                if let Some(handshake) = metric.quic_handshake_us {
                    quic_handshakes.push(handshake);
                }
            }
            // SSE aggregation
            if let Some(events) = metric.sse_events_received {
                sse_stats.total_events_received += events;
            }
            if let Some(first) = metric.sse_first_event_us {
                sse_first_events.push(first);
            }
            // Chaos aggregation
            if let Some(ref fault) = metric.chaos_fault {
                chaos_injected_total += 1;
                *chaos_faults_by_type
                    .entry(fault.name().to_string())
                    .or_insert(0) += 1;
            }
            *status_codes.entry(metric.status_code).or_insert(0) += 1;
            total_bytes += metric.bytes_received;
        }

        // Compute QUIC avg handshake (convert us to ms)
        if !quic_handshakes.is_empty() {
            let sum: u64 = quic_handshakes.iter().sum();
            quic_stats.avg_handshake_ms =
                Some((sum as f64 / quic_handshakes.len() as f64) / 1000.0);
        }

        // Compute SSE avg TTFB (convert us to ms)
        if !sse_first_events.is_empty() {
            let sum: u64 = sse_first_events.iter().sum();
            sse_stats.avg_ttfb_ms = Some((sum as f64 / sse_first_events.len() as f64) / 1000.0);
        }

        let quic_opt = if has_quic { Some(quic_stats) } else { None };
        let sse_opt = if sse_stats.total_events_received > 0 || !sse_first_events.is_empty() {
            Some(sse_stats)
        } else {
            None
        };

        (
            latencies,
            e2e_latencies,
            status_codes,
            total_bytes,
            connection_latencies,
            quic_opt,
            sse_opt,
            chaos_injected_total,
            chaos_faults_by_type,
        )
    });

    // Spawn progress render task (only on TTY when enabled)
    let use_progress = !no_progress && std::io::stderr().is_terminal();
    let pb = if use_progress {
        Some(progress::create_progress_bar())
    } else {
        None
    };
    let render_handle = pb.as_ref().map(|pb| {
        tokio::spawn(progress::render_loop(
            pb.clone(),
            Arc::clone(&counters),
            test_start,
            total_duration,
            cancel_token.clone(),
        ))
    });

    // --- Dynamic Strategy Dispatch ---
    match strategy {
        ConcurrencyStrategy::Constant {
            concurrency,
            duration_secs,
        } => {
            tracing::info!(
                url,
                concurrency,
                duration_secs,
                "starting constant load test"
            );
            let duration = Duration::from_secs(duration_secs);
            let mut handles = Vec::with_capacity(concurrency);

            for _ in 0..concurrency {
                handles.push(spawn_worker(
                    Arc::clone(&engine),
                    url.clone(),
                    Arc::clone(&counters),
                    tx.clone(),
                    cancel_token.clone(),
                    duration,
                ));
            }

            // NOTE: Drop outer `tx` so only worker clones hold channel senders
            drop(tx);

            tracing::debug!(workers = concurrency, "worker tasks spawned");

            for handle in handles {
                if let Err(e) = handle.await {
                    tracing::warn!(error = %e, "worker task panicked");
                }
            }
        }
        ConcurrencyStrategy::Dynamic { profile } => {
            let total_duration_secs = profile.total_duration();
            tracing::info!(url, total_duration_secs, "starting profile load test");

            let counters_clone = Arc::clone(&counters);
            let engine_clone = Arc::clone(&engine);
            let url_clone = url.clone();
            let cancel_clone = cancel_token.clone();
            let tx_supervisor = tx.clone();

            // NOTE: Drop outer `tx` so supervisor/workers hold remaining channel senders
            drop(tx);

            let supervisor = tokio::spawn(async move {
                let mut child_tokens: Vec<CancellationToken> = Vec::new();
                let mut handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();
                let mut reaped_handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();
                let mut current_concurrency = 0usize;
                let start = Instant::now();

                loop {
                    let elapsed = start.elapsed();
                    if elapsed >= total_duration || cancel_clone.is_cancelled() {
                        break;
                    }

                    let target = profile.target_concurrency(elapsed);

                    while current_concurrency < target {
                        let child_token = cancel_clone.child_token();
                        let remaining = total_duration.saturating_sub(elapsed);

                        let handle = spawn_worker(
                            Arc::clone(&engine_clone),
                            url_clone.clone(),
                            Arc::clone(&counters_clone),
                            tx_supervisor.clone(),
                            child_token.clone(),
                            remaining,
                        );

                        child_tokens.push(child_token);
                        handles.push(handle);
                        current_concurrency += 1;
                    }

                    while current_concurrency > target {
                        if let Some(token) = child_tokens.pop() {
                            token.cancel();
                            if let Some(handle) = handles.pop() {
                                reaped_handles.push(handle);
                            }
                            current_concurrency -= 1;
                        }
                    }

                    // Join reaped workers concurrently during scale down
                    for handle in reaped_handles.drain(..) {
                        if let Err(e) = handle.await {
                            tracing::debug!(error = %e, "cancelled worker panicked during scale down");
                        }
                    }

                    tracing::debug!(current_concurrency, target, "supervisor tick");
                    tokio::time::sleep(Duration::from_millis(SUPERVISOR_TICK_MS)).await;
                }

                // Clean up remaining workers when load profile completes or cancels
                for token in &child_tokens {
                    token.cancel();
                }
                for handle in handles {
                    if let Err(e) = handle.await {
                        tracing::warn!(error = %e, "worker task panicked during final teardown");
                    }
                }
            });

            supervisor
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        }
    }

    // --- Shared Teardown & Aggregation ---

    // Wait for progress rendering task to finish
    if let Some(handle) = render_handle
        && let Err(e) = handle.await
    {
        tracing::debug!(error = %e, "render task panicked");
    }

    // Receive latency results (channel closes automatically as all tx references dropped)
    let (
        latencies,
        e2e_latencies,
        status_codes,
        total_bytes,
        connection_latencies,
        quic_metrics,
        sse_metrics,
        chaos_injected_total,
        chaos_faults_by_type,
    ) = aggregator
        .await
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

    let total = counters.total_requests.load(Ordering::Relaxed);
    let errors = counters.errors.load(Ordering::Relaxed);
    let elapsed = test_start.elapsed().as_secs_f64();

    tracing::info!(total, errors, "load test completed");

    // Raise KeyboardInterrupt to Python if test was canceled by user
    if interrupted_check.load(Ordering::SeqCst) {
        return Err(pyo3::exceptions::PyKeyboardInterrupt::new_err(
            "load test interrupted by user",
        ));
    }

    Ok(metrics::calculate_summary(
        url,
        total,
        errors,
        latencies,
        total_bytes,
        elapsed,
        workers,
        status_codes,
        e2e_latencies,
        connection_latencies,
        quic_metrics,
        sse_metrics,
        chaos_injected_total,
        chaos_faults_by_type,
    ))
}

#[pyfunction]
fn run_load_test(py: Python<'_>, config: TestConfig) -> PyResult<metrics::TestSummary> {
    py.detach(move || {
        let url = config.url.clone();
        let chaos = ChaosEngine::new(config.chaos, config.chaos_rate);
        let no_progress = config.no_progress;

        // Build protocol engine based on URL scheme
        let engine: Arc<dyn ProtocolEngine> =
            if protocols::is_protocol_url(&url) || config.sse_enabled {
                protocols::detect_protocol(&url, &config, chaos)
            } else {
                let method = parse_method(&config.method)?;
                let body = parse_body(config.body.as_deref());
                let form = parse_form(config.form.as_deref());
                let header_map = parse_headers(config.headers.as_deref())?;

                // Resolve payload and auto-inject Content-Type
                let is_form = form.is_some();
                let final_body = if form.is_some() {
                    if body.is_some() {
                        return Err(pyo3::exceptions::PyValueError::new_err(
                            "Cannot specify both --body and --form simultaneously",
                        ));
                    }
                    form
                } else {
                    body
                };

                let mut header_map = header_map;
                if final_body.is_some() && !header_map.contains_key(CONTENT_TYPE) {
                    let ct = if is_form {
                        "application/x-www-form-urlencoded"
                    } else {
                        "application/json"
                    };
                    header_map.insert(CONTENT_TYPE, HeaderValue::from_static(ct));
                }

                let client = build_client(config.concurrency, config.timeout_secs, header_map)
                    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
                Arc::new(
                    protocols::http::HttpEngine::new()
                        .with_client(client)
                        .with_method(method)
                        .with_body(final_body)
                        .with_chaos(chaos),
                )
            };

        let strategy = ConcurrencyStrategy::Constant {
            concurrency: config.concurrency,
            duration_secs: config.duration_secs,
        };

        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

        let summary = rt.block_on(execute_test(engine, url, no_progress, strategy))?;

        // Persist report artifact to disk
        if !config.no_save {
            let artifact =
                report::schema::ReportArtifact::from_summary_and_config(&summary, &config);
            if let Err(e) = report::writer::save_report_json(
                &artifact,
                config.output_dir.as_deref().map(std::path::Path::new),
                false,
            ) {
                tracing::warn!(error = %e, "failed to save report artifact");
            }
        }

        Ok(summary)
    })
}

#[pyfunction]
#[pyo3(signature = (
    url,
    timeout_secs,
    profile,
    chaos=false,
    chaos_rate=crate::chaos::DEFAULT_CHAOS_RATE,
    no_progress=false,
    method="GET",
    body=None,
    form=None,
    headers=None,
))]
#[allow(clippy::too_many_arguments)]
fn run_load_profiles(
    py: Python<'_>,
    url: String,
    timeout_secs: u64,
    profile: LoadProfile,
    chaos: bool,
    chaos_rate: f32,
    no_progress: bool,
    method: &str,
    body: Option<String>,
    form: Option<Vec<(String, String)>>,
    headers: Option<Vec<(String, String)>>,
) -> PyResult<metrics::TestSummary> {
    py.detach(move || {
        let chaos_engine = ChaosEngine::new(chaos, chaos_rate);

        let method = parse_method(method)?;
        let raw_body = parse_body(body.as_deref());
        let raw_form = parse_form(form.as_deref());
        let mut header_map = parse_headers(headers.as_deref())?;

        // Resolve payload and auto-inject Content-Type
        let is_form = raw_form.is_some();
        let final_body = if raw_form.is_some() {
            if raw_body.is_some() {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "Cannot specify both --body and --form simultaneously",
                ));
            }
            raw_form
        } else {
            raw_body
        };

        if final_body.is_some() && !header_map.contains_key(CONTENT_TYPE) {
            let ct = if is_form {
                "application/x-www-form-urlencoded"
            } else {
                "application/json"
            };
            header_map.insert(CONTENT_TYPE, HeaderValue::from_static(ct));
        }

        // Build protocol engine based on URL scheme
        let engine: Arc<dyn ProtocolEngine> = if protocols::is_protocol_url(&url) {
            // Build a minimal TestConfig for protocol detection
            let ws_config = TestConfig::for_protocol_detection(
                url.clone(),
                profile.max_concurrency(),
                profile.total_duration(),
                timeout_secs,
            );
            protocols::detect_protocol(&url, &ws_config, chaos_engine)
        } else {
            let client = build_client(profile.max_concurrency(), timeout_secs, header_map)
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            Arc::new(
                protocols::http::HttpEngine::new()
                    .with_client(client)
                    .with_method(method)
                    .with_body(final_body)
                    .with_chaos(chaos_engine),
            )
        };

        let strategy = ConcurrencyStrategy::Dynamic { profile };

        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

        rt.block_on(execute_test(engine, url, no_progress, strategy))
    })
}

/// A Python module implemented in Rust.
#[pymodule]
fn _strobengine(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(init_logging, m)?)?;
    m.add_function(wrap_pyfunction!(run_load_test, m)?)?;
    m.add_function(wrap_pyfunction!(run_load_profiles, m)?)?;
    m.add_class::<config::TestConfig>()?;
    m.add_class::<config::LoadProfile>()?;
    m.add_class::<config::WsMode>()?;
    m.add_class::<metrics::TestSummary>()?;
    Ok(())
}
