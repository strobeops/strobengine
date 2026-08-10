use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use bytes::Bytes;
use http::Method;
use reqwest::Url;
use tokio_util::sync::CancellationToken;

use crate::chaos::{ChaosEngine, ChaosFault};
use crate::metrics::{LiveCounters, RequestMetric};

static CORRUPTED_BODY: &[u8] = b"{\"payload\": \"\\xff\\xfe\\xbd\\xef\"}";
static CHAOS_HEADER: &str = "x-chaos-fault";
static BAD_HEADER_VALUE: &str = "invalid-header-value";

/// RAII Guard that automatically decrements `active_workers` when dropped.
struct WorkerGuard(Arc<LiveCounters>);

impl Drop for WorkerGuard {
    fn drop(&mut self) {
        self.0.active_workers.fetch_sub(1, Ordering::Relaxed);
    }
}

pub async fn prewarm(client: &reqwest::Client, url: &str, method: &Method, body: &Option<Bytes>) {
    let mut prewarm_req = client.request(method.clone(), url);
    if let Some(b) = body {
        prewarm_req = prewarm_req.body(b.clone());
    }
    let _ = prewarm_req.send().await;
}

#[allow(clippy::too_many_arguments)]
pub async fn worker_loop(
    client: reqwest::Client,
    url: String,
    method: Method,
    body: Option<Bytes>,
    counters: Arc<LiveCounters>,
    tx: tokio::sync::mpsc::Sender<RequestMetric>,
    duration: Duration,
    token: CancellationToken,
    chaos: ChaosEngine,
) {
    tracing::debug!("worker spawned");

    counters.active_workers.fetch_add(1, Ordering::Relaxed);
    let _guard = WorkerGuard(Arc::clone(&counters));

    // Pre-warm: always HEAD regardless of test method to avoid side effects
    tracing::trace!("pre-warming connection");
    prewarm(&client, &url, &method, &body).await;

    // Parse URL ONCE before entering the hot path
    let target_url = match Url::parse(&url) {
        Ok(u) => u,
        Err(e) => {
            tracing::error!(error = %e, "failed to parse URL in worker");
            return;
        }
    };

    let start = Instant::now();

    while start.elapsed() < duration && !token.is_cancelled() {
        counters.total_requests.fetch_add(1, Ordering::Relaxed);

        let req_start = Instant::now();

        // Zero-allocation request builder closure
        let base_request = || {
            let mut req = client.request(method.clone(), target_url.clone());
            if let Some(ref b) = body {
                req = req.body(b.clone());
            }
            req
        };

        let request = match chaos.select_fault() {
            Some(ChaosFault::LatencySpike { duration_ms }) => {
                tracing::trace!(duration_ms, "chaos: latency spike injected");
                // Chaos sleep is also cancellable
                tokio::select! {
                    _ = token.cancelled() => {
                        tracing::debug!("worker cancelled during chaos latency spike");
                        break;
                    }
                    _ = tokio::time::sleep(Duration::from_millis(duration_ms)) => {}
                }
                base_request()
            }
            Some(ChaosFault::CorruptedPayload) => {
                tracing::trace!("chaos: corrupted payload injected");
                client
                    .post(&url)
                    .header(CHAOS_HEADER, "corrupted-payload")
                    .body(CORRUPTED_BODY)
            }
            Some(ChaosFault::MetadataCorruption) => {
                tracing::trace!("chaos: metadata corruption injected");
                base_request().header(CHAOS_HEADER, BAD_HEADER_VALUE)
            }
            Some(ChaosFault::ConnectionDrop) => {
                tracing::trace!("chaos: connection drop injected");
                base_request().timeout(Duration::from_nanos(1))
            }
            None => base_request(),
        };

        // Race request against cancellation — abandon stuck requests instantly
        let (status_code, bytes_received) = tokio::select! {
            _ = token.cancelled() => {
                tracing::debug!("worker cancelled, abandoning in-flight request");
                break;
            }
            res = request.send() => {
                match res {
                    Ok(response) => {
                        let code = response.status().as_u16();
                        let bytes = response.content_length().unwrap_or(0);
                        if !response.status().is_success() {
                            counters.errors.fetch_add(1, Ordering::Relaxed);
                            tracing::debug!(status_code = code, "non-success HTTP status");
                        }
                        (code, bytes)
                    }
                    Err(e) => {
                        counters.errors.fetch_add(1, Ordering::Relaxed);
                        tracing::debug!(error = %e, "request failed");
                        (0, 0)
                    }
                }
            }
        };

        // Fallback to u64::MAX (effectively infinity) to prevent overflow on extreme or hanging durations
        let latency_micros = u64::try_from(req_start.elapsed().as_micros()).unwrap_or(u64::MAX);

        if tracing::enabled!(tracing::Level::TRACE) {
            tracing::trace!(
                status = status_code,
                latency_us = latency_micros,
                "request completed"
            );
        }

        counters.completed_requests.fetch_add(1, Ordering::Relaxed);
        counters
            .latency_sum_micros
            .fetch_add(latency_micros, Ordering::Relaxed);
        counters.latency_count.fetch_add(1, Ordering::Relaxed);
        counters
            .bytes_received
            .fetch_add(bytes_received, Ordering::Relaxed);

        let metric = RequestMetric {
            latency_micros: latency_micros as u128,
            status_code,
            bytes_received,
        };

        let _ = tx.send(metric).await;
    }
}
