use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use http::Method;
use reqwest::Url;

use crate::chaos::{ChaosEngine, ChaosFault};
use crate::metrics::RequestMetric;

use super::ProtocolEngine;

static CORRUPTED_BODY: &[u8] = b"{\"payload\": \"\\xff\\xfe\\xbd\\xef\"}";
static CHAOS_HEADER: &str = "x-chaos-fault";
static BAD_HEADER_VALUE: &str = "invalid-header-value";

pub struct HttpEngine {
    client: reqwest::Client,
    method: Method,
    body: Option<Bytes>,
    chaos: ChaosEngine,
}

impl Default for HttpEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl HttpEngine {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
            method: Method::GET,
            body: None,
            chaos: ChaosEngine::default(),
        }
    }

    pub fn with_client(mut self, client: reqwest::Client) -> Self {
        self.client = client;
        self
    }

    pub fn with_method(mut self, method: Method) -> Self {
        self.method = method;
        self
    }

    pub fn with_body(mut self, body: Option<Bytes>) -> Self {
        self.body = body;
        self
    }

    pub fn with_chaos(mut self, chaos: ChaosEngine) -> Self {
        self.chaos = chaos;
        self
    }

    pub async fn prewarm(&self, url: &str) {
        let mut req = self.client.request(self.method.clone(), url);
        if let Some(ref b) = self.body {
            req = req.body(b.clone());
        }
        let _ = req.send().await;
    }
}

#[async_trait]
impl ProtocolEngine for HttpEngine {
    async fn execute_iteration(&self, target_url: &str) -> RequestMetric {
        let req_start = Instant::now();

        // Parse URL once — cached across iterations by the caller
        let parsed_url = match Url::parse(target_url) {
            Ok(u) => u,
            Err(e) => {
                tracing::error!(error = %e, "failed to parse URL");
                return RequestMetric::error(req_start.elapsed().as_micros());
            }
        };

        let base_request = || {
            let mut req = self.client.request(self.method.clone(), parsed_url.clone());
            if let Some(ref b) = self.body {
                req = req.body(b.clone());
            }
            req
        };

        let chaos_fault = self.chaos.select_fault();
        let request = match chaos_fault {
            Some(ChaosFault::LatencySpike { duration_ms }) => {
                tracing::trace!(duration_ms, "chaos: latency spike injected");
                tokio::time::sleep(Duration::from_millis(duration_ms)).await;
                base_request()
            }
            Some(ChaosFault::CorruptedPayload) => {
                tracing::trace!("chaos: corrupted payload injected");
                self.client
                    .request(self.method.clone(), target_url)
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

        let (status_code, bytes_received) = match request.send().await {
            Ok(response) => {
                let code = response.status().as_u16();
                let bytes = response.content_length().unwrap_or(0);
                (code, bytes)
            }
            Err(e) => {
                tracing::debug!(error = %e, "request failed");
                (0, 0)
            }
        };

        let latency_micros = req_start.elapsed().as_micros();

        if tracing::enabled!(tracing::Level::TRACE) {
            tracing::trace!(
                status = status_code,
                latency_us = latency_micros,
                "request completed"
            );
        }

        RequestMetric {
            latency_micros,
            status_code,
            bytes_received,
            is_reconnect: false,
            connection_latency_us: None,
            timestamp_sent_ns: None,
            e2e_latency_us: None,
            quic_handshake_us: None,
            quic_0rtt_used: false,
            quic_retransmits: None,
            sse_events_received: None,
            sse_first_event_us: None,
            sse_event_interval_us: None,
            chaos_fault,
        }
    }
}
