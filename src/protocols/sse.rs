use std::pin::Pin;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::StreamExt;
use futures_util::stream::Stream;

use crate::chaos::{ChaosEngine, ChaosFault};
use crate::metrics::RequestMetric;

use super::ProtocolEngine;

type ByteStream = Pin<Box<dyn Stream<Item = Result<Bytes, reqwest::Error>> + Send>>;

/// A parsed SSE event.
#[derive(Debug, Clone)]
pub struct SseEvent {
    pub event_type: String,
    pub data: String,
    pub id: Option<String>,
}

/// Worker-local SSE session holding a persistent stream and buffer state.
pub struct SseSession {
    pub stream: Option<ByteStream>,
    pub status_code: u16,
    pub events_received: u64,
    pub first_event_time: Option<Instant>,
    pub last_event_time: Option<Instant>,
    pub buffer: String,
    pub max_events: Option<u64>,
}

#[async_trait::async_trait]
impl super::WorkerSession for SseSession {
    async fn shutdown(&mut self) {}
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

impl SseSession {
    fn new(max_events: Option<u64>) -> Self {
        Self {
            stream: None,
            status_code: 0,
            events_received: 0,
            first_event_time: None,
            last_event_time: None,
            buffer: String::new(),
            max_events,
        }
    }

    fn is_max_reached(&self) -> bool {
        self.max_events
            .is_some_and(|max| self.events_received >= max)
    }
}

/// Parse SSE events from a chunk appended to a buffer.
///
/// Frames are delimited by `\n\n` or `\r\n\r\n`. Incomplete frames remain
/// in the buffer for the next chunk. Each frame is parsed for `data:`,
/// `event:`, `id:`, and `retry:` fields per the SSE spec.
pub fn parse_sse_chunk(buffer: &mut String, chunk: &str) -> Vec<SseEvent> {
    buffer.push_str(chunk);
    let mut events = Vec::new();

    while let Some(frame_end) = find_frame_end(buffer) {
        let frame = buffer[..frame_end].to_string();
        let delim_len = if buffer[frame_end..].starts_with("\r\n\r\n") {
            4
        } else {
            2
        };
        buffer.drain(..frame_end + delim_len);

        if let Some(event) = parse_frame(&frame) {
            events.push(event);
        }
    }

    events
}

/// Find the byte index of the first frame delimiter in the buffer.
fn find_frame_end(buffer: &str) -> Option<usize> {
    let crlf = buffer.find("\r\n\r\n");
    let lf = buffer.find("\n\n");
    match (crlf, lf) {
        (Some(c), Some(l)) => Some(c.min(l)),
        (Some(c), None) => Some(c),
        (None, Some(l)) => Some(l),
        (None, None) => None,
    }
}

/// Parse a single SSE frame into an event.
///
/// Multi-line `data:` fields are joined with `\n` per the SSE spec.
/// Lines starting with `:` (comments) are ignored.
fn parse_frame(frame: &str) -> Option<SseEvent> {
    let mut event_type = String::new();
    let mut data_lines: Vec<String> = Vec::new();
    let mut id: Option<String> = None;

    for line in frame.lines() {
        if line.starts_with(':') {
            continue;
        }

        if let Some(value) = line.strip_prefix("data:") {
            let value = value.strip_prefix(' ').unwrap_or(value);
            data_lines.push(value.to_string());
        } else if let Some(value) = line.strip_prefix("event:") {
            let value = value.strip_prefix(' ').unwrap_or(value);
            event_type = value.to_string();
        } else if let Some(value) = line.strip_prefix("id:") {
            let value = value.strip_prefix(' ').unwrap_or(value);
            id = Some(value.to_string());
        }
    }

    if data_lines.is_empty() {
        return None;
    }

    let data = data_lines.join("\n");

    Some(SseEvent {
        event_type,
        data,
        id,
    })
}

pub struct SseEngine {
    client: reqwest::Client,
    headers: Vec<(String, String)>,
    chaos: ChaosEngine,
    max_events: Option<u64>,
}

impl SseEngine {
    pub fn new(
        headers: Vec<(String, String)>,
        chaos: ChaosEngine,
        max_events: Option<u64>,
    ) -> Self {
        let client = reqwest::Client::new();
        Self::with_client(client, headers, chaos, max_events)
    }

    pub fn with_client(
        client: reqwest::Client,
        headers: Vec<(String, String)>,
        chaos: ChaosEngine,
        max_events: Option<u64>,
    ) -> Self {
        Self {
            client,
            headers,
            chaos,
            max_events,
        }
    }

    /// Build request headers, merging user-supplied headers with SSE defaults.
    fn build_headers(&self) -> Vec<(String, String)> {
        let mut merged = self.headers.clone();

        let has_accept = merged.iter().any(|(k, _)| k.eq_ignore_ascii_case("accept"));
        if !has_accept {
            merged.push(("accept".to_string(), "text/event-stream".to_string()));
        }

        let has_cache = merged
            .iter()
            .any(|(k, _)| k.eq_ignore_ascii_case("cache-control"));
        if !has_cache {
            merged.push(("cache-control".to_string(), "no-cache".to_string()));
        }

        merged
    }

    /// Normalize `sse://` → `http://` and `sses://` → `https://` for reqwest.
    fn normalize_url(url: &str) -> String {
        if let Some(rest) = url.strip_prefix("sse://") {
            format!("http://{rest}")
        } else if let Some(rest) = url.strip_prefix("sses://") {
            format!("https://{rest}")
        } else {
            url.to_string()
        }
    }

    /// Connect to the SSE endpoint and return the raw response.
    async fn connect(&self, url: &str) -> Result<reqwest::Response, reqwest::Error> {
        let normalized = Self::normalize_url(url);
        let mut req = self.client.get(&normalized);
        for (key, value) in self.build_headers() {
            req = req.header(key, value);
        }
        req.send().await
    }

    /// Apply a selected chaos fault and return early if the connection should be
    /// dropped. The fault itself stays with the caller so every metric this
    /// iteration returns can report it to the chaos aggregation.
    async fn apply_chaos(&self, fault: Option<ChaosFault>) -> Option<RequestMetric> {
        match fault {
            Some(ChaosFault::ConnectionDrop) => {
                tracing::trace!("sse chaos: connection drop");
                Some(RequestMetric::error(0, fault))
            }
            Some(ChaosFault::LatencySpike { duration_ms }) => {
                tracing::trace!(duration_ms, "sse chaos: latency spike");
                tokio::time::sleep(Duration::from_millis(duration_ms)).await;
                None
            }
            _ => None,
        }
    }
}

#[async_trait]
impl ProtocolEngine for SseEngine {
    /// Stateless mode: connect, read a single frame, close, return metric.
    async fn execute_iteration(&self, target_url: &str) -> RequestMetric {
        let req_start = Instant::now();

        let fault = self.chaos.select_fault();
        if let Some(mut metric) = self.apply_chaos(fault).await {
            metric.latency_micros = req_start.elapsed().as_micros();
            return metric;
        }

        let response = match self.connect(target_url).await {
            Ok(resp) => resp,
            Err(e) => {
                tracing::debug!(error = %e, "sse connection failed");
                return RequestMetric::error(req_start.elapsed().as_micros(), fault);
            }
        };

        let status_code = response.status().as_u16();
        if response.status().is_server_error() || response.status().is_client_error() {
            let latency_micros = req_start.elapsed().as_micros();
            return RequestMetric::builder(latency_micros, fault)
                .status(status_code)
                .sse_events(Some(0), None, None)
                .build();
        }

        // Read one frame from the stream
        let mut stream = response.bytes_stream();
        let mut buffer = String::new();
        let mut total_bytes: u64 = 0;

        while let Some(chunk_result) = stream.next().await {
            match chunk_result {
                Ok(chunk) => {
                    total_bytes += chunk.len() as u64;
                    let chunk_str = String::from_utf8_lossy(&chunk);
                    let events = parse_sse_chunk(&mut buffer, &chunk_str);
                    if !events.is_empty() {
                        let latency_micros = req_start.elapsed().as_micros();
                        return RequestMetric::builder(latency_micros, fault)
                            .status(status_code)
                            .bytes(total_bytes)
                            .sse_events(
                                Some(events.len() as u64),
                                Some(latency_micros as u64),
                                None,
                            )
                            .build();
                    }
                }
                Err(e) => {
                    tracing::debug!(error = %e, "sse stream read failed");
                    return RequestMetric::error(req_start.elapsed().as_micros(), fault);
                }
            }
        }

        // Stream ended without yielding a frame
        let latency_micros = req_start.elapsed().as_micros();
        RequestMetric::builder(latency_micros, fault)
            .status(status_code)
            .bytes(total_bytes)
            .sse_events(Some(0), None, None)
            .build()
    }

    /// Persistent mode: return a session for subsequent lazy-connect reads.
    async fn create_worker_context(&self) -> Option<Box<dyn super::WorkerSession>> {
        Some(Box::new(SseSession::new(self.max_events)))
    }

    /// Persistent mode: read the next frame from an existing session.
    async fn execute_iteration_with_context(
        &self,
        target_url: &str,
        ctx: &mut dyn super::WorkerSession,
    ) -> RequestMetric {
        let session = match ctx.as_any_mut().downcast_mut::<SseSession>() {
            Some(s) => s,
            None => return RequestMetric::error(0, None),
        };

        // TTFB measures from request start (including a lazy connect), so it
        // matches the stateless path's request-to-first-event semantics.
        let iteration_start = Instant::now();

        // Track whether connection existed before this iteration (reuse detection)
        let was_connected = session.stream.is_some();

        // Check max events before (re)connecting so a budget-spent session
        // never reopens a connection just to be capped again.
        if session.is_max_reached() {
            tracing::trace!(
                events = session.events_received,
                max = ?session.max_events,
                "sse max events reached"
            );
            return RequestMetric::builder(0, None)
                .status(session.status_code)
                .socket_reused(was_connected)
                // No first event arrives in this iteration: the connection
                // that received one already reported its TTFB.
                .sse_events(Some(session.events_received), None, None)
                .build();
        }

        // Chaos is selected only when (re)connecting: read iterations over an
        // established stream inject nothing, so `fault` stays `None` there.
        let mut fault: Option<ChaosFault> = None;

        // Lazy connect on first call or after the previous stream ended
        if session.stream.is_none() {
            let req_start = Instant::now();

            fault = self.chaos.select_fault();
            if let Some(mut metric) = self.apply_chaos(fault).await {
                metric.latency_micros = req_start.elapsed().as_micros();
                return metric;
            }

            match self.connect(target_url).await {
                Ok(resp) => {
                    session.status_code = resp.status().as_u16();
                    // `--sse-max-events` caps events per connection.
                    session.events_received = 0;
                    // TTFB is per connection: the next first event starts a
                    // fresh measurement instead of reusing the session's one.
                    session.first_event_time = None;
                    session.stream = Some(Box::pin(resp.bytes_stream()));
                }
                Err(e) => {
                    tracing::debug!(error = %e, "sse persistent connect failed");
                    return RequestMetric::error(req_start.elapsed().as_micros(), fault);
                }
            }
        }

        let req_start = Instant::now();
        let status_code = session.status_code;
        let mut total_bytes: u64 = 0;

        // Stream the next frame
        let Some(stream) = session.stream.as_mut() else {
            return RequestMetric::error(req_start.elapsed().as_micros(), fault);
        };
        let mut read_error = false;
        while let Some(chunk_result) = stream.next().await {
            match chunk_result {
                Ok(chunk) => {
                    total_bytes += chunk.len() as u64;
                    let chunk_str = String::from_utf8_lossy(&chunk);
                    let events = parse_sse_chunk(&mut session.buffer, &chunk_str);

                    if !events.is_empty() {
                        session.events_received += 1;

                        let now = Instant::now();
                        let first_of_connection = session.first_event_time.is_none();
                        if first_of_connection {
                            session.first_event_time = Some(now);
                        }

                        let interval_us = session
                            .last_event_time
                            .map(|last| now.duration_since(last).as_micros() as u64);
                        session.last_event_time = Some(now);

                        let latency_micros = req_start.elapsed().as_micros();
                        // TTFB only on the connection's first event; later
                        // iterations measure wait-for-next via latency and
                        // interval instead of re-reporting a stale value.
                        let first_event_us = first_of_connection
                            .then(|| now.duration_since(iteration_start).as_micros() as u64);

                        return RequestMetric::builder(latency_micros, fault)
                            .status(status_code)
                            .bytes(total_bytes)
                            .socket_reused(was_connected)
                            .sse_events(Some(session.events_received), first_event_us, interval_us)
                            .build();
                    }
                }
                Err(e) => {
                    tracing::debug!(error = %e, "sse persistent stream read failed");
                    read_error = true;
                    break;
                }
            }
        }

        // The stream finished (EOF or error): drop it so the next iteration
        // reconnects instead of reading an exhausted stream forever.
        // `session.status_code` is intentionally kept: it describes the last
        // (now closed) connection, and `0` would falsely count a
        // budget-completed session as a network error.
        session.stream = None;
        session.buffer.clear();

        if read_error {
            return RequestMetric::error(req_start.elapsed().as_micros(), fault);
        }

        // Stream EOF: server closed the connection; the next iteration reconnects.
        let latency_micros = req_start.elapsed().as_micros();
        RequestMetric::builder(latency_micros, fault)
            .status(status_code)
            .bytes(total_bytes)
            .socket_reused(was_connected)
            // No first event arrived in this iteration (see max path above).
            .sse_events(Some(session.events_received), None, None)
            .build()
    }
}
