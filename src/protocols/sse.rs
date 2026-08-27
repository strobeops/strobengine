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

    /// Connect to the SSE endpoint and return the raw response.
    async fn connect(&self, url: &str) -> Result<reqwest::Response, reqwest::Error> {
        let mut req = self.client.get(url);
        for (key, value) in self.build_headers() {
            req = req.header(key, value);
        }
        req.send().await
    }

    /// Apply chaos fault and return early if connection should be dropped.
    async fn maybe_inject_chaos(&self) -> Option<RequestMetric> {
        let fault = self.chaos.select_fault();
        match fault {
            Some(ChaosFault::ConnectionDrop) => {
                tracing::trace!("sse chaos: connection drop");
                Some(RequestMetric::error(0))
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

        if let Some(mut metric) = self.maybe_inject_chaos().await {
            metric.latency_micros = req_start.elapsed().as_micros();
            return metric;
        }

        let response = match self.connect(target_url).await {
            Ok(resp) => resp,
            Err(e) => {
                tracing::debug!(error = %e, "sse connection failed");
                return RequestMetric::error(req_start.elapsed().as_micros());
            }
        };

        let status_code = response.status().as_u16();
        if response.status().is_server_error() || response.status().is_client_error() {
            let latency_micros = req_start.elapsed().as_micros();
            return RequestMetric {
                latency_micros,
                status_code,
                bytes_received: 0,
                is_reconnect: false,
                connection_latency_us: None,
                timestamp_sent_ns: None,
                e2e_latency_us: None,
                quic_handshake_us: None,
                quic_0rtt_used: false,
                quic_retransmits: None,
                sse_events_received: Some(0),
                sse_first_event_us: None,
                sse_event_interval_us: None,
            };
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
                        return RequestMetric {
                            latency_micros,
                            status_code,
                            bytes_received: total_bytes,
                            is_reconnect: false,
                            connection_latency_us: None,
                            timestamp_sent_ns: None,
                            e2e_latency_us: None,
                            quic_handshake_us: None,
                            quic_0rtt_used: false,
                            quic_retransmits: None,
                            sse_events_received: Some(events.len() as u64),
                            sse_first_event_us: Some(latency_micros as u64),
                            sse_event_interval_us: None,
                        };
                    }
                }
                Err(e) => {
                    tracing::debug!(error = %e, "sse stream read failed");
                    break;
                }
            }
        }

        // Stream ended without yielding a frame
        let latency_micros = req_start.elapsed().as_micros();
        RequestMetric {
            latency_micros,
            status_code,
            bytes_received: total_bytes,
            is_reconnect: false,
            connection_latency_us: None,
            timestamp_sent_ns: None,
            e2e_latency_us: None,
            quic_handshake_us: None,
            quic_0rtt_used: false,
            quic_retransmits: None,
            sse_events_received: Some(0),
            sse_first_event_us: None,
            sse_event_interval_us: None,
        }
    }

    /// Persistent mode: return a session for subsequent lazy-connect reads.
    async fn create_worker_context(&self) -> Option<Box<dyn std::any::Any + Send>> {
        Some(Box::new(SseSession::new(self.max_events)))
    }

    /// Persistent mode: read the next frame from an existing session.
    async fn execute_iteration_with_context(
        &self,
        target_url: &str,
        ctx: &mut (dyn std::any::Any + Send),
    ) -> RequestMetric {
        let session = match ctx.downcast_mut::<SseSession>() {
            Some(s) => s,
            None => return RequestMetric::error(0),
        };

        // Lazy connect on first call: consume response to get owned stream
        if session.stream.is_none() {
            let req_start = Instant::now();

            if let Some(mut metric) = self.maybe_inject_chaos().await {
                metric.latency_micros = req_start.elapsed().as_micros();
                return metric;
            }

            match self.connect(target_url).await {
                Ok(resp) => {
                    session.status_code = resp.status().as_u16();
                    session.stream = Some(Box::pin(resp.bytes_stream()));
                }
                Err(e) => {
                    tracing::debug!(error = %e, "sse persistent connect failed");
                    return RequestMetric::error(req_start.elapsed().as_micros());
                }
            }
        }

        // Check max events
        if session.is_max_reached() {
            tracing::trace!(
                events = session.events_received,
                max = ?session.max_events,
                "sse max events reached"
            );
            return RequestMetric {
                latency_micros: 0,
                status_code: session.status_code,
                bytes_received: 0,
                is_reconnect: false,
                connection_latency_us: None,
                timestamp_sent_ns: None,
                e2e_latency_us: None,
                quic_handshake_us: None,
                quic_0rtt_used: false,
                quic_retransmits: None,
                sse_events_received: Some(session.events_received),
                sse_first_event_us: session
                    .first_event_time
                    .map(|t| t.elapsed().as_micros() as u64),
                sse_event_interval_us: None,
            };
        }

        let req_start = Instant::now();
        let status_code = session.status_code;
        let mut total_bytes: u64 = 0;

        // Stream the next frame
        while let Some(chunk_result) = session.stream.as_mut().unwrap().next().await {
            match chunk_result {
                Ok(chunk) => {
                    total_bytes += chunk.len() as u64;
                    let chunk_str = String::from_utf8_lossy(&chunk);
                    let events = parse_sse_chunk(&mut session.buffer, &chunk_str);

                    if !events.is_empty() {
                        session.events_received += 1;

                        let now = Instant::now();
                        if session.first_event_time.is_none() {
                            session.first_event_time = Some(now);
                        }

                        let interval_us = session
                            .last_event_time
                            .map(|last| now.duration_since(last).as_micros() as u64);
                        session.last_event_time = Some(now);

                        let latency_micros = req_start.elapsed().as_micros();
                        let first_event_us = session
                            .first_event_time
                            .map(|t| t.elapsed().as_micros() as u64);

                        return RequestMetric {
                            latency_micros,
                            status_code,
                            bytes_received: total_bytes,
                            is_reconnect: false,
                            connection_latency_us: None,
                            timestamp_sent_ns: None,
                            e2e_latency_us: None,
                            quic_handshake_us: None,
                            quic_0rtt_used: false,
                            quic_retransmits: None,
                            sse_events_received: Some(session.events_received),
                            sse_first_event_us: first_event_us,
                            sse_event_interval_us: interval_us,
                        };
                    }
                }
                Err(e) => {
                    tracing::debug!(error = %e, "sse persistent stream read failed");
                    break;
                }
            }
        }

        // Stream EOF
        let latency_micros = req_start.elapsed().as_micros();
        RequestMetric {
            latency_micros,
            status_code,
            bytes_received: total_bytes,
            is_reconnect: false,
            connection_latency_us: None,
            timestamp_sent_ns: None,
            e2e_latency_us: None,
            quic_handshake_us: None,
            quic_0rtt_used: false,
            quic_retransmits: None,
            sse_events_received: Some(session.events_received),
            sse_first_event_us: session
                .first_event_time
                .map(|t| t.elapsed().as_micros() as u64),
            sse_event_interval_us: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_single_frame() {
        let mut buffer = String::new();
        let events = parse_sse_chunk(&mut buffer, "data: hello\n\n");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].data, "hello");
        assert_eq!(events[0].event_type, "");
    }

    #[test]
    fn test_parse_multiple_frames() {
        let mut buffer = String::new();
        let events = parse_sse_chunk(&mut buffer, "data: first\n\ndata: second\n\n");
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].data, "first");
        assert_eq!(events[1].data, "second");
    }

    #[test]
    fn test_parse_split_across_chunks() {
        let mut buffer = String::new();
        let events1 = parse_sse_chunk(&mut buffer, "data: hel");
        assert_eq!(events1.len(), 0);
        let events2 = parse_sse_chunk(&mut buffer, "lo\n\n");
        assert_eq!(events2.len(), 1);
        assert_eq!(events2[0].data, "hello");
    }

    #[test]
    fn test_parse_crlf_separator() {
        let mut buffer = String::new();
        let events = parse_sse_chunk(&mut buffer, "data: crlf-test\r\n\r\n");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].data, "crlf-test");
    }

    #[test]
    fn test_parse_event_type() {
        let mut buffer = String::new();
        let events = parse_sse_chunk(&mut buffer, "event: message\ndata: payload\n\n");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, "message");
        assert_eq!(events[0].data, "payload");
    }

    #[test]
    fn test_parse_data_concatenation() {
        let mut buffer = String::new();
        let events = parse_sse_chunk(&mut buffer, "data: line1\ndata: line2\ndata: line3\n\n");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].data, "line1\nline2\nline3");
    }

    #[test]
    fn test_parse_id_field() {
        let mut buffer = String::new();
        let events = parse_sse_chunk(&mut buffer, "id: 42\ndata: test\n\n");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].id, Some("42".to_string()));
    }

    #[test]
    fn test_parse_comment_ignored() {
        let mut buffer = String::new();
        let events = parse_sse_chunk(&mut buffer, ": this is a comment\ndata: real\n\n");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].data, "real");
    }

    #[test]
    fn test_parse_empty_data_no_event() {
        let mut buffer = String::new();
        let events = parse_sse_chunk(&mut buffer, "event: ping\n\n");
        assert_eq!(events.len(), 0);
    }

    #[test]
    fn test_parse_data_with_space_prefix() {
        let mut buffer = String::new();
        let events = parse_sse_chunk(&mut buffer, "data: with space\n\n");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].data, "with space");
    }

    #[test]
    fn test_parse_data_without_space_prefix() {
        let mut buffer = String::new();
        let events = parse_sse_chunk(&mut buffer, "data:nospace\n\n");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].data, "nospace");
    }

    #[test]
    fn test_buffer_partial_frame_persists() {
        let mut buffer = String::new();
        parse_sse_chunk(&mut buffer, "data: partial");
        assert_eq!(buffer, "data: partial");
    }

    #[test]
    fn test_multiple_delimiters_in_buffer() {
        let mut buffer = String::new();
        let events = parse_sse_chunk(&mut buffer, "data: a\n\ndata: b\r\n\r\ndata: c\n\n");
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].data, "a");
        assert_eq!(events[1].data, "b");
        assert_eq!(events[2].data, "c");
    }

    #[test]
    fn test_mixed_delimiters_split() {
        let mut buffer = String::new();
        let events1 = parse_sse_chunk(&mut buffer, "data: x\r\n\r");
        assert_eq!(events1.len(), 0);
        let events2 = parse_sse_chunk(&mut buffer, "\ndata: y\n\n");
        assert_eq!(events2.len(), 2);
        assert_eq!(events2[0].data, "x");
        assert_eq!(events2[1].data, "y");
    }
}
