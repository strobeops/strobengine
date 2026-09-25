use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{Sink, SinkExt, StreamExt};
use http::header::HeaderName;
use tokio_tungstenite::tungstenite::Error as WsError;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

use crate::chaos::{ChaosEngine, ChaosFault};
use crate::config::WsMode;
use crate::metrics::{
    ConnectionMetrics, RequestMetric, WsSample, create_pubsub_payload, parse_pubsub_payload,
    wallclock_ns,
};

use super::ProtocolEngine;

type WsStream = futures_util::stream::SplitSink<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    Message,
>;

type WsStreamReader = futures_util::stream::SplitStream<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
>;

/// Byte length of a tungstenite frame's payload (used for write-buffer accounting).
fn message_payload_len(msg: &Message) -> u64 {
    match msg {
        Message::Text(t) => t.len() as u64,
        Message::Binary(b) => b.len() as u64,
        Message::Ping(p) => p.len() as u64,
        Message::Pong(p) => p.len() as u64,
        Message::Frame(f) => f.payload().len() as u64,
        Message::Close(_) => 0,
    }
}

/// Build an optional per-iteration heartbeat sample, returning `None` when the
/// sample carries no meaningful counters (so non-WS / idle iterations do not
/// flip the aggregate `has_ws` flag).
fn ws_sample(
    pings_sent: u64,
    pings_received: u64,
    pongs_solicited: u64,
    pongs_unsolicited: u64,
) -> Option<WsSample> {
    let sample = WsSample {
        pings_sent,
        pings_received,
        pongs_solicited,
        pongs_unsolicited,
        ..Default::default()
    };
    if sample.is_significant() {
        Some(sample)
    } else {
        None
    }
}

/// A [`Sink`] wrapper that accounts for bytes handed to tungstenite's write path
/// versus bytes acknowledged on flush, exposing an in-flight (buffered) depth
/// high-water marker.
///
/// This is a *proxy* for socket backpressure: tungstenite 0.26 does not expose
/// the OS TCP send-buffer depth, so `written - flushed` reflects frames queued in
/// the WebSocket write buffer awaiting a flush drain, not kernel socket state.
pub struct CountingSink<S> {
    inner: S,
    written: u64,
    flushed: u64,
    max_in_flight: u64,
    warn_bytes: u64,
    over_threshold: bool,
}

impl<S> CountingSink<S> {
    pub fn new(inner: S, warn_bytes: u64) -> Self {
        Self {
            inner,
            written: 0,
            flushed: 0,
            max_in_flight: 0,
            warn_bytes,
            over_threshold: false,
        }
    }

    /// Bytes queued into the write buffer but not yet acknowledged by a flush.
    pub fn in_flight(&self) -> u64 {
        self.written.saturating_sub(self.flushed)
    }

    /// Peak in-flight bytes observed over the sink's lifetime.
    pub fn max_in_flight(&self) -> u64 {
        self.max_in_flight
    }
}

impl<S> Sink<Message> for CountingSink<S>
where
    S: Sink<Message, Error = WsError> + Unpin,
{
    type Error = WsError;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.get_mut().inner).poll_ready(cx)
    }

    fn start_send(self: Pin<&mut Self>, item: Message) -> Result<(), Self::Error> {
        let this = self.get_mut();
        this.written = this.written.saturating_add(message_payload_len(&item));
        let depth = this.in_flight();
        if depth > this.max_in_flight {
            this.max_in_flight = depth;
        }
        if this.warn_bytes > 0 {
            if depth >= this.warn_bytes && !this.over_threshold {
                this.over_threshold = true;
                tracing::warn!(
                    in_flight_bytes = depth,
                    warn_bytes = this.warn_bytes,
                    "websocket write buffer backpressure threshold exceeded"
                );
            } else if depth < this.warn_bytes {
                this.over_threshold = false;
            }
        }
        Pin::new(&mut this.inner).start_send(item)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        let result = Pin::new(&mut this.inner).poll_flush(cx);
        if result.is_ready() {
            this.flushed = this.written;
        }
        result
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.get_mut().inner).poll_close(cx)
    }
}

impl<S: Unpin> Unpin for CountingSink<S> {}

/// Result of a bounded read pass over a WebSocket stream: terminal bytes plus the
/// control frames observed en route.
#[derive(Debug, Default)]
struct ReadOutcome {
    bytes: u64,
    got_pong: bool,
    pings_received: u64,
    pongs_received: u64,
}

pub struct PublisherSession {
    write: Option<CountingSink<WsStream>>,
    user_payload: Vec<u8>,
    headers: Vec<(String, String)>,
    timeout: Duration,
}

pub struct SubscriberSession {
    read: Option<WsStreamReader>,
    pub received_count: u64,
    headers: Vec<(String, String)>,
    timeout: Duration,
}

/// Error type for WebSocket engine operations.
#[derive(Debug)]
pub enum EngineError {
    NotConnected,
    ConnectionFailed(String),
    MaxMessagesReached,
}

impl std::fmt::Display for EngineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EngineError::NotConnected => write!(f, "not connected"),
            EngineError::ConnectionFailed(msg) => write!(f, "connection failed: {msg}"),
            EngineError::MaxMessagesReached => write!(f, "max messages reached"),
        }
    }
}

/// Persistent WebSocket session that maintains a connection across iterations.
/// Each worker thread owns its own session — no locking required.
pub struct PersistentWsSession {
    stream: Option<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    >,
    messages_sent: u64,
    max_messages: Option<u64>,
    headers: Vec<(String, String)>,
    timeout_secs: u64,
    last_pings_received: u64,
    last_pongs_received: u64,
}

impl PersistentWsSession {
    pub fn new(
        headers: Vec<(String, String)>,
        max_messages: Option<u64>,
        timeout_secs: u64,
    ) -> Self {
        Self {
            stream: None,
            messages_sent: 0,
            max_messages,
            headers,
            timeout_secs,
            last_pings_received: 0,
            last_pongs_received: 0,
        }
    }

    /// Ensure the WebSocket connection is active. Returns connection latency in microseconds.
    pub async fn ensure_connected(&mut self, url: &str) -> Result<u128, EngineError> {
        if self.stream.is_some() && !self.is_max_reached() {
            return Ok(0); // Already connected
        }

        let start = Instant::now();

        // Build request with custom headers
        let mut request = url
            .into_client_request()
            .map_err(|e| EngineError::ConnectionFailed(e.to_string()))?;

        for (key, value) in &self.headers {
            match (
                key.as_str().parse::<http::header::HeaderName>(),
                value.as_str().parse(),
            ) {
                (Ok(name), Ok(val)) => {
                    request.headers_mut().insert(name, val);
                }
                _ => {
                    tracing::warn!(key = %key, "invalid WebSocket header skipped");
                }
            }
        }

        let result = tokio::time::timeout(
            Duration::from_secs(self.timeout_secs),
            tokio_tungstenite::connect_async(request),
        )
        .await;

        match result {
            Ok(Ok((ws_stream, _response))) => {
                self.stream = Some(ws_stream);
                self.messages_sent = 0;
                Ok(start.elapsed().as_micros())
            }
            Ok(Err(e)) => Err(EngineError::ConnectionFailed(e.to_string())),
            Err(_) => Err(EngineError::ConnectionFailed("connection timed out".into())),
        }
    }

    /// Send a payload and receive a response. Returns the response bytes.
    pub async fn send_and_receive(&mut self, payload: &[u8]) -> Result<Vec<u8>, EngineError> {
        let stream = self.stream.as_mut().ok_or(EngineError::NotConnected)?;

        stream
            .send(Message::Text(
                String::from_utf8_lossy(payload).into_owned().into(),
            ))
            .await
            .map_err(|e| EngineError::ConnectionFailed(e.to_string()))?;

        self.messages_sent += 1;
        self.last_pings_received = 0;
        self.last_pongs_received = 0;

        // Read response with timeout
        let timeout = Duration::from_secs(if self.timeout_secs > 0 {
            self.timeout_secs
        } else {
            5
        });
        let read_result = tokio::time::timeout(timeout, async {
            let mut response_bytes = Vec::new();
            let mut pings = 0u64;
            let mut pongs = 0u64;
            while let Some(Ok(msg)) = stream.next().await {
                match msg {
                    Message::Text(text) => {
                        response_bytes.extend_from_slice(text.as_bytes());
                        break;
                    }
                    Message::Binary(bin) => {
                        response_bytes.extend_from_slice(&bin);
                        break;
                    }
                    Message::Ping(_) => pings += 1,
                    Message::Pong(_) => pongs += 1,
                    Message::Close(_) => break,
                    _ => {}
                }
            }
            (response_bytes, pings, pongs)
        })
        .await;

        match read_result {
            Ok((response_bytes, pings, pongs)) => {
                self.last_pings_received = pings;
                self.last_pongs_received = pongs;
                Ok(response_bytes)
            }
            Err(_) => Err(EngineError::ConnectionFailed("read timed out".into())),
        }
    }

    /// Clean up the connection.
    pub async fn close(&mut self) {
        if let Some(mut stream) = self.stream.take() {
            let _ = stream.close(None).await;
        }
    }

    fn is_max_reached(&self) -> bool {
        self.max_messages
            .is_some_and(|max| self.messages_sent >= max)
    }
}

#[async_trait::async_trait]
impl super::WorkerSession for PersistentWsSession {
    async fn shutdown(&mut self) {
        self.close().await;
    }
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

#[async_trait::async_trait]
impl super::WorkerSession for PublisherSession {
    async fn shutdown(&mut self) {}
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

#[async_trait::async_trait]
impl super::WorkerSession for SubscriberSession {
    async fn shutdown(&mut self) {}
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

pub struct WebSocketEngine {
    headers: Vec<(String, String)>,
    ws_mode: WsMode,
    payload: Option<String>,
    chaos: ChaosEngine,
    timeout_secs: u64,
    ws_persistent: bool,
    #[allow(dead_code)] // Reserved for future keepalive implementation
    ws_keepalive_secs: Option<u64>,
    ws_max_messages: Option<u64>,
    ws_role: Option<String>,
    ws_publish_interval_ms: Option<u64>,
    ws_max_buffer_bytes: u64,
    ws_backpressure_warn_ratio: f32,
}

impl WebSocketEngine {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        headers: Vec<(String, String)>,
        ws_mode: WsMode,
        payload: Option<String>,
        chaos: ChaosEngine,
        timeout_secs: u64,
        ws_persistent: bool,
        #[allow(dead_code)] // Reserved for future keepalive implementation
        ws_keepalive_secs: Option<u64>,
        ws_max_messages: Option<u64>,
    ) -> Self {
        Self {
            headers,
            ws_mode,
            payload,
            chaos,
            timeout_secs,
            ws_persistent,
            ws_keepalive_secs,
            ws_max_messages,
            ws_role: None,
            ws_publish_interval_ms: None,
            ws_max_buffer_bytes: 1_048_576,
            ws_backpressure_warn_ratio: 0.8,
        }
    }

    pub fn with_role(mut self, role: Option<String>, publish_interval_ms: Option<u64>) -> Self {
        self.ws_role = role;
        self.ws_publish_interval_ms = publish_interval_ms;
        self
    }

    /// Configure the outbound write-buffer backpressure threshold.
    ///
    /// `max_buffer_bytes` bounds the frame queue depth; `warn_ratio` (0.0..=1.0)
    /// triggers an edge-detected warning once in-flight bytes cross that fraction.
    pub fn with_backpressure(mut self, max_buffer_bytes: u64, warn_ratio: f32) -> Self {
        self.ws_max_buffer_bytes = max_buffer_bytes;
        self.ws_backpressure_warn_ratio = warn_ratio;
        self
    }

    /// Threshold in bytes at which backpressure is flagged (0 disables warnings).
    fn warn_bytes(&self) -> u64 {
        if self.ws_backpressure_warn_ratio <= 0.0 {
            return 0;
        }
        ((self.ws_max_buffer_bytes as f32) * self.ws_backpressure_warn_ratio).max(1.0) as u64
    }

    fn is_publisher(&self) -> bool {
        self.ws_role.as_deref() == Some("publisher")
    }

    fn is_subscriber(&self) -> bool {
        self.ws_role.as_deref() == Some("subscriber")
    }

    fn effective_timeout(&self) -> Duration {
        Duration::from_secs(if self.timeout_secs > 0 {
            self.timeout_secs
        } else {
            5
        })
    }

    /// Read from WebSocket stream with timeout, tallying control frames.
    ///
    /// Incoming `Ping` frames are counted (tungstenite auto-replies the `Pong` at
    /// the protocol layer) and the loop keeps reading until a data frame, a `Pong`,
    /// or close terminates it.
    async fn read_with_timeout(
        &self,
        ws_stream: &mut tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    ) -> ReadOutcome {
        let timeout = self.effective_timeout();
        let read_result = tokio::time::timeout(timeout, async {
            let mut outcome = ReadOutcome::default();
            while let Some(Ok(msg)) = ws_stream.next().await {
                match msg {
                    Message::Text(text) => {
                        outcome.bytes += text.len() as u64;
                        break;
                    }
                    Message::Binary(bin) => {
                        outcome.bytes += bin.len() as u64;
                        break;
                    }
                    Message::Ping(data) => {
                        outcome.bytes += data.len() as u64;
                        outcome.pings_received += 1;
                    }
                    Message::Pong(data) => {
                        outcome.bytes += data.len() as u64;
                        outcome.got_pong = true;
                        outcome.pongs_received += 1;
                        break;
                    }
                    Message::Close(_) => break,
                    _ => {}
                }
            }
            outcome
        })
        .await;

        read_result.unwrap_or_default()
    }

    async fn connect_ws(
        headers: &[(String, String)],
        timeout: Duration,
        _target_url: &str,
    ) -> Result<(WsStream, WsStreamReader), EngineError> {
        let mut request = _target_url
            .into_client_request()
            .map_err(|e| EngineError::ConnectionFailed(e.to_string()))?;

        for (key, value) in headers {
            match (key.as_str().parse::<HeaderName>(), value.as_str().parse()) {
                (Ok(name), Ok(val)) => {
                    request.headers_mut().insert(name, val);
                }
                _ => {
                    tracing::warn!(key = %key, "invalid WebSocket header skipped");
                }
            }
        }

        let result = tokio::time::timeout(timeout, tokio_tungstenite::connect_async(request)).await;

        match result {
            Ok(Ok((ws_stream, _response))) => {
                let (write, read) = ws_stream.split();
                Ok((write, read))
            }
            Ok(Err(e)) => Err(EngineError::ConnectionFailed(e.to_string())),
            Err(_) => Err(EngineError::ConnectionFailed("connection timed out".into())),
        }
    }

    async fn execute_publisher_iteration(
        &self,
        target_url: &str,
        session: &mut PublisherSession,
    ) -> RequestMetric {
        let req_start = Instant::now();

        // Lazy connect on first iteration
        if session.write.is_none() {
            match Self::connect_ws(&session.headers, session.timeout, target_url).await {
                Ok((write, _read)) => {
                    session.write = Some(CountingSink::new(write, self.warn_bytes()));
                }
                Err(_) => {
                    return RequestMetric::error(req_start.elapsed().as_micros());
                }
            }
        }

        // Apply chaos
        let fault = self.chaos.select_fault();

        if let Some(ChaosFault::ConnectionDrop) = fault {
            tracing::trace!("ws chaos: connection drop (publisher)");
            return RequestMetric::error(req_start.elapsed().as_micros());
        }

        if let Some(ChaosFault::LatencySpike { duration_ms }) = fault {
            tracing::trace!(duration_ms, "ws chaos: latency spike (publisher)");
            tokio::time::sleep(Duration::from_millis(duration_ms)).await;
        }

        let payload_bytes = match fault {
            Some(ChaosFault::CorruptedPayload) => b"\xff\xfe\xbd\xef".to_vec(),
            _ => create_pubsub_payload(&session.user_payload),
        };

        let write = match session.write.as_mut() {
            Some(w) => w,
            None => return RequestMetric::error(req_start.elapsed().as_micros()),
        };
        let payload_len = payload_bytes.len() as u64;

        // Queue the frame without flushing, sample the write-buffer depth at that
        // instant (bytes enqueued but not yet drained), then drive the flush.
        let feed_result = write
            .feed(Message::Binary(Bytes::from(payload_bytes)))
            .await;
        let in_flight = write.in_flight();
        let flushed = write.flush().await;
        let depth_breach = self.warn_bytes() > 0 && in_flight >= self.warn_bytes();

        let latency_micros = req_start.elapsed().as_micros();

        match feed_result.and(flushed) {
            Ok(()) => {
                let ws = Some(WsSample {
                    pings_sent: 0,
                    pings_received: 0,
                    pongs_solicited: 0,
                    pongs_unsolicited: 0,
                    backpressure_max_bytes: in_flight,
                    backpressure_samples_sum: in_flight,
                    backpressure_sample_count: 1,
                    threshold_breaches: if depth_breach { 1 } else { 0 },
                });
                RequestMetric {
                    latency_micros,
                    status_code: 200,
                    bytes_received: payload_len,
                    is_reconnect: false,
                    connection: ConnectionMetrics {
                        connection_latency_us: None,
                        timestamp_sent_ns: Some(wallclock_ns()),
                        e2e_latency_us: None,
                        dns_resolution_us: None,
                        is_socket_reused: session.write.is_some(),
                    },
                    quic_handshake_us: None,
                    quic_0rtt_used: false,
                    quic_retransmits: None,
                    sse_events_received: None,
                    sse_first_event_us: None,
                    sse_event_interval_us: None,
                    ws,
                    chaos_fault: fault,
                }
            }
            Err(e) => {
                tracing::debug!(error = %e, "publisher send failed");
                RequestMetric::error(latency_micros)
            }
        }
    }

    async fn execute_subscriber_iteration(
        &self,
        target_url: &str,
        session: &mut SubscriberSession,
    ) -> RequestMetric {
        let req_start = Instant::now();

        // Lazy connect on first iteration
        if session.read.is_none() {
            match Self::connect_ws(&session.headers, session.timeout, target_url).await {
                Ok((_write, read)) => {
                    session.read = Some(read);
                }
                Err(_) => {
                    return RequestMetric::error(req_start.elapsed().as_micros());
                }
            }
        }

        // Apply chaos
        let fault = self.chaos.select_fault();

        if let Some(ChaosFault::ConnectionDrop) = fault {
            tracing::trace!("ws chaos: connection drop (subscriber)");
            return RequestMetric::error(req_start.elapsed().as_micros());
        }

        if let Some(ChaosFault::LatencySpike { duration_ms }) = fault {
            tracing::trace!(duration_ms, "ws chaos: latency spike (subscriber)");
            tokio::time::sleep(Duration::from_millis(duration_ms)).await;
        }

        let read = match session.read.as_mut() {
            Some(r) => r,
            None => return RequestMetric::error(req_start.elapsed().as_micros()),
        };

        let timeout = self.effective_timeout();
        let mut pings_recv = 0u64;
        let mut pongs_recv = 0u64;
        let read_result = tokio::time::timeout(timeout, async {
            let mut total_bytes = 0u64;
            while let Some(Ok(msg)) = read.next().await {
                match msg {
                    Message::Binary(bin) => {
                        total_bytes += bin.len() as u64;
                        session.received_count += 1;
                        return Some((total_bytes, bin.to_vec()));
                    }
                    Message::Text(text) => {
                        total_bytes += text.len() as u64;
                        session.received_count += 1;
                        return Some((total_bytes, text.as_bytes().to_vec()));
                    }
                    Message::Ping(_) => {
                        // Server-initiated keepalive (auto-Ponged by tungstenite).
                        pings_recv += 1;
                    }
                    Message::Pong(_) => {
                        // No Ping was solicited by a subscriber, so this is anomalous.
                        pongs_recv += 1;
                    }
                    Message::Close(_) => return None,
                    _ => {}
                }
            }
            None
        })
        .await;

        let latency_micros = req_start.elapsed().as_micros();

        match read_result {
            Ok(Some((bytes, data))) => {
                let (e2e_latency_us, _user_payload) =
                    if let Some((sent_ns, rest)) = parse_pubsub_payload(&data) {
                        let now_ns = wallclock_ns();
                        let e2e_us = now_ns.saturating_sub(sent_ns) / 1000;
                        (Some(e2e_us), rest.to_vec())
                    } else {
                        (None, data)
                    };

                let ws = if pings_recv > 0 || pongs_recv > 0 {
                    Some(WsSample {
                        pings_sent: 0,
                        pings_received: pings_recv,
                        pongs_solicited: 0,
                        pongs_unsolicited: pongs_recv,
                        ..Default::default()
                    })
                } else {
                    None
                };

                RequestMetric {
                    latency_micros,
                    status_code: 200,
                    bytes_received: bytes,
                    is_reconnect: false,
                    connection: ConnectionMetrics {
                        connection_latency_us: None,
                        timestamp_sent_ns: None,
                        e2e_latency_us,
                        dns_resolution_us: None,
                        is_socket_reused: session.read.is_some(),
                    },
                    quic_handshake_us: None,
                    quic_0rtt_used: false,
                    quic_retransmits: None,
                    sse_events_received: None,
                    sse_first_event_us: None,
                    sse_event_interval_us: None,
                    ws,
                    chaos_fault: fault,
                }
            }
            Ok(None) => RequestMetric::error(latency_micros),
            Err(_) => {
                tracing::debug!("subscriber receive timed out");
                RequestMetric::error(latency_micros)
            }
        }
    }
}

#[async_trait]
impl ProtocolEngine for WebSocketEngine {
    async fn execute_iteration(&self, target_url: &str) -> RequestMetric {
        let req_start = Instant::now();

        // Phase 1: Pre-connection chaos
        let fault = self.chaos.select_fault();

        // ConnectionDrop: short-circuit with immediate timeout
        if let Some(ChaosFault::ConnectionDrop) = fault {
            tracing::trace!("ws chaos: connection drop");
            let _ = tokio::time::timeout(Duration::from_nanos(1), async {
                let _ = target_url.into_client_request();
            })
            .await;
            return RequestMetric::error(req_start.elapsed().as_micros());
        }

        // LatencySpike: sleep before connecting
        if let Some(ChaosFault::LatencySpike { duration_ms }) = fault {
            tracing::trace!(duration_ms, "ws chaos: latency spike");
            tokio::time::sleep(Duration::from_millis(duration_ms)).await;
        }

        // Build request with custom headers
        let mut request = match target_url.into_client_request() {
            Ok(r) => r,
            Err(e) => {
                tracing::debug!(error = %e, "invalid WebSocket URL");
                return RequestMetric::error(req_start.elapsed().as_micros());
            }
        };

        for (key, value) in &self.headers {
            if let (Ok(name), Ok(val)) =
                (key.as_str().parse::<HeaderName>(), value.as_str().parse())
            {
                request.headers_mut().insert(name, val);
            }
        }

        let result = tokio_tungstenite::connect_async(request).await;

        let (status_code, bytes_received, ws) = match result {
            Ok((mut ws_stream, _response)) => {
                // Phase 2: Post-connection chaos
                match fault {
                    Some(ChaosFault::CorruptedPayload) => {
                        // Send binary frame with raw corrupted bytes
                        tracing::trace!("ws chaos: corrupted payload (binary)");
                        let _ = ws_stream
                            .send(Message::Binary(Bytes::from_static(b"\xff\xfe\xbd\xef")))
                            .await;
                        let outcome = self.read_with_timeout(&mut ws_stream).await;
                        let _ = ws_stream.close(None).await;
                        (
                            200,
                            outcome.bytes,
                            ws_sample(0, outcome.pings_received, 0, outcome.pongs_received),
                        )
                    }
                    _ => {
                        // Normal execution (LatencySpike already applied, or no fault)
                        match self.ws_mode {
                            WsMode::Handshake => {
                                let _ = ws_stream.close(None).await;
                                (200, 0, None)
                            }
                            WsMode::PingPong => {
                                let sent_ok =
                                    ws_stream.send(Message::Ping(Bytes::new())).await.is_ok();
                                let pings_sent = if sent_ok { 1 } else { 0 };
                                let outcome = self.read_with_timeout(&mut ws_stream).await;
                                let _ = ws_stream.close(None).await;
                                // A Pong is solicited iff it answers the Ping we sent.
                                let pongs_solicited = outcome.pongs_received.min(pings_sent);
                                let pongs_unsolicited =
                                    outcome.pongs_received.saturating_sub(pongs_solicited);
                                let ws = ws_sample(
                                    pings_sent,
                                    outcome.pings_received,
                                    pongs_solicited,
                                    pongs_unsolicited,
                                );
                                if outcome.got_pong {
                                    (200, outcome.bytes, ws)
                                } else {
                                    (0, 0, ws)
                                }
                            }
                            WsMode::Stream => {
                                let payload_str = self.payload.as_deref().unwrap_or("ping");
                                let _ = ws_stream.send(Message::Text(payload_str.into())).await;
                                let outcome = self.read_with_timeout(&mut ws_stream).await;
                                let _ = ws_stream.close(None).await;
                                let ws =
                                    ws_sample(0, outcome.pings_received, 0, outcome.pongs_received);
                                (200, outcome.bytes, ws)
                            }
                        }
                    }
                }
            }
            Err(e) => {
                tracing::debug!(error = %e, "websocket handshake failed");
                (0, 0, None)
            }
        };

        let latency_micros = req_start.elapsed().as_micros();

        if tracing::enabled!(tracing::Level::TRACE) {
            tracing::trace!(
                status = status_code,
                latency_us = latency_micros,
                "websocket iteration completed"
            );
        }

        RequestMetric {
            latency_micros,
            status_code,
            bytes_received,
            is_reconnect: false,
            connection: ConnectionMetrics {
                connection_latency_us: None,
                timestamp_sent_ns: None,
                e2e_latency_us: None,
                dns_resolution_us: None,
                is_socket_reused: false,
            },
            quic_handshake_us: None,
            quic_0rtt_used: false,
            quic_retransmits: None,
            sse_events_received: None,
            sse_first_event_us: None,
            sse_event_interval_us: None,
            ws,
            chaos_fault: fault,
        }
    }

    async fn create_worker_context(&self) -> Option<Box<dyn super::WorkerSession>> {
        let timeout = self.effective_timeout();

        if self.is_publisher() {
            let user_payload = self.payload.as_deref().unwrap_or("").as_bytes().to_vec();
            Some(Box::new(PublisherSession {
                write: None,
                user_payload,
                headers: self.headers.clone(),
                timeout,
            }))
        } else if self.is_subscriber() {
            Some(Box::new(SubscriberSession {
                read: None,
                received_count: 0,
                headers: self.headers.clone(),
                timeout,
            }))
        } else if self.ws_persistent {
            Some(Box::new(PersistentWsSession::new(
                self.headers.clone(),
                self.ws_max_messages,
                self.timeout_secs,
            )))
        } else {
            None
        }
    }

    async fn execute_iteration_with_context(
        &self,
        target_url: &str,
        ctx: &mut dyn super::WorkerSession,
    ) -> RequestMetric {
        // Publisher dispatch
        if let Some(session) = ctx.as_any_mut().downcast_mut::<PublisherSession>() {
            return self.execute_publisher_iteration(target_url, session).await;
        }

        // Subscriber dispatch
        if let Some(session) = ctx.as_any_mut().downcast_mut::<SubscriberSession>() {
            return self.execute_subscriber_iteration(target_url, session).await;
        }

        // Persistent session dispatch (existing logic)
        let req_start = Instant::now();

        let session = match ctx.as_any_mut().downcast_mut::<PersistentWsSession>() {
            Some(s) => s,
            None => return RequestMetric::error(req_start.elapsed().as_micros()),
        };

        // Apply pre-connection chaos (LatencySpike)
        let fault = self.chaos.select_fault();

        if let Some(ChaosFault::ConnectionDrop) = fault {
            tracing::trace!("ws chaos: connection drop");
            return RequestMetric::error(req_start.elapsed().as_micros());
        }

        if let Some(ChaosFault::LatencySpike { duration_ms }) = fault {
            tracing::trace!(duration_ms, "ws chaos: latency spike");
            tokio::time::sleep(Duration::from_millis(duration_ms)).await;
        }

        // Ensure connection is active
        let connection_latency_us = match session.ensure_connected(target_url).await {
            Ok(lat) => lat,
            Err(_) => {
                return RequestMetric::error(0);
            }
        };

        // Determine payload based on chaos fault
        let payload_bytes = match fault {
            Some(ChaosFault::CorruptedPayload) => b"\xff\xfe\xbd\xef".to_vec(),
            _ => self
                .payload
                .as_ref()
                .map(|s| s.as_bytes().to_vec())
                .unwrap_or_else(|| b"ping".to_vec()),
        };

        // Send and receive
        match session.send_and_receive(&payload_bytes).await {
            Ok(response_bytes) => {
                let frame_latency = req_start.elapsed().as_micros();
                let ws = ws_sample(
                    0,
                    session.last_pings_received,
                    0,
                    session.last_pongs_received,
                );
                RequestMetric {
                    latency_micros: frame_latency,
                    status_code: 200,
                    bytes_received: response_bytes.len() as u64,
                    is_reconnect: connection_latency_us > 0,
                    connection: ConnectionMetrics {
                        connection_latency_us: Some(connection_latency_us),
                        timestamp_sent_ns: None,
                        e2e_latency_us: None,
                        dns_resolution_us: None,
                        is_socket_reused: connection_latency_us == 0,
                    },
                    quic_handshake_us: None,
                    quic_0rtt_used: false,
                    quic_retransmits: None,
                    sse_events_received: None,
                    sse_first_event_us: None,
                    sse_event_interval_us: None,
                    ws,
                    chaos_fault: fault,
                }
            }
            Err(_) => {
                let ws = ws_sample(
                    0,
                    session.last_pings_received,
                    0,
                    session.last_pongs_received,
                );
                RequestMetric {
                    latency_micros: req_start.elapsed().as_micros(),
                    status_code: 0,
                    bytes_received: 0,
                    is_reconnect: connection_latency_us > 0,
                    connection: ConnectionMetrics {
                        connection_latency_us: Some(connection_latency_us),
                        timestamp_sent_ns: None,
                        e2e_latency_us: None,
                        dns_resolution_us: None,
                        is_socket_reused: connection_latency_us == 0,
                    },
                    quic_handshake_us: None,
                    quic_0rtt_used: false,
                    quic_retransmits: None,
                    sse_events_received: None,
                    sse_first_event_us: None,
                    sse_event_interval_us: None,
                    ws,
                    chaos_fault: fault,
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;
    use tokio_tungstenite::accept_async;

    #[tokio::test]
    async fn test_websocket_handshake_iteration() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let local_addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            if let Ok((stream, _)) = listener.accept().await
                && let Ok(mut ws_stream) = accept_async(stream).await
            {
                while ws_stream.next().await.is_some() {}
            }
        });

        let ws_url = format!("ws://{}", local_addr);
        let engine = WebSocketEngine::new(
            vec![],
            WsMode::Handshake,
            None,
            ChaosEngine::default(),
            5,
            false,
            None,
            None,
        );
        let metric = engine.execute_iteration(&ws_url).await;

        assert_eq!(metric.status_code, 200);
        assert!(metric.latency_micros > 0);
    }

    #[tokio::test]
    async fn test_websocket_ping_pong() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let local_addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            if let Ok((stream, _)) = listener.accept().await
                && let Ok(mut ws_stream) = accept_async(stream).await
            {
                while let Some(Ok(msg)) = ws_stream.next().await {
                    if let Message::Ping(data) = msg {
                        let _ = ws_stream.send(Message::Pong(data)).await;
                    }
                }
            }
        });

        let ws_url = format!("ws://{}", local_addr);
        let engine = WebSocketEngine::new(
            vec![],
            WsMode::PingPong,
            None,
            ChaosEngine::default(),
            10,
            false,
            None,
            None,
        );
        let metric = engine.execute_iteration(&ws_url).await;

        assert_eq!(metric.status_code, 200);
        assert!(metric.latency_micros > 0);
    }

    #[tokio::test]
    async fn test_ping_pong_classifies_solicited_pong() {
        // Server echoes a Pong for our Ping -> that Pong is solicited.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let local_addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            if let Ok((stream, _)) = listener.accept().await
                && let Ok(mut ws_stream) = accept_async(stream).await
            {
                while let Some(Ok(msg)) = ws_stream.next().await {
                    if let Message::Ping(data) = msg {
                        let _ = ws_stream.send(Message::Pong(data)).await;
                    }
                }
            }
        });

        let engine = WebSocketEngine::new(
            vec![],
            WsMode::PingPong,
            None,
            ChaosEngine::default(),
            10,
            false,
            None,
            None,
        );
        let metric = engine
            .execute_iteration(&format!("ws://{}", local_addr))
            .await;

        let ws = metric.ws.expect("ping/pong iteration yields a ws sample");
        assert_eq!(ws.pings_sent, 1);
        assert_eq!(ws.pongs_solicited, 1);
        assert_eq!(ws.pongs_unsolicited, 0);
    }

    #[tokio::test]
    async fn test_server_pushed_control_frames_are_unsolicited() {
        // Server pushes an unsolicited Ping + Pong; the client solicited neither,
        // so the Pong must classify as unsolicited and the Ping is counted.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let local_addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            if let Ok((stream, _)) = listener.accept().await
                && let Ok(mut ws_stream) = accept_async(stream).await
            {
                let _ = ws_stream.next().await; // consume client's stream text
                let _ = ws_stream
                    .send(Message::Ping(Bytes::from_static(b"s1")))
                    .await;
                let _ = ws_stream
                    .send(Message::Pong(Bytes::from_static(b"u1")))
                    .await;
            }
        });

        let engine = WebSocketEngine::new(
            vec![],
            WsMode::Stream,
            Some("ping".into()),
            ChaosEngine::default(),
            10,
            false,
            None,
            None,
        );
        let metric = engine
            .execute_iteration(&format!("ws://{}", local_addr))
            .await;

        let ws = metric.ws.expect("control frames yield a ws sample");
        assert_eq!(ws.pings_sent, 0);
        assert_eq!(ws.pings_received, 1);
        assert_eq!(ws.pongs_solicited, 0);
        assert_eq!(ws.pongs_unsolicited, 1);
    }

    struct NullSink;
    impl Sink<Message> for NullSink {
        type Error = WsError;
        fn poll_ready(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn start_send(self: Pin<&mut Self>, _item: Message) -> Result<(), Self::Error> {
            Ok(())
        }
        fn poll_flush(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn poll_close(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn test_counting_sink_increments_on_enqueue_and_drains_on_flush() {
        use futures_util::SinkExt;
        let mut sink = CountingSink::new(NullSink, 64);

        // Enqueue without flushing -> in-flight grows by the queued payload bytes.
        sink.feed(Message::Binary(Bytes::from(vec![0u8; 100])))
            .await
            .unwrap();
        assert_eq!(sink.in_flight(), 100);
        assert_eq!(sink.max_in_flight(), 100);

        // Flush drains the buffer -> in-flight returns to zero, high-water retained.
        sink.flush().await.unwrap();
        assert_eq!(sink.in_flight(), 0);
        assert_eq!(sink.max_in_flight(), 100);
    }

    #[tokio::test]
    async fn test_websocket_custom_headers() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let local_addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            if let Ok((stream, _)) = listener.accept().await
                && let Ok(mut ws_stream) = accept_async(stream).await
            {
                while ws_stream.next().await.is_some() {}
            }
        });

        let ws_url = format!("ws://{}", local_addr);
        let headers = vec![("X-Custom-Test".to_string(), "e2e-value".to_string())];
        let engine = WebSocketEngine::new(
            headers,
            WsMode::Handshake,
            None,
            ChaosEngine::default(),
            5,
            false,
            None,
            None,
        );
        let metric = engine.execute_iteration(&ws_url).await;

        assert_eq!(metric.status_code, 200);
    }

    #[tokio::test]
    async fn test_websocket_stream_mode() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let local_addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            if let Ok((stream, _)) = listener.accept().await
                && let Ok(mut ws_stream) = accept_async(stream).await
            {
                while let Some(Ok(msg)) = ws_stream.next().await {
                    if let Message::Text(text) = msg {
                        let _ = ws_stream.send(Message::Text(text)).await;
                    }
                }
            }
        });

        let ws_url = format!("ws://{}", local_addr);
        let engine = WebSocketEngine::new(
            vec![],
            WsMode::Stream,
            Some("hello".to_string()),
            ChaosEngine::default(),
            5,
            false,
            None,
            None,
        );
        let metric = engine.execute_iteration(&ws_url).await;

        assert_eq!(metric.status_code, 200);
        assert!(metric.bytes_received > 0);
        assert!(metric.latency_micros > 0);
    }

    #[tokio::test]
    async fn test_pubsub_payload_roundtrip() {
        let user_payload = b"hello world";
        let encoded = create_pubsub_payload(user_payload);
        assert_eq!(encoded.len(), 16 + user_payload.len());

        let (sent_ns, rest) = parse_pubsub_payload(&encoded).unwrap();
        assert!(sent_ns > 0);
        assert_eq!(rest, user_payload);
    }

    #[tokio::test]
    async fn test_pubsub_payload_too_short() {
        assert!(parse_pubsub_payload(&[0u8; 15]).is_none());
        assert!(parse_pubsub_payload(&[]).is_none());
    }

    #[tokio::test]
    async fn test_publisher_subscriber_exchange() {
        use tokio::sync::broadcast;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let local_addr = listener.local_addr().unwrap();

        // Server that broadcasts every binary message to all connected clients
        let (tx, _rx) = broadcast::channel::<Vec<u8>>(32);

        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let mut ws = accept_async(stream).await.unwrap();
                let mut rx = tx.subscribe();
                let tx_inner = tx.clone();
                tokio::spawn(async move {
                    loop {
                        tokio::select! {
                            msg = ws.next() => {
                                match msg {
                                    Some(Ok(Message::Binary(bin))) => {
                                        let _ = tx_inner.send(bin.to_vec());
                                    }
                                    Some(Ok(Message::Close(_))) | None => break,
                                    _ => {}
                                }
                            }
                            msg = rx.recv() => {
                                if let Ok(data) = msg {
                                    let _ = ws.send(Message::Binary(Bytes::from(data))).await;
                                }
                            }
                        }
                    }
                });
            }
        });

        tokio::time::sleep(Duration::from_millis(50)).await;

        let ws_url = format!("ws://{}", local_addr);

        // Connect subscriber
        let sub_ws = tokio_tungstenite::connect_async(&ws_url).await.unwrap().0;
        let (_, mut sub_read) = sub_ws.split();

        // Spawn publisher
        let pub_url = ws_url.clone();
        let pub_handle = tokio::spawn(async move {
            let (mut pub_ws, _) = tokio_tungstenite::connect_async(&pub_url).await.unwrap();
            let payload = create_pubsub_payload(b"test-message");
            pub_ws
                .send(Message::Binary(Bytes::from(payload)))
                .await
                .unwrap();
        });

        // Subscriber waits for broadcast
        let recv_result = tokio::time::timeout(Duration::from_secs(2), async {
            while let Some(Ok(msg)) = sub_read.next().await {
                if let Message::Binary(bin) = msg
                    && let Some((sent_ns, _user_data)) = parse_pubsub_payload(&bin)
                {
                    let now_ns = wallclock_ns();
                    let e2e_us = now_ns.saturating_sub(sent_ns) / 1000;
                    return Some(e2e_us);
                }
            }
            None
        })
        .await;

        let _ = pub_handle.await;

        assert!(recv_result.is_ok());
        let e2e = recv_result.unwrap().unwrap();
        assert!(e2e < 1_000_000);
    }

    #[tokio::test]
    async fn test_publisher_iteration_via_context() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let local_addr = listener.local_addr().unwrap();

        // Echo server
        tokio::spawn(async move {
            if let Ok((stream, _)) = listener.accept().await
                && let Ok(mut ws_stream) = accept_async(stream).await
            {
                while let Some(Ok(msg)) = ws_stream.next().await {
                    if let Message::Binary(bin) = msg {
                        let _ = ws_stream.send(Message::Binary(bin)).await;
                    }
                }
            }
        });

        tokio::time::sleep(Duration::from_millis(50)).await;

        let ws_url = format!("ws://{}", local_addr);
        let engine = WebSocketEngine::new(
            vec![],
            WsMode::Stream,
            Some("test".to_string()),
            ChaosEngine::default(),
            5,
            false,
            None,
            None,
        )
        .with_role(Some("publisher".into()), Some(100));

        let mut ctx = engine.create_worker_context().await.unwrap();
        let metric = engine
            .execute_iteration_with_context(&ws_url, ctx.as_mut())
            .await;

        assert_eq!(metric.status_code, 200);
        assert!(metric.connection.timestamp_sent_ns.is_some());

        // The CountingSink feed->flush cycle yields a per-iteration backpressure sample.
        let ws = metric.ws.expect("publisher iteration yields a ws sample");
        assert_eq!(ws.backpressure_sample_count, 1);
        assert!(ws.backpressure_max_bytes > 0);
        // Payload is far below the default 1 MiB warn threshold.
        assert_eq!(ws.threshold_breaches, 0);
    }

    #[tokio::test]
    async fn test_publisher_backpressure_breach_counted_under_tiny_buffer() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let local_addr = listener.local_addr().unwrap();

        // Discard server (publisher never reads).
        tokio::spawn(async move {
            if let Ok((stream, _)) = listener.accept().await
                && let Ok(mut ws_stream) = accept_async(stream).await
            {
                while ws_stream.next().await.is_some() {}
            }
        });

        tokio::time::sleep(Duration::from_millis(50)).await;

        let ws_url = format!("ws://{}", local_addr);
        let engine = WebSocketEngine::new(
            vec![],
            WsMode::Stream,
            Some("x".repeat(2048)),
            ChaosEngine::default(),
            5,
            false,
            None,
            None,
        )
        .with_role(Some("publisher".into()), Some(100))
        // 1 KiB buffer, warn at 50% -> a 2 KiB payload crosses the threshold.
        .with_backpressure(1024, 0.5);

        let mut ctx = engine.create_worker_context().await.unwrap();
        let metric = engine
            .execute_iteration_with_context(&ws_url, ctx.as_mut())
            .await;

        assert_eq!(metric.status_code, 200);
        let ws = metric.ws.expect("publisher yields a ws sample");
        assert!(ws.backpressure_max_bytes > 1024);
        assert_eq!(ws.threshold_breaches, 1);
    }

    #[tokio::test]
    async fn test_subscriber_iteration_via_context() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let local_addr = listener.local_addr().unwrap();

        // Server that sends a timestamped message immediately
        tokio::spawn(async move {
            if let Ok((stream, _)) = listener.accept().await
                && let Ok(mut ws_stream) = accept_async(stream).await
            {
                let payload = create_pubsub_payload(b"hello");
                let _ = ws_stream.send(Message::Binary(Bytes::from(payload))).await;
                let _ = ws_stream.close(None).await;
            }
        });

        tokio::time::sleep(Duration::from_millis(50)).await;

        let ws_url = format!("ws://{}", local_addr);
        let engine = WebSocketEngine::new(
            vec![],
            WsMode::Stream,
            None,
            ChaosEngine::default(),
            5,
            false,
            None,
            None,
        )
        .with_role(Some("subscriber".into()), None);

        let mut ctx = engine.create_worker_context().await.unwrap();
        let metric = engine
            .execute_iteration_with_context(&ws_url, ctx.as_mut())
            .await;

        assert_eq!(metric.status_code, 200);
        assert!(metric.bytes_received > 0);
        assert!(metric.connection.e2e_latency_us.is_some());
    }
}
