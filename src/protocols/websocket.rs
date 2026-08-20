use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use http::header::HeaderName;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

use crate::chaos::{ChaosEngine, ChaosFault};
use crate::config::WsMode;
use crate::metrics::RequestMetric;

use super::ProtocolEngine;

/// Error type for WebSocket engine operations.
#[derive(Debug)]
pub enum EngineError {
    NotConnected,
    ConnectionFailed(String),
    MaxMessagesReached,
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
            if let (Ok(name), Ok(val)) = (
                key.as_str().parse::<http::header::HeaderName>(),
                value.as_str().parse(),
            ) {
                request.headers_mut().insert(name, val);
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

        // Read response with timeout
        let timeout = Duration::from_secs(5);
        let read_result = tokio::time::timeout(timeout, async {
            let mut response_bytes = Vec::new();
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
                    Message::Close(_) => break,
                    _ => {}
                }
            }
            response_bytes
        })
        .await;

        read_result.map_err(|_| EngineError::ConnectionFailed("read timed out".into()))
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
        }
    }

    fn effective_timeout(&self) -> Duration {
        Duration::from_secs(if self.timeout_secs > 0 {
            self.timeout_secs
        } else {
            5
        })
    }

    /// Read from WebSocket stream with timeout, returning bytes received.
    async fn read_with_timeout(
        &self,
        ws_stream: &mut tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    ) -> u64 {
        let timeout = self.effective_timeout();
        let read_result = tokio::time::timeout(timeout, async {
            let mut total_bytes = 0u64;
            while let Some(Ok(msg)) = ws_stream.next().await {
                match msg {
                    Message::Text(text) => {
                        total_bytes += text.len() as u64;
                        break;
                    }
                    Message::Binary(bin) => {
                        total_bytes += bin.len() as u64;
                        break;
                    }
                    Message::Pong(_) => {
                        total_bytes += 1;
                        break;
                    }
                    Message::Close(_) => break,
                    _ => {}
                }
            }
            total_bytes
        })
        .await;

        read_result.unwrap_or(0)
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

        let (status_code, bytes_received) = match result {
            Ok((mut ws_stream, _response)) => {
                // Phase 2: Post-connection chaos
                match fault {
                    Some(ChaosFault::CorruptedPayload) => {
                        // Send binary frame with raw corrupted bytes
                        tracing::trace!("ws chaos: corrupted payload (binary)");
                        let _ = ws_stream
                            .send(Message::Binary(Bytes::from_static(b"\xff\xfe\xbd\xef")))
                            .await;
                        let total_bytes = self.read_with_timeout(&mut ws_stream).await;
                        let _ = ws_stream.close(None).await;
                        (200, total_bytes)
                    }
                    _ => {
                        // Normal execution (LatencySpike already applied, or no fault)
                        match self.ws_mode {
                            WsMode::Handshake => {
                                let _ = ws_stream.close(None).await;
                                (200, 0)
                            }
                            WsMode::PingPong => {
                                let _ = ws_stream.send(Message::Ping(Bytes::new())).await;
                                let total_bytes = self.read_with_timeout(&mut ws_stream).await;
                                let _ = ws_stream.close(None).await;
                                if total_bytes > 0 {
                                    (200, total_bytes)
                                } else {
                                    (0, 0)
                                }
                            }
                            WsMode::Stream => {
                                let payload_str = self.payload.as_deref().unwrap_or("ping");
                                let _ = ws_stream.send(Message::Text(payload_str.into())).await;
                                let total_bytes = self.read_with_timeout(&mut ws_stream).await;
                                let _ = ws_stream.close(None).await;
                                (200, total_bytes)
                            }
                        }
                    }
                }
            }
            Err(e) => {
                tracing::debug!(error = %e, "websocket handshake failed");
                (0, 0)
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
            connection_latency_us: None,
            timestamp_sent_ns: None,
            e2e_latency_us: None,
        }
    }

    async fn create_worker_context(&self) -> Option<Box<dyn std::any::Any + Send>> {
        if self.ws_persistent {
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
        ctx: &mut (dyn std::any::Any + Send),
    ) -> RequestMetric {
        let session = ctx
            .downcast_mut::<PersistentWsSession>()
            .expect("invalid context type");

        let req_start = Instant::now();

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
                RequestMetric {
                    latency_micros: frame_latency,
                    status_code: 200,
                    bytes_received: response_bytes.len() as u64,
                    is_reconnect: connection_latency_us > 0,
                    connection_latency_us: Some(connection_latency_us),
                    timestamp_sent_ns: None,
                    e2e_latency_us: None,
                }
            }
            Err(_) => RequestMetric {
                latency_micros: req_start.elapsed().as_micros(),
                status_code: 0,
                bytes_received: 0,
                is_reconnect: connection_latency_us > 0,
                connection_latency_us: Some(connection_latency_us),
                timestamp_sent_ns: None,
                e2e_latency_us: None,
            },
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
        assert!(metric.bytes_received > 0);
        assert!(metric.latency_micros > 0);
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
}
