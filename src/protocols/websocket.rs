use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use http::header::HeaderName;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

use crate::chaos::{ChaosEngine, ChaosFault};
use crate::config::WsMode;
use crate::metrics::RequestMetric;

use super::ProtocolEngine;

pub struct WebSocketEngine {
    headers: Vec<(String, String)>,
    ws_mode: WsMode,
    payload: Option<String>,
    chaos: ChaosEngine,
    timeout_secs: u64,
}

impl WebSocketEngine {
    pub fn new(
        headers: Vec<(String, String)>,
        ws_mode: WsMode,
        payload: Option<String>,
        chaos: ChaosEngine,
        timeout_secs: u64,
    ) -> Self {
        Self {
            headers,
            ws_mode,
            payload,
            chaos,
            timeout_secs,
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
        ws_stream: &mut WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
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
            return RequestMetric {
                latency_micros: req_start.elapsed().as_micros(),
                status_code: 0,
                bytes_received: 0,
            };
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
                return RequestMetric {
                    latency_micros: req_start.elapsed().as_micros(),
                    status_code: 0,
                    bytes_received: 0,
                };
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
        let engine =
            WebSocketEngine::new(vec![], WsMode::Handshake, None, ChaosEngine::default(), 5);
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
        let engine =
            WebSocketEngine::new(vec![], WsMode::PingPong, None, ChaosEngine::default(), 10);
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
        let engine =
            WebSocketEngine::new(headers, WsMode::Handshake, None, ChaosEngine::default(), 5);
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
        );
        let metric = engine.execute_iteration(&ws_url).await;

        assert_eq!(metric.status_code, 200);
        assert!(metric.bytes_received > 0);
        assert!(metric.latency_micros > 0);
    }
}
