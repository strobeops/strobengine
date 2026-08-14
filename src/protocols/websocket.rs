use std::time::Instant;

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use http::header::HeaderName;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

use crate::config::WsMode;
use crate::metrics::RequestMetric;

use super::ProtocolEngine;

pub struct WebSocketEngine {
    headers: Vec<(String, String)>,
    ws_mode: WsMode,
    payload: Option<String>,
}

impl WebSocketEngine {
    pub fn new(headers: Vec<(String, String)>, ws_mode: WsMode, payload: Option<String>) -> Self {
        Self {
            headers,
            ws_mode,
            payload,
        }
    }
}

#[async_trait]
impl ProtocolEngine for WebSocketEngine {
    async fn execute_iteration(&self, target_url: &str) -> RequestMetric {
        let req_start = Instant::now();

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
                match self.ws_mode {
                    WsMode::Handshake => {
                        let _ = ws_stream.close(None).await;
                        (200, 0)
                    }
                    WsMode::PingPong => {
                        // Send ping frame
                        let _ = ws_stream.send(Message::Ping(Bytes::new())).await;

                        // Wait for pong response
                        let mut got_pong = false;
                        while let Some(Ok(msg)) = ws_stream.next().await {
                            match msg {
                                Message::Pong(_) => {
                                    got_pong = true;
                                    break;
                                }
                                Message::Close(_) => break,
                                _ => {}
                            }
                        }

                        let _ = ws_stream.close(None).await;
                        if got_pong { (200, 1) } else { (0, 0) }
                    }
                    WsMode::Stream => {
                        // Send text payload (fallback to "ping" if None)
                        let payload_str = self.payload.as_deref().unwrap_or("ping");
                        let _ = ws_stream.send(Message::Text(payload_str.into())).await;

                        // Await response frame
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
                                Message::Close(_) => break,
                                _ => {}
                            }
                        }

                        let _ = ws_stream.close(None).await;
                        (200, total_bytes)
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
        let engine = WebSocketEngine::new(vec![], WsMode::Handshake, None);
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
                // Echo ping back as pong
                while let Some(Ok(msg)) = ws_stream.next().await {
                    if let Message::Ping(data) = msg {
                        let _ = ws_stream.send(Message::Pong(data)).await;
                    }
                }
            }
        });

        let ws_url = format!("ws://{}", local_addr);
        let engine = WebSocketEngine::new(vec![], WsMode::PingPong, None);
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
        let engine = WebSocketEngine::new(headers, WsMode::Handshake, None);
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
                // Echo text messages back
                while let Some(Ok(msg)) = ws_stream.next().await {
                    if let Message::Text(text) = msg {
                        let _ = ws_stream.send(Message::Text(text)).await;
                    }
                }
            }
        });

        let ws_url = format!("ws://{}", local_addr);
        let engine = WebSocketEngine::new(vec![], WsMode::Stream, Some("hello".to_string()));
        let metric = engine.execute_iteration(&ws_url).await;

        assert_eq!(metric.status_code, 200);
        assert!(metric.bytes_received > 0);
        assert!(metric.latency_micros > 0);
    }
}
