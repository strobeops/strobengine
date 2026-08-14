use std::time::Instant;

use super::ProtocolEngine;
use crate::metrics::RequestMetric;
use async_trait::async_trait;

pub struct WebSocketEngine;

#[async_trait]
impl ProtocolEngine for WebSocketEngine {
    async fn execute_iteration(&self, target_url: &str) -> RequestMetric {
        let req_start = Instant::now();

        let result = tokio_tungstenite::connect_async(target_url).await;

        let (status_code, bytes_received) = match result {
            Ok((mut ws_stream, _response)) => {
                // Clean WebSocket shutdown — avoid abrupt TCP FIN
                let _ = ws_stream.close(None).await;
                (200, 0)
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
                "websocket handshake completed"
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
    use futures_util::StreamExt;
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
                // Drain incoming frames/close frames until the connection ends
                while ws_stream.next().await.is_some() {}
            }
        });

        let ws_url = format!("ws://{}", local_addr);
        let engine = WebSocketEngine;
        let metric = engine.execute_iteration(&ws_url).await;

        assert_eq!(metric.status_code, 200);
        assert!(metric.latency_micros > 0);
    }
}
