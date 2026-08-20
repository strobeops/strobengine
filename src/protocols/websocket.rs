use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use http::header::HeaderName;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

use crate::chaos::{ChaosEngine, ChaosFault};
use crate::config::WsMode;
use crate::metrics::{RequestMetric, create_pubsub_payload, wallclock_ns};

use super::ProtocolEngine;

type WsStream = futures_util::stream::SplitSink<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    Message,
>;

type WsStreamReader = futures_util::stream::SplitStream<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
>;

pub struct PublisherSession {
    pub write: WsStream,
    pub user_payload: Vec<u8>,
    pub publish_interval: Duration,
}

pub struct SubscriberSession {
    pub read: WsStreamReader,
    pub received_count: u64,
}

pub struct WebSocketEngine {
    headers: Vec<(String, String)>,
    ws_mode: WsMode,
    payload: Option<String>,
    chaos: ChaosEngine,
    ws_role: Option<String>,
    ws_publish_interval_ms: Option<u64>,
}

impl WebSocketEngine {
    pub fn new(
        headers: Vec<(String, String)>,
        ws_mode: WsMode,
        payload: Option<String>,
        chaos: ChaosEngine,
    ) -> Self {
        Self {
            headers,
            ws_mode,
            payload,
            chaos,
            ws_role: None,
            ws_publish_interval_ms: None,
        }
    }

    pub fn with_role(mut self, role: Option<String>, publish_interval_ms: Option<u64>) -> Self {
        self.ws_role = role;
        self.ws_publish_interval_ms = publish_interval_ms;
        self
    }

    #[allow(dead_code)]
    fn is_publisher(&self) -> bool {
        self.ws_role.as_deref() == Some("publisher")
    }

    #[allow(dead_code)]
    fn build_request(&self, target_url: &str) -> Result<http::Request<()>, String> {
        let mut request = target_url
            .into_client_request()
            .map_err(|e| format!("invalid WebSocket URL: {e}"))?;

        for (key, value) in &self.headers {
            if let (Ok(name), Ok(val)) =
                (key.as_str().parse::<HeaderName>(), value.as_str().parse())
            {
                request.headers_mut().insert(name, val);
            }
        }
        Ok(request)
    }

    #[allow(dead_code)]
    async fn connect(
        &self,
        target_url: &str,
        chaos_fault: Option<ChaosFault>,
    ) -> Option<(WsStream, WsStreamReader)> {
        if let Some(ChaosFault::ConnectionDrop) = chaos_fault {
            tracing::trace!("ws chaos: connection drop");
            return None;
        }

        if let Some(ChaosFault::LatencySpike { duration_ms }) = chaos_fault {
            tracing::trace!(duration_ms, "ws chaos: latency spike");
            tokio::time::sleep(Duration::from_millis(duration_ms)).await;
        }

        let request = match self.build_request(target_url) {
            Ok(r) => r,
            Err(e) => {
                tracing::debug!(error = %e, "invalid WebSocket URL");
                return None;
            }
        };

        match tokio_tungstenite::connect_async(request).await {
            Ok((ws_stream, _response)) => {
                let (write, read) = ws_stream.split();
                Some((write, read))
            }
            Err(e) => {
                tracing::debug!(error = %e, "websocket handshake failed");
                None
            }
        }
    }

    pub async fn execute_publisher_iteration(
        &self,
        _target_url: &str,
        session: &mut PublisherSession,
    ) -> RequestMetric {
        let req_start = Instant::now();

        let payload_bytes = create_pubsub_payload(&session.user_payload);

        let send_result = session
            .write
            .send(Message::Binary(Bytes::from(payload_bytes.clone())))
            .await;

        let latency_micros = req_start.elapsed().as_micros();

        match send_result {
            Ok(()) => {
                let _ = session.write.flush().await;
                RequestMetric {
                    latency_micros,
                    status_code: 200,
                    bytes_received: payload_bytes.len() as u64,
                    is_reconnect: false,
                    connection_latency_us: None,
                    timestamp_sent_ns: Some(wallclock_ns()),
                    e2e_latency_us: None,
                }
            }
            Err(e) => {
                tracing::debug!(error = %e, "publisher send failed");
                RequestMetric::error(latency_micros)
            }
        }
    }

    pub async fn execute_subscriber_iteration(
        &self,
        _target_url: &str,
        session: &mut SubscriberSession,
    ) -> RequestMetric {
        let req_start = Instant::now();
        let timeout = Duration::from_secs(5);

        let read_result = tokio::time::timeout(timeout, async {
            let mut total_bytes = 0u64;
            while let Some(Ok(msg)) = session.read.next().await {
                match msg {
                    Message::Binary(bin) => {
                        total_bytes += bin.len() as u64;
                        session.received_count += 1;
                        return Some(total_bytes);
                    }
                    Message::Text(text) => {
                        total_bytes += text.len() as u64;
                        session.received_count += 1;
                        return Some(total_bytes);
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
            Ok(Some(bytes)) => {
                let e2e_latency_us = None;
                RequestMetric {
                    latency_micros,
                    status_code: 200,
                    bytes_received: bytes,
                    is_reconnect: false,
                    connection_latency_us: None,
                    timestamp_sent_ns: None,
                    e2e_latency_us,
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
                        // Still try to read response
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
                    _ => {
                        // Normal execution (LatencySpike already applied, or no fault)
                        match self.ws_mode {
                            WsMode::Handshake => {
                                let _ = ws_stream.close(None).await;
                                (200, 0)
                            }
                            WsMode::PingPong => {
                                let _ = ws_stream.send(Message::Ping(Bytes::new())).await;
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
                                let payload_str = self.payload.as_deref().unwrap_or("ping");
                                let _ = ws_stream.send(Message::Text(payload_str.into())).await;
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::parse_pubsub_payload;
    use std::sync::{Arc, Mutex};
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
        let engine = WebSocketEngine::new(vec![], WsMode::Handshake, None, ChaosEngine::default());
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
        let engine = WebSocketEngine::new(vec![], WsMode::PingPong, None, ChaosEngine::default());
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
        let engine = WebSocketEngine::new(headers, WsMode::Handshake, None, ChaosEngine::default());
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
        use tokio::net::TcpListener;
        use tokio_tungstenite::accept_async;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let local_addr = listener.local_addr().unwrap();

        // Server that broadcasts every binary message to all connected clients
        let server_clients: Arc<Mutex<Vec<tokio::sync::broadcast::Sender<Vec<u8>>>>> =
            Arc::new(Mutex::new(Vec::new()));

        let _clients = server_clients.clone();
        tokio::spawn(async move {
            let (tx, _) = tokio::sync::broadcast::channel::<Vec<u8>>(32);
            while let Ok((stream, _)) = listener.accept().await {
                let mut ws = accept_async(stream).await.unwrap();
                let tx = tx.clone();
                let mut rx = tx.subscribe();
                tokio::spawn(async move {
                    loop {
                        tokio::select! {
                            msg = ws.next() => {
                                match msg {
                                    Some(Ok(Message::Binary(bin))) => {
                                        let _ = tx.send(bin.to_vec());
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

        // Connect subscriber first
        let _engine = WebSocketEngine::new(vec![], WsMode::Stream, None, ChaosEngine::default());
        let sub_ws = tokio_tungstenite::connect_async(&ws_url).await.unwrap().0;
        let (_, mut sub_read) = sub_ws.split();

        // Spawn publisher that sends a timestamped message
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
                    let e2e_us = (now_ns.saturating_sub(sent_ns)) / 1000;
                    return Some(e2e_us);
                }
            }
            None
        })
        .await;

        let _ = pub_handle.await;

        assert!(recv_result.is_ok());
        let e2e = recv_result.unwrap().unwrap();
        // E2E latency should be reasonable (less than 1 second)
        assert!(e2e < 1_000_000);
    }

    #[tokio::test]
    async fn test_publisher_iteration_method() {
        use tokio::net::TcpListener;
        use tokio_tungstenite::accept_async;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let local_addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            if let Ok((stream, _)) = listener.accept().await
                && let Ok(mut ws_stream) = accept_async(stream).await
            {
                // Echo back any binary messages
                while let Some(Ok(msg)) = ws_stream.next().await {
                    if let Message::Binary(bin) = msg {
                        let _ = ws_stream.send(Message::Binary(bin)).await;
                    }
                }
            }
        });

        let ws_url = format!("ws://{}", local_addr);
        let engine = WebSocketEngine::new(vec![], WsMode::Stream, None, ChaosEngine::default());

        let mut session = {
            let ws = tokio_tungstenite::connect_async(&ws_url).await.unwrap().0;
            let (write, _read) = ws.split();
            PublisherSession {
                write,
                user_payload: b"test".to_vec(),
                publish_interval: Duration::from_millis(100),
            }
        };

        let metric = engine
            .execute_publisher_iteration(&ws_url, &mut session)
            .await;
        assert_eq!(metric.status_code, 200);
        assert!(metric.timestamp_sent_ns.is_some());
    }

    #[tokio::test]
    async fn test_subscriber_iteration_method() {
        use tokio::net::TcpListener;
        use tokio_tungstenite::accept_async;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let local_addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            if let Ok((stream, _)) = listener.accept().await
                && let Ok(mut ws_stream) = accept_async(stream).await
            {
                // Send a binary message immediately
                let _ = ws_stream
                    .send(Message::Binary(Bytes::from(create_pubsub_payload(
                        b"hello",
                    ))))
                    .await;
                let _ = ws_stream.close(None).await;
            }
        });

        let ws_url = format!("ws://{}", local_addr);
        let engine = WebSocketEngine::new(vec![], WsMode::Stream, None, ChaosEngine::default());

        let mut session = {
            let ws = tokio_tungstenite::connect_async(&ws_url).await.unwrap().0;
            let (_, read) = ws.split();
            SubscriberSession {
                read,
                received_count: 0,
            }
        };

        let metric = engine
            .execute_subscriber_iteration(&ws_url, &mut session)
            .await;
        assert_eq!(metric.status_code, 200);
        assert!(metric.bytes_received > 0);
        assert_eq!(session.received_count, 1);
    }
}
