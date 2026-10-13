use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use http::header::HeaderValue;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

use crate::chaos::{ChaosEngine, ChaosFault};
use crate::metrics::RequestMetric;

use super::ProtocolEngine;

/// Subprotocol for GraphQL over WebSocket (the modern `graphql-ws` library).
const SUBPROTOCOL: &str = "graphql-transport-ws";
/// Single-operation id; one subscription is active per connection at a time.
const SUBSCRIPTION_ID: &str = "1";

type WsStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// The GraphQL document to execute as a subscription.
#[derive(Debug, Clone)]
pub struct SubscriptionPayload {
    pub query: String,
    pub variables: Option<String>,
    pub operation_name: Option<String>,
}

impl SubscriptionPayload {
    /// Build the `subscribe` frame: `{"id":"1","type":"subscribe","payload":{...}}`.
    pub fn subscribe_frame(&self) -> Result<String, String> {
        let mut payload = serde_json::Map::new();
        payload.insert(
            "query".to_string(),
            serde_json::Value::String(self.query.clone()),
        );
        if let Some(raw) = &self.variables {
            let parsed: serde_json::Value = serde_json::from_str(raw)
                .map_err(|e| format!("graphql_variables is not valid JSON: {e}"))?;
            payload.insert("variables".to_string(), parsed);
        }
        if let Some(name) = &self.operation_name {
            payload.insert(
                "operationName".to_string(),
                serde_json::Value::String(name.clone()),
            );
        }
        let mut msg = serde_json::Map::new();
        msg.insert(
            "id".to_string(),
            serde_json::Value::String(SUBSCRIPTION_ID.to_string()),
        );
        msg.insert(
            "type".to_string(),
            serde_json::Value::String("subscribe".to_string()),
        );
        msg.insert("payload".to_string(), serde_json::Value::Object(payload));
        serde_json::to_string(&serde_json::Value::Object(msg))
            .map_err(|e| format!("failed to serialize subscribe frame: {e}"))
    }
}

/// A decoded `graphql-transport-ws` server frame.
#[derive(Debug, PartialEq, Eq)]
enum ServerFrame {
    Next,
    Error(u64),
    Complete,
    Ping,
    Pong,
    Ack,
    ConnectionError,
    Other,
}

fn decode_frame(text: &str) -> ServerFrame {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(text) else {
        return ServerFrame::Other;
    };
    match value.get("type").and_then(|t| t.as_str()) {
        Some("next") => ServerFrame::Next,
        Some("error") => {
            let count = value
                .get("payload")
                .and_then(|p| p.as_array())
                .map(|a| a.len() as u64)
                .unwrap_or(1);
            ServerFrame::Error(count)
        }
        Some("complete") => ServerFrame::Complete,
        Some("ping") => ServerFrame::Ping,
        Some("pong") => ServerFrame::Pong,
        Some("connection_ack") => ServerFrame::Ack,
        Some("connection_error") => ServerFrame::ConnectionError,
        _ => ServerFrame::Other,
    }
}

/// Outcome of a bounded read pass.
enum ReadEvent {
    Next { bytes: u64 },
    Error { errors: u64, bytes: u64 },
    Complete,
    Ack,
    ConnectionError,
    Timeout,
    Closed,
}

/// Per-worker subscription session, persistent across iterations.
pub struct GraphqlWsSession {
    stream: Option<WsStream>,
    headers: Vec<(String, String)>,
    timeout: Duration,
    initialized: bool,
    subscribed: bool,
    subscribe_sent_at: Option<Instant>,
    last_message_at: Option<Instant>,
    ttfm_recorded: bool,
}

impl GraphqlWsSession {
    pub fn new(headers: Vec<(String, String)>, timeout_secs: u64) -> Self {
        Self {
            stream: None,
            headers,
            timeout: Duration::from_secs(if timeout_secs > 0 { timeout_secs } else { 5 }),
            initialized: false,
            subscribed: false,
            subscribe_sent_at: None,
            last_message_at: None,
            ttfm_recorded: false,
        }
    }

    /// Drop the connection and reset the protocol state so the next iteration
    /// reconnects and re-initialises from scratch.
    fn reset(&mut self) {
        self.stream = None;
        self.initialized = false;
        self.subscribed = false;
        self.subscribe_sent_at = None;
        self.last_message_at = None;
        self.ttfm_recorded = false;
    }

    async fn connect(&mut self, url: &str) -> Result<(), String> {
        let mut request = url
            .into_client_request()
            .map_err(|e| format!("invalid WebSocket URL: {e}"))?;
        request.headers_mut().insert(
            "sec-websocket-protocol",
            HeaderValue::from_static(SUBPROTOCOL),
        );
        for (key, value) in &self.headers {
            match (
                key.as_str().parse::<http::header::HeaderName>(),
                value.as_str().parse(),
            ) {
                (Ok(name), Ok(val)) => {
                    request.headers_mut().insert(name, val);
                }
                _ => tracing::warn!(key = %key, "invalid WebSocket header skipped"),
            }
        }

        let result = tokio::time::timeout(self.timeout, tokio_tungstenite::connect_async(request))
            .await
            .map_err(|_| "connection timed out".to_string())?
            .map_err(|e| format!("connection failed: {e}"))?;

        let (stream, response) = result;
        let negotiated = response
            .headers()
            .get("sec-websocket-protocol")
            .and_then(|v| v.to_str().ok());
        if negotiated != Some(SUBPROTOCOL) {
            return Err(format!(
                "server did not negotiate {SUBPROTOCOL} (got {negotiated:?})"
            ));
        }

        self.stream = Some(stream);
        Ok(())
    }

    /// Send `connection_init` and await `connection_ack`.
    async fn initialize(&mut self) -> Result<(), String> {
        let init = "{\"type\":\"connection_init\"}";
        self.send_text(init).await?;

        let deadline = Instant::now() + self.timeout;
        loop {
            match self.next_frame(deadline).await {
                ReadEvent::Ack => return Ok(()),
                ReadEvent::Next { .. } | ReadEvent::Error { .. } | ReadEvent::Complete => {
                    // Frames before ack are protocol violations; ignore and wait.
                    continue;
                }
                ReadEvent::ConnectionError | ReadEvent::Closed | ReadEvent::Timeout => {
                    return Err("connection init not acknowledged".to_string());
                }
            }
        }
    }
}

impl GraphqlWsSession {
    async fn send_text(&mut self, text: &str) -> Result<(), String> {
        let Some(stream) = self.stream.as_mut() else {
            return Err("not connected".to_string());
        };
        stream
            .send(Message::Text(text.to_string().into()))
            .await
            .map_err(|e| format!("send failed: {e}"))
    }

    async fn send_binary(&mut self, bytes: Vec<u8>) -> Result<(), String> {
        let Some(stream) = self.stream.as_mut() else {
            return Err("not connected".to_string());
        };
        stream
            .send(Message::Binary(bytes.into()))
            .await
            .map_err(|e| format!("send failed: {e}"))
    }

    /// Read frames until one delivering a GraphQL event, replying to pings.
    async fn next_frame(&mut self, deadline: Instant) -> ReadEvent {
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return ReadEvent::Timeout;
            }
            let next = {
                let Some(stream) = self.stream.as_mut() else {
                    return ReadEvent::Closed;
                };
                tokio::time::timeout(remaining, stream.next()).await
            };
            match next {
                Err(_) => return ReadEvent::Timeout,
                Ok(None) => return ReadEvent::Closed,
                Ok(Some(Err(_))) => return ReadEvent::Closed,
                Ok(Some(Ok(Message::Text(text)))) => {
                    let bytes = text.len() as u64;
                    match decode_frame(&text) {
                        ServerFrame::Next => return ReadEvent::Next { bytes },
                        ServerFrame::Error(errors) => {
                            return ReadEvent::Error { errors, bytes };
                        }
                        ServerFrame::Complete => return ReadEvent::Complete,
                        ServerFrame::Ack => return ReadEvent::Ack,
                        ServerFrame::ConnectionError => return ReadEvent::ConnectionError,
                        ServerFrame::Ping => {
                            let _ = self.send_text("{\"type\":\"pong\"}").await;
                        }
                        ServerFrame::Pong | ServerFrame::Other => {}
                    }
                }
                Ok(Some(Ok(Message::Binary(bin)))) => {
                    return ReadEvent::Next {
                        bytes: bin.len() as u64,
                    };
                }
                Ok(Some(Ok(Message::Ping(_)))) => {
                    // tungstenite auto-replies with a Pong at the protocol layer.
                }
                Ok(Some(Ok(Message::Close(_)))) => return ReadEvent::Closed,
                Ok(Some(Ok(_))) => {}
            }
        }
    }
}

#[async_trait::async_trait]
impl super::WorkerSession for GraphqlWsSession {
    async fn shutdown(&mut self) {
        if let Some(mut stream) = self.stream.take() {
            let _ = stream.close(None).await;
        }
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

pub struct GraphqlWsEngine {
    payload: SubscriptionPayload,
    headers: Vec<(String, String)>,
    chaos: ChaosEngine,
    timeout_secs: u64,
}

impl GraphqlWsEngine {
    pub fn new(
        payload: SubscriptionPayload,
        headers: Vec<(String, String)>,
        chaos: ChaosEngine,
        timeout_secs: u64,
    ) -> Self {
        Self {
            payload,
            headers,
            chaos,
            timeout_secs,
        }
    }

    async fn run_iteration(&self, url: &str, session: &mut GraphqlWsSession) -> RequestMetric {
        let req_start = Instant::now();
        let fault = self.chaos.select_fault();

        if let Some(ChaosFault::ConnectionDrop) = fault {
            tracing::trace!("graphql-ws chaos: connection drop");
            return RequestMetric::error(req_start.elapsed().as_micros(), fault);
        }
        if let Some(ChaosFault::LatencySpike { duration_ms }) = fault {
            tracing::trace!(duration_ms, "graphql-ws chaos: latency spike");
            tokio::time::sleep(Duration::from_millis(duration_ms)).await;
        }

        if session.stream.is_none() && session.connect(url).await.is_err() {
            session.reset();
            return RequestMetric::error(req_start.elapsed().as_micros(), fault);
        }

        if !session.initialized {
            if session.initialize().await.is_err() {
                session.reset();
                return RequestMetric::error(req_start.elapsed().as_micros(), fault);
            }
            session.initialized = true;
        }

        // Corrupted-payload chaos sends a malformed frame instead of subscribing.
        if let Some(ChaosFault::CorruptedPayload) = fault {
            if session
                .send_binary(b"\xff\xfe\xbd\xef".to_vec())
                .await
                .is_err()
            {
                session.reset();
                return RequestMetric::error(req_start.elapsed().as_micros(), fault);
            }
            let deadline = Instant::now() + session.timeout;
            let metric = match session.next_frame(deadline).await {
                ReadEvent::Next { bytes } => {
                    RequestMetric::builder(req_start.elapsed().as_micros(), fault)
                        .status(200)
                        .bytes(bytes)
                        .build()
                }
                _ => RequestMetric::error(req_start.elapsed().as_micros(), fault),
            };
            return metric;
        }

        // (Re)subscribe when no subscription is active.
        let mut operations = 0u64;
        if !session.subscribed {
            let frame = match self.payload.subscribe_frame() {
                Ok(f) => f,
                Err(_) => return RequestMetric::error(req_start.elapsed().as_micros(), fault),
            };
            if session.send_text(&frame).await.is_err() {
                session.reset();
                return RequestMetric::error(req_start.elapsed().as_micros(), fault);
            }
            session.subscribed = true;
            session.subscribe_sent_at = Some(Instant::now());
            session.last_message_at = None;
            session.ttfm_recorded = false;
            operations = 1;
        }
        // `None` when this iteration reused an active subscription.
        let ops_sample = (operations > 0).then_some(operations);

        let deadline = Instant::now() + session.timeout;
        let event = session.next_frame(deadline).await;
        let latency_micros = req_start.elapsed().as_micros();

        match event {
            ReadEvent::Next { bytes } => {
                let now = Instant::now();
                let ttfm_us = if session.ttfm_recorded {
                    None
                } else {
                    session.ttfm_recorded = true;
                    session
                        .subscribe_sent_at
                        .map(|t| now.duration_since(t).as_micros() as u64)
                };
                let interval_us = session
                    .last_message_at
                    .map(|t| now.duration_since(t).as_micros() as u64);
                session.last_message_at = Some(now);
                RequestMetric::builder(latency_micros, fault)
                    .status(200)
                    .bytes(bytes)
                    .graphql_ws(ops_sample, ttfm_us, interval_us, None)
                    .build()
            }
            ReadEvent::Error { errors, bytes } => {
                session.subscribed = false;
                RequestMetric::builder(latency_micros, fault)
                    .status(200)
                    .bytes(bytes)
                    .graphql_ws(ops_sample, None, None, Some(errors))
                    .build()
            }
            ReadEvent::Complete => {
                // Subscription ended normally; re-subscribe on the next iteration.
                session.subscribed = false;
                RequestMetric::builder(latency_micros, fault)
                    .status(200)
                    .graphql_ws(ops_sample, None, None, None)
                    .build()
            }
            ReadEvent::Ack => RequestMetric::builder(latency_micros, fault)
                .status(200)
                .graphql_ws(ops_sample, None, None, None)
                .build(),
            ReadEvent::ConnectionError => {
                session.reset();
                RequestMetric::error(latency_micros, fault)
            }
            ReadEvent::Timeout => {
                tracing::debug!("graphql-ws subscription read timed out");
                RequestMetric::error(latency_micros, fault)
            }
            ReadEvent::Closed => {
                session.reset();
                RequestMetric::error(latency_micros, fault)
            }
        }
    }
}

#[async_trait]
impl ProtocolEngine for GraphqlWsEngine {
    async fn execute_iteration(&self, target_url: &str) -> RequestMetric {
        let mut session = GraphqlWsSession::new(self.headers.clone(), self.timeout_secs);
        self.run_iteration(target_url, &mut session).await
    }

    async fn create_worker_context(&self) -> Option<Box<dyn super::WorkerSession>> {
        Some(Box::new(GraphqlWsSession::new(
            self.headers.clone(),
            self.timeout_secs,
        )))
    }

    async fn execute_iteration_with_context(
        &self,
        target_url: &str,
        ctx: &mut dyn super::WorkerSession,
    ) -> RequestMetric {
        match ctx.as_any_mut().downcast_mut::<GraphqlWsSession>() {
            Some(session) => self.run_iteration(target_url, session).await,
            None => RequestMetric::error(0, None),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures_util::{SinkExt, StreamExt};
    use tokio::net::TcpListener;
    use tokio_tungstenite::accept_hdr_async;
    use tokio_tungstenite::tungstenite::handshake::server::{
        Callback, ErrorResponse, Request, Response,
    };

    use super::*;

    fn payload() -> SubscriptionPayload {
        SubscriptionPayload {
            query: "subscription { ticks }".to_string(),
            variables: None,
            operation_name: None,
        }
    }

    /// Verify the client offers `graphql-transport-ws` and echo it back so the
    /// engine's negotiation check passes.
    struct EchoSubprotocol;
    impl Callback for EchoSubprotocol {
        fn on_request(
            self,
            request: &Request,
            mut response: Response,
        ) -> Result<Response, ErrorResponse> {
            if let Some(proto) = request.headers().get("sec-websocket-protocol") {
                response
                    .headers_mut()
                    .insert("sec-websocket-protocol", proto.clone());
            }
            Ok(response)
        }
    }

    #[test]
    fn subscribe_frame_includes_payload() {
        let p = SubscriptionPayload {
            query: "subscription { x }".to_string(),
            variables: Some("{\"id\": 1}".to_string()),
            operation_name: Some("Sub".to_string()),
        };
        let frame: serde_json::Value = serde_json::from_str(&p.subscribe_frame().unwrap()).unwrap();
        assert_eq!(frame["type"], "subscribe");
        assert_eq!(frame["id"], "1");
        assert_eq!(frame["payload"]["query"], "subscription { x }");
        assert_eq!(frame["payload"]["variables"]["id"], 1);
        assert_eq!(frame["payload"]["operationName"], "Sub");
    }

    #[test]
    fn decode_frame_classifies_messages() {
        assert_eq!(
            decode_frame(r#"{"id":"1","type":"next","payload":{}}"#),
            ServerFrame::Next
        );
        assert_eq!(
            decode_frame(r#"{"id":"1","type":"error","payload":[{"message":"a"}]}"#),
            ServerFrame::Error(1)
        );
        assert_eq!(
            decode_frame(r#"{"type":"complete"}"#),
            ServerFrame::Complete
        );
        assert_eq!(decode_frame(r#"{"type":"ping"}"#), ServerFrame::Ping);
        assert_eq!(decode_frame("not json"), ServerFrame::Other);
    }

    /// Spawn a mock subscription server that acknowledges init, then replies to
    /// every `subscribe` with `count` `next` frames followed by `complete`.
    async fn spawn_subscription_server(count: usize) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut ws = match accept_hdr_async(stream, EchoSubprotocol).await {
                        Ok(ws) => ws,
                        Err(_) => return,
                    };
                    while let Some(Ok(msg)) = ws.next().await {
                        let Message::Text(text) = msg else { continue };
                        let Ok(value) = serde_json::from_str::<serde_json::Value>(text.as_str())
                        else {
                            continue;
                        };
                        match value.get("type").and_then(|t| t.as_str()) {
                            Some("connection_init") => {
                                let _ = ws
                                    .send(Message::Text(
                                        "{\"type\":\"connection_ack\"}".to_string().into(),
                                    ))
                                    .await;
                            }
                            Some("subscribe") => {
                                for i in 0..count {
                                    let _ = ws
                                        .send(Message::Text(
                                            format!(
                                                "{{\"id\":\"1\",\"type\":\"next\",\"payload\":{{\"data\":{{\"i\":{i}}}}}}}"
                                            )
                                            .into(),
                                        ))
                                        .await;
                                }
                                let _ = ws
                                    .send(Message::Text(
                                        "{\"id\":\"1\",\"type\":\"complete\"}".to_string().into(),
                                    ))
                                    .await;
                            }
                            _ => {}
                        }
                    }
                });
            }
        });
        format!("ws://{addr}")
    }

    #[tokio::test]
    async fn subscription_captures_ttfm_and_intervals() {
        let url = spawn_subscription_server(3).await;
        let engine = GraphqlWsEngine::new(payload(), vec![], ChaosEngine::default(), 2);

        let mut session = GraphqlWsSession::new(vec![], 2);
        let first = engine.run_iteration(&url, &mut session).await;
        assert_eq!(first.status_code, 200);
        assert_eq!(first.graphql_ws_operations, Some(1));
        assert!(
            first.graphql_ws_ttfm_us.is_some(),
            "first message must record TTFM"
        );
        assert_eq!(first.graphql_ws_interval_us, None);

        let second = engine.run_iteration(&url, &mut session).await;
        assert_eq!(second.status_code, 200);
        assert_eq!(second.graphql_ws_operations, None);
        assert_eq!(second.graphql_ws_ttfm_us, None, "TTFM is recorded once");
        assert!(
            second.graphql_ws_interval_us.is_some(),
            "subsequent messages record inter-message intervals"
        );
    }

    #[tokio::test]
    async fn error_frame_is_reported_as_graphql_error() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            if let Ok((stream, _)) = listener.accept().await {
                let mut ws = accept_hdr_async(stream, EchoSubprotocol).await.unwrap();
                while let Some(Ok(msg)) = ws.next().await {
                    let Message::Text(text) = msg else { continue };
                    let value: serde_json::Value = serde_json::from_str(text.as_str()).unwrap();
                    match value.get("type").and_then(|t| t.as_str()) {
                        Some("connection_init") => {
                            let _ = ws
                                .send(Message::Text(
                                    "{\"type\":\"connection_ack\"}".to_string().into(),
                                ))
                                .await;
                        }
                        Some("subscribe") => {
                            let _ = ws
                                .send(Message::Text(
                                    "{\"id\":\"1\",\"type\":\"error\",\"payload\":[{\"message\":\"boom\"}]}"
                                        .to_string()
                                        .into(),
                                ))
                                .await;
                        }
                        _ => {}
                    }
                }
            }
        });
        let url = format!("ws://{addr}");
        let engine = GraphqlWsEngine::new(payload(), vec![], ChaosEngine::default(), 2);

        let metric = engine.execute_iteration(&url).await;
        assert_eq!(metric.status_code, 200);
        assert_eq!(metric.graphql_ws_errors, Some(1));
        assert_eq!(metric.graphql_ws_operations, Some(1));
    }

    #[tokio::test]
    async fn unreachable_server_is_an_error() {
        let engine = GraphqlWsEngine::new(payload(), vec![], ChaosEngine::default(), 1);
        let metric = engine.execute_iteration("ws://127.0.0.1:59998").await;
        assert_eq!(metric.status_code, 0);
    }

    #[allow(dead_code)]
    fn _assert_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<GraphqlWsEngine>();
        assert_send_sync::<GraphqlWsSession>();
        let _ = Arc::new(0u8);
    }
}
