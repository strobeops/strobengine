pub mod grpc;
pub mod grpc_h2;
pub mod grpc_parser;
pub mod http;
pub mod http3;
pub mod sse;
pub mod websocket;

use std::sync::Arc;

use async_trait::async_trait;

use crate::chaos::ChaosEngine;
use crate::config::TestConfig;
use crate::metrics::RequestMetric;

/// Errors that can occur during protocol engine construction.
#[derive(Debug)]
pub enum SetupError {
    /// gRPC engine construction failed (proto compilation, endpoint, payload encoding, etc.)
    Grpc(crate::protocols::grpc_parser::ProtoError),
    /// HTTP/3 engine construction failed (URL parse, QUIC setup, etc.)
    Http3(String),
}

impl std::fmt::Display for SetupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Grpc(e) => write!(f, "gRPC setup failed: {e}"),
            Self::Http3(e) => write!(f, "HTTP/3 setup failed: {e}"),
        }
    }
}

impl std::error::Error for SetupError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Grpc(e) => Some(e),
            _ => None,
        }
    }
}

impl From<crate::protocols::grpc_parser::ProtoError> for SetupError {
    fn from(err: crate::protocols::grpc_parser::ProtoError) -> Self {
        Self::Grpc(err)
    }
}

/// Returns whether the provided URL scheme matches a non-HTTP protocol engine.
pub fn is_protocol_url(url: &str) -> bool {
    url.starts_with("ws://")
        || url.starts_with("wss://")
        || url.starts_with("grpc://")
        || url.starts_with("grpcs://")
        || url.starts_with("http3://")
        || url.starts_with("h3://")
        || url.starts_with("sse://")
        || url.starts_with("sses://")
}

/// A protocol engine that executes a single load-test iteration.
/// Orchestration (strategy, metrics aggregation, progress, SIGINT)
/// is handled by `execute_test` in `lib.rs`.
#[async_trait]
pub trait ProtocolEngine: Send + Sync {
    /// Execute a single iteration (HTTP request, WS handshake, etc.)
    /// and return the metric for this unit.
    async fn execute_iteration(&self, target_url: &str) -> RequestMetric;

    /// Create worker-local context (session) for persistent connections.
    /// Returns None for stateless protocols (HTTP, gRPC).
    async fn create_worker_context(&self) -> Option<Box<dyn WorkerSession>> {
        None
    }

    /// Execute with worker-local state (for persistent connections).
    /// Default: falls back to execute_iteration.
    async fn execute_iteration_with_context(
        &self,
        target_url: &str,
        _ctx: &mut dyn WorkerSession,
    ) -> RequestMetric {
        self.execute_iteration(target_url).await
    }
}

/// A typed worker session with explicit async teardown.
/// Replaces `Box<dyn Any>` context to eliminate orchestrator downcasting.
#[async_trait]
pub trait WorkerSession: Send + 'static {
    /// Protocol-specific async teardown (e.g., sending close frames, draining streams).
    async fn shutdown(&mut self);

    /// Downcast support for engine-internal dispatch.
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any;
}

/// Detect the appropriate protocol engine from the URL scheme and config.
///
/// Returns `Err(SetupError)` if the engine cannot be constructed (e.g., bad
/// proto path, invalid QUIC scheme, malformed payload). Callers must propagate
/// the error rather than silently falling back to a degraded engine.
pub fn detect_protocol(
    url: &str,
    config: &TestConfig,
    chaos: ChaosEngine,
) -> Result<Arc<dyn ProtocolEngine>, SetupError> {
    // Options that are recognized for forward-compatibility / CLI symmetry but
    // not yet functional: surface them instead of silently ignoring them.
    if config.grpc_use_reflection {
        tracing::warn!(
            "--grpc-use-reflection is recognized for forward-compatibility but not yet functional; ignoring"
        );
    }
    if config.http3_enabled {
        tracing::warn!(
            "--http3 is recognized for forward-compatibility but not yet functional; use an http3:// URL scheme instead"
        );
    }
    if config.ws_subscribers.is_some() {
        tracing::warn!(
            "--ws-subscribers is recognized for forward-compatibility but not yet functional; ignoring"
        );
    }

    let headers = config.headers.clone().unwrap_or_default();
    if url.starts_with("ws://") || url.starts_with("wss://") {
        let engine = websocket::WebSocketEngine::new(
            headers,
            config.ws_mode,
            config.ws_payload.clone(),
            chaos,
            config.timeout_secs,
            config.ws_persistent,
            config.ws_keepalive_secs,
            config.ws_max_messages,
        )
        .with_role(config.ws_role.clone(), config.ws_publish_interval_ms)
        .with_backpressure(
            config.ws_max_buffer_bytes,
            config.ws_backpressure_warn_ratio,
        );
        Ok(Arc::new(engine))
    } else if url.starts_with("grpc://") || url.starts_with("grpcs://") {
        if config.grpc_h2_multiplex && url.starts_with("grpc://") {
            let engine = grpc_h2::GrpcH2Engine::new(
                url,
                headers,
                chaos,
                config.grpc_service.clone(),
                config.grpc_method.clone(),
                config.grpc_payload.clone(),
                config.grpc_deadline_ms,
                config.proto_path.clone(),
            )?;
            return Ok(Arc::new(engine));
        }
        if config.grpc_h2_multiplex {
            tracing::warn!(
                "--grpc-h2-multiplex supports cleartext grpc:// only; using tonic for {url}"
            );
        }
        let engine = grpc::GrpcEngine::new(
            url,
            headers,
            chaos,
            config.grpc_service.clone(),
            config.grpc_method.clone(),
            config.grpc_payload.clone(),
            config.grpc_deadline_ms,
            config.proto_path.clone(),
        )?;
        Ok(Arc::new(engine))
    } else if url.starts_with("http3://") || url.starts_with("h3://") {
        let engine = http3::Http3Engine::new(
            url,
            headers,
            config.method.clone(),
            config.body.as_ref().map(|b| bytes::Bytes::from(b.clone())),
            chaos,
            config.quic_max_idle_timeout_ms,
            config.quic_zero_rtt,
        )
        .map_err(SetupError::Http3)?
        .with_migration(config.http3_migrate, config.http3_migrate_every);
        Ok(Arc::new(engine))
    } else if url.starts_with("sse://") || url.starts_with("sses://") {
        let engine = sse::SseEngine::new(headers, chaos, config.sse_max_events);
        Ok(Arc::new(engine))
    } else if config.sse_enabled {
        Ok(Arc::new(sse::SseEngine::new(
            headers,
            chaos,
            config.sse_max_events,
        )))
    } else {
        Ok(Arc::new(http::HttpEngine::new()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn grpc_config(proto_path: Option<String>, payload: Option<String>) -> TestConfig {
        let mut config =
            TestConfig::for_protocol_detection("grpc://127.0.0.1:50051".into(), 1, 10, 10);
        config.grpc_service = Some("pkg.Svc".into());
        config.grpc_method = Some("Method".into());
        config.grpc_payload = payload;
        config.proto_path = proto_path;
        config
    }

    fn http3_config() -> TestConfig {
        TestConfig::for_protocol_detection("http3://".into(), 1, 10, 10)
    }

    fn ws_config() -> TestConfig {
        TestConfig::for_protocol_detection("ws://127.0.0.1:8080".into(), 1, 10, 10)
    }

    fn sse_config() -> TestConfig {
        TestConfig::for_protocol_detection("sse://127.0.0.1:8080".into(), 1, 10, 10)
    }

    #[test]
    fn test_grpc_bad_proto_path_returns_err() {
        let config = grpc_config(Some("/nonexistent/path.proto".into()), Some("{}".into()));
        let result = detect_protocol("grpc://127.0.0.1:50051", &config, ChaosEngine::default());
        let err_msg = match result {
            Ok(_) => panic!("expected error, got Ok"),
            Err(e) => e.to_string(),
        };
        assert!(
            err_msg.contains("gRPC setup failed"),
            "unexpected error: {err_msg}"
        );
    }

    #[test]
    fn test_grpc_bad_hex_payload_returns_err() {
        let config = grpc_config(None, Some("0xZZZZ".into()));
        let result = detect_protocol("grpc://127.0.0.1:50051", &config, ChaosEngine::default());
        let err_msg = match result {
            Ok(_) => panic!("expected error, got Ok"),
            Err(e) => e.to_string(),
        };
        assert!(
            err_msg.contains("gRPC setup failed"),
            "unexpected error: {err_msg}"
        );
    }

    #[test]
    fn test_grpc_bad_base64_payload_returns_err() {
        let config = grpc_config(None, Some("not-valid-base64!!!".into()));
        let result = detect_protocol("grpc://127.0.0.1:50051", &config, ChaosEngine::default());
        let err_msg = match result {
            Ok(_) => panic!("expected error, got Ok"),
            Err(e) => e.to_string(),
        };
        assert!(
            err_msg.contains("gRPC setup failed"),
            "unexpected error: {err_msg}"
        );
    }

    #[test]
    fn test_http3_bad_url_returns_err() {
        let config = http3_config();
        let result = detect_protocol("http3://", &config, ChaosEngine::default());
        let err_msg = match result {
            Ok(_) => panic!("expected error, got Ok"),
            Err(e) => e.to_string(),
        };
        assert!(
            err_msg.contains("HTTP/3 setup failed"),
            "unexpected error: {err_msg}"
        );
    }

    #[test]
    fn test_valid_http_returns_ok() {
        let config = TestConfig::for_protocol_detection("http://127.0.0.1:8080".into(), 1, 10, 10);
        assert!(
            detect_protocol("http://127.0.0.1:8080", &config, ChaosEngine::default()).is_ok(),
            "expected Ok for valid HTTP"
        );
    }

    #[test]
    fn test_valid_ws_returns_ok() {
        let config = ws_config();
        assert!(
            detect_protocol("ws://127.0.0.1:8080", &config, ChaosEngine::default()).is_ok(),
            "expected Ok for valid WebSocket"
        );
    }

    #[test]
    fn test_valid_sse_returns_ok() {
        let config = sse_config();
        assert!(
            detect_protocol("sse://127.0.0.1:8080", &config, ChaosEngine::default()).is_ok(),
            "expected Ok for valid SSE"
        );
    }

    /// Captures emitted event messages so tests can assert on log output.
    #[derive(Clone, Default)]
    struct EventCapture(std::sync::Arc<std::sync::Mutex<Vec<String>>>);

    impl<S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>>
        tracing_subscriber::Layer<S> for EventCapture
    {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            struct MessageVisitor(String);
            impl tracing::field::Visit for MessageVisitor {
                fn record_debug(
                    &mut self,
                    field: &tracing::field::Field,
                    value: &dyn std::fmt::Debug,
                ) {
                    if field.name() == "message" {
                        self.0 = format!("{value:?}");
                    }
                }
                fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                    if field.name() == "message" {
                        self.0 = value.to_string();
                    }
                }
            }
            let mut visitor = MessageVisitor(String::new());
            event.record(&mut visitor);
            self.0.lock().unwrap().push(visitor.0);
        }
    }

    #[test]
    fn test_dead_flags_emit_warnings() {
        use tracing_subscriber::layer::SubscriberExt as _;

        let capture = EventCapture::default();
        let subscriber = tracing_subscriber::registry().with(capture.clone());
        let _guard = tracing::subscriber::set_default(subscriber);

        let mut config =
            TestConfig::for_protocol_detection("http://127.0.0.1:8080".into(), 1, 10, 10);
        config.grpc_use_reflection = true;
        config.http3_enabled = true;
        config.ws_subscribers = Some(10);
        assert!(
            detect_protocol("http://127.0.0.1:8080", &config, ChaosEngine::default()).is_ok(),
            "warnings must not change engine selection"
        );

        let events = capture.0.lock().unwrap();
        assert!(
            events.iter().any(|m| m.contains("--grpc-use-reflection")),
            "expected --grpc-use-reflection warning, got {events:?}"
        );
        assert!(
            events.iter().any(|m| m.contains("--http3 ")),
            "expected --http3 warning, got {events:?}"
        );
        assert!(
            events.iter().any(|m| m.contains("--ws-subscribers")),
            "expected --ws-subscribers warning, got {events:?}"
        );
    }
}
