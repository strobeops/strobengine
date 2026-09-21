pub mod grpc;
pub mod grpc_parser;
pub mod grpc_reflection;
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
        .with_role(config.ws_role.clone(), config.ws_publish_interval_ms);
        Ok(Arc::new(engine))
    } else if url.starts_with("grpc://") || url.starts_with("grpcs://") {
        let engine = grpc::GrpcEngine::new(
            url,
            headers,
            chaos,
            config.grpc_service.clone(),
            config.grpc_method.clone(),
            config.grpc_payload.clone(),
            config.grpc_deadline_ms,
            config.proto_path.clone(),
            config.grpc_use_reflection,
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
        .map_err(SetupError::Http3)?;
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
}
