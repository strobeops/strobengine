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
    async fn create_worker_context(&self) -> Option<Box<dyn std::any::Any + Send>> {
        None
    }

    /// Execute with worker-local state (for persistent connections).
    /// Default: falls back to execute_iteration.
    async fn execute_iteration_with_context(
        &self,
        target_url: &str,
        _ctx: &mut (dyn std::any::Any + Send),
    ) -> RequestMetric {
        self.execute_iteration(target_url).await
    }
}

/// Detect the appropriate protocol engine from the URL scheme and config.
pub fn detect_protocol(
    url: &str,
    config: &TestConfig,
    chaos: ChaosEngine,
) -> Arc<dyn ProtocolEngine> {
    if url.starts_with("ws://") || url.starts_with("wss://") {
        let engine = websocket::WebSocketEngine::new(
            config.headers.clone().unwrap_or_default(),
            config.ws_mode,
            config.ws_payload.clone(),
            chaos,
            config.timeout_secs,
            config.ws_persistent,
            config.ws_keepalive_secs,
            config.ws_max_messages,
        )
        .with_role(config.ws_role.clone(), config.ws_publish_interval_ms);
        Arc::new(engine)
    } else if url.starts_with("grpc://") || url.starts_with("grpcs://") {
        match grpc::GrpcEngine::new(
            url,
            config.headers.clone().unwrap_or_default(),
            chaos,
            config.grpc_service.clone(),
            config.grpc_method.clone(),
            config.grpc_payload.clone(),
            config.grpc_deadline_ms,
            config.proto_path.clone(),
            config.grpc_use_reflection,
        ) {
            Ok(engine) => Arc::new(engine),
            Err(e) => {
                tracing::warn!(error = %e, "failed to create gRPC engine, falling back to HTTP");
                Arc::new(http::HttpEngine::new())
            }
        }
    } else if url.starts_with("http3://") || url.starts_with("h3://") {
        match http3::Http3Engine::new(
            url,
            config.headers.clone().unwrap_or_default(),
            config.method.clone(),
            config.body.as_ref().map(|b| bytes::Bytes::from(b.clone())),
            chaos,
            config.quic_max_idle_timeout_ms,
            config.quic_zero_rtt,
        ) {
            Ok(engine) => Arc::new(engine),
            Err(e) => {
                tracing::warn!(error = %e, "failed to create HTTP/3 engine, falling back to HTTP/1.1");
                Arc::new(http::HttpEngine::new())
            }
        }
    } else if url.starts_with("sse://") || url.starts_with("sses://") {
        let engine = sse::SseEngine::new(
            config.headers.clone().unwrap_or_default(),
            chaos,
            config.sse_max_events,
        );
        Arc::new(engine)
    } else if config.sse_enabled {
        Arc::new(sse::SseEngine::new(
            config.headers.clone().unwrap_or_default(),
            chaos,
            config.sse_max_events,
        ))
    } else {
        Arc::new(http::HttpEngine::new())
    }
}
