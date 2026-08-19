pub mod grpc;
pub mod grpc_parser;
pub mod grpc_reflection;
pub mod http;
pub mod websocket;

use std::sync::Arc;

use async_trait::async_trait;

use crate::chaos::ChaosEngine;
use crate::config::TestConfig;
use crate::metrics::RequestMetric;

/// A protocol engine that executes a single load-test iteration.
/// Orchestration (strategy, metrics aggregation, progress, SIGINT)
/// is handled by `execute_test` in `lib.rs`.
#[async_trait]
pub trait ProtocolEngine: Send + Sync {
    /// Execute a single iteration (HTTP request, WS handshake, etc.)
    /// and return the metric for this unit.
    async fn execute_iteration(&self, target_url: &str) -> RequestMetric;
}

/// Detect the appropriate protocol engine from the URL scheme and config.
pub fn detect_protocol(
    url: &str,
    config: &TestConfig,
    chaos: ChaosEngine,
) -> Arc<dyn ProtocolEngine> {
    if url.starts_with("ws://") || url.starts_with("wss://") {
        Arc::new(websocket::WebSocketEngine::new(
            config.headers.clone().unwrap_or_default(),
            config.ws_mode,
            config.ws_payload.clone(),
            chaos,
            config.timeout_secs,
        ))
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
                tracing::warn!(error = %e, "failed to create gRPC engine, falling back to raw payload");
                // Fallback: create engine without proto schema
                Arc::new(
                    grpc::GrpcEngine::new(
                        url,
                        config.headers.clone().unwrap_or_default(),
                        chaos,
                        config.grpc_service.clone(),
                        config.grpc_method.clone(),
                        config.grpc_payload.clone(),
                        config.grpc_deadline_ms,
                        None,
                        false,
                    )
                    .expect("failed to create fallback gRPC engine"),
                )
            }
        }
    } else {
        Arc::new(http::HttpEngine::new())
    }
}
