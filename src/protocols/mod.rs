pub mod http;
pub mod websocket;

use std::sync::Arc;

use async_trait::async_trait;

use crate::chaos::ChaosEngine;
use crate::config::WsMode;
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

/// Detect the appropriate protocol engine from the URL scheme.
pub fn detect_protocol(
    url: &str,
    headers: Vec<(String, String)>,
    ws_mode: WsMode,
    ws_payload: Option<String>,
    chaos: ChaosEngine,
) -> Arc<dyn ProtocolEngine> {
    if url.starts_with("ws://") || url.starts_with("wss://") {
        Arc::new(websocket::WebSocketEngine::new(
            headers, ws_mode, ws_payload, chaos,
        ))
    } else {
        Arc::new(http::HttpEngine::new())
    }
}
