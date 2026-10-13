use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::StreamExt;
use http::Method;

use crate::chaos::{ChaosEngine, ChaosFault};
use crate::metrics::RequestMetric;

use super::ProtocolEngine;

static CORRUPTED_BODY: &[u8] = b"{\"payload\": \"\\xff\\xfe\\xbd\\xef\"}";
static CHAOS_HEADER: &str = "x-chaos-fault";
static BAD_HEADER_VALUE: &str = "invalid-header-value";

/// Cap on how much of a response body is buffered for JSON parsing. Larger
/// bodies are still fully drained (to preserve keep-alive) but only the first
/// `MAX_PARSE_BYTES` are inspected.
const MAX_PARSE_BYTES: usize = 4 * 1024 * 1024;

/// Build the GraphQL POST body: `{"query": ..., "variables"?: ..., "operationName"?: ...}`.
///
/// `variables` is a JSON object string supplied by the caller; a non-object or
/// malformed value is rejected so the failure surfaces at config time rather
/// than silently sending an invalid request.
pub fn build_payload(
    query: &str,
    variables: Option<&str>,
    operation_name: Option<&str>,
) -> Result<Bytes, String> {
    let mut obj = serde_json::Map::new();
    obj.insert(
        "query".to_string(),
        serde_json::Value::String(query.to_string()),
    );
    if let Some(raw) = variables {
        let parsed: serde_json::Value = serde_json::from_str(raw)
            .map_err(|e| format!("graphql_variables is not valid JSON: {e}"))?;
        if !parsed.is_object() {
            return Err("graphql_variables must be a JSON object".to_string());
        }
        obj.insert("variables".to_string(), parsed);
    }
    if let Some(name) = operation_name {
        obj.insert(
            "operationName".to_string(),
            serde_json::Value::String(name.to_string()),
        );
    }
    let bytes = serde_json::to_vec(&serde_json::Value::Object(obj))
        .map_err(|e| format!("failed to serialize GraphQL payload: {e}"))?;
    Ok(Bytes::from(bytes))
}

/// Best-effort extraction of the operation type from a GraphQL document.
///
/// Leading comments (`# ...`) and whitespace are skipped, then the first
/// alphabetic keyword decides. The anonymous `{ ... }` shorthand and the
/// explicit `query` keyword both report `"query"`.
pub fn parse_operation_type(query: &str) -> &'static str {
    let mut rest = query;
    loop {
        rest = rest.trim_start();
        let Some(comment) = rest.strip_prefix('#') else {
            break;
        };
        match comment.find('\n') {
            Some(i) => rest = &comment[i + 1..],
            None => return "query",
        }
    }
    let token: String = rest
        .chars()
        .take_while(|c| c.is_ascii_alphabetic())
        .collect();
    match token.as_str() {
        "mutation" => "mutation",
        "subscription" => "subscription",
        _ => "query",
    }
}

/// Parse a GraphQL response body, returning the number of `errors` entries.
///
/// Returns `None` when the body is not JSON or does not look like a GraphQL
/// response (no `data`/`errors` keys), so non-GraphQL bodies are not
/// misclassified. A present-but-malformed `errors` value counts as one error.
fn count_graphql_errors(body: &[u8]) -> Option<u64> {
    let value: serde_json::Value = serde_json::from_slice(body).ok()?;
    let obj = value.as_object()?;
    if !obj.contains_key("data") && !obj.contains_key("errors") {
        return None;
    }
    match obj.get("errors") {
        None | Some(serde_json::Value::Null) => Some(0),
        Some(serde_json::Value::Array(errors)) => Some(errors.len() as u64),
        Some(_) => Some(1),
    }
}

pub struct GraphqlEngine {
    client: reqwest::Client,
    payload: Bytes,
    chaos: ChaosEngine,
}

impl GraphqlEngine {
    pub fn new(client: reqwest::Client, payload: Bytes, chaos: ChaosEngine) -> Self {
        Self {
            client,
            payload,
            chaos,
        }
    }
}

/// Read the response body to EOF, counting the bytes actually received and
/// buffering a bounded prefix for GraphQL parsing. Draining fully is what
/// returns the connection to the reqwest pool (see `http.rs`).
async fn drain_body(response: reqwest::Response) -> (u64, Option<Vec<u8>>, Option<reqwest::Error>) {
    let mut stream = response.bytes_stream();
    let mut total_bytes = 0u64;
    let mut buffered: Vec<u8> = Vec::new();
    while let Some(chunk) = stream.next().await {
        match chunk {
            Ok(chunk) => {
                total_bytes += chunk.len() as u64;
                if buffered.len() < MAX_PARSE_BYTES {
                    let remaining = MAX_PARSE_BYTES - buffered.len();
                    buffered.extend_from_slice(&chunk[..chunk.len().min(remaining)]);
                }
            }
            Err(e) => return (total_bytes, None, Some(e)),
        }
    }
    (total_bytes, Some(buffered), None)
}

#[async_trait]
impl ProtocolEngine for GraphqlEngine {
    async fn execute_iteration(&self, target_url: &str) -> RequestMetric {
        let req_start = Instant::now();

        let base_request = || {
            self.client
                .request(Method::POST, target_url)
                .body(self.payload.clone())
        };

        let chaos_fault = self.chaos.select_fault();
        let request = match chaos_fault {
            Some(ChaosFault::LatencySpike { duration_ms }) => {
                tracing::trace!(duration_ms, "chaos: latency spike injected");
                tokio::time::sleep(Duration::from_millis(duration_ms)).await;
                base_request()
            }
            Some(ChaosFault::CorruptedPayload) => {
                tracing::trace!("chaos: corrupted payload injected");
                self.client
                    .request(Method::POST, target_url)
                    .header(CHAOS_HEADER, "corrupted-payload")
                    .body(CORRUPTED_BODY)
            }
            Some(ChaosFault::MetadataCorruption) => {
                tracing::trace!("chaos: metadata corruption injected");
                base_request().header(CHAOS_HEADER, BAD_HEADER_VALUE)
            }
            Some(ChaosFault::ConnectionDrop) => {
                tracing::trace!("chaos: connection drop injected");
                base_request().timeout(Duration::from_nanos(1))
            }
            None => base_request(),
        };

        let metric = match request.send().await {
            Ok(response) => {
                let status_code = response.status().as_u16();
                let (bytes_received, body, body_error) = drain_body(response).await;
                if let Some(ref e) = body_error {
                    tracing::debug!(error = %e, bytes_received, "response body read failed");
                    RequestMetric::builder(req_start.elapsed().as_micros(), chaos_fault)
                        .status(0)
                        .bytes(bytes_received)
                        .build()
                } else {
                    let graphql_errors = body.as_deref().and_then(count_graphql_errors);
                    RequestMetric::builder(req_start.elapsed().as_micros(), chaos_fault)
                        .status(status_code)
                        .bytes(bytes_received)
                        .graphql(graphql_errors)
                        .build()
                }
            }
            Err(e) => {
                tracing::debug!(error = %e, "request failed");
                RequestMetric::error(req_start.elapsed().as_micros(), chaos_fault)
            }
        };

        if tracing::enabled!(tracing::Level::TRACE) {
            tracing::trace!(
                status = metric.status_code,
                latency_us = metric.latency_micros,
                graphql_errors = ?metric.graphql_errors,
                "graphql request completed"
            );
        }

        metric
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::*;

    #[test]
    fn payload_includes_query_variables_and_operation_name() {
        let payload = build_payload(
            "query Q($id: ID!) { node(id: $id) { id } }",
            Some(r#"{"id": "42"}"#),
            Some("Q"),
        )
        .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&payload).unwrap();
        assert_eq!(value["query"], "query Q($id: ID!) { node(id: $id) { id } }");
        assert_eq!(value["variables"]["id"], "42");
        assert_eq!(value["operationName"], "Q");
    }

    #[test]
    fn payload_omits_absent_optional_fields() {
        let payload = build_payload("mutation { inc }", None, None).unwrap();
        let value: serde_json::Value = serde_json::from_slice(&payload).unwrap();
        assert!(value.get("variables").is_none());
        assert!(value.get("operationName").is_none());
    }

    #[test]
    fn payload_rejects_non_object_variables() {
        let err = build_payload("{ x }", Some("[1, 2]"), None).unwrap_err();
        assert!(err.contains("JSON object"), "{err}");
    }

    #[test]
    fn payload_rejects_malformed_variables() {
        let err = build_payload("{ x }", Some("{not json"), None).unwrap_err();
        assert!(err.contains("not valid JSON"), "{err}");
    }

    #[test]
    fn operation_type_is_detected() {
        assert_eq!(parse_operation_type("query { x }"), "query");
        assert_eq!(parse_operation_type("  mutation { inc }"), "mutation");
        assert_eq!(parse_operation_type("{ x }"), "query");
        assert_eq!(
            parse_operation_type("# leading comment\nmutation { inc }"),
            "mutation"
        );
        assert_eq!(
            parse_operation_type("subscription { events }"),
            "subscription"
        );
    }

    #[test]
    fn errors_array_is_counted() {
        let body = br#"{"data": null, "errors": [{"message": "a"}, {"message": "b"}]}"#;
        assert_eq!(count_graphql_errors(body), Some(2));
    }

    #[test]
    fn response_with_data_only_has_zero_errors() {
        assert_eq!(count_graphql_errors(br#"{"data": {"ok": true}}"#), Some(0));
    }

    #[test]
    fn non_graphql_json_is_not_counted() {
        assert_eq!(count_graphql_errors(br#"{"status": "ok"}"#), None);
    }

    #[test]
    fn non_json_body_is_not_counted() {
        assert_eq!(count_graphql_errors(b"not json"), None);
    }

    /// Raw local server: reads one request, echoes an HTTP 200 with the
    /// configured body, then closes. Used to exercise the full
    /// `execute_iteration` path including body parsing.
    async fn spawn_server(body: &'static str) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            if let Ok((mut socket, _)) = listener.accept().await {
                let mut buf = [0u8; 4096];
                let mut seen = 0usize;
                while !buf[..seen].windows(4).any(|w| w == b"\r\n\r\n") {
                    match socket.read(&mut buf[seen..]).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => seen += n,
                    }
                }
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = socket.write_all(response.as_bytes()).await;
                let _ = socket.flush().await;
            }
        });
        format!("http://{addr}")
    }

    #[tokio::test]
    async fn iteration_reports_graphql_errors_on_http_200() {
        let url = spawn_server(r#"{"data": null, "errors": [{"message": "boom"}]}"#).await;
        let engine = GraphqlEngine::new(
            reqwest::Client::new(),
            build_payload("{ x }", None, None).unwrap(),
            ChaosEngine::default(),
        );

        let metric = engine.execute_iteration(&url).await;

        assert_eq!(metric.status_code, 200);
        assert_eq!(metric.graphql_errors, Some(1));
    }

    #[tokio::test]
    async fn iteration_reports_zero_errors_for_success() {
        let url = spawn_server(r#"{"data": {"ok": true}}"#).await;
        let engine = GraphqlEngine::new(
            reqwest::Client::new(),
            build_payload("{ x }", None, None).unwrap(),
            ChaosEngine::default(),
        );

        let metric = engine.execute_iteration(&url).await;

        assert_eq!(metric.status_code, 200);
        assert_eq!(metric.graphql_errors, Some(0));
        assert!(metric.bytes_received > 0);
    }

    #[tokio::test]
    async fn non_graphql_response_yields_no_graphql_sample() {
        let url = spawn_server(r#"{"status": "ok"}"#).await;
        let engine = GraphqlEngine::new(
            reqwest::Client::new(),
            build_payload("{ x }", None, None).unwrap(),
            ChaosEngine::default(),
        );

        let metric = engine.execute_iteration(&url).await;

        assert_eq!(metric.status_code, 200);
        assert_eq!(metric.graphql_errors, None);
    }
}
