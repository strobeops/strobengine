use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::StreamExt;
use http::Method;
use reqwest::Url;

use crate::chaos::{ChaosEngine, ChaosFault};
use crate::metrics::RequestMetric;

use super::ProtocolEngine;

static CORRUPTED_BODY: &[u8] = b"{\"payload\": \"\\xff\\xfe\\xbd\\xef\"}";
static CHAOS_HEADER: &str = "x-chaos-fault";
static BAD_HEADER_VALUE: &str = "invalid-header-value";

pub struct HttpEngine {
    client: reqwest::Client,
    method: Method,
    body: Option<Bytes>,
    chaos: ChaosEngine,
}

impl Default for HttpEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl HttpEngine {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
            method: Method::GET,
            body: None,
            chaos: ChaosEngine::default(),
        }
    }

    pub fn with_client(mut self, client: reqwest::Client) -> Self {
        self.client = client;
        self
    }

    pub fn with_method(mut self, method: Method) -> Self {
        self.method = method;
        self
    }

    pub fn with_body(mut self, body: Option<Bytes>) -> Self {
        self.body = body;
        self
    }

    pub fn with_chaos(mut self, chaos: ChaosEngine) -> Self {
        self.chaos = chaos;
        self
    }

    pub async fn prewarm(&self, url: &str) {
        let mut req = self.client.request(self.method.clone(), url);
        if let Some(ref b) = self.body {
            req = req.body(b.clone());
        }
        let _ = req.send().await;
    }
}

/// Read the response body to EOF, counting the bytes actually received.
///
/// Draining is what returns the connection to the reqwest pool: dropping a
/// response with its body unread makes hyper discard the socket, silently
/// defeating keep-alive. On a mid-body error the bytes counted so far are
/// returned alongside the error so the metric still accounts for them.
async fn drain_body(response: reqwest::Response) -> (u64, Option<reqwest::Error>) {
    let mut stream = response.bytes_stream();
    let mut total_bytes = 0u64;
    while let Some(chunk) = stream.next().await {
        match chunk {
            Ok(chunk) => total_bytes += chunk.len() as u64,
            Err(e) => return (total_bytes, Some(e)),
        }
    }
    (total_bytes, None)
}

#[async_trait]
impl ProtocolEngine for HttpEngine {
    async fn execute_iteration(&self, target_url: &str) -> RequestMetric {
        let req_start = Instant::now();

        // Parse URL once — cached across iterations by the caller
        let parsed_url = match Url::parse(target_url) {
            Ok(u) => u,
            Err(e) => {
                tracing::error!(error = %e, "failed to parse URL");
                return RequestMetric::error(req_start.elapsed().as_micros(), None);
            }
        };

        let base_request = || {
            let mut req = self.client.request(self.method.clone(), parsed_url.clone());
            if let Some(ref b) = self.body {
                req = req.body(b.clone());
            }
            req
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
                    .request(self.method.clone(), target_url)
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
                let (bytes_received, body_error) = drain_body(response).await;
                if let Some(ref e) = body_error {
                    tracing::debug!(error = %e, bytes_received, "response body read failed");
                    // A truncated body is a failed request: report status 0
                    // with the bytes actually transferred before the failure.
                    RequestMetric::builder(req_start.elapsed().as_micros(), chaos_fault)
                        .status(0)
                        .bytes(bytes_received)
                        .build()
                } else {
                    RequestMetric::builder(req_start.elapsed().as_micros(), chaos_fault)
                        .status(status_code)
                        .bytes(bytes_received)
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
                "request completed"
            );
        }

        // reqwest handles DNS and connection pooling internally;
        // dns_resolution_us and is_socket_reused are not extractable
        // without hyper-level access. Fully draining the body (drain_body)
        // is what lets hyper return the socket to the keep-alive pool.
        metric
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::*;

    /// Raw local HTTP server. Accepts connections, counts them, and serves a
    /// per-mode response. Reads each request fully (so the close is a clean
    /// FIN, not an RST).
    ///
    /// - `"chunked"`: `Transfer-Encoding: chunked`, body `hello`, then close
    ///   (no Content-Length at all).
    /// - `"short_body"`: `Content-Length: 100` with only `abc` sent, then
    ///   close -- hyper reports a premature-close error after 3 bytes.
    /// - `"keep_alive"`: `Content-Length: 10` + `Connection: keep-alive`,
    ///   body `hello` sent immediately and `world` after a 100ms delay, then
    ///   loops reading further requests on the same socket (until the client
    ///   closes). The split body makes reuse meaningful: a client that drops
    ///   the response unread sees an incomplete message and must reconnect,
    ///   while a client that drains the body gets the pooled socket back.
    async fn spawn_raw_http_server(mode: &'static str) -> (String, Arc<AtomicUsize>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accepts = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&accepts);

        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };
                counter.fetch_add(1, Ordering::Relaxed);

                tokio::spawn(async move {
                    loop {
                        // Drain one request's headers so the later close is
                        // a clean FIN (GET carries no body).
                        let mut buf = [0u8; 4096];
                        let mut seen = 0usize;
                        while !buf[..seen].windows(4).any(|w| w == b"\r\n\r\n") {
                            if seen == buf.len() {
                                break;
                            }
                            match socket.read(&mut buf[seen..]).await {
                                Ok(0) | Err(_) => return,
                                Ok(n) => seen += n,
                            }
                        }

                        let response = match mode {
                            "chunked" => "HTTP/1.1 200 OK\r\n\
                                Transfer-Encoding: chunked\r\n\r\n\
                                5\r\nhello\r\n0\r\n\r\n"
                                .to_string(),
                            "short_body" => {
                                "HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\nabc".to_string()
                            }
                            _ => "HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\
                                Connection: keep-alive\r\n\r\nhello"
                                .to_string(),
                        };
                        if socket.write_all(response.as_bytes()).await.is_err() {
                            return;
                        }
                        let _ = socket.flush().await;

                        if mode != "keep_alive" {
                            // One response per connection, then a clean FIN.
                            return;
                        }

                        // Finish the split body, then serve the next request
                        // on this socket.
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        if socket.write_all(b"world").await.is_err() {
                            return;
                        }
                        let _ = socket.flush().await;
                    }
                });
            }
        });

        (format!("http://{addr}"), accepts)
    }

    #[tokio::test]
    async fn chunked_response_bytes_are_counted_without_content_length() {
        let (url, _accepts) = spawn_raw_http_server("chunked").await;
        let engine = HttpEngine::new();

        let metric = engine.execute_iteration(&url).await;

        assert_eq!(metric.status_code, 200);
        assert_eq!(
            metric.bytes_received, 5,
            "bytes must come from the streamed body, not a header guess"
        );
    }

    #[tokio::test]
    async fn keep_alive_reuses_the_connection_across_iterations() {
        let (url, accepts) = spawn_raw_http_server("keep_alive").await;
        let engine = HttpEngine::new();

        let first = engine.execute_iteration(&url).await;
        let second = engine.execute_iteration(&url).await;

        assert_eq!(first.status_code, 200);
        assert_eq!(first.bytes_received, 10);
        assert_eq!(second.status_code, 200);
        assert_eq!(second.bytes_received, 10);
        assert_eq!(
            accepts.load(Ordering::Relaxed),
            1,
            "the second iteration must reuse the pooled socket"
        );
    }

    #[tokio::test]
    async fn premature_body_close_is_status_zero_with_partial_bytes() {
        let (url, _accepts) = spawn_raw_http_server("short_body").await;
        let engine = HttpEngine::new();

        let metric = engine.execute_iteration(&url).await;

        assert_eq!(metric.status_code, 0);
        assert_eq!(
            metric.bytes_received, 3,
            "bytes received before the premature close must be kept"
        );
    }
}
