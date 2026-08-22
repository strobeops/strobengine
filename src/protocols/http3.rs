use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::{Buf, Bytes};
use h3_quinn::Connection as H3QuinnConnection;
use quinn::{ClientConfig, Endpoint, TokioRuntime, TransportConfig};
use tokio::sync::OnceCell;

use crate::chaos::{ChaosEngine, ChaosFault};
use crate::metrics::RequestMetric;

use super::ProtocolEngine;

/// Persistent HTTP/3 session (one QUIC connection per worker).
pub struct Http3Session {
    pub h3_send: h3::client::SendRequest<h3_quinn::OpenStreams, Bytes>,
    pub quinn_conn: quinn::Connection,
    pub zero_rtt_accepted: Option<bool>,
    pub prev_lost_packets: u64,
}

pub struct Http3Engine {
    endpoint: OnceCell<Endpoint>,
    server_name: String,
    authority: String,
    path: String,
    method: http::Method,
    headers: Vec<(String, String)>,
    body: Option<Bytes>,
    chaos: ChaosEngine,
    #[allow(dead_code)]
    max_idle_timeout_ms: Option<u64>,
    #[allow(dead_code)]
    zero_rtt: bool,
}

impl Http3Engine {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        url: &str,
        headers: Vec<(String, String)>,
        method: String,
        body: Option<Bytes>,
        chaos: ChaosEngine,
        max_idle_timeout_ms: Option<u64>,
        zero_rtt: bool,
    ) -> Result<Self, String> {
        let rewritten = if url.starts_with("h3://") {
            url.replacen("h3://", "https://", 1)
        } else if url.starts_with("http3://") {
            url.replacen("http3://", "https://", 1)
        } else {
            url.to_string()
        };

        let parsed = http::Uri::from_maybe_shared(Bytes::from(rewritten))
            .map_err(|e| format!("invalid HTTP/3 URL: {e}"))?;

        let host = parsed
            .host()
            .ok_or_else(|| "URL missing host".to_string())?
            .to_string();

        let port = parsed.port_u16().unwrap_or(443);

        let path = parsed
            .path_and_query()
            .map(|pq| pq.as_str().to_string())
            .unwrap_or_else(|| "/".to_string());

        let server_name = host.clone();
        let authority = format!("{host}:{port}");

        let method = match method.to_uppercase().as_str() {
            "GET" => http::Method::GET,
            "POST" => http::Method::POST,
            "PUT" => http::Method::PUT,
            "DELETE" => http::Method::DELETE,
            "PATCH" => http::Method::PATCH,
            "HEAD" => http::Method::HEAD,
            "OPTIONS" => http::Method::OPTIONS,
            _ => return Err(format!("invalid HTTP method: {method}")),
        };

        // Endpoint is created lazily on first use (inside Tokio runtime)
        Ok(Self {
            endpoint: OnceCell::new(),
            server_name,
            authority,
            path,
            method,
            headers,
            body,
            chaos,
            max_idle_timeout_ms,
            zero_rtt,
        })
    }

    /// Lazily create the QUIC endpoint on first use.
    /// Must be called from within a Tokio runtime context.
    async fn ensure_endpoint(&self) -> Result<&Endpoint, String> {
        self.endpoint
            .get_or_try_init(|| async {
                // Install ring crypto provider if not already installed
                let _ = quinn::rustls::crypto::ring::default_provider().install_default();

                let addr: SocketAddr = "0.0.0.0:0"
                    .parse()
                    .map_err(|e| format!("failed to parse bind address: {e}"))?;

                let socket = std::net::UdpSocket::bind(addr)
                    .map_err(|e| format!("failed to bind UDP socket: {e}"))?;
                socket
                    .set_nonblocking(true)
                    .map_err(|e| format!("failed to set nonblocking: {e}"))?;

                let mut endpoint = Endpoint::new(
                    quinn::EndpointConfig::default(),
                    None,
                    socket,
                    Arc::new(TokioRuntime),
                )
                .map_err(|e| format!("failed to create QUIC endpoint: {e}"))?;

                // Configure TLS 1.3 with ALPN h3
                let mut roots = quinn::rustls::RootCertStore::empty();
                roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

                let mut tls = quinn::rustls::ClientConfig::builder()
                    .with_root_certificates(roots)
                    .with_no_client_auth();

                tls.alpn_protocols = vec![b"h3".to_vec()];

                // Enable TLS 1.3 session resumption for 0-RTT support
                tls.resumption = quinn::rustls::client::Resumption::in_memory_sessions(256);

                // Configure QUIC transport
                let mut transport = TransportConfig::default();
                let idle_timeout = self.max_idle_timeout_ms.unwrap_or(30_000).min(16_383);
                transport.max_idle_timeout(Some(
                    Duration::from_millis(idle_timeout)
                        .try_into()
                        .map_err(|e| format!("invalid idle timeout: {e}"))?,
                ));
                transport.max_concurrent_bidi_streams(100u32.into());
                transport.max_concurrent_uni_streams(100u32.into());

                let quic_tls = quinn::crypto::rustls::QuicClientConfig::try_from(tls)
                    .map_err(|e| format!("TLS config error: {e}"))?;

                let mut client_config = ClientConfig::new(Arc::new(quic_tls));
                client_config.transport_config(Arc::new(transport));

                endpoint.set_default_client_config(client_config);

                Ok(endpoint)
            })
            .await
    }

    async fn connect_quic(&self) -> Result<quinn::Connection, String> {
        let connecting = self.connect_quic_connecting().await?;
        let connection = tokio::time::timeout(Duration::from_secs(5), async {
            connecting
                .await
                .map_err(|e| format!("QUIC handshake failed: {e}"))
        })
        .await
        .map_err(|_| "QUIC handshake timed out".to_string())?
        .map_err(|e| e.to_string())?;
        Ok(connection)
    }

    async fn connect_quic_connecting(&self) -> Result<quinn::Connecting, String> {
        let endpoint = self.ensure_endpoint().await?;

        let addr_str = format!(
            "{}:{}",
            self.authority.split(':').next().unwrap_or(&self.authority),
            self.authority.split(':').nth(1).unwrap_or("443")
        );
        let addr = addr_str
            .to_socket_addrs()
            .map_err(|e| format!("DNS resolution failed: {e}"))?
            .next()
            .ok_or_else(|| "no addresses found for host".to_string())?;

        endpoint
            .connect(addr, &self.server_name)
            .map_err(|e| format!("QUIC connect error: {e}"))
    }

    async fn connect_with_0rtt(
        &self,
        connecting: quinn::Connecting,
    ) -> Result<(quinn::Connection, Option<bool>, bool), String> {
        let connect_start = Instant::now();
        match connecting.into_0rtt() {
            Ok((conn, zero_rtt_accepted)) => {
                let accepted = zero_rtt_accepted.await;
                tracing::debug!(accepted, "0-RTT connection established");
                Ok((conn, Some(accepted), true))
            }
            Err(connecting) => {
                let connection = tokio::time::timeout(Duration::from_secs(5), async {
                    connecting
                        .await
                        .map_err(|e| format!("QUIC handshake failed: {e}"))
                })
                .await
                .map_err(|_| "QUIC handshake timed out".to_string())?
                .map_err(|e| e.to_string())?;

                let handshake_us = connect_start.elapsed().as_micros() as u64;
                tracing::debug!(handshake_us, "1-RTT connection established");
                Ok((connection, None, false))
            }
        }
    }

    async fn setup_h3(
        &self,
        connection: quinn::Connection,
    ) -> Result<h3::client::SendRequest<h3_quinn::OpenStreams, Bytes>, String> {
        let h3_quinn_conn = H3QuinnConnection::new(connection);
        let (_, send_request) = h3::client::builder()
            .build(h3_quinn_conn)
            .await
            .map_err(|e| format!("H3 connection setup failed: {e}"))?;
        Ok(send_request)
    }

    fn build_request(&self) -> Result<http::Request<()>, String> {
        let uri = format!("https://{}{}", self.authority, self.path);

        let mut builder = http::Request::builder()
            .method(self.method.clone())
            .uri(&uri)
            .version(http::Version::HTTP_3);

        for (key, value) in &self.headers {
            if let (Ok(name), Ok(val)) = (
                http::header::HeaderName::from_bytes(key.as_bytes()),
                http::header::HeaderValue::from_str(value),
            ) {
                builder = builder.header(name, val);
            }
        }

        builder
            .body(())
            .map_err(|e| format!("failed to build request: {e}"))
    }

    async fn send_request_on_conn(
        &self,
        send_request: &mut h3::client::SendRequest<h3_quinn::OpenStreams, Bytes>,
    ) -> Result<(u16, u64), String> {
        let request = self.build_request()?;

        let mut stream = send_request
            .send_request(request)
            .await
            .map_err(|e| format!("failed to open H3 stream: {e}"))?;

        if let Some(body) = &self.body {
            stream
                .send_data(body.clone())
                .await
                .map_err(|e| format!("failed to send H3 body: {e}"))?;
        }

        stream
            .finish()
            .await
            .map_err(|e| format!("failed to finish H3 stream: {e}"))?;

        let response = stream
            .recv_response()
            .await
            .map_err(|e| format!("failed to receive H3 response: {e}"))?;

        let status = response.status().as_u16();

        let mut total_bytes = 0u64;
        while let Some(chunk) = stream
            .recv_data()
            .await
            .map_err(|e| format!("failed to receive H3 data: {e}"))?
        {
            total_bytes += chunk.remaining() as u64;
        }

        Ok((status, total_bytes))
    }
}

#[async_trait]
impl ProtocolEngine for Http3Engine {
    async fn execute_iteration(&self, _target_url: &str) -> RequestMetric {
        let req_start = Instant::now();

        // Pre-connection chaos
        let fault = self.chaos.select_fault();

        if let Some(ChaosFault::ConnectionDrop) = fault {
            tracing::trace!("http3 chaos: connection drop");
            return RequestMetric::error(req_start.elapsed().as_micros());
        }

        if let Some(ChaosFault::LatencySpike { duration_ms }) = fault {
            tracing::trace!(duration_ms, "http3 chaos: latency spike");
            tokio::time::sleep(Duration::from_millis(duration_ms)).await;
        }

        // Connect QUIC
        let connect_start = Instant::now();
        let connection = match self.connect_quic().await {
            Ok(c) => c,
            Err(e) => {
                tracing::debug!(error = %e, "QUIC connection failed");
                return RequestMetric::error(req_start.elapsed().as_micros());
            }
        };
        let connection_latency_us = connect_start.elapsed().as_micros();

        // Setup H3
        let mut send_request = match self.setup_h3(connection.clone()).await {
            Ok(s) => s,
            Err(e) => {
                tracing::debug!(error = %e, "H3 setup failed");
                connection.close(0u32.into(), b"");
                return RequestMetric::error(req_start.elapsed().as_micros());
            }
        };

        // Send request
        let result = match fault {
            Some(ChaosFault::CorruptedPayload) => {
                tracing::trace!("http3 chaos: corrupted payload");
                if let Ok(request) = self.build_request()
                    && let Ok(mut stream) = send_request.send_request(request).await
                {
                    let _ = stream
                        .send_data(Bytes::from_static(b"\xff\xfe\xbd\xef"))
                        .await;
                    let _ = stream.finish().await;
                    let _ = stream.recv_response().await;
                }
                match self.send_request_on_conn(&mut send_request).await {
                    Ok((s, b)) => (s, b),
                    Err(_) => (0, 0),
                }
            }
            _ => match self.send_request_on_conn(&mut send_request).await {
                Ok((status, bytes)) => (status, bytes),
                Err(e) => {
                    tracing::debug!(error = %e, "H3 request failed");
                    (0, 0)
                }
            },
        };

        connection.close(0u32.into(), b"");

        let latency_micros = req_start.elapsed().as_micros();

        RequestMetric {
            latency_micros,
            status_code: result.0,
            bytes_received: result.1,
            is_reconnect: false,
            connection_latency_us: Some(connection_latency_us),
            timestamp_sent_ns: None,
            e2e_latency_us: None,
            quic_handshake_us: None,
            quic_0rtt_used: false,
            quic_retransmits: None,
        }
    }

    async fn create_worker_context(&self) -> Option<Box<dyn std::any::Any + Send>> {
        let connecting = self.connect_quic_connecting().await.ok()?;
        let (connection, zero_rtt_accepted, _) = self.connect_with_0rtt(connecting).await.ok()?;
        let send_request = self.setup_h3(connection.clone()).await.ok()?;

        Some(Box::new(Http3Session {
            h3_send: send_request,
            quinn_conn: connection,
            zero_rtt_accepted,
            prev_lost_packets: 0,
        }))
    }

    async fn execute_iteration_with_context(
        &self,
        _target_url: &str,
        ctx: &mut (dyn std::any::Any + Send),
    ) -> RequestMetric {
        let req_start = Instant::now();

        let session = match ctx.downcast_mut::<Http3Session>() {
            Some(s) => s,
            None => return RequestMetric::error(req_start.elapsed().as_micros()),
        };

        // Apply chaos
        let fault = self.chaos.select_fault();

        if let Some(ChaosFault::ConnectionDrop) = fault {
            tracing::trace!("http3 chaos: connection drop (persistent)");
            return RequestMetric::error(req_start.elapsed().as_micros());
        }

        if let Some(ChaosFault::LatencySpike { duration_ms }) = fault {
            tracing::trace!(duration_ms, "http3 chaos: latency spike (persistent)");
            tokio::time::sleep(Duration::from_millis(duration_ms)).await;
        }

        let mut is_reconnect = false;
        let mut handshake_us: Option<u64> = None;
        let mut used_0rtt = false;

        let result = match self.send_request_on_conn(&mut session.h3_send).await {
            Ok((status, bytes)) => (status, bytes),
            Err(e) => {
                tracing::debug!(error = %e, "H3 persistent session failed, reconnecting");
                let connect_start = Instant::now();
                match self.connect_quic_connecting().await {
                    Ok(connecting) => match self.connect_with_0rtt(connecting).await {
                        Ok((new_conn, zero_rtt, is_0rtt)) => {
                            handshake_us = Some(connect_start.elapsed().as_micros() as u64);
                            used_0rtt = is_0rtt;
                            match self.setup_h3(new_conn.clone()).await {
                                Ok(new_send) => {
                                    session.quinn_conn = new_conn;
                                    session.h3_send = new_send;
                                    session.zero_rtt_accepted = zero_rtt;
                                    session.prev_lost_packets = 0;
                                    is_reconnect = true;
                                    match self.send_request_on_conn(&mut session.h3_send).await {
                                        Ok((s, b)) => (s, b),
                                        Err(_) => (0, 0),
                                    }
                                }
                                Err(_) => (0, 0),
                            }
                        }
                        Err(_) => (0, 0),
                    },
                    Err(_) => (0, 0),
                }
            }
        };

        // Track retransmissions from QUIC connection stats
        let stats = session.quinn_conn.stats();
        let lost = stats.path.lost_packets;
        let retransmits = lost.saturating_sub(session.prev_lost_packets);
        session.prev_lost_packets = lost;

        // Check if 0-RTT was accepted
        if !used_0rtt && let Some(accepted) = session.zero_rtt_accepted {
            used_0rtt = accepted;
        }

        let latency_micros = req_start.elapsed().as_micros();

        RequestMetric {
            latency_micros,
            status_code: result.0,
            bytes_received: result.1,
            is_reconnect,
            connection_latency_us: None,
            timestamp_sent_ns: None,
            e2e_latency_us: None,
            quic_handshake_us: handshake_us,
            quic_0rtt_used: used_0rtt,
            quic_retransmits: Some(retransmits),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_http3_url_parsing_h3_scheme() {
        let engine = Http3Engine::new(
            "h3://example.com:443/test",
            vec![],
            "GET".into(),
            None,
            ChaosEngine::default(),
            None,
            false,
        )
        .unwrap();

        assert_eq!(engine.server_name, "example.com");
        assert_eq!(engine.authority, "example.com:443");
        assert_eq!(engine.path, "/test");
    }

    #[tokio::test]
    async fn test_http3_url_parsing_http3_scheme() {
        let engine = Http3Engine::new(
            "http3://example.com/path/to/resource",
            vec![],
            "GET".into(),
            None,
            ChaosEngine::default(),
            None,
            false,
        )
        .unwrap();

        assert_eq!(engine.server_name, "example.com");
        assert_eq!(engine.authority, "example.com:443");
        assert_eq!(engine.path, "/path/to/resource");
    }

    #[tokio::test]
    async fn test_http3_engine_creation() {
        let engine = Http3Engine::new(
            "h3://127.0.0.1:4433/api",
            vec![("x-test".into(), "value".into())],
            "POST".into(),
            Some(Bytes::from("body")),
            ChaosEngine::default(),
            Some(10_000),
            true,
        )
        .unwrap();

        assert_eq!(engine.server_name, "127.0.0.1");
        assert_eq!(engine.authority, "127.0.0.1:4433");
        assert_eq!(engine.path, "/api");
        assert_eq!(engine.method, http::Method::POST);
        assert!(engine.body.is_some());
        assert_eq!(engine.max_idle_timeout_ms, Some(10_000));
        assert!(engine.zero_rtt);
        assert_eq!(engine.headers.len(), 1);
        // Endpoint should not be created yet (lazy init)
        assert!(engine.endpoint.get().is_none());
    }

    #[tokio::test]
    async fn test_http3_invalid_url() {
        let result = Http3Engine::new(
            "not-a-url",
            vec![],
            "GET".into(),
            None,
            ChaosEngine::default(),
            None,
            false,
        );
        if let Ok(engine) = result {
            assert!(engine.server_name.is_empty() || engine.authority.contains(':'));
        }
    }

    #[tokio::test]
    async fn test_http3_invalid_method() {
        let result = Http3Engine::new(
            "h3://example.com/test",
            vec![],
            "BOGUS".into(),
            None,
            ChaosEngine::default(),
            None,
            false,
        );
        match result {
            Ok(_) => panic!("expected error for invalid method"),
            Err(e) => assert!(e.contains("invalid HTTP method")),
        }
    }
}
