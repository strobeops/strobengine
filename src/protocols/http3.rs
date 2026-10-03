use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::{Buf, Bytes};
use h3_quinn::Connection as H3QuinnConnection;
use quinn::{ClientConfig, Endpoint, TokioRuntime, TransportConfig};
use tokio::sync::OnceCell;

use crate::chaos::{ChaosEngine, ChaosFault};
use crate::metrics::{ConnectionMetrics, Http3Sample, RequestMetric};

use super::ProtocolEngine;

/// Persistent HTTP/3 session (one QUIC connection per worker).
pub struct Http3Session {
    pub h3_send: h3::client::SendRequest<h3_quinn::OpenStreams, Bytes>,
    pub quinn_conn: quinn::Connection,
    pub zero_rtt_accepted: Option<bool>,
    pub prev_lost_packets: u64,
    /// DNS resolution time (μs) from initial connection. Only set on first connect.
    pub initial_dns_resolution_us: u64,
    /// Iterations executed on this persistent connection (for migration cadence).
    pub iterations: u64,
}

#[async_trait::async_trait]
impl super::WorkerSession for Http3Session {
    async fn shutdown(&mut self) {}
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
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
    /// Opt-in: periodically rebind the endpoint's UDP socket to exercise QUIC
    /// client connection migration (path validation) and count its outcome.
    migrate: bool,
    migrate_every: u64,
    /// Test-only extra trust anchor (DER) so in-process self-signed servers can
    /// be verified. Never set from config/CLI; defaults to `None`.
    extra_roots_der: Option<Vec<u8>>,
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
            migrate: false,
            migrate_every: 50,
            extra_roots_der: None,
        })
    }

    /// Enable opt-in QUIC connection migration (default off).
    pub fn with_migration(mut self, migrate: bool, every: u64) -> Self {
        self.migrate = migrate;
        self.migrate_every = every.max(1);
        self
    }

    /// Trust an additional DER root (test-only: self-signed local servers).
    pub fn with_test_ca(mut self, der: Vec<u8>) -> Self {
        self.extra_roots_der = Some(der);
        self
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
                if let Some(der) = self.extra_roots_der.clone() {
                    let _ = roots.add_parsable_certificates([
                        quinn::rustls::pki_types::CertificateDer::from(der),
                    ]);
                }

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

    async fn connect_quic(&self) -> Result<(quinn::Connection, u64), String> {
        let (connecting, dns_us) = self.connect_quic_connecting().await?;
        let connection = tokio::time::timeout(Duration::from_secs(5), async {
            connecting
                .await
                .map_err(|e| format!("QUIC handshake failed: {e}"))
        })
        .await
        .map_err(|_| "QUIC handshake timed out".to_string())?
        .map_err(|e| e.to_string())?;
        Ok((connection, dns_us))
    }

    async fn connect_quic_connecting(&self) -> Result<(quinn::Connecting, u64), String> {
        let endpoint = self.ensure_endpoint().await?;

        let addr_str = format!(
            "{}:{}",
            self.authority.split(':').next().unwrap_or(&self.authority),
            self.authority.split(':').nth(1).unwrap_or("443")
        );
        let dns_start = Instant::now();
        let addr = addr_str
            .to_socket_addrs()
            .map_err(|e| format!("DNS resolution failed: {e}"))?
            .next()
            .ok_or_else(|| "no addresses found for host".to_string())?;
        let dns_us = dns_start.elapsed().as_micros() as u64;

        let connecting = endpoint
            .connect(addr, &self.server_name)
            .map_err(|e| format!("QUIC connect error: {e}"))?;
        Ok((connecting, dns_us))
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
        let (mut h3_conn, send_request) = h3::client::builder()
            .build(h3_quinn_conn)
            .await
            .map_err(|e| format!("H3 connection setup failed: {e}"))?;
        // The h3 client Connection must be continuously polled to process the
        // control/QPACK/settings streams; drop-guarded in a background task.
        tokio::spawn(async move {
            let _ = h3_conn.wait_idle().await;
        });
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
            return RequestMetric::error(req_start.elapsed().as_micros(), fault);
        }

        if let Some(ChaosFault::LatencySpike { duration_ms }) = fault {
            tracing::trace!(duration_ms, "http3 chaos: latency spike");
            tokio::time::sleep(Duration::from_millis(duration_ms)).await;
        }

        // Connect QUIC
        let connect_start = Instant::now();
        let (connection, dns_us) = match self.connect_quic().await {
            Ok(c) => c,
            Err(e) => {
                tracing::debug!(error = %e, "QUIC connection failed");
                return RequestMetric::error(req_start.elapsed().as_micros(), fault);
            }
        };
        let connection_latency_us = connect_start.elapsed().as_micros();

        // Setup H3
        let mut send_request = match self.setup_h3(connection.clone()).await {
            Ok(s) => s,
            Err(e) => {
                tracing::debug!(error = %e, "H3 setup failed");
                connection.close(0u32.into(), b"");
                return RequestMetric::error(req_start.elapsed().as_micros(), fault);
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

        // Sample QUIC congestion window before teardown.
        let cwnd_bytes = connection.stats().path.cwnd;
        connection.close(0u32.into(), b"");

        let latency_micros = req_start.elapsed().as_micros();

        RequestMetric {
            latency_micros,
            status_code: result.0,
            bytes_received: result.1,
            is_reconnect: false,
            connection: ConnectionMetrics {
                connection_latency_us: Some(connection_latency_us),
                timestamp_sent_ns: None,
                e2e_latency_us: None,
                dns_resolution_us: Some(dns_us),
                is_socket_reused: false,
            },
            quic_handshake_us: None,
            quic_0rtt_used: false,
            quic_retransmits: None,
            sse_events_received: None,
            sse_first_event_us: None,
            sse_event_interval_us: None,
            ws: None,
            grpc: None,
            http3: Some(Http3Sample {
                cwnd_bytes,
                migrations_attempted: 0,
                migrations_successful: 0,
            }),
            chaos_fault: fault,
        }
    }

    async fn create_worker_context(&self) -> Option<Box<dyn super::WorkerSession>> {
        let (connecting, dns_us) = self.connect_quic_connecting().await.ok()?;
        let (connection, zero_rtt_accepted, _) = self.connect_with_0rtt(connecting).await.ok()?;
        let send_request = self.setup_h3(connection.clone()).await.ok()?;

        Some(Box::new(Http3Session {
            h3_send: send_request,
            quinn_conn: connection,
            zero_rtt_accepted,
            prev_lost_packets: 0,
            initial_dns_resolution_us: dns_us,
            iterations: 0,
        }))
    }

    async fn execute_iteration_with_context(
        &self,
        _target_url: &str,
        ctx: &mut dyn super::WorkerSession,
    ) -> RequestMetric {
        let req_start = Instant::now();

        let session = match ctx.as_any_mut().downcast_mut::<Http3Session>() {
            Some(s) => s,
            None => return RequestMetric::error(req_start.elapsed().as_micros(), None),
        };

        // Apply chaos
        let fault = self.chaos.select_fault();

        if let Some(ChaosFault::ConnectionDrop) = fault {
            tracing::trace!("http3 chaos: connection drop (persistent)");
            return RequestMetric::error(req_start.elapsed().as_micros(), fault);
        }

        if let Some(ChaosFault::LatencySpike { duration_ms }) = fault {
            tracing::trace!(duration_ms, "http3 chaos: latency spike (persistent)");
            tokio::time::sleep(Duration::from_millis(duration_ms)).await;
        }

        let mut is_reconnect = false;
        let mut handshake_us: Option<u64> = None;
        let mut used_0rtt = false;
        session.iterations += 1;
        let mut mig_att = 0u64;
        let mut mig_succ = 0u64;

        // Opt-in migration: rebind the endpoint's UDP socket to a fresh local
        // address, forcing QUIC path validation (PATH_CHALLENGE/RESPONSE) for the
        // in-flight connection. Success is observed indirectly: the connection
        // staying open and the subsequent round-trip completing.
        if self.migrate
            && session.iterations.is_multiple_of(self.migrate_every)
            && let Some(endpoint) = self.endpoint.get()
        {
            match bind_migration_socket() {
                Ok(socket) => {
                    if endpoint.rebind(socket).is_ok() {
                        mig_att = 1;
                    } else {
                        tracing::debug!("http3 migration rebind failed");
                    }
                }
                Err(e) => tracing::debug!(error = %e, "http3 migration socket bind failed"),
            }
        }

        let result = match self.send_request_on_conn(&mut session.h3_send).await {
            Ok((status, bytes)) => (status, bytes),
            Err(e) => {
                tracing::debug!(error = %e, "H3 persistent session failed, reconnecting");
                let connect_start = Instant::now();
                match self.connect_quic_connecting().await {
                    Ok((connecting, _dns_us)) => match self.connect_with_0rtt(connecting).await {
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
        let cwnd_bytes = stats.path.cwnd;

        // A migration "succeeds" when the connection survived the rebind and the
        // round-trip completed on it (no reconnect) with a valid response.
        if mig_att == 1 && !is_reconnect && result.0 != 0 {
            mig_succ = 1;
        }

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
            connection: ConnectionMetrics {
                connection_latency_us: None,
                timestamp_sent_ns: None,
                e2e_latency_us: None,
                // DNS was measured once during create_worker_context; not per-iteration
                dns_resolution_us: None,
                is_socket_reused: !is_reconnect,
            },
            quic_handshake_us: handshake_us,
            quic_0rtt_used: used_0rtt,
            quic_retransmits: Some(retransmits),
            sse_events_received: None,
            sse_first_event_us: None,
            sse_event_interval_us: None,
            ws: None,
            grpc: None,
            http3: Some(Http3Sample {
                cwnd_bytes,
                migrations_attempted: mig_att,
                migrations_successful: mig_succ,
            }),
            chaos_fault: fault,
        }
    }
}

/// Bind a fresh non-blocking UDP socket for a client migration rebind.
fn bind_migration_socket() -> Result<std::net::UdpSocket, String> {
    let socket = std::net::UdpSocket::bind("0.0.0.0:0")
        .map_err(|e| format!("failed to bind migration socket: {e}"))?;
    socket
        .set_nonblocking(true)
        .map_err(|e| format!("failed to set nonblocking: {e}"))?;
    Ok(socket)
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

    // ---- In-process HTTP/3 (quinn+h3) server for cwnd/migration tests ----
    use super::ProtocolEngine;

    /// Start a minimal h3 echo server on 127.0.0.1/::1 (whichever "localhost"
    /// resolves to). Returns (port, DER trust anchor to hand the client).
    async fn spawn_h3_server() -> (u16, Vec<u8>) {
        // rustls requires a process-default CryptoProvider before builder(); the
        // client normally installs it lazily, so ensure it here too.
        let _ = quinn::rustls::crypto::ring::default_provider().install_default();
        let ck = rcgen::generate_simple_self_signed(vec!["127.0.0.1".to_string()]).unwrap();
        let cert_der = ck.cert.der().clone();
        let trust_der = ck.cert.der().to_vec();
        let key_der =
            quinn::rustls::pki_types::PrivatePkcs8KeyDer::from(ck.key_pair.serialize_der());

        let mut tls = quinn::rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(
                vec![cert_der],
                quinn::rustls::pki_types::PrivateKeyDer::Pkcs8(key_der),
            )
            .unwrap();
        tls.alpn_protocols = vec![b"h3".to_vec()];
        let quic_crypto = quinn::crypto::rustls::QuicServerConfig::try_from(tls).unwrap();
        let server_config = quinn::ServerConfig::with_crypto(std::sync::Arc::new(quic_crypto));

        let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        socket.set_nonblocking(true).unwrap();
        let port = socket.local_addr().unwrap().port();
        let endpoint = quinn::Endpoint::new(
            quinn::EndpointConfig::default(),
            Some(server_config),
            socket,
            std::sync::Arc::new(quinn::TokioRuntime),
        )
        .unwrap();

        tokio::spawn(async move {
            while let Some(incoming) = endpoint.accept().await {
                tokio::spawn(async move {
                    let Ok(conn) = incoming.await else { return };
                    let Ok(mut h3conn) = h3::server::builder()
                        .build(h3_quinn::Connection::new(conn))
                        .await
                    else {
                        return;
                    };
                    while let Ok(Some(resolver)) = h3conn.accept().await {
                        tokio::spawn(async move {
                            let Ok((_req, mut stream)) = resolver.resolve_request().await else {
                                return;
                            };
                            let resp = http::Response::builder()
                                .status(http::StatusCode::OK)
                                .body(())
                                .unwrap();
                            if stream.send_response(resp).await.is_ok() {
                                let _ = stream
                                    .send_data(bytes::Bytes::from_static(b"hello-h3"))
                                    .await;
                                let _ = stream.finish().await;
                            }
                        });
                    }
                });
            }
        });
        (port, trust_der)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn http3_engine_samples_cwnd_over_real_transfer() {
        let (port, trust) = spawn_h3_server().await;
        let engine = Http3Engine::new(
            &format!("h3://127.0.0.1:{port}/test"),
            vec![],
            "GET".into(),
            None,
            ChaosEngine::default(),
            None,
            false,
        )
        .unwrap()
        .with_test_ca(trust);

        let m = tokio::time::timeout(Duration::from_secs(5), engine.execute_iteration(""))
            .await
            .expect("h3 request should complete");

        assert_eq!(m.status_code, 200);
        let h = m.http3.expect("http3 sample present");
        // A real QUIC connection has a non-zero initial congestion window.
        assert!(h.cwnd_bytes > 0, "expected cwnd > 0, got {}", h.cwnd_bytes);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn http3_engine_counts_opt_in_migration() {
        let (port, trust) = spawn_h3_server().await;
        let engine = std::sync::Arc::new(
            Http3Engine::new(
                &format!("h3://127.0.0.1:{port}/test"),
                vec![],
                "GET".into(),
                None,
                ChaosEngine::default(),
                None,
                false,
            )
            .unwrap()
            .with_test_ca(trust)
            .with_migration(true, 1), // migrate every iteration
        );

        let mut ctx = engine.create_worker_context().await.expect("session");
        let mut attempted = 0u64;
        for _ in 0..3 {
            let m = tokio::time::timeout(
                Duration::from_secs(5),
                engine.execute_iteration_with_context("", ctx.as_mut()),
            )
            .await
            .expect("iteration should finish");
            if let Some(h) = &m.http3 {
                attempted += h.migrations_attempted;
            }
        }
        assert!(
            attempted >= 1,
            "expected at least one migration attempt, got {attempted}"
        );
    }
}
