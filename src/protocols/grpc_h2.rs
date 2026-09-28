//! Config-gated raw-`h2` gRPC engine that emits real HTTP/2 stream-concurrency
//! and send flow-control telemetry.
//!
//! Enabled by `TestConfig::grpc_h2_multiplex`. Unlike the tonic engine (a fresh
//! connection per call), this holds a single multiplexed HTTP/2 connection and
//! clones its `SendRequest` to every worker iteration, so `num_active_streams()`
//! reflects true concurrent streams against the peer's negotiated
//! `SETTINGS_MAX_CONCURRENT_STREAMS`. Driving the send path ourselves via
//! `SendStream::poll_capacity()` lets us time zero-credit stalls (window
//! exhaustion) that the tonic/hyper stack hides.
//!
//! Scope: cleartext `grpc://` (h2c prior-knowledge) only. `grpcs://` under this
//! flag falls back to the tonic engine (see `detect_protocol`). The gRPC wire
//! format (Length-Prefixed-Message + `grpc-status` trailers) is implemented
//! directly on top of `h2`.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::Poll;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};
use futures_util::future::poll_fn;
use h2::client::SendRequest;
use http::header::{CONTENT_TYPE, HeaderName, HeaderValue};
use http::{Method, Request, Uri};
use tokio::net::TcpStream;
use tokio::sync::OnceCell;

use crate::chaos::{ChaosEngine, ChaosFault};
use crate::metrics::{ConnectionMetrics, GrpcSample, RequestMetric};
use crate::protocols::grpc_parser::ProtoError;

use super::ProtocolEngine;
use super::grpc::{decode_grpc_payload, grpc_to_http_status};

/// Builds a gRPC `Length-Prefixed-Message` (1-byte compression flag + u32BE
/// length + payload). `pub` for benchmarking the per-request framing cost.
pub fn build_frame(payload: &[u8]) -> Bytes {
    let mut buf = BytesMut::with_capacity(5 + payload.len());
    buf.put_u8(0);
    buf.put_u32(payload.len() as u32);
    buf.put_slice(payload);
    buf.freeze()
}

/// Extracts the numeric `grpc-status` from a header map, if present.
fn grpc_status_from(map: Option<&http::HeaderMap>) -> Option<u16> {
    map?.get("grpc-status")?.to_str().ok()?.parse::<u16>().ok()
}

/// RAII counter tracking in-flight streams on the shared multiplexed
/// connection: incremented when an RPC is issued, decremented when its
/// status/trailers arrive or the stream aborts (guard drop on any exit path).
struct ActiveGuard(Arc<AtomicU64>);

impl ActiveGuard {
    fn new(counter: Arc<AtomicU64>) -> Self {
        counter.fetch_add(1, Ordering::Relaxed);
        Self(counter)
    }
}

impl Drop for ActiveGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

pub struct GrpcH2Engine {
    authority: String,
    service: String,
    method: String,
    payload: Vec<u8>,
    headers: Vec<(String, String)>,
    deadline: Option<Duration>,
    chaos: ChaosEngine,
    /// Shared multiplexed connection; the `SendRequest` handle is cheap to clone
    /// and each clone drives the same underlying HTTP/2 connection.
    send: OnceCell<SendRequest<Bytes>>,
    /// Client-side active-stream gauge for the shared connection.
    active: Arc<AtomicU64>,
}

impl GrpcH2Engine {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        url: &str,
        headers: Vec<(String, String)>,
        chaos: ChaosEngine,
        service: Option<String>,
        method: Option<String>,
        grpc_payload: Option<String>,
        deadline_ms: Option<u64>,
        proto_path: Option<String>,
    ) -> Result<Self, ProtoError> {
        let uri: Uri = url
            .parse()
            .map_err(|e| ProtoError::ConnectionError(format!("invalid gRPC URL: {e}")))?;
        let authority = uri
            .authority()
            .ok_or_else(|| {
                ProtoError::ConnectionError(format!("gRPC URL missing authority: {url}"))
            })?
            .as_str()
            .to_string();

        let svc = service.unwrap_or_default();
        let mth = method.unwrap_or_default();
        let payload = decode_grpc_payload(&grpc_payload, &proto_path, &svc, &mth)?;

        Ok(Self {
            authority,
            service: svc,
            method: mth,
            payload,
            headers,
            deadline: deadline_ms.map(Duration::from_millis),
            chaos,
            send: OnceCell::new(),
            active: Arc::new(AtomicU64::new(0)),
        })
    }

    async fn connect(&self) -> Result<SendRequest<Bytes>, ProtoError> {
        let stream = TcpStream::connect(&self.authority)
            .await
            .map_err(|e| ProtoError::ConnectionError(format!("h2 connect failed: {e}")))?;
        let (send, conn) = h2::client::handshake(stream)
            .await
            .map_err(|e| ProtoError::ConnectionError(format!("h2 handshake failed: {e}")))?;
        // Drive the connection in the background for its lifetime.
        tokio::spawn(async move {
            if let Err(e) = conn.await {
                tracing::debug!(error = %e, "multiplexed h2 connection closed");
            }
        });
        Ok(send)
    }

    /// Returns (send handle, connection-latency us, reused flag). The connection
    /// is established once (races deduped by `OnceLock`) and cloned thereafter.
    async fn get_send(&self) -> Result<(SendRequest<Bytes>, u64, bool), ProtoError> {
        let was_set = self.send.get().is_some();
        let t0 = Instant::now();
        let send = self.send.get_or_try_init(|| self.connect()).await?.clone();
        let conn_us = if was_set {
            0
        } else {
            t0.elapsed().as_micros() as u64
        };
        Ok((send, conn_us, was_set))
    }
}

#[async_trait]
impl ProtocolEngine for GrpcH2Engine {
    async fn execute_iteration(&self, _target_url: &str) -> RequestMetric {
        let req_start = Instant::now();

        let fault = self.chaos.select_fault();
        if let Some(ChaosFault::ConnectionDrop) = fault {
            tracing::trace!("grpc-h2 chaos: connection drop");
            return RequestMetric::error(req_start.elapsed().as_micros());
        }
        if let Some(ChaosFault::LatencySpike { duration_ms }) = fault {
            tokio::time::sleep(Duration::from_millis(duration_ms)).await;
        }

        let payload_bytes = match fault {
            Some(ChaosFault::CorruptedPayload) => b"\xff\xfe\xbd\xef".to_vec(),
            _ => self.payload.clone(),
        };

        let (mut send, conn_us, reused) = match self.get_send().await {
            Ok(v) => v,
            Err(_) => return RequestMetric::error(req_start.elapsed().as_micros()),
        };

        // Gate on stream capacity: `poll_ready` blocks when the peer's
        // SETTINGS_MAX_CONCURRENT_STREAMS is saturated (multiplexing headroom).
        if poll_fn(|cx| send.poll_ready(cx)).await.is_err() {
            return RequestMetric::error(req_start.elapsed().as_micros());
        }

        // Build the gRPC request (:path = /<service>/<method>).
        let path = format!("/{}{}", self.service, self.method);
        let uri_str = format!("http://{}{}", self.authority, path);
        let mut builder = Request::builder()
            .method(Method::POST)
            .uri(uri_str)
            .header(CONTENT_TYPE, "application/grpc")
            .header("te", "trailers");
        for (k, v) in &self.headers {
            if let (Ok(name), Ok(val)) = (
                HeaderName::from_lowercase(k.to_lowercase().as_bytes()),
                HeaderValue::from_str(v),
            ) {
                builder = builder.header(name, val);
            }
        }
        if let Some(ChaosFault::MetadataCorruption) = fault {
            builder = builder.header("x-chaos-fault", "corrupted_value");
        }
        let req = match builder.body(()) {
            Ok(r) => r,
            Err(_) => return RequestMetric::error(req_start.elapsed().as_micros()),
        };

        let (resp_fut, mut send_stream) = match send.send_request(req, false) {
            Ok(v) => v,
            Err(_) => return RequestMetric::error(req_start.elapsed().as_micros()),
        };

        // Snapshot multiplexing state immediately after opening our stream.
        // The guard keeps `active` accurate for the whole RPC lifetime, so
        // concurrent iterations on the shared connection observe each other.
        let _active_guard = ActiveGuard::new(self.active.clone());
        let active_streams = self.active.load(Ordering::Relaxed);
        let max_concurrent = send.current_max_send_streams() as u64;
        let utilization = if max_concurrent > 0 {
            active_streams as f64 / max_concurrent as f64
        } else {
            0.0
        };

        // Capacity-gated send: measures flow-control stalls (zero send credit).
        let frame = build_frame(&payload_bytes);
        let (events, stall_us, cap_min, has_cap) = match drive_send(&mut send_stream, &frame).await
        {
            Ok(m) => m,
            Err(_) => return RequestMetric::error(req_start.elapsed().as_micros()),
        };

        // Read the response (optional deadline).
        let read_fut = read_response(resp_fut);
        let outcome = match self.deadline {
            Some(d) => tokio::time::timeout(d, read_fut).await.unwrap_or(None),
            None => read_fut.await,
        };

        let (grpc_status, body_len) = match outcome {
            Some(v) => v,
            None => return RequestMetric::error(req_start.elapsed().as_micros()),
        };
        let status_code = grpc_to_http_status(grpc_status);

        let ws_sample = GrpcSample {
            active_streams,
            max_concurrent_streams: max_concurrent,
            utilization,
            window_exhaustion_events: events,
            window_stall_us: stall_us,
            send_capacity_bytes: cap_min,
            has_send_capacity: has_cap,
        };

        let latency_micros = req_start.elapsed().as_micros();

        RequestMetric {
            latency_micros,
            status_code,
            bytes_received: body_len,
            is_reconnect: false,
            connection: ConnectionMetrics {
                connection_latency_us: if conn_us > 0 {
                    Some(conn_us as u128)
                } else {
                    None
                },
                timestamp_sent_ns: None,
                e2e_latency_us: None,
                dns_resolution_us: None,
                is_socket_reused: reused,
            },
            quic_handshake_us: None,
            quic_0rtt_used: false,
            quic_retransmits: None,
            sse_events_received: None,
            sse_first_event_us: None,
            sse_event_interval_us: None,
            ws: None,
            grpc: Some(ws_sample),
            chaos_fault: fault,
        }
    }
}

/// Writes the frame respecting flow-control credit, timing zero-credit stalls.
/// Returns `(exhaustion_events, stall_us, min_capacity_bytes, saw_capacity)`.
async fn drive_send(
    send_stream: &mut h2::SendStream<Bytes>,
    frame: &[u8],
) -> Result<(u64, u64, u64, bool), ()> {
    let mut events = 0u64;
    let mut stall_us = 0u64;
    let mut cap_min = u64::MAX;
    let mut has_cap = false;

    if frame.is_empty() {
        if send_stream.send_data(Bytes::new(), true).is_err() {
            return Err(());
        }
        return Ok((0, 0, 0, false));
    }

    let mut idx = 0usize;
    while idx < frame.len() {
        let remaining = frame.len() - idx;
        // h2 assigns send capacity to a stream only after it is requested;
        // `poll_capacity` then resolves once that much window is available
        // (increased by a peer WINDOW_UPDATE), so a Pending here == a stall.
        send_stream.reserve_capacity(remaining);
        let poll_start = Instant::now();
        let mut stalled = false;
        let got = poll_fn(|cx| match send_stream.poll_capacity(cx) {
            Poll::Ready(v) => Poll::Ready(v),
            Poll::Pending => {
                stalled = true;
                Poll::Pending
            }
        })
        .await;

        match got {
            Some(Ok(n)) => {
                // Only count a stall once we've already begun sending (a real
                // window exhaustion), not the initial wait for the first credit.
                if stalled && idx > 0 {
                    events += 1;
                    stall_us += poll_start.elapsed().as_micros() as u64;
                }
                if n == 0 {
                    continue;
                }
                cap_min = cap_min.min(n as u64);
                has_cap = true;
                let take = n.min(frame.len() - idx);
                let is_last = idx + take >= frame.len();
                let chunk = Bytes::copy_from_slice(&frame[idx..idx + take]);
                if send_stream.send_data(chunk, is_last).is_err() {
                    return Err(());
                }
                idx += take;
                if is_last {
                    break;
                }
            }
            Some(Err(_)) => return Err(()),
            None => return Err(()),
        }
    }
    Ok((events, stall_us, cap_min, has_cap))
}

/// Awaits response headers, reads DATA, resolves trailers, extracts
/// `grpc-status`. Returns `(grpc_status, message_bytes)` or None on transport error.
async fn read_response(resp_fut: h2::client::ResponseFuture) -> Option<(u16, u64)> {
    let resp = resp_fut.await.ok()?;
    let status = resp.status();
    let mut recv = resp.into_body();
    if status != http::StatusCode::OK {
        // HTTP-level failure: gRPC treats it as an error status.
        return Some((2, 0));
    }

    let mut body = Vec::new();
    loop {
        match poll_fn(|cx| recv.poll_data(cx)).await {
            Some(Ok(data)) => {
                let n = data.len();
                body.extend_from_slice(&data);
                let _ = recv.flow_control().release_capacity(n);
            }
            Some(Err(_)) => return None,
            None => break,
        }
    }

    // Trailers (grpc-status) or trailers-only in the head headers.
    let trailers = poll_fn(|cx| recv.poll_trailers(cx)).await.ok().flatten();
    let grpc_status = grpc_status_from(trailers.as_ref())
        // Some(gRPC servers put status in initial headers for trailers-only responses)
        .unwrap_or(0);

    // Message length is the inner payload (strip the 5-byte length prefix).
    let msg_len = if body.len() >= 5 {
        u32::from_be_bytes([body[1], body[2], body[3], body[4]]) as u64
    } else {
        0
    };
    Some((grpc_status, msg_len))
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;
    use http::{Response, StatusCode};
    use std::net::SocketAddr;
    use tokio::net::TcpListener;

    #[derive(Clone, Copy)]
    struct ServerCfg {
        /// Advertised SETTINGS_INITIAL_WINDOW_SIZE (limits how much the client
        /// may send before a WINDOW_UPDATE is needed).
        window: u32,
        /// Delay before the server reads/drains the request body (forces client
        /// send-side flow-control stalls when `window` is small).
        read_delay_ms: u64,
        /// Delay before responding (keeps streams open to exercise concurrency).
        resp_delay_ms: u64,
        max_streams: u32,
    }

    fn frame(msg: &[u8]) -> Vec<u8> {
        let mut v = vec![0u8];
        v.extend_from_slice(&(msg.len() as u32).to_be_bytes());
        v.extend_from_slice(msg);
        v
    }

    async fn serve_stream(
        req: http::Request<h2::RecvStream>,
        mut resp: h2::server::SendResponse<Bytes>,
        cfg: ServerCfg,
    ) {
        let mut recv = req.into_body();
        if cfg.read_delay_ms > 0 {
            tokio::time::sleep(Duration::from_millis(cfg.read_delay_ms)).await;
        }
        // Drain the request body, releasing capacity so the client's window is
        // replenished (a no-op delay here keeps the round-trip flowing).
        while let Some(Ok(d)) = poll_fn(|cx| recv.poll_data(cx)).await {
            let n = d.len();
            let _ = recv.flow_control().release_capacity(n);
        }
        if cfg.resp_delay_ms > 0 {
            tokio::time::sleep(Duration::from_millis(cfg.resp_delay_ms)).await;
        }
        let response = Response::builder()
            .status(StatusCode::OK)
            .header(CONTENT_TYPE, "application/grpc")
            .body(())
            .unwrap();
        // Send a non-empty reply message so bytes_received is observable.
        let msg = frame(b"hello-from-server");
        if let Ok(mut ss) = resp.send_response(response, false) {
            let _ = ss.send_data(Bytes::from(msg), false);
            let mut trailers = http::HeaderMap::new();
            trailers.insert("grpc-status", HeaderValue::from_static("0"));
            let _ = ss.send_trailers(trailers);
        }
    }

    async fn serve(listener: TcpListener, cfg: ServerCfg) {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            let Ok(mut conn) = h2::server::Builder::new()
                .initial_window_size(cfg.window)
                .max_concurrent_streams(cfg.max_streams)
                .handshake::<_, Bytes>(stream)
                .await
            else {
                continue;
            };
            tokio::spawn(async move {
                while let Some(Ok((req, resp))) = conn.accept().await {
                    tokio::spawn(serve_stream(req, resp, cfg));
                }
            });
        }
    }

    async fn spawn_server(cfg: ServerCfg) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve(listener, cfg));
        addr
    }

    fn engine_for(addr: SocketAddr, payload_b64: Option<String>) -> GrpcH2Engine {
        GrpcH2Engine::new(
            &format!("grpc://{addr}"),
            vec![],
            ChaosEngine::default(),
            Some("pkg.Svc".into()),
            Some("Method".into()),
            payload_b64,
            None,
            None,
        )
        .unwrap()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn grpc_h2_round_trip_and_concurrency_snapshot() {
        let addr = spawn_server(ServerCfg {
            window: 65535,
            read_delay_ms: 0,
            resp_delay_ms: 0,
            max_streams: 100,
        })
        .await;
        let engine = engine_for(addr, Some("dGVzdA==".into())); // "test"
        // First RPC establishes the connection and round-trips a message.
        let metric = engine.execute_iteration("").await;
        assert_eq!(metric.status_code, 200);
        assert_eq!(metric.bytes_received, 17); // len("hello-from-server")
        let g = metric.grpc.expect("grpc sample present");
        assert!(g.active_streams >= 1);
        assert_eq!(g.window_exhaustion_events, 0);

        // Wait (bounded) for the driver to apply the server's SETTINGS, then the
        // negotiated SETTINGS_MAX_CONCURRENT_STREAMS is reflected and drives the
        // utilization ratio.
        let mut g2 = g;
        for _ in 0..50 {
            let m = engine.execute_iteration("").await;
            g2 = m.grpc.expect("grpc sample present");
            if g2.max_concurrent_streams != u64::MAX {
                break;
            }
        }
        assert_eq!(g2.max_concurrent_streams, 100);
        assert_eq!(g2.active_streams, 1);
        assert!((g2.utilization - 0.01).abs() < 1e-9);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn grpc_h2_active_streams_reflect_concurrency() {
        // Slow responses keep streams open so concurrent iterations overlap.
        let addr = spawn_server(ServerCfg {
            window: 65535,
            read_delay_ms: 0,
            resp_delay_ms: 250,
            max_streams: 100,
        })
        .await;
        let engine = std::sync::Arc::new(engine_for(addr, None));
        let e1 = engine.clone();
        let e2 = engine.clone();
        let e3 = engine.clone();
        let (a, b, c) = tokio::join!(
            e1.execute_iteration(""),
            e2.execute_iteration(""),
            e3.execute_iteration("")
        );
        let peak = [a, b, c]
            .iter()
            .filter_map(|m| m.grpc.as_ref())
            .map(|g| g.active_streams)
            .max()
            .unwrap_or(0);
        assert!(
            peak >= 2,
            "expected concurrent in-flight streams, got peak {peak}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn grpc_h2_detects_window_exhaustion_stall() {
        // Tiny advertised window + delayed server read forces a send stall.
        let addr = spawn_server(ServerCfg {
            window: 16,
            read_delay_ms: 60,
            resp_delay_ms: 0,
            max_streams: 100,
        })
        .await;
        let raw = vec![0xABu8; 1024];
        let b64 = base64::engine::general_purpose::STANDARD.encode(&raw);
        let engine = engine_for(addr, Some(b64));
        let metric = engine.execute_iteration("").await;

        assert_eq!(metric.status_code, 200);
        let g = metric.grpc.expect("grpc sample present");
        assert!(
            g.window_exhaustion_events >= 1,
            "expected at least one window-exhaustion stall, got {}",
            g.window_exhaustion_events
        );
        assert!(g.window_stall_us > 0);
        // Send credit is capped by the tiny advertised window.
        assert!(g.has_send_capacity && g.send_capacity_bytes <= 16);
    }
}
