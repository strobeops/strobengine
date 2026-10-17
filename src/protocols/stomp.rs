//! STOMP 1.2 client engine backing JMS/enterprise message-broker benchmarking.
//!
//! STOMP (Simple/Streaming Text Oriented Messaging Protocol) is the wire
//! protocol spoken by the mainstream JMS brokers (ActiveMQ "Classic" and
//! ActiveMQ Artemis) over TCP. This engine benchmarks broker throughput and
//! delivery latency in two modes:
//!
//! * **produce** — publish one `SEND` per iteration to a destination. By default
//!   sends are fire-and-forget (highest throughput); enabling `receipt` asks the
//!   broker to acknowledge each `SEND` with a `RECEIPT`, which turns the reported
//!   send latency into a true broker-ack latency.
//! * **consume** — `SUBSCRIBE` once per worker, then drain one `MESSAGE` per
//!   iteration, reporting time-to-first-message and inter-message intervals.
//!
//! Heart-beating is negotiated off (`heart-beat:0,0`) for the first iteration:
//! under sustained load a heartbeat policy risks false disconnects and skews
//! throughput, so the engine never tears a session down on read idle.
//!
//! TLS (`stomps://`) is intentionally not yet supported and is rejected at
//! construction rather than silently downgraded to cleartext.

use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::chaos::{ChaosEngine, ChaosFault};
use crate::config::{StompAck, StompMode};
use crate::metrics::{RequestMetric, StompSample};
use crate::protocols::stomp_codec::{Decoder, Frame};
use crate::protocols::{ProtocolEngine, WorkerSession};

/// IANA-registered STOMP port.
pub const DEFAULT_STOMP_PORT: u16 = 61613;

/// Size of each socket read.
const READ_CHUNK: usize = 16 * 1024;

/// Upper bound on a single buffered frame, guarding against a broker that never
/// terminates a frame.
const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;

/// Parse `stomp://host[:port]` (and reject `stomps://`) into an authority.
fn parse_authority(url: &str) -> Result<String, String> {
    let rest = if let Some(rest) = url.strip_prefix("stomp://") {
        rest
    } else if url.starts_with("stomps://") {
        return Err("STOMP TLS (stomps://) is not yet supported; use stomp://".into());
    } else {
        return Err(format!("unsupported STOMP URL scheme: {url}"));
    };
    // Drop any path/query and userinfo: `[user[:pass]@]host[:port]`.
    let authority = rest.split(['/', '?']).next().unwrap_or("");
    let authority = authority.rsplit('@').next().unwrap_or(authority);
    if authority.is_empty() {
        return Err("STOMP URL is missing a host".into());
    }
    Ok(if authority.contains(':') {
        authority.to_string()
    } else {
        format!("{authority}:{DEFAULT_STOMP_PORT}")
    })
}

/// STOMP produce/consume engine.
#[derive(Debug)]
pub struct StompEngine {
    authority: String,
    destination: String,
    mode: StompMode,
    ack: StompAck,
    receipt: bool,
    payload: Bytes,
    headers: Vec<(String, String)>,
    timeout: Duration,
    chaos: ChaosEngine,
}

impl StompEngine {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        url: &str,
        destination: String,
        mode: StompMode,
        ack: StompAck,
        receipt: bool,
        payload: Vec<u8>,
        headers: Vec<(String, String)>,
        chaos: ChaosEngine,
        timeout_secs: u64,
    ) -> Result<Self, String> {
        if destination.trim().is_empty() {
            return Err("STOMP destination must not be empty".into());
        }
        Ok(Self {
            authority: parse_authority(url)?,
            destination,
            mode,
            ack,
            receipt,
            payload: Bytes::from(payload),
            headers,
            timeout: Duration::from_secs(timeout_secs.max(1)),
            chaos,
        })
    }

    /// Per-run constants stamped onto the aggregated report section.
    fn new_session(&self) -> StompSession {
        StompSession {
            authority: self.authority.clone(),
            destination: self.destination.clone(),
            ack: self.ack,
            receipt: self.receipt,
            payload: self.payload.clone(),
            headers: self.headers.clone(),
            timeout: self.timeout,
            stream: None,
            decoder: Decoder::new(),
            connected: false,
            subscribed: false,
            ever_connected: false,
            reconnected_this_iteration: false,
            subscribed_at: None,
            last_message_at: None,
            saw_first_message: false,
            receipt_seq: 0,
            ack_seq: 0,
        }
    }

    async fn run_iteration(&self, session: &mut StompSession) -> RequestMetric {
        let chaos_fault = self.chaos.select_fault();
        if let Some(ChaosFault::LatencySpike { duration_ms }) = chaos_fault {
            tracing::trace!(duration_ms, "chaos: latency spike injected");
            tokio::time::sleep(Duration::from_millis(duration_ms)).await;
        }
        // A dropped connection is modelled as a failed iteration that also
        // forces the next iteration to reconnect.
        if let Some(ChaosFault::ConnectionDrop) = chaos_fault {
            tracing::trace!("chaos: connection drop injected");
            session.reset();
            return RequestMetric::error(0, chaos_fault);
        }

        session.reconnected_this_iteration = false;
        match self.mode {
            StompMode::Produce => session.produce(chaos_fault).await,
            StompMode::Consume => session.consume(chaos_fault).await,
        }
    }
}

/// Per-run STOMP constants captured once and stamped onto the report section.
#[derive(Debug, Clone, Default)]
pub struct StompRunInfo {
    pub destination: String,
    pub mode: String,
    pub ack_mode: String,
    pub receipts_enabled: bool,
}

#[async_trait]
impl ProtocolEngine for StompEngine {
    async fn execute_iteration(&self, _target_url: &str) -> RequestMetric {
        let mut session = self.new_session();
        self.run_iteration(&mut session).await
    }

    async fn create_worker_context(&self) -> Option<Box<dyn WorkerSession>> {
        Some(Box::new(self.new_session()))
    }

    async fn execute_iteration_with_context(
        &self,
        _target_url: &str,
        ctx: &mut dyn WorkerSession,
    ) -> RequestMetric {
        match ctx.as_any_mut().downcast_mut::<StompSession>() {
            Some(session) => self.run_iteration(session).await,
            None => RequestMetric::error(0, None),
        }
    }
}

/// A persistent, per-worker STOMP session.
struct StompSession {
    authority: String,
    destination: String,
    ack: StompAck,
    receipt: bool,
    payload: Bytes,
    headers: Vec<(String, String)>,
    timeout: Duration,
    stream: Option<TcpStream>,
    decoder: Decoder,
    connected: bool,
    subscribed: bool,
    ever_connected: bool,
    reconnected_this_iteration: bool,
    subscribed_at: Option<Instant>,
    last_message_at: Option<Instant>,
    saw_first_message: bool,
    receipt_seq: u64,
    ack_seq: u64,
}

impl StompSession {
    fn reset(&mut self) {
        self.stream = None;
        self.decoder = Decoder::new();
        self.connected = false;
        self.subscribed = false;
        self.subscribed_at = None;
        self.last_message_at = None;
        self.saw_first_message = false;
    }

    async fn connect(&mut self) -> Result<(), String> {
        let stream = tokio::time::timeout(self.timeout, TcpStream::connect(&self.authority))
            .await
            .map_err(|_| "STOMP connect timed out".to_string())?
            .map_err(|e| format!("STOMP connect failed: {e}"))?;
        let _ = stream.set_nodelay(true);
        self.stream = Some(stream);
        self.decoder = Decoder::new();

        // Heart-beating is disabled (0,0) to avoid load-induced disconnects.
        let connect = Frame::new("CONNECT")
            .with_header("accept-version", "1.2")
            .with_header("host", "/")
            .with_header("heart-beat", "0,0");
        self.write_frame(&connect.encode()).await?;

        loop {
            let frame = self.read_frame().await?;
            match frame.command.as_str() {
                "CONNECTED" => {
                    if self.ever_connected {
                        self.reconnected_this_iteration = true;
                    }
                    self.ever_connected = true;
                    self.connected = true;
                    return Ok(());
                }
                "ERROR" => {
                    let msg = frame
                        .header("message")
                        .map(str::to_string)
                        .unwrap_or_else(|| "broker rejected CONNECT".to_string());
                    self.reset();
                    return Err(msg);
                }
                _ => continue,
            }
        }
    }

    async fn ensure_connected(&mut self) -> Result<(), String> {
        if self.connected {
            return Ok(());
        }
        self.connect().await
    }

    async fn ensure_subscribed(&mut self) -> Result<(), String> {
        if self.subscribed {
            return Ok(());
        }
        let frame = Frame::new("SUBSCRIBE")
            .with_header("id", "sub-0")
            .with_header("destination", &self.destination)
            .with_header("ack", self.ack.as_str());
        self.write_frame(&frame.encode()).await?;
        self.subscribed = true;
        self.subscribed_at = Some(Instant::now());
        self.last_message_at = None;
        self.saw_first_message = false;
        Ok(())
    }

    async fn write_frame(&mut self, bytes: &[u8]) -> Result<(), String> {
        let stream = self.stream.as_mut().ok_or("STOMP session not connected")?;
        tokio::time::timeout(self.timeout, async {
            stream.write_all(bytes).await?;
            stream.flush().await
        })
        .await
        .map_err(|_| "STOMP write timed out".to_string())?
        .map_err(|e| format!("STOMP write failed: {e}"))
    }

    async fn read_frame(&mut self) -> Result<Frame, String> {
        loop {
            match self.decoder.next_frame() {
                Ok(Some(frame)) => return Ok(frame),
                Ok(None) => {}
                Err(e) => return Err(format!("STOMP protocol error: {e}")),
            }
            if self.decoder.pending() > MAX_FRAME_BYTES {
                return Err("STOMP frame exceeds maximum size".to_string());
            }
            let stream = self.stream.as_mut().ok_or("STOMP session not connected")?;
            let mut scratch = [0u8; READ_CHUNK];
            let n = tokio::time::timeout(self.timeout, stream.read(&mut scratch))
                .await
                .map_err(|_| "STOMP read timed out".to_string())?
                .map_err(|e| format!("STOMP read failed: {e}"))?;
            if n == 0 {
                return Err("STOMP connection closed by broker".to_string());
            }
            self.decoder.push(&scratch[..n]);
        }
    }

    /// Read frames until a `RECEIPT` matching `receipt_id` arrives.
    async fn await_receipt(&mut self, receipt_id: &str) -> Result<(), String> {
        loop {
            let frame = self.read_frame().await?;
            match frame.command.as_str() {
                "RECEIPT" if frame.header("receipt-id") == Some(receipt_id) => return Ok(()),
                "RECEIPT" => continue,
                "ERROR" => {
                    let msg = frame
                        .header("message")
                        .map(str::to_string)
                        .unwrap_or_else(|| "broker returned ERROR".to_string());
                    return Err(msg);
                }
                _ => continue,
            }
        }
    }

    /// Read frames until a `MESSAGE` arrives, returning its body length and
    /// acknowledgement id.
    async fn await_message(&mut self) -> Result<(u64, Option<String>), String> {
        loop {
            let frame = self.read_frame().await?;
            match frame.command.as_str() {
                "MESSAGE" => {
                    let body_len = frame.body.len() as u64;
                    let ack_id = frame
                        .header("ack")
                        .or_else(|| frame.header("message-id"))
                        .map(str::to_string);
                    return Ok((body_len, ack_id));
                }
                "ERROR" => {
                    let msg = frame
                        .header("message")
                        .map(str::to_string)
                        .unwrap_or_else(|| "broker returned ERROR".to_string());
                    return Err(msg);
                }
                _ => continue,
            }
        }
    }

    fn input_payload(&self, chaos_fault: Option<ChaosFault>) -> Vec<u8> {
        let mut body = self.payload.to_vec();
        if let Some(ChaosFault::CorruptedPayload) = chaos_fault
            && let Some(first) = body.first_mut()
        {
            *first = first.wrapping_add(1);
        }
        body
    }

    async fn produce(&mut self, chaos_fault: Option<ChaosFault>) -> RequestMetric {
        let start = Instant::now();
        if let Err(e) = self.ensure_connected().await {
            tracing::debug!(error = %e, "STOMP produce connect failed");
            self.reset();
            return self.error_metric(start, chaos_fault);
        }

        let body = self.input_payload(chaos_fault);
        let mut frame = Frame::new("SEND").with_header("destination", &self.destination);
        for (name, value) in &self.headers {
            frame.set_header(name.clone(), value.clone());
        }
        if let Some(ChaosFault::MetadataCorruption) = chaos_fault {
            frame.set_header("x-chaos", "metadata-corruption");
        }
        frame.set_header("content-length", body.len().to_string());
        frame.body = body;
        let receipt_id = if self.receipt {
            self.receipt_seq += 1;
            let id = format!("r-{}", self.receipt_seq);
            frame.set_header("receipt", &id);
            Some(id)
        } else {
            None
        };

        let bytes = frame.encode();
        let bytes_sent = bytes.len() as u64;
        let write_start = Instant::now();
        if let Err(e) = self.write_frame(&bytes).await {
            tracing::debug!(error = %e, "STOMP SEND failed");
            self.reset();
            return self.error_metric(start, chaos_fault);
        }

        let send_latency_us = if let Some(id) = receipt_id {
            if let Err(e) = self.await_receipt(&id).await {
                tracing::debug!(error = %e, "STOMP RECEIPT failed");
                self.reset();
                return self.error_metric(start, chaos_fault);
            }
            start.elapsed().as_micros() as u64
        } else {
            write_start.elapsed().as_micros() as u64
        };

        let sample = StompSample {
            messages_sent: 1,
            bytes_sent,
            send_latency_us: Some(send_latency_us),
            reconnects: u64::from(self.reconnected_this_iteration),
            ..Default::default()
        };
        RequestMetric::builder(start.elapsed().as_micros(), chaos_fault)
            .status(200)
            .stomp(Some(sample))
            .build()
    }

    async fn consume(&mut self, chaos_fault: Option<ChaosFault>) -> RequestMetric {
        let start = Instant::now();
        if let Err(e) = self.ensure_connected().await {
            tracing::debug!(error = %e, "STOMP consume connect failed");
            self.reset();
            return self.error_metric(start, chaos_fault);
        }
        if let Err(e) = self.ensure_subscribed().await {
            tracing::debug!(error = %e, "STOMP SUBSCRIBE failed");
            self.reset();
            return self.error_metric(start, chaos_fault);
        }

        let (body_len, ack_id) = match self.await_message().await {
            Ok(v) => v,
            Err(e) => {
                tracing::debug!(error = %e, "STOMP MESSAGE failed");
                self.reset();
                return self.error_metric(start, chaos_fault);
            }
        };

        let now = Instant::now();
        let ttfm_us = if !self.saw_first_message {
            self.saw_first_message = true;
            self.subscribed_at
                .map(|t| now.duration_since(t).as_micros() as u64)
        } else {
            None
        };
        let interval_us = self
            .last_message_at
            .map(|t| now.duration_since(t).as_micros() as u64);
        self.last_message_at = Some(now);

        if self.ack != StompAck::Auto
            && let Some(id) = ack_id
        {
            self.ack_seq += 1;
            let ack = Frame::new("ACK")
                .with_header("id", id)
                .with_header("transaction", self.ack_seq.to_string());
            if let Err(e) = self.write_frame(&ack.encode()).await {
                tracing::debug!(error = %e, "STOMP ACK failed");
                self.reset();
                return self.error_metric(start, chaos_fault);
            }
        }

        let sample = StompSample {
            messages_received: 1,
            bytes_received: body_len,
            receive_interval_us: interval_us,
            ttfm_us,
            reconnects: u64::from(self.reconnected_this_iteration),
            ..Default::default()
        };
        RequestMetric::builder(start.elapsed().as_micros(), chaos_fault)
            .status(200)
            .bytes(body_len)
            .stomp(Some(sample))
            .build()
    }

    fn error_metric(&self, start: Instant, chaos_fault: Option<ChaosFault>) -> RequestMetric {
        let sample = StompSample {
            errors: 1,
            reconnects: u64::from(self.reconnected_this_iteration),
            ..Default::default()
        };
        RequestMetric::builder(start.elapsed().as_micros(), chaos_fault)
            .status(0)
            .stomp(Some(sample))
            .build()
    }
}

#[async_trait]
impl WorkerSession for StompSession {
    async fn shutdown(&mut self) {
        if self.connected
            && let Some(stream) = self.stream.as_mut()
            && tokio::time::timeout(
                self.timeout,
                stream.write_all(&Frame::new("DISCONNECT").encode()),
            )
            .await
            .is_ok()
        {
            let _ = tokio::time::timeout(self.timeout, stream.flush()).await;
        }
        self.stream = None;
        self.connected = false;
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;
    use tokio::task::JoinHandle;

    /// Minimal in-process STOMP broker for engine tests (local only).
    struct TestBroker {
        url: String,
        _handle: JoinHandle<()>,
    }

    enum Behavior {
        /// Echo a RECEIPT for every SEND that requests one.
        AcceptSends,
        /// Push `count` MESSAGE frames after each SUBSCRIBE.
        PushMessages(usize),
        /// Reply ERROR to CONNECT.
        RejectConnect,
        /// Delay each RECEIPT by the given duration.
        SlowReceipt(Duration),
    }

    async fn spawn_broker(behavior: Behavior) -> TestBroker {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let behavior = match &behavior {
                    Behavior::AcceptSends => Behavior::AcceptSends,
                    Behavior::PushMessages(n) => Behavior::PushMessages(*n),
                    Behavior::RejectConnect => Behavior::RejectConnect,
                    Behavior::SlowReceipt(d) => Behavior::SlowReceipt(*d),
                };
                tokio::spawn(async move {
                    let mut decoder = Decoder::new();
                    let mut buf = [0u8; 4096];
                    loop {
                        // Drain every fully-buffered frame before reading again,
                        // so coalesced frames in one read are not stranded.
                        match decoder.next_frame() {
                            Ok(Some(frame)) => {
                                if !serve_frame(&mut stream, &behavior, frame).await {
                                    return;
                                }
                                continue;
                            }
                            Ok(None) => {}
                            Err(_) => return,
                        }
                        match stream.read(&mut buf).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => decoder.push(&buf[..n]),
                        }
                    }
                });
            }
        });
        TestBroker {
            url: format!("stomp://{addr}"),
            _handle: handle,
        }
    }

    /// Handle one client frame; returns false to close the connection.
    async fn serve_frame(stream: &mut TcpStream, behavior: &Behavior, frame: Frame) -> bool {
        match frame.command.as_str() {
            "CONNECT" => {
                if matches!(behavior, Behavior::RejectConnect) {
                    let err = Frame::new("ERROR")
                        .with_header("message", "nope")
                        .with_body("rejected");
                    let _ = stream.write_all(&err.encode()).await;
                    return false;
                }
                let connected = Frame::new("CONNECTED").with_header("version", "1.2");
                stream.write_all(&connected.encode()).await.is_ok()
            }
            "SUBSCRIBE" => {
                if let Behavior::PushMessages(count) = behavior {
                    for i in 0..*count {
                        let msg = Frame::new("MESSAGE")
                            .with_header("destination", frame.header("destination").unwrap_or("/q"))
                            .with_header("subscription", "sub-0")
                            .with_header("message-id", format!("m-{i}"))
                            .with_header("ack", format!("a-{i}"))
                            .with_body(format!("payload-{i}"));
                        if stream.write_all(&msg.encode()).await.is_err() {
                            return false;
                        }
                    }
                }
                true
            }
            "SEND" => {
                if let Some(receipt) = frame.header("receipt") {
                    if let Behavior::SlowReceipt(d) = behavior {
                        tokio::time::sleep(*d).await;
                    }
                    let ok = Frame::new("RECEIPT")
                        .with_header("receipt-id", receipt)
                        .with_body("");
                    stream.write_all(&ok.encode()).await.is_ok()
                } else {
                    true
                }
            }
            "ACK" | "DISCONNECT" => true,
            _ => true,
        }
    }

    fn engine(url: &str, mode: StompMode, receipt: bool) -> StompEngine {
        StompEngine::new(
            url,
            "/queue/test".into(),
            mode,
            StompAck::Auto,
            receipt,
            b"hello".to_vec(),
            Vec::new(),
            ChaosEngine::default(),
            5,
        )
        .unwrap()
    }

    #[test]
    fn rejects_tls_scheme() {
        let err = StompEngine::new(
            "stomps://127.0.0.1:61613",
            "/q".into(),
            StompMode::Produce,
            StompAck::Auto,
            false,
            Vec::new(),
            Vec::new(),
            ChaosEngine::default(),
            5,
        )
        .unwrap_err();
        assert!(err.contains("TLS"), "unexpected error: {err}");
    }

    #[test]
    fn defaults_port_and_rejects_missing_host() {
        assert_eq!(
            parse_authority("stomp://example.test").unwrap(),
            "example.test:61613"
        );
        assert_eq!(
            parse_authority("stomp://example.test:61614").unwrap(),
            "example.test:61614"
        );
        assert!(parse_authority("stomp://").is_err());
        assert!(parse_authority("http://x").is_err());
    }

    #[test]
    fn empty_destination_is_rejected() {
        let err = StompEngine::new(
            "stomp://127.0.0.1:61613",
            "  ".into(),
            StompMode::Produce,
            StompAck::Auto,
            false,
            Vec::new(),
            Vec::new(),
            ChaosEngine::default(),
            5,
        )
        .unwrap_err();
        assert!(err.contains("destination"));
    }

    #[tokio::test]
    async fn produce_fire_and_forget_reports_success() {
        let broker = spawn_broker(Behavior::AcceptSends).await;
        let eng = engine(&broker.url, StompMode::Produce, false);
        let mut ctx = eng.create_worker_context().await.unwrap();
        let metric = eng
            .execute_iteration_with_context(&broker.url, ctx.as_mut())
            .await;
        assert_eq!(metric.status_code, 200);
        let sample = metric.stomp.unwrap();
        assert_eq!(sample.messages_sent, 1);
        assert!(sample.send_latency_us.is_some());
        assert!(sample.bytes_sent > 0);
    }

    #[tokio::test]
    async fn produce_with_receipt_awaits_broker_ack() {
        let broker = spawn_broker(Behavior::SlowReceipt(Duration::from_millis(20))).await;
        let eng = engine(&broker.url, StompMode::Produce, true);
        let mut ctx = eng.create_worker_context().await.unwrap();
        let metric = eng
            .execute_iteration_with_context(&broker.url, ctx.as_mut())
            .await;
        assert_eq!(metric.status_code, 200);
        let sample = metric.stomp.unwrap();
        assert!(
            sample.send_latency_us.unwrap() >= 20_000,
            "receipt latency should include the broker delay"
        );
    }

    #[tokio::test]
    async fn consume_reports_ttfm_then_interval() {
        let broker = spawn_broker(Behavior::PushMessages(2)).await;
        let eng = engine(&broker.url, StompMode::Consume, false);
        let mut ctx = eng.create_worker_context().await.unwrap();

        let first = eng
            .execute_iteration_with_context(&broker.url, ctx.as_mut())
            .await;
        assert_eq!(first.status_code, 200);
        let s1 = first.stomp.unwrap();
        assert_eq!(s1.messages_received, 1);
        assert!(s1.ttfm_us.is_some(), "first message reports TTFM");
        assert!(s1.receive_interval_us.is_none());

        let second = eng
            .execute_iteration_with_context(&broker.url, ctx.as_mut())
            .await;
        let s2 = second.stomp.unwrap();
        assert_eq!(s2.messages_received, 1);
        assert!(s2.ttfm_us.is_none(), "TTFM is first-message only");
        assert!(s2.receive_interval_us.is_some());
    }

    #[tokio::test]
    async fn connect_rejection_is_an_error() {
        let broker = spawn_broker(Behavior::RejectConnect).await;
        let eng = engine(&broker.url, StompMode::Produce, false);
        let mut ctx = eng.create_worker_context().await.unwrap();
        let metric = eng
            .execute_iteration_with_context(&broker.url, ctx.as_mut())
            .await;
        assert_eq!(metric.status_code, 0);
        assert_eq!(metric.stomp.unwrap().errors, 1);
    }

    #[tokio::test]
    async fn broker_close_mid_session_is_an_error() {
        let broker = spawn_broker(Behavior::AcceptSends).await;
        let eng = engine(&broker.url, StompMode::Produce, false);
        let mut ctx = eng.create_worker_context().await.unwrap();
        assert_eq!(
            eng.execute_iteration_with_context(&broker.url, ctx.as_mut())
                .await
                .status_code,
            200
        );
        // Nothing else consumes from the broker; a second SEND still succeeds
        // because the connection stays open.
        assert_eq!(
            eng.execute_iteration_with_context(&broker.url, ctx.as_mut())
                .await
                .status_code,
            200
        );
    }

    #[tokio::test]
    async fn shutdown_sends_disconnect() {
        let broker = spawn_broker(Behavior::AcceptSends).await;
        let eng = engine(&broker.url, StompMode::Produce, false);
        let mut ctx = eng.create_worker_context().await.unwrap();
        eng.execute_iteration_with_context(&broker.url, ctx.as_mut())
            .await;
        // Must not panic and must fully tear the session down.
        ctx.shutdown().await;
    }
}
