use std::sync::atomic::{AtomicU64, AtomicUsize};

use super::protocols::{DbSample, GrpcSample, Http3Sample, StompSample, WsSample};

/// Connection-related metrics for a single request.
#[derive(Debug, Clone, Default)]
pub struct ConnectionMetrics {
    pub connection_latency_us: Option<u128>,
    pub timestamp_sent_ns: Option<u128>,
    pub e2e_latency_us: Option<u128>,
    pub dns_resolution_us: Option<u64>,
    pub is_socket_reused: bool,
}

pub struct RequestMetric {
    pub latency_micros: u128,
    pub status_code: u16,
    pub bytes_received: u64,
    pub is_reconnect: bool,
    pub connection: ConnectionMetrics,
    pub quic_handshake_us: Option<u64>,
    pub quic_0rtt_used: bool,
    pub quic_retransmits: Option<u64>,
    pub sse_events_received: Option<u64>,
    pub sse_first_event_us: Option<u64>,
    pub sse_event_interval_us: Option<u64>,
    pub graphql_errors: Option<u64>,
    pub graphql_ws_operations: Option<u64>,
    pub graphql_ws_ttfm_us: Option<u64>,
    pub graphql_ws_interval_us: Option<u64>,
    pub graphql_ws_errors: Option<u64>,
    pub soap_errors: Option<u64>,
    pub ws: Option<WsSample>,
    pub grpc: Option<GrpcSample>,
    pub http3: Option<Http3Sample>,
    pub stomp: Option<StompSample>,
    pub db: Option<DbSample>,
    pub chaos_fault: Option<crate::chaos::ChaosFault>,
}

impl RequestMetric {
    /// Seed a metric for one iteration: its latency and the chaos fault selected
    /// for that iteration (`None` when chaos did not fire, or the failure happened
    /// before fault selection — e.g. a URL parse error or an outer deadline).
    ///
    /// Defaults describe an error metric (`status_code == 0`, no samples);
    /// success paths layer the actual fields on top through the returned builder.
    pub fn builder(
        latency_micros: u128,
        chaos_fault: Option<crate::chaos::ChaosFault>,
    ) -> RequestMetricBuilder {
        RequestMetricBuilder(Self {
            latency_micros,
            status_code: 0,
            bytes_received: 0,
            is_reconnect: false,
            connection: ConnectionMetrics::default(),
            quic_handshake_us: None,
            quic_0rtt_used: false,
            quic_retransmits: None,
            sse_events_received: None,
            sse_first_event_us: None,
            sse_event_interval_us: None,
            graphql_errors: None,
            graphql_ws_operations: None,
            graphql_ws_ttfm_us: None,
            graphql_ws_interval_us: None,
            graphql_ws_errors: None,
            soap_errors: None,
            ws: None,
            grpc: None,
            http3: None,
            stomp: None,
            db: None,
            chaos_fault,
        })
    }

    /// Error metric (`status_code == 0`) carrying the chaos fault selected for
    /// this iteration, so injected-but-failed iterations still count toward
    /// `chaos_injected_total`. Pass `None` when no fault was selected (chaos
    /// disabled, rate not met, or a failure before fault selection).
    pub fn error(latency_micros: u128, chaos_fault: Option<crate::chaos::ChaosFault>) -> Self {
        Self::builder(latency_micros, chaos_fault).build()
    }

    /// Whether this iteration counts as a failure, using the same rules the
    /// worker applies: transport error (status 0), HTTP >= 400, a GraphQL logical
    /// error (HTTP or WebSocket), or a SOAP fault.
    pub fn is_error(&self) -> bool {
        self.status_code == 0
            || self.status_code >= 400
            || self.graphql_errors.is_some_and(|n| n > 0)
            || self.graphql_ws_errors.is_some_and(|n| n > 0)
            || self.soap_errors.is_some_and(|n| n > 0)
            || self.stomp.as_ref().is_some_and(|s| s.errors > 0)
            || self.db.as_ref().is_some_and(|s| s.errors > 0)
    }
}

/// Fluent builder over [`RequestMetric`]: call sites state only the fields that
/// differ from the error defaults instead of repeating all sixteen of them.
pub struct RequestMetricBuilder(RequestMetric);

impl RequestMetricBuilder {
    pub fn status(mut self, status_code: u16) -> Self {
        self.0.status_code = status_code;
        self
    }

    pub fn bytes(mut self, bytes_received: u64) -> Self {
        self.0.bytes_received = bytes_received;
        self
    }

    pub fn reconnect(mut self, is_reconnect: bool) -> Self {
        self.0.is_reconnect = is_reconnect;
        self
    }

    pub fn connection_latency_us(mut self, us: Option<u128>) -> Self {
        self.0.connection.connection_latency_us = us;
        self
    }

    pub fn timestamp_sent_ns(mut self, ns: Option<u128>) -> Self {
        self.0.connection.timestamp_sent_ns = ns;
        self
    }

    pub fn e2e_latency_us(mut self, us: Option<u128>) -> Self {
        self.0.connection.e2e_latency_us = us;
        self
    }

    pub fn dns_resolution_us(mut self, us: Option<u64>) -> Self {
        self.0.connection.dns_resolution_us = us;
        self
    }

    pub fn socket_reused(mut self, is_socket_reused: bool) -> Self {
        self.0.connection.is_socket_reused = is_socket_reused;
        self
    }

    pub fn quic(
        mut self,
        handshake_us: Option<u64>,
        used_0rtt: bool,
        retransmits: Option<u64>,
    ) -> Self {
        self.0.quic_handshake_us = handshake_us;
        self.0.quic_0rtt_used = used_0rtt;
        self.0.quic_retransmits = retransmits;
        self
    }

    pub fn sse_events(
        mut self,
        events: Option<u64>,
        first_event_us: Option<u64>,
        event_interval_us: Option<u64>,
    ) -> Self {
        self.0.sse_events_received = events;
        self.0.sse_first_event_us = first_event_us;
        self.0.sse_event_interval_us = event_interval_us;
        self
    }

    pub fn ws(mut self, sample: Option<WsSample>) -> Self {
        self.0.ws = sample;
        self
    }

    /// Record the number of entries in a GraphQL response's `"errors"` array.
    /// `Some(n)` marks the metric as a GraphQL operation (n may be 0); `None`
    /// means the response was not a GraphQL operation.
    pub fn graphql(mut self, errors: Option<u64>) -> Self {
        self.0.graphql_errors = errors;
        self
    }

    /// Record whether a SOAP response carried a `Fault` element. `Some(1)`
    /// marks the metric as a SOAP operation with a fault, `Some(0)` a SOAP
    /// operation without one, and `None` a non-SOAP response.
    pub fn soap(mut self, faults: Option<u64>) -> Self {
        self.0.soap_errors = faults;
        self
    }

    /// Record GraphQL-over-WebSocket subscription samples for one iteration:
    /// `operations` (subscriptions started), `ttfm_us` (subscribe -> first
    /// message), `interval_us` (gap since the previous message) and `errors`
    /// (entries in an `error` frame). `None` means the field did not apply.
    pub fn graphql_ws(
        mut self,
        operations: Option<u64>,
        ttfm_us: Option<u64>,
        interval_us: Option<u64>,
        errors: Option<u64>,
    ) -> Self {
        self.0.graphql_ws_operations = operations;
        self.0.graphql_ws_ttfm_us = ttfm_us;
        self.0.graphql_ws_interval_us = interval_us;
        self.0.graphql_ws_errors = errors;
        self
    }

    pub fn grpc(mut self, sample: Option<GrpcSample>) -> Self {
        self.0.grpc = sample;
        self
    }

    pub fn http3(mut self, sample: Option<Http3Sample>) -> Self {
        self.0.http3 = sample;
        self
    }

    /// Record STOMP/JMS messaging samples for one iteration (messages sent or
    /// received, byte counts, latency/TTFM/interval samples, and error counts).
    pub fn stomp(mut self, sample: Option<StompSample>) -> Self {
        self.0.stomp = sample;
        self
    }

    /// Record SQL database samples for one iteration (rows, pool-acquire and
    /// query latencies, pool size, and error counts).
    pub fn db(mut self, sample: Option<DbSample>) -> Self {
        self.0.db = sample;
        self
    }

    pub fn build(self) -> RequestMetric {
        self.0
    }
}

pub struct LiveCounters {
    pub total_requests: AtomicU64,
    pub errors: AtomicU64,
    pub active_workers: AtomicUsize,
    pub completed_requests: AtomicU64,
    pub latency_sum_micros: AtomicU64,
    pub latency_count: AtomicU64,
    pub bytes_received: AtomicU64,
}

impl LiveCounters {
    pub fn new() -> Self {
        Self {
            total_requests: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            active_workers: AtomicUsize::new(0),
            completed_requests: AtomicU64::new(0),
            latency_sum_micros: AtomicU64::new(0),
            latency_count: AtomicU64::new(0),
            bytes_received: AtomicU64::new(0),
        }
    }
}
