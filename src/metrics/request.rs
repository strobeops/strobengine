use std::sync::atomic::{AtomicU64, AtomicUsize};

use super::protocols::{
    DbSample, GraphqlSample, GraphqlWsSample, GrpcSample, Http3Sample, ProtocolSample, QuicSample,
    SoapSample, SseSample, StompSample, WsSample,
};

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
    pub protocol: Option<ProtocolSample>,
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
            protocol: None,
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

    /// Whether this iteration counts as a failure: transport error (status 0),
    /// HTTP >= 400, or a protocol-level error (GraphQL, GraphQL-WS, SOAP,
    /// STOMP, or database).
    pub fn is_error(&self) -> bool {
        if self.status_code == 0 || self.status_code >= 400 {
            return true;
        }
        match &self.protocol {
            Some(ProtocolSample::Graphql(s)) => s.errors > 0,
            Some(ProtocolSample::GraphqlWs(s)) => s.errors.is_some_and(|n| n > 0),
            Some(ProtocolSample::Soap(s)) => s.faults > 0,
            Some(ProtocolSample::Stomp(s)) => s.errors > 0,
            Some(ProtocolSample::Db(s)) => s.errors > 0,
            _ => false,
        }
    }

    pub fn ws(&self) -> Option<&WsSample> {
        match &self.protocol {
            Some(ProtocolSample::Ws(sample)) => Some(sample),
            _ => None,
        }
    }

    pub fn grpc(&self) -> Option<&GrpcSample> {
        match &self.protocol {
            Some(ProtocolSample::Grpc(sample)) => Some(sample),
            _ => None,
        }
    }

    pub fn http3(&self) -> Option<&Http3Sample> {
        match &self.protocol {
            Some(ProtocolSample::Http3 {
                http3: Some(sample),
                ..
            }) => Some(sample),
            _ => None,
        }
    }

    pub fn quic(&self) -> Option<&QuicSample> {
        match &self.protocol {
            Some(ProtocolSample::Http3 {
                quic: Some(sample), ..
            }) => Some(sample),
            _ => None,
        }
    }

    pub fn sse(&self) -> Option<&SseSample> {
        match &self.protocol {
            Some(ProtocolSample::Sse(sample)) => Some(sample),
            _ => None,
        }
    }

    pub fn graphql(&self) -> Option<&GraphqlSample> {
        match &self.protocol {
            Some(ProtocolSample::Graphql(sample)) => Some(sample),
            _ => None,
        }
    }

    pub fn graphql_ws(&self) -> Option<&GraphqlWsSample> {
        match &self.protocol {
            Some(ProtocolSample::GraphqlWs(sample)) => Some(sample),
            _ => None,
        }
    }

    pub fn soap(&self) -> Option<&SoapSample> {
        match &self.protocol {
            Some(ProtocolSample::Soap(sample)) => Some(sample),
            _ => None,
        }
    }

    pub fn stomp(&self) -> Option<&StompSample> {
        match &self.protocol {
            Some(ProtocolSample::Stomp(sample)) => Some(sample),
            _ => None,
        }
    }

    pub fn db(&self) -> Option<&DbSample> {
        match &self.protocol {
            Some(ProtocolSample::Db(sample)) => Some(sample),
            _ => None,
        }
    }
}

/// Fluent builder over [`RequestMetric`]: call sites state only the fields that
/// differ from the error defaults.
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
        let sample = QuicSample {
            handshake_us,
            used_0rtt,
            retransmits,
        };
        match &mut self.0.protocol {
            Some(ProtocolSample::Http3 { quic, .. }) => *quic = Some(sample),
            _ => {
                self.0.protocol = Some(ProtocolSample::Http3 {
                    http3: None,
                    quic: Some(sample),
                });
            }
        }
        self
    }

    pub fn sse_events(
        mut self,
        events: Option<u64>,
        first_event_us: Option<u64>,
        event_interval_us: Option<u64>,
    ) -> Self {
        self.0.protocol = Some(ProtocolSample::Sse(SseSample {
            events_received: events,
            first_event_us,
            event_interval_us,
        }));
        self
    }

    pub fn ws(mut self, sample: Option<WsSample>) -> Self {
        if let Some(sample) = sample {
            self.0.protocol = Some(ProtocolSample::Ws(sample));
        }
        self
    }

    /// Record the number of entries in a GraphQL response's `"errors"` array.
    /// `Some(n)` marks the metric as a GraphQL operation (n may be 0); `None`
    /// means the response was not a GraphQL operation.
    pub fn graphql(mut self, errors: Option<u64>) -> Self {
        if let Some(errors) = errors {
            self.0.protocol = Some(ProtocolSample::Graphql(GraphqlSample { errors }));
        }
        self
    }

    /// Record whether a SOAP response carried a `Fault` element. `Some(1)`
    /// marks the metric as a SOAP operation with a fault, `Some(0)` a SOAP
    /// operation without one, and `None` a non-SOAP response.
    pub fn soap(mut self, faults: Option<u64>) -> Self {
        if let Some(faults) = faults {
            self.0.protocol = Some(ProtocolSample::Soap(SoapSample { faults }));
        }
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
        self.0.protocol = Some(ProtocolSample::GraphqlWs(GraphqlWsSample {
            operations,
            ttfm_us,
            interval_us,
            errors,
        }));
        self
    }

    pub fn grpc(mut self, sample: Option<GrpcSample>) -> Self {
        if let Some(sample) = sample {
            self.0.protocol = Some(ProtocolSample::Grpc(sample));
        }
        self
    }

    pub fn http3(mut self, sample: Option<Http3Sample>) -> Self {
        if let Some(sample) = sample {
            match &mut self.0.protocol {
                Some(ProtocolSample::Http3 { http3, .. }) => *http3 = Some(sample),
                _ => {
                    self.0.protocol = Some(ProtocolSample::Http3 {
                        http3: Some(sample),
                        quic: None,
                    });
                }
            }
        }
        self
    }

    /// Record STOMP/JMS messaging samples for one iteration (messages sent or
    /// received, byte counts, latency/TTFM/interval samples, and error counts).
    pub fn stomp(mut self, sample: Option<StompSample>) -> Self {
        if let Some(sample) = sample {
            self.0.protocol = Some(ProtocolSample::Stomp(sample));
        }
        self
    }

    /// Record SQL database samples for one iteration (rows, pool-acquire and
    /// query latencies, pool size, and error counts).
    pub fn db(mut self, sample: Option<DbSample>) -> Self {
        if let Some(sample) = sample {
            self.0.protocol = Some(ProtocolSample::Db(sample));
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn with_protocol(protocol: ProtocolSample) -> RequestMetric {
        let mut metric = RequestMetric::builder(1, None).status(200).build();
        metric.protocol = Some(protocol);
        metric
    }

    #[test]
    fn is_error_when_protocol_is_none() {
        assert!(RequestMetric::error(1, None).is_error());
        assert!(
            !RequestMetric::builder(1, None)
                .status(200)
                .build()
                .is_error()
        );
        assert!(
            RequestMetric::builder(1, None)
                .status(400)
                .build()
                .is_error()
        );
    }

    #[test]
    fn is_error_delegates_to_protocol_sample() {
        let cases = [
            (ProtocolSample::Graphql(GraphqlSample { errors: 1 }), true),
            (ProtocolSample::Graphql(GraphqlSample { errors: 0 }), false),
            (
                ProtocolSample::GraphqlWs(GraphqlWsSample {
                    errors: Some(1),
                    ..Default::default()
                }),
                true,
            ),
            (
                ProtocolSample::GraphqlWs(GraphqlWsSample {
                    errors: None,
                    ..Default::default()
                }),
                false,
            ),
            (ProtocolSample::Soap(SoapSample { faults: 1 }), true),
            (ProtocolSample::Soap(SoapSample { faults: 0 }), false),
            (
                ProtocolSample::Stomp(StompSample {
                    errors: 1,
                    ..Default::default()
                }),
                true,
            ),
            (
                ProtocolSample::Db(DbSample {
                    errors: 1,
                    ..Default::default()
                }),
                true,
            ),
            (ProtocolSample::Ws(WsSample::default()), false),
            (
                ProtocolSample::Http3 {
                    http3: Some(Http3Sample::default()),
                    quic: None,
                },
                false,
            ),
            (ProtocolSample::Sse(SseSample::default()), false),
        ];

        for (protocol, expected) in cases {
            assert_eq!(with_protocol(protocol).is_error(), expected);
        }
    }

    #[test]
    fn http3_builder_merges_quic_and_sample() {
        let http3 = Http3Sample {
            cwnd_bytes: 1200,
            ..Default::default()
        };
        let quic_first = RequestMetric::builder(1, None)
            .quic(Some(10), true, Some(2))
            .http3(Some(http3.clone()))
            .build();
        let http3_first = RequestMetric::builder(1, None)
            .http3(Some(http3))
            .quic(Some(10), true, Some(2))
            .build();

        for metric in [quic_first, http3_first] {
            assert_eq!(metric.http3().unwrap().cwnd_bytes, 1200);
            let quic = metric.quic().unwrap();
            assert_eq!(quic.handshake_us, Some(10));
            assert!(quic.used_0rtt);
            assert_eq!(quic.retransmits, Some(2));
        }
    }

    #[test]
    fn request_metric_stays_compact() {
        let size = std::mem::size_of::<RequestMetric>();
        assert!(
            size <= 384,
            "RequestMetric grew to {size} bytes; consider boxing the largest protocol sample"
        );
    }
}
