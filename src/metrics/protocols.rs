use hdrhistogram::Histogram;
use pyo3::prelude::*;

use super::helpers::fold_percentiles;

#[pyclass(skip_from_py_object)]
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct QuicMetrics {
    #[pyo3(get)]
    pub zero_rtt_accepted_count: u64,
    #[pyo3(get)]
    pub retransmissions: u64,
    #[pyo3(get)]
    pub avg_handshake_ms: Option<f64>,
}

#[pyclass(skip_from_py_object)]
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct SseMetrics {
    #[pyo3(get)]
    pub total_events_received: u64,
    #[pyo3(get)]
    pub avg_ttfb_ms: Option<f64>,
}

/// Aggregated GraphQL operation telemetry.
///
/// GraphQL reports logical failures in an `"errors"` array on an otherwise
/// successful HTTP 200 response; `error_responses_total` and `errors_total`
/// expose those, while the run's `status_codes` stays truthful.
#[pyclass(skip_from_py_object)]
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct GraphqlMetrics {
    #[pyo3(get)]
    pub operations_total: u64,
    #[pyo3(get)]
    pub error_responses_total: u64,
    #[pyo3(get)]
    pub errors_total: u64,
    #[pyo3(get)]
    pub operation_type: String,
}

/// Aggregated GraphQL-over-WebSocket subscription telemetry.
///
/// TTFM (time-to-first-message) is measured from the `subscribe` frame to the
/// first `next` frame; the event interval is the gap between consecutive `next`
/// frames. Both are backed by HDR histograms and surfaced as millisecond
/// percentiles. GraphQL `error` frames count toward `errors_total`.
#[pyclass(skip_from_py_object)]
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct GraphqlWsMetrics {
    #[pyo3(get)]
    pub operations_total: u64,
    #[pyo3(get)]
    pub messages_total: u64,
    #[pyo3(get)]
    pub errors_total: u64,
    #[pyo3(get)]
    pub ttfm_mean_ms: f64,
    #[pyo3(get)]
    pub ttfm_p50_ms: f64,
    #[pyo3(get)]
    pub ttfm_p90_ms: f64,
    #[pyo3(get)]
    pub ttfm_p99_ms: f64,
    #[pyo3(get)]
    pub interval_mean_ms: f64,
    #[pyo3(get)]
    pub interval_p50_ms: f64,
    #[pyo3(get)]
    pub interval_p90_ms: f64,
    #[pyo3(get)]
    pub interval_p99_ms: f64,
}

/// Aggregated SOAP operation telemetry.
///
/// SOAP faults are reported inside the XML envelope (commonly on an HTTP 200
/// response for SOAP 1.2); `fault_responses_total` exposes those while the run's
/// `status_codes` stays truthful.
#[pyclass(skip_from_py_object)]
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct SoapMetrics {
    #[pyo3(get)]
    pub operations_total: u64,
    #[pyo3(get)]
    pub fault_responses_total: u64,
    #[pyo3(get)]
    pub soap_version: String,
}

/// Aggregated STOMP (JMS broker) messaging telemetry.
///
/// `send_latency_*` is the broker-acknowledged send latency when receipts are
/// enabled, otherwise the local write latency (see `receipts_enabled`).
/// `receive_interval_*` is the gap between consecutive consumed messages and
/// `ttfm_*` is the time from `SUBSCRIBE` to the first message. `destination`,
/// `mode`, `ack_mode` and `receipts_enabled` are per-run constants.
#[pyclass(skip_from_py_object)]
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct StompMetrics {
    #[pyo3(get)]
    pub messages_sent_total: u64,
    #[pyo3(get)]
    pub messages_received_total: u64,
    #[pyo3(get)]
    pub bytes_sent_total: u64,
    #[pyo3(get)]
    pub errors_total: u64,
    #[pyo3(get)]
    pub reconnects_total: u64,
    #[pyo3(get)]
    pub destination: String,
    #[pyo3(get)]
    pub mode: String,
    #[pyo3(get)]
    pub ack_mode: String,
    #[pyo3(get)]
    pub receipts_enabled: bool,
    #[pyo3(get)]
    pub send_latency_mean_ms: f64,
    #[pyo3(get)]
    pub send_latency_p50_ms: f64,
    #[pyo3(get)]
    pub send_latency_p90_ms: f64,
    #[pyo3(get)]
    pub send_latency_p99_ms: f64,
    #[pyo3(get)]
    pub receive_interval_mean_ms: f64,
    #[pyo3(get)]
    pub receive_interval_p50_ms: f64,
    #[pyo3(get)]
    pub receive_interval_p90_ms: f64,
    #[pyo3(get)]
    pub receive_interval_p99_ms: f64,
    #[pyo3(get)]
    pub ttfm_mean_ms: f64,
    #[pyo3(get)]
    pub ttfm_p50_ms: f64,
    #[pyo3(get)]
    pub ttfm_p90_ms: f64,
    #[pyo3(get)]
    pub ttfm_p99_ms: f64,
}

/// Aggregated SQL database (relational, JDBC-style) telemetry.
///
/// `pool_acquire_*` is the connection-pool wait and `query_*` the query
/// execution time; keeping them separate distinguishes pool starvation from
/// slow queries. `driver`, `pool_max`, and `fetch_mode` are per-run constants
/// (the query text is intentionally never recorded).
#[pyclass(skip_from_py_object)]
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct DatabaseMetrics {
    #[pyo3(get)]
    pub driver: String,
    #[pyo3(get)]
    pub queries_total: u64,
    #[pyo3(get)]
    pub rows_total: u64,
    #[pyo3(get)]
    pub errors_total: u64,
    #[pyo3(get)]
    pub pool_max: u64,
    #[pyo3(get)]
    pub pool_size: u64,
    #[pyo3(get)]
    pub fetch_mode: String,
    #[pyo3(get)]
    pub pool_acquire_mean_ms: f64,
    #[pyo3(get)]
    pub pool_acquire_p50_ms: f64,
    #[pyo3(get)]
    pub pool_acquire_p90_ms: f64,
    #[pyo3(get)]
    pub pool_acquire_p99_ms: f64,
    #[pyo3(get)]
    pub query_mean_ms: f64,
    #[pyo3(get)]
    pub query_p50_ms: f64,
    #[pyo3(get)]
    pub query_p90_ms: f64,
    #[pyo3(get)]
    pub query_p99_ms: f64,
}

/// Aggregated WebSocket heartbeat and write-buffer telemetry.
#[pyclass(skip_from_py_object)]
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct WebsocketMetrics {
    #[pyo3(get)]
    pub pings_sent_total: u64,
    #[pyo3(get)]
    pub pings_received_total: u64,
    #[pyo3(get)]
    pub pongs_solicited_total: u64,
    #[pyo3(get)]
    pub pongs_unsolicited_total: u64,
    #[pyo3(get)]
    pub backpressure_max_bytes: u64,
    #[pyo3(get)]
    pub backpressure_mean_bytes: f64,
    #[pyo3(get)]
    pub backpressure_threshold_breaches: u64,
}

/// Per-iteration WebSocket sample fed from the protocol engine.
///
/// Counters are deltas for a single iteration; `backpressure_*` fields carry the
/// high-water in-flight bytes and the running sum/count of write-buffer samples
/// taken during the iteration (zero count means no outbound write occurred).
#[derive(Debug, Clone, Default)]
pub struct WsSample {
    pub pings_sent: u64,
    pub pings_received: u64,
    pub pongs_solicited: u64,
    pub pongs_unsolicited: u64,
    pub backpressure_max_bytes: u64,
    pub backpressure_samples_sum: u64,
    pub backpressure_sample_count: u64,
    pub threshold_breaches: u64,
}

impl WsSample {
    /// True when the sample carries any meaningful WebSocket telemetry.
    pub fn is_significant(&self) -> bool {
        self.pings_sent > 0
            || self.pings_received > 0
            || self.pongs_solicited > 0
            || self.pongs_unsolicited > 0
            || self.backpressure_max_bytes > 0
            || self.backpressure_sample_count > 0
            || self.threshold_breaches > 0
    }
}

/// Aggregated gRPC/HTTP2 stream-concurrency and flow-control telemetry.
#[pyclass(skip_from_py_object)]
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct GrpcMetrics {
    #[pyo3(get)]
    pub active_streams_peak: u64,
    #[pyo3(get)]
    pub concurrency_utilization_peak: f64,
    #[pyo3(get)]
    pub concurrency_utilization_mean: f64,
    #[pyo3(get)]
    pub window_exhaustion_events_total: u64,
    #[pyo3(get)]
    pub window_stall_duration_ms_total: f64,
    #[pyo3(get)]
    pub send_capacity_min_bytes: u64,
}

/// Per-iteration gRPC sample fed from the protocol engine.
///
/// `active_streams` / `max_concurrent_streams` snapshot the connection's HTTP/2
/// multiplexing state; `send_capacity_bytes`/`has_send_capacity` report the
/// effective send flow-control credit (a client-side proxy for the connection
/// window); stall fields carry zero-credit send blocks timed during the write.
#[derive(Debug, Clone, Default)]
pub struct GrpcSample {
    pub active_streams: u64,
    pub max_concurrent_streams: u64,
    pub utilization: f64,
    pub window_exhaustion_events: u64,
    pub window_stall_us: u64,
    pub send_capacity_bytes: u64,
    pub has_send_capacity: bool,
}

impl GrpcSample {
    /// True when the sample carries any meaningful gRPC telemetry.
    pub fn is_significant(&self) -> bool {
        self.active_streams > 0
            || self.utilization > 0.0
            || self.window_exhaustion_events > 0
            || self.window_stall_us > 0
            || self.has_send_capacity
    }
}

/// Aggregated HTTP/3 (QUIC) congestion-control and connection-migration telemetry.
#[pyclass(skip_from_py_object)]
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct Http3Metrics {
    #[pyo3(get)]
    pub cwnd_bytes_current: u64,
    #[pyo3(get)]
    pub cwnd_bytes_min: u64,
    #[pyo3(get)]
    pub cwnd_bytes_max: u64,
    #[pyo3(get)]
    pub cwnd_bytes_mean: f64,
    #[pyo3(get)]
    pub migrations_attempted_total: u64,
    #[pyo3(get)]
    pub migrations_successful_total: u64,
    #[pyo3(get)]
    pub migration_success_rate: f64,
}

/// Per-iteration HTTP/3 sample fed from the protocol engine.
///
/// `cwnd_bytes` is an instantaneous QUIC congestion-window sample (from
/// `Connection::stats().path.cwnd`); migration counters carry per-iteration
/// deltas from the opt-in rebind harness (zero unless `--http3-migrate` is on).
#[derive(Debug, Clone, Default)]
pub struct Http3Sample {
    pub cwnd_bytes: u64,
    pub migrations_attempted: u64,
    pub migrations_successful: u64,
}

impl Http3Sample {
    /// True when the sample carries any meaningful HTTP/3 telemetry.
    pub fn is_significant(&self) -> bool {
        self.cwnd_bytes > 0 || self.migrations_attempted > 0
    }
}

/// Per-iteration STOMP messaging sample fed from the protocol engine.
///
/// Counters are deltas for a single iteration; `send_latency_us` is set on a
/// produce iteration (broker-ack when receipts are enabled, otherwise local
/// write latency), `ttfm_us` on the first consumed message, and
/// `receive_interval_us` on subsequent consumed messages.
#[derive(Debug, Clone, Default)]
pub struct StompSample {
    pub messages_sent: u64,
    pub messages_received: u64,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub errors: u64,
    pub reconnects: u64,
    pub send_latency_us: Option<u64>,
    pub receive_interval_us: Option<u64>,
    pub ttfm_us: Option<u64>,
}

impl StompSample {
    /// True when the sample carries any meaningful STOMP telemetry.
    pub fn is_significant(&self) -> bool {
        self.messages_sent > 0
            || self.messages_received > 0
            || self.bytes_sent > 0
            || self.errors > 0
            || self.reconnects > 0
            || self.send_latency_us.is_some()
            || self.receive_interval_us.is_some()
            || self.ttfm_us.is_some()
    }
}

/// Per-iteration SQL database sample fed from the protocol engine.
///
/// `rows` is rows affected (`execute`) or rows returned (`fetch`); `acquire_us`
/// is the pool-acquire wait and `query_us` the query execution time, so pool
/// starvation and slow queries can be told apart. `pool_size`/`pool_idle` are
/// instantaneous snapshots after the query.
#[derive(Debug, Clone, Default)]
pub struct DbSample {
    pub rows: u64,
    pub errors: u64,
    pub acquire_us: Option<u64>,
    pub query_us: Option<u64>,
    pub pool_size: u64,
    pub pool_idle: u64,
}

impl DbSample {
    /// True when the sample carries any meaningful database telemetry.
    pub fn is_significant(&self) -> bool {
        self.rows > 0
            || self.errors > 0
            || self.acquire_us.is_some()
            || self.query_us.is_some()
            || self.pool_size > 0
    }
}

/// Per-iteration QUIC connection telemetry for HTTP/3.
#[derive(Debug, Clone, Default)]
pub struct QuicSample {
    pub handshake_us: Option<u64>,
    pub used_0rtt: bool,
    pub retransmits: Option<u64>,
}

impl QuicSample {
    /// True when the sample carries any meaningful QUIC telemetry.
    pub fn is_significant(&self) -> bool {
        self.handshake_us.is_some() || self.used_0rtt || self.retransmits.is_some()
    }
}

/// Per-iteration Server-Sent Events telemetry.
#[derive(Debug, Clone, Default)]
pub struct SseSample {
    pub events_received: Option<u64>,
    pub first_event_us: Option<u64>,
    pub event_interval_us: Option<u64>,
}

/// Per-iteration GraphQL HTTP operation telemetry.
#[derive(Debug, Clone, Default)]
pub struct GraphqlSample {
    pub errors: u64,
}

/// Per-iteration GraphQL-over-WebSocket subscription telemetry.
#[derive(Debug, Clone, Default)]
pub struct GraphqlWsSample {
    pub operations: Option<u64>,
    pub ttfm_us: Option<u64>,
    pub interval_us: Option<u64>,
    pub errors: Option<u64>,
}

/// Per-iteration SOAP fault telemetry.
#[derive(Debug, Clone, Default)]
pub struct SoapSample {
    pub faults: u64,
}

/// Protocol-specific telemetry for a single iteration.
///
/// An iteration belongs to one protocol family; HTTP/3 carries its QUIC
/// connection sample alongside the HTTP/3 sample.
#[derive(Debug, Clone)]
pub enum ProtocolSample {
    Ws(WsSample),
    Grpc(GrpcSample),
    Http3 {
        http3: Option<Http3Sample>,
        quic: Option<QuicSample>,
    },
    Sse(SseSample),
    Stomp(StompSample),
    Db(DbSample),
    Graphql(GraphqlSample),
    GraphqlWs(GraphqlWsSample),
    Soap(SoapSample),
}

/// Fold HDR histogram percentiles (microseconds) into `GraphqlWsMetrics`.
pub(crate) fn graphql_ws_stats_from_histograms(
    mut stats: GraphqlWsMetrics,
    ttfm: &Histogram<u64>,
    interval: &Histogram<u64>,
) -> GraphqlWsMetrics {
    fold_percentiles(ttfm, |p| {
        stats.ttfm_mean_ms = p.mean_ms;
        stats.ttfm_p50_ms = p.p50_ms;
        stats.ttfm_p90_ms = p.p90_ms;
        stats.ttfm_p99_ms = p.p99_ms;
    });
    fold_percentiles(interval, |p| {
        stats.interval_mean_ms = p.mean_ms;
        stats.interval_p50_ms = p.p50_ms;
        stats.interval_p90_ms = p.p90_ms;
        stats.interval_p99_ms = p.p99_ms;
    });
    stats
}

/// Fold HDR histogram percentiles (microseconds) into `StompMetrics`.
pub(crate) fn stomp_stats_from_histograms(
    mut stats: StompMetrics,
    send: &Histogram<u64>,
    interval: &Histogram<u64>,
    ttfm: &Histogram<u64>,
) -> StompMetrics {
    fold_percentiles(send, |p| {
        stats.send_latency_mean_ms = p.mean_ms;
        stats.send_latency_p50_ms = p.p50_ms;
        stats.send_latency_p90_ms = p.p90_ms;
        stats.send_latency_p99_ms = p.p99_ms;
    });
    fold_percentiles(interval, |p| {
        stats.receive_interval_mean_ms = p.mean_ms;
        stats.receive_interval_p50_ms = p.p50_ms;
        stats.receive_interval_p90_ms = p.p90_ms;
        stats.receive_interval_p99_ms = p.p99_ms;
    });
    fold_percentiles(ttfm, |p| {
        stats.ttfm_mean_ms = p.mean_ms;
        stats.ttfm_p50_ms = p.p50_ms;
        stats.ttfm_p90_ms = p.p90_ms;
        stats.ttfm_p99_ms = p.p99_ms;
    });
    stats
}

/// Fold HDR histogram percentiles (microseconds) into `DatabaseMetrics`.
pub(crate) fn db_stats_from_histograms(
    mut stats: DatabaseMetrics,
    acquire: &Histogram<u64>,
    query: &Histogram<u64>,
) -> DatabaseMetrics {
    fold_percentiles(acquire, |p| {
        stats.pool_acquire_mean_ms = p.mean_ms;
        stats.pool_acquire_p50_ms = p.p50_ms;
        stats.pool_acquire_p90_ms = p.p90_ms;
        stats.pool_acquire_p99_ms = p.p99_ms;
    });
    fold_percentiles(query, |p| {
        stats.query_mean_ms = p.mean_ms;
        stats.query_p50_ms = p.p50_ms;
        stats.query_p90_ms = p.p90_ms;
        stats.query_p99_ms = p.p99_ms;
    });
    stats
}
