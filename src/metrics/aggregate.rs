use std::collections::HashMap;
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;
use tokio::sync::mpsc;

use super::helpers::new_histogram;
use super::protocols::{
    DatabaseMetrics, GraphqlMetrics, GraphqlWsMetrics, GrpcMetrics, Http3Metrics, QuicMetrics,
    SoapMetrics, SseMetrics, StompMetrics, WebsocketMetrics,
};
use super::protocols::{
    db_stats_from_histograms, graphql_ws_stats_from_histograms, stomp_stats_from_histograms,
};
use super::request::RequestMetric;
use super::timeline::{Timeline, TimelineSample, make_timeline_sample};

/// Intermediate aggregated values collected from the metrics channel.
/// Uses fixed-memory HDR Histograms instead of unbounded Vec<u128>.
pub struct AggregatedMetrics {
    pub latency_histogram: Histogram<u64>,
    pub e2e_latency_histogram: Histogram<u64>,
    pub connection_latency_histogram: Histogram<u64>,
    pub status_codes: HashMap<u16, u64>,
    pub total_bytes: u64,
    pub quic_metrics: Option<QuicMetrics>,
    pub sse_metrics: Option<SseMetrics>,
    pub ws_metrics: Option<WebsocketMetrics>,
    pub grpc_metrics: Option<GrpcMetrics>,
    pub http3_metrics: Option<Http3Metrics>,
    pub graphql_metrics: Option<GraphqlMetrics>,
    pub graphql_ws_metrics: Option<GraphqlWsMetrics>,
    pub soap_metrics: Option<SoapMetrics>,
    pub stomp_metrics: Option<StompMetrics>,
    pub db_metrics: Option<DatabaseMetrics>,
    pub timeline: Option<Timeline>,
    pub chaos_injected_total: u64,
    pub chaos_faults_by_type: HashMap<String, u64>,
    pub total_sockets_created: u64,
    pub total_connections_reused: u64,
    pub dns_resolution_sum_us: u64,
    pub dns_resolution_count: u64,
}

impl Default for AggregatedMetrics {
    fn default() -> Self {
        Self {
            latency_histogram: new_histogram(),
            e2e_latency_histogram: new_histogram(),
            connection_latency_histogram: new_histogram(),
            status_codes: HashMap::new(),
            total_bytes: 0,
            quic_metrics: None,
            sse_metrics: None,
            ws_metrics: None,
            grpc_metrics: None,
            http3_metrics: None,
            graphql_metrics: None,
            graphql_ws_metrics: None,
            soap_metrics: None,
            stomp_metrics: None,
            db_metrics: None,
            timeline: None,
            chaos_injected_total: 0,
            chaos_faults_by_type: HashMap::new(),
            total_sockets_created: 0,
            total_connections_reused: 0,
            dns_resolution_sum_us: 0,
            dns_resolution_count: 0,
        }
    }
}

/// Record the metrics common to every protocol plus chaos/connection accounting.
fn record_common(metrics: &mut AggregatedMetrics, metric: &RequestMetric, latency_us: u64) {
    metrics.latency_histogram.saturating_record(latency_us);
    if let Some(e2e) = metric.connection.e2e_latency_us {
        let v = u64::try_from(e2e).unwrap_or(u64::MAX);
        metrics.e2e_latency_histogram.saturating_record(v);
    }
    if let Some(conn) = metric.connection.connection_latency_us {
        let v = u64::try_from(conn).unwrap_or(u64::MAX);
        metrics.connection_latency_histogram.saturating_record(v);
    }
    if let Some(fault) = &metric.chaos_fault {
        metrics.chaos_injected_total += 1;
        *metrics
            .chaos_faults_by_type
            .entry(fault.name().to_string())
            .or_insert(0) += 1;
    }
    // Note: uninstrumented engines default to is_socket_reused=false, so
    // connection_reuse_ratio stays 0% until per-engine tracking is added.
    if metric.connection.is_socket_reused {
        metrics.total_connections_reused += 1;
    } else {
        metrics.total_sockets_created += 1;
    }
    if let Some(dns_us) = metric.connection.dns_resolution_us {
        metrics.dns_resolution_sum_us += dns_us;
        metrics.dns_resolution_count += 1;
    }
    *metrics.status_codes.entry(metric.status_code).or_insert(0) += 1;
    metrics.total_bytes += metric.bytes_received;
}

#[derive(Default)]
struct QuicAccumulator {
    stats: QuicMetrics,
    has: bool,
    handshake_sum_us: u64,
    handshake_count: u64,
}

impl QuicAccumulator {
    fn record(&mut self, metric: &RequestMetric) {
        if metric.quic_handshake_us.is_some()
            || metric.quic_0rtt_used
            || metric.quic_retransmits.is_some()
        {
            self.has = true;
            if metric.quic_0rtt_used {
                self.stats.zero_rtt_accepted_count += 1;
            }
            if let Some(retrans) = metric.quic_retransmits {
                self.stats.retransmissions += retrans;
            }
            if let Some(handshake) = metric.quic_handshake_us {
                self.handshake_sum_us += handshake;
                self.handshake_count += 1;
            }
        }
    }

    fn finish(mut self) -> Option<QuicMetrics> {
        if !self.has {
            return None;
        }
        if self.handshake_count > 0 {
            self.stats.avg_handshake_ms =
                Some((self.handshake_sum_us as f64 / self.handshake_count as f64) / 1000.0);
        }
        Some(self.stats)
    }
}

#[derive(Default)]
struct SseAccumulator {
    stats: SseMetrics,
    first_event_sum_us: u64,
    first_event_count: u64,
}

impl SseAccumulator {
    fn record(&mut self, metric: &RequestMetric) {
        if let Some(events) = metric.sse_events_received {
            self.stats.total_events_received += events;
        }
        if let Some(first) = metric.sse_first_event_us {
            self.first_event_sum_us += first;
            self.first_event_count += 1;
        }
    }

    fn finish(mut self) -> Option<SseMetrics> {
        if self.first_event_count > 0 {
            self.stats.avg_ttfb_ms =
                Some((self.first_event_sum_us as f64 / self.first_event_count as f64) / 1000.0);
        }
        if self.stats.total_events_received > 0 || self.first_event_count > 0 {
            Some(self.stats)
        } else {
            None
        }
    }
}

#[derive(Default)]
struct GraphqlAccumulator {
    stats: GraphqlMetrics,
    has: bool,
}

impl GraphqlAccumulator {
    fn record(&mut self, metric: &RequestMetric) {
        if let Some(errors) = metric.graphql_errors {
            self.has = true;
            self.stats.operations_total += 1;
            if errors > 0 {
                self.stats.error_responses_total += 1;
            }
            self.stats.errors_total += errors;
        }
    }

    fn finish(self) -> Option<GraphqlMetrics> {
        self.has.then_some(self.stats)
    }
}

struct GraphqlWsAccumulator {
    stats: GraphqlWsMetrics,
    has: bool,
    ttfm: Histogram<u64>,
    interval: Histogram<u64>,
}

impl Default for GraphqlWsAccumulator {
    fn default() -> Self {
        Self {
            stats: GraphqlWsMetrics::default(),
            has: false,
            ttfm: new_histogram(),
            interval: new_histogram(),
        }
    }
}

impl GraphqlWsAccumulator {
    fn record(&mut self, metric: &RequestMetric) {
        if let Some(ops) = metric.graphql_ws_operations {
            self.has = true;
            self.stats.operations_total += ops;
        }
        if let Some(ttfm) = metric.graphql_ws_ttfm_us {
            self.has = true;
            self.ttfm.saturating_record(ttfm);
        }
        if let Some(interval) = metric.graphql_ws_interval_us {
            self.has = true;
            self.stats.messages_total += 1;
            self.interval.saturating_record(interval);
        }
        if let Some(errors) = metric.graphql_ws_errors {
            self.has = true;
            self.stats.errors_total += errors;
        }
    }

    fn finish(self) -> Option<GraphqlWsMetrics> {
        if !self.has {
            return None;
        }
        Some(graphql_ws_stats_from_histograms(
            self.stats,
            &self.ttfm,
            &self.interval,
        ))
    }
}

#[derive(Default)]
struct SoapAccumulator {
    stats: SoapMetrics,
    has: bool,
}

impl SoapAccumulator {
    fn record(&mut self, metric: &RequestMetric) {
        if let Some(faults) = metric.soap_errors {
            self.has = true;
            self.stats.operations_total += 1;
            if faults > 0 {
                self.stats.fault_responses_total += 1;
            }
        }
    }

    fn finish(self) -> Option<SoapMetrics> {
        self.has.then_some(self.stats)
    }
}

struct StompAccumulator {
    stats: StompMetrics,
    has: bool,
    send: Histogram<u64>,
    interval: Histogram<u64>,
    ttfm: Histogram<u64>,
}

impl Default for StompAccumulator {
    fn default() -> Self {
        Self {
            stats: StompMetrics::default(),
            has: false,
            send: new_histogram(),
            interval: new_histogram(),
            ttfm: new_histogram(),
        }
    }
}

impl StompAccumulator {
    fn record(&mut self, metric: &RequestMetric) {
        let Some(sample) = metric.stomp.as_ref().filter(|s| s.is_significant()) else {
            return;
        };
        self.has = true;
        self.stats.messages_sent_total += sample.messages_sent;
        self.stats.messages_received_total += sample.messages_received;
        self.stats.bytes_sent_total += sample.bytes_sent;
        self.stats.errors_total += sample.errors;
        self.stats.reconnects_total += sample.reconnects;
        if let Some(us) = sample.send_latency_us {
            self.send.saturating_record(us);
        }
        if let Some(us) = sample.receive_interval_us {
            self.interval.saturating_record(us);
        }
        if let Some(us) = sample.ttfm_us {
            self.ttfm.saturating_record(us);
        }
    }

    fn finish(self) -> Option<StompMetrics> {
        if !self.has {
            return None;
        }
        Some(stomp_stats_from_histograms(
            self.stats,
            &self.send,
            &self.interval,
            &self.ttfm,
        ))
    }
}

struct DbAccumulator {
    stats: DatabaseMetrics,
    has: bool,
    acquire: Histogram<u64>,
    query: Histogram<u64>,
}

impl Default for DbAccumulator {
    fn default() -> Self {
        Self {
            stats: DatabaseMetrics::default(),
            has: false,
            acquire: new_histogram(),
            query: new_histogram(),
        }
    }
}

impl DbAccumulator {
    fn record(&mut self, metric: &RequestMetric) {
        let Some(sample) = metric.db.as_ref().filter(|s| s.is_significant()) else {
            return;
        };
        self.has = true;
        self.stats.queries_total += 1;
        self.stats.rows_total += sample.rows;
        self.stats.errors_total += sample.errors;
        if sample.pool_size > 0 {
            self.stats.pool_size = sample.pool_size;
        }
        if let Some(us) = sample.acquire_us {
            self.acquire.saturating_record(us);
        }
        if let Some(us) = sample.query_us {
            self.query.saturating_record(us);
        }
    }

    fn finish(self) -> Option<DatabaseMetrics> {
        if !self.has {
            return None;
        }
        Some(db_stats_from_histograms(
            self.stats,
            &self.acquire,
            &self.query,
        ))
    }
}

#[derive(Default)]
struct WsAccumulator {
    stats: WebsocketMetrics,
    has: bool,
    bp_sum: u64,
    bp_count: u64,
}

impl WsAccumulator {
    fn record(&mut self, metric: &RequestMetric) {
        let Some(sample) = metric.ws.as_ref().filter(|s| s.is_significant()) else {
            return;
        };
        self.has = true;
        self.stats.pings_sent_total += sample.pings_sent;
        self.stats.pings_received_total += sample.pings_received;
        self.stats.pongs_solicited_total += sample.pongs_solicited;
        self.stats.pongs_unsolicited_total += sample.pongs_unsolicited;
        if sample.backpressure_max_bytes > self.stats.backpressure_max_bytes {
            self.stats.backpressure_max_bytes = sample.backpressure_max_bytes;
        }
        self.stats.backpressure_threshold_breaches += sample.threshold_breaches;
        self.bp_sum += sample.backpressure_samples_sum;
        self.bp_count += sample.backpressure_sample_count;
    }

    fn finish(mut self) -> Option<WebsocketMetrics> {
        if self.bp_count > 0 {
            self.stats.backpressure_mean_bytes = self.bp_sum as f64 / self.bp_count as f64;
        }
        self.has.then_some(self.stats)
    }
}

struct GrpcAccumulator {
    stats: GrpcMetrics,
    has: bool,
    util_sum: f64,
    util_count: u64,
    capacity_min: u64,
    stall_sum_us: u64,
}

impl Default for GrpcAccumulator {
    fn default() -> Self {
        Self {
            stats: GrpcMetrics::default(),
            has: false,
            util_sum: 0.0,
            util_count: 0,
            capacity_min: u64::MAX,
            stall_sum_us: 0,
        }
    }
}

impl GrpcAccumulator {
    fn record(&mut self, metric: &RequestMetric) {
        let Some(sample) = metric.grpc.as_ref().filter(|s| s.is_significant()) else {
            return;
        };
        self.has = true;
        if sample.active_streams > self.stats.active_streams_peak {
            self.stats.active_streams_peak = sample.active_streams;
        }
        if sample.utilization > self.stats.concurrency_utilization_peak {
            self.stats.concurrency_utilization_peak = sample.utilization;
        }
        self.util_sum += sample.utilization;
        self.util_count += 1;
        self.stats.window_exhaustion_events_total += sample.window_exhaustion_events;
        self.stall_sum_us += sample.window_stall_us;
        if sample.has_send_capacity && sample.send_capacity_bytes < self.capacity_min {
            self.capacity_min = sample.send_capacity_bytes;
        }
    }

    fn finish(mut self) -> Option<GrpcMetrics> {
        if !self.has {
            return None;
        }
        if self.util_count > 0 {
            self.stats.concurrency_utilization_mean = self.util_sum / self.util_count as f64;
        }
        self.stats.window_stall_duration_ms_total = self.stall_sum_us as f64 / 1000.0;
        self.stats.send_capacity_min_bytes = if self.capacity_min == u64::MAX {
            0
        } else {
            self.capacity_min
        };
        Some(self.stats)
    }
}

struct Http3Accumulator {
    stats: Http3Metrics,
    has: bool,
    cwnd_sum: u64,
    cwnd_count: u64,
    cwnd_min: u64,
}

impl Default for Http3Accumulator {
    fn default() -> Self {
        Self {
            stats: Http3Metrics::default(),
            has: false,
            cwnd_sum: 0,
            cwnd_count: 0,
            cwnd_min: u64::MAX,
        }
    }
}

impl Http3Accumulator {
    fn record(&mut self, metric: &RequestMetric) {
        let Some(sample) = metric.http3.as_ref().filter(|s| s.is_significant()) else {
            return;
        };
        self.has = true;
        // "current" = last observed sample; min/max/mean over the session.
        self.stats.cwnd_bytes_current = sample.cwnd_bytes;
        if sample.cwnd_bytes > self.stats.cwnd_bytes_max {
            self.stats.cwnd_bytes_max = sample.cwnd_bytes;
        }
        if sample.cwnd_bytes < self.cwnd_min {
            self.cwnd_min = sample.cwnd_bytes;
        }
        self.cwnd_sum += sample.cwnd_bytes;
        self.cwnd_count += 1;
        self.stats.migrations_attempted_total += sample.migrations_attempted;
        self.stats.migrations_successful_total += sample.migrations_successful;
    }

    fn finish(mut self) -> Option<Http3Metrics> {
        if !self.has {
            return None;
        }
        if self.cwnd_count > 0 {
            self.stats.cwnd_bytes_mean = self.cwnd_sum as f64 / self.cwnd_count as f64;
        }
        self.stats.cwnd_bytes_min = if self.cwnd_min == u64::MAX {
            0
        } else {
            self.cwnd_min
        };
        self.stats.migration_success_rate = if self.stats.migrations_attempted_total == 0 {
            // Sentinel: no migrations required => trivially successful.
            1.0
        } else {
            self.stats.migrations_successful_total as f64
                / self.stats.migrations_attempted_total as f64
        };
        Some(self.stats)
    }
}

#[derive(Default)]
struct ProtocolAccumulators {
    quic: QuicAccumulator,
    sse: SseAccumulator,
    graphql: GraphqlAccumulator,
    graphql_ws: GraphqlWsAccumulator,
    soap: SoapAccumulator,
    stomp: StompAccumulator,
    db: DbAccumulator,
    ws: WsAccumulator,
    grpc: GrpcAccumulator,
    http3: Http3Accumulator,
}

impl ProtocolAccumulators {
    fn record(&mut self, metric: &RequestMetric) {
        self.quic.record(metric);
        self.sse.record(metric);
        self.graphql.record(metric);
        self.graphql_ws.record(metric);
        self.soap.record(metric);
        self.stomp.record(metric);
        self.db.record(metric);
        self.ws.record(metric);
        self.grpc.record(metric);
        self.http3.record(metric);
    }

    fn finish_into(self, metrics: &mut AggregatedMetrics) {
        metrics.quic_metrics = self.quic.finish();
        metrics.sse_metrics = self.sse.finish();
        metrics.graphql_metrics = self.graphql.finish();
        metrics.graphql_ws_metrics = self.graphql_ws.finish();
        metrics.soap_metrics = self.soap.finish();
        metrics.stomp_metrics = self.stomp.finish();
        metrics.db_metrics = self.db.finish();
        metrics.ws_metrics = self.ws.finish();
        metrics.grpc_metrics = self.grpc.finish();
        metrics.http3_metrics = self.http3.finish();
    }
}

struct TimelineAccumulator {
    enabled: bool,
    interval_ms: u64,
    sample_interval: Duration,
    run_start: Instant,
    last_sample_at: Instant,
    samples: Vec<TimelineSample>,
    histogram: Histogram<u64>,
    requests: u64,
    errors: u64,
}

impl TimelineAccumulator {
    fn new(timeline_interval_ms: u64) -> Self {
        let enabled = timeline_interval_ms > 0;
        let run_start = Instant::now();
        Self {
            enabled,
            interval_ms: timeline_interval_ms,
            sample_interval: Duration::from_millis(timeline_interval_ms.max(1)),
            run_start,
            last_sample_at: Instant::now(),
            samples: Vec::new(),
            histogram: new_histogram(),
            requests: 0,
            errors: 0,
        }
    }

    fn record_latency(&mut self, latency_us: u64, is_error: bool) {
        if self.enabled {
            self.histogram.saturating_record(latency_us);
            self.requests += 1;
            if is_error {
                self.errors += 1;
            }
        }
    }

    fn maybe_emit(&mut self) {
        if !self.enabled || self.last_sample_at.elapsed() < self.sample_interval {
            return;
        }
        if self.requests > 0 {
            self.samples.push(make_timeline_sample(
                self.run_start.elapsed().as_secs_f64(),
                self.requests,
                self.errors,
                self.last_sample_at.elapsed().as_secs_f64(),
                &self.histogram,
            ));
        }
        self.histogram = new_histogram();
        self.requests = 0;
        self.errors = 0;
        self.last_sample_at = Instant::now();
    }

    fn finish(mut self) -> Option<Timeline> {
        if self.enabled && self.requests > 0 {
            self.samples.push(make_timeline_sample(
                self.run_start.elapsed().as_secs_f64(),
                self.requests,
                self.errors,
                self.last_sample_at.elapsed().as_secs_f64().max(1e-9),
                &self.histogram,
            ));
        }
        if self.enabled && !self.samples.is_empty() {
            Some(Timeline {
                interval_ms: self.interval_ms,
                samples: self.samples,
            })
        } else {
            None
        }
    }
}

/// Finalize metric aggregation from a receiver channel.
/// Returns aggregated metrics using fixed-memory HDR Histograms.
pub async fn finalize_metrics(
    mut rx: mpsc::Receiver<RequestMetric>,
    timeline_interval_ms: u64,
) -> AggregatedMetrics {
    let mut metrics = AggregatedMetrics::default();
    let mut protocols = ProtocolAccumulators::default();
    let mut timeline = TimelineAccumulator::new(timeline_interval_ms);

    while let Some(metric) = rx.recv().await {
        let latency_us = u64::try_from(metric.latency_micros).unwrap_or(u64::MAX);
        record_common(&mut metrics, &metric, latency_us);
        timeline.record_latency(latency_us, metric.is_error());
        protocols.record(&metric);
        timeline.maybe_emit();
    }

    protocols.finish_into(&mut metrics);
    metrics.timeline = timeline.finish();
    metrics
}
