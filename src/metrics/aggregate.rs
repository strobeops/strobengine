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

/// Finalize metric aggregation from a receiver channel.
/// Returns aggregated metrics using fixed-memory HDR Histograms.
pub async fn finalize_metrics(
    mut rx: mpsc::Receiver<RequestMetric>,
    timeline_interval_ms: u64,
) -> AggregatedMetrics {
    let mut metrics = AggregatedMetrics::default();
    let mut quic_stats = QuicMetrics::default();
    let mut sse_stats = SseMetrics::default();
    let mut ws_stats = WebsocketMetrics::default();
    let mut grpc_stats = GrpcMetrics::default();
    let mut http3_stats = Http3Metrics::default();
    let mut graphql_stats = GraphqlMetrics::default();
    let mut graphql_ws_stats = GraphqlWsMetrics::default();
    let mut has_quic = false;
    let mut has_ws = false;
    let mut has_grpc = false;
    let mut has_graphql = false;
    let mut has_graphql_ws = false;
    let mut soap_stats = SoapMetrics::default();
    let mut has_soap = false;
    let mut stomp_stats = StompMetrics::default();
    let mut has_stomp = false;
    let mut stomp_send_histogram = new_histogram();
    let mut stomp_interval_histogram = new_histogram();
    let mut stomp_ttfm_histogram = new_histogram();
    let mut db_stats = DatabaseMetrics::default();
    let mut has_db = false;
    let mut db_acquire_histogram = new_histogram();
    let mut db_query_histogram = new_histogram();
    let mut graphql_ws_ttfm_histogram = new_histogram();
    let mut graphql_ws_interval_histogram = new_histogram();
    let mut ws_bp_sum: u64 = 0;
    let mut ws_bp_count: u64 = 0;
    let mut grpc_util_sum: f64 = 0.0;
    let mut grpc_util_count: u64 = 0;
    let mut grpc_capacity_min: u64 = u64::MAX;
    let mut grpc_stall_sum_us: u64 = 0;
    let mut has_http3 = false;
    let mut http3_cwnd_sum: u64 = 0;
    let mut http3_cwnd_count: u64 = 0;
    let mut http3_cwnd_min: u64 = u64::MAX;
    let mut quic_handshake_sum_us: u64 = 0;
    let mut quic_handshake_count: u64 = 0;
    let mut sse_first_event_sum_us: u64 = 0;
    let mut sse_first_event_count: u64 = 0;
    let timeline_enabled = timeline_interval_ms > 0;
    let timeline_sample_interval = Duration::from_millis(timeline_interval_ms.max(1));
    let run_start = Instant::now();
    let mut last_sample_at = Instant::now();
    let mut timeline_samples: Vec<TimelineSample> = Vec::new();
    let mut interval_histogram = new_histogram();
    let mut interval_requests: u64 = 0;
    let mut interval_errors: u64 = 0;

    while let Some(metric) = rx.recv().await {
        // Record latencies into fixed-memory histograms (safe u128->u64 clamping)
        let lat = u64::try_from(metric.latency_micros).unwrap_or(u64::MAX);
        metrics.latency_histogram.saturating_record(lat);
        if timeline_enabled {
            interval_histogram.saturating_record(lat);
            interval_requests += 1;
            if metric.is_error() {
                interval_errors += 1;
            }
        }

        if let Some(e2e) = metric.connection.e2e_latency_us {
            let v = u64::try_from(e2e).unwrap_or(u64::MAX);
            metrics.e2e_latency_histogram.saturating_record(v);
        }
        if let Some(conn) = metric.connection.connection_latency_us {
            let v = u64::try_from(conn).unwrap_or(u64::MAX);
            metrics.connection_latency_histogram.saturating_record(v);
        }

        // QUIC aggregation
        if metric.quic_handshake_us.is_some()
            || metric.quic_0rtt_used
            || metric.quic_retransmits.is_some()
        {
            has_quic = true;
            if metric.quic_0rtt_used {
                quic_stats.zero_rtt_accepted_count += 1;
            }
            if let Some(retrans) = metric.quic_retransmits {
                quic_stats.retransmissions += retrans;
            }
            if let Some(handshake) = metric.quic_handshake_us {
                quic_handshake_sum_us += handshake;
                quic_handshake_count += 1;
            }
        }

        // SSE aggregation
        if let Some(events) = metric.sse_events_received {
            sse_stats.total_events_received += events;
        }
        if let Some(first) = metric.sse_first_event_us {
            sse_first_event_sum_us += first;
            sse_first_event_count += 1;
        }

        // GraphQL aggregation
        if let Some(errors) = metric.graphql_errors {
            has_graphql = true;
            graphql_stats.operations_total += 1;
            if errors > 0 {
                graphql_stats.error_responses_total += 1;
            }
            graphql_stats.errors_total += errors;
        }

        // GraphQL-over-WebSocket subscription aggregation
        if let Some(ops) = metric.graphql_ws_operations {
            has_graphql_ws = true;
            graphql_ws_stats.operations_total += ops;
        }
        if let Some(ttfm) = metric.graphql_ws_ttfm_us {
            has_graphql_ws = true;
            graphql_ws_ttfm_histogram.saturating_record(ttfm);
        }
        if let Some(interval) = metric.graphql_ws_interval_us {
            has_graphql_ws = true;
            graphql_ws_stats.messages_total += 1;
            graphql_ws_interval_histogram.saturating_record(interval);
        }
        if let Some(errors) = metric.graphql_ws_errors {
            has_graphql_ws = true;
            graphql_ws_stats.errors_total += errors;
        }

        // SOAP aggregation
        if let Some(faults) = metric.soap_errors {
            has_soap = true;
            soap_stats.operations_total += 1;
            if faults > 0 {
                soap_stats.fault_responses_total += 1;
            }
        }

        // STOMP (JMS broker) aggregation
        if let Some(sample) = metric.stomp.as_ref()
            && sample.is_significant()
        {
            has_stomp = true;
            stomp_stats.messages_sent_total += sample.messages_sent;
            stomp_stats.messages_received_total += sample.messages_received;
            stomp_stats.bytes_sent_total += sample.bytes_sent;
            stomp_stats.errors_total += sample.errors;
            stomp_stats.reconnects_total += sample.reconnects;
            if let Some(us) = sample.send_latency_us {
                stomp_send_histogram.saturating_record(us);
            }
            if let Some(us) = sample.receive_interval_us {
                stomp_interval_histogram.saturating_record(us);
            }
            if let Some(us) = sample.ttfm_us {
                stomp_ttfm_histogram.saturating_record(us);
            }
        }

        // SQL database aggregation
        if let Some(sample) = metric.db.as_ref()
            && sample.is_significant()
        {
            has_db = true;
            db_stats.queries_total += 1;
            db_stats.rows_total += sample.rows;
            db_stats.errors_total += sample.errors;
            if sample.pool_size > 0 {
                db_stats.pool_size = sample.pool_size;
            }
            if let Some(us) = sample.acquire_us {
                db_acquire_histogram.saturating_record(us);
            }
            if let Some(us) = sample.query_us {
                db_query_histogram.saturating_record(us);
            }
        }

        // WebSocket aggregation (O(1) running counters)
        if let Some(sample) = metric.ws.as_ref()
            && sample.is_significant()
        {
            has_ws = true;
            ws_stats.pings_sent_total += sample.pings_sent;
            ws_stats.pings_received_total += sample.pings_received;
            ws_stats.pongs_solicited_total += sample.pongs_solicited;
            ws_stats.pongs_unsolicited_total += sample.pongs_unsolicited;
            if sample.backpressure_max_bytes > ws_stats.backpressure_max_bytes {
                ws_stats.backpressure_max_bytes = sample.backpressure_max_bytes;
            }
            ws_stats.backpressure_threshold_breaches += sample.threshold_breaches;
            ws_bp_sum += sample.backpressure_samples_sum;
            ws_bp_count += sample.backpressure_sample_count;
        }

        // gRPC aggregation (O(1) running counters)
        if let Some(sample) = metric.grpc.as_ref()
            && sample.is_significant()
        {
            has_grpc = true;
            if sample.active_streams > grpc_stats.active_streams_peak {
                grpc_stats.active_streams_peak = sample.active_streams;
            }
            if sample.utilization > grpc_stats.concurrency_utilization_peak {
                grpc_stats.concurrency_utilization_peak = sample.utilization;
            }
            grpc_util_sum += sample.utilization;
            grpc_util_count += 1;
            grpc_stats.window_exhaustion_events_total += sample.window_exhaustion_events;
            grpc_stall_sum_us += sample.window_stall_us;
            if sample.has_send_capacity && sample.send_capacity_bytes < grpc_capacity_min {
                grpc_capacity_min = sample.send_capacity_bytes;
            }
        }

        // HTTP/3 aggregation (O(1) running counters)
        if let Some(sample) = metric.http3.as_ref()
            && sample.is_significant()
        {
            has_http3 = true;
            // "current" = last observed sample; min/max/mean over the session.
            http3_stats.cwnd_bytes_current = sample.cwnd_bytes;
            if sample.cwnd_bytes > http3_stats.cwnd_bytes_max {
                http3_stats.cwnd_bytes_max = sample.cwnd_bytes;
            }
            if sample.cwnd_bytes < http3_cwnd_min {
                http3_cwnd_min = sample.cwnd_bytes;
            }
            http3_cwnd_sum += sample.cwnd_bytes;
            http3_cwnd_count += 1;
            http3_stats.migrations_attempted_total += sample.migrations_attempted;
            http3_stats.migrations_successful_total += sample.migrations_successful;
        }

        // Chaos aggregation
        if let Some(ref fault) = metric.chaos_fault {
            metrics.chaos_injected_total += 1;
            *metrics
                .chaos_faults_by_type
                .entry(fault.name().to_string())
                .or_insert(0) += 1;
        }
        // Connection pool & DNS aggregation
        // Note: uninstrumented engines default to is_socket_reused=false,
        // so connection_reuse_ratio will be 0% until per-engine tracking is added.
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

        if timeline_enabled && last_sample_at.elapsed() >= timeline_sample_interval {
            if interval_requests > 0 {
                timeline_samples.push(make_timeline_sample(
                    run_start.elapsed().as_secs_f64(),
                    interval_requests,
                    interval_errors,
                    last_sample_at.elapsed().as_secs_f64(),
                    &interval_histogram,
                ));
            }
            interval_histogram = new_histogram();
            interval_requests = 0;
            interval_errors = 0;
            last_sample_at = Instant::now();
        }
    }

    if timeline_enabled && interval_requests > 0 {
        timeline_samples.push(make_timeline_sample(
            run_start.elapsed().as_secs_f64(),
            interval_requests,
            interval_errors,
            last_sample_at.elapsed().as_secs_f64().max(1e-9),
            &interval_histogram,
        ));
    }
    if timeline_enabled && !timeline_samples.is_empty() {
        metrics.timeline = Some(Timeline {
            interval_ms: timeline_interval_ms,
            samples: timeline_samples,
        });
    }

    // Compute QUIC avg handshake via O(1) running counters
    if quic_handshake_count > 0 {
        quic_stats.avg_handshake_ms =
            Some((quic_handshake_sum_us as f64 / quic_handshake_count as f64) / 1000.0);
    }

    // Compute SSE avg TTFB via O(1) running counters
    if sse_first_event_count > 0 {
        sse_stats.avg_ttfb_ms =
            Some((sse_first_event_sum_us as f64 / sse_first_event_count as f64) / 1000.0);
    }

    // Compute WebSocket backpressure mean via O(1) running counters
    if ws_bp_count > 0 {
        ws_stats.backpressure_mean_bytes = ws_bp_sum as f64 / ws_bp_count as f64;
    }

    // Compute gRPC concurrency mean + stall total via O(1) running counters
    if grpc_util_count > 0 {
        grpc_stats.concurrency_utilization_mean = grpc_util_sum / grpc_util_count as f64;
    }
    grpc_stats.window_stall_duration_ms_total = grpc_stall_sum_us as f64 / 1000.0;
    grpc_stats.send_capacity_min_bytes = if grpc_capacity_min == u64::MAX {
        0
    } else {
        grpc_capacity_min
    };

    // Compute HTTP/3 cwnd mean/min + migration success rate.
    if http3_cwnd_count > 0 {
        http3_stats.cwnd_bytes_mean = http3_cwnd_sum as f64 / http3_cwnd_count as f64;
    }
    http3_stats.cwnd_bytes_min = if http3_cwnd_min == u64::MAX {
        0
    } else {
        http3_cwnd_min
    };
    http3_stats.migration_success_rate = if http3_stats.migrations_attempted_total == 0 {
        // Sentinel: no migrations required => trivially successful.
        1.0
    } else {
        http3_stats.migrations_successful_total as f64
            / http3_stats.migrations_attempted_total as f64
    };

    metrics.quic_metrics = if has_quic { Some(quic_stats) } else { None };
    metrics.sse_metrics = if sse_stats.total_events_received > 0 || sse_first_event_count > 0 {
        Some(sse_stats)
    } else {
        None
    };
    metrics.ws_metrics = if has_ws { Some(ws_stats) } else { None };
    metrics.grpc_metrics = if has_grpc { Some(grpc_stats) } else { None };
    metrics.http3_metrics = if has_http3 { Some(http3_stats) } else { None };
    metrics.graphql_metrics = if has_graphql {
        Some(graphql_stats)
    } else {
        None
    };

    metrics.graphql_ws_metrics = if has_graphql_ws {
        Some(graphql_ws_stats_from_histograms(
            graphql_ws_stats,
            &graphql_ws_ttfm_histogram,
            &graphql_ws_interval_histogram,
        ))
    } else {
        None
    };

    metrics.soap_metrics = if has_soap { Some(soap_stats) } else { None };

    metrics.stomp_metrics = if has_stomp {
        Some(stomp_stats_from_histograms(
            stomp_stats,
            &stomp_send_histogram,
            &stomp_interval_histogram,
            &stomp_ttfm_histogram,
        ))
    } else {
        None
    };

    metrics.db_metrics = if has_db {
        Some(db_stats_from_histograms(
            db_stats,
            &db_acquire_histogram,
            &db_query_histogram,
        ))
    } else {
        None
    };

    metrics
}
