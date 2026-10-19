use std::collections::HashMap;
use std::time::UNIX_EPOCH;

use hdrhistogram::Histogram;
use tokio::sync::mpsc;

use super::helpers::{HISTOGRAM_HIGHEST_TRACKABLE, HISTOGRAM_SIGNIFICANT_FIGURES, ns_since_epoch};
use super::summary::calculate_histogram_from_hdr;
use super::*;

/// Helper to create a histogram pre-loaded with values.
fn create_test_histogram(values: &[u64]) -> Histogram<u64> {
    let mut h = Histogram::new_with_max(HISTOGRAM_HIGHEST_TRACKABLE, HISTOGRAM_SIGNIFICANT_FIGURES)
        .unwrap();
    for &v in values {
        h.saturating_record(v);
    }
    h
}

#[test]
fn ns_since_epoch_at_epoch_is_zero() {
    assert_eq!(ns_since_epoch(UNIX_EPOCH), 0);
}

#[test]
fn ns_since_epoch_after_epoch() {
    use std::time::Duration;
    let when = UNIX_EPOCH + Duration::from_secs(5);
    assert_eq!(ns_since_epoch(when), 5_000_000_000);
}

#[test]
fn ns_since_epoch_before_epoch_returns_zero() {
    use std::time::Duration;
    let when = UNIX_EPOCH - Duration::from_secs(5);
    // Clock before the epoch must not panic; it degrades to 0 (with a warn).
    assert_eq!(ns_since_epoch(when), 0);
}

#[test]
fn empty_latencies_returns_zeros() {
    let s = calculate_summary(SummaryInput {
        url: "http://example.com".into(),
        total_requests: 10,
        total_errors: 3,
        latency_histogram: create_test_histogram(&[]),
        total_bytes: 0,
        duration_secs: 1.0,
        workers: 4,
        status_codes: HashMap::new(),
        e2e_latency_histogram: create_test_histogram(&[]),
        connection_latency_histogram: create_test_histogram(&[]),
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
        resource_samples: Vec::new(),
        total_sockets_created: 0,
        total_connections_reused: 0,
        dns_resolution_sum_us: 0,
        dns_resolution_count: 0,
    });
    assert_eq!(s.url, "http://example.com");
    assert_eq!(s.total_requests, 10);
    assert_eq!(s.total_errors, 3);
    assert_eq!(s.average_latency_ms, 0.0);
    assert_eq!(s.p95_latency_ms, 0.0);
    assert_eq!(s.p99_latency_ms, 0.0);
    assert_eq!(s.min_latency_ms, 0.0);
    assert_eq!(s.p50_latency_ms, 0.0);
    assert_eq!(s.p90_latency_ms, 0.0);
    assert_eq!(s.max_latency_ms, 0.0);
}

#[test]
fn single_request() {
    let s = calculate_summary(SummaryInput {
        url: "http://example.com".into(),
        total_requests: 1,
        total_errors: 0,
        latency_histogram: create_test_histogram(&[5000]),
        total_bytes: 1024,
        duration_secs: 2.0,
        workers: 2,
        status_codes: HashMap::new(),
        e2e_latency_histogram: create_test_histogram(&[]),
        connection_latency_histogram: create_test_histogram(&[]),
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
        resource_samples: Vec::new(),
        total_sockets_created: 0,
        total_connections_reused: 0,
        dns_resolution_sum_us: 0,
        dns_resolution_count: 0,
    });
    assert_eq!(s.total_requests, 1);
    // HDR quantization may shift values by up to 0.1% of range
    assert!((s.average_latency_ms - 5.0).abs() < 0.01);
    assert!((s.min_latency_ms - 5.0).abs() < 0.01);
    assert!((s.p50_latency_ms - 5.0).abs() < 0.01);
    assert!((s.p90_latency_ms - 5.0).abs() < 0.01);
    assert!((s.p95_latency_ms - 5.0).abs() < 0.01);
    assert!((s.p99_latency_ms - 5.0).abs() < 0.01);
    assert!((s.max_latency_ms - 5.0).abs() < 0.01);
    assert_eq!(s.total_bytes_received, 1024);
    assert_eq!(s.workers, 2);
}

#[test]
fn two_requests() {
    let s = calculate_summary(SummaryInput {
        url: "http://example.com".into(),
        total_requests: 2,
        total_errors: 0,
        latency_histogram: create_test_histogram(&[1000, 2000]),
        total_bytes: 0,
        duration_secs: 1.0,
        workers: 1,
        status_codes: HashMap::new(),
        e2e_latency_histogram: create_test_histogram(&[]),
        connection_latency_histogram: create_test_histogram(&[]),
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
        resource_samples: Vec::new(),
        total_sockets_created: 0,
        total_connections_reused: 0,
        dns_resolution_sum_us: 0,
        dns_resolution_count: 0,
    });
    assert!((s.average_latency_ms - 1.5).abs() < 0.01);
    assert!((s.min_latency_ms - 1.0).abs() < 0.01);
    assert!((s.p95_latency_ms - 2.0).abs() < 0.01);
    assert!((s.p99_latency_ms - 2.0).abs() < 0.01);
    assert!((s.max_latency_ms - 2.0).abs() < 0.01);
}

#[test]
fn uniform_hundred_values() {
    let latencies: Vec<u64> = (1..=100).collect();
    let s = calculate_summary(SummaryInput {
        url: "http://example.com".into(),
        total_requests: 100,
        total_errors: 0,
        latency_histogram: create_test_histogram(&latencies),
        total_bytes: 0,
        duration_secs: 1.0,
        workers: 1,
        status_codes: HashMap::new(),
        e2e_latency_histogram: create_test_histogram(&[]),
        connection_latency_histogram: create_test_histogram(&[]),
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
        resource_samples: Vec::new(),
        total_sockets_created: 0,
        total_connections_reused: 0,
        dns_resolution_sum_us: 0,
        dns_resolution_count: 0,
    });
    assert!((s.average_latency_ms - 0.0505).abs() < 0.001);
    assert!((s.min_latency_ms - 0.001).abs() < 0.001);
    assert!((s.p50_latency_ms - 0.051).abs() < 0.001);
    assert!((s.p90_latency_ms - 0.091).abs() < 0.01);
    assert!((s.p95_latency_ms - 0.096).abs() < 0.01);
    assert!((s.p99_latency_ms - 0.1).abs() < 0.01);
    assert!((s.max_latency_ms - 0.1).abs() < 0.01);
}

#[test]
fn all_errors() {
    let s = calculate_summary(SummaryInput {
        url: "http://example.com".into(),
        total_requests: 5,
        total_errors: 5,
        latency_histogram: create_test_histogram(&[100, 200, 300]),
        total_bytes: 0,
        duration_secs: 1.0,
        workers: 1,
        status_codes: HashMap::new(),
        e2e_latency_histogram: create_test_histogram(&[]),
        connection_latency_histogram: create_test_histogram(&[]),
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
        resource_samples: Vec::new(),
        total_sockets_created: 0,
        total_connections_reused: 0,
        dns_resolution_sum_us: 0,
        dns_resolution_count: 0,
    });
    assert_eq!(s.total_requests, 5);
    assert_eq!(s.total_errors, 5);
}

#[test]
fn microsecond_to_millisecond_conversion() {
    let s = calculate_summary(SummaryInput {
        url: "http://example.com".into(),
        total_requests: 1,
        total_errors: 0,
        latency_histogram: create_test_histogram(&[12345]),
        total_bytes: 0,
        duration_secs: 1.0,
        workers: 1,
        status_codes: HashMap::new(),
        e2e_latency_histogram: create_test_histogram(&[]),
        connection_latency_histogram: create_test_histogram(&[]),
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
        resource_samples: Vec::new(),
        total_sockets_created: 0,
        total_connections_reused: 0,
        dns_resolution_sum_us: 0,
        dns_resolution_count: 0,
    });
    assert!((s.average_latency_ms - 12.345).abs() < 0.01);
}

#[test]
fn unsorted_latencies_are_handled() {
    let s = calculate_summary(SummaryInput {
        url: "http://example.com".into(),
        total_requests: 3,
        total_errors: 0,
        latency_histogram: create_test_histogram(&[3000, 1000, 2000]),
        total_bytes: 0,
        duration_secs: 1.0,
        workers: 1,
        status_codes: HashMap::new(),
        e2e_latency_histogram: create_test_histogram(&[]),
        connection_latency_histogram: create_test_histogram(&[]),
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
        resource_samples: Vec::new(),
        total_sockets_created: 0,
        total_connections_reused: 0,
        dns_resolution_sum_us: 0,
        dns_resolution_count: 0,
    });
    assert!((s.p95_latency_ms - 3.0).abs() < 0.01);
    assert!((s.p99_latency_ms - 3.0).abs() < 0.01);
    assert!((s.min_latency_ms - 1.0).abs() < 0.01);
    assert!((s.max_latency_ms - 3.0).abs() < 0.01);
}

#[test]
fn status_codes_preserved() {
    let mut codes = HashMap::new();
    codes.insert(200, 10);
    codes.insert(500, 3);
    let s = calculate_summary(SummaryInput {
        url: "http://example.com".into(),
        total_requests: 13,
        total_errors: 3,
        latency_histogram: create_test_histogram(&[100]),
        total_bytes: 0,
        duration_secs: 1.0,
        workers: 1,
        status_codes: codes,
        e2e_latency_histogram: create_test_histogram(&[]),
        connection_latency_histogram: create_test_histogram(&[]),
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
        resource_samples: Vec::new(),
        total_sockets_created: 0,
        total_connections_reused: 0,
        dns_resolution_sum_us: 0,
        dns_resolution_count: 0,
    });
    assert_eq!(s.status_codes.get(&200), Some(&10));
    assert_eq!(s.status_codes.get(&500), Some(&3));
}

#[test]
fn test_aggregate_quic_and_sse_metrics() {
    let s = calculate_summary(SummaryInput {
        url: "http://example.com".into(),
        total_requests: 5,
        total_errors: 0,
        latency_histogram: create_test_histogram(&[1000, 2000, 3000, 4000, 5000]),
        total_bytes: 5120,
        duration_secs: 5.0,
        workers: 2,
        status_codes: HashMap::new(),
        e2e_latency_histogram: create_test_histogram(&[]),
        connection_latency_histogram: create_test_histogram(&[100, 200, 300, 400, 500]),
        quic_metrics: Some(QuicMetrics {
            zero_rtt_accepted_count: 3,
            retransmissions: 10,
            avg_handshake_ms: Some(1.5),
        }),
        sse_metrics: Some(SseMetrics {
            total_events_received: 100,
            avg_ttfb_ms: Some(0.5),
        }),
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
        resource_samples: Vec::new(),
        total_sockets_created: 0,
        total_connections_reused: 0,
        dns_resolution_sum_us: 0,
        dns_resolution_count: 0,
    });

    assert!((s.avg_connection_latency_us - 300.0).abs() < 1.0);

    let quic = s.quic.as_ref().unwrap();
    assert_eq!(quic.zero_rtt_accepted_count, 3);
    assert_eq!(quic.retransmissions, 10);
    assert!((quic.avg_handshake_ms.unwrap() - 1.5).abs() < 1e-6);

    let sse = s.sse.as_ref().unwrap();
    assert_eq!(sse.total_events_received, 100);
    assert!((sse.avg_ttfb_ms.unwrap() - 0.5).abs() < 1e-6);
}

#[test]
fn test_aggregate_websocket_metrics() {
    let s = calculate_summary(SummaryInput {
        url: "ws://example.com".into(),
        total_requests: 10,
        total_errors: 0,
        latency_histogram: create_test_histogram(&[1000, 2000, 3000]),
        total_bytes: 3000,
        duration_secs: 5.0,
        workers: 2,
        status_codes: HashMap::new(),
        e2e_latency_histogram: create_test_histogram(&[]),
        connection_latency_histogram: create_test_histogram(&[]),
        quic_metrics: None,
        sse_metrics: None,
        ws_metrics: Some(WebsocketMetrics {
            pings_sent_total: 8,
            pings_received_total: 2,
            pongs_solicited_total: 7,
            pongs_unsolicited_total: 1,
            backpressure_max_bytes: 2097152,
            backpressure_mean_bytes: 786432.0,
            backpressure_threshold_breaches: 3,
        }),
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
        resource_samples: Vec::new(),
        total_sockets_created: 0,
        total_connections_reused: 0,
        dns_resolution_sum_us: 0,
        dns_resolution_count: 0,
    });

    let ws = s.ws.as_ref().unwrap();
    assert_eq!(ws.pings_sent_total, 8);
    assert_eq!(ws.pings_received_total, 2);
    assert_eq!(ws.pongs_solicited_total, 7);
    assert_eq!(ws.pongs_unsolicited_total, 1);
    assert_eq!(ws.backpressure_max_bytes, 2097152);
    assert!((ws.backpressure_mean_bytes - 786432.0).abs() < 1e-6);
    assert_eq!(ws.backpressure_threshold_breaches, 3);
}

#[tokio::test]
async fn test_finalize_metrics_aggregates_websocket_samples() {
    let (tx, rx) = tokio::sync::mpsc::channel::<RequestMetric>(8);
    let sample = |sent, recv, sol, un, bp_max, bp_sum, bp_count, breaches| WsSample {
        pings_sent: sent,
        pings_received: recv,
        pongs_solicited: sol,
        pongs_unsolicited: un,
        backpressure_max_bytes: bp_max,
        backpressure_samples_sum: bp_sum,
        backpressure_sample_count: bp_count,
        threshold_breaches: breaches,
    };

    let mut first = RequestMetric::error(1000, None);
    first.ws = Some(sample(1, 0, 1, 0, 1000, 1000, 1, 0));
    tx.send(first).await.unwrap();

    let mut second = RequestMetric::error(1000, None);
    second.ws = Some(sample(0, 0, 0, 0, 2000, 3000, 2, 1));
    tx.send(second).await.unwrap();

    // Insignificant samples must not flip `has_ws`.
    let mut third = RequestMetric::error(1000, None);
    third.ws = Some(WsSample::default());
    tx.send(third).await.unwrap();

    drop(tx);
    let agg = finalize_metrics(rx, 0).await;

    let ws = agg
        .ws_metrics
        .expect("ws metrics present after significant samples");
    assert_eq!(ws.pings_sent_total, 1);
    assert_eq!(ws.pongs_solicited_total, 1);
    assert_eq!(ws.backpressure_max_bytes, 2000);
    assert_eq!(ws.backpressure_threshold_breaches, 1);
    assert!((ws.backpressure_mean_bytes - (4000.0_f64 / 3.0)).abs() < 1e-6);
}

#[tokio::test]
async fn test_finalize_metrics_aggregates_grpc_samples() {
    let (tx, rx) = tokio::sync::mpsc::channel::<RequestMetric>(8);

    let mut first = RequestMetric::error(1000, None);
    first.grpc = Some(GrpcSample {
        active_streams: 4,
        max_concurrent_streams: 100,
        utilization: 0.04,
        window_exhaustion_events: 1,
        window_stall_us: 1500,
        send_capacity_bytes: 65535,
        has_send_capacity: true,
    });
    tx.send(first).await.unwrap();

    let mut second = RequestMetric::error(1000, None);
    second.grpc = Some(GrpcSample {
        active_streams: 6,
        max_concurrent_streams: 100,
        utilization: 0.06,
        window_exhaustion_events: 2,
        window_stall_us: 3000,
        send_capacity_bytes: 32768,
        has_send_capacity: true,
    });
    tx.send(second).await.unwrap();

    // Insignificant sample must not flip `has_grpc`.
    let mut third = RequestMetric::error(1000, None);
    third.grpc = Some(GrpcSample::default());
    tx.send(third).await.unwrap();

    drop(tx);
    let agg = finalize_metrics(rx, 0).await;
    let grpc = agg
        .grpc_metrics
        .expect("grpc metrics present after significant samples");

    assert_eq!(grpc.active_streams_peak, 6);
    assert!((grpc.concurrency_utilization_peak - 0.06).abs() < 1e-6);
    assert!((grpc.concurrency_utilization_mean - 0.05).abs() < 1e-6);
    assert_eq!(grpc.window_exhaustion_events_total, 3);
    assert!((grpc.window_stall_duration_ms_total - 4.5).abs() < 1e-6); // 4500us
    assert_eq!(grpc.send_capacity_min_bytes, 32768);
}

#[tokio::test]
async fn test_finalize_metrics_aggregates_http3_samples() {
    let (tx, rx) = tokio::sync::mpsc::channel::<RequestMetric>(8);
    let s = |cwnd: u64, att: u64, succ: u64| Http3Sample {
        cwnd_bytes: cwnd,
        migrations_attempted: att,
        migrations_successful: succ,
    };
    for sample in [s(2000, 0, 0), s(1000, 0, 0), s(4000, 2, 1)] {
        let mut m = RequestMetric::error(1000, None);
        m.http3 = Some(sample);
        tx.send(m).await.unwrap();
    }
    // Insignificant sample must not flip has_http3.
    let mut none = RequestMetric::error(1000, None);
    none.http3 = Some(Http3Sample::default());
    tx.send(none).await.unwrap();
    drop(tx);

    let agg = finalize_metrics(rx, 0).await;
    let h = agg.http3_metrics.expect("http3 metrics present");
    // current = last significant sample (4000); min/max/mean over 2000,1000,4000.
    assert_eq!(h.cwnd_bytes_current, 4000);
    assert_eq!(h.cwnd_bytes_min, 1000);
    assert_eq!(h.cwnd_bytes_max, 4000);
    assert!((h.cwnd_bytes_mean - (7000.0 / 3.0)).abs() < 1e-6);
    assert_eq!(h.migrations_attempted_total, 2);
    assert_eq!(h.migrations_successful_total, 1);
    assert!((h.migration_success_rate - 0.5).abs() < 1e-9);
}

#[tokio::test]
async fn test_finalize_metrics_counts_chaos_faults() {
    use crate::chaos::ChaosFault;

    let (tx, rx) = tokio::sync::mpsc::channel::<RequestMetric>(8);

    // A fault counts whether the iteration succeeded or failed...
    tx.send(
        RequestMetric::builder(1000, Some(ChaosFault::ConnectionDrop))
            .status(200)
            .build(),
    )
    .await
    .unwrap();
    tx.send(RequestMetric::error(1000, Some(ChaosFault::ConnectionDrop)))
        .await
        .unwrap();
    tx.send(RequestMetric::error(
        1000,
        Some(ChaosFault::LatencySpike { duration_ms: 150 }),
    ))
    .await
    .unwrap();
    // ...but an iteration without a fault must not be counted.
    tx.send(RequestMetric::error(1000, None)).await.unwrap();
    drop(tx);

    let agg = finalize_metrics(rx, 0).await;
    assert_eq!(agg.chaos_injected_total, 3);
    assert_eq!(agg.chaos_faults_by_type.get("ConnectionDrop"), Some(&2));
    assert_eq!(agg.chaos_faults_by_type.get("LatencySpike"), Some(&1));
    assert_eq!(agg.chaos_faults_by_type.len(), 2);
}

#[tokio::test]
async fn test_http3_migration_rate_sentinel_no_migrations() {
    let (tx, rx) = tokio::sync::mpsc::channel::<RequestMetric>(8);
    let mut m = RequestMetric::error(1000, None);
    m.http3 = Some(Http3Sample {
        cwnd_bytes: 5000,
        migrations_attempted: 0,
        migrations_successful: 0,
    });
    tx.send(m).await.unwrap();
    drop(tx);
    let agg = finalize_metrics(rx, 0).await;
    let h = agg.http3_metrics.expect("http3 present via cwnd");
    assert_eq!(h.migrations_attempted_total, 0);
    // Zero migrations attempted => trivially successful (1.0 sentinel).
    assert!((h.migration_success_rate - 1.0).abs() < 1e-9);
}

#[test]
fn test_summary_optional_protocol_metrics_defaults() {
    let s = calculate_summary(SummaryInput {
        url: "http://example.com".into(),
        total_requests: 1,
        total_errors: 0,
        latency_histogram: create_test_histogram(&[1000]),
        total_bytes: 0,
        duration_secs: 1.0,
        workers: 1,
        status_codes: HashMap::new(),
        e2e_latency_histogram: create_test_histogram(&[]),
        connection_latency_histogram: create_test_histogram(&[]),
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
        resource_samples: Vec::new(),
        total_sockets_created: 0,
        total_connections_reused: 0,
        dns_resolution_sum_us: 0,
        dns_resolution_count: 0,
    });
    assert!(s.quic.is_none());
    assert!(s.sse.is_none());
    assert!(s.ws.is_none());
    assert!(s.grpc.is_none());
    assert!(s.http3.is_none());
    assert_eq!(s.avg_connection_latency_us, 0.0);
}

#[test]
fn test_p99_99_percentile() {
    let latencies: Vec<u64> = (1..=10000).collect();
    let s = calculate_summary(SummaryInput {
        url: "http://example.com".into(),
        total_requests: 10000,
        total_errors: 0,
        latency_histogram: create_test_histogram(&latencies),
        total_bytes: 0,
        duration_secs: 1.0,
        workers: 1,
        status_codes: HashMap::new(),
        e2e_latency_histogram: create_test_histogram(&[]),
        connection_latency_histogram: create_test_histogram(&[]),
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
        resource_samples: Vec::new(),
        total_sockets_created: 0,
        total_connections_reused: 0,
        dns_resolution_sum_us: 0,
        dns_resolution_count: 0,
    });
    assert!(s.p99_99_latency_ms >= s.p99_latency_ms);
    assert!(s.p99_99_latency_ms <= s.max_latency_ms);
}

#[test]
fn test_histogram_bucket_distribution() {
    let hist = create_test_histogram(&[500, 1500, 7500, 15000, 75000]);
    let buckets = calculate_histogram_from_hdr(&hist);
    assert_eq!(buckets["<1ms"], 1);
    assert_eq!(buckets["1-5ms"], 1);
    assert_eq!(buckets["5-10ms"], 1);
    assert_eq!(buckets["10-25ms"], 1);
    assert_eq!(buckets["50-100ms"], 1);
}

#[test]
fn test_histogram_all_empty() {
    let hist = create_test_histogram(&[]);
    let buckets = calculate_histogram_from_hdr(&hist);
    assert_eq!(buckets["<1ms"], 0);
    assert_eq!(buckets[">1000ms"], 0);
    assert_eq!(buckets.len(), 10);
}

#[test]
fn test_histogram_empty_returns_zeroed_summary() {
    let hist = create_test_histogram(&[]);
    let s = calculate_summary(SummaryInput {
        url: "http://test".into(),
        total_requests: 0,
        total_errors: 0,
        latency_histogram: hist,
        total_bytes: 0,
        duration_secs: 1.0,
        workers: 1,
        status_codes: HashMap::new(),
        e2e_latency_histogram: create_test_histogram(&[]),
        connection_latency_histogram: create_test_histogram(&[]),
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
        resource_samples: Vec::new(),
        total_sockets_created: 0,
        total_connections_reused: 0,
        dns_resolution_sum_us: 0,
        dns_resolution_count: 0,
    });
    assert_eq!(s.total_requests, 0);
    assert_eq!(s.p50_latency_ms, 0.0);
}

#[test]
fn test_std_dev_single_value() {
    let hist = create_test_histogram(&[5000]);
    let s = calculate_summary(SummaryInput {
        url: "http://example.com".into(),
        total_requests: 1,
        total_errors: 0,
        latency_histogram: hist,
        total_bytes: 0,
        duration_secs: 1.0,
        workers: 1,
        status_codes: HashMap::new(),
        e2e_latency_histogram: create_test_histogram(&[]),
        connection_latency_histogram: create_test_histogram(&[]),
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
        resource_samples: Vec::new(),
        total_sockets_created: 0,
        total_connections_reused: 0,
        dns_resolution_sum_us: 0,
        dns_resolution_count: 0,
    });
    assert_eq!(s.std_dev_latency_ms, 0.0);
}

#[test]
fn test_std_dev_calculation() {
    // Known values: [1000, 2000, 3000, 4000, 5000] us
    // StdDev = sqrt(2000000) ≈ 1414.21 us
    let hist = create_test_histogram(&[1000, 2000, 3000, 4000, 5000]);
    let s = calculate_summary(SummaryInput {
        url: "http://example.com".into(),
        total_requests: 5,
        total_errors: 0,
        latency_histogram: hist,
        total_bytes: 0,
        duration_secs: 1.0,
        workers: 1,
        status_codes: HashMap::new(),
        e2e_latency_histogram: create_test_histogram(&[]),
        connection_latency_histogram: create_test_histogram(&[]),
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
        resource_samples: Vec::new(),
        total_sockets_created: 0,
        total_connections_reused: 0,
        dns_resolution_sum_us: 0,
        dns_resolution_count: 0,
    });
    assert!((s.std_dev_latency_ms * MICROS_PER_MILLI - 1414.21).abs() < 15.0);
}

#[test]
fn connection_reuse_all_new() {
    let s = calculate_summary(SummaryInput {
        url: "http://example.com".into(),
        total_requests: 10,
        total_errors: 0,
        latency_histogram: create_test_histogram(&[1000]),
        total_bytes: 0,
        duration_secs: 1.0,
        workers: 1,
        status_codes: HashMap::new(),
        e2e_latency_histogram: create_test_histogram(&[]),
        connection_latency_histogram: create_test_histogram(&[]),
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
        resource_samples: Vec::new(),
        total_sockets_created: 10,
        total_connections_reused: 0,
        dns_resolution_sum_us: 0,
        dns_resolution_count: 0,
    });
    assert!((s.connection_reuse_ratio - 0.0).abs() < f64::EPSILON);
    assert_eq!(s.avg_dns_resolution_ms, 0.0);
}

#[test]
fn connection_reuse_all_reused() {
    let s = calculate_summary(SummaryInput {
        url: "http://example.com".into(),
        total_requests: 10,
        total_errors: 0,
        latency_histogram: create_test_histogram(&[1000]),
        total_bytes: 0,
        duration_secs: 1.0,
        workers: 1,
        status_codes: HashMap::new(),
        e2e_latency_histogram: create_test_histogram(&[]),
        connection_latency_histogram: create_test_histogram(&[]),
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
        resource_samples: Vec::new(),
        total_sockets_created: 0,
        total_connections_reused: 10,
        dns_resolution_sum_us: 0,
        dns_resolution_count: 0,
    });
    assert!((s.connection_reuse_ratio - 1.0).abs() < f64::EPSILON);
}

#[test]
fn connection_reuse_mixed() {
    let s = calculate_summary(SummaryInput {
        url: "http://example.com".into(),
        total_requests: 10,
        total_errors: 0,
        latency_histogram: create_test_histogram(&[1000]),
        total_bytes: 0,
        duration_secs: 1.0,
        workers: 1,
        status_codes: HashMap::new(),
        e2e_latency_histogram: create_test_histogram(&[]),
        connection_latency_histogram: create_test_histogram(&[]),
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
        resource_samples: Vec::new(),
        total_sockets_created: 5,
        total_connections_reused: 5,
        dns_resolution_sum_us: 0,
        dns_resolution_count: 0,
    });
    assert!((s.connection_reuse_ratio - 0.5).abs() < f64::EPSILON);
}

#[test]
fn connection_reuse_zero_requests() {
    let s = calculate_summary(SummaryInput {
        url: "http://example.com".into(),
        total_requests: 0,
        total_errors: 0,
        latency_histogram: create_test_histogram(&[]),
        total_bytes: 0,
        duration_secs: 1.0,
        workers: 1,
        status_codes: HashMap::new(),
        e2e_latency_histogram: create_test_histogram(&[]),
        connection_latency_histogram: create_test_histogram(&[]),
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
        resource_samples: Vec::new(),
        total_sockets_created: 0,
        total_connections_reused: 0,
        dns_resolution_sum_us: 0,
        dns_resolution_count: 0,
    });
    assert_eq!(s.connection_reuse_ratio, 0.0);
    assert_eq!(s.avg_dns_resolution_ms, 0.0);
}

#[test]
fn dns_resolution_average() {
    let s = calculate_summary(SummaryInput {
        url: "http://example.com".into(),
        total_requests: 3,
        total_errors: 0,
        latency_histogram: create_test_histogram(&[1000, 2000, 3000]),
        total_bytes: 0,
        duration_secs: 1.0,
        workers: 1,
        status_codes: HashMap::new(),
        e2e_latency_histogram: create_test_histogram(&[]),
        connection_latency_histogram: create_test_histogram(&[]),
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
        resource_samples: Vec::new(),
        total_sockets_created: 3,
        total_connections_reused: 0,
        dns_resolution_sum_us: 6000,
        dns_resolution_count: 3,
    });
    assert!((s.avg_dns_resolution_ms - 2.0).abs() < f64::EPSILON);
}

#[test]
fn dns_resolution_no_data() {
    let s = calculate_summary(SummaryInput {
        url: "http://example.com".into(),
        total_requests: 5,
        total_errors: 0,
        latency_histogram: create_test_histogram(&[1000]),
        total_bytes: 0,
        duration_secs: 1.0,
        workers: 1,
        status_codes: HashMap::new(),
        e2e_latency_histogram: create_test_histogram(&[]),
        connection_latency_histogram: create_test_histogram(&[]),
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
        resource_samples: Vec::new(),
        total_sockets_created: 5,
        total_connections_reused: 0,
        dns_resolution_sum_us: 0,
        dns_resolution_count: 0,
    });
    assert_eq!(s.avg_dns_resolution_ms, 0.0);
}

#[tokio::test]
async fn timeline_samples_are_emitted_when_enabled() {
    let (tx, rx) = mpsc::channel(16);
    for _ in 0..3 {
        tx.send(RequestMetric::builder(1_000, None).status(200).build())
            .await
            .unwrap();
    }
    drop(tx);

    let agg = finalize_metrics(rx, 1).await;

    let timeline = agg.timeline.expect("timeline present when enabled");
    assert_eq!(timeline.interval_ms, 1);
    assert!(!timeline.samples.is_empty());
    let last = timeline.samples.last().unwrap();
    assert!(last.rps >= 0.0);
    assert!(last.error_rate >= 0.0);
}

#[tokio::test]
async fn timeline_is_none_when_disabled() {
    let (tx, rx) = mpsc::channel(4);
    tx.send(RequestMetric::builder(1_000, None).status(200).build())
        .await
        .unwrap();
    drop(tx);

    let agg = finalize_metrics(rx, 0).await;
    assert!(agg.timeline.is_none());
}
