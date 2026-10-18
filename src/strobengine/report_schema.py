"""Python source of truth for the ``ReportArtifact`` JSON schema.

Mirrors the Rust structs in ``src/report/schema.rs`` (``ReportArtifact`` and
its nested types). Contract rules:

- Optional top-level metric blocks are **omitted entirely** when the run has
  no such metrics -- they are never emitted as ``null``. Rust enforces this
  with ``#[serde(skip_serializing_if = "Option::is_none")]``; the Python
  fallback builder in :mod:`strobengine.artifact` must skip them too.
- Fields typed ``... | None`` *without* ``NotRequired`` (e.g.
  ``cli_options.body``, ``quic.avg_handshake_ms``) are always present and may
  be ``null`` -- serde serializes those ``Option`` fields unconditionally.

Adding a field requires updating this file, ``src/report/schema.rs``, and the
fallback builder in ``src/strobengine/artifact.py`` together; the parity tests
(``TestArtifactSchemaConsistency`` / ``TestRustDictParity`` in
``tests/test_reporting.py`` and ``test_rust_fallback_artifact_parity`` in
``tests/e2e/test_persistence_e2e.py``) enforce the mirror.
"""

from __future__ import annotations

from typing import NotRequired, Protocol, TypedDict


class SystemInfoDict(TypedDict):
    hostname: str
    platform: str
    version: str


class CliOptionsDict(TypedDict):
    method: str
    concurrency: int
    timeout_secs: int
    chaos: bool
    chaos_rate: float
    body: str | None
    headers: list[tuple[str, str]] | None
    slo_p95_ms: float | None
    slo_p99_ms: float | None
    slo_error_rate: float | None
    slo_min_rps: float | None


class SupportsCliOptions(Protocol):
    """Structural type for config objects the report builders can read.

    Mirrors :class:`CliOptionsDict` key-for-key (enforced by
    ``tests/test_report_boundaries.py``): the Rust ``TestConfig`` satisfies it
    via its PyO3 getters, and the Python fallback builder reads exactly these
    attributes instead of duck-typed ``getattr`` defaults.
    """

    method: str
    concurrency: int
    timeout_secs: int
    chaos: bool
    chaos_rate: float
    body: str | None
    headers: list[tuple[str, str]] | None
    slo_p95_ms: float | None
    slo_p99_ms: float | None
    slo_error_rate: float | None
    slo_min_rps: float | None


class MetadataDict(TypedDict):
    timestamp: str
    duration_secs: float
    target_url: str
    cli_options: CliOptionsDict
    system_info: SystemInfoDict


class SummaryDict(TypedDict):
    total_requests: int
    successful_requests: int
    failed_requests: int
    rps: float
    bytes_transferred: int


class LatencyPercentilesDict(TypedDict):
    p50_us: float
    p90_us: float
    p95_us: float
    p99_us: float
    p99_99_us: float
    min_us: float
    max_us: float
    mean_us: float
    std_dev_us: float


class QuicMetricsDict(TypedDict):
    zero_rtt_accepted_count: int
    retransmissions: int
    avg_handshake_ms: float | None


class SseMetricsDict(TypedDict):
    total_events_received: int
    avg_ttfb_ms: float | None


class GraphqlMetricsDict(TypedDict):
    operations_total: int
    error_responses_total: int
    errors_total: int
    operation_type: str


class SoapMetricsDict(TypedDict):
    operations_total: int
    fault_responses_total: int
    soap_version: str


class StompMetricsDict(TypedDict):
    messages_sent_total: int
    messages_received_total: int
    bytes_sent_total: int
    errors_total: int
    reconnects_total: int
    destination: str
    mode: str
    ack_mode: str
    receipts_enabled: bool
    send_latency_mean_ms: float
    send_latency_p50_ms: float
    send_latency_p90_ms: float
    send_latency_p99_ms: float
    receive_interval_mean_ms: float
    receive_interval_p50_ms: float
    receive_interval_p90_ms: float
    receive_interval_p99_ms: float
    ttfm_mean_ms: float
    ttfm_p50_ms: float
    ttfm_p90_ms: float
    ttfm_p99_ms: float


class DatabaseMetricsDict(TypedDict):
    driver: str
    queries_total: int
    rows_total: int
    errors_total: int
    pool_max: int
    pool_size: int
    fetch_mode: str
    pool_acquire_mean_ms: float
    pool_acquire_p50_ms: float
    pool_acquire_p90_ms: float
    pool_acquire_p99_ms: float
    query_mean_ms: float
    query_p50_ms: float
    query_p90_ms: float
    query_p99_ms: float


class GraphqlWsMetricsDict(TypedDict):
    operations_total: int
    messages_total: int
    errors_total: int
    ttfm_mean_ms: float
    ttfm_p50_ms: float
    ttfm_p90_ms: float
    ttfm_p99_ms: float
    interval_mean_ms: float
    interval_p50_ms: float
    interval_p90_ms: float
    interval_p99_ms: float


class WebsocketMetricsDict(TypedDict):
    pings_sent_total: int
    pings_received_total: int
    pongs_solicited_total: int
    pongs_unsolicited_total: int
    backpressure_max_bytes: int
    backpressure_mean_bytes: float
    backpressure_threshold_breaches: int


class GrpcMetricsDict(TypedDict):
    active_streams_peak: int
    concurrency_utilization_peak: float
    concurrency_utilization_mean: float
    window_exhaustion_events_total: int
    window_stall_duration_ms_total: float
    send_capacity_min_bytes: int


class Http3MetricsDict(TypedDict):
    cwnd_bytes_current: int
    cwnd_bytes_min: int
    cwnd_bytes_max: int
    cwnd_bytes_mean: float
    migrations_attempted_total: int
    migrations_successful_total: int
    migration_success_rate: float


class ChaosFaultsDict(TypedDict):
    injected_total: int
    by_type: dict[str, int]


class ConnectionPoolDict(TypedDict):
    socket_creation_rate: float
    socket_reuse_rate: float
    dns_lookup_ms: float


class SystemSummaryDict(TypedDict):
    peak_cpu_percent: float
    avg_cpu_percent: float
    peak_memory_mb: float
    avg_memory_mb: float
    peak_threads: int


class SystemSampleDict(TypedDict):
    elapsed_sec: float
    cpu_percent: float
    memory_mb: float
    threads: int


class SystemMetricsDict(TypedDict):
    summary: SystemSummaryDict
    samples: list[SystemSampleDict]


class SloCheckDict(TypedDict):
    name: str
    observed: float
    threshold: float
    comparator: str
    passed: bool


class SloResultDict(TypedDict):
    passed: bool
    checks: list[SloCheckDict]


class TimelineSampleDict(TypedDict):
    elapsed_sec: float
    rps: float
    error_rate: float
    p50_us: float
    p95_us: float
    p99_us: float


class TimelineDict(TypedDict):
    interval_ms: int
    samples: list[TimelineSampleDict]


class ReportArtifactDict(TypedDict):
    """Top-level report artifact persisted to disk after each load test.

    Required keys are always present; ``NotRequired`` keys follow the omit
    contract above (see module docstring).
    """

    metadata: MetadataDict
    summary: SummaryDict
    latency_percentiles: LatencyPercentilesDict
    latency_histogram: dict[str, int]
    error_breakdown: dict[str, int]
    avg_connection_latency_us: NotRequired[float]
    quic: NotRequired[QuicMetricsDict]
    sse: NotRequired[SseMetricsDict]
    websocket: NotRequired[WebsocketMetricsDict]
    grpc: NotRequired[GrpcMetricsDict]
    http3: NotRequired[Http3MetricsDict]
    graphql: NotRequired[GraphqlMetricsDict]
    graphql_ws: NotRequired[GraphqlWsMetricsDict]
    soap: NotRequired[SoapMetricsDict]
    stomp: NotRequired[StompMetricsDict]
    database: NotRequired[DatabaseMetricsDict]
    slo: NotRequired[SloResultDict]
    timeline: NotRequired[TimelineDict]
    chaos_faults: NotRequired[ChaosFaultsDict]
    system_metrics: NotRequired[SystemMetricsDict]
    connection_pool: NotRequired[ConnectionPoolDict]
