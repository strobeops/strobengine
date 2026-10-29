import asyncio
from dataclasses import dataclass, fields
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from strobengine._strobengine import (
    LoadProfile,
    SoapVersion,
    StompAck,
    StompMode,
    TestConfig,
    TestSummary,
    WsMode,
    run_load_profiles,
    run_load_test,
)
from strobengine.constants import (
    DEFAULT_BASELINE,
    DEFAULT_CONCURRENCY,
    DEFAULT_DURATION_SECS,
    DEFAULT_HOLD_SECS,
    DEFAULT_HTTP3_MIGRATE_EVERY,
    DEFAULT_MAX_CONCURRENCY,
    DEFAULT_METHOD,
    DEFAULT_PEAK_CONCURRENCY,
    DEFAULT_POST_SPIKE_SECS,
    DEFAULT_PRE_SPIKE_SECS,
    DEFAULT_RAMP_SECS,
    DEFAULT_SPIKE_SECS,
    DEFAULT_START_CONCURRENCY,
    DEFAULT_SYS_SAMPLE_INTERVAL_MS,
    DEFAULT_TEST_SEED,
    DEFAULT_TIMEOUT_SECS,
    DEFAULT_WS_BACKPRESSURE_WARN_RATIO,
    DEFAULT_WS_MAX_BUFFER_BYTES,
    MAX_CONCURRENCY,
)


class SoapVersionEnum(StrEnum):
    soap11 = "1.1"
    soap12 = "1.2"


class WsModeEnum(StrEnum):
    handshake = "handshake"
    ping_pong = "ping_pong"
    stream = "stream"


class StompModeEnum(StrEnum):
    produce = "produce"
    consume = "consume"


class StompAckEnum(StrEnum):
    auto = "auto"
    client = "client"
    client_individual = "client-individual"


_SOAP_VERSION_MAP: dict[SoapVersionEnum, SoapVersion] = {
    SoapVersionEnum.soap11: SoapVersion.soap11(),
    SoapVersionEnum.soap12: SoapVersion.soap12(),
}

_WS_MODE_MAP: dict[WsModeEnum, WsMode] = {
    WsModeEnum.handshake: WsMode.handshake(),
    WsModeEnum.ping_pong: WsMode.ping_pong(),
    WsModeEnum.stream: WsMode.stream(),
}

_STOMP_MODE_MAP: dict[StompModeEnum, StompMode] = {
    StompModeEnum.produce: StompMode.produce(),
    StompModeEnum.consume: StompMode.consume(),
}

_STOMP_ACK_MAP: dict[StompAckEnum, StompAck] = {
    StompAckEnum.auto: StompAck.auto(),
    StompAckEnum.client: StompAck.client(),
    StompAckEnum.client_individual: StompAck.client_individual(),
}

# RequestOptions field -> TestConfig kwarg (only `timeout` differs).
_TEST_CONFIG_RENAMES: dict[str, str] = {"timeout": "timeout_secs"}

# Fields whose Python enum must be translated to a native PyO3 enum value.
_TEST_CONFIG_ENUMS: dict[str, tuple[dict[Any, Any], Any]] = {
    "ws_mode": (_WS_MODE_MAP, WsMode.handshake()),
    "soap_version": (_SOAP_VERSION_MAP, SoapVersion.soap11()),
    "stomp_mode": (_STOMP_MODE_MAP, StompMode.produce()),
    "stomp_ack": (_STOMP_ACK_MAP, StompAck.auto()),
}


@dataclass
class RequestOptions:
    """Encapsulates common HTTP and execution parameters with validation."""

    timeout: int = DEFAULT_TIMEOUT_SECS
    chaos: bool = False
    chaos_rate: float = 0.1
    no_progress: bool = False
    method: str = DEFAULT_METHOD
    body: str | None = None
    form: list[tuple[str, str]] | None = None
    headers: list[tuple[str, str]] | None = None
    ws_mode: WsModeEnum = WsModeEnum.handshake
    ws_payload: str | None = None
    ws_persistent: bool = False
    ws_keepalive_secs: int | None = None
    ws_role: str | None = None
    ws_publish_interval_ms: int | None = None
    ws_subscribers: int | None = None
    grpc_service: str | None = None
    grpc_method: str | None = None
    grpc_payload: str | None = None
    grpc_deadline_ms: int | None = None
    proto_path: str | None = None
    grpc_use_reflection: bool = False
    http3_enabled: bool = False
    quic_zero_rtt: bool = False
    quic_max_idle_timeout_ms: int | None = None
    sse_enabled: bool = False
    sse_max_events: int | None = None
    graphql_query: str | None = None
    graphql_variables: str | None = None
    graphql_operation_name: str | None = None
    output_dir: str | None = None
    no_save: bool = False
    sys_sample_interval: int = DEFAULT_SYS_SAMPLE_INTERVAL_MS
    ws_max_buffer_bytes: int = DEFAULT_WS_MAX_BUFFER_BYTES
    ws_backpressure_warn_ratio: float = DEFAULT_WS_BACKPRESSURE_WARN_RATIO
    grpc_h2_multiplex: bool = False
    http3_migrate: bool = False
    http3_migrate_every: int = DEFAULT_HTTP3_MIGRATE_EVERY
    slo_p95_ms: float | None = None
    slo_p99_ms: float | None = None
    slo_error_rate: float | None = None
    slo_min_rps: float | None = None
    timeline_interval_ms: int = 1000
    soap_enabled: bool = False
    soap_action: str | None = None
    soap_version: SoapVersionEnum = SoapVersionEnum.soap11
    stomp_destination: str | None = None
    stomp_message: str | None = None
    stomp_mode: StompModeEnum = StompModeEnum.produce
    stomp_ack: StompAckEnum = StompAckEnum.auto
    stomp_receipt: bool = False
    stomp_headers: list[tuple[str, str]] | None = None
    db_query: str | None = None
    db_pool_size: int | None = None
    db_fetch: bool = False
    db_max_rows: int | None = None
    db_params: list[str] | None = None
    test_seed: int = DEFAULT_TEST_SEED

    def __post_init__(self) -> None:
        if self.timeout <= 0:
            raise ValueError("timeout must be greater than 0")
        if not 0 <= self.test_seed <= (1 << 64) - 1:
            raise ValueError("test_seed must be between 0 and 2**64 - 1")


def _test_config_options(opts: RequestOptions) -> dict[str, Any]:
    """Map ``RequestOptions`` fields onto ``TestConfig`` constructor kwargs.

    Names match one-to-one except ``timeout`` (``timeout_secs``); enum options
    are translated to their native PyO3 equivalents.
    """
    kwargs: dict[str, Any] = {}
    for field in fields(RequestOptions):
        value: Any = getattr(opts, field.name)
        enum_spec = _TEST_CONFIG_ENUMS.get(field.name)
        if enum_spec is not None:
            mapping, default = enum_spec
            value = mapping.get(value, default)
        kwargs[_TEST_CONFIG_RENAMES.get(field.name, field.name)] = value
    return kwargs


def _build_test_config(
    url: str, opts: RequestOptions, concurrency: int, duration: int
) -> TestConfig:
    """Build a TestConfig from RequestOptions with the given concurrency and duration."""
    return TestConfig(
        url=url,
        concurrency=concurrency,
        duration_secs=duration,
        **_test_config_options(opts),
    )


class StrobEngine:
    def __init__(
        self,
        url: str,
        concurrency: int = DEFAULT_CONCURRENCY,
        duration: int = DEFAULT_DURATION_SECS,
        options: RequestOptions | None = None,
        profile: LoadProfile | None = None,
    ) -> None:
        self._url = url
        self._options = options if options is not None else RequestOptions()
        self._profile = profile
        self._saved_report_path: str | None = None

        if profile is None:
            if concurrency <= 0:
                raise ValueError("Concurrency must be greater than 0")
            if concurrency > MAX_CONCURRENCY:
                raise ValueError(f"Concurrency must be <= {MAX_CONCURRENCY}")
            if duration <= 0:
                raise ValueError("Duration must be greater than 0")
            self.config = _build_test_config(url, self._options, concurrency, duration)
        else:
            self.config = _build_test_config(
                url,
                self._options,
                profile.max_concurrency(),
                profile.total_duration(),
            )

    @classmethod
    def load_test(
        cls,
        url: str,
        concurrency: int = DEFAULT_CONCURRENCY,
        duration: int = DEFAULT_DURATION_SECS,
        options: RequestOptions | None = None,
    ) -> "StrobEngine":
        return cls(
            url=url,
            concurrency=concurrency,
            duration=duration,
            options=options,
        )

    @classmethod
    def stress_test(
        cls,
        url: str,
        start_concurrency: int = DEFAULT_START_CONCURRENCY,
        max_concurrency: int = DEFAULT_MAX_CONCURRENCY,
        ramp_duration: int = DEFAULT_RAMP_SECS,
        hold_duration: int = DEFAULT_HOLD_SECS,
        options: RequestOptions | None = None,
    ) -> "StrobEngine":
        if start_concurrency <= 0:
            raise ValueError("start_concurrency must be greater than 0")
        if max_concurrency <= 0:
            raise ValueError("max_concurrency must be greater than 0")
        if max_concurrency > MAX_CONCURRENCY:
            raise ValueError(f"max_concurrency must be <= {MAX_CONCURRENCY}")
        if start_concurrency > max_concurrency:
            raise ValueError("start_concurrency must be <= max_concurrency")
        if ramp_duration < 0:
            raise ValueError("ramp_duration must be >= 0")
        if hold_duration < 0:
            raise ValueError("hold_duration must be >= 0")

        profile = LoadProfile.ramp(
            start_concurrency=start_concurrency,
            target_concurrency=max_concurrency,
            ramp_secs=ramp_duration,
            hold_secs=hold_duration,
        )
        return cls(
            url=url,
            profile=profile,
            options=options,
        )

    @classmethod
    def spike_test(
        cls,
        url: str,
        baseline: int = DEFAULT_BASELINE,
        peak_concurrency: int = DEFAULT_PEAK_CONCURRENCY,
        pre_spike_duration: int = DEFAULT_PRE_SPIKE_SECS,
        spike_duration: int = DEFAULT_SPIKE_SECS,
        post_spike_duration: int = DEFAULT_POST_SPIKE_SECS,
        options: RequestOptions | None = None,
    ) -> "StrobEngine":
        if baseline <= 0:
            raise ValueError("baseline must be greater than 0")
        if baseline > MAX_CONCURRENCY:
            raise ValueError(f"baseline must be <= {MAX_CONCURRENCY}")
        if peak_concurrency <= 0:
            raise ValueError("peak_concurrency must be greater than 0")
        if peak_concurrency > MAX_CONCURRENCY:
            raise ValueError(f"peak_concurrency must be <= {MAX_CONCURRENCY}")
        if pre_spike_duration < 0:
            raise ValueError("pre_spike_duration must be >= 0")
        if spike_duration < 0:
            raise ValueError("spike_duration must be >= 0")
        if post_spike_duration < 0:
            raise ValueError("post_spike_duration must be >= 0")

        profile = LoadProfile.spike(
            baseline_concurrency=baseline,
            peak_concurrency=peak_concurrency,
            pre_spike_secs=pre_spike_duration,
            spike_secs=spike_duration,
            post_spike_secs=post_spike_duration,
        )
        return cls(
            url=url,
            profile=profile,
            options=options,
        )

    def _enrich_summary(self, summary: TestSummary) -> TestSummary:
        enriched = summary.clone()
        enriched.timestamp = datetime.now(UTC).isoformat()
        enriched.workers = self.config.concurrency
        enriched.raw_command = (
            f"strobengine.run(url='{summary.url}', workers={enriched.workers})"
        )
        return enriched

    def run(self) -> TestSummary:
        if self._profile is not None:
            summary = run_load_profiles(self.config, self._profile)
        else:
            summary = run_load_test(self.config)
        enriched = self._enrich_summary(summary)
        # Persist artifact (single owner, always with enriched metadata)
        if not self._options.no_save:
            from strobengine.reporter import save_report

            self._saved_report_path = save_report(
                enriched,
                self.config,
                output_dir=self._options.output_dir,
                no_save=self._options.no_save,
            )
        else:
            self._saved_report_path = None
        return enriched

    async def run_async(self) -> TestSummary:
        return await asyncio.to_thread(self.run)

    def get_config(self) -> TestConfig:
        """Return the active TestConfig."""
        return self.config

    @property
    def saved_report_path(self) -> str | None:
        """Return path to the last saved report artifact, or None."""
        return self._saved_report_path
