import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum

from strobengine._strobengine import (
    LoadProfile,
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
    DEFAULT_MAX_CONCURRENCY,
    DEFAULT_PEAK_CONCURRENCY,
    DEFAULT_POST_SPIKE_SECS,
    DEFAULT_PRE_SPIKE_SECS,
    DEFAULT_RAMP_SECS,
    DEFAULT_SPIKE_SECS,
    DEFAULT_START_CONCURRENCY,
    DEFAULT_TIMEOUT_SECS,
)


class WsModeEnum(StrEnum):
    handshake = "handshake"
    ping_pong = "ping_pong"
    stream = "stream"


_WS_MODE_MAP: dict[WsModeEnum, WsMode] = {
    WsModeEnum.handshake: WsMode.handshake(),
    WsModeEnum.ping_pong: WsMode.ping_pong(),
    WsModeEnum.stream: WsMode.stream(),
}


@dataclass
class RequestOptions:
    """Encapsulates common HTTP and execution parameters with validation."""

    timeout: int = DEFAULT_TIMEOUT_SECS
    chaos: bool = False
    chaos_rate: float = 0.1
    no_progress: bool = False
    method: str = "GET"
    body: str | None = None
    form: list[tuple[str, str]] | None = None
    headers: list[tuple[str, str]] = field(default_factory=list)
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
    output_dir: str | None = None
    no_save: bool = False
    sys_sample_interval: int = 1000

    def __post_init__(self) -> None:
        if self.timeout <= 0:
            raise ValueError("timeout must be greater than 0")


def _build_test_config(
    url: str, opts: RequestOptions, concurrency: int, duration: int
) -> TestConfig:
    """Build a TestConfig from RequestOptions with the given concurrency and duration."""
    return TestConfig(
        url=url,
        concurrency=concurrency,
        duration_secs=duration,
        timeout_secs=opts.timeout,
        chaos=opts.chaos,
        chaos_rate=opts.chaos_rate,
        no_progress=opts.no_progress,
        method=opts.method,
        body=opts.body,
        form=opts.form,
        headers=opts.headers,
        ws_mode=_WS_MODE_MAP.get(opts.ws_mode, WsMode.handshake()),
        ws_payload=opts.ws_payload,
        ws_persistent=opts.ws_persistent,
        ws_keepalive_secs=opts.ws_keepalive_secs,
        ws_role=opts.ws_role,
        ws_publish_interval_ms=opts.ws_publish_interval_ms,
        ws_subscribers=opts.ws_subscribers,
        grpc_service=opts.grpc_service,
        grpc_method=opts.grpc_method,
        grpc_payload=opts.grpc_payload,
        grpc_deadline_ms=opts.grpc_deadline_ms,
        proto_path=opts.proto_path,
        grpc_use_reflection=opts.grpc_use_reflection,
        http3_enabled=opts.http3_enabled,
        quic_zero_rtt=opts.quic_zero_rtt,
        quic_max_idle_timeout_ms=opts.quic_max_idle_timeout_ms,
        sse_enabled=opts.sse_enabled,
        sse_max_events=opts.sse_max_events,
        output_dir=opts.output_dir,
        no_save=opts.no_save,
        sys_sample_interval=opts.sys_sample_interval,
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
        if peak_concurrency <= 0:
            raise ValueError("peak_concurrency must be greater than 0")
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

    def get_config(self):
        """Return the active TestConfig."""
        return self.config

    @property
    def saved_report_path(self) -> str | None:
        """Return path to the last saved report artifact, or None."""
        return self._saved_report_path
