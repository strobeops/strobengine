import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime

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


@dataclass
class RequestOptions:
    """Encapsulates common HTTP and execution parameters with validation."""

    timeout: int = DEFAULT_TIMEOUT_SECS
    chaos: bool = False
    no_progress: bool = False
    method: str = "GET"
    body: str | None = None
    form: list[tuple[str, str]] | None = None
    headers: list[tuple[str, str]] = field(default_factory=list)
    ws_mode: str = "handshake"
    ws_payload: str | None = None
    grpc_service: str | None = None
    grpc_method: str | None = None
    grpc_payload: str | None = None
    grpc_deadline_ms: int | None = None

    def __post_init__(self) -> None:
        if self.timeout <= 0:
            raise ValueError("timeout must be greater than 0")


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

        if profile is None:
            if concurrency <= 0:
                raise ValueError("Concurrency must be greater than 0")
            if duration <= 0:
                raise ValueError("Duration must be greater than 0")

            self.config = TestConfig(
                url=url,
                concurrency=concurrency,
                duration_secs=duration,
                timeout_secs=self._options.timeout,
                chaos=self._options.chaos,
                no_progress=self._options.no_progress,
                method=self._options.method,
                body=self._options.body,
                form=self._options.form,
                headers=self._options.headers,
                ws_mode=WsMode.ping_pong()
                if self._options.ws_mode == "ping_pong"
                else WsMode.stream()
                if self._options.ws_mode == "stream"
                else WsMode.handshake(),
                ws_payload=self._options.ws_payload,
                grpc_service=self._options.grpc_service,
                grpc_method=self._options.grpc_method,
                grpc_payload=self._options.grpc_payload,
                grpc_deadline_ms=self._options.grpc_deadline_ms,
            )
            self._profile = None
        else:
            self.config = None

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
        summary.timestamp = datetime.now(UTC).isoformat()
        if self.config is not None:
            summary.workers = self.config.concurrency
        summary.raw_command = (
            f"strobengine.run(url='{summary.url}', workers={summary.workers})"
        )
        return summary

    def run(self) -> TestSummary:
        opts = self._options
        if self._profile is not None:
            summary = run_load_profiles(
                self._url,
                opts.timeout,
                self._profile,
                opts.chaos,
                no_progress=opts.no_progress,
                method=opts.method,
                body=opts.body,
                form=opts.form,
                headers=opts.headers,
            )
        else:
            summary = run_load_test(self.config)
        return self._enrich_summary(summary)

    async def run_async(self) -> TestSummary:
        return await asyncio.to_thread(self.run)
