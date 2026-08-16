from typing import Any

class WsMode:
    @staticmethod
    def handshake() -> WsMode: ...
    @staticmethod
    def ping_pong() -> WsMode: ...
    @staticmethod
    def stream() -> WsMode: ...

class LoadProfile:
    @staticmethod
    def constant(concurrency: int = 10, duration_secs: int = 10) -> LoadProfile: ...
    @staticmethod
    def ramp(
        start_concurrency: int,
        target_concurrency: int,
        ramp_secs: int,
        hold_secs: int,
    ) -> LoadProfile: ...
    @staticmethod
    def spike(
        baseline_concurrency: int,
        peak_concurrency: int,
        pre_spike_secs: int,
        spike_secs: int,
        post_spike_secs: int,
    ) -> LoadProfile: ...
    def total_duration(self) -> int: ...
    def max_concurrency(self) -> int: ...
    def target_concurrency(self, elapsed: float) -> int: ...

class TestConfig:
    url: str
    concurrency: int
    duration_secs: int
    timeout_secs: int
    chaos: bool
    chaos_rate: float
    no_progress: bool
    method: str
    body: str | None
    form: list[tuple[str, str]] | None
    headers: list[tuple[str, str]] | None
    ws_mode: WsMode
    ws_payload: str | None
    grpc_service: str | None
    grpc_method: str | None
    grpc_payload: str | None
    grpc_deadline_ms: int | None
    def __init__(
        self,
        url: str,
        concurrency: int = 10,
        duration_secs: int = 10,
        timeout_secs: int = 10,
        chaos: bool = False,
        chaos_rate: float = 0.1,
        no_progress: bool = False,
        method: str = "GET",
        body: str | None = None,
        form: list[tuple[str, str]] | None = None,
        headers: list[tuple[str, str]] | None = None,
        ws_mode: WsMode | None = None,
        ws_payload: str | None = None,
        grpc_service: str | None = None,
        grpc_method: str | None = None,
        grpc_payload: str | None = None,
        grpc_deadline_ms: int | None = None,
    ) -> None: ...

class TestSummary:
    @property
    def url(self) -> str: ...
    @property
    def total_requests(self) -> int: ...
    @property
    def total_errors(self) -> int: ...
    @property
    def average_latency_ms(self) -> float: ...
    @property
    def p95_latency_ms(self) -> float: ...
    @property
    def p99_latency_ms(self) -> float: ...
    @property
    def min_latency_ms(self) -> float: ...
    @property
    def p50_latency_ms(self) -> float: ...
    @property
    def p90_latency_ms(self) -> float: ...
    @property
    def max_latency_ms(self) -> float: ...
    @property
    def total_bytes_received(self) -> int: ...
    @property
    def duration_secs(self) -> float: ...
    @property
    def workers(self) -> int: ...
    @property
    def timestamp(self) -> str: ...
    @property
    def raw_command(self) -> str | None: ...
    @property
    def status_codes(self) -> dict[int, int]: ...
    def to_dict(self) -> dict[str, Any]: ...
    def to_json(self, indent: int | None = None) -> str: ...

def init_logging(level: str, log_file: str | None = None) -> None: ...
def run_load_test(config: TestConfig) -> TestSummary: ...
def run_load_profiles(
    url: str,
    timeout_secs: int,
    profile: LoadProfile,
    chaos: bool = False,
    chaos_rate: float = 0.1,
    no_progress: bool = False,
    method: str = "GET",
    body: str | None = None,
    form: list[tuple[str, str]] | None = None,
    headers: list[tuple[str, str]] | None = None,
) -> TestSummary: ...
