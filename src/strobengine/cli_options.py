"""Shared Typer option definitions and request-building helpers for the CLI.

The ``load``, ``stress``, and ``spike`` subcommands previously triplicated an
~185-line block of identical ``Annotated[type, typer.Option(...)]`` definitions.
Each option is now declared once here as a module-level alias and reused across
commands, so a change to a flag/`min`/help lives in a single place. The command
functions still supply the per-parameter default (``= 10`` etc.) in their
signature; only the verbose ``Annotated[...]`` metadata is shared.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Annotated

import typer

from strobengine.engine import RequestOptions, WsModeEnum

VALID_METHODS = {"GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"}


class WsRole(StrEnum):
    publisher = "publisher"
    subscriber = "subscriber"


# --- Positional argument --------------------------------------------------

UrlArg = Annotated[str, typer.Argument(help="Target HTTP/HTTPS URL")]

# --- Shared options (identical across all three subcommands) --------------

TimeoutOpt = Annotated[
    int,
    typer.Option("-t", "--timeout", min=1, help="Request timeout in seconds"),
]
MethodOpt = Annotated[
    str,
    typer.Option(
        "--method",
        help="HTTP method (GET, POST, PUT, DELETE, PATCH, HEAD, OPTIONS)",
    ),
]
BodyOpt = Annotated[
    str | None, typer.Option("--body", help="Request body (raw string)")
]
FormOpt = Annotated[
    str | None,
    typer.Option("--form", help="Form data body (e.g. key1=val1&key2=val2)"),
]
HeaderOpt = Annotated[
    list[str] | None,
    typer.Option("--header", help="Custom header key:value (repeatable)"),
]
ChaosOpt = Annotated[
    bool, typer.Option("--chaos", help="Enable fault injection (~10%% of requests)")
]
NoProgressOpt = Annotated[
    bool, typer.Option("--no-progress", help="Suppress live progress output")
]
JsonOutputOpt = Annotated[bool, typer.Option("--json", help="Output raw JSON results")]
VerboseOpt = Annotated[
    int,
    typer.Option("-v", "--verbose", count=True, help="Increase verbosity"),
]
QuietOpt = Annotated[bool, typer.Option("-q", "--quiet", help="Suppress all output")]
LogFileOpt = Annotated[
    str | None, typer.Option("--log-file", help="Write logs to file")
]
WsModeOpt = Annotated[
    WsModeEnum | None,
    typer.Option(
        "--ws-mode",
        help="WebSocket mode: handshake, ping_pong, stream",
        case_sensitive=False,
    ),
]
WsPayloadOpt = Annotated[
    str | None,
    typer.Option("--ws-payload", help="WebSocket payload for stream mode"),
]
WsPersistentOpt = Annotated[
    bool,
    typer.Option(
        "--ws-persistent/--no-ws-persistent",
        help="Use persistent WebSocket connections",
    ),
]
WsKeepaliveSecsOpt = Annotated[
    int | None,
    typer.Option("--ws-keepalive-secs", help="WebSocket keepalive interval in seconds"),
]
WsRoleOpt = Annotated[
    WsRole | None,
    typer.Option(
        "--ws-role",
        help="WebSocket Pub/Sub role: publisher, subscriber",
        case_sensitive=False,
    ),
]
WsPublishIntervalOpt = Annotated[
    int | None,
    typer.Option("--ws-publish-interval", help="Publisher send interval in ms"),
]
WsSubscribersOpt = Annotated[
    int | None,
    typer.Option("--ws-subscribers", help="Number of subscriber workers"),
]
GrpcServiceOpt = Annotated[
    str | None,
    typer.Option("--grpc-service", help="gRPC service name (e.g. helloworld.Greeter)"),
]
GrpcMethodOpt = Annotated[
    str | None,
    typer.Option("--grpc-method", help="gRPC method name (e.g. SayHello)"),
]
GrpcPayloadOpt = Annotated[
    str | None,
    typer.Option("--grpc-payload", help="Base64-encoded protobuf payload"),
]
GrpcDeadlineMsOpt = Annotated[
    int | None,
    typer.Option("--grpc-deadline-ms", help="gRPC deadline in milliseconds"),
]
ProtoPathOpt = Annotated[
    str | None,
    typer.Option(
        "--proto-path", help="Path to .proto file for JSON payload conversion"
    ),
]
GrpcUseReflectionOpt = Annotated[
    bool,
    typer.Option(
        "--grpc-use-reflection",
        help="Use server reflection for schema discovery",
    ),
]
Http3Opt = Annotated[
    bool, typer.Option("--http3/--no-http3", help="Enable HTTP/3 over QUIC")
]
QuicZeroRttOpt = Annotated[
    bool,
    typer.Option("--quic-zero-rtt", help="Enable QUIC 0-RTT connection testing"),
]
QuicMaxIdleTimeoutOpt = Annotated[
    int | None,
    typer.Option("--quic-max-idle-timeout", help="QUIC max idle timeout in ms"),
]
SseEnabledOpt = Annotated[
    bool, typer.Option("--sse/--no-sse", help="Enable SSE streaming mode")
]
SseMaxEventsOpt = Annotated[
    int | None,
    typer.Option("--sse-max-events", help="Maximum events to receive per connection"),
]
OutputDirOpt = Annotated[
    str | None,
    typer.Option("--output-dir", help="Report output directory"),
]
NoSaveOpt = Annotated[
    bool, typer.Option("--no-save", help="Disable report persistence")
]
SysSampleIntervalOpt = Annotated[
    int,
    typer.Option(
        "--sys-sample-interval",
        min=0,
        help="Resource monitor sample interval in ms (0 to disable)",
    ),
]
WsMaxBufferBytesOpt = Annotated[
    int,
    typer.Option(
        "--ws-max-buffer-bytes",
        min=1,
        help="WebSocket outbound write-buffer capacity for the backpressure gauge",
    ),
]
WsBackpressureWarnRatioOpt = Annotated[
    float,
    typer.Option(
        "--ws-backpressure-warn-ratio",
        min=0.0,
        max=1.0,
        help="Warn when WebSocket in-flight bytes exceed this fraction of the buffer",
    ),
]
HtmlOpt = Annotated[
    str | None,
    typer.Option("--html", help="Generate standalone HTML report"),
]
CompareToOpt = Annotated[
    str | None,
    typer.Option("--compare-to", help="Baseline JSON report path for comparison"),
]
MarkdownOpt = Annotated[
    str | None,
    typer.Option("--markdown", help="Export results as Markdown report"),
]
JunitOpt = Annotated[
    str | None,
    typer.Option("--junit", help="Export results as JUnit XML report"),
]
CsvOpt = Annotated[
    str | None,
    typer.Option("--csv", help="Export results as CSV report"),
]

# --- Profile-specific options (per subcommand) ----------------------------

ConcurrencyOpt = Annotated[
    int,
    typer.Option("-c", "--concurrency", min=1, help="Number of concurrent workers"),
]
DurationOpt = Annotated[
    int,
    typer.Option("-d", "--duration", min=1, help="Test duration in seconds"),
]
FromOpt = Annotated[
    int,
    typer.Option("--from", help="Starting concurrency", min=1),
]
ToOpt = Annotated[
    int,
    typer.Option("--to", help="Target concurrency", min=1),
]
RampOpt = Annotated[
    int,
    typer.Option("--ramp", help="Ramp duration in seconds", min=1),
]
HoldOpt = Annotated[
    int,
    typer.Option("--hold", help="Hold duration in seconds", min=0),
]
BaselineOpt = Annotated[
    int,
    typer.Option("--baseline", help="Baseline concurrency", min=1),
]
PeakOpt = Annotated[
    int,
    typer.Option("--peak", help="Peak concurrency", min=1),
]
PreSpikeOpt = Annotated[
    int,
    typer.Option("--pre-spike", help="Pre-spike duration in seconds", min=0),
]
SpikeDurationOpt = Annotated[
    int,
    typer.Option("--spike-duration", help="Spike duration in seconds", min=1),
]
PostSpikeOpt = Annotated[
    int,
    typer.Option("--post-spike", help="Post-spike duration in seconds", min=0),
]


# Subset of parsed parameters that map onto RequestOptions. Kept as an explicit
# allow-list so profile args and export/logging flags are routed elsewhere.
_REQUEST_FIELDS = frozenset(
    {
        "timeout",
        "method",
        "body",
        "form",
        "header",
        "chaos",
        "no_progress",
        "ws_mode",
        "ws_payload",
        "ws_persistent",
        "ws_keepalive_secs",
        "ws_role",
        "ws_publish_interval_ms",
        "ws_subscribers",
        "grpc_service",
        "grpc_method",
        "grpc_payload",
        "grpc_deadline_ms",
        "proto_path",
        "grpc_use_reflection",
        "http3_enabled",
        "quic_zero_rtt",
        "quic_max_idle_timeout_ms",
        "sse_enabled",
        "sse_max_events",
        "output_dir",
        "no_save",
        "sys_sample_interval",
        "ws_max_buffer_bytes",
        "ws_backpressure_warn_ratio",
    }
)


def _validate_method(method: str) -> str:
    """Normalizes and validates the HTTP method against supported verbs."""
    upper_method = method.strip().upper()
    if upper_method not in VALID_METHODS:
        valid_list = ", ".join(sorted(VALID_METHODS))
        raise typer.BadParameter(
            f"Invalid HTTP method '{method}'. Must be one of: {valid_list}"
        )
    return upper_method


def _parse_headers(header: list[str] | None) -> list[tuple[str, str]] | None:
    """
    Parses CLI header flags into a list of key-value tuples.
    Preserves duplicate header names (e.g., multiple 'Set-Cookie' or 'Accept' flags).
    """
    if not header:
        return None

    parsed_headers: list[tuple[str, str]] = []

    for h in header:
        if ":" not in h:
            raise typer.BadParameter(f"Header '{h}' must be in 'Key: Value' format.")
        key, value = h.split(":", 1)
        parsed_headers.append((key.strip(), value.strip()))
    return parsed_headers


def _parse_form(form_str: str | None) -> list[tuple[str, str]] | None:
    """Parses a URL-encoded form string into decoded key-value pairs."""
    from urllib.parse import unquote_plus

    if not form_str:
        return None
    pairs = []
    for item in form_str.split("&"):
        if "=" in item:
            k, v = item.split("=", 1)
            pairs.append((unquote_plus(k), unquote_plus(v)))
        elif item:
            pairs.append((unquote_plus(item), ""))
    return pairs or None


def _build_request_options(**kwargs: object) -> RequestOptions:
    ws_mode_raw = kwargs.get("ws_mode")
    ws_role_raw = kwargs.get("ws_role")
    return RequestOptions(
        timeout=kwargs["timeout"],
        method=_validate_method(kwargs["method"]),
        body=kwargs["body"],
        form=_parse_form(kwargs.get("form")),
        headers=_parse_headers(kwargs.get("header")),
        chaos=kwargs["chaos"],
        no_progress=kwargs["no_progress"],
        ws_mode=WsModeEnum(ws_mode_raw) if ws_mode_raw else WsModeEnum.handshake,
        ws_payload=kwargs.get("ws_payload"),
        ws_persistent=kwargs.get("ws_persistent", False),
        ws_keepalive_secs=kwargs.get("ws_keepalive_secs"),
        ws_role=ws_role_raw.value if hasattr(ws_role_raw, "value") else ws_role_raw,
        ws_publish_interval_ms=kwargs.get("ws_publish_interval_ms"),
        ws_subscribers=kwargs.get("ws_subscribers"),
        grpc_service=kwargs.get("grpc_service"),
        grpc_method=kwargs.get("grpc_method"),
        grpc_payload=kwargs.get("grpc_payload"),
        grpc_deadline_ms=kwargs.get("grpc_deadline_ms"),
        proto_path=kwargs.get("proto_path"),
        grpc_use_reflection=kwargs.get("grpc_use_reflection", False),
        http3_enabled=kwargs.get("http3_enabled", False),
        quic_zero_rtt=kwargs.get("quic_zero_rtt", False),
        quic_max_idle_timeout_ms=kwargs.get("quic_max_idle_timeout_ms"),
        sse_enabled=kwargs.get("sse_enabled", False),
        sse_max_events=kwargs.get("sse_max_events"),
        output_dir=kwargs.get("output_dir"),
        no_save=kwargs.get("no_save", False),
        sys_sample_interval=kwargs.get("sys_sample_interval", 1000),
        ws_max_buffer_bytes=kwargs.get("ws_max_buffer_bytes", 1_048_576),
        ws_backpressure_warn_ratio=kwargs.get("ws_backpressure_warn_ratio", 0.8),
    )
