"""Shared Typer option definitions and request-building helpers for the CLI.

The ``load``, ``stress``, and ``spike`` subcommands previously triplicated an
~185-line block of identical ``Annotated[type, typer.Option(...)]`` definitions.
Each option is now declared once here as a module-level alias and reused across
commands, so a change to a flag/`min`/help lives in a single place. The command
functions still supply the per-parameter default (``= 10`` etc.) in their
signature; only the verbose ``Annotated[...]`` metadata is shared.
"""

from __future__ import annotations

from dataclasses import fields
from enum import StrEnum
from typing import Annotated, Any

import typer

from strobengine.engine import (
    RequestOptions,
    SoapVersionEnum,
    StompAckEnum,
    StompModeEnum,
    WsModeEnum,
)

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
GraphqlQueryOpt = Annotated[
    str | None,
    typer.Option(
        "--graphql-query",
        help=(
            "GraphQL query/mutation document (HTTP POST, or a subscription "
            "when the URL is ws:// or wss://)"
        ),
    ),
]
GraphqlVariablesOpt = Annotated[
    str | None,
    typer.Option("--graphql-variables", help="JSON object of GraphQL variables"),
]
GraphqlOperationNameOpt = Annotated[
    str | None,
    typer.Option("--graphql-operation-name", help="Named GraphQL operation to execute"),
]
SoapOpt = Annotated[
    bool,
    typer.Option(
        "--soap/--no-soap",
        help=(
            "Enable SOAP mode: POST the XML envelope from --body with SOAP "
            "content-type/SOAPAction and count <Fault> responses as errors"
        ),
    ),
]
SoapActionOpt = Annotated[
    str | None,
    typer.Option(
        "--soap-action",
        help=(
            "SOAP operation name: SOAP 1.1 SOAPAction header value, or the "
            "action= parameter in the SOAP 1.2 Content-Type (optional)"
        ),
    ),
]
SoapVersionOpt = Annotated[
    SoapVersionEnum,
    typer.Option(
        "--soap-version",
        help=(
            "SOAP 1.1 (text/xml + SOAPAction header) or 1.2 "
            "(application/soap+xml + action parameter)"
        ),
        case_sensitive=False,
    ),
]
StompDestinationOpt = Annotated[
    str | None,
    typer.Option(
        "--stomp-destination",
        help="STOMP queue/topic destination (e.g. /queue/orders); required for stomp:// URLs",
    ),
]
StompMessageOpt = Annotated[
    str | None,
    typer.Option("--stomp-message", help="STOMP message body to publish"),
]
StompModeOpt = Annotated[
    StompModeEnum,
    typer.Option(
        "--stomp-mode",
        help="STOMP mode: produce (SEND) or consume (SUBSCRIBE/MESSAGE)",
        case_sensitive=False,
    ),
]
StompAckOpt = Annotated[
    StompAckEnum,
    typer.Option(
        "--stomp-ack",
        help="STOMP subscription ack mode: auto, client, client-individual",
        case_sensitive=False,
    ),
]
StompReceiptOpt = Annotated[
    bool,
    typer.Option(
        "--stomp-receipt/--no-stomp-receipt",
        help="Await a broker RECEIPT per SEND (true ack latency) instead of fire-and-forget",
    ),
]
StompHeaderOpt = Annotated[
    list[str] | None,
    typer.Option("--stomp-header", help="Custom STOMP header key:value (repeatable)"),
]
DbQueryOpt = Annotated[
    str | None,
    typer.Option(
        "--db-query",
        help="SQL query executed per iteration (postgres://, mysql://, sqlite:// URLs)",
    ),
]
DbPoolSizeOpt = Annotated[
    int | None,
    typer.Option(
        "--db-pool-size",
        min=1,
        help="Maximum pooled database connections (default: concurrency)",
    ),
]
DbFetchOpt = Annotated[
    bool,
    typer.Option(
        "--db-fetch/--no-db-fetch",
        help="Fetch and count returned rows instead of using rows affected",
    ),
]
DbMaxRowsOpt = Annotated[
    int | None,
    typer.Option(
        "--db-max-rows", min=0, help="Cap rows fetched per query (0 = unlimited)"
    ),
]
DbParamOpt = Annotated[
    list[str] | None,
    typer.Option(
        "--db-param", help="SQL bind parameter as text (repeatable, positional)"
    ),
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
GrpcH2MultiplexOpt = Annotated[
    bool,
    typer.Option(
        "--grpc-h2-multiplex/--no-grpc-h2-multiplex",
        help="Use a multiplexed raw-h2 gRPC engine emitting stream/window metrics (grpc:// only)",
    ),
]
Http3MigrateOpt = Annotated[
    bool,
    typer.Option(
        "--http3-migrate/--no-http3-migrate",
        help="Opt-in QUIC connection migration (periodic socket rebind) for HTTP/3 metrics",
    ),
]
Http3MigrateEveryOpt = Annotated[
    int,
    typer.Option(
        "--http3-migrate-every",
        min=1,
        help="Perform an HTTP/3 migration every N iterations on a persistent session",
    ),
]
SloP95MsOpt = Annotated[
    float | None,
    typer.Option("--slo-p95-ms", help="Fail the run if P95 latency exceeds this (ms)"),
]
SloP99MsOpt = Annotated[
    float | None,
    typer.Option("--slo-p99-ms", help="Fail the run if P99 latency exceeds this (ms)"),
]
SloErrorRateOpt = Annotated[
    float | None,
    typer.Option(
        "--slo-error-rate",
        help="Fail the run if the error rate exceeds this (percent)",
    ),
]
SloMinRpsOpt = Annotated[
    float | None,
    typer.Option("--slo-min-rps", help="Fail the run if RPS falls below this"),
]
TimelineIntervalOpt = Annotated[
    int,
    typer.Option(
        "--timeline-interval",
        min=0,
        help="Sample interval for the RPS/latency timeline in ms (0 to disable)",
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


# CLI parameter names that feed RequestOptions. Derived from the dataclass so a
# new option is picked up automatically; only the renamed flags and the fields
# intentionally hidden from the CLI are listed manually.
_CLI_ALIASES: dict[str, str] = {
    "header": "headers",
    "stomp_header": "stomp_headers",
    "db_param": "db_params",
}
_HIDDEN_CLI_FIELDS = frozenset({"chaos_rate", "headers", "stomp_headers", "db_params"})

_REQUEST_FIELDS = frozenset(
    ({field.name for field in fields(RequestOptions)} - _HIDDEN_CLI_FIELDS)
    | set(_CLI_ALIASES)
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


def _build_request_options(**values: Any) -> RequestOptions:
    """Assemble ``RequestOptions`` from the parsed CLI option values.

    Accepts the full filtered set (via the ``_REQUEST_FIELDS`` splat) or a
    partial subset. Only flags whose representation differs are handled
    explicitly; every other field maps one-to-one onto ``RequestOptions``.
    """
    kwargs: dict[str, Any] = {}

    kwargs["headers"] = _parse_headers(values.pop("header", None))
    kwargs["stomp_headers"] = _parse_headers(values.pop("stomp_header", None))
    kwargs["db_params"] = values.pop("db_param", None)

    kwargs["method"] = _validate_method(values.pop("method", RequestOptions.method))
    kwargs["form"] = _parse_form(values.pop("form", None))

    ws_mode = values.pop("ws_mode", None)
    kwargs["ws_mode"] = ws_mode if ws_mode is not None else RequestOptions.ws_mode
    ws_role = values.pop("ws_role", None)
    kwargs["ws_role"] = ws_role.value if ws_role is not None else None

    kwargs.update(values)
    return RequestOptions(**kwargs)
