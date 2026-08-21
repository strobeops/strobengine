from __future__ import annotations

import sys
from enum import StrEnum
from typing import Annotated

import typer
from typer.core import TyperOption
from typer.main import get_command

from strobengine._strobengine import TestSummary, init_logging
from strobengine.engine import RequestOptions, StrobEngine
from strobengine.reporter import print_summary

VALID_METHODS = {"GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"}


class WsRole(StrEnum):
    publisher = "publisher"
    subscriber = "subscriber"


def _get_version() -> str:
    from importlib.metadata import PackageNotFoundError, version

    try:
        return version("strobengine")
    except PackageNotFoundError:
        return "0.0.0-dev"


def _version_callback(value: bool) -> None:
    if value:
        typer.echo(f"strobengine {_get_version()}")
        raise typer.Exit()


def _resolve_log_level(verbose_count: int, quiet: bool) -> str:
    if quiet:
        return "off"
    return {0: "warn", 1: "info", 2: "debug"}.get(verbose_count, "trace")


def _configure_logging(level: str, log_file: str | None = None) -> None:
    import logging

    TRACE = 5
    logging.addLevelName(TRACE, "TRACE")

    python_level = {
        "off": logging.CRITICAL + 1,
        "warn": logging.WARNING,
        "info": logging.INFO,
        "debug": logging.DEBUG,
        "trace": TRACE,
    }.get(level, logging.WARNING)

    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stderr)]
    if log_file:
        handlers.append(logging.FileHandler(log_file))

    logging.basicConfig(
        level=python_level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=handlers,
        force=True,
    )
    init_logging(level, log_file)


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
    """Parses a URL-encoded form string (e.g. 'key1=val1&key2=val2') into key-value pairs."""
    if not form_str:
        return None
    pairs = []
    for item in form_str.split("&"):
        if "=" in item:
            k, v = item.split("=", 1)
            pairs.append((k, v))
        elif item:
            pairs.append((item, ""))
    return pairs or None


def _collect_value_flags(app: typer.Typer) -> set[str]:
    """Build the set of flags that consume the next argument."""
    root = get_command(app)
    flags: set[str] = set()
    for cmd in root.commands.values():
        for param in cmd.params:
            if isinstance(param, TyperOption) and not param.is_flag and not param.count:
                flags.update(param.opts)
    return flags


def _validate_method(method: str) -> str:
    """Normalizes and validates the HTTP method against supported verbs."""
    upper_method = method.strip().upper()
    if upper_method not in VALID_METHODS:
        valid_list = ", ".join(sorted(VALID_METHODS))
        raise typer.BadParameter(
            f"Invalid HTTP method '{method}'. Must be one of: {valid_list}"
        )
    return upper_method


app = typer.Typer(
    name="strobengine",
    help="High-performance load testing engine powered by Rust.",
    no_args_is_help=True,
)

KNOWN_SUBCOMMANDS = {"load", "stress", "spike"}
HELP_FLAGS = {"-h", "--help"}
VERSION_FLAGS = {"-V", "--version"}


@app.callback()
def _global_options(
    version: Annotated[
        bool,
        typer.Option(
            "-V",
            "--version",
            help="Show version and exit",
            is_eager=True,
            callback=_version_callback,
        ),
    ] = False,
) -> None:
    pass


def _output_results(
    summary: TestSummary, url: str, duration_secs: int, json_output: bool
) -> None:
    print_summary(summary, json_output=json_output)


def _build_request_options(
    timeout: int,
    method: str,
    body: str | None,
    form: str | None,
    header: list[str] | None,
    chaos: bool,
    no_progress: bool,
    ws_mode: str | None,
    ws_payload: str | None,
    ws_role: WsRole | None,
    ws_publish_interval_ms: int | None,
    ws_subscribers: int | None,
    grpc_service: str | None,
    grpc_method: str | None,
    grpc_payload: str | None,
    grpc_deadline_ms: int | None,
    proto_path: str | None,
    grpc_use_reflection: bool,
    http3_enabled: bool,
    quic_zero_rtt: bool,
    quic_max_idle_timeout_ms: int | None,
) -> RequestOptions:
    return RequestOptions(
        timeout=timeout,
        method=method,
        body=body,
        form=_parse_form(form),
        headers=_parse_headers(header),
        chaos=chaos,
        no_progress=no_progress,
        ws_mode=ws_mode or "handshake",
        ws_payload=ws_payload,
        ws_role=ws_role.value if ws_role is not None else None,
        ws_publish_interval_ms=ws_publish_interval_ms,
        ws_subscribers=ws_subscribers,
        grpc_service=grpc_service,
        grpc_method=grpc_method,
        grpc_payload=grpc_payload,
        grpc_deadline_ms=grpc_deadline_ms,
        proto_path=proto_path,
        grpc_use_reflection=grpc_use_reflection,
        http3_enabled=http3_enabled,
        quic_zero_rtt=quic_zero_rtt,
        quic_max_idle_timeout_ms=quic_max_idle_timeout_ms,
    )


@app.command()
def load(
    url: Annotated[str, typer.Argument(help="Target HTTP/HTTPS URL")],
    concurrency: Annotated[
        int,
        typer.Option("-c", "--concurrency", min=1, help="Number of concurrent workers"),
    ] = 10,
    duration: Annotated[
        int,
        typer.Option("-d", "--duration", min=1, help="Test duration in seconds"),
    ] = 10,
    timeout: Annotated[
        int,
        typer.Option("-t", "--timeout", min=1, help="Request timeout in seconds"),
    ] = 10,
    method: Annotated[
        str,
        typer.Option(
            "--method",
            help="HTTP method (GET, POST, PUT, DELETE, PATCH, HEAD, OPTIONS)",
        ),
    ] = "GET",
    body: Annotated[
        str | None, typer.Option("--body", help="Request body (raw string)")
    ] = None,
    form: Annotated[
        str | None,
        typer.Option("--form", help="Form data body (e.g. key1=val1&key2=val2)"),
    ] = None,
    header: Annotated[
        list[str] | None,
        typer.Option("--header", help="Custom header key:value (repeatable)"),
    ] = None,
    chaos: Annotated[
        bool, typer.Option("--chaos", help="Enable fault injection (~10%% of requests)")
    ] = False,
    no_progress: Annotated[
        bool, typer.Option("--no-progress", help="Suppress live progress output")
    ] = False,
    json_output: Annotated[
        bool, typer.Option("--json", help="Output raw JSON results")
    ] = False,
    verbose: Annotated[
        int,
        typer.Option("-v", "--verbose", count=True, help="Increase verbosity"),
    ] = 0,
    quiet: Annotated[
        bool, typer.Option("-q", "--quiet", help="Suppress all output")
    ] = False,
    log_file: Annotated[
        str | None, typer.Option("--log-file", help="Write logs to file")
    ] = None,
    ws_mode: Annotated[
        str | None,
        typer.Option(
            "--ws-mode",
            help="WebSocket mode: handshake, ping_pong, stream",
        ),
    ] = None,
    ws_payload: Annotated[
        str | None,
        typer.Option("--ws-payload", help="WebSocket payload for stream mode"),
    ] = None,
    ws_role: Annotated[
        WsRole | None,
        typer.Option(
            "--ws-role",
            help="WebSocket Pub/Sub role: publisher, subscriber",
            case_sensitive=False,
        ),
    ] = None,
    ws_publish_interval_ms: Annotated[
        int | None,
        typer.Option("--ws-publish-interval", help="Publisher send interval in ms"),
    ] = None,
    ws_subscribers: Annotated[
        int | None,
        typer.Option("--ws-subscribers", help="Number of subscriber workers"),
    ] = None,
    grpc_service: Annotated[
        str | None,
        typer.Option(
            "--grpc-service", help="gRPC service name (e.g. helloworld.Greeter)"
        ),
    ] = None,
    grpc_method: Annotated[
        str | None,
        typer.Option("--grpc-method", help="gRPC method name (e.g. SayHello)"),
    ] = None,
    grpc_payload: Annotated[
        str | None,
        typer.Option("--grpc-payload", help="Base64-encoded protobuf payload"),
    ] = None,
    grpc_deadline_ms: Annotated[
        int | None,
        typer.Option("--grpc-deadline-ms", help="gRPC deadline in milliseconds"),
    ] = None,
    proto_path: Annotated[
        str | None,
        typer.Option(
            "--proto-path", help="Path to .proto file for JSON payload conversion"
        ),
    ] = None,
    grpc_use_reflection: Annotated[
        bool,
        typer.Option(
            "--grpc-use-reflection",
            help="Use server reflection for schema discovery",
        ),
    ] = False,
    http3_enabled: Annotated[
        bool, typer.Option("--http3/--no-http3", help="Enable HTTP/3 over QUIC")
    ] = False,
    quic_zero_rtt: Annotated[
        bool,
        typer.Option("--quic-zero-rtt", help="Enable QUIC 0-RTT connection testing"),
    ] = False,
    quic_max_idle_timeout_ms: Annotated[
        int | None,
        typer.Option("--quic-max-idle-timeout", help="QUIC max idle timeout in ms"),
    ] = None,
) -> None:
    _configure_logging(_resolve_log_level(verbose, quiet), log_file)
    method = _validate_method(method)
    engine = StrobEngine.load_test(
        url=url,
        concurrency=concurrency,
        duration=duration,
        options=_build_request_options(
            timeout=timeout,
            method=method,
            body=body,
            form=form,
            header=header,
            chaos=chaos,
            no_progress=no_progress,
            ws_mode=ws_mode,
            ws_payload=ws_payload,
            ws_role=ws_role,
            ws_publish_interval_ms=ws_publish_interval_ms,
            ws_subscribers=ws_subscribers,
            grpc_service=grpc_service,
            grpc_method=grpc_method,
            grpc_payload=grpc_payload,
            grpc_deadline_ms=grpc_deadline_ms,
            proto_path=proto_path,
            grpc_use_reflection=grpc_use_reflection,
            http3_enabled=http3_enabled,
            quic_zero_rtt=quic_zero_rtt,
            quic_max_idle_timeout_ms=quic_max_idle_timeout_ms,
        ),
    )
    summary = engine.run()
    _output_results(summary, url, duration, json_output)


@app.command()
def stress(
    url: Annotated[str, typer.Argument(help="Target HTTP/HTTPS URL")],
    start: Annotated[
        int,
        typer.Option("--from", help="Starting concurrency", min=1),
    ] = 10,
    target: Annotated[
        int,
        typer.Option("--to", help="Target concurrency", min=1),
    ] = 200,
    ramp: Annotated[
        int,
        typer.Option("--ramp", help="Ramp duration in seconds", min=1),
    ] = 60,
    hold: Annotated[
        int,
        typer.Option("--hold", help="Hold duration in seconds", min=0),
    ] = 30,
    timeout: Annotated[
        int,
        typer.Option("-t", "--timeout", help="Request timeout in seconds", min=1),
    ] = 10,
    method: Annotated[
        str,
        typer.Option(
            "--method",
            help="HTTP method (GET, POST, PUT, DELETE, PATCH, HEAD, OPTIONS)",
        ),
    ] = "GET",
    body: Annotated[
        str | None, typer.Option("--body", help="Request body (raw string)")
    ] = None,
    form: Annotated[
        str | None,
        typer.Option("--form", help="Form data body (e.g. key1=val1&key2=val2)"),
    ] = None,
    header: Annotated[
        list[str] | None,
        typer.Option("--header", help="Custom header key:value (repeatable)"),
    ] = None,
    chaos: Annotated[
        bool, typer.Option("--chaos", help="Enable fault injection (~10%% of requests)")
    ] = False,
    no_progress: Annotated[
        bool, typer.Option("--no-progress", help="Suppress live progress output")
    ] = False,
    json_output: Annotated[
        bool, typer.Option("--json", help="Output raw JSON results")
    ] = False,
    verbose: Annotated[
        int,
        typer.Option("-v", "--verbose", count=True, help="Increase verbosity"),
    ] = 0,
    quiet: Annotated[
        bool, typer.Option("-q", "--quiet", help="Suppress all output")
    ] = False,
    log_file: Annotated[
        str | None, typer.Option("--log-file", help="Write logs to file")
    ] = None,
    ws_mode: Annotated[
        str | None,
        typer.Option(
            "--ws-mode",
            help="WebSocket mode: handshake, ping_pong, stream",
        ),
    ] = None,
    ws_payload: Annotated[
        str | None,
        typer.Option("--ws-payload", help="WebSocket payload for stream mode"),
    ] = None,
    ws_role: Annotated[
        WsRole | None,
        typer.Option(
            "--ws-role",
            help="WebSocket Pub/Sub role: publisher, subscriber",
            case_sensitive=False,
        ),
    ] = None,
    ws_publish_interval_ms: Annotated[
        int | None,
        typer.Option("--ws-publish-interval", help="Publisher send interval in ms"),
    ] = None,
    ws_subscribers: Annotated[
        int | None,
        typer.Option("--ws-subscribers", help="Number of subscriber workers"),
    ] = None,
    grpc_service: Annotated[
        str | None,
        typer.Option(
            "--grpc-service", help="gRPC service name (e.g. helloworld.Greeter)"
        ),
    ] = None,
    grpc_method: Annotated[
        str | None,
        typer.Option("--grpc-method", help="gRPC method name (e.g. SayHello)"),
    ] = None,
    grpc_payload: Annotated[
        str | None,
        typer.Option("--grpc-payload", help="Base64-encoded protobuf payload"),
    ] = None,
    grpc_deadline_ms: Annotated[
        int | None,
        typer.Option("--grpc-deadline-ms", help="gRPC deadline in milliseconds"),
    ] = None,
    proto_path: Annotated[
        str | None,
        typer.Option(
            "--proto-path", help="Path to .proto file for JSON payload conversion"
        ),
    ] = None,
    grpc_use_reflection: Annotated[
        bool,
        typer.Option(
            "--grpc-use-reflection",
            help="Use server reflection for schema discovery",
        ),
    ] = False,
    http3_enabled: Annotated[
        bool, typer.Option("--http3/--no-http3", help="Enable HTTP/3 over QUIC")
    ] = False,
    quic_zero_rtt: Annotated[
        bool,
        typer.Option("--quic-zero-rtt", help="Enable QUIC 0-RTT connection testing"),
    ] = False,
    quic_max_idle_timeout_ms: Annotated[
        int | None,
        typer.Option("--quic-max-idle-timeout", help="QUIC max idle timeout in ms"),
    ] = None,
) -> None:
    _configure_logging(_resolve_log_level(verbose, quiet), log_file)
    method = _validate_method(method)
    engine = StrobEngine.stress_test(
        url=url,
        start_concurrency=start,
        max_concurrency=target,
        ramp_duration=ramp,
        hold_duration=hold,
        options=_build_request_options(
            timeout=timeout,
            method=method,
            body=body,
            form=form,
            header=header,
            chaos=chaos,
            no_progress=no_progress,
            ws_mode=ws_mode,
            ws_payload=ws_payload,
            ws_role=ws_role,
            ws_publish_interval_ms=ws_publish_interval_ms,
            ws_subscribers=ws_subscribers,
            grpc_service=grpc_service,
            grpc_method=grpc_method,
            grpc_payload=grpc_payload,
            grpc_deadline_ms=grpc_deadline_ms,
            proto_path=proto_path,
            grpc_use_reflection=grpc_use_reflection,
            http3_enabled=http3_enabled,
            quic_zero_rtt=quic_zero_rtt,
            quic_max_idle_timeout_ms=quic_max_idle_timeout_ms,
        ),
    )
    summary = engine.run()
    _output_results(summary, url, ramp + hold, json_output)


@app.command()
def spike(
    url: Annotated[str, typer.Argument(help="Target HTTP/HTTPS URL")],
    baseline: Annotated[
        int,
        typer.Option("--baseline", help="Baseline concurrency", min=1),
    ] = 5,
    peak: Annotated[
        int,
        typer.Option("--peak", help="Peak concurrency", min=1),
    ] = 500,
    pre_spike: Annotated[
        int,
        typer.Option("--pre-spike", help="Pre-spike duration in seconds", min=0),
    ] = 5,
    spike_duration: Annotated[
        int,
        typer.Option("--spike-duration", help="Spike duration in seconds", min=1),
    ] = 10,
    post_spike: Annotated[
        int,
        typer.Option("--post-spike", help="Post-spike duration in seconds", min=0),
    ] = 5,
    timeout: Annotated[
        int,
        typer.Option("-t", "--timeout", help="Request timeout in seconds", min=1),
    ] = 10,
    method: Annotated[
        str,
        typer.Option(
            "--method",
            help="HTTP method (GET, POST, PUT, DELETE, PATCH, HEAD, OPTIONS)",
        ),
    ] = "GET",
    body: Annotated[
        str | None, typer.Option("--body", help="Request body (raw string)")
    ] = None,
    form: Annotated[
        str | None,
        typer.Option("--form", help="Form data body (e.g. key1=val1&key2=val2)"),
    ] = None,
    header: Annotated[
        list[str] | None,
        typer.Option("--header", help="Custom header key:value (repeatable)"),
    ] = None,
    chaos: Annotated[
        bool, typer.Option("--chaos", help="Enable fault injection (~10%% of requests)")
    ] = False,
    no_progress: Annotated[
        bool, typer.Option("--no-progress", help="Suppress live progress output")
    ] = False,
    json_output: Annotated[
        bool, typer.Option("--json", help="Output raw JSON results")
    ] = False,
    verbose: Annotated[
        int,
        typer.Option("-v", "--verbose", count=True, help="Increase verbosity"),
    ] = 0,
    quiet: Annotated[
        bool, typer.Option("-q", "--quiet", help="Suppress all output")
    ] = False,
    log_file: Annotated[
        str | None, typer.Option("--log-file", help="Write logs to file")
    ] = None,
    ws_mode: Annotated[
        str | None,
        typer.Option(
            "--ws-mode",
            help="WebSocket mode: handshake, ping_pong, stream",
        ),
    ] = None,
    ws_payload: Annotated[
        str | None,
        typer.Option("--ws-payload", help="WebSocket payload for stream mode"),
    ] = None,
    ws_role: Annotated[
        WsRole | None,
        typer.Option(
            "--ws-role",
            help="WebSocket Pub/Sub role: publisher, subscriber",
            case_sensitive=False,
        ),
    ] = None,
    ws_publish_interval_ms: Annotated[
        int | None,
        typer.Option("--ws-publish-interval", help="Publisher send interval in ms"),
    ] = None,
    ws_subscribers: Annotated[
        int | None,
        typer.Option("--ws-subscribers", help="Number of subscriber workers"),
    ] = None,
    grpc_service: Annotated[
        str | None,
        typer.Option(
            "--grpc-service", help="gRPC service name (e.g. helloworld.Greeter)"
        ),
    ] = None,
    grpc_method: Annotated[
        str | None,
        typer.Option("--grpc-method", help="gRPC method name (e.g. SayHello)"),
    ] = None,
    grpc_payload: Annotated[
        str | None,
        typer.Option("--grpc-payload", help="Base64-encoded protobuf payload"),
    ] = None,
    grpc_deadline_ms: Annotated[
        int | None,
        typer.Option("--grpc-deadline-ms", help="gRPC deadline in milliseconds"),
    ] = None,
    proto_path: Annotated[
        str | None,
        typer.Option(
            "--proto-path", help="Path to .proto file for JSON payload conversion"
        ),
    ] = None,
    grpc_use_reflection: Annotated[
        bool,
        typer.Option(
            "--grpc-use-reflection",
            help="Use server reflection for schema discovery",
        ),
    ] = False,
    http3_enabled: Annotated[
        bool, typer.Option("--http3/--no-http3", help="Enable HTTP/3 over QUIC")
    ] = False,
    quic_zero_rtt: Annotated[
        bool,
        typer.Option("--quic-zero-rtt", help="Enable QUIC 0-RTT connection testing"),
    ] = False,
    quic_max_idle_timeout_ms: Annotated[
        int | None,
        typer.Option("--quic-max-idle-timeout", help="QUIC max idle timeout in ms"),
    ] = None,
) -> None:
    _configure_logging(_resolve_log_level(verbose, quiet), log_file)
    method = _validate_method(method)
    engine = StrobEngine.spike_test(
        url=url,
        baseline=baseline,
        peak_concurrency=peak,
        pre_spike_duration=pre_spike,
        spike_duration=spike_duration,
        post_spike_duration=post_spike,
        options=_build_request_options(
            timeout=timeout,
            method=method,
            body=body,
            form=form,
            header=header,
            chaos=chaos,
            no_progress=no_progress,
            ws_mode=ws_mode,
            ws_payload=ws_payload,
            ws_role=ws_role,
            ws_publish_interval_ms=ws_publish_interval_ms,
            ws_subscribers=ws_subscribers,
            grpc_service=grpc_service,
            grpc_method=grpc_method,
            grpc_payload=grpc_payload,
            grpc_deadline_ms=grpc_deadline_ms,
            proto_path=proto_path,
            grpc_use_reflection=grpc_use_reflection,
            http3_enabled=http3_enabled,
            quic_zero_rtt=quic_zero_rtt,
            quic_max_idle_timeout_ms=quic_max_idle_timeout_ms,
        ),
    )
    summary = engine.run()
    _output_results(summary, url, pre_spike + spike_duration + post_spike, json_output)


_VALUE_FLAGS: set[str] = _collect_value_flags(app)


def _first_positional(argv: list[str]) -> str | None:
    skip_next = False
    for arg in argv:
        if skip_next:
            skip_next = False
            continue
        if arg.startswith("-"):
            if arg in _VALUE_FLAGS:
                skip_next = True
            continue
        return arg
    return None


def main(argv: list[str] | None = None) -> None:
    if argv is None:
        argv = sys.argv[1:]

    if argv and set(argv) & (HELP_FLAGS | VERSION_FLAGS):
        app(args=argv)
        return

    first = _first_positional(argv)
    if first is not None and first not in KNOWN_SUBCOMMANDS:
        argv = ["load", *argv]

    try:
        app(args=argv)
    except SystemExit as e:
        # Re-raise to prevent KeyboardInterrupt handler from catching typer exits
        raise e
    except KeyboardInterrupt:
        typer.echo("\nInterrupted.", err=True)
        raise SystemExit(130) from None


if __name__ == "__main__":
    main()
