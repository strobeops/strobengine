from __future__ import annotations

import sys
from collections.abc import Callable
from dataclasses import dataclass
from typing import Annotated

import typer
from typer.core import TyperOption
from typer.main import get_command

from strobengine._strobengine import TestSummary, init_logging
from strobengine.engine import RequestOptions, StrobEngine
from strobengine.reporter import print_summary

from .cli_options import (
    _REQUEST_FIELDS,
    BaselineOpt,
    BodyOpt,
    ChaosOpt,
    CompareToOpt,
    ConcurrencyOpt,
    CsvOpt,
    DurationOpt,
    FormOpt,
    FromOpt,
    GrpcDeadlineMsOpt,
    GrpcMethodOpt,
    GrpcPayloadOpt,
    GrpcServiceOpt,
    GrpcUseReflectionOpt,
    HeaderOpt,
    HoldOpt,
    HtmlOpt,
    Http3Opt,
    JsonOutputOpt,
    JunitOpt,
    LogFileOpt,
    MarkdownOpt,
    MethodOpt,
    NoProgressOpt,
    NoSaveOpt,
    OutputDirOpt,
    PeakOpt,
    PostSpikeOpt,
    PreSpikeOpt,
    ProtoPathOpt,
    QuicMaxIdleTimeoutOpt,
    QuicZeroRttOpt,
    QuietOpt,
    RampOpt,
    SpikeDurationOpt,
    SseEnabledOpt,
    SseMaxEventsOpt,
    SysSampleIntervalOpt,
    TimeoutOpt,
    ToOpt,
    UrlArg,
    VerboseOpt,
    WsBackpressureWarnRatioOpt,
    WsKeepaliveSecsOpt,
    WsMaxBufferBytesOpt,
    WsModeOpt,
    WsPayloadOpt,
    WsPersistentOpt,
    WsPublishIntervalOpt,
    WsRoleOpt,
    WsSubscribersOpt,
    _build_request_options,
    _parse_headers,
    _validate_method,
)

# Typer option aliases + request-building helpers now live in cli_options.
# Re-export the request-parsing helpers so the historical public surface
# `from strobengine.cli import _build_request_options, _parse_headers,
# _validate_method` keeps resolving (they are otherwise only used in tests).
__all__ = [
    "_build_request_options",
    "_parse_headers",
    "_validate_method",
    "app",
    "main",
]


@dataclass
class ExportOptions:
    """Options controlling report output and persistence."""

    output_dir: str | None = None
    no_save: bool = False
    html_output: str | None = None
    compare_to: str | None = None
    export_markdown: str | None = None
    export_junit: str | None = None
    export_csv: str | None = None
    json_output: bool = False


def _report_saved(filepath: str | None, exports: ExportOptions) -> None:
    """Print report path to stderr unless in JSON mode or saving is disabled."""
    if filepath and not exports.json_output and not exports.no_save:
        print(f"Report saved to {filepath}", file=sys.stderr)


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


def _collect_value_flags(app: typer.Typer) -> set[str]:
    """Build the set of flags that consume the next argument."""
    root = get_command(app)
    flags: set[str] = set()
    for cmd in root.commands.values():
        for param in cmd.params:
            if isinstance(param, TyperOption) and not param.is_flag and not param.count:
                flags.update(param.opts)
    return flags


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
    summary: TestSummary,
    url: str,
    duration_secs: int,
    config: object | None,
    exports: ExportOptions,
    saved_report_path: str | None = None,
) -> None:
    print_summary(summary, json_output=exports.json_output)
    _report_saved(saved_report_path, exports)

    # Compute comparison (runs for both --html and terminal display)
    comparison = None
    if exports.compare_to:
        from pathlib import Path

        from strobengine.reporter import build_artifact_dict
        from strobengine.reporting.baseline import (
            compute_comparison,
            load_baseline_artifact,
        )

        baseline = load_baseline_artifact(baseline_file=Path(exports.compare_to))
        if baseline and config is not None:
            current = build_artifact_dict(summary, config)
            comparison = compute_comparison(current, baseline)
            if not exports.json_output:
                from strobengine.reporting.baseline import print_cli_comparison

                print_cli_comparison(comparison)

    if exports.html_output:
        from strobengine.reporting.html_report import save_html_report

        save_html_report(summary, config, exports.html_output, comparison=comparison)
        _report_saved(exports.html_output, exports)

    # Export formats
    if exports.export_markdown:
        from strobengine.reporting.markdown_report import save_markdown_report

        save_markdown_report(summary, config, exports.export_markdown, duration_secs)
        _report_saved(exports.export_markdown, exports)

    if exports.export_junit:
        from strobengine.reporting.junit_report import save_junit_report

        save_junit_report(summary, config, exports.export_junit, duration_secs)
        _report_saved(exports.export_junit, exports)

    if exports.export_csv:
        from strobengine.reporting.csv_report import save_csv_report

        save_csv_report(summary, config, exports.export_csv, duration_secs)
        _report_saved(exports.export_csv, exports)


def _run_load_test(
    url: str,
    options: RequestOptions,
    engine_factory: Callable[..., StrobEngine],
    duration: int,
    json_output: bool = False,
    log_file: str | None = None,
    html_output: str | None = None,
    compare_to: str | None = None,
    export_markdown: str | None = None,
    export_junit: str | None = None,
    export_csv: str | None = None,
) -> None:
    """Consolidated runner across load, stress, and spike subcommands."""
    # Suppress logging when JSON output is requested
    if json_output:
        _configure_logging("off", log_file)
    else:
        _configure_logging(_resolve_log_level(options.no_progress, False), log_file)

    # engine_factory may raise config-validation errors and engine.run() may
    # surface FFI errors as ValueError/RuntimeError; present them cleanly
    # instead of a raw traceback. except Exception does not catch
    # KeyboardInterrupt, so graceful-interrupt handling in main() is preserved.
    try:
        engine = engine_factory(url=url, options=options)
        summary = engine.run()
    except Exception as e:
        typer.echo(f"Error: load test failed: {e}", err=True)
        raise typer.Exit(1) from e

    exports = ExportOptions(
        output_dir=options.output_dir,
        no_save=options.no_save,
        html_output=html_output,
        compare_to=compare_to,
        export_markdown=export_markdown,
        export_junit=export_junit,
        export_csv=export_csv,
        json_output=json_output,
    )
    try:
        _output_results(
            summary,
            url,
            duration,
            engine.get_config(),
            exports,
            saved_report_path=engine.saved_report_path,
        )
    except Exception as e:
        typer.echo(f"Error: report generation failed: {e}", err=True)
        raise typer.Exit(1) from e


@app.command()
def load(
    url: UrlArg,
    concurrency: ConcurrencyOpt = 10,
    duration: DurationOpt = 10,
    timeout: TimeoutOpt = 10,
    method: MethodOpt = "GET",
    body: BodyOpt = None,
    form: FormOpt = None,
    header: HeaderOpt = None,
    chaos: ChaosOpt = False,
    no_progress: NoProgressOpt = False,
    json_output: JsonOutputOpt = False,
    verbose: VerboseOpt = 0,
    quiet: QuietOpt = False,
    log_file: LogFileOpt = None,
    ws_mode: WsModeOpt = None,
    ws_payload: WsPayloadOpt = None,
    ws_persistent: WsPersistentOpt = False,
    ws_keepalive_secs: WsKeepaliveSecsOpt = None,
    ws_role: WsRoleOpt = None,
    ws_publish_interval_ms: WsPublishIntervalOpt = None,
    ws_subscribers: WsSubscribersOpt = None,
    grpc_service: GrpcServiceOpt = None,
    grpc_method: GrpcMethodOpt = None,
    grpc_payload: GrpcPayloadOpt = None,
    grpc_deadline_ms: GrpcDeadlineMsOpt = None,
    proto_path: ProtoPathOpt = None,
    grpc_use_reflection: GrpcUseReflectionOpt = False,
    http3_enabled: Http3Opt = False,
    quic_zero_rtt: QuicZeroRttOpt = False,
    quic_max_idle_timeout_ms: QuicMaxIdleTimeoutOpt = None,
    sse_enabled: SseEnabledOpt = False,
    sse_max_events: SseMaxEventsOpt = None,
    output_dir: OutputDirOpt = None,
    no_save: NoSaveOpt = False,
    sys_sample_interval: SysSampleIntervalOpt = 1000,
    ws_max_buffer_bytes: WsMaxBufferBytesOpt = 1_048_576,
    ws_backpressure_warn_ratio: WsBackpressureWarnRatioOpt = 0.8,
    html_output: HtmlOpt = None,
    compare_to: CompareToOpt = None,
    export_markdown: MarkdownOpt = None,
    export_junit: JunitOpt = None,
    export_csv: CsvOpt = None,
) -> None:
    options = _build_request_options(
        **{k: v for k, v in locals().items() if k in _REQUEST_FIELDS}
    )
    _run_load_test(
        url=url,
        options=options,
        engine_factory=lambda **kw: StrobEngine.load_test(
            concurrency=concurrency, duration=duration, **kw
        ),
        duration=duration,
        json_output=json_output,
        log_file=log_file,
        html_output=html_output,
        compare_to=compare_to,
        export_markdown=export_markdown,
        export_junit=export_junit,
        export_csv=export_csv,
    )


@app.command()
def stress(
    url: UrlArg,
    start: FromOpt = 10,
    target: ToOpt = 200,
    ramp: RampOpt = 60,
    hold: HoldOpt = 30,
    timeout: TimeoutOpt = 10,
    method: MethodOpt = "GET",
    body: BodyOpt = None,
    form: FormOpt = None,
    header: HeaderOpt = None,
    chaos: ChaosOpt = False,
    no_progress: NoProgressOpt = False,
    json_output: JsonOutputOpt = False,
    verbose: VerboseOpt = 0,
    quiet: QuietOpt = False,
    log_file: LogFileOpt = None,
    ws_mode: WsModeOpt = None,
    ws_payload: WsPayloadOpt = None,
    ws_persistent: WsPersistentOpt = False,
    ws_keepalive_secs: WsKeepaliveSecsOpt = None,
    ws_role: WsRoleOpt = None,
    ws_publish_interval_ms: WsPublishIntervalOpt = None,
    ws_subscribers: WsSubscribersOpt = None,
    grpc_service: GrpcServiceOpt = None,
    grpc_method: GrpcMethodOpt = None,
    grpc_payload: GrpcPayloadOpt = None,
    grpc_deadline_ms: GrpcDeadlineMsOpt = None,
    proto_path: ProtoPathOpt = None,
    grpc_use_reflection: GrpcUseReflectionOpt = False,
    http3_enabled: Http3Opt = False,
    quic_zero_rtt: QuicZeroRttOpt = False,
    quic_max_idle_timeout_ms: QuicMaxIdleTimeoutOpt = None,
    sse_enabled: SseEnabledOpt = False,
    sse_max_events: SseMaxEventsOpt = None,
    output_dir: OutputDirOpt = None,
    no_save: NoSaveOpt = False,
    sys_sample_interval: SysSampleIntervalOpt = 1000,
    ws_max_buffer_bytes: WsMaxBufferBytesOpt = 1_048_576,
    ws_backpressure_warn_ratio: WsBackpressureWarnRatioOpt = 0.8,
    html_output: HtmlOpt = None,
    compare_to: CompareToOpt = None,
    export_markdown: MarkdownOpt = None,
    export_junit: JunitOpt = None,
    export_csv: CsvOpt = None,
) -> None:
    options = _build_request_options(
        **{k: v for k, v in locals().items() if k in _REQUEST_FIELDS}
    )
    _run_load_test(
        url=url,
        options=options,
        engine_factory=lambda **kw: StrobEngine.stress_test(
            start_concurrency=start,
            max_concurrency=target,
            ramp_duration=ramp,
            hold_duration=hold,
            **kw,
        ),
        duration=ramp + hold,
        json_output=json_output,
        log_file=log_file,
        html_output=html_output,
        compare_to=compare_to,
        export_markdown=export_markdown,
        export_junit=export_junit,
        export_csv=export_csv,
    )


@app.command()
def spike(
    url: UrlArg,
    baseline: BaselineOpt = 5,
    peak: PeakOpt = 500,
    pre_spike: PreSpikeOpt = 5,
    spike_duration: SpikeDurationOpt = 10,
    post_spike: PostSpikeOpt = 5,
    timeout: TimeoutOpt = 10,
    method: MethodOpt = "GET",
    body: BodyOpt = None,
    form: FormOpt = None,
    header: HeaderOpt = None,
    chaos: ChaosOpt = False,
    no_progress: NoProgressOpt = False,
    json_output: JsonOutputOpt = False,
    verbose: VerboseOpt = 0,
    quiet: QuietOpt = False,
    log_file: LogFileOpt = None,
    ws_mode: WsModeOpt = None,
    ws_payload: WsPayloadOpt = None,
    ws_persistent: WsPersistentOpt = False,
    ws_keepalive_secs: WsKeepaliveSecsOpt = None,
    ws_role: WsRoleOpt = None,
    ws_publish_interval_ms: WsPublishIntervalOpt = None,
    ws_subscribers: WsSubscribersOpt = None,
    grpc_service: GrpcServiceOpt = None,
    grpc_method: GrpcMethodOpt = None,
    grpc_payload: GrpcPayloadOpt = None,
    grpc_deadline_ms: GrpcDeadlineMsOpt = None,
    proto_path: ProtoPathOpt = None,
    grpc_use_reflection: GrpcUseReflectionOpt = False,
    http3_enabled: Http3Opt = False,
    quic_zero_rtt: QuicZeroRttOpt = False,
    quic_max_idle_timeout_ms: QuicMaxIdleTimeoutOpt = None,
    sse_enabled: SseEnabledOpt = False,
    sse_max_events: SseMaxEventsOpt = None,
    output_dir: OutputDirOpt = None,
    no_save: NoSaveOpt = False,
    sys_sample_interval: SysSampleIntervalOpt = 1000,
    ws_max_buffer_bytes: WsMaxBufferBytesOpt = 1_048_576,
    ws_backpressure_warn_ratio: WsBackpressureWarnRatioOpt = 0.8,
    html_output: HtmlOpt = None,
    compare_to: CompareToOpt = None,
    export_markdown: MarkdownOpt = None,
    export_junit: JunitOpt = None,
    export_csv: CsvOpt = None,
) -> None:
    options = _build_request_options(
        **{k: v for k, v in locals().items() if k in _REQUEST_FIELDS}
    )
    _run_load_test(
        url=url,
        options=options,
        engine_factory=lambda **kw: StrobEngine.spike_test(
            baseline=baseline,
            peak_concurrency=peak,
            pre_spike_duration=pre_spike,
            spike_duration=spike_duration,
            post_spike_duration=post_spike,
            **kw,
        ),
        duration=pre_spike + spike_duration + post_spike,
        json_output=json_output,
        log_file=log_file,
        html_output=html_output,
        compare_to=compare_to,
        export_markdown=export_markdown,
        export_junit=export_junit,
        export_csv=export_csv,
    )


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
