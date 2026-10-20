from __future__ import annotations

import sys
from collections.abc import Callable
from typing import Annotated, Any

import typer
from typer.core import TyperGroup, TyperOption
from typer.main import get_command

from strobengine._strobengine import init_logging
from strobengine.engine import RequestOptions, StrobEngine

from .cli_export import ExportOptions, _output_results
from .cli_options import (
    _REQUEST_FIELDS,
    LOAD_OPTION_SPECS,
    SPIKE_OPTION_SPECS,
    STRESS_OPTION_SPECS,
    UrlArg,
    _build_request_options,
    _parse_headers,
    _validate_method,
    export_option_kwargs,
    with_command_options,
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
    if not isinstance(root, TyperGroup):
        return flags
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


def _run_load_test(
    url: str,
    options: RequestOptions,
    engine_factory: Callable[..., StrobEngine],
    json_output: bool = False,
    log_file: str | None = None,
    verbose: int = 0,
    quiet: bool = False,
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
        _configure_logging(_resolve_log_level(verbose, quiet), log_file)

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
            engine.get_config(),
            exports,
            saved_report_path=engine.saved_report_path,
        )
    except Exception as e:
        typer.echo(f"Error: report generation failed: {e}", err=True)
        raise typer.Exit(1) from e


@app.command()
@with_command_options(*LOAD_OPTION_SPECS)
def load(url: UrlArg, **opts: Any) -> None:
    options = _build_request_options(
        **{key: value for key, value in opts.items() if key in _REQUEST_FIELDS}
    )
    _run_load_test(
        url=url,
        options=options,
        engine_factory=lambda **kw: StrobEngine.load_test(
            concurrency=opts["concurrency"], duration=opts["duration"], **kw
        ),
        **export_option_kwargs(opts),
    )


@app.command()
@with_command_options(*STRESS_OPTION_SPECS)
def stress(url: UrlArg, **opts: Any) -> None:
    options = _build_request_options(
        **{key: value for key, value in opts.items() if key in _REQUEST_FIELDS}
    )
    _run_load_test(
        url=url,
        options=options,
        engine_factory=lambda **kw: StrobEngine.stress_test(
            start_concurrency=opts["start"],
            max_concurrency=opts["target"],
            ramp_duration=opts["ramp"],
            hold_duration=opts["hold"],
            **kw,
        ),
        **export_option_kwargs(opts),
    )


@app.command()
@with_command_options(*SPIKE_OPTION_SPECS)
def spike(url: UrlArg, **opts: Any) -> None:
    options = _build_request_options(
        **{key: value for key, value in opts.items() if key in _REQUEST_FIELDS}
    )
    _run_load_test(
        url=url,
        options=options,
        engine_factory=lambda **kw: StrobEngine.spike_test(
            baseline=opts["baseline"],
            peak_concurrency=opts["peak"],
            pre_spike_duration=opts["pre_spike"],
            spike_duration=opts["spike_duration"],
            post_spike_duration=opts["post_spike"],
            **kw,
        ),
        **export_option_kwargs(opts),
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
