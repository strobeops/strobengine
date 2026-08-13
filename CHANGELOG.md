# Changelog

All notable changes to `strobengine` will be documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.0] - 2026-08-13

### Bug Fixes

- *(logging)* Log warning on invalid EnvFilter string (#59)
- *(logging)* Warn when log file creation fails (#60)
- *(cli)* Auto-detect value-taking flags in positional arg detection (#61)
- *(cli)* Map trace log level to custom level 5 instead of DEBUG (#63)

### Documentation

- *(readme)* Add custom HTTP headers usage examples (#55)
- Add --form flag to README CLI reference
- Update roadmap to reflect recent project changes (#62)
- *(worker)* Document u64::MAX fallback for extreme latencies (#66)
- Consolidate testing guide into docs/testing.md
- *(docker)* Add Docker usage documentation

### Features

- *(rust)* Add form payload support to TestConfig and Content-Type logic
- *(python)* Add --form flag and type stubs
- *(cli)* Add --form payload support for URL-encoded request bodies (#56)
- *(metrics)* Add latency distribution, status code aggregation, and structured JSON repor (#72)

### Miscellaneous Tasks

- *(dev)* Setup pre-commit hooks and Makefile for local pre-push checks (#58)
- *(github)* Add Docker build and push workflow for releases

### Other

- *(deps)* Add urlencoding crate for form payload encoding
- *(deps)* Add aiohttp dev dependency for e2e test server
- *(docker)* Add multi-stage Dockerfile for Maturin package
- *(docker)* Add .dockerignore to optimize build context
- *(docker)* Switch runtime container execution to non-root user

### Refactoring

- *(engine)* Unify load test execution via ConcurrencyStrategy (#54)
- *(worker)* Remove unused status_code and is_error from RequestMetric (#57)
- *(progress)* Remove unused `_total_duration` param from `create_progress_bar` (#64)

### Testing

- Remove trivial allocator unit test (#65)
- *(e2e)* Add async mock server with status, delay, echo, and flaky endpoints
- Add e2e mock server (#67)
- *(e2e)* Add `cli_bin` fixture to detect strobengine CLI path (#68)
- *(e2e)* Harden mock server with AppKey types and header normalization
- *(e2e)* Switch mock_server fixture to threaded AppRunner
- *(e2e)* Add HTTP scenario tests against mock server
- *(e2e)* Harden mock server and consolidate testing docs
- *(e2e)* Add subprocess-based CLI interface tests (#70)
- *(e2e)* Configure --e2e pytest flag and CI pipeline step (#71)
## [0.2.1] - 2026-08-03

### Bug Fixes

- *(types)* Correct headers type in _strobengine.pyi from dict to list of tuples (#41)
- Replace build_client panic with proper error propagation (#42)
- Replace std::process::exit with safe PyKeyboardInterrupt on SIGINT (#43)
- *(progress)* Guard against zero total_duration in progress bar calculation (#44)
- Improve worker shutdown and panic handling (#45)
- *(metrics)* Normalize counter types to AtomicU64 for 32-bit safety (#46)
- *(cli)* Add type annotation to _output_results summary parameter (#48)

### Documentation

- Add comment explaining SystemExit re-raise pattern (#50)

### Miscellaneous Tasks

- Remove unnecessary clippy allow on run_load_test (#47)
- Add multi-architecture matrix for macOS wheel builds (#52)
- Prepare for release 0.2.1

### Refactoring

- Extract magic number 8192 to named constant METRIC_CHANNEL_BUFFER (#49)
- Extract magic numbers to named constants across Rust and Python (#51)
## [0.2.0] - 2026-08-02

### Bug Fixes

- *(progress)* Downgrade expected HTTP errors from warn to debug
- *(cli)* Move logging flags to subcommands for natural syntax
- *(engine)* Resolve null options and attribute access bugs (#38)

### Documentation

- Add benchmark methodology, infrastructure setup, and project roadmap
- Add tool versions to benchmark methodology (#29)
- Document verbosity, progress bar, and CLI flags
- Update roadmap to reflect changes (#34)
- Add HTTP method, body, and header documentation
- Add code examples (#37)
- *(changelog)* Update CHANGELOG.md for v0.2.0 release

### Features

- *(reporter)* Add metric descriptions to CLI summary output (#30)
- Add chaos testing engine (#32)
- Add indicatif dependency for progress bar rendering
- Add progress bar module and live render loop
- *(metrics)* Add atomic fields to LiveCounters for active workers and latency tracking
- *(worker)* Track active workers and update live request metrics
- *(config)* Support no_progress flag in TestConfig
- *(engine)* Spawn live progress bar during load test runs
- *(cli)* Add --no-progress option to suppress live progress bar
- Progress indicators (#33)
- Add HTTP request configuration
- Support custom HTTP requests
- *(cli)* Add HTTP request options
- Add support for HTTP methods, request bodies, and custom headers (#35)
- Implement graceful shutdown on SIGINT (#39)

### Miscellaneous Tasks

- *(release)* Prepare v0.1.0 changelog and tag
- Add PyPI publish workflow with tag-based triggering (#26)
- *(pyproject)* Add license metadata and project URLs, bump version
- *(release)* Merge v0.2.0 release prep into main (#40)

### Performance

- Switch global allocator to mimalloc for reduced lock contention (#28)
- Optimize connection pooling with tcp_nodelay, pre-warming, and body consumption (#31)

### Refactoring

- *(engine)* Encapsulate request options into RequestOptions dataclass (#36)

### Testing

- *(config)* Simplify header setup in custom config test
## [0.1.0] - 2026-07-23

### Bug Fixes

- *(tests)* Fix backward-compat tests to go through main()
- *(docs)* Correct print_summary import and add results display to examples
- *(docs)* Correct print_summary import and add results display to examples (#23)

### Documentation

- *(changelog)* Configure git-cliff and generate initial CHANGELOG.md (#16)
- Add logging flags and tracing crates to README

### Features

- *(config)* Add TestConfig pyclass with Python default arguments (#5)
- *(metrics)* Add TestSummary pyclass and calculate_summary (#6)
- Add StrobEngine class with sync and async interfaces (#11)
- Dynamic load profiling (#17)
- *(cli)* Add -V/--version flag with importlib.metadata
- *(rust)* Add tracing instrumentation and init_logging binding
- *(cli)* Add -v/-q/--log-file flags with stderr logging
- *(logging)* Unify system logging (#20)

### Miscellaneous Tasks

- Add Python environment and build artifacts to .gitignore
- Initialize strobengine hybrid workspace architecture
- *(cargo)* Fix formatting and wrap comment for abi3-py38 feature (#1)
- *(python)* Add __all__ to package init for explicit public API (#8)
- Add GitHub Actions workflow for Rust and Python checks (#10)
- Update minimum Python version to 3.13 for abi3-py313
- *(lint)* Add ruff rules and auto-fix pyupgrade suggestions
- Bump minimum python version to 3.13 and expand ruff rules (#21)
- Add TestPyPI publish workflow with OIDC trusted publishing
- Lower minimum Python version to 3.11 for broader compatibility
- Lower minimum Python version to 3.11 for broader compatibility (#24)
- Add testpypi environment to publish workflow
- Add testpypi environment to publish workflow (#25)
- Add PyPI publish workflow with tag-based triggering

### Refactoring

- Clean up __init__.py public API exports (#14)
- *(cli)* Migrate CLI from argparse to typer (#18)
