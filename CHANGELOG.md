# Changelog

All notable changes to `strobengine` will be documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.5.1] - 2026-09-01

### Bug Fixes

- *(websocket)* Use user-configured timeout for persistent session read
- *(http)* Use user-configured method for CorruptedPayload chaos fault
- *(report)* Handle Windows fs::rename target collision in writer
- *(reporting)* Move rich import inside print_cli_comparison
- *(websocket)* Warn on invalid WebSocket headers instead of silent drop

### Refactoring

- *(reporting)* Remove dead render_markdown_report function

### Testing

- *(reporting)* Add unit tests for baseline, html_report, and cli helpers
## [0.5.0] - 2026-08-30

### Documentation

- Convert README links to absolute GitHub URLs
- *(cli)* Add --sse/--no-sse and --sse-max-events to option tables
- *(examples)* Add SSE streaming load test examples
- Add SSE load testing reference guide
- Add SSE to README, quickstart, and table of contents
- Mark HTML report generator as complete in roadmap
- Mark baseline comparison as complete in roadmap
- Add reporting and visualizations reference guide
- Add Reporting & Visualizations to documentation indexes
- Add Advanced Metrics & Observability epic to roadmap
- Mark Export Formats as complete in roadmap

### Features

- *(config)* Add sse_enabled, sse_max_events to TestConfig
- *(metrics)* Add sse_events_received, sse_first_event_us, sse_event_interval_us to RequestMetric
- *(sse)* Implement SSE frame parser and SseEngine
- *(sse)* Wire SSE engine into protocol detection and config routing
- *(sse)* Centralize protocol routing and add URL normalization
- *(deps)* Add serde with derive feature for report serialization
- *(report)* Add ReportArtifact schema with serde serialization
- *(config)* Add output_dir and no_save to TestConfig
- *(python)* Wire report persistence through CLI and engine
- *(report)* Add ReportArtifact::from_summary_and_config converter
- *(report)* Add save_report_json with atomic writes and latest.json
- *(report)* Invoke save_report_json on benchmark completion
- *(reporting)* Add standalone HTML report generator with Chart.js
- *(cli)* Add --html flag for standalone HTML report generation
- *(reporting)* Add baseline artifact loading and delta comparison
- *(reporting)* Add Historical Comparison section to HTML report
- *(cli)* Add --compare-to flag for baseline comparison
- *(reporting)* Inline Chart.js from local asset for 100% offline reports
- *(reporting)* Add CLI terminal comparison and extend compute_comparison
- *(engine)* Add get_config() convenience method to StrobEngine
- *(reporting)* Add Markdown, JUnit XML, and CSV export generators
- *(cli)* Add --markdown, --junit, --csv export flags
- *(reporting)* Add generate_markdown_summary for GitHub Actions / PR comments
- *(reporting)* Add generate_junit_report and generate_csv_report

### Miscellaneous Tasks

- *(pyproject)* Simplify readme config and update documentation URL
- *(scripts)* Run uv lock during release version bump
- *(release)* Bump version to 0.5.0

### Styling

- *(tests)* Reorder imports in test_reporting.py for ruff isort compliance

### Testing

- *(config)* Update TestConfig::new calls with SSE parameters
- *(e2e)* Add SSE streaming endpoint to mock server
- *(e2e)* Add SSE integration tests for error paths and streaming
- *(e2e)* Add persistence E2E tests for report artifact disk writing
- *(reporting)* Add unit tests for Markdown summary generator
- *(reporting)* Add JUnit XML and CSV export unit tests
## [0.4.1] - 2026-08-26

### Bug Fixes

- *(pypi)* Configure readme base-url for relative link resolution
- *(cli)* Validate ws_mode with StrEnum to reject invalid strings
- *(cli)* URL-decode form values before passing to Rust
- *(websocket)* Count actual Pong payload size and fix PingPong status code
- *(reporter)* Handle GB and TB in _format_bytes

### Documentation

- Add dependencies and installation guides
- Add quick start guide and CLI reference
- Update table of contents with new pages
- Slim README and link to documentation pages
- Add multi-target, reporting, SSE, messaging, and caching epics to roadmap

### Miscellaneous Tasks

- *(release)* Bump version to 0.4.1

### Testing

- *(e2e)* Remove bytes_received assertion from PingPong mode test
## [0.4.0] - 2026-08-23

### Bug Fixes

- *(e2e)* Add binary echo and error handling to WebSocket mock server
- *(websocket)* Add per-iteration timeout to prevent worker hangs
- *(protocols)* Pass timeout_secs to WebSocketEngine
- *(websocket)* Lazy connect for pub/sub sessions and test fixes
- *(metrics)* Populate new QUIC fields in all protocol constructors
- *(http3)* Defer endpoint creation to first use via OnceCell
- *(protocol)* Replace panic with graceful HTTP/1.1 fallback for HTTP/3 errors
- *(grpc)* Replace .expect() panics with proper error propagation
- *(grpc)* Use copy_to_bytes to read all chunks in RawDecoder #105
- *(http3)* Reject invalid HTTP methods instead of silently defaulting to GET #109
- *(http3)* Reset prev_lost_packets after reconnect
- *(websocket)* Replace unwrap() with safe pattern matching on session streams #113
- *(websocket)* Replace expect() with safe downcast on PersistentWsSession
- *(e2e)* Add discard WebSocket endpoint and fix flaky pub/sub test

### Documentation

- Update project description to include WebSocket support
- *(websocket)* Add CLI usage examples and table of contents
- *(gRPC)* Add gRPC load testing documentation
- Add gRPC entry to documentation index
- Add gRPC to README tagline, crates, examples, and options
- Update roadmap to reflect actual implementation status
- Remove stale limitations from gRPC documentation
- *(readme)* Add pub/sub CLI flags to subcommand tables
- *(websockets)* Add pub/sub mode documentation and examples
- Update TOC, endpoint table, and roadmap for pub/sub
- *(examples)* Add WebSocket load testing examples
- *(http3)* Add HTTP/3 protocol documentation
- Update TOC, roadmap, and README for HTTP/3
- *(examples)* Add HTTP/3 load testing examples
- *(release)* Add operational release process documentation

### Features

- *(protocols)* Define ProtocolEngine trait and detect_protocol factory
- *(protocols)* Implement HttpEngine with ProtocolEngine trait
- *(protocols)* Implement WebSocketEngine for handshake testing
- *(protocols)* Implement modular ProtocolEngine trait and WebSocket handshake engine #75
- *(config)* Add WsMode enum for WebSocket execution modes
- *(protocols)* Add WebSocket headers and PingPong mode support
- *(engine)* Wire WsMode through Python API to Rust engine
- *(websocket)* Add WsMode execution modes, headers, and E2E tests #77
- *(config)* Add Stream variant and ws_payload to TestConfig
- *(protocols)* Implement WsMode::Stream with payload injection
- *(engine)* Pass ws_payload through to WebSocket engine
- *(websocket)* Add WsMode::Stream support and custom payload injection #78
- *(engine)* Wire ws_payload and Stream mode through Python API
- *(engine)* Wire ws_payload and Stream mode through Python API #79
- *(cli)* Add --ws-mode and --ws-payload options to CLI commands
- *(protocols)* Add chaos injection to WebSocket engine
- *(engine)* Pass chaos to WebSocket engine in both entry points
- *(ws)* Add websocket chaos testing (#82)
- *(config)* Add gRPC fields to TestConfig
- *(protocols)* Implement GrpcEngine with ProtocolEngine trait
- *(stubs)* Add gRPC fields to TestConfig type stubs
- *(engine)* Add gRPC fields to RequestOptions and TestConfig wiring
- *(cli)* Add gRPC flags to load, stress, and spike subcommands
- *(grpc)* Handle MetadataCorruption chaos fault
- *(reporter)* Annotate status code 0 and gRPC protocol in output
- *(grpc)* Add hex payload decoding with 0x prefix convention
- *(grpc)* Add runtime .proto parsing and JSON-to-protobuf conversion
- *(grpc)* Add proto_schema support and pre-serialize JSON payloads
- Add proto_path to TestConfig, RequestOptions, and CLI
- Add grpc_use_reflection config option
- *(grpc)* Add server reflection client with v1/v1alpha fallback
- *(grpc)* Add lazy reflection initialization with OnceCell
- *(metrics)* Add is_reconnect and connection_latency_us fields
- *(protocol)* Add new RequestMetric fields to HTTP and gRPC engines
- *(protocol)* Add worker context methods to ProtocolEngine
- *(config)* Add WebSocket persistent connection fields
- *(websocket)* Add PersistentWsSession and context-based iteration
- *(core)* Integrate worker-local context in execute_test
- *(metrics)* Add pub/sub timestamp fields and payload encoding
- *(config)* Add ws_role, ws_publish_interval_ms, ws_subscribers
- *(websocket)* Add publisher/subscriber sessions and role dispatch
- *(protocols)* Wire pub/sub config to WebSocketEngine
- *(core)* Aggregate e2e latencies in worker metric collector
- *(typing)* Add pub/sub config and e2e latency to type stubs
- *(engine)* Add pub/sub fields to RequestOptions
- *(cli)* Add --ws-role, --ws-publish-interval, --ws-subscribers
- *(reporter)* Display avg E2E latency for pub/sub tests
- *(config)* Add http3_enabled, quic_zero_rtt, quic_max_idle_timeout_ms
- *(protocol)* Implement Http3Engine with QUIC transport
- *(protocol)* Register http3 module and add URL routing
- *(core)* Route http3:// and h3:// URLs through detect_protocol
- *(metrics)* Add quic_handshake_us, quic_0rtt_used, quic_retransmits
- *(http3)* Enable TLS session resumption and 0-RTT connection attempts
- *(cli)* Add --http3, --quic-zero-rtt, --quic-max-idle-timeout flags
- *(scripts)* Add automated release script with dry-run support

### Miscellaneous Tasks

- *(pre-commit)* Enable e2e tests in pytest hook
- Add rust-toolchain.toml for reproducible builds
- Remove e2e test from pre commit hooks
- *(ci)* Pin github actions to full commit SHAs
- *(release)* Bump version to 0.4.0
- Bump to 0.4.0 in uv.lock

### Other

- *(deps)* Add async-trait and tokio-tungstenite, switch reqwest to rustls
- *(deps)* Add tonic, prost, prost-types, and base64 for gRPC support
- *(deps)* Add hex crate for protobuf payload decoding
- *(deps)* Add prost-reflect, protox, serde_json for proto parsing
- *(deps)* Add tonic-reflection and tokio-stream
- *(deps)* Add quinn, h3, h3-quinn for HTTP/3 support

### Refactoring

- *(core)* Use ProtocolEngine trait in execute_test worker loop
- *(engine)* Simplify detect_protocol to accept TestConfig
- *(grpc)* Wire proto_path through detect_protocol and lib.rs
- *(grpc)* Pass grpc_use_reflection through detect_protocol
- *(cli)* Validate --ws-role with StrEnum choices
- *(protocol)* Extract is_protocol_url helper to deduplicate scheme checks #103
- *(cli)* Extract _build_request_options helper to deduplicate construction #107

### Styling

- *(scripts)* Format release.py with ruff

### Testing

- *(e2e)* Add WebSocket echo endpoint to mock server
- *(e2e)* Add WebSocket load test and unreachable server test
- *(e2e)* Add WebSocket tests
- *(e2e)* Add WebSocket PingPong and custom header E2E tests
- *(e2e)* Add WebSocket Stream mode and default payload E2E tests (#80)
- *(e2e)* Add WebSocket Stream mode and default payload E2E tests
- *(e2e)* Add WebSocket chaos mode E2E test
- *(e2e)* Strengthen WebSocket chaos test assertions
- *(e2e)* Add gRPC unreachable server test
- *(e2e)* Add gRPC chaos, headers, and deadline E2E tests
- *(e2e)* Add asyncio.wait_for timeout to gRPC E2E tests
- *(e2e)* Add WebSocket broadcast endpoint to mock server
- *(e2e)* Add pub/sub and streaming E2E tests with timeout guards
- *(e2e)* Add HTTP/3 error-path and CLI option tests
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
- *(changelog)* Add v0.3.0 release notes

### Features

- *(rust)* Add form payload support to TestConfig and Content-Type logic
- *(python)* Add --form flag and type stubs
- *(cli)* Add --form payload support for URL-encoded request bodies (#56)
- *(metrics)* Add latency distribution, status code aggregation, and structured JSON repor (#72)

### Miscellaneous Tasks

- *(dev)* Setup pre-commit hooks and Makefile for local pre-push checks (#58)
- *(github)* Add Docker build and push workflow for releases
- Rename GitHub organization strobe-ops to strobeops
- *(release)* Merge release branch for v0.3.0 #74

### Other

- *(deps)* Add urlencoding crate for form payload encoding
- *(deps)* Add aiohttp dev dependency for e2e test server
- *(docker)* Add multi-stage Dockerfile for Maturin package
- *(docker)* Add .dockerignore to optimize build context
- *(docker)* Switch runtime container execution to non-root user
- *(rust)* Bump version to 0.3.0

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
