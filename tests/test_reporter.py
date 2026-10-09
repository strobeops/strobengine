from unittest.mock import patch

from strobengine.reporter import _error_rate, _format_number, print_summary

from .factories import make_summary as _make_summary


class TestFormatNumber:
    def test_thousands(self):
        assert _format_number(15234) == "15,234"

    def test_zero(self):
        assert _format_number(0) == "0"

    def test_small_number(self):
        assert _format_number(42) == "42"


class TestErrorRate:
    def test_with_errors(self):
        assert _error_rate(1000, 5) == "0.50%"

    def test_no_errors(self):
        assert _error_rate(1000, 0) == "0.00%"

    def test_zero_total(self):
        assert _error_rate(0, 0) == "0.00%"

    def test_high_error_rate(self):
        assert _error_rate(100, 50) == "50.00%"


class TestPrintSummary:
    def test_with_duration(self, capsys):
        summary = _make_summary(url="http://example.com", total_requests=1000)
        print_summary(summary)
        output = capsys.readouterr().out
        assert "http://example.com" in output
        assert "1,000" in output or "1000" in output

    def test_without_duration(self, capsys):
        summary = _make_summary(url="http://example.com")
        print_summary(summary)
        output = capsys.readouterr().out
        assert "http://example.com" in output

    def test_errors_highlighted(self, capsys):
        summary = _make_summary(total_errors=50)
        print_summary(summary)
        output = capsys.readouterr().out
        assert "50" in output

    def test_no_errors(self, capsys):
        summary = _make_summary(url="http://example.com", total_errors=0)
        print_summary(summary)
        output = capsys.readouterr().out
        assert "http://example.com" in output


class TestRichFallback:
    @patch("strobengine.reporter._HAS_RICH", False)
    def test_plain_fallback_used(self, capsys):
        summary = _make_summary(url="http://example.com")
        print_summary(summary)
        output = capsys.readouterr().out
        assert "Load Test Results" in output
        assert "http://example.com" in output


class TestPrintSummaryWebsocket:
    def test_plain_renders_websocket_rows(self, capsys, monkeypatch):
        import strobengine.reporter as reporter

        class FakeWs:
            pings_sent_total = 5
            pings_received_total = 2
            pongs_solicited_total = 5
            pongs_unsolicited_total = 1
            backpressure_max_bytes = 1_048_576
            backpressure_mean_bytes = 524_288.0
            backpressure_threshold_breaches = 2

        monkeypatch.setattr(reporter, "WebsocketMetrics", FakeWs)
        summary = _make_summary(url="http://example.com")
        summary.ws = FakeWs()

        with patch("strobengine.reporter._HAS_RICH", False):
            print_summary(summary)
        output = capsys.readouterr().out
        assert "WS Heartbeat" in output
        assert "WS Backpressure" in output

    def test_plain_omits_websocket_rows_when_absent(self, capsys):
        summary = _make_summary(url="http://example.com")
        with patch("strobengine.reporter._HAS_RICH", False):
            print_summary(summary)
        output = capsys.readouterr().out
        # Mock's auto `.ws` child is not a real WebsocketMetrics -> section skipped.
        assert "WS Heartbeat" not in output


class TestPrintSummaryGrpc:
    def test_plain_renders_grpc_rows(self, capsys, monkeypatch):
        import strobengine.reporter as reporter

        class FakeGrpc:
            active_streams_peak = 6
            concurrency_utilization_peak = 0.06
            concurrency_utilization_mean = 0.05
            window_exhaustion_events_total = 3
            window_stall_duration_ms_total = 42.0
            send_capacity_min_bytes = 32768

        monkeypatch.setattr(reporter, "GrpcMetrics", FakeGrpc)
        summary = _make_summary(url="http://example.com")
        summary.grpc = FakeGrpc()

        with patch("strobengine.reporter._HAS_RICH", False):
            print_summary(summary)
        output = capsys.readouterr().out
        assert "gRPC Streams" in output
        assert "gRPC Window" in output

    def test_plain_omits_grpc_rows_when_absent(self, capsys):
        summary = _make_summary(url="http://example.com")
        with patch("strobengine.reporter._HAS_RICH", False):
            print_summary(summary)
        output = capsys.readouterr().out
        assert "gRPC Streams" not in output


class TestPrintSummaryHttp3:
    def test_plain_renders_http3_rows(self, capsys, monkeypatch):
        import strobengine.reporter as reporter

        class FakeHttp3:
            cwnd_bytes_current = 4000
            cwnd_bytes_min = 1000
            cwnd_bytes_max = 8000
            cwnd_bytes_mean = 3500.0
            migrations_attempted_total = 4
            migrations_successful_total = 3
            migration_success_rate = 0.75

        monkeypatch.setattr(reporter, "Http3Metrics", FakeHttp3)
        summary = _make_summary(url="http://example.com")
        summary.http3 = FakeHttp3()

        with patch("strobengine.reporter._HAS_RICH", False):
            print_summary(summary)
        output = capsys.readouterr().out
        assert "HTTP/3 cwnd" in output
        assert "HTTP/3 Migr" in output

    def test_plain_omits_http3_rows_when_absent(self, capsys):
        summary = _make_summary(url="http://example.com")
        with patch("strobengine.reporter._HAS_RICH", False):
            print_summary(summary)
        output = capsys.readouterr().out
        assert "HTTP/3 cwnd" not in output


class TestSummaryRowParity:
    def test_labels_rendered_in_both_paths(self, capsys, monkeypatch):
        import strobengine.reporter as reporter

        class FakeWs:
            pings_sent_total = 5
            pings_received_total = 2
            pongs_solicited_total = 5
            pongs_unsolicited_total = 1
            backpressure_max_bytes = 1_048_576
            backpressure_mean_bytes = 524_288.0
            backpressure_threshold_breaches = 2

        class FakeGrpc:
            active_streams_peak = 6
            concurrency_utilization_peak = 0.06
            concurrency_utilization_mean = 0.05
            window_exhaustion_events_total = 3
            window_stall_duration_ms_total = 42.0
            send_capacity_min_bytes = 32768

        class FakeHttp3:
            cwnd_bytes_current = 4000
            cwnd_bytes_min = 1000
            cwnd_bytes_max = 8000
            cwnd_bytes_mean = 3500.0
            migrations_attempted_total = 4
            migrations_successful_total = 3
            migration_success_rate = 0.75

        class FakeSystemMetrics:
            peak_memory_rss_bytes = 64 * 1024 * 1024
            peak_cpu_percent = 45.5
            peak_thread_count = 32

        monkeypatch.setattr(reporter, "WebsocketMetrics", FakeWs)
        monkeypatch.setattr(reporter, "GrpcMetrics", FakeGrpc)
        monkeypatch.setattr(reporter, "Http3Metrics", FakeHttp3)
        monkeypatch.setattr(reporter, "SystemMetrics", FakeSystemMetrics)

        summary = _make_summary(
            total_errors=7,
            status_codes={200: 993, 499: 7},
            avg_e2e_latency_us=1500.0,
            chaos_injected_total=3,
            chaos_faults_by_type={"LatencySpike": 2, "ConnectionReset": 1},
        )
        summary.ws = FakeWs()
        summary.grpc = FakeGrpc()
        summary.http3 = FakeHttp3()
        summary.system_metrics = FakeSystemMetrics()

        rows = reporter._summary_rows(summary)
        assert rows

        print_summary(summary)
        rich_output = capsys.readouterr().out

        with patch("strobengine.reporter._HAS_RICH", False):
            print_summary(summary)
        plain_output = capsys.readouterr().out

        rich_labels = {
            cell.strip()
            for line in rich_output.splitlines()
            if line.startswith("│")
            for cell in (line.split("│")[1],)
            if cell.strip()
        }
        raw_lines = {row.value for row in rows if not row.label}
        plain_labels = set()
        for line in plain_output.splitlines():
            if not line.startswith("  ") or line in raw_lines or ":" not in line:
                continue
            plain_labels.add(line.strip().split(":", 1)[0])

        assert rich_labels == {row.label for row in rows if row.label}
        assert plain_labels == {
            row.label for row in rows if row.label and not row.is_header
        }
