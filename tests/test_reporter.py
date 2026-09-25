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
