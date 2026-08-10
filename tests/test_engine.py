from unittest.mock import Mock, patch

import pytest

from strobengine.engine import RequestOptions, StrobEngine


def _make_summary(**kwargs):
    defaults = {
        "url": "http://example.com",
        "total_requests": 100,
        "total_errors": 0,
        "average_latency_ms": 10.0,
        "p95_latency_ms": 20.0,
        "p99_latency_ms": 30.0,
        "min_latency_ms": 1.0,
        "p50_latency_ms": 15.0,
        "p90_latency_ms": 25.0,
        "max_latency_ms": 50.0,
        "total_bytes_received": 102400,
        "duration_secs": 5.0,
        "workers": 10,
        "timestamp": "2026-08-10T10:00:00+00:00",
        "raw_command": "strobengine.run(url='http://example.com', workers=10)",
        "status_codes": {200: 100},
        "to_dict": lambda: {},
        "to_json": lambda indent=None: "{}",
    }
    defaults.update(kwargs)
    return Mock(**defaults)


class TestStrobEngineInit:
    def test_default_values(self):
        engine = StrobEngine(url="http://example.com")
        assert engine.config.url == "http://example.com"
        assert engine.config.concurrency == 10
        assert engine.config.duration_secs == 10
        assert engine.config.timeout_secs == 10

    def test_custom_values(self):
        engine = StrobEngine(
            url="http://test.io",
            concurrency=50,
            duration=30,
            options=RequestOptions(timeout=5),
        )
        assert engine.config.url == "http://test.io"
        assert engine.config.concurrency == 50
        assert engine.config.duration_secs == 30
        assert engine.config.timeout_secs == 5

    def test_invalid_concurrency(self):
        with pytest.raises(ValueError, match="Concurrency must be greater than 0"):
            StrobEngine(url="http://example.com", concurrency=0)

    def test_negative_concurrency(self):
        with pytest.raises(ValueError, match="Concurrency must be greater than 0"):
            StrobEngine(url="http://example.com", concurrency=-5)

    def test_invalid_duration(self):
        with pytest.raises(ValueError, match="Duration must be greater than 0"):
            StrobEngine(url="http://example.com", duration=0)

    def test_invalid_timeout(self):
        with pytest.raises(ValueError, match="timeout must be greater than 0"):
            StrobEngine(url="http://example.com", options=RequestOptions(timeout=0))


class TestStrobEngineRun:
    @patch("strobengine.engine.run_load_test")
    def test_run_returns_summary(self, mock_run):
        expected = _make_summary()
        mock_run.return_value = expected

        engine = StrobEngine(url="http://example.com")
        result = engine.run()

        mock_run.assert_called_once_with(engine.config)
        assert result is expected

    @patch("strobengine.engine.run_load_test")
    def test_run_with_custom_params(self, mock_run):
        mock_run.return_value = _make_summary()

        engine = StrobEngine(url="http://test.io", concurrency=25)
        engine.run()

        config = mock_run.call_args[0][0]
        assert config.url == "http://test.io"
        assert config.concurrency == 25


class TestStrobEngineRunAsync:
    @patch("strobengine.engine.run_load_test")
    async def test_run_async_returns_summary(self, mock_run):
        expected = _make_summary()
        mock_run.return_value = expected

        engine = StrobEngine(url="http://example.com")
        result = await engine.run_async()

        assert result is expected


class TestLoadTestFactory:
    def test_load_test_defaults(self):
        engine = StrobEngine.load_test(url="http://example.com")
        assert engine.config.url == "http://example.com"
        assert engine.config.concurrency == 10
        assert engine.config.duration_secs == 10
        assert engine.config.timeout_secs == 10
        assert engine._profile is None

    def test_load_test_custom(self):
        engine = StrobEngine.load_test(
            url="http://test.io",
            concurrency=50,
            duration=30,
            options=RequestOptions(timeout=5),
        )
        assert engine.config.concurrency == 50
        assert engine.config.duration_secs == 30
        assert engine.config.timeout_secs == 5


class TestStressTestFactory:
    def test_stress_test_creates_profile(self):
        engine = StrobEngine.stress_test(
            url="http://example.com",
            start_concurrency=10,
            max_concurrency=200,
            ramp_duration=60,
            hold_duration=30,
        )
        assert engine._url == "http://example.com"
        assert engine._profile is not None
        assert engine.config is None

    def test_stress_test_invalid_start(self):
        with pytest.raises(
            ValueError, match="start_concurrency must be greater than 0"
        ):
            StrobEngine.stress_test(
                url="http://example.com", start_concurrency=0, max_concurrency=200
            )

    def test_stress_test_invalid_max(self):
        with pytest.raises(ValueError, match="max_concurrency must be greater than 0"):
            StrobEngine.stress_test(
                url="http://example.com", start_concurrency=10, max_concurrency=0
            )

    def test_stress_test_start_exceeds_max(self):
        with pytest.raises(
            ValueError, match="start_concurrency must be <= max_concurrency"
        ):
            StrobEngine.stress_test(
                url="http://example.com", start_concurrency=200, max_concurrency=10
            )

    def test_stress_test_negative_ramp(self):
        with pytest.raises(ValueError, match="ramp_duration must be >= 0"):
            StrobEngine.stress_test(url="http://example.com", ramp_duration=-1)


class TestSpikeTestFactory:
    def test_spike_test_creates_profile(self):
        engine = StrobEngine.spike_test(
            url="http://example.com",
            baseline=5,
            peak_concurrency=500,
            pre_spike_duration=5,
            spike_duration=10,
            post_spike_duration=5,
        )
        assert engine._url == "http://example.com"
        assert engine._profile is not None
        assert engine.config is None

    def test_spike_test_invalid_baseline(self):
        with pytest.raises(ValueError, match="baseline must be greater than 0"):
            StrobEngine.spike_test(url="http://example.com", baseline=0)

    def test_spike_test_invalid_peak(self):
        with pytest.raises(ValueError, match="peak_concurrency must be greater than 0"):
            StrobEngine.spike_test(url="http://example.com", peak_concurrency=0)

    def test_spike_test_negative_pre_spike(self):
        with pytest.raises(ValueError, match="pre_spike_duration must be >= 0"):
            StrobEngine.spike_test(url="http://example.com", pre_spike_duration=-1)


class TestProfileRun:
    @patch("strobengine.engine.run_load_profiles")
    def test_stress_test_run_calls_profiles(self, mock_run):
        mock_run.return_value = _make_summary()

        engine = StrobEngine.stress_test(url="http://example.com")
        result = engine.run()

        mock_run.assert_called_once()
        assert result is not None

    @patch("strobengine.engine.run_load_profiles")
    def test_spike_test_run_calls_profiles(self, mock_run):
        mock_run.return_value = _make_summary()

        engine = StrobEngine.spike_test(url="http://example.com")
        result = engine.run()

        mock_run.assert_called_once()
        assert result is not None
