from unittest.mock import patch

import pytest

from strobengine._strobengine import (
    LoadProfile,
    SoapVersion,
    TestConfig as _TestConfig,
    run_load_profiles,
    run_load_test,
)
from strobengine.engine import RequestOptions, StrobEngine

from .factories import make_summary as _make_summary


class TestStrobEngineInit:
    def test_default_values(self):
        engine = StrobEngine(url="http://example.com")
        assert engine.config.url == "http://example.com"
        assert engine.config.concurrency == 10
        assert engine.config.duration_secs == 10
        assert engine.config.timeout_secs == 10

    def test_custom_values(self):
        engine = StrobEngine(
            url="http://example.com",
            concurrency=50,
            duration=30,
            options=RequestOptions(timeout=5),
        )
        assert engine.config.url == "http://example.com"
        assert engine.config.concurrency == 50
        assert engine.config.duration_secs == 30
        assert engine.config.timeout_secs == 5

    def test_invalid_concurrency(self):
        with pytest.raises(ValueError, match="Concurrency must be greater than 0"):
            StrobEngine(url="http://example.com", concurrency=0)

    def test_negative_concurrency(self):
        with pytest.raises(ValueError, match="Concurrency must be greater than 0"):
            StrobEngine(url="http://example.com", concurrency=-5)

    def test_concurrency_above_limit(self):
        with pytest.raises(ValueError, match="Concurrency must be <= 10000"):
            StrobEngine(url="http://example.com", concurrency=10001)

    def test_invalid_duration(self):
        with pytest.raises(ValueError, match="Duration must be greater than 0"):
            StrobEngine(url="http://example.com", duration=0)

    def test_invalid_timeout(self):
        with pytest.raises(ValueError, match="timeout must be greater than 0"):
            StrobEngine(url="http://example.com", options=RequestOptions(timeout=0))


class TestStrobEngineRun:
    @patch("strobengine.reporter.save_report")
    @patch("strobengine.engine.run_load_test")
    def test_run_returns_summary(self, mock_run, mock_save):
        mock_run.return_value = _make_summary(url="http://example.com")
        mock_save.return_value = "/fake/path.json"

        engine = StrobEngine(url="http://example.com")
        result = engine.run()

        mock_run.assert_called_once_with(engine.config)
        mock_save.assert_called_once()
        # Verify enrichment was applied (clone creates independent copy)
        assert result.raw_command is not None
        assert "http://example.com" in result.raw_command

    @patch("strobengine.reporter.save_report")
    @patch("strobengine.engine.run_load_test")
    def test_run_with_custom_params(self, mock_run, mock_save):
        mock_run.return_value = _make_summary()
        mock_save.return_value = "/fake/path.json"

        engine = StrobEngine(url="http://example.com", concurrency=25)
        engine.run()

        config = mock_run.call_args[0][0]
        assert config.url == "http://example.com"
        assert config.concurrency == 25


class TestStrobEngineRunAsync:
    @patch("strobengine.reporter.save_report")
    @patch("strobengine.engine.run_load_test")
    async def test_run_async_returns_summary(self, mock_run, mock_save):
        mock_run.return_value = _make_summary(url="http://example.com")
        mock_save.return_value = "/fake/path.json"

        engine = StrobEngine(url="http://example.com")
        result = await engine.run_async()

        # Verify enrichment was applied (clone creates independent copy)
        assert result.raw_command is not None
        assert "http://example.com" in result.raw_command


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
            url="http://example.com",
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
        assert engine.config is not None
        assert engine.config.url == "http://example.com"
        assert engine.config.concurrency == 200  # max_concurrency
        assert engine.config.duration_secs == 90  # ramp + hold

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

    def test_stress_test_max_above_limit(self):
        with pytest.raises(ValueError, match="max_concurrency must be <= 10000"):
            StrobEngine.stress_test(
                url="http://example.com", start_concurrency=10, max_concurrency=10001
            )


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
        assert engine.config is not None
        assert engine.config.url == "http://example.com"
        assert engine.config.concurrency == 500  # peak_concurrency
        assert engine.config.duration_secs == 20  # pre + spike + post

    def test_spike_test_invalid_baseline(self):
        with pytest.raises(ValueError, match="baseline must be greater than 0"):
            StrobEngine.spike_test(url="http://example.com", baseline=0)

    def test_spike_test_invalid_peak(self):
        with pytest.raises(ValueError, match="peak_concurrency must be greater than 0"):
            StrobEngine.spike_test(url="http://example.com", peak_concurrency=0)

    def test_spike_test_negative_pre_spike(self):
        with pytest.raises(ValueError, match="pre_spike_duration must be >= 0"):
            StrobEngine.spike_test(url="http://example.com", pre_spike_duration=-1)

    def test_spike_test_baseline_above_limit(self):
        with pytest.raises(ValueError, match="baseline must be <= 10000"):
            StrobEngine.spike_test(url="http://example.com", baseline=10001)

    def test_spike_test_peak_above_limit(self):
        with pytest.raises(ValueError, match="peak_concurrency must be <= 10000"):
            StrobEngine.spike_test(url="http://example.com", peak_concurrency=10001)


class TestProfileRun:
    @patch("strobengine.reporter.save_report")
    @patch("strobengine.engine.run_load_profiles")
    def test_stress_test_run_calls_profiles(self, mock_run, mock_save):
        mock_run.return_value = _make_summary()
        mock_save.return_value = "/fake/path.json"

        engine = StrobEngine.stress_test(url="http://example.com")
        result = engine.run()

        mock_run.assert_called_once()
        assert result is not None

    @patch("strobengine.reporter.save_report")
    @patch("strobengine.engine.run_load_profiles")
    def test_spike_test_run_calls_profiles(self, mock_run, mock_save):
        mock_run.return_value = _make_summary()
        mock_save.return_value = "/fake/path.json"

        engine = StrobEngine.spike_test(url="http://example.com")
        result = engine.run()

        mock_run.assert_called_once()
        assert result is not None


class TestConcurrencyLimitFFI:
    """Guards on the pyfunction entries bypass all Python-side validation."""

    def test_run_load_test_rejects_oversized_config(self):
        config = _TestConfig(url="http://127.0.0.1:1", concurrency=1, duration_secs=1)
        config.concurrency = 10001
        with pytest.raises(ValueError, match="concurrency must be between 1 and 10000"):
            run_load_test(config)

    def test_run_load_test_rejects_zero_concurrency(self):
        config = _TestConfig(url="http://127.0.0.1:1", concurrency=1, duration_secs=1)
        config.concurrency = 0
        with pytest.raises(ValueError, match="concurrency must be between 1 and 10000"):
            run_load_test(config)

    def test_config_construction_rejects_oversized_concurrency(self):
        with pytest.raises(ValueError, match="concurrency must be between 1 and 10000"):
            _TestConfig(url="http://127.0.0.1:1", concurrency=10001, duration_secs=1)

    def test_config_construction_rejects_zero_concurrency(self):
        with pytest.raises(ValueError, match="concurrency must be between 1 and 10000"):
            _TestConfig(url="http://127.0.0.1:1", concurrency=0, duration_secs=1)

    @pytest.mark.parametrize("rate", [1.5, -0.1, float("nan")])
    def test_config_construction_rejects_invalid_chaos_rate(self, rate):
        with pytest.raises(
            ValueError, match=r"chaos_rate must be between 0\.0 and 1\.0"
        ):
            _TestConfig(
                url="http://127.0.0.1:1",
                concurrency=1,
                duration_secs=1,
                chaos_rate=rate,
            )

    def test_run_load_profiles_rejects_oversized_profile(self):
        config = _TestConfig(url="http://127.0.0.1:1", concurrency=10, duration_secs=1)
        profile = LoadProfile.constant(concurrency=10001, duration_secs=1)
        with pytest.raises(ValueError, match="concurrency must be between 1 and 10000"):
            run_load_profiles(config, profile)


class TestGraphqlConfigValidation:
    def test_graphql_query_cannot_combine_with_body(self):
        with pytest.raises(ValueError, match="cannot be combined with body or form"):
            _TestConfig(
                url="http://127.0.0.1:1",
                concurrency=1,
                duration_secs=1,
                graphql_query="{ x }",
                body="{}",
            )

    def test_graphql_variables_require_query(self):
        with pytest.raises(ValueError, match="require graphql_query"):
            _TestConfig(
                url="http://127.0.0.1:1",
                concurrency=1,
                duration_secs=1,
                graphql_variables='{"id": 1}',
            )

    def test_graphql_variables_must_be_json_object(self):
        with pytest.raises(ValueError, match="must be a JSON object"):
            _TestConfig(
                url="http://127.0.0.1:1",
                concurrency=1,
                duration_secs=1,
                graphql_query="{ x }",
                graphql_variables="[1, 2]",
            )

    def test_graphql_query_must_not_be_empty(self):
        with pytest.raises(ValueError, match="must not be empty"):
            _TestConfig(
                url="http://127.0.0.1:1",
                concurrency=1,
                duration_secs=1,
                graphql_query="   ",
            )

    def test_valid_graphql_query_is_accepted(self):
        config = _TestConfig(
            url="http://127.0.0.1:1",
            concurrency=1,
            duration_secs=1,
            graphql_query="query { x }",
            graphql_variables='{"id": 1}',
            graphql_operation_name="Q",
        )
        assert config.graphql_query == "query { x }"


class TestSloConfigValidation:
    def test_negative_p95_rejected(self):
        with pytest.raises(ValueError, match="slo_p95_ms must be a non-negative"):
            _TestConfig(
                url="http://127.0.0.1:1",
                concurrency=1,
                duration_secs=1,
                slo_p95_ms=-1.0,
            )

    def test_error_rate_above_100_rejected(self):
        with pytest.raises(
            ValueError, match="slo_error_rate must be between 0 and 100"
        ):
            _TestConfig(
                url="http://127.0.0.1:1",
                concurrency=1,
                duration_secs=1,
                slo_error_rate=150.0,
            )

    def test_valid_slo_thresholds_accepted(self):
        config = _TestConfig(
            url="http://127.0.0.1:1",
            concurrency=1,
            duration_secs=1,
            slo_p95_ms=50.0,
            slo_error_rate=0.1,
            slo_min_rps=100.0,
        )
        assert config.slo_p95_ms == 50.0
        assert config.slo_error_rate == 0.1


class TestSoapConfigValidation:
    def test_soap_requires_body(self):
        with pytest.raises(ValueError, match="requires an XML envelope"):
            _TestConfig(
                url="http://127.0.0.1:1",
                concurrency=1,
                duration_secs=1,
                soap_enabled=True,
            )

    def test_soap_rejects_form(self):
        with pytest.raises(ValueError, match="cannot be combined with form"):
            _TestConfig(
                url="http://127.0.0.1:1",
                concurrency=1,
                duration_secs=1,
                soap_enabled=True,
                body="<Envelope/>",
                form=[("a", "b")],
            )

    def test_soap_rejects_graphql(self):
        with pytest.raises(ValueError, match="cannot be combined with graphql_query"):
            _TestConfig(
                url="http://127.0.0.1:1",
                concurrency=1,
                duration_secs=1,
                soap_enabled=True,
                graphql_query="{ q }",
            )

    def test_valid_soap_config_accepted(self):
        config = _TestConfig(
            url="http://127.0.0.1:1",
            concurrency=1,
            duration_secs=1,
            soap_enabled=True,
            body="<Envelope/>",
            soap_version=SoapVersion.soap12(),
        )
        assert config.soap_enabled is True


class TestRequestOptionsMapping:
    def test_every_option_maps_to_test_config(self):
        import inspect

        from strobengine.engine import _test_config_options

        kwargs = _test_config_options(RequestOptions())
        params = set(inspect.signature(_TestConfig).parameters)
        explicit = {"url", "concurrency", "duration_secs"}
        # `ws_max_messages` is currently only settable through the low-level class.
        assert set(kwargs) | explicit == params - {"ws_max_messages"}

        config = _TestConfig(
            url="http://127.0.0.1:1",
            concurrency=1,
            duration_secs=1,
            **kwargs,
        )
        assert config.timeout_secs == RequestOptions.timeout

    def test_enum_options_are_translated(self):
        from strobengine._strobengine import (
            SoapVersion as NativeSoapVersion,
            StompAck as NativeStompAck,
            StompMode as NativeStompMode,
            WsMode as NativeWsMode,
        )
        from strobengine.engine import (
            SoapVersionEnum,
            StompAckEnum,
            StompModeEnum,
            WsModeEnum,
            _test_config_options,
        )

        kwargs = _test_config_options(
            RequestOptions(
                ws_mode=WsModeEnum.stream,
                soap_version=SoapVersionEnum.soap12,
                stomp_mode=StompModeEnum.consume,
                stomp_ack=StompAckEnum.client,
            )
        )
        assert isinstance(kwargs["ws_mode"], NativeWsMode)
        assert isinstance(kwargs["soap_version"], NativeSoapVersion)
        assert isinstance(kwargs["stomp_mode"], NativeStompMode)
        assert isinstance(kwargs["stomp_ack"], NativeStompAck)
