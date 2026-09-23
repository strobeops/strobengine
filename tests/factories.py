"""Shared mock factories for the unit test suite.

Consolidates the previously-duplicated ``_make_summary`` builders. The
baseline mirrors the richest existing scenario (``test_reporting``) so the
majority of call sites need no overrides; tests that assert on specific values
pass explicit keyword overrides instead.
"""

from unittest.mock import Mock

# Complete float-bearing baseline: every attribute that ``print_summary`` and
# the report builders read numerically is a real value (not a Mock) so f-string
# ``:.2f`` formatting and ``> 0`` guards never hit an un-configured Mock child.
_SUMMARY_BASELINE = {
    "url": "http://localhost:8080",
    "total_requests": 100,
    "total_errors": 0,
    "average_latency_ms": 1.5,
    "p50_latency_ms": 1.0,
    "p90_latency_ms": 2.0,
    "p95_latency_ms": 3.0,
    "p99_latency_ms": 5.0,
    "p99_99_latency_ms": 9.5,
    "min_latency_ms": 0.1,
    "max_latency_ms": 10.0,
    "std_dev_latency_ms": 0.5,
    "total_bytes_received": 1024,
    "duration_secs": 10.0,
    "workers": 5,
    "timestamp": "2026-08-28T10:00:00Z",
    "raw_command": None,
    "status_codes": {200: 100},
    "avg_e2e_latency_us": 0.0,
    "chaos_injected_total": 0,
    "chaos_faults_by_type": {},
    "latency_histogram": {"<1ms": 10, "1-5ms": 50, "5-10ms": 40},
    "to_dict": lambda: {"quic": None, "sse": None},
    "to_json": lambda indent=None: "{}",
}


def make_summary(**overrides: object) -> Mock:
    """Return a Mock TestSummary seeded with realistic defaults.

    Pass keyword arguments to override any baseline field for a specific test.
    """
    summary = Mock()
    values = {**_SUMMARY_BASELINE, **overrides}
    for key, value in values.items():
        setattr(summary, key, value)
    return summary
