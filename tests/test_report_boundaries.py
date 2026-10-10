"""Guard tests for the report-boundary typing contracts.

Three contracts that pyright alone cannot re-check over time:

1. ``artifact.py`` must read summary/config attributes directly — reintroducing
   ``getattr(x, "attr", default)`` would mask Rust field renames as silent
   defaults (ruff's B009 only flags two-argument ``getattr``).
2. :class:`SupportsCliOptions` (Protocol) must mirror :class:`CliOptionsDict`
   key-for-key, so the typed config boundary and the serialized shape cannot
   drift apart.
3. :class:`ComparisonDict` must match the keys actually produced by
   :func:`compute_comparison`, so the HTML/CLI consumers stay type-safe.
"""

from __future__ import annotations

from pathlib import Path

from strobengine.report_schema import CliOptionsDict, SupportsCliOptions
from strobengine.reporting.baseline import ComparisonDict, compute_comparison

_ARTIFACT_PY = Path(__file__).parent.parent / "src" / "strobengine" / "artifact.py"


def _minimal_artifact(
    rps: float = 10.0, total: int = 100, failed: int = 0, p95_us: float = 1000.0
) -> dict:
    return {
        "metadata": {"timestamp": "2026-01-01T00:00:00Z", "target_url": "http://x"},
        "summary": {"rps": rps, "total_requests": total, "failed_requests": failed},
        "latency_percentiles": {"p95_us": p95_us},
    }


class TestNoDynamicAttrAccess:
    def test_artifact_builders_use_direct_attribute_access(self):
        source = _ARTIFACT_PY.read_text()
        for needle in ("getattr(summary", "getattr(config"):
            assert needle not in source, (
                f"{needle!r} found in artifact.py — direct attribute access is "
                "required so Rust renames fail loudly instead of falling back "
                "to a silent default"
            )


class TestProtocolParity:
    def test_supports_cli_options_mirrors_cli_options_dict(self):
        protocol_keys = set(SupportsCliOptions.__annotations__)
        typed_dict_keys = set(CliOptionsDict.__annotations__)
        assert protocol_keys == typed_dict_keys, (
            f"SupportsCliOptions drift: missing={typed_dict_keys - protocol_keys} "
            f"extra={protocol_keys - typed_dict_keys}"
        )


class TestComparisonDictParity:
    def test_comparison_dict_matches_compute_comparison_output(self):
        result = compute_comparison(_minimal_artifact(), _minimal_artifact())
        produced = set(result)
        declared = set(ComparisonDict.__annotations__)
        assert produced == declared, (
            f"ComparisonDict drift: missing={declared - produced} "
            f"extra={produced - declared}"
        )
