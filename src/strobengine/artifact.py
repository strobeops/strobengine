"""Report artifact construction.

Thin bridge to the Rust schema builder: :func:`build_artifact_dict` delegates to
``build_report_artifact_dict``, so the Rust ``ReportArtifact`` serde output is
the single source of truth for the report schema. The typed shape lives in
:mod:`strobengine.report_schema`.
"""

from __future__ import annotations

from typing import cast

from strobengine._strobengine import (
    TestConfig,
    TestSummary,
    build_report_artifact_dict,
)
from strobengine.report_schema import ReportArtifactDict


def build_artifact_dict(summary: TestSummary, config: TestConfig) -> ReportArtifactDict:
    """Build a ``ReportArtifact`` dict from a run summary and its config."""
    if not isinstance(config, TestConfig):
        raise TypeError("build_artifact_dict requires a Rust TestConfig")
    return cast(ReportArtifactDict, build_report_artifact_dict(summary, config))
