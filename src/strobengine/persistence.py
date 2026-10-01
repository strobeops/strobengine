"""Report persistence: atomic on-disk writing of the ReportArtifact JSON.

Keeps disk I/O (temp file + ``os.replace``, ``latest.json`` pointer, cleanup)
separate from artifact construction (:mod:`strobengine.artifact`) and console
display (:mod:`strobengine.reporter`).
"""

from __future__ import annotations

import os

from strobengine._strobengine import TestSummary

from .artifact import build_artifact_dict

DEFAULT_REPORT_DIR = ".strobengine/reports"


def _slugify_url(url: str) -> str:
    """Convert URL to a safe filename slug."""
    import re

    return re.sub(r"[^a-zA-Z0-9_-]", "_", url)[:80]


def save_report(
    summary: TestSummary,
    config: object,
    output_dir: str | None = None,
    no_save: bool = False,
) -> str | None:
    """Persist ReportArtifact to disk atomically. Returns filepath or None."""
    if no_save:
        return None

    import contextlib
    import json
    import tempfile
    from pathlib import Path

    dirpath = Path(output_dir or DEFAULT_REPORT_DIR)
    dirpath.mkdir(parents=True, exist_ok=True)

    filename = f"{summary.timestamp}_{_slugify_url(summary.url)}.json"
    filepath = dirpath / filename
    artifact = build_artifact_dict(summary, config)

    # Atomic write: temp file + os.replace
    tmp_fd, tmp_path = tempfile.mkstemp(dir=str(dirpath), suffix=".tmp")
    try:
        with os.fdopen(tmp_fd, "w") as f:
            json.dump(artifact, f, indent=2)
        os.replace(tmp_path, filepath)
    finally:
        # Cleanup runs on every path (success is a no-op: replace already moved
        # the file). contextlib.suppress ensures an unlink failure never masks
        # the original exception or an in-flight KeyboardInterrupt/SystemExit.
        if os.path.exists(tmp_path):
            with contextlib.suppress(OSError):
                os.unlink(tmp_path)

    # Update latest.json pointer atomically
    latest_path = dirpath / "latest.json"
    latest_tmp = dirpath / ".tmp_latest.json"
    latest_payload = {"latest_report": filename}
    with open(latest_tmp, "w") as f:
        json.dump(latest_payload, f, indent=2)
    os.replace(latest_tmp, latest_path)

    return str(filepath)
