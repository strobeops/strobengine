"""JUnit XML report generator for CI pipeline ingestion."""

from __future__ import annotations

from pathlib import Path
from xml.etree.ElementTree import Element, SubElement, tostring

from strobengine.reporter import build_artifact_dict


def render_junit_report(summary, config, duration_secs: float) -> str:
    """Render a JUnit XML report from TestSummary."""
    artifact = build_artifact_dict(summary, config)
    meta = artifact["metadata"]
    s = artifact["summary"]
    lp = artifact["latency_percentiles"]

    duration = duration_secs or meta.get("duration_secs", 0)
    failures = s["failed_requests"]
    error_rate = (
        (s["failed_requests"] / s["total_requests"] * 100)
        if s["total_requests"] > 0
        else 0.0
    )

    # Build XML
    testsuites = Element("testsuites")
    testsuite = SubElement(
        testsuites,
        "testsuite",
        attrib={
            "name": "strobengine",
            "tests": "1",
            "failures": str(1 if failures > 0 else 0),
            "time": f"{duration:.1f}",
        },
    )

    testcase = SubElement(
        testsuite,
        "testcase",
        attrib={
            "name": f"load_test {meta['target_url']}",
            "classname": "strobengine",
            "time": f"{duration:.1f}",
        },
    )

    # System-out with metrics
    system_out = SubElement(testcase, "system-out")
    system_out.text = (
        f"RPS: {s['rps']:.2f}, "
        f"P50: {lp['p50_us'] / 1000:.2f}ms, "
        f"P95: {lp['p95_us'] / 1000:.2f}ms, "
        f"P99: {lp['p99_us'] / 1000:.2f}ms, "
        f"Errors: {error_rate:.2f}%, "
        f"Total: {s['total_requests']:,}"
    )

    # If there are failures, add a failure element
    if failures > 0:
        failure = SubElement(
            testcase,
            "failure",
            attrib={
                "message": f"{failures} requests failed ({error_rate:.2f}%)",
                "type": "performance_regression",
            },
        )
        failure.text = f"{failures} out of {s['total_requests']} requests failed"

    xml_bytes = tostring(testsuites, encoding="unicode", xml_declaration=True)
    return xml_bytes


def save_junit_report(summary, config, filepath: str, duration_secs: float) -> str:
    """Render and write JUnit XML report to disk. Returns filepath."""
    filepath = str(Path(filepath).expanduser().resolve())
    xml = render_junit_report(summary, config, duration_secs)
    Path(filepath).parent.mkdir(parents=True, exist_ok=True)
    Path(filepath).write_text(xml)
    return filepath
