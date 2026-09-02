"""JUnit XML report generator for CI pipeline ingestion."""

from __future__ import annotations

from pathlib import Path
from xml.etree.ElementTree import Element, SubElement, tostring

from strobengine.reporter import build_artifact_dict


def generate_junit_report(artifact: dict) -> str:
    """Generate JUnit XML with performance assertion testcases.

    Includes three testcases:
    1. Overall load test with system-out metrics
    2. Error rate threshold (> 1% triggers failure)
    3. P95 latency threshold (> 100ms triggers failure)
    """
    meta = artifact["metadata"]
    s = artifact["summary"]
    lp = artifact["latency_percentiles"]

    duration = meta["duration_secs"]
    failed = s["failed_requests"]
    total = s["total_requests"]
    error_rate = (failed / total * 100) if total > 0 else 0.0
    from strobengine.reporting import us_to_ms

    p95_ms = us_to_ms(lp["p95_us"])

    testsuites = Element("testsuites")
    testsuite = SubElement(
        testsuites,
        "testsuite",
        attrib={"name": "strobengine", "tests": "3", "time": f"{duration:.1f}"},
    )

    failure_count = 0

    # Testcase 1: Overall load test
    tc1 = SubElement(
        testsuite,
        "testcase",
        attrib={
            "name": f"load_test {meta['target_url']}",
            "classname": "strobengine",
            "time": f"{duration:.1f}",
        },
    )
    so1 = SubElement(tc1, "system-out")
    so1.text = (
        f"RPS: {s['rps']:.2f}, "
        f"P50: {us_to_ms(lp['p50_us'])}ms, "
        f"P95: {p95_ms}ms, "
        f"P99: {us_to_ms(lp['p99_us'])}ms, "
        f"Errors: {error_rate:.2f}%, "
        f"Total: {total:,}"
    )
    if failed > 0:
        SubElement(
            tc1,
            "failure",
            attrib={
                "message": f"{failed} requests failed ({error_rate:.2f}%)",
                "type": "performance_regression",
            },
        )
        failure_count += 1

    # Testcase 2: Error rate threshold (> 1%)
    tc2 = SubElement(
        testsuite,
        "testcase",
        attrib={
            "name": "error_rate_threshold",
            "classname": "strobengine",
            "time": "0.0",
        },
    )
    if error_rate > 1.0:
        SubElement(
            tc2,
            "failure",
            attrib={
                "message": f"Error rate {error_rate:.2f}% exceeds 1% threshold",
                "type": "threshold_breach",
            },
        )
        failure_count += 1

    # Testcase 3: P95 latency threshold (> 100ms)
    tc3 = SubElement(
        testsuite,
        "testcase",
        attrib={
            "name": "p95_latency_threshold",
            "classname": "strobengine",
            "time": "0.0",
        },
    )
    if p95_ms > 100.0:
        SubElement(
            tc3,
            "failure",
            attrib={
                "message": f"P95 latency {p95_ms:.2f}ms exceeds 100ms threshold",
                "type": "threshold_breach",
            },
        )
        failure_count += 1

    # Set accurate failure count
    testsuite.set("failures", str(failure_count))

    xml_bytes = tostring(testsuites, encoding="unicode", xml_declaration=True)
    return xml_bytes


def render_junit_report(summary, config, duration_secs: float) -> str:
    """Render a JUnit XML report from TestSummary (convenience wrapper)."""
    artifact = build_artifact_dict(summary, config)
    return generate_junit_report(artifact)


def save_junit_report(summary, config, filepath: str, duration_secs: float) -> str:
    """Render and write JUnit XML report to disk. Returns filepath."""
    filepath = str(Path(filepath).expanduser().resolve())
    artifact = build_artifact_dict(summary, config)
    xml = generate_junit_report(artifact)
    Path(filepath).parent.mkdir(parents=True, exist_ok=True)
    Path(filepath).write_text(xml)
    return filepath
