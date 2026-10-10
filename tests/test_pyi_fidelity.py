"""Stub-vs-Rust fidelity checks for ``_strobengine.pyi``.

Parses the Rust binding sources and the type stub as text (no compilation)
and asserts the mutability contract in both directions:

- pyi plain (writable) fields == Rust ``#[pyo3(get, set)]`` fields
- pyi ``@property`` fields == Rust ``#[pyo3(get)]`` fields
- ``TestConfig.__init__`` params match Rust ``#[new]`` on names, Option
  nullability, and default presence

Drift here means pyright blesses (or rejects) assignments that behave the
opposite way at runtime.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
_PYI_PATH = _ROOT / "src" / "strobengine" / "_strobengine.pyi"
_RUST_SOURCES = [
    _ROOT / "src" / "config.rs",
    _ROOT / "src" / "metrics" / "mod.rs",
    _ROOT / "src" / "metrics" / "system.rs",
]
_LIB_RS = _ROOT / "src" / "lib.rs"

_EXPECTED_FIDELITY_CLASSES = {
    "GrpcMetrics",
    "Http3Metrics",
    "QuicMetrics",
    "ResourceSample",
    "SseMetrics",
    "SystemMetrics",
    "TestConfig",
    "TestSummary",
    "WebsocketMetrics",
}


def _rust_pyclass_structs() -> dict[str, dict[str, str]]:
    """Map pyclass struct name -> {field name: 'get' | 'get, set'}."""
    structs: dict[str, dict[str, str]] = {}
    for path in _RUST_SOURCES:
        text = path.read_text()
        for match in re.finditer(r"#\[pyclass[^\]]*\]", text):
            struct = re.match(
                r"\s*(?:#\[[^\]]*\]\s*)*pub struct (\w+)\s*\{",
                text[match.end() :],
            )
            if struct is None:
                continue
            body_start = match.end() + struct.end()
            body = text[body_start : text.index("}", body_start)]
            fields = {
                field.group(2): field.group(1)
                for field in re.finditer(
                    r"#\[pyo3\((get, set|get)\)\]\s*pub (\w+):", body
                )
            }
            structs[struct.group(1)] = fields
    return structs


def _pyi_class_members() -> dict[str, tuple[set[str], set[str]]]:
    """Map pyi class name -> (writable field names, property names)."""
    classes: dict[str, tuple[set[str], set[str]]] = {}
    current: str | None = None
    writable: set[str] = set()
    properties: set[str] = set()
    lines = _PYI_PATH.read_text().splitlines()
    index = 0
    while index < len(lines):
        line = lines[index]
        class_match = re.match(r"class (\w+):", line)
        if class_match:
            if current is not None:
                classes[current] = (writable, properties)
            current = class_match.group(1)
            writable, properties = set(), set()
        elif line and not line[0].isspace():
            if current is not None:
                classes[current] = (writable, properties)
            current = None
        elif current is not None:
            if re.match(r"\s*@property", line) and index + 1 < len(lines):
                def_match = re.match(r"\s*def (\w+)\(", lines[index + 1])
                if def_match:
                    properties.add(def_match.group(1))
                index += 1
            else:
                field_match = re.match(r"    (\w+): ", line)
                if field_match:
                    writable.add(field_match.group(1))
        index += 1
    if current is not None:
        classes[current] = (writable, properties)
    return classes


def _split_top_level(text: str) -> list[str]:
    parts: list[str] = []
    current: list[str] = []
    depth = 0
    for char in text:
        if char in "(<[":
            depth += 1
        elif char in ")]>":
            depth -= 1
        if char == "," and depth == 0:
            parts.append("".join(current))
            current = []
        else:
            current.append(char)
    tail = "".join(current).strip()
    if tail:
        parts.append(tail)
    return [part.strip() for part in parts if part.strip()]


def _rust_new_params() -> dict[str, tuple[bool, bool]]:
    """Map TestConfig::new param name -> (is_option, has_default)."""
    text = _RUST_SOURCES[0].read_text()
    signature = re.search(
        r"#\[new\]\s*#\[pyo3\(signature = \((?P<body>.*?)\)\)\]", text, re.S
    )
    assert signature is not None, "TestConfig #[new] signature not found"
    defaults: dict[str, bool] = {}
    for entry in _split_top_level(signature.group("body")):
        name = entry.split("=", 1)[0].strip()
        defaults[name] = "=" in entry

    function = re.search(
        r"pub fn new\s*\((?P<params>.*?)\)\s*->\s*PyResult<Self>", text, re.S
    )
    assert function is not None, "TestConfig::new not found"
    params: dict[str, tuple[bool, bool]] = {}
    for entry in _split_top_level(function.group("params")):
        name, _, param_type = entry.partition(":")
        params[name.strip()] = (
            param_type.strip().startswith("Option<"),
            defaults[name.strip()],
        )
    return params


def _pyi_init_params() -> dict[str, tuple[bool, bool]]:
    """Map TestConfig.__init__ param name -> (is_optional, has_default)."""
    text = _PYI_PATH.read_text()
    init = re.search(
        r"class TestConfig:.*?def __init__\((?P<params>.*?)\) -> None:",
        text,
        re.S,
    )
    assert init is not None, "TestConfig.__init__ not found"
    params: dict[str, tuple[bool, bool]] = {}
    for entry in _split_top_level(init.group("params")):
        if entry == "self":
            continue
        name, _, annotation = entry.partition(":")
        params[name.strip()] = (
            "| None" in annotation.partition("=")[0],
            "=" in annotation,
        )
    return params


RUST_STRUCTS = _rust_pyclass_structs()
PYI_CLASSES = _pyi_class_members()
SHARED_CLASSES = sorted(set(RUST_STRUCTS) & set(PYI_CLASSES))


class TestPyiFidelity:
    def test_expected_classes_covered(self):
        assert set(SHARED_CLASSES) == _EXPECTED_FIDELITY_CLASSES

    @pytest.mark.parametrize("class_name", SHARED_CLASSES)
    def test_field_mutability_matches_rust(self, class_name):
        rust_fields = RUST_STRUCTS[class_name]
        expected_writable = {
            name for name, access in rust_fields.items() if access == "get, set"
        }
        expected_readonly = {
            name for name, access in rust_fields.items() if access == "get"
        }
        pyi_writable, pyi_properties = PYI_CLASSES[class_name]
        assert pyi_writable == expected_writable, (
            f"{class_name}: pyi writable {sorted(pyi_writable)} != rust get,set {sorted(expected_writable)}"
        )
        assert pyi_properties == expected_readonly, (
            f"{class_name}: pyi properties {sorted(pyi_properties)} != rust get-only {sorted(expected_readonly)}"
        )

    def test_registered_classes_declared_in_pyi(self):
        registered = set(
            re.findall(r"m\.add_class::<[\w:]+::(\w+)>\(\)", _LIB_RS.read_text())
        )
        assert registered
        assert registered <= set(PYI_CLASSES), sorted(registered - set(PYI_CLASSES))

    def test_config_init_params_match_rust_new(self):
        rust_params = _rust_new_params()
        pyi_params = _pyi_init_params()
        assert len(rust_params) >= 35, "parser missed TestConfig::new params"
        assert set(rust_params) == set(pyi_params), (
            f"param set drift: rust-only {sorted(set(rust_params) - set(pyi_params))}, "
            f"pyi-only {sorted(set(pyi_params) - set(rust_params))}"
        )
        for name, (rust_option, rust_default) in rust_params.items():
            pyi_option, pyi_default = pyi_params[name]
            assert pyi_option == rust_option, f"{name}: nullability drift"
            assert pyi_default == rust_default, f"{name}: default presence drift"
