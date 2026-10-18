"""Static contract checks for the ``examples/`` scripts.

The examples are runnable snippets, not part of the test suite, so they are not
executed here (most require an external target). Instead these tests parse each
example and verify that every ``RequestOptions(...)`` and ``StrobEngine(...)``
keyword is a real API parameter, catching drift when engine options are renamed
or removed.
"""

from __future__ import annotations

import ast
import dataclasses
import inspect
from collections.abc import Iterator
from pathlib import Path

import pytest

from strobengine.engine import RequestOptions, StrobEngine

_EXAMPLES_DIR = Path(__file__).resolve().parent.parent / "examples"
_EXAMPLE_FILES = sorted(_EXAMPLES_DIR.glob("*.py"))

_STROBENGINE_CLASSMETHODS = ("load_test", "stress_test", "spike_test")


def _request_option_fields() -> set[str]:
    return {field.name for field in dataclasses.fields(RequestOptions)}


def _callee_name(call: ast.Call) -> tuple[str | None, str | None]:
    func = call.func
    if isinstance(func, ast.Name):
        return func.id, None
    if isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name):
        return func.value.id, func.attr
    return None, None


def _iter_calls(tree: ast.AST) -> Iterator[ast.Call]:
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            yield node


def _keyword_args(call: ast.Call) -> list[str]:
    return [kw.arg for kw in call.keywords if kw.arg is not None]


def test_examples_exist() -> None:
    assert _EXAMPLE_FILES, f"no example scripts found in {_EXAMPLES_DIR}"


@pytest.mark.parametrize("path", _EXAMPLE_FILES, ids=lambda p: p.name)
def test_request_options_kwargs_are_valid(path: Path) -> None:
    tree = ast.parse(path.read_text(), filename=str(path))
    valid = _request_option_fields()
    for call in _iter_calls(tree):
        name, _ = _callee_name(call)
        if name != "RequestOptions":
            continue
        for keyword in _keyword_args(call):
            assert keyword in valid, (
                f"{path.name}: unknown RequestOptions option '{keyword}'"
            )


@pytest.mark.parametrize("path", _EXAMPLE_FILES, ids=lambda p: p.name)
def test_strobengine_kwargs_are_valid(path: Path) -> None:
    tree = ast.parse(path.read_text(), filename=str(path))
    signatures: dict[str | None, set[str]] = {
        None: set(inspect.signature(StrobEngine.__init__).parameters) - {"self"}
    }
    for name in _STROBENGINE_CLASSMETHODS:
        signatures[name] = set(inspect.signature(getattr(StrobEngine, name)).parameters)

    for call in _iter_calls(tree):
        owner, attr = _callee_name(call)
        if owner != "StrobEngine" or attr not in signatures:
            continue
        valid = signatures[attr]
        for keyword in _keyword_args(call):
            target = f"StrobEngine.{attr}" if attr else "StrobEngine"
            assert keyword in valid, f"{path.name}: unknown {target} option '{keyword}'"
