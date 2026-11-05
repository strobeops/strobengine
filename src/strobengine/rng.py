"""Deterministic per-VU seed derivation for data generation.

The derivation is intentionally small and dependency-free: SplitMix64 mixing
over a run-level seed, VU id, and iteration number. It is independent of
Python's hash randomization and of worker scheduling, so the same inputs
produce the same seed across runs and processes.

This seeds Python-side fixture/data generation only; Rust chaos stays on
``fastrand`` and is not controlled by this seed.

``RequestOptions.rng_for`` and ``StrobEngine.rng_for`` expose the public
``random.Random`` stream. Streams are stable for a given Python version;
CPython does not guarantee every higher-level ``random.Random`` method across
interpreter versions.
"""

import operator

_MASK64 = (1 << 64) - 1
_GAMMA = 0x9E3779B97F4A7C15
_MUL1 = 0xBF58476D1CE4E5B9
_MUL2 = 0x94D049BB133111EB
_DOMAIN = 0x5354524F42


def _splitmix64(value: int) -> int:
    value = (value + _GAMMA) & _MASK64
    value = ((value ^ (value >> 30)) * _MUL1) & _MASK64
    value = ((value ^ (value >> 27)) * _MUL2) & _MASK64
    return value ^ (value >> 31)


def _as_u64(name: str, value: int) -> int:
    try:
        index = operator.index(value)
    except TypeError:
        raise TypeError(f"{name} must be an integer") from None
    if not 0 <= index <= _MASK64:
        raise ValueError(f"{name} must be between 0 and 2**64 - 1")
    return index


def _derive_seed(test_seed: int, vu_id: int, iteration: int) -> int:
    """Return a stable 64-bit seed for ``(test_seed, vu_id, iteration)``.

    ``iteration`` is zero-based. Inputs must be integer-like values in
    ``0..2**64-1``.
    """
    test_seed = _as_u64("test_seed", test_seed)
    vu_id = _as_u64("vu_id", vu_id)
    iteration = _as_u64("iteration", iteration)
    mixed = _splitmix64(test_seed ^ _DOMAIN)
    mixed = _splitmix64(mixed ^ _splitmix64(vu_id))
    return _splitmix64(mixed ^ _splitmix64(iteration))
