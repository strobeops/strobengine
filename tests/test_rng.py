import os
import subprocess
import sys

import pytest

from strobengine.rng import _MASK64, _derive_seed


def test_known_vectors():
    assert _derive_seed(0, 0, 0) == 7353880644836609869
    assert _derive_seed(0, 0, 1) == 3991335417537642247
    assert _derive_seed(0, 1, 0) == 11622201985095632708
    assert _derive_seed(42, 7, 13) == 5263063952022198637
    assert _derive_seed(_MASK64, _MASK64, _MASK64) == 6023548976005198156


def test_derivation_is_deterministic():
    assert _derive_seed(42, 7, 13) == _derive_seed(42, 7, 13)


def test_vu_and_iteration_are_separated():
    seeds = {
        _derive_seed(0, vu, iteration) for vu in range(10) for iteration in range(10)
    }
    assert len(seeds) == 100
    assert _derive_seed(0, 0, 1) != _derive_seed(0, 1, 0)


def test_inputs_must_be_integers():
    for args in ((0.0, 0, 0), (0, 0.0, 0), (0, 0, 0.0)):
        with pytest.raises(TypeError, match="must be an integer"):
            _derive_seed(*args)


def test_rng_stream_is_stable_across_processes():
    code = (
        "from strobengine.engine import RequestOptions\n"
        "rng = RequestOptions(test_seed=42).rng_for(7, 13)\n"
        "print(' '.join(str(rng.getrandbits(64)) for _ in range(3)))\n"
    )
    outputs = []
    for hashseed in ("0", "random"):
        env = {**os.environ, "PYTHONHASHSEED": hashseed}
        outputs.append(
            subprocess.check_output(
                [sys.executable, "-c", code], env=env, text=True
            ).strip()
        )
    assert outputs[0] == outputs[1]
    assert outputs[0] == (
        "17802986416972374973 8903682632858472175 10154987994014175804"
    )


def test_output_is_u64():
    for seed in (
        _derive_seed(0, 0, 0),
        _derive_seed(1, 2, 3),
        _derive_seed(_MASK64, _MASK64, _MASK64),
    ):
        assert 0 <= seed <= _MASK64


@pytest.mark.parametrize(
    ("test_seed", "vu_id", "iteration"),
    [
        (-1, 0, 0),
        (0, -1, 0),
        (0, 0, -1),
        (1 << 64, 0, 0),
        (0, 1 << 64, 0),
        (0, 0, 1 << 64),
    ],
)
def test_inputs_must_fit_in_u64(test_seed, vu_id, iteration):
    with pytest.raises(ValueError, match="must be between 0 and 2\\*\\*64 - 1"):
        _derive_seed(test_seed, vu_id, iteration)
