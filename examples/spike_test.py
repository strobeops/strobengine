"""Spike test -- baseline 5 -> peak 500 -> back to 5."""

from strobengine import StrobEngine
from strobengine.reporter import print_summary

engine = StrobEngine.spike_test(
    "http://localhost:8080/get",
    baseline=5,
    peak_concurrency=500,
    pre_spike_duration=5,
    spike_duration=10,
    post_spike_duration=5,
)
summary = engine.run()

print_summary(summary)
