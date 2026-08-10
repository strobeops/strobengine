"""Stress test -- ramps from 10 to 200 workers over 60s, holds for 30s."""

from strobengine import StrobEngine
from strobengine.reporter import print_summary

engine = StrobEngine.stress_test(
    "http://localhost:8080/get",
    start_concurrency=10,
    max_concurrency=200,
    ramp_duration=60,
    hold_duration=30,
)
summary = engine.run()

print_summary(summary)
