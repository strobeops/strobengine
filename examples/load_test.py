"""Basic constant load test -- fires N concurrent GET requests for D seconds."""

from strobengine import StrobEngine
from strobengine.reporter import print_summary


def load_test():
    # 50 concurrent workers hitting the endpoint for 30 seconds
    engine = StrobEngine(
        url="http://localhost:8080/get",
        concurrency=50,
        duration=30,
    )
    summary = engine.run()
    print_summary(summary)


def load_test_json():
    engine = StrobEngine(
        url="http://localhost:8080/get",
    )
    summary = engine.run()
    print_summary(summary, json_output=True)


load_test()
load_test_json()
