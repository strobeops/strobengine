"""JMS / STOMP message broker load testing examples.

JMS is a Java API, so brokers are benchmarked over STOMP 1.2 — the wire protocol
spoken by the mainstream JMS brokers (ActiveMQ "Classic" and Artemis). These
examples produce (SEND) and consume (SUBSCRIBE/MESSAGE) against a STOMP broker.
A local ActiveMQ Classic exposes STOMP on port 61613:

  podman run -d --rm -p 61613:61613 docker.io/rmohr/activemq
"""

from strobengine import RequestOptions, StrobEngine
from strobengine.engine import StompAckEnum, StompModeEnum
from strobengine.reporter import print_summary

STOMP_URL = "stomp://localhost:61613"
DESTINATION = "/queue/orders"
MESSAGE = '{"order_id": 1}'


def produce_test():
    """Publish fire-and-forget (highest throughput)."""
    engine = StrobEngine(
        url=STOMP_URL,
        concurrency=10,
        duration=30,
        options=RequestOptions(
            stomp_destination=DESTINATION,
            stomp_message=MESSAGE,
            stomp_mode=StompModeEnum.produce,
        ),
    )
    summary = engine.run()
    print_summary(summary)


def produce_with_receipt_test():
    """Publish awaiting a broker ``RECEIPT`` (true ack latency)."""
    engine = StrobEngine(
        url=STOMP_URL,
        concurrency=10,
        duration=30,
        options=RequestOptions(
            stomp_destination=DESTINATION,
            stomp_message=MESSAGE,
            stomp_mode=StompModeEnum.produce,
            stomp_receipt=True,
        ),
    )
    summary = engine.run()
    print_summary(summary)


def consume_test():
    """Subscribe and drain messages (delivery throughput + intervals)."""
    engine = StrobEngine(
        url=STOMP_URL,
        concurrency=4,
        duration=30,
        options=RequestOptions(
            stomp_destination=DESTINATION,
            stomp_mode=StompModeEnum.consume,
            stomp_ack=StompAckEnum.auto,
        ),
    )
    summary = engine.run()
    print_summary(summary)


def chaos_test():
    """Producer with chaos fault injection."""
    engine = StrobEngine(
        url=STOMP_URL,
        concurrency=10,
        duration=30,
        options=RequestOptions(
            stomp_destination=DESTINATION,
            stomp_message=MESSAGE,
            stomp_mode=StompModeEnum.produce,
            chaos=True,
        ),
    )
    summary = engine.run()
    print_summary(summary)


def result_analysis():
    """Inspect STOMP/JMS messaging metrics."""
    engine = StrobEngine(
        url=STOMP_URL,
        concurrency=5,
        duration=5,
        options=RequestOptions(
            stomp_destination=DESTINATION,
            stomp_message=MESSAGE,
            stomp_mode=StompModeEnum.produce,
        ),
    )
    summary = engine.run()

    print(f"Total requests:      {summary.total_requests}")
    print(f"Total errors:        {summary.total_errors}")
    if summary.stomp is not None:
        print(f"Messages sent:       {summary.stomp.messages_sent_total}")
        print(f"Messages received:   {summary.stomp.messages_received_total}")
        print(f"Send latency p50:    {summary.stomp.send_latency_p50_ms:.2f} ms")


if __name__ == "__main__":
    produce_test()
    produce_with_receipt_test()
    consume_test()
    chaos_test()
    result_analysis()
