"""SDK-free unit tests plus an opt-in round trip against a live broker.

Run the integration test with a broker from `scripts/dev.sh up`:
    SOLACE_INTEGRATION=1 uv run pytest tests/test_broker.py
"""

import dataclasses
import logging
import os
import threading

import pytest

from kai_sim_cowork.broker import Broker, Inbound, Outbound, deliver, to_inbound
from kai_sim_cowork.config import broker_config

TOPIC = "cowork/itest/apse1/orchestration/task/created/v1"


class FakeSdkMessage:
    def __init__(self, topic, payload, props):
        self._topic, self._payload, self._props = topic, payload, props

    def get_destination_name(self):
        return self._topic

    def get_payload_as_bytes(self):
        return self._payload

    def get_properties(self):
        return self._props


def test_to_inbound_stringifies_properties():
    msg = FakeSdkMessage(TOPIC, b"{}", {"eventType": "x", "n": 1})
    assert to_inbound(msg) == Inbound(TOPIC, b"{}", {"eventType": "x", "n": "1"})


def test_to_inbound_tolerates_empty_payload_and_properties():
    assert to_inbound(FakeSdkMessage("t", None, None)) == Inbound("t", b"", {})


def test_deliver_reports_a_handled_message_for_ack():
    seen = []
    assert deliver(seen.append, Inbound(TOPIC, b"{}"), "q.x") is True
    assert seen == [Inbound(TOPIC, b"{}")]


def test_deliver_reports_a_raising_handler_for_failed_settlement(caplog):
    def broken(msg: Inbound) -> None:
        raise RuntimeError("publish timed out")

    with caplog.at_level(logging.ERROR):
        assert deliver(broken, Inbound(TOPIC, b"{}"), "q.x") is False
    assert "settling FAILED" in caplog.text
    assert "dead message queue" in caplog.text
    assert TOPIC in caplog.text and "q.x" in caplog.text
    assert "publish timed out" in caplog.text  # the cause survives into the log


def test_broker_close_is_safe_before_connect():
    Broker(broker_config({})).close()


def test_stop_unblocks_wait():
    broker = Broker(broker_config({}))
    threading.Timer(0.1, broker.stop).start()
    broker.wait()  # returns instead of hanging


def _itest_config():
    """The broker's built-in `default` user, allowed to create its own throwaway queue.

    The simulation's services never connect this way: their provisioned
    profiles may not create endpoints or add subscriptions.
    """
    return dataclasses.replace(broker_config(), create_queues=True)


@pytest.mark.skipif(not os.environ.get("SOLACE_INTEGRATION"), reason="needs a live broker")
def test_round_trip_through_a_dynamically_created_queue():
    """Publish persistently, consume from a queue created on start; the queue survives."""
    received = []
    got = threading.Event()
    cfg = _itest_config()
    queue = f"{cfg.queue_prefix}.itest"
    broker = Broker(cfg).connect()
    try:
        broker.subscribe(
            queue, ["cowork/itest/apse1/orchestration/task/*/v1"],
            lambda m: (received.append(m), got.set()),
        )
        broker.publish(Outbound(TOPIC, b'{"x":1}', {"eventType": "x"}))
        assert got.wait(10), f"no message received from {queue} within 10s"
    finally:
        broker.close()
    assert received[0] == Inbound(TOPIC, b'{"x":1}', {"eventType": "x"})


@pytest.mark.skipif(not os.environ.get("SOLACE_INTEGRATION"), reason="needs a live broker")
def test_queue_spools_while_consumer_is_down():
    """The point of queues: an event published before the consumer binds is still delivered."""
    cfg = _itest_config()
    queue = f"{cfg.queue_prefix}.itest-spool"
    sub = "cowork/itest/apse1/orchestration/>"
    setup = Broker(cfg).connect()  # first bind creates the queue and its subscription
    try:
        setup.subscribe(queue, [sub], lambda m: None)
    finally:
        setup.close()
    publisher = Broker(cfg).connect()
    try:
        publisher.publish(Outbound(TOPIC, b"spooled", {}))
    finally:
        publisher.close()
    received = []
    got = threading.Event()
    consumer = Broker(cfg).connect()
    try:
        consumer.subscribe(queue, [sub], lambda m: (received.append(m), got.set()))
        assert got.wait(10), "spooled message was not delivered on rebind"
    finally:
        consumer.close()
    assert received[0].payload == b"spooled"
