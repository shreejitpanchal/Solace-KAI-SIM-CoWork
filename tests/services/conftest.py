"""Shared by the per-service tests: a stand-in for the broker behind consumer.run()."""

from __future__ import annotations

import pytest

from kai_sim_cowork.envelope import Envelope, Producer
from kai_sim_cowork.services import SERVICE_NAMES, SERVICES
from kai_sim_cowork.topics import TopicSpace


@pytest.fixture
def run_reaction(space: TopicSpace):
    """Run a service's reaction the way the consumer loop would, after parsing the topic."""

    def run(service_name: str, env: Envelope) -> list[Envelope]:
        parts = space.parse(space.topic_for(env.event_type))
        me = Producer(service_name, "0.1.0", f"{service_name}-1")
        return SERVICES[service_name].react(env, parts, me)

    return run


@pytest.fixture
def fake_broker(monkeypatch):
    """Record what a service's entry point does with the broker; nothing connects."""
    calls = []

    class FakeBroker:
        def __init__(self, cfg):
            calls.append(("connect-as", cfg.username))

        def connect(self):
            return self

        def publish(self, msg):  # pragma: no cover - no message is delivered in these tests
            calls.append(("publish", msg.topic))

        def subscribe(self, queue, subs, handler):
            calls.append(("subscribe", queue, subs))

        def wait(self):
            calls.append(("wait",))

        def close(self):
            calls.append(("close",))

    monkeypatch.setattr("kai_sim_cowork.consumer.Broker", FakeBroker)
    monkeypatch.setenv("SOLACE_QUEUE_PREFIX", "q.test")
    monkeypatch.setenv("COWORK_ENVIRONMENT", "dev")
    monkeypatch.setenv("COWORK_REGION", "apse1")
    monkeypatch.delenv("SOLACE_PASSWORD", raising=False)
    for name in SERVICE_NAMES:
        monkeypatch.setenv("SOLACE_PASSWORD_" + name.upper().replace("-", "_"), f"pw-{name}")
    return calls
