"""The consumer validates at the trust boundary, reacts, and binds the right queue."""

import logging

import pytest

from kai_sim_cowork import consumer
from kai_sim_cowork.broker import Inbound
from kai_sim_cowork.envelope import Producer
from kai_sim_cowork.services import AGENT_EXECUTE, SERVICE_NAMES, SERVICES, TASK_STATE_CHANGED

ME = Producer("orchestrator", "0.1.0", "orc-1")


def inbound(space, env) -> Inbound:
    return Inbound(space.topic_for(env.event_type), env.to_json().encode(), env.properties())


def test_handler_publishes_the_reactions_to_their_topics(space, task_created, caplog):
    published = []
    handle = consumer.make_handler(SERVICES["orchestrator"], space, ME, published.append)
    with caplog.at_level(logging.INFO):
        handle(inbound(space, task_created))
    assert [m.topic for m in published] == [
        "cowork/dev/apse1/agent/command/execute/v1",
        "cowork/dev/apse1/orchestration/task/state-changed/v1",
    ]
    assert published[0].properties["eventType"] == AGENT_EXECUTE
    assert published[0].properties["correlationId"] == task_created.correlation_id
    assert b'"causationId":"' + task_created.event_id.encode() in published[0].payload.encode()
    assert "recorded workflow state" in caplog.text
    assert "2 reaction(s)" in caplog.text
    assert TASK_STATE_CHANGED in caplog.text


def test_sink_services_consume_without_publishing(space, task_created, caplog):
    published = []
    handle = consumer.make_handler(SERVICES["audit-pipeline"], space, ME, published.append)
    with caplog.at_level(logging.INFO):
        handle(inbound(space, task_created))
    assert published == []
    assert "stored an immutable audit record" in caplog.text


@pytest.mark.parametrize(
    "topic, payload, match",
    [
        ("other/topic", b"{}", "contract violation, dropped"),
        ("cowork/dev/apse1/orchestration/task/created/v1", b"not json", "not valid JSON"),
        ("cowork/dev/apse1/orchestration/task/created/v1", b"{}", "missing required fields"),
    ],
)
def test_contract_violations_are_dropped_and_logged(space, topic, payload, match, caplog):
    published = []
    handle = consumer.make_handler(SERVICES["orchestrator"], space, ME, published.append)
    with caplog.at_level(logging.WARNING):
        handle(Inbound(topic, payload, {}))
    assert published == []
    assert match in caplog.text


def test_envelope_whose_type_disagrees_with_the_topic_is_dropped(space, task_created, caplog):
    published = []
    handle = consumer.make_handler(SERVICES["orchestrator"], space, ME, published.append)
    wrong_topic = "cowork/dev/apse1/agent/agent/completed/v1"  # envelope says task.created
    with caplog.at_level(logging.WARNING):
        handle(Inbound(wrong_topic, task_created.to_json().encode(), {}))
    assert published == []
    assert "does not match the topic" in caplog.text


def test_a_failed_publish_propagates_so_the_message_is_redelivered(space, task_created):
    """The handler must not swallow it: broker.deliver turns the raise into a FAILED settlement."""

    def publish(msg):
        raise TimeoutError("broker did not acknowledge")

    handle = consumer.make_handler(SERVICES["orchestrator"], space, ME, publish)
    with pytest.raises(TimeoutError, match="did not acknowledge"):
        handle(inbound(space, task_created))


@pytest.fixture
def fake_broker(monkeypatch):
    calls = []

    class FakeBroker:
        def __init__(self, cfg):
            calls.append(("connect-as", cfg.username, cfg.password))

        def connect(self):
            return self

        def publish(self, msg):
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
        variable = "SOLACE_PASSWORD_" + name.upper().replace("-", "_")
        monkeypatch.setenv(variable, f"pw-{name}")
    return calls


@pytest.mark.parametrize("name", SERVICE_NAMES)
def test_each_service_binds_its_own_queue_as_its_own_client_username(fake_broker, space, name):
    assert consumer.main(["--service", name, "--instance", "i-1"]) == 0
    assert fake_broker == [
        ("connect-as", f"cowork-{name}", f"pw-{name}"),
        ("subscribe", f"q.test.dev.apse1.{name}", SERVICES[name].subscriptions(space)),
        ("wait",),
        ("close",),
    ]


def test_missing_password_returns_2_before_connecting(fake_broker, monkeypatch, caplog):
    monkeypatch.delenv("SOLACE_PASSWORD_ORCHESTRATOR")
    with caplog.at_level(logging.ERROR):
        assert consumer.main(["--service", "orchestrator"]) == 2
    assert "SOLACE_PASSWORD_ORCHESTRATOR" in caplog.text
    assert "cowork-orchestrator" in caplog.text
    assert fake_broker == []


@pytest.mark.parametrize(
    "argv",
    [["--service", "billing"], [], ["--service", "orchestrator", "--instance", "a/b"]],
)
def test_unknown_service_or_unsafe_instance_is_rejected_before_connecting(fake_broker, argv):
    with pytest.raises(SystemExit) as exc:
        consumer.main(argv)
    assert exc.value.code == 2
    assert fake_broker == []


def test_bad_cowork_config_returns_2_before_connecting(fake_broker, monkeypatch, caplog):
    monkeypatch.setenv("COWORK_REGION", "ap/se1")
    with caplog.at_level(logging.ERROR):
        assert consumer.main(["--service", "orchestrator"]) == 2
    assert "COWORK_REGION" in caplog.text
    assert fake_broker == []
