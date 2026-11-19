import json
import logging

import pytest

from kai_sim_cowork.publisher import FLAKY_EVERY, build_task, main
from kai_sim_cowork.services import FLAKY_TOOL, TASK_CREATED


def test_build_task_is_a_root_envelope_with_the_task_payload(gateway):
    env = build_task(3, "acme", gateway, "user-1", "ws-1")
    assert env.event_type == TASK_CREATED
    assert env.payload == {
        "taskId": "task-0003",
        "goal": "Summarise document 3 for the weekly digest",
        "tool": "search",
    }
    assert env.correlation_id == env.event_id
    assert env.idempotency_key == "task-0003"
    assert env.tenant_id == "acme"


def test_every_fifth_task_uses_the_flaky_tool(gateway):
    tools = [build_task(n, "acme", gateway, "u", "w").payload["tool"] for n in range(1, 11)]
    assert tools.count(FLAKY_TOOL) == 2
    assert tools[FLAKY_EVERY - 1] == FLAKY_TOOL


@pytest.fixture
def fake_broker(monkeypatch):
    published = []

    class FakeBroker:
        def __init__(self, cfg):
            pass

        def connect(self):
            return self

        def publish(self, msg):
            published.append(msg)

        def close(self):
            published.append("closed")

    monkeypatch.setattr("kai_sim_cowork.publisher.Broker", FakeBroker)
    monkeypatch.setenv("COWORK_ENVIRONMENT", "dev")
    monkeypatch.setenv("COWORK_REGION", "apse1")
    monkeypatch.setenv("COWORK_TENANT_ID", "acme")
    return published


def test_main_publishes_count_tasks_to_the_task_created_topic(fake_broker, caplog):
    with caplog.at_level(logging.INFO):
        code = main(["--count", "2", "--interval", "0", "--instance", "gw-1"])
    assert code == 0
    msgs = fake_broker[:2]
    assert [m.topic for m in msgs] == ["cowork/dev/apse1/orchestration/task/created/v1"] * 2
    bodies = [json.loads(m.payload) for m in msgs]
    assert [b["payload"]["taskId"] for b in bodies] == ["task-0001", "task-0002"]
    assert bodies[0]["producer"] == {
        "service": "api-gateway", "version": "0.1.0", "instance": "gw-1"
    }
    assert msgs[0].properties["eventType"] == TASK_CREATED
    assert fake_broker[-1] == "closed"
    assert "published task task-0001" in caplog.text


def test_env_count_and_interval_are_used_when_flags_absent(monkeypatch, fake_broker):
    monkeypatch.setenv("PUBLISH_COUNT", "3")
    monkeypatch.setenv("PUBLISH_INTERVAL_SECONDS", "0")
    assert main([]) == 0
    assert len([m for m in fake_broker if m != "closed"]) == 3


def test_main_waits_interval_between_tasks(monkeypatch, fake_broker):
    waits = []
    monkeypatch.setattr("threading.Event.wait", lambda self, t=None: waits.append(t) or False)
    main(["--count", "3", "--interval", "5"])
    assert waits == [5.0, 5.0]  # no pause after the last task


def test_unlimited_run_stops_cleanly_on_ctrl_c(monkeypatch, fake_broker):
    def interrupt(self, t=None):
        raise KeyboardInterrupt

    monkeypatch.setattr("threading.Event.wait", interrupt)
    assert main(["--interval", "1"]) == 0  # count 0 = unlimited
    assert fake_broker[-1] == "closed"


@pytest.mark.parametrize(
    "argv", [["--principal-id", "a/b"], ["--workspace-id", ">"], ["--count", "-1"]]
)
def test_bad_arguments_are_rejected_before_connecting(fake_broker, argv):
    with pytest.raises(SystemExit) as exc:
        main(argv)
    assert exc.value.code == 2
    assert fake_broker == []


def test_bad_config_returns_2(monkeypatch, fake_broker, caplog):
    monkeypatch.setenv("PUBLISH_INTERVAL_SECONDS", "soon")
    with caplog.at_level(logging.ERROR):
        assert main([]) == 2
    assert "PUBLISH_INTERVAL_SECONDS" in caplog.text
    assert fake_broker == []
