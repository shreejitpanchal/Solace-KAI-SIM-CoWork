"""The registry's subscriptions and reactions, and the full task flow they produce together."""

from __future__ import annotations

import pytest

from kai_sim_cowork import services as s
from kai_sim_cowork.envelope import Envelope, Producer
from kai_sim_cowork.topics import TopicSpace


def me(name: str) -> Producer:
    return Producer(name, "0.1.0", f"{name}-1")


def react(service_name: str, env: Envelope, space: TopicSpace) -> list[Envelope]:
    """Run one service's reaction the way the consumer would, after parsing the topic."""
    parts = space.parse(space.topic_for(env.event_type))
    return s.SERVICES[service_name].react(env, parts, me(service_name))


def test_registry_names_the_five_services_and_their_subscriptions(space):
    assert s.SERVICE_NAMES == (
        "orchestrator", "agent-runtime", "tool-execution", "streaming-gateway", "audit-pipeline"
    )
    subs = {name: svc.subscriptions(space) for name, svc in s.SERVICES.items()}
    assert subs["orchestrator"] == [
        "cowork/dev/apse1/orchestration/task/created/v1",
        "cowork/dev/apse1/agent/agent/completed/v1",
        "cowork/dev/apse1/agent/agent/failed/v1",
    ]
    assert subs["agent-runtime"] == [
        "cowork/dev/apse1/agent/command/execute/v1",
        "cowork/dev/apse1/tooling/tool/completed/v1",
        "cowork/dev/apse1/tooling/tool/failed/v1",
    ]
    assert subs["tool-execution"] == ["cowork/dev/apse1/tooling/command/execute/v1"]
    assert subs["streaming-gateway"] == [
        "cowork/dev/apse1/orchestration/task/*/v1",
        "cowork/dev/apse1/tooling/tool/progressed/v1",
    ]
    assert subs["audit-pipeline"] == ["cowork/dev/apse1/>"]


def test_queue_specs_give_each_service_its_own_queue(space):
    specs = s.queue_specs(space, "q.cowork")
    assert [q.name for q in specs] == [
        "q.cowork.dev.apse1.orchestrator",
        "q.cowork.dev.apse1.agent-runtime",
        "q.cowork.dev.apse1.tool-execution",
        "q.cowork.dev.apse1.streaming-gateway",
        "q.cowork.dev.apse1.audit-pipeline",
    ]
    assert specs[4].subscriptions == ("cowork/dev/apse1/>",)


def test_orchestrator_starts_an_agent_for_a_new_task(space, task_created):
    out = react("orchestrator", task_created, space)
    assert [e.event_type for e in out] == [s.AGENT_EXECUTE, s.TASK_STATE_CHANGED]
    command, state = out
    assert command.payload == {
        "taskId": "task-0001",
        "goal": "Summarise document 1",
        "tool": "search",
        "deadlineSeconds": 60,
    }
    assert state.payload == {"taskId": "task-0001", "state": "running"}
    for e in out:
        assert e.correlation_id == task_created.correlation_id
        assert e.causation_id == task_created.event_id
        assert e.producer.service == "orchestrator"


def test_tool_execution_completes_a_normal_tool_and_fails_the_flaky_one(space, task_created):
    agent = me("agent-runtime")
    command = task_created.derive(s.TOOL_EXECUTE, agent, {"taskId": "t", "tool": "search"})
    out = react("tool-execution", command, space)
    assert [e.event_type for e in out] == [s.TOOL_ACCEPTED, s.TOOL_PROGRESSED, s.TOOL_COMPLETED]
    assert out[2].payload["resultRef"] == "s3://cowork-results/acme/t.json"

    flaky = task_created.derive(s.TOOL_EXECUTE, agent, {"taskId": "t", "tool": s.FLAKY_TOOL})
    out = react("tool-execution", flaky, space)
    assert [e.event_type for e in out] == [s.TOOL_ACCEPTED, s.TOOL_PROGRESSED, s.TOOL_FAILED]
    assert "simulated" in out[2].payload["reason"]


@pytest.mark.parametrize("name", ["streaming-gateway", "audit-pipeline"])
def test_sinks_never_publish(space, task_created, name):
    assert react(name, task_created, space) == []


@pytest.mark.parametrize("name", ["orchestrator", "agent-runtime", "tool-execution"])
def test_unexpected_event_types_produce_no_reaction(space, task_created, name):
    stray = task_created.derive("cowork.platform.config.changed", me("platform"), {})
    assert react(name, stray, space) == []


def _run_flow(space: TopicSpace, root: Envelope) -> list[Envelope]:
    """Simulate the broker: route every published envelope to the services whose
    subscriptions match its topic, collect everything published, in order."""
    import re

    def matches(sub: str, topic: str) -> bool:
        if sub.endswith("/>"):
            return topic.startswith(sub[:-1])
        # Solace `*` matches exactly one level: anything but a slash.
        return re.fullmatch(re.escape(sub).replace(r"\*", "[^/]+"), topic) is not None

    published: list[Envelope] = []
    queue: list[Envelope] = [root]
    while queue:
        env = queue.pop(0)
        topic = space.topic_for(env.event_type)
        parts = space.parse(topic)
        for name, svc in s.SERVICES.items():
            if any(matches(sub, topic) for sub in svc.subscriptions(space)):
                for out in svc.react(env, parts, me(name)):
                    published.append(out)
                    queue.append(out)
    return published


def test_full_flow_completes_a_task_with_one_correlation_chain(space, task_created):
    published = _run_flow(space, task_created)
    assert [e.event_type for e in published] == [
        s.AGENT_EXECUTE,
        s.TASK_STATE_CHANGED,
        s.AGENT_STARTED,
        s.TOOL_EXECUTE,
        s.TOOL_ACCEPTED,
        s.TOOL_PROGRESSED,
        s.TOOL_COMPLETED,
        s.AGENT_COMPLETED,
        s.TASK_COMPLETED,
    ]
    assert {e.correlation_id for e in published} == {task_created.correlation_id}
    assert {e.traceparent for e in published} == {task_created.traceparent}
    ids = {task_created.event_id} | {e.event_id for e in published}
    assert all(e.causation_id in ids for e in published)  # every reaction points at its cause
    assert published[-1].payload == {
        "taskId": "task-0001", "resultRef": "s3://cowork-results/acme/task-0001.json"
    }


def test_full_flow_fails_a_task_when_the_tool_is_flaky(space, gateway):
    root = Envelope.new(
        s.TASK_CREATED, gateway, "acme", {"taskId": "task-0005", "goal": "g", "tool": s.FLAKY_TOOL}
    )
    published = _run_flow(space, root)
    assert published[-3:] and [e.event_type for e in published[-3:]] == [
        s.TOOL_FAILED, s.AGENT_FAILED, s.TASK_FAILED
    ]
    assert published[-1].payload["taskId"] == "task-0005"
    assert "simulated" in published[-1].payload["reason"]
