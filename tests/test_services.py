"""The registry: the services it lists, what is derived from them, and the flow they produce.

Each service's own declaration and handlers are tested in tests/services/test_<service>.py.
"""

from __future__ import annotations

from kai_sim_cowork import services as s
from kai_sim_cowork.envelope import Envelope, Producer
from kai_sim_cowork.topics import TopicSpace


def me(name: str) -> Producer:
    return Producer(name, "0.1.0", f"{name}-1")


def test_each_service_is_declared_by_its_own_package():
    from kai_sim_cowork.services import (
        agent_runtime,
        api_gateway,
        audit_pipeline,
        orchestrator,
        streaming_gateway,
        tool_execution,
    )

    packages = (orchestrator, agent_runtime, tool_execution, streaming_gateway, audit_pipeline)
    assert [s.SERVICES[p.NAME] for p in packages] == [p.SERVICE for p in packages]
    assert s.GATEWAY == api_gateway.NAME and s.GATEWAY not in s.SERVICES
    assert s.PUBLISHES[s.GATEWAY] == api_gateway.PUBLISHES


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
    assert [q.dead_msg_queue for q in specs] == [f"{q.name}.dmq" for q in specs]
    assert [q.owner for q in specs] == [f"cowork-{name}" for name in s.SERVICE_NAMES]


def test_every_principal_gets_its_own_username_and_acl_profile(space):
    specs = {a.principal: a for a in s.access_specs(space)}
    assert tuple(specs) == s.PRINCIPALS == ("api-gateway", *s.SERVICE_NAMES)
    assert [a.username for a in specs.values()] == [f"cowork-{p}" for p in s.PRINCIPALS]
    assert all(a.acl_profile == a.username for a in specs.values())
    assert len({a.username for a in specs.values()}) == 6  # nothing shared, nothing `default`


def test_client_profile_is_chosen_by_role(space):
    profile = {a.principal: a.client_profile for a in s.access_specs(space)}
    assert profile["api-gateway"] == "cowork-publish"  # publishes, has no queue
    assert profile["orchestrator"] == "cowork-publish-consume"
    assert profile["agent-runtime"] == "cowork-publish-consume"
    assert profile["tool-execution"] == "cowork-publish-consume"
    assert profile["streaming-gateway"] == "cowork-consume"  # sinks never publish
    assert profile["audit-pipeline"] == "cowork-consume"
    rights = {p.name: (p.guaranteed_send, p.guaranteed_receive) for p in s.CLIENT_PROFILES}
    assert rights == {
        "cowork-publish": (True, False),
        "cowork-consume": (False, True),
        "cowork-publish-consume": (True, True),
    }


def test_publish_rights_are_exactly_the_declared_event_topics(space):
    topics = {a.principal: a.publish_topics for a in s.access_specs(space)}
    assert topics["api-gateway"] == ("cowork/dev/apse1/orchestration/task/created/v1",)
    assert topics["agent-runtime"] == (
        "cowork/dev/apse1/agent/agent/started/v1",
        "cowork/dev/apse1/tooling/command/execute/v1",
        "cowork/dev/apse1/agent/agent/completed/v1",
        "cowork/dev/apse1/agent/agent/failed/v1",
    )
    assert topics["streaming-gateway"] == () and topics["audit-pipeline"] == ()
    # no wildcard ever reaches a publish right
    assert not any("*" in t or ">" in t for ts in topics.values() for t in ts)


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
                    # The broker's ACL would refuse anything else: a reaction that
                    # emits an undeclared type must fail here, not in a running stack.
                    assert out.event_type in svc.publishes, (name, out.event_type)
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
