"""The agent runtime component: its declaration, its handlers, its entry point."""

from __future__ import annotations

from kai_sim_cowork.envelope import Producer
from kai_sim_cowork.services import agent_runtime
from kai_sim_cowork.services import events as ev
from kai_sim_cowork.services.agent_runtime import handlers, main

ME = Producer("agent-runtime", "0.1.0", "agent-1")
ORCHESTRATOR = Producer("orchestrator", "0.1.0", "orc-1")
TOOLING = Producer("tool-execution", "0.1.0", "tool-1")


def test_declaration_names_the_queue_subscriptions_and_publish_rights(space):
    svc = agent_runtime.SERVICE
    assert svc.name == agent_runtime.NAME == "agent-runtime"
    assert svc.subscriptions(space) == [
        "cowork/dev/apse1/agent/command/execute/v1",
        "cowork/dev/apse1/tooling/tool/completed/v1",
        "cowork/dev/apse1/tooling/tool/failed/v1",
    ]
    assert svc.publishes == (
        ev.AGENT_STARTED, ev.TOOL_EXECUTE, ev.AGENT_COMPLETED, ev.AGENT_FAILED
    )


def test_every_subscription_has_a_handler(space):
    subscribed = {space.parse(t).event_type for t in agent_runtime.SERVICE.subscriptions(space)}
    assert set(handlers.HANDLERS) == subscribed


def test_the_execute_command_starts_the_agent_and_asks_for_the_tool(task_created):
    command = task_created.derive(
        ev.AGENT_EXECUTE, ORCHESTRATOR, {"taskId": "t", "goal": "find it", "tool": "search"}
    )
    started, tool = handlers.on_agent_execute(command, ME)
    assert started.event_type == ev.AGENT_STARTED
    assert started.payload == {"taskId": "t"}
    assert tool.event_type == ev.TOOL_EXECUTE
    assert tool.payload == {"taskId": "t", "tool": "search", "args": {"query": "find it"}}
    for out in (started, tool):
        assert out.correlation_id == task_created.correlation_id  # still the same task
        assert out.causation_id == command.event_id
        assert out.producer.service == "agent-runtime"


def test_tool_completed_completes_the_agent_turn_with_the_result_reference(task_created):
    done = task_created.derive(ev.TOOL_COMPLETED, TOOLING, {"taskId": "t", "resultRef": "s3://r"})
    (out,) = handlers.on_tool_completed(done, ME)
    assert out.event_type == ev.AGENT_COMPLETED
    assert out.payload == {"taskId": "t", "resultRef": "s3://r"}


def test_tool_failed_fails_the_agent_turn_with_the_reason(task_created):
    failed = task_created.derive(ev.TOOL_FAILED, TOOLING, {"taskId": "t", "reason": "boom"})
    (out,) = handlers.on_tool_failed(failed, ME)
    assert out.event_type == ev.AGENT_FAILED
    assert out.payload == {"taskId": "t", "reason": "boom"}


def test_react_ignores_event_types_it_has_no_handler_for(task_created, run_reaction):
    # task.created is not for this service; its queue never attracts it either.
    assert run_reaction("agent-runtime", task_created) == []


def test_handlers_emit_only_declared_event_types(task_created):
    emitted = {
        e.event_type
        for handler in handlers.HANDLERS.values()
        for e in handler(task_created, ME)
    }
    assert emitted <= set(agent_runtime.PUBLISHES)


def test_entry_point_binds_the_agent_runtime_queue_as_its_own_username(fake_broker, space):
    assert main.main(["--instance", "i-1"]) == 0
    assert fake_broker == [
        ("connect-as", "cowork-agent-runtime"),
        ("subscribe", "q.test.dev.apse1.agent-runtime", agent_runtime.SERVICE.subscriptions(space)),
        ("wait",),
        ("close",),
    ]
