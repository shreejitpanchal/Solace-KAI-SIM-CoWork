"""The tool execution component: its declaration, its handler, its entry point."""

from __future__ import annotations

from kai_sim_cowork.envelope import Producer
from kai_sim_cowork.services import events as ev
from kai_sim_cowork.services import tool_execution
from kai_sim_cowork.services.tool_execution import handlers, main

ME = Producer("tool-execution", "0.1.0", "tool-1")
AGENT = Producer("agent-runtime", "0.1.0", "agent-1")


def test_declaration_subscribes_to_its_own_command_only(space):
    svc = tool_execution.SERVICE
    assert svc.name == tool_execution.NAME == "tool-execution"
    assert svc.subscriptions(space) == ["cowork/dev/apse1/tooling/command/execute/v1"]
    assert svc.publishes == (
        ev.TOOL_ACCEPTED, ev.TOOL_PROGRESSED, ev.TOOL_COMPLETED, ev.TOOL_FAILED
    )


def test_every_subscription_has_a_handler(space):
    subscribed = {space.parse(t).event_type for t in tool_execution.SERVICE.subscriptions(space)}
    assert set(handlers.HANDLERS) == subscribed


def test_a_normal_tool_is_accepted_progresses_and_completes_with_a_reference(task_created):
    command = task_created.derive(ev.TOOL_EXECUTE, AGENT, {"taskId": "t", "tool": "search"})
    out = handlers.on_tool_execute(command, ME)
    assert [e.event_type for e in out] == [ev.TOOL_ACCEPTED, ev.TOOL_PROGRESSED, ev.TOOL_COMPLETED]
    assert out[1].payload == {"taskId": "t", "tool": "search", "percent": 50}
    # the result is a reference into object storage, scoped by tenant; never the content
    assert out[2].payload == {
        "taskId": "t", "tool": "search", "resultRef": "s3://cowork-results/acme/t.json"
    }
    assert all(e.causation_id == command.event_id for e in out)


def test_the_flaky_tool_ends_in_exactly_one_failed_outcome(task_created):
    flaky = task_created.derive(ev.TOOL_EXECUTE, AGENT, {"taskId": "t", "tool": ev.FLAKY_TOOL})
    out = handlers.on_tool_execute(flaky, ME)
    assert [e.event_type for e in out] == [ev.TOOL_ACCEPTED, ev.TOOL_PROGRESSED, ev.TOOL_FAILED]
    assert "simulated" in out[2].payload["reason"]


def test_react_ignores_event_types_it_has_no_handler_for(task_created, run_reaction):
    assert run_reaction("tool-execution", task_created) == []


def test_handler_emits_only_declared_event_types(task_created):
    for tool in ("search", ev.FLAKY_TOOL):
        command = task_created.derive(ev.TOOL_EXECUTE, AGENT, {"taskId": "t", "tool": tool})
        emitted = {e.event_type for e in handlers.on_tool_execute(command, ME)}
        assert emitted <= set(tool_execution.PUBLISHES)


def test_entry_point_binds_the_tool_execution_queue_as_its_own_username(fake_broker, space):
    assert main.main(["--instance", "i-1"]) == 0
    assert fake_broker == [
        ("connect-as", "cowork-tool-execution"),
        (
            "subscribe",
            "q.test.dev.apse1.tool-execution",
            tool_execution.SERVICE.subscriptions(space),
        ),
        ("wait",),
        ("close",),
    ]
