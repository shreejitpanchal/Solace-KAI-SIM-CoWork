"""The orchestrator component: its declaration, its handlers, its entry point."""

from __future__ import annotations

from kai_sim_cowork.envelope import Producer
from kai_sim_cowork.services import events as ev
from kai_sim_cowork.services import orchestrator
from kai_sim_cowork.services.orchestrator import handlers, main

ME = Producer("orchestrator", "0.1.0", "orc-1")
AGENT = Producer("agent-runtime", "0.1.0", "agent-1")


def test_declaration_names_the_queue_subscriptions_and_publish_rights(space):
    svc = orchestrator.SERVICE
    assert svc.name == orchestrator.NAME == "orchestrator"
    assert svc.subscriptions(space) == [
        "cowork/dev/apse1/orchestration/task/created/v1",
        "cowork/dev/apse1/agent/agent/completed/v1",
        "cowork/dev/apse1/agent/agent/failed/v1",
    ]
    assert svc.publishes == (
        ev.AGENT_EXECUTE, ev.TASK_STATE_CHANGED, ev.TASK_COMPLETED, ev.TASK_FAILED
    )


def test_every_subscription_has_a_handler(space):
    """The handler table mirrors the queue: nothing subscribed is silently ignored."""
    subscribed = {space.parse(t).event_type for t in orchestrator.SERVICE.subscriptions(space)}
    assert set(handlers.HANDLERS) == subscribed


def test_a_new_task_starts_an_agent_and_is_announced_as_running(task_created):
    command, state = handlers.on_task_created(task_created, ME)
    assert (command.event_type, state.event_type) == (ev.AGENT_EXECUTE, ev.TASK_STATE_CHANGED)
    assert command.payload == {
        "taskId": "task-0001",
        "goal": "Summarise document 1",
        "tool": "search",
        "deadlineSeconds": 60,
    }
    assert state.payload == {"taskId": "task-0001", "state": "running"}
    for out in (command, state):
        assert out.correlation_id == task_created.correlation_id
        assert out.traceparent == task_created.traceparent
        assert out.causation_id == task_created.event_id
        assert out.producer.service == "orchestrator"


def test_agent_completed_closes_the_task_with_the_result_reference(task_created):
    done = task_created.derive(ev.AGENT_COMPLETED, AGENT, {"taskId": "t", "resultRef": "s3://r"})
    (out,) = handlers.on_agent_completed(done, ME)
    assert out.event_type == ev.TASK_COMPLETED
    assert out.payload == {"taskId": "t", "resultRef": "s3://r"}
    assert out.causation_id == done.event_id


def test_agent_failed_fails_the_task_with_the_reason(task_created):
    failed = task_created.derive(ev.AGENT_FAILED, AGENT, {"taskId": "t", "reason": "boom"})
    (out,) = handlers.on_agent_failed(failed, ME)
    assert out.event_type == ev.TASK_FAILED
    assert out.payload == {"taskId": "t", "reason": "boom"}


def test_react_dispatches_by_event_type_and_ignores_the_rest(task_created, run_reaction):
    out = run_reaction("orchestrator", task_created)
    assert [e.event_type for e in out] == [ev.AGENT_EXECUTE, ev.TASK_STATE_CHANGED]
    stray = task_created.derive("cowork.platform.config.changed", AGENT, {})
    assert run_reaction("orchestrator", stray) == []


def test_handlers_emit_only_declared_event_types(task_created):
    emitted = {
        e.event_type
        for handler in handlers.HANDLERS.values()
        for e in handler(task_created, ME)
    }
    assert emitted <= set(orchestrator.PUBLISHES)


def test_entry_point_binds_the_orchestrator_queue_as_its_own_username(fake_broker, space):
    assert main.main(["--instance", "i-1"]) == 0
    assert fake_broker == [
        ("connect-as", "cowork-orchestrator"),
        ("subscribe", "q.test.dev.apse1.orchestrator", orchestrator.SERVICE.subscriptions(space)),
        ("wait",),
        ("close",),
    ]
