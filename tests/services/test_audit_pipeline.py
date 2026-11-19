"""The audit pipeline component: a sink. Its declaration, its handler, its entry point."""

from __future__ import annotations

from kai_sim_cowork.envelope import Producer
from kai_sim_cowork.services import audit_pipeline
from kai_sim_cowork.services import events as ev
from kai_sim_cowork.services.audit_pipeline import handlers, main

ME = Producer("audit-pipeline", "0.1.0", "audit-1")


def test_declaration_takes_everything_in_the_space_and_publishes_nothing(space):
    svc = audit_pipeline.SERVICE
    assert svc.name == audit_pipeline.NAME == "audit-pipeline"
    assert svc.subscriptions(space) == ["cowork/dev/apse1/>"]
    assert svc.publishes == ()


def test_the_handler_records_and_publishes_nothing(task_created):
    assert handlers.record(task_created, ME) == []


def test_react_treats_events_and_commands_the_same(task_created, run_reaction):
    assert run_reaction("audit-pipeline", task_created) == []
    command = task_created.derive(ev.AGENT_EXECUTE, ME, {"taskId": "t"})
    assert run_reaction("audit-pipeline", command) == []


def test_entry_point_binds_the_audit_queue_as_its_own_username(fake_broker, space):
    assert main.main(["--instance", "i-1"]) == 0
    assert fake_broker == [
        ("connect-as", "cowork-audit-pipeline"),
        ("subscribe", "q.test.dev.apse1.audit-pipeline", ["cowork/dev/apse1/>"]),
        ("wait",),
        ("close",),
    ]
