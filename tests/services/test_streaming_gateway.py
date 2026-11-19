"""The streaming gateway component: a sink. Its declaration, its handler, its entry point."""

from __future__ import annotations

from kai_sim_cowork.envelope import Producer
from kai_sim_cowork.services import events as ev
from kai_sim_cowork.services import streaming_gateway
from kai_sim_cowork.services.streaming_gateway import handlers, main

ME = Producer("streaming-gateway", "0.1.0", "stream-1")


def test_declaration_takes_every_task_event_and_tool_progress_and_publishes_nothing(space):
    svc = streaming_gateway.SERVICE
    assert svc.name == streaming_gateway.NAME == "streaming-gateway"
    assert svc.subscriptions(space) == [
        "cowork/dev/apse1/orchestration/task/*/v1",  # one wildcard: any task event
        "cowork/dev/apse1/tooling/tool/progressed/v1",
    ]
    assert svc.publishes == ()


def test_the_handler_forwards_and_publishes_nothing(task_created):
    assert handlers.push_to_session(task_created, ME) == []


def test_react_treats_every_event_type_the_same(task_created, run_reaction):
    assert run_reaction("streaming-gateway", task_created) == []
    progress = task_created.derive(ev.TOOL_PROGRESSED, ME, {"taskId": "t", "percent": 50})
    assert run_reaction("streaming-gateway", progress) == []


def test_entry_point_binds_the_streaming_gateway_queue_as_its_own_username(fake_broker, space):
    assert main.main(["--instance", "i-1"]) == 0
    assert fake_broker == [
        ("connect-as", "cowork-streaming-gateway"),
        (
            "subscribe",
            "q.test.dev.apse1.streaming-gateway",
            streaming_gateway.SERVICE.subscriptions(space),
        ),
        ("wait",),
        ("close",),
    ]
