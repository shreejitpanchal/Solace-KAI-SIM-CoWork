"""dispatch(): how a service's handler table becomes its reaction."""

from __future__ import annotations

from kai_sim_cowork.envelope import Producer
from kai_sim_cowork.services.base import dispatch
from kai_sim_cowork.services.events import AGENT_STARTED, TASK_CREATED

ME = Producer("svc", "0.1.0", "svc-1")


def test_dispatch_calls_the_handler_registered_for_the_inbound_event_type(space, task_created):
    seen = []

    def on_task_created(inbound, me):
        seen.append((inbound.event_id, me.service))
        return [inbound.derive(AGENT_STARTED, me, {})]

    react = dispatch({TASK_CREATED: on_task_created})
    parts = space.parse(space.topic_for(TASK_CREATED))
    (out,) = react(task_created, parts, ME)
    assert out.event_type == AGENT_STARTED
    assert seen == [(task_created.event_id, "svc")]


def test_dispatch_returns_no_reaction_for_an_event_type_without_a_handler(space, task_created):
    react = dispatch({AGENT_STARTED: lambda inbound, me: [inbound]})
    parts = space.parse(space.topic_for(TASK_CREATED))
    assert react(task_created, parts, ME) == []
