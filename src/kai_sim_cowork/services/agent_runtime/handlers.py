"""Agent runtime business logic: what to publish for each message it receives.

Where it sits in the flow
-------------------------
    consumer loop (validated envelope) --> [handlers] --> envelopes to publish --> consumer loop

Every handler is a pure function: one inbound envelope in, a list of
envelopes out. The shared consumer loop (consumer.py) connects, publishes,
acknowledges and logs. A real agent runtime would call its model here and
decide which tool to ask for; the simulation asks for the tool named in the
command.

This service receives a command (agent/command/execute) as well as events.
A command is handled exactly like an event; the difference is on the broker,
where only this service's queue subscribes to it.
"""

from __future__ import annotations

from kai_sim_cowork.envelope import Envelope, Producer
from kai_sim_cowork.services.base import Handler, dispatch
from kai_sim_cowork.services.events import (
    AGENT_COMPLETED,
    AGENT_EXECUTE,
    AGENT_FAILED,
    AGENT_STARTED,
    TOOL_COMPLETED,
    TOOL_EXECUTE,
    TOOL_FAILED,
)


def on_agent_execute(inbound: Envelope, me: Producer) -> list[Envelope]:
    """The command to run a task: announce the start, then ask tooling for the tool."""
    task_id = inbound.payload.get("taskId")
    return [
        inbound.derive(AGENT_STARTED, me, {"taskId": task_id}),
        inbound.derive(
            TOOL_EXECUTE,
            me,
            {
                "taskId": task_id,
                "tool": inbound.payload.get("tool"),
                "args": {"query": inbound.payload.get("goal")},
            },
        ),
    ]


def on_tool_completed(inbound: Envelope, me: Producer) -> list[Envelope]:
    """The tool returned: the agent turn is complete; pass on the result reference."""
    return [
        inbound.derive(
            AGENT_COMPLETED,
            me,
            {
                "taskId": inbound.payload.get("taskId"),
                "resultRef": inbound.payload.get("resultRef"),
            },
        )
    ]


def on_tool_failed(inbound: Envelope, me: Producer) -> list[Envelope]:
    """The tool failed: the agent turn fails with the tool's reason."""
    return [
        inbound.derive(
            AGENT_FAILED,
            me,
            {"taskId": inbound.payload.get("taskId"), "reason": inbound.payload.get("reason")},
        )
    ]


# The routing table of this service: inbound event type -> handler. It mirrors
# the queue subscriptions in __init__.py; anything else is ignored.
HANDLERS: dict[str, Handler] = {
    AGENT_EXECUTE: on_agent_execute,
    TOOL_COMPLETED: on_tool_completed,
    TOOL_FAILED: on_tool_failed,
}

react = dispatch(HANDLERS)
