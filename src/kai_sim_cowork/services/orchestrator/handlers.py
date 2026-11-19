"""Orchestrator business logic: what to publish for each event it receives.

Where it sits in the flow
-------------------------
    consumer loop (validated envelope) --> [handlers] --> envelopes to publish --> consumer loop

Every handler is a pure function: one inbound envelope in, a list of
envelopes out. It does not connect, publish, acknowledge or log; the shared
consumer loop (consumer.py) does all of that. That is what makes the
business logic testable without a broker, and it is the part a real service
would replace with its own (here: persisting workflow state before returning).

Each outgoing envelope is made with `inbound.derive(...)`, never built from
scratch, so correlationId, traceparent, tenant, principal and workspace carry
over and causationId points at the message being handled.
"""

from __future__ import annotations

from kai_sim_cowork.envelope import Envelope, Producer
from kai_sim_cowork.services.base import Handler, dispatch
from kai_sim_cowork.services.events import (
    AGENT_COMPLETED,
    AGENT_EXECUTE,
    AGENT_FAILED,
    TASK_COMPLETED,
    TASK_CREATED,
    TASK_FAILED,
    TASK_STATE_CHANGED,
)

# How long the agent runtime is given for one task, carried in the command.
AGENT_DEADLINE_SECONDS = 60


def on_task_created(inbound: Envelope, me: Producer) -> list[Envelope]:
    """A new task: command the agent capability to run it, and announce it is running."""
    task_id = inbound.payload.get("taskId")
    return [
        inbound.derive(
            AGENT_EXECUTE,
            me,
            {
                "taskId": task_id,
                "goal": inbound.payload.get("goal"),
                "tool": inbound.payload.get("tool"),
                "deadlineSeconds": AGENT_DEADLINE_SECONDS,
            },
        ),
        inbound.derive(TASK_STATE_CHANGED, me, {"taskId": task_id, "state": "running"}),
    ]


def on_agent_completed(inbound: Envelope, me: Producer) -> list[Envelope]:
    """The agent finished: close the task, passing on the reference to the result."""
    return [
        inbound.derive(
            TASK_COMPLETED,
            me,
            {
                "taskId": inbound.payload.get("taskId"),
                "resultRef": inbound.payload.get("resultRef"),
            },
        )
    ]


def on_agent_failed(inbound: Envelope, me: Producer) -> list[Envelope]:
    """The agent failed: fail the task with the agent's reason."""
    return [
        inbound.derive(
            TASK_FAILED,
            me,
            {"taskId": inbound.payload.get("taskId"), "reason": inbound.payload.get("reason")},
        )
    ]


# The routing table of this service: inbound event type -> handler. It mirrors
# the queue subscriptions in __init__.py; anything else is ignored.
HANDLERS: dict[str, Handler] = {
    TASK_CREATED: on_task_created,
    AGENT_COMPLETED: on_agent_completed,
    AGENT_FAILED: on_agent_failed,
}

react = dispatch(HANDLERS)
