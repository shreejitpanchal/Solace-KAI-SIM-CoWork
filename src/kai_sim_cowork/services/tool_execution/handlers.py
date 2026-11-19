"""Tool execution business logic: what to publish for the command it receives.

Where it sits in the flow
-------------------------
    consumer loop (validated envelope) --> [handlers] --> envelopes to publish --> consumer loop

Every handler is a pure function: one inbound envelope in, a list of
envelopes out. The shared consumer loop (consumer.py) connects, publishes,
acknowledges and logs. A real tool worker would run the tool here; the
simulation reports one progress tick and then an outcome.

Two rules from the brief show up in this file:
* a tool's result is never put in the message. It would live in object
  storage, and only a reference (`resultRef`) travels;
* every command ends in exactly one terminal outcome, completed or failed.
"""

from __future__ import annotations

from kai_sim_cowork.envelope import Envelope, Producer
from kai_sim_cowork.services.base import Handler, dispatch
from kai_sim_cowork.services.events import (
    FLAKY_TOOL,
    TOOL_ACCEPTED,
    TOOL_COMPLETED,
    TOOL_EXECUTE,
    TOOL_FAILED,
    TOOL_PROGRESSED,
)


def on_tool_execute(inbound: Envelope, me: Producer) -> list[Envelope]:
    """Run the tool: accepted, one progress tick, then completed (or failed if it is flaky)."""
    task_id = inbound.payload.get("taskId")
    tool = inbound.payload.get("tool")
    out = [
        inbound.derive(TOOL_ACCEPTED, me, {"taskId": task_id, "tool": tool}),
        inbound.derive(TOOL_PROGRESSED, me, {"taskId": task_id, "tool": tool, "percent": 50}),
    ]
    if tool == FLAKY_TOOL:
        out.append(
            inbound.derive(
                TOOL_FAILED,
                me,
                {"taskId": task_id, "tool": tool, "reason": "tool returned an error (simulated)"},
            )
        )
    else:
        out.append(
            inbound.derive(
                TOOL_COMPLETED,
                me,
                {
                    "taskId": task_id,
                    "tool": tool,
                    # The result itself would live in object storage; only a reference travels.
                    "resultRef": f"s3://cowork-results/{inbound.tenant_id}/{task_id}.json",
                },
            )
        )
    return out


# The routing table of this service: inbound event type -> handler. It mirrors
# the queue subscriptions in __init__.py; anything else is ignored.
HANDLERS: dict[str, Handler] = {TOOL_EXECUTE: on_tool_execute}

react = dispatch(HANDLERS)
