"""Tool execution service: runs an approved tool in an isolated worker.

Where it sits in the flow
-------------------------
    agent-runtime --tooling/command/execute--> [tool-execution]
    [tool-execution] --tool.accepted, tool.progressed, tool.completed | tool.failed-->

The component, file by file
---------------------------
    __init__.py   this declaration: what the service is called, what its queue
                  subscribes to and what it is allowed to publish
    handlers.py   the business logic, one pure function per inbound event type
    main.py       the process entry point, `kai-sim-tool-execution`

On the broker this declaration becomes
    queue            q.cowork.{env}.{region}.tool-execution   (+ .dmq)
    client username  cowork-tool-execution, owner of that queue
    ACL profile      cowork-tool-execution: may publish the PUBLISHES topics, nothing else
    client profile   cowork-publish-consume
"""

from __future__ import annotations

from kai_sim_cowork.services.base import Service
from kai_sim_cowork.services.events import (
    TOOL_ACCEPTED,
    TOOL_COMPLETED,
    TOOL_FAILED,
    TOOL_PROGRESSED,
)
from kai_sim_cowork.services.tool_execution import handlers
from kai_sim_cowork.topics import TopicSpace

NAME = "tool-execution"

# Everything the handlers may emit. The broker refuses any other topic.
PUBLISHES = (TOOL_ACCEPTED, TOOL_PROGRESSED, TOOL_COMPLETED, TOOL_FAILED)


def subscriptions(space: TopicSpace) -> list[str]:
    """Its own command only: a command has exactly one owning capability."""
    return [space.command_subscription("tooling", "execute")]


SERVICE = Service(
    name=NAME,
    receipt="ran the tool in an isolated worker",
    subscriptions=subscriptions,
    react=handlers.react,
    publishes=PUBLISHES,
)
