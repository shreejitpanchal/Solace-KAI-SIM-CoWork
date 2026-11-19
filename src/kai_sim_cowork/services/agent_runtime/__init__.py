"""Agent runtime: runs one bounded agent turn for a task.

Where it sits in the flow
-------------------------
    orchestrator --agent/command/execute--> [agent-runtime] --tooling/command/execute-->
    tool-execution --tool.completed | failed--> [agent-runtime] --agent.completed | failed-->

The component, file by file
---------------------------
    __init__.py   this declaration: what the service is called, what its queue
                  subscribes to and what it is allowed to publish
    handlers.py   the business logic, one pure function per inbound event type
    main.py       the process entry point, `kai-sim-agent-runtime`

On the broker this declaration becomes
    queue            q.cowork.{env}.{region}.agent-runtime   (+ .dmq)
    client username  cowork-agent-runtime, owner of that queue
    ACL profile      cowork-agent-runtime: may publish the PUBLISHES topics, nothing else
    client profile   cowork-publish-consume
"""

from __future__ import annotations

from kai_sim_cowork.services.agent_runtime import handlers
from kai_sim_cowork.services.base import Service
from kai_sim_cowork.services.events import (
    AGENT_COMPLETED,
    AGENT_FAILED,
    AGENT_STARTED,
    TOOL_EXECUTE,
)
from kai_sim_cowork.topics import TopicSpace

NAME = "agent-runtime"

# Everything the handlers may emit. The broker refuses any other topic.
PUBLISHES = (AGENT_STARTED, TOOL_EXECUTE, AGENT_COMPLETED, AGENT_FAILED)


def subscriptions(space: TopicSpace) -> list[str]:
    """Its own command, and the tool outcomes it waits for."""
    return [
        space.command_subscription("agent", "execute"),
        space.event_subscription("tooling", "tool", "completed"),
        space.event_subscription("tooling", "tool", "failed"),
    ]


SERVICE = Service(
    name=NAME,
    receipt="ran the agent turn",
    subscriptions=subscriptions,
    react=handlers.react,
    publishes=PUBLISHES,
)
