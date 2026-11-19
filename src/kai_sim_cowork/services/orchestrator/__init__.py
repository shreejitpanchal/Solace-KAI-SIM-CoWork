"""Orchestrator: owns the task's workflow state.

Where it sits in the flow
-------------------------
    api-gateway --task.created--> [orchestrator] --agent/command/execute--> agent-runtime
    agent-runtime --agent.completed | agent.failed--> [orchestrator] --task.completed | failed-->

The component, file by file
---------------------------
    __init__.py   this declaration: what the service is called, what its queue
                  subscribes to and what it is allowed to publish
    handlers.py   the business logic, one pure function per inbound event type
    main.py       the process entry point, `kai-sim-orchestrator`

On the broker this declaration becomes
    queue            q.cowork.{env}.{region}.orchestrator   (+ .dmq)
    client username  cowork-orchestrator, owner of that queue
    ACL profile      cowork-orchestrator: may publish the PUBLISHES topics, nothing else
    client profile   cowork-publish-consume
"""

from __future__ import annotations

from kai_sim_cowork.services.base import Service
from kai_sim_cowork.services.events import (
    AGENT_EXECUTE,
    TASK_COMPLETED,
    TASK_FAILED,
    TASK_STATE_CHANGED,
)
from kai_sim_cowork.services.orchestrator import handlers
from kai_sim_cowork.topics import TopicSpace

NAME = "orchestrator"

# Everything the handlers may emit. The broker refuses any other topic.
PUBLISHES = (AGENT_EXECUTE, TASK_STATE_CHANGED, TASK_COMPLETED, TASK_FAILED)


def subscriptions(space: TopicSpace) -> list[str]:
    """New tasks, and the agent outcomes that close them. Exact topics, no wildcards."""
    return [
        space.event_subscription("orchestration", "task", "created"),
        space.event_subscription("agent", "agent", "completed"),
        space.event_subscription("agent", "agent", "failed"),
    ]


SERVICE = Service(
    name=NAME,
    receipt="recorded workflow state",
    subscriptions=subscriptions,
    react=handlers.react,
    publishes=PUBLISHES,
)
