"""Streaming gateway: forwards task progress to the user's session. A sink.

Where it sits in the flow
-------------------------
    every task lifecycle event, and tool progress --> [streaming-gateway] --> (WebSocket / SSE)

The component, file by file
---------------------------
    __init__.py   this declaration: what the service is called and what its
                  queue subscribes to. It publishes nothing.
    handlers.py   the business logic: one function for every message it receives
    main.py       the process entry point, `kai-sim-streaming-gateway`

On the broker this declaration becomes
    queue            q.cowork.{env}.{region}.streaming-gateway   (+ .dmq)
    client username  cowork-streaming-gateway, owner of that queue
    ACL profile      cowork-streaming-gateway: may publish nothing
    client profile   cowork-consume (not even allowed to send guaranteed messages)
"""

from __future__ import annotations

from kai_sim_cowork.services.base import Service
from kai_sim_cowork.services.streaming_gateway import handlers
from kai_sim_cowork.topics import TopicSpace

NAME = "streaming-gateway"

# A sink: it consumes and never publishes back into the flow.
PUBLISHES: tuple[str, ...] = ()


def subscriptions(space: TopicSpace) -> list[str]:
    """Every task lifecycle event (one wildcard on the event level) plus tool progress.

    The `*` means a task event added later reaches this service with no
    change here or on the queue.
    """
    return [
        space.event_subscription("orchestration", "task"),
        space.event_subscription("tooling", "tool", "progressed"),
    ]


SERVICE = Service(
    name=NAME,
    receipt="pushed the update to the user's session",
    subscriptions=subscriptions,
    react=handlers.react,
    publishes=PUBLISHES,
)
