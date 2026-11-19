"""Audit pipeline: keeps an immutable record of every event and command. A sink.

Where it sits in the flow
-------------------------
    every message in the environment and region --> [audit-pipeline] --> (audit store)

The component, file by file
---------------------------
    __init__.py   this declaration: what the service is called and what its
                  queue subscribes to. It publishes nothing.
    handlers.py   the business logic: one function for every message it receives
    main.py       the process entry point, `kai-sim-audit-pipeline`

On the broker this declaration becomes
    queue            q.cowork.{env}.{region}.audit-pipeline   (+ .dmq)
    client username  cowork-audit-pipeline, owner of that queue
    ACL profile      cowork-audit-pipeline: may publish nothing
    client profile   cowork-consume (not even allowed to send guaranteed messages)
"""

from __future__ import annotations

from kai_sim_cowork.services.audit_pipeline import handlers
from kai_sim_cowork.services.base import Service
from kai_sim_cowork.topics import TopicSpace

NAME = "audit-pipeline"

# A sink: it consumes and never publishes back into the flow.
PUBLISHES: tuple[str, ...] = ()


def subscriptions(space: TopicSpace) -> list[str]:
    """Everything: cowork/{env}/{region}/>.

    The one justified broad subscription (brief, section 14). It is a
    property of this queue, which only cowork-audit-pipeline can bind; no
    client has the right to subscribe to `>` itself.
    """
    return [space.all_subscription()]


SERVICE = Service(
    name=NAME,
    receipt="stored an immutable audit record",
    subscriptions=subscriptions,
    react=handlers.react,
    publishes=PUBLISHES,
)
