"""Streaming gateway business logic: what it does with every message it receives.

Where it sits in the flow
-------------------------
    consumer loop (validated envelope) --> [handlers] --> nothing to publish

A sink has one handler for everything its queue attracts, and it returns no
envelopes. A real gateway would look up the user's session from the
envelope's tenantId, principalId and workspaceId, check that the session may
see this task, and push the update over WebSocket or Server-Sent Events. The
shared consumer loop logs the receipt ("pushed the update to the user's
session") and acknowledges the message.
"""

from __future__ import annotations

from kai_sim_cowork.envelope import Envelope, Producer
from kai_sim_cowork.topics import TopicParts


def push_to_session(inbound: Envelope, me: Producer) -> list[Envelope]:
    """Forward the update to the client. Publishes nothing back to the broker."""
    return []


def react(inbound: Envelope, parts: TopicParts, me: Producer) -> list[Envelope]:
    """Every event type goes to the one handler: the queue's subscriptions already chose."""
    return push_to_session(inbound, me)
