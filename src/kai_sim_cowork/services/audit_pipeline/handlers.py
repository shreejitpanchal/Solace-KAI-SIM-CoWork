"""Audit pipeline business logic: what it does with every message it receives.

Where it sits in the flow
-------------------------
    consumer loop (validated envelope) --> [handlers] --> nothing to publish

A sink has one handler for everything its queue attracts, and it returns no
envelopes. A real pipeline would append the envelope to an immutable store,
keyed by eventId so a redelivered message is not recorded twice. The shared
consumer loop logs the receipt ("stored an immutable audit record") and
acknowledges the message only after this handler has returned.
"""

from __future__ import annotations

from kai_sim_cowork.envelope import Envelope, Producer
from kai_sim_cowork.topics import TopicParts


def record(inbound: Envelope, me: Producer) -> list[Envelope]:
    """Store the audit record. Publishes nothing back to the broker."""
    return []


def react(inbound: Envelope, parts: TopicParts, me: Producer) -> list[Envelope]:
    """Every event type goes to the one handler: this service records all of them."""
    return record(inbound, me)
