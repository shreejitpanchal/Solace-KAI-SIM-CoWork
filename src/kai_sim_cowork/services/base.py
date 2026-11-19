"""The shapes every service component is built from. No logic about any one service.

Where it sits in the flow
-------------------------
    base.py --> each service package (declares a Service) --> services/__init__.py (registry)

A service package fills in one `Service`; the registry turns the set of them
into what the provisioner creates (`QueueSpec`, `AccessSpec`,
`ClientProfileSpec`). Keeping the shapes here lets a service package depend
on this module alone, never on another service.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass

from kai_sim_cowork.envelope import Envelope, Producer
from kai_sim_cowork.topics import TopicParts, TopicSpace

# A reaction never touches the broker: it maps one validated inbound
# envelope to the envelopes that should be published in response, so it is
# unit-testable and the consumer loop stays generic.
Reaction = Callable[[Envelope, TopicParts, Producer], list[Envelope]]

# One handler per inbound event type: the inbound envelope and this service's
# producer identity in, the envelopes to publish out.
Handler = Callable[[Envelope, Producer], list[Envelope]]


def dispatch(handlers: Mapping[str, Handler]) -> Reaction:
    """Build a service's reaction from its handler table, keyed by inbound eventType.

    An event type with no handler produces no reaction: a queue subscription
    may be wider than what the service acts on (a wildcard), and ignoring is
    the safe default.
    """

    def react(inbound: Envelope, parts: TopicParts, me: Producer) -> list[Envelope]:
        handler = handlers.get(inbound.event_type)
        return handler(inbound, me) if handler else []

    return react


@dataclass(frozen=True)
class Service:
    """One consuming service, as its package declares it."""

    name: str  # also the last level of its queue name and of its client username
    receipt: str  # what it does with a message it receives (for the log)
    subscriptions: Callable[[TopicSpace], list[str]]  # what its queue attracts
    react: Reaction  # inbound envelope -> envelopes to publish
    publishes: tuple[str, ...] = ()  # event types the reaction may emit


@dataclass(frozen=True)
class QueueSpec:
    """One queue to exist on the broker: its subscriptions, DMQ and owning client username."""

    name: str
    subscriptions: tuple[str, ...]
    dead_msg_queue: str = ""  # where expired / undeliverable messages go; "" = none
    owner: str = ""  # the only client username that may bind; "" = no owner


@dataclass(frozen=True)
class ClientProfileSpec:
    """A client profile: what a class of client may do. Shared by every service in that role."""

    name: str
    guaranteed_send: bool  # may publish persistent messages
    guaranteed_receive: bool  # may bind to a queue


@dataclass(frozen=True)
class AccessSpec:
    """One principal's identity on the broker and the least it needs to do its job."""

    principal: str  # api-gateway or a service name
    username: str  # its client username
    acl_profile: str  # its own ACL profile
    client_profile: str  # the role profile it shares
    publish_topics: tuple[str, ...]  # the only topics it may publish to
