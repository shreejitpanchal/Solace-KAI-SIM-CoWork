"""Cowork topic convention, subscriptions, queue names, and the one gate for topic levels.

Topic convention (from the Cowork-Solace architecture brief, section 4)
-----------------------------------------------------------------------
    events     cowork/{environment}/{region}/{domain}/{entity}/{event}/{version}
               e.g. cowork/dev/apse1/orchestration/task/created/v1
    commands   cowork/{environment}/{region}/{capability}/command/{command-name}/{version}
               e.g. cowork/dev/apse1/agent/command/execute/v1

Both shapes have seven levels; a command is recognised by `command` in the
fifth level. The brief's dotted names (`agent.execute`, `tool.execute`) map
to capability `agent` / `tooling` plus the command name, so the capability
level is not repeated inside the name.

Why the topic carries this structure
------------------------------------
The topic is routing metadata. Every level is something a consumer may want
to filter on with a wildcard, so the broker can route one published event
into exactly the queues whose subscription matches:

    cowork/dev/apse1/orchestration/task/*/v1     every task lifecycle event
    cowork/dev/apse1/agent/command/execute/v1    one command, one owning capability
    cowork/dev/apse1/>                           everything (audit pipeline only)

Topics never carry secrets, free text or unbounded identifiers; those live in
the envelope payload (envelope.py). Every level is validated by
validate_level() before it is spliced into a topic or a queue name.

Queues
------
    <prefix>.<environment>.<region>.<service>    e.g. q.cowork.dev.apse1.orchestrator
One durable exclusive queue per consuming service (brief, section 7), carrying
that service's subscriptions. Each service package (services/<name>/) declares its own.

    <queue>.dmq                                  e.g. q.cowork.dev.apse1.orchestrator.dmq
One dead message queue per service queue. It has no subscriptions: the broker
moves a message there when it expires on the service queue or exceeds the
queue's redelivery limit (provision.py sets both), so nothing is lost silently.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

ROOT = "cowork"
VERSION = "v1"
COMMAND_LEVEL = "command"
DMQ_SUFFIX = "dmq"

# Letters, digits, '.', '_' and '-', 1 to 64 characters. Nothing else.
_LEVEL = re.compile(r"^[A-Za-z0-9._-]{1,64}$")


def validate_level(value: str, name: str) -> str:
    """Return `value` if it is a safe literal topic level, else raise ValueError naming it."""
    if not isinstance(value, str) or not _LEVEL.match(value):
        raise ValueError(
            f"{name} must be 1-64 characters of letters, digits, '.', '_' or '-'; got {value!r}"
        )
    return value


def _wild(value: str, name: str) -> str:
    """A level in a subscription: a literal level or the single-level wildcard `*`."""
    return value if value == "*" else validate_level(value, name)


@dataclass(frozen=True)
class TopicParts:
    """A parsed Cowork topic. For a command, `entity` is "command" and `name` the command."""

    environment: str
    region: str
    domain: str  # or capability, for a command
    entity: str
    name: str  # event name, or command name
    version: str

    @property
    def is_command(self) -> bool:
        return self.entity == COMMAND_LEVEL

    @property
    def event_type(self) -> str:
        """The envelope's eventType, cowork.{domain}.{entity}.{name}."""
        return f"{ROOT}.{self.domain}.{self.entity}.{self.name}"


@dataclass(frozen=True)
class TopicSpace:
    """The environment/region a deployment publishes into; builds its topics and subscriptions."""

    environment: str
    region: str

    def __post_init__(self) -> None:
        validate_level(self.environment, "environment")
        validate_level(self.region, "region")

    @property
    def prefix(self) -> str:
        return f"{ROOT}/{self.environment}/{self.region}"

    # --- topics (published to) ------------------------------------------

    def event_topic(self, domain: str, entity: str, event: str, version: str = VERSION) -> str:
        """cowork/{env}/{region}/{domain}/{entity}/{event}/{version}."""
        return "/".join(
            (
                self.prefix,
                validate_level(domain, "domain"),
                validate_level(entity, "entity"),
                validate_level(event, "event"),
                validate_level(version, "version"),
            )
        )

    def command_topic(self, capability: str, command: str, version: str = VERSION) -> str:
        """cowork/{env}/{region}/{capability}/command/{command}/{version}."""
        return "/".join(
            (
                self.prefix,
                validate_level(capability, "capability"),
                COMMAND_LEVEL,
                validate_level(command, "command"),
                validate_level(version, "version"),
            )
        )

    def topic_for(self, event_type: str, version: str = VERSION) -> str:
        """Topic for an envelope eventType such as cowork.orchestration.task.created."""
        parts = event_type.split(".")
        if len(parts) != 4 or parts[0] != ROOT:
            raise ValueError(
                f"eventType {event_type!r} must be {ROOT}.{{domain}}.{{entity}}.{{name}}"
            )
        _, domain, entity, name = parts
        if entity == COMMAND_LEVEL:
            return self.command_topic(domain, name, version)
        return self.event_topic(domain, entity, name, version)

    # --- subscriptions (attached to queues) --------------------------------

    def event_subscription(self, domain: str, entity: str = "*", event: str = "*") -> str:
        """Events of a domain; `*` for any entity / any event, e.g. orchestration/task/*/v1."""
        return "/".join(
            (
                self.prefix,
                validate_level(domain, "domain"),
                _wild(entity, "entity"),
                _wild(event, "event"),
                VERSION,
            )
        )

    def command_subscription(self, capability: str, command: str = "*") -> str:
        """Commands owned by a capability, e.g. agent/command/execute/v1 or agent/command/*/v1."""
        return "/".join(
            (
                self.prefix,
                validate_level(capability, "capability"),
                COMMAND_LEVEL,
                _wild(command, "command"),
                VERSION,
            )
        )

    def all_subscription(self) -> str:
        """Every topic in this environment/region: cowork/{env}/{region}/>. Audit only."""
        return f"{self.prefix}/>"

    # --- queues ----------------------------------------------------------

    def queue_name(self, prefix: str, service: str) -> str:
        """<prefix>.<env>.<region>.<service>, e.g. q.cowork.dev.apse1.orchestrator."""
        return f"{prefix}.{self.environment}.{self.region}.{validate_level(service, 'service')}"

    def dmq_name(self, prefix: str, service: str) -> str:
        """The service queue's dead message queue: <queue>.dmq."""
        return f"{self.queue_name(prefix, service)}.{DMQ_SUFFIX}"

    # --- parsing inbound topics ------------------------------------------

    def parse(self, topic: str) -> TopicParts:
        """Split an inbound topic; ValueError if it is not a Cowork topic of this space."""
        levels = topic.split("/")
        if len(levels) != 7:
            raise ValueError(
                f"topic {topic!r} must have 7 levels: "
                f"{ROOT}/{{environment}}/{{region}}/{{domain}}/{{entity}}/{{event}}/{{version}}"
            )
        root, environment, region, domain, entity, name, version = levels
        if root != ROOT or environment != self.environment or region != self.region:
            raise ValueError(f"topic {topic!r} is outside {self.prefix}/")
        return TopicParts(
            environment,
            region,
            validate_level(domain, "domain"),
            validate_level(entity, "entity"),
            validate_level(name, "name"),
            validate_level(version, "version"),
        )
