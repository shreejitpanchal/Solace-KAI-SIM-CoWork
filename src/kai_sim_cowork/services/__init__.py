"""The service registry: one line per service component, and what the broker needs for them.

Where it sits in the flow
-------------------------
    service packages --> [this registry] --> consumer.py   (which queue to bind, how to react)
                                         --> provision.py  (queues, DMQs, access control)

How the code is organised
-------------------------
Each service is its own package and owns everything about itself:

    services/
      base.py               the shapes every component is built from
      events.py             the event and command types: the shared contract
      access.py             how a principal becomes a broker identity
      api_gateway/          publishes task.created; has no queue
      orchestrator/         owns workflow state
      agent_runtime/        runs the agent turn
      tool_execution/       runs the tool
      streaming_gateway/    sink: pushes updates to the user
      audit_pipeline/       sink: records everything

    <service>/
      __init__.py           the declaration: name, queue subscriptions, publish rights
      handlers.py           the business logic: one pure function per inbound event type
      main.py               the process entry point (kai-sim-<service>)

A service package imports base.py, events.py, envelope.py and topics.py, and
nothing from another service. Services meet only on the broker.

The flow that emerges (brief, section 13), one task at a time:

    api-gateway ─task/created─► orchestrator ─agent/command/execute─► agent-runtime
                                    ▲                                  │
                                    │ agent/completed | failed         │ tooling/command/execute
                                    │                                  ▼
                                    └────── tool/completed | failed ── tool-execution
                                            (+ accepted, progressed)

    streaming-gateway  receives every task lifecycle event and tool progress
    audit-pipeline     receives a copy of everything (cowork/{env}/{region}/>)

Adding a service is one new package and one line in _MANIFEST below (plus
its compose service); its queue, dead message queue, client username and ACL
profile are derived from its declaration by queue_specs() and access_specs().

Access control is derived from the same declarations, so what a service is
allowed to do on the broker can never drift from what it does:

    client username   cowork-<service>, one per service plus cowork-api-gateway
    ACL profile       same name; publish and subscribe default to disallow, with a
                      publish exception for exactly the topics in `publishes`
    client profile    one of three, by role: cowork-publish, cowork-consume,
                      cowork-publish-consume; none may create endpoints
    queue             owned by the service's client username, no access for others
"""

from __future__ import annotations

from kai_sim_cowork.services import (
    agent_runtime,
    api_gateway,
    audit_pipeline,
    orchestrator,
    streaming_gateway,
    tool_execution,
)
from kai_sim_cowork.services.access import (
    ACCESS_PREFIX,
    CLIENT_PROFILES,
    MAX_PROFILE_NAME,
    PROFILE_CONSUME,
    PROFILE_PUBLISH,
    PROFILE_PUBLISH_CONSUME,
    client_username,
    profile_for,
)
from kai_sim_cowork.services.base import (
    AccessSpec,
    ClientProfileSpec,
    QueueSpec,
    Reaction,
    Service,
)
from kai_sim_cowork.services.events import (
    AGENT_COMPLETED,
    AGENT_EXECUTE,
    AGENT_FAILED,
    AGENT_STARTED,
    FLAKY_TOOL,
    TASK_COMPLETED,
    TASK_CREATED,
    TASK_FAILED,
    TASK_STATE_CHANGED,
    TOOL_ACCEPTED,
    TOOL_COMPLETED,
    TOOL_EXECUTE,
    TOOL_FAILED,
    TOOL_PROGRESSED,
)
from kai_sim_cowork.topics import TopicSpace

__all__ = [
    "ACCESS_PREFIX",
    "AGENT_COMPLETED",
    "AGENT_EXECUTE",
    "AGENT_FAILED",
    "AGENT_STARTED",
    "CLIENT_PROFILES",
    "FLAKY_TOOL",
    "GATEWAY",
    "MAX_PROFILE_NAME",
    "PRINCIPALS",
    "PROFILE_CONSUME",
    "PROFILE_PUBLISH",
    "PROFILE_PUBLISH_CONSUME",
    "PUBLISHES",
    "SERVICES",
    "SERVICE_NAMES",
    "TASK_COMPLETED",
    "TASK_CREATED",
    "TASK_FAILED",
    "TASK_STATE_CHANGED",
    "TOOL_ACCEPTED",
    "TOOL_COMPLETED",
    "TOOL_EXECUTE",
    "TOOL_FAILED",
    "TOOL_PROGRESSED",
    "AccessSpec",
    "ClientProfileSpec",
    "QueueSpec",
    "Reaction",
    "Service",
    "access_specs",
    "client_username",
    "queue_specs",
]

# --- the manifest: one line per consuming service ------------------------------
# Commenting a line out removes the service, its queue and its identity from
# everything that is derived below.
_MANIFEST: tuple[Service, ...] = (
    orchestrator.SERVICE,
    agent_runtime.SERVICE,
    tool_execution.SERVICE,
    streaming_gateway.SERVICE,
    audit_pipeline.SERVICE,
)

SERVICES: dict[str, Service] = {svc.name: svc for svc in _MANIFEST}
SERVICE_NAMES: tuple[str, ...] = tuple(SERVICES)

# The one principal that publishes without consuming. It has no queue, so it
# is not in SERVICES.
GATEWAY = api_gateway.NAME

# Everything that connects to the broker, and the event types each may publish.
PUBLISHES: dict[str, tuple[str, ...]] = {
    GATEWAY: api_gateway.PUBLISHES,
    **{name: svc.publishes for name, svc in SERVICES.items()},
}
PRINCIPALS: tuple[str, ...] = tuple(PUBLISHES)


def access_specs(space: TopicSpace) -> list[AccessSpec]:
    """Every principal's client username, ACL profile and client profile, least privilege."""
    specs = []
    for principal, event_types in PUBLISHES.items():
        name = client_username(principal)
        specs.append(
            AccessSpec(
                principal=principal,
                username=name,
                acl_profile=name,
                client_profile=profile_for(bool(event_types), principal in SERVICES).name,
                publish_topics=tuple(space.topic_for(t) for t in event_types),
            )
        )
    return specs


def queue_specs(space: TopicSpace, prefix: str) -> list[QueueSpec]:
    """Every service's queue with its subscriptions, DMQ and owner, for kai-sim-provision."""
    return [
        QueueSpec(
            space.queue_name(prefix, svc.name),
            tuple(svc.subscriptions(space)),
            space.dmq_name(prefix, svc.name),
            client_username(svc.name),
        )
        for svc in SERVICES.values()
    ]
