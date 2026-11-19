"""The simulated Cowork services: what each one subscribes to, and how it reacts.

This is the single registry of consumers. Each entry says:

    name            the service, also the last level of its queue name
    receipt         what the service does with a message it receives (for the log)
    subscriptions   the topic subscriptions its queue carries, built for a TopicSpace
    react           inbound envelope -> list of envelopes to publish in response

The flow that emerges (brief, section 13), one task at a time:

    api-gateway ─task/created─► orchestrator ─agent/command/execute─► agent-runtime
                                    ▲                                  │
                                    │ agent/completed | failed         │ tooling/command/execute
                                    │                                  ▼
                                    └────── tool/completed | failed ── tool-execution
                                            (+ accepted, progressed)

    streaming-gateway  receives every task lifecycle event and tool progress
    audit-pipeline     receives a copy of everything (cowork/{env}/{region}/>)

Adding a service is one more entry in SERVICES and one more compose service;
its queue is provisioned from queue_specs() automatically.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from kai_sim_cowork.envelope import Envelope, Producer
from kai_sim_cowork.topics import TopicParts, TopicSpace

# A reaction never touches the broker: it maps one validated inbound
# envelope to the envelopes that should be published in response, so it is
# unit-testable and the consumer loop stays generic.
Reaction = Callable[[Envelope, TopicParts, Producer], list[Envelope]]

# Event types used in the simulation (envelope eventType; the topic follows).
TASK_CREATED = "cowork.orchestration.task.created"
TASK_STATE_CHANGED = "cowork.orchestration.task.state-changed"
TASK_COMPLETED = "cowork.orchestration.task.completed"
TASK_FAILED = "cowork.orchestration.task.failed"
AGENT_EXECUTE = "cowork.agent.command.execute"
AGENT_STARTED = "cowork.agent.agent.started"
AGENT_COMPLETED = "cowork.agent.agent.completed"
AGENT_FAILED = "cowork.agent.agent.failed"
TOOL_EXECUTE = "cowork.tooling.command.execute"
TOOL_ACCEPTED = "cowork.tooling.tool.accepted"
TOOL_PROGRESSED = "cowork.tooling.tool.progressed"
TOOL_COMPLETED = "cowork.tooling.tool.completed"
TOOL_FAILED = "cowork.tooling.tool.failed"

# The tool name the simulated gateway uses to exercise the failure path.
FLAKY_TOOL = "flaky"


@dataclass(frozen=True)
class Service:
    name: str
    receipt: str
    subscriptions: Callable[[TopicSpace], list[str]]
    react: Reaction


@dataclass(frozen=True)
class QueueSpec:
    """One queue to exist on the broker and the topic subscriptions it carries."""

    name: str
    subscriptions: tuple[str, ...]


# --- reactions ---------------------------------------------------------------


def orchestrator_react(inbound: Envelope, parts: TopicParts, me: Producer) -> list[Envelope]:
    """Owns workflow state: starts an agent for a new task, closes the task on the outcome."""
    task_id = inbound.payload.get("taskId")
    if inbound.event_type == TASK_CREATED:
        return [
            inbound.derive(
                AGENT_EXECUTE,
                me,
                {
                    "taskId": task_id,
                    "goal": inbound.payload.get("goal"),
                    "tool": inbound.payload.get("tool"),
                    "deadlineSeconds": 60,
                },
            ),
            inbound.derive(TASK_STATE_CHANGED, me, {"taskId": task_id, "state": "running"}),
        ]
    if inbound.event_type == AGENT_COMPLETED:
        return [
            inbound.derive(
                TASK_COMPLETED,
                me,
                {"taskId": task_id, "resultRef": inbound.payload.get("resultRef")},
            )
        ]
    if inbound.event_type == AGENT_FAILED:
        return [
            inbound.derive(
                TASK_FAILED, me, {"taskId": task_id, "reason": inbound.payload.get("reason")}
            )
        ]
    return []


def agent_runtime_react(inbound: Envelope, parts: TopicParts, me: Producer) -> list[Envelope]:
    """Runs the agent turn: asks the tooling capability for the tool, reports the outcome."""
    task_id = inbound.payload.get("taskId")
    if inbound.event_type == AGENT_EXECUTE:
        return [
            inbound.derive(AGENT_STARTED, me, {"taskId": task_id}),
            inbound.derive(
                TOOL_EXECUTE,
                me,
                {
                    "taskId": task_id,
                    "tool": inbound.payload.get("tool"),
                    "args": {"query": inbound.payload.get("goal")},
                },
            ),
        ]
    if inbound.event_type == TOOL_COMPLETED:
        return [
            inbound.derive(
                AGENT_COMPLETED,
                me,
                {"taskId": task_id, "resultRef": inbound.payload.get("resultRef")},
            )
        ]
    if inbound.event_type == TOOL_FAILED:
        return [
            inbound.derive(
                AGENT_FAILED, me, {"taskId": task_id, "reason": inbound.payload.get("reason")}
            )
        ]
    return []


def tool_execution_react(inbound: Envelope, parts: TopicParts, me: Producer) -> list[Envelope]:
    """Runs the tool: accepted, one progress tick, then completed (or failed for the flaky tool)."""
    if inbound.event_type != TOOL_EXECUTE:
        return []
    task_id = inbound.payload.get("taskId")
    tool = inbound.payload.get("tool")
    out = [
        inbound.derive(TOOL_ACCEPTED, me, {"taskId": task_id, "tool": tool}),
        inbound.derive(TOOL_PROGRESSED, me, {"taskId": task_id, "tool": tool, "percent": 50}),
    ]
    if tool == FLAKY_TOOL:
        out.append(
            inbound.derive(
                TOOL_FAILED,
                me,
                {"taskId": task_id, "tool": tool, "reason": "tool returned an error (simulated)"},
            )
        )
    else:
        out.append(
            inbound.derive(
                TOOL_COMPLETED,
                me,
                {
                    "taskId": task_id,
                    "tool": tool,
                    # The result itself would live in object storage; only a reference travels.
                    "resultRef": f"s3://cowork-results/{inbound.tenant_id}/{task_id}.json",
                },
            )
        )
    return out


def no_reaction(inbound: Envelope, parts: TopicParts, me: Producer) -> list[Envelope]:
    """Sinks: they consume, they do not publish back into the flow."""
    return []


# --- registry ----------------------------------------------------------------

SERVICES: dict[str, Service] = {
    "orchestrator": Service(
        "orchestrator",
        "recorded workflow state",
        lambda s: [
            s.event_subscription("orchestration", "task", "created"),
            s.event_subscription("agent", "agent", "completed"),
            s.event_subscription("agent", "agent", "failed"),
        ],
        orchestrator_react,
    ),
    "agent-runtime": Service(
        "agent-runtime",
        "ran the agent turn",
        lambda s: [
            s.command_subscription("agent", "execute"),
            s.event_subscription("tooling", "tool", "completed"),
            s.event_subscription("tooling", "tool", "failed"),
        ],
        agent_runtime_react,
    ),
    "tool-execution": Service(
        "tool-execution",
        "ran the tool in an isolated worker",
        lambda s: [s.command_subscription("tooling", "execute")],
        tool_execution_react,
    ),
    "streaming-gateway": Service(
        "streaming-gateway",
        "pushed the update to the user's session",
        lambda s: [
            s.event_subscription("orchestration", "task"),  # every task lifecycle event
            s.event_subscription("tooling", "tool", "progressed"),
        ],
        no_reaction,
    ),
    "audit-pipeline": Service(
        "audit-pipeline",
        "stored an immutable audit record",
        lambda s: [s.all_subscription()],  # the one justified broad subscription
        no_reaction,
    ),
}

SERVICE_NAMES: tuple[str, ...] = tuple(SERVICES)


def queue_specs(space: TopicSpace, prefix: str) -> list[QueueSpec]:
    """Every service's queue with its subscriptions; what kai-sim-provision creates."""
    return [
        QueueSpec(space.queue_name(prefix, svc.name), tuple(svc.subscriptions(space)))
        for svc in SERVICES.values()
    ]
