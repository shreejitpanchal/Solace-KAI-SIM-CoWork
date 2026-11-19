"""The event and command types of the simulation: the contract the services share.

Where it sits in the flow
-------------------------
    events.py --> every service package (what it subscribes to, handles and publishes)

A service never imports another service. What they have in common is this
catalogue: the envelope `eventType` of every event and command. The topic
follows from the type (topics.TopicSpace.topic_for), so naming a type here is
all it takes to publish or subscribe to it.

    cowork.{domain}.{entity}.{name}          an event: a fact that happened
    cowork.{capability}.command.{name}       a command: a request to one owning capability
"""

from __future__ import annotations

# Orchestration domain: the task lifecycle, owned by the orchestrator.
TASK_CREATED = "cowork.orchestration.task.created"  # published by the API gateway
TASK_STATE_CHANGED = "cowork.orchestration.task.state-changed"
TASK_COMPLETED = "cowork.orchestration.task.completed"
TASK_FAILED = "cowork.orchestration.task.failed"

# Agent capability: owned by the agent runtime.
AGENT_EXECUTE = "cowork.agent.command.execute"  # command, sent by the orchestrator
AGENT_STARTED = "cowork.agent.agent.started"
AGENT_COMPLETED = "cowork.agent.agent.completed"
AGENT_FAILED = "cowork.agent.agent.failed"

# Tooling capability: owned by the tool execution service.
TOOL_EXECUTE = "cowork.tooling.command.execute"  # command, sent by the agent runtime
TOOL_ACCEPTED = "cowork.tooling.tool.accepted"
TOOL_PROGRESSED = "cowork.tooling.tool.progressed"
TOOL_COMPLETED = "cowork.tooling.tool.completed"
TOOL_FAILED = "cowork.tooling.tool.failed"

# The tool name the simulated gateway uses to exercise the failure path.
FLAKY_TOOL = "flaky"
