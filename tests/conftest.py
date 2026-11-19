from __future__ import annotations

import pytest

from kai_sim_cowork.envelope import Envelope, Producer
from kai_sim_cowork.services import TASK_CREATED
from kai_sim_cowork.topics import TopicSpace


@pytest.fixture(scope="session")
def space() -> TopicSpace:
    return TopicSpace("dev", "apse1")


@pytest.fixture(scope="session")
def gateway() -> Producer:
    return Producer("api-gateway", "0.1.0", "gw-1")


@pytest.fixture
def task_created(gateway: Producer) -> Envelope:
    """A root task.created envelope as the simulated gateway publishes it."""
    return Envelope.new(
        TASK_CREATED,
        gateway,
        "acme",
        {"taskId": "task-0001", "goal": "Summarise document 1", "tool": "search"},
        principal_id="user-1",
        workspace_id="ws-1",
        idempotency_key="task-0001",
    )
