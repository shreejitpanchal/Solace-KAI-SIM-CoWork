"""API gateway business logic: the root envelope of a new task.

Where it sits in the flow
-------------------------
    main.py (publish loop) --> [tasks.build_task] --> envelope --> main.py publishes it

A pure function, like the handlers of the consuming services: no broker, no
I/O. A real gateway would call this after authenticating the request and
persisting the task record, with the tenant, principal and workspace taken
from the caller's token rather than from arguments.
"""

from __future__ import annotations

from kai_sim_cowork.envelope import Envelope, Producer
from kai_sim_cowork.services.events import FLAKY_TOOL, TASK_CREATED

FLAKY_EVERY = 5  # every Nth task uses the flaky tool, to exercise the failure branch


def build_task(
    seq: int, tenant_id: str, me: Producer, principal_id: str, workspace_id: str
) -> Envelope:
    """The root envelope for task number `seq`: starts a new correlation and trace."""
    return Envelope.new(
        TASK_CREATED,
        me,
        tenant_id,
        {
            "taskId": f"task-{seq:04d}",
            "goal": f"Summarise document {seq} for the weekly digest",
            "tool": FLAKY_TOOL if seq % FLAKY_EVERY == 0 else "search",
        },
        principal_id=principal_id,
        workspace_id=workspace_id,
        # The same task submitted twice is the same business operation.
        idempotency_key=f"task-{seq:04d}",
    )
