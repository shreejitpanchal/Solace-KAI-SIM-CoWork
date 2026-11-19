"""Publisher: plays the API gateway / task service that starts each task.

    kai-sim-publisher [--count N] [--interval S]

Where it sits in the flow
-------------------------
    [publisher] --cowork/{env}/{region}/orchestration/task/created/v1--> Solace --> queues ...

What it does
------------
Every PUBLISH_INTERVAL_SECONDS (default 5) it publishes one
`cowork.orchestration.task.created` envelope for a new task, persistently,
and waits for the broker's acknowledgement. It knows nothing about queues
or consumers: the topic is the whole contract. The broker places a copy in
every queue whose subscription matches (orchestrator, streaming-gateway,
audit-pipeline), and the orchestrator's reaction starts the rest of the flow.

Every fifth task asks for the "flaky" tool so the failure branch
(tool.failed -> agent.failed -> task.failed) is exercised as well.
"""

from __future__ import annotations

import argparse
import logging
import socket
import threading

from kai_sim_cowork.broker import Broker, Outbound
from kai_sim_cowork.config import (
    ConfigError,
    broker_config,
    configure_logging,
    cowork_config,
    publisher_config,
)
from kai_sim_cowork.envelope import Envelope, Producer
from kai_sim_cowork.services import FLAKY_TOOL, TASK_CREATED
from kai_sim_cowork.topics import TopicSpace, validate_level

log = logging.getLogger(__name__)

PRODUCER_NAME = "api-gateway"
PRODUCER_VERSION = "0.1.0"
FLAKY_EVERY = 5  # every Nth task uses the flaky tool


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
        idempotency_key=f"task-{seq:04d}",
    )


def main(argv: list[str] | None = None) -> int:
    """Process entry point (`kai-sim-publisher`). Publishes until Ctrl-C or --count tasks."""
    parser = argparse.ArgumentParser(description="Simulated API gateway: publishes task.created")
    parser.add_argument(
        "--count",
        type=int,
        default=None,
        help="tasks to publish, then exit; 0 = until stopped (default: PUBLISH_COUNT or 0)",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=None,
        help="seconds between tasks (default: PUBLISH_INTERVAL_SECONDS or 5)",
    )
    parser.add_argument("--principal-id", default="user-1", help="principalId in the envelope")
    parser.add_argument("--workspace-id", default="ws-1", help="workspaceId in the envelope")
    parser.add_argument("--instance", default=socket.gethostname())
    args = parser.parse_args(argv)
    configure_logging()

    try:
        validate_level(args.principal_id, "--principal-id")
        validate_level(args.workspace_id, "--workspace-id")
        validate_level(args.instance, "--instance")
    except ValueError as exc:
        parser.error(str(exc))

    # Command-line flags win over the environment, which wins over defaults.
    try:
        cowork = cowork_config()
        pub_cfg = publisher_config()
    except ConfigError as exc:
        log.error("%s", exc)
        return 2
    count = pub_cfg.count if args.count is None else args.count
    interval = pub_cfg.interval_seconds if args.interval is None else args.interval
    if count < 0 or interval < 0:
        parser.error("--count and --interval must not be negative")

    space = TopicSpace(cowork.environment, cowork.region)
    topic = space.topic_for(TASK_CREATED)
    me = Producer(PRODUCER_NAME, PRODUCER_VERSION, args.instance)
    log.info(
        "publishing %s to %s every %.1fs for tenant %s (%s); Ctrl-C to stop",
        TASK_CREATED,
        topic,
        interval,
        cowork.tenant_id,
        f"{count} tasks" if count else "until stopped",
    )

    # Broker.publish() blocks until the broker has acknowledged each message.
    # The Event makes the pause interruptible so Ctrl-C stops the loop promptly.
    pause = threading.Event()
    broker = Broker(broker_config()).connect()
    seq = 0
    try:
        while count == 0 or seq < count:
            seq += 1
            env = build_task(seq, cowork.tenant_id, me, args.principal_id, args.workspace_id)
            broker.publish(Outbound(topic, env.to_json(), env.properties()))
            log.info(
                "published task %s (%s) corr=%s -> %s; the broker now routes it by topic",
                env.payload["taskId"],
                env.payload["tool"],
                env.correlation_id[:8],
                topic,
            )
            if count and seq >= count:
                break
            pause.wait(interval)
    except KeyboardInterrupt:
        log.info("stopped by user after %d task(s)", seq)
    finally:
        broker.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
