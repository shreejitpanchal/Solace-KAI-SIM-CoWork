"""Consumer: one simulated Cowork service bound to its own durable queue.

    kai-sim-consumer --service orchestrator

Where it sits in the flow
-------------------------
    publisher / other services --topic--> Solace --queue--> [this service] --topic--> Solace ...

What happens per message
------------------------
1. The broker delivers a message from the service's queue. The queue's topic
   subscriptions (services.py) decided that this service should see it; the
   service itself never filters.
2. The topic is parsed (topics.TopicSpace.parse) and the payload is validated
   as an envelope (envelope.Envelope.from_json). Either failing is a contract
   violation: the message is logged with the reason and dropped (acked), so a
   malformed message can never block the queue.
3. The service's reaction (services.Service.react) turns the envelope into
   zero or more derived envelopes, each carrying the same correlationId and
   traceparent and this message's eventId as causationId.
4. Each reaction is published persistently to the topic its eventType maps
   to, and the broker routes it into whichever queues subscribe to it.
"""

from __future__ import annotations

import argparse
import logging
import socket
from collections.abc import Callable

from kai_sim_cowork.broker import Broker, Handler, Inbound, Outbound
from kai_sim_cowork.config import ConfigError, broker_config, configure_logging, cowork_config
from kai_sim_cowork.envelope import Envelope, EnvelopeError, Producer
from kai_sim_cowork.services import SERVICE_NAMES, SERVICES, Service
from kai_sim_cowork.topics import TopicSpace, validate_level

log = logging.getLogger(__name__)

SERVICE_VERSION = "0.1.0"


def make_handler(
    service: Service, space: TopicSpace, me: Producer, publish: Callable[[Outbound], None]
) -> Handler:
    """Build the per-message callback: validate, react, publish the reactions.

    `publish` is injected (normally Broker.publish) so tests can capture
    what would have been published.
    """

    def handle(msg: Inbound) -> None:
        # Trust boundary: topic shape first, then the envelope. Nothing from
        # the payload is used before from_json() has accepted it.
        try:
            parts = space.parse(msg.topic)
        except ValueError as exc:
            log.warning("%s: contract violation, dropped: %s", service.name, exc)
            return
        try:
            inbound = Envelope.from_json(msg.payload)
        except EnvelopeError as exc:
            log.warning(
                "%s: contract violation on %s, dropped: %s", service.name, msg.topic, exc
            )
            return
        if inbound.event_type != parts.event_type:
            log.warning(
                "%s: contract violation on %s, dropped: envelope eventType %s does not match "
                "the topic (%s)",
                service.name,
                msg.topic,
                inbound.event_type,
                parts.event_type,
            )
            return

        reactions = service.react(inbound, parts, me)
        log.info(
            "%s: received %s from queue (topic %s) and %s; %d reaction(s)",
            service.name,
            inbound.summary(),
            msg.topic,
            service.receipt,
            len(reactions),
        )
        for out in reactions:
            topic = space.topic_for(out.event_type, out.schema_version)
            publish(Outbound(topic, out.to_json(), out.properties()))
            log.info(
                "%s: published %s -> %s (cause=%s)",
                service.name,
                out.event_type,
                topic,
                inbound.event_id[:8],
            )

    return handle


def main(argv: list[str] | None = None) -> int:
    """Process entry point (`kai-sim-consumer --service <name>`). Runs until Ctrl-C."""
    parser = argparse.ArgumentParser(description="One simulated Cowork service")
    # choices= rejects anything outside the registry before any connection
    # is made; the name becomes part of a queue name, so it is a closed set.
    parser.add_argument("--service", required=True, choices=SERVICE_NAMES)
    parser.add_argument(
        "--instance",
        default=socket.gethostname(),
        help="bounded instance identifier put in producer.instance (default: hostname)",
    )
    args = parser.parse_args(argv)
    configure_logging()

    try:
        validate_level(args.instance, "--instance")
    except ValueError as exc:
        parser.error(str(exc))
    try:
        cowork = cowork_config()
    except ConfigError as exc:
        log.error("%s", exc)
        return 2

    service = SERVICES[args.service]
    space = TopicSpace(cowork.environment, cowork.region)
    broker_cfg = broker_config()
    queue = space.queue_name(broker_cfg.queue_prefix, service.name)
    subscriptions = service.subscriptions(space)
    me = Producer(service.name, SERVICE_VERSION, args.instance)
    log.info(
        "%s starting: queue=%s subscriptions=%s (the broker routes by these; "
        "this service never filters)",
        service.name,
        queue,
        subscriptions,
    )

    broker = Broker(broker_cfg).connect()
    try:
        broker.subscribe(queue, subscriptions, make_handler(service, space, me, broker.publish))
        log.info("%s ready; waiting for events (Ctrl-C to stop)", service.name)
        broker.wait()
    finally:
        broker.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
