"""Thin wrapper over the Solace Python API (solace-pubsubplus): only what the simulation needs.

Why a wrapper
-------------
The simulation services and their tests talk to `Broker`, `Inbound` and `Outbound`, not
to the SDK. That keeps the application logic (the service packages under
services/, consumer.py) free of broker details and unit-testable without a broker. The
SDK, which ships a native library, is imported inside the methods that need
it, so importing this module never requires a broker or the native library.

Messaging model
---------------
Guaranteed (persistent) messaging throughout:
  * publish() waits for the broker's acknowledgement, so a returned call means
    the event is spooled on the broker;
  * subscribe() binds a persistent receiver to a durable exclusive queue that
    carries the topic subscription, so events are spooled while the consumer
    is down and delivered when it rebinds;
  * each message is acknowledged after the handler returns;
  * a handler that raises settles the message FAILED instead: the broker
    redelivers it, and after the queue's maxRedeliveryCount moves it to the
    queue's dead message queue (provision.py sets both);
  * every message is published DMQ-eligible, without which the broker would
    discard an expired or undeliverable message instead of moving it.

Message shape used by the flow
------------------------------
    topic        cowork/{env}/{region}/{domain}/{entity}/{event}/v1
    properties   user properties: eventType, correlationId, tenantId (routing metadata)
    payload      bytes, the JSON envelope (see envelope.py)
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any

from kai_sim_cowork.config import BrokerConfig

log = logging.getLogger(__name__)

# How long publish() waits for the broker's ack before raising.
PUBLISH_ACK_TIMEOUT_MS = 10_000


@dataclass(frozen=True)
class Inbound:
    """A received message, SDK-free: what handlers get."""

    topic: str  # the exact topic the message was published to
    payload: bytes
    properties: Mapping[str, str] = field(default_factory=dict)  # user properties


@dataclass(frozen=True)
class Outbound:
    """A message to publish."""

    topic: str
    payload: bytes | str
    properties: Mapping[str, str] = field(default_factory=dict)


# A handler receives one Inbound per message. It should return normally
# (the message is then acked) and never raise for a bad message: log and drop.
# Raising means "I could not process this": the message is redelivered and
# ends in the dead message queue if it keeps failing.
Handler = Callable[[Inbound], None]


def deliver(handler: Handler, inbound: Inbound, queue_name: str) -> bool:
    """Run `handler` on one message. True = ack it; False = it raised, settle it FAILED."""
    try:
        handler(inbound)
    except Exception:
        log.exception(
            "handler failed on %s from queue %s; settling FAILED so the broker redelivers it, "
            "then moves it to the queue's dead message queue after maxRedeliveryCount",
            inbound.topic,
            queue_name,
        )
        return False
    return True


def to_inbound(message: Any) -> Inbound:
    """Convert an SDK InboundMessage into the SDK-free shape handlers consume.

    get_properties() returns the user properties set by the publisher via
    with_property(): eventType, correlationId and tenantId.
    """
    props = message.get_properties() or {}
    return Inbound(
        topic=message.get_destination_name(),
        payload=message.get_payload_as_bytes() or b"",
        properties={str(k): str(v) for k, v in props.items()},
    )


class Broker:
    """One connection to the broker, used for publishing and/or consuming."""

    def __init__(self, cfg: BrokerConfig) -> None:
        self._cfg = cfg
        self._service: Any = None  # MessagingService once connected
        self._publisher: Any = None  # PersistentMessagePublisher, created lazily
        self._receivers: list[Any] = []  # PersistentMessageReceiver per subscribe()
        self._stop = threading.Event()

    def connect(self) -> Broker:
        """Open the SMF connection using SOLACE_HOST / VPN / USERNAME / PASSWORD."""
        from solace.messaging.messaging_service import MessagingService, RetryStrategy

        props = {
            "solace.messaging.transport.host": self._cfg.host,
            "solace.messaging.service.vpn-name": self._cfg.vpn,
            "solace.messaging.authentication.scheme.basic.username": self._cfg.username,
            "solace.messaging.authentication.scheme.basic.password": self._cfg.password,
        }
        # Initial-connect retries (60 x 3 s) cover a broker container that is
        # still booting; reconnection retries cover a broker restart mid-run.
        self._service = (
            MessagingService.builder()
            .from_properties(props)
            .with_connection_retry_strategy(RetryStrategy.parametrized_retry(60, 3000))
            .with_reconnection_retry_strategy(RetryStrategy.parametrized_retry(20, 3000))
            .build()
        )
        self._service.connect()
        log.info("connected to broker %s", self._cfg.masked())  # password masked
        return self

    def publish(self, msg: Outbound) -> None:
        """Persistent publish; returns once the broker has spooled the message.

        User properties are attached with with_property(); the envelope
        itself travels in the payload, as given. The message is marked
        DMQ-eligible so a queue can move it to its dead message queue.
        """
        from solace.messaging.config.solace_properties.message_properties import (
            PERSISTENT_DMQ_ELIGIBLE,
        )
        from solace.messaging.resources.topic import Topic

        if self._publisher is None:
            self._publisher = self._service.create_persistent_message_publisher_builder().build()
            self._publisher.start()
        builder = self._service.message_builder().from_properties({PERSISTENT_DMQ_ELIGIBLE: True})
        for key, value in msg.properties.items():
            builder = builder.with_property(key, value)
        log.debug(
            "publishing to %s: %d bytes, properties=%s, waiting for broker ack",
            msg.topic,
            len(msg.payload),
            sorted(msg.properties),
        )
        self._publisher.publish_await_acknowledgement(
            builder.build(msg.payload), Topic.of(msg.topic), PUBLISH_ACK_TIMEOUT_MS
        )
        log.debug("broker acknowledged %s (spooled to every matching queue)", msg.topic)

    def subscribe(self, queue_name: str, subscriptions: list[str], handler: Handler) -> None:
        """Bind to a durable exclusive queue carrying `subscriptions`; deliver to `handler`.

        With `create_queues` off (the default) this only binds: the queue and
        its subscriptions must have been provisioned (provision.py), and a
        missing queue fails loudly here. That is all a least-privilege client
        may do; `subscriptions` is then used for the log line only.

        With `create_queues` on (SOLACE_CREATE_QUEUES=true) a missing queue
        is created and the subscriptions are added on start. That needs
        "allow guaranteed endpoint create" on the client profile and
        subscribe rights in the ACL profile, which the provisioned profiles
        deliberately withhold; it is for a throwaway broker's `default` user.

        Delivery is asynchronous: the SDK calls the handler on its own thread.
        A message whose handler raises is settled FAILED (see deliver()), which
        needs a broker that supports negative acknowledgements (10.2.1 or later).
        """
        from solace.messaging.config.message_acknowledgement_configuration import Outcome
        from solace.messaging.config.missing_resources_creation_configuration import (
            MissingResourcesCreationStrategy,
        )
        from solace.messaging.receiver.message_receiver import MessageHandler
        from solace.messaging.resources.queue import Queue
        from solace.messaging.resources.topic_subscription import TopicSubscription

        strategy = (
            MissingResourcesCreationStrategy.CREATE_ON_START
            if self._cfg.create_queues
            else MissingResourcesCreationStrategy.DO_NOT_CREATE
        )
        builder = (
            self._service.create_persistent_message_receiver_builder()
            .with_missing_resources_creation_strategy(strategy)
            .with_required_message_outcome_support(Outcome.FAILED)
        )
        if self._cfg.create_queues:
            builder = builder.with_subscriptions([TopicSubscription.of(s) for s in subscriptions])
        receiver = builder.build(Queue.durable_exclusive_queue(queue_name))

        class _Adapter(MessageHandler):
            """SDK callback -> our Handler, with ack / failed-settlement handling."""

            def on_message(self, message: Any) -> None:
                # Handlers drop bad events deliberately and return normally, so
                # those are acked. An exception is a failure to process: the
                # message stays on the queue and is redelivered, and the queue's
                # maxRedeliveryCount sends a poison message to the DMQ instead
                # of letting it loop forever.
                inbound = to_inbound(message)
                log.debug("queue %s delivered a message on %s", queue_name, inbound.topic)
                if deliver(handler, inbound, queue_name):
                    receiver.ack(message)
                    log.debug("acked message on %s from queue %s", inbound.topic, queue_name)
                else:
                    receiver.settle(message, Outcome.FAILED)

        receiver.start()
        receiver.receive_async(_Adapter())
        self._receivers.append(receiver)
        log.info("consuming queue %s with subscriptions %s", queue_name, subscriptions)

    def wait(self) -> None:
        """Block the main thread until Ctrl-C or `stop()`; messages arrive on SDK threads."""
        try:
            while not self._stop.wait(1.0):
                pass
        except KeyboardInterrupt:
            pass

    def stop(self) -> None:
        self._stop.set()

    def close(self) -> None:
        """Terminate receivers and publisher, then disconnect. Safe to call twice."""
        for receiver in self._receivers:
            receiver.terminate()
        self._receivers.clear()
        if self._publisher is not None:
            self._publisher.terminate()
            self._publisher = None
        if self._service is not None:
            self._service.disconnect()
            self._service = None
