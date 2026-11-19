"""The message envelope every simulated event and command travels in.

Shape (from the Cowork-Solace architecture brief, section 5)
------------------------------------------------------------
    eventId          unique id, used for de-duplication and audit correlation
    eventType        cowork.{domain}.{entity}.{name}, e.g. cowork.orchestration.task.created
    schemaVersion    v1; together with eventType it fixes the topic (topics.TopicSpace.topic_for)
    occurredAt       UTC ISO-8601, set by the producer
    producer         {service, version, instance}
    tenantId, principalId, workspaceId
    correlationId    the whole workflow (here: the task); copied to every derived message
    causationId      the eventId of the message this one reacts to
    traceparent      W3C trace context, propagated unchanged across hops
    idempotencyKey   optional business-level uniqueness key
    classification   data-sensitivity label
    payload          the small domain payload (dict); large content goes behind payloadRef
    payloadRef       optional signed object-store reference for large content

Rules enforced here
-------------------
* Envelope.new() creates a root message (correlationId = eventId).
* Envelope.derive() creates a reaction: same tenant, principal, workspace,
  correlationId and traceparent; causationId = the inbound eventId. This is
  what makes a whole task traceable end to end through every queue.
* Envelope.from_json() is the consumer's trust-boundary validation: size
  limit, JSON, required fields, eventType shape. A failure raises
  EnvelopeError and the consumer drops the message as a contract violation.
* Nothing here is a secret; the brief forbids tokens and credentials in
  messages and this simulation carries none.
"""

from __future__ import annotations

import json
import secrets
import uuid
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from typing import Any

from kai_sim_cowork.topics import ROOT, VERSION

# Documented maximum, well below broker limits: envelopes are control
# messages. Larger content belongs in object storage behind payloadRef.
MAX_MESSAGE_BYTES = 64 * 1024

REQUIRED_FIELDS = (
    "eventId",
    "eventType",
    "schemaVersion",
    "occurredAt",
    "producer",
    "tenantId",
    "correlationId",
    "traceparent",
    "classification",
    "payload",
)


class EnvelopeError(Exception):
    """The bytes on the wire are not a valid envelope; the message says what is wrong."""


def new_traceparent() -> str:
    """A fresh W3C traceparent: version 00, random trace-id and span-id, sampled."""
    return f"00-{secrets.token_hex(16)}-{secrets.token_hex(8)}-01"


@dataclass(frozen=True)
class Producer:
    service: str  # e.g. orchestrator
    version: str  # deployment version
    instance: str  # bounded instance / workload identifier (container hostname)

    def to_dict(self) -> dict[str, str]:
        return {"service": self.service, "version": self.version, "instance": self.instance}


@dataclass(frozen=True)
class Envelope:
    event_id: str
    event_type: str
    schema_version: str
    occurred_at: str
    producer: Producer
    tenant_id: str
    principal_id: str
    workspace_id: str
    correlation_id: str
    causation_id: str | None
    traceparent: str
    idempotency_key: str | None
    classification: str
    payload: dict[str, Any] = field(default_factory=dict)
    payload_ref: dict[str, Any] | None = None

    # --- construction ------------------------------------------------------

    @classmethod
    def new(
        cls,
        event_type: str,
        producer: Producer,
        tenant_id: str,
        payload: dict[str, Any],
        *,
        principal_id: str = "",
        workspace_id: str = "",
        classification: str = "internal",
        idempotency_key: str | None = None,
    ) -> Envelope:
        """A root message: starts a new correlation and a new trace."""
        event_id = str(uuid.uuid4())
        _check_event_type(event_type)
        return cls(
            event_id=event_id,
            event_type=event_type,
            schema_version=VERSION,
            occurred_at=datetime.now(UTC).isoformat(timespec="milliseconds"),
            producer=producer,
            tenant_id=tenant_id,
            principal_id=principal_id,
            workspace_id=workspace_id,
            correlation_id=event_id,
            causation_id=None,
            traceparent=new_traceparent(),
            idempotency_key=idempotency_key,
            classification=classification,
            payload=dict(payload),
        )

    def derive(
        self,
        event_type: str,
        producer: Producer,
        payload: dict[str, Any],
        *,
        classification: str | None = None,
    ) -> Envelope:
        """A reaction to this message: same workflow context, causation = this message."""
        _check_event_type(event_type)
        return replace(
            self,
            event_id=str(uuid.uuid4()),
            event_type=event_type,
            occurred_at=datetime.now(UTC).isoformat(timespec="milliseconds"),
            producer=producer,
            causation_id=self.event_id,
            idempotency_key=None,
            classification=self.classification if classification is None else classification,
            payload=dict(payload),
            payload_ref=None,
        )

    # --- wire format -------------------------------------------------------

    def to_dict(self) -> dict[str, Any]:
        data: dict[str, Any] = {
            "eventId": self.event_id,
            "eventType": self.event_type,
            "schemaVersion": self.schema_version,
            "occurredAt": self.occurred_at,
            "producer": self.producer.to_dict(),
            "tenantId": self.tenant_id,
            "principalId": self.principal_id,
            "workspaceId": self.workspace_id,
            "correlationId": self.correlation_id,
            "causationId": self.causation_id,
            "traceparent": self.traceparent,
            "idempotencyKey": self.idempotency_key,
            "classification": self.classification,
            "payload": self.payload,
        }
        if self.payload_ref is not None:
            data["payloadRef"] = self.payload_ref
        return data

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), separators=(",", ":"), ensure_ascii=False)

    @classmethod
    def from_json(cls, raw: bytes | str) -> Envelope:
        """Validate and parse an inbound envelope; EnvelopeError names the first problem."""
        size = len(raw.encode("utf-8")) if isinstance(raw, str) else len(raw)
        if size > MAX_MESSAGE_BYTES:
            raise EnvelopeError(
                f"message is {size} bytes, above the {MAX_MESSAGE_BYTES}-byte limit; "
                "large content belongs behind payloadRef"
            )
        try:
            data = json.loads(raw)
        except (ValueError, UnicodeDecodeError) as exc:
            raise EnvelopeError(f"payload is not valid JSON: {exc}") from exc
        if not isinstance(data, dict):
            raise EnvelopeError("envelope must be a JSON object")
        missing = [name for name in REQUIRED_FIELDS if name not in data]
        if missing:
            raise EnvelopeError(f"envelope is missing required fields: {', '.join(missing)}")
        producer = data["producer"]
        if not isinstance(producer, dict) or not {"service", "version", "instance"} <= set(
            producer
        ):
            raise EnvelopeError("producer must be an object with service, version and instance")
        if not isinstance(data["payload"], dict):
            raise EnvelopeError("payload must be a JSON object")
        for name in ("eventId", "eventType", "schemaVersion", "occurredAt", "tenantId",
                     "correlationId", "traceparent", "classification"):
            if not isinstance(data[name], str) or not data[name]:
                raise EnvelopeError(f"{name} must be a non-empty string")
        _check_event_type(data["eventType"])
        return cls(
            event_id=data["eventId"],
            event_type=data["eventType"],
            schema_version=data["schemaVersion"],
            occurred_at=data["occurredAt"],
            producer=Producer(
                str(producer["service"]), str(producer["version"]), str(producer["instance"])
            ),
            tenant_id=data["tenantId"],
            principal_id=str(data.get("principalId") or ""),
            workspace_id=str(data.get("workspaceId") or ""),
            correlation_id=data["correlationId"],
            causation_id=data.get("causationId") or None,
            traceparent=data["traceparent"],
            idempotency_key=data.get("idempotencyKey") or None,
            classification=data["classification"],
            payload=data["payload"],
            payload_ref=data.get("payloadRef"),
        )

    # --- helpers -----------------------------------------------------------

    def properties(self) -> dict[str, str]:
        """Routing metadata duplicated as message user properties (usable in selectors)."""
        return {
            "eventType": self.event_type,
            "correlationId": self.correlation_id,
            "tenantId": self.tenant_id,
        }

    def summary(self) -> str:
        """One log-friendly line: type, ids and the payload."""
        return (
            f"{self.event_type} id={self.event_id[:8]} corr={self.correlation_id[:8]} "
            f"cause={(self.causation_id or '-')[:8]} tenant={self.tenant_id} "
            f"from={self.producer.service} payload={self.payload}"
        )


def _check_event_type(event_type: str) -> None:
    parts = event_type.split(".")
    if len(parts) != 4 or parts[0] != ROOT or not all(parts):
        raise EnvelopeError(
            f"eventType {event_type!r} must be {ROOT}.{{domain}}.{{entity}}.{{name}}"
        )
