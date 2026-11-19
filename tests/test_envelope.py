import json

import pytest

from kai_sim_cowork.envelope import (
    MAX_MESSAGE_BYTES,
    Envelope,
    EnvelopeError,
    Producer,
    new_traceparent,
)


def test_new_starts_a_correlation_and_a_trace(task_created):
    assert task_created.correlation_id == task_created.event_id
    assert task_created.causation_id is None
    assert task_created.schema_version == "v1"
    assert task_created.traceparent.startswith("00-") and len(task_created.traceparent) == 55
    assert task_created.occurred_at.endswith("+00:00")


def test_derive_keeps_workflow_context_and_sets_causation(task_created):
    me = Producer("orchestrator", "0.1.0", "orc-1")
    out = task_created.derive("cowork.agent.command.execute", me, {"taskId": "task-0001"})
    assert out.event_id != task_created.event_id
    assert out.correlation_id == task_created.correlation_id
    assert out.causation_id == task_created.event_id
    assert out.traceparent == task_created.traceparent
    assert out.tenant_id == "acme"
    assert out.principal_id == "user-1"
    assert out.workspace_id == "ws-1"
    assert out.producer == me
    assert out.idempotency_key is None  # not inherited: it belongs to the caller's operation
    assert out.payload == {"taskId": "task-0001"}


def test_wire_format_uses_the_briefs_field_names(task_created):
    data = json.loads(task_created.to_json())
    for name in (
        "eventId", "eventType", "schemaVersion", "occurredAt", "producer", "tenantId",
        "principalId", "workspaceId", "correlationId", "causationId", "traceparent",
        "idempotencyKey", "classification", "payload",
    ):
        assert name in data, name
    assert data["producer"] == {"service": "api-gateway", "version": "0.1.0", "instance": "gw-1"}
    assert "payloadRef" not in data  # absent, not null, when there is no reference


def test_from_json_round_trips(task_created):
    assert Envelope.from_json(task_created.to_json().encode()) == task_created
    assert Envelope.from_json(task_created.to_json()) == task_created


def test_properties_carry_routing_metadata_only(task_created):
    assert task_created.properties() == {
        "eventType": "cowork.orchestration.task.created",
        "correlationId": task_created.correlation_id,
        "tenantId": "acme",
    }


def test_summary_is_short_and_names_the_producer(task_created):
    s = task_created.summary()
    assert "cowork.orchestration.task.created" in s
    assert "from=api-gateway" in s
    assert "task-0001" in s


@pytest.mark.parametrize(
    "raw, match",
    [
        (b"not json", "not valid JSON"),
        (b"[]", "JSON object"),
        (b"{}", "missing required fields"),
        (b"\xff\xfe", "not valid JSON"),
    ],
)
def test_from_json_rejects_garbage_with_a_reason(raw, match):
    with pytest.raises(EnvelopeError, match=match):
        Envelope.from_json(raw)


def test_from_json_rejects_oversized_messages(task_created):
    data = task_created.to_dict()
    data["payload"] = {"blob": "x" * MAX_MESSAGE_BYTES}
    with pytest.raises(EnvelopeError, match="payloadRef"):
        Envelope.from_json(json.dumps(data))


@pytest.mark.parametrize(
    "mutation, match",
    [
        ({"producer": "orchestrator"}, "producer must be an object"),
        ({"payload": "text"}, "payload must be a JSON object"),
        ({"tenantId": ""}, "tenantId must be a non-empty string"),
        ({"eventType": "task.created"}, "eventType"),
    ],
)
def test_from_json_rejects_bad_field_shapes(task_created, mutation, match):
    data = task_created.to_dict()
    data.update(mutation)
    with pytest.raises(EnvelopeError, match=match):
        Envelope.from_json(json.dumps(data))


def test_event_type_shape_is_checked_on_construction(gateway):
    with pytest.raises(EnvelopeError, match="eventType"):
        Envelope.new("task.created", gateway, "acme", {})


def test_traceparents_are_unique():
    assert new_traceparent() != new_traceparent()
