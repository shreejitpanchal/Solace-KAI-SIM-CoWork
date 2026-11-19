"""kai-sim-provision creates every service queue over SEMP v2 with a 24-hour TTL, idempotently."""

from __future__ import annotations

import json
import logging
import re
from urllib.parse import unquote

import httpx
import pytest

from kai_sim_cowork import provision
from kai_sim_cowork.services import QueueSpec, SERVICE_NAMES

BASE = "/SEMP/v2/config/msgVpns/default"
EXPECTED = [f"q.test.dev.apse1.{name}" for name in SERVICE_NAMES]


def _error(status: str, http: int = 400) -> httpx.Response:
    return httpx.Response(http, json={"meta": {"error": {"status": status, "description": status}}})


class FakeSemp:
    """Just enough of SEMP v2 config for queues and their subscriptions."""

    def __init__(self, refuse_first: int = 0, unauthorized: bool = False) -> None:
        self.queues: dict[str, dict] = {}
        self.subs: dict[str, list[str]] = {}
        self.calls: list[tuple[str, str]] = []
        self.refuse_first = refuse_first
        self.unauthorized = unauthorized
        self.seen = 0

    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self.handle)

    def handle(self, request: httpx.Request) -> httpx.Response:
        self.seen += 1
        if self.seen <= self.refuse_first:
            raise httpx.ConnectError("connection refused")
        if self.unauthorized:
            return httpx.Response(401, json={"meta": {"error": {"status": "UNAUTHORIZED"}}})
        path = request.url.path
        assert path.startswith(BASE), path
        rest = path[len(BASE) :]
        self.calls.append((request.method, rest))
        body = json.loads(request.content) if request.content else {}

        if rest == "":
            return httpx.Response(200, json={"data": {"msgVpnName": "default"}})
        if rest == "/queues" and request.method == "POST":
            name = body["queueName"]
            if name in self.queues:
                return _error("ALREADY_EXISTS")
            self.queues[name] = body
            self.subs.setdefault(name, [])
            return httpx.Response(200, json={"data": body})
        if m := re.fullmatch(r"/queues/([^/]+)", rest):
            name = unquote(m.group(1))
            if name not in self.queues:
                return _error("NOT_FOUND")
            if request.method == "PATCH":
                self.queues[name].update(body)
            return httpx.Response(200, json={"data": self.queues[name]})
        if m := re.fullmatch(r"/queues/([^/]+)/subscriptions", rest):
            name = unquote(m.group(1))
            if name not in self.queues:
                return _error("NOT_FOUND")
            if request.method == "GET":
                data = [{"subscriptionTopic": t} for t in self.subs[name]]
                return httpx.Response(200, json={"data": data})
            topic = body["subscriptionTopic"]
            if topic in self.subs[name]:
                return _error("ALREADY_EXISTS")
            self.subs[name].append(topic)
            return httpx.Response(200, json={"data": body})
        return _error("NOT_FOUND", 404)


@pytest.fixture
def env(monkeypatch):
    monkeypatch.setenv("SOLACE_QUEUE_PREFIX", "q.test")
    monkeypatch.setenv("COWORK_ENVIRONMENT", "dev")
    monkeypatch.setenv("COWORK_REGION", "apse1")
    monkeypatch.setenv("QUEUE_MAX_TTL_SECONDS", "86400")
    monkeypatch.setenv("PROVISION_TIMEOUT_SECONDS", "1")
    monkeypatch.setattr(provision.time, "sleep", lambda s: None)  # retries without waiting


def test_queue_body_is_exclusive_with_ttl_enforced():
    body = provision.queue_body(QueueSpec("q.x", ("a/>",)), 86400)
    assert body["queueName"] == "q.x"
    assert body["accessType"] == "exclusive"
    assert body["permission"] == "consume"
    assert body["respectTtlEnabled"] is True
    assert body["maxTtl"] == 86400
    assert provision.ttl_body(60) == {"respectTtlEnabled": True, "maxTtl": 60}


def test_creates_every_service_queue_with_24h_ttl_and_subscriptions(env):
    semp = FakeSemp()
    assert provision.main([], transport=semp.transport()) == 0
    assert sorted(semp.queues) == sorted(EXPECTED)
    for name, body in semp.queues.items():
        assert body["maxTtl"] == 86400, name
        assert body["respectTtlEnabled"] is True, name
        assert body["accessType"] == "exclusive", name
    assert semp.subs["q.test.dev.apse1.orchestrator"] == [
        "cowork/dev/apse1/orchestration/task/created/v1",
        "cowork/dev/apse1/agent/agent/completed/v1",
        "cowork/dev/apse1/agent/agent/failed/v1",
    ]
    assert semp.subs["q.test.dev.apse1.audit-pipeline"] == ["cowork/dev/apse1/>"]


def test_second_run_is_idempotent_and_patches_ttl(env, monkeypatch):
    semp = FakeSemp()
    assert provision.main([], transport=semp.transport()) == 0
    assert [c for c in semp.calls if c == ("POST", "/queues")] == [("POST", "/queues")] * 5
    semp.calls.clear()

    monkeypatch.setenv("QUEUE_MAX_TTL_SECONDS", "3600")
    assert provision.main([], transport=semp.transport()) == 0
    assert ("POST", "/queues") not in semp.calls  # nothing re-created
    assert sum(1 for m, p in semp.calls if m == "PATCH") == 5  # every TTL updated in place
    assert all(body["maxTtl"] == 3600 for body in semp.queues.values())
    assert not [c for c in semp.calls if c[0] == "POST" and c[1].endswith("/subscriptions")]


def test_existing_queue_without_ttl_is_fixed_and_missing_subscription_added(env):
    semp = FakeSemp()
    name = "q.test.dev.apse1.orchestrator"
    semp.queues[name] = {"queueName": name, "respectTtlEnabled": False, "maxTtl": 0}
    semp.subs[name] = ["cowork/dev/apse1/orchestration/task/created/v1"]
    assert provision.main([], transport=semp.transport()) == 0
    assert semp.queues[name]["maxTtl"] == 86400
    assert semp.queues[name]["respectTtlEnabled"] is True
    assert len(semp.subs[name]) == 3


def test_waits_for_the_broker_to_boot(env, caplog):
    semp = FakeSemp(refuse_first=3)
    with caplog.at_level(logging.INFO):
        assert provision.main([], transport=semp.transport()) == 0
    assert semp.seen > 3
    assert "waiting for SEMP" in caplog.text
    assert len(semp.queues) == 5


def test_gives_up_with_an_actionable_error_when_semp_never_answers(env, monkeypatch, caplog):
    monkeypatch.setenv("PROVISION_TIMEOUT_SECONDS", "0.001")
    semp = FakeSemp(refuse_first=10_000)
    with caplog.at_level(logging.ERROR):
        assert provision.main([], transport=semp.transport()) == 2
    assert "not ready" in caplog.text
    assert "SEMP_URL" in caplog.text
    assert semp.queues == {}


def test_bad_credentials_fail_fast_without_retrying(env, caplog):
    semp = FakeSemp(unauthorized=True)
    with caplog.at_level(logging.ERROR):
        assert provision.main([], transport=semp.transport()) == 2
    assert semp.seen == 1
    assert "SEMP_USERNAME" in caplog.text


def test_semp_error_on_create_is_reported_with_the_brokers_reason(env, caplog):
    def handle(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/msgVpns/default"):
            return httpx.Response(200, json={"data": {}})
        if request.method == "GET":
            return _error("NOT_FOUND")
        return _error("INVALID_PARAMETER")

    with caplog.at_level(logging.ERROR):
        assert provision.main([], transport=httpx.MockTransport(handle)) == 2
    assert "creating queue q.test.dev.apse1.orchestrator" in caplog.text
    assert "INVALID_PARAMETER" in caplog.text


def test_bad_ttl_setting_returns_2_before_any_semp_call(env, monkeypatch, caplog):
    monkeypatch.setenv("QUEUE_MAX_TTL_SECONDS", "one-day")
    semp = FakeSemp()
    with caplog.at_level(logging.ERROR):
        assert provision.main([], transport=semp.transport()) == 2
    assert "QUEUE_MAX_TTL_SECONDS" in caplog.text
    assert semp.seen == 0


def test_semp_password_never_appears_in_logs(env, monkeypatch, caplog):
    monkeypatch.setenv("SEMP_PASSWORD", "s3cret-semp-pw")
    semp = FakeSemp()
    with caplog.at_level(logging.DEBUG):
        assert provision.main([], transport=semp.transport()) == 0
    assert "s3cret-semp-pw" not in caplog.text
    assert "***" in caplog.text
