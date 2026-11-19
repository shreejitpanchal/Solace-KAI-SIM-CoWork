"""kai-sim-provision creates the access control, queues and DMQs over SEMP v2, idempotently."""

from __future__ import annotations

import json
import logging
import re
from urllib.parse import unquote

import httpx
import pytest

from kai_sim_cowork import provision
from kai_sim_cowork.services import (
    CLIENT_PROFILES,
    PRINCIPALS,
    PROFILE_PUBLISH_CONSUME,
    SERVICE_NAMES,
    AccessSpec,
    QueueSpec,
)

BASE = "/SEMP/v2/config/msgVpns/default"
EXPECTED = [f"q.test.dev.apse1.{name}" for name in SERVICE_NAMES]
DMQS = [f"{name}.dmq" for name in EXPECTED]
USERNAMES = [f"cowork-{principal}" for principal in PRINCIPALS]

# collection -> the attribute that names an object in it
KEYS = {
    "queues": "queueName",
    "aclProfiles": "aclProfileName",
    "clientProfiles": "clientProfileName",
    "clientUsernames": "clientUsername",
}
# member list under an object -> the attribute that names a member
MEMBERS = {
    "subscriptions": "subscriptionTopic",
    "publishTopicExceptions": "publishTopicException",
}


def _error(status: str, http: int = 400) -> httpx.Response:
    return httpx.Response(http, json={"meta": {"error": {"status": status, "description": status}}})


class FakeSemp:
    """Just enough of SEMP v2 config: four object collections and their member lists."""

    def __init__(
        self, refuse_first: int = 0, unauthorized: bool = False, spool_down_first: int = 0
    ) -> None:
        self.objects: dict[str, dict[str, dict]] = {name: {} for name in KEYS}
        self.members: dict[tuple[str, str], list[str]] = {}
        self.calls: list[tuple[str, str]] = []
        self.bodies: list[dict] = []  # every request body, in order
        self.refuse_first = refuse_first
        self.unauthorized = unauthorized
        self.spool_down_first = spool_down_first  # queue-list probes answered "spool not ready"
        self.seen = 0

    # Shorthands the assertions read better with.
    @property
    def queues(self) -> dict[str, dict]:
        return self.objects["queues"]

    @property
    def acls(self) -> dict[str, dict]:
        return self.objects["aclProfiles"]

    @property
    def profiles(self) -> dict[str, dict]:
        return self.objects["clientProfiles"]

    @property
    def users(self) -> dict[str, dict]:
        return self.objects["clientUsernames"]

    def subs(self, queue: str) -> list[str]:
        return self.members.setdefault(("queues", queue), [])

    def exceptions(self, acl: str) -> list[str]:
        return self.members.setdefault(("aclProfiles", acl), [])

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
        if body:
            self.bodies.append(body)

        if rest == "":
            return httpx.Response(200, json={"data": {"msgVpnName": "default"}})
        if rest == "/queues" and request.method == "GET":
            # SEMP is up before the message spool is: queue calls fail until then.
            if self.spool_down_first > 0:
                self.spool_down_first -= 1
                return _error("MESSAGE_SPOOL_DATA_NOT_AVAILABLE")
            return httpx.Response(200, json={"data": []})
        if m := re.fullmatch(r"/(\w+)", rest):
            collection = m.group(1)
            name = body[KEYS[collection]]
            if name in self.objects[collection]:
                return _error("ALREADY_EXISTS")
            self.objects[collection][name] = body
            return httpx.Response(200, json={"data": body})
        if m := re.fullmatch(r"/(\w+)/([^/]+)", rest):
            collection, name = m.group(1), unquote(m.group(2))
            if name not in self.objects[collection]:
                return _error("NOT_FOUND")
            if request.method == "PATCH":
                self.objects[collection][name].update(body)
            return httpx.Response(200, json={"data": self.objects[collection][name]})
        if m := re.fullmatch(r"/(\w+)/([^/]+)/(\w+)", rest):
            collection, name, kind = m.group(1), unquote(m.group(2)), m.group(3)
            if name not in self.objects[collection]:
                return _error("NOT_FOUND")
            key = MEMBERS[kind]
            have = self.members.setdefault((collection, name), [])
            if request.method == "GET":
                return httpx.Response(200, json={"data": [{key: value} for value in have]})
            if body[key] in have:
                return _error("ALREADY_EXISTS")
            have.append(body[key])
            return httpx.Response(200, json={"data": body})
        return _error("NOT_FOUND", 404)


def _client(semp: FakeSemp) -> provision.SempClient:
    return provision.SempClient(
        provision.SempConfig("http://semp", "admin", "admin", 60, 3, 1.0),
        "default",
        transport=semp.transport(),
    )


@pytest.fixture
def env(monkeypatch):
    monkeypatch.setenv("SOLACE_QUEUE_PREFIX", "q.test")
    monkeypatch.setenv("COWORK_ENVIRONMENT", "dev")
    monkeypatch.setenv("COWORK_REGION", "apse1")
    monkeypatch.setenv("QUEUE_MAX_TTL_SECONDS", "86400")
    monkeypatch.setenv("QUEUE_MAX_REDELIVERY_COUNT", "3")
    monkeypatch.setenv("PROVISION_TIMEOUT_SECONDS", "1")
    for principal in PRINCIPALS:
        variable = "SOLACE_PASSWORD_" + principal.upper().replace("-", "_")
        monkeypatch.setenv(variable, f"pw-{principal}")
    monkeypatch.setattr(provision.time, "sleep", lambda s: None)  # retries without waiting


# --- object bodies ---------------------------------------------------------------


def test_queue_body_is_exclusive_owned_with_ttl_redelivery_limit_and_dmq():
    spec = QueueSpec("q.x", ("a/>",), "q.x.dmq", "cowork-x")
    body = provision.queue_body(spec, 86400, 3)
    assert body["queueName"] == "q.x"
    assert body["accessType"] == "exclusive"
    assert body["owner"] == "cowork-x"
    assert body["permission"] == "no-access"  # nobody but the owner
    assert body["respectTtlEnabled"] is True
    assert body["maxTtl"] == 86400
    assert body["redeliveryEnabled"] is True
    assert body["maxRedeliveryCount"] == 3
    assert body["deadMsgQueue"] == "q.x.dmq"
    assert provision.policy_body(spec, 60, 5) == {
        "owner": "cowork-x",
        "permission": "no-access",
        "respectTtlEnabled": True,
        "maxTtl": 60,
        "redeliveryEnabled": True,
        "maxRedeliveryCount": 5,
        "deadMsgQueue": "q.x.dmq",
    }


def test_dmq_body_never_expires_what_it_holds_and_admits_no_client():
    body = provision.dmq_body("q.x.dmq")
    assert body["queueName"] == "q.x.dmq"
    assert body["accessType"] == "exclusive"
    assert body["permission"] == "no-access"
    assert "owner" not in body
    assert body["respectTtlEnabled"] is False
    assert "deadMsgQueue" not in body  # a DMQ has no DMQ of its own


def test_acl_profile_body_denies_publish_and_subscribe_by_default():
    spec = AccessSpec("x", "cowork-x", "cowork-x", "cowork-publish", ("a/b",))
    assert provision.acl_profile_body(spec) == {
        "aclProfileName": "cowork-x",
        "clientConnectDefaultAction": "allow",
        "publishTopicDefaultAction": "disallow",
        "subscribeTopicDefaultAction": "disallow",
    }


@pytest.mark.parametrize("profile", CLIENT_PROFILES, ids=lambda p: p.name)
def test_client_profile_body_grants_only_the_roles_messaging_rights(profile):
    body = provision.client_profile_body(profile)
    assert body["clientProfileName"] == profile.name
    assert body["allowGuaranteedMsgSendEnabled"] is profile.guaranteed_send
    assert body["allowGuaranteedMsgReceiveEnabled"] is profile.guaranteed_receive
    assert body["allowGuaranteedEndpointCreateEnabled"] is False
    assert body["allowTransactedSessionsEnabled"] is False
    assert body["allowBridgeConnectionsEnabled"] is False
    assert body["maxConnectionCountPerClientUsername"] == 5


def test_client_username_body_binds_both_profiles():
    spec = AccessSpec("x", "cowork-x", "cowork-x", "cowork-publish", ())
    assert provision.client_username_body(spec, "pw") == {
        "clientUsername": "cowork-x",
        "password": "pw",
        "aclProfileName": "cowork-x",
        "clientProfileName": "cowork-publish",
        "enabled": True,
    }


# --- access control ---------------------------------------------------------------


def test_creates_a_client_username_and_acl_profile_per_principal(env, caplog):
    semp = FakeSemp()
    with caplog.at_level(logging.INFO):
        assert provision.main([], transport=semp.transport()) == 0
    assert sorted(semp.users) == sorted(USERNAMES)
    assert sorted(semp.acls) == sorted(USERNAMES)  # one ACL profile each, same name
    assert sorted(semp.profiles) == sorted(p.name for p in CLIENT_PROFILES)
    assert "default" not in semp.users and "default" not in semp.acls
    for name, user in semp.users.items():
        assert user["aclProfileName"] == name
        assert user["enabled"] is True
    for acl in semp.acls.values():
        assert acl["publishTopicDefaultAction"] == "disallow"
        assert acl["subscribeTopicDefaultAction"] == "disallow"
    assert "client username cowork-orchestrator created" in caplog.text
    assert "ACL profile cowork-orchestrator created" in caplog.text
    assert "client profile cowork-publish-consume created" in caplog.text


def test_each_principal_may_publish_exactly_its_own_topics(env):
    semp = FakeSemp()
    assert provision.main([], transport=semp.transport()) == 0
    assert semp.exceptions("cowork-api-gateway") == [
        "cowork/dev/apse1/orchestration/task/created/v1"
    ]
    assert semp.exceptions("cowork-orchestrator") == [
        "cowork/dev/apse1/agent/command/execute/v1",
        "cowork/dev/apse1/orchestration/task/state-changed/v1",
        "cowork/dev/apse1/orchestration/task/completed/v1",
        "cowork/dev/apse1/orchestration/task/failed/v1",
    ]
    assert semp.exceptions("cowork-tool-execution") == [
        "cowork/dev/apse1/tooling/tool/accepted/v1",
        "cowork/dev/apse1/tooling/tool/progressed/v1",
        "cowork/dev/apse1/tooling/tool/completed/v1",
        "cowork/dev/apse1/tooling/tool/failed/v1",
    ]
    # the sinks may publish nothing at all
    assert semp.exceptions("cowork-streaming-gateway") == []
    assert semp.exceptions("cowork-audit-pipeline") == []
    added = [b for b in semp.bodies if "publishTopicException" in b]
    assert {b["publishTopicExceptionSyntax"] for b in added} == {"smf"}


def test_client_profile_follows_the_role(env):
    semp = FakeSemp()
    assert provision.main([], transport=semp.transport()) == 0
    assert semp.users["cowork-api-gateway"]["clientProfileName"] == "cowork-publish"
    assert semp.users["cowork-orchestrator"]["clientProfileName"] == "cowork-publish-consume"
    assert semp.users["cowork-agent-runtime"]["clientProfileName"] == "cowork-publish-consume"
    assert semp.users["cowork-streaming-gateway"]["clientProfileName"] == "cowork-consume"
    assert semp.users["cowork-audit-pipeline"]["clientProfileName"] == "cowork-consume"
    assert semp.profiles["cowork-consume"]["allowGuaranteedMsgSendEnabled"] is False
    assert semp.profiles["cowork-publish"]["allowGuaranteedMsgReceiveEnabled"] is False


def test_each_username_gets_its_own_password(env):
    semp = FakeSemp()
    assert provision.main([], transport=semp.transport()) == 0
    for principal in PRINCIPALS:
        assert semp.users[f"cowork-{principal}"]["password"] == f"pw-{principal}"


def test_access_control_exists_before_any_queue_names_an_owner(env):
    semp = FakeSemp()
    assert provision.main([], transport=semp.transport()) == 0
    first_queue = semp.calls.index(("POST", "/queues"))
    last_user = max(i for i, c in enumerate(semp.calls) if c == ("POST", "/clientUsernames"))
    assert last_user < first_queue


def test_existing_username_on_the_right_profiles_only_has_its_password_rotated(env, monkeypatch):
    semp = FakeSemp()
    assert provision.main([], transport=semp.transport()) == 0
    semp.calls.clear()
    semp.bodies.clear()
    monkeypatch.setenv("SOLACE_PASSWORD_ORCHESTRATOR", "rotated")
    assert provision.main([], transport=semp.transport()) == 0
    assert ("POST", "/clientUsernames") not in semp.calls
    assert semp.users["cowork-orchestrator"]["password"] == "rotated"
    # never disabled, so connected services are not dropped by a re-run
    assert not [b for b in semp.bodies if b.get("enabled") is False]


def test_existing_username_on_other_profiles_is_disabled_and_rebound(env, caplog):
    semp = FakeSemp()
    semp.users["cowork-orchestrator"] = {
        "clientUsername": "cowork-orchestrator",
        "aclProfileName": "default",
        "clientProfileName": "default",
        "enabled": True,
    }
    with caplog.at_level(logging.INFO):
        assert provision.main([], transport=semp.transport()) == 0
    user = semp.users["cowork-orchestrator"]
    assert user["aclProfileName"] == "cowork-orchestrator"
    assert user["clientProfileName"] == PROFILE_PUBLISH_CONSUME.name
    assert user["enabled"] is True
    assert user["password"] == "pw-orchestrator"
    patches = [b for b in semp.bodies if set(b) == {"enabled"} or "aclProfileName" in b]
    disable = patches.index({"enabled": False})
    assert "aclProfileName" in patches[disable + 1]  # disabled first, then rebound
    assert "client username cowork-orchestrator rebound" in caplog.text


def test_existing_acl_profile_is_reset_to_deny_and_gains_missing_exceptions(env):
    semp = FakeSemp()
    semp.acls["cowork-orchestrator"] = {
        "aclProfileName": "cowork-orchestrator",
        "publishTopicDefaultAction": "allow",
        "subscribeTopicDefaultAction": "allow",
    }
    semp.exceptions("cowork-orchestrator").append("cowork/dev/apse1/agent/command/execute/v1")
    assert provision.main([], transport=semp.transport()) == 0
    assert semp.acls["cowork-orchestrator"]["publishTopicDefaultAction"] == "disallow"
    assert semp.acls["cowork-orchestrator"]["subscribeTopicDefaultAction"] == "disallow"
    assert len(semp.exceptions("cowork-orchestrator")) == 4


def test_missing_service_passwords_return_2_naming_each_before_any_semp_call(
    env, monkeypatch, caplog
):
    monkeypatch.delenv("SOLACE_PASSWORD_ORCHESTRATOR")
    monkeypatch.delenv("SOLACE_PASSWORD_AUDIT_PIPELINE")
    semp = FakeSemp()
    with caplog.at_level(logging.ERROR):
        assert provision.main([], transport=semp.transport()) == 2
    assert "SOLACE_PASSWORD_ORCHESTRATOR" in caplog.text
    assert "SOLACE_PASSWORD_AUDIT_PIPELINE" in caplog.text
    assert ".env.example" in caplog.text
    assert semp.seen == 0


def test_no_password_ever_appears_in_logs(env, monkeypatch, caplog):
    monkeypatch.setenv("SEMP_PASSWORD", "s3cret-semp-pw")
    semp = FakeSemp()
    with caplog.at_level(logging.DEBUG):
        assert provision.main([], transport=semp.transport()) == 0
    assert "s3cret-semp-pw" not in caplog.text
    for principal in PRINCIPALS:  # client passwords travel in SEMP bodies, logged at DEBUG
        assert f"pw-{principal}" not in caplog.text
    assert "'password': '***'" in caplog.text


# --- queues -------------------------------------------------------------------------


def test_spec_without_a_dmq_creates_only_the_queue():
    semp = FakeSemp()
    provision.provision(_client(semp), [QueueSpec("q.solo", ("a/>",))], 60, 3)
    assert list(semp.queues) == ["q.solo"]


def test_creates_every_service_queue_with_24h_ttl_and_subscriptions(env):
    semp = FakeSemp()
    assert provision.main([], transport=semp.transport()) == 0
    assert sorted(semp.queues) == sorted(EXPECTED + DMQS)
    for name in EXPECTED:
        body = semp.queues[name]
        assert body["maxTtl"] == 86400, name
        assert body["respectTtlEnabled"] is True, name
        assert body["accessType"] == "exclusive", name
    assert semp.subs("q.test.dev.apse1.orchestrator") == [
        "cowork/dev/apse1/orchestration/task/created/v1",
        "cowork/dev/apse1/agent/agent/completed/v1",
        "cowork/dev/apse1/agent/agent/failed/v1",
    ]
    assert semp.subs("q.test.dev.apse1.audit-pipeline") == ["cowork/dev/apse1/>"]


def test_every_service_queue_points_at_its_own_dmq_created_first(env, caplog):
    semp = FakeSemp()
    with caplog.at_level(logging.INFO):
        assert provision.main([], transport=semp.transport()) == 0
    created = list(semp.queues)  # insertion order = creation order
    for name in EXPECTED:
        dmq = f"{name}.dmq"
        assert semp.queues[name]["deadMsgQueue"] == dmq
        assert semp.queues[name]["maxRedeliveryCount"] == 3
        assert semp.queues[name]["redeliveryEnabled"] is True
        assert created.index(dmq) < created.index(name)
        assert semp.queues[dmq]["respectTtlEnabled"] is False
        assert semp.subs(dmq) == []  # only the broker puts messages there
    assert "dead message queue q.test.dev.apse1.orchestrator.dmq created" in caplog.text
    assert "maxRedelivery=3, dmq=q.test.dev.apse1.orchestrator.dmq" in caplog.text


def test_each_queue_is_owned_by_its_service_and_closed_to_everyone_else(env, caplog):
    semp = FakeSemp()
    with caplog.at_level(logging.INFO):
        assert provision.main([], transport=semp.transport()) == 0
    for service in SERVICE_NAMES:
        queue = semp.queues[f"q.test.dev.apse1.{service}"]
        assert queue["owner"] == f"cowork-{service}"
        assert queue["permission"] == "no-access"
        assert queue["owner"] in semp.users  # the owner is a provisioned username
    for name in DMQS:  # only the broker writes there, only an admin reads
        assert "owner" not in semp.queues[name]
        assert semp.queues[name]["permission"] == "no-access"
    assert "owner=cowork-orchestrator, others=no-access" in caplog.text


def test_second_run_is_idempotent_and_patches_ttl(env, monkeypatch, caplog):
    semp = FakeSemp()
    assert provision.main([], transport=semp.transport()) == 0
    # five service queues and five DMQs
    assert [c for c in semp.calls if c == ("POST", "/queues")] == [("POST", "/queues")] * 10
    semp.calls.clear()

    monkeypatch.setenv("QUEUE_MAX_TTL_SECONDS", "3600")
    monkeypatch.setenv("QUEUE_MAX_REDELIVERY_COUNT", "7")
    with caplog.at_level(logging.INFO):
        assert provision.main([], transport=semp.transport()) == 0
    # nothing re-created, no member added twice
    assert not [c for c in semp.calls if c[0] == "POST"]
    # every service queue updated in place; existing DMQs are left alone
    assert sum(1 for m, p in semp.calls if m == "PATCH" and p.startswith("/queues/")) == 5
    assert all(semp.queues[name]["maxTtl"] == 3600 for name in EXPECTED)
    assert all(semp.queues[name]["maxRedeliveryCount"] == 7 for name in EXPECTED)
    assert "dead message queue q.test.dev.apse1.orchestrator.dmq exists" in caplog.text


def test_existing_open_queue_is_locked_down_and_missing_subscription_added(env):
    semp = FakeSemp()
    name = "q.test.dev.apse1.orchestrator"
    semp.queues[name] = {
        "queueName": name,
        "permission": "consume",
        "respectTtlEnabled": False,
        "maxTtl": 0,
    }
    semp.subs(name).append("cowork/dev/apse1/orchestration/task/created/v1")
    assert provision.main([], transport=semp.transport()) == 0
    assert semp.queues[name]["owner"] == "cowork-orchestrator"  # an ownerless queue is taken over
    assert semp.queues[name]["permission"] == "no-access"
    assert semp.queues[name]["maxTtl"] == 86400
    assert semp.queues[name]["respectTtlEnabled"] is True
    assert semp.queues[name]["deadMsgQueue"] == f"{name}.dmq"
    assert f"{name}.dmq" in semp.queues
    assert len(semp.subs(name)) == 3


# --- readiness and failures ---------------------------------------------------------


def test_waits_for_the_broker_to_boot(env, caplog):
    semp = FakeSemp(refuse_first=3)
    with caplog.at_level(logging.INFO):
        assert provision.main([], transport=semp.transport()) == 0
    assert semp.seen > 3
    assert "waiting for SEMP" in caplog.text
    assert len(semp.queues) == 10  # five service queues, five DMQs


def test_waits_for_the_message_spool_after_semp_answers(env, caplog):
    """A freshly booted broker serves the VPN before queue calls work."""
    semp = FakeSemp(spool_down_first=2)
    with caplog.at_level(logging.INFO):
        assert provision.main([], transport=semp.transport()) == 0
    assert "message spool not ready: 400 MESSAGE_SPOOL_DATA_NOT_AVAILABLE" in caplog.text
    assert len(semp.queues) == 10
    # nothing was looked up or created until the spool answered
    first_object_call = next(i for i, c in enumerate(semp.calls) if c[1].count("/") == 2)
    assert semp.calls[:first_object_call].count(("GET", "/queues")) == 3


def test_gives_up_when_the_message_spool_never_comes_up(env, monkeypatch, caplog):
    monkeypatch.setenv("PROVISION_TIMEOUT_SECONDS", "0.001")
    semp = FakeSemp(spool_down_first=10_000)
    with caplog.at_level(logging.ERROR):
        assert provision.main([], transport=semp.transport()) == 2
    assert "not ready" in caplog.text
    assert "MESSAGE_SPOOL_DATA_NOT_AVAILABLE" in caplog.text
    assert semp.queues == {} and semp.users == {}


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
        if request.url.path.endswith(("/msgVpns/default", "/queues")):  # the readiness probes
            return httpx.Response(200, json={"data": {}})
        if request.method == "GET":
            return _error("NOT_FOUND")
        return _error("INVALID_PARAMETER")

    with caplog.at_level(logging.ERROR):
        assert provision.main([], transport=httpx.MockTransport(handle)) == 2
    assert "creating client profile cowork-publish" in caplog.text  # the first object made
    assert "INVALID_PARAMETER" in caplog.text


@pytest.mark.parametrize(
    "seed, expected",
    [
        ("clientUsernames", "looking up client username cowork-api-gateway"),
        ("queues", "looking up queue q.test.dev.apse1.orchestrator.dmq"),
    ],
)
def test_semp_error_on_lookup_is_reported(env, caplog, seed, expected):
    semp = FakeSemp()

    def handle(request: httpx.Request) -> httpx.Response:
        single = re.fullmatch(rf"{BASE}/{seed}/[^/]+", request.url.path)
        if single and request.method == "GET":
            return _error("NOT_ALLOWED")
        return semp.handle(request)

    with caplog.at_level(logging.ERROR):
        assert provision.main([], transport=httpx.MockTransport(handle)) == 2
    assert expected in caplog.text
    assert "NOT_ALLOWED" in caplog.text


def test_bad_ttl_setting_returns_2_before_any_semp_call(env, monkeypatch, caplog):
    monkeypatch.setenv("QUEUE_MAX_TTL_SECONDS", "one-day")
    semp = FakeSemp()
    with caplog.at_level(logging.ERROR):
        assert provision.main([], transport=semp.transport()) == 2
    assert "QUEUE_MAX_TTL_SECONDS" in caplog.text
    assert semp.seen == 0


def test_bad_redelivery_setting_returns_2_before_any_semp_call(env, monkeypatch, caplog):
    monkeypatch.setenv("QUEUE_MAX_REDELIVERY_COUNT", "256")
    semp = FakeSemp()
    with caplog.at_level(logging.ERROR):
        assert provision.main([], transport=semp.transport()) == 2
    assert "QUEUE_MAX_REDELIVERY_COUNT" in caplog.text
    assert semp.seen == 0
