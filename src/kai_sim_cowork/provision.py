"""Provisioner: creates the access control, queues and DMQs on the broker over SEMP v2.

    kai-sim-provision

Where it sits in the flow
-------------------------
    [kai-sim-provision] --SEMP v2--> broker      runs once, before any service connects

Why it exists
-------------
The brief (section 13) wants queues, subscriptions, ACLs and client profiles
declared in version control rather than created ad hoc. The service packages
(services/) are that declaration; this service applies it. Nothing here can be done by a service
itself: the services connect with profiles that may not create endpoints or
change subscriptions, so the management API is the only way in.

Access control (services.access_specs(), services.CLIENT_PROFILES)
------------------------------------------------------------------
No service uses the broker's built-in `default` client username, client
profile or ACL profile. In this order it makes sure there is:

    client profile    three, by role. Each allows only what the role needs
                      (guaranteed send and/or receive); none allows endpoint
                      creation, transacted sessions or bridge connections.
    ACL profile       one per principal. Connect is allowed; publish and
                      subscribe default to disallow; a publish exception is
                      added for each topic the principal emits. There are no
                      subscribe exceptions: services receive only through
                      their queue, whose subscriptions are set here.
    client username   one per principal, bound to its ACL profile and its
                      role's client profile, with the password from
                      SOLACE_PASSWORD_<PRINCIPAL>.

Queues (services.queue_specs())
-------------------------------
    accessType          exclusive            (what the consumers bind as)
    owner               the service's client username
    permission          no-access            (for every other client username)
    respectTtlEnabled   true                 (without this maxTtl is ignored)
    maxTtl              QUEUE_MAX_TTL_SECONDS
    redeliveryEnabled   true
    maxRedeliveryCount  QUEUE_MAX_REDELIVERY_COUNT
    deadMsgQueue        <queue>.dmq
    subscriptions       the service's topic subscriptions

So a queue can be bound by exactly one identity, and a compromised service
can neither read another service's queue nor publish another's events.

Dead message queues
-------------------
Each service queue gets its own DMQ, `<queue>.dmq`, created first: exclusive,
no owner, no access for clients, no subscriptions, and no TTL so a dead
message stays until an administrator looks at it. The broker moves a message
from the service queue to its DMQ when the message expires (maxTtl) or has
been redelivered maxRedeliveryCount times (the consumer settles a message
FAILED when its handler raises, broker.py). Only messages published as
DMQ-eligible are moved; broker.py sets that flag on every publish. Without
it the broker discards them.

It is idempotent: existing objects are updated in place, an existing DMQ is
left alone and only missing subscriptions and publish exceptions are added,
so it is safe to run on every `up`. It never removes anything: a
subscription or exception dropped from a service package must be deleted by hand.

SEMP v2 calls used (all under /SEMP/v2/config/msgVpns/{vpn})
    GET / POST / PATCH  /clientProfiles, /aclProfiles, /clientUsernames, /queues
    GET / POST          /aclProfiles/{name}/publishTopicExceptions
    GET / POST          /queues/{name}/subscriptions

The broker takes 1-2 minutes to boot in compose, and SEMP answers before the
message spool is usable, so it waits for both (the VPN and the queue list)
for up to PROVISION_TIMEOUT_SECONDS before giving up.
"""

from __future__ import annotations

import argparse
import logging
import time
from typing import Any
from urllib.parse import quote

import httpx

from kai_sim_cowork.config import (
    ConfigError,
    SempConfig,
    broker_config,
    client_passwords,
    configure_logging,
    cowork_config,
    semp_config,
)
from kai_sim_cowork.services import (
    CLIENT_PROFILES,
    PRINCIPALS,
    AccessSpec,
    ClientProfileSpec,
    QueueSpec,
    access_specs,
    queue_specs,
)
from kai_sim_cowork.topics import TopicSpace

log = logging.getLogger(__name__)

# Connections one client username may hold: one per running instance, with
# room for a restart overlapping the old connection.
MAX_CONNECTIONS_PER_USERNAME = 5
# SEMP lists 10 items per page unless asked; no object here has near 100 members.
LIST_PAGE = "?count=100"


class ProvisionError(Exception):
    """A SEMP call failed; the message names the call, the broker's reason and the fix."""


# --- SEMP v2 objects -----------------------------------------------------------


def client_profile_settings(spec: ClientProfileSpec) -> dict[str, Any]:
    """What a role may do; PATCHed onto a client profile that already exists."""
    return {
        "allowGuaranteedMsgSendEnabled": spec.guaranteed_send,
        "allowGuaranteedMsgReceiveEnabled": spec.guaranteed_receive,
        # Endpoints are created here, never by a service.
        "allowGuaranteedEndpointCreateEnabled": False,
        "allowTransactedSessionsEnabled": False,
        "allowBridgeConnectionsEnabled": False,
        "maxConnectionCountPerClientUsername": MAX_CONNECTIONS_PER_USERNAME,
    }


def client_profile_body(spec: ClientProfileSpec) -> dict[str, Any]:
    return {"clientProfileName": spec.name, **client_profile_settings(spec)}


def acl_profile_settings() -> dict[str, Any]:
    """Deny by default: only the listed publish exceptions open anything up."""
    return {
        "clientConnectDefaultAction": "allow",
        "publishTopicDefaultAction": "disallow",
        "subscribeTopicDefaultAction": "disallow",
    }


def acl_profile_body(spec: AccessSpec) -> dict[str, Any]:
    return {"aclProfileName": spec.acl_profile, **acl_profile_settings()}


def client_username_body(spec: AccessSpec, password: str) -> dict[str, Any]:
    """The client username bound to its ACL profile and client profile. Holds a secret."""
    return {
        "clientUsername": spec.username,
        "password": password,
        "aclProfileName": spec.acl_profile,
        "clientProfileName": spec.client_profile,
        "enabled": True,
    }


def policy_body(spec: QueueSpec, max_ttl_seconds: int, max_redelivery_count: int) -> dict[str, Any]:
    """Access, expiry, redelivery limit and DMQ: PATCHed onto a queue that already exists."""
    return {
        "owner": spec.owner,
        "permission": "no-access",
        "respectTtlEnabled": True,
        "maxTtl": max_ttl_seconds,
        "redeliveryEnabled": True,
        "maxRedeliveryCount": max_redelivery_count,
        "deadMsgQueue": spec.dead_msg_queue,
    }


def queue_body(spec: QueueSpec, max_ttl_seconds: int, max_redelivery_count: int) -> dict[str, Any]:
    """The SEMP v2 queue object for a service queue: exclusive, plus policy_body."""
    return {
        "queueName": spec.name,
        "accessType": "exclusive",
        "ingressEnabled": True,
        "egressEnabled": True,
        **policy_body(spec, max_ttl_seconds, max_redelivery_count),
    }


def dmq_body(name: str) -> dict[str, Any]:
    """The SEMP v2 queue object for a dead message queue: no expiry, no client access."""
    return {
        "queueName": name,
        "accessType": "exclusive",
        "permission": "no-access",
        "ingressEnabled": True,
        "egressEnabled": True,
        "respectTtlEnabled": False,
    }


def _loggable(body: dict[str, Any] | None) -> dict[str, Any] | str:
    """A request body safe to log: any password is masked."""
    if not body:
        return ""
    return {key: "***" if key == "password" else value for key, value in body.items()}


def _semp_status(response: httpx.Response) -> str:
    """SEMP's symbolic error status (NOT_FOUND, ALREADY_EXISTS, ...) or '' if none."""
    try:
        return str(response.json()["meta"]["error"]["status"])
    except (ValueError, KeyError, TypeError):
        return ""


def _semp_description(response: httpx.Response) -> str:
    try:
        return str(response.json()["meta"]["error"]["description"])
    except (ValueError, KeyError, TypeError):
        return response.text[:200]


class SempClient:
    """Thin SEMP v2 config client scoped to one message VPN."""

    def __init__(
        self, cfg: SempConfig, vpn: str, transport: httpx.BaseTransport | None = None
    ) -> None:
        self._base = f"{cfg.url.rstrip('/')}/SEMP/v2/config/msgVpns/{quote(vpn, safe='')}"
        self._cfg = cfg
        self._vpn = vpn
        # Basic auth over SEMP; the password is never logged (SempConfig.masked()).
        self._http = httpx.Client(
            auth=(cfg.username, cfg.password), timeout=10.0, transport=transport
        )

    def close(self) -> None:
        self._http.close()

    def _call(self, method: str, path: str, body: dict[str, Any] | None = None) -> httpx.Response:
        url = self._base + path
        log.debug("SEMP %s %s %s", method, url, _loggable(body))
        try:
            return self._http.request(method, url, json=body)
        except httpx.HTTPError as exc:
            raise ProvisionError(
                f"SEMP {method} {url} failed: {exc}; check SEMP_URL ({self._cfg.url}) "
                "and that the broker's management port is reachable"
            ) from exc

    def _check(self, response: httpx.Response, what: str) -> None:
        if response.status_code in (200, 201):
            return
        if response.status_code == 401:
            raise ProvisionError(
                f"{what}: SEMP rejected the credentials for {self._cfg.username!r}; "
                "set SEMP_USERNAME / SEMP_PASSWORD to a broker admin"
            )
        raise ProvisionError(
            f"{what}: SEMP returned {response.status_code} {_semp_status(response)}: "
            f"{_semp_description(response)}"
        )

    def _not_ready(self) -> str:
        """Why the broker cannot be provisioned yet, or '' when it can.

        Two probes: the VPN (SEMP is up) and the queue list (the message
        spool is up). SEMP answers a little before the spool does, and queue
        calls in that window fail with MESSAGE_SPOOL_DATA_NOT_AVAILABLE.
        """
        for path, what in (("", "VPN"), ("/queues?count=1", "message spool")):
            try:
                response = self._call("GET", path)
            except ProvisionError as exc:
                return str(exc)  # connection refused etc.: the broker is still booting
            if response.status_code == 401:
                self._check(response, "connecting to SEMP")  # wrong credentials: no retry
            if response.status_code != 200:
                return f"{what} not ready: {response.status_code} {_semp_status(response)}"
        return ""

    def wait_ready(self, timeout_seconds: float, poll_seconds: float = 5.0) -> None:
        """Block until SEMP and the message spool answer, or raise after `timeout_seconds`.

        Covers the broker's boot time in compose. Bad credentials fail fast.
        """
        deadline = time.monotonic() + timeout_seconds
        attempt = 0
        while True:
            attempt += 1
            reason = self._not_ready()
            if not reason:
                log.info("SEMP ready at %s (VPN %s)", self._cfg.url, self._vpn)
                return
            if time.monotonic() >= deadline:
                raise ProvisionError(
                    f"SEMP at {self._cfg.url} not ready after {timeout_seconds:.0f}s "
                    f"({reason}); is the broker up, and is SEMP_URL its management URL?"
                )
            log.info("waiting for SEMP at %s (attempt %d: %s)", self._cfg.url, attempt, reason)
            time.sleep(poll_seconds)

    # --- generic create-or-update -------------------------------------------

    def _ensure(
        self,
        collection: str,
        noun: str,
        name: str,
        create: dict[str, Any],
        update: dict[str, Any] | None,
    ) -> str:
        """Create `name` in `collection`, or PATCH `update` onto it if it exists.

        `update` None leaves an existing object alone. Returns 'created',
        'updated' or 'exists'.
        """
        path = f"/{collection}/{quote(name, safe='')}"
        existing = self._call("GET", path)
        if existing.status_code == 200:
            if update is None:
                return "exists"
            updated = self._call("PATCH", path, update)
            self._check(updated, f"updating {noun} {name}")
            return "updated"
        if _semp_status(existing) != "NOT_FOUND":
            self._check(existing, f"looking up {noun} {name}")
        created = self._call("POST", f"/{collection}", create)
        self._check(created, f"creating {noun} {name}")
        return "created"

    def _add_missing(
        self, path: str, key: str, wanted: tuple[str, ...], extra: dict[str, Any], what: str
    ) -> list[str]:
        """POST each of `wanted` that the list at `path` lacks; returns the ones added."""
        listing = self._call("GET", path + LIST_PAGE)
        self._check(listing, f"listing {what}")
        present = {item[key] for item in listing.json().get("data", [])}
        added: list[str] = []
        for value in wanted:
            if value in present:
                continue
            response = self._call("POST", path, {key: value, **extra})
            # A concurrent creator may have won the race; that is still success.
            ok = response.status_code in (200, 201)
            if not ok and _semp_status(response) != "ALREADY_EXISTS":
                self._check(response, f"adding {value} to {what}")
            added.append(value)
        return added

    # --- access control -----------------------------------------------------

    def ensure_client_profile(self, spec: ClientProfileSpec) -> str:
        """Create the role's client profile or reset its permissions; 'created' or 'updated'."""
        return self._ensure(
            "clientProfiles",
            "client profile",
            spec.name,
            client_profile_body(spec),
            client_profile_settings(spec),
        )

    def ensure_acl_profile(self, spec: AccessSpec) -> tuple[str, list[str]]:
        """Create the deny-by-default ACL profile and add its missing publish exceptions.

        Returns the outcome ('created' or 'updated') and the exceptions added.
        """
        outcome = self._ensure(
            "aclProfiles",
            "ACL profile",
            spec.acl_profile,
            acl_profile_body(spec),
            acl_profile_settings(),
        )
        added = self._add_missing(
            f"/aclProfiles/{quote(spec.acl_profile, safe='')}/publishTopicExceptions",
            "publishTopicException",
            spec.publish_topics,
            {"publishTopicExceptionSyntax": "smf"},
            f"publish exceptions of ACL profile {spec.acl_profile}",
        )
        return outcome, added

    def ensure_client_username(self, spec: AccessSpec, password: str) -> str:
        """Create the client username, or bring an existing one in line.

        An existing username already on the right profiles only has its
        password set (connected clients stay connected). One on other
        profiles is disabled, rebound and re-enabled, because the broker does
        not change profiles on an enabled username. Returns 'created',
        'updated' or 'rebound'.
        """
        path = f"/clientUsernames/{quote(spec.username, safe='')}"
        what = f"client username {spec.username}"
        existing = self._call("GET", path)
        if existing.status_code != 200:
            if _semp_status(existing) != "NOT_FOUND":
                self._check(existing, f"looking up {what}")
            created = self._call("POST", "/clientUsernames", client_username_body(spec, password))
            self._check(created, f"creating {what}")
            return "created"
        current = existing.json().get("data", {})
        bound = (
            current.get("aclProfileName") == spec.acl_profile
            and current.get("clientProfileName") == spec.client_profile
            and current.get("enabled") is True
        )
        if bound:
            self._check(self._call("PATCH", path, {"password": password}), f"updating {what}")
            return "updated"
        self._check(self._call("PATCH", path, {"enabled": False}), f"disabling {what}")
        body = client_username_body(spec, password)
        del body["clientUsername"]
        self._check(self._call("PATCH", path, body), f"rebinding {what}")
        return "rebound"

    # --- queues -------------------------------------------------------------

    def ensure_queue(self, spec: QueueSpec, max_ttl_seconds: int, max_redelivery_count: int) -> str:
        """Create the service queue, or update its access / TTL / redelivery / DMQ if it exists.

        Returns 'created' or 'updated'.
        """
        return self._ensure(
            "queues",
            "queue",
            spec.name,
            queue_body(spec, max_ttl_seconds, max_redelivery_count),
            policy_body(spec, max_ttl_seconds, max_redelivery_count),
        )

    def ensure_dmq(self, name: str) -> str:
        """Create the dead message queue if it is missing; 'created' or 'exists'."""
        return self._ensure("queues", "queue", name, dmq_body(name), None)

    def ensure_subscriptions(self, spec: QueueSpec) -> list[str]:
        """Add each subscription the queue does not have yet; returns the ones added."""
        return self._add_missing(
            f"/queues/{quote(spec.name, safe='')}/subscriptions",
            "subscriptionTopic",
            spec.subscriptions,
            {},
            f"subscriptions of {spec.name}",
        )


def provision_access(
    client: SempClient, access: list[AccessSpec], passwords: dict[str, str]
) -> None:
    """Client profiles, then ACL profiles, then the client usernames that refer to both."""
    for profile in CLIENT_PROFILES:
        log.info(
            "client profile %s %s: guaranteed send=%s, receive=%s, endpoint create=off",
            profile.name,
            client.ensure_client_profile(profile),
            "on" if profile.guaranteed_send else "off",
            "on" if profile.guaranteed_receive else "off",
        )
    for spec in access:
        outcome, added = client.ensure_acl_profile(spec)
        log.info(
            "ACL profile %s %s: publish and subscribe disallowed by default; "
            "publish exceptions %s (%s)",
            spec.acl_profile,
            outcome,
            list(spec.publish_topics),
            f"added {added}" if added else "already present",
        )
    for spec in access:
        # The password itself is never logged, only that it was set.
        log.info(
            "client username %s %s: ACL profile %s, client profile %s, password set",
            spec.username,
            client.ensure_client_username(spec, passwords[spec.principal]),
            spec.acl_profile,
            spec.client_profile,
        )


def provision(
    client: SempClient, specs: list[QueueSpec], max_ttl_seconds: int, max_redelivery_count: int
) -> None:
    """Make every spec exist with its DMQ, owner, TTL / redelivery settings and subscriptions."""
    for spec in specs:
        # The DMQ first, so the service queue never points at a queue that is not there.
        if spec.dead_msg_queue:
            dmq_outcome = client.ensure_dmq(spec.dead_msg_queue)
            log.info("dead message queue %s %s", spec.dead_msg_queue, dmq_outcome)
        outcome = client.ensure_queue(spec, max_ttl_seconds, max_redelivery_count)
        added = client.ensure_subscriptions(spec)
        log.info(
            "queue %s %s: exclusive, owner=%s, others=no-access, maxTtl=%ds, respectTtl=on, "
            "maxRedelivery=%d, dmq=%s; subscriptions %s (%s)",
            spec.name,
            outcome,
            spec.owner or "none",
            max_ttl_seconds,
            max_redelivery_count,
            spec.dead_msg_queue or "none",
            list(spec.subscriptions),
            f"added {added}" if added else "already present",
        )


def main(argv: list[str] | None = None, transport: httpx.BaseTransport | None = None) -> int:
    """Process entry point (`kai-sim-provision`). Exits when done.

    `transport` is injectable so tests can serve a fake SEMP without a network.
    """
    parser = argparse.ArgumentParser(
        description="Create the simulation's access control, queues and DMQs over SEMP v2"
    )
    parser.parse_args(argv)
    configure_logging()

    try:
        cowork = cowork_config()
        semp = semp_config()
        passwords = client_passwords(PRINCIPALS)
    except ConfigError as exc:
        log.error("%s", exc)
        return 2
    broker = broker_config()  # VPN and queue prefix only; this never opens a messaging connection
    space = TopicSpace(cowork.environment, cowork.region)
    specs = queue_specs(space, broker.queue_prefix)
    access = access_specs(space)
    log.info(
        "provisioning %d client usernames, %d ACL profiles, %d client profiles and %d queues, "
        "each with a dead message queue, on VPN %s via %s (maxTtl=%ds, maxRedelivery=%d): %s",
        len(access),
        len(access),
        len(CLIENT_PROFILES),
        len(specs),
        broker.vpn,
        semp.masked(),
        semp.max_ttl_seconds,
        semp.max_redelivery_count,
        [s.name for s in specs],
    )

    client = SempClient(semp, broker.vpn, transport=transport)
    try:
        client.wait_ready(semp.timeout_seconds)
        # Identities first: a queue's owner must exist before the queue names it.
        provision_access(client, access, passwords)
        provision(client, specs, semp.max_ttl_seconds, semp.max_redelivery_count)
    except ProvisionError as exc:
        log.error("%s", exc)
        return 2
    finally:
        client.close()
    log.info(
        "provisioning complete: %d client usernames and %d queues ready", len(access), len(specs)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
