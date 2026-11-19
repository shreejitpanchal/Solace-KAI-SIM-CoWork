"""Provisioner: creates every service queue on the broker over SEMP v2, with a message TTL.

    kai-sim-provision

Where it sits in the flow
-------------------------
    [kai-sim-provision] --SEMP v2--> broker      runs once, before any consumer binds

Why it exists
-------------
The brief (section 13) wants queues, subscriptions and their policies
declared in version control rather than created ad hoc. services.py is that
declaration; this service applies it. The messaging API can create a missing
queue when a consumer binds (SOLACE_CREATE_QUEUES=true), but only with broker
defaults; the message expiry (QUEUE_MAX_TTL_SECONDS, default 24 hours) is a
queue property that only the management API can set. So for every queue in
services.queue_specs() this makes sure it exists with:

    accessType          exclusive            (what the consumers bind as)
    permission          consume
    respectTtlEnabled   true                 (without this maxTtl is ignored)
    maxTtl              QUEUE_MAX_TTL_SECONDS
    subscriptions       the service's topic subscriptions

It is idempotent: an existing queue is updated in place and only missing
subscriptions are added, so it is safe to run on every `up`.

SEMP v2 calls used (all under /SEMP/v2/config/msgVpns/{vpn})
    GET   /queues/{name}                      exists?
    POST  /queues                             create with the properties above
    PATCH /queues/{name}                      update TTL settings on an existing queue
    GET   /queues/{name}/subscriptions        which subscriptions it already has
    POST  /queues/{name}/subscriptions        add a missing one

The broker takes 1-2 minutes to boot in compose, so the first call is
retried for up to PROVISION_TIMEOUT_SECONDS before giving up.
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
    configure_logging,
    cowork_config,
    semp_config,
)
from kai_sim_cowork.services import QueueSpec, queue_specs
from kai_sim_cowork.topics import TopicSpace

log = logging.getLogger(__name__)


class ProvisionError(Exception):
    """A SEMP call failed; the message names the call, the broker's reason and the fix."""


def queue_body(spec: QueueSpec, max_ttl_seconds: int) -> dict[str, Any]:
    """The SEMP v2 queue object the simulation wants: exclusive, consumable, TTL-enforcing."""
    return {
        "queueName": spec.name,
        "accessType": "exclusive",
        "permission": "consume",
        "ingressEnabled": True,
        "egressEnabled": True,
        "respectTtlEnabled": True,
        "maxTtl": max_ttl_seconds,
    }


def ttl_body(max_ttl_seconds: int) -> dict[str, Any]:
    """The subset PATCHed onto a queue that already exists."""
    return {"respectTtlEnabled": True, "maxTtl": max_ttl_seconds}


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
        log.debug("SEMP %s %s %s", method, url, body or "")
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

    def wait_ready(self, timeout_seconds: float, poll_seconds: float = 5.0) -> None:
        """Block until GET on the VPN answers 200, or raise after `timeout_seconds`.

        Covers the broker's boot time in compose. Bad credentials fail fast.
        """
        deadline = time.monotonic() + timeout_seconds
        attempt = 0
        while True:
            attempt += 1
            try:
                response = self._call("GET", "")
            except ProvisionError as exc:
                reason = str(exc)  # connection refused etc.: the broker is still booting
            else:
                if response.status_code == 200:
                    log.info("SEMP ready at %s (VPN %s)", self._cfg.url, self._vpn)
                    return
                if response.status_code == 401:
                    self._check(response, "connecting to SEMP")  # wrong credentials: no retry
                reason = f"{response.status_code} {_semp_status(response)}"
            if time.monotonic() >= deadline:
                raise ProvisionError(
                    f"SEMP at {self._cfg.url} not ready after {timeout_seconds:.0f}s "
                    f"({reason}); is the broker up, and is SEMP_URL its management URL?"
                )
            log.info("waiting for SEMP at %s (attempt %d: %s)", self._cfg.url, attempt, reason)
            time.sleep(poll_seconds)

    def ensure_queue(self, spec: QueueSpec, max_ttl_seconds: int) -> str:
        """Create the queue, or update its TTL settings if it exists; 'created' or 'updated'."""
        path = f"/queues/{quote(spec.name, safe='')}"
        existing = self._call("GET", path)
        if existing.status_code == 200:
            updated = self._call("PATCH", path, ttl_body(max_ttl_seconds))
            self._check(updated, f"updating queue {spec.name}")
            return "updated"
        if _semp_status(existing) != "NOT_FOUND":
            self._check(existing, f"looking up queue {spec.name}")
        created = self._call("POST", "/queues", queue_body(spec, max_ttl_seconds))
        self._check(created, f"creating queue {spec.name}")
        return "created"

    def ensure_subscriptions(self, spec: QueueSpec) -> list[str]:
        """Add each subscription the queue does not have yet; returns the ones added."""
        path = f"/queues/{quote(spec.name, safe='')}/subscriptions"
        listing = self._call("GET", path)
        self._check(listing, f"listing subscriptions of {spec.name}")
        present = {item["subscriptionTopic"] for item in listing.json().get("data", [])}
        added: list[str] = []
        for topic in spec.subscriptions:
            if topic in present:
                continue
            response = self._call("POST", path, {"subscriptionTopic": topic})
            # A concurrent creator may have won the race; that is still success.
            ok = response.status_code in (200, 201)
            if not ok and _semp_status(response) != "ALREADY_EXISTS":
                self._check(response, f"adding subscription {topic} to {spec.name}")
            added.append(topic)
        return added


def provision(client: SempClient, specs: list[QueueSpec], max_ttl_seconds: int) -> None:
    """Make every spec exist with the TTL settings and its subscriptions."""
    for spec in specs:
        outcome = client.ensure_queue(spec, max_ttl_seconds)
        added = client.ensure_subscriptions(spec)
        log.info(
            "queue %s %s: exclusive, maxTtl=%ds, respectTtl=on; subscriptions %s (%s)",
            spec.name,
            outcome,
            max_ttl_seconds,
            list(spec.subscriptions),
            f"added {added}" if added else "already present",
        )


def main(argv: list[str] | None = None, transport: httpx.BaseTransport | None = None) -> int:
    """Process entry point (`kai-sim-provision`). Exits when done.

    `transport` is injectable so tests can serve a fake SEMP without a network.
    """
    parser = argparse.ArgumentParser(
        description="Create the simulation's queues on the broker over SEMP v2, with message TTL"
    )
    parser.parse_args(argv)
    configure_logging()

    try:
        cowork = cowork_config()
        semp = semp_config()
    except ConfigError as exc:
        log.error("%s", exc)
        return 2
    broker = broker_config()
    space = TopicSpace(cowork.environment, cowork.region)
    specs = queue_specs(space, broker.queue_prefix)
    log.info(
        "provisioning %d queues on VPN %s via %s (maxTtl=%ds): %s",
        len(specs),
        broker.vpn,
        semp.masked(),
        semp.max_ttl_seconds,
        [s.name for s in specs],
    )

    client = SempClient(semp, broker.vpn, transport=transport)
    try:
        client.wait_ready(semp.timeout_seconds)
        provision(client, specs, semp.max_ttl_seconds)
    except ProvisionError as exc:
        log.error("%s", exc)
        return 2
    finally:
        client.close()
    log.info("provisioning complete: %d queues ready", len(specs))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
