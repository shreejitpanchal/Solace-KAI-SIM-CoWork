"""Environment-driven settings. A default applies only when its variable is unset."""

from __future__ import annotations

import logging
import os
from collections.abc import Mapping
from dataclasses import dataclass


class ConfigError(Exception):
    """A setting is present but unusable; the message names the variable and the fix."""


# --- broker ------------------------------------------------------------------


@dataclass(frozen=True)
class BrokerConfig:
    host: str
    vpn: str
    username: str
    password: str
    queue_prefix: str  # consumer queues are named <prefix>.<environment>.<region>.<service>
    # False (default): bind to the provisioned queue and touch nothing else.
    # True: create a missing queue and add its subscriptions on bind, which a
    # least-privilege client profile and ACL profile do not allow.
    create_queues: bool

    def masked(self) -> dict[str, str]:
        """Safe to log: the password is never echoed."""
        return {
            "host": self.host,
            "vpn": self.vpn,
            "username": self.username,
            "password": "***",
            "queue_prefix": self.queue_prefix,
            "create_queues": str(self.create_queues),
        }


_TRUE = {"1", "true", "yes", "on"}
_FALSE = {"0", "false", "no", "off"}


def _env_bool(env: Mapping[str, str], name: str, default: bool) -> bool:
    raw = env.get(name)
    if raw is None or raw == "":
        return default
    value = raw.strip().lower()
    if value in _TRUE:
        return True
    if value in _FALSE:
        return False
    raise ConfigError(f"{name} must be true or false, got {raw!r}")


# The broker's limit for a client username's password.
MAX_PASSWORD_LENGTH = 128


def password_variable(principal: str) -> str:
    """The variable holding a principal's password: api-gateway -> SOLACE_PASSWORD_API_GATEWAY."""
    return "SOLACE_PASSWORD_" + principal.upper().replace("-", "_")


def _check_password(value: str, variable: str) -> str:
    if not value or len(value) > MAX_PASSWORD_LENGTH:
        raise ConfigError(
            f"{variable} must be set to a password of 1-{MAX_PASSWORD_LENGTH} characters "
            "(see .env.example)"
        )
    return value


def client_passwords(
    principals: tuple[str, ...], env: Mapping[str, str] | None = None
) -> dict[str, str]:
    """Every principal's password, for the provisioner that creates their client usernames.

    Fails naming every missing variable at once, before any broker call.
    """
    env = os.environ if env is None else env
    missing = [password_variable(p) for p in principals if not env.get(password_variable(p))]
    if missing:
        raise ConfigError(
            f"set {', '.join(missing)}: each service connects as its own client username "
            "and needs its own password (see .env.example)"
        )
    return {
        p: _check_password(env[password_variable(p)], password_variable(p)) for p in principals
    }


def broker_config(
    env: Mapping[str, str] | None = None,
    *,
    principal: str | None = None,
    username: str | None = None,
) -> BrokerConfig:
    """Connection settings; defaults match the broker container in docker-compose.yml.

    A service passes its `principal` and the `username` the registry derives
    for it (services.client_username): it then connects as that dedicated
    client username, with the password from SOLACE_PASSWORD (what compose
    hands each container) or else SOLACE_PASSWORD_<PRINCIPAL> (a host run
    with the whole .env). Without a principal the broker's built-in `default`
    username is used; only the provisioner (which never opens a messaging
    connection) and the opt-in integration tests do that.
    """
    env = os.environ if env is None else env
    if principal is None:
        user = env.get("SOLACE_USERNAME", "default")
        password = env.get("SOLACE_PASSWORD", "default")
    else:
        if not username:
            raise ConfigError(f"no client username given for principal {principal!r}")
        user = username
        variable = "SOLACE_PASSWORD" if env.get("SOLACE_PASSWORD") else password_variable(principal)
        if not env.get(variable):
            raise ConfigError(
                f"no password for client username {user!r}: set {password_variable(principal)} "
                "(or SOLACE_PASSWORD for this process alone); see .env.example"
            )
        password = _check_password(env[variable], variable)
    return BrokerConfig(
        host=env.get("SOLACE_HOST", "tcp://localhost:55555"),
        vpn=env.get("SOLACE_VPN", "default"),
        username=user,
        password=password,
        queue_prefix=env.get("SOLACE_QUEUE_PREFIX", "q.cowork"),
        create_queues=_env_bool(env, "SOLACE_CREATE_QUEUES", False),
    )


# --- cowork topic space ------------------------------------------------------


@dataclass(frozen=True)
class CoworkConfig:
    """The two topic levels every Cowork topic starts with, plus the simulated tenant."""

    environment: str  # cowork/{environment}/...   e.g. dev, staging, prod
    region: str  # cowork/{environment}/{region}/...   e.g. apse1
    tenant_id: str  # tenantId claim the simulated gateway puts in every envelope


def cowork_config(env: Mapping[str, str] | None = None) -> CoworkConfig:
    """COWORK_ENVIRONMENT (dev), COWORK_REGION (apse1), COWORK_TENANT_ID (acme).

    Values become topic levels, so they are validated with topics.validate_level.
    """
    from kai_sim_cowork.topics import validate_level  # local: topics does not import config

    env = os.environ if env is None else env
    try:
        return CoworkConfig(
            environment=validate_level(env.get("COWORK_ENVIRONMENT", "dev"), "COWORK_ENVIRONMENT"),
            region=validate_level(env.get("COWORK_REGION", "apse1"), "COWORK_REGION"),
            tenant_id=validate_level(env.get("COWORK_TENANT_ID", "acme"), "COWORK_TENANT_ID"),
        )
    except ValueError as exc:
        raise ConfigError(str(exc)) from exc


# --- publisher ---------------------------------------------------------------


@dataclass(frozen=True)
class PublisherConfig:
    interval_seconds: float  # pause between tasks; 0 publishes as fast as the broker acks
    count: int  # tasks to publish before exiting; 0 means until stopped


def publisher_config(env: Mapping[str, str] | None = None) -> PublisherConfig:
    """PUBLISH_INTERVAL_SECONDS (default 5) and PUBLISH_COUNT (default 0 = run until Ctrl-C)."""
    env = os.environ if env is None else env
    raw_interval = env.get("PUBLISH_INTERVAL_SECONDS", "5")
    try:
        interval = float(raw_interval)
    except ValueError as exc:
        raise ConfigError(
            f"PUBLISH_INTERVAL_SECONDS must be a number of seconds, got {raw_interval!r}; "
            "unset it for the default of 5"
        ) from exc
    if interval < 0:
        raise ConfigError(f"PUBLISH_INTERVAL_SECONDS must not be negative, got {interval}")
    raw_count = env.get("PUBLISH_COUNT", "0")
    try:
        count = int(raw_count)
    except ValueError as exc:
        raise ConfigError(
            f"PUBLISH_COUNT must be an integer (0 = until stopped), got {raw_count!r}"
        ) from exc
    if count < 0:
        raise ConfigError(f"PUBLISH_COUNT must not be negative, got {count}")
    return PublisherConfig(interval_seconds=interval, count=count)


# --- SEMP (queue provisioning) -----------------------------------------------


@dataclass(frozen=True)
class SempConfig:
    """Broker management (SEMP v2) settings used by kai-sim-provision to create the queues."""

    url: str  # base URL of the broker's SEMP endpoint, e.g. http://localhost:8080
    username: str
    password: str
    max_ttl_seconds: int  # message expiry applied to every queue (0 = no limit)
    max_redelivery_count: int  # redeliveries before a message goes to the DMQ (0 = forever)
    timeout_seconds: float  # how long to wait for SEMP to answer while the broker boots

    def masked(self) -> dict[str, str]:
        """Safe to log: the password is never echoed."""
        return {
            "url": self.url,
            "username": self.username,
            "password": "***",
            "max_ttl_seconds": str(self.max_ttl_seconds),
            "max_redelivery_count": str(self.max_redelivery_count),
            "timeout_seconds": str(self.timeout_seconds),
        }


# The broker's own limit for a queue's maxRedeliveryCount.
MAX_REDELIVERY_LIMIT = 255


def semp_config(env: Mapping[str, str] | None = None) -> SempConfig:
    """SEMP_URL / SEMP_USERNAME / SEMP_PASSWORD, QUEUE_MAX_TTL_SECONDS (default 86400 = 24 h),
    QUEUE_MAX_REDELIVERY_COUNT (default 3) and PROVISION_TIMEOUT_SECONDS (default 180)."""
    env = os.environ if env is None else env
    raw_ttl = env.get("QUEUE_MAX_TTL_SECONDS", "86400")
    try:
        ttl = int(raw_ttl)
    except ValueError as exc:
        raise ConfigError(
            f"QUEUE_MAX_TTL_SECONDS must be an integer number of seconds, got {raw_ttl!r}; "
            "unset it for the default of 86400 (24 hours)"
        ) from exc
    if ttl < 0:
        raise ConfigError(f"QUEUE_MAX_TTL_SECONDS must not be negative, got {ttl}")
    raw_redelivery = env.get("QUEUE_MAX_REDELIVERY_COUNT", "3")
    try:
        redelivery = int(raw_redelivery)
    except ValueError as exc:
        raise ConfigError(
            f"QUEUE_MAX_REDELIVERY_COUNT must be an integer, got {raw_redelivery!r}; "
            "unset it for the default of 3"
        ) from exc
    if not 0 <= redelivery <= MAX_REDELIVERY_LIMIT:
        raise ConfigError(
            f"QUEUE_MAX_REDELIVERY_COUNT must be between 0 (redeliver forever) and "
            f"{MAX_REDELIVERY_LIMIT}, got {redelivery}"
        )
    raw_timeout = env.get("PROVISION_TIMEOUT_SECONDS", "180")
    try:
        timeout = float(raw_timeout)
    except ValueError as exc:
        raise ConfigError(
            f"PROVISION_TIMEOUT_SECONDS must be a number of seconds, got {raw_timeout!r}; "
            "unset it for the default of 180"
        ) from exc
    if timeout <= 0:
        raise ConfigError(f"PROVISION_TIMEOUT_SECONDS must be positive, got {timeout}")
    return SempConfig(
        url=env.get("SEMP_URL", "http://localhost:8080"),
        username=env.get("SEMP_USERNAME", "admin"),
        password=env.get("SEMP_PASSWORD", "admin"),
        max_ttl_seconds=ttl,
        max_redelivery_count=redelivery,
        timeout_seconds=timeout,
    )


# --- logging -----------------------------------------------------------------


def configure_logging(level: int | None = None, env: Mapping[str, str] | None = None) -> None:
    """Root logger to stderr. LOG_LEVEL=DEBUG shows every broker call and envelope field.

    INFO (the default) logs each activity once: task published, event
    received on a queue, reaction published, event refused.
    """
    env = os.environ if env is None else env
    if level is None:
        name = env.get("LOG_LEVEL", "INFO").strip().upper()
        if name not in logging.getLevelNamesMapping():
            raise ConfigError(
                f"LOG_LEVEL must be one of DEBUG, INFO, WARNING, ERROR; got {name!r}"
            )
        level = logging.getLevelNamesMapping()[name]
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )
    # The Solace SDK logs a lot at INFO; keep it to warnings unless DEBUG is asked for.
    sdk_level = logging.DEBUG if level <= logging.DEBUG else logging.WARNING
    logging.getLogger("solace").setLevel(sdk_level)
