"""Naming and roles for access control: how a principal becomes a broker identity.

Where it sits in the flow
-------------------------
    access.py --> services/__init__.py (access_specs) --> provision.py (creates them)
              --> consumer.py and each service's main.py (connect as that username)

One rule, applied everywhere: a principal (the gateway or a service) is the
client username `cowork-<principal>`, with an ACL profile of the same name
and one of three client profiles chosen by what it does, not by who it is.
"""

from __future__ import annotations

from kai_sim_cowork.services.base import ClientProfileSpec
from kai_sim_cowork.topics import validate_level

# Client usernames and ACL profiles are named <prefix>-<principal>.
ACCESS_PREFIX = "cowork"
# The broker's limit for ACL profile and client profile names.
MAX_PROFILE_NAME = 32

# Client profiles by role, not by service: what differs between services is
# which topics they may use (the ACL profile), not what kind of client they are.
PROFILE_PUBLISH = ClientProfileSpec(f"{ACCESS_PREFIX}-publish", True, False)
PROFILE_CONSUME = ClientProfileSpec(f"{ACCESS_PREFIX}-consume", False, True)
PROFILE_PUBLISH_CONSUME = ClientProfileSpec(f"{ACCESS_PREFIX}-publish-consume", True, True)
CLIENT_PROFILES: tuple[ClientProfileSpec, ...] = (
    PROFILE_PUBLISH,
    PROFILE_CONSUME,
    PROFILE_PUBLISH_CONSUME,
)


def client_username(principal: str) -> str:
    """cowork-<principal>: the client username, and the name of its ACL profile."""
    name = f"{ACCESS_PREFIX}-{validate_level(principal, 'principal')}"
    if len(name) > MAX_PROFILE_NAME:
        raise ValueError(
            f"{name!r} is {len(name)} characters; ACL profile names are limited to "
            f"{MAX_PROFILE_NAME}, so shorten the service name"
        )
    return name


def profile_for(publishes: bool, consumes: bool) -> ClientProfileSpec:
    """The role profile for a principal that publishes and/or consumes."""
    if publishes and consumes:
        return PROFILE_PUBLISH_CONSUME
    return PROFILE_PUBLISH if publishes else PROFILE_CONSUME
