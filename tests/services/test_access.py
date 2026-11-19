"""How a principal becomes a broker identity: the username rule and the role profiles."""

from __future__ import annotations

import pytest

from kai_sim_cowork.services import access


def test_client_username_is_the_prefixed_principal():
    assert access.client_username("orchestrator") == "cowork-orchestrator"
    assert access.client_username("api-gateway") == "cowork-api-gateway"


def test_client_username_rejects_unsafe_and_overlong_names():
    with pytest.raises(ValueError, match="principal"):
        access.client_username("a/b")
    with pytest.raises(ValueError, match="limited to 32"):
        access.client_username("a" * 30)


@pytest.mark.parametrize(
    "publishes, consumes, expected",
    [
        (True, False, access.PROFILE_PUBLISH),
        (False, True, access.PROFILE_CONSUME),
        (True, True, access.PROFILE_PUBLISH_CONSUME),
    ],
)
def test_profile_follows_what_the_principal_does(publishes, consumes, expected):
    assert access.profile_for(publishes, consumes) is expected


def test_no_role_profile_shares_a_name_or_exceeds_the_brokers_limit():
    names = [p.name for p in access.CLIENT_PROFILES]
    assert len(set(names)) == 3
    assert all(len(name) <= access.MAX_PROFILE_NAME for name in names)
