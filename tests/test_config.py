import logging

import pytest

from kai_sim_cowork.config import (
    ConfigError,
    broker_config,
    client_passwords,
    configure_logging,
    cowork_config,
    password_variable,
    publisher_config,
    semp_config,
)


def test_broker_defaults_apply_only_when_unset():
    cfg = broker_config({})
    assert cfg.host == "tcp://localhost:55555"
    assert cfg.queue_prefix == "q.cowork"
    assert cfg.create_queues is False  # bind only; the provisioner owns the queues
    overridden = broker_config({"SOLACE_HOST": "tcps://broker:55443", "SOLACE_PASSWORD": "s3cret"})
    assert overridden.host == "tcps://broker:55443"
    assert overridden.password == "s3cret"


def test_password_variable_is_derived_from_the_principal():
    assert password_variable("api-gateway") == "SOLACE_PASSWORD_API_GATEWAY"
    assert password_variable("orchestrator") == "SOLACE_PASSWORD_ORCHESTRATOR"


def test_a_service_connects_as_its_own_username_with_its_own_password():
    env = {"SOLACE_PASSWORD_ORCHESTRATOR": "orc-pw", "SOLACE_PASSWORD_AUDIT_PIPELINE": "aud-pw"}
    cfg = broker_config(env, principal="orchestrator", username="cowork-orchestrator")
    assert (cfg.username, cfg.password) == ("cowork-orchestrator", "orc-pw")
    # SOLACE_USERNAME cannot redirect a service onto another identity
    env["SOLACE_USERNAME"] = "default"
    cfg = broker_config(env, principal="orchestrator", username="cowork-orchestrator")
    assert cfg.username == "cowork-orchestrator"


def test_solace_password_wins_for_a_single_process():
    """What compose does: each container is handed only its own password."""
    env = {"SOLACE_PASSWORD": "mine", "SOLACE_PASSWORD_ORCHESTRATOR": "from-dotenv"}
    cfg = broker_config(env, principal="orchestrator", username="cowork-orchestrator")
    assert cfg.password == "mine"


def test_a_service_without_a_password_is_refused_with_the_variable_to_set():
    with pytest.raises(ConfigError, match="SOLACE_PASSWORD_ORCHESTRATOR") as exc:
        broker_config({}, principal="orchestrator", username="cowork-orchestrator")
    assert "cowork-orchestrator" in str(exc.value)
    with pytest.raises(ConfigError, match="no client username"):
        broker_config({"SOLACE_PASSWORD": "x"}, principal="orchestrator")


def test_an_overlong_password_is_refused_without_echoing_it():
    secret = "x" * 129
    with pytest.raises(ConfigError, match="SOLACE_PASSWORD_ORCHESTRATOR") as exc:
        broker_config(
            {"SOLACE_PASSWORD_ORCHESTRATOR": secret},
            principal="orchestrator",
            username="cowork-orchestrator",
        )
    assert secret not in str(exc.value)


def test_client_passwords_reads_one_per_principal():
    env = {"SOLACE_PASSWORD_API_GATEWAY": "a", "SOLACE_PASSWORD_ORCHESTRATOR": "b"}
    assert client_passwords(("api-gateway", "orchestrator"), env) == {
        "api-gateway": "a",
        "orchestrator": "b",
    }


def test_client_passwords_names_every_missing_variable():
    with pytest.raises(ConfigError) as exc:
        client_passwords(("api-gateway", "orchestrator"), {"SOLACE_PASSWORD_API_GATEWAY": ""})
    assert "SOLACE_PASSWORD_API_GATEWAY" in str(exc.value)
    assert "SOLACE_PASSWORD_ORCHESTRATOR" in str(exc.value)
    with pytest.raises(ConfigError, match="1-128 characters"):
        client_passwords(("orchestrator",), {"SOLACE_PASSWORD_ORCHESTRATOR": "x" * 129})


def test_broker_masked_never_exposes_password():
    masked = broker_config({"SOLACE_PASSWORD": "s3cret"}).masked()
    assert masked["password"] == "***"
    assert "s3cret" not in str(masked)


@pytest.mark.parametrize(
    "raw, expected",
    [("false", False), ("0", False), ("No", False), ("true", True), ("ON", True), ("", False)],
)
def test_create_queues_parses_booleans(raw, expected):
    assert broker_config({"SOLACE_CREATE_QUEUES": raw}).create_queues is expected


def test_create_queues_rejects_garbage():
    with pytest.raises(ConfigError, match="SOLACE_CREATE_QUEUES"):
        broker_config({"SOLACE_CREATE_QUEUES": "maybe"})


def test_cowork_defaults_and_overrides():
    cfg = cowork_config({})
    assert (cfg.environment, cfg.region, cfg.tenant_id) == ("dev", "apse1", "acme")
    cfg = cowork_config({"COWORK_ENVIRONMENT": "staging", "COWORK_REGION": "euw1"})
    assert (cfg.environment, cfg.region) == ("staging", "euw1")


@pytest.mark.parametrize(
    "var", ["COWORK_ENVIRONMENT", "COWORK_REGION", "COWORK_TENANT_ID"]
)
def test_cowork_levels_are_validated(var):
    with pytest.raises(ConfigError, match=var):
        cowork_config({var: "a/b"})


def test_publisher_defaults_and_errors():
    cfg = publisher_config({})
    assert (cfg.interval_seconds, cfg.count) == (5.0, 0)
    assert publisher_config({"PUBLISH_INTERVAL_SECONDS": "0.5", "PUBLISH_COUNT": "3"}).count == 3
    with pytest.raises(ConfigError, match="PUBLISH_INTERVAL_SECONDS"):
        publisher_config({"PUBLISH_INTERVAL_SECONDS": "-1"})
    with pytest.raises(ConfigError, match="PUBLISH_COUNT"):
        publisher_config({"PUBLISH_COUNT": "many"})


def test_semp_defaults_give_a_24_hour_queue_ttl():
    cfg = semp_config({})
    assert cfg.url == "http://localhost:8080"
    assert cfg.max_ttl_seconds == 86400
    assert cfg.max_redelivery_count == 3
    assert cfg.timeout_seconds == 180.0
    assert semp_config({"SEMP_PASSWORD": "s3cret"}).masked()["password"] == "***"


def test_redelivery_count_is_read_and_logged():
    cfg = semp_config({"QUEUE_MAX_REDELIVERY_COUNT": "0"})  # 0 = redeliver forever
    assert cfg.max_redelivery_count == 0
    assert cfg.masked()["max_redelivery_count"] == "0"
    assert semp_config({"QUEUE_MAX_REDELIVERY_COUNT": "255"}).max_redelivery_count == 255


@pytest.mark.parametrize("raw", ["thrice", "-1", "256"])
def test_redelivery_count_rejects_bad_values(raw):
    with pytest.raises(ConfigError, match="QUEUE_MAX_REDELIVERY_COUNT"):
        semp_config({"QUEUE_MAX_REDELIVERY_COUNT": raw})


@pytest.mark.parametrize("raw", ["day", "-1"])
def test_queue_ttl_rejects_bad_values(raw):
    with pytest.raises(ConfigError, match="QUEUE_MAX_TTL_SECONDS"):
        semp_config({"QUEUE_MAX_TTL_SECONDS": raw})


@pytest.mark.parametrize("raw", ["soon", "0"])
def test_provision_timeout_rejects_bad_values(raw):
    with pytest.raises(ConfigError, match="PROVISION_TIMEOUT_SECONDS"):
        semp_config({"PROVISION_TIMEOUT_SECONDS": raw})


def test_logging_level_comes_from_env_and_is_validated():
    configure_logging(env={"LOG_LEVEL": "debug"})
    assert logging.getLogger().level == logging.DEBUG
    assert logging.getLogger("solace").level == logging.DEBUG
    configure_logging(env={"LOG_LEVEL": "INFO"})
    assert logging.getLogger("solace").level == logging.WARNING
    with pytest.raises(ConfigError, match="LOG_LEVEL"):
        configure_logging(env={"LOG_LEVEL": "LOUD"})
