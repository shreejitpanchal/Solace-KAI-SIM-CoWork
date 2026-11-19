import logging

import pytest

from kai_sim_cowork.config import (
    ConfigError,
    broker_config,
    configure_logging,
    cowork_config,
    publisher_config,
    semp_config,
)


def test_broker_defaults_apply_only_when_unset():
    cfg = broker_config({})
    assert cfg.host == "tcp://localhost:55555"
    assert cfg.queue_prefix == "q.cowork"
    assert cfg.create_queues is True
    overridden = broker_config({"SOLACE_HOST": "tcps://broker:55443", "SOLACE_PASSWORD": "s3cret"})
    assert overridden.host == "tcps://broker:55443"
    assert overridden.password == "s3cret"


def test_broker_masked_never_exposes_password():
    masked = broker_config({"SOLACE_PASSWORD": "s3cret"}).masked()
    assert masked["password"] == "***"
    assert "s3cret" not in str(masked)


@pytest.mark.parametrize(
    "raw, expected",
    [("false", False), ("0", False), ("No", False), ("true", True), ("ON", True), ("", True)],
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
    assert cfg.timeout_seconds == 180.0
    assert semp_config({"SEMP_PASSWORD": "s3cret"}).masked()["password"] == "***"


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
