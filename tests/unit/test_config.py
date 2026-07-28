from __future__ import annotations

import pytest

from bcadastros_etl.config import ConfigurationError, CouchDBConfig

ENV_NAMES = (
    "COUCHDB_URL",
    "COUCHDB_SCHEME",
    "COUCH_SCHEME",
    "COUCHDB_HOST",
    "COUCH_HOST",
    "COUCHDB_PORT",
    "COUCH_PORT",
    "COUCHDB_USER_ETL",
    "COUCHDB_USER",
    "COUCH_USER",
    "COUCHDB_PASSWORD_ETL",
    "COUCHDB_PASSWORD",
    "COUCH_PASS",
    "COUCHDB_TIMEOUT",
    "COUCHDB_MAX_ATTEMPTS",
    "COUCHDB_VERIFY_SSL",
    "LOG_LEVEL",
)


def _clean_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in ENV_NAMES:
        monkeypatch.delenv(name, raising=False)


def test_config_accepts_base_url_and_controlled_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clean_environment(monkeypatch)
    monkeypatch.setenv("COUCHDB_URL", "https://couch.test:6984/")
    monkeypatch.setenv("COUCHDB_USER", "etl")
    monkeypatch.setenv("COUCHDB_PASSWORD", "secret")
    monkeypatch.setenv("COUCHDB_TIMEOUT", "12.5")
    monkeypatch.setenv("COUCHDB_MAX_ATTEMPTS", "4")
    monkeypatch.setenv("COUCHDB_VERIFY_SSL", "false")
    monkeypatch.setenv("LOG_LEVEL", "debug")

    config = CouchDBConfig.from_env(dotenv_path="/arquivo/inexistente")
    assert config.base_url == "https://couch.test:6984"
    assert config.timeout == 12.5
    assert config.max_attempts == 4
    assert config.verify_ssl is False
    assert config.log_level == "DEBUG"
    assert config.workers == 4


def test_config_keeps_legacy_aliases(monkeypatch: pytest.MonkeyPatch) -> None:
    _clean_environment(monkeypatch)
    monkeypatch.setenv("COUCH_SCHEME", "http")
    monkeypatch.setenv("COUCH_HOST", "legacy.test")
    monkeypatch.setenv("COUCH_PORT", "15984")
    monkeypatch.setenv("COUCH_USER", "etl")
    monkeypatch.setenv("COUCH_PASS", "secret")

    config = CouchDBConfig.from_env(dotenv_path="/arquivo/inexistente")
    assert config.base_url == "http://legacy.test:15984"
    assert config.cnpj_database == "chcnpj_bcadastros_replica"


@pytest.mark.parametrize(
    ("name", "value", "message"),
    [
        ("COUCHDB_TIMEOUT", "zero", "COUCHDB_TIMEOUT"),
        ("COUCHDB_MAX_ATTEMPTS", "0", "COUCHDB_MAX_ATTEMPTS"),
        ("COUCHDB_VERIFY_SSL", "talvez", "COUCHDB_VERIFY_SSL"),
        ("LOG_LEVEL", "TRACE", "LOG_LEVEL"),
    ],
)
def test_config_rejects_invalid_values(
    monkeypatch: pytest.MonkeyPatch,
    name: str,
    value: str,
    message: str,
) -> None:
    _clean_environment(monkeypatch)
    monkeypatch.setenv("COUCHDB_URL", "http://couch.test:5984")
    monkeypatch.setenv("COUCHDB_USER", "etl")
    monkeypatch.setenv("COUCHDB_PASSWORD", "secret")
    monkeypatch.setenv(name, value)
    with pytest.raises(ConfigurationError, match=message):
        CouchDBConfig.from_env(dotenv_path="/arquivo/inexistente")


def test_config_requires_host_and_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    _clean_environment(monkeypatch)
    with pytest.raises(ConfigurationError, match="COUCHDB_HOST"):
        CouchDBConfig.from_env(dotenv_path="/arquivo/inexistente")

    monkeypatch.setenv("COUCHDB_URL", "http://couch.test:5984")
    with pytest.raises(ConfigurationError, match="COUCHDB_USER"):
        CouchDBConfig.from_env(dotenv_path="/arquivo/inexistente")


def test_config_rejects_credentials_inside_url(monkeypatch: pytest.MonkeyPatch) -> None:
    _clean_environment(monkeypatch)
    monkeypatch.setenv("COUCHDB_URL", "http://etl:secret@couch.test:5984")
    monkeypatch.setenv("COUCHDB_USER", "etl")
    monkeypatch.setenv("COUCHDB_PASSWORD", "secret")
    with pytest.raises(ConfigurationError, match="nao inclua credenciais"):
        CouchDBConfig.from_env(dotenv_path="/arquivo/inexistente")
