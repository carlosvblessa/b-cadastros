from __future__ import annotations

import os

import pytest

from bcadastros_etl.config import (
    DEFAULT_WORKERS,
    ConfigurationError,
    CouchDBConfig,
)

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
    "COUCH_DB_CNPJ",
    "COUCH_DB_CPF",
    "COUCH_DB_SN",
    "COUCHDB_TIMEOUT",
    "COUCHDB_PAGE_LIMIT",
    "COUCHDB_MAX_ATTEMPTS",
    "COUCHDB_BACKOFF_FACTOR",
    "WORKERS",
    "COUCHDB_WORKERS",
    "COUCHDB_VERIFY_SSL",
    "COUCH_IDX_RESP_DDOC",
    "COUCH_IDX_RESP_NAME",
    "COUCH_CPF_ID_PREFIX",
    "CACHE_ROOT_MAXSIZE",
    "CACHE_SIMPLES_MAXSIZE",
    "CACHE_PERSON_MAXSIZE",
    "CACHE_ACCOUNTANT_MAXSIZE",
    "CACHE_RESPONSIBLE_COUNT_MAXSIZE",
    "LOG_LEVEL",
)


def _clean_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in ENV_NAMES:
        monkeypatch.delenv(name, raising=False)


def _set_minimal_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("COUCHDB_URL", "http://couch.test:5984")
    monkeypatch.setenv("COUCHDB_USER", "etl")
    monkeypatch.setenv("COUCHDB_PASSWORD", "secret")


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

    config = CouchDBConfig.from_env()
    assert config.base_url == "https://couch.test:6984"
    assert config.timeout == 12.5
    assert config.max_attempts == 4
    assert config.verify_ssl is False
    assert config.log_level == "DEBUG"
    assert config.workers == DEFAULT_WORKERS == 4


def test_config_keeps_connection_legacy_aliases(monkeypatch: pytest.MonkeyPatch) -> None:
    _clean_environment(monkeypatch)
    monkeypatch.setenv("COUCH_SCHEME", "http")
    monkeypatch.setenv("COUCH_HOST", "legacy.test")
    monkeypatch.setenv("COUCH_PORT", "15984")
    monkeypatch.setenv("COUCH_USER", "etl")
    monkeypatch.setenv("COUCH_PASS", "secret")

    config = CouchDBConfig.from_env()
    assert config.base_url == "http://legacy.test:15984"
    assert config.cnpj_database == "chcnpj_bcadastros_replica"


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("COUCHDB_TIMEOUT", "zero"),
        ("COUCHDB_TIMEOUT", "0"),
        ("COUCHDB_TIMEOUT", "nan"),
        ("COUCHDB_TIMEOUT", "inf"),
        ("COUCHDB_PAGE_LIMIT", "0"),
        ("COUCHDB_MAX_ATTEMPTS", "-1"),
        ("COUCHDB_BACKOFF_FACTOR", "-0.1"),
        ("COUCHDB_VERIFY_SSL", "talvez"),
        ("WORKERS", "true"),
        ("WORKERS", "0"),
        ("WORKERS", "-2"),
        ("LOG_LEVEL", "TRACE"),
    ],
)
def test_config_rejects_invalid_values(
    monkeypatch: pytest.MonkeyPatch,
    name: str,
    value: str,
) -> None:
    _clean_environment(monkeypatch)
    _set_minimal_environment(monkeypatch)
    monkeypatch.setenv(name, value)
    with pytest.raises(ConfigurationError, match=name):
        CouchDBConfig.from_env()


@pytest.mark.parametrize("port", ["0", "65536", "texto"])
def test_config_validates_component_port(
    monkeypatch: pytest.MonkeyPatch,
    port: str,
) -> None:
    _clean_environment(monkeypatch)
    monkeypatch.setenv("COUCHDB_HOST", "couch.test")
    monkeypatch.setenv("COUCHDB_PORT", port)
    monkeypatch.setenv("COUCHDB_USER", "etl")
    monkeypatch.setenv("COUCHDB_PASSWORD", "secret")
    with pytest.raises(ConfigurationError, match="COUCHDB_PORT"):
        CouchDBConfig.from_env()


def test_config_validates_port_inside_url(monkeypatch: pytest.MonkeyPatch) -> None:
    _clean_environment(monkeypatch)
    _set_minimal_environment(monkeypatch)
    monkeypatch.setenv("COUCHDB_URL", "http://couch.test:70000")
    with pytest.raises(ConfigurationError, match="porta"):
        CouchDBConfig.from_env()


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("COUCHDB_HOST", "   "),
        ("COUCHDB_USER", "   "),
        ("COUCHDB_PASSWORD", "   "),
        ("COUCH_DB_CNPJ", "   "),
        ("COUCH_IDX_RESP_NAME", "   "),
    ],
)
def test_config_rejects_required_whitespace(
    monkeypatch: pytest.MonkeyPatch,
    name: str,
    value: str,
) -> None:
    _clean_environment(monkeypatch)
    if name != "COUCHDB_HOST":
        _set_minimal_environment(monkeypatch)
    else:
        monkeypatch.setenv("COUCHDB_USER", "etl")
        monkeypatch.setenv("COUCHDB_PASSWORD", "secret")
    monkeypatch.setenv(name, value)
    with pytest.raises(ConfigurationError, match=name):
        CouchDBConfig.from_env()


@pytest.mark.parametrize(
    "name",
    [
        "CACHE_ROOT_MAXSIZE",
        "CACHE_SIMPLES_MAXSIZE",
        "CACHE_PERSON_MAXSIZE",
        "CACHE_ACCOUNTANT_MAXSIZE",
        "CACHE_RESPONSIBLE_COUNT_MAXSIZE",
    ],
)
def test_config_accepts_zero_and_rejects_negative_cache_limits(
    monkeypatch: pytest.MonkeyPatch,
    name: str,
) -> None:
    _clean_environment(monkeypatch)
    _set_minimal_environment(monkeypatch)
    monkeypatch.setenv(name, "0")
    config = CouchDBConfig.from_env()
    assert 0 in {
        config.cache_root_maxsize,
        config.cache_simples_maxsize,
        config.cache_person_maxsize,
        config.cache_accountant_maxsize,
        config.cache_responsible_count_maxsize,
    }

    monkeypatch.setenv(name, "-1")
    with pytest.raises(ConfigurationError, match=name):
        CouchDBConfig.from_env()


def test_workers_environment_and_default_precedence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clean_environment(monkeypatch)
    _set_minimal_environment(monkeypatch)
    assert CouchDBConfig.from_env().workers == 4

    monkeypatch.setenv("WORKERS", "8")
    assert CouchDBConfig.from_env().workers == 8


def test_config_requires_host_and_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    _clean_environment(monkeypatch)
    with pytest.raises(ConfigurationError, match="COUCHDB_HOST"):
        CouchDBConfig.from_env()

    monkeypatch.setenv("COUCHDB_URL", "http://couch.test:5984")
    with pytest.raises(ConfigurationError, match="COUCHDB_USER"):
        CouchDBConfig.from_env()


def test_config_rejects_credentials_inside_url(monkeypatch: pytest.MonkeyPatch) -> None:
    _clean_environment(monkeypatch)
    monkeypatch.setenv("COUCHDB_URL", "http://etl:secret@couch.test:5984")
    monkeypatch.setenv("COUCHDB_USER", "etl")
    monkeypatch.setenv("COUCHDB_PASSWORD", "secret")
    with pytest.raises(ConfigurationError, match="nao inclua credenciais"):
        CouchDBConfig.from_env()


def test_config_loads_only_explicit_env_file(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    _clean_environment(monkeypatch)
    env_file = tmp_path / "config.env"
    env_file.write_text(
        "COUCHDB_URL=http://from-file.test:5984\n"
        "COUCHDB_USER=etl\n"
        "COUCHDB_PASSWORD=secret\n"
        "WORKERS=8\n",
        encoding="utf-8",
    )

    config = CouchDBConfig.from_env(env_file)
    assert config.base_url == "http://from-file.test:5984"
    assert config.workers == 8
    for name in ("COUCHDB_URL", "COUCHDB_USER", "COUCHDB_PASSWORD", "WORKERS"):
        os.environ.pop(name, None)

    with pytest.raises(ConfigurationError, match="nao encontrado"):
        CouchDBConfig.from_env(tmp_path / "missing.env")
