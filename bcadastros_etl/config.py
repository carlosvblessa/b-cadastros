"""Configuracao por ambiente, compativel com os nomes usados pelos scripts R."""

from __future__ import annotations

import os
from dataclasses import dataclass
from urllib.parse import urlparse

from dotenv import load_dotenv


class ConfigurationError(ValueError):
    """Configuracao ausente ou invalida."""


def _first_env(*names: str, default: str = "") -> str:
    for name in names:
        value = os.getenv(name)
        if value is not None and value != "":
            return value
    return default


def _positive_int(name: str, value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise ConfigurationError(f"{name} deve ser inteiro positivo") from exc
    if parsed <= 0:
        raise ConfigurationError(f"{name} deve ser inteiro positivo")
    return parsed


def _positive_float(name: str, value: str, *, allow_zero: bool = False) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise ConfigurationError(f"{name} deve ser numerico") from exc
    if parsed < 0 or (parsed == 0 and not allow_zero):
        qualifier = "nao negativo" if allow_zero else "positivo"
        raise ConfigurationError(f"{name} deve ser {qualifier}")
    return parsed


def _boolean(name: str, value: str) -> bool:
    normalized = value.strip().lower()
    if normalized in {"1", "true", "t", "yes", "y", "sim", "s"}:
        return True
    if normalized in {"0", "false", "f", "no", "n", "nao", "não"}:
        return False
    raise ConfigurationError(f"{name} deve ser true ou false")


@dataclass(frozen=True, slots=True)
class CouchDBConfig:
    base_url: str
    username: str
    password: str
    cnpj_database: str
    cpf_database: str
    simples_database: str
    timeout: float
    page_limit: int
    max_attempts: int
    backoff_factor: float
    workers: int
    verify_ssl: bool
    responsible_index_ddoc: str
    responsible_index_name: str
    cpf_id_prefix: str
    log_level: str

    @classmethod
    def from_env(cls, dotenv_path: str = ".env") -> CouchDBConfig:
        load_dotenv(dotenv_path=dotenv_path, override=False)

        base_url = _first_env("COUCHDB_URL")
        if not base_url:
            scheme = _first_env("COUCHDB_SCHEME", "COUCH_SCHEME", default="http")
            host = _first_env("COUCHDB_HOST", "COUCH_HOST")
            port = _first_env("COUCHDB_PORT", "COUCH_PORT", default="5984")
            if not host:
                raise ConfigurationError("configure COUCHDB_URL ou COUCHDB_HOST")
            base_url = f"{scheme}://{host}:{port}"

        base_url = base_url.rstrip("/")
        parsed_url = urlparse(base_url)
        if parsed_url.scheme not in {"http", "https"} or not parsed_url.netloc:
            raise ConfigurationError("COUCHDB_URL deve ser uma URL HTTP(S) valida")
        if parsed_url.username or parsed_url.password:
            raise ConfigurationError(
                "nao inclua credenciais em COUCHDB_URL; use COUCHDB_USER e COUCHDB_PASSWORD"
            )

        username = _first_env("COUCHDB_USER_ETL", "COUCHDB_USER", "COUCH_USER")
        password = _first_env("COUCHDB_PASSWORD_ETL", "COUCHDB_PASSWORD", "COUCH_PASS")
        if not username:
            raise ConfigurationError("COUCHDB_USER nao configurado")
        if not password:
            raise ConfigurationError("COUCHDB_PASSWORD nao configurado")

        log_level = _first_env("LOG_LEVEL", default="INFO").upper()
        valid_log_levels = {"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"}
        if log_level not in valid_log_levels:
            raise ConfigurationError("LOG_LEVEL invalido")

        return cls(
            base_url=base_url,
            username=username,
            password=password,
            cnpj_database=_first_env(
                "COUCH_DB_CNPJ", default="chcnpj_bcadastros_replica"
            ),
            cpf_database=_first_env("COUCH_DB_CPF", default="chcpf_bcadastros_replica"),
            simples_database=_first_env("COUCH_DB_SN", default="chsn_bcadastros_replica"),
            timeout=_positive_float(
                "COUCHDB_TIMEOUT", _first_env("COUCHDB_TIMEOUT", default="30")
            ),
            page_limit=_positive_int(
                "COUCHDB_PAGE_LIMIT", _first_env("COUCHDB_PAGE_LIMIT", default="1000")
            ),
            max_attempts=_positive_int(
                "COUCHDB_MAX_ATTEMPTS",
                _first_env("COUCHDB_MAX_ATTEMPTS", default="3"),
            ),
            backoff_factor=_positive_float(
                "COUCHDB_BACKOFF_FACTOR",
                _first_env("COUCHDB_BACKOFF_FACTOR", default="0.5"),
                allow_zero=True,
            ),
            workers=_positive_int(
                "COUCHDB_WORKERS", _first_env("COUCHDB_WORKERS", default="4")
            ),
            verify_ssl=_boolean(
                "COUCHDB_VERIFY_SSL", _first_env("COUCHDB_VERIFY_SSL", default="true")
            ),
            responsible_index_ddoc=_first_env(
                "COUCH_IDX_RESP_DDOC", default="_design/idx_cpf_responsavel"
            ),
            responsible_index_name=_first_env(
                "COUCH_IDX_RESP_NAME", default="idx-cpf-responsavel"
            ),
            cpf_id_prefix=_first_env("COUCH_CPF_ID_PREFIX"),
            log_level=log_level,
        )
