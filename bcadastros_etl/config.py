"""Environment-based configuration with strict operational validation."""

from __future__ import annotations

import math
import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

from dotenv import load_dotenv

DEFAULT_WORKERS = 4
DEFAULT_CACHE_ROOT_MAXSIZE = 20_000
DEFAULT_CACHE_SIMPLES_MAXSIZE = 20_000
DEFAULT_CACHE_PERSON_MAXSIZE = 10_000
DEFAULT_CACHE_ACCOUNTANT_MAXSIZE = 10_000
DEFAULT_CACHE_RESPONSIBLE_COUNT_MAXSIZE = 10_000


class ConfigurationError(ValueError):
    """Configuration is absent, malformed, or operationally unsafe."""


def _first_env(*names: str, default: str = "") -> str:
    for name in names:
        value = os.getenv(name)
        if value is not None and value != "":
            return value
    return default


def _required_text(name: str, value: str) -> str:
    if not value.strip():
        raise ConfigurationError(f"{name} nao pode estar vazio")
    return value.strip()


def _positive_int(name: str, value: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ConfigurationError(f"{name} deve ser inteiro positivo") from exc
    if isinstance(value, bool) or parsed <= 0:
        raise ConfigurationError(f"{name} deve ser inteiro positivo")
    return parsed


def _non_negative_int(name: str, value: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ConfigurationError(f"{name} deve ser inteiro nao negativo") from exc
    if isinstance(value, bool) or parsed < 0:
        raise ConfigurationError(f"{name} deve ser inteiro nao negativo")
    return parsed


def _port(name: str, value: str) -> int:
    parsed = _positive_int(name, value)
    if parsed > 65_535:
        raise ConfigurationError(f"{name} deve estar entre 1 e 65535")
    return parsed


def _positive_float(name: str, value: str, *, allow_zero: bool = False) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ConfigurationError(f"{name} deve ser numerico") from exc
    if not math.isfinite(parsed) or parsed < 0 or (parsed == 0 and not allow_zero):
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


def _base_url_from_env() -> str:
    base_url = _first_env("COUCHDB_URL").strip()
    if not base_url:
        scheme = _required_text(
            "COUCHDB_SCHEME",
            _first_env("COUCHDB_SCHEME", "COUCH_SCHEME", default="http"),
        ).lower()
        host = _required_text(
            "COUCHDB_HOST",
            _first_env("COUCHDB_HOST", "COUCH_HOST"),
        )
        port = _port(
            "COUCHDB_PORT",
            _first_env("COUCHDB_PORT", "COUCH_PORT", default="5984"),
        )
        base_url = f"{scheme}://{host}:{port}"

    base_url = base_url.rstrip("/")
    parsed_url = urlparse(base_url)
    if parsed_url.scheme not in {"http", "https"} or not parsed_url.netloc:
        raise ConfigurationError("COUCHDB_URL deve ser uma URL HTTP(S) valida")
    if parsed_url.username or parsed_url.password:
        raise ConfigurationError(
            "nao inclua credenciais em COUCHDB_URL; use COUCHDB_USER e COUCHDB_PASSWORD"
        )
    try:
        parsed_port = parsed_url.port
    except ValueError as exc:
        raise ConfigurationError("porta de COUCHDB_URL deve estar entre 1 e 65535") from exc
    if parsed_port is not None and not 1 <= parsed_port <= 65_535:
        raise ConfigurationError("porta de COUCHDB_URL deve estar entre 1 e 65535")
    if parsed_url.hostname is None or not parsed_url.hostname.strip():
        raise ConfigurationError("COUCHDB_URL deve informar um host valido")
    return base_url


@dataclass(frozen=True, slots=True)
class CouchDBConfig:
    """Validated configuration consumed by HTTP clients and the pipeline."""

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
    cache_root_maxsize: int = DEFAULT_CACHE_ROOT_MAXSIZE
    cache_simples_maxsize: int = DEFAULT_CACHE_SIMPLES_MAXSIZE
    cache_person_maxsize: int = DEFAULT_CACHE_PERSON_MAXSIZE
    cache_accountant_maxsize: int = DEFAULT_CACHE_ACCOUNTANT_MAXSIZE
    cache_responsible_count_maxsize: int = DEFAULT_CACHE_RESPONSIBLE_COUNT_MAXSIZE

    @classmethod
    def from_env(
        cls,
        dotenv_path: str | os.PathLike[str] | None = ".env",
    ) -> CouchDBConfig:
        """Load and validate configuration from environment variables.

        By default, ``.env`` is loaded from the current working directory when
        the file exists. Values already exported by the process take
        precedence. Passing ``None`` disables dotenv loading.

        Args:
            dotenv_path: Dotenv path, ``.env`` by default, or ``None`` to use
                only the process environment.

        Returns:
            Fully validated immutable configuration.

        Raises:
            ConfigurationError: If the env file or a setting is invalid.
        """
        if dotenv_path is not None:
            env_file = Path(dotenv_path).expanduser()
            if env_file.is_file():
                load_dotenv(dotenv_path=env_file, override=False)
            elif env_file != Path(".env"):
                raise ConfigurationError(f"arquivo de ambiente nao encontrado: {env_file}")

        base_url = _base_url_from_env()
        username = _required_text(
            "COUCHDB_USER",
            _first_env("COUCHDB_USER_ETL", "COUCHDB_USER", "COUCH_USER"),
        )
        password = _required_text(
            "COUCHDB_PASSWORD",
            _first_env("COUCHDB_PASSWORD_ETL", "COUCHDB_PASSWORD", "COUCH_PASS"),
        )

        log_level = _first_env("LOG_LEVEL", default="INFO").strip().upper()
        valid_log_levels = {"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"}
        if log_level not in valid_log_levels:
            raise ConfigurationError("LOG_LEVEL invalido")

        return cls(
            base_url=base_url,
            username=username,
            password=password,
            cnpj_database=_required_text(
                "COUCH_DB_CNPJ",
                _first_env("COUCH_DB_CNPJ", default="chcnpj_bcadastros_replica"),
            ),
            cpf_database=_required_text(
                "COUCH_DB_CPF",
                _first_env("COUCH_DB_CPF", default="chcpf_bcadastros_replica"),
            ),
            simples_database=_required_text(
                "COUCH_DB_SN",
                _first_env("COUCH_DB_SN", default="chsn_bcadastros_replica"),
            ),
            timeout=_positive_float(
                "COUCHDB_TIMEOUT",
                _first_env("COUCHDB_TIMEOUT", default="30"),
            ),
            page_limit=_positive_int(
                "COUCHDB_PAGE_LIMIT",
                _first_env("COUCHDB_PAGE_LIMIT", default="1000"),
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
                "WORKERS",
                _first_env("WORKERS", default=str(DEFAULT_WORKERS)),
            ),
            verify_ssl=_boolean(
                "COUCHDB_VERIFY_SSL",
                _first_env("COUCHDB_VERIFY_SSL", default="true"),
            ),
            responsible_index_ddoc=_required_text(
                "COUCH_IDX_RESP_DDOC",
                _first_env(
                    "COUCH_IDX_RESP_DDOC",
                    default="_design/idx_cpf_responsavel",
                ),
            ),
            responsible_index_name=_required_text(
                "COUCH_IDX_RESP_NAME",
                _first_env("COUCH_IDX_RESP_NAME", default="idx-cpf-responsavel"),
            ),
            cpf_id_prefix=_first_env("COUCH_CPF_ID_PREFIX"),
            log_level=log_level,
            cache_root_maxsize=_non_negative_int(
                "CACHE_ROOT_MAXSIZE",
                _first_env(
                    "CACHE_ROOT_MAXSIZE",
                    default=str(DEFAULT_CACHE_ROOT_MAXSIZE),
                ),
            ),
            cache_simples_maxsize=_non_negative_int(
                "CACHE_SIMPLES_MAXSIZE",
                _first_env(
                    "CACHE_SIMPLES_MAXSIZE",
                    default=str(DEFAULT_CACHE_SIMPLES_MAXSIZE),
                ),
            ),
            cache_person_maxsize=_non_negative_int(
                "CACHE_PERSON_MAXSIZE",
                _first_env(
                    "CACHE_PERSON_MAXSIZE",
                    default=str(DEFAULT_CACHE_PERSON_MAXSIZE),
                ),
            ),
            cache_accountant_maxsize=_non_negative_int(
                "CACHE_ACCOUNTANT_MAXSIZE",
                _first_env(
                    "CACHE_ACCOUNTANT_MAXSIZE",
                    default=str(DEFAULT_CACHE_ACCOUNTANT_MAXSIZE),
                ),
            ),
            cache_responsible_count_maxsize=_non_negative_int(
                "CACHE_RESPONSIBLE_COUNT_MAXSIZE",
                _first_env(
                    "CACHE_RESPONSIBLE_COUNT_MAXSIZE",
                    default=str(DEFAULT_CACHE_RESPONSIBLE_COUNT_MAXSIZE),
                ),
            ),
        )
