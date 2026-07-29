from __future__ import annotations

from dataclasses import replace
from typing import Any

import pytest

from bcadastros_etl.config import CouchDBConfig


@pytest.fixture
def couch_config() -> CouchDBConfig:
    return CouchDBConfig(
        base_url="http://couch.test:5984",
        username="etl",
        password="secret",
        cnpj_database="cnpj",
        cpf_database="cpf",
        simples_database="sn",
        timeout=3.0,
        page_limit=2,
        max_attempts=3,
        backoff_factor=0.1,
        workers=2,
        verify_ssl=True,
        responsible_index_ddoc="_design/idx_cpf_responsavel",
        responsible_index_name="idx-cpf-responsavel",
        cpf_id_prefix="",
        log_level="INFO",
        cache_root_maxsize=4,
        cache_simples_maxsize=4,
        cache_person_maxsize=4,
        cache_accountant_maxsize=4,
        cache_responsible_count_maxsize=4,
    )


@pytest.fixture
def config_factory(couch_config: CouchDBConfig):
    def factory(**changes: Any) -> CouchDBConfig:
        return replace(couch_config, **changes)

    return factory
