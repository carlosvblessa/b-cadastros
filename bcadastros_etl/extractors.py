"""Orchestrate CouchDB queries required for one cadastral record."""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import TypeVar

from .couchdb_client import CouchDBClient
from .models import (
    CadastroRecord,
    DocumentNotFoundError,
    RecordProcessingError,
)
from .normalization import normalize_numeric_document
from .transformers import build_record, is_filled, rule_g

LOGGER = logging.getLogger(__name__)
QueryResultT = TypeVar("QueryResultT")


class CadastroExtractor:
    """Extract and transform one normalized CNPJ using reusable client data."""

    def __init__(self, client: CouchDBClient) -> None:
        """Initialize the extractor.

        Args:
            client: Shared thread-safe CouchDB client.
        """
        self.client = client

    @staticmethod
    def _query(
        stage: str,
        operation: Callable[[], QueryResultT | None],
        *,
        required: bool = False,
    ) -> QueryResultT | None:
        """Run one query and attach record-stage context to any failure."""
        try:
            result = operation()
        except Exception as exc:
            raise RecordProcessingError(stage, exc) from exc
        if required and result is None:
            error = DocumentNotFoundError("Documento nao encontrado no b-cadastros")
            raise RecordProcessingError(stage, error)
        return result

    def extract(self, cnpj: str) -> CadastroRecord:
        """Extract a complete record or raise an isolatable processing error.

        Args:
            cnpj: Normalized 14-position numeric or alphanumeric CNPJ.

        Returns:
            Fully transformed output record.

        Raises:
            RecordProcessingError: If a required document, HTTP operation, or
                complete Rule I pagination fails for this CNPJ.
        """
        root_id = cnpj[:8]
        root = self._query(
            "consulta_cnpj_raiz",
            lambda: self.client.get_root_projection(root_id),
            required=True,
        )
        establishment = self._query(
            "consulta_cnpj",
            lambda: self.client.get_establishment_document(cnpj),
            required=True,
        )
        simples = self._query(
            "consulta_simples_mei",
            lambda: self.client.get_simples_projection(root_id),
        )
        assert root is not None
        assert establishment is not None

        responsible_name: str | None = None
        responsible_company_count = 0
        responsible_raw = root.responsible_cpf
        responsible_document = normalize_numeric_document(responsible_raw, 11)
        if responsible_document:
            responsible_name = self._query(
                "consulta_responsavel",
                lambda: self.client.get_person_name(responsible_document),
            )
            responsible_company_count_result = self._query(
                "quantidade_empresas_responsavel",
                lambda: self.client.count_companies_for_responsible(responsible_document),
                required=True,
            )
            assert responsible_company_count_result is not None
            responsible_company_count = responsible_company_count_result
        elif is_filled(responsible_raw):
            LOGGER.warning(
                "cpfResponsavel ignorado por nao possuir 11 digitos no CNPJ %s",
                cnpj,
            )

        accountant_name: str | None = None
        accountant = rule_g(establishment)
        if accountant.document_type is not None and accountant.document is not None:
            accountant_type = accountant.document_type
            accountant_document = accountant.document
            stage = "consulta_contador_pj" if accountant_type == "CNPJ" else "consulta_contador_pf"
            accountant_name = self._query(
                stage,
                lambda: self.client.get_accountant_name(
                    accountant_type,
                    accountant_document,
                ),
            )
        elif accountant.source is not None:
            LOGGER.warning(
                "Documento de contador %s invalido no CNPJ %s",
                accountant.source,
                cnpj,
            )

        return build_record(
            cnpj,
            root.to_mapping(),
            establishment,
            simples.to_mapping() if simples is not None else None,
            responsible_name=responsible_name,
            responsible_company_count=responsible_company_count,
            accountant_name=accountant_name,
            partner_count=root.partner_count,
        )
