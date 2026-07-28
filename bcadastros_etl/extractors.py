"""Orquestracao das consultas necessarias para montar um registro cadastral."""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from typing import Any

from .couchdb_client import CouchDBClient
from .models import (
    CadastroRecord,
    DocumentNotFoundError,
    RecordProcessingError,
)
from .transformers import build_record, is_filled, normalize_document, rule_g

LOGGER = logging.getLogger(__name__)


class CadastroExtractor:
    def __init__(self, client: CouchDBClient) -> None:
        self.client = client

    @staticmethod
    def _query(
        stage: str,
        operation: Callable[[], Mapping[str, Any] | None],
        *,
        required: bool = False,
    ) -> Mapping[str, Any] | None:
        try:
            document = operation()
        except Exception as exc:
            raise RecordProcessingError(stage, exc) from exc
        if required and document is None:
            error = DocumentNotFoundError("Documento nao encontrado no b-cadastros")
            raise RecordProcessingError(stage, error)
        return document

    def extract(self, cnpj: str) -> CadastroRecord:
        root_id = cnpj[:8]
        root = self._query(
            "consulta_cnpj_raiz",
            lambda: self.client.get_cnpj_document(root_id),
            required=True,
        )
        establishment = self._query(
            "consulta_cnpj",
            lambda: self.client.get_cnpj_document(cnpj),
            required=True,
        )
        simples = self._query(
            "consulta_simples_mei",
            lambda: self.client.get_simples_document(root_id),
        )
        assert root is not None
        assert establishment is not None

        responsible_name: Any = None
        responsible_company_count = 0
        responsible_raw = root.get("cpfResponsavel")
        responsible_document = normalize_document(responsible_raw, 11)
        if responsible_document:
            person = self._query(
                "consulta_responsavel",
                lambda: self.client.get_cpf_document(responsible_document),
            )
            if person is not None:
                responsible_name = person.get("nomeContribuinte")
            try:
                responsible_company_count = self.client.count_companies_for_responsible(
                    responsible_document
                )
            except Exception as exc:
                raise RecordProcessingError("quantidade_empresas_responsavel", exc) from exc
        elif is_filled(responsible_raw):
            LOGGER.warning("cpfResponsavel ignorado por nao possuir 11 digitos no CNPJ %s", cnpj)

        accountant_name: Any = None
        accountant = rule_g(establishment)
        if accountant.document_type == "CNPJ" and accountant.document is not None:
            accountant_document_id = accountant.document
            accountant_document = self._query(
                "consulta_contador_pj",
                lambda: self.client.get_cnpj_document(accountant_document_id[:8]),
            )
            if accountant_document is not None:
                accountant_name = accountant_document.get("nomeEmpresarial")
        elif accountant.document_type == "CPF" and accountant.document is not None:
            accountant_document_id = accountant.document
            accountant_document = self._query(
                "consulta_contador_pf",
                lambda: self.client.get_cpf_document(accountant_document_id),
            )
            if accountant_document is not None:
                accountant_name = accountant_document.get("nomeContribuinte")
        elif accountant.document is not None:
            LOGGER.warning("Documento de contador com tamanho invalido no CNPJ %s", cnpj)

        return build_record(
            cnpj,
            root,
            establishment,
            simples,
            responsible_name=responsible_name,
            responsible_company_count=responsible_company_count,
            accountant_name=accountant_name,
        )
