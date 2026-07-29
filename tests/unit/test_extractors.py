from __future__ import annotations

from typing import Any

import pytest

from bcadastros_etl.extractors import CadastroExtractor
from bcadastros_etl.models import RecordProcessingError, RootProjection, SimplesProjection


class FakeClient:
    def __init__(self, *, only_pf: bool = False, alphanumeric: bool = False) -> None:
        self.only_pf = only_pf
        self.alphanumeric = alphanumeric
        self.root_queries: list[str] = []
        self.establishment_queries: list[str] = []
        self.person_queries: list[str] = []
        self.accountant_queries: list[tuple[str, str]] = []

    def get_root_projection(self, root_id: str) -> RootProjection | None:
        self.root_queries.append(root_id)
        valid_root = "AA345678" if self.alphanumeric else "00123456"
        if root_id != valid_root:
            return None
        return RootProjection(
            company_name=" Empresa Principal ",
            responsible_cpf="01234567890",
            share_capital="100",
            company_size="01",
            partner_count=0,
        )

    def get_establishment_document(self, cnpj: str) -> dict[str, Any] | None:
        self.establishment_queries.append(cnpj)
        valid_cnpj = "AA345678000329" if self.alphanumeric else "00123456000199"
        if cnpj != valid_cnpj:
            return None
        return {
            "dataInicioAtividade": "20200101",
            "situacaoCadastral": "02",
            "dataSituacaoCadastral": "20200101",
            "indicadorMatriz": "1",
            "contadorPJ": (
                "ab.345.678/000a-08"
                if self.alphanumeric
                else ("" if self.only_pf else "98.765.432/0001-00")
            ),
            "contadorPF": "111.222.333-44",
            "ufCrcContadorPJ": "SE",
            "ufCrcContadorPF": "AL",
        }

    def get_simples_projection(self, _root: str) -> SimplesProjection:
        return SimplesProjection(
            simples_period=[{"Inicio": "2024-01-01", "Fim": None}],
            mei_period=None,
        )

    def get_person_name(self, cpf: str) -> str | None:
        self.person_queries.append(cpf)
        names = {
            "01234567890": " Responsável ",
            "11122233344": " Contador PF ",
        }
        return names.get(cpf)

    def count_companies_for_responsible(self, cpf: str) -> int:
        assert cpf == "01234567890"
        return 3

    def get_accountant_name(self, document_type: str, document: str) -> str | None:
        self.accountant_queries.append((document_type, document))
        if document_type == "CPF":
            return self.get_person_name(document)
        return " Contabilidade PJ "


def test_extractor_prioritizes_pj_and_queries_its_root() -> None:
    client = FakeClient()
    record = CadastroExtractor(client).extract("00123456000199")
    assert record.NUM_DOC_CONTADOR == "98765432000100"
    assert record.COD_TIPDOC_CONTADOR == "CNPJ"
    assert record.NOM_RAZAO_SOCIAL_CONT == "CONTABILIDADE PJ"
    assert record.IND_ENDERECO_CONT_FORA_AL == "S"
    assert client.accountant_queries == [("CNPJ", "98765432000100")]
    assert "11122233344" not in client.person_queries
    assert record.NOM_RAZAO_SOCIAL_RESP == "RESPONSÁVEL"
    assert record.QTD_EMPRESAS_RESP == 3


def test_extractor_uses_pf_when_pj_absent() -> None:
    client = FakeClient(only_pf=True)
    record = CadastroExtractor(client).extract("00123456000199")
    assert record.NUM_DOC_CONTADOR == "11122233344"
    assert record.COD_TIPDOC_CONTADOR == "CPF"
    assert record.NOM_RAZAO_SOCIAL_CONT == "CONTADOR PF"
    assert record.IND_ENDERECO_CONT_FORA_AL == "N"


def test_extractor_supports_alphanumeric_roots_and_pj_accountants() -> None:
    client = FakeClient(alphanumeric=True)
    record = CadastroExtractor(client).extract("AA345678000329")
    assert client.root_queries == ["AA345678"]
    assert client.establishment_queries == ["AA345678000329"]
    assert record.NUM_CNPJ == "AA345678000329"
    assert record.NUM_DOC_CONTADOR == "AB345678000A08"
    assert record.COD_TIPDOC_CONTADOR == "CNPJ"
    assert client.accountant_queries == [("CNPJ", "AB345678000A08")]


def test_extractor_isolates_missing_main_document() -> None:
    client = FakeClient()
    with pytest.raises(RecordProcessingError) as caught:
        CadastroExtractor(client).extract("99999999000100")
    assert caught.value.stage == "consulta_cnpj_raiz"
