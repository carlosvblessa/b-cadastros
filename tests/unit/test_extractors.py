from __future__ import annotations

from typing import Any

import pytest

from bcadastros_etl.extractors import CadastroExtractor
from bcadastros_etl.models import RecordProcessingError


class FakeClient:
    def __init__(self, *, only_pf: bool = False) -> None:
        self.only_pf = only_pf
        self.cnpj_queries: list[str] = []
        self.cpf_queries: list[str] = []

    def get_cnpj_document(self, document_id: str) -> dict[str, Any] | None:
        self.cnpj_queries.append(document_id)
        docs = {
            "00123456": {
                "nomeEmpresarial": " Empresa Principal ",
                "cpfResponsavel": "01234567890",
                "capitalSocial": "100",
                "porteEmpresa": "01",
                "socios": [],
            },
            "00123456000199": {
                "dataInicioAtividade": "20200101",
                "situacaoCadastral": "02",
                "dataSituacaoCadastral": "20200101",
                "indicadorMatriz": "1",
                "contadorPJ": "" if self.only_pf else "98.765.432/0001-00",
                "contadorPF": "111.222.333-44",
                "ufCrcContadorPJ": "SE",
                "ufCrcContadorPF": "AL",
            },
            "98765432": {"nomeEmpresarial": " Contabilidade PJ "},
        }
        return docs.get(document_id)

    def get_cpf_document(self, cpf: str) -> dict[str, str] | None:
        self.cpf_queries.append(cpf)
        docs = {
            "01234567890": {"nomeContribuinte": " Responsável "},
            "11122233344": {"nomeContribuinte": " Contador PF "},
        }
        return docs.get(cpf)

    def get_simples_document(self, _root: str) -> dict[str, Any] | None:
        return {"PeriodoSimples": [{"Inicio": "2024-01-01", "Fim": None}]}

    def count_companies_for_responsible(self, cpf: str) -> int:
        assert cpf == "01234567890"
        return 3


def test_extractor_prioritizes_pj_and_queries_its_root() -> None:
    client = FakeClient()
    record = CadastroExtractor(client).extract("00123456000199")
    assert record.NUM_DOC_CONTADOR == "98765432000100"
    assert record.COD_TIPDOC_CONTADOR == "CNPJ"
    assert record.NOM_RAZAO_SOCIAL_CONT == "CONTABILIDADE PJ"
    assert record.IND_ENDERECO_CONT_FORA_AL == "S"
    assert "98765432" in client.cnpj_queries
    assert "11122233344" not in client.cpf_queries
    assert record.NOM_RAZAO_SOCIAL_RESP == "RESPONSÁVEL"
    assert record.QTD_EMPRESAS_RESP == 3


def test_extractor_uses_pf_when_pj_absent() -> None:
    client = FakeClient(only_pf=True)
    record = CadastroExtractor(client).extract("00123456000199")
    assert record.NUM_DOC_CONTADOR == "11122233344"
    assert record.COD_TIPDOC_CONTADOR == "CPF"
    assert record.NOM_RAZAO_SOCIAL_CONT == "CONTADOR PF"
    assert record.IND_ENDERECO_CONT_FORA_AL == "N"


def test_extractor_isolates_missing_main_document() -> None:
    client = FakeClient()
    with pytest.raises(RecordProcessingError) as caught:
        CadastroExtractor(client).extract("99999999000100")
    assert caught.value.stage == "consulta_cnpj_raiz"
