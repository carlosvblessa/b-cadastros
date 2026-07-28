from __future__ import annotations

import time
from io import StringIO

import pytest
import simplejson

from bcadastros_etl.application import run_pipeline
from bcadastros_etl.jsonl_writer import JsonlWriter
from bcadastros_etl.models import ErrorRecord, RecordProcessingError
from bcadastros_etl.transformers import build_record


class FakeExtractor:
    def extract(self, cnpj: str):
        if cnpj == "00123456000199":
            time.sleep(0.02)
        if cnpj == "11111111000111":
            raise RecordProcessingError("consulta_cnpj", RuntimeError("falha simulada"))
        return build_record(
            cnpj,
            {
                "nomeEmpresarial": f"Empresa {cnpj}",
                "capitalSocial": "100",
                "socios": [],
            },
            {"indicadorMatriz": "1"},
            None,
            responsible_name=None,
            responsible_company_count=0,
            accountant_name=None,
        )


@pytest.mark.integration
def test_pipeline_preserves_order_and_separates_errors() -> None:
    output = StringIO()
    errors = StringIO()
    stats = run_pipeline(
        ["00123456000199", "11111111000111", "98765432000100"],
        [
            ErrorRecord(
                cnpj="123",
                etapa="validacao_entrada",
                tipo_erro="CNPJInvalido",
                mensagem="invalido",
            )
        ],
        FakeExtractor(),
        JsonlWriter(output),
        JsonlWriter(errors),
        workers=3,
    )

    output_rows = [simplejson.loads(line) for line in output.getvalue().splitlines()]
    error_rows = [simplejson.loads(line) for line in errors.getvalue().splitlines()]
    assert [row["NUM_CNPJ"] for row in output_rows] == [
        "00123456000199",
        "98765432000100",
    ]
    assert [row["cnpj"] for row in error_rows] == ["123", "11111111000111"]
    assert stats.received == 4
    assert stats.written == 2
    assert stats.failed == 2
    assert "falha simulada" not in output.getvalue()
