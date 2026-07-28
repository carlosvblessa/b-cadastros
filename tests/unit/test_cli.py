from __future__ import annotations

from typing import Any

import simplejson

from bcadastros_etl import cli
from bcadastros_etl.config import ConfigurationError


class FakeCouchClient:
    def __init__(self, _config: Any) -> None:
        pass

    def __enter__(self) -> FakeCouchClient:
        return self

    def __exit__(self, *_args: object) -> None:
        pass

    def get_cnpj_document(self, document_id: str) -> dict[str, Any] | None:
        if document_id == "00123456":
            return {
                "nomeEmpresarial": "Empresa Teste",
                "capitalSocial": "100",
                "socios": [],
            }
        if document_id == "00123456000199":
            return {"indicadorMatriz": "1", "situacaoCadastral": "02"}
        return None

    def get_simples_document(self, _root: str) -> None:
        return None

    def get_cpf_document(self, _cpf: str) -> None:
        return None

    def count_companies_for_responsible(self, _cpf: str) -> int:
        return 0


def test_cli_completes_with_isolated_input_error(
    monkeypatch, tmp_path, couch_config
) -> None:
    input_path = tmp_path / "cnpjs.txt"
    output_path = tmp_path / "resultado.jsonl"
    errors_path = tmp_path / "erros.jsonl"
    input_path.write_text("00.123.456/0001-99\n123\n", encoding="utf-8")
    monkeypatch.setattr(cli.CouchDBConfig, "from_env", lambda: couch_config)
    monkeypatch.setattr(cli, "CouchDBClient", FakeCouchClient)

    exit_code = cli.main(
        [
            "--input",
            str(input_path),
            "--output",
            str(output_path),
            "--errors",
            str(errors_path),
            "--workers",
            "1",
            "--log-level",
            "ERROR",
        ]
    )

    assert exit_code == 0
    output = simplejson.loads(output_path.read_text(encoding="utf-8"))
    error = simplejson.loads(errors_path.read_text(encoding="utf-8"))
    assert output["NUM_CNPJ"] == "00123456000199"
    assert error["cnpj"] == "123"
    assert error["etapa"] == "validacao_entrada"


def test_cli_returns_nonzero_for_structural_configuration_error(monkeypatch) -> None:
    def fail_config() -> None:
        raise ConfigurationError("configuracao ausente")

    monkeypatch.setattr(cli.CouchDBConfig, "from_env", fail_config)
    assert cli.main(["--output", "-"]) == 2
