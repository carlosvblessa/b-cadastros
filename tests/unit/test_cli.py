from __future__ import annotations

import logging
import sys
from dataclasses import replace
from io import StringIO
from typing import Any

import pytest
import simplejson

from bcadastros_etl import cli
from bcadastros_etl.application import ProcessingStats
from bcadastros_etl.config import ConfigurationError
from bcadastros_etl.models import RootProjection, SimplesProjection


class FakeCouchClient:
    def __init__(self, _config: Any) -> None:
        pass

    def __enter__(self) -> FakeCouchClient:
        return self

    def __exit__(self, *_args: object) -> None:
        pass

    def cache_metrics(self) -> dict[str, Any]:
        return {}

    def get_root_projection(self, root: str) -> RootProjection | None:
        if root != "00123456":
            return None
        return RootProjection(
            company_name="Empresa Teste",
            responsible_cpf=None,
            share_capital="100",
            company_size=None,
            partner_count=0,
        )

    def get_establishment_document(self, cnpj: str) -> dict[str, Any] | None:
        if cnpj == "00123456000199":
            return {"indicadorMatriz": "1", "situacaoCadastral": "02"}
        return None

    def get_simples_projection(self, _root: str) -> SimplesProjection | None:
        return None

    def get_person_name(self, _cpf: str) -> None:
        return None

    def count_companies_for_responsible(self, _cpf: str) -> int:
        return 0

    def get_accountant_name(self, _document_type: str, _document: str) -> None:
        return None


def test_cli_completes_with_isolated_input_error(
    monkeypatch,
    tmp_path,
    couch_config,
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
    assert output["COD_TIPDOC_RESP"] == "CPF"
    assert error["cnpj"] == "123"
    assert error["etapa"] == "validacao_entrada"
    assert "Entrada invalida" not in output_path.read_text(encoding="utf-8")


def test_cli_supports_stdin_and_stdout(monkeypatch, couch_config) -> None:
    input_stream = StringIO("00.123.456/0001-99\n")
    output_stream = StringIO()
    monkeypatch.setattr(cli.CouchDBConfig, "from_env", lambda: couch_config)
    monkeypatch.setattr(cli, "CouchDBClient", FakeCouchClient)
    monkeypatch.setattr(sys, "stdin", input_stream)
    monkeypatch.setattr(sys, "stdout", output_stream)

    assert cli.main(["--output", "-", "--log-level", "ERROR"]) == 0
    row = simplejson.loads(output_stream.getvalue())
    assert row["NUM_CNPJ"] == "00123456000199"


@pytest.mark.parametrize(
    ("configured_workers", "cli_workers", "expected"),
    [(4, None, 4), (8, None, 8), (8, 12, 12)],
)
def test_pipeline_receives_exact_resolved_workers(
    monkeypatch,
    tmp_path,
    couch_config,
    configured_workers: int,
    cli_workers: int | None,
    expected: int,
) -> None:
    captured: list[int] = []
    output_path = tmp_path / "output.jsonl"
    input_path = tmp_path / "input.txt"
    input_path.write_text("", encoding="utf-8")
    configured = replace(couch_config, workers=configured_workers)
    monkeypatch.setattr(cli.CouchDBConfig, "from_env", lambda: configured)
    monkeypatch.setattr(cli, "CouchDBClient", FakeCouchClient)

    def fake_run_pipeline(*_args: Any, workers: int, **_kwargs: Any) -> ProcessingStats:
        captured.append(workers)
        return ProcessingStats(received=0, written=0, failed=0)

    monkeypatch.setattr(cli, "run_pipeline", fake_run_pipeline)
    arguments = ["--input", str(input_path), "--output", str(output_path)]
    if cli_workers is not None:
        arguments.extend(["--workers", str(cli_workers)])

    assert cli.main(arguments) == 0
    assert captured == [expected]


def test_cli_logs_effective_worker_count(
    monkeypatch,
    tmp_path,
    couch_config,
    caplog: pytest.LogCaptureFixture,
) -> None:
    input_path = tmp_path / "input.txt"
    output_path = tmp_path / "output.jsonl"
    input_path.write_text("", encoding="utf-8")
    configured = replace(couch_config, workers=8)
    monkeypatch.setattr(cli.CouchDBConfig, "from_env", lambda: configured)
    monkeypatch.setattr(cli, "CouchDBClient", FakeCouchClient)

    with caplog.at_level(logging.INFO):
        assert (
            cli.main(
                [
                    "--input",
                    str(input_path),
                    "--output",
                    str(output_path),
                    "--workers",
                    "12",
                ]
            )
            == 0
        )
    assert "Inicio do processamento: workers=12" in caplog.text


@pytest.mark.parametrize("workers", ["0", "-1"])
def test_cli_rejects_non_positive_workers(
    monkeypatch,
    couch_config,
    workers: str,
) -> None:
    monkeypatch.setattr(cli.CouchDBConfig, "from_env", lambda: couch_config)
    assert cli.main(["--workers", workers]) == 2


def test_cli_rejects_non_numeric_workers() -> None:
    with pytest.raises(SystemExit) as caught:
        cli.main(["--workers", "texto"])
    assert caught.value.code == 2


def test_cli_returns_nonzero_for_structural_configuration_error(monkeypatch) -> None:
    def fail_config() -> None:
        raise ConfigurationError("configuracao ausente")

    monkeypatch.setattr(cli.CouchDBConfig, "from_env", fail_config)
    assert cli.main(["--output", "-"]) == 2


def test_cli_returns_nonzero_for_structural_output_error(
    monkeypatch,
    tmp_path,
    couch_config,
) -> None:
    input_path = tmp_path / "input.txt"
    input_path.write_text("", encoding="utf-8")
    monkeypatch.setattr(cli.CouchDBConfig, "from_env", lambda: couch_config)
    assert (
        cli.main(
            [
                "--input",
                str(input_path),
                "--output",
                str(tmp_path),
            ]
        )
        == 2
    )
