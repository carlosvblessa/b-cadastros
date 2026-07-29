from __future__ import annotations

import json
import threading
import time
from dataclasses import replace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from io import StringIO

import pytest
import simplejson

from bcadastros_etl.application import run_pipeline
from bcadastros_etl.couchdb_client import CouchDBClient, CouchDBPaginationError
from bcadastros_etl.extractors import CadastroExtractor
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


@pytest.mark.integration
def test_pagination_failure_never_writes_partial_record() -> None:
    class PaginationFailingExtractor:
        def extract(self, _cnpj: str):
            raise RecordProcessingError(
                "quantidade_empresas_responsavel",
                CouchDBPaginationError("contagem parcial descartada"),
            )

    output = StringIO()
    errors = StringIO()
    stats = run_pipeline(
        ["00123456000199"],
        [],
        PaginationFailingExtractor(),
        JsonlWriter(output),
        JsonlWriter(errors),
        workers=1,
    )

    assert output.getvalue() == ""
    error = simplejson.loads(errors.getvalue())
    assert error["etapa"] == "quantidade_empresas_responsavel"
    assert error["tipo_erro"] == "CouchDBPaginationError"
    assert stats.written == 0
    assert stats.failed == 1


@pytest.mark.integration
def test_pipeline_uses_real_http_client_against_local_server(couch_config) -> None:
    routes = {
        "/cnpj/AA345678": {
            "nomeEmpresarial": "Empresa HTTP",
            "capitalSocial": "100",
            "porteEmpresa": "01",
            "socios": [{"nome": "socio"}],
        },
        "/cnpj/AA345678000329": {
            "indicadorMatriz": "1",
            "situacaoCadastral": "02",
            "dataInicioAtividade": "20200101",
        },
    }

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            payload = routes.get(self.path)
            if payload is None:
                self.send_response(404)
                self.end_headers()
                return
            body = json.dumps(payload).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, _format: str, *_args: object) -> None:
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    output = StringIO()
    errors = StringIO()
    try:
        config = replace(
            couch_config,
            base_url=f"http://127.0.0.1:{server.server_port}",
        )
        with CouchDBClient(config) as client:
            stats = run_pipeline(
                ["AA345678000329"],
                [],
                CadastroExtractor(client),
                JsonlWriter(output),
                JsonlWriter(errors),
                workers=1,
            )
    finally:
        server.shutdown()
        server.server_close()
        server_thread.join(timeout=2)

    row = simplejson.loads(output.getvalue())
    assert row["NUM_CNPJ"] == "AA345678000329"
    assert row["NOM_RAZAO_SOCIAL"] == "EMPRESA HTTP"
    assert row["QTD_SOCIO_RAIZ"] == 1
    assert errors.getvalue() == ""
    assert stats.written == 1
