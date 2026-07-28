"""Interface de linha de comando da extracao cadastral."""

from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Sequence
from contextlib import ExitStack
from dataclasses import replace
from pathlib import Path
from typing import TextIO

from . import __version__
from .application import run_pipeline
from .config import ConfigurationError, CouchDBConfig
from .couchdb_client import CouchDBClient
from .extractors import CadastroExtractor
from .input_reader import read_cnpjs
from .jsonl_writer import JsonlWriter
from .logging_config import configure_logging

LOGGER = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m bcadastros_etl",
        description="Extrai dados cadastrais do CouchDB e gera JSON Lines para o Pentaho.",
    )
    parser.add_argument(
        "--input",
        metavar="ARQUIVO",
        help="arquivo com um CNPJ por linha; omitido ou '-' le stdin",
    )
    parser.add_argument(
        "--output",
        metavar="ARQUIVO",
        default="-",
        help="arquivo JSONL de saida; '-' escreve em stdout (padrao)",
    )
    parser.add_argument("--errors", metavar="ARQUIVO", help="JSONL opcional de erros isolados")
    parser.add_argument("--workers", type=int, help="sobrescreve COUCHDB_WORKERS")
    parser.add_argument(
        "--log-level",
        choices=("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"),
        help="sobrescreve LOG_LEVEL",
    )
    parser.add_argument("--version", action="version", version=__version__)
    return parser


def _resolved_file(path: str | None) -> Path | None:
    if path is None or path == "-":
        return None
    return Path(path).expanduser().resolve()


def _validate_paths(input_path: str | None, output_path: str, errors_path: str | None) -> None:
    resolved_input = _resolved_file(input_path)
    resolved_output = _resolved_file(output_path)
    resolved_errors = _resolved_file(errors_path)
    if errors_path == "-":
        raise ConfigurationError("--errors deve apontar para um arquivo, nao stdout")
    if resolved_input is not None and resolved_input in {resolved_output, resolved_errors}:
        raise ConfigurationError("arquivos de entrada e saida devem ser diferentes")
    if resolved_output is not None and resolved_output == resolved_errors:
        raise ConfigurationError("--output e --errors devem ser arquivos diferentes")


def _read_input(path: str | None, stack: ExitStack) -> TextIO:
    if path is None or path == "-":
        return sys.stdin
    return stack.enter_context(open(path, encoding="utf-8"))


def _open_output(path: str, stack: ExitStack) -> TextIO:
    if path == "-":
        return sys.stdout
    return stack.enter_context(open(path, "w", encoding="utf-8", newline="\n"))


def execute(args: argparse.Namespace) -> int:
    _validate_paths(args.input, args.output, args.errors)
    config = CouchDBConfig.from_env()
    if args.workers is not None:
        if args.workers <= 0:
            raise ConfigurationError("--workers deve ser inteiro positivo")
        config = replace(config, workers=args.workers)
    if args.log_level is not None:
        config = replace(config, log_level=args.log_level)
    configure_logging(config.log_level)

    with ExitStack() as stack:
        input_stream = _read_input(args.input, stack)
        cnpjs, input_errors = read_cnpjs(input_stream)
        output_stream = _open_output(args.output, stack)
        error_stream = _open_output(args.errors, stack) if args.errors else None

        with CouchDBClient(config) as client:
            stats = run_pipeline(
                cnpjs,
                input_errors,
                CadastroExtractor(client),
                JsonlWriter(output_stream),
                JsonlWriter(error_stream) if error_stream is not None else None,
                workers=config.workers,
            )

    LOGGER.info(
        "Processamento concluido: recebidos=%d escritos=%d erros=%d",
        stats.received,
        stats.written,
        stats.failed,
    )
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    configure_logging("INFO")
    try:
        return execute(args)
    except (ConfigurationError, OSError) as exc:
        LOGGER.error("Falha estrutural: %s", exc)
        return 2
    except Exception as exc:
        LOGGER.error("Falha estrutural inesperada (%s): %s", exc.__class__.__name__, exc)
        return 3
