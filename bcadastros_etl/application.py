"""Execucao concorrente limitada com preservacao da ordem de entrada."""

from __future__ import annotations

import logging
from collections import deque
from collections.abc import Iterator
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass

from .extractors import CadastroExtractor
from .jsonl_writer import JsonlWriter
from .models import CadastroRecord, ErrorRecord, RecordProcessingError

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ProcessingStats:
    received: int
    written: int
    failed: int


def _ordered_futures(
    cnpjs: list[str], extractor: CadastroExtractor, workers: int
) -> Iterator[tuple[str, Future[CadastroRecord]]]:
    window_size = max(workers * 2, 1)
    source = iter(cnpjs)
    pending: deque[tuple[str, Future[CadastroRecord]]] = deque()

    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="bcad-etl") as executor:
        for _ in range(min(window_size, len(cnpjs))):
            cnpj = next(source)
            pending.append((cnpj, executor.submit(extractor.extract, cnpj)))

        while pending:
            cnpj, future = pending.popleft()
            yield cnpj, future
            try:
                next_cnpj = next(source)
            except StopIteration:
                continue
            pending.append((next_cnpj, executor.submit(extractor.extract, next_cnpj)))


def _to_error(cnpj: str, error: Exception) -> ErrorRecord:
    if isinstance(error, RecordProcessingError):
        stage = error.stage
        cause = error.cause
    else:
        stage = "processamento"
        cause = error
    message = str(cause) or cause.__class__.__name__
    return ErrorRecord(
        cnpj=cnpj,
        etapa=stage,
        tipo_erro=cause.__class__.__name__,
        mensagem=message,
    )


def run_pipeline(
    cnpjs: list[str],
    input_errors: list[ErrorRecord],
    extractor: CadastroExtractor,
    output_writer: JsonlWriter,
    error_writer: JsonlWriter | None,
    *,
    workers: int,
) -> ProcessingStats:
    failed = len(input_errors)
    written = 0

    for error in input_errors:
        LOGGER.error("Entrada invalida: %s", error.mensagem)
        if error_writer is not None:
            error_writer.write(error.to_dict())

    for cnpj, future in _ordered_futures(cnpjs, extractor, workers):
        try:
            record = future.result()
        except Exception as exc:
            failed += 1
            error = _to_error(cnpj, exc)
            LOGGER.error("CNPJ %s falhou em %s: %s", cnpj, error.etapa, error.mensagem)
            if error_writer is not None:
                error_writer.write(error.to_dict())
            continue
        output_writer.write(record.to_dict())
        written += 1

    output_writer.flush()
    if error_writer is not None:
        error_writer.flush()
    return ProcessingStats(received=len(cnpjs) + len(input_errors), written=written, failed=failed)
