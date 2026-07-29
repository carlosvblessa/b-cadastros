"""Execucao concorrente limitada com preservacao da ordem de entrada."""

from __future__ import annotations

import logging
from collections import deque
from collections.abc import Iterator, Sequence
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from typing import Protocol

from .jsonl_writer import JsonlWriter
from .models import CadastroRecord, ErrorRecord, RecordProcessingError

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ProcessingStats:
    """Aggregate counts for one completed batch."""

    received: int
    written: int
    failed: int


class RecordExtractor(Protocol):
    """Minimal extractor contract consumed by the concurrent pipeline."""

    def extract(self, cnpj: str) -> CadastroRecord:
        """Extract one record for a normalized CNPJ."""
        ...


def _ordered_futures(
    cnpjs: Sequence[str], extractor: RecordExtractor, workers: int
) -> Iterator[tuple[str, Future[CadastroRecord]]]:
    """Submit a bounded future window and yield in original input order."""
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
    cnpjs: Sequence[str],
    input_errors: Sequence[ErrorRecord],
    extractor: RecordExtractor,
    output_writer: JsonlWriter,
    error_writer: JsonlWriter | None,
    *,
    workers: int,
) -> ProcessingStats:
    """Process a batch while isolating record failures from structural errors.

    Args:
        cnpjs: Valid normalized CNPJs in output order.
        input_errors: Isolated validation errors already found in the input.
        extractor: Record extraction implementation.
        output_writer: Main JSONL destination.
        error_writer: Optional isolated-error JSONL destination.
        workers: Exact positive worker count selected by configuration.

    Returns:
        Aggregate received, written, and failed counts.

    Raises:
        ValueError: If ``workers`` is not a positive integer.
        OSError: If a structural output operation fails.
    """
    if isinstance(workers, bool) or not isinstance(workers, int) or workers < 1:
        raise ValueError("workers deve ser inteiro positivo")
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
