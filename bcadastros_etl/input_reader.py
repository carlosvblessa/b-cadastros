"""Leitura e validacao da lista de CNPJs."""

from __future__ import annotations

from collections.abc import Iterable

from .models import ErrorRecord
from .transformers import digits_only


def read_cnpjs(lines: Iterable[str]) -> tuple[list[str], list[ErrorRecord]]:
    """Normaliza, deduplica na primeira ocorrencia e separa erros de entrada."""
    valid: list[str] = []
    errors: list[ErrorRecord] = []
    seen: set[str] = set()

    for line in lines:
        raw = line.strip()
        if not raw:
            continue
        normalized = digits_only(raw) or ""
        if normalized in seen:
            continue
        seen.add(normalized)
        if len(normalized) != 14:
            errors.append(
                ErrorRecord(
                    cnpj=normalized,
                    etapa="validacao_entrada",
                    tipo_erro="CNPJInvalido",
                    mensagem=f"CNPJ deve possuir 14 digitos; recebido com {len(normalized)}",
                )
            )
            continue
        valid.append(normalized)

    return valid, errors
