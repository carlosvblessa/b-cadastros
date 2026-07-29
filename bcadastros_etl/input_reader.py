"""Leitura e validacao da lista de CNPJs."""

from __future__ import annotations

from collections.abc import Iterable

from .models import ErrorRecord
from .normalization import normalize_cnpj


def read_cnpjs(lines: Iterable[str]) -> tuple[list[str], list[ErrorRecord]]:
    """Normalize, deduplicate, and separate isolated input errors.

    Args:
        lines: Text lines containing one numeric or alphanumeric CNPJ each.

    Returns:
        Valid normalized CNPJs in first-occurrence order and isolated errors.
    """
    valid: list[str] = []
    errors: list[ErrorRecord] = []
    seen: set[str] = set()
    seen_invalid: set[str] = set()

    for line in lines:
        raw = line.strip()
        if not raw:
            continue
        normalized = normalize_cnpj(raw)
        if normalized is None:
            invalid_key = raw.upper()
            if invalid_key in seen_invalid:
                continue
            seen_invalid.add(invalid_key)
            errors.append(
                ErrorRecord(
                    cnpj=raw,
                    etapa="validacao_entrada",
                    tipo_erro="CNPJInvalido",
                    mensagem=(
                        "CNPJ invalido: informe 14 posicoes; as 12 primeiras "
                        "aceitam letras ASCII ou digitos e as duas ultimas exigem digitos"
                    ),
                )
            )
            continue
        if normalized in seen:
            continue
        seen.add(normalized)
        valid.append(normalized)

    return valid, errors
