"""Document normalization helpers used by the cadastral pipeline."""

from __future__ import annotations

import re

_CNPJ_PLAIN_RE = re.compile(r"^[A-Z0-9]{12}[0-9]{2}$")
_CNPJ_MASKED_RE = re.compile(r"^[A-Z0-9]{2}\.[A-Z0-9]{3}\.[A-Z0-9]{3}/[A-Z0-9]{4}-[0-9]{2}$")
_CPF_MASKED_RE = re.compile(r"^[0-9]{3}\.[0-9]{3}\.[0-9]{3}-[0-9]{2}$")


def normalize_cnpj(value: object) -> str | None:
    """Normalize and validate a numeric or alphanumeric CNPJ.

    The normalized value contains exactly 14 characters. The first 12
    positions may contain uppercase ASCII letters or digits, while the final
    two positions must contain digits. A mask is accepted only in the standard
    ``XX.XXX.XXX/XXXX-XX`` layout. Whitespace is ignored.

    Args:
        value: Raw CNPJ string, optionally containing the standard mask.

    Returns:
        The normalized CNPJ, or ``None`` when the value is absent or invalid.
    """
    if not isinstance(value, str):
        return None

    compact = "".join(value.split()).upper()
    if not compact:
        return None

    if any(character in compact for character in "./-"):
        if _CNPJ_MASKED_RE.fullmatch(compact) is None:
            return None
        compact = compact.replace(".", "").replace("/", "").replace("-", "")

    if _CNPJ_PLAIN_RE.fullmatch(compact) is None:
        return None
    return compact


def normalize_numeric_document(value: object, expected_length: int) -> str | None:
    """Normalize a document whose content must remain exclusively numeric.

    This helper is intended for CPF and other numeric-only identifiers. It
    deliberately does not share the alphanumeric CNPJ rules.

    Args:
        value: Raw string or integer, optionally containing mask characters.
        expected_length: Required number of digits after mask removal.

    Returns:
        The normalized numeric document, or ``None`` for an invalid value.
    """
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        return None
    if isinstance(value, int) and value < 0:
        return None
    text = str(value)
    compact = "".join(text.split())
    if not compact:
        return None
    if expected_length == 11 and _CPF_MASKED_RE.fullmatch(compact) is not None:
        compact = compact.replace(".", "").replace("-", "")
    if len(compact) != expected_length or not compact.isascii():
        return None
    return compact if compact.isdigit() else None
