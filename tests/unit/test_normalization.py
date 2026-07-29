from __future__ import annotations

import pytest

from bcadastros_etl.normalization import normalize_cnpj, normalize_numeric_document


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("12345678000195", "12345678000195"),
        ("12.345.678/0001-95", "12345678000195"),
        ("12345678000A08", "12345678000A08"),
        ("12.345.678/000A-08", "12345678000A08"),
        ("AA.345.678/0003-29", "AA345678000329"),
        ("aa.345.678/0003-29", "AA345678000329"),
        ("  00.000.000/000A-08  ", "00000000000A08"),
    ],
)
def test_normalize_cnpj_accepts_numeric_and_alphanumeric_values(
    raw: str,
    expected: str,
) -> None:
    assert normalize_cnpj(raw) == expected


@pytest.mark.parametrize(
    "raw",
    [
        "12.345.678/0001-AA",
        "12.345.678/000@-08",
        "12-345-678/0001-95",
        "1234567800019",
        "123456780001950",
        "ÁA345678000329",
        "",
        "   ",
        True,
        False,
        12345678000195,
        None,
    ],
)
def test_normalize_cnpj_rejects_invalid_values(raw: object) -> None:
    assert normalize_cnpj(raw) is None


def test_numeric_document_remains_numeric_only_and_preserves_zeros() -> None:
    assert normalize_numeric_document("012.345.678-90", 11) == "01234567890"
    assert normalize_numeric_document("0123456789A", 11) is None
    assert normalize_numeric_document("012/345/678-90", 11) is None
    assert normalize_numeric_document(-12345678901, 11) is None
    assert normalize_numeric_document(True, 11) is None
