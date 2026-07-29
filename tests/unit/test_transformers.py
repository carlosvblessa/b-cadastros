from __future__ import annotations

import logging
from decimal import Decimal

import pytest

from bcadastros_etl.mappings import MOTIVO_SITUACAO
from bcadastros_etl.transformers import (
    build_record,
    format_timestamp,
    normalize_document,
    normalize_text,
    parse_capital_social,
    rule_a,
    rule_b,
    rule_c,
    rule_d,
    rule_e,
    rule_f,
    rule_g,
    rule_h,
)


@pytest.mark.parametrize(
    ("period", "expected"),
    [
        (None, "N"),
        ("", "N"),
        (True, "N"),
        ({}, "N"),
        ({"Inicio": "2025-01-01", "Fim": None}, "S"),
        ({"Inicio": " 2025-01-01 ", "Fim": "  "}, "S"),
        ({"Inicio": "2025-01-01", "Fim": "2025-12-31"}, "N"),
        ({"Inicio": "", "Fim": ""}, "N"),
        ('{"Inicio":"2025-01-01","Fim":null}', "S"),
    ],
)
def test_rule_a(period: object, expected: str) -> None:
    assert rule_a(period) == expected


def test_rule_a_uses_latest_period_from_list() -> None:
    periods = [
        {"Inicio": "2020-01-01", "Fim": None},
        {"Inicio": "2025-01-01", "Fim": "2025-02-01"},
    ]
    assert rule_a(periods) == "N"


def test_invalid_period_json_warns_and_does_not_stop(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.WARNING):
        assert rule_a("{invalido") == "N"
    assert "JSON invalido" in caplog.text


@pytest.mark.parametrize("status", ["01", "1", "04", "4", "08", "8"])
def test_rule_b_matches_terminal_status_and_date_ignoring_time(status: str) -> None:
    period = {"Inicio": "2020-01-01", "Fim": "2025-12-31T23:59:59-03:00"}
    assert rule_b(period, status, "20251231") == "S"


def test_rule_b_inherits_active_main_indicator() -> None:
    assert rule_b({"Inicio": "2025-01-01", "Fim": None}, "02", "invalida") == "S"


@pytest.mark.parametrize(
    ("period", "status", "status_date"),
    [
        ({"Inicio": "2020-01-01", "Fim": "2025-12-31"}, "02", "20251231"),
        ({"Inicio": "2020-01-01", "Fim": "2025-12-30"}, "08", "20251231"),
        ({"Inicio": "2020-01-01", "Fim": "invalida"}, "08", "20251231"),
        ({"Inicio": "2020-01-01", "Fim": "2025-12-31"}, "08", "invalida"),
    ],
)
def test_rule_b_non_matching_cases(period: object, status: str, status_date: str) -> None:
    assert rule_b(period, status, status_date) == "N"


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, None),
        (" ", None),
        (1, "NULA"),
        ("02", "ATIVA"),
        ("3", "SUSPENSA"),
        ("04", "INAPTA"),
        ("05", "ATIVA NAO REGULAR"),
        ("08", "BAIXADO"),
        ("99", "NAO INFORMADA"),
    ],
)
def test_rule_c(value: object, expected: str | None) -> None:
    assert rule_c(value) == expected


def test_rule_d() -> None:
    assert rule_d("4", "20250131") == "2025-01-31T00:00:00"
    assert rule_d("02", "20250131") is None
    assert rule_d("08", "20250230") is None
    assert rule_d(None, "20250131") is None


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, None),
        (" ", None),
        (1, "MICRO-EMPRESA"),
        ("03", "EMPRESA PEQUENO PORTE"),
        (5, "DEMAIS"),
        ("77", "NAO INFORMADO"),
    ],
)
def test_rule_e(value: object, expected: str | None) -> None:
    assert rule_e(value) == expected


def test_rule_f_contains_complete_local_mapping() -> None:
    for code, description in MOTIVO_SITUACAO.items():
        assert rule_f(code) == description
    assert rule_f(1) == MOTIVO_SITUACAO["01"]
    assert rule_f("99") == "NAO INFORMADO"
    assert rule_f(" ") is None


def test_rule_g_prioritizes_masked_pj_and_uses_pj_uf() -> None:
    selected = rule_g(
        {
            "contadorPJ": " 12.345.678/0001-90 ",
            "contadorPF": "123.456.789-01",
            "ufCrcContadorPJ": " pe ",
            "ufCrcContadorPF": "AL",
        }
    )
    assert selected.document == "12345678000190"
    assert selected.document_type == "CNPJ"
    assert selected.source == "PJ"
    assert selected.uf == "PE"
    assert selected.outside_al == "S"


def test_rule_g_selects_pf_when_pj_is_empty() -> None:
    selected = rule_g({"contadorPJ": " ", "contadorPF": "012.345.678-90", "ufCrcContadorPF": "al"})
    assert selected.document == "01234567890"
    assert selected.document_type == "CPF"
    assert selected.source == "PF"
    assert selected.outside_al == "N"


def test_rule_g_handles_absent_and_invalid_accountant() -> None:
    assert rule_g({}).document is None
    selected = rule_g({"contadorPJ": "123", "ufCrcContadorPJ": ""})
    assert selected.document is None
    assert selected.document_type is None
    assert selected.source == "PJ"
    assert selected.outside_al is None


def test_rule_g_accepts_alphanumeric_pj_and_rejects_alphanumeric_pf() -> None:
    selected_pj = rule_g(
        {
            "contadorPJ": "ab.345.678/000a-08",
            "contadorPF": "111.222.333-44",
            "ufCrcContadorPJ": "AL",
        }
    )
    assert selected_pj.document == "AB345678000A08"
    assert selected_pj.document_type == "CNPJ"
    assert selected_pj.source == "PJ"

    selected_pf = rule_g({"contadorPF": "111.222.33A-44"})
    assert selected_pf.document is None
    assert selected_pf.document_type is None
    assert selected_pf.source == "PF"


def test_rule_h_counts_lists_and_warns_on_unexpected_type(
    caplog: pytest.LogCaptureFixture,
) -> None:
    assert rule_h(None) == 0
    assert rule_h([]) == 0
    assert rule_h([{}, {}, {}]) == 3
    with caplog.at_level(logging.WARNING):
        assert rule_h({"socio": 1}) == 0
    assert "Tipo inesperado" in caplog.text


def test_document_text_date_and_capital_helpers() -> None:
    assert normalize_document("012.345.678-90", 11) == "01234567890"
    assert normalize_document("123", 11) is None
    assert normalize_document("012.345.67A-90", 11) is None
    assert normalize_text("  José da Silva  ") == "JOSÉ DA SILVA"
    assert normalize_text(" USUARIO@EXEMPLO.COM ", lowercase=True) == "usuario@exemplo.com"
    assert normalize_text("   ") is None
    assert format_timestamp("2025-12-31T23:00:00-03:00") == "2025-12-31T00:00:00"
    assert format_timestamp("20250230") is None
    assert parse_capital_social("00000018000000") == Decimal("180000.00")
    assert parse_capital_social("00000001800000") == Decimal("18000.00")
    assert parse_capital_social("") is None
    assert parse_capital_social("18,00") is None
    assert parse_capital_social(True) is None


def test_build_record_has_stable_complete_fields_and_normalization() -> None:
    record = build_record(
        "00123456000199",
        {
            "nomeEmpresarial": " Empresa Árvore Ltda ",
            "cpfResponsavel": "012.345.678-90",
            "capitalSocial": "00000001800000",
            "porteEmpresa": "1",
            "socios": [{}, {}],
        },
        {
            "dataInicioAtividade": "20200102",
            "situacaoCadastral": "08",
            "dataSituacaoCadastral": "20251231",
            "indicadorMatriz": "1",
            "cnaeFiscal": "47.89-0-05",
            "motivoSituacao": "1",
            "email": " CADASTRO@EXEMPLO.COM ",
        },
        {"PeriodoSimples": None, "PeriodoMEI": None},
        responsible_name=" José Ávila ",
        responsible_company_count=2,
        accountant_name=None,
    )
    data = record.to_dict()
    assert list(data)[:6] == [
        "NUM_CNPJ",
        "NOM_RAZAO_SOCIAL",
        "NUM_DOC_RESP",
        "COD_TIPDOC_RESP",
        "NOM_RAZAO_SOCIAL_RESP",
        "QTD_EMPRESAS_RESP",
    ]
    assert len(data) == 26
    assert data["NOM_RAZAO_SOCIAL"] == "EMPRESA ÁRVORE LTDA"
    assert data["NOM_EMAIL_CAD"] == "cadastro@exemplo.com"
    assert data["DTH_INICIO_CNPJ"] == "2020-01-02T00:00:00"
    assert data["DTH_INICIO_RAIZ"] == data["DTH_INICIO_CNPJ"]
    assert data["DTH_TERMINO_CNPJ"] == "2025-12-31T00:00:00"
    assert data["COD_CNAE"] == "4789005"
    assert data["QTD_SOCIO_RAIZ"] == 2


def test_build_record_uses_fixed_responsible_document_type_when_absent() -> None:
    record = build_record(
        "AA345678000329",
        {
            "nomeEmpresarial": "Empresa alfanumérica",
            "cpfResponsavel": None,
            "capitalSocial": None,
            "porteEmpresa": None,
        },
        {"indicadorMatriz": "0"},
        None,
        responsible_name=None,
        responsible_company_count=0,
        accountant_name=None,
        partner_count=0,
    )
    assert record.NUM_CNPJ == "AA345678000329"
    assert record.NUM_DOC_RESP is None
    assert record.COD_TIPDOC_RESP == "CPF"
