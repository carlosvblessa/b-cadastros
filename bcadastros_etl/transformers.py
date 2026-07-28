"""Regras puras de transformacao descritas em tmp/regras.md."""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from .mappings import MOTIVO_SITUACAO, PORTE_EMPRESA, SITUACAO_CADASTRAL, SITUACOES_TERMINAIS
from .models import AccountantSelection, CadastroRecord

LOGGER = logging.getLogger(__name__)
_DIGITS_RE = re.compile(r"\D+")
_YYYYMMDD_RE = re.compile(r"^\d{8}$")


def is_filled(value: Any) -> bool:
    """Retorna se um valor escalar esta preenchido segundo as regras A/G."""
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    return isinstance(value, (int, float, Decimal))


def normalize_text(value: Any, *, lowercase: bool = False) -> str | None:
    """Remove espacos externos e normaliza caixa sem remover acentos."""
    if value is None or isinstance(value, (dict, list, tuple, set, bool)):
        return None
    text = str(value).strip()
    if not text:
        return None
    return text.lower() if lowercase else text.upper()


def digits_only(value: Any) -> str | None:
    if value is None or isinstance(value, bool):
        return None
    digits = _DIGITS_RE.sub("", str(value))
    return digits or None


def normalize_document(value: Any, expected_length: int) -> str | None:
    document = digits_only(value)
    if document is None or len(document) != expected_length:
        return None
    return document


def normalize_code(value: Any) -> str | None:
    if value is None:
        return None
    code = str(value).strip()
    if not code:
        return None
    return code.zfill(2) if len(code) == 1 else code


def parse_date(value: Any) -> date | None:
    """Aceita YYYYMMDD, ISO date/datetime e objetos date/datetime."""
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        if _YYYYMMDD_RE.fullmatch(text):
            return datetime.strptime(text, "%Y%m%d").date()
        if len(text) == 10:
            return date.fromisoformat(text)
        normalized = text[:-1] + "+00:00" if text.endswith(("Z", "z")) else text
        return datetime.fromisoformat(normalized).date()
    except (ValueError, TypeError):
        return None


def format_timestamp(value: Any) -> str | None:
    parsed = parse_date(value)
    return parsed.strftime("%Y-%m-%dT00:00:00") if parsed else None


def parse_period(value: Any, *, field_name: str = "periodo") -> Mapping[str, Any]:
    """Desserializa periodo e seleciona o mais recente quando a fonte traz uma lista."""
    parsed = value
    if isinstance(value, str):
        if not value.strip():
            return {}
        try:
            parsed = json.loads(value)
        except (json.JSONDecodeError, TypeError):
            LOGGER.warning("JSON invalido em %s; periodo tratado como nao preenchido", field_name)
            return {}

    if isinstance(parsed, Mapping):
        return parsed
    if isinstance(parsed, Sequence) and not isinstance(parsed, (str, bytes, bytearray)):
        candidates = [
            (index, item) for index, item in enumerate(parsed) if isinstance(item, Mapping)
        ]
        if not candidates:
            return {}

        def sort_key(candidate: tuple[int, Mapping[str, Any]]) -> tuple[date, int]:
            index, item = candidate
            return (parse_date(item.get("Inicio")) or date.min, index)

        return max(candidates, key=sort_key)[1]
    if parsed is not None:
        LOGGER.warning("Tipo inesperado em %s; periodo tratado como nao preenchido", field_name)
    return {}


def rule_a(period_value: Any, *, field_name: str = "periodo") -> str:
    """Regra A: periodo ativo quando Inicio existe e Fim nao existe."""
    period = parse_period(period_value, field_name=field_name)
    return "S" if is_filled(period.get("Inicio")) and not is_filled(period.get("Fim")) else "N"


def rule_b(
    period_value: Any,
    situacao_cadastral: Any,
    data_situacao_cadastral: Any,
    *,
    field_name: str = "periodo",
) -> str:
    """Regra B: indicador principal mais excecao por encerramento na mesma data."""
    period = parse_period(period_value, field_name=field_name)
    if rule_a(period, field_name=field_name) == "S":
        return "S"
    status = normalize_code(situacao_cadastral)
    end_date = parse_date(period.get("Fim"))
    status_date = parse_date(data_situacao_cadastral)
    if status in SITUACOES_TERMINAIS and end_date is not None and end_date == status_date:
        return "S"
    return "N"


def rule_c(situacao_cadastral: Any) -> str | None:
    """Regra C: descricao da situacao cadastral do CNPJ."""
    code = normalize_code(situacao_cadastral)
    if code is None:
        return None
    return SITUACAO_CADASTRAL.get(code, "NAO INFORMADA")


def rule_d(situacao_cadastral: Any, data_situacao_cadastral: Any) -> str | None:
    """Regra D: data de termino para situacoes terminais."""
    if normalize_code(situacao_cadastral) not in SITUACOES_TERMINAIS:
        return None
    return format_timestamp(data_situacao_cadastral)


def rule_e(porte_empresa: Any) -> str | None:
    """Regra E: descricao do porte."""
    code = normalize_code(porte_empresa)
    if code is None:
        return None
    return PORTE_EMPRESA.get(code, "NAO INFORMADO")


def rule_f(motivo_situacao: Any) -> str | None:
    """Regra F: fallback local completo do motivo da situacao."""
    code = normalize_code(motivo_situacao)
    if code is None:
        return None
    return MOTIVO_SITUACAO.get(code, "NAO INFORMADO")


def rule_g(establishment: Mapping[str, Any]) -> AccountantSelection:
    """Regra G: seleciona contador PJ antes de PF e a UF da mesma fonte."""
    if is_filled(establishment.get("contadorPJ")):
        source = "PJ"
        raw_document = establishment.get("contadorPJ")
        uf = normalize_text(establishment.get("ufCrcContadorPJ"))
    elif is_filled(establishment.get("contadorPF")):
        source = "PF"
        raw_document = establishment.get("contadorPF")
        uf = normalize_text(establishment.get("ufCrcContadorPF"))
    else:
        return AccountantSelection(None, None, None, None, None)

    document = digits_only(raw_document)
    if document is not None and len(document) == 14:
        document_type = "CNPJ"
    elif document is not None and len(document) == 11:
        document_type = "CPF"
    else:
        document_type = None

    outside_al = None if uf is None else ("N" if uf == "AL" else "S")
    return AccountantSelection(document, document_type, uf, source, outside_al)


def rule_h(socios: Any) -> int:
    """Regra H: quantidade de elementos da lista de socios."""
    if socios is None:
        return 0
    if isinstance(socios, list):
        return len(socios)
    LOGGER.warning(
        "Tipo inesperado no campo socios: %s; quantidade definida como zero",
        type(socios).__name__,
    )
    return 0


def parse_capital_social(value: Any) -> Decimal | None:
    """Converte a representacao em centavos usando Decimal."""
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if not text.isdigit():
        LOGGER.warning("capitalSocial invalido; valor tratado como nulo")
        return None
    try:
        return (Decimal(text) / Decimal(100)).quantize(Decimal("0.01"))
    except InvalidOperation:
        LOGGER.warning("capitalSocial invalido; valor tratado como nulo")
        return None


def _first_present(field: str, *documents: Mapping[str, Any] | None) -> Any:
    for document in documents:
        if document is not None and field in document:
            return document[field]
    return None


def build_record(
    cnpj: str,
    root: Mapping[str, Any],
    establishment: Mapping[str, Any],
    simples: Mapping[str, Any] | None,
    *,
    responsible_name: Any,
    responsible_company_count: int,
    accountant_name: Any,
) -> CadastroRecord:
    """Combina as tres estruturas Couch e aplica as regras locais."""
    responsible_document = normalize_document(root.get("cpfResponsavel"), 11)
    accountant = rule_g(establishment)
    period_simples = _first_present("PeriodoSimples", simples, root, establishment)
    period_mei = _first_present("PeriodoMEI", simples, root, establishment)
    situacao = establishment.get("situacaoCadastral")
    data_situacao = establishment.get("dataSituacaoCadastral")
    data_inicio = establishment.get("dataInicioAtividade")

    return CadastroRecord(
        NUM_CNPJ=cnpj,
        NOM_RAZAO_SOCIAL=normalize_text(root.get("nomeEmpresarial")),
        NUM_DOC_RESP=responsible_document,
        COD_TIPDOC_RESP="CPF" if responsible_document else None,
        NOM_RAZAO_SOCIAL_RESP=normalize_text(responsible_name),
        QTD_EMPRESAS_RESP=responsible_company_count,
        IND_OPCAO_SIMPLES=rule_a(period_simples, field_name="PeriodoSimples"),
        IND_SIMPLES_AUXILIAR=rule_b(
            period_simples,
            situacao,
            data_situacao,
            field_name="PeriodoSimples",
        ),
        IND_MEI=rule_a(period_mei, field_name="PeriodoMEI"),
        IND_MEI_AUXILIAR=rule_b(
            period_mei,
            situacao,
            data_situacao,
            field_name="PeriodoMEI",
        ),
        VAL_CAPITAL_SOCIAL_PJ=parse_capital_social(root.get("capitalSocial")),
        DTH_INICIO_CNPJ=format_timestamp(data_inicio),
        DTH_TERMINO_CNPJ=rule_d(situacao, data_situacao),
        DTH_INICIO_RAIZ=format_timestamp(data_inicio),
        DSC_PORTE=rule_e(root.get("porteEmpresa")),
        IND_MATRIZ="S" if str(establishment.get("indicadorMatriz", "")).strip() == "1" else "N",
        COD_CNAE=digits_only(establishment.get("cnaeFiscal")),
        DSC_SITUACAO_CADASTRAL_CNPJ=rule_c(situacao),
        DTH_SITUACADA_CNPJ=format_timestamp(data_situacao),
        DSC_MOTIVO_SITUCADA_CNPJ=rule_f(establishment.get("motivoSituacao")),
        NUM_DOC_CONTADOR=accountant.document,
        COD_TIPDOC_CONTADOR=accountant.document_type,
        NOM_RAZAO_SOCIAL_CONT=normalize_text(accountant_name),
        IND_ENDERECO_CONT_FORA_AL=accountant.outside_al,
        NOM_EMAIL_CAD=normalize_text(establishment.get("email"), lowercase=True),
        QTD_SOCIO_RAIZ=rule_h(root.get("socios")),
    )
