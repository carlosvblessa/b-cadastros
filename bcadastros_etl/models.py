"""Modelos ordenados usados na fronteira JSONL."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from decimal import Decimal
from typing import Any


@dataclass(frozen=True, slots=True)
class AccountantSelection:
    document: str | None
    document_type: str | None
    uf: str | None
    source: str | None
    outside_al: str | None


@dataclass(frozen=True, slots=True)
class CadastroRecord:
    NUM_CNPJ: str
    NOM_RAZAO_SOCIAL: str | None
    NUM_DOC_RESP: str | None
    COD_TIPDOC_RESP: str | None
    NOM_RAZAO_SOCIAL_RESP: str | None
    QTD_EMPRESAS_RESP: int
    IND_OPCAO_SIMPLES: str
    IND_SIMPLES_AUXILIAR: str
    IND_MEI: str
    IND_MEI_AUXILIAR: str
    VAL_CAPITAL_SOCIAL_PJ: Decimal | None
    DTH_INICIO_CNPJ: str | None
    DTH_TERMINO_CNPJ: str | None
    DTH_INICIO_RAIZ: str | None
    DSC_PORTE: str | None
    IND_MATRIZ: str
    COD_CNAE: str | None
    DSC_SITUACAO_CADASTRAL_CNPJ: str | None
    DTH_SITUACADA_CNPJ: str | None
    DSC_MOTIVO_SITUCADA_CNPJ: str | None
    NUM_DOC_CONTADOR: str | None
    COD_TIPDOC_CONTADOR: str | None
    NOM_RAZAO_SOCIAL_CONT: str | None
    IND_ENDERECO_CONT_FORA_AL: str | None
    NOM_EMAIL_CAD: str | None
    QTD_SOCIO_RAIZ: int

    def to_dict(self) -> dict[str, Any]:
        """Mantem a ordem declarada das colunas na serializacao."""
        return asdict(self)


@dataclass(frozen=True, slots=True)
class ErrorRecord:
    cnpj: str
    etapa: str
    tipo_erro: str
    mensagem: str

    def to_dict(self) -> dict[str, str]:
        return asdict(self)


class RecordProcessingError(Exception):
    """Erro isolavel com a etapa funcional que falhou."""

    def __init__(self, stage: str, cause: Exception) -> None:
        super().__init__(str(cause))
        self.stage = stage
        self.cause = cause


class DocumentNotFoundError(Exception):
    """Documento principal necessario para compor o registro nao existe."""
