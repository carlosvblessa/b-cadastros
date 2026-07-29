"""Typed projections and ordered models used at the JSONL boundary."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass
from decimal import Decimal
from typing import Any


@dataclass(frozen=True, slots=True)
class AccountantSelection:
    """Accountant document selected according to functional Rule G."""

    document: str | None
    document_type: str | None
    uf: str | None
    source: str | None
    outside_al: str | None


@dataclass(frozen=True, slots=True)
class CadastroRecord:
    """Successful cadastral output record in the stable JSONL field order."""

    NUM_CNPJ: str
    NOM_RAZAO_SOCIAL: str | None
    NUM_DOC_RESP: str | None
    COD_TIPDOC_RESP: str
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
        """Convert the record while preserving the declared field order."""
        return asdict(self)


@dataclass(frozen=True, slots=True)
class ErrorRecord:
    """Isolated input or processing error written to the error JSONL."""

    cnpj: str
    etapa: str
    tipo_erro: str
    mensagem: str

    def to_dict(self) -> dict[str, str]:
        """Convert the error record to its public JSONL representation."""
        return asdict(self)


@dataclass(frozen=True, slots=True)
class RootProjection:
    """Compact subset of a root document required by transformations."""

    company_name: object
    responsible_cpf: object
    share_capital: object
    company_size: object
    partner_count: int

    @staticmethod
    def _scalar(value: object) -> object:
        """Discard nested unexpected values that would defeat compaction."""
        if value is None or isinstance(value, (str, int, float)):
            return None if isinstance(value, bool) else value
        return None

    @classmethod
    def from_document(cls, document: dict[str, Any] | Mapping[str, Any]) -> RootProjection:
        """Build a compact projection without retaining the partners list.

        Args:
            document: Full CouchDB root document.

        Returns:
            A compact immutable projection.
        """
        partners = document.get("socios")
        partner_count = len(partners) if isinstance(partners, list) else 0
        return cls(
            company_name=cls._scalar(document.get("nomeEmpresarial")),
            responsible_cpf=cls._scalar(document.get("cpfResponsavel")),
            share_capital=cls._scalar(document.get("capitalSocial")),
            company_size=cls._scalar(document.get("porteEmpresa")),
            partner_count=partner_count,
        )

    def to_mapping(self) -> dict[str, object]:
        """Return source field names expected by transformation functions."""
        return {
            "nomeEmpresarial": self.company_name,
            "cpfResponsavel": self.responsible_cpf,
            "capitalSocial": self.share_capital,
            "porteEmpresa": self.company_size,
        }


@dataclass(frozen=True, slots=True)
class SimplesProjection:
    """Compact Simples/MEI values used by Rules A and B."""

    simples_period: object
    mei_period: object

    @classmethod
    def from_document(cls, document: dict[str, Any] | Mapping[str, Any]) -> SimplesProjection:
        """Build a projection containing only Simples and MEI periods.

        Args:
            document: Full CouchDB Simples document.

        Returns:
            A compact immutable projection.
        """
        return cls(
            simples_period=document.get("PeriodoSimples"),
            mei_period=document.get("PeriodoMEI"),
        )

    def to_mapping(self) -> dict[str, object]:
        """Return source field names expected by transformation functions."""
        return {
            "PeriodoSimples": self.simples_period,
            "PeriodoMEI": self.mei_period,
        }


class RecordProcessingError(Exception):
    """Erro isolavel com a etapa funcional que falhou."""

    def __init__(self, stage: str, cause: Exception) -> None:
        """Initialize an isolated error with functional-stage context."""
        super().__init__(str(cause))
        self.stage = stage
        self.cause = cause


class DocumentNotFoundError(Exception):
    """Documento principal necessario para compor o registro nao existe."""
