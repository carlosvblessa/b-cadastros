"""Read-only HTTP client with bounded caches for b-cadastros CouchDB."""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable, Mapping
from typing import Any
from urllib.parse import quote

import requests
from requests.adapters import HTTPAdapter

from .cache import CacheMetrics, ThreadSafeLRUCache
from .config import CouchDBConfig
from .models import RootProjection, SimplesProjection

LOGGER = logging.getLogger(__name__)


class CouchDBError(RuntimeError):
    """Base error for safe CouchDB transport or protocol failures."""


class CouchDBTransportError(CouchDBError):
    """Network connection or timeout failed after configured retries."""


class CouchDBHTTPError(CouchDBError):
    """CouchDB returned an unsuccessful HTTP status."""


class CouchDBResponseError(CouchDBError):
    """CouchDB returned invalid JSON or an unexpected JSON structure."""


class CouchDBPaginationError(CouchDBResponseError):
    """Mango pagination cannot continue without risking a partial count."""


class CouchDBClient:
    """Thread-aware CouchDB client with compact, independently bounded caches.

    Args:
        config: Validated HTTP, retry, concurrency, and cache configuration.
        session_factory: Optional factory used by tests to replace HTTP sessions.
        sleep: Optional retry delay callback.
    """

    def __init__(
        self,
        config: CouchDBConfig,
        *,
        session_factory: Callable[[], requests.Session] | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        """Initialize isolated caches and lazy worker-local HTTP sessions."""
        self.config = config
        self._session_factory = session_factory or self._build_session
        self._sleep = sleep
        self._local = threading.local()
        self._sessions: list[requests.Session] = []
        self._sessions_lock = threading.Lock()
        self._root_cache: ThreadSafeLRUCache[str, RootProjection | None] = ThreadSafeLRUCache(
            config.cache_root_maxsize
        )
        self._simples_cache: ThreadSafeLRUCache[str, SimplesProjection | None] = ThreadSafeLRUCache(
            config.cache_simples_maxsize
        )
        self._person_cache: ThreadSafeLRUCache[str, str | None] = ThreadSafeLRUCache(
            config.cache_person_maxsize
        )
        self._accountant_cache: ThreadSafeLRUCache[tuple[str, str], str | None] = (
            ThreadSafeLRUCache(config.cache_accountant_maxsize)
        )
        self._responsible_count_cache: ThreadSafeLRUCache[str, int] = ThreadSafeLRUCache(
            config.cache_responsible_count_maxsize
        )

    def _build_session(self) -> requests.Session:
        session = requests.Session()
        adapter = HTTPAdapter(
            pool_connections=self.config.workers,
            pool_maxsize=self.config.workers,
            max_retries=0,
        )
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.auth = (self.config.username, self.config.password)
        session.headers.update({"Accept": "application/json"})
        return session

    def _get_session(self) -> requests.Session:
        session = getattr(self._local, "session", None)
        if session is None:
            session = self._session_factory()
            if session.auth is None:
                session.auth = (self.config.username, self.config.password)
            session.headers.update({"Accept": "application/json"})
            self._local.session = session
            with self._sessions_lock:
                self._sessions.append(session)
        return session

    def close(self) -> None:
        """Close every worker-local HTTP session created by this client."""
        with self._sessions_lock:
            sessions = tuple(self._sessions)
            self._sessions.clear()
        for session in sessions:
            session.close()

    def __enter__(self) -> CouchDBClient:
        """Enter a context that closes worker sessions on exit."""
        return self

    def __exit__(self, *_args: object) -> None:
        """Close worker sessions when leaving a context."""
        self.close()

    def _retry_delay(self, attempt: int, response: requests.Response | None = None) -> float:
        if response is not None:
            retry_after = response.headers.get("Retry-After", "").strip()
            try:
                parsed_delay = float(str(retry_after))
                return max(0.0, parsed_delay)
            except ValueError:
                pass
        return float(self.config.backoff_factor * (2.0 ** (attempt - 1)))

    def _request_json(
        self,
        method: str,
        url: str,
        *,
        json_body: Mapping[str, Any] | None = None,
        allow_not_found: bool = False,
    ) -> Any:
        session = self._get_session()
        for attempt in range(1, self.config.max_attempts + 1):
            try:
                response = session.request(
                    method,
                    url,
                    json=json_body,
                    timeout=self.config.timeout,
                    verify=self.config.verify_ssl,
                )
            except (requests.Timeout, requests.ConnectionError) as exc:
                if attempt < self.config.max_attempts:
                    self._sleep(self._retry_delay(attempt))
                    continue
                raise CouchDBTransportError(
                    f"Falha de rede apos {self.config.max_attempts} tentativas"
                ) from exc

            if response.status_code == 404 and allow_not_found:
                return None
            if response.status_code == 429 or 500 <= response.status_code < 600:
                if attempt < self.config.max_attempts:
                    self._sleep(self._retry_delay(attempt, response))
                    continue
                raise CouchDBHTTPError(
                    f"Resposta HTTP {response.status_code} apos "
                    f"{self.config.max_attempts} tentativas"
                )
            if response.status_code >= 400:
                raise CouchDBHTTPError(f"Resposta HTTP {response.status_code}")

            try:
                return response.json()
            except ValueError as exc:
                raise CouchDBResponseError("Resposta do CouchDB nao contem JSON valido") from exc

        raise AssertionError("fluxo de retentativa terminou sem resposta")

    def _fetch_document(
        self,
        database: str,
        document_id: str,
    ) -> Mapping[str, Any] | None:
        url = f"{self.config.base_url}/{quote(database, safe='')}/{quote(document_id, safe='')}"
        response = self._request_json("GET", url, allow_not_found=True)
        if response is not None and not isinstance(response, Mapping):
            raise CouchDBResponseError("Documento CouchDB nao e um objeto JSON")
        return response

    def get_document(self, database: str, document_id: str) -> Mapping[str, Any] | None:
        """Fetch a document without retaining it in a cache.

        This compatibility API is intentionally uncached. Callers that reuse
        root, Simples, person, or accountant data should use their explicit
        projection APIs.

        Args:
            database: CouchDB database name.
            document_id: Exact document identifier.

        Returns:
            The JSON object, or ``None`` for HTTP 404.
        """
        return self._fetch_document(database, document_id)

    def get_cnpj_document(self, document_id: str) -> Mapping[str, Any] | None:
        """Fetch a CNPJ document without cache for compatibility callers."""
        return self._fetch_document(self.config.cnpj_database, document_id)

    def get_establishment_document(self, cnpj: str) -> Mapping[str, Any] | None:
        """Fetch one full establishment document without caching it.

        Args:
            cnpj: Normalized 14-position numeric or alphanumeric CNPJ.

        Returns:
            The establishment object, or ``None`` when not found.
        """
        return self._fetch_document(self.config.cnpj_database, cnpj)

    def get_root_projection(self, cnpj_root: str) -> RootProjection | None:
        """Fetch and cache a compact root projection.

        Args:
            cnpj_root: First eight positions of a normalized CNPJ.

        Returns:
            Compact root data, or ``None`` when not found.
        """

        def load() -> RootProjection | None:
            document = self._fetch_document(self.config.cnpj_database, cnpj_root)
            return RootProjection.from_document(document) if document is not None else None

        return self._root_cache.get_or_load(cnpj_root, load)

    def get_simples_projection(self, cnpj_root: str) -> SimplesProjection | None:
        """Fetch and cache only the Simples and MEI periods for a root.

        Args:
            cnpj_root: First eight positions of a normalized CNPJ.

        Returns:
            Compact periods, or ``None`` when not found.
        """

        def load() -> SimplesProjection | None:
            document = self._fetch_document(self.config.simples_database, cnpj_root)
            return SimplesProjection.from_document(document) if document is not None else None

        return self._simples_cache.get_or_load(cnpj_root, load)

    def get_cpf_document(self, cpf: str) -> Mapping[str, Any] | None:
        """Fetch a full CPF document without caching it.

        Prefer :meth:`get_person_name` when only ``nomeContribuinte`` is used.
        """
        document_id = f"{self.config.cpf_id_prefix}{cpf}"
        return self._fetch_document(self.config.cpf_database, document_id)

    @staticmethod
    def _optional_name(document: Mapping[str, Any] | None, field: str) -> str | None:
        if document is None:
            return None
        value = document.get(field)
        if value is None or isinstance(value, (bool, dict, list, tuple, set)):
            return None
        text = str(value).strip()
        return text or None

    def get_person_name(self, cpf: str) -> str | None:
        """Fetch and cache a person's name using a normalized CPF.

        Args:
            cpf: Eleven-digit normalized CPF.

        Returns:
            Contributor name, or ``None`` when absent.
        """

        def load() -> str | None:
            return self._optional_name(self.get_cpf_document(cpf), "nomeContribuinte")

        return self._person_cache.get_or_load(cpf, load)

    def get_simples_document(self, cnpj_root: str) -> Mapping[str, Any] | None:
        """Return a compatibility mapping backed by the compact periods cache."""
        projection = self.get_simples_projection(cnpj_root)
        return projection.to_mapping() if projection is not None else None

    def get_accountant_name(self, document_type: str, document: str) -> str | None:
        """Fetch and cache an accountant name according to its source type.

        Args:
            document_type: ``CNPJ`` for a PJ accountant or ``CPF`` for PF.
            document: Normalized document selected by Rule G.

        Returns:
            Accountant name, or ``None`` when its document is absent.

        Raises:
            ValueError: If ``document_type`` is unsupported.
        """
        if document_type not in {"CNPJ", "CPF"}:
            raise ValueError("tipo de documento de contador invalido")
        key = (document_type, document)

        def load() -> str | None:
            if document_type == "CPF":
                return self.get_person_name(document)
            projection = self.get_root_projection(document[:8])
            if projection is None:
                return None
            value = projection.company_name
            if value is None or isinstance(value, (bool, dict, list, tuple, set)):
                return None
            text = str(value).strip()
            return text or None

        return self._accountant_cache.get_or_load(key, load)

    def _count_companies_uncached(self, cpf: str) -> int:
        """Count every Mango page or raise before returning a partial total."""
        url = f"{self.config.base_url}/{quote(self.config.cnpj_database, safe='')}/_find"
        bookmark: str | None = None
        seen_bookmarks: set[str] = set()
        total = 0

        while True:
            body: dict[str, Any] = {
                "selector": {"cpfResponsavel": cpf},
                "use_index": [
                    self.config.responsible_index_ddoc,
                    self.config.responsible_index_name,
                ],
                "fields": ["_id"],
                "limit": self.config.page_limit,
            }
            if bookmark is not None:
                body["bookmark"] = bookmark

            response = self._request_json("POST", url, json_body=body)
            if not isinstance(response, Mapping):
                raise CouchDBResponseError("Resposta de _find nao e um objeto JSON")
            if "docs" not in response or response["docs"] is None:
                raise CouchDBResponseError("Campo docs ausente ou nulo na resposta de _find")

            docs = response["docs"]
            if not isinstance(docs, list):
                raise CouchDBResponseError("Campo docs de _find nao e uma lista")
            page_size = len(docs)
            total += page_size
            if page_size < self.config.page_limit:
                return total

            new_bookmark_raw = response.get("bookmark")
            new_bookmark = new_bookmark_raw.strip() if isinstance(new_bookmark_raw, str) else ""
            if not new_bookmark:
                raise CouchDBPaginationError(
                    "Pagina cheia sem bookmark valido; contagem descartada"
                )
            if new_bookmark == bookmark:
                raise CouchDBPaginationError(
                    "CouchDB repetiu o bookmark anterior; contagem descartada"
                )
            if new_bookmark in seen_bookmarks:
                raise CouchDBPaginationError("CouchDB reutilizou um bookmark; contagem descartada")
            seen_bookmarks.add(new_bookmark)
            bookmark = new_bookmark

    def count_companies_for_responsible(self, cpf: str) -> int:
        """Return Rule I's complete count with bounded result caching.

        Args:
            cpf: Eleven-digit normalized responsible CPF.

        Returns:
            Number of matching root documents across all valid pages.

        Raises:
            CouchDBPaginationError: If a full page cannot safely continue.
            CouchDBResponseError: If a response has an invalid structure.
            CouchDBError: If transport or HTTP processing fails.
        """
        return self._responsible_count_cache.get_or_load(
            cpf,
            lambda: self._count_companies_uncached(cpf),
        )

    def cache_metrics(self) -> dict[str, CacheMetrics]:
        """Return safe metrics for every reusable cache."""
        return {
            "root": self._root_cache.metrics(),
            "simples": self._simples_cache.metrics(),
            "person": self._person_cache.metrics(),
            "accountant": self._accountant_cache.metrics(),
            "responsible_count": self._responsible_count_cache.metrics(),
        }
