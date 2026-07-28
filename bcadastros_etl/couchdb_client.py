"""Cliente HTTP somente leitura para os bancos CouchDB do b-cadastros."""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable, Mapping
from typing import Any
from urllib.parse import quote

import requests
from requests.adapters import HTTPAdapter

from .config import CouchDBConfig

LOGGER = logging.getLogger(__name__)


class CouchDBError(RuntimeError):
    """Falha segura de comunicacao ou protocolo com o CouchDB."""


class CouchDBHTTPError(CouchDBError):
    """Resposta HTTP nao recuperavel."""


class CouchDBResponseError(CouchDBError):
    """Resposta JSON ausente ou estruturalmente invalida."""


class CouchDBClient:
    def __init__(
        self,
        config: CouchDBConfig,
        *,
        session_factory: Callable[[], requests.Session] | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.config = config
        self._session_factory = session_factory or self._build_session
        self._sleep = sleep
        self._local = threading.local()
        self._sessions: list[requests.Session] = []
        self._sessions_lock = threading.Lock()
        self._document_cache: dict[tuple[str, str], Mapping[str, Any] | None] = {}
        self._count_cache: dict[str, int] = {}
        self._cache_lock = threading.Lock()

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
        with self._sessions_lock:
            sessions = tuple(self._sessions)
            self._sessions.clear()
        for session in sessions:
            session.close()

    def __enter__(self) -> CouchDBClient:
        return self

    def __exit__(self, *_args: object) -> None:
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
                raise CouchDBError(
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

    def get_document(self, database: str, document_id: str) -> Mapping[str, Any] | None:
        cache_key = (database, document_id)
        with self._cache_lock:
            if cache_key in self._document_cache:
                return self._document_cache[cache_key]

        url = f"{self.config.base_url}/{quote(database, safe='')}/{quote(document_id, safe='')}"
        response = self._request_json("GET", url, allow_not_found=True)
        if response is not None and not isinstance(response, Mapping):
            raise CouchDBResponseError("Documento CouchDB nao e um objeto JSON")

        with self._cache_lock:
            self._document_cache[cache_key] = response
        return response

    def get_cnpj_document(self, document_id: str) -> Mapping[str, Any] | None:
        return self.get_document(self.config.cnpj_database, document_id)

    def get_cpf_document(self, cpf: str) -> Mapping[str, Any] | None:
        return self.get_document(self.config.cpf_database, f"{self.config.cpf_id_prefix}{cpf}")

    def get_simples_document(self, cnpj_root: str) -> Mapping[str, Any] | None:
        return self.get_document(self.config.simples_database, cnpj_root)

    def count_companies_for_responsible(self, cpf: str) -> int:
        """Regra I com paginacao defensiva por bookmark."""
        with self._cache_lock:
            cached = self._count_cache.get(cpf)
        if cached is not None:
            return cached

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
            if bookmark:
                body["bookmark"] = bookmark

            response = self._request_json("POST", url, json_body=body)
            if not isinstance(response, Mapping):
                raise CouchDBResponseError("Resposta de _find nao e um objeto JSON")

            docs = response.get("docs", [])
            if not isinstance(docs, list):
                raise CouchDBResponseError("Campo docs de _find nao e uma lista")
            page_size = len(docs)
            total += page_size
            if page_size < self.config.page_limit:
                break

            new_bookmark_raw = response.get("bookmark")
            new_bookmark = (
                new_bookmark_raw.strip() if isinstance(new_bookmark_raw, str) else ""
            )
            if not new_bookmark:
                LOGGER.warning("Paginacao encerrada: CouchDB retornou bookmark vazio")
                break
            if new_bookmark == bookmark or new_bookmark in seen_bookmarks:
                LOGGER.warning("Paginacao encerrada: CouchDB repetiu o bookmark")
                break
            seen_bookmarks.add(new_bookmark)
            bookmark = new_bookmark

        with self._cache_lock:
            self._count_cache[cpf] = total
        return total
