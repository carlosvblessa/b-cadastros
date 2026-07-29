from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pytest
import requests

from bcadastros_etl.couchdb_client import (
    CouchDBClient,
    CouchDBHTTPError,
    CouchDBPaginationError,
    CouchDBResponseError,
    CouchDBTransportError,
)


class FakeResponse:
    def __init__(
        self,
        status_code: int,
        payload: Any = None,
        *,
        headers: Mapping[str, str] | None = None,
        json_error: bool = False,
    ) -> None:
        self.status_code = status_code
        self.payload = payload
        self.headers = dict(headers or {})
        self.json_error = json_error

    def json(self) -> Any:
        if self.json_error:
            raise ValueError("invalid json")
        return self.payload


class FakeSession:
    def __init__(self, outcomes: list[Any]) -> None:
        self.outcomes = outcomes
        self.calls: list[dict[str, Any]] = []
        self.auth: tuple[str, str] | None = None
        self.headers: dict[str, str] = {}
        self.closed = False

    def request(self, method: str, url: str, **kwargs: Any) -> FakeResponse:
        self.calls.append({"method": method, "url": url, **kwargs})
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    def close(self) -> None:
        self.closed = True


def test_rule_i_paginates_all_bookmarks(couch_config) -> None:
    session = FakeSession(
        [
            FakeResponse(200, {"docs": [{"_id": "a"}, {"_id": "b"}], "bookmark": "bm1"}),
            FakeResponse(200, {"docs": [{"_id": "c"}], "bookmark": "bm2"}),
        ]
    )
    client = CouchDBClient(couch_config, session_factory=lambda: session, sleep=lambda _: None)

    assert client.count_companies_for_responsible("01234567890") == 3
    assert len(session.calls) == 2
    assert "bookmark" not in session.calls[0]["json"]
    assert session.calls[1]["json"]["bookmark"] == "bm1"
    assert session.calls[0]["json"]["fields"] == ["_id"]


def test_rule_i_rejects_repeated_bookmark_without_caching_partial_count(
    couch_config,
) -> None:
    session = FakeSession(
        [
            FakeResponse(200, {"docs": [{}, {}], "bookmark": "same"}),
            FakeResponse(200, {"docs": [{}, {}], "bookmark": "same"}),
        ]
    )
    client = CouchDBClient(couch_config, session_factory=lambda: session, sleep=lambda _: None)

    with pytest.raises(CouchDBPaginationError, match="repetiu"):
        client.count_companies_for_responsible("01234567890")
    metrics = client.cache_metrics()["responsible_count"]
    assert metrics.current_size == 0
    assert metrics.insertions == 0


@pytest.mark.parametrize("bookmark", [None, "", "   "])
def test_rule_i_rejects_missing_or_empty_bookmark_after_full_page(
    couch_config,
    bookmark: str | None,
) -> None:
    payload: dict[str, Any] = {"docs": [{}, {}]}
    if bookmark is not None:
        payload["bookmark"] = bookmark
    session = FakeSession([FakeResponse(200, payload)])
    client = CouchDBClient(couch_config, session_factory=lambda: session)
    with pytest.raises(CouchDBPaginationError, match="sem bookmark"):
        client.count_companies_for_responsible("01234567890")


def test_rule_i_rejects_bookmark_used_in_an_earlier_page(couch_config) -> None:
    session = FakeSession(
        [
            FakeResponse(200, {"docs": [{}, {}], "bookmark": "bm1"}),
            FakeResponse(200, {"docs": [{}, {}], "bookmark": "bm2"}),
            FakeResponse(200, {"docs": [{}, {}], "bookmark": "bm1"}),
        ]
    )
    client = CouchDBClient(couch_config, session_factory=lambda: session)
    with pytest.raises(CouchDBPaginationError, match="reutilizou"):
        client.count_companies_for_responsible("01234567890")


def test_rule_i_empty_docs_is_a_valid_zero_count(couch_config) -> None:
    session = FakeSession([FakeResponse(200, {"docs": []})])
    client = CouchDBClient(couch_config, session_factory=lambda: session)
    assert client.count_companies_for_responsible("01234567890") == 0


@pytest.mark.parametrize("payload", [{}, {"docs": None}, {"docs": "invalido"}])
def test_rule_i_rejects_invalid_docs(couch_config, payload) -> None:
    session = FakeSession([FakeResponse(200, payload)])
    client = CouchDBClient(couch_config, session_factory=lambda: session)
    with pytest.raises(CouchDBResponseError):
        client.count_companies_for_responsible("01234567890")


def test_transient_http_failures_are_retried(config_factory) -> None:
    sleeps: list[float] = []
    session = FakeSession(
        [
            FakeResponse(503, {}),
            FakeResponse(429, {}, headers={"Retry-After": "0"}),
            FakeResponse(200, {"_id": "00123456"}),
        ]
    )
    client = CouchDBClient(
        config_factory(max_attempts=3, backoff_factor=0.25),
        session_factory=lambda: session,
        sleep=sleeps.append,
    )

    assert client.get_root_projection("00123456") is not None
    assert sleeps == [0.25, 0.0]
    assert len(session.calls) == 3


def test_timeout_is_retried(config_factory) -> None:
    session = FakeSession([requests.Timeout(), FakeResponse(200, {"_id": "00123456"})])
    client = CouchDBClient(
        config_factory(max_attempts=2),
        session_factory=lambda: session,
        sleep=lambda _: None,
    )
    assert client.get_root_projection("00123456") is not None


def test_timeout_exhaustion_has_transport_error(config_factory) -> None:
    session = FakeSession([requests.Timeout(), requests.Timeout()])
    client = CouchDBClient(
        config_factory(max_attempts=2),
        session_factory=lambda: session,
        sleep=lambda _: None,
    )
    with pytest.raises(CouchDBTransportError, match="2 tentativas"):
        client.get_establishment_document("00123456000199")


def test_transient_http_exhaustion_has_safe_error(config_factory) -> None:
    session = FakeSession([FakeResponse(503, {}), FakeResponse(503, {})])
    client = CouchDBClient(
        config_factory(max_attempts=2),
        session_factory=lambda: session,
        sleep=lambda _: None,
    )
    with pytest.raises(CouchDBHTTPError, match="HTTP 503 apos 2 tentativas") as caught:
        client.get_establishment_document("00123456000199")
    assert "secret" not in str(caught.value)


def test_invalid_json_and_non_object_document_are_rejected(couch_config) -> None:
    invalid_json = FakeSession([FakeResponse(200, json_error=True)])
    client = CouchDBClient(couch_config, session_factory=lambda: invalid_json)
    with pytest.raises(CouchDBResponseError, match="JSON valido"):
        client.get_establishment_document("00123456000199")

    non_object = FakeSession([FakeResponse(200, [1, 2])])
    client = CouchDBClient(couch_config, session_factory=lambda: non_object)
    with pytest.raises(CouchDBResponseError, match="objeto JSON"):
        client.get_establishment_document("00123456000199")


def test_establishments_are_never_cached(couch_config) -> None:
    session = FakeSession(
        [
            FakeResponse(200, {"_id": "AA345678000329"}),
            FakeResponse(200, {"_id": "AA345678000329"}),
        ]
    )
    client = CouchDBClient(couch_config, session_factory=lambda: session)

    assert client.get_establishment_document("AA345678000329") is not None
    assert client.get_establishment_document("AA345678000329") is not None
    assert len(session.calls) == 2
    assert all(metric.current_size == 0 for metric in client.cache_metrics().values())


def test_root_projection_is_compact_and_cached(couch_config) -> None:
    session = FakeSession(
        [
            FakeResponse(
                200,
                {
                    "_id": "AA345678",
                    "nomeEmpresarial": "Empresa",
                    "cpfResponsavel": "01234567890",
                    "capitalSocial": "100",
                    "porteEmpresa": "01",
                    "socios": [{"payload": "large"}, {"payload": "large"}],
                    "campoDesnecessario": {"large": [1, 2, 3]},
                },
            )
        ]
    )
    client = CouchDBClient(couch_config, session_factory=lambda: session)

    first = client.get_root_projection("AA345678")
    second = client.get_root_projection("AA345678")

    assert first is second
    assert first is not None
    assert first.partner_count == 2
    assert not hasattr(first, "socios")
    assert not hasattr(first, "campoDesnecessario")
    assert len(session.calls) == 1
    metrics = client.cache_metrics()["root"]
    assert metrics.hits == 1
    assert metrics.misses == 1
    assert metrics.current_size == 1


def test_negative_root_result_is_cached(couch_config) -> None:
    session = FakeSession([FakeResponse(404, {})])
    client = CouchDBClient(couch_config, session_factory=lambda: session)
    assert client.get_root_projection("00123456") is None
    assert client.get_root_projection("00123456") is None
    assert len(session.calls) == 1


def test_count_result_is_cached(couch_config) -> None:
    session = FakeSession([FakeResponse(200, {"docs": [{}]})])
    client = CouchDBClient(couch_config, session_factory=lambda: session)
    assert client.count_companies_for_responsible("01234567890") == 1
    assert client.count_companies_for_responsible("01234567890") == 1
    assert len(session.calls) == 1
    assert client.cache_metrics()["responsible_count"].hits == 1


def test_person_and_accountant_names_use_separate_bounded_caches(couch_config) -> None:
    session = FakeSession([FakeResponse(200, {"nomeContribuinte": "Pessoa Contadora"})])
    client = CouchDBClient(couch_config, session_factory=lambda: session)

    assert client.get_person_name("01234567890") == "Pessoa Contadora"
    assert client.get_person_name("01234567890") == "Pessoa Contadora"
    assert client.get_accountant_name("CPF", "01234567890") == "Pessoa Contadora"
    assert client.get_accountant_name("CPF", "01234567890") == "Pessoa Contadora"

    assert len(session.calls) == 1
    metrics = client.cache_metrics()
    assert metrics["person"].current_size == 1
    assert metrics["person"].hits == 2
    assert metrics["accountant"].current_size == 1
    assert metrics["accountant"].hits == 1
