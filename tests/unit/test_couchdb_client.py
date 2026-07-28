from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pytest
import requests

from bcadastros_etl.couchdb_client import (
    CouchDBClient,
    CouchDBHTTPError,
    CouchDBResponseError,
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


def test_rule_i_stops_on_repeated_bookmark(couch_config, caplog) -> None:
    session = FakeSession(
        [
            FakeResponse(200, {"docs": [{}, {}], "bookmark": "same"}),
            FakeResponse(200, {"docs": [{}, {}], "bookmark": "same"}),
        ]
    )
    client = CouchDBClient(couch_config, session_factory=lambda: session, sleep=lambda _: None)

    assert client.count_companies_for_responsible("01234567890") == 4
    assert "repetiu o bookmark" in caplog.text


@pytest.mark.parametrize("payload", [{}, {"docs": []}])
def test_rule_i_empty_or_missing_docs_returns_zero(couch_config, payload) -> None:
    session = FakeSession([FakeResponse(200, payload)])
    client = CouchDBClient(couch_config, session_factory=lambda: session, sleep=lambda _: None)
    assert client.count_companies_for_responsible("01234567890") == 0


def test_rule_i_rejects_invalid_docs(couch_config) -> None:
    session = FakeSession([FakeResponse(200, {"docs": "invalido"})])
    client = CouchDBClient(couch_config, session_factory=lambda: session, sleep=lambda _: None)
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

    assert client.get_cnpj_document("00123456") == {"_id": "00123456"}
    assert sleeps == [0.25, 0.0]
    assert len(session.calls) == 3


def test_timeout_is_retried(config_factory) -> None:
    session = FakeSession([requests.Timeout(), FakeResponse(200, {"_id": "00123456"})])
    client = CouchDBClient(
        config_factory(max_attempts=2),
        session_factory=lambda: session,
        sleep=lambda _: None,
    )
    assert client.get_cnpj_document("00123456") is not None


def test_transient_http_exhaustion_has_safe_error(config_factory) -> None:
    session = FakeSession([FakeResponse(503, {}), FakeResponse(503, {})])
    client = CouchDBClient(
        config_factory(max_attempts=2),
        session_factory=lambda: session,
        sleep=lambda _: None,
    )
    with pytest.raises(CouchDBHTTPError, match="HTTP 503 apos 2 tentativas") as caught:
        client.get_cnpj_document("00123456")
    assert "secret" not in str(caught.value)


def test_invalid_json_and_non_object_document_are_rejected(couch_config) -> None:
    invalid_json = FakeSession([FakeResponse(200, json_error=True)])
    client = CouchDBClient(couch_config, session_factory=lambda: invalid_json)
    with pytest.raises(CouchDBResponseError, match="JSON valido"):
        client.get_cnpj_document("00123456")

    non_object = FakeSession([FakeResponse(200, [1, 2])])
    client = CouchDBClient(couch_config, session_factory=lambda: non_object)
    with pytest.raises(CouchDBResponseError, match="objeto JSON"):
        client.get_cnpj_document("00123456")


def test_document_and_count_results_are_cached(couch_config) -> None:
    session = FakeSession(
        [
            FakeResponse(200, {"_id": "00123456"}),
            FakeResponse(200, {"docs": [{}]}),
        ]
    )
    client = CouchDBClient(couch_config, session_factory=lambda: session)
    assert client.get_cnpj_document("00123456") is not None
    assert client.get_cnpj_document("00123456") is not None
    assert client.count_companies_for_responsible("01234567890") == 1
    assert client.count_companies_for_responsible("01234567890") == 1
    assert len(session.calls) == 2


def test_404_is_cached_as_absent(couch_config) -> None:
    session = FakeSession([FakeResponse(404, {})])
    client = CouchDBClient(couch_config, session_factory=lambda: session)
    assert client.get_cnpj_document("00123456") is None
    assert client.get_cnpj_document("00123456") is None
    assert len(session.calls) == 1
