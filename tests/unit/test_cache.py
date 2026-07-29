from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import pytest

from bcadastros_etl.cache import ThreadSafeLRUCache


def test_lru_never_exceeds_limit_and_evicts_least_recently_used() -> None:
    cache: ThreadSafeLRUCache[str, int] = ThreadSafeLRUCache(2)
    cache.put("a", 1)
    cache.put("b", 2)
    assert cache.get("a") == (True, 1)

    cache.put("c", 3)

    assert len(cache) == 2
    assert cache.get("b") == (False, None)
    assert cache.get("a") == (True, 1)
    assert cache.get("c") == (True, 3)


def test_lru_zero_limit_disables_storage() -> None:
    calls = 0
    cache: ThreadSafeLRUCache[str, str] = ThreadSafeLRUCache(0)

    def load() -> str:
        nonlocal calls
        calls += 1
        return "value"

    assert cache.get_or_load("key", load) == "value"
    assert cache.get_or_load("key", load) == "value"
    metrics = cache.metrics()
    assert calls == 2
    assert metrics.current_size == 0
    assert metrics.misses == 2
    assert metrics.insertions == 0


def test_lru_caches_none_and_reports_metrics() -> None:
    calls = 0
    cache: ThreadSafeLRUCache[str, str | None] = ThreadSafeLRUCache(1)

    def load() -> None:
        nonlocal calls
        calls += 1

    assert cache.get_or_load("missing", load) is None
    assert cache.get_or_load("missing", load) is None
    cache.put("other", "value")
    metrics = cache.metrics()
    assert calls == 1
    assert metrics.hits == 1
    assert metrics.misses == 1
    assert metrics.insertions == 2
    assert metrics.evictions == 1
    assert metrics.current_size == 1
    assert metrics.max_size == 1


def test_lru_concurrent_access_remains_bounded() -> None:
    cache: ThreadSafeLRUCache[int, int] = ThreadSafeLRUCache(32)

    def access(index: int) -> None:
        key = index % 100
        cache.put(key, index)
        cache.get(key)

    with ThreadPoolExecutor(max_workers=12) as executor:
        list(executor.map(access, range(10_000)))

    assert len(cache) <= 32
    assert cache.metrics().current_size <= 32


@pytest.mark.parametrize("max_size", [-1, True])
def test_lru_rejects_invalid_limit(max_size: int) -> None:
    with pytest.raises(ValueError):
        ThreadSafeLRUCache(max_size)
