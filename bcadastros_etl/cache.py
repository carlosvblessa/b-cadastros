"""Small thread-safe LRU caches with operational metrics."""

from __future__ import annotations

import threading
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass
from typing import Generic, TypeVar, cast

KeyT = TypeVar("KeyT")
ValueT = TypeVar("ValueT")


@dataclass(frozen=True, slots=True)
class CacheMetrics:
    """Immutable snapshot of one cache's counters and size."""

    hits: int
    misses: int
    insertions: int
    evictions: int
    current_size: int
    max_size: int


class ThreadSafeLRUCache(Generic[KeyT, ValueT]):
    """Bounded least-recently-used cache safe for concurrent worker access.

    Values, including ``None`` when present in ``ValueT``, are distinguished
    from missing keys. Loaders run outside the internal lock so an HTTP request
    for one key does not serialize unrelated keys.

    Args:
        max_size: Maximum number of retained entries. Zero disables storage.

    Raises:
        ValueError: If ``max_size`` is negative or boolean.
    """

    def __init__(self, max_size: int) -> None:
        """Initialize an empty cache with a fixed entry limit."""
        if isinstance(max_size, bool) or max_size < 0:
            raise ValueError("max_size deve ser inteiro nao negativo")
        self._max_size = max_size
        self._entries: OrderedDict[KeyT, ValueT] = OrderedDict()
        self._lock = threading.Lock()
        self._hits = 0
        self._misses = 0
        self._insertions = 0
        self._evictions = 0

    def get(self, key: KeyT) -> tuple[bool, ValueT | None]:
        """Return whether a key exists and its cached value.

        Args:
            key: Cache key.

        Returns:
            A pair containing the presence flag and cached value. The value may
            be ``None`` even when the presence flag is true.
        """
        with self._lock:
            if key not in self._entries:
                self._misses += 1
                return False, None
            value = self._entries[key]
            self._entries.move_to_end(key)
            self._hits += 1
            return True, value

    def put(self, key: KeyT, value: ValueT) -> None:
        """Insert or refresh a value while enforcing the configured limit.

        Args:
            key: Cache key.
            value: Value to retain.
        """
        if self._max_size == 0:
            return
        with self._lock:
            is_new = key not in self._entries
            self._entries[key] = value
            self._entries.move_to_end(key)
            if is_new:
                self._insertions += 1
            if len(self._entries) > self._max_size:
                self._entries.popitem(last=False)
                self._evictions += 1

    def get_or_load(self, key: KeyT, loader: Callable[[], ValueT]) -> ValueT:
        """Return a cached value or load and conditionally retain it.

        Concurrent misses for the same key may execute the loader more than
        once. This avoids holding a global cache lock during network I/O and
        does not compromise the size bound or stored values.

        Args:
            key: Cache key.
            loader: Callback used after a miss.

        Returns:
            The cached or loaded value.
        """
        found, cached = self.get(key)
        if found:
            return cast(ValueT, cached)
        loaded = loader()
        self.put(key, loaded)
        return loaded

    def metrics(self) -> CacheMetrics:
        """Return an atomic snapshot of cache metrics."""
        with self._lock:
            return CacheMetrics(
                hits=self._hits,
                misses=self._misses,
                insertions=self._insertions,
                evictions=self._evictions,
                current_size=len(self._entries),
                max_size=self._max_size,
            )

    def __len__(self) -> int:
        """Return the current number of retained entries."""
        with self._lock:
            return len(self._entries)
