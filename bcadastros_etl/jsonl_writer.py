"""Serializacao JSON Lines UTF-8 com suporte numerico a Decimal."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TextIO

import simplejson


class JsonlWriter:
    """Write one compact UTF-8 JSON object per line to a text stream."""

    def __init__(self, stream: TextIO) -> None:
        """Initialize a writer without taking ownership of the stream."""
        self.stream = stream

    def write(self, record: Mapping[str, Any]) -> None:
        """Serialize and append one record.

        Args:
            record: JSON-compatible mapping. ``Decimal`` values stay numeric.
        """
        payload = simplejson.dumps(
            record,
            ensure_ascii=False,
            use_decimal=True,
            separators=(",", ":"),
        )
        self.stream.write(f"{payload}\n")

    def flush(self) -> None:
        """Flush buffered output to the underlying stream."""
        self.stream.flush()
