"""Serializacao JSON Lines UTF-8 com suporte numerico a Decimal."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TextIO

import simplejson


class JsonlWriter:
    def __init__(self, stream: TextIO) -> None:
        self.stream = stream

    def write(self, record: Mapping[str, Any]) -> None:
        payload = simplejson.dumps(
            record,
            ensure_ascii=False,
            use_decimal=True,
            separators=(",", ":"),
        )
        self.stream.write(f"{payload}\n")

    def flush(self) -> None:
        self.stream.flush()
