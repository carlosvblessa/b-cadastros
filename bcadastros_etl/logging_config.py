"""Logging sempre direcionado a stderr pela biblioteca padrao."""

from __future__ import annotations

import logging
import sys


def configure_logging(level: str) -> None:
    numeric_level = getattr(logging, level.upper())
    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
        stream=sys.stderr,
    )
    logging.getLogger().setLevel(numeric_level)
