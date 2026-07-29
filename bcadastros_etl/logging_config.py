"""Logging sempre direcionado a stderr pela biblioteca padrao."""

from __future__ import annotations

import logging
import sys


def configure_logging(level: str) -> None:
    """Configure root logging on stderr without exposing application data.

    Args:
        level: Standard logging level name already validated by configuration.
    """
    numeric_level = getattr(logging, level.upper())
    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
        stream=sys.stderr,
    )
    logging.getLogger().setLevel(numeric_level)
