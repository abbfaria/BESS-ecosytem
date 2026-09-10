"""Structured logging setup (structlog + stdlib fallback)."""

from __future__ import annotations

import logging
import os
import sys
from typing import Any


def _configure_stdlib() -> None:
    level_str = os.environ.get("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_str, logging.INFO)
    fmt = "%(asctime)s %(levelname)-8s %(name)s %(message)s"
    logging.basicConfig(stream=sys.stdout, level=level, format=fmt)


try:
    import structlog  # type: ignore

    _configure_stdlib()

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.dev.ConsoleRenderer() if sys.stdout.isatty()
            else structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper(), logging.INFO)
        ),
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
    )

    def get_logger(name: str) -> Any:
        return structlog.get_logger(name)

except ImportError:
    _configure_stdlib()

    class _KwLogger:
        """Stdlib logger wrapper accepting structlog-style keyword arguments."""
        def __init__(self, logger: logging.Logger) -> None:
            self._l = logger

        def _fmt(self, msg: str, kw: dict) -> str:
            if kw:
                extras = "  " + "  ".join(f"{k}={v!r}" for k, v in kw.items())
                return msg + extras
            return msg

        def debug(self, msg: str, **kw: Any)    -> None: self._l.debug(self._fmt(msg, kw))
        def info(self, msg: str, **kw: Any)     -> None: self._l.info(self._fmt(msg, kw))
        def warning(self, msg: str, **kw: Any)  -> None: self._l.warning(self._fmt(msg, kw))
        def error(self, msg: str, **kw: Any)    -> None: self._l.error(self._fmt(msg, kw))
        def critical(self, msg: str, **kw: Any) -> None: self._l.critical(self._fmt(msg, kw))

    def get_logger(name: str) -> Any:  # type: ignore[misc]
        return _KwLogger(logging.getLogger(name))
