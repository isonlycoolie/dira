from __future__ import annotations

import logging
import os
import sys

import structlog


def setup_logging(env: str, service_name: str = "dira") -> structlog.BoundLogger:
    log_level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    log_level = getattr(logging, log_level_name, logging.INFO)

    structlog.contextvars.clear_contextvars()
    structlog.contextvars.bind_contextvars(service_name=service_name, env=env)

    renderer: structlog.types.Processor
    if env == "prod":
        renderer = structlog.processors.JSONRenderer(sort_keys=True)
    else:
        renderer = structlog.dev.ConsoleRenderer(colors=True)

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
        cache_logger_on_first_use=True,
    )
    return structlog.get_logger().bind(service_name=service_name, env=env)
