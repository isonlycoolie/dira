from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
import time
from typing import ParamSpec, TypeVar

import structlog

try:
    from tenacity import RetryCallState, retry as tenacity_retry
    from tenacity import retry_if_exception_type, stop_after_attempt, wait_exponential
    from tenacity.nap import sleep as tenacity_sleep
except ModuleNotFoundError:
    RetryCallState = object
    tenacity_retry = None
    retry_if_exception_type = None
    stop_after_attempt = None
    wait_exponential = None
    tenacity_sleep = time.sleep

P = ParamSpec("P")
R = TypeVar("R")


@dataclass
class _FallbackOutcome:
    exc: Exception

    @property
    def failed(self) -> bool:
        return True

    def exception(self) -> Exception:
        return self.exc


@dataclass
class _FallbackNextAction:
    sleep: float


@dataclass
class _FallbackRetryState:
    fn: Callable[..., object]
    attempt_number: int
    outcome: _FallbackOutcome
    next_action: _FallbackNextAction


def _log_retry_attempt(retry_state: RetryCallState) -> None:
    logger = structlog.get_logger(__name__)
    exception = None
    if retry_state.outcome is not None and retry_state.outcome.failed:
        exception = retry_state.outcome.exception()

    logger.warning(
        "retrying",
        function=getattr(retry_state.fn, "__name__", "unknown"),
        attempt=retry_state.attempt_number,
        exception=str(exception) if exception is not None else None,
        wait_seconds=getattr(getattr(retry_state, "next_action", None), "sleep", None),
    )


def retry(max_attempts: int = 3, sleep: Callable[[float], None] = tenacity_sleep) -> Callable[[Callable[P, R]], Callable[P, R]]:
    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        if tenacity_retry is None:
            def wrapped(*args: P.args, **kwargs: P.kwargs) -> R:
                for attempt_number in range(1, max_attempts + 1):
                    try:
                        return func(*args, **kwargs)
                    except Exception as exc:  # noqa: BLE001
                        if attempt_number >= max_attempts:
                            raise
                        wait_seconds = min(2 ** (attempt_number - 1), 60)
                        _log_retry_attempt(
                            _FallbackRetryState(
                                fn=func,
                                attempt_number=attempt_number,
                                outcome=_FallbackOutcome(exc),
                                next_action=_FallbackNextAction(wait_seconds),
                            )
                        )
                        sleep(wait_seconds)

                raise RuntimeError("retry loop exhausted unexpectedly")

            return wrapped

        return tenacity_retry(
            stop=stop_after_attempt(max_attempts),
            wait=wait_exponential(min=1, max=60),
            retry=retry_if_exception_type(Exception),
            before_sleep=_log_retry_attempt,
            reraise=True,
            sleep=sleep,
        )(func)

    return decorator
