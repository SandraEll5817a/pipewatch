"""Retry policy for webhook notifications and pipeline checks."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Callable, Optional, TypeVar

T = TypeVar("T")


def _sleep(seconds: float) -> None:  # pragma: no cover
    time.sleep(seconds)


@dataclass
class RetryPolicy:
    """Configuration for retry behaviour."""

    max_attempts: int = 3
    backoff_seconds: float = 2.0
    backoff_multiplier: float = 2.0
    max_backoff_seconds: float = 30.0
    exceptions: tuple = field(default_factory=lambda: (Exception,))

    def delays(self) -> list[float]:
        """Return the list of delay values for each retry attempt."""
        delays: list[float] = []
        wait = self.backoff_seconds
        for _ in range(self.max_attempts - 1):
            delays.append(min(wait, self.max_backoff_seconds))
            wait *= self.backoff_multiplier
        return delays


@dataclass
class RetryResult:
    """Outcome of a retried operation."""

    success: bool
    value: object = None
    attempts: int = 0
    last_exception: Optional[Exception] = None


def with_retry(
    fn: Callable[[], T],
    policy: RetryPolicy,
    _sleep_fn: Callable[[float], None] = _sleep,
) -> RetryResult:
    """Execute *fn* up to *policy.max_attempts* times.

    Returns a :class:`RetryResult` describing the outcome.
    """
    delays = policy.delays()
    last_exc: Optional[Exception] = None

    for attempt in range(1, policy.max_attempts + 1):
        try:
            value = fn()
            return RetryResult(success=True, value=value, attempts=attempt)
        except policy.exceptions as exc:  # type: ignore[misc]
            last_exc = exc
            if attempt < policy.max_attempts:
                _sleep_fn(delays[attempt - 1])

    return RetryResult(
        success=False,
        attempts=policy.max_attempts,
        last_exception=last_exc,
    )
