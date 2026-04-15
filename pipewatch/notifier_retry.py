"""Webhook notification with automatic retry support."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from pipewatch.notifier import NotificationPayload, send_webhook
from pipewatch.retry import RetryPolicy, RetryResult, with_retry


_DEFAULT_POLICY = RetryPolicy(
    max_attempts=3,
    backoff_seconds=2.0,
    backoff_multiplier=2.0,
    max_backoff_seconds=30.0,
    exceptions=(Exception,),
)


@dataclass
class WebhookDeliveryResult:
    """Outcome of a webhook delivery attempt (with retries)."""

    delivered: bool
    attempts: int
    last_exception: Optional[Exception] = None

    def __str__(self) -> str:
        if self.delivered:
            return f"Delivered after {self.attempts} attempt(s)."
        return (
            f"Failed after {self.attempts} attempt(s): {self.last_exception}"
        )


def send_with_retry(
    url: str,
    payload: NotificationPayload,
    policy: RetryPolicy = _DEFAULT_POLICY,
) -> WebhookDeliveryResult:
    """Send *payload* to *url*, retrying according to *policy*.

    :func:`pipewatch.notifier.send_webhook` raises on HTTP errors or network
    failures; this wrapper retries those failures transparently.
    """

    def _attempt() -> bool:
        ok = send_webhook(url, payload)
        if not ok:
            raise RuntimeError("send_webhook returned False")
        return ok

    result: RetryResult = with_retry(_attempt, policy)
    return WebhookDeliveryResult(
        delivered=result.success,
        attempts=result.attempts,
        last_exception=result.last_exception,
    )
