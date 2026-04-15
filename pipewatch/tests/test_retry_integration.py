"""Integration-style tests: retry policy wired into notifier_retry."""

from __future__ import annotations

from unittest.mock import patch

from pipewatch.metrics import ThresholdViolation
from pipewatch.notifier import NotificationPayload
from pipewatch.notifier_retry import send_with_retry
from pipewatch.retry import RetryPolicy


_INSTANT = RetryPolicy(max_attempts=4, backoff_seconds=0.0, backoff_multiplier=1.0)


def _payload() -> NotificationPayload:
    v = ThresholdViolation(metric="duration_seconds", threshold=60.0, actual=120.0)
    return NotificationPayload(pipeline="orders", violations=[v])


def test_retry_count_matches_attempts_before_success():
    """Ensure the attempt counter increments correctly across retries."""
    log: list[int] = []

    def counting_send(url, p):
        log.append(len(log) + 1)
        if len(log) < 3:
            raise IOError("temporary")
        return True

    with patch("pipewatch.notifier_retry.send_webhook", side_effect=counting_send):
        result = send_with_retry("http://hook", _payload(), _INSTANT)

    assert result.delivered is True
    assert result.attempts == 3
    assert len(log) == 3


def test_all_failures_reports_correct_attempt_count():
    with patch(
        "pipewatch.notifier_retry.send_webhook",
        side_effect=TimeoutError("no route"),
    ):
        result = send_with_retry("http://hook", _payload(), _INSTANT)

    assert result.delivered is False
    assert result.attempts == _INSTANT.max_attempts


def test_single_attempt_policy_no_retries():
    policy = RetryPolicy(max_attempts=1, backoff_seconds=0.0)
    with patch(
        "pipewatch.notifier_retry.send_webhook",
        side_effect=RuntimeError("crash"),
    ):
        result = send_with_retry("http://hook", _payload(), policy)

    assert result.delivered is False
    assert result.attempts == 1


def test_successful_delivery_has_no_exception():
    with patch("pipewatch.notifier_retry.send_webhook", return_value=True):
        result = send_with_retry("http://hook", _payload(), _INSTANT)

    assert result.last_exception is None
