"""Tests for pipewatch.notifier_retry."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from pipewatch.metrics import ThresholdViolation
from pipewatch.notifier import NotificationPayload
from pipewatch.notifier_retry import WebhookDeliveryResult, send_with_retry
from pipewatch.retry import RetryPolicy


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

_FAST_POLICY = RetryPolicy(
    max_attempts=3,
    backoff_seconds=0.0,
    backoff_multiplier=1.0,
)


def _make_payload() -> NotificationPayload:
    v = ThresholdViolation(metric="error_rate", threshold=0.05, actual=0.12)
    return NotificationPayload(pipeline="sales", violations=[v])


# ---------------------------------------------------------------------------
# WebhookDeliveryResult.__str__
# ---------------------------------------------------------------------------

def test_delivery_result_str_success():
    r = WebhookDeliveryResult(delivered=True, attempts=1)
    assert "Delivered" in str(r)
    assert "1" in str(r)


def test_delivery_result_str_failure():
    exc = RuntimeError("timeout")
    r = WebhookDeliveryResult(delivered=False, attempts=3, last_exception=exc)
    assert "Failed" in str(r)
    assert "timeout" in str(r)


# ---------------------------------------------------------------------------
# send_with_retry — success
# ---------------------------------------------------------------------------

def test_send_with_retry_success_on_first_attempt():
    payload = _make_payload()
    with patch("pipewatch.notifier_retry.send_webhook", return_value=True) as mock_send:
        result = send_with_retry("http://example.com/hook", payload, _FAST_POLICY)
    assert result.delivered is True
    assert result.attempts == 1
    mock_send.assert_called_once()


def test_send_with_retry_succeeds_after_transient_failure():
    payload = _make_payload()
    call_count = {"n": 0}

    def flaky_send(url, p):
        call_count["n"] += 1
        if call_count["n"] < 3:
            raise ConnectionError("network blip")
        return True

    with patch("pipewatch.notifier_retry.send_webhook", side_effect=flaky_send):
        result = send_with_retry("http://example.com/hook", payload, _FAST_POLICY)

    assert result.delivered is True
    assert result.attempts == 3


# ---------------------------------------------------------------------------
# send_with_retry — failure
# ---------------------------------------------------------------------------

def test_send_with_retry_exhausts_attempts():
    payload = _make_payload()
    with patch(
        "pipewatch.notifier_retry.send_webhook",
        side_effect=ConnectionError("down"),
    ):
        result = send_with_retry("http://example.com/hook", payload, _FAST_POLICY)

    assert result.delivered is False
    assert result.attempts == _FAST_POLICY.max_attempts
    assert result.last_exception is not None


def test_send_with_retry_false_return_treated_as_failure():
    """send_webhook returning False should also trigger retries."""
    payload = _make_payload()
    with patch("pipewatch.notifier_retry.send_webhook", return_value=False):
        result = send_with_retry("http://example.com/hook", payload, _FAST_POLICY)

    assert result.delivered is False
    assert result.attempts == _FAST_POLICY.max_attempts
