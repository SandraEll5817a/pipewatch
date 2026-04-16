"""Tests for pipewatch.notifier."""

import json
from io import BytesIO
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.metrics import ThresholdViolation
from pipewatch.notifier import (
    NotificationPayload,
    notify_violations,
    send_webhook,
)


VIOLATION = ThresholdViolation(metric="error_rate", value=0.15, threshold=0.05)
WEBHOOK_URL = "https://hooks.example.com/notify"


def make_payload(violations=None):
    return NotificationPayload(
        pipeline="orders",
        violations=violations or [VIOLATION],
        webhook_url=WEBHOOK_URL,
    )


# ---------------------------------------------------------------------------
# NotificationPayload
# ---------------------------------------------------------------------------

def test_payload_to_dict_structure():
    payload = make_payload()
    data = payload.to_dict()
    assert data["pipeline"] == "orders"
    assert data["violation_count"] == 1
    assert data["violations"][0]["metric"] == "error_rate"
    assert data["violations"][0]["value"] == 0.15
    assert data["violations"][0]["threshold"] == 0.05


def test_payload_to_dict_multiple_violations():
    v2 = ThresholdViolation(metric="duration_seconds", value=600, threshold=300)
    payload = make_payload(violations=[VIOLATION, v2])
    data = payload.to_dict()
    assert data["violation_count"] == 2


# ---------------------------------------------------------------------------
# send_webhook
# ---------------------------------------------------------------------------

def _mock_response(status=200):
    mock_resp = MagicMock()
    mock_resp.status = status
    mock_resp.__enter__ = lambda s: s
    mock_resp.__exit__ = MagicMock(return_value=False)
    return mock_resp


def test_send_webhook_returns_true_on_success():
    with patch("urllib.request.urlopen", return_value=_mock_response(200)):
        result = send_webhook(make_payload())
    assert result is True


def test_send_webhook_returns_false_on_http_error():
    import urllib.error
    with patch("urllib.request.urlopen", side_effect=urllib.error.HTTPError(
        WEBHOOK_URL, 500, "Server Error", {}, None
    )):
        result = send_webhook(make_payload())
    assert result is False


def test_send_webhook_returns_false_on_url_error():
    import urllib.error
    with patch("urllib.request.urlopen", side_effect=urllib.error.URLError(
        "Connection refused"
    )):
        result = send_webhook(make_payload())
    assert result is False


def test_send_webhook_skips_empty_violations():
    payload = make_payload(violations=[])
    with patch("urllib.request.urlopen") as mock_open:
        result = send_webhook(payload)
    mock_open.assert_not_called()
    assert result is False


# ---------------------------------------------------------------------------
# notify_violations
# ---------------------------------------------------------------------------

def test_notify_violations_returns_false_without_url():
    result = notify_violations("orders", [VIOLATION], webhook_url=None)
    assert result is False


def test_notify_violations_calls_send_webhook():
    with patch("pipewatch.notifier.send_webhook", return_value=True) as mock_send:
        result = notify_violations("orders", [VIOLATION], webhook_url=WEBHOOK_URL)
    assert result is True
    mock_send.assert_called_once()
