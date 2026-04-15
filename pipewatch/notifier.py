"""Webhook notification module for pipewatch."""

import json
import logging
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.metrics import ThresholdViolation

logger = logging.getLogger(__name__)


@dataclass
class NotificationPayload:
    pipeline: str
    violations: List[ThresholdViolation]
    webhook_url: str

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "violations": [
                {
                    "metric": v.metric,
                    "value": v.value,
                    "threshold": v.threshold,
                    "message": str(v),
                }
                for v in self.violations
            ],
            "violation_count": len(self.violations),
        }


def send_webhook(payload: NotificationPayload, timeout: int = 10) -> bool:
    """Send a webhook POST request with violation data.

    Returns True if the request succeeded, False otherwise.
    """
    if not payload.violations:
        logger.debug("No violations for pipeline '%s'; skipping webhook.", payload.pipeline)
        return False

    body = json.dumps(payload.to_dict()).encode("utf-8")
    request = urllib.request.Request(
        payload.webhook_url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            status = response.status
            logger.info(
                "Webhook sent for pipeline '%s': HTTP %s", payload.pipeline, status
            )
            return 200 <= status < 300
    except urllib.error.HTTPError as exc:
        logger.error("Webhook HTTP error for pipeline '%s': %s", payload.pipeline, exc)
    except urllib.error.URLError as exc:
        logger.error("Webhook URL error for pipeline '%s': %s", payload.pipeline, exc)
    except Exception as exc:  # noqa: BLE001
        logger.error("Unexpected error sending webhook: %s", exc)

    return False


def notify_violations(
    pipeline: str,
    violations: List[ThresholdViolation],
    webhook_url: Optional[str],
) -> bool:
    """Build a payload and fire the webhook if a URL is configured."""
    if not webhook_url:
        logger.warning("No webhook URL configured; cannot send notifications.")
        return False

    payload = NotificationPayload(
        pipeline=pipeline,
        violations=violations,
        webhook_url=webhook_url,
    )
    return send_webhook(payload)
