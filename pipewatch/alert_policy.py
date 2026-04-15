"""Alert policy module: controls when and how often alerts are sent."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Optional


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class AlertPolicy:
    """Defines suppression and repeat rules for pipeline alerts."""

    # Minimum seconds between repeated alerts for the same pipeline
    cooldown_seconds: int = 300
    # Maximum number of alerts per pipeline per hour (0 = unlimited)
    max_alerts_per_hour: int = 0


@dataclass
class AlertState:
    """Tracks in-memory alert history for a single pipeline."""

    pipeline_name: str
    last_alerted_at: Optional[datetime] = None
    alerts_this_hour: int = 0
    hour_window_start: Optional[datetime] = field(default=None)

    def _reset_hour_if_needed(self, now: datetime) -> None:
        if self.hour_window_start is None:
            self.hour_window_start = now
            self.alerts_this_hour = 0
            return
        elapsed = (now - self.hour_window_start).total_seconds()
        if elapsed >= 3600:
            self.hour_window_start = now
            self.alerts_this_hour = 0

    def should_alert(self, policy: AlertPolicy, now: Optional[datetime] = None) -> bool:
        """Return True if an alert should be sent given the policy."""
        now = now or _utcnow()
        self._reset_hour_if_needed(now)

        if self.last_alerted_at is not None:
            elapsed = (now - self.last_alerted_at).total_seconds()
            if elapsed < policy.cooldown_seconds:
                return False

        if policy.max_alerts_per_hour > 0:
            if self.alerts_this_hour >= policy.max_alerts_per_hour:
                return False

        return True

    def record_alert(self, now: Optional[datetime] = None) -> None:
        """Update state after an alert has been sent."""
        now = now or _utcnow()
        self._reset_hour_if_needed(now)
        self.last_alerted_at = now
        self.alerts_this_hour += 1


class AlertPolicyManager:
    """Manages AlertState instances keyed by pipeline name."""

    def __init__(self, policy: AlertPolicy) -> None:
        self.policy = policy
        self._states: Dict[str, AlertState] = {}

    def _get_state(self, pipeline_name: str) -> AlertState:
        if pipeline_name not in self._states:
            self._states[pipeline_name] = AlertState(pipeline_name=pipeline_name)
        return self._states[pipeline_name]

    def should_alert(self, pipeline_name: str, now: Optional[datetime] = None) -> bool:
        return self._get_state(pipeline_name).should_alert(self.policy, now=now)

    def record_alert(self, pipeline_name: str, now: Optional[datetime] = None) -> None:
        self._get_state(pipeline_name).record_alert(now=now)
