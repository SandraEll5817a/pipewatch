"""Rate limiting for webhook notifications per pipeline."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Optional


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class RateLimitPolicy:
    min_interval_seconds: int = 300  # 5 minutes default
    max_per_hour: int = 10


@dataclass
class RateLimitState:
    pipeline: str
    last_sent: Optional[datetime] = None
    sent_this_hour: int = 0
    hour_window_start: Optional[datetime] = None

    def _reset_hour_if_needed(self, now: datetime) -> None:
        if self.hour_window_start is None:
            self.hour_window_start = now
            self.sent_this_hour = 0
            return
        elapsed = (now - self.hour_window_start).total_seconds()
        if elapsed >= 3600:
            self.hour_window_start = now
            self.sent_this_hour = 0

    def allowed(self, policy: RateLimitPolicy, now: Optional[datetime] = None) -> bool:
        now = now or _utcnow()
        self._reset_hour_if_needed(now)
        if self.sent_this_hour >= policy.max_per_hour:
            return False
        if self.last_sent is not None:
            elapsed = (now - self.last_sent).total_seconds()
            if elapsed < policy.min_interval_seconds:
                return False
        return True

    def record_sent(self, now: Optional[datetime] = None) -> None:
        now = now or _utcnow()
        self._reset_hour_if_needed(now)
        self.last_sent = now
        self.sent_this_hour += 1


@dataclass
class RateLimiter:
    policy: RateLimitPolicy = field(default_factory=RateLimitPolicy)
    _states: Dict[str, RateLimitState] = field(default_factory=dict)

    def _get_state(self, pipeline: str) -> RateLimitState:
        if pipeline not in self._states:
            self._states[pipeline] = RateLimitState(pipeline=pipeline)
        return self._states[pipeline]

    def is_allowed(self, pipeline: str, now: Optional[datetime] = None) -> bool:
        return self._get_state(pipeline).allowed(self.policy, now)

    def record_sent(self, pipeline: str, now: Optional[datetime] = None) -> None:
        self._get_state(pipeline).record_sent(now)
