"""Pipeline run quota tracking — enforce max runs per time window."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class QuotaPolicy:
    max_runs_per_hour: int = 60
    max_runs_per_day: int = 200


@dataclass
class QuotaState:
    pipeline: str
    runs_this_hour: int = 0
    runs_today: int = 0
    hour_window: str = ""
    day_window: str = ""


@dataclass
class QuotaResult:
    pipeline: str
    allowed: bool
    reason: str

    def __str__(self) -> str:
        status = "allowed" if self.allowed else "blocked"
        return f"{self.pipeline}: {status} — {self.reason}"


def _hour_key(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H")


def _day_key(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")


def check_quota(
    pipeline: str,
    policy: QuotaPolicy,
    state: QuotaState,
    now: datetime | None = None,
) -> tuple[QuotaResult, QuotaState]:
    now = now or _utcnow()
    hk = _hour_key(now)
    dk = _day_key(now)

    runs_hour = state.runs_this_hour if state.hour_window == hk else 0
    runs_day = state.runs_today if state.day_window == dk else 0

    if runs_hour >= policy.max_runs_per_hour:
        result = QuotaResult(pipeline, False, f"hourly limit {policy.max_runs_per_hour} reached")
        new_state = QuotaState(pipeline, runs_hour, runs_day, hk, dk)
        return result, new_state

    if runs_day >= policy.max_runs_per_day:
        result = QuotaResult(pipeline, False, f"daily limit {policy.max_runs_per_day} reached")
        new_state = QuotaState(pipeline, runs_hour, runs_day, hk, dk)
        return result, new_state

    new_state = QuotaState(pipeline, runs_hour + 1, runs_day + 1, hk, dk)
    result = QuotaResult(pipeline, True, "within quota")
    return result, new_state


def check_all_quotas(
    pipelines: List[str],
    policy: QuotaPolicy,
    states: Dict[str, QuotaState],
    now: datetime | None = None,
) -> List[QuotaResult]:
    results = []
    for name in pipelines:
        state = states.get(name, QuotaState(pipeline=name))
        result, new_state = check_quota(name, policy, state, now=now)
        states[name] = new_state
        results.append(result)
    return results
