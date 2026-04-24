"""Stale alert detection: flag pipelines that have not reported metrics recently."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import List, Optional

from pipewatch.history import PipelineRun


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class StaleAlertResult:
    pipeline: str
    is_stale: bool
    last_run_at: Optional[datetime]
    stale_after_minutes: int
    age_minutes: Optional[float]

    def __str__(self) -> str:
        if self.is_stale:
            age = f"{self.age_minutes:.1f}m" if self.age_minutes is not None else "never"
            return f"[STALE] {self.pipeline} — last run {age} ago (threshold {self.stale_after_minutes}m)"
        return f"[OK] {self.pipeline} — last run {self.age_minutes:.1f}m ago"


def check_stale(
    pipeline: str,
    runs: List[PipelineRun],
    stale_after_minutes: int,
    now: Optional[datetime] = None,
) -> StaleAlertResult:
    """Return a StaleAlertResult for a single pipeline."""
    if now is None:
        now = _utcnow()

    if not runs:
        return StaleAlertResult(
            pipeline=pipeline,
            is_stale=True,
            last_run_at=None,
            stale_after_minutes=stale_after_minutes,
            age_minutes=None,
        )

    last = max(runs, key=lambda r: r.ran_at)
    last_dt = last.ran_at
    if last_dt.tzinfo is None:
        last_dt = last_dt.replace(tzinfo=timezone.utc)

    age = (now - last_dt).total_seconds() / 60.0
    threshold = timedelta(minutes=stale_after_minutes)
    is_stale = (now - last_dt) > threshold

    return StaleAlertResult(
        pipeline=pipeline,
        is_stale=is_stale,
        last_run_at=last_dt,
        stale_after_minutes=stale_after_minutes,
        age_minutes=age,
    )


def check_all_stale(
    pipeline_runs: dict,
    stale_after_minutes: int,
    now: Optional[datetime] = None,
) -> List[StaleAlertResult]:
    """Check staleness for multiple pipelines.

    Args:
        pipeline_runs: mapping of pipeline name -> list of PipelineRun
        stale_after_minutes: threshold in minutes
        now: optional fixed timestamp (for testing)
    """
    if now is None:
        now = _utcnow()
    return [
        check_stale(name, runs, stale_after_minutes, now=now)
        for name, runs in pipeline_runs.items()
    ]
