"""Heartbeat monitoring: detect pipelines that have stopped reporting entirely."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Optional

from pipewatch.history import PipelineRun


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class HeartbeatResult:
    pipeline: str
    expected_interval_seconds: int
    last_run_at: Optional[datetime]
    seconds_since_last_run: Optional[float]
    alive: bool
    message: str

    def __str__(self) -> str:
        if self.alive:
            return f"{self.pipeline}: alive (last run {self.seconds_since_last_run:.0f}s ago)"
        if self.last_run_at is None:
            return f"{self.pipeline}: DEAD (never run)"
        return (
            f"{self.pipeline}: DEAD "
            f"(last run {self.seconds_since_last_run:.0f}s ago, "
            f"expected every {self.expected_interval_seconds}s)"
        )

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "expected_interval_seconds": self.expected_interval_seconds,
            "last_run_at": self.last_run_at.isoformat() if self.last_run_at else None,
            "seconds_since_last_run": self.seconds_since_last_run,
            "alive": self.alive,
            "message": self.message,
        }


def check_heartbeat(
    pipeline: str,
    runs: List[PipelineRun],
    expected_interval_seconds: int,
    grace_seconds: int = 60,
    now: Optional[datetime] = None,
) -> HeartbeatResult:
    """Return a HeartbeatResult for a single pipeline."""
    if now is None:
        now = _utcnow()

    if not runs:
        return HeartbeatResult(
            pipeline=pipeline,
            expected_interval_seconds=expected_interval_seconds,
            last_run_at=None,
            seconds_since_last_run=None,
            alive=False,
            message="Pipeline has never run.",
        )

    last = max(runs, key=lambda r: r.ran_at)
    last_run_at = last.ran_at
    if last_run_at.tzinfo is None:
        last_run_at = last_run_at.replace(tzinfo=timezone.utc)

    elapsed = (now - last_run_at).total_seconds()
    threshold = expected_interval_seconds + grace_seconds
    alive = elapsed <= threshold

    message = (
        f"Last run {elapsed:.0f}s ago; threshold {threshold}s."
        if alive
        else f"No heartbeat for {elapsed:.0f}s; threshold {threshold}s."
    )

    return HeartbeatResult(
        pipeline=pipeline,
        expected_interval_seconds=expected_interval_seconds,
        last_run_at=last_run_at,
        seconds_since_last_run=elapsed,
        alive=alive,
        message=message,
    )


def check_all_heartbeats(
    pipelines_with_intervals: List[tuple],
    runs_by_pipeline: dict,
    grace_seconds: int = 60,
    now: Optional[datetime] = None,
) -> List[HeartbeatResult]:
    """Check heartbeats for multiple pipelines.

    pipelines_with_intervals: list of (pipeline_name, interval_seconds)
    runs_by_pipeline: dict mapping pipeline_name -> List[PipelineRun]
    """
    results = []
    for name, interval in pipelines_with_intervals:
        runs = runs_by_pipeline.get(name, [])
        results.append(
            check_heartbeat(name, runs, interval, grace_seconds=grace_seconds, now=now)
        )
    return results
