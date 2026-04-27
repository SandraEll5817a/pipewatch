"""Pipeline lag detection — measures how far behind a pipeline is
relative to its expected execution cadence."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Optional

from pipewatch.history import PipelineRun


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class LagResult:
    pipeline: str
    lag_seconds: float
    expected_interval_seconds: float
    is_lagging: bool
    last_run_at: Optional[datetime]
    message: str

    def __str__(self) -> str:
        if self.last_run_at is None:
            return f"{self.pipeline}: never run (lag=unknown)"
        lag_min = self.lag_seconds / 60
        status = "LAGGING" if self.is_lagging else "OK"
        return (
            f"{self.pipeline}: {status} "
            f"lag={lag_min:.1f}m "
            f"threshold={self.expected_interval_seconds / 60:.1f}m"
        )

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "lag_seconds": round(self.lag_seconds, 2),
            "expected_interval_seconds": self.expected_interval_seconds,
            "is_lagging": self.is_lagging,
            "last_run_at": self.last_run_at.isoformat() if self.last_run_at else None,
            "message": self.message,
        }


def detect_lag(
    pipeline: str,
    runs: List[PipelineRun],
    expected_interval_seconds: float,
    tolerance: float = 0.2,
    now: Optional[datetime] = None,
) -> LagResult:
    """Detect whether a pipeline is lagging behind its expected cadence.

    A pipeline is considered lagging when the time since its last run
    exceeds ``expected_interval_seconds * (1 + tolerance)``.
    """
    if now is None:
        now = _utcnow()

    if not runs:
        return LagResult(
            pipeline=pipeline,
            lag_seconds=0.0,
            expected_interval_seconds=expected_interval_seconds,
            is_lagging=False,
            last_run_at=None,
            message="no runs recorded",
        )

    last = max(runs, key=lambda r: r.run_at)
    last_run_at = last.run_at
    if last_run_at.tzinfo is None:
        last_run_at = last_run_at.replace(tzinfo=timezone.utc)

    elapsed = (now - last_run_at).total_seconds()
    threshold = expected_interval_seconds * (1.0 + tolerance)
    lag = max(0.0, elapsed - expected_interval_seconds)
    is_lagging = elapsed > threshold

    if is_lagging:
        msg = (
            f"pipeline has not run for {elapsed:.0f}s; "
            f"expected every {expected_interval_seconds:.0f}s"
        )
    else:
        msg = "within expected cadence"

    return LagResult(
        pipeline=pipeline,
        lag_seconds=lag,
        expected_interval_seconds=expected_interval_seconds,
        is_lagging=is_lagging,
        last_run_at=last_run_at,
        message=msg,
    )
