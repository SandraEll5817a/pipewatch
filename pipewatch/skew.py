"""Detect timing skew between pipelines that are expected to run in sync."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional

from pipewatch.history import PipelineRun


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class SkewResult:
    pipeline_a: str
    pipeline_b: str
    skew_seconds: float
    threshold_seconds: float
    is_skewed: bool
    last_run_a: Optional[datetime]
    last_run_b: Optional[datetime]

    def __str__(self) -> str:
        if self.is_skewed:
            return (
                f"SKEWED {self.pipeline_a} vs {self.pipeline_b}: "
                f"{self.skew_seconds:.1f}s apart (threshold {self.threshold_seconds:.1f}s)"
            )
        return (
            f"OK {self.pipeline_a} vs {self.pipeline_b}: "
            f"{self.skew_seconds:.1f}s apart"
        )

    def to_dict(self) -> dict:
        return {
            "pipeline_a": self.pipeline_a,
            "pipeline_b": self.pipeline_b,
            "skew_seconds": round(self.skew_seconds, 3),
            "threshold_seconds": self.threshold_seconds,
            "is_skewed": self.is_skewed,
            "last_run_a": self.last_run_a.isoformat() if self.last_run_a else None,
            "last_run_b": self.last_run_b.isoformat() if self.last_run_b else None,
        }


def _latest_run(runs: List[PipelineRun]) -> Optional[PipelineRun]:
    if not runs:
        return None
    return max(runs, key=lambda r: r.timestamp)


def detect_skew(
    pipeline_a: str,
    pipeline_b: str,
    runs_a: List[PipelineRun],
    runs_b: List[PipelineRun],
    threshold_seconds: float = 300.0,
) -> SkewResult:
    """Compare the most recent run timestamps of two pipelines."""
    latest_a = _latest_run(runs_a)
    latest_b = _latest_run(runs_b)

    if latest_a is None or latest_b is None:
        return SkewResult(
            pipeline_a=pipeline_a,
            pipeline_b=pipeline_b,
            skew_seconds=0.0,
            threshold_seconds=threshold_seconds,
            is_skewed=False,
            last_run_a=latest_a.timestamp if latest_a else None,
            last_run_b=latest_b.timestamp if latest_b else None,
        )

    skew = abs((latest_a.timestamp - latest_b.timestamp).total_seconds())
    return SkewResult(
        pipeline_a=pipeline_a,
        pipeline_b=pipeline_b,
        skew_seconds=skew,
        threshold_seconds=threshold_seconds,
        is_skewed=skew > threshold_seconds,
        last_run_a=latest_a.timestamp,
        last_run_b=latest_b.timestamp,
    )
