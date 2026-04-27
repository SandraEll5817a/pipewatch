"""Jitter detection: flags pipelines whose run duration varies excessively."""
from __future__ import annotations

from dataclasses import dataclass, field
from statistics import mean, stdev
from typing import List, Optional

from pipewatch.history import PipelineRun


@dataclass
class JitterResult:
    pipeline: str
    sample_size: int
    avg_duration: float
    stddev: float
    cv: float  # coefficient of variation = stddev / mean
    threshold_cv: float
    is_jittery: bool
    message: str

    def __str__(self) -> str:
        status = "JITTERY" if self.is_jittery else "STABLE"
        return (
            f"[{status}] {self.pipeline}: cv={self.cv:.3f} "
            f"(threshold={self.threshold_cv:.3f}), "
            f"avg={self.avg_duration:.1f}s stddev={self.stddev:.1f}s"
        )

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "sample_size": self.sample_size,
            "avg_duration": round(self.avg_duration, 3),
            "stddev": round(self.stddev, 3),
            "cv": round(self.cv, 4),
            "threshold_cv": self.threshold_cv,
            "is_jittery": self.is_jittery,
            "message": self.message,
        }


_MIN_RUNS = 4


def detect_jitter(
    pipeline: str,
    runs: List[PipelineRun],
    threshold_cv: float = 0.5,
    min_runs: int = _MIN_RUNS,
) -> JitterResult:
    """Return a JitterResult for *pipeline* based on recent *runs*.

    Jitter is measured as the coefficient of variation (stddev / mean) of
    successful run durations.  A CV above *threshold_cv* is considered jittery.
    Requires at least *min_runs* data points; returns a stable placeholder when
    there are insufficient samples.
    """
    durations = [
        r.duration_seconds
        for r in runs
        if r.pipeline == pipeline and r.duration_seconds is not None
    ]

    if len(durations) < min_runs:
        return JitterResult(
            pipeline=pipeline,
            sample_size=len(durations),
            avg_duration=0.0,
            stddev=0.0,
            cv=0.0,
            threshold_cv=threshold_cv,
            is_jittery=False,
            message=f"insufficient data ({len(durations)}/{min_runs} runs)",
        )

    avg = mean(durations)
    sd = stdev(durations) if len(durations) > 1 else 0.0
    cv = (sd / avg) if avg > 0 else 0.0
    jittery = cv > threshold_cv

    return JitterResult(
        pipeline=pipeline,
        sample_size=len(durations),
        avg_duration=avg,
        stddev=sd,
        cv=cv,
        threshold_cv=threshold_cv,
        is_jittery=jittery,
        message="high duration variance detected" if jittery else "duration is stable",
    )
