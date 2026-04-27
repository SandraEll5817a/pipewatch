"""Back-pressure detection: flag pipelines whose queue depth or lag is growing."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Optional

from pipewatch.history import PipelineRun


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class BackpressureResult:
    pipeline: str
    is_pressured: bool
    current_lag: float          # latest rows_pending / queue depth proxy
    avg_lag: float
    trend_slope: float          # positive = growing lag
    message: str

    def __str__(self) -> str:
        status = "PRESSURED" if self.is_pressured else "OK"
        return (
            f"[{status}] {self.pipeline}: lag={self.current_lag:.1f} "
            f"avg={self.avg_lag:.1f} slope={self.trend_slope:+.2f}"
        )

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "is_pressured": self.is_pressured,
            "current_lag": self.current_lag,
            "avg_lag": self.avg_lag,
            "trend_slope": self.trend_slope,
            "message": self.message,
        }


def _lag_series(runs: List[PipelineRun]) -> List[float]:
    """Use rows_processed as an inverse proxy for lag (lower = more backlog)."""
    return [float(r.rows_processed) for r in runs if r.rows_processed is not None]


def _linear_slope(values: List[float]) -> float:
    n = len(values)
    if n < 2:
        return 0.0
    xs = list(range(n))
    x_mean = sum(xs) / n
    y_mean = sum(values) / n
    num = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, values))
    den = sum((x - x_mean) ** 2 for x in xs)
    return num / den if den != 0.0 else 0.0


def detect_backpressure(
    pipeline: str,
    runs: List[PipelineRun],
    min_runs: int = 5,
    slope_threshold: float = -5.0,  # negative = throughput dropping = pressure rising
) -> BackpressureResult:
    """Detect back-pressure by checking if throughput (rows_processed) is declining."""
    series = _lag_series(runs[-min_runs * 2:])

    if len(series) < min_runs:
        return BackpressureResult(
            pipeline=pipeline,
            is_pressured=False,
            current_lag=series[-1] if series else 0.0,
            avg_lag=sum(series) / len(series) if series else 0.0,
            trend_slope=0.0,
            message="insufficient data",
        )

    slope = _linear_slope(series)
    avg = sum(series) / len(series)
    current = series[-1]
    pressured = slope < slope_threshold

    message = (
        f"throughput declining (slope={slope:+.2f})"
        if pressured
        else "throughput stable"
    )
    return BackpressureResult(
        pipeline=pipeline,
        is_pressured=pressured,
        current_lag=current,
        avg_lag=avg,
        trend_slope=slope,
        message=message,
    )
