"""Simple anomaly detection: flag metrics that deviate significantly from recent history."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional
import statistics

from pipewatch.history import PipelineRun


@dataclass
class AnomalyResult:
    pipeline: str
    metric: str
    current_value: float
    mean: float
    stddev: float
    z_score: float
    is_anomaly: bool

    def __str__(self) -> str:
        flag = "ANOMALY" if self.is_anomaly else "ok"
        return (
            f"[{flag}] {self.pipeline}/{self.metric}: "
            f"current={self.current_value:.2f} mean={self.mean:.2f} "
            f"stddev={self.stddev:.2f} z={self.z_score:.2f}"
        )


def _z_score(value: float, mean: float, stddev: float) -> float:
    if stddev == 0.0:
        return 0.0
    return (value - mean) / stddev


def detect_anomalies(
    pipeline: str,
    runs: List[PipelineRun],
    current: PipelineRun,
    z_threshold: float = 2.5,
    min_samples: int = 5,
) -> List[AnomalyResult]:
    """Compare current run metrics against historical mean/stddev."""
    results: List[AnomalyResult] = []

    metrics = {
        "duration_seconds": [r.duration_seconds for r in runs if r.duration_seconds is not None],
        "error_rate": [r.error_rate for r in runs if r.error_rate is not None],
        "rows_processed": [r.rows_processed for r in runs if r.rows_processed is not None],
    }
    current_values = {
        "duration_seconds": current.duration_seconds,
        "error_rate": current.error_rate,
        "rows_processed": current.rows_processed,
    }

    for metric, history in metrics.items():
        current_val = current_values[metric]
        if current_val is None or len(history) < min_samples:
            continue
        mean = statistics.mean(history)
        stddev = statistics.pstdev(history)
        z = _z_score(current_val, mean, stddev)
        results.append(
            AnomalyResult(
                pipeline=pipeline,
                metric=metric,
                current_value=current_val,
                mean=mean,
                stddev=stddev,
                z_score=z,
                is_anomaly=abs(z) >= z_threshold,
            )
        )
    return results
