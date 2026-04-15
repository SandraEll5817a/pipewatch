"""Metrics collection and threshold evaluation for ETL pipelines."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from pipewatch.config import PipelineConfig, ThresholdConfig


@dataclass
class PipelineMetrics:
    """Snapshot of metrics for a single pipeline run."""

    pipeline_name: str
    duration_seconds: float
    error_rate: float  # 0.0 – 1.0
    rows_processed: int
    timestamp: datetime = field(default_factory=datetime.utcnow)


@dataclass
class ThresholdViolation:
    """Represents a single threshold breach."""

    pipeline_name: str
    metric: str
    threshold: float
    actual: float
    timestamp: datetime = field(default_factory=datetime.utcnow)

    def __str__(self) -> str:
        return (
            f"[{self.pipeline_name}] {self.metric} exceeded threshold "
            f"({self.actual:.4g} > {self.threshold:.4g})"
        )


def evaluate_thresholds(
    metrics: PipelineMetrics,
    config: PipelineConfig,
) -> list[ThresholdViolation]:
    """Compare *metrics* against *config* thresholds and return any violations."""

    violations: list[ThresholdViolation] = []
    thresholds: ThresholdConfig = config.thresholds

    if (
        thresholds.max_duration_seconds is not None
        and metrics.duration_seconds > thresholds.max_duration_seconds
    ):
        violations.append(
            ThresholdViolation(
                pipeline_name=metrics.pipeline_name,
                metric="duration_seconds",
                threshold=thresholds.max_duration_seconds,
                actual=metrics.duration_seconds,
                timestamp=metrics.timestamp,
            )
        )

    if (
        thresholds.max_error_rate is not None
        and metrics.error_rate > thresholds.max_error_rate
    ):
        violations.append(
            ThresholdViolation(
                pipeline_name=metrics.pipeline_name,
                metric="error_rate",
                threshold=thresholds.max_error_rate,
                actual=metrics.error_rate,
                timestamp=metrics.timestamp,
            )
        )

    if (
        thresholds.min_rows_processed is not None
        and metrics.rows_processed < thresholds.min_rows_processed
    ):
        violations.append(
            ThresholdViolation(
                pipeline_name=metrics.pipeline_name,
                metric="rows_processed",
                threshold=float(thresholds.min_rows_processed),
                actual=float(metrics.rows_processed),
                timestamp=metrics.timestamp,
            )
        )

    return violations
