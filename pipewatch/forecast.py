"""Simple run-count and error-rate forecasting based on linear trend."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Optional
from pipewatch.history import PipelineRun


@dataclass
class ForecastResult:
    pipeline: str
    metric: str
    predicted_value: float
    confidence: str  # "low" | "medium" | "high"
    horizon: int  # steps ahead
    insufficient_data: bool = False

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "metric": self.metric,
            "predicted_value": round(self.predicted_value, 4),
            "confidence": self.confidence,
            "horizon": self.horizon,
            "insufficient_data": self.insufficient_data,
        }

    def __str__(self) -> str:
        if self.insufficient_data:
            return f"{self.pipeline}/{self.metric}: insufficient data"
        return (
            f"{self.pipeline}/{self.metric}: predicted={self.predicted_value:.4f} "
            f"(confidence={self.confidence}, horizon={self.horizon})"
        )


def _linear_forecast(values: List[float], horizon: int) -> float:
    n = len(values)
    xs = list(range(n))
    mean_x = sum(xs) / n
    mean_y = sum(values) / n
    denom = sum((x - mean_x) ** 2 for x in xs)
    if denom == 0:
        return mean_y
    slope = sum((xs[i] - mean_x) * (values[i] - mean_y) for i in range(n)) / denom
    return mean_y + slope * (n - 1 + horizon)


def _confidence(n: int) -> str:
    if n >= 20:
        return "high"
    if n >= 10:
        return "medium"
    return "low"


MIN_RUNS = 5


def forecast_metric(
    pipeline: str,
    runs: List[PipelineRun],
    metric: str = "error_rate",
    horizon: int = 1,
) -> ForecastResult:
    if len(runs) < MIN_RUNS:
        return ForecastResult(
            pipeline=pipeline,
            metric=metric,
            predicted_value=0.0,
            confidence="low",
            horizon=horizon,
            insufficient_data=True,
        )
    values = [getattr(r.metrics, metric) for r in runs]
    predicted = _linear_forecast(values, horizon)
    return ForecastResult(
        pipeline=pipeline,
        metric=metric,
        predicted_value=predicted,
        confidence=_confidence(len(runs)),
        horizon=horizon,
    )
