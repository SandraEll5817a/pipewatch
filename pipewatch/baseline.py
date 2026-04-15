"""Baseline management for pipeline metrics.

Stores and compares current metrics against a recorded baseline
to detect regressions over time.
"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Optional

DEFAULT_BASELINE_PATH = Path("pipewatch_baseline.json")


@dataclass
class PipelineBaseline:
    pipeline_name: str
    avg_duration_seconds: float
    avg_error_rate: float
    avg_row_count: float

    def to_dict(self) -> dict:
        return asdict(self)

    @staticmethod
    def from_dict(data: dict) -> "PipelineBaseline":
        return PipelineBaseline(
            pipeline_name=data["pipeline_name"],
            avg_duration_seconds=float(data["avg_duration_seconds"]),
            avg_error_rate=float(data["avg_error_rate"]),
            avg_row_count=float(data["avg_row_count"]),
        )


@dataclass
class BaselineComparison:
    pipeline_name: str
    duration_delta_pct: Optional[float]
    error_rate_delta_pct: Optional[float]
    row_count_delta_pct: Optional[float]
    regressed: bool


def load_baselines(path: Path = DEFAULT_BASELINE_PATH) -> dict[str, PipelineBaseline]:
    """Load baselines from a JSON file. Returns empty dict if missing or corrupt."""
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text())
        return {entry["pipeline_name"]: PipelineBaseline.from_dict(entry) for entry in data}
    except (json.JSONDecodeError, KeyError, TypeError):
        return {}


def save_baselines(
    baselines: dict[str, PipelineBaseline],
    path: Path = DEFAULT_BASELINE_PATH,
) -> None:
    """Persist baselines to a JSON file."""
    path.write_text(json.dumps([b.to_dict() for b in baselines.values()], indent=2))


def _pct_delta(baseline: float, current: float) -> Optional[float]:
    if baseline == 0.0:
        return None
    return round((current - baseline) / baseline * 100, 2)


def compare_to_baseline(
    baseline: PipelineBaseline,
    duration: float,
    error_rate: float,
    row_count: float,
    regression_threshold_pct: float = 20.0,
) -> BaselineComparison:
    """Compare current metrics against a baseline and flag regressions."""
    dur_delta = _pct_delta(baseline.avg_duration_seconds, duration)
    err_delta = _pct_delta(baseline.avg_error_rate, error_rate)
    row_delta = _pct_delta(baseline.avg_row_count, row_count)

    regressed = any(
        d is not None and d > regression_threshold_pct
        for d in (dur_delta, err_delta)
    )

    return BaselineComparison(
        pipeline_name=baseline.pipeline_name,
        duration_delta_pct=dur_delta,
        error_rate_delta_pct=err_delta,
        row_count_delta_pct=row_delta,
        regressed=regressed,
    )
