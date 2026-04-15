"""Periodic digest report: summarise multiple pipeline run summaries into a
single human-readable or machine-readable digest payload."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Optional

from pipewatch.summary import PipelineSummary


def _utcnow() -> datetime:  # pragma: no cover – thin wrapper for test patching
    return datetime.now(timezone.utc)


@dataclass
class DigestEntry:
    pipeline_name: str
    total_runs: int
    healthy_runs: int
    failing_runs: int
    last_status: str  # "healthy" | "unhealthy" | "unknown"

    def to_dict(self) -> dict:
        return {
            "pipeline_name": self.pipeline_name,
            "total_runs": self.total_runs,
            "healthy_runs": self.healthy_runs,
            "failing_runs": self.failing_runs,
            "last_status": self.last_status,
        }


@dataclass
class DigestReport:
    generated_at: datetime
    period_label: str
    entries: List[DigestEntry] = field(default_factory=list)

    @property
    def total_pipelines(self) -> int:
        return len(self.entries)

    @property
    def all_healthy(self) -> bool:
        return all(e.last_status == "healthy" for e in self.entries)

    def to_dict(self) -> dict:
        return {
            "generated_at": self.generated_at.isoformat(),
            "period_label": self.period_label,
            "total_pipelines": self.total_pipelines,
            "all_healthy": self.all_healthy,
            "entries": [e.to_dict() for e in self.entries],
        }


def build_digest(
    summaries: List[PipelineSummary],
    period_label: str = "latest",
    now: Optional[datetime] = None,
) -> DigestReport:
    """Build a DigestReport from a list of PipelineSummary objects."""
    if now is None:
        now = _utcnow()

    entries: List[DigestEntry] = []
    for summary in summaries:
        healthy_runs = sum(1 for r in summary.runs if r.healthy)
        total_runs = len(summary.runs)
        failing_runs = total_runs - healthy_runs

        if total_runs == 0:
            last_status = "unknown"
        elif summary.runs[-1].healthy:
            last_status = "healthy"
        else:
            last_status = "unhealthy"

        entries.append(
            DigestEntry(
                pipeline_name=summary.pipeline_name,
                total_runs=total_runs,
                healthy_runs=healthy_runs,
                failing_runs=failing_runs,
                last_status=last_status,
            )
        )

    return DigestReport(generated_at=now, period_label=period_label, entries=entries)
