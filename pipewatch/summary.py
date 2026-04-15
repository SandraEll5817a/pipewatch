"""Summary report generation for pipeline check runs."""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List

from pipewatch.runner import PipelineResult


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class PipelineSummary:
    pipeline_name: str
    healthy: bool
    violation_count: int
    violation_messages: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline_name,
            "healthy": self.healthy,
            "violation_count": self.violation_count,
            "violations": self.violation_messages,
        }


@dataclass
class RunSummary:
    timestamp: str
    total_pipelines: int
    healthy_count: int
    unhealthy_count: int
    pipelines: List[PipelineSummary] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "timestamp": self.timestamp,
            "total_pipelines": self.total_pipelines,
            "healthy_count": self.healthy_count,
            "unhealthy_count": self.unhealthy_count,
            "pipelines": [p.to_dict() for p in self.pipelines],
        }


def build_summary(results: List[PipelineResult]) -> RunSummary:
    """Build a RunSummary from a list of PipelineResult objects."""
    pipeline_summaries = []
    for result in results:
        messages = [str(v) for v in result.violations]
        pipeline_summaries.append(
            PipelineSummary(
                pipeline_name=result.pipeline_name,
                healthy=result.healthy,
                violation_count=len(result.violations),
                violation_messages=messages,
            )
        )

    healthy_count = sum(1 for r in results if r.healthy)
    return RunSummary(
        timestamp=_utcnow().isoformat(),
        total_pipelines=len(results),
        healthy_count=healthy_count,
        unhealthy_count=len(results) - healthy_count,
        pipelines=pipeline_summaries,
    )


def format_summary_text(summary: RunSummary) -> str:
    """Render a RunSummary as a human-readable text report."""
    lines = [
        f"PipeWatch Run Summary  [{summary.timestamp}]",
        f"  Pipelines checked : {summary.total_pipelines}",
        f"  Healthy           : {summary.healthy_count}",
        f"  Unhealthy         : {summary.unhealthy_count}",
        "",
    ]
    for ps in summary.pipelines:
        status = "OK" if ps.healthy else "FAIL"
        lines.append(f"  [{status}] {ps.pipeline_name}")
        for msg in ps.violation_messages:
            lines.append(f"        - {msg}")
    return "\n".join(lines)
