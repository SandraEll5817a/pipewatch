"""Tag-based pipeline filtering for pipewatch."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.config import PipelineConfig


@dataclass
class TagFilter:
    """Filter pipelines by required and excluded tags."""
    require: List[str] = field(default_factory=list)
    exclude: List[str] = field(default_factory=list)

    def matches(self, pipeline: PipelineConfig) -> bool:
        """Return True if the pipeline satisfies this filter."""
        tags = set(getattr(pipeline, "tags", []) or [])
        if self.require and not all(t in tags for t in self.require):
            return False
        if self.exclude and any(t in tags for t in self.exclude):
            return False
        return True


def filter_pipelines(
    pipelines: List[PipelineConfig],
    require: Optional[List[str]] = None,
    exclude: Optional[List[str]] = None,
) -> List[PipelineConfig]:
    """Return pipelines matching the given tag constraints."""
    f = TagFilter(require=require or [], exclude=exclude or [])
    return [p for p in pipelines if f.matches(p)]


def pipelines_by_tag(pipelines: List[PipelineConfig]) -> dict:
    """Return a mapping of tag -> list of pipeline names."""
    index: dict = {}
    for p in pipelines:
        for tag in getattr(p, "tags", []) or []:
            index.setdefault(tag, []).append(p.name)
    return index
