"""Pipeline labeling: attach arbitrary key-value labels to pipelines and filter by them."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.config import PipelineConfig


@dataclass
class LabelSelector:
    """Match pipelines whose labels satisfy all required key=value pairs."""
    require: Dict[str, str] = field(default_factory=dict)
    exclude_keys: List[str] = field(default_factory=list)

    def matches(self, pipeline: PipelineConfig) -> bool:
        labels: Dict[str, str] = getattr(pipeline, "labels", {}) or {}
        for key, value in self.require.items():
            if labels.get(key) != value:
                return False
        for key in self.exclude_keys:
            if key in labels:
                return False
        return True


def filter_by_labels(
    pipelines: List[PipelineConfig],
    selector: LabelSelector,
) -> List[PipelineConfig]:
    """Return pipelines that satisfy the given LabelSelector."""
    return [p for p in pipelines if selector.matches(p)]


def group_by_label(
    pipelines: List[PipelineConfig],
    key: str,
) -> Dict[str, List[PipelineConfig]]:
    """Group pipelines by the value of a specific label key.

    Pipelines that do not have the key are placed under the empty-string bucket.
    """
    groups: Dict[str, List[PipelineConfig]] = {}
    for pipeline in pipelines:
        labels: Dict[str, str] = getattr(pipeline, "labels", {}) or {}
        value = labels.get(key, "")
        groups.setdefault(value, []).append(pipeline)
    return groups


def label_index(pipelines: List[PipelineConfig]) -> Dict[str, Dict[str, List[str]]]:
    """Return a nested index: {label_key: {label_value: [pipeline_names]}}."""
    index: Dict[str, Dict[str, List[str]]] = {}
    for pipeline in pipelines:
        labels: Dict[str, str] = getattr(pipeline, "labels", {}) or {}
        for k, v in labels.items():
            index.setdefault(k, {}).setdefault(v, []).append(pipeline.name)
    return index
