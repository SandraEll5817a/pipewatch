"""Pipeline evaluation runner — ties config, metrics, and notifications together."""

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.config import AppConfig, get_pipeline
from pipewatch.metrics import PipelineMetrics, ThresholdViolation, evaluate_thresholds
from pipewatch.notifier import notify_violations

logger = logging.getLogger(__name__)


@dataclass
class PipelineResult:
    pipeline: str
    violations: List[ThresholdViolation] = field(default_factory=list)
    notified: bool = False

    @property
    def healthy(self) -> bool:
        return len(self.violations) == 0


def run_pipeline_check(
    pipeline_name: str,
    metrics: PipelineMetrics,
    config: AppConfig,
    dry_run: bool = False,
) -> PipelineResult:
    """Evaluate thresholds for a single pipeline and fire webhook if needed."""
    pipeline_cfg = get_pipeline(config, pipeline_name)
    if pipeline_cfg is None:
        raise ValueError(f"Pipeline '{pipeline_name}' not found in config.")

    violations = evaluate_thresholds(metrics, pipeline_cfg.thresholds)
    result = PipelineResult(pipeline=pipeline_name, violations=violations)

    if violations:
        logger.warning(
            "Pipeline '%s' has %d violation(s).", pipeline_name, len(violations)
        )
        for v in violations:
            logger.warning("  - %s", v)

        if not dry_run:
            webhook_url = pipeline_cfg.webhook_url or config.default_webhook_url
            result.notified = notify_violations(
                pipeline=pipeline_name,
                violations=violations,
                webhook_url=webhook_url,
            )
    else:
        logger.info("Pipeline '%s' is healthy.", pipeline_name)

    return result


def run_all_checks(
    metrics_map: Dict[str, PipelineMetrics],
    config: AppConfig,
    dry_run: bool = False,
) -> List[PipelineResult]:
    """Run checks for every pipeline present in metrics_map."""
    results: List[PipelineResult] = []
    for name, metrics in metrics_map.items():
        try:
            result = run_pipeline_check(name, metrics, config, dry_run=dry_run)
        except ValueError as exc:
            logger.error("Skipping pipeline '%s': %s", name, exc)
            continue
        results.append(result)
    return results
