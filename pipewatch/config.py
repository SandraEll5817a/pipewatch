"""Configuration loader and validator for pipewatch."""

import os
import yaml
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class ThresholdConfig:
    max_duration_seconds: Optional[float] = None
    max_error_rate: Optional[float] = None
    min_rows_processed: Optional[int] = None


@dataclass
class PipelineConfig:
    name: str
    thresholds: ThresholdConfig = field(default_factory=ThresholdConfig)
    webhook_url: Optional[str] = None


@dataclass
class AppConfig:
    pipelines: list[PipelineConfig] = field(default_factory=list)
    default_webhook_url: Optional[str] = None


def load_config(path: str = "pipewatch.yaml") -> AppConfig:
    """Load and parse configuration from a YAML file."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"Config file not found: {path}")

    with open(path, "r") as f:
        raw = yaml.safe_load(f)

    if not isinstance(raw, dict):
        raise ValueError("Config file must be a YAML mapping.")

    default_webhook = raw.get("default_webhook_url")
    pipelines = []

    for entry in raw.get("pipelines", []):
        name = entry.get("name")
        if not name:
            raise ValueError("Each pipeline entry must have a 'name' field.")

        thresholds_raw = entry.get("thresholds", {})
        thresholds = ThresholdConfig(
            max_duration_seconds=thresholds_raw.get("max_duration_seconds"),
            max_error_rate=thresholds_raw.get("max_error_rate"),
            min_rows_processed=thresholds_raw.get("min_rows_processed"),
        )

        pipelines.append(
            PipelineConfig(
                name=name,
                thresholds=thresholds,
                webhook_url=entry.get("webhook_url", default_webhook),
            )
        )

    return AppConfig(pipelines=pipelines, default_webhook_url=default_webhook)
