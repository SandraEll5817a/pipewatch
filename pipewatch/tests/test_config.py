"""Tests for the configuration loader."""

import os
import pytest
import tempfile
import yaml

from pipewatch.config import load_config, AppConfig, PipelineConfig, ThresholdConfig


SAMPLE_CONFIG = {
    "default_webhook_url": "https://hooks.example.com/default",
    "pipelines": [
        {
            "name": "ingest_orders",
            "thresholds": {
                "max_duration_seconds": 300,
                "max_error_rate": 0.05,
                "min_rows_processed": 100,
            },
            "webhook_url": "https://hooks.example.com/orders",
        },
        {
            "name": "sync_users",
            "thresholds": {
                "max_duration_seconds": 60,
            },
        },
    ],
}


@pytest.fixture
def config_file():
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(SAMPLE_CONFIG, f)
        path = f.name
    yield path
    os.unlink(path)


def test_load_config_returns_app_config(config_file):
    config = load_config(config_file)
    assert isinstance(config, AppConfig)


def test_load_config_default_webhook(config_file):
    config = load_config(config_file)
    assert config.default_webhook_url == "https://hooks.example.com/default"


def test_load_config_pipeline_count(config_file):
    config = load_config(config_file)
    assert len(config.pipelines) == 2


def test_load_config_pipeline_thresholds(config_file):
    config = load_config(config_file)
    pipeline = config.pipelines[0]
    assert isinstance(pipeline, PipelineConfig)
    assert pipeline.name == "ingest_orders"
    assert pipeline.thresholds.max_duration_seconds == 300
    assert pipeline.thresholds.max_error_rate == 0.05
    assert pipeline.thresholds.min_rows_processed == 100


def test_load_config_inherits_default_webhook(config_file):
    config = load_config(config_file)
    sync_pipeline = next(p for p in config.pipelines if p.name == "sync_users")
    assert sync_pipeline.webhook_url == "https://hooks.example.com/default"


def test_load_config_missing_file():
    with pytest.raises(FileNotFoundError):
        load_config("/nonexistent/path/pipewatch.yaml")


def test_load_config_missing_name():
    bad_config = {"pipelines": [{"thresholds": {"max_duration_seconds": 10}}]}
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(bad_config, f)
        path = f.name
    try:
        with pytest.raises(ValueError, match="name"):
            load_config(path)
    finally:
        os.unlink(path)
