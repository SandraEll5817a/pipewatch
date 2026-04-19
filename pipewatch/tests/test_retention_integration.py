"""Integration tests: retention applied against real history round-trip."""
import json
import tempfile
from datetime import datetime, timezone, timedelta
from pathlib import Path
from unittest.mock import patch

from pipewatch.history import PipelineRun, save_history, load_history
from pipewatch.retention import RetentionPolicy, apply_retention


def _run(days_ago: float, pipeline: str = "pipe") -> PipelineRun:
    ts = datetime.now(timezone.utc) - timedelta(days=days_ago)
    return PipelineRun(pipeline=pipeline, timestamp=ts, healthy=True, violations=[])


def test_save_prune_reload_roundtrip(tmp_path):
    runs = [_run(1), _run(5), _run(15), _run(40)]
    policy = RetentionPolicy(max_age_days=30)

    with patch("pipewatch.history._history_path", return_value=tmp_path / "pipe.json"):
        save_history("pipe", runs)
        loaded = load_history("pipe")

    kept, result = apply_retention("pipe", loaded, policy)
    assert len(kept) == 3
    assert result.pruned_count == 1


def test_max_runs_integration():
    runs = [_run(i) for i in range(10)]
    policy = RetentionPolicy(max_runs=3)
    kept, result = apply_retention("pipe", runs, policy)
    assert len(kept) == 3
    assert result.pruned_count == 7
    ages = sorted([(datetime.now(timezone.utc) - r.timestamp).days for r in kept])
    assert ages[0] <= 3


def test_combined_policy_integration():
    runs = [_run(i * 3) for i in range(10)]
    policy = RetentionPolicy(max_age_days=20, max_runs=4)
    kept, result = apply_retention("pipe", runs, policy)
    # age filter: days 0,3,6,9,12,15,18 -> 7 runs within 20 days
    # max_runs=4 -> 4 kept
    assert len(kept) == 4
