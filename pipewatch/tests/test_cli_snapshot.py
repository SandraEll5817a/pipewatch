"""Tests for pipewatch.cli_snapshot."""
import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from pipewatch.cli_snapshot import snapshot_command
from pipewatch.snapshot import MetricSnapshot, save_snapshots, capture_snapshot
from datetime import datetime, timezone

_NOW = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture()
def runner():
    return CliRunner()


def _make_snap(pipeline: str, healthy: bool = True) -> MetricSnapshot:
    return capture_snapshot(pipeline=pipeline, duration_seconds=8.0, error_rate=0.01,
                            rows_processed=200, healthy=healthy, now=_NOW)


def test_list_no_snapshots(runner, tmp_path):
    p = tmp_path / "snap.json"
    result = runner.invoke(snapshot_command, ["list", "--file", str(p)])
    assert result.exit_code == 0
    assert "No snapshots found" in result.output


def test_list_shows_snapshots(runner, tmp_path):
    p = tmp_path / "snap.json"
    save_snapshots(p, [_make_snap("pipe_a"), _make_snap("pipe_b", healthy=False)])
    result = runner.invoke(snapshot_command, ["list", "--file", str(p)])
    assert result.exit_code == 0
    assert "pipe_a" in result.output
    assert "pipe_b" in result.output


def test_list_filter_by_pipeline(runner, tmp_path):
    p = tmp_path / "snap.json"
    save_snapshots(p, [_make_snap("alpha"), _make_snap("beta")])
    result = runner.invoke(snapshot_command, ["list", "--file", str(p), "--pipeline", "alpha"])
    assert result.exit_code == 0
    assert "alpha" in result.output
    assert "beta" not in result.output


def test_list_respects_limit(runner, tmp_path):
    p = tmp_path / "snap.json"
    save_snapshots(p, [_make_snap(f"p{i}") for i in range(10)])
    result = runner.invoke(snapshot_command, ["list", "--file", str(p), "--limit", "3"])
    assert result.exit_code == 0
    lines = [l for l in result.output.splitlines() if l.startswith("p")]
    assert len(lines) == 3


def test_clear_removes_file(runner, tmp_path):
    p = tmp_path / "snap.json"
    save_snapshots(p, [_make_snap("x")])
    result = runner.invoke(snapshot_command, ["clear", "--file", str(p)], input="y\n")
    assert result.exit_code == 0
    assert not p.exists()


def test_clear_missing_file(runner, tmp_path):
    p = tmp_path / "snap.json"
    result = runner.invoke(snapshot_command, ["clear", "--file", str(p)], input="y\n")
    assert result.exit_code == 0
    assert "No snapshot file found" in result.output
