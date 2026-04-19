"""Tests for pipewatch.cli_throttle."""
import pytest
from click.testing import CliRunner
from pathlib import Path
from pipewatch.cli_throttle import throttle_command
from pipewatch.throttle import save_throttle, record_sent


@pytest.fixture()
def runner():
    return CliRunner()


def _write_state(path: Path):
    state = {}
    record_sent("pipe_a", "duration", state, now=1_000_000.0)
    record_sent("pipe_b", "error_rate", state, now=1_000_010.0)
    save_throttle(state, path)
    return state


def test_list_no_entries(runner, tmp_path):
    path = tmp_path / "throttle.json"
    result = runner.invoke(throttle_command, ["list", "--path", str(path)])
    assert result.exit_code == 0
    assert "No throttle entries" in result.output


def test_list_shows_entries(runner, tmp_path):
    path = tmp_path / "throttle.json"
    _write_state(path)
    result = runner.invoke(throttle_command, ["list", "--path", str(path)])
    assert result.exit_code == 0
    assert "pipe_a" in result.output
    assert "pipe_b" in result.output


def test_clear_all(runner, tmp_path):
    path = tmp_path / "throttle.json"
    _write_state(path)
    result = runner.invoke(throttle_command, ["clear", "--path", str(path)])
    assert result.exit_code == 0
    assert "Cleared all" in result.output


def test_clear_by_pipeline(runner, tmp_path):
    path = tmp_path / "throttle.json"
    _write_state(path)
    result = runner.invoke(
        throttle_command, ["clear", "--path", str(path), "--pipeline", "pipe_a"]
    )
    assert result.exit_code == 0
    assert "1 entries" in result.output
    # pipe_b should remain
    result2 = runner.invoke(throttle_command, ["list", "--path", str(path)])
    assert "pipe_b" in result2.output
    assert "pipe_a" not in result2.output
