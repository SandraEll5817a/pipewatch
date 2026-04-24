"""Tests for pipewatch.cli_circuit_breaker."""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from pipewatch.circuit_breaker import CircuitState, save_circuit_states
from pipewatch.cli_circuit_breaker import circuit_command


@pytest.fixture()
def runner():
    return CliRunner()


def _write_states(tmp_path: Path, states: dict) -> Path:
    path = tmp_path / "circuit.json"
    path.write_text(json.dumps({k: v.to_dict() for k, v in states.items()}))
    return path


def test_list_no_state(runner, tmp_path):
    path = tmp_path / "missing.json"
    with patch("pipewatch.cli_circuit_breaker.load_circuit_states", return_value={}):
        result = runner.invoke(circuit_command, ["list"])
    assert result.exit_code == 0
    assert "No circuit" in result.output


def test_list_shows_closed_circuit(runner):
    states = {"etl": CircuitState(pipeline="etl", consecutive_failures=0, state="closed")}
    with patch("pipewatch.cli_circuit_breaker.load_circuit_states", return_value=states):
        result = runner.invoke(circuit_command, ["list"])
    assert result.exit_code == 0
    assert "etl" in result.output
    assert "CLOSED" in result.output


def test_list_shows_open_circuit(runner):
    states = {
        "etl": CircuitState(
            pipeline="etl",
            consecutive_failures=3,
            state="open",
            opened_at="2024-06-01T12:00:00+00:00",
        )
    }
    with (
        patch("pipewatch.cli_circuit_breaker.load_circuit_states", return_value=states),
        patch("pipewatch.circuit_breaker._utcnow") as mock_now,
    ):
        from datetime import datetime, timezone
        mock_now.return_value = datetime(2024, 6, 1, 12, 1, 0, tzinfo=timezone.utc)
        result = runner.invoke(circuit_command, ["list"])
    assert result.exit_code == 0
    assert "OPEN" in result.output


def test_reset_unknown_pipeline(runner):
    with patch("pipewatch.cli_circuit_breaker.load_circuit_states", return_value={}):
        result = runner.invoke(circuit_command, ["reset", "unknown"])
    assert result.exit_code == 1
    assert "No circuit state" in result.output


def test_reset_closes_circuit(runner):
    states = {
        "etl": CircuitState(pipeline="etl", consecutive_failures=3, state="open", opened_at="2024-01-01T00:00:00+00:00")
    }
    saved = {}

    def _save(s, path=None):
        saved.update(s)

    with (
        patch("pipewatch.cli_circuit_breaker.load_circuit_states", return_value=states),
        patch("pipewatch.cli_circuit_breaker.save_circuit_states", side_effect=_save),
    ):
        result = runner.invoke(circuit_command, ["reset", "etl"])
    assert result.exit_code == 0
    assert "reset" in result.output
    assert saved["etl"].state == "closed"
    assert saved["etl"].consecutive_failures == 0


def test_clear_removes_state_file(runner, tmp_path):
    fake_file = tmp_path / "circuit.json"
    fake_file.write_text("{}")
    with patch("pipewatch.cli_circuit_breaker._STATE_FILE", fake_file):
        result = runner.invoke(circuit_command, ["clear"])
    assert result.exit_code == 0
    assert not fake_file.exists()
    assert "cleared" in result.output
