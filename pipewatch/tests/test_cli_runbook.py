import json
import pytest
from click.testing import CliRunner
from pipewatch.cli_runbook import runbook_command


@pytest.fixture
def runner():
    return CliRunner()


def test_add_creates_entry(runner, tmp_path):
    db = str(tmp_path / "runbooks.json")
    result = runner.invoke(
        runbook_command,
        ["add", "my_pipeline", "Fix Guide", "https://wiki/fix", "--db", db],
    )
    assert result.exit_code == 0
    assert "Added runbook" in result.output
    with open(db) as f:
        data = json.load(f)
    assert len(data) == 1
    assert data[0]["pipeline"] == "my_pipeline"
    assert data[0]["title"] == "Fix Guide"


def test_list_no_entries(runner, tmp_path):
    db = str(tmp_path / "runbooks.json")
    result = runner.invoke(runbook_command, ["list", "--db", db])
    assert result.exit_code == 0
    assert "No runbook entries found" in result.output


def test_list_shows_entries(runner, tmp_path):
    db = str(tmp_path / "runbooks.json")
    runner.invoke(runbook_command, ["add", "pipe_a", "Ops Guide", "https://ops", "--db", db])
    runner.invoke(runbook_command, ["add", "pipe_b", "Dev Guide", "https://dev", "--db", db])
    result = runner.invoke(runbook_command, ["list", "--db", db])
    assert "pipe_a" in result.output
    assert "pipe_b" in result.output


def test_list_filter_by_pipeline(runner, tmp_path):
    db = str(tmp_path / "runbooks.json")
    runner.invoke(runbook_command, ["add", "pipe_a", "Guide A", "https://a", "--db", db])
    runner.invoke(runbook_command, ["add", "pipe_b", "Guide B", "https://b", "--db", db])
    result = runner.invoke(runbook_command, ["list", "--pipeline", "pipe_a", "--db", db])
    assert "pipe_a" in result.output
    assert "pipe_b" not in result.output


def test_remove_entry(runner, tmp_path):
    db = str(tmp_path / "runbooks.json")
    runner.invoke(runbook_command, ["add", "pipe_a", "Guide A", "https://a", "--db", db])
    result = runner.invoke(runbook_command, ["remove", "pipe_a", "Guide A", "--db", db])
    assert result.exit_code == 0
    assert "Removed" in result.output
    with open(db) as f:
        data = json.load(f)
    assert data == []


def test_remove_nonexistent_exits_one(runner, tmp_path):
    db = str(tmp_path / "runbooks.json")
    result = runner.invoke(runbook_command, ["remove", "pipe_x", "Missing", "--db", db])
    assert result.exit_code == 1


def test_clear_all(runner, tmp_path):
    db = str(tmp_path / "runbooks.json")
    runner.invoke(runbook_command, ["add", "pipe_a", "G", "https://x", "--db", db])
    result = runner.invoke(runbook_command, ["clear", "--db", db])
    assert result.exit_code == 0
    with open(db) as f:
        assert json.load(f) == []


def test_clear_by_pipeline(runner, tmp_path):
    db = str(tmp_path / "runbooks.json")
    runner.invoke(runbook_command, ["add", "pipe_a", "G", "https://a", "--db", db])
    runner.invoke(runbook_command, ["add", "pipe_b", "G", "https://b", "--db", db])
    runner.invoke(runbook_command, ["clear", "--pipeline", "pipe_a", "--db", db])
    result = runner.invoke(runbook_command, ["list", "--db", db])
    assert "pipe_b" in result.output
    assert "pipe_a" not in result.output
