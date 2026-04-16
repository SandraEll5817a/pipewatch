"""Tests for pipewatch.cli_tags."""
import pytest
from click.testing import CliRunner
from unittest.mock import patch, MagicMock

from pipewatch.cli_tags import tags_command


def _pipeline(name, tags):
    p = MagicMock()
    p.name = name
    p.tags = tags
    return p


def _make_app(*pipelines):
    app = MagicMock()
    app.pipelines = list(pipelines)
    return app


@pytest.fixture
def runner():
    return CliRunner()


def test_list_by_tags_all_match(runner):
    app = _make_app(_pipeline("a", ["etl"]), _pipeline("b", ["etl", "daily"]))
    with patch("pipewatch.cli_tags.load_config", return_value=app):
        result = runner.invoke(tags_command, ["list"])
    assert result.exit_code == 0
    assert "a" in result.output
    assert "b" in result.output


def test_list_by_tags_require_filters(runner):
    app = _make_app(_pipeline("a", ["etl"]), _pipeline("b", ["weekly"]))
    with patch("pipewatch.cli_tags.load_config", return_value=app):
        result = runner.invoke(tags_command, ["list", "--require", "etl"])
    assert result.exit_code == 0
    assert "a" in result.output
    assert "b" not in result.output


def test_list_by_tags_no_match(runner):
    app = _make_app(_pipeline("a", ["etl"]))
    with patch("pipewatch.cli_tags.load_config", return_value=app):
        result = runner.invoke(tags_command, ["list", "--require", "weekly"])
    assert result.exit_code == 0
    assert "No pipelines" in result.output


def test_tag_index_shows_all_tags(runner):
    app = _make_app(
        _pipeline("a", ["etl", "daily"]),
        _pipeline("b", ["etl"]),
    )
    with patch("pipewatch.cli_tags.load_config", return_value=app):
        result = runner.invoke(tags_command, ["index"])
    assert result.exit_code == 0
    assert "etl" in result.output
    assert "daily" in result.output


def test_tag_index_empty(runner):
    app = _make_app(_pipeline("a", None))
    with patch("pipewatch.cli_tags.load_config", return_value=app):
        result = runner.invoke(tags_command, ["index"])
    assert result.exit_code == 0
    assert "No tags found" in result.output
