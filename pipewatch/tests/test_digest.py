"""Tests for pipewatch.digest."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from pipewatch.digest import DigestEntry, DigestReport, build_digest
from pipewatch.summary import PipelineSummary, RunSummary

_FIXED_NOW = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


def _run(healthy: bool) -> RunSummary:
    return RunSummary(
        checked_at=_FIXED_NOW.isoformat(),
        healthy=healthy,
        violations=[],
    )


def _summary(name: str, runs: list[RunSummary]) -> PipelineSummary:
    return PipelineSummary(pipeline_name=name, runs=runs)


# ---------------------------------------------------------------------------
# DigestEntry
# ---------------------------------------------------------------------------

def test_digest_entry_to_dict_keys():
    entry = DigestEntry(
        pipeline_name="pipe_a",
        total_runs=5,
        healthy_runs=4,
        failing_runs=1,
        last_status="healthy",
    )
    d = entry.to_dict()
    assert set(d.keys()) == {
        "pipeline_name", "total_runs", "healthy_runs", "failing_runs", "last_status"
    }
    assert d["pipeline_name"] == "pipe_a"
    assert d["failing_runs"] == 1


# ---------------------------------------------------------------------------
# DigestReport
# ---------------------------------------------------------------------------

def test_digest_report_all_healthy_true():
    entries = [
        DigestEntry("a", 2, 2, 0, "healthy"),
        DigestEntry("b", 3, 3, 0, "healthy"),
    ]
    report = DigestReport(generated_at=_FIXED_NOW, period_label="daily", entries=entries)
    assert report.all_healthy is True
    assert report.total_pipelines == 2


def test_digest_report_all_healthy_false_when_one_unhealthy():
    entries = [
        DigestEntry("a", 2, 2, 0, "healthy"),
        DigestEntry("b", 3, 2, 1, "unhealthy"),
    ]
    report = DigestReport(generated_at=_FIXED_NOW, period_label="daily", entries=entries)
    assert report.all_healthy is False


def test_digest_report_to_dict_structure():
    entries = [DigestEntry("x", 1, 1, 0, "healthy")]
    report = DigestReport(generated_at=_FIXED_NOW, period_label="weekly", entries=entries)
    d = report.to_dict()
    assert d["period_label"] == "weekly"
    assert d["total_pipelines"] == 1
    assert d["all_healthy"] is True
    assert isinstance(d["entries"], list)
    assert d["generated_at"] == _FIXED_NOW.isoformat()


# ---------------------------------------------------------------------------
# build_digest
# ---------------------------------------------------------------------------

def test_build_digest_empty_summaries():
    report = build_digest([], period_label="test", now=_FIXED_NOW)
    assert report.total_pipelines == 0
    assert report.all_healthy is True  # vacuously true
    assert report.period_label == "test"


def test_build_digest_counts_correctly():
    runs = [_run(True), _run(False), _run(True)]
    summaries = [_summary("etl_load", runs)]
    report = build_digest(summaries, now=_FIXED_NOW)
    entry = report.entries[0]
    assert entry.total_runs == 3
    assert entry.healthy_runs == 2
    assert entry.failing_runs == 1


def test_build_digest_last_status_unhealthy_when_last_run_fails():
    runs = [_run(True), _run(True), _run(False)]
    summaries = [_summary("etl_load", runs)]
    report = build_digest(summaries, now=_FIXED_NOW)
    assert report.entries[0].last_status == "unhealthy"


def test_build_digest_last_status_unknown_when_no_runs():
    summaries = [_summary("empty_pipe", [])]
    report = build_digest(summaries, now=_FIXED_NOW)
    assert report.entries[0].last_status == "unknown"


def test_build_digest_uses_utcnow_when_no_now_provided():
    fake_now = datetime(2024, 1, 15, 8, 0, 0, tzinfo=timezone.utc)
    with patch("pipewatch.digest._utcnow", return_value=fake_now):
        report = build_digest([])
    assert report.generated_at == fake_now
