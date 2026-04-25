"""Tests for pipewatch.budget."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from pipewatch.budget import (
    BudgetPolicy,
    BudgetResult,
    BudgetViolation,
    check_budget,
    check_all_budgets,
)
from pipewatch.history import PipelineRun


def _run(duration: float = 10.0, rows: int = 100, error_rate: float = 0.0) -> PipelineRun:
    return PipelineRun(
        pipeline="pipe_a",
        timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc),
        duration_seconds=duration,
        rows_processed=rows,
        error_rate=error_rate,
        healthy=error_rate == 0.0,
    )


def test_no_runs_returns_compliant():
    policy = BudgetPolicy(max_duration_seconds=60.0)
    result = check_budget("pipe_a", [], policy)
    assert result.compliant is True
    assert result.violations == []


def test_within_all_limits_is_compliant():
    policy = BudgetPolicy(max_duration_seconds=60.0, max_rows_processed=1000, max_error_rate=0.05)
    result = check_budget("pipe_a", [_run(30.0, 500, 0.01)], policy)
    assert result.compliant is True
    assert result.violations == []


def test_duration_violation():
    policy = BudgetPolicy(max_duration_seconds=5.0)
    result = check_budget("pipe_a", [_run(duration=10.0)], policy)
    assert result.compliant is False
    assert len(result.violations) == 1
    assert result.violations[0].metric == "duration_seconds"
    assert result.violations[0].actual == 10.0
    assert result.violations[0].limit == 5.0


def test_rows_violation():
    policy = BudgetPolicy(max_rows_processed=50)
    result = check_budget("pipe_a", [_run(rows=200)], policy)
    assert result.compliant is False
    assert result.violations[0].metric == "rows_processed"


def test_error_rate_violation():
    policy = BudgetPolicy(max_error_rate=0.01)
    result = check_budget("pipe_a", [_run(error_rate=0.10)], policy)
    assert result.compliant is False
    assert result.violations[0].metric == "error_rate"


def test_multiple_violations_reported():
    policy = BudgetPolicy(max_duration_seconds=5.0, max_error_rate=0.01)
    result = check_budget("pipe_a", [_run(duration=20.0, error_rate=0.5)], policy)
    assert result.compliant is False
    assert len(result.violations) == 2


def test_budget_violation_str():
    v = BudgetViolation(metric="duration_seconds", limit=5.0, actual=10.0)
    assert "duration_seconds" in str(v)
    assert "10.00" in str(v)


def test_budget_result_str_compliant():
    result = BudgetResult(pipeline="pipe_a", compliant=True)
    assert "within budget" in str(result)


def test_budget_result_str_breach():
    v = BudgetViolation("duration_seconds", 5.0, 10.0)
    result = BudgetResult(pipeline="pipe_a", compliant=False, violations=[v])
    assert "budget exceeded" in str(result)


def test_budget_result_to_dict_keys():
    result = BudgetResult(pipeline="pipe_a", compliant=True)
    d = result.to_dict()
    assert set(d.keys()) == {"pipeline", "compliant", "violations", "checked_at"}


def test_check_all_budgets_skips_missing_policy():
    runs = {"pipe_a": [_run()], "pipe_b": [_run()]}
    policies = {"pipe_a": BudgetPolicy(max_duration_seconds=60.0)}
    results = check_all_budgets(["pipe_a", "pipe_b"], runs, policies)
    assert len(results) == 1
    assert results[0].pipeline == "pipe_a"


def test_check_all_budgets_returns_all_with_policies():
    runs = {"pipe_a": [_run(duration=100.0)], "pipe_b": [_run(duration=1.0)]}
    policy = BudgetPolicy(max_duration_seconds=50.0)
    results = check_all_budgets(["pipe_a", "pipe_b"], runs, {"pipe_a": policy, "pipe_b": policy})
    assert len(results) == 2
    breaches = [r for r in results if not r.compliant]
    assert len(breaches) == 1
    assert breaches[0].pipeline == "pipe_a"
