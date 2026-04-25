"""CLI commands for pipeline budget checking."""
from __future__ import annotations

import click

from pipewatch.budget import BudgetPolicy, check_all_budgets
from pipewatch.config import load_config
from pipewatch.history import load_history


@click.group(name="budget")
def budget_command() -> None:
    """Check pipeline runtime budgets."""


@budget_command.command(name="check")
@click.option("--config", "config_path", default="pipewatch.yaml", show_default=True)
@click.option("--max-duration", type=float, default=None, help="Max duration in seconds.")
@click.option("--max-rows", type=int, default=None, help="Max rows processed.")
@click.option("--max-error-rate", type=float, default=None, help="Max error rate (0.0–1.0).")
@click.option("--pipeline", "pipeline_filter", default=None, help="Limit to one pipeline.")
def check_budgets(
    config_path: str,
    max_duration: float | None,
    max_rows: int | None,
    max_error_rate: float | None,
    pipeline_filter: str | None,
) -> None:
    """Check all (or one) pipeline against a budget policy."""
    app = load_config(config_path)
    policy = BudgetPolicy(
        max_duration_seconds=max_duration,
        max_rows_processed=max_rows,
        max_error_rate=max_error_rate,
    )

    if policy.max_duration_seconds is None and policy.max_rows_processed is None and policy.max_error_rate is None:
        click.echo("No budget constraints specified. Use --max-duration, --max-rows, or --max-error-rate.")
        raise SystemExit(1)

    names = [p.name for p in app.pipelines]
    if pipeline_filter:
        names = [n for n in names if n == pipeline_filter]
        if not names:
            click.echo(f"Pipeline '{pipeline_filter}' not found.")
            raise SystemExit(1)

    runs_by_pipeline = {name: load_history(name) for name in names}
    policies = {name: policy for name in names}

    results = check_all_budgets(names, runs_by_pipeline, policies)

    any_breach = False
    for result in results:
        status = "OK" if result.compliant else "BREACH"
        click.echo(f"[{status}] {result}")
        if not result.compliant:
            any_breach = True

    raise SystemExit(1 if any_breach else 0)
