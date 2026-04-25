"""CLI commands for SLA compliance checking."""

from __future__ import annotations

import sys

import click

from pipewatch.config import load_config
from pipewatch.history import load_history
from pipewatch.sla import SLAPolicy, SLAResult, check_sla


@click.group(name="sla")
def sla_command() -> None:
    """SLA compliance commands."""


@sla_command.command(name="check")
@click.option("--config", "config_path", default="pipewatch.yaml", show_default=True)
@click.option("--pipeline", "pipeline_name", default=None, help="Check a single pipeline.")
@click.option("--history", "history_path", default=".pipewatch_history.json", show_default=True)
def check_sla_cmd(
    config_path: str,
    pipeline_name: str | None,
    history_path: str,
) -> None:
    """Check SLA compliance for all (or one) pipeline."""
    app = load_config(config_path)
    pipelines = app.pipelines
    if pipeline_name:
        pipelines = [p for p in pipelines if p.name == pipeline_name]
        if not pipelines:
            click.echo(f"Pipeline '{pipeline_name}' not found.", err=True)
            sys.exit(1)

    all_runs = load_history(history_path)
    any_breach = False

    for pipeline in pipelines:
        t = pipeline.thresholds
        policy = SLAPolicy(
            max_duration_seconds=t.max_duration_seconds,
            max_error_rate=t.max_error_rate,
            min_rows_processed=t.min_rows_processed,
        )
        runs = [r for r in all_runs if r.pipeline == pipeline.name]
        result: SLAResult = check_sla(pipeline.name, runs, policy)

        status = "OK" if result.compliant else "BREACH"
        click.echo(f"[{status}] {pipeline.name}")
        for v in result.violations:
            click.echo(f"  - {v}")

        if not result.compliant:
            any_breach = True

    sys.exit(1 if any_breach else 0)
