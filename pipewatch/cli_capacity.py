"""CLI sub-command: capacity — check pipeline capacity utilisation."""
from __future__ import annotations

import sys
from typing import Optional

import click

from pipewatch.capacity import check_capacity
from pipewatch.config import load_config
from pipewatch.history import load_history


@click.group(name="capacity")
def capacity_command() -> None:
    """Capacity planning commands."""


@capacity_command.command(name="check")
@click.option("--config", "config_path", default="pipewatch.yaml", show_default=True)
@click.option("--pipeline", "pipeline_filter", default=None,
              help="Limit check to a single pipeline.")
@click.option("--history", "history_path", default=".pipewatch_history.json",
              show_default=True)
def check_capacity_cmd(
    config_path: str,
    pipeline_filter: Optional[str],
    history_path: str,
) -> None:
    """Report capacity utilisation for all (or one) pipeline(s)."""
    app = load_config(config_path)
    pipelines = app.pipelines
    if pipeline_filter:
        pipelines = [p for p in pipelines if p.name == pipeline_filter]
        if not pipelines:
            click.echo(f"Pipeline '{pipeline_filter}' not found.", err=True)
            sys.exit(1)

    at_risk_count = 0
    for pipeline in pipelines:
        ceiling = getattr(pipeline.thresholds, "max_rows", None)
        if ceiling is None or ceiling <= 0:
            click.echo(f"{pipeline.name}: no max_rows threshold configured – skipped")
            continue

        runs = load_history(history_path, pipeline.name)
        result = check_capacity(pipeline.name, runs, ceiling=ceiling)
        status = click.style("AT RISK", fg="red") if result.at_risk else click.style("ok", fg="green")
        click.echo(f"{pipeline.name}: [{status}] {result.message}")
        if result.at_risk:
            at_risk_count += 1

    sys.exit(1 if at_risk_count else 0)
