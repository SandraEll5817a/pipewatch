"""CLI commands for stale-alert detection."""
from __future__ import annotations

import sys
from typing import Optional

import click

from pipewatch.config import load_config
from pipewatch.history import load_history
from pipewatch.stale_alert import check_all_stale


@click.group(name="stale")
def stale_command() -> None:
    """Commands for detecting stale pipelines."""


@stale_command.command(name="check")
@click.option("--config", "config_path", default="pipewatch.yaml", show_default=True)
@click.option("--history", "history_path", default="pipewatch_history.json", show_default=True)
@click.option(
    "--threshold",
    "threshold_minutes",
    default=60,
    show_default=True,
    help="Minutes without a run before a pipeline is considered stale.",
)
@click.option("--pipeline", "pipeline_filter", default=None, help="Check only this pipeline.")
def check_stale_cmd(
    config_path: str,
    history_path: str,
    threshold_minutes: int,
    pipeline_filter: Optional[str],
) -> None:
    """Check whether pipelines have reported runs recently."""
    app = load_config(config_path)
    all_runs = load_history(history_path)

    # Group runs by pipeline name
    runs_by_pipeline: dict = {p.name: [] for p in app.pipelines}
    for run in all_runs:
        if run.pipeline in runs_by_pipeline:
            runs_by_pipeline[run.pipeline].append(run)

    if pipeline_filter:
        if pipeline_filter not in runs_by_pipeline:
            click.echo(f"Pipeline '{pipeline_filter}' not found in config.", err=True)
            sys.exit(2)
        runs_by_pipeline = {pipeline_filter: runs_by_pipeline[pipeline_filter]}

    results = check_all_stale(runs_by_pipeline, stale_after_minutes=threshold_minutes)

    stale_count = 0
    for result in sorted(results, key=lambda r: r.pipeline):
        click.echo(str(result))
        if result.is_stale:
            stale_count += 1

    if stale_count:
        click.echo(f"\n{stale_count} pipeline(s) are stale.", err=True)
        sys.exit(1)
    else:
        click.echo("\nAll pipelines are up to date.")
        sys.exit(0)
