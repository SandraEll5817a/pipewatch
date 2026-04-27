"""CLI sub-command: pipewatch backpressure — detect throughput decline."""
from __future__ import annotations

import sys

import click

from pipewatch.backpressure import detect_backpressure
from pipewatch.config import load_config
from pipewatch.history import load_history


@click.group(name="backpressure")
def backpressure_command() -> None:
    """Detect back-pressure by monitoring throughput trends."""


@backpressure_command.command(name="check")
@click.option("--config", "config_path", default="pipewatch.yaml", show_default=True)
@click.option("--history", "history_path", default=".pipewatch_history.json", show_default=True)
@click.option("--min-runs", default=5, show_default=True, help="Minimum runs required.")
@click.option("--slope-threshold", default=-5.0, show_default=True,
              help="Slope below which pressure is flagged.")
@click.option("--pipeline", "pipeline_filter", default=None,
              help="Limit check to a single pipeline.")
def check_backpressure(
    config_path: str,
    history_path: str,
    min_runs: int,
    slope_threshold: float,
    pipeline_filter: str | None,
) -> None:
    """Check all (or one) pipeline(s) for back-pressure."""
    app = load_config(config_path)
    pipelines = (
        [p for p in app.pipelines if p.name == pipeline_filter]
        if pipeline_filter
        else app.pipelines
    )

    if not pipelines:
        click.echo("No matching pipelines found.", err=True)
        sys.exit(1)

    pressured = []
    for pipeline in pipelines:
        runs = load_history(pipeline.name, path=history_path)
        result = detect_backpressure(
            pipeline.name, runs,
            min_runs=min_runs,
            slope_threshold=slope_threshold,
        )
        click.echo(str(result))
        if result.is_pressured:
            pressured.append(result)

    if pressured:
        click.echo(f"\n{len(pressured)} pipeline(s) under back-pressure.", err=True)
        sys.exit(1)

    sys.exit(0)
