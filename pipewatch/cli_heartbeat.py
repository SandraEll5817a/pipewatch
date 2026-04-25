"""CLI commands for heartbeat monitoring."""

from __future__ import annotations

import sys
from typing import Optional

import click

from pipewatch.config import load_config
from pipewatch.heartbeat import check_all_heartbeats
from pipewatch.history import load_history


@click.group(name="heartbeat")
def heartbeat_command() -> None:
    """Monitor pipeline heartbeats."""


@heartbeat_command.command(name="check")
@click.option("--config", "config_path", default="pipewatch.yaml", show_default=True)
@click.option(
    "--grace",
    default=60,
    show_default=True,
    help="Grace period in seconds beyond the expected interval.",
)
@click.option(
    "--pipeline",
    "pipeline_filter",
    default=None,
    help="Check only this pipeline.",
)
def check_heartbeats(
    config_path: str, grace: int, pipeline_filter: Optional[str]
) -> None:
    """Check that all pipelines are reporting within their expected intervals."""
    app = load_config(config_path)

    pipelines = app.pipelines
    if pipeline_filter:
        pipelines = [p for p in pipelines if p.name == pipeline_filter]
        if not pipelines:
            click.echo(f"No pipeline named '{pipeline_filter}' found.", err=True)
            sys.exit(2)

    pairs = []
    runs_by_pipeline = {}
    for p in pipelines:
        interval = getattr(p, "expected_interval_seconds", None)
        if interval is None:
            continue
        pairs.append((p.name, interval))
        runs_by_pipeline[p.name] = load_history(p.name)

    if not pairs:
        click.echo("No pipelines with 'expected_interval_seconds' configured.")
        sys.exit(0)

    results = check_all_heartbeats(
        pairs, runs_by_pipeline, grace_seconds=grace
    )

    dead_count = 0
    for r in results:
        status = "OK" if r.alive else "DEAD"
        click.echo(f"[{status}] {r}")
        if not r.alive:
            dead_count += 1

    if dead_count:
        click.echo(
            f"\n{dead_count} pipeline(s) missed their heartbeat.", err=True
        )
        sys.exit(1)

    click.echo(f"\nAll {len(results)} pipeline(s) alive.")
    sys.exit(0)
