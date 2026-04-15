"""CLI commands for managing and inspecting pipeline baselines."""
from __future__ import annotations

from pathlib import Path

import click

from pipewatch.baseline import (
    DEFAULT_BASELINE_PATH,
    PipelineBaseline,
    load_baselines,
    save_baselines,
)
from pipewatch.config import load_config
from pipewatch.history import load_history


@click.group("baseline")
def baseline_command() -> None:
    """Manage pipeline metric baselines."""


@baseline_command.command("set")
@click.argument("pipeline_name")
@click.option("--duration", type=float, required=True, help="Avg duration in seconds.")
@click.option("--error-rate", type=float, required=True, help="Avg error rate (0.0-1.0).")
@click.option("--row-count", type=float, required=True, help="Avg row count.")
@click.option(
    "--baseline-file",
    default=str(DEFAULT_BASELINE_PATH),
    show_default=True,
    help="Path to baseline JSON file.",
)
def set_baseline(
    pipeline_name: str,
    duration: float,
    error_rate: float,
    row_count: float,
    baseline_file: str,
) -> None:
    """Record a manual baseline for a pipeline."""
    path = Path(baseline_file)
    baselines = load_baselines(path)
    baselines[pipeline_name] = PipelineBaseline(
        pipeline_name=pipeline_name,
        avg_duration_seconds=duration,
        avg_error_rate=error_rate,
        avg_row_count=row_count,
    )
    save_baselines(baselines, path=path)
    click.echo(f"Baseline saved for '{pipeline_name}' → {path}")


@baseline_command.command("show")
@click.option(
    "--baseline-file",
    default=str(DEFAULT_BASELINE_PATH),
    show_default=True,
    help="Path to baseline JSON file.",
)
def show_baselines(baseline_file: str) -> None:
    """Display all recorded baselines."""
    path = Path(baseline_file)
    baselines = load_baselines(path)
    if not baselines:
        click.echo("No baselines recorded.")
        return
    for name, b in baselines.items():
        click.echo(
            f"{name}: duration={b.avg_duration_seconds}s  "
            f"error_rate={b.avg_error_rate}  "
            f"row_count={b.avg_row_count}"
        )


@baseline_command.command("clear")
@click.argument("pipeline_name")
@click.option(
    "--baseline-file",
    default=str(DEFAULT_BASELINE_PATH),
    show_default=True,
)
def clear_baseline(pipeline_name: str, baseline_file: str) -> None:
    """Remove the baseline for a specific pipeline."""
    path = Path(baseline_file)
    baselines = load_baselines(path)
    if pipeline_name not in baselines:
        click.echo(f"No baseline found for '{pipeline_name}'.")
        raise SystemExit(1)
    del baselines[pipeline_name]
    save_baselines(baselines, path=path)
    click.echo(f"Baseline cleared for '{pipeline_name}'.")
