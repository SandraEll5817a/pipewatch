"""CLI entry point for pipewatch using Click."""

import sys
import click

from pipewatch.config import load_config
from pipewatch.runner import run_all_checks


@click.group()
@click.version_option(version="0.1.0", prog_name="pipewatch")
def cli():
    """pipewatch — Monitor and alert on ETL pipeline health."""
    pass


@cli.command("check")
@click.option(
    "--config",
    "-c",
    default="pipewatch.yaml",
    show_default=True,
    help="Path to the configuration file.",
)
@click.option(
    "--pipeline",
    "-p",
    default=None,
    help="Run check for a single named pipeline only.",
)
@click.option(
    "--notify/--no-notify",
    default=True,
    show_default=True,
    help="Send webhook notifications on violations.",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    default=False,
    help="Print detailed output for every pipeline.",
)
def check(config: str, pipeline: str | None, notify: bool, verbose: bool):
    """Run health checks against configured pipelines."""
    try:
        app_config = load_config(config)
    except FileNotFoundError:
        click.echo(f"[error] Config file not found: {config}", err=True)
        sys.exit(1)
    except Exception as exc:  # noqa: BLE001
        click.echo(f"[error] Failed to load config: {exc}", err=True)
        sys.exit(1)

    pipelines = app_config.pipelines
    if pipeline:
        pipelines = [p for p in pipelines if p.name == pipeline]
        if not pipelines:
            click.echo(f"[error] Pipeline '{pipeline}' not found in config.", err=True)
            sys.exit(1)

    results = run_all_checks(app_config, send_notifications=notify)

    any_unhealthy = False
    for result in results:
        status = "OK" if result.healthy else "FAIL"
        click.echo(f"[{status}] {result.pipeline_name}")
        if verbose or not result.healthy:
            for v in result.violations:
                click.echo(f"       - {v}")
        if not result.healthy:
            any_unhealthy = True

    sys.exit(1 if any_unhealthy else 0)


@cli.command("list")
@click.option(
    "--config",
    "-c",
    default="pipewatch.yaml",
    show_default=True,
    help="Path to the configuration file.",
)
def list_pipelines(config: str):
    """List all configured pipelines."""
    try:
        app_config = load_config(config)
    except Exception as exc:  # noqa: BLE001
        click.echo(f"[error] {exc}", err=True)
        sys.exit(1)

    click.echo(f"Pipelines ({len(app_config.pipelines)}):")
    for p in app_config.pipelines:
        click.echo(f"  - {p.name}")


if __name__ == "__main__":
    cli()
