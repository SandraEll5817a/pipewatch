"""CLI command for running pipewatch in scheduled/daemon mode."""

from __future__ import annotations

import click

from pipewatch.config import load_config
from pipewatch.runner import run_pipeline_check
from pipewatch.notifier import notify_violations
from pipewatch.schedule import build_schedule, SchedulerConfig, run_scheduler


def _make_check_fn(app_config, webhook_url: str | None):
    """Return a callable that checks a single pipeline by name."""

    def _check(pipeline_name: str) -> None:
        from pipewatch.config import get_pipeline

        pipeline_cfg = get_pipeline(app_config, pipeline_name)
        if pipeline_cfg is None:
            click.echo(f"[schedule] Pipeline '{pipeline_name}' not found — skipping.", err=True)
            return

        result = run_pipeline_check(pipeline_cfg)
        status = "healthy" if result.healthy else "UNHEALTHY"
        click.echo(f"[schedule] {pipeline_name}: {status}")

        if not result.healthy:
            url = webhook_url or app_config.default_webhook
            if url:
                sent = notify_violations(
                    pipeline_name=pipeline_name,
                    violations=result.violations,
                    webhook_url=url,
                )
                if not sent:
                    click.echo(f"[schedule] Warning: webhook notification failed for '{pipeline_name}'.", err=True)

    return _check


@click.command("schedule")
@click.option(
    "--config",
    "config_path",
    default="pipewatch.yaml",
    show_default=True,
    help="Path to pipewatch config file.",
)
@click.option(
    "--interval",
    default=300,
    show_default=True,
    type=int,
    help="Seconds between checks for each pipeline.",
)
@click.option(
    "--tick",
    default=10,
    show_default=True,
    type=int,
    help="Scheduler loop tick interval in seconds.",
)
@click.option(
    "--max-ticks",
    default=None,
    type=int,
    help="Stop after this many ticks (useful for testing).",
)
@click.option("--webhook", default=None, help="Override webhook URL for alerts.")
def schedule_command(config_path, interval, tick, max_ticks, webhook):
    """Run pipeline checks on a repeating schedule (daemon mode)."""
    app_config = load_config(config_path)
    pipeline_names = [p.name for p in app_config.pipelines]

    if not pipeline_names:
        click.echo("No pipelines configured. Exiting.", err=True)
        raise SystemExit(1)

    click.echo(
        f"Starting scheduler: {len(pipeline_names)} pipeline(s), "
        f"interval={interval}s, tick={tick}s"
    )

    pipelines = build_schedule(pipeline_names, interval_seconds=interval)
    config = SchedulerConfig(pipelines=pipelines, tick_seconds=tick, max_ticks=max_ticks)
    check_fn = _make_check_fn(app_config, webhook_url=webhook)

    total = run_scheduler(config, check_fn=check_fn)
    click.echo(f"Scheduler finished. Total checks run: {total}")
