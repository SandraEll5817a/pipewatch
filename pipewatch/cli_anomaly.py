"""CLI command for anomaly detection against pipeline run history."""
from __future__ import annotations

import click
from pipewatch.config import load_config
from pipewatch.history import load_history, PipelineRun
from pipewatch.anomaly import detect_anomalies


@click.group(name="anomaly")
def anomaly_command():
    """Detect anomalies in pipeline metrics."""


@anomaly_command.command("check")
@click.option("--config", "config_path", default="pipewatch.yaml", show_default=True)
@click.option("--history-file", default="pipewatch_history.json", show_default=True)
@click.option("--z-threshold", default=2.5, show_default=True, type=float)
@click.option("--min-samples", default=5, show_default=True, type=int)
@click.option("--pipeline", "pipeline_name", default=None, help="Limit to one pipeline.")
def check_anomalies(config_path, history_file, z_threshold, min_samples, pipeline_name):
    """Check recent runs for anomalous metric values."""
    app_config = load_config(config_path)
    pipelines = app_config.pipelines
    if pipeline_name:
        pipelines = [p for p in pipelines if p.name == pipeline_name]
        if not pipelines:
            click.echo(f"Pipeline '{pipeline_name}' not found.", err=True)
            raise SystemExit(1)

    all_runs = load_history(history_file)
    found_any = False

    for pipeline in pipelines:
        runs = [r for r in all_runs if r.pipeline == pipeline.name]
        if len(runs) < min_samples + 1:
            click.echo(f"{pipeline.name}: not enough history (need {min_samples + 1} runs).")
            continue

        *history, current = runs
        results = detect_anomalies(
            pipeline.name, history, current,
            z_threshold=z_threshold,
            min_samples=min_samples,
        )
        anomalies = [r for r in results if r.is_anomaly]
        if anomalies:
            found_any = True
            for a in anomalies:
                click.echo(str(a))
        else:
            click.echo(f"{pipeline.name}: no anomalies detected.")

    raise SystemExit(1 if found_any else 0)
