"""CLI commands for pipeline metric forecasting."""
import click
from pipewatch.config import load_config
from pipewatch.history import load_history
from pipewatch.forecast import forecast_metric

METRICS = ["error_rate", "duration", "rows_processed"]


@click.group(name="forecast")
def forecast_command():
    """Forecast future pipeline metrics using historical trends."""


@forecast_command.command(name="show")
@click.option("--config", "config_path", default="pipewatch.yaml", show_default=True)
@click.option("--pipeline", "pipeline_name", default=None, help="Filter to one pipeline.")
@click.option("--metric", default="error_rate", type=click.Choice(METRICS), show_default=True)
@click.option("--horizon", default=1, show_default=True, help="Steps ahead to forecast.")
@click.option("--history", "history_path", default=".pipewatch_history.json", show_default=True)
def show_forecast(config_path, pipeline_name, metric, horizon, history_path):
    """Show metric forecasts for configured pipelines."""
    app = load_config(config_path)
    pipelines = app.pipelines
    if pipeline_name:
        pipelines = [p for p in pipelines if p.name == pipeline_name]
        if not pipelines:
            click.echo(f"Pipeline '{pipeline_name}' not found.", err=True)
            raise SystemExit(1)

    history = load_history(history_path)
    any_flagged = False

    for pipeline in pipelines:
        runs = [r for r in history if r.pipeline == pipeline.name]
        result = forecast_metric(pipeline.name, runs, metric, horizon)
        click.echo(str(result))
        if not result.insufficient_data:
            threshold = getattr(pipeline.thresholds, metric, None)
            if threshold is not None and result.predicted_value > threshold:
                click.echo(
                    f"  WARNING: predicted {metric} {result.predicted_value:.4f} "
                    f"exceeds threshold {threshold}"
                )
                any_flagged = True

    raise SystemExit(1 if any_flagged else 0)
