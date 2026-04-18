"""CLI commands for escalation policy inspection."""
from __future__ import annotations

import click
from pipewatch.config import load_config
from pipewatch.history import load_history
from pipewatch.escalation import (
    EscalationPolicy,
    EscalationState,
    should_escalate,
    update_state,
)


@click.group(name="escalation")
def escalation_command():
    """Manage and inspect pipeline escalation state."""


@escalation_command.command(name="check")
@click.option("--config", "config_path", default="pipewatch.yaml", show_default=True)
@click.option("--secondary-webhook", required=True, help="Secondary webhook URL for escalation.")
@click.option("--threshold", default=3, show_default=True, help="Consecutive failures before escalating.")
@click.option("--cooldown", default=60, show_default=True, help="Minutes between escalations.")
def check_escalation(config_path: str, secondary_webhook: str, threshold: int, cooldown: int):
    """Check which pipelines should be escalated based on recent history."""
    app = load_config(config_path)
    policy = EscalationPolicy(
        secondary_webhook=secondary_webhook,
        failure_threshold=threshold,
        cooldown_minutes=cooldown,
    )

    any_escalated = False
    for pipeline in app.pipelines:
        runs = load_history(pipeline.name)
        state = EscalationState(pipeline=pipeline.name)
        for run in runs:
            state = update_state(state, healthy=run.healthy)

        result = should_escalate(state, policy)
        click.echo(str(result))
        if result.escalated:
            any_escalated = True

    raise SystemExit(1 if any_escalated else 0)
