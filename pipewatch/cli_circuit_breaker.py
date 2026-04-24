"""CLI commands for inspecting and resetting circuit breaker state."""
from __future__ import annotations

import click

from pipewatch.circuit_breaker import (
    CircuitBreakerPolicy,
    CircuitState,
    _STATE_FILE,
    is_open,
    load_circuit_states,
    save_circuit_states,
)


@click.group(name="circuit")
def circuit_command() -> None:
    """Inspect and manage circuit breaker state for webhook delivery."""


@circuit_command.command(name="list")
def list_circuits() -> None:
    """List all circuit breaker states."""
    states = load_circuit_states()
    if not states:
        click.echo("No circuit breaker state recorded.")
        return
    policy = CircuitBreakerPolicy()
    for name, state in sorted(states.items()):
        open_flag = is_open(state, policy)
        status = "OPEN" if open_flag else state.state.upper()
        click.echo(
            f"{name}: {status}  failures={state.consecutive_failures}"
            + (f"  opened_at={state.opened_at}" if state.opened_at else "")
        )


@circuit_command.command(name="reset")
@click.argument("pipeline")
def reset_circuit(pipeline: str) -> None:
    """Reset circuit breaker for PIPELINE to closed state."""
    states = load_circuit_states()
    if pipeline not in states:
        click.echo(f"No circuit state found for '{pipeline}'.")
        raise SystemExit(1)
    state = states[pipeline]
    state.state = "closed"
    state.consecutive_failures = 0
    state.opened_at = None
    save_circuit_states(states)
    click.echo(f"Circuit for '{pipeline}' reset to closed.")


@circuit_command.command(name="clear")
def clear_circuits() -> None:
    """Remove all circuit breaker state."""
    if _STATE_FILE.exists():
        _STATE_FILE.unlink()
    click.echo("All circuit breaker state cleared.")
