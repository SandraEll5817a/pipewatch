"""Circuit breaker for pipeline webhook notifications.

Prevents hammering a failing webhook endpoint by tracking consecutive
failures and opening the circuit after a configurable threshold.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

_STATE_FILE = Path(".pipewatch_circuit.json")


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class CircuitBreakerPolicy:
    failure_threshold: int = 3       # open after this many consecutive failures
    recovery_timeout: int = 300      # seconds before attempting half-open


@dataclass
class CircuitState:
    pipeline: str
    consecutive_failures: int = 0
    opened_at: Optional[str] = None  # ISO timestamp when circuit opened
    state: str = "closed"            # closed | open | half-open

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "consecutive_failures": self.consecutive_failures,
            "opened_at": self.opened_at,
            "state": self.state,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "CircuitState":
        return cls(
            pipeline=d["pipeline"],
            consecutive_failures=d.get("consecutive_failures", 0),
            opened_at=d.get("opened_at"),
            state=d.get("state", "closed"),
        )


def load_circuit_states(path: Path = _STATE_FILE) -> dict[str, CircuitState]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text())
        return {k: CircuitState.from_dict(v) for k, v in data.items()}
    except (json.JSONDecodeError, KeyError):
        return {}


def save_circuit_states(states: dict[str, CircuitState], path: Path = _STATE_FILE) -> None:
    path.write_text(json.dumps({k: v.to_dict() for k, v in states.items()}, indent=2))


def is_open(state: CircuitState, policy: CircuitBreakerPolicy) -> bool:
    """Return True if the circuit is open (calls should be blocked)."""
    if state.state == "closed":
        return False
    if state.state == "open" and state.opened_at:
        opened = datetime.fromisoformat(state.opened_at)
        elapsed = (_utcnow() - opened).total_seconds()
        if elapsed >= policy.recovery_timeout:
            state.state = "half-open"
            return False
        return True
    return False  # half-open: allow one probe


def record_success(state: CircuitState) -> None:
    """Record a successful delivery; close the circuit."""
    state.consecutive_failures = 0
    state.opened_at = None
    state.state = "closed"


def record_failure(state: CircuitState, policy: CircuitBreakerPolicy) -> None:
    """Record a failed delivery; open circuit if threshold reached."""
    state.consecutive_failures += 1
    if state.consecutive_failures >= policy.failure_threshold:
        state.state = "open"
        state.opened_at = _utcnow().isoformat()
