"""Escalation policy: send to a secondary webhook after repeated failures."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Optional


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class EscalationPolicy:
    secondary_webhook: str
    failure_threshold: int = 3  # consecutive failures before escalating
    cooldown_minutes: int = 60


@dataclass
class EscalationState:
    pipeline: str
    consecutive_failures: int = 0
    last_escalated: Optional[datetime] = None
    escalation_count: int = 0


@dataclass
class EscalationResult:
    pipeline: str
    escalated: bool
    reason: str
    consecutive_failures: int
    secondary_webhook: Optional[str] = None

    def __str__(self) -> str:
        if self.escalated:
            return f"[ESCALATED] {self.pipeline}: {self.reason}"
        return f"[OK] {self.pipeline}: {self.reason}"


def should_escalate(
    state: EscalationState,
    policy: EscalationPolicy,
    now: Optional[datetime] = None,
) -> EscalationResult:
    now = now or _utcnow()

    if state.consecutive_failures < policy.failure_threshold:
        return EscalationResult(
            pipeline=state.pipeline,
            escalated=False,
            reason=f"only {state.consecutive_failures} consecutive failures",
            consecutive_failures=state.consecutive_failures,
        )

    if state.last_escalated is not None:
        elapsed = (now - state.last_escalated).total_seconds() / 60
        if elapsed < policy.cooldown_minutes:
            return EscalationResult(
                pipeline=state.pipeline,
                escalated=False,
                reason=f"cooldown active ({elapsed:.1f}/{policy.cooldown_minutes} min)",
                consecutive_failures=state.consecutive_failures,
            )

    return EscalationResult(
        pipeline=state.pipeline,
        escalated=True,
        reason=f"{state.consecutive_failures} consecutive failures exceed threshold {policy.failure_threshold}",
        consecutive_failures=state.consecutive_failures,
        secondary_webhook=policy.secondary_webhook,
    )


def update_state(state: EscalationState, healthy: bool, now: Optional[datetime] = None) -> EscalationState:
    now = now or _utcnow()
    if healthy:
        return EscalationState(pipeline=state.pipeline)
    new_failures = state.consecutive_failures + 1
    return EscalationState(
        pipeline=state.pipeline,
        consecutive_failures=new_failures,
        last_escalated=state.last_escalated,
        escalation_count=state.escalation_count,
    )
