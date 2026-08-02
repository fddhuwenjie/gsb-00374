"""Durable, recoverable, provably-idempotent workflow execution engine.

This package is a self-contained subsystem that layers a persistent, crash-safe
execution state machine on top of a simple flow model. It is intentionally kept
separate from the in-memory debugging executor in ``engine/executor.py`` so that
the time-travel / breakpoint features remain untouched.

Key guarantees:
  * Every state transition is appended to a monotonic, append-only event log that
    records the previous state and a monotonically increasing sequence number.
  * Illegal state transitions are rejected.
  * Recovery reconstructs state purely from the persisted event log and node
    boundary checkpoints -- never from in-memory task state.
  * Side effects (HTTP, file writes, ...) are guarded by an idempotency key
    derived from ``executionId + nodeId + attempt`` and a durable ledger, so a
    restart never repeats an already-succeeded side effect.
"""

from engine.durable.state_machine import (
    ExecState,
    TransitionError,
    ALLOWED_TRANSITIONS,
    is_terminal,
    allowed_targets,
    allowed_commands,
)

__all__ = [
    "ExecState",
    "TransitionError",
    "ALLOWED_TRANSITIONS",
    "is_terminal",
    "allowed_targets",
    "allowed_commands",
]
