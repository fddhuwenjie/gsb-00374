"""The execution state machine: states, legal transitions and the command map.

States (all persisted, none inferred from memory):

    queued      -> execution has been created and persisted, not yet started
    running     -> an executor owns the execution and is advancing nodes
    pausing     -> a pause was requested while an uninterruptible node is running;
                   the executor will finish + flush the current node boundary and
                   only then move to ``paused``
    paused      -> execution is durably paused at a completed node boundary
    awaiting_approval -> a human approval node is blocking; the execution is
                   suspended (durably, with a deadline) until a decision arrives,
                   a timeout expires, or the execution is cancelled
    retry_wait  -> a node failed and is waiting for its backoff delay before the
                   next attempt
    succeeded   -> terminal: reached the End node
    failed      -> terminal: a node exhausted its retries (or a fatal error)
    cancelled   -> terminal: a cancel command was honoured

The transition table is the single source of truth. The runner, the recovery
logic, the REST control endpoints and the frontend button state are all driven
by it, so the UI can never offer a command the server would reject.
"""

from __future__ import annotations

from typing import Dict, Set


class ExecState:
    QUEUED = "queued"
    RUNNING = "running"
    PAUSING = "pausing"
    PAUSED = "paused"
    AWAITING_APPROVAL = "awaiting_approval"
    RETRY_WAIT = "retry_wait"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"

    ALL: Set[str] = {
        QUEUED, RUNNING, PAUSING, PAUSED, AWAITING_APPROVAL, RETRY_WAIT,
        SUCCEEDED, FAILED, CANCELLED,
    }
    TERMINAL: Set[str] = {SUCCEEDED, FAILED, CANCELLED}


class TransitionError(Exception):
    """Raised when an illegal state transition is attempted."""

    def __init__(self, from_state: str, to_state: str):
        self.from_state = from_state
        self.to_state = to_state
        super().__init__(f"Illegal transition: {from_state} -> {to_state}")


# The complete, closed set of legal transitions. Anything not listed is illegal.
ALLOWED_TRANSITIONS: Dict[str, Set[str]] = {
    ExecState.QUEUED: {ExecState.RUNNING, ExecState.CANCELLED},
    ExecState.RUNNING: {
        ExecState.PAUSING,     # pause requested mid uninterruptible node
        ExecState.PAUSED,      # pause honoured at a boundary directly
        ExecState.AWAITING_APPROVAL,  # hit a human approval node
        ExecState.RETRY_WAIT,  # node failed, waiting to retry
        ExecState.SUCCEEDED,
        ExecState.FAILED,
        ExecState.CANCELLED,
    },
    ExecState.PAUSING: {
        ExecState.PAUSED,      # boundary flushed, now durably paused
        ExecState.CANCELLED,
        ExecState.FAILED,      # the in-flight node ultimately failed
    },
    ExecState.PAUSED: {
        ExecState.RUNNING,     # resume
        ExecState.CANCELLED,
    },
    ExecState.AWAITING_APPROVAL: {
        ExecState.RUNNING,     # approved -> resume the flow
        ExecState.FAILED,      # rejected or timed out
        ExecState.CANCELLED,   # execution cancelled while awaiting
    },
    ExecState.RETRY_WAIT: {
        ExecState.RUNNING,     # backoff elapsed, retrying
        ExecState.PAUSING,     # pause requested during backoff
        ExecState.PAUSED,
        ExecState.FAILED,      # retries exhausted
        ExecState.CANCELLED,
    },
    ExecState.SUCCEEDED: set(),
    ExecState.FAILED: set(),
    ExecState.CANCELLED: set(),
}


# User/client commands mapped to the *intent* target used for button gating.
# The runner may route through intermediate states (e.g. pause -> pausing), but
# for surfacing "which buttons are allowed", these are the commands accepted in
# each state.
COMMAND_PRECONDITIONS: Dict[str, Set[str]] = {
    "start": {ExecState.QUEUED},
    "pause": {ExecState.RUNNING, ExecState.RETRY_WAIT},
    "resume": {ExecState.PAUSED},
    "approve": {ExecState.AWAITING_APPROVAL},
    "reject": {ExecState.AWAITING_APPROVAL},
    "cancel": {
        ExecState.QUEUED, ExecState.RUNNING, ExecState.PAUSING,
        ExecState.PAUSED, ExecState.AWAITING_APPROVAL, ExecState.RETRY_WAIT,
    },
}


def is_terminal(state: str) -> bool:
    return state in ExecState.TERMINAL


def allowed_targets(state: str) -> Set[str]:
    return set(ALLOWED_TRANSITIONS.get(state, set()))


def is_legal(from_state: str, to_state: str) -> bool:
    return to_state in ALLOWED_TRANSITIONS.get(from_state, set())


def check_transition(from_state: str, to_state: str) -> None:
    if to_state not in ExecState.ALL:
        raise TransitionError(from_state, to_state)
    if not is_legal(from_state, to_state):
        raise TransitionError(from_state, to_state)


def allowed_commands(state: str) -> Set[str]:
    """Return the set of client commands that are legal in ``state``.

    The frontend uses this so its buttons are driven entirely by the server's
    view of legal transitions rather than by client-side guesses.
    """
    return {cmd for cmd, states in COMMAND_PRECONDITIONS.items() if state in states}
