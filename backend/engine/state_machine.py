from typing import Dict, Set


QUEUED = "queued"
RUNNING = "running"
PAUSING = "pausing"
PAUSED = "paused"
AWAITING_APPROVAL = "awaiting_approval"
RETRY_WAIT = "retry_wait"
SUCCEEDED = "succeeded"
FAILED = "failed"
CANCELLED = "cancelled"

TERMINAL_STATES: Set[str] = {SUCCEEDED, FAILED, CANCELLED}


ALLOWED_TRANSITIONS: Dict[str, Set[str]] = {
    QUEUED: {RUNNING, CANCELLED, FAILED},
    RUNNING: {PAUSING, PAUSED, AWAITING_APPROVAL, RETRY_WAIT, SUCCEEDED, FAILED, CANCELLED},
    PAUSING: {PAUSED, RUNNING, FAILED, CANCELLED},
    PAUSED: {RUNNING, CANCELLED, FAILED},
    AWAITING_APPROVAL: {RUNNING, FAILED, CANCELLED},
    RETRY_WAIT: {RUNNING, FAILED, CANCELLED},
    SUCCEEDED: set(),
    FAILED: {QUEUED},
    CANCELLED: {QUEUED},
}


COMMAND_TARGETS: Dict[str, Set[str]] = {
    "start": {QUEUED},
    "pause": {RUNNING, PAUSING},
    "resume": {PAUSED, RETRY_WAIT, QUEUED},
    "cancel": {QUEUED, RUNNING, PAUSING, PAUSED, RETRY_WAIT, AWAITING_APPROVAL},
    "retry": {FAILED, CANCELLED},
    "approve": {AWAITING_APPROVAL},
    "reject": {AWAITING_APPROVAL},
}


class IllegalTransitionError(Exception):
    def __init__(self, from_state: str, to_state: str):
        super().__init__(f"Illegal state transition: {from_state} -> {to_state}")
        self.from_state = from_state
        self.to_state = to_state


def can_transition(from_state: str, to_state: str) -> bool:
    return to_state in ALLOWED_TRANSITIONS.get(from_state, set())


def assert_transition(from_state: str, to_state: str) -> None:
    if not can_transition(from_state, to_state):
        raise IllegalTransitionError(from_state, to_state)


def command_allowed(command: str, current_state: str) -> bool:
    return current_state in COMMAND_TARGETS.get(command, set())


def command_target_state(command: str) -> str:
    mapping = {
        "start": RUNNING,
        "pause": PAUSING,
        "resume": RUNNING,
        "cancel": CANCELLED,
        "retry": QUEUED,
        "approve": RUNNING,
        "reject": FAILED,
    }
    return mapping[command]


def is_terminal(state: str) -> bool:
    return state in TERMINAL_STATES


def allowed_actions(state: str) -> Set[str]:
    actions: Set[str] = set()
    for command, targets in COMMAND_TARGETS.items():
        if state in targets:
            actions.add(command)
    return actions
