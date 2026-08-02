from typing import Dict, List, Optional, Set

from models.flow import ExecutionStatus, ControlAction


QUEUED: ExecutionStatus = 'queued'
RUNNING: ExecutionStatus = 'running'
PAUSING: ExecutionStatus = 'pausing'
PAUSED: ExecutionStatus = 'paused'
RETRY_WAIT: ExecutionStatus = 'retry_wait'
AWAITING_APPROVAL: ExecutionStatus = 'awaiting_approval'
SUCCEEDED: ExecutionStatus = 'succeeded'
FAILED: ExecutionStatus = 'failed'
CANCELLED: ExecutionStatus = 'cancelled'

TERMINAL_STATES: Set[ExecutionStatus] = {SUCCEEDED, FAILED, CANCELLED}
PERSISTENT_STATES: Set[ExecutionStatus] = {
    PAUSED, RETRY_WAIT, AWAITING_APPROVAL, SUCCEEDED, FAILED, CANCELLED,
}

LEGAL_TRANSITIONS: Dict[ExecutionStatus, Set[ExecutionStatus]] = {
    QUEUED: {RUNNING, CANCELLED},
    RUNNING: {PAUSING, PAUSED, RETRY_WAIT, AWAITING_APPROVAL, SUCCEEDED, FAILED, CANCELLED},
    PAUSING: {PAUSED, RUNNING, SUCCEEDED, FAILED, CANCELLED},
    PAUSED: {RUNNING, CANCELLED},
    RETRY_WAIT: {RUNNING, CANCELLED, FAILED},
    AWAITING_APPROVAL: {RUNNING, FAILED, CANCELLED},
    SUCCEEDED: set(),
    FAILED: set(),
    CANCELLED: set(),
}

ALLOWED_ACTIONS: Dict[ExecutionStatus, List[ControlAction]] = {
    QUEUED: ['cancel'],
    RUNNING: ['pause', 'cancel'],
    PAUSING: ['resume', 'cancel'],
    PAUSED: ['resume', 'cancel', 'step'],
    RETRY_WAIT: ['resume', 'cancel'],
    AWAITING_APPROVAL: ['cancel'],
    SUCCEEDED: [],
    FAILED: [],
    CANCELLED: [],
}

COMMAND_TRANSITIONS: Dict[ControlAction, Dict[ExecutionStatus, ExecutionStatus]] = {
    'pause': {
        RUNNING: PAUSING,
        PAUSING: PAUSING,
        PAUSED: PAUSED,
    },
    'resume': {
        PAUSED: RUNNING,
        PAUSING: RUNNING,
        RETRY_WAIT: RUNNING,
    },
    'cancel': {
        QUEUED: CANCELLED,
        RUNNING: CANCELLED,
        PAUSING: CANCELLED,
        PAUSED: CANCELLED,
        RETRY_WAIT: CANCELLED,
        AWAITING_APPROVAL: CANCELLED,
    },
    'step': {
        PAUSED: RUNNING,
    },
}

SIDE_EFFECT_NODE_TYPES = {'http', 'file', 'sql'}
INTERRUPTIBLE_NODE_TYPES = {'wait'}


class IllegalTransitionError(Exception):
    def __init__(self, from_state: ExecutionStatus, to_state: ExecutionStatus):
        self.from_state = from_state
        self.to_state = to_state
        super().__init__(
            f"Illegal state transition: {from_state} -> {to_state}"
        )


def can_transition(from_state: ExecutionStatus, to_state: ExecutionStatus) -> bool:
    return to_state in LEGAL_TRANSITIONS.get(from_state, set())


def assert_transition(from_state: ExecutionStatus, to_state: ExecutionStatus) -> None:
    if not can_transition(from_state, to_state):
        raise IllegalTransitionError(from_state, to_state)


def allowed_actions(state: ExecutionStatus) -> List[ControlAction]:
    return list(ALLOWED_ACTIONS.get(state, []))


def is_terminal(state: ExecutionStatus) -> bool:
    return state in TERMINAL_STATES


def derive_state(events: list) -> ExecutionStatus:
    state: ExecutionStatus = QUEUED
    for event in events:
        if event.eventType == 'transition' and event.toState is not None:
            state = event.toState
    return state


def target_for_command(command: ControlAction, state: ExecutionStatus) -> Optional[ExecutionStatus]:
    return COMMAND_TRANSITIONS.get(command, {}).get(state)


def is_side_effect_node(node_type: str) -> bool:
    return node_type in SIDE_EFFECT_NODE_TYPES


def is_interruptible_node(node) -> bool:
    if node.data.interruptible is not None:
        return bool(node.data.interruptible)
    return node.type in INTERRUPTIBLE_NODE_TYPES


def idempotency_key(execution_id: str, node_id: str, attempt: int) -> str:
    return f"{execution_id}:{node_id}:{attempt}"


def branch_idempotency_key(execution_id: str, node_id: str, attempt: int, generation: int) -> str:
    return f"{execution_id}:{node_id}:{attempt}:g{generation}"
