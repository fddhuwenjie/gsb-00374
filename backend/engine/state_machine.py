from typing import Optional, Dict, Any, List, Literal

PersistedStatus = Literal[
    'queued', 'running', 'pausing', 'paused',
    'retry_wait', 'awaiting_approval',
    'succeeded', 'failed', 'cancelled',
]

ALL_STATES = {
    'queued', 'running', 'pausing', 'paused',
    'retry_wait', 'awaiting_approval',
    'succeeded', 'failed', 'cancelled',
}

TERMINAL_STATES = {'succeeded', 'failed', 'cancelled'}

# Legal transition table. Every persisted status change must go through
# ExecutionStateMachine.transition(), which validates against this table and
# rejects anything not listed here.
ALLOWED_TRANSITIONS: Dict[str, set] = {
    'queued':     {'running', 'cancelled'},
    # running -> queued: process restart requeue (resume from last boundary)
    'running':    {'pausing', 'retry_wait', 'awaiting_approval',
                   'succeeded', 'failed', 'cancelled', 'queued'},
    'pausing':    {'paused', 'running', 'cancelled', 'failed', 'queued'},
    'paused':     {'running', 'cancelled'},
    'retry_wait': {'running', 'cancelled', 'failed', 'queued'},
    # awaiting_approval -> running: approved; -> failed: rejected/expired;
    # -> retry_wait: a sibling branch failed while approval was pending
    'awaiting_approval': {'running', 'failed', 'cancelled', 'queued', 'retry_wait'},
    'failed':     {'queued'},
    'succeeded':  set(),
    'cancelled':  set(),
}

# Which statuses each client command may act on. Anything else is rejected
# (stale / out-of-date request) and can never spawn a runner.
COMMAND_ALLOWED_STATUSES: Dict[str, set] = {
    'pause':  {'running'},
    'resume': {'paused', 'pausing'},
    'cancel': {'queued', 'running', 'pausing', 'paused', 'retry_wait',
               'awaiting_approval'},
    'retry':  {'failed'},
    # approve/reject additionally require a matching pending approval token;
    # 'running' is included for the parallel-branch case where one approval
    # was already released while another is still pending.
    'approve': {'awaiting_approval', 'running'},
    'reject':  {'awaiting_approval', 'running'},
}

ALL_COMMANDS = set(COMMAND_ALLOWED_STATUSES.keys())


class IllegalTransitionError(Exception):
    pass


class ExecutionStateMachine:
    """Status state machine bound to a persistent journal.

    Every transition is validated against ALLOWED_TRANSITIONS, then appended
    to the journal with a monotonic sequence number and the previous status.
    Illegal transitions raise IllegalTransitionError and are never persisted.
    """

    def __init__(self, journal):
        self._journal = journal
        self._status: str = journal.current_status()

    @property
    def status(self) -> str:
        return self._status

    def is_terminal(self) -> bool:
        return self._status in TERMINAL_STATES

    def allowed_transitions(self) -> List[str]:
        return sorted(ALLOWED_TRANSITIONS.get(self._status, set()))

    def allowed_commands(self) -> List[str]:
        return sorted(
            cmd for cmd, statuses in COMMAND_ALLOWED_STATUSES.items()
            if self._status in statuses
        )

    def can_transition(self, to_status: str) -> bool:
        return to_status in ALLOWED_TRANSITIONS.get(self._status, set())

    def transition(self, to_status: str, reason: Optional[str] = None,
                   node_id: Optional[str] = None,
                   extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        if to_status not in ALL_STATES:
            raise IllegalTransitionError(f"Unknown status: {to_status}")
        from_status = self._status
        if not self.can_transition(to_status):
            raise IllegalTransitionError(
                f"Illegal transition: {from_status} -> {to_status}"
            )
        event = self._journal.append_status_event(
            from_status=from_status,
            to_status=to_status,
            reason=reason,
            node_id=node_id,
            extra=extra,
        )
        self._status = to_status
        return event
